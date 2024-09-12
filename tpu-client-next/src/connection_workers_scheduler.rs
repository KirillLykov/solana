//! This module defines [`ConnectionWorkersScheduler`] which creates and
//! orchestrates `ConnectionWorker` instances.
use {
    super::{leader_updater::LeaderUpdater, SendTransactionStatsPerAddr},
    crate::{
        connection_worker::ConnectionWorker,
        quic_networking::{
            create_client_config, create_client_endpoint, QuicClientCertificate, QuicError,
        },
        transaction_batch::TransactionBatch,
        workers_cache::{WorkerInfo, WorkersCache, WorkersCacheError},
    },
    log::*,
    quinn::Endpoint,
    solana_sdk::signature::Keypair,
    std::{net::SocketAddr, sync::Arc},
    thiserror::Error,
    tokio::sync::mpsc,
};

/// Size of the channel to transmit transaction batches to the target workers.
const WORKER_CHANNEL_SIZE: usize = 2;

/// [`ConnectionWorkersScheduler`] is responsible for managing and scheduling
/// connection workers that handle transactions over the network.
pub struct ConnectionWorkersScheduler;

#[derive(Debug, Error, PartialEq)]
pub enum ConnectionWorkersSchedulerError {
    #[error(transparent)]
    QuicError(#[from] QuicError),
    #[error(transparent)]
    WorkersCacheError(#[from] WorkersCacheError),
    #[error("Leader receiver unexpectedly dropped.")]
    LeaderReceiverDropped,
}

impl ConnectionWorkersScheduler {
    /// Runs the main loop that handles worker scheduling and management for
    /// connections. Returns the error quic statistics per connection address or
    /// an error if something goes wrong. Importantly, if some transactions were
    /// not delivered due to network problems, they will not be retried when the
    /// problem is resolved.
    pub async fn run(
        bind: SocketAddr,
        validator_identity: Option<Keypair>,
        mut leader_updater: Box<dyn LeaderUpdater>,
        mut transaction_receiver: mpsc::Receiver<TransactionBatch>,
        num_connections: usize,
    ) -> Result<SendTransactionStatsPerAddr, ConnectionWorkersSchedulerError> {
        let endpoint = Self::setup_endpoint(bind, validator_identity)?;
        debug!("Client endpoint bind address: {:?}", endpoint.local_addr());
        let mut workers = WorkersCache::new(num_connections);
        loop {
            let Some(transaction_batch) = transaction_receiver.recv().await else {
                info!("Transaction generator has stopped, stopping ConnectionWorkersScheduler.");
                break;
            };
            let updated_leaders = leader_updater.get_leaders();
            let new_leader = &updated_leaders[0];
            let future_leaders = &updated_leaders[1..];
            if !workers.contains(new_leader) {
                debug!("Could not find pre-fetch leader worker for {new_leader:?}.");
                let worker = Self::spawn_worker(&endpoint, new_leader).await;
                workers.push(*new_leader, worker).await;
            }

            if let Err(error) = workers
                .send_txs_to_worker(new_leader, transaction_batch)
                .await
            {
                warn!(
                    "Connection to the leader {new_leader} has been closed, worker error: {error}",
                );
                // if we has failed to send batch, it will be dropped.
            }

            // regardless of who is leader, add future leaders to the cache to
            // hide the latency of opening the connection.
            for peer in future_leaders {
                if !workers.contains(peer) {
                    let worker = Self::spawn_worker(&endpoint, peer).await;
                    workers.push(*peer, worker).await;
                }
            }
        }
        workers.close().await;

        endpoint.close(0u32.into(), b"Closing connection");
        leader_updater.stop_and_join().await;
        Ok(workers.get_transaction_stats().clone())
    }

    /// Sets up the QUIC endpoint for the scheduler to handle connections.
    fn setup_endpoint(
        bind: SocketAddr,
        validator_identity: Option<Keypair>,
    ) -> Result<Endpoint, ConnectionWorkersSchedulerError> {
        let client_certificate = if let Some(validator_identity) = validator_identity {
            Arc::new(QuicClientCertificate::new(&validator_identity))
        } else {
            Arc::new(QuicClientCertificate::default())
        };
        let client_config = create_client_config(client_certificate);
        let endpoint = create_client_endpoint(bind, client_config)?;
        Ok(endpoint)
    }

    /// Spawns a worker to handle communication with a given peer.
    async fn spawn_worker(endpoint: &Endpoint, peer: &SocketAddr) -> WorkerInfo {
        let (txs_sender, txs_receiver) = mpsc::channel(WORKER_CHANNEL_SIZE);
        let endpoint = endpoint.clone();
        let peer = *peer;
        let handle = tokio::spawn(async move {
            let mut new_connection_worker = ConnectionWorker::new(endpoint, peer, txs_receiver);
            new_connection_worker.run().await;
            new_connection_worker.get_transaction_stats().clone()
        });
        WorkerInfo::new(txs_sender, handle)
    }
}

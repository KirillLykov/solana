//! This module defines `ConnectionWorker` which encapsulates the
//! functionality needed to handle one connection within the scope of task.
use {
    super::SendTransactionStats,
    crate::{
        quic_networking::send_data_over_stream, send_transaction_stats::record_error,
        transaction_batch::TransactionBatch,
    },
    log::*,
    quinn::{ConnectError, Connection, Endpoint},
    solana_measure::measure::Measure,
    solana_sdk::{
        clock::{DEFAULT_MS_PER_SLOT, MAX_PROCESSING_AGE, NUM_CONSECUTIVE_LEADER_SLOTS},
        timing::timestamp,
    },
    std::net::SocketAddr,
    tokio::{
        sync::mpsc,
        time::{sleep, Duration},
    },
};

/// Maximum number of reconnection attempts in case the connection errors out.
const MAX_RECONNECT_ATTEMPTS: usize = 4;

/// Interval between retry attempts for creating a new connection. This value is
/// a best-effort estimate, based on current network conditions.
const RETRY_SLEEP_INTERVAL: Duration =
    Duration::from_millis(NUM_CONSECUTIVE_LEADER_SLOTS * DEFAULT_MS_PER_SLOT);

/// Maximum age (in milliseconds) of a blockhash, beyond which transaction
/// batches are dropped.
const MAX_PROCESSING_AGE_MS: u64 = MAX_PROCESSING_AGE as u64 * DEFAULT_MS_PER_SLOT;

/// [`ConnectionState`] represents the current state of a quic connection. This
/// enum tracks the lifecycle of connection from initial setup to closing phase.
/// The transition function between states is defined in `ConnectionWorker`
/// implementation.
enum ConnectionState {
    NotSetup,
    Active(Connection),
    Retry(usize),
    Closing,
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        if let Self::Active(connection) = self {
            info!(
                "Close connection with {:?}, stats: {:?}. All pending streams will be dropped.",
                connection.remote_address(),
                connection.stats()
            );
            // no guarantee that all the streams will finish
            connection.close(0u32.into(), b"done");
        }
    }
}

/// [`ConnectionWorker`] holds connection to the validator with address `peer`. If
/// connection has been closed, [`ConnectionWorker`] tries to reconnect
/// [`MAX_RECONNECT_ATTEMPTS`] times. If connection is in `Active` state, it sends
/// transactions received from `transactions_receiver`. Additionally, it
/// accumulates statistics about connections and streams failures.
pub(crate) struct ConnectionWorker {
    endpoint: Endpoint,
    peer: SocketAddr,
    transactions_receiver: mpsc::Receiver<TransactionBatch>,
    connection: ConnectionState,
    send_txs_stats: SendTransactionStats,
}

impl ConnectionWorker {
    pub fn new(
        endpoint: Endpoint,
        peer: SocketAddr,
        command_receiver: mpsc::Receiver<TransactionBatch>,
    ) -> Self {
        Self {
            endpoint,
            peer,
            transactions_receiver: command_receiver,
            connection: ConnectionState::NotSetup,
            send_txs_stats: SendTransactionStats::default(),
        }
    }

    /// Starts the main loop of the [`ConnectionWorker`]. This method manages the
    /// connection to the peer and handles state transitions. It runs
    /// indefinitely until the connection is closed or an unrecoverable error
    /// occurs.
    pub async fn run(&mut self) {
        loop {
            match &self.connection {
                ConnectionState::Closing => {
                    break;
                }
                ConnectionState::NotSetup => {
                    self.create_connection(0).await;
                }
                ConnectionState::Active(connection) => {
                    let Some(transactions) = self.transactions_receiver.recv().await else {
                        info!("Transactions sender has been dropped.");
                        self.connection = ConnectionState::Closing;
                        continue;
                    };
                    self.send_transactions(connection.clone(), transactions)
                        .await;
                }
                ConnectionState::Retry(num_reconnects) => {
                    if *num_reconnects > MAX_RECONNECT_ATTEMPTS {
                        error!(
                            "Didn't manage to establish connection: reach max reconnect attempts."
                        );
                        self.connection = ConnectionState::Closing;
                        continue;
                    }
                    sleep(RETRY_SLEEP_INTERVAL).await;
                    self.reconnect(*num_reconnects).await;
                }
            }
        }
    }

    /// Retrieves the statistics for transactions sent by this worker.
    pub fn get_transaction_stats(&self) -> &SendTransactionStats {
        &self.send_txs_stats
    }

    /// Sends a batch of transactions using the provided `connection`. Each
    /// transaction in the batch is sent over the QUIC streams one at the time,
    /// which prevents traffic fragmentation and shows better TPS in comparison
    /// with multistream send. If the batch is determined to be outdated, it
    /// will be dropped without being sent. In case of error, it doesn't retry
    /// to send the same transactions again.
    async fn send_transactions(&mut self, connection: Connection, transactions: TransactionBatch) {
        let now = timestamp();
        if now.saturating_sub(transactions.get_timestamp()) > MAX_PROCESSING_AGE_MS {
            info!("Drop outdated transaction batch.");
            return;
        }
        let mut measure_send = Measure::start("send transaction batch");
        for data in transactions.into_iter() {
            let result = send_data_over_stream(&connection, &data).await;

            if let Err(error) = result {
                record_error(error, &mut self.send_txs_stats);
                self.connection = ConnectionState::Retry(0);
            } else {
                self.send_txs_stats.successfully_sent =
                    self.send_txs_stats.successfully_sent.saturating_add(1);
            }
        }
        measure_send.stop();
        info!(
            "Time to send transactions batch: {} us",
            measure_send.as_us()
        );
    }

    /// Attempts to create a new connection to the specified `peer` address. If
    /// the connection is successful, the state is updated to `Active`. If an
    /// error occurs, the state may transition to `Retry` or `Closing`,
    /// depending on the nature of the error.
    async fn create_connection(&mut self, num_reconnects: usize) {
        let connecting = self.endpoint.connect(self.peer, "connect");
        match connecting {
            Ok(connecting) => {
                let mut measure_connection = Measure::start("establish connection");
                let res = connecting.await;
                measure_connection.stop();
                info!(
                    "Establishing connection with {} took: {} us",
                    self.peer,
                    measure_connection.as_us()
                );
                match res {
                    Ok(connection) => {
                        self.connection = ConnectionState::Active(connection);
                    }
                    Err(err) => {
                        warn!("Connection error {}: {}", self.peer, err);
                        record_error(err.into(), &mut self.send_txs_stats);
                        self.connection = ConnectionState::Retry(num_reconnects.saturating_add(1));
                    }
                }
            }
            Err(connecting_error) => {
                record_error(connecting_error.clone().into(), &mut self.send_txs_stats);
                match connecting_error {
                    ConnectError::EndpointStopping => {
                        info!("Endpoint stopping, exit connection worker.");
                        self.connection = ConnectionState::Closing;
                    }
                    ConnectError::InvalidRemoteAddress(_) => {
                        warn!("Invalid remote address.");
                        self.connection = ConnectionState::Closing;
                    }
                    ConnectError::TooManyConnections => {
                        warn!("Too many connections have been opened. Wait.");
                        self.connection = ConnectionState::Retry(num_reconnects.saturating_add(1));
                    }
                    e => {
                        error!("Unexpected error has happen while trying to create connection {e}");
                        self.connection = ConnectionState::Closing;
                    }
                }
            }
        }
    }

    /// Attempts to reconnect to the peer after a connection failure.
    async fn reconnect(&mut self, num_reconnects: usize) {
        info!("Trying to reconnect. Reopen connection, 0rtt is not implemented yet.");
        // We can reconnect using 0rtt, but not a priority for now. Check if we
        // need to call config.enable_0rtt() on the client side and where
        // session tickets are stored.
        self.create_connection(num_reconnects).await;
    }
}

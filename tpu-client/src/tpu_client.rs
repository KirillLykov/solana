use core::time;

use log::debug;

pub use crate::nonblocking::tpu_client::TpuSenderError;
use {
    crate::nonblocking::tpu_client::TpuClient as NonblockingTpuClient,
    rayon::iter::{IntoParallelIterator, ParallelIterator},
    solana_client_traits::AsyncClient,
    solana_clock::Slot,
    solana_connection_cache::{
        client_connection::ClientConnection,
        connection_cache::{
            ConnectionCache, ConnectionManager, ConnectionPool, NewConnectionConfig,
        },
    },
    solana_rpc_client::rpc_client::RpcClient,
    solana_signature::Signature,
    solana_time_utils::timestamp,
    solana_transaction::{versioned::VersionedTransaction, Transaction},
    solana_transaction_error::{TransportError, TransportResult},
    std::{
        collections::VecDeque,
        sync::{Arc, RwLock},
    },
};
#[cfg(feature = "spinner")]
use {
    solana_message::Message, solana_signer::signers::Signers,
    solana_transaction_error::TransactionError, tokio::time::Duration,
};

pub const DEFAULT_TPU_ENABLE_UDP: bool = false;
pub const DEFAULT_TPU_USE_QUIC: bool = true;
pub const DEFAULT_VOTE_USE_QUIC: bool = false;

/// The default connection count is set to 1 -- it should
/// be sufficient for most use cases. Validators can use
/// --tpu-connection-pool-size to override this default value.
pub const DEFAULT_TPU_CONNECTION_POOL_SIZE: usize = 1;

pub type Result<T> = std::result::Result<T, TpuSenderError>;

/// Send at ~100 TPS
#[cfg(feature = "spinner")]
pub(crate) const SEND_TRANSACTION_INTERVAL: Duration = Duration::from_millis(10);
/// Retry batch send after 4 seconds
#[cfg(feature = "spinner")]
pub(crate) const TRANSACTION_RESEND_INTERVAL: Duration = Duration::from_secs(4);

/// Default number of slots used to build TPU socket fanout set
pub const DEFAULT_FANOUT_SLOTS: u64 = 12;

/// Maximum number of slots used to build TPU socket fanout set
pub const MAX_FANOUT_SLOTS: u64 = 100;

/// Config params for `TpuClient`
#[derive(Clone, Debug)]
pub struct TpuClientConfig {
    /// The range of upcoming slots to include when determining which
    /// leaders to send transactions to (min: 1, max: `MAX_FANOUT_SLOTS`)
    pub fanout_slots: u64,
}

impl Default for TpuClientConfig {
    fn default() -> Self {
        Self {
            fanout_slots: DEFAULT_FANOUT_SLOTS,
        }
    }
}

/// Client which sends transactions directly to the current leader's TPU port over UDP.
/// The client uses RPC to determine the current leader and fetch node contact info
pub struct TpuClient<
    P, // ConnectionPool
    M, // ConnectionManager
    C, // NewConnectionConfig
> {
    rpc_client: Arc<RpcClient>,
    tpu_client: Arc<NonblockingTpuClient<P, M, C>>,
}

impl<P, M, C> TpuClient<P, M, C>
where
    P: ConnectionPool<NewConnectionConfig = C>,
    M: ConnectionManager<ConnectionPool = P, NewConnectionConfig = C>,
    C: NewConnectionConfig,
{
    /// Serialize and send transaction to the current and upcoming leader TPUs according to fanout
    /// size
    pub fn send_transaction(&self, transaction: &Transaction) -> bool {
        self.invoke(self.tpu_client.send_transaction(transaction))
    }

    /// Send a wire transaction to the current and upcoming leader TPUs according to fanout size
    pub fn send_wire_transaction(&self, wire_transaction: Vec<u8>) -> bool {
        self.invoke(self.tpu_client.send_wire_transaction(wire_transaction))
    }

    /// Serialize and send transaction to the current and upcoming leader TPUs according to fanout
    /// size
    /// Returns the last error if all sends fail
    pub fn try_send_transaction(&self, transaction: &Transaction) -> TransportResult<()> {
        self.invoke(self.tpu_client.try_send_transaction(transaction))
    }

    /// Serialize and send transaction to the current and upcoming leader TPUs according to fanout.
    ///
    /// Returns an error if:
    /// 1. there are no known tpu sockets to send to
    /// 2. any of the sends fail, even if other sends succeeded.
    pub fn send_transaction_to_upcoming_leaders(
        &self,
        transaction: &Transaction,
    ) -> TransportResult<()> {
        let wire_transaction =
            Arc::new(bincode::serialize(&transaction).expect("should serialize transaction"));

        let leaders = self
            .tpu_client
            .get_leader_tpu_service()
            .unique_leader_tpu_sockets(self.tpu_client.get_fanout_slots());

        let mut last_error: Option<TransportError> = None;
        let mut some_success = false;
        for tpu_address in &leaders {
            let cache = self.tpu_client.get_connection_cache();
            let conn = cache.get_connection(tpu_address);
            if let Err(err) = conn.send_data_async(wire_transaction.clone()) {
                last_error = Some(err);
            } else {
                some_success = true;
            }
        }

        if let Some(err) = last_error {
            Err(err)
        } else if !some_success {
            Err(std::io::Error::other("No sends attempted").into())
        } else {
            Ok(())
        }
    }

    /// Serialize and send a batch of transactions to the current and upcoming leader TPUs according
    /// to fanout size
    /// Returns the last error if all sends fail
    pub fn try_send_transaction_batch(&self, transactions: &[Transaction]) -> TransportResult<()> {
        let wire_transactions = transactions
            .into_par_iter()
            .map(|tx| bincode::serialize(&tx).expect("serialize Transaction in send_batch"))
            .collect::<Vec<_>>();
        self.invoke(
            self.tpu_client
                .try_send_wire_transaction_batch(wire_transactions),
        )
    }

    /// Send a wire transaction to the current and upcoming leader TPUs according to fanout size
    /// Returns the last error if all sends fail
    pub fn try_send_wire_transaction(&self, wire_transaction: Vec<u8>) -> TransportResult<()> {
        self.invoke(self.tpu_client.try_send_wire_transaction(wire_transaction))
    }

    pub fn try_send_wire_transaction_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> TransportResult<()> {
        self.invoke(
            self.tpu_client
                .try_send_wire_transaction_batch(wire_transactions),
        )
    }

    /// Create a new client that disconnects when dropped
    pub fn new(
        name: &'static str,
        rpc_client: Arc<RpcClient>,
        websocket_url: &str,
        config: TpuClientConfig,
        connection_manager: M,
    ) -> Result<Self> {
        let create_tpu_client = NonblockingTpuClient::new(
            name,
            rpc_client.get_inner_client().clone(),
            websocket_url,
            config,
            connection_manager,
        );
        let tpu_client =
            tokio::task::block_in_place(|| rpc_client.runtime().block_on(create_tpu_client))?;

        Ok(Self {
            rpc_client,
            tpu_client: Arc::new(tpu_client),
        })
    }

    /// Create a new client that disconnects when dropped
    pub fn new_with_connection_cache(
        rpc_client: Arc<RpcClient>,
        websocket_url: &str,
        config: TpuClientConfig,
        connection_cache: Arc<ConnectionCache<P, M, C>>,
    ) -> Result<Self> {
        let create_tpu_client = NonblockingTpuClient::new_with_connection_cache(
            rpc_client.get_inner_client().clone(),
            websocket_url,
            config,
            connection_cache,
        );
        let tpu_client =
            tokio::task::block_in_place(|| rpc_client.runtime().block_on(create_tpu_client))?;

        Ok(Self {
            rpc_client,
            tpu_client: Arc::new(tpu_client),
        })
    }

    #[cfg(feature = "spinner")]
    pub fn send_and_confirm_messages_with_spinner<T: Signers + ?Sized>(
        &self,
        messages: &[Message],
        signers: &T,
    ) -> Result<Vec<Option<TransactionError>>> {
        self.invoke(
            self.tpu_client
                .send_and_confirm_messages_with_spinner(messages, signers),
        )
    }

    pub fn rpc_client(&self) -> &RpcClient {
        &self.rpc_client
    }

    fn invoke<T, F: std::future::Future<Output = T>>(&self, f: F) -> T {
        // `block_on()` panics if called within an asynchronous execution context. Whereas
        // `block_in_place()` only panics if called from a current_thread runtime, which is the
        // lesser evil.
        tokio::task::block_in_place(move || self.rpc_client.runtime().block_on(f))
    }
}

// Methods below are required for calls to client.async_transfer()
// where client is of type TpuClient<P, M, C>
impl<P, M, C> AsyncClient for TpuClient<P, M, C>
where
    P: ConnectionPool<NewConnectionConfig = C>,
    M: ConnectionManager<ConnectionPool = P, NewConnectionConfig = C>,
    C: NewConnectionConfig,
{
    fn async_send_versioned_transaction(
        &self,
        transaction: VersionedTransaction,
    ) -> TransportResult<Signature> {
        let wire_transaction =
            bincode::serialize(&transaction).expect("serialize Transaction in send_batch");
        self.send_wire_transaction(wire_transaction);
        Ok(transaction.signatures[0])
    }

    fn async_send_versioned_transaction_batch(
        &self,
        batch: Vec<VersionedTransaction>,
    ) -> TransportResult<()> {
        let buffers = batch
            .into_par_iter()
            .map(|tx| bincode::serialize(&tx).expect("serialize Transaction in send_batch"))
            .collect::<Vec<_>>();
        self.try_send_wire_transaction_batch(buffers)?;
        Ok(())
    }
}

// 48 chosen because it's unlikely that 12 leaders in a row will miss their slots
const MAX_SLOT_SKIP_DISTANCE: u64 = 48;

#[derive(Clone, Debug)]
pub(crate) struct RecentLeaderSlots(Arc<RwLock<(VecDeque<(Slot, u64)>, IntervalWindow)>>);
impl RecentLeaderSlots {
    pub(crate) fn new(current_slot: Slot) -> Self {
        let mut recent_slots = VecDeque::new();
        recent_slots.push_back((current_slot, timestamp()));
        Self(Arc::new(RwLock::new((
            recent_slots,
            IntervalWindow::new(current_slot, 100),
        ))))
    }

    pub(crate) fn record_slot(&self, current_slot: Slot, is_start: bool) {
        let timestamp = timestamp();
        let mut recent_slots = self.0.write().unwrap();
        recent_slots.0.push_back((current_slot, timestamp));
        if is_start {
            recent_slots.1.start(current_slot, timestamp);
        } else {
            recent_slots.1.end(current_slot, timestamp);
        }
        // 12 recent slots should be large enough to avoid a misbehaving
        // validator from affecting the median recent slot
        while recent_slots.0.len() > 12 {
            recent_slots.0.pop_front();
        }
    }

    // Estimate the current slot from recent slot notifications.
    pub(crate) fn estimated_current_slot(&self) -> Slot {
        let (mut recent_slots, median_duration) = {
            let x = self.0.read().unwrap();
            let median_duration = x.1.median_duration();
            (x.0.iter().cloned().collect::<Vec<_>>(), median_duration)
        };
        assert!(!recent_slots.is_empty());
        recent_slots.sort_unstable();

        // Validators can broadcast invalid blocks that are far in the future
        // so check if the current slot is in line with the recent progression.
        let max_index = recent_slots.len() - 1;
        let median_index = max_index / 2;
        let median_recent_slot = recent_slots[median_index].0;
        let expected_current_slot = median_recent_slot + (max_index - median_index) as u64;
        let max_reasonable_current_slot = expected_current_slot + MAX_SLOT_SKIP_DISTANCE;

        let rs = recent_slots.clone();
        // Return the highest slot that doesn't exceed what we believe is a
        // reasonable slot.
        let (slot, slot_timestamp) = recent_slots
            .into_iter()
            .rev()
            .find(|(slot, _timestamp)| *slot <= max_reasonable_current_slot)
            .unwrap();
        debug!("@@@ estimated_current_slot: {slot}, recent_slots: {rs:?}, median_duration: {median_duration}");
        // TODO Doesn't work, not sure why
        if (timestamp() - slot_timestamp) as f64 > median_duration * 0.8 {
            debug!("@@@ correction + 1");
            slot + 1
        } else {
            slot
        }
    }
}

use std::collections::HashMap;

#[derive(Debug)]
struct IntervalWindow {
    active: HashMap<Slot, u64>,
    window: VecDeque<(Slot, u64)>,
    cap: usize,
}

impl IntervalWindow {
    fn new(current_slot: Slot, cap: usize) -> Self {
        let timestamp = timestamp();
        let mut active = HashMap::new();
        active.insert(current_slot, timestamp);
        let window = VecDeque::from([(current_slot, 400)]);
        Self {
            active,
            window,
            cap,
        }
    }

    fn start(&mut self, id: Slot, t: u64) {
        self.active.insert(id, t);
    }

    fn end(&mut self, id: Slot, t: u64) {
        if let Some(t0) = self.active.remove(&id) {
            let len = t.saturating_sub(t0);
            self.window.push_back((id, len));
            if self.window.len() > self.cap {
                self.window.pop_front();
            }
        }
    }

    fn median_duration(&self) -> f64 {
        assert!(!self.window.is_empty());
        let mut recent_slots: Vec<(Slot, u64)> = self.window.iter().copied().collect();
        recent_slots.sort_unstable();
        let mid = recent_slots.len() / 2;
        if recent_slots.len() % 2 == 1 {
            recent_slots[mid].1 as f64
        } else {
            (recent_slots[mid - 1].1 as f64 + recent_slots[mid].1 as f64) / 2.0
        }
    }
}

#[cfg(test)]
impl From<Vec<(Slot, u64)>> for RecentLeaderSlots {
    fn from(recent_slots: Vec<(Slot, u64)>) -> Self {
        assert!(!recent_slots.is_empty());
        Self(Arc::new(RwLock::new(recent_slots.into_iter().collect())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_slot(recent_slots: RecentLeaderSlots, expected_slot: Slot) {
        assert_eq!(recent_slots.estimated_current_slot(), expected_slot);
    }

    #[test]
    fn test_recent_leader_slots() {
        assert_slot(RecentLeaderSlots::new(0), 0);

        let mut recent_slots: Vec<Slot> = (1..=12).collect();
        assert_slot(RecentLeaderSlots::from(recent_slots.clone()), 12);

        recent_slots.reverse();
        assert_slot(RecentLeaderSlots::from(recent_slots), 12);

        assert_slot(
            RecentLeaderSlots::from(vec![0, 1 + MAX_SLOT_SKIP_DISTANCE]),
            1 + MAX_SLOT_SKIP_DISTANCE,
        );
        assert_slot(
            RecentLeaderSlots::from(vec![0, 2 + MAX_SLOT_SKIP_DISTANCE]),
            0,
        );

        assert_slot(RecentLeaderSlots::from(vec![1]), 1);
        assert_slot(RecentLeaderSlots::from(vec![1, 100]), 1);
        assert_slot(RecentLeaderSlots::from(vec![1, 2, 100]), 2);
        assert_slot(RecentLeaderSlots::from(vec![1, 2, 3, 100]), 3);
        assert_slot(RecentLeaderSlots::from(vec![1, 2, 3, 99, 100]), 3);
    }
}

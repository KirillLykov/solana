//! This module provides [`LeaderUpdater`] trait.
//!
//! Currently, the main purpose of [`LeaderUpdater`] is to abstract over leader
//! updates, hiding the details of how leaders are retrieved and which
//! structures are used.
//!
use {
    std::{fmt, net::SocketAddr},
    thiserror::Error,
};

/// [`LeaderUpdater`] trait abstracts out functionality required for the
/// [`ConnectionWorkersScheduler`](crate::ConnectionWorkersScheduler) to
/// identify next leaders to send transactions to.
pub trait LeaderUpdater: Send {
    /// Writes upcoming leaders starting from the current estimated slot into `leaders` and returns
    /// the populated prefix.
    ///
    /// Leaders are returned per [`NUM_CONSECUTIVE_LEADER_SLOTS`] to avoid unnecessary repetition.
    /// `lookahead_leaders` controls the scheduled-leader lookahead. Implementations may also
    /// include configured fixed peers or one additional scheduled leader when the current slot is
    /// the last slot in a leader's consecutive slots. If the supplied buffer is too small,
    /// additional addresses are omitted.
    ///
    /// If the current leader estimation is incorrect and transactions are sent to
    /// only one estimated leader, there is a risk of losing all the transactions,
    /// depending on the forwarding policy.
    fn next_leaders<'leaders>(
        &mut self,
        lookahead_leaders: usize,
        leaders: &'leaders mut [SocketAddr],
    ) -> &'leaders [SocketAddr];
}

/// Error type for [`LeaderUpdater`].
#[derive(Error, PartialEq)]
pub struct LeaderUpdaterError;

impl fmt::Display for LeaderUpdaterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Leader updater encountered an error")
    }
}

impl fmt::Debug for LeaderUpdaterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeaderUpdaterError")
    }
}

#[cfg(feature = "dev-context-only-utils")]
pub fn create_pinned_leader_updater(address: SocketAddr) -> Box<dyn LeaderUpdater> {
    Box::new(PinnedLeaderUpdater {
        address: vec![address],
    })
}

/// `PinnedLeaderUpdater` is an implementation of [`LeaderUpdater`] that always
/// returns a fixed, "pinned" leader address.
#[cfg(feature = "dev-context-only-utils")]
struct PinnedLeaderUpdater {
    pub address: Vec<SocketAddr>,
}

#[cfg(feature = "dev-context-only-utils")]
impl LeaderUpdater for PinnedLeaderUpdater {
    fn next_leaders<'leaders>(
        &mut self,
        _lookahead_leaders: usize,
        leaders: &'leaders mut [SocketAddr],
    ) -> &'leaders [SocketAddr] {
        let len = leaders.len().min(self.address.len());
        leaders[..len].copy_from_slice(&self.address[..len]);
        &leaders[..len]
    }
}

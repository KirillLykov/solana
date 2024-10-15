//! Convenience wrapper which allows to switch between ConnectionCache and
//! tpu-client-next tpu clients implementations. For now it is PoC implementation.

use {solana_client::connection_cache::ConnectionCache, std::sync::Arc};

// TODO(klykov): try to wrap whatever clients are used
#[derive(Clone)]
pub enum ClientWrapper {
    ConnectionCache(Arc<ConnectionCache>),
}

impl From<Arc<ConnectionCache>> for ClientWrapper {
    fn from(cache: Arc<ConnectionCache>) -> Self {
        ClientWrapper::ConnectionCache(cache)
    }
}

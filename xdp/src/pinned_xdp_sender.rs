//! This module defines [`PinnedXdpSender`] which is a convenience wrapper around
//! [`agave_xdp::transmitter::XdpSender`] for the case when source address is pinned for all items
//! like it is in turbine.
use {
    crate::transmitter::{BytesTxPacket, XdpAddrs, XdpSender},
    bytes::Bytes,
    crossbeam_channel::TrySendError,
    std::net::SocketAddrV4,
};

/// [`PinnedXdpSender`] is a structure that simplifies sending packets over XDP with `XdpSender`
/// when source address is pinned for all items.
#[derive(Clone)]
pub struct PinnedXdpSender {
    sender: XdpSender,
    src_addr: SocketAddrV4,
}

impl PinnedXdpSender {
    pub fn new(sender: XdpSender, src_addr: SocketAddrV4) -> Self {
        Self { sender, src_addr }
    }

    #[inline]
    pub fn try_send(
        &self,
        sender_index: usize,
        addr: impl Into<XdpAddrs>,
        payload: Bytes,
    ) -> Result<(), TrySendError<BytesTxPacket>> {
        self.sender.try_send(
            sender_index,
            BytesTxPacket::new(self.src_addr, addr, None, payload),
        )
    }
}

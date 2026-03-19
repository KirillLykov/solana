//! This module defines [`QuicSocket`], which selects between kernel-UDP and XDP-backed QUIC
//! socket configurations.
use {
    agave_xdp::transmitter::{BytesTxPacket, XdpSender},
    bytes::Bytes,
    crossbeam_channel::TrySendError,
    quinn::{
        AsyncUdpSocket, UdpPoller,
        udp::{RecvMeta, Transmit, UdpSocketState},
    },
    std::{
        fmt::{self, Debug},
        io::{self, IoSliceMut},
        net::{SocketAddr, SocketAddrV4},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, ready},
    },
    tokio::io::Interest,
};

/// [`QuicSocket`] is a thin wrapper that simplifies switching between a kernel UDP socket and an
/// XDP-backed socket configuration.
#[derive(Debug)]
pub enum QuicSocket {
    /// A QUIC socket that uses XDP for sending and kernel UDP socket for receiving.
    Xdp(QuicXdpSocketBundle),
    /// A QUIC socket that uses kernel UDP socket for both sending and receiving. This is used when
    /// XDP is not available or disabled.
    Kernel(std::net::UdpSocket),
}

impl From<std::net::UdpSocket> for QuicSocket {
    fn from(socket: std::net::UdpSocket) -> Self {
        QuicSocket::Kernel(socket)
    }
}

impl QuicSocket {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            QuicSocket::Xdp(cfg) => cfg.socket.local_addr(),
            QuicSocket::Kernel(socket) => socket.local_addr(),
        }
    }
}

/// [`QuicXdpSocketBundle`] bundles the resources required to construct an XDP-backed QUIC socket.
///
/// It carries both an [`XdpSender`] and a [`std::net::UdpSocket`], rather than constructing an
/// `AsyncUdpSocket` directly, because the underlying sockets can be created only when a Tokio
/// runtime is present. In Streamer and related components, that runtime is created deep in the
/// call stack, so this bundle is propagated up to endpoint creation.
pub struct QuicXdpSocketBundle {
    pub socket: std::net::UdpSocket,
    pub xdp_sender: XdpSender,
}

impl Debug for QuicXdpSocketBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpSocketBundle")
            .field("socket", &self.socket)
            .finish()
    }
}

/// [`QuicXdpTxSocket`] is an implementation of `AsyncUdpSocket` that uses an underlying `XdpSender`
/// to send packets and a kernel UDP socket to receive packets.
pub(crate) struct QuicXdpTxSocket {
    ingress_kernel_udp: UdpSocket,
    egress_xdp: IndexedXdpSender,
}

impl QuicXdpTxSocket {
    pub fn new(
        QuicXdpSocketBundle { socket, xdp_sender }: QuicXdpSocketBundle,
    ) -> io::Result<Self> {
        let src_addr = socket.local_addr()?;
        let SocketAddr::V4(src_addr) = src_addr else {
            panic!("IPv6 not supported");
        };

        Ok(Self {
            ingress_kernel_udp: UdpSocket::new(socket)?,
            egress_xdp: IndexedXdpSender {
                xdp_sender,
                src_addr,
                next_sender_index: AtomicUsize::new(0),
            },
        })
    }
}

impl fmt::Debug for QuicXdpTxSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpTxSocket")
            .field("local_addr", &self.ingress_kernel_udp.local_addr())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct ReadyPoller;

impl UdpPoller for ReadyPoller {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncUdpSocket for QuicXdpTxSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(ReadyPoller)
    }

    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        let payload = Bytes::from(t.contents.to_vec());
        match self.egress_xdp.try_send(t.destination, payload) {
            Ok(()) => Ok(()),

            Err(TrySendError::Full(_)) => Err(io::ErrorKind::WouldBlock.into()),

            Err(TrySendError::Disconnected(_)) => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.ingress_kernel_udp.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.ingress_kernel_udp.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        // no GSO batches, so each transmit describes exactly one datagram
        1
    }

    fn max_receive_segments(&self) -> usize {
        self.ingress_kernel_udp.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.ingress_kernel_udp.may_fragment()
    }
}

/// [`IndexedXdpSender`] wraps `XdpSender` to provide a simple round-robin sender index for each
/// packet sent. It is needed because `AsyncUdpSocket::try_send` does not provide a way to specify
/// the sender index.
struct IndexedXdpSender {
    xdp_sender: XdpSender,
    src_addr: SocketAddrV4,
    next_sender_index: AtomicUsize,
}

impl IndexedXdpSender {
    fn try_send(
        &self,
        destination: SocketAddr,
        payload: Bytes,
    ) -> Result<(), TrySendError<BytesTxPacket>> {
        let sender_idx = self.next_sender_index.fetch_add(1, Ordering::Relaxed);
        self.xdp_sender.try_send(
            sender_idx,
            BytesTxPacket::new(self.src_addr, destination, payload),
        )
    }
}

/// [`UdpSocket`] adapts a Tokio [`tokio::net::UdpSocket`] and its [`UdpSocketState`]
/// to implement the receive path of [`AsyncUdpSocket`].
///
/// This type is intentionally receive-only. Egress is provided by [`QuicXdpTxSocket`] via AF_XDP,
/// so send-related trait methods are intentionally not supported here.
#[derive(Debug)]
struct UdpSocket {
    io: tokio::net::UdpSocket,
    inner: UdpSocketState,
}

impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            inner: UdpSocketState::new((&sock).into())?,
            io: tokio::net::UdpSocket::from_std(sock)?,
        })
    }
}

impl AsyncUdpSocket for UdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        unimplemented!("quic_socket::UdpSocket does not support async IO on the kernel UDP socket")
    }

    fn try_send(&self, _transmit: &Transmit) -> io::Result<()> {
        unimplemented!("quic_socket::UdpSocket does not support sending on the kernel UDP socket")
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_recv_ready(cx))?;
            if let Ok(res) = self.io.try_io(Interest::READABLE, || {
                self.inner.recv((&self.io).into(), bufs, meta)
            }) {
                return Poll::Ready(Ok(res));
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.gro_segments()
    }
}

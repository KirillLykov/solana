//! This module defines [`QuicSocket`], which selects between kernel-UDP and XDP-backed QUIC socket
//! configurations.
use {
    agave_xdp::transmitter::{BytesTxPacket, XdpSender},
    bytes::Bytes,
    crossbeam_channel::TrySendError,
    nix::ifaddrs::getifaddrs,
    quinn::{
        AsyncUdpSocket, UdpPoller,
        udp::{RecvMeta, Transmit, UdpSocketState},
    },
    std::{
        fmt::{self, Debug},
        future::Future,
        io::{self, IoSliceMut},
        net::{IpAddr, SocketAddr, SocketAddrV4},
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
/// runtime is present. In Streamer and related components, that runtime is created deep in the call
/// stack, so this bundle is propagated up to endpoint creation.
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

/// [`QuicXdpTxSocket`] implements `AsyncUdpSocket`. It always uses `UdpSocket` for ingress traffic.
/// For egress traffic, it employs an underlying `IndexedXdpSender` for non-local destinations. For
/// destinations owned by the local host (routed via `lo`, including loopback and local interface
/// IPs), it falls back to a kernel `UdpSocket`, because AF_XDP cannot transmit to `lo`.
pub(crate) struct QuicXdpTxSocket {
    udp_socket: Arc<UdpSocket>,
    xdp_sender: IndexedXdpSender,
    local_ips: Vec<IpAddr>,
}

impl QuicXdpTxSocket {
    pub fn new(
        QuicXdpSocketBundle { socket, xdp_sender }: QuicXdpSocketBundle,
    ) -> io::Result<Self> {
        let src_addr = socket.local_addr()?;
        let SocketAddr::V4(src_addr) = src_addr else {
            panic!("IPv6 not supported");
        };

        // Collect local interface IPs once at construction time. We intentionally do not refresh
        // them if interface addresses change later. This is a low-risk tradeoff because
        // local-destination egress is expected to be rare: only RPC's sendTransaction traffic or
        // local testing.
        let local_ips = collect_local_ipv4_ips()?;

        Ok(Self {
            udp_socket: Arc::new(UdpSocket::new(socket)?),
            xdp_sender: IndexedXdpSender {
                xdp_sender,
                src_addr,
                next_sender_index: AtomicUsize::new(0),
            },
            local_ips,
        })
    }

    fn should_use_kernel_udp(&self, dst: SocketAddr) -> bool {
        dst.ip().is_loopback() || self.local_ips.contains(&dst.ip())
    }
}

impl fmt::Debug for QuicXdpTxSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpTxSocket")
            .field("local_addr", &self.udp_socket.local_addr())
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for QuicXdpTxSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        // The kernel UDP socket poller is always returned here, ignoring the XDP sender. It is
        // correct implementation under the following 2 assumptions:
        // 1. When egress AF_XDP is enabled, the kernel UDP socket is used rarely and only for local
        //    destinations, so it shall always be writable.
        // 2. XdpSender is almost always Ready.
        //
        // Hence, the following situation is very improbable: if it happens that the UDP socket is
        // not writable, but actually XdpSender is writable, we might have suboptimal performance
        // until the UDP socket becomes writable. The other situation is when the UDP Poller is
        // Ready but the selected channel in `IndexedXdpSender` is full. In this case, the try_send
        // will fail with `WouldBlock`, and the caller will call `poll_writable` again, which leads
        // to selecting the other channel in the next round.
        self.udp_socket.clone().create_io_poller()
    }

    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        if self.should_use_kernel_udp(t.destination) {
            return self.udp_socket.try_send(t);
        }
        let payload = Bytes::from(t.contents.to_vec());
        match self.xdp_sender.try_send(t.destination, payload) {
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
        self.udp_socket.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.udp_socket.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        // no GSO batches, so each transmit describes exactly one datagram
        1
    }

    fn max_receive_segments(&self) -> usize {
        self.udp_socket.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.udp_socket.may_fragment()
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
/// to implement [`AsyncUdpSocket`].
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
        Box::pin(UdpPollHelper::new(move || {
            let socket = self.clone();
            async move { socket.io.writable().await }
        }))
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        self.io.try_io(Interest::WRITABLE, || {
            self.inner.send((&self.io).into(), transmit)
        })
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

pin_project_lite::pin_project! {
    /// Helper adapting a function `MakeFut` that constructs a single-use future `Fut` into a
    /// [`UdpPoller`] that may be reused indefinitely
    struct UdpPollHelper<MakeFut, Fut> {
        make_fut: MakeFut,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<MakeFut, Fut> UdpPollHelper<MakeFut, Fut> {
    /// Construct a [`UdpPoller`] that calls `make_fut` to get the future to poll, storing it until
    /// it yields [`Poll::Ready`], then creating a new one on the next
    /// [`poll_writable`](UdpPoller::poll_writable)
    fn new(make_fut: MakeFut) -> Self {
        Self {
            make_fut,
            fut: None,
        }
    }
}

impl<MakeFut, Fut> UdpPoller for UdpPollHelper<MakeFut, Fut>
where
    MakeFut: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + Sync + 'static,
{
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let mut this = self.project();
        if this.fut.is_none() {
            this.fut.set(Some((this.make_fut)()));
        }
        // We're forced to `unwrap` here because `Fut` may be `!Unpin`, which means we can't safely
        // obtain an `&mut Fut` after storing it in `self.fut` when `self` is already behind `Pin`,
        // and if we didn't store it then we wouldn't be able to keep it alive between
        // `poll_writable` calls.
        let result = this.fut.as_mut().as_pin_mut().unwrap().poll(cx);
        if result.is_ready() {
            // Polling an arbitrary `Future` after it becomes ready is a logic error, so arrange for
            // a new `Future` to be created on the next call.
            this.fut.set(None);
        }
        result
    }
}

impl<MakeFut, Fut> Debug for UdpPollHelper<MakeFut, Fut> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpPollHelper").finish_non_exhaustive()
    }
}

/// Collects IPv4 addresses assigned to local network interfaces.
fn collect_local_ipv4_ips() -> io::Result<Vec<IpAddr>> {
    let mut ips = Vec::new();
    for ifa in getifaddrs().map_err(io::Error::other)? {
        let Some(addr) = ifa.address else { continue };
        if let Some(v4) = addr.as_sockaddr_in() {
            let ip = IpAddr::V4(v4.ip());
            if !ips.contains(&ip) {
                ips.push(ip);
            }
        }
    }
    Ok(ips)
}

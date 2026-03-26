use {
    agave_xdp::xdp_retransmitter::{XdpAddrs, XdpSender},
    bytes::Bytes,
    crossbeam_channel::TrySendError,
    quinn::{
        AsyncUdpSocket, UdpPoller,
        udp::{RecvMeta, Transmit, UdpSocketState},
    },
    std::{
        fmt::{self, Debug},
        future::Future,
        io::{self, IoSliceMut},
        net::{SocketAddr, SocketAddrV4},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicU8, AtomicUsize, Ordering},
        },
        task::{Context, Poll, ready},
    },
    tokio::io::Interest,
};

#[derive(Debug)]
pub enum QuicSocket {
    /// A QUIC socket that uses XDP for sending and kernel UDP socket for receiving.
    Xdp(QuicXdpSocketConfig),
    /// A QUIC socket that uses kernel UDP socket for both sending and receiving. This is used when
    /// XDP is not available or disabled.
    Kernel(std::net::UdpSocket),
}

impl QuicSocket {
    pub fn new(socket: std::net::UdpSocket, xdp_sender: Option<XdpSender>) -> Self {
        if let Some(xdp_sender) = xdp_sender {
            Self::Xdp(QuicXdpSocketConfig { socket, xdp_sender })
        } else {
            Self::Kernel(socket)
        }
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

/// Config is required because we may construct underlying sockets only when tokio runtime is
/// present but in case of Streamer and other components runtimes are created deeply inside the call
/// stack. Hence, we propagte this Config up to the Endpoint creation.
pub struct QuicXdpSocketConfig {
    pub socket: std::net::UdpSocket,
    pub xdp_sender: XdpSender,
}

impl Debug for QuicXdpSocketConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpSocketConfig")
            .field("socket", &self.socket)
            .finish()
    }
}

struct IndexedXdpSender {
    xdp_sender: XdpSender,
    src_addr: SocketAddrV4,
    next_sender: AtomicUsize,
}

impl IndexedXdpSender {
    fn try_send(
        &self,
        destination: SocketAddr,
        payload: Bytes,
    ) -> Result<(), TrySendError<(XdpAddrs, Bytes, Option<SocketAddrV4>)>> {
        let sender_idx = self.next_sender.fetch_add(1, Ordering::Relaxed);
        self.xdp_sender
            .try_send(sender_idx, destination, payload, Some(self.src_addr))
    }
}

pub struct QuicXdpSocket {
    ingress_kernel_udp: Arc<UdpSocket>,
    egress_xdp: IndexedXdpSender,
    block_choice: Arc<AtomicU8>,
}

impl QuicXdpSocket {
    pub fn new(
        QuicXdpSocketConfig { socket, xdp_sender }: QuicXdpSocketConfig,
    ) -> io::Result<Self> {
        let src_addr = socket.local_addr()?;
        let SocketAddr::V4(src_addr) = src_addr else {
            panic!("IPv6 not supported");
        };

        Ok(Self {
            ingress_kernel_udp: Arc::new(UdpSocket::new(socket)?),
            egress_xdp: IndexedXdpSender {
                xdp_sender,
                src_addr,
                next_sender: AtomicUsize::new(0),
            },
            block_choice: Arc::new(AtomicU8::new(0)),
        })
    }

    fn should_use_kernel_udp(&self, dst: SocketAddr) -> bool {
        info!(
            "@@@ ip {:?}, loopback: {}",
            dst.ip(),
            dst.ip().is_loopback()
        );
        dst.ip().is_loopback() || dst.ip() == std::net::IpAddr::V4(*self.egress_xdp.src_addr.ip())
    }
}

impl fmt::Debug for QuicXdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicXdpSocket")
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

#[derive(Debug)]
struct PollerAdapter {
    xdp_poller: Pin<Box<dyn UdpPoller>>,
    udp_poller: Pin<Box<dyn UdpPoller>>,
    blocked_choice: Arc<AtomicU8>,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum BlockedChoice {
    None = 0,
    Udp = 1,
    Xdp = 2,
}

impl BlockedChoice {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Udp,
            2 => Self::Xdp,
            _ => unreachable!("invalid value for BlockedChoice: {v}"),
        }
    }
}

impl From<BlockedChoice> for u8 {
    fn from(value: BlockedChoice) -> Self {
        value as u8
    }
}

impl From<u8> for BlockedChoice {
    fn from(value: u8) -> Self {
        BlockedChoice::from(value)
    }
}

impl UdpPoller for PollerAdapter {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.blocked_choice.load(Ordering::Acquire).into() {
            BlockedChoice::None => Poll::Ready(Ok(())),
            BlockedChoice::Udp => this.udp_poller.as_mut().poll_writable(cx),
            BlockedChoice::Xdp => this.xdp_poller.as_mut().poll_writable(cx),
        }
    }
}

impl AsyncUdpSocket for QuicXdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(PollerAdapter {
            xdp_poller: Box::pin(ReadyPoller),
            udp_poller: self.ingress_kernel_udp.clone().create_io_poller(),
            blocked_choice: self.block_choice.clone(),
        })
    }

    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        if self.should_use_kernel_udp(t.destination) {
            self.block_choice
                .store(BlockedChoice::Udp.into(), Ordering::Release);
            return match self.ingress_kernel_udp.try_send(t) {
                Ok(()) => {
                    self.block_choice
                        .store(BlockedChoice::None.into(), Ordering::Release);
                    Ok(())
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.block_choice
                        .store(BlockedChoice::Udp.into(), Ordering::Release);
                    Err(io::ErrorKind::WouldBlock.into())
                }
                Err(e) => {
                    self.block_choice
                        .store(BlockedChoice::None.into(), Ordering::Release);
                    Err(e)
                }
            };
        }

        self.block_choice
            .store(BlockedChoice::Xdp.into(), Ordering::Release);
        let payload = Bytes::copy_from_slice(t.contents);
        match self.egress_xdp.try_send(t.destination, payload) {
            Ok(()) => {
                self.block_choice
                    .store(BlockedChoice::None.into(), Ordering::Release);
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.block_choice
                    .store(BlockedChoice::Xdp.into(), Ordering::Release);
                Err(io::ErrorKind::WouldBlock.into())
            }

            Err(TrySendError::Disconnected(_)) => {
                self.block_choice
                    .store(BlockedChoice::None.into(), Ordering::Release);
                Err(io::ErrorKind::BrokenPipe.into())
            }
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

/// Adapted from quinn's `UdpSocket` which is private.
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

//todo: maybe there are somewhere else helpers like this one
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

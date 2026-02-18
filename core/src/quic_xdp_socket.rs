use {
    crossbeam_channel::TrySendError,
    quinn::{
        udp::{RecvMeta, Transmit, UdpSocketState},
        AsyncUdpSocket, UdpPoller,
    },
    solana_ledger::shred,
    solana_turbine::xdp::{XdpAddrs, XdpSender},
    std::{
        fmt,
        fmt::Debug,
        future::Future,
        io::{self, IoSliceMut},
        net::{SocketAddr, SocketAddrV4},
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{ready, Context, Poll},
    },
    tokio::io::Interest,
};

pub fn udpsocket_to_quic_xdp_socket(
    sockets: Vec<std::net::UdpSocket>,
    xdp_sender: Option<XdpSender>,
) -> Vec<Arc<dyn AsyncUdpSocket>> {
    //TODO(klykov): get rid of unwraps later. Maybe we contruct these sockets as AsyncUdpSocket from
    //the beginning instead.
    let xdp_sender = xdp_sender.map(Arc::new);

    sockets
        .into_iter()
        .map(|socket| {
            if let Some(ref sender) = xdp_sender {
                Arc::new(QuicXdpSocket::new(socket, sender.clone()).unwrap())
                    as Arc<dyn AsyncUdpSocket>
            } else {
                Arc::new(UdpSocket::new(socket).unwrap()) as Arc<dyn AsyncUdpSocket>
            }
        })
        .collect()
}

struct IndexedXdpSender {
    xdp: Arc<XdpSender>,
    src_addr: SocketAddrV4,
    next_sender: AtomicUsize,
}

impl IndexedXdpSender {
    fn try_send(
        &self,
        destination: SocketAddr,
        payload: shred::Payload,
    ) -> Result<(), TrySendError<(XdpAddrs, shred::Payload, Option<SocketAddrV4>)>> {
        let sender_idx = self.next_sender.fetch_add(1, Ordering::Relaxed);
        self.xdp
            .try_send(sender_idx, destination, payload, Some(self.src_addr))
    }
}

pub struct QuicXdpSocket {
    ingress_kernel_udp: UdpSocket,
    egress_xdp: IndexedXdpSender,
}

impl QuicXdpSocket {
    pub fn new(sock: std::net::UdpSocket, xdp: Arc<XdpSender>) -> io::Result<Self> {
        let src_addr = sock.local_addr()?;
        let SocketAddr::V4(src_addr) = src_addr else {
            panic!("IPv6 not supported");
        };

        Ok(Self {
            ingress_kernel_udp: UdpSocket::new(sock)?,
            egress_xdp: IndexedXdpSender {
                xdp,
                src_addr: src_addr.into(),
                next_sender: AtomicUsize::new(0),
            },
        })
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

impl AsyncUdpSocket for QuicXdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(ReadyPoller)
    }

    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        let payload = shred::Payload::from(t.contents.to_vec());
        match self.egress_xdp.try_send(t.destination, payload) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(_)) => return Err(io::ErrorKind::WouldBlock.into()),
            Err(TrySendError::Disconnected(_)) => return Err(io::ErrorKind::BrokenPipe.into()),
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
pub struct UdpSocket {
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

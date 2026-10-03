//! A chaos worker for the distributed failure-path tests (finding 411).
//!
//! A real `sqe_worker::flight_service::WorkerFlightService` runs in-process on
//! an ephemeral port, so every scan reads the real Parquet from the test
//! stack's RustFS with the credentials the coordinator put in the ticket. The
//! only thing this wrapper changes is the `do_get` *response*: it can refuse
//! to answer (a hung worker), cut the Arrow stream after N frames (a worker
//! lost mid-scan), or slow every frame down (a straggler). Everything else is
//! delegated untouched.
//!
//! Why not a mock that fabricates batches: the coordinator reassembles worker
//! batches against the plan schema and the whole point of 411 is to exercise
//! that path with real data, not a stub's idea of it. Why not a network proxy
//! in front of a separate worker process: a scan ticket is bound to the
//! worker URL it was issued for (finding 601), so a proxy would be rejected by
//! the upstream worker; wrapping the service object sidesteps that.
//!
//! The coordinator side needs no secret for these tests: with an empty
//! `coordinator.worker_secret` it sends no secret and no signature, and the
//! worker's `verify_worker_secret` / `verify_scan_signature` accept that when
//! its own secret is empty. Production always sets one; the seams under test
//! here are retry, eviction, timeout and cancel, not authentication.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};

/// Same shape as the worker's own associated stream types: a boxed stream of
/// `Result<T, Status>`.
type BoxStream<T> =
    std::pin::Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_worker::flight_service::WorkerFlightService;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

/// What the chaos layer does to a `do_get`.
#[derive(Clone, Debug)]
pub enum ChaosMode {
    /// Behave exactly like the real worker.
    PassThrough,
    /// Send the first `n` Flight frames (frame 0 is the schema message), then
    /// end the stream with `Status::unavailable`: the worker died mid-scan.
    FailAfterFrames(usize),
    /// Never answer `do_get`: the connection is accepted, the request read,
    /// and nothing comes back. The coordinator's `worker_rpc_timeout_secs`
    /// is what should end this.
    Hang,
    /// Sleep this long before every frame: a straggler, or a stream that is
    /// still producing rows when the query deadline passes.
    DelayPerFrame(Duration),
}

/// The wrapped worker as a trait object: a compile firewall, keep it. Held
/// concretely, every `async_trait` method below re-proved `Shared: Sync`
/// through `WorkerFlightService`'s DataFusion type graph (1.3 s each, 14 s
/// of this test binary). Same device as the coordinator's `StoredPlan`.
type InnerFlight = dyn FlightService<
    HandshakeStream = BoxStream<HandshakeResponse>,
    ListFlightsStream = BoxStream<FlightInfo>,
    DoGetStream = BoxStream<FlightData>,
    DoPutStream = BoxStream<PutResult>,
    DoExchangeStream = BoxStream<FlightData>,
    DoActionStream = BoxStream<arrow_flight::Result>,
    ListActionsStream = BoxStream<ActionType>,
>;

struct Shared {
    inner: Box<InnerFlight>,
    mode: Mutex<ChaosMode>,
    do_gets: AtomicUsize,
    active_streams: Arc<AtomicUsize>,
    frames_sent: Arc<AtomicU64>,
}

/// The Flight service the coordinator dials. Cloning shares the counters.
#[derive(Clone)]
pub struct ChaosWorker {
    shared: Arc<Shared>,
}

/// A running chaos worker: its URL, its counters, and the server task.
pub struct ChaosHandle {
    pub url: String,
    pub addr: SocketAddr,
    worker: ChaosWorker,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ChaosHandle {
    pub fn set_mode(&self, mode: ChaosMode) {
        *self.worker.shared.mode.lock().unwrap() = mode;
    }
    /// `do_get` calls received, including ones that were hung or cut.
    pub fn do_gets(&self) -> usize {
        self.worker.shared.do_gets.load(Ordering::SeqCst)
    }
    /// Streams the coordinator is still holding open.
    pub fn active_streams(&self) -> usize {
        self.worker.shared.active_streams.load(Ordering::SeqCst)
    }
    /// Flight frames actually handed to the coordinator.
    pub fn frames_sent(&self) -> u64 {
        self.worker.shared.frames_sent.load(Ordering::SeqCst)
    }
    /// Poll until `active_streams()` is zero or `within` elapses.
    pub async fn wait_streams_closed(&self, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if self.active_streams() == 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        self.active_streams() == 0
    }
}

impl Drop for ChaosHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Start a chaos worker on an ephemeral loopback port. The inner worker is a
/// production `WorkerFlightService` over a default worker runtime; it reads
/// object storage with the credentials the ticket carries.
pub async fn spawn_chaos_worker(mode: ChaosMode) -> ChaosHandle {
    // Bind first: the worker must advertise the URL the coordinator dials.
    // Scan tickets carry that URL as their audience, and a worker refuses a
    // ticket addressed elsewhere (#724).
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind chaos worker");
    let addr = listener.local_addr().expect("local addr");
    let url = format!("http://{}", addr);

    let worker_config = sqe_core::config::WorkerConfig::default();
    let (session_ctx, _pool) = sqe_worker::runtime::build_session_context(&worker_config)
        .expect("worker session context");
    let metrics = Arc::new(WorkerMetricsRegistry::new().expect("worker metrics"));
    let inner = WorkerFlightService::new(metrics, session_ctx).with_advertise_url(url.clone());

    let worker = ChaosWorker {
        shared: Arc::new(Shared {
            inner: Box::new(inner),
            mode: Mutex::new(mode),
            do_gets: AtomicUsize::new(0),
            active_streams: Arc::new(AtomicUsize::new(0)),
            frames_sent: Arc::new(AtomicU64::new(0)),
        }),
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let service = worker.clone();
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    ChaosHandle {
        url,
        addr,
        worker,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    }
}

/// Decrements `active_streams` when the wrapped stream is dropped, whether it
/// ended, errored, or the coordinator stopped polling (cancel, timeout).
struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl FlightService for ChaosWorker {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let shared = &self.shared;
        shared.do_gets.fetch_add(1, Ordering::SeqCst);
        let mode = shared.mode.lock().unwrap().clone();

        if let ChaosMode::Hang = mode {
            // The request is fully received and never answered. tonic drops
            // this future when the client gives up.
            futures::future::pending::<()>().await;
            unreachable!("pending future resolved");
        }

        let response = shared.inner.do_get(request).await?;
        let (metadata, inner_stream, extensions) = response.into_parts();

        shared.active_streams.fetch_add(1, Ordering::SeqCst);
        let guard = ActiveGuard(Arc::clone(&shared.active_streams));
        let frames = Arc::clone(&shared.frames_sent);
        let counted = inner_stream.inspect(move |_| {
            // The closure owns the guard, so dropping the stream drops it.
            let _keep = &guard;
            frames.fetch_add(1, Ordering::SeqCst);
        });

        let stream: BoxStream<FlightData> = match mode {
            ChaosMode::PassThrough | ChaosMode::Hang => counted.boxed(),
            ChaosMode::FailAfterFrames(n) => counted
                .enumerate()
                .take(n + 1)
                .map(move |(i, item)| {
                    if i >= n {
                        Err(Status::unavailable(format!(
                            "chaos worker: connection lost after {n} frames"
                        )))
                    } else {
                        item
                    }
                })
                .boxed(),
            ChaosMode::DelayPerFrame(delay) => counted
                .then(move |item| async move {
                    tokio::time::sleep(delay).await;
                    item
                })
                .boxed(),
        };

        Ok(Response::from_parts(metadata, stream, extensions))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.shared.inner.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.shared.inner.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.shared.inner.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.shared.inner.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.shared.inner.get_schema(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.shared.inner.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.shared.inner.do_exchange(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.shared.inner.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.shared.inner.list_actions(request).await
    }
}

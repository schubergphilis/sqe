//! Shared fixtures for the protocol scenarios: an in-memory object store that
//! records which credentials each read used and can fail chosen paths with
//! typed `object_store` errors, a `ScanTask` builder over it, a service
//! builder, and an ephemeral-port Flight server.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{FlightData, Ticket};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_planner::{ScanCredentials, ScanStorage, ScanTask};
use sqe_spill::ByteBudget;
use sqe_worker::executor::ObjectStoreFactory;
use sqe_worker::flight_service::WorkerFlightService;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

/// Bucket every fixture task names; the stub store ignores it.
pub const BUCKET: &str = "test-bucket";

/// Credentials the ticket carries before any refresh.
pub const OLD_KEY: &str = "AKID_OLD";
/// Credentials the coordinator pushes mid-scan.
pub const NEW_KEY: &str = "AKID_NEW";

/// One parquet blob with a single `id: Int64` column and one row group per
/// entry of `rows_per_group`. A 1024-row group decodes to an 8 KiB array,
/// which is what the resource-exhaustion scenario sizes its budget against.
pub fn parquet_blob(rows_per_group: &[usize]) -> Vec<u8> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).expect("writer");
    let mut next = 0i64;
    for &rows in rows_per_group {
        let ids: Vec<i64> = (next..next + rows as i64).collect();
        next += rows as i64;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        writer.write(&batch).expect("write");
        // Close the row group here so each entry is exactly one group.
        writer.flush().expect("flush");
    }
    writer.close().expect("close");
    buf
}

/// Typed failure a [`StubStore`] raises for a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// `object_store::Error::PermissionDenied` (S3 403 / expired token).
    Denied,
    /// `object_store::Error::NotFound` (S3 404 / NoSuchKey).
    Missing,
    /// `object_store::Error::Generic` carrying an S3 503 SlowDown body, which
    /// is how the AWS client surfaces throttling.
    Throttled,
}

impl FaultKind {
    fn error(self, path: &OsPath) -> object_store::Error {
        match self {
            FaultKind::Denied => object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: Box::new(std::io::Error::other(
                    "Client error with status 403 Forbidden: <Code>ExpiredToken</Code>",
                )),
            },
            FaultKind::Missing => object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(std::io::Error::other("<Code>NoSuchKey</Code>")),
            },
            FaultKind::Throttled => object_store::Error::Generic {
                store: "S3",
                source: Box::new(std::io::Error::other(
                    "Client error with status 503 Service Unavailable: <Code>SlowDown</Code>",
                )),
            },
        }
    }
}

/// `(object path, credential label)` per read, in order.
pub type AccessLog = Arc<Mutex<Vec<(String, String)>>>;

/// Fault table keyed by object path.
pub type FaultTable = Arc<HashMap<String, FaultKind>>;

/// In-memory store that records the credential label of every read and fails
/// configured paths with typed errors. One instance per credential set: the
/// factory builds a new one with the pushed key's label on every refresh.
#[derive(Debug)]
pub struct StubStore {
    inner: Arc<InMemory>,
    label: String,
    log: AccessLog,
    faults: FaultTable,
}

impl std::fmt::Display for StubStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StubStore({})", self.label)
    }
}

#[async_trait::async_trait]
impl ObjectStore for StubStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.log
            .lock()
            .unwrap()
            .push((location.to_string(), self.label.clone()));
        if let Some(kind) = self.faults.get(location.as_ref()) {
            return Err(kind.error(location));
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<OsPath>>,
    ) -> BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// The objects a scenario reads, plus the log and fault table its stores
/// share.
pub struct StubBucket {
    pub inner: Arc<InMemory>,
    pub log: AccessLog,
    pub faults: HashMap<String, FaultKind>,
    /// `(key, size)` in insertion order, for the ticket.
    pub objects: Vec<(String, u64)>,
}

impl StubBucket {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            log: Arc::new(Mutex::new(Vec::new())),
            faults: HashMap::new(),
            objects: Vec::new(),
        }
    }

    /// Store a parquet blob under `key`.
    pub async fn put_parquet(&mut self, key: &str, rows_per_group: &[usize]) {
        let blob = parquet_blob(rows_per_group);
        let size = blob.len() as u64;
        self.inner
            .put(&OsPath::from(key), PutPayload::from(blob))
            .await
            .expect("put");
        self.objects.push((key.to_string(), size));
    }

    /// Fail every read of `key` with `kind`. The object may or may not exist.
    pub fn fail(&mut self, key: &str, kind: FaultKind) {
        self.faults.insert(key.to_string(), kind);
        if !self.objects.iter().any(|(k, _)| k == key) {
            // A plausible size so the ticket skips HEAD and the failure is
            // the first ranged read, as with a real 403.
            self.objects.push((key.to_string(), 4096));
        }
    }

    /// Object-store factory labelling each store with the credentials it was
    /// built from: the ticket's access key initially, the pushed key after a
    /// refresh.
    pub fn factory(&self) -> ObjectStoreFactory {
        let inner = Arc::clone(&self.inner);
        let log = Arc::clone(&self.log);
        let faults: FaultTable = Arc::new(self.faults.clone());
        Arc::new(move |task: &ScanTask, refreshed| {
            let label = match refreshed {
                Some(creds) => creds.access_key_id.clone(),
                None => task.credentials.access_key().to_string(),
            };
            let store: Arc<dyn ObjectStore> = Arc::new(StubStore {
                inner: Arc::clone(&inner),
                label,
                log: Arc::clone(&log),
                faults: Arc::clone(&faults),
            });
            Ok(store)
        })
    }

    /// A v1 whole-file ticket over every object, with sizes so the executor
    /// never issues HEAD.
    pub fn scan_task(&self, fragment_id: &str) -> ScanTask {
        let paths = self
            .objects
            .iter()
            .map(|(k, _)| format!("s3://{BUCKET}/{k}"))
            .collect();
        let sizes = self.objects.iter().map(|(_, s)| *s).collect();
        ScanTask::builder(
            fragment_id,
            paths,
            ScanStorage {
                s3_region: "us-east-1".to_string(),
                s3_path_style: true,
                s3_allow_http: true,
                credentials: ScanCredentials::static_keys(OLD_KEY, "secret", ""),
                ..ScanStorage::default()
            },
        )
        .with_file_sizes(sizes)
        .build()
    }

    /// Labels of every read of `key`, in order.
    pub fn labels_for(&self, key: &str) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == key)
            .map(|(_, label)| label.clone())
            .collect()
    }
}

impl Default for StubBucket {
    fn default() -> Self {
        Self::new()
    }
}

/// A worker Flight service in anonymous mode (empty secret) reading through
/// `factory`, with optional shared scan and flight budgets.
pub fn worker_service(
    factory: ObjectStoreFactory,
    scan_budget: Option<ByteBudget>,
    flight_budget: Option<ByteBudget>,
) -> WorkerFlightService {
    let metrics = Arc::new(WorkerMetricsRegistry::new().expect("metrics"));
    let mut svc = WorkerFlightService::new(metrics, SessionContext::new())
        .with_object_store_factory(factory)
        .with_flight_compression(sqe_core::FlightCompression::None);
    if let Some(b) = scan_budget {
        svc = svc.with_scan_budget(b);
    }
    if let Some(b) = flight_budget {
        svc = svc.with_flight_budget(b);
    }
    svc
}

/// The `do_get` response stream type of the worker service.
pub type DoGetStream = <WorkerFlightService as FlightService>::DoGetStream;

/// In-process `do_get` for `task`, anonymous (no secret, no signature).
pub async fn do_get(
    svc: &WorkerFlightService,
    task: &ScanTask,
) -> Result<Response<DoGetStream>, Status> {
    let ticket = Ticket {
        ticket: bytes::Bytes::from(task.to_bytes().expect("ticket")),
    };
    svc.do_get(Request::new(ticket)).await
}

/// Drain a `do_get` stream: the number of frames before the first error, and
/// that error if any.
pub async fn drain(mut stream: DoGetStream) -> (usize, Option<Status>) {
    let mut frames = 0;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => frames += 1,
            Err(status) => return (frames, Some(status)),
        }
    }
    (frames, None)
}

/// The `x-sqe-fault` label on `status`, if any.
pub fn fault_label(status: &Status) -> Option<String> {
    status
        .metadata()
        .get(sqe_planner::FAULT_METADATA_KEY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// A Flight server on an ephemeral loopback port, shut down on drop.
pub struct ServedFlight {
    pub addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ServedFlight {
    pub async fn start<S: FlightService>(service: S) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = rx.await;
                })
                .await
                .expect("flight server runs");
        });
        Self {
            addr,
            shutdown: Some(tx),
            task: Some(task),
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn client(&self) -> FlightServiceClient<Channel> {
        let channel = Channel::from_shared(self.url())
            .expect("uri")
            .connect()
            .await
            .expect("connect");
        FlightServiceClient::new(channel)
    }
}

impl Drop for ServedFlight {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Poll `probe` every 10 ms until it yields or `timeout` passes.
pub async fn wait_for<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Number of frames in a `Vec<FlightData>` that carry a record batch body.
pub fn data_frames(frames: &[FlightData]) -> usize {
    frames.iter().filter(|f| !f.data_body.is_empty()).count()
}

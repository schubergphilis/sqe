use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
#[cfg(feature = "worker-shuffle")]
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, info_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use datafusion::prelude::SessionContext;
use iceberg::io::FileIO;
use sqe_catalog::FooterCache;
use sqe_compaction::wire::{CompactGroupFrame, CompactGroupRequest, CompactGroupResponse};
use sqe_compaction::writer::{new_upload_tracker, UploadedPaths};
use sqe_core::FlightCompression;
use sqe_metrics::propagation::extract_trace_context;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_planner::{ScanTask, WorkerCapacity, WorkerFault, SCAN_TASK_VERSION_CURRENT};
#[cfg(feature = "worker-shuffle")]
use sqe_spill::{
    split_default_read_headroom, AdmissionRequest, AttemptManifest, ExchangeAttemptStore,
    ReclaimableConsumer, SpillScope, TaskKey, WorkloadClass,
};
use sqe_spill::{ByteBudget, BytePermit, LiveConsumerRegistry, MemoryGovernor, SpillManager};
use tokio_util::sync::CancellationToken;

use crate::compaction::{
    build_file_io, delete_orphan_uploads, run_group_rewrite, CleanupOutcome, GroupRewriteCtx,
};
use crate::credential_channel::{CredentialStore, RefreshableCredentials};
use crate::executor::{self, ObjectStoreFactory};
#[cfg(feature = "worker-shuffle")]
use crate::shuffle::{ExchangeDescriptor, ShuffleManager};
use crate::status::WorkerStatus;

/// Yields `inner`'s items unchanged and drops `held` when the stream ends,
/// errors out, or is dropped by a disconnected client.
///
/// Replaces the cleanup-sentinel idiom (finding 602): end-of-stream
/// cleanup used to be a fake error chained after the batches and filtered back
/// out by substring match, so a control signal travelled in the error channel
/// and any real error containing the sentinel text was swallowed. Here the
/// guard is plain state: the item stream is never touched.
struct HoldUntilEnd<S, G> {
    inner: Pin<Box<S>>,
    held: Option<G>,
}

impl<S, G> HoldUntilEnd<S, G> {
    fn new(inner: S, held: G) -> Self {
        Self {
            inner: Box::pin(inner),
            held: Some(held),
        }
    }
}

impl<S, G> Stream for HoldUntilEnd<S, G>
where
    S: Stream,
    G: Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(None) = item {
            self.held = None;
        }
        item
    }
}
#[cfg(feature = "worker-shuffle")]
use crate::spill_buffer::SpillablePartitionBuffer;

/// Streams `RecordBatch`es into the Flight encoder while holding each batch's
/// scan-budget permit until the encoder polls the next item (or the stream
/// ends / is dropped on client disconnect).
///
/// This is the Phase 1 `AccountedFlightStream` ownership wrapper: Arrow
/// residency is charged until encoding no longer needs the batch, then the
/// permit drops. Encoded FlightData is metered separately via the inflight
/// gauge on the outer map.
struct AccountedEncodeStream<S> {
    inner: Pin<Box<S>>,
    /// Permit for the batch most recently yielded to the encoder.
    held_permit: Option<BytePermit>,
    metrics: Arc<WorkerMetricsRegistry>,
}

impl<S> Stream for AccountedEncodeStream<S>
where
    S: Stream<Item = Result<executor::AccountedBatch, arrow_flight::error::FlightError>>,
{
    type Item = Result<RecordBatch, arrow_flight::error::FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Release the previous batch's permit only when the encoder asks for
        // the next one (or when we see end-of-stream below).
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(accounted))) => {
                let charged = accounted.charged_bytes();
                let (batch, permit) = accounted.into_parts();
                // Drop the previous permit now that the encoder has finished
                // with that batch (it polled for the next).
                self.held_permit = Some(permit);
                self.metrics
                    .flight_encode_resident_bytes
                    .set(charged as f64);
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.held_permit = None;
                self.metrics.flight_encode_resident_bytes.set(0.0);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.held_permit = None;
                self.metrics.flight_encode_resident_bytes.set(0.0);
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Being polled again is the proof the encoder is done with the
                // previous batch, whether or not the next one is ready.
                // Holding the permit across Pending deadlocked a budget
                // smaller than two batches plus the fetch charge: the producer
                // waited for this permit while the encoder waited for the
                // producer (surfaced by the finding-611 refresh scenario).
                self.held_permit = None;
                self.metrics.flight_encode_resident_bytes.set(0.0);
                Poll::Pending
            }
        }
    }
}

impl<S> Drop for AccountedEncodeStream<S> {
    fn drop(&mut self) {
        // Client disconnect / cancellation: release any held encode permit.
        self.held_permit = None;
        self.metrics.flight_encode_resident_bytes.set(0.0);
    }
}

/// Flight-frame budget for a service built without `[worker.memory]`
/// (tests, embedded use). Mirrors `executor::default_scan_budget`: a tenth of
/// the pool, matching the `flight_budget` config default, and finite even on an
/// unbounded pool so `ItemTooLarge` still has a threshold to compare against.
///
/// No pool reservation. Encoded IPC frames are not DataFusion allocations, so
/// charging them to the pool would fail queries on operator pressure; the
/// capacity is already reserved via `configured_need_bytes` at startup.
fn default_flight_budget(session_ctx: &SessionContext) -> ByteBudget {
    use datafusion::execution::memory_pool::MemoryLimit;
    let limit = match session_ctx.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(n) => n,
        _ => 512 * 1024 * 1024,
    };
    ByteBudget::new("flight", (limit / 10).max(64 * 1024), None)
}

/// Charge encoded Flight frames against `budget` so the encoded copy of a batch
/// is bounded, not merely observed (issue #407).
///
/// The Arrow-side permit (`AccountedEncodeStream`) releases as soon as the
/// encoder is done with a batch, which leaves the *encoded* IPC bytes -- the
/// copy tonic hands to h2 -- charged to nothing. Under concurrency that is the
/// unbounded term: N streams x one encoded frame each, outside the DataFusion
/// pool. `flight_inflight_bytes` reported it and nothing gated on it.
///
/// Each frame's charge is held until the transport polls for the NEXT frame,
/// which is the same idiom `AccountedEncodeStream` uses for batches: being
/// polled again is the observable proof the previous item was consumed. Dropping
/// the stream (client disconnect) releases through the state tuple.
///
/// The charge is per-stream-at-a-time, so the budget bounds the SUM across
/// concurrent DoGet streams. That is where it bites: one stream never blocks
/// itself, but the 50th concurrent scan waits for the 49 ahead of it.
///
/// A frame larger than the whole budget is passed through UNCHARGED rather than
/// failed. `ByteBudget::acquire` returns `ItemTooLarge` instead of hanging, and
/// failing a query that works today (unaccounted) would be a regression. Same
/// non-fatal treatment as the scan fetch charge in `executor.rs`.
fn accounted_frame_stream<S>(
    inner: S,
    budget: ByteBudget,
    metrics: Arc<WorkerMetricsRegistry>,
) -> impl Stream<Item = Result<FlightData, arrow_flight::error::FlightError>>
where
    S: Stream<Item = Result<FlightData, arrow_flight::error::FlightError>> + Send + 'static,
{
    stream::unfold(
        (Box::pin(inner), None::<BytePermit>, budget, metrics),
        |(mut inner, held, budget, metrics)| async move {
            // Being polled again is the observable proof the transport took the
            // previous frame. Release BEFORE acquiring: holding two charges at
            // once would deadlock a budget sized for a single frame.
            drop(held);
            let item = inner.next().await?;
            let permit = match &item {
                Ok(frame) => {
                    let bytes = frame.data_body.len() + frame.data_header.len();
                    match budget.acquire(bytes).await {
                        Ok(p) => Some(p),
                        Err(e) => {
                            debug!(error = %e, frame_bytes = bytes, "flight frame charge skipped");
                            None
                        }
                    }
                }
                Err(_) => None,
            };
            metrics
                .flight_inflight_bytes
                .set(budget.used_bytes() as f64);
            Some((item, (inner, permit, budget, metrics)))
        },
    )
}

/// Build [`IpcWriteOptions`] for a given compression setting.
fn ipc_options_for(compression: FlightCompression) -> Result<IpcWriteOptions, Status> {
    let codec = match compression {
        FlightCompression::None => None,
        FlightCompression::Lz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        FlightCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
    };
    IpcWriteOptions::default()
        .try_with_compression(codec)
        .map_err(|e| Status::internal(format!("Failed to set IPC compression: {e}")))
}

/// Metadata header carrying the shared coordinator/worker secret.
/// Same name as the coordinator's heartbeat handler so a single rotation
/// covers both directions.
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Metadata header carrying the HMAC-SHA256 tag (hex) over the ScanTask ticket
/// bytes (issue #206). The worker recomputes the tag over the received bytes
/// and constant-time compares before executing the task, proving the
/// coordinator authored the exact file paths, credentials, predicate, and
/// limit. Empty `worker_secret` (dev mode) skips this check.
const SCAN_SIGNATURE_HEADER: &str = "x-sqe-scan-signature";

/// Metadata header carrying the HMAC-SHA256 tag (hex, via
/// `sqe_compaction::wire::sign`) over the raw `CompactGroupRequest` wire
/// bytes (Phase 4c Task 3). Mirrors `SCAN_SIGNATURE_HEADER`: the worker
/// recomputes the tag over the exact bytes it received and constant-time
/// compares before decoding, so a tampered compaction request (swapped file
/// path, forged S3 credentials) fails here rather than being executed.
const COMPACT_SIGNATURE_HEADER: &str = "x-sqe-compact-signature";

/// Lowercase hex encoding for the 32-byte HMAC tag (#206). Kept local to avoid
/// an extra crate dependency for such a small need.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Worker's Arrow Flight service.
///
/// Handles three operations:
/// - `do_get`: Execute a scan task and stream results back
/// - `do_action("health_check")`: Return OK for coordinator health monitoring
/// - `do_action("refresh_credentials")`: Accept refreshed S3 credentials from coordinator
///
/// The service holds a [`SessionContext`] whose `RuntimeEnv` carries the
/// configured memory pool and disk manager so that every scan execution
/// respects the worker's memory limits.
#[derive(Clone)]
pub struct WorkerFlightService {
    metrics: Arc<WorkerMetricsRegistry>,
    credential_store: CredentialStore,
    session_ctx: SessionContext,
    footer_cache: Option<Arc<FooterCache>>,
    #[cfg(feature = "worker-shuffle")]
    shuffle_manager: ShuffleManager,
    /// Maximum duration for a single scan task. 0 means no timeout.
    scan_timeout: std::time::Duration,
    /// IPC compression for DoGet responses (worker -> coordinator).
    /// Default: ZSTD (internal traffic benefits from better ratio).
    flight_compression: FlightCompression,
    /// IPC compression for DoExchange shuffle responses.
    /// Default: ZSTD.
    #[cfg(feature = "worker-shuffle")]
    shuffle_compression: FlightCompression,
    /// Shared secret used to authenticate inbound Flight calls. When empty
    /// the worker accepts unauthenticated traffic (operators must opt in
    /// via `worker.allow_unauthenticated = true`, enforced at config load).
    worker_secret: String,
    /// Finding 601: pinned coordinator public key for Ed25519 ticket signatures.
    ticket_pin: Option<Arc<crate::ticket_pin::TicketKeyPin>>,
    /// Finding 601: this worker's advertised URL; tickets must name it.
    advertise_url: String,
    /// Finding 601: accept legacy HMAC ticket tags (migration only).
    accept_hmac_tickets: bool,
    /// Shared spill manager for shuffle / operator spill (Phase 3+).
    spill_manager: Option<Arc<SpillManager>>,
    /// Byte budget for shuffle partition buffers (from
    /// `worker.memory.shuffle_memory_budget`). Atomic for hot config reload.
    #[cfg(feature = "worker-shuffle")]
    shuffle_memory_budget: Arc<std::sync::atomic::AtomicUsize>,
    /// Scan timeout seconds (0 = none). Atomic for hot config reload.
    scan_timeout_secs: Arc<std::sync::atomic::AtomicU64>,
    /// Worker-wide temporary memory governor (Phase 7). Admits shuffle /
    /// operator grants so concurrent stages cannot race the FairSpillPool.
    memory_governor: Option<Arc<MemoryGovernor>>,
    /// Live reclaimable operators (join/agg/sort) registered under the governor.
    live_consumers: Arc<LiveConsumerRegistry>,
    /// Durable exchange attempt manifests (Phase 8).
    #[cfg(feature = "worker-shuffle")]
    exchange_store: Arc<ExchangeAttemptStore>,
    /// Byte budget for encoded Flight frames in flight to the coordinator
    /// (`worker.memory.flight_budget`, issue #407). `None` means "not
    /// configured": `do_get` then falls back to [`default_flight_budget`], so
    /// frames are charged either way.
    flight_budget: Option<ByteBudget>,
    /// Worker-side ceiling on one `compact_file_group` rewrite
    /// (`worker.compaction_timeout_secs`, finding 608). `None` means the
    /// operator disabled it with 0.
    compaction_timeout: Option<std::time::Duration>,
    /// Process identity and load counters the heartbeat reports (finding 609).
    status: Arc<WorkerStatus>,
    /// Builds the object store a scan reads through, initially and on every
    /// credential refresh. Production: [`executor::s3_object_store_factory`].
    /// The protocol tests install stubs so faults, refresh and cancellation
    /// are observable without S3.
    store_factory: ObjectStoreFactory,
    /// Worker-wide scan byte budget shared by every `do_get`. `None` (the
    /// default, and what bootstrap wires today) derives one budget per scan
    /// from the session pool, as before.
    scan_budget: Option<ByteBudget>,
    /// TEST-ONLY (`#[cfg(test)]`, so it compiles only for this crate's own
    /// unit test binary) replacement for the real rewrite, plus the `FileIO`
    /// the orphan cleanup runs against. Lets the finding-608 tests drive the
    /// supervision logic -- abort on drop, deadline, cleanup, bounded
    /// progress -- with a stub that sleeps or fails after N uploads, instead
    /// of needing a live S3 endpoint and a real Iceberg table. Always `None`
    /// unless a test opts in via `with_test_compaction_seam`.
    #[cfg(test)]
    compaction_seam: Option<Arc<CompactionSeam>>,
}

/// TEST-ONLY stub for the `compact_file_group` rewrite. See the
/// `compaction_seam` field on [`WorkerFlightService`].
#[cfg(test)]
pub(crate) struct CompactionSeam {
    /// Runs in place of [`run_group_rewrite`], with the same `GroupRewriteCtx`
    /// the real rewrite would have received (so a stub can record uploads in
    /// `ctx.tracker`, push progress through `ctx.progress_tx`, or observe
    /// `ctx.cancel`).
    #[allow(clippy::type_complexity)]
    pub(crate) run: Box<
        dyn Fn(
                GroupRewriteCtx,
            )
                -> futures::future::BoxFuture<'static, sqe_core::Result<CompactGroupResponse>>
            + Send
            + Sync,
    >,
    /// Handed to the stub as `ctx.file_io` and used for orphan cleanup, in
    /// place of the `FileIO` built from the request's S3 credentials. Tests
    /// pass an in-memory `FileIO` so the stub's "uploads" and the cleanup
    /// share one store.
    pub(crate) file_io: FileIO,
}

/// Lightweight reclaimable consumer for one DoExchange partition grant.
#[cfg(feature = "worker-shuffle")]
struct ShufflePartitionConsumer {
    name: String,
    desired: usize,
    minimum: usize,
}

#[cfg(feature = "worker-shuffle")]
impl ReclaimableConsumer for ShufflePartitionConsumer {
    fn name(&self) -> &str {
        &self.name
    }
    fn desired_bytes(&self) -> usize {
        self.desired
    }
    fn minimum_bytes(&self) -> usize {
        self.minimum
    }
    fn try_reclaim(&self, _target: usize) -> usize {
        0
    }
}

/// Default shuffle budget when not configured (64 MiB) — enough for laptop
/// tests and small clusters without an explicit sub-budget.
#[cfg(feature = "worker-shuffle")]
const DEFAULT_SHUFFLE_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// Worker-side compaction deadline used when nothing configured one
/// (embedded / test construction). Mirrors `worker.compaction_timeout_secs`'s
/// own default so a service built without `with_compaction_timeout` is not
/// silently deadline-free — the exact hole finding 608 is about.
const DEFAULT_COMPACTION_TIMEOUT_SECS: u64 = 3600;

/// Depth of the `compact_file_group` progress channel (finding 608).
///
/// Progress frames are advisory heartbeats, so this is a drop queue, not a
/// buffer: when it is full the writer's callback drops the tick rather than
/// awaiting capacity. Sixteen is far more than the coordinator needs (it only
/// rearms a timer per frame) and small enough that a coordinator which
/// stopped reading cannot pin meaningful memory per in-flight group.
const PROGRESS_CHANNEL_CAPACITY: usize = 16;

/// A `compact_file_group` rewrite that did not produce a response, together
/// with what the worker managed to clean up before giving up (finding 608).
struct GroupRewriteFailure {
    /// Human-readable cause, already carrying its own stage prefix.
    cause: String,
    /// True when the cause was the worker-side deadline rather than a
    /// failure inside the rewrite. Selects `DeadlineExceeded` over
    /// `Internal`, which is what lets the coordinator tell "this group is
    /// too big / this worker is wedged" apart from "this group is broken".
    deadline_exceeded: bool,
    /// Orphan cleanup result. `leaked` is what the coordinator must retry.
    cleanup: CleanupOutcome,
}

impl GroupRewriteFailure {
    /// Render as the `Status` that terminates the `do_action` response
    /// stream. The full uploaded/deleted/leaked path lists travel in the
    /// details payload so the coordinator can retry deletion without
    /// scraping the message (finding 608, proposed change 3).
    fn into_status(self, job_id: &str, group_id: u32) -> Status {
        let code = if self.deadline_exceeded {
            tonic::Code::DeadlineExceeded
        } else {
            tonic::Code::Internal
        };
        let message = format!(
            "compact_file_group failed for group {group_id} of job '{job_id}': {}{}",
            self.cause,
            self.cleanup.summary()
        );
        Status::with_details(
            code,
            message,
            bytes::Bytes::from(self.cleanup.to_details_json(job_id, group_id)),
        )
    }
}

/// Run one group rewrite under the worker-side deadline, and clean up its
/// orphan uploads on any failure it can observe (finding 608).
///
/// Free function rather than a method so both the production and the
/// test-seam spawn paths share exactly one supervision body.
async fn supervise_group_rewrite(
    rewrite: futures::future::BoxFuture<'static, sqe_core::Result<CompactGroupResponse>>,
    deadline: Option<std::time::Duration>,
    file_io: FileIO,
    tracker: UploadedPaths,
) -> Result<CompactGroupResponse, GroupRewriteFailure> {
    // `timeout` drops the rewrite future on expiry, which is what actually
    // stops the read/sort/upload; the cleanup below then runs against
    // whatever the tracker recorded up to that point.
    let result = match deadline {
        Some(d) => match tokio::time::timeout(d, rewrite).await {
            Ok(inner) => inner.map_err(|e| (e.to_string(), false)),
            Err(_) => Err((
                format!(
                    "exceeded the worker-side compaction deadline of {d:?} \
                     (worker.compaction_timeout_secs)"
                ),
                true,
            )),
        },
        None => rewrite.await.map_err(|e| (e.to_string(), false)),
    };

    match result {
        Ok(response) => Ok(response),
        Err((cause, deadline_exceeded)) => {
            let cleanup = delete_orphan_uploads(&file_io, &tracker).await;
            Err(GroupRewriteFailure {
                cause,
                deadline_exceeded,
                cleanup,
            })
        }
    }
}

/// Ties the spawned rewrite task to the RPC that started it (finding 608).
///
/// Before this, `do_action` spawned the rewrite and returned a stream that
/// merely joined it: a coordinator that dropped the RPC (heartbeat timeout,
/// job cancelled, coordinator restart) left the worker reading, sorting and
/// uploading the same group the coordinator had already retried elsewhere.
///
/// Dropping this guard -- which happens exactly when the response stream is
/// dropped, because the `done` half of that stream owns it -- does three
/// things in order:
///
/// 1. cancels the [`CancellationToken`] the rewrite selects on, the
///    cooperative stop that lets it quit between record batches;
/// 2. aborts the task, so a rewrite that somehow never reaches an await
///    point still cannot outlive its RPC;
/// 3. spawns a best-effort deletion of every object the rewrite had already
///    uploaded, after waiting for the abort to land so no path can be
///    generated behind the drain.
struct CompactionRpcGuard {
    /// `None` once [`Self::join`] has consumed it: the task is finished and
    /// has already run its own cleanup, so `Drop` must not fire.
    handle: Option<tokio::task::JoinHandle<Result<CompactGroupResponse, GroupRewriteFailure>>>,
    cancel: CancellationToken,
    tracker: UploadedPaths,
    file_io: FileIO,
    job_id: String,
    group_id: u32,
}

impl CompactionRpcGuard {
    fn new(
        handle: tokio::task::JoinHandle<Result<CompactGroupResponse, GroupRewriteFailure>>,
        cancel: CancellationToken,
        tracker: UploadedPaths,
        file_io: FileIO,
        job_id: String,
        group_id: u32,
    ) -> Self {
        Self {
            handle: Some(handle),
            cancel,
            tracker,
            file_io,
            job_id,
            group_id,
        }
    }

    /// Await the rewrite. Disarms the guard: whatever comes back, the task
    /// is over and either it or this call has already cleaned up.
    async fn join(&mut self) -> Result<CompactGroupResponse, Status> {
        let joined = match self.handle.as_mut() {
            Some(handle) => handle.await,
            None => {
                return Err(Status::internal(
                    "compact_file_group: internal error, rewrite task joined twice",
                ))
            }
        };
        self.handle = None;

        match joined {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(failure)) => Err(failure.into_status(&self.job_id, self.group_id)),
            Err(join_err) => {
                // Panic (or an abort from outside this guard): the in-task
                // cleanup never ran, so do it here where we can await it.
                let cleanup = delete_orphan_uploads(&self.file_io, &self.tracker).await;
                let failure = GroupRewriteFailure {
                    cause: format!("rewrite task did not complete: {join_err}"),
                    deadline_exceeded: false,
                    cleanup,
                };
                Err(failure.into_status(&self.job_id, self.group_id))
            }
        }
    }

    /// Delete orphan uploads for a failure discovered after a successful
    /// join (frame encoding), where the task itself saw no error.
    async fn cleanup_now(&self) -> CleanupOutcome {
        delete_orphan_uploads(&self.file_io, &self.tracker).await
    }
}

impl Drop for CompactionRpcGuard {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.cancel.cancel();
        handle.abort();

        let file_io = self.file_io.clone();
        let tracker = self.tracker.clone();
        let job_id = self.job_id.clone();
        let group_id = self.group_id;
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    warn!(
                        job_id = %job_id,
                        group_id,
                        "compact_file_group: response stream dropped; cancelling the rewrite \
                         and deleting whatever it already uploaded"
                    );
                    // Wait for the abort to actually land before draining the
                    // tracker: the rolling writer records a path before it
                    // opens the file, so a path generated between `abort()`
                    // and the task stopping would otherwise slip past the
                    // drain and be left for the orphan sweep.
                    let _ = handle.await;
                    let outcome = delete_orphan_uploads(&file_io, &tracker).await;
                    if !outcome.leaked.is_empty() {
                        warn!(
                            job_id = %job_id,
                            group_id,
                            leaked = outcome.leaked.len(),
                            "compact_file_group: orphan cleanup after RPC drop left files on \
                             object storage; the orphan sweep is the backstop"
                        );
                    }
                });
            }
            Err(_) => {
                // No runtime at drop: nothing can be deleted here. The
                // age-thresholded orphan sweep remains the backstop.
                warn!(
                    job_id = %self.job_id,
                    group_id,
                    "compact_file_group: response stream dropped outside a tokio runtime; \
                     uploaded files are left for the orphan sweep"
                );
            }
        }
    }
}

impl WorkerFlightService {
    pub fn new(metrics: Arc<WorkerMetricsRegistry>, session_ctx: SessionContext) -> Self {
        Self {
            metrics,
            credential_store: CredentialStore::new(),
            session_ctx,
            footer_cache: None,
            #[cfg(feature = "worker-shuffle")]
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            #[cfg(feature = "worker-shuffle")]
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
            ticket_pin: None,
            advertise_url: String::new(),
            accept_hmac_tickets: false,
            spill_manager: None,
            flight_budget: None,
            #[cfg(feature = "worker-shuffle")]
            shuffle_memory_budget: Arc::new(std::sync::atomic::AtomicUsize::new(
                DEFAULT_SHUFFLE_MEMORY_BUDGET,
            )),
            scan_timeout_secs: Arc::new(std::sync::atomic::AtomicU64::new(600)),
            memory_governor: None,
            live_consumers: Arc::new(LiveConsumerRegistry::new()),
            #[cfg(feature = "worker-shuffle")]
            exchange_store: Arc::new(ExchangeAttemptStore::new()),
            compaction_timeout: Some(std::time::Duration::from_secs(
                DEFAULT_COMPACTION_TIMEOUT_SECS,
            )),
            status: WorkerStatus::new(),
            store_factory: executor::s3_object_store_factory(),
            scan_budget: None,
            #[cfg(test)]
            compaction_seam: None,
        }
    }

    /// Create a new service with an externally provided credential store.
    ///
    /// This is useful when the store needs to be shared with other components
    /// (e.g. the executor needs to subscribe before the Flight service starts).
    pub fn with_credential_store(
        metrics: Arc<WorkerMetricsRegistry>,
        session_ctx: SessionContext,
        credential_store: CredentialStore,
    ) -> Self {
        Self {
            metrics,
            credential_store,
            session_ctx,
            footer_cache: None,
            #[cfg(feature = "worker-shuffle")]
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            #[cfg(feature = "worker-shuffle")]
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
            ticket_pin: None,
            advertise_url: String::new(),
            accept_hmac_tickets: false,
            spill_manager: None,
            flight_budget: None,
            #[cfg(feature = "worker-shuffle")]
            shuffle_memory_budget: Arc::new(std::sync::atomic::AtomicUsize::new(
                DEFAULT_SHUFFLE_MEMORY_BUDGET,
            )),
            scan_timeout_secs: Arc::new(std::sync::atomic::AtomicU64::new(600)),
            memory_governor: None,
            live_consumers: Arc::new(LiveConsumerRegistry::new()),
            #[cfg(feature = "worker-shuffle")]
            exchange_store: Arc::new(ExchangeAttemptStore::new()),
            compaction_timeout: Some(std::time::Duration::from_secs(
                DEFAULT_COMPACTION_TIMEOUT_SECS,
            )),
            status: WorkerStatus::new(),
            store_factory: executor::s3_object_store_factory(),
            scan_budget: None,
            #[cfg(test)]
            compaction_seam: None,
        }
    }

    /// Set the Parquet footer cache for this service.
    #[must_use = "with_footer_cache consumes self; bind the returned service"]
    pub fn with_footer_cache(mut self, cache: Arc<FooterCache>) -> Self {
        self.footer_cache = Some(cache);
        self
    }

    /// Set the scan timeout from config (also shared with hot-reload atom).
    #[must_use = "with_scan_timeout consumes self; bind the returned service"]
    pub fn with_scan_timeout(mut self, timeout_secs: u64) -> Self {
        self.scan_timeout = std::time::Duration::from_secs(timeout_secs);
        self.scan_timeout_secs
            .store(timeout_secs, std::sync::atomic::Ordering::Release);
        self
    }

    /// Set the worker-side ceiling on one `compact_file_group` rewrite from
    /// config (`worker.compaction_timeout_secs`, finding 608). 0 disables it.
    #[must_use = "with_compaction_timeout consumes self; bind the returned service"]
    pub fn with_compaction_timeout(mut self, timeout_secs: u64) -> Self {
        self.compaction_timeout = match timeout_secs {
            0 => None,
            secs => Some(std::time::Duration::from_secs(secs)),
        };
        self
    }

    /// TEST-ONLY: replace the `compact_file_group` rewrite with a stub and
    /// point orphan cleanup at an in-memory `FileIO`. See the
    /// `compaction_seam` field doc.
    #[cfg(test)]
    #[must_use = "with_test_compaction_seam consumes self; bind the returned service"]
    pub(crate) fn with_test_compaction_seam(mut self, seam: Arc<CompactionSeam>) -> Self {
        self.compaction_seam = Some(seam);
        self
    }

    /// TEST-ONLY: a sub-second compaction deadline. `worker.compaction_timeout_secs`
    /// has one-second granularity, which is a whole second of wall clock per
    /// deadline test; the seconds-to-`Duration` mapping is covered separately
    /// by `compaction_timeout_maps_zero_to_disabled`.
    #[cfg(test)]
    #[must_use = "with_test_compaction_deadline consumes self; bind the returned service"]
    pub(crate) fn with_test_compaction_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.compaction_timeout = Some(deadline);
        self
    }

    /// The `FileIO` a group rewrite reads/writes through, and that its orphan
    /// cleanup deletes through. Built from the request's S3 credentials in
    /// production; overridden by the test seam.
    fn compaction_file_io(&self, request: &CompactGroupRequest) -> Result<FileIO, Status> {
        #[cfg(test)]
        {
            if let Some(seam) = self.compaction_seam.as_ref() {
                return Ok(seam.file_io.clone());
            }
        }
        build_file_io(&request.s3).map_err(|e| {
            Status::invalid_argument(format!(
                "compact_file_group: unusable S3 configuration for group {}: {e}",
                request.group_id
            ))
        })
    }

    /// Spawn the rewrite under its deadline, cleaning up its own orphan
    /// uploads on every failure it can observe (finding 608).
    ///
    /// The deadline lives INSIDE the task on purpose. The response stream is
    /// `progress.chain(done)`, so the `done` half -- and anything wrapped
    /// around it -- is not polled until the progress channel closes, which
    /// only happens when the rewrite future is dropped. A `timeout()` on the
    /// outer join would therefore never be armed for exactly the wedged
    /// rewrite it exists to bound.
    #[cfg(not(test))]
    fn spawn_group_rewrite(
        &self,
        ctx: GroupRewriteCtx,
        deadline: Option<std::time::Duration>,
    ) -> tokio::task::JoinHandle<Result<CompactGroupResponse, GroupRewriteFailure>> {
        let file_io = ctx.file_io.clone();
        let tracker = ctx.tracker.clone();
        tokio::spawn(supervise_group_rewrite(
            Box::pin(run_group_rewrite(ctx)),
            deadline,
            file_io,
            tracker,
        ))
    }

    /// Test build of [`Self::spawn_group_rewrite`]: routes through the
    /// installed [`CompactionSeam`] stub when a test set one.
    #[cfg(test)]
    fn spawn_group_rewrite(
        &self,
        ctx: GroupRewriteCtx,
        deadline: Option<std::time::Duration>,
    ) -> tokio::task::JoinHandle<Result<CompactGroupResponse, GroupRewriteFailure>> {
        let file_io = ctx.file_io.clone();
        let tracker = ctx.tracker.clone();
        let rewrite: futures::future::BoxFuture<'static, sqe_core::Result<CompactGroupResponse>> =
            match self.compaction_seam.as_ref() {
                Some(seam) => (seam.run)(ctx),
                None => Box::pin(run_group_rewrite(ctx)),
            };
        tokio::spawn(supervise_group_rewrite(rewrite, deadline, file_io, tracker))
    }

    /// Share the scan-timeout atom with hot-reload (keeps existing value).
    #[must_use = "with_scan_timeout_atom consumes self; bind the returned service"]
    pub fn with_scan_timeout_atom(mut self, atom: Arc<std::sync::atomic::AtomicU64>) -> Self {
        atom.store(
            self.scan_timeout_secs
                .load(std::sync::atomic::Ordering::Acquire),
            std::sync::atomic::Ordering::Release,
        );
        self.scan_timeout_secs = atom;
        self
    }

    /// Set the IPC compression for DoGet responses.
    #[must_use = "with_flight_compression consumes self; bind the returned service"]
    pub fn with_flight_compression(mut self, compression: FlightCompression) -> Self {
        self.flight_compression = compression;
        self
    }

    /// Set the IPC compression for DoExchange shuffle responses.
    #[cfg(feature = "worker-shuffle")]
    #[must_use = "with_shuffle_compression consumes self; bind the returned service"]
    pub fn with_shuffle_compression(mut self, compression: FlightCompression) -> Self {
        self.shuffle_compression = compression;
        self
    }

    /// Set the shared secret used to authenticate inbound Flight calls
    /// (`do_get` scan tickets and `do_action("refresh_credentials")`).
    /// An empty secret disables enforcement: callers must explicitly opt
    /// in via `worker.allow_unauthenticated = true` at config load time.
    #[must_use = "with_worker_secret consumes self; bind the returned service"]
    pub fn with_worker_secret(mut self, secret: String) -> Self {
        self.worker_secret = secret;
        self
    }

    /// Finding 601: the pinned coordinator public key used to verify
    /// `ed25519:` ticket signatures.
    #[must_use = "with_ticket_pin consumes self; bind the returned service"]
    pub fn with_ticket_pin(mut self, pin: Arc<crate::ticket_pin::TicketKeyPin>) -> Self {
        self.ticket_pin = Some(pin);
        self
    }

    /// Finding 601: the URL this worker advertises; signed tickets must carry
    /// it as their audience.
    #[must_use = "with_advertise_url consumes self; bind the returned service"]
    pub fn with_advertise_url(mut self, url: String) -> Self {
        self.advertise_url = url;
        self
    }

    /// Finding 601: accept legacy HMAC ticket tags keyed by `worker_secret`.
    #[must_use = "with_accept_hmac_tickets consumes self; bind the returned service"]
    pub fn with_accept_hmac_tickets(mut self, yes: bool) -> Self {
        self.accept_hmac_tickets = yes;
        self
    }

    /// Attach the worker-wide spill manager (Phase 3).
    #[must_use = "with_spill_manager consumes self; bind the returned service"]
    pub fn with_spill_manager(mut self, manager: Arc<SpillManager>) -> Self {
        self.spill_manager = Some(manager);
        self
    }

    /// Set the byte budget for encoded Flight frames awaiting shipment
    /// (`worker.memory.flight_budget`, issue #407).
    ///
    /// Worker-wide on purpose: one stream never fills this budget by itself, so
    /// what it bounds is the SUM of encoded frames across concurrent DoGet
    /// streams. That sum is the term that exhausted the 4 GB worker pool on
    /// SF10 inventory queries.
    #[must_use = "with_flight_budget consumes self; bind the returned service"]
    pub fn with_flight_budget(mut self, budget: ByteBudget) -> Self {
        self.flight_budget = Some(budget);
        self
    }

    /// Set the shuffle partition byte budget used by spillable DoExchange.
    #[cfg(feature = "worker-shuffle")]
    #[must_use = "with_shuffle_memory_budget consumes self; bind the returned service"]
    pub fn with_shuffle_memory_budget(self, bytes: usize) -> Self {
        self.shuffle_memory_budget
            .store(bytes.max(64 * 1024), std::sync::atomic::Ordering::Release);
        self
    }

    /// Share the shuffle-budget atom with hot-reload.
    #[cfg(feature = "worker-shuffle")]
    #[must_use = "with_shuffle_memory_budget_atom consumes self; bind the returned service"]
    pub fn with_shuffle_memory_budget_atom(
        mut self,
        atom: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        // Adopt the shared hot-reload atom as-is. It is already seeded with the
        // resolved shuffle budget by `WorkerHotConfig::new`. Copying this
        // service's default (64 MiB) value into it, as the old direction did,
        // clobbered the configured budget on every boot until the next
        // sqe.toml change forced a hot-reload to write the real value back.
        self.shuffle_memory_budget = atom;
        self
    }

    /// Attach the worker-wide memory governor (Phase 7).
    #[must_use = "with_memory_governor consumes self; bind the returned service"]
    pub fn with_memory_governor(mut self, governor: Arc<MemoryGovernor>) -> Self {
        self.memory_governor = Some(governor);
        self
    }

    /// Replace the object-store factory scans read through. Tests and
    /// embedded hosts only; production keeps the S3 default.
    #[must_use = "with_object_store_factory consumes self; bind the returned service"]
    pub fn with_object_store_factory(mut self, factory: ObjectStoreFactory) -> Self {
        self.store_factory = factory;
        self
    }

    /// Share one scan byte budget across every `do_get` instead of deriving
    /// one per scan from the session pool.
    #[must_use = "with_scan_budget consumes self; bind the returned service"]
    pub fn with_scan_budget(mut self, budget: ByteBudget) -> Self {
        self.scan_budget = Some(budget);
        self
    }

    /// Share a [`WorkerStatus`] created elsewhere (the heartbeat task reads
    /// the same one).
    #[must_use = "with_status consumes self; bind the returned service"]
    pub fn with_status(mut self, status: Arc<WorkerStatus>) -> Self {
        self.status = status;
        self
    }

    /// Process identity and load counters (finding 609).
    pub fn status(&self) -> &Arc<WorkerStatus> {
        &self.status
    }

    /// Current load vector for the heartbeat hello.
    ///
    /// `memory_limit_bytes` is the session pool limit (0 when unbounded).
    /// `scan_budget_free_bytes` is what the shared scan budget would still
    /// admit, or, without one, what the pool would (limit minus reserved).
    /// `spill_free_bytes` is 0 = unknown: the `SegmentStore` trait exposes
    /// used bytes only, not the volume's free space. `inflight_scans` comes
    /// from [`WorkerStatus`].
    pub fn capacity(&self) -> WorkerCapacity {
        let mut capacity = (self.capacity_probe())();
        capacity.inflight_scans = self.status.inflight_scans();
        capacity
    }

    /// A closure that measures [`Self::capacity`] without holding the
    /// service itself, for [`WorkerStatus::set_capacity_probe`].
    pub fn capacity_probe(&self) -> crate::status::CapacityProbe {
        use datafusion::execution::memory_pool::MemoryLimit;
        let pool = self.session_ctx.runtime_env().memory_pool.clone();
        let scan_budget = self.scan_budget.clone();
        Arc::new(move || {
            let (limit, free) = match pool.memory_limit() {
                MemoryLimit::Finite(limit) => {
                    (limit as u64, limit.saturating_sub(pool.reserved()) as u64)
                }
                _ => (0, 0),
            };
            let scan_budget_free_bytes = match &scan_budget {
                Some(budget) => budget.free_bytes() as u64,
                None => free,
            };
            WorkerCapacity {
                memory_limit_bytes: limit,
                scan_budget_free_bytes,
                spill_free_bytes: 0,
                inflight_scans: 0,
            }
        })
    }

    /// Spill manager when local spill is enabled at bootstrap.
    pub fn spill_manager(&self) -> Option<&Arc<SpillManager>> {
        self.spill_manager.as_ref()
    }

    /// Memory governor when wired at bootstrap.
    pub fn memory_governor(&self) -> Option<&Arc<MemoryGovernor>> {
        self.memory_governor.as_ref()
    }

    /// Live consumer registry for join/agg/sort reclaim.
    pub fn live_consumers(&self) -> &Arc<LiveConsumerRegistry> {
        &self.live_consumers
    }

    /// Durable exchange attempt store (Phase 8).
    #[cfg(feature = "worker-shuffle")]
    pub fn exchange_store(&self) -> &Arc<ExchangeAttemptStore> {
        &self.exchange_store
    }

    /// Configured shuffle memory budget in bytes.
    #[cfg(feature = "worker-shuffle")]
    pub fn shuffle_memory_budget(&self) -> usize {
        self.shuffle_memory_budget
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Release every worker-side registry entry keyed by `query_id`.
    ///
    /// The DoExchange registries are keyed by a network-supplied
    /// `query_id`/`stage_id`, so the coordinator's "query finished" action must
    /// call this or they grow for the lifetime of the process (review finding
    /// 600). Returns the number of entries dropped across the receiver
    /// registry, the attempt gate and the durable exchange store.
    #[cfg(feature = "worker-shuffle")]
    pub async fn forget_query(&self, query_id: &str) -> usize {
        self.shuffle_manager.forget_query(query_id).await
            + self.exchange_store.forget_query(query_id)
    }

    /// Live scan timeout, honouring hot-reloaded `scan_timeout_secs` when set.
    pub fn live_scan_timeout(&self) -> std::time::Duration {
        let secs = self
            .scan_timeout_secs
            .load(std::sync::atomic::Ordering::Acquire);
        if secs == 0 {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_secs(secs)
        }
    }

    /// Constant-time check of the `x-sqe-worker-secret` metadata header.
    /// Returns `Ok(())` when the secret matches or when no secret is
    /// configured. Returns `Status::unauthenticated` on mismatch.
    fn verify_worker_secret(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        if self.worker_secret.is_empty() {
            return Ok(());
        }
        use subtle::ConstantTimeEq;
        let provided = metadata
            .get(WORKER_SECRET_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided_bytes = provided.as_bytes();
        let secret_bytes = self.worker_secret.as_bytes();
        if provided_bytes.len() != secret_bytes.len()
            || !bool::from(provided_bytes.ct_eq(secret_bytes))
        {
            return Err(Status::unauthenticated("Invalid worker secret"));
        }
        Ok(())
    }

    /// Verify the HMAC-SHA256 signature over the raw ScanTask ticket bytes
    /// (issue #206). Recomputes the tag over `ticket_bytes` keyed by the shared
    /// `worker_secret` and constant-time compares it to the
    /// `x-sqe-scan-signature` header.
    ///
    /// Signing the wire bytes (rather than a re-serialized struct) means the
    /// tag covers exactly what we decode: file paths, credentials, and the
    /// #233 predicate/limit fields. A tampered ticket (swapped path, stripped
    /// predicate) changes the bytes and fails verification.
    ///
    /// When `worker_secret` is empty the deployment opted into
    /// `worker.allow_unauthenticated`; there is no key to sign with, so this
    /// returns `Ok(())` (the insecure dev path, already gated at config load).
    fn verify_scan_signature(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        ticket_bytes: &[u8],
    ) -> Result<(), Status> {
        if self.worker_secret.is_empty() {
            return Ok(());
        }
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        use subtle::ConstantTimeEq;

        let provided = metadata
            .get(SCAN_SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        // Finding 601: an `ed25519:` tag is checked against the pinned
        // coordinator public key. Only the coordinator can produce it, so a
        // compromised worker holding the shared secret cannot forge tickets.
        if sqe_core::ticket_signing::TicketVerifier::is_asymmetric_signature(provided) {
            let verifier = self.ticket_pin.as_ref().and_then(|p| p.verifier());
            return match verifier {
                Some(v) if v.verify(ticket_bytes, provided) => Ok(()),
                Some(_) => Err(Status::unauthenticated("Invalid scan task signature")),
                None => Err(Status::unauthenticated(
                    "Coordinator ticket-signing key not pinned yet; retry after the next heartbeat",
                )),
            };
        }
        if !self.accept_hmac_tickets {
            return Err(Status::unauthenticated(
                "HMAC scan-ticket signatures are not accepted; set worker.accept_hmac_tickets \
                 only while migrating from pre-Ed25519 coordinators",
            ));
        }

        let mut mac = <Hmac<Sha256>>::new_from_slice(self.worker_secret.as_bytes())
            .expect("HMAC accepts keys of any length");
        mac.update(ticket_bytes);
        let expected = mac.finalize().into_bytes();
        let expected_hex = hex_encode(&expected);

        let provided_bytes = provided.as_bytes();
        let expected_bytes = expected_hex.as_bytes();
        if provided_bytes.len() != expected_bytes.len()
            || !bool::from(provided_bytes.ct_eq(expected_bytes))
        {
            return Err(Status::unauthenticated("Invalid scan task signature"));
        }
        Ok(())
    }

    /// Verify the HMAC-SHA256 signature over the raw `CompactGroupRequest`
    /// bytes (Phase 4c Task 3). Delegates the tag computation to
    /// `sqe_compaction::wire::verify` (rather than recomputing the HMAC
    /// locally, as [`Self::verify_scan_signature`] does for scan tickets) so
    /// the worker and the Task 4 coordinator job runner that signs these
    /// requests share one signing implementation instead of two parallel
    /// HMAC call sites that could drift apart.
    ///
    /// When `worker_secret` is empty the deployment opted into
    /// `worker.allow_unauthenticated`; there is no key to sign with, so this
    /// returns `Ok(())`, exactly like `verify_scan_signature`'s dev-mode
    /// path.
    fn verify_compaction_signature(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        body: &[u8],
    ) -> Result<(), Status> {
        if self.worker_secret.is_empty() {
            return Ok(());
        }
        let provided = metadata
            .get(COMPACT_SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !sqe_compaction::wire::verify(body, provided, &self.worker_secret) {
            return Err(Status::unauthenticated(
                "Invalid compaction request signature",
            ));
        }
        Ok(())
    }

    /// Finding 601: a signed ticket names the worker it was issued to and a
    /// deadline. Refuse tickets addressed elsewhere, expired, or (on the
    /// asymmetric path) lacking either, so a captured ticket cannot be
    /// replayed to another worker or later.
    fn verify_ticket_binding(
        &self,
        task: &ScanTask,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), Status> {
        let asymmetric = metadata
            .get(SCAN_SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(sqe_core::ticket_signing::TicketVerifier::is_asymmetric_signature);
        if asymmetric && (task.audience.is_empty() || task.expires_at == 0) {
            return Err(Status::unauthenticated(
                "Signed scan ticket lacks audience or expiry",
            ));
        }
        if !task.audience.is_empty()
            && !self.advertise_url.is_empty()
            && task.audience != self.advertise_url
        {
            warn!(
                audience = %task.audience,
                advertise_url = %self.advertise_url,
                "Rejected scan ticket addressed to another worker"
            );
            return Err(Status::unauthenticated(
                "Scan ticket audience does not match this worker",
            ));
        }
        if task.expires_at != 0 && chrono::Utc::now().timestamp() > task.expires_at {
            return Err(Status::unauthenticated("Scan ticket has expired"));
        }
        Ok(())
    }

    /// Returns a reference to the credential store for use by executors.
    pub fn credential_store(&self) -> &CredentialStore {
        &self.credential_store
    }

    /// Returns a reference to the shuffle manager.
    #[cfg(feature = "worker-shuffle")]
    pub fn shuffle_manager(&self) -> &ShuffleManager {
        &self.shuffle_manager
    }

    /// Spillable DoExchange path: append into a per-attempt
    /// [`SpillablePartitionBuffer`], finish (force residual spill), then stream
    /// segments back without holding the full exchange volume in RAM.
    #[cfg(feature = "worker-shuffle")]
    async fn do_exchange_spill(
        &self,
        query_id: &str,
        stage_id: &str,
        partition_id: u32,
        attempt_id: u32,
        spill_manager: Arc<SpillManager>,
        mut flight_batch_stream: FlightRecordBatchStream,
    ) -> Result<Response<BoxStream<FlightData>>, Status> {
        let attempt_gate = self.shuffle_manager.attempts().clone();
        let budget_bytes = self.shuffle_memory_budget();

        // Phase 8: reject late data against durable exchange winner registry.
        let task_key = TaskKey::new(query_id, stage_id, format!("p{partition_id}"), partition_id);
        if !self.exchange_store.admit(&task_key, attempt_id) {
            return Err(Status::aborted(format!(
                "rejecting late exchange attempt query={query_id} stage={stage_id} \
                 partition={partition_id} attempt={attempt_id}"
            )));
        }

        // Phase 7: admit a shuffle partition grant through the worker governor
        // when configured. RAII guard releases the grant on all exit paths.
        let grant_name = format!("{stage_id}-p{partition_id}-a{attempt_id}");
        let consumer = ShufflePartitionConsumer {
            name: grant_name.clone(),
            desired: budget_bytes,
            // Minimum: enough for one soft-watermark slice so a partition can
            // always spill-progress rather than fail admission immediately.
            minimum: (budget_bytes / 8).max(64 * 1024),
        };
        // Hold for the full DoExchange spill path; Drop releases the grant.
        let mut _grant_guard: Option<sqe_spill::GrantGuard> = None;
        let granted_capacity = if let Some(gov) = self.memory_governor.clone() {
            match gov.try_admit_guarded(
                AdmissionRequest {
                    query_id: query_id.to_string(),
                    name: grant_name.clone(),
                    class: WorkloadClass::Shuffle,
                    desired_bytes: consumer.desired,
                    minimum_bytes: consumer.minimum,
                },
                &consumer,
            ) {
                Ok((grant, guard)) => {
                    let cap = grant.capacity_bytes();
                    debug!(
                        query_id = %query_id,
                        stage_id = %stage_id,
                        partition_id = partition_id,
                        granted = cap,
                        desired = budget_bytes,
                        admissions = gov.admissions(),
                        "DoExchange shuffle grant admitted"
                    );
                    _grant_guard = Some(guard);
                    cap
                }
                Err(sqe_spill::AdmissionDecision::Rejected {
                    reason,
                    pool_bytes,
                    minima_sum,
                }) => {
                    return Err(Status::resource_exhausted(format!(
                        "shuffle memory admission rejected for query={query_id} \
                         stage={stage_id} partition={partition_id}: {reason} \
                         (pool={pool_bytes}, minima={minima_sum})"
                    )));
                }
                Err(sqe_spill::AdmissionDecision::Granted(_)) => budget_bytes,
            }
        } else {
            budget_bytes
        };

        // Carve spill-read/merge headroom out of the *granted* capacity so
        // writers cannot pin the entire grant while a drain still needs memory.
        let (writer_cap, read_headroom) = split_default_read_headroom(granted_capacity);
        let pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(
            writer_cap.max(1024 * 1024),
        ));
        let budget = ByteBudget::new(
            format!("shuffle-{query_id}-{stage_id}-p{partition_id}"),
            writer_cap,
            Some(pool),
        );
        let _read_headroom = read_headroom;
        debug!(
            query_id = %query_id,
            stage_id = %stage_id,
            partition_id = partition_id,
            writer_cap = writer_cap,
            read_headroom = read_headroom,
            granted_capacity = granted_capacity,
            parent_budget = budget_bytes,
            "DoExchange shuffle budget split (writer + spill-read headroom)"
        );
        let scope = SpillScope::new(query_id, stage_id, "do_exchange", partition_id, attempt_id);

        // Lazy buffer: schema comes from the first non-empty batch.
        let mut buffer: Option<SpillablePartitionBuffer> = None;
        let mut batch_count = 0u64;
        let mut rejected = 0u64;
        let mut peak_resident = 0usize;
        let mut schema: Option<arrow_schema::SchemaRef> = None;

        while let Some(batch_result) = flight_batch_stream.next().await {
            match batch_result {
                Ok(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if !attempt_gate
                        .admit(query_id, stage_id, partition_id, attempt_id)
                        .await
                    {
                        rejected += 1;
                        if let Some(ref mut buf) = buffer {
                            buf.cancel();
                        }
                        break;
                    }
                    if buffer.is_none() {
                        schema = Some(batch.schema());
                        buffer = Some(SpillablePartitionBuffer::new(
                            spill_manager.clone(),
                            scope.clone(),
                            batch.schema(),
                            budget.clone(),
                            Some(self.metrics.clone()),
                        ));
                    }
                    let buf = buffer.as_mut().expect("buffer just created");
                    if let Err(e) = buf.append(batch).await {
                        buf.fail(e.to_string());
                        return Err(Status::resource_exhausted(format!(
                            "shuffle spill intake failed: {e}"
                        )));
                    }
                    peak_resident = peak_resident.max(buf.resident_bytes());
                    batch_count += 1;
                }
                Err(e) => {
                    // Client disconnect / RPC cancel must not finish partial data.
                    if let Some(ref mut buf) = buffer {
                        if is_exchange_cancelled(&e) {
                            buf.cancel();
                        } else {
                            buf.fail(e.to_string());
                        }
                    }
                    if is_exchange_cancelled(&e) {
                        return Err(Status::cancelled(format!(
                            "DoExchange cancelled mid-intake: {e}"
                        )));
                    }
                    return Err(Status::internal(format!(
                        "Error decoding flight data in DoExchange: {e}"
                    )));
                }
            }
        }

        debug!(
            query_id = %query_id,
            stage_id = %stage_id,
            partition_id = partition_id,
            attempt_id = attempt_id,
            batch_count = batch_count,
            rejected_late = rejected,
            peak_resident = peak_resident,
            budget = budget_bytes,
            "DoExchange spill intake complete"
        );

        if rejected > 0 {
            // Drop buffer (armed guard cleans spill) without shipping partial rows.
            drop(buffer);
            return Err(Status::aborted(format!(
                "rejecting late shuffle data for query={query_id} stage={stage_id} \
                 partition={partition_id} attempt={attempt_id} after {batch_count} batches"
            )));
        }

        // Empty exchange: no schema observed — return an empty Flight stream.
        let Some(schema) = schema else {
            let shuffle_opts = ipc_options_for(self.shuffle_compression)?;
            let empty =
                futures::stream::empty::<Result<RecordBatch, arrow_flight::error::FlightError>>();
            let flight_stream = FlightDataEncoderBuilder::new()
                .with_schema(Arc::new(arrow_schema::Schema::empty()))
                .with_options(shuffle_opts)
                .build(empty)
                .map_err(Status::from);
            return Ok(Response::new(
                Box::pin(flight_stream) as BoxStream<FlightData>
            ));
        };

        let mut buffer = buffer.expect("schema implies buffer");
        let manifest = buffer
            .finish()
            .await
            .map_err(|e| Status::internal(format!("shuffle spill finish failed: {e}")))?;
        info!(
            query_id = %query_id,
            stage_id = %stage_id,
            partition_id = partition_id,
            rows = manifest.rows,
            batches = manifest.batches,
            segments = manifest.segments,
            physical_bytes = manifest.physical_bytes,
            peak_resident = peak_resident,
            "DoExchange spill partition finished"
        );

        // Phase 8: publish durable attempt manifest and commit as winner so
        // retries can reuse segments and late lower attempts are rejected.
        let task_id = format!("p{partition_id}");
        let mut attempt_manifest =
            AttemptManifest::new(query_id, stage_id, &task_id, partition_id, attempt_id);
        attempt_manifest.rows = manifest.rows;
        attempt_manifest.batches = manifest.batches;
        attempt_manifest.logical_bytes = manifest.logical_bytes;
        attempt_manifest.physical_bytes = manifest.physical_bytes;
        attempt_manifest.segments = (0..manifest.segments)
            .map(|i| format!("seg-{i:08}"))
            .collect();
        match self.exchange_store.publish(attempt_manifest) {
            Err(e) => {
                // publish() rejects only when this attempt is strictly lower
                // than an already-committed winner, i.e. it is a stale loser.
                // Falling through to drain would stream the losing attempt's
                // rows to the consumer and duplicate the winner's output on
                // retry, so abort rather than warn-and-continue.
                return Err(Status::aborted(format!(
                    "exchange attempt superseded by a committed winner: {e}"
                )));
            }
            Ok(()) => {
                let key = TaskKey::new(query_id, stage_id, &task_id, partition_id);
                if let Err(e) = self.exchange_store.commit_winner(&key, attempt_id) {
                    // Higher winner already committed — treat as late attempt.
                    return Err(Status::aborted(format!(
                        "exchange winner commit rejected: {e}"
                    )));
                }
            }
        }

        let drain = buffer
            .into_drain_stream()
            .await
            .map_err(|e| Status::internal(format!("shuffle spill drain open failed: {e}")))?;

        // Keep the governor grant alive until the client finishes draining
        // (or disconnects). Dropping it at the end of this function would free
        // the grant while FlightData is still in flight.
        let output_stream = GrantHeldStream {
            inner: Box::pin(drain.map(|item| {
                item.map_err(|e| {
                    arrow_flight::error::FlightError::Tonic(Box::new(Status::internal(
                        e.to_string(),
                    )))
                })
            })),
            _guard: _grant_guard,
        };

        let shuffle_opts = ipc_options_for(self.shuffle_compression)?;
        self.metrics
            .flight_encode_resident_bytes
            .set(peak_resident as f64);
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(shuffle_opts)
            .build(output_stream)
            .map_err(Status::from);

        Ok(Response::new(
            Box::pin(flight_stream) as BoxStream<FlightData>
        ))
    }

    /// Wrap this service in a tonic server with an explicit inbound message
    /// ceiling (#432).
    ///
    /// tonic's 4 MiB receive default silently caps how large a scan ticket may
    /// be, and a ticket carrying a pushed runtime-filter predicate over a few
    /// hundred thousand distinct keys clears it. The coordinator refuses to
    /// send an oversized ticket in the first place, so this is the second of
    /// two guards, not the only one.
    pub fn into_server(self, max_decoding_message_bytes: usize) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self).max_decoding_message_size(max_decoding_message_bytes)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Holds a governor [`sqe_spill::GrantGuard`] for the lifetime of a Flight
/// response stream so shuffle memory is not released mid-drain.
#[cfg(feature = "worker-shuffle")]
struct GrantHeldStream<S> {
    inner: Pin<Box<S>>,
    _guard: Option<sqe_spill::GrantGuard>,
}

#[cfg(feature = "worker-shuffle")]
impl<S> Stream for GrantHeldStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Detect client disconnect / RPC cancellation on the DoExchange request stream.
#[cfg(feature = "worker-shuffle")]
fn is_exchange_cancelled(err: &arrow_flight::error::FlightError) -> bool {
    match err {
        arrow_flight::error::FlightError::Tonic(status) => {
            matches!(status.code(), tonic::Code::Cancelled | tonic::Code::Aborted)
        }
        other => {
            let s = other.to_string().to_ascii_lowercase();
            s.contains("cancel")
                || s.contains("broken pipe")
                || s.contains("connection reset")
                || s.contains("h2 protocol error")
        }
    }
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
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
        // Reject anonymous callers before parsing the ticket: the ticket
        // body carries the user's S3 credentials, so we must not log the
        // scan task or even validate its shape until the coordinator has
        // proven itself.
        self.verify_worker_secret(request.metadata())?;

        // Verify the HMAC over the exact ticket bytes (#206) before decoding or
        // executing. This proves the coordinator authored this precise task; a
        // tampered ticket (swapped file path, stripped predicate) fails here.
        // Must run before from_bytes so a forged task is never even parsed.
        let metadata_owned = request.metadata().clone();
        self.verify_scan_signature(&metadata_owned, &request.get_ref().ticket)?;

        // Extract W3C TraceContext from incoming gRPC metadata so this
        // worker span becomes a child of the coordinator's trace.
        let parent_cx = extract_trace_context(request.metadata());

        let ticket = request.into_inner();

        // Finding 109: one decoder that checks the version range, that the
        // populated fields do not need a higher version than declared, and
        // that no top-level key was silently dropped. All three are the
        // coordinator's fault, so they are `TaskInvalid` (INVALID_ARGUMENT).
        let scan_task = ScanTask::decode_for_worker(&ticket.ticket, SCAN_TASK_VERSION_CURRENT)
            .map_err(|e| WorkerFault::TaskInvalid.into_status(format!("ScanTask rejected: {e}")))?;
        self.verify_ticket_binding(&scan_task, &metadata_owned)?;

        let worker_span = info_span!(
            "sqe.worker.scan",
            fragment_id = %scan_task.fragment_id,
            file_count = scan_task.data_file_paths.len(),
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        // Link this span to the coordinator's trace
        let _set_parent_result = worker_span.set_parent(parent_cx);
        sqe_metrics::propagation::record_trace_fields(&worker_span);

        let metrics = self.metrics.clone();
        let credential_store = self.credential_store.clone();
        let session_ctx = self.session_ctx.clone();
        let footer_cache = self.footer_cache.clone();
        let scan_timeout = self.live_scan_timeout();
        let flight_compression = self.flight_compression;
        // Cloned (not Option-matched at the call site) so every DoGet charges
        // its frames: an unconfigured worker gets the pool-derived default
        // rather than silently reverting to the ungated path (#407).
        let flight_budget = self
            .flight_budget
            .clone()
            .unwrap_or_else(|| default_flight_budget(&self.session_ctx));
        let store_factory = self.store_factory.clone();
        let scan_budget = self.scan_budget.clone();
        let status = Arc::clone(&self.status);
        let stream_span = worker_span.clone();
        let result_span = worker_span.clone();
        let result = async move {
            info!(
                fragment_id = %scan_task.fragment_id,
                file_count = scan_task.data_file_paths.len(),
                "Worker received scan task"
            );

            // Finding 609: in flight from here until the response stream is
            // dropped, whichever way that happens.
            let inflight_guard = status.scan_started();

            // Subscribe to credential updates for this fragment. The guard
            // removes the entry on drop so timeouts, setup errors, and panics
            // can't leak `watch::Sender`s into the store. Issue #76.
            let cred_rx = credential_store.subscribe(&scan_task.fragment_id).await;
            let fragment_id = scan_task.fragment_id.clone();
            let cleanup_guard = credential_store.cleanup_guard(fragment_id.clone());

            // The pushed-down predicate (#233) is carried inside `scan_task`
            // (`predicate_proto`) and decoded by the executor; the late
            // materialization RowFilter is wired from it there.
            let prepare = executor::execute_scan_streaming_with_factory(
                scan_task,
                Some(metrics.clone()),
                session_ctx.clone(),
                Some(cred_rx),
                footer_cache.clone(),
                None, // Coordinator metrics (workers don't have coordinator registry)
                store_factory,
                scan_budget,
            );

            let (schema, batch_stream) = if scan_timeout.is_zero() {
                prepare.await
            } else {
                tokio::time::timeout(scan_timeout, prepare)
                    .await
                    .map_err(|_| {
                        warn!(
                            fragment_id = %fragment_id,
                            timeout_secs = scan_timeout.as_secs(),
                            "Scan task setup timed out"
                        );
                        Status::deadline_exceeded(format!(
                            "Scan task {} setup timed out after {}s",
                            fragment_id,
                            scan_timeout.as_secs()
                        ))
                    })?
            }
            .map_err(|e| {
                // Finding 602: classify from the typed chain (the executor
                // tagged it) and ship the fault in the Status metadata plus
                // the matching gRPC code, so the coordinator can tell a
                // user's expired token from a worker fault.
                let fault = executor::scan_fault(&e);
                status.record_fault(fault);
                warn!(error = %format!("{e:#}"), fault = %fault, "Scan task setup failed");
                fault.into_status(format!("Scan execution failed: {e:#}"))
            })?;

            // Mid-stream failures travel as `FlightError::Tonic(Status)`:
            // the encoder passes them through untouched and `Status::from`
            // unwraps the box, so the fault metadata reaches the trailers.
            let status_for_stream = Arc::clone(&status);
            let fragment_for_stream = fragment_id.clone();
            let mapped_stream = batch_stream.map(move |item| match item {
                Ok(accounted) => Ok(accounted),
                Err(e) => {
                    let fault = executor::scan_fault(&e);
                    status_for_stream.record_fault(fault);
                    warn!(
                        fragment_id = %fragment_for_stream,
                        error = %format!("{e:#}"),
                        fault = %fault,
                        "Scan task failed mid-stream"
                    );
                    Err(arrow_flight::error::FlightError::Tonic(Box::new(
                        fault.into_status(format!("Scan failed: {e:#}")),
                    )))
                }
            });
            // Stream lifetime carries both guards: once the encoder finishes
            // (or the client disconnects mid-stream) they drop, the credential
            // entry is removed via `tokio::spawn` and the in-flight count
            // steps down. No sentinel item is ever emitted.
            let mapped_stream = HoldUntilEnd::new(mapped_stream, (cleanup_guard, inflight_guard));
            // AccountedFlightStream: retain each batch's scan-budget permit
            // until the Flight encoder polls for the next batch (or the
            // client disconnects and the stream drops).
            let batch_for_encoder = AccountedEncodeStream {
                inner: Box::pin(mapped_stream),
                held_permit: None,
                metrics: metrics.clone(),
            };

            let schema_arc = Arc::new((*schema).clone());
            let ipc_opts = ipc_options_for(flight_compression)?;
            // #407: charge the ENCODED frame, not just gauge it. The previous
            // `.map` set flight_inflight_bytes to the last frame's size and
            // gated on nothing, so N concurrent streams could hold N encoded
            // frames outside the DataFusion pool. Backpressure propagates:
            // waiting for a frame permit stops polling the encoder, which stops
            // draining AccountedEncodeStream, which stops the scan queue.
            let flight_stream = accounted_frame_stream(
                FlightDataEncoderBuilder::new()
                    .with_schema(schema_arc)
                    .with_options(ipc_opts)
                    .build(batch_for_encoder),
                flight_budget,
                metrics.clone(),
            )
            .map_err(Status::from);
            let flight_stream = tracing_futures::Instrument::instrument(flight_stream, stream_span);

            Ok(Response::new(Box::pin(flight_stream) as Self::DoGetStream))
        }
        .instrument(worker_span)
        .await;
        if result.is_ok() {
            result_span.record("otel.status_code", "OK");
        } else {
            result_span.record("otel.status_code", "ERROR");
        }
        result
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let (metadata, _, action) = request.into_parts();

        match action.r#type.as_str() {
            "health_check" => {
                debug!("Health check OK");
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "refresh_credentials" => {
                // Credential refresh swaps the S3 keys that the executor
                // will use for the next file read. An attacker who pushes
                // their own bucket here either exfiltrates data or causes
                // a table-swap. Require the worker secret on every call.
                self.verify_worker_secret(&metadata)?;

                let creds: RefreshableCredentials =
                    serde_json::from_slice(&action.body).map_err(|e| {
                        Status::invalid_argument(format!(
                            "Failed to decode RefreshableCredentials: {e}"
                        ))
                    })?;

                info!(
                    fragment_id = %creds.fragment_id,
                    expiry = %creds.expiry,
                    "Received credential refresh from coordinator"
                );

                let published = self.credential_store.publish(creds).await;

                let body = if published {
                    b"accepted".to_vec()
                } else {
                    b"no_active_scan".to_vec()
                };

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(body),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "compact_file_group" => {
                // Compaction requests carry S3 credentials for the group's
                // input/output files and drive a real rewrite. Require the
                // worker secret AND the request-specific HMAC (#Phase 4c
                // Task 3, mirrors the do_get scan-ticket gate from #206)
                // over the exact wire bytes before decoding.
                self.verify_worker_secret(&metadata)?;
                self.verify_compaction_signature(&metadata, &action.body)?;

                let request = CompactGroupRequest::from_bytes(&action.body).map_err(|e| {
                    Status::invalid_argument(format!("Failed to decode CompactGroupRequest: {e}"))
                })?;
                let group_id = request.group_id;

                info!(
                    job_id = %request.job_id,
                    group_id,
                    table = %request.table_ident,
                    file_count = request.group_file_paths.len(),
                    "Received compact_file_group request"
                );

                // Run the rewrite on its own task and drain its progress
                // channel concurrently, so `Progress` frames reach the
                // coordinator WHILE the rewrite is still running instead of
                // being buffered until it finishes. This is what makes the
                // coordinator's `group_heartbeat_timeout` (which rearms on
                // every frame it receives, see
                // `compaction_dispatch::dispatch_group_to_worker`) a real
                // bound on a mid-compute stall: previously both frames were
                // only ever produced back-to-back after the whole group was
                // computed, so the heartbeat only ever bounded frame
                // delivery, never the compute itself.
                //
                // Finding 608 adds the other half of that story: the task is
                // now tied to this RPC (`CompactionRpcGuard`), bounded by
                // `worker.compaction_timeout_secs`, and cleans up after
                // itself on every failure path. The channel is bounded, so a
                // coordinator that stops reading drops frames instead of
                // growing a queue for the life of the rewrite.
                let (progress_tx, progress_rx) =
                    tokio::sync::mpsc::channel::<u64>(PROGRESS_CHANNEL_CAPACITY);
                let file_io = self.compaction_file_io(&request)?;
                let tracker = new_upload_tracker();
                let cancel = CancellationToken::new();
                let deadline = self.compaction_timeout;
                let job_id = request.job_id.clone();

                let handle = self.spawn_group_rewrite(
                    GroupRewriteCtx {
                        session_ctx: self.session_ctx.clone(),
                        request: request.clone(),
                        progress_tx: Some(progress_tx),
                        tracker: tracker.clone(),
                        cancel: cancel.clone(),
                        file_io: file_io.clone(),
                    },
                    deadline,
                );
                let guard = CompactionRpcGuard::new(
                    handle,
                    cancel,
                    tracker,
                    file_io,
                    job_id.clone(),
                    group_id,
                );

                let progress_stream = tokio_stream::wrappers::ReceiverStream::new(progress_rx).map(
                    move |rows_read| {
                        let frame = CompactGroupFrame::Progress {
                            group_id,
                            rows_read,
                        };
                        let body = frame.to_bytes().map_err(|e| {
                            Status::internal(format!("Failed to encode CompactGroupFrame: {e}"))
                        })?;
                        Ok(arrow_flight::Result {
                            body: bytes::Bytes::from(body),
                        })
                    },
                );

                // `guard` lives inside this future, which is owned by the
                // returned stream: dropping the response drops the future,
                // which drops the guard, which cancels + aborts + cleans up.
                let done_stream = stream::once(async move {
                    let mut guard = guard;
                    let response = guard.join().await?;
                    let frame = CompactGroupFrame::Done(response);
                    match frame.to_bytes() {
                        Ok(body) => Ok(arrow_flight::Result {
                            body: bytes::Bytes::from(body),
                        }),
                        Err(e) => {
                            // The rewrite succeeded but its result can never
                            // be delivered, so its output files are orphans
                            // exactly like a failed rewrite's.
                            let cleanup = guard.cleanup_now().await;
                            Err(Status::internal(format!(
                                "Failed to encode CompactGroupFrame for group {group_id} of \
                                 job '{job_id}': {e}{}",
                                cleanup.summary()
                            )))
                        }
                    }
                });

                Ok(Response::new(Box::pin(progress_stream.chain(done_stream))))
            }
            other => Err(Status::unimplemented(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Workers don't support handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Workers don't support list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Workers don't support get_flight_info",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Workers don't support get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Workers don't support do_put"))
    }

    /// Inert DoExchange for the default build (`worker-shuffle` off).
    ///
    /// No query plan produces shuffle traffic, so serving an intake here would
    /// be pure attack surface: a secret holder could fill the spill volume and
    /// grow two process-lifetime maps keyed by a caller-chosen
    /// `query_id`/`stage_id` (review finding 600). With the feature off those
    /// maps do not exist in this binary at all; the secret gate is kept so an
    /// unauthenticated caller still learns nothing about the build.
    #[cfg(not(feature = "worker-shuffle"))]
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.verify_worker_secret(request.metadata())?;
        Err(Status::unimplemented(
            "DoExchange shuffle intake is not enabled on this worker",
        ))
    }

    #[cfg(feature = "worker-shuffle")]
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        // Gate the shuffle stream behind the worker secret before consuming
        // the request, exactly as `do_get`/`refresh_credentials` do. Without
        // this an attacker with network access could push arbitrary
        // RecordBatches into a stage receiver (result poisoning) or drain a
        // partition channel for an in-flight distributed query.
        self.verify_worker_secret(request.metadata())?;

        let mut stream = request.into_inner();

        // 1. Read the first FlightData message to get the descriptor.
        let first_msg = stream.next().await.ok_or_else(|| {
            Status::invalid_argument("DoExchange stream ended before descriptor message")
        })??;

        let descriptor = first_msg.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument("First DoExchange message must contain a FlightDescriptor")
        })?;

        let exchange_desc = ExchangeDescriptor::from_bytes(&descriptor.cmd).map_err(|e| {
            Status::invalid_argument(format!(
                "Failed to decode ExchangeDescriptor from descriptor cmd: {e}"
            ))
        })?;

        let (query_id, stage_id) = exchange_desc.stage_key();
        let partition_id = exchange_desc.partition_id();
        let attempt_id = exchange_desc.attempt_id();

        info!(
            query_id = %query_id,
            stage_id = %stage_id,
            partition_id = partition_id,
            attempt_id = attempt_id,
            "DoExchange: receiving shuffle data"
        );

        // Phase 4: reject late data from a losing/obsolete task attempt before
        // touching partition buffers.
        if !self
            .shuffle_manager
            .attempts()
            .admit(&query_id, &stage_id, partition_id, attempt_id)
            .await
        {
            return Err(Status::aborted(format!(
                "rejecting late shuffle data for query={query_id} stage={stage_id} \
                 partition={partition_id} attempt={attempt_id} (a newer attempt already won)"
            )));
        }

        // 3. Decode incoming RecordBatches. Chain the first message (which may
        //    also contain data) with the rest of the request stream.
        let remaining_stream =
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)));
        let first_stream = futures::stream::once(async move { Ok(first_msg) });
        let combined = first_stream.chain(remaining_stream);
        let flight_batch_stream = FlightRecordBatchStream::new_from_flight_data(combined);

        // Prefer spillable intake when a SpillManager is configured: bounded
        // resident memory under shuffle_memory_budget with soft-watermark
        // spill. Falls back to the legacy mpsc ShuffleReceiver path otherwise.
        if let Some(spill_manager) = self.spill_manager.clone() {
            return self
                .do_exchange_spill(
                    query_id.as_str(),
                    stage_id.as_str(),
                    partition_id,
                    attempt_id,
                    spill_manager,
                    flight_batch_stream,
                )
                .await;
        }

        let mut flight_batch_stream = flight_batch_stream;

        // ── Legacy mpsc path (no spill substrate) ──────────────────────────
        let shuffle_receiver = self
            .shuffle_manager
            .get(&query_id, &stage_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "No shuffle receiver registered for query={query_id}, stage={stage_id} \
                     (and spill is disabled)"
                ))
            })?;

        if shuffle_receiver.sender(partition_id).is_none() {
            return Err(Status::not_found(format!(
                "No sender for partition {partition_id} in query={query_id}, stage={stage_id}"
            )));
        }

        let schema = shuffle_receiver.schema().clone();
        let query_id_clone = query_id.clone();
        let stage_id_clone = stage_id.clone();
        let intake_receiver = shuffle_receiver.clone();
        let attempt_gate = self.shuffle_manager.attempts().clone();
        tokio::spawn(async move {
            let mut batch_count = 0u64;
            let mut rejected = 0u64;
            while let Some(batch_result) = flight_batch_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        if !attempt_gate
                            .admit(&query_id_clone, &stage_id_clone, partition_id, attempt_id)
                            .await
                        {
                            rejected += 1;
                            break;
                        }
                        batch_count += 1;
                        if intake_receiver
                            .send_batch(partition_id, batch)
                            .await
                            .is_err()
                        {
                            warn!(
                                query_id = %query_id_clone,
                                stage_id = %stage_id_clone,
                                partition_id = partition_id,
                                "Shuffle receiver channel closed, stopping intake"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(
                            query_id = %query_id_clone,
                            stage_id = %stage_id_clone,
                            partition_id = partition_id,
                            error = %e,
                            "Error decoding flight data in DoExchange"
                        );
                        break;
                    }
                }
            }
            debug!(
                query_id = %query_id_clone,
                stage_id = %stage_id_clone,
                partition_id = partition_id,
                attempt_id = attempt_id,
                batch_count = batch_count,
                rejected_late = rejected,
                "DoExchange intake complete (mpsc path)"
            );
        });

        let rx = shuffle_receiver
            .take_receiver(partition_id)
            .await
            .ok_or_else(|| {
                Status::already_exists(format!(
                    "Receiver for partition {partition_id} already taken \
                     (query={query_id}, stage={stage_id})"
                ))
            })?;

        let output_stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        });

        let shuffle_opts = ipc_options_for(self.shuffle_compression)?;
        self.metrics
            .flight_encode_resident_bytes
            .set(shuffle_receiver.resident_bytes() as f64);
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(shuffle_opts)
            .build(output_stream.map(Ok))
            .map_err(Status::from);

        Ok(Response::new(
            Box::pin(flight_stream) as Self::DoExchangeStream
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "health_check".to_string(),
                description: "Check worker health".to_string(),
            },
            ActionType {
                r#type: "refresh_credentials".to_string(),
                description: "Accept refreshed S3 credentials from coordinator".to_string(),
            },
            ActionType {
                r#type: "compact_file_group".to_string(),
                description: "Rewrite one Iceberg file group for distributed compaction"
                    .to_string(),
            },
        ];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use datafusion::prelude::SessionContext;
    use tonic::Request;

    // ── #407: encoded Flight frames are charged, not just observed ──────────

    /// One frame carrying `body` bytes. Only the two length fields matter to the
    /// accounting, so a body of zeros is a faithful stand-in for an encoded
    /// RecordBatch here.
    fn frame(body: usize) -> FlightData {
        FlightData::new().with_data_body(vec![0u8; body])
    }

    /// Budget with a 1 KiB accounting unit so the arithmetic in these tests is
    /// exact (the 64 KiB production granularity would round every frame to one
    /// unit and hide over- and under-charging alike).
    fn frame_budget(capacity: usize) -> ByteBudget {
        ByteBudget::with_granularity("flight-test", capacity, 1024, None)
    }

    fn test_metrics() -> Arc<WorkerMetricsRegistry> {
        Arc::new(WorkerMetricsRegistry::new().unwrap())
    }

    /// A worker built without `[worker.memory] flight_budget` (tests, embedded
    /// use) must still charge frames. `do_get` wraps unconditionally, so the
    /// fallback has to be finite: an infinite one would silently restore the
    /// ungated behavior #407 is about, and `ItemTooLarge` would stop working.
    #[test]
    fn default_flight_budget_is_finite_and_pool_derived() {
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let unbounded = default_flight_budget(&SessionContext::new());
        assert!(
            unbounded.capacity_bytes() > 0,
            "unbounded pool must still yield a finite flight budget"
        );

        let pool = Arc::new(FairSpillPool::new(400 * 1024 * 1024));
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("runtime env");
        let ctx = SessionContext::new_with_config_rt(Default::default(), env);
        let sized = default_flight_budget(&ctx);
        assert_eq!(
            sized.capacity_bytes(),
            40 * 1024 * 1024,
            "fallback must match the config default of a tenth of the pool"
        );
    }

    /// The charge must follow the frame the transport currently holds: one
    /// frame's worth outstanding while streaming, zero once drained. A stream
    /// that charged on encode without releasing would climb to 3 frames.
    #[tokio::test]
    async fn frame_charge_tracks_one_frame_at_a_time() {
        let budget = frame_budget(64 * 1024);
        let frames = stream::iter(vec![Ok(frame(1024)), Ok(frame(1024)), Ok(frame(1024))]);
        let mut s = Box::pin(accounted_frame_stream(
            frames,
            budget.clone(),
            test_metrics(),
        ));

        assert!(s.next().await.is_some(), "first frame");
        let after_first = budget.used_bytes();
        assert!(
            after_first >= 1024,
            "frame 1 must be charged, used={after_first}"
        );

        assert!(s.next().await.is_some(), "second frame");
        assert_eq!(
            budget.used_bytes(),
            after_first,
            "frame 1's charge must be released when the transport polls for \
             frame 2; charge is per-frame-in-flight, not cumulative"
        );

        assert!(s.next().await.is_some(), "third frame");
        assert!(s.next().await.is_none(), "end of stream");
        assert_eq!(
            budget.used_bytes(),
            0,
            "draining the stream must release the last frame's charge"
        );
    }

    /// The point of the budget: encoded frames from CONCURRENT streams cannot
    /// all be resident at once. One stream never blocks itself, so this is the
    /// only shape that proves backpressure exists.
    #[tokio::test]
    async fn full_budget_blocks_a_second_stream_until_the_first_releases() {
        // Room for exactly one 4 KiB frame.
        let budget = frame_budget(4 * 1024);
        let metrics = test_metrics();

        let mut first = Box::pin(accounted_frame_stream(
            stream::iter(vec![Ok(frame(4096)), Ok(frame(4096))]),
            budget.clone(),
            metrics.clone(),
        ));
        assert!(
            first.next().await.is_some(),
            "first stream takes the budget"
        );
        assert_eq!(budget.used_bytes(), 4096);

        let mut second = Box::pin(accounted_frame_stream(
            stream::iter(vec![Ok(frame(4096))]),
            budget.clone(),
            metrics,
        ));
        let blocked = tokio::time::timeout(std::time::Duration::from_millis(150), second.next());
        assert!(
            blocked.await.is_err(),
            "second stream must wait: the budget is fully held by the first"
        );

        // Releasing the first stream's frame admits the second.
        drop(first);
        let admitted = tokio::time::timeout(std::time::Duration::from_secs(5), second.next());
        assert!(
            admitted.await.expect("must not time out").is_some(),
            "second stream proceeds once the budget frees"
        );
    }

    /// A frame wider than the whole budget must still ship. `acquire` reports
    /// ItemTooLarge rather than blocking, and failing a query that works today
    /// (unaccounted) would be a regression, so the charge is skipped.
    #[tokio::test]
    async fn oversized_frame_ships_uncharged_instead_of_failing() {
        let budget = frame_budget(4 * 1024);
        let mut s = Box::pin(accounted_frame_stream(
            stream::iter(vec![Ok(frame(64 * 1024))]),
            budget.clone(),
            test_metrics(),
        ));
        let item = tokio::time::timeout(std::time::Duration::from_secs(5), s.next())
            .await
            .expect("must not hang on an oversized frame");
        assert!(
            matches!(item, Some(Ok(_))),
            "oversized frame must be yielded, not turned into an error"
        );
        assert_eq!(
            budget.used_bytes(),
            0,
            "an unchargeable frame must not leak a charge"
        );
    }

    fn make_service(secret: &str) -> WorkerFlightService {
        let metrics = Arc::new(WorkerMetricsRegistry::new().unwrap());
        WorkerFlightService::new(metrics, SessionContext::new())
            .with_worker_secret(secret.to_string())
            .with_accept_hmac_tickets(true)
    }

    /// Recompute the HMAC-SHA256 tag (hex) the coordinator would attach, used
    /// by the #206 signature tests to forge a valid header.
    fn sign(secret: &str, bytes: &[u8]) -> String {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(bytes);
        hex_encode(&mac.finalize().into_bytes())
    }

    /// Build a real ScanTask ticket so the signature test exercises the actual
    /// wire bytes (and so decoding succeeds past the signature gate).
    fn make_scan_task_bytes() -> Vec<u8> {
        sqe_planner::ScanTask {
            hive: None,
            version: 1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
            fragment_id: "frag-sig".to_string(),
            data_file_paths: vec!["s3://bucket/f.parquet".to_string()],
            file_sizes_bytes: vec![1024],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            credentials: sqe_planner::ScanCredentials::default(),
            audience: String::new(),
            expires_at: 0,
            s3_path_style: false,
            s3_allow_http: false,
            predicate_proto: None,
            limit: None,
        }
        .to_bytes()
        .unwrap()
    }

    fn make_refresh_creds() -> RefreshableCredentials {
        RefreshableCredentials {
            fragment_id: "frag-test".to_string(),
            access_key_id: "AKID".to_string(),
            secret_access_key: "SECRET".to_string(),
            session_token: "TOKEN".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        }
    }

    fn unwrap_err<T>(r: Result<T, Status>) -> Status {
        match r {
            Ok(_) => panic!("expected Status error, got Ok"),
            Err(s) => s,
        }
    }

    fn unwrap_ok<T>(r: Result<T, Status>) -> T {
        match r {
            Ok(v) => v,
            Err(s) => panic!("expected Ok, got Status: {s}"),
        }
    }

    /// Drain a `do_action` response stream until either a stream item error
    /// (returns it) or the stream ends without ever erroring (panics).
    ///
    /// `compact_file_group` streams `Progress` frames concurrently with the
    /// rewrite (see the `do_action` handler), so a downstream failure (no
    /// real S3/StaticTable in these tests) can no longer surface as the
    /// `do_action` call's own `Result::Err`: it only appears once the
    /// spawned rewrite task resolves and the `Done` frame's construction
    /// fails. This mirrors `unwrap_err` for tests that need to authenticate
    /// through the gates and then observe that downstream failure.
    async fn drain_to_first_error(
        response: Response<<WorkerFlightService as FlightService>::DoActionStream>,
    ) -> Status {
        let mut stream = response.into_inner();
        loop {
            match stream.next().await {
                Some(Ok(_)) => continue,
                Some(Err(e)) => return e,
                None => panic!("do_action stream ended without ever producing an error"),
            }
        }
    }

    #[tokio::test]
    async fn do_get_rejects_missing_secret_header() {
        let svc = make_service("expected-secret");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let request = Request::new(ticket);
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_rejects_wrong_secret() {
        let svc = make_service("expected-secret");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let mut request = Request::new(ticket);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "wrong".parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    /// Sign `ticket` with the HMAC secret and run it through `do_get`.
    async fn do_get_hmac_signed(svc: &WorkerFlightService, ticket: Vec<u8>) -> Status {
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(ticket.clone()),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request.metadata_mut().insert(
            SCAN_SIGNATURE_HEADER,
            sign("expected-secret", &ticket).parse().unwrap(),
        );
        unwrap_err(svc.do_get(request).await)
    }

    fn assert_task_invalid(err: &Status, needle: &str) {
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
        assert_eq!(
            err.metadata()
                .get(sqe_planner::FAULT_METADATA_KEY)
                .and_then(|v| v.to_str().ok()),
            Some(WorkerFault::TaskInvalid.metric_label()),
            "{err}"
        );
        assert!(err.message().contains(needle), "{err}");
    }

    /// Finding 109: a byte-range morsel that still declares `version: 1` would
    /// have made a pre-morsel worker scan the whole file once per morsel and
    /// duplicate every row. The worker refuses it as INVALID_ARGUMENT before
    /// the auth-independent parts of setup run.
    #[tokio::test]
    async fn do_get_rejects_v1_ticket_carrying_byte_range_fields() {
        let svc = make_service("expected-secret");
        let mut task = sqe_planner::ScanTask::from_bytes(&make_scan_task_bytes()).unwrap();
        task.version = sqe_planner::SCAN_TASK_VERSION_V1;
        task.start_byte = Some(0);
        task.end_byte = Some(512);
        // `to_bytes` would refuse this on the producer; serialise directly to
        // model a coordinator that bypassed the builder.
        let bytes = serde_json::to_vec(&task).unwrap();
        let err = do_get_hmac_signed(&svc, bytes).await;
        assert_task_invalid(&err, "start_byte");
    }

    /// Finding 109: a top-level key this build does not know means a newer
    /// coordinator filled a field that would be silently dropped. Refuse.
    #[tokio::test]
    async fn do_get_rejects_ticket_with_unknown_top_level_keys() {
        let svc = make_service("expected-secret");
        let mut value: serde_json::Value = serde_json::from_slice(&make_scan_task_bytes()).unwrap();
        value["delete_files"] = serde_json::json!(["s3://bucket/d.parquet"]);
        let bytes = serde_json::to_vec(&value).unwrap();
        let err = do_get_hmac_signed(&svc, bytes).await;
        assert_task_invalid(&err, "delete_files");
    }

    /// And a version above what this worker implements is refused the same way.
    #[tokio::test]
    async fn do_get_rejects_ticket_from_the_future() {
        let svc = make_service("expected-secret");
        let mut task = sqe_planner::ScanTask::from_bytes(&make_scan_task_bytes()).unwrap();
        task.version = sqe_planner::SCAN_TASK_VERSION_CURRENT + 1;
        let bytes = serde_json::to_vec(&task).unwrap();
        let err = do_get_hmac_signed(&svc, bytes).await;
        assert_task_invalid(&err, "unsupported ScanTask version");
    }

    #[tokio::test]
    async fn do_get_accepts_correct_secret_and_signature_then_fails_on_bad_ticket() {
        // With the right secret AND a valid signature over the (junk) body, both
        // auth gates pass; ticket decoding then fails. The error must NOT be
        // Unauthenticated, proving both gates let the call through.
        let svc = make_service("expected-secret");
        let body = b"junk";
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from_static(body),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request.metadata_mut().insert(
            SCAN_SIGNATURE_HEADER,
            sign("expected-secret", body).parse().unwrap(),
        );
        let err = unwrap_err(svc.do_get(request).await);
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    // ------------------------------------------------------------------
    // Finding 601: Ed25519 ticket signatures, audience and expiry binding.
    // ------------------------------------------------------------------

    const W1: &str = "http://w1.cluster:50052";

    fn ed25519_service(
        signer: Option<&sqe_core::ticket_signing::TicketSigner>,
    ) -> WorkerFlightService {
        let pin = Arc::new(crate::ticket_pin::TicketKeyPin::floating());
        if let Some(s) = signer {
            pin.pin_b64(s.public_key_b64()).unwrap();
        }
        let metrics = Arc::new(WorkerMetricsRegistry::new().unwrap());
        WorkerFlightService::new(metrics, SessionContext::new())
            .with_worker_secret("expected-secret".to_string())
            .with_ticket_pin(pin)
            .with_advertise_url(W1.to_string())
    }

    fn bound_ticket(audience: &str, expires_at: i64) -> Vec<u8> {
        let mut task = sqe_planner::ScanTask::from_bytes(&make_scan_task_bytes()).unwrap();
        task.audience = audience.to_string();
        task.expires_at = expires_at;
        task.to_bytes().unwrap()
    }

    async fn do_get_signed(
        svc: &WorkerFlightService,
        signer: &sqe_core::ticket_signing::TicketSigner,
        ticket: Vec<u8>,
    ) -> Status {
        let signature = signer.sign(&ticket);
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(ticket),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(SCAN_SIGNATURE_HEADER, signature.parse().unwrap());
        unwrap_err(svc.do_get(request).await)
    }

    fn future() -> i64 {
        Utc::now().timestamp() + 300
    }

    /// A ticket signed by the pinned coordinator key, addressed to this worker
    /// and not yet expired passes every trust gate; it then fails downstream
    /// (no real S3), which must NOT be Unauthenticated.
    #[tokio::test]
    async fn do_get_accepts_ed25519_ticket_bound_to_this_worker() {
        let signer = sqe_core::ticket_signing::TicketSigner::generate().unwrap();
        let svc = ed25519_service(Some(&signer));
        let err = do_get_signed(&svc, &signer, bound_ticket(W1, future())).await;
        assert_ne!(err.code(), tonic::Code::Unauthenticated, "{err}");
    }

    /// 601 acceptance: a ticket signed for audience w1 presented to w2 is
    /// UNAUTHENTICATED.
    #[tokio::test]
    async fn do_get_rejects_ed25519_ticket_for_another_worker() {
        let signer = sqe_core::ticket_signing::TicketSigner::generate().unwrap();
        let svc = ed25519_service(Some(&signer));
        let err = do_get_signed(
            &svc,
            &signer,
            bound_ticket("http://w2.cluster:50052", future()),
        )
        .await;
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("audience"), "{err}");
    }

    #[tokio::test]
    async fn do_get_rejects_expired_or_unbound_ed25519_ticket() {
        let signer = sqe_core::ticket_signing::TicketSigner::generate().unwrap();
        let svc = ed25519_service(Some(&signer));
        let expired =
            do_get_signed(&svc, &signer, bound_ticket(W1, Utc::now().timestamp() - 1)).await;
        assert_eq!(expired.code(), tonic::Code::Unauthenticated);
        assert!(expired.message().contains("expired"), "{expired}");
        let unbound = do_get_signed(&svc, &signer, bound_ticket("", 0)).await;
        assert_eq!(unbound.code(), tonic::Code::Unauthenticated);
        assert!(
            unbound.message().contains("audience or expiry"),
            "{unbound}"
        );
    }

    /// 601: only the coordinator's key verifies. Another key pair (a worker
    /// that generated its own) and a worker with nothing pinned yet both fail.
    #[tokio::test]
    async fn do_get_rejects_ed25519_ticket_from_other_key_or_before_pinning() {
        let coordinator = sqe_core::ticket_signing::TicketSigner::generate().unwrap();
        let impostor = sqe_core::ticket_signing::TicketSigner::generate().unwrap();
        let svc = ed25519_service(Some(&coordinator));
        let err = do_get_signed(&svc, &impostor, bound_ticket(W1, future())).await;
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        let unpinned = ed25519_service(None);
        let err = do_get_signed(&unpinned, &coordinator, bound_ticket(W1, future())).await;
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("not pinned"), "{err}");
    }

    /// 601: the legacy HMAC tag is refused unless the operator opts in.
    #[tokio::test]
    async fn do_get_rejects_hmac_tag_without_opt_in() {
        let svc = ed25519_service(None);
        let body = make_scan_task_bytes();
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(body.clone()),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request.metadata_mut().insert(
            SCAN_SIGNATURE_HEADER,
            sign("expected-secret", &body).parse().unwrap(),
        );
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("HMAC"), "{err}");
    }

    #[tokio::test]
    async fn do_get_rejects_missing_signature() {
        // Right secret, no signature header: the #206 gate rejects before decode.
        let svc = make_service("expected-secret");
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(make_scan_task_bytes()),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_rejects_tampered_ticket() {
        // A signature valid for the ORIGINAL bytes must fail once the ticket is
        // mutated (e.g. a swapped file path). Sign the original, then tamper.
        let svc = make_service("expected-secret");
        let original = make_scan_task_bytes();
        let signature = sign("expected-secret", &original);
        let mut tampered = original.clone();
        // Flip a byte to simulate a swapped file path / stripped predicate.
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(tampered),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(SCAN_SIGNATURE_HEADER, signature.parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_accepts_correctly_signed_ticket() {
        // A correctly-signed, well-formed ScanTask passes both gates. Execution
        // then fails downstream (no real S3), but NOT with Unauthenticated.
        let svc = make_service("expected-secret");
        let body = make_scan_task_bytes();
        let signature = sign("expected-secret", &body);
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(body),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(SCAN_SIGNATURE_HEADER, signature.parse().unwrap());
        let result = svc.do_get(request).await;
        if let Err(s) = result {
            assert_ne!(s.code(), tonic::Code::Unauthenticated);
        }
    }

    #[tokio::test]
    async fn do_get_empty_secret_accepts_anonymous() {
        // Empty worker_secret means unauthenticated mode (opt-in via
        // config). Auth gate disabled; failure comes from ticket decoding.
        let svc = make_service("");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let request = Request::new(ticket);
        let err = unwrap_err(svc.do_get(request).await);
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    // ── Review finding 600: DoExchange is inert without `worker-shuffle` ─────

    /// Stands in for the gRPC frame decoder. The inert `do_exchange` must
    /// refuse before it reads a frame, so decoding here is a test failure.
    #[cfg(not(feature = "worker-shuffle"))]
    struct NeverDecoder;

    #[cfg(not(feature = "worker-shuffle"))]
    impl tonic::codec::Decoder for NeverDecoder {
        type Item = FlightData;
        type Error = Status;

        fn decode(
            &mut self,
            _src: &mut tonic::codec::DecodeBuf<'_>,
        ) -> Result<Option<FlightData>, Status> {
            panic!("inert do_exchange must not decode the request stream")
        }
    }

    #[cfg(not(feature = "worker-shuffle"))]
    fn empty_exchange_request() -> Request<Streaming<FlightData>> {
        Request::new(Streaming::new_request(
            NeverDecoder,
            tonic::body::Body::empty(),
            None,
            None,
        ))
    }

    /// With the shuffle subsystem compiled out there is no receiver registry,
    /// attempt gate or exchange store in the process, so the endpoint must
    /// refuse outright rather than half-serve.
    #[cfg(not(feature = "worker-shuffle"))]
    #[tokio::test]
    async fn do_exchange_is_unimplemented_without_worker_shuffle() {
        let svc = make_service("");
        let err = unwrap_err(svc.do_exchange(empty_exchange_request()).await);
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert_eq!(
            err.message(),
            "DoExchange shuffle intake is not enabled on this worker"
        );
    }

    /// The secret gate stays in front of the refusal, so an anonymous caller
    /// cannot probe which features a worker was built with.
    #[cfg(not(feature = "worker-shuffle"))]
    #[tokio::test]
    async fn do_exchange_still_rejects_a_bad_secret_first() {
        let svc = make_service("expected-secret");
        let err = unwrap_err(svc.do_exchange(empty_exchange_request()).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        let mut request = empty_exchange_request();
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let err = unwrap_err(svc.do_exchange(request).await);
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn refresh_credentials_rejects_missing_secret() {
        let svc = make_service("expected-secret");
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let request = Request::new(action);
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_credentials_rejects_wrong_secret() {
        let svc = make_service("expected-secret");
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "wrong".parse().unwrap());
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_credentials_accepts_correct_secret() {
        let svc = make_service("expected-secret");
        let _rx = svc.credential_store.subscribe("frag-test").await;
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let response = unwrap_ok(svc.do_action(request).await);
        let mut stream = response.into_inner();
        let first = stream.next().await.expect("body present").expect("ok");
        assert_eq!(first.body.as_ref(), b"accepted");
    }

    #[tokio::test]
    async fn health_check_remains_open_when_secret_configured() {
        // Health probes from the coordinator worker_registry do not carry
        // the secret today; keep them open so liveness still works. The
        // call must not leak any credential state.
        let svc = make_service("expected-secret");
        let action = Action {
            r#type: "health_check".to_string(),
            body: bytes::Bytes::new(),
        };
        let request = Request::new(action);
        let response = unwrap_ok(svc.do_action(request).await);
        let mut stream = response.into_inner();
        let first = stream.next().await.expect("body").expect("ok");
        assert_eq!(first.body.as_ref(), b"ok");
    }

    // ---- compact_file_group (Phase 4c Task 3) --------------------------
    //
    // These exercise only the worker-secret + HMAC gate and request decode;
    // there is no real S3/StaticTable in this environment, so a
    // successfully-authenticated call still fails downstream (asserted via
    // `assert_ne!(.., Unauthenticated)`, mirroring `do_get`'s own signed-but-
    // no-real-backend tests above).

    fn make_compact_request_bytes() -> Vec<u8> {
        sqe_compaction::wire::CompactGroupRequest {
            job_id: "job-sig".to_string(),
            group_id: 1,
            table_ident: "catalog.ns.tbl".to_string(),
            metadata_location: "s3://bucket/warehouse/tbl/metadata/v1.metadata.json".to_string(),
            snapshot_id: 42,
            group_file_paths: vec!["s3://bucket/data/f1.parquet".to_string()],
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            sort: None,
            s3: sqe_compaction::wire::S3Conn {
                endpoint: String::new(),
                region: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                session_token: String::new(),
                path_style: false,
                allow_http: false,
            },
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn compact_file_group_rejects_missing_secret_header() {
        let svc = make_service("expected-secret");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(make_compact_request_bytes()),
        };
        let request = Request::new(action);
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_rejects_wrong_secret() {
        let svc = make_service("expected-secret");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(make_compact_request_bytes()),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "wrong".parse().unwrap());
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_rejects_missing_signature() {
        // Right worker secret, no compaction signature header: the HMAC
        // gate must reject before decoding the request.
        let svc = make_service("expected-secret");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(make_compact_request_bytes()),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_rejects_tampered_body() {
        // A signature valid for the ORIGINAL bytes must fail once the body
        // is mutated (e.g. a swapped file path or forged S3 credential).
        let svc = make_service("expected-secret");
        let original = make_compact_request_bytes();
        let signature = sqe_compaction::wire::sign(&original, "expected-secret");
        let mut tampered = original.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(tampered),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(COMPACT_SIGNATURE_HEADER, signature.parse().unwrap());
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_accepts_correct_secret_and_signature_then_fails_downstream() {
        // Both gates pass with a correctly-signed, well-formed request;
        // execution then fails downstream (no real S3/StaticTable in this
        // environment), but NOT with Unauthenticated, proving both gates
        // let the call through. The failure now surfaces as a stream item
        // error (the rewrite runs concurrently with the response stream,
        // see the `do_action` handler), not as `do_action`'s own `Err`, so
        // `do_action` itself must return `Ok` here.
        let svc = make_service("expected-secret");
        let body = make_compact_request_bytes();
        let signature = sqe_compaction::wire::sign(&body, "expected-secret");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(body),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(COMPACT_SIGNATURE_HEADER, signature.parse().unwrap());
        let response = unwrap_ok(svc.do_action(request).await);
        let err = drain_to_first_error(response).await;
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_empty_secret_skips_signature_check() {
        // Empty worker_secret is dev mode (opt-in via
        // worker.allow_unauthenticated): both gates are skipped, and
        // failure comes only from downstream execution (no real S3), not
        // from Unauthenticated. As above, that failure now surfaces as a
        // stream item error rather than `do_action`'s own `Err`.
        let svc = make_service("");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(make_compact_request_bytes()),
        };
        let request = Request::new(action);
        let response = unwrap_ok(svc.do_action(request).await);
        let err = drain_to_first_error(response).await;
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn compact_file_group_rejects_malformed_body_after_auth_passes() {
        // Empty secret => auth gates skipped => decode failure surfaces as
        // InvalidArgument, not Unauthenticated or Internal.
        let svc = make_service("");
        let action = Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from_static(b"not json"),
        };
        let request = Request::new(action);
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ---- finding 608: the rewrite is tied to the RPC -------------------
    //
    // These drive the supervision logic (abort on drop, worker-side
    // deadline, orphan cleanup, bounded progress) through a stubbed rewrite
    // installed via `with_test_compaction_seam`, so they need neither a live
    // S3 endpoint nor a real Iceberg table. The stub receives the very same
    // `GroupRewriteCtx` the real rewrite would have: it records "uploads" in
    // `ctx.tracker` against an in-memory `FileIO`, pushes progress through
    // `ctx.progress_tx`, and can observe `ctx.cancel`.

    use crate::compaction::GroupRewriteCtx;
    use iceberg::io::FileIOBuilder;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    // Plain `Duration` is `chrono::Duration` in this module (imported for the
    // credential-refresh tests above), hence the alias.
    use std::time::Duration as StdDuration;

    /// In-memory object store. Every clone shares one map, so the stub's
    /// uploads and the handler's cleanup see the same objects.
    fn memory_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_object(io: &FileIO, path: &str) {
        io.new_output(path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"parquet"))
            .await
            .unwrap();
    }

    fn install_seam<F>(svc: WorkerFlightService, io: FileIO, run: F) -> WorkerFlightService
    where
        F: Fn(
                GroupRewriteCtx,
            )
                -> futures::future::BoxFuture<'static, sqe_core::Result<CompactGroupResponse>>
            + Send
            + Sync
            + 'static,
    {
        svc.with_test_compaction_seam(Arc::new(CompactionSeam {
            run: Box::new(run),
            file_io: io,
        }))
    }

    /// A well-formed, unsigned `compact_file_group` action (paired with
    /// `make_service("")`, which skips both auth gates).
    fn compact_action() -> Request<Action> {
        Request::new(Action {
            r#type: "compact_file_group".to_string(),
            body: bytes::Bytes::from(make_compact_request_bytes()),
        })
    }

    fn stub_response(uploaded_paths: Vec<String>) -> CompactGroupResponse {
        CompactGroupResponse {
            group_id: 1,
            new_data_files_avro: Vec::new(),
            rows_written: 7,
            bytes_written: 70,
            uploaded_paths,
        }
    }

    /// Signals on drop, so a test can observe that an aborted task's future
    /// was actually torn down rather than left running.
    struct NotifyOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            // `notify_one` stores a permit when nobody is waiting yet, so a
            // test that calls `notified()` after the drop still wakes.
            self.0.notify_one();
        }
    }

    fn error_details(status: &Status) -> serde_json::Value {
        serde_json::from_slice(status.details()).expect("failure details must be JSON")
    }

    #[tokio::test]
    async fn dropping_the_do_action_response_stops_the_rewrite() {
        // The regression: `tokio::spawn` + a stream that merely joins the
        // handle meant a coordinator which gave up on a group (heartbeat
        // timeout, cancelled job, restart) left the worker rewriting it
        // anyway -- competing with the retry it had already placed elsewhere.
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(tokio::sync::Notify::new());
        let ran_to_completion = Arc::new(AtomicBool::new(false));
        let token_slot: Arc<std::sync::Mutex<Option<CancellationToken>>> =
            Arc::new(std::sync::Mutex::new(None));

        let svc = install_seam(make_service(""), memory_io(), {
            let started = started.clone();
            let stopped = stopped.clone();
            let ran_to_completion = ran_to_completion.clone();
            let token_slot = token_slot.clone();
            move |ctx: GroupRewriteCtx| {
                let started = started.clone();
                let stopped = stopped.clone();
                let ran_to_completion = ran_to_completion.clone();
                *token_slot.lock().unwrap() = Some(ctx.cancel.clone());
                Box::pin(async move {
                    let _stop_signal = NotifyOnDrop(stopped);
                    started.notify_one();
                    // Far longer than any test would wait: only an abort can
                    // end this future early.
                    tokio::time::sleep(StdDuration::from_secs(30)).await;
                    ran_to_completion.store(true, Ordering::SeqCst);
                    Ok(stub_response(vec![]))
                })
            }
        });

        let response = unwrap_ok(svc.do_action(compact_action()).await);
        tokio::time::timeout(StdDuration::from_secs(5), started.notified())
            .await
            .expect("the stubbed rewrite must start");

        drop(response);

        tokio::time::timeout(StdDuration::from_millis(100), stopped.notified())
            .await
            .expect("dropping the response must stop the rewrite within 100ms");
        assert!(
            !ran_to_completion.load(Ordering::SeqCst),
            "the rewrite must not have finished its work"
        );
        assert!(
            token_slot
                .lock()
                .unwrap()
                .as_ref()
                .expect("the seam must have seen a token")
                .is_cancelled(),
            "the cooperative cancellation token must be signalled too, so a \
             rewrite mid-batch stops between batches rather than only when \
             the abort lands"
        );
    }

    #[tokio::test]
    async fn a_failed_rewrite_deletes_exactly_the_objects_it_uploaded() {
        let io = memory_io();
        // A file the rewrite never wrote: cleanup must not touch it.
        write_object(&io, "/warehouse/ns/tbl/data/committed.parquet").await;

        let svc = install_seam(make_service(""), io.clone(), |ctx: GroupRewriteCtx| {
            Box::pin(async move {
                for i in 0..2 {
                    let path = format!("/warehouse/ns/tbl/data/orphan-{i}.parquet");
                    ctx.file_io
                        .new_output(&path)
                        .unwrap()
                        .write(bytes::Bytes::from_static(b"parquet"))
                        .await
                        .unwrap();
                    ctx.tracker.lock().unwrap().push(path);
                }
                Err(sqe_core::SqeError::Execution(
                    "rewrite blew up after two uploads".to_string(),
                ))
            })
        });

        let response = unwrap_ok(svc.do_action(compact_action()).await);
        let err = drain_to_first_error(response).await;

        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message()
                .contains("uploaded 2 orphan file(s), deleted 2"),
            "the failure must report what it cleaned up: {}",
            err.message()
        );

        for i in 0..2 {
            let path = format!("/warehouse/ns/tbl/data/orphan-{i}.parquet");
            assert!(
                !io.exists(&path).await.unwrap(),
                "{path} must have been deleted before the error returned"
            );
        }
        assert!(
            io.exists("/warehouse/ns/tbl/data/committed.parquet")
                .await
                .unwrap(),
            "cleanup must delete only the paths the rewrite recorded"
        );

        // The coordinator retries deletion from the details payload, so the
        // full list has to be machine-readable, not just in the message.
        let details = error_details(&err);
        assert_eq!(details["kind"], "compact_file_group_cleanup");
        assert_eq!(details["group_id"], 1);
        assert_eq!(details["uploaded_paths"].as_array().unwrap().len(), 2);
        assert_eq!(details["deleted_paths"].as_array().unwrap().len(), 2);
        assert!(details["leaked_paths"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn compaction_timeout_maps_zero_to_disabled() {
        // 0 is the documented "no deadline" value; anything else is seconds.
        assert_eq!(
            make_service("")
                .with_compaction_timeout(0)
                .compaction_timeout,
            None
        );
        assert_eq!(
            make_service("")
                .with_compaction_timeout(5)
                .compaction_timeout,
            Some(StdDuration::from_secs(5))
        );
        assert_eq!(
            make_service("").compaction_timeout,
            Some(StdDuration::from_secs(DEFAULT_COMPACTION_TIMEOUT_SECS)),
            "a service nobody configured must still carry a deadline"
        );
    }

    #[tokio::test]
    async fn a_rewrite_past_the_compaction_deadline_fails_and_cleans_up() {
        let io = memory_io();
        let svc = install_seam(
            // 50ms rather than the 1s config granularity: the mapping from
            // `worker.compaction_timeout_secs` is asserted just above.
            make_service("").with_test_compaction_deadline(StdDuration::from_millis(50)),
            io.clone(),
            |ctx: GroupRewriteCtx| {
                Box::pin(async move {
                    let path = "/warehouse/ns/tbl/data/slow-0.parquet".to_string();
                    ctx.file_io
                        .new_output(&path)
                        .unwrap()
                        .write(bytes::Bytes::from_static(b"parquet"))
                        .await
                        .unwrap();
                    ctx.tracker.lock().unwrap().push(path);
                    // Wedged: only the deadline can end this.
                    tokio::time::sleep(StdDuration::from_secs(3600)).await;
                    Ok(stub_response(vec![]))
                })
            },
        );

        let response = unwrap_ok(svc.do_action(compact_action()).await);
        let err = drain_to_first_error(response).await;

        assert_eq!(
            err.code(),
            tonic::Code::DeadlineExceeded,
            "a worker-side deadline must be distinguishable from a broken \
             group: {err}"
        );
        assert!(
            err.message().contains("compaction deadline of 50ms"),
            "the error must name the deadline it hit: {}",
            err.message()
        );
        assert!(
            !io.exists("/warehouse/ns/tbl/data/slow-0.parquet")
                .await
                .unwrap(),
            "a timed-out rewrite must not leave its partial output behind"
        );
        assert_eq!(
            error_details(&err)["uploaded_paths"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn progress_frames_are_dropped_not_queued_when_the_reader_stalls() {
        // Progress is advisory. A coordinator that stops reading used to
        // grow an unbounded queue for the life of the rewrite.
        const ATTEMPTS: usize = 1000;
        let sends_done = Arc::new(tokio::sync::Notify::new());
        let accepted = Arc::new(AtomicUsize::new(0));

        let svc = install_seam(make_service(""), memory_io(), {
            let sends_done = sends_done.clone();
            let accepted = accepted.clone();
            move |ctx: GroupRewriteCtx| {
                let sends_done = sends_done.clone();
                let accepted = accepted.clone();
                Box::pin(async move {
                    let tx = ctx.progress_tx.as_ref().expect("handler wires progress");
                    for rows in 0..ATTEMPTS as u64 {
                        if tx.try_send(rows).is_ok() {
                            accepted.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    sends_done.notify_one();
                    Err(sqe_core::SqeError::Execution("done sending".to_string()))
                })
            }
        });

        let response = unwrap_ok(svc.do_action(compact_action()).await);
        // Deliberately do NOT poll the response stream here: the reader is
        // stalled, which is the whole point.
        tokio::time::timeout(StdDuration::from_secs(5), sends_done.notified())
            .await
            .expect("the stub must finish its sends without blocking");

        let accepted = accepted.load(Ordering::SeqCst);
        assert!(
            accepted <= PROGRESS_CHANNEL_CAPACITY,
            "a stalled reader must cap the queue at {PROGRESS_CHANNEL_CAPACITY} \
             frames, accepted {accepted} of {ATTEMPTS}"
        );
        assert!(accepted > 0, "some progress must still get through");

        // What was accepted is what the coordinator eventually reads: the
        // dropped frames are gone, not deferred.
        let mut stream = response.into_inner();
        let mut frames = 0usize;
        loop {
            match stream.next().await {
                Some(Ok(_)) => frames += 1,
                Some(Err(_)) => break,
                None => panic!("stream ended without the terminal error"),
            }
        }
        assert_eq!(frames, accepted);
    }

    #[tokio::test]
    async fn a_successful_rewrite_keeps_its_output_and_returns_a_done_frame() {
        // The other half of the cleanup contract: the guard must not delete
        // the files of a rewrite that actually succeeded.
        let io = memory_io();
        let svc = install_seam(make_service(""), io.clone(), |ctx: GroupRewriteCtx| {
            Box::pin(async move {
                let path = "/warehouse/ns/tbl/data/good-0.parquet".to_string();
                ctx.file_io
                    .new_output(&path)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"parquet"))
                    .await
                    .unwrap();
                ctx.tracker.lock().unwrap().push(path.clone());
                Ok(stub_response(vec![path]))
            })
        });

        let response = unwrap_ok(svc.do_action(compact_action()).await);
        let mut stream = response.into_inner();
        let frame = stream
            .next()
            .await
            .expect("a Done frame")
            .expect("a successful rewrite must not error");
        match CompactGroupFrame::from_bytes(&frame.body).unwrap() {
            CompactGroupFrame::Done(done) => {
                assert_eq!(done.group_id, 1);
                assert_eq!(done.rows_written, 7);
            }
            other => panic!("expected a Done frame, got {other:?}"),
        }
        assert!(stream.next().await.is_none(), "Done is terminal");
        assert!(
            io.exists("/warehouse/ns/tbl/data/good-0.parquet")
                .await
                .unwrap(),
            "a successful rewrite's output must survive"
        );
    }
}

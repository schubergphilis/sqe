use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
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
use sqe_catalog::FooterCache;
use sqe_compaction::wire::{CompactGroupFrame, CompactGroupRequest};
use sqe_core::FlightCompression;
use sqe_metrics::propagation::extract_trace_context;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_planner::ScanTask;
use sqe_spill::{
    split_default_read_headroom, AdmissionRequest, AttemptManifest, ByteBudget, BytePermit,
    ExchangeAttemptStore, LiveConsumerRegistry, MemoryGovernor, ReclaimableConsumer, SpillManager,
    SpillScope, TaskKey, WorkloadClass,
};

use crate::compaction::compact_file_group;
use crate::credential_channel::{CredentialStore, RefreshableCredentials};
use crate::executor;
use crate::shuffle::{ExchangeDescriptor, ShuffleManager};
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
            Poll::Pending => Poll::Pending,
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
    shuffle_manager: ShuffleManager,
    /// Maximum duration for a single scan task. 0 means no timeout.
    scan_timeout: std::time::Duration,
    /// IPC compression for DoGet responses (worker -> coordinator).
    /// Default: ZSTD (internal traffic benefits from better ratio).
    flight_compression: FlightCompression,
    /// IPC compression for DoExchange shuffle responses.
    /// Default: ZSTD.
    shuffle_compression: FlightCompression,
    /// Shared secret used to authenticate inbound Flight calls. When empty
    /// the worker accepts unauthenticated traffic (operators must opt in
    /// via `worker.allow_unauthenticated = true`, enforced at config load).
    worker_secret: String,
    /// Shared spill manager for shuffle / operator spill (Phase 3+).
    spill_manager: Option<Arc<SpillManager>>,
    /// Byte budget for shuffle partition buffers (from
    /// `worker.memory.shuffle_memory_budget`). Atomic for hot config reload.
    shuffle_memory_budget: Arc<std::sync::atomic::AtomicUsize>,
    /// Scan timeout seconds (0 = none). Atomic for hot config reload.
    scan_timeout_secs: Arc<std::sync::atomic::AtomicU64>,
    /// Worker-wide temporary memory governor (Phase 7). Admits shuffle /
    /// operator grants so concurrent stages cannot race the FairSpillPool.
    memory_governor: Option<Arc<MemoryGovernor>>,
    /// Live reclaimable operators (join/agg/sort) registered under the governor.
    live_consumers: Arc<LiveConsumerRegistry>,
    /// Durable exchange attempt manifests (Phase 8).
    exchange_store: Arc<ExchangeAttemptStore>,
    /// Byte budget for encoded Flight frames in flight to the coordinator
    /// (`worker.memory.flight_budget`, issue #407). `None` means "not
    /// configured": `do_get` then falls back to [`default_flight_budget`], so
    /// frames are charged either way.
    flight_budget: Option<ByteBudget>,
}

/// Lightweight reclaimable consumer for one DoExchange partition grant.
struct ShufflePartitionConsumer {
    name: String,
    desired: usize,
    minimum: usize,
}

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
const DEFAULT_SHUFFLE_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

impl WorkerFlightService {
    pub fn new(metrics: Arc<WorkerMetricsRegistry>, session_ctx: SessionContext) -> Self {
        Self {
            metrics,
            credential_store: CredentialStore::new(),
            session_ctx,
            footer_cache: None,
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
            spill_manager: None,
            flight_budget: None,
            shuffle_memory_budget: Arc::new(std::sync::atomic::AtomicUsize::new(
                DEFAULT_SHUFFLE_MEMORY_BUDGET,
            )),
            scan_timeout_secs: Arc::new(std::sync::atomic::AtomicU64::new(600)),
            memory_governor: None,
            live_consumers: Arc::new(LiveConsumerRegistry::new()),
            exchange_store: Arc::new(ExchangeAttemptStore::new()),
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
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
            spill_manager: None,
            flight_budget: None,
            shuffle_memory_budget: Arc::new(std::sync::atomic::AtomicUsize::new(
                DEFAULT_SHUFFLE_MEMORY_BUDGET,
            )),
            scan_timeout_secs: Arc::new(std::sync::atomic::AtomicU64::new(600)),
            memory_governor: None,
            live_consumers: Arc::new(LiveConsumerRegistry::new()),
            exchange_store: Arc::new(ExchangeAttemptStore::new()),
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
    #[must_use = "with_shuffle_memory_budget consumes self; bind the returned service"]
    pub fn with_shuffle_memory_budget(self, bytes: usize) -> Self {
        self.shuffle_memory_budget
            .store(bytes.max(64 * 1024), std::sync::atomic::Ordering::Release);
        self
    }

    /// Share the shuffle-budget atom with hot-reload.
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
    pub fn exchange_store(&self) -> &Arc<ExchangeAttemptStore> {
        &self.exchange_store
    }

    /// Configured shuffle memory budget in bytes.
    pub fn shuffle_memory_budget(&self) -> usize {
        self.shuffle_memory_budget
            .load(std::sync::atomic::Ordering::Acquire)
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

    /// Returns a reference to the credential store for use by executors.
    pub fn credential_store(&self) -> &CredentialStore {
        &self.credential_store
    }

    /// Returns a reference to the shuffle manager.
    pub fn shuffle_manager(&self) -> &ShuffleManager {
        &self.shuffle_manager
    }

    /// Spillable DoExchange path: append into a per-attempt
    /// [`SpillablePartitionBuffer`], finish (force residual spill), then stream
    /// segments back without holding the full exchange volume in RAM.
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

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Holds a governor [`sqe_spill::GrantGuard`] for the lifetime of a Flight
/// response stream so shuffle memory is not released mid-drain.
struct GrantHeldStream<S> {
    inner: Pin<Box<S>>,
    _guard: Option<sqe_spill::GrantGuard>,
}

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

        let scan_task = ScanTask::from_bytes(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("Failed to decode ScanTask: {e}")))?;
        scan_task
            .validate_version()
            .map_err(|e| Status::invalid_argument(format!("ScanTask version rejected: {e}")))?;

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
        let stream_span = worker_span.clone();
        let result_span = worker_span.clone();
        let result = async move {
            info!(
                fragment_id = %scan_task.fragment_id,
                file_count = scan_task.data_file_paths.len(),
                "Worker received scan task"
            );

            // Subscribe to credential updates for this fragment. The guard
            // removes the entry on drop so timeouts, setup errors, and panics
            // can't leak `watch::Sender`s into the store. Issue #76.
            let cred_rx = credential_store.subscribe(&scan_task.fragment_id).await;
            let fragment_id = scan_task.fragment_id.clone();
            let cleanup_guard = credential_store.cleanup_guard(fragment_id.clone());

            // The pushed-down predicate (#233) is carried inside `scan_task`
            // (`predicate_proto`) and decoded by the executor; the late
            // materialization RowFilter is wired from it there.
            let prepare = executor::execute_scan_streaming(
                scan_task,
                Some(metrics.clone()),
                session_ctx.clone(),
                Some(cred_rx),
                footer_cache.clone(),
                None, // Coordinator metrics (workers don't have coordinator registry)
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
                warn!(error = %e, "Scan task setup failed");
                Status::internal(format!("Scan execution failed: {e}"))
            })?;

            // Stream lifetime carries the guard; once the encoder finishes
            // (or the client disconnects mid-stream) the guard drops and the
            // credential entry is removed via `tokio::spawn`.
            // AccountedFlightStream: retain each batch's scan-budget permit
            // until the Flight encoder polls for the next batch (or the
            // client disconnects and the stream drops).
            let mapped_stream = batch_stream
                .map(|item| match item {
                    Ok(accounted) => Ok(accounted),
                    Err(e) => Err(arrow_flight::error::FlightError::from_external_error(
                        Box::new(std::io::Error::other(e.to_string())),
                    )),
                })
                .chain(stream::once(async move {
                    drop(cleanup_guard);
                    Err::<executor::AccountedBatch, arrow_flight::error::FlightError>(
                        arrow_flight::error::FlightError::from_external_error(Box::new(
                            std::io::Error::other("__SQE_CLEANUP_SENTINEL__"),
                        )),
                    )
                }))
                .filter_map(|item| async move {
                    match item {
                        Ok(b) => Some(Ok(b)),
                        Err(e) if e.to_string().contains("__SQE_CLEANUP_SENTINEL__") => None,
                        Err(e) => Some(Err(e)),
                    }
                });

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
                let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
                let session_ctx = self.session_ctx.clone();
                let request_for_task = request.clone();
                let rewrite_task = tokio::spawn(async move {
                    compact_file_group(&session_ctx, &request_for_task, Some(progress_tx)).await
                });

                let progress_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(
                    progress_rx,
                )
                .map(move |rows_read| {
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
                });

                let done_stream = stream::once(async move {
                    let response = match rewrite_task.await {
                        Ok(Ok(resp)) => resp,
                        Ok(Err(e)) => {
                            return Err(Status::internal(format!(
                                "compact_file_group failed for group {group_id}: {e}"
                            )));
                        }
                        Err(join_err) => {
                            return Err(Status::internal(format!(
                                "compact_file_group task for group {group_id} panicked: \
                                 {join_err}"
                            )));
                        }
                    };
                    let frame = CompactGroupFrame::Done(response);
                    let body = frame.to_bytes().map_err(|e| {
                        Status::internal(format!("Failed to encode CompactGroupFrame: {e}"))
                    })?;
                    Ok(arrow_flight::Result {
                        body: bytes::Bytes::from(body),
                    })
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
        assert!(first.next().await.is_some(), "first stream takes the budget");
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
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
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
}

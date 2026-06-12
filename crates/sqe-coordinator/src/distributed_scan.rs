use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    PushedDown,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_proto::bytes::Serializeable;
use futures::{Stream, StreamExt, TryStreamExt};
use tracing::{error, info, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use sqe_metrics::propagation::inject_trace_context;
use sqe_planner::ScanTask;

use crate::channel_pool::ChannelPool;
use crate::credential_refresh::CredentialRefreshTracker;
use crate::worker_registry::WorkerRegistry;

/// Default maximum number of retry attempts per fragment before giving up
/// or falling back to local execution.
const DEFAULT_MAX_RETRIES: u32 = 2;

/// Metadata header carrying the coordinator/worker shared secret. The
/// worker rejects `do_get` calls that don't carry it (issue #22).
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Metadata header carrying the HMAC-SHA256 tag (hex) over the exact ticket
/// bytes, keyed by the shared worker_secret (issue #206). The worker
/// recomputes the tag over the received bytes and constant-time compares
/// before executing, proving the coordinator authored the exact ScanTask
/// (file paths, credentials, predicate, and limit included).
const SCAN_SIGNATURE_HEADER: &str = "x-sqe-scan-signature";

/// Compute the HMAC-SHA256 tag (lowercase hex) over `bytes` keyed by `secret`.
///
/// Returns `None` when `secret` is empty: an empty key means the deployment
/// opted into `worker.allow_unauthenticated`, so there is nothing to sign.
pub(crate) fn sign_ticket(secret: &str, bytes: &[u8]) -> Option<String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    if secret.is_empty() {
        return None;
    }
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(bytes);
    let tag = mac.finalize().into_bytes();
    Some(hex_encode(&tag))
}

/// Lowercase hex encoding (avoids pulling in an extra crate for a 32-byte tag).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// DataFusion `ExecutionPlan` that distributes scan work across workers.
///
/// Each partition maps to one worker. When DataFusion calls `execute(i)`,
/// the DistributedScanExec sends a `ScanTask` to worker[i] via Arrow Flight
/// `do_get` and returns the result stream.
///
/// When a `credential_tracker` is set, the exec registers each dispatched
/// fragment so the coordinator's background refresh loop can push new
/// credentials before they expire.
///
/// If a worker fails, the executor will:
/// 1. Mark the worker as unhealthy in the registry.
/// 2. Re-assign the fragment to another healthy worker (up to `max_retries` times).
/// 3. If no healthy workers remain, fall back to local execution via
///    the coordinator's DataFusion `TaskContext`.
#[derive(Debug)]
pub struct DistributedScanExec {
    scan_tasks: Vec<ScanTask>,
    worker_urls: Vec<String>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Optional credential expiry for the vended credentials included in scan tasks.
    credential_expiry: Option<DateTime<Utc>>,
    /// Optional tracker for coordinating credential refresh pushes.
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    /// Worker registry for health tracking and failover.
    worker_registry: Option<Arc<WorkerRegistry>>,
    /// Maximum number of retry attempts per fragment.
    max_retries: u32,
    /// Optional local execution callback for fallback.
    /// When set, fragments that cannot be executed on any worker
    /// will be executed locally using the coordinator's DataFusion runtime.
    local_executor: Option<Arc<dyn LocalExecutor>>,
    /// Optional callback fired when each fragment completes or fails.
    fragment_callback: FragmentCallbackOpt,
    /// Shared secret attached to every outbound `do_get` so the worker
    /// can authenticate the coordinator. Empty when distributed mode is
    /// running with `allow_unauthenticated_workers = true`.
    worker_secret: String,
    /// gRPC connect timeout for the per-call (`pool = None`) dispatch path.
    /// Issue #29.
    worker_connect_timeout: std::time::Duration,
    /// gRPC request timeout for `do_get`. Issue #29.
    worker_rpc_timeout: std::time::Duration,
    /// Per-operator metrics so EXPLAIN ANALYZE and passive query profiles
    /// show real elapsed/rows for this node instead of blanks.
    metrics: ExecutionPlanMetricsSet,
    /// Dynamic join filters (Path B-2) accepted from parent `HashJoinExec`s
    /// via `handle_child_pushdown_result`. Snapshotted at dispatch time and
    /// ANDed into each `ScanTask`'s `predicate_proto` so WORKERS prune rows
    /// before shipping them over Flight. Without this, a forced-distribution
    /// fact scan ships every row the static predicate allows (SSB SF1
    /// lineorder: 6M rows / ~115MB per query) even when the dim build side
    /// already proved only a few percent can survive the join.
    pushed_down_filters: Vec<Arc<dyn PhysicalExpr>>,
}

/// Trait for local execution fallback.
///
/// Implemented by the coordinator to execute scan tasks locally when all
/// workers are unavailable.
pub trait LocalExecutor: Send + Sync + std::fmt::Debug {
    /// Execute a scan task locally and return a record batch stream.
    fn execute_local(
        &self,
        task: &ScanTask,
        schema: SchemaRef,
    ) -> DFResult<SendableRecordBatchStream>;
}

/// Callback invoked when a fragment stream completes or fails.
/// Args: (fragment_id, success: bool, elapsed_ms, output_rows)
pub type FragmentCallback = Arc<dyn Fn(&str, bool, u64, usize) + Send + Sync>;

/// Newtype wrapper around [`FragmentCallback`] that provides a `Debug` implementation
/// so it can be used inside `#[derive(Debug)]` structs.
#[derive(Clone)]
struct FragmentCallbackOpt(Option<FragmentCallback>);

impl fmt::Debug for FragmentCallbackOpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_some() {
            write!(f, "Some(<FragmentCallback>)")
        } else {
            write!(f, "None")
        }
    }
}

impl DistributedScanExec {
    /// Returns the scan tasks for all partitions.
    pub fn scan_tasks(&self) -> &[ScanTask] {
        &self.scan_tasks
    }

    /// Returns the worker URLs corresponding to each scan task.
    pub fn worker_urls(&self) -> &[String] {
        &self.worker_urls
    }

    pub fn new(
        scan_tasks: Vec<ScanTask>,
        worker_urls: Vec<String>,
        schema: SchemaRef,
    ) -> Self {
        assert_eq!(scan_tasks.len(), worker_urls.len());
        let num_partitions = scan_tasks.len();

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            scan_tasks,
            worker_urls,
            schema,
            properties,
            credential_expiry: None,
            credential_tracker: None,
            worker_registry: None,
            max_retries: DEFAULT_MAX_RETRIES,
            local_executor: None,
            fragment_callback: FragmentCallbackOpt(None),
            worker_secret: String::new(),
            worker_connect_timeout: std::time::Duration::from_secs(5),
            worker_rpc_timeout: std::time::Duration::from_secs(630),
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters: vec![],
        }
    }

    /// Copy of this node with `pushed_down_filters` replaced; used by
    /// `handle_child_pushdown_result` to absorb dynamic join filters.
    fn clone_with_pushed_filters(&self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            scan_tasks: self.scan_tasks.clone(),
            worker_urls: self.worker_urls.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            credential_expiry: self.credential_expiry,
            credential_tracker: self.credential_tracker.clone(),
            worker_registry: self.worker_registry.clone(),
            max_retries: self.max_retries,
            local_executor: self.local_executor.clone(),
            fragment_callback: FragmentCallbackOpt(self.fragment_callback.0.clone()),
            worker_secret: self.worker_secret.clone(),
            worker_connect_timeout: self.worker_connect_timeout,
            worker_rpc_timeout: self.worker_rpc_timeout,
            metrics: self.metrics.clone(),
            pushed_down_filters: filters,
        }
    }

    /// Carry over dynamic join filters from the `IcebergScanExec` this node
    /// replaces. `try_distribute` runs AFTER the physical optimizer, so
    /// DataFusion's filter-pushdown rule has already deposited the
    /// `DynamicFilterPhysicalExpr`s on the Iceberg scan — swapping in the
    /// distributed scan without carrying them over silently discarded them
    /// (every SSB SF1 fact scan shipped all 6M rows). The exprs are shared
    /// `Arc`s still updated by the parent `HashJoinExec` at runtime, so the
    /// dispatch-time snapshot sees the materialized bounds.
    #[must_use = "with_pushed_down_filters consumes self; bind the returned scan"]
    pub fn with_pushed_down_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.pushed_down_filters = filters;
        self
    }

    /// Set the shared secret attached to every outbound worker `do_get`.
    /// When unset the dispatcher sends no auth header; the worker must
    /// also be running with `allow_unauthenticated = true` for the call
    /// to succeed.
    #[must_use = "with_worker_secret consumes self; bind the returned scan"]
    pub fn with_worker_secret(mut self, secret: String) -> Self {
        self.worker_secret = secret;
        self
    }

    /// Set the credential expiry for the vended credentials in these scan tasks.
    ///
    /// When combined with a credential tracker, this enables the coordinator to
    /// detect when credentials are approaching expiry and push refreshed ones
    /// to workers.
    #[must_use = "with_credential_expiry consumes self; bind the returned scan"]
    pub fn with_credential_expiry(mut self, expiry: DateTime<Utc>) -> Self {
        self.credential_expiry = Some(expiry);
        self
    }

    /// Set the credential refresh tracker for monitoring expiring credentials.
    #[must_use = "with_credential_tracker consumes self; bind the returned scan"]
    pub fn with_credential_tracker(mut self, tracker: Arc<CredentialRefreshTracker>) -> Self {
        self.credential_tracker = Some(tracker);
        self
    }

    /// Set the worker registry for health tracking and failover.
    #[must_use = "with_worker_registry consumes self; bind the returned scan"]
    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    /// Set the maximum number of retry attempts per fragment.
    #[must_use = "with_max_retries consumes self; bind the returned scan"]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the local executor for fallback when all workers are down.
    #[must_use = "with_local_executor consumes self; bind the returned scan"]
    pub fn with_local_executor(mut self, executor: Arc<dyn LocalExecutor>) -> Self {
        self.local_executor = Some(executor);
        self
    }

    /// Set an optional callback that fires when each fragment stream completes or fails.
    #[must_use = "with_fragment_callback consumes self; bind the returned scan"]
    pub fn with_fragment_callback(mut self, cb: FragmentCallback) -> Self {
        self.fragment_callback = FragmentCallbackOpt(Some(cb));
        self
    }

    /// Set the gRPC connect and request timeouts used when dispatching scan
    /// fragments to workers. Connect timeout caps TCP+TLS+HTTP/2 handshake;
    /// rpc timeout caps the `do_get` round-trip so a kernel-paused worker
    /// surfaces as `DeadlineExceeded` instead of an unbounded await that
    /// starves the coordinator's query-permit pool. Issue #29.
    pub fn with_timeouts(
        mut self,
        connect_timeout: std::time::Duration,
        rpc_timeout: std::time::Duration,
    ) -> Self {
        self.worker_connect_timeout = connect_timeout;
        self.worker_rpc_timeout = rpc_timeout;
        self
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec: workers={}, total_files={}, max_retries={}, dynamic_filters={}",
            self.worker_urls.len(),
            self.scan_tasks
                .iter()
                .map(|t| t.data_file_paths.len())
                .sum::<usize>(),
            self.max_retries,
            self.pushed_down_filters.len(),
        )
    }
}

impl ExecutionPlan for DistributedScanExec {
    fn name(&self) -> &str {
        "DistributedScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Declare ourselves a pushdown leaf, exactly like `IcebergScanExec`:
    /// the default `all_unsupported` would make the optimizer abandon the
    /// dynamic filters a parent `HashJoinExec` tries to push down, and
    /// `handle_child_pushdown_result` would never run. See the matching
    /// comment in `sqe_catalog::iceberg_scan` for the investigation trail.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        _parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        Ok(FilterDescription::new())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Accept only dynamic join filters (snapshotted at dispatch time);
        // static parent filters stay in the coordinator's FilterExec, which
        // remains authoritative either way — the worker-side predicate is a
        // pure row-shipping optimization.
        let mut dynamic_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut filter_results: Vec<PushedDown> = Vec::new();
        for pf in &child_pushdown_result.parent_filters {
            if pf
                .filter
                .as_any()
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .is_some()
            {
                dynamic_filters.push(Arc::clone(&pf.filter));
                filter_results.push(PushedDown::Yes);
            } else {
                filter_results.push(PushedDown::No);
            }
        }

        if dynamic_filters.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                filter_results,
            ));
        }

        let mut all_pushed = self.pushed_down_filters.clone();
        all_pushed.extend(dynamic_filters);
        let new_scan = self.clone_with_pushed_filters(all_pushed);
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(filter_results)
                .with_updated_node(Arc::new(new_scan)),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.scan_tasks.len() {
            return Err(DataFusionError::Internal(format!(
                "DistributedScanExec partition {partition} out of range (max {})",
                self.scan_tasks.len()
            )));
        }

        let mut task = self.scan_tasks[partition].clone();
        let pushed_down_filters = self.pushed_down_filters.clone();
        let dynamic_filters_pushed =
            MetricBuilder::new(&self.metrics).counter("dynamic_filters_pushed", partition);
        let initial_worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();
        let schema_for_stream = self.schema.clone();
        let credential_expiry = self.credential_expiry;
        let credential_tracker = self.credential_tracker.clone();
        let max_retries = self.max_retries;
        let worker_registry = self.worker_registry.clone();
        let channel_pool = self
            .worker_registry
            .as_ref()
            .map(|r| r.channel_pool());
        let local_executor = self.local_executor.clone();
        let fragment_callback = self.fragment_callback.0.clone();
        let worker_secret = self.worker_secret.clone();
        let worker_connect_timeout = self.worker_connect_timeout;
        let worker_rpc_timeout = self.worker_rpc_timeout;

        let dispatch_span = info_span!(
            "dispatch_to_worker",
            fragment_id = %task.fragment_id,
            worker = %initial_worker_url,
            file_count = task.data_file_paths.len(),
        );
        let _guard = dispatch_span.enter();

        info!(
            parent: &dispatch_span,
            "Dispatching scan to worker"
        );

        // Capture the current OTel context so it can be propagated to the worker
        let parent_cx = dispatch_span.context();

        // Phase 1: resolve which stream to use (retry across workers, then local fallback).
        // Phase 2: stream batches from the resolved source.
        //
        // We split these into two phases because the retry loop is async and
        // must complete before we know which inner stream to poll.
        let resolve_future = async move {
            // Path B-2 for distributed scans: snapshot the dynamic join
            // filters NOW — this future runs on the stream's first poll,
            // which for a hash-join probe side happens only after the build
            // side has completed and materialized its bounds. Convert each
            // snapshot to a logical Expr and AND it into the ticket's
            // predicate_proto so the WORKER prunes rows (via its RowFilter /
            // late-materialization path) before they cross the network.
            // A filter that is still `true` (build not finished) or has a
            // shape the converter does not handle is skipped: the scan then
            // ships exactly what it ships today, and the coordinator's join
            // stays authoritative either way.
            //
            // Bounded readiness wait (Trino-style): with stacked joins only
            // the LOWEST join's build is guaranteed done at first poll — the
            // upper joins' builds (whose filters are usually the selective
            // ones) may still be running, leaving their filters at
            // `lit(true)`. Dispatching immediately then ships the full scan.
            // Wait up to 100ms for the filters to materialize (the most
            // selective one is often the LAST build to finish); dim builds
            // finish in low tens of ms, and a scan whose builds are
            // genuinely slow falls back to dispatching with whatever has
            // materialized by the deadline. Bounded, so no deadlock is
            // possible regardless of plan shape.
            if !pushed_down_filters.is_empty() {
                let dynamic: Vec<&DynamicFilterPhysicalExpr> = pushed_down_filters
                    .iter()
                    .filter_map(|f| f.as_any().downcast_ref::<DynamicFilterPhysicalExpr>())
                    .collect();
                if !dynamic.is_empty() {
                    let deadline =
                        std::time::Instant::now() + std::time::Duration::from_millis(100);
                    loop {
                        let all_ready = dynamic.iter().all(|d| {
                            d.current()
                                .map(|e| !crate::scan_pushdown::is_trivially_true(&e))
                                .unwrap_or(false)
                        });
                        if all_ready || std::time::Instant::now() >= deadline {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                }
            }
            if !pushed_down_filters.is_empty() {
                let mut snapshots: Vec<datafusion::logical_expr::Expr> = Vec::new();
                for f in &pushed_down_filters {
                    let resolved = match f
                        .as_any()
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                    {
                        Some(dynamic) => match dynamic.current() {
                            Ok(expr) => expr,
                            Err(_) => continue,
                        },
                        None => Arc::clone(f),
                    };
                    if crate::scan_pushdown::is_trivially_true(&resolved) {
                        continue;
                    }
                    match crate::scan_pushdown::physical_filter_to_logical_lenient(&resolved) {
                        Some(logical) => snapshots.push(logical),
                        None => tracing::debug!(
                            fragment_id = %task.fragment_id,
                            filter = %resolved,
                            "dynamic filter snapshot not convertible to a logical Expr; skipping"
                        ),
                    }
                }
                if !snapshots.is_empty() {
                    let snapshot_count = snapshots.len();
                    let existing = task
                        .predicate_proto
                        .as_deref()
                        .and_then(|b| datafusion::logical_expr::Expr::from_bytes(b).ok());
                    if let Some(combined) =
                        existing.into_iter().chain(snapshots).reduce(|a, b| a.and(b))
                    {
                        match combined.to_bytes() {
                            Ok(bytes) => {
                                task.predicate_proto = Some(bytes.to_vec());
                                dynamic_filters_pushed.add(snapshot_count);
                                info!(
                                    fragment_id = %task.fragment_id,
                                    dynamic_filters = snapshot_count,
                                    "ANDed dynamic join filter snapshots into worker predicate"
                                );
                            }
                            Err(e) => warn!(
                                fragment_id = %task.fragment_id,
                                error = %e,
                                "failed to serialize dynamic filter snapshot; shipping unfiltered"
                            ),
                        }
                    }
                }
            }

            // Register fragment with the credential tracker if available
            if let Some(ref tracker) = credential_tracker {
                tracker
                    .register(
                        task.fragment_id.clone(),
                        initial_worker_url.clone(),
                        credential_expiry,
                    )
                    .await;
            }

            let start = std::time::Instant::now();
            let mut last_error: Option<DataFusionError> = None;
            let mut current_worker_url = initial_worker_url;
            let mut failed_workers: Vec<String> = Vec::new();

            // Attempt the initial worker + up to max_retries reassignments
            for attempt in 0..=max_retries {
                if attempt > 0 {
                    let delay = std::time::Duration::from_millis(50 * (1 << attempt.min(4)));
                    tokio::time::sleep(delay).await;

                    warn!(
                        fragment_id = %task.fragment_id,
                        attempt = attempt,
                        max_retries = max_retries,
                        worker = %current_worker_url,
                        "Retrying fragment on different worker"
                    );
                }

                match dispatch_to_worker(
                    &task,
                    &current_worker_url,
                    channel_pool.as_deref(),
                    &parent_cx,
                    &worker_secret,
                    worker_connect_timeout,
                    worker_rpc_timeout,
                )
                .await
                {
                    Ok(flight_stream) => {
                        // Project received batches to match the expected schema.
                        // Workers return full table columns, but the plan may expect
                        // fewer columns (e.g., COUNT(*) expects 0 columns).
                        let expected_schema = schema.clone();
                        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                            Box::pin(
                                flight_stream
                                    .map_err(|e| DataFusionError::External(Box::new(e)))
                                    .map(move |batch_result| {
                                        reassemble_worker_batch(batch_result?, &expected_schema)
                                    }),
                            );
                        // Terminate the fragment stream on the first mid-stream
                        // error so downstream operators do not see Ok batches
                        // arriving after an Err from this fragment.
                        let terminated: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                            Box::pin(TerminateOnErrorStream::new(
                                inner,
                                task.fragment_id.clone(),
                            ));
                        // Wrap the stream so the callback fires when it
                        // completes and the credential-tracker entry is
                        // released on completion (COORD-02). Wrap whenever
                        // either a callback or a tracker is present.
                        let wrapped: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                            if fragment_callback.is_some() || credential_tracker.is_some() {
                                Box::pin(CallbackStream::new(
                                    terminated,
                                    task.fragment_id.clone(),
                                    fragment_callback.clone(),
                                    start,
                                    credential_tracker.clone(),
                                ))
                            } else {
                                terminated
                            };
                        return Ok(wrapped);
                    }
                    Err(e) => {
                        warn!(
                            fragment_id = %task.fragment_id,
                            worker = %current_worker_url,
                            error = %e,
                            attempt = attempt,
                            "Worker execution failed"
                        );

                        // Fire callback with failure for each failed attempt
                        if let Some(ref cb) = fragment_callback {
                            cb(
                                &task.fragment_id,
                                false,
                                start.elapsed().as_millis() as u64,
                                0,
                            );
                        }

                        // Mark the worker as unhealthy in the registry
                        if let Some(ref registry) = worker_registry {
                            registry.mark_unhealthy(&current_worker_url).await;
                        }

                        failed_workers.push(current_worker_url.clone());
                        last_error = Some(e);

                        // Try to find another healthy worker for next attempt
                        if attempt < max_retries {
                            if let Some(ref registry) = worker_registry {
                                let healthy = registry.healthy_workers().await;
                                if let Some(next_worker) = healthy
                                    .into_iter()
                                    .find(|w| !failed_workers.contains(w))
                                {
                                    current_worker_url = next_worker;
                                    continue;
                                }
                            }
                            // No healthy workers available for retry
                            warn!(
                                fragment_id = %task.fragment_id,
                                "No healthy workers available for retry"
                            );
                            break;
                        }
                    }
                }
            }

            // COORD-02: all remote attempts exhausted. The credential-tracker
            // entry is released by the local-fallback stream's CallbackStream
            // (on completion) or, when there is no fallback, by the explicit
            // unregister on the error path below. No early unregister here so
            // the local-fallback stream still tracks its own credentials.

            // Try local fallback
            if let Some(ref executor) = local_executor {
                warn!(
                    fragment_id = %task.fragment_id,
                    failed_workers = ?failed_workers,
                    "All workers failed, falling back to local execution"
                );
                let local_stream = match executor.execute_local(&task, schema) {
                    Ok(s) => s,
                    Err(e) => {
                        // Local fallback failed to start: no stream is created,
                        // so release the tracker entry here (COORD-02).
                        if let Some(ref tracker) = credential_tracker {
                            tracker.unregister(&task.fragment_id).await;
                        }
                        return Err(e);
                    }
                };
                let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                    Box::pin(local_stream);
                let terminated: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                    Box::pin(TerminateOnErrorStream::new(
                        inner,
                        task.fragment_id.clone(),
                    ));
                // Wrap the fallback stream so the callback fires on completion
                // and the credential-tracker entry is released (COORD-02).
                let wrapped: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                    if fragment_callback.is_some() || credential_tracker.is_some() {
                        Box::pin(CallbackStream::new(
                            terminated,
                            task.fragment_id.clone(),
                            fragment_callback,
                            start,
                            credential_tracker,
                        ))
                    } else {
                        terminated
                    };
                return Ok(wrapped);
            }

            // No local fallback available — release the tracker entry
            // (COORD-02: no stream is returned on this path), fire callback with
            // failure, and propagate the last error.
            if let Some(ref tracker) = credential_tracker {
                tracker.unregister(&task.fragment_id).await;
            }
            let err = last_error.unwrap_or_else(|| {
                DataFusionError::Execution(
                    "All workers failed and no local fallback configured".to_string(),
                )
            });
            error!(
                fragment_id = %task.fragment_id,
                failed_workers = ?failed_workers,
                "Fragment execution failed after all retries with no local fallback"
            );
            Err(err)
        };

        // Wrap the two-phase logic into a single stream:
        // once(resolve_future) produces Result<Stream, Error>, try_flatten
        // flattens it into a single Stream<Item = Result<RecordBatch, Error>>.
        let stream = futures::stream::once(resolve_future).try_flatten();

        Ok(Box::pin(DistributedRecordBatchStream {
            schema: schema_for_stream,
            inner: Box::pin(stream),
            baseline: BaselineMetrics::new(&self.metrics, partition),
        }))
    }
}

/// Dispatch a scan task to a single worker via Arrow Flight `do_get`.
///
/// Returns the `FlightRecordBatchStream` on success, or a `DataFusionError`
/// on connection/transport failure. When `pool` is provided, the channel
/// is fetched (or built once and cached) from it; otherwise a fresh
/// connect runs per call bounded by `connect_timeout`.
///
/// The `rpc_timeout` bounds the `do_get` round-trip itself. A kernel-paused
/// worker that accepts the TCP connection but never replies on the gRPC
/// stream surfaces as `DeadlineExceeded` instead of stalling the coordinator's
/// query-semaphore permit indefinitely (issue #29).
async fn dispatch_to_worker(
    task: &ScanTask,
    worker_url: &str,
    pool: Option<&ChannelPool>,
    parent_cx: &opentelemetry::Context,
    worker_secret: &str,
    connect_timeout: std::time::Duration,
    rpc_timeout: std::time::Duration,
) -> Result<FlightRecordBatchStream, DataFusionError> {
    let ticket_bytes = task
        .to_bytes()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let channel = match pool {
        Some(pool) => pool.get(worker_url).await.map_err(|e| {
            pool.invalidate(worker_url);
            DataFusionError::Execution(format!(
                "Failed to connect to worker {worker_url}: {e}"
            ))
        })?,
        None => tonic::transport::Endpoint::new(worker_url.to_string())
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to create endpoint for worker {worker_url}: {e}"
                ))
            })?
            .connect_timeout(connect_timeout)
            .timeout(rpc_timeout)
            .connect()
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to connect to worker {worker_url}: {e}"
                ))
            })?,
    };
    let mut client = FlightServiceClient::new(channel);

    // Sign the exact ticket bytes (#206) before they are moved into the Ticket.
    // Signing the wire bytes rather than a re-serialized struct guarantees the
    // tag covers precisely what the worker decodes: file paths, credentials,
    // and the #233 predicate and limit fields, with no canonicalization gap.
    let scan_signature = sign_ticket(worker_secret, &ticket_bytes);

    let ticket = Ticket::new(ticket_bytes);
    let mut request = tonic::Request::new(ticket);

    if !worker_secret.is_empty() {
        let value = worker_secret.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "worker_secret cannot be encoded as a metadata header value: {e}"
            ))
        })?;
        request.metadata_mut().insert(WORKER_SECRET_HEADER, value);

        if let Some(sig) = scan_signature {
            let sig_value = sig.parse().map_err(|e| {
                DataFusionError::Execution(format!(
                    "scan signature cannot be encoded as a metadata header value: {e}"
                ))
            })?;
            request
                .metadata_mut()
                .insert(SCAN_SIGNATURE_HEADER, sig_value);
        }
    }

    // Inject W3C TraceContext (traceparent/tracestate) into gRPC metadata
    inject_trace_context(parent_cx, request.metadata_mut());

    // Wrap `do_get` with `tokio::time::timeout` so a stalled worker surfaces
    // as a failure within the configured `rpc_timeout`. Pooled channels also
    // carry an Endpoint-level `.timeout(request_timeout)` (see
    // `channel_pool.rs`), now built from the same configured worker timeouts
    // via `ChannelPool::shared_with_timeouts`, so pooled and freshly-connected
    // channels share one budget rather than the old fixed 30s pool default.
    // The explicit wrap stays as the uniform per-deployment ceiling. (#237 /
    // COORD-05: the pool timeout previously ignored `worker_rpc_timeout`.)
    let response = tokio::time::timeout(rpc_timeout, client.do_get(request))
        .await
        .map_err(|_| {
            if let Some(pool) = pool {
                pool.invalidate(worker_url);
            }
            DataFusionError::Execution(format!(
                "Worker {worker_url} do_get exceeded {}s rpc timeout",
                rpc_timeout.as_secs()
            ))
        })?
        .map_err(|e| {
            if let Some(pool) = pool {
                if matches!(
                    e.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) {
                    pool.invalidate(worker_url);
                }
            }
            DataFusionError::Execution(format!(
                "Worker {worker_url} do_get failed: {e}"
            ))
        })?;

    let flight_stream = FlightRecordBatchStream::new_from_flight_data(
        response
            .into_inner()
            .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    );

    Ok(flight_stream)
}

struct DistributedRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    /// Records output rows and coordinator-side poll time so the exec's
    /// `metrics()` reflect what actually flowed through this partition.
    baseline: BaselineMetrics,
}

impl Stream for DistributedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = {
            // The poll itself is mostly waiting on the Flight stream; the
            // timer still attributes coordinator-side decode/reassembly work
            // to this node so the profile row is not blank.
            let _timer = this.baseline.elapsed_compute().timer();
            this.inner.as_mut().poll_next(cx)
        };
        this.baseline.record_poll(poll)
    }
}

impl datafusion::physical_plan::RecordBatchStream for DistributedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream wrapper that fires a [`FragmentCallback`] when the inner stream is fully
/// consumed (returns `Poll::Ready(None)`) or when an error is produced.
///
/// The callback receives:
/// - `fragment_id` — the fragment that was executed
/// - `success` — `true` if the stream ended cleanly, `false` on the first error
/// - `elapsed_ms` — wall-clock time from the start of `execute()` to completion
/// - `output_rows` — total number of rows produced across all batches
struct CallbackStream {
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    fragment_id: String,
    callback: Option<FragmentCallback>,
    start: std::time::Instant,
    output_rows: usize,
    /// Whether the teardown (callback + credential unregister) has already
    /// fired (ensures exactly-once semantics).
    fired: bool,
    /// COORD-02: credential tracker to unregister this fragment from on
    /// completion. The success path used to leak an entry per finished fragment
    /// (register on dispatch, unregister only on the all-attempts-failed path),
    /// growing the tracker map for the life of the process. Firing the
    /// unregister here -- on the same exactly-once teardown that fires the
    /// callback -- covers success, error, and local-fallback completions.
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
}

impl CallbackStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        fragment_id: String,
        callback: Option<FragmentCallback>,
        start: std::time::Instant,
        credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    ) -> Self {
        Self {
            inner,
            fragment_id,
            callback,
            start,
            output_rows: 0,
            fired: false,
            credential_tracker,
        }
    }

    /// COORD-02: fire-and-forget unregister of this fragment from the
    /// credential tracker. Called once on stream completion (EOF or error) and
    /// from `Drop` (cancellation / early stop, e.g. `LIMIT`). `unregister` is
    /// async; the poll/drop context is sync, so spawn it -- but only when a
    /// tokio runtime is still alive. A bare `tokio::spawn` during runtime
    /// teardown panics, and a panic inside `Drop` aborts the process, so guard
    /// with `Handle::try_current()`.
    fn unregister_credentials(&self) {
        if let Some(tracker) = self.credential_tracker.clone() {
            let fragment_id = self.fragment_id.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    tracker.unregister(&fragment_id).await;
                });
            }
        }
    }
}

/// Stream adapter that terminates after surfacing a single error.
///
/// Wraps a `Stream<Item = DFResult<RecordBatch>>` so that:
/// - Every `Ok` value passes through unchanged until an error occurs.
/// - The first `Err` is yielded, then the stream returns `Poll::Ready(None)`
///   on every subsequent poll.
///
/// This protects downstream operators (`HashJoinExec`, `AggregateExec`)
/// from seeing `Ok` batches arriving after an `Err` from the same source,
/// which was the silent-non-determinism gap called out in #50: the
/// consumer would receive partial rows, the join would build state
/// against them, and then a tail error would abort the query. Different
/// retries returned different result sets. With this wrapper the
/// consumer sees a clean prefix of `Ok` values followed by exactly one
/// terminal event (error or normal EOF).
struct TerminateOnErrorStream {
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    fragment_id: String,
    terminated: bool,
}

impl TerminateOnErrorStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        fragment_id: String,
    ) -> Self {
        Self {
            inner,
            fragment_id,
            terminated: false,
        }
    }
}

impl Stream for TerminateOnErrorStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Err(e))) => {
                error!(
                    fragment_id = %self.fragment_id,
                    error = %e,
                    "Mid-stream worker failure; terminating fragment stream after one error"
                );
                self.terminated = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.terminated = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl Stream for CallbackStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                // Stream exhausted — fire callback with success=true
                if !self.fired {
                    self.fired = true;
                    let elapsed_ms = self.start.elapsed().as_millis() as u64;
                    if let Some(cb) = &self.callback {
                        cb(&self.fragment_id, true, elapsed_ms, self.output_rows);
                    }
                    // COORD-02: release the credential-tracker entry on success.
                    self.unregister_credentials();
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                // Stream produced an error — fire callback with success=false
                if !self.fired {
                    self.fired = true;
                    let elapsed_ms = self.start.elapsed().as_millis() as u64;
                    if let Some(cb) = &self.callback {
                        cb(&self.fragment_id, false, elapsed_ms, self.output_rows);
                    }
                    // COORD-02: release the credential-tracker entry on error too.
                    self.unregister_credentials();
                }
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                self.output_rows += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CallbackStream {
    /// COORD-02: when the stream is dropped before reaching EOF or an error --
    /// `SELECT ... LIMIT n` stops polling once it has `n` rows, and query
    /// cancellation / client disconnect drops the stream where it sits -- the
    /// poll-path teardown never runs. Release the credential-tracker entry here
    /// so cancelled / short-circuited fragments do not leak.
    ///
    /// Deliberately tracker-only: the `fragment_callback` marks the fragment
    /// `Failed` and removes its memory reservation, which would mislabel a
    /// cleanly-cancelled fragment, so it is NOT fired from `Drop`. The `fired`
    /// guard gives exactly-once semantics shared with the poll path.
    fn drop(&mut self) {
        if !self.fired {
            self.fired = true;
            self.unregister_credentials();
        }
    }
}

/// Reassemble a worker fragment batch to the schema `DistributedScanExec`
/// advertises (the scan's projected schema). Workers project the ScanTask's
/// `projected_columns`, so a batch normally arrives with exactly the expected
/// columns -- but the parquet `ProjectionMask` emits them in FILE order, which
/// can differ from the plan's projection order, and a worker that could not
/// apply the projection (no matching columns, old worker) ships the full
/// table width. Three cases:
/// - expected is empty (COUNT(*)) -> empty-column batch preserving the row count,
/// - width AND positional field names match -> pass through,
/// - otherwise -> select the expected fields by name from the worker batch
///   (narrows a full-width batch, reorders a projected one).
///
/// A batch missing an expected column fails loudly (fails closed, no silent
/// wrong results).
fn reassemble_worker_batch(
    batch: RecordBatch,
    expected_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let expected_cols = expected_schema.fields().len();
    if expected_cols == 0 {
        // COUNT(*) case: return empty-column batch with row count.
        return Ok(RecordBatch::try_new_with_options(
            expected_schema.clone(),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?);
    }
    let positional_names_match = batch.num_columns() == expected_cols
        && expected_schema
            .fields()
            .iter()
            .zip(batch.schema().fields())
            .all(|(e, a)| e.name() == a.name());
    if positional_names_match {
        return Ok(batch);
    }
    let all_expected_present = expected_schema
        .fields()
        .iter()
        .all(|f| batch.schema().column_with_name(f.name()).is_some());
    if all_expected_present {
        // Select the expected columns by name from the worker batch. Handles
        // both a full-width batch (narrow) and a projected batch whose file
        // order differs from the plan's projection order (reorder).
        let columns: Vec<_> = expected_schema
            .fields()
            .iter()
            .map(|f| {
                batch.column_by_name(f.name()).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Column '{}' not found in worker batch",
                        f.name()
                    ))
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(RecordBatch::try_new(expected_schema.clone(), columns)?)
    } else if batch.num_columns() == expected_cols {
        // Width matches but some expected names are absent: field-ID
        // projection (#43) on a pre-rename file ships the file's OLD column
        // names. The worker projected the right columns by field ID, so
        // accept them positionally; `try_new` still validates the data types
        // against the expected schema.
        Ok(RecordBatch::try_new(
            expected_schema.clone(),
            batch.columns().to_vec(),
        )?)
    } else {
        Err(DataFusionError::Internal(format!(
            "Worker batch ({} columns: {:?}) cannot satisfy the expected scan schema \
             ({} columns: {:?})",
            batch.num_columns(),
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            expected_cols,
            expected_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod reassemble {
        use super::*;
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        fn col(v: i64) -> Arc<dyn arrow_array::Array> {
            Arc::new(Int64Array::from(vec![v]))
        }

        fn batch(names: &[&str]) -> RecordBatch {
            let fields: Vec<Field> = names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, false))
                .collect();
            let cols: Vec<Arc<dyn arrow_array::Array>> =
                names.iter().enumerate().map(|(i, _)| col(i as i64)).collect();
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
        }

        fn schema(names: &[&str]) -> SchemaRef {
            Arc::new(Schema::new(
                names
                    .iter()
                    .map(|n| Field::new(*n, DataType::Int64, false))
                    .collect::<Vec<_>>(),
            ))
        }

        #[test]
        fn full_batch_narrows_to_projected_schema_by_name() {
            // Safety net: a worker that could not apply the projection ships
            // all 3 columns; the coordinator narrows to the 2 the exec
            // expects, reordered by name.
            let worker = batch(&["a", "b", "c"]);
            let expected = schema(&["c", "a"]);
            let out = reassemble_worker_batch(worker, &expected).unwrap();
            assert_eq!(out.num_columns(), 2);
            assert_eq!(out.schema().field(0).name(), "c");
            assert_eq!(out.schema().field(1).name(), "a");
        }

        #[test]
        fn equal_width_batch_passes_through() {
            // The common projected case: worker projected exactly the expected
            // columns in the expected order.
            let out = reassemble_worker_batch(batch(&["a", "b"]), &schema(&["a", "b"])).unwrap();
            assert_eq!(out.num_columns(), 2);
        }

        #[test]
        fn equal_width_reordered_batch_is_reordered_by_name() {
            // Parquet's ProjectionMask emits columns in FILE order. When the
            // plan's projection order differs, the equal-width batch must be
            // reordered by name, NOT passed through positionally.
            let worker = batch(&["a", "c"]); // file order; values a=0, c=1
            let expected = schema(&["c", "a"]); // plan order
            let out = reassemble_worker_batch(worker, &expected).unwrap();
            assert_eq!(out.schema().field(0).name(), "c");
            assert_eq!(out.schema().field(1).name(), "a");
            let c = out
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let a = out
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(c.value(0), 1, "column 'c' carries the file's c values");
            assert_eq!(a.value(0), 0, "column 'a' carries the file's a values");
        }

        #[test]
        fn count_star_empty_schema_preserves_row_count() {
            let worker = batch(&["a", "b", "c"]); // 1 row
            let out = reassemble_worker_batch(worker, &schema(&[])).unwrap();
            assert_eq!(out.num_columns(), 0);
            assert_eq!(out.num_rows(), 1);
        }

        #[test]
        fn equal_width_renamed_columns_accepted_positionally() {
            // RENAME COLUMN survival (#43): field-ID projection on a
            // pre-rename file ships the right columns under the file's OLD
            // names. Width matches, names do not; accept positionally and
            // restamp with the expected schema.
            let worker = batch(&["old_a", "old_b"]);
            let expected = schema(&["new_a", "new_b"]);
            let out = reassemble_worker_batch(worker, &expected).unwrap();
            assert_eq!(out.schema().field(0).name(), "new_a");
            assert_eq!(out.schema().field(1).name(), "new_b");
        }

        #[test]
        fn projected_worker_batch_narrower_than_expected_fails() {
            // Fails closed: a 1-column worker batch can never satisfy a
            // 2-field expected schema (missing column, wrong width).
            let projected_worker = batch(&["a"]);
            let expected = schema(&["a", "b"]);
            assert!(reassemble_worker_batch(projected_worker, &expected).is_err());
        }
    }
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlanProperties;

    #[test]
    fn sign_ticket_empty_secret_yields_none() {
        // Dev mode: no key to sign with, so no signature is attached.
        assert!(sign_ticket("", b"payload").is_none());
    }

    #[test]
    fn sign_ticket_is_deterministic_and_payload_sensitive() {
        let a = sign_ticket("secret", b"payload").unwrap();
        let b = sign_ticket("secret", b"payload").unwrap();
        let c = sign_ticket("secret", b"payloaX").unwrap();
        assert_eq!(a, b, "same key + payload must yield the same tag");
        assert_ne!(a, c, "a changed payload must change the tag");
        // HMAC-SHA256 is 32 bytes -> 64 hex chars.
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn sign_ticket_is_key_sensitive() {
        let a = sign_ticket("secret-1", b"payload").unwrap();
        let b = sign_ticket("secret-2", b"payload").unwrap();
        assert_ne!(a, b, "a changed key must change the tag");
    }

    fn make_task(id: &str) -> ScanTask {
        ScanTask {
            fragment_id: id.to_string(),
            data_file_paths: vec![],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
        }
    }

    #[test]
    fn test_distributed_scan_exec_properties() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let task = ScanTask {
            fragment_id: "frag-001".to_string(),
            data_file_paths: vec!["s3://bucket/file.parquet".to_string()],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: true,
            predicate_proto: None,
            limit: None,
        };

        let exec: Arc<dyn ExecutionPlan> = Arc::new(DistributedScanExec::new(
            vec![task],
            vec!["http://worker1:50052".to_string()],
            schema,
        ));

        assert_eq!(exec.name(), "DistributedScanExec");
        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_distributed_scan_exec_partition_count() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let exec: Arc<dyn ExecutionPlan> = Arc::new(DistributedScanExec::new(
            vec![make_task("f1"), make_task("f2"), make_task("f3")],
            vec![
                "http://w1:50052".to_string(),
                "http://w2:50052".to_string(),
                "http://w3:50052".to_string(),
            ],
            schema,
        ));

        assert_eq!(exec.output_partitioning().partition_count(), 3);
    }

    #[test]
    fn test_execute_out_of_range_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = Arc::new(DistributedScanExec::new(vec![], vec![], schema));

        let context = Arc::new(TaskContext::default());
        let result = exec.execute(0, context);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("out of range"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_with_credential_expiry() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let expiry = Utc::now() + chrono::Duration::hours(1);

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_credential_expiry(expiry);

        assert_eq!(exec.credential_expiry, Some(expiry));
    }

    #[test]
    fn test_with_credential_tracker() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let tracker = Arc::new(CredentialRefreshTracker::new());

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_credential_tracker(tracker);

        assert!(exec.credential_tracker.is_some());
    }

    /// COORD-02 regression: a CallbackStream that completes successfully (EOF)
    /// must unregister its fragment from the credential tracker. Before the fix
    /// the success path returned the stream with no unregister, so the tracker
    /// map grew by one entry per finished fragment for the life of the process.
    #[tokio::test]
    async fn coord02_callback_stream_unregisters_on_success() {
        use futures::StreamExt;

        let tracker = Arc::new(CredentialRefreshTracker::new());
        let fragment_id = "frag-coord02".to_string();
        tracker
            .register(fragment_id.clone(), "http://w1:50052".to_string(), None)
            .await;
        assert_eq!(tracker.active_count().await, 1, "fragment registered");

        // Empty inner stream -> immediate EOF -> success teardown.
        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::empty());
        let mut stream = CallbackStream::new(
            inner,
            fragment_id.clone(),
            None, // no fragment callback; tracker teardown must still fire
            std::time::Instant::now(),
            Some(Arc::clone(&tracker)),
        );

        // Drain to EOF; this fires the teardown which spawns the unregister.
        assert!(stream.next().await.is_none(), "empty stream yields EOF");

        // unregister is spawned async; yield until the tracker is empty.
        for _ in 0..100 {
            if tracker.active_count().await == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            tracker.active_count().await,
            0,
            "COORD-02: completed fragment must be unregistered from the credential tracker"
        );
    }

    /// COORD-02: the same teardown must fire when the stream ends in an error.
    #[tokio::test]
    async fn coord02_callback_stream_unregisters_on_error() {
        use futures::StreamExt;

        let tracker = Arc::new(CredentialRefreshTracker::new());
        let fragment_id = "frag-coord02-err".to_string();
        tracker
            .register(fragment_id.clone(), "http://w1:50052".to_string(), None)
            .await;

        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> = Box::pin(
            futures::stream::once(async { Err(DataFusionError::Execution("boom".into())) }),
        );
        let mut stream = CallbackStream::new(
            inner,
            fragment_id.clone(),
            None,
            std::time::Instant::now(),
            Some(Arc::clone(&tracker)),
        );

        assert!(stream.next().await.unwrap().is_err(), "error surfaced");

        for _ in 0..100 {
            if tracker.active_count().await == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            tracker.active_count().await,
            0,
            "COORD-02: errored fragment must also be unregistered"
        );
    }

    /// COORD-02: dropping the stream BEFORE it completes (the `LIMIT` /
    /// cancellation case) must still unregister. This is the headline leak
    /// vector -- LIMIT queries stop polling mid-stream and never reach EOF.
    /// The poll-path teardown never fires here; only the `Drop` impl does.
    #[tokio::test]
    async fn coord02_callback_stream_unregisters_on_early_drop() {
        let tracker = Arc::new(CredentialRefreshTracker::new());
        let fragment_id = "frag-coord02-drop".to_string();
        tracker
            .register(fragment_id.clone(), "http://w1:50052".to_string(), None)
            .await;
        assert_eq!(tracker.active_count().await, 1);

        // A non-empty, never-fully-drained stream: build it, never poll to EOF,
        // then drop it (mimics LimitExec / cancellation dropping the child).
        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::pending());
        let stream = CallbackStream::new(
            inner,
            fragment_id.clone(),
            None,
            std::time::Instant::now(),
            Some(Arc::clone(&tracker)),
        );
        drop(stream);

        for _ in 0..100 {
            if tracker.active_count().await == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            tracker.active_count().await,
            0,
            "COORD-02: a fragment dropped before EOF (LIMIT / cancel) must be unregistered"
        );
    }

    #[test]
    fn test_default_max_retries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        );
        assert_eq!(exec.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn test_with_max_retries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_max_retries(5);
        assert_eq!(exec.max_retries, 5);
    }

    #[test]
    fn test_with_worker_registry() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let registry = Arc::new(WorkerRegistry::new(vec!["http://w1:50052".to_string()]));
        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry);
        assert!(exec.worker_registry.is_some());
    }

    #[test]
    fn test_display_shows_max_retries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_max_retries(3);

        let mut buf = String::new();
        use std::fmt::Write;
        write!(buf, "{}", DisplayWrapper(&exec)).unwrap();
        assert!(buf.contains("max_retries=3"), "display should contain max_retries: {buf}");
    }

    /// Helper to use DisplayAs in tests.
    struct DisplayWrapper<'a>(&'a DistributedScanExec);
    impl fmt::Display for DisplayWrapper<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_as(DisplayFormatType::Default, f)
        }
    }

    // ---- Retry / fallback integration tests ----

    use std::sync::atomic::{AtomicU32, Ordering};
    use arrow_array::Int64Array;

    /// A test local executor that returns a single batch with a marker value.
    #[derive(Debug)]
    struct TestLocalExecutor {
        call_count: AtomicU32,
    }

    impl TestLocalExecutor {
        fn new() -> Self {
            Self {
                call_count: AtomicU32::new(0),
            }
        }

        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl LocalExecutor for TestLocalExecutor {
        fn execute_local(
            &self,
            _task: &ScanTask,
            schema: SchemaRef,
        ) -> DFResult<SendableRecordBatchStream> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![42])) as _],
            )?;
            Ok(Box::pin(TestRecordBatchStream {
                schema,
                inner: Box::pin(futures::stream::iter(vec![Ok(batch)])),
            }))
        }
    }

    /// Minimal `RecordBatchStream` for test use.
    struct TestRecordBatchStream {
        schema: SchemaRef,
        inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    }

    impl Stream for TestRecordBatchStream {
        type Item = DFResult<RecordBatch>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.inner.as_mut().poll_next(cx)
        }
    }

    impl datafusion::physical_plan::RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[tokio::test]
    async fn test_retry_marks_worker_unhealthy() {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(0);

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        assert!(
            results.iter().any(|r| r.is_err()),
            "Expected error from unreachable worker"
        );

        assert!(
            registry.healthy_workers().await.is_empty(),
            "Failed worker should be marked unhealthy"
        );
    }

    #[tokio::test]
    async fn test_local_fallback_when_all_workers_fail() {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local_exec = Arc::new(TestLocalExecutor::new());

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(0)
        .with_local_executor(Arc::new(LocalExecutorWrapper(local_exec.clone())));

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;

        assert_eq!(local_exec.calls(), 1, "Local executor should be called once");
        assert_eq!(results.len(), 1);
        let batch = results[0].as_ref().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42, "Should get marker value from local executor");
    }

    #[tokio::test]
    async fn metrics_populated_after_execution() {
        // Drive a partition through the local-fallback path (worker is
        // unreachable) and assert the exec's metrics carry the rows that
        // flowed through the stream, so passive profiles and EXPLAIN
        // ANALYZE show real numbers on the DistributedScanExec row.
        let registry = Arc::new(WorkerRegistry::new(vec!["http://w1:50052".to_string()]));
        registry.mark_healthy("http://w1:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local_exec = Arc::new(TestLocalExecutor::new());

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry)
        .with_max_retries(0)
        .with_local_executor(Arc::new(LocalExecutorWrapper(local_exec)));

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let metrics = exec.metrics().expect("metrics must be Some after execution");
        assert_eq!(
            metrics.output_rows(),
            Some(1),
            "baseline metrics must record the one fallback row"
        );
        assert!(
            metrics.elapsed_compute().is_some(),
            "elapsed_compute must be recorded"
        );
    }

    #[tokio::test]
    async fn test_no_fallback_returns_error() {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(0);

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;
        assert!(
            results.iter().any(|r| r.is_err()),
            "Expected error when no fallback is configured"
        );
    }

    #[tokio::test]
    async fn test_retry_tries_different_worker() {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        registry.mark_healthy("http://w2:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 2);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(1);

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let _results: Vec<_> = stream.collect().await;

        assert!(
            registry.healthy_workers().await.is_empty(),
            "Both workers should be marked unhealthy after failures"
        );
    }

    #[tokio::test]
    async fn test_local_fallback_after_retries_exhausted() {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        registry.mark_healthy("http://w2:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local_exec = Arc::new(TestLocalExecutor::new());

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(1)
        .with_local_executor(Arc::new(LocalExecutorWrapper(local_exec.clone())));

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;

        assert_eq!(local_exec.calls(), 1);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
    }

    /// Wrapper to make `Arc<TestLocalExecutor>` implement `LocalExecutor`.
    #[derive(Debug)]
    struct LocalExecutorWrapper(Arc<TestLocalExecutor>);

    impl LocalExecutor for LocalExecutorWrapper {
        fn execute_local(
            &self,
            task: &ScanTask,
            schema: SchemaRef,
        ) -> DFResult<SendableRecordBatchStream> {
            self.0.execute_local(task, schema)
        }
    }

    #[tokio::test]
    async fn test_concurrent_retry_two_partitions_fail_simultaneously() {
        // Two partitions fail simultaneously, both should retry on different workers
        // and ultimately fall back to local execution.
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        registry.mark_healthy("http://w2:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local_exec = Arc::new(TestLocalExecutor::new());

        let exec = Arc::new(
            DistributedScanExec::new(
                vec![make_task("f1"), make_task("f2")],
                vec![
                    "http://w1:50052".to_string(),
                    "http://w2:50052".to_string(),
                ],
                schema,
            )
            .with_worker_registry(registry.clone())
            .with_max_retries(1)
            .with_local_executor(Arc::new(LocalExecutorWrapper(local_exec.clone()))),
        );

        let context = Arc::new(TaskContext::default());

        // Execute both partitions concurrently
        let exec0 = exec.clone();
        let ctx0 = context.clone();
        let exec1 = exec.clone();
        let ctx1 = context.clone();

        let (res0, res1) = tokio::join!(
            async move {
                use futures::StreamExt;
                let stream = exec0.execute(0, ctx0).unwrap();
                stream.collect::<Vec<_>>().await
            },
            async move {
                use futures::StreamExt;
                let stream = exec1.execute(1, ctx1).unwrap();
                stream.collect::<Vec<_>>().await
            }
        );

        // Both partitions should have fallen back to local execution
        // (workers are unreachable), so the local executor should be called twice
        assert_eq!(
            local_exec.calls(),
            2,
            "Local executor should be called for each failed partition"
        );
        assert_eq!(res0.len(), 1, "partition 0 should produce one batch");
        assert_eq!(res1.len(), 1, "partition 1 should produce one batch");
        assert!(res0[0].is_ok(), "partition 0 batch should be Ok");
        assert!(res1[0].is_ok(), "partition 1 batch should be Ok");
    }

    #[tokio::test]
    async fn test_fallback_after_all_workers_fail() {
        // When all workers in the registry are marked unhealthy,
        // verify the fallback executor is called.
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
            "http://w3:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        registry.mark_healthy("http://w2:50052").await;
        registry.mark_healthy("http://w3:50052").await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let local_exec = Arc::new(TestLocalExecutor::new());

        let exec = DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            schema,
        )
        .with_worker_registry(registry.clone())
        .with_max_retries(2) // allow retries across workers
        .with_local_executor(Arc::new(LocalExecutorWrapper(local_exec.clone())));

        let context = Arc::new(TaskContext::default());
        let stream = exec.execute(0, context).unwrap();

        use futures::StreamExt;
        let results: Vec<_> = stream.collect().await;

        // All workers are unreachable, so after exhausting retries the fallback
        // should have been invoked.
        assert_eq!(
            local_exec.calls(),
            1,
            "Local executor should be called when all workers fail"
        );
        assert_eq!(results.len(), 1);
        let batch = results[0].as_ref().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42, "Should get marker value from fallback");

        // Verify the workers got marked unhealthy
        assert!(
            registry.healthy_workers().await.is_empty()
                || registry.healthy_workers().await.len() < 3,
            "At least the attempted workers should be marked unhealthy"
        );
    }

    #[tokio::test]
    async fn terminate_on_error_drops_trailing_oks() {
        // Construct a stream Ok, Ok, Err, Ok, Ok and assert the wrapper
        // surfaces Ok, Ok, Err only.
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let s = schema.clone();
        let b1 = RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let b2 = RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap();
        let b3 = RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![3]))]).unwrap();
        let b4 = RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![4]))]).unwrap();
        let items: Vec<DFResult<RecordBatch>> = vec![
            Ok(b1),
            Ok(b2),
            Err(DataFusionError::Execution("worker died".to_string())),
            Ok(b3),
            Ok(b4),
        ];
        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::iter(items));
        let mut wrapped = TerminateOnErrorStream::new(inner, "frag-test".to_string());

        use futures::StreamExt;
        let collected: Vec<_> = (&mut wrapped).collect().await;
        assert_eq!(
            collected.len(),
            3,
            "Should see exactly Ok, Ok, Err - subsequent Oks must be dropped"
        );
        assert!(collected[0].is_ok());
        assert!(collected[1].is_ok());
        assert!(collected[2].is_err());

        // After termination, further polls must yield None forever.
        assert!(wrapped.next().await.is_none());
        assert!(wrapped.next().await.is_none());
    }

    #[tokio::test]
    async fn terminate_on_error_passthrough_when_no_error() {
        // A clean stream must pass through all batches without truncation.
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let s = schema.clone();
        let batches: Vec<DFResult<RecordBatch>> = (0..5)
            .map(|i| {
                RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![i as i64]))])
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            })
            .collect();
        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::iter(batches));
        let wrapped = TerminateOnErrorStream::new(inner, "frag-clean".to_string());

        use futures::StreamExt;
        let collected: Vec<_> = wrapped.collect().await;
        assert_eq!(collected.len(), 5);
        assert!(collected.iter().all(|r| r.is_ok()));
    }
}

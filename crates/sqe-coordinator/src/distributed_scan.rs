use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_plan::apply_expression_roots;
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
use tracing::{debug, error, info, info_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use sqe_core::SqeError;
use sqe_metrics::propagation::inject_trace_context;
use sqe_planner::{ScanTask, WorkerFault};

use crate::channel_pool::ChannelPool;
use crate::credential_refresh::CredentialRefreshTracker;
use crate::worker_registry::WorkerRegistry;

/// Default maximum number of retry attempts per fragment before giving up.
const DEFAULT_MAX_RETRIES: u32 = 2;

/// Fallback inbound gRPC message ceiling when no config was plumbed in.
/// Mirrors `GrpcTransportConfig::max_decoding_message_bytes`'s default; both
/// exist so a `DistributedScanExec` built in a test is not silently pinned to
/// tonic's 4 MiB (#432).
const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Bytes reserved for gRPC/Flight framing when checking a ticket against the
/// message ceiling. The `Ticket` proto wraps the ScanTask JSON in a length
/// prefix and field tag; a KB of headroom keeps a ticket that measures just
/// under the limit from crossing it on the wire.
const TICKET_FRAMING_HEADROOM_BYTES: usize = 4 * 1024;

/// Metadata header carrying the coordinator/worker shared secret. The
/// worker rejects `do_get` calls that don't carry it (issue #22).
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Metadata header carrying the HMAC-SHA256 tag (hex) over the exact ticket
/// bytes, keyed by the shared worker_secret (issue #206). The worker
/// recomputes the tag over the received bytes and constant-time compares
/// before executing, proving the coordinator authored the exact ScanTask
/// (file paths, credentials, predicate, and limit included).
const SCAN_SIGNATURE_HEADER: &str = "x-sqe-scan-signature";

// The ticket-predicate budget lives in `scan_pushdown`
// (`MAX_PUSHED_PREDICATE_BYTES`), next to the serialization it guards.
//
// Scope, honestly: it bounds ONE ticket's predicate. It does NOT explain the
// refusals measured on 2026-08-18, where SF10 SSB with
// `runtime_filter_inlist_max_values = 1000000` had workers reject 9.1 MB
// messages ("decoded message length too large") while this guard never fired,
// so that oversized message is aggregated somewhere else on the wire (#432). The
// shipped 65536-value cap does not reach it: the same rig at the default is
// 13/13 with zero refusals.

/// Compute the HMAC-SHA256 tag (lowercase hex) over `bytes` keyed by `secret`.
///
/// Returns `None` when `secret` is empty: an empty key means the deployment
/// opted into `worker.allow_unauthenticated`, so there is nothing to sign.
pub(crate) fn sign_ticket(secret: &str, bytes: &[u8]) -> Option<String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    if secret.is_empty() {
        return None;
    }
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
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
/// When an attempt fails -- at dispatch, or mid-stream before any batch has
/// reached the consumer -- the executor consults the worker fault (finding
/// 602, [`WorkerFault`]) and either:
/// 1. marks the worker unhealthy and re-dispatches the (idempotent) fragment
///    to another healthy worker, up to `max_retries` times (worker faults,
///    transport errors);
/// 2. backs off and retries the same worker (`SourceThrottled`);
/// 3. fails the fragment immediately and leaves the registry alone
///    (`TaskInvalid`, `PrincipalDenied`, `SourceMissing`, `Cancelled`): no
///    other worker would do better, and the worker did nothing wrong.
///
/// A mid-stream error AFTER batches were yielded cannot be retried (the
/// consumer already holds a prefix), so the fragment fails; the worker is
/// still marked unhealthy under the same fault policy (finding 402).
///
/// There is no per-fragment local fallback. Running one fragment of an
/// N-worker scan on the coordinator hid worker outages behind a slow query;
/// the plan-level `query.distribution_fallback` setting is the deliberate
/// replacement.
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
    /// Reloads the scanned table as the querying user for fresh vended
    /// keys; registered with each fragment so the refresh task can use it.
    credential_reloader: Option<sqe_catalog::vended_credentials::TableReloader>,
    /// Worker registry for health tracking and failover.
    worker_registry: Option<Arc<WorkerRegistry>>,
    /// Maximum number of retry attempts per fragment.
    max_retries: u32,
    /// How a fragment stream is obtained from a worker. `None` means the
    /// production Flight `do_get` path; tests script outcomes per worker.
    dispatcher: Option<Arc<dyn FragmentDispatcher>>,
    /// Optional callback fired when each fragment completes or fails.
    fragment_callback: FragmentCallbackOpt,
    /// Shared secret attached to every outbound `do_get` so the worker
    /// can authenticate the coordinator. Empty when distributed mode is
    /// running with `allow_unauthenticated_workers = true`.
    worker_secret: String,
    /// Finding 601: Ed25519 signer for scan tickets. `None` falls back to the
    /// legacy HMAC over `worker_secret` (test/dev only).
    ticket_signer: Option<Arc<sqe_core::ticket_signing::TicketSigner>>,
    /// How long a signed ticket stays valid on the worker.
    ticket_ttl: std::time::Duration,
    /// gRPC connect timeout for the per-call (`pool = None`) dispatch path.
    /// Issue #29.
    worker_connect_timeout: std::time::Duration,
    /// gRPC request timeout for `do_get`. Issue #29.
    worker_rpc_timeout: std::time::Duration,
    /// Inbound gRPC message ceiling on the coordinator/worker link (#432).
    /// Bounds both what this coordinator will accept back from a worker and
    /// the size of the ticket it is willing to send, since the worker enforces
    /// the same number on its own server.
    max_message_bytes: usize,
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
    /// Prometheus registry for the ticket cost metrics (#809). `None` (tests,
    /// no `/metrics` server) skips the timing entirely.
    metrics_registry: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

/// A fragment's batches as they arrive from a worker, before reassembly to
/// the plan's schema.
pub(crate) type WorkerStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>;

/// Why one dispatch attempt failed, kept typed so the retry loop can consult
/// the fault policy (finding 602) instead of guessing from a message.
#[derive(Debug)]
pub(crate) enum DispatchError {
    /// The coordinator could not build the ticket or its headers. No worker
    /// can fix that; the producer has a bug.
    Ticket(DataFusionError),
    /// Connect failure, rpc timeout, dead channel: no `Status` reached us.
    Transport(String),
    /// The worker (or tonic on its behalf) answered with a gRPC `Status`.
    Status(tonic::Status),
}

/// Single choke point for "get this fragment's stream from that worker", and
/// the seam the retry tests script through. Production uses
/// [`FlightDispatcher`] (`do_get` over the channel pool).
pub(crate) trait FragmentDispatcher: Send + Sync + fmt::Debug {
    fn dispatch(
        &self,
        task: ScanTask,
        worker_url: String,
    ) -> Pin<Box<dyn Future<Output = Result<WorkerStream, DispatchError>> + Send>>;
}

/// Callback invoked when a fragment stream completes, fails, or is
/// re-dispatched. Args: (fragment_id, outcome, elapsed_ms, output_rows)
pub type FragmentCallback = Arc<dyn Fn(&str, FragmentOutcome, u64, usize) + Send + Sync>;

/// What went wrong with a fragment attempt, as reported to the
/// [`FragmentCallback`] and used as the `fault` label of
/// `sqe_worker_fragment_failures_total` (finding 602).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentFault {
    /// The worker classified the failure ([`WorkerFault`]), or the bare gRPC
    /// code mapped to one.
    Worker(WorkerFault),
    /// The connection died or timed out; no classifiable `Status` arrived.
    Transport,
    /// A coordinator-side failure with no worker fault behind it (ticket
    /// encoding, Flight decode, batch reassembly).
    Unknown,
}

impl FragmentFault {
    /// Stable label for metrics. `WorkerFault` labels are reused verbatim.
    pub fn metric_label(self) -> &'static str {
        match self {
            FragmentFault::Worker(f) => f.metric_label(),
            FragmentFault::Transport => "transport",
            FragmentFault::Unknown => "unknown",
        }
    }

}

impl fmt::Display for FragmentFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.metric_label())
    }
}

/// How a fragment stream ended, as reported to the [`FragmentCallback`].
///
/// Finding 408: `Cancelled` is distinct from `Failed` so the callback can
/// release the worker-load reservation for a stream dropped before EOF
/// (`LIMIT`-terminated or cancelled query) without mislabelling a clean stop
/// as a failure. Before this the reservation lived until the plan `Arc`
/// dropped.
///
/// Finding 402/602: `Retrying` is NOT terminal. It reports one failed attempt
/// whose fragment is being re-dispatched, so the failure is counted by fault
/// without the query tracker marking the fragment finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentOutcome {
    /// The stream reached EOF.
    Finished,
    /// The fragment failed for good: retries exhausted, a fault that must not
    /// be retried, or an error after batches had already been yielded.
    Failed(FragmentFault),
    /// One attempt failed and the fragment is being dispatched again.
    Retrying(FragmentFault),
    /// The stream was dropped before EOF or error.
    Cancelled,
}

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

    pub fn new(scan_tasks: Vec<ScanTask>, worker_urls: Vec<String>, schema: SchemaRef) -> Self {
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
            credential_reloader: None,
            worker_registry: None,
            max_retries: DEFAULT_MAX_RETRIES,
            dispatcher: None,
            fragment_callback: FragmentCallbackOpt(None),
            worker_secret: String::new(),
            ticket_signer: None,
            ticket_ttl: std::time::Duration::from_secs(300),
            worker_connect_timeout: std::time::Duration::from_secs(5),
            worker_rpc_timeout: std::time::Duration::from_secs(630),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters: vec![],
            metrics_registry: None,
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
            credential_reloader: self.credential_reloader.clone(),
            worker_registry: self.worker_registry.clone(),
            max_retries: self.max_retries,
            dispatcher: self.dispatcher.clone(),
            fragment_callback: FragmentCallbackOpt(self.fragment_callback.0.clone()),
            worker_secret: self.worker_secret.clone(),
            ticket_signer: self.ticket_signer.clone(),
            ticket_ttl: self.ticket_ttl,
            worker_connect_timeout: self.worker_connect_timeout,
            worker_rpc_timeout: self.worker_rpc_timeout,
            max_message_bytes: self.max_message_bytes,
            metrics: self.metrics.clone(),
            pushed_down_filters: filters,
            metrics_registry: self.metrics_registry.clone(),
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

    /// Finding 601: sign tickets with the coordinator's Ed25519 key and
    /// stamp each with its target worker and an expiry.
    #[must_use = "with_ticket_signer consumes self; bind the returned scan"]
    pub fn with_ticket_signer(
        mut self,
        signer: Arc<sqe_core::ticket_signing::TicketSigner>,
        ttl: std::time::Duration,
    ) -> Self {
        self.ticket_signer = Some(signer);
        self.ticket_ttl = ttl;
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

    /// Set the reloader the refresh task uses to fetch this scan's next
    /// vended keys (as the querying user).
    #[must_use = "with_credential_reloader consumes self; bind the returned scan"]
    pub fn with_credential_reloader(
        mut self,
        reloader: sqe_catalog::vended_credentials::TableReloader,
    ) -> Self {
        self.credential_reloader = Some(reloader);
        self
    }

    /// Set the worker registry for health tracking and failover.
    #[must_use = "with_worker_registry consumes self; bind the returned scan"]
    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    /// Set the maximum number of retry attempts per fragment. Production
    /// runs on `DEFAULT_MAX_RETRIES`; the retry and fault-policy tests set it.
    #[cfg(test)]
    #[must_use = "with_max_retries consumes self; bind the returned scan"]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Replace the Flight `do_get` dispatch with a scripted one. Test seam
    /// for the retry / fault-policy tests; production never sets it.
    #[cfg(test)]
    #[must_use = "with_dispatcher consumes self; bind the returned scan"]
    pub(crate) fn with_dispatcher(mut self, dispatcher: Arc<dyn FragmentDispatcher>) -> Self {
        self.dispatcher = Some(dispatcher);
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

    /// Set the inbound gRPC message ceiling for this link (#432). Must match
    /// the workers' `[coordinator.transport].max_decoding_message_bytes`; the
    /// coordinator uses it both to bound what it accepts and to decide when a
    /// ticket is too large to send.
    #[must_use = "with_message_limit consumes self; bind the returned scan"]
    pub fn with_message_limit(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    /// Record ticket size and encode / dynamic-filter rewrite time into the
    /// coordinator's Prometheus registry (#809).
    #[must_use = "with_metrics consumes self; bind the returned scan"]
    pub fn with_metrics(mut self, registry: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics_registry = Some(registry);
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// SQE PATCH (DataFusion 55): report the filters pushed into this scan.
    /// This is a leaf, so no other node in the tree owns them; leaving them
    /// unreported hides the hash-join dynamic filters from DataFusion's
    /// expression traversal.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        apply_expression_roots(self.pushed_down_filters.iter(), f)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// DataFusion 55 deprecated this but kept it a *required* trait method, and
    /// its own migration example is exactly this shim. The real logic lives in
    /// `replace_children`; this forwards with `Recompute`, which is what
    /// `with_new_children` always meant.
    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        // DataFusion 55 replaced with_new_children. `Keep` only *permits*
        // skipping property recomputation; these nodes rebuild and recompute
        // unconditionally, which is valid under both modes (recomputing when
        // Keep was allowed is redundant, never wrong), so it is not consulted.
        _options: ReplaceChildrenOptions,
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
        // Conjuncts of a dynamic filter snapshot that had no logical form and
        // were therefore NOT pushed to the worker (issue #415). Non-zero means
        // the worker's predicate is wider than the coordinator's join filter:
        // correct results, more rows over Flight than necessary.
        let dynamic_filter_conjuncts_dropped = MetricBuilder::new(&self.metrics)
            .counter("dynamic_filter_conjuncts_dropped", partition);
        // Fragments that had a convertible dynamic filter but could not carry it
        // because the serialized predicate would not fit in one gRPC message.
        let predicate_too_large =
            MetricBuilder::new(&self.metrics).counter("dynamic_filter_predicate_too_large", partition);
        // Tickets larger than this are refused before dispatch: the worker
        // enforces the same ceiling on its own server, so sending one buys a
        // guaranteed `decoded message length too large` instead of a scan.
        let max_message_bytes = self.max_message_bytes;
        let ticket_predicate_dropped = MetricBuilder::new(&self.metrics)
            .counter("ticket_predicate_dropped_oversize", partition);
        let initial_worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();
        let schema_for_stream = self.schema.clone();
        let credential_expiry = self.credential_expiry;
        let credential_tracker = self.credential_tracker.clone();
        let credential_reloader = self.credential_reloader.clone();
        let max_retries = self.max_retries;
        let worker_registry = self.worker_registry.clone();
        let channel_pool = self.worker_registry.as_ref().map(|r| r.channel_pool());
        let fragment_callback = self.fragment_callback.0.clone();
        let metrics_registry = self.metrics_registry.clone();
        let dispatch_limits = DispatchLimits {
            connect_timeout: self.worker_connect_timeout,
            rpc_timeout: self.worker_rpc_timeout,
            max_message_bytes: self.max_message_bytes,
        };

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

        let dispatcher: Arc<dyn FragmentDispatcher> = match &self.dispatcher {
            Some(scripted) => Arc::clone(scripted),
            None => Arc::new(FlightDispatcher {
                pool: channel_pool.clone(),
                parent_cx,
                worker_secret: self.worker_secret.clone(),
                ticket_signer: self.ticket_signer.clone(),
                ticket_ttl: self.ticket_ttl,
                limits: dispatch_limits,
                ticket_predicate_dropped,
                metrics_registry: metrics_registry.clone(),
            }),
        };

        // Phase 1: finalise the ticket (dynamic filters) and register it.
        // Phase 2: drive the fragment: dispatch, stream, and on failure consult
        // the fault policy to re-dispatch or fail (finding 402 / 602).
        //
        // Phase 1 is a future rather than inline because the dynamic-filter
        // snapshot must run on the stream's first poll, after the join build
        // side has materialised its bounds.
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
                    .filter_map(|f| f.downcast_ref::<DynamicFilterPhysicalExpr>())
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
                // #809: starts after the readiness wait so it measures the
                // decode + AND + re-serialize work, not the join builds.
                let rewrite_started = metrics_registry
                    .as_ref()
                    .map(|_| std::time::Instant::now());
                let mut snapshots: Vec<datafusion::logical_expr::Expr> = Vec::new();
                for f in &pushed_down_filters {
                    let resolved = match f.downcast_ref::<DynamicFilterPhysicalExpr>() {
                        Some(dynamic) => match dynamic.current() {
                            Ok(expr) => expr,
                            Err(_) => continue,
                        },
                        None => Arc::clone(f),
                    };
                    if crate::scan_pushdown::is_trivially_true(&resolved) {
                        continue;
                    }
                    // Report what the conversion had to leave behind. A
                    // snapshot that keeps only its min/max bounds and drops
                    // the `hash_lookup` membership term still counts as a
                    // successful push, so without this the lost selectivity
                    // is invisible -- the case issue #415 is about.
                    let report =
                        crate::scan_pushdown::physical_filter_to_logical_reporting(&resolved);
                    if !report.dropped.is_empty() {
                        dynamic_filter_conjuncts_dropped.add(report.dropped.len());
                        tracing::debug!(
                            fragment_id = %task.fragment_id,
                            filter = %resolved,
                            dropped = ?report.dropped,
                            kept = report.expr.is_some(),
                            "dynamic filter conjunct(s) had no logical form; \
                             worker predicate is wider than the join filter"
                        );
                    }
                    if let Some(logical) = report.expr {
                        snapshots.push(logical);
                    }
                }
                if !snapshots.is_empty() {
                    let snapshot_count = snapshots.len();
                    let existing = task
                        .predicate_proto
                        .as_deref()
                        .and_then(|b| datafusion::logical_expr::Expr::from_bytes(b).ok());
                    if let Some(combined) = existing
                        .into_iter()
                        .chain(snapshots)
                        .reduce(|a, b| a.and(b))
                    {
                        match crate::scan_pushdown::serialize_pushed_predicate(
                            &combined,
                            crate::scan_pushdown::raw_predicate_budget(max_message_bytes),
                        ) {
                            crate::scan_pushdown::PushedPredicate::TooLarge(len) => {
                                // A ScanTask ticket rides one gRPC message, so a
                                // large InList membership set (raise
                                // `runtime_filter_inlist_max_size` and SF10 SSB
                                // partkey gets there) does not make the query
                                // slower, it makes the worker refuse the ticket:
                                // "decoded message length too large". The push is
                                // an optimization, so drop it and keep the pre-push
                                // predicate; the coordinator's filter is
                                // authoritative either way. The ceiling is now
                                // configurable and enforced on both ends (#432).
                                predicate_too_large.add(1);
                                warn!(
                                    fragment_id = %task.fragment_id,
                                    bytes = len,
                                    budget =
                                        crate::scan_pushdown::raw_predicate_budget(max_message_bytes),
                                    dynamic_filters = snapshot_count,
                                    "dynamic filter predicate exceeds the ticket budget; \
                                     shipping without it"
                                );
                            }
                            crate::scan_pushdown::PushedPredicate::Fits(bytes) => {
                                task.predicate_proto = Some(bytes);
                                dynamic_filters_pushed.add(snapshot_count);
                                info!(
                                    fragment_id = %task.fragment_id,
                                    dynamic_filters = snapshot_count,
                                    "ANDed dynamic join filter snapshots into worker predicate"
                                );
                            }
                            crate::scan_pushdown::PushedPredicate::Failed(e) => warn!(
                                fragment_id = %task.fragment_id,
                                error = %e,
                                "failed to serialize dynamic filter snapshot; shipping unfiltered"
                            ),
                        }
                    }
                }
                if let (Some(m), Some(started)) = (&metrics_registry, rewrite_started) {
                    m.scan_ticket_dynamic_filter_seconds
                        .observe(started.elapsed().as_secs_f64());
                }
            }

            // Register fragment with the credential tracker if available
            if let Some(ref tracker) = credential_tracker {
                tracker
                    .register_with_reloader(
                        task.fragment_id.clone(),
                        initial_worker_url.clone(),
                        credential_expiry,
                        credential_reloader.clone(),
                    )
                    .await;
            }

            let start = std::time::Instant::now();
            let fragment_id = task.fragment_id.clone();
            let last_fault: Arc<Mutex<Option<FragmentFault>>> = Arc::new(Mutex::new(None));

            let run = Arc::new(FragmentRun {
                task,
                schema,
                dispatcher,
                registry: worker_registry,
                channel_pool,
                callback: fragment_callback.clone(),
                start,
                max_retries,
                last_fault: Arc::clone(&last_fault),
            });
            let driven: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                Box::pin(run.drive(initial_worker_url));

            // The driver already stops after its terminal error; the wrapper
            // keeps the "clean prefix, then exactly one terminal event"
            // guarantee (#50) independent of the driver's internals.
            let terminated: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                Box::pin(TerminateOnErrorStream::new(driven, fragment_id.clone()));
            // Wrap the stream so the callback fires when it completes and the
            // credential-tracker entry is released on completion (COORD-02).
            // Wrap whenever either a callback or a tracker is present.
            let wrapped: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                if fragment_callback.is_some() || credential_tracker.is_some() {
                    Box::pin(CallbackStream::new(
                        terminated,
                        fragment_id,
                        fragment_callback,
                        start,
                        credential_tracker,
                        last_fault,
                    ))
                } else {
                    terminated
                };
            Ok::<_, DataFusionError>(wrapped)
        }
        .instrument(dispatch_span.clone());

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

/// What the retry loop does with one failed attempt (finding 602).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptPolicy {
    /// Evict the worker and re-dispatch the fragment to another healthy one.
    RetryElsewhere,
    /// Back off and retry on the same worker; the registry is untouched.
    RetrySameWorker,
    /// Fail the fragment now; the registry is untouched.
    FailFast,
}

/// One classified attempt failure: the fault for metrics and the callback,
/// the policy for the loop, and the error the consumer sees if it is final.
struct AttemptFailure {
    fault: FragmentFault,
    policy: AttemptPolicy,
    error: DataFusionError,
}

/// Map a decoded [`WorkerFault`] to the retry policy. This is the whole 602
/// table in one place:
///
/// | fault                     | registry        | action                 |
/// |---------------------------|-----------------|------------------------|
/// | `WorkerResourceExhausted` | mark unhealthy  | retry on another worker|
/// | `WorkerInternal`          | mark unhealthy  | retry on another worker|
/// | transport (no fault)      | mark unhealthy  | retry on another worker|
/// | `SourceThrottled`         | untouched       | back off, same worker  |
/// | `TaskInvalid`             | untouched       | fail now               |
/// | `PrincipalDenied`         | untouched       | fail now               |
/// | `SourceMissing`           | untouched       | fail now               |
/// | `Cancelled`               | untouched       | fail now               |
fn policy_for(fault: WorkerFault) -> AttemptPolicy {
    if fault.is_retryable_on_other_worker() {
        AttemptPolicy::RetryElsewhere
    } else if fault.is_retryable_same_worker() {
        AttemptPolicy::RetrySameWorker
    } else {
        AttemptPolicy::FailFast
    }
}

/// Classify a gRPC `Status` from a worker, at dispatch or mid-stream.
///
/// `Unavailable` / `DeadlineExceeded` / `Unknown` decode to no fault: tonic's
/// transport layer produces those for a dead connection, so they take the
/// transport path (evict, retry elsewhere) rather than being mistaken for a
/// worker-reported throttle.
fn classify_status(status: &tonic::Status, worker_url: &str, phase: &str) -> AttemptFailure {
    match WorkerFault::from_status(status) {
        Some(fault) => AttemptFailure {
            fault: FragmentFault::Worker(fault),
            policy: policy_for(fault),
            error: DataFusionError::External(Box::new(SqeError::with_code(
                fault.sqe_error_code(),
                format!(
                    "Worker {worker_url} {phase} failed ({fault}): {}",
                    status.message()
                ),
            ))),
        },
        None => AttemptFailure {
            fault: FragmentFault::Transport,
            policy: AttemptPolicy::RetryElsewhere,
            error: DataFusionError::Execution(format!(
                "Worker {worker_url} {phase} failed: {status}"
            )),
        },
    }
}

fn classify_dispatch_error(err: DispatchError, worker_url: &str) -> AttemptFailure {
    match err {
        DispatchError::Status(status) => classify_status(&status, worker_url, "do_get"),
        DispatchError::Transport(msg) => AttemptFailure {
            fault: FragmentFault::Transport,
            policy: AttemptPolicy::RetryElsewhere,
            error: DataFusionError::Execution(msg),
        },
        // The coordinator built a ticket it cannot encode: retrying it on any
        // worker asks the same question. Producer bug; fail loudly.
        DispatchError::Ticket(e) => AttemptFailure {
            fault: FragmentFault::Worker(WorkerFault::TaskInvalid),
            policy: AttemptPolicy::FailFast,
            error: e,
        },
    }
}

fn classify_stream_error(err: FlightError, worker_url: &str) -> AttemptFailure {
    match err {
        FlightError::Tonic(status) => classify_status(&status, worker_url, "stream"),
        // Flight decode / protocol errors: the worker shipped something this
        // coordinator could not read. Another worker may not; the fault is
        // not classifiable, so it is counted as `unknown`.
        other => AttemptFailure {
            fault: FragmentFault::Unknown,
            policy: AttemptPolicy::RetryElsewhere,
            error: DataFusionError::Execution(format!(
                "Worker {worker_url} stream failed: {other}"
            )),
        },
    }
}

/// Exponential backoff before attempt `attempt` (1-based for retries).
fn retry_backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(50 * (1 << attempt.min(4)))
}

/// Where the fragment driver is in its attempt sequence.
enum Phase {
    /// About to (re)dispatch to `worker`; `attempt` is 0 for the first try.
    Dispatch { worker: String, attempt: u32 },
    /// Streaming batches from `worker`.
    Streaming {
        worker: String,
        inner: WorkerStream,
        attempt: u32,
    },
    /// Terminal: EOF or a final error has been yielded.
    Done,
}

/// Mutable state threaded through the driver's `unfold`.
struct RunState {
    phase: Phase,
    /// Workers this fragment has already failed on; never retried.
    failed_workers: Vec<String>,
    /// Batches handed downstream so far. Once non-zero a mid-stream error
    /// cannot be retried: the consumer already holds a prefix.
    yielded_batches: usize,
}

/// Everything one fragment's attempts share (finding 402).
///
/// The retry loop used to live in a "resolve the stream" future, so it could
/// only retry `do_get`; once a stream was returned, a mid-stream loss ended
/// the query. [`FragmentRun::drive`] is a stream that owns the attempt loop,
/// so a worker lost before the first batch is retried like a dispatch
/// failure.
struct FragmentRun {
    task: ScanTask,
    schema: SchemaRef,
    dispatcher: Arc<dyn FragmentDispatcher>,
    registry: Option<Arc<WorkerRegistry>>,
    channel_pool: Option<Arc<ChannelPool>>,
    callback: Option<FragmentCallback>,
    start: std::time::Instant,
    max_retries: u32,
    /// The fault behind the terminal error, read by [`CallbackStream`] so the
    /// `Failed` outcome carries it.
    last_fault: Arc<Mutex<Option<FragmentFault>>>,
}

impl FragmentRun {
    fn set_fault(&self, fault: FragmentFault) {
        if let Ok(mut slot) = self.last_fault.lock() {
            *slot = Some(fault);
        }
    }

    /// Apply the registry side of the policy: evict the worker (and drop its
    /// pooled channel on a transport failure) for `RetryElsewhere`; leave it
    /// alone otherwise. User-caused faults never count against the worker.
    async fn penalise(&self, worker: &str, failure: &AttemptFailure) {
        if failure.policy != AttemptPolicy::RetryElsewhere {
            return;
        }
        if let Some(registry) = &self.registry {
            registry.mark_unhealthy(worker).await;
        }
        if failure.fault == FragmentFault::Transport {
            if let Some(pool) = &self.channel_pool {
                pool.invalidate(worker);
            }
        }
    }

    /// Decide what follows a failed attempt that has not yielded batches:
    /// `Ok(next phase)` to retry, `Err(error)` to fail the fragment.
    async fn after_failure(
        &self,
        state: &mut RunState,
        worker: String,
        attempt: u32,
        failure: AttemptFailure,
    ) -> Result<Phase, DataFusionError> {
        warn!(
            fragment_id = %self.task.fragment_id,
            worker = %worker,
            fault = %failure.fault,
            policy = ?failure.policy,
            attempt,
            max_retries = self.max_retries,
            error = %failure.error,
            "Fragment attempt failed"
        );
        self.penalise(&worker, &failure).await;

        if failure.policy == AttemptPolicy::FailFast {
            self.set_fault(failure.fault);
            return Err(failure.error);
        }
        if attempt >= self.max_retries {
            error!(
                fragment_id = %self.task.fragment_id,
                failed_workers = ?state.failed_workers,
                fault = %failure.fault,
                "Fragment failed after all retries"
            );
            self.set_fault(failure.fault);
            return Err(failure.error);
        }

        let next_worker = match failure.policy {
            AttemptPolicy::RetrySameWorker => Some(worker),
            AttemptPolicy::RetryElsewhere => {
                state.failed_workers.push(worker);
                match &self.registry {
                    Some(registry) => registry
                        .healthy_workers()
                        .await
                        .into_iter()
                        .find(|w| !state.failed_workers.contains(w)),
                    None => None,
                }
            }
            AttemptPolicy::FailFast => unreachable!("handled above"),
        };
        let Some(next_worker) = next_worker else {
            warn!(
                fragment_id = %self.task.fragment_id,
                failed_workers = ?state.failed_workers,
                "No healthy workers available for retry"
            );
            self.set_fault(failure.fault);
            return Err(failure.error);
        };

        if let Some(cb) = &self.callback {
            cb(
                &self.task.fragment_id,
                FragmentOutcome::Retrying(failure.fault),
                self.start.elapsed().as_millis() as u64,
                0,
            );
        }
        Ok(Phase::Dispatch {
            worker: next_worker,
            attempt: attempt + 1,
        })
    }

    /// The fragment as a stream: dispatch, stream, and on failure consult the
    /// fault policy. Ends after EOF or exactly one terminal error.
    fn drive(
        self: Arc<Self>,
        initial_worker: String,
    ) -> impl Stream<Item = DFResult<RecordBatch>> + Send {
        let state = RunState {
            phase: Phase::Dispatch {
                worker: initial_worker,
                attempt: 0,
            },
            failed_workers: Vec::new(),
            yielded_batches: 0,
        };
        futures::stream::unfold(state, move |mut state| {
            let run = Arc::clone(&self);
            async move {
                loop {
                    match std::mem::replace(&mut state.phase, Phase::Done) {
                        Phase::Done => return None,
                        Phase::Dispatch { worker, attempt } => {
                            if attempt > 0 {
                                tokio::time::sleep(retry_backoff(attempt)).await;
                                warn!(
                                    fragment_id = %run.task.fragment_id,
                                    attempt,
                                    max_retries = run.max_retries,
                                    worker = %worker,
                                    "Retrying fragment"
                                );
                            }
                            match run
                                .dispatcher
                                .dispatch(run.task.clone(), worker.clone())
                                .await
                            {
                                Ok(inner) => {
                                    state.phase = Phase::Streaming {
                                        worker,
                                        inner,
                                        attempt,
                                    };
                                }
                                Err(e) => {
                                    let failure = classify_dispatch_error(e, &worker);
                                    match run
                                        .after_failure(&mut state, worker, attempt, failure)
                                        .await
                                    {
                                        Ok(next) => state.phase = next,
                                        Err(err) => return Some((Err(err), state)),
                                    }
                                }
                            }
                        }
                        Phase::Streaming {
                            worker,
                            mut inner,
                            attempt,
                        } => match inner.next().await {
                            None => return None,
                            Some(Ok(batch)) => {
                                match reassemble_worker_batch(batch, &run.schema) {
                                    Ok(batch) => {
                                        state.yielded_batches += 1;
                                        state.phase = Phase::Streaming {
                                            worker,
                                            inner,
                                            attempt,
                                        };
                                        return Some((Ok(batch), state));
                                    }
                                    Err(e) => {
                                        // Contract mismatch between what the
                                        // worker shipped and what the plan
                                        // expects; another worker would ship
                                        // the same thing.
                                        run.set_fault(FragmentFault::Unknown);
                                        return Some((Err(e), state));
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                let failure = classify_stream_error(e, &worker);
                                if state.yielded_batches > 0 {
                                    // Downstream already consumed a prefix of
                                    // this fragment; re-dispatching would
                                    // duplicate rows. The query fails, but the
                                    // worker is still judged by the fault
                                    // policy (finding 402).
                                    error!(
                                        fragment_id = %run.task.fragment_id,
                                        worker = %worker,
                                        fault = %failure.fault,
                                        yielded_batches = state.yielded_batches,
                                        error = %failure.error,
                                        "Mid-stream worker failure after batches were yielded; \
                                         cannot retry"
                                    );
                                    run.penalise(&worker, &failure).await;
                                    run.set_fault(failure.fault);
                                    return Some((Err(failure.error), state));
                                }
                                debug!(
                                    fragment_id = %run.task.fragment_id,
                                    worker = %worker,
                                    "Mid-stream failure before any batch; treating as a dispatch failure"
                                );
                                match run
                                    .after_failure(&mut state, worker, attempt, failure)
                                    .await
                                {
                                    Ok(next) => state.phase = next,
                                    Err(err) => return Some((Err(err), state)),
                                }
                            }
                        },
                    }
                }
            }
        })
    }
}

/// Production [`FragmentDispatcher`]: Arrow Flight `do_get` against the
/// worker, over the channel pool when there is one.
struct FlightDispatcher {
    pool: Option<Arc<ChannelPool>>,
    parent_cx: opentelemetry::Context,
    worker_secret: String,
    ticket_signer: Option<Arc<sqe_core::ticket_signing::TicketSigner>>,
    ticket_ttl: std::time::Duration,
    limits: DispatchLimits,
    ticket_predicate_dropped: datafusion::physical_plan::metrics::Count,
    metrics_registry: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

impl fmt::Debug for FlightDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlightDispatcher")
            .field("pooled", &self.pool.is_some())
            .field("signed", &self.ticket_signer.is_some())
            .finish()
    }
}

impl FragmentDispatcher for FlightDispatcher {
    fn dispatch(
        &self,
        task: ScanTask,
        worker_url: String,
    ) -> Pin<Box<dyn Future<Output = Result<WorkerStream, DispatchError>> + Send>> {
        let pool = self.pool.clone();
        let parent_cx = self.parent_cx.clone();
        let worker_secret = self.worker_secret.clone();
        let ticket_signer = self.ticket_signer.clone();
        let ticket_ttl = self.ticket_ttl;
        let limits = self.limits;
        let dropped = self.ticket_predicate_dropped.clone();
        let metrics_registry = self.metrics_registry.clone();
        Box::pin(async move {
            let flight_stream = dispatch_to_worker(
                &task,
                &worker_url,
                pool.as_deref(),
                &parent_cx,
                &worker_secret,
                ticket_signer.as_deref(),
                ticket_ttl,
                &limits,
                &dropped,
                metrics_registry.as_deref(),
            )
            .await?;
            let boxed: WorkerStream = Box::pin(flight_stream);
            Ok(boxed)
        })
    }
}

/// Dispatch a scan task to a single worker via Arrow Flight `do_get`.
///
/// Returns the `FlightRecordBatchStream` on success, or a typed
/// [`DispatchError`] otherwise. When `pool` is provided, the channel is
/// fetched (or built once and cached) from it; otherwise a fresh connect runs
/// per call bounded by `connect_timeout`.
///
/// The `rpc_timeout` bounds the `do_get` round-trip itself. A kernel-paused
/// worker that accepts the TCP connection but never replies on the gRPC
/// stream surfaces as `DeadlineExceeded` instead of stalling the coordinator's
/// query-semaphore permit indefinitely (issue #29).
/// gRPC settings for dispatching one fragment, identical across retries.
/// Bundled so `dispatch_to_worker` stays inside clippy's argument budget as
/// the message ceiling joins the two timeouts (#432).
#[derive(Debug, Clone, Copy)]
struct DispatchLimits {
    /// Caps TCP+TLS+HTTP/2 handshake. Issue #29.
    connect_timeout: std::time::Duration,
    /// Caps the `do_get` round-trip. Issue #29.
    rpc_timeout: std::time::Duration,
    /// Inbound gRPC message ceiling on this link, enforced on both ends.
    max_message_bytes: usize,
}

/// Serialize a ticket, dropping the pushed predicate if that is the only way
/// to stay under the link's gRPC message ceiling (#432).
///
/// The predicate is a pure optimization: the coordinator keeps the
/// authoritative `FilterExec` above `DistributedScanExec`, so a worker that
/// receives no predicate returns more rows, never wrong ones. Shipping an
/// oversized ticket instead buys `decoded message length too large` and a
/// failed fragment, so degrading here is strictly better.
///
/// Checking the encoded ticket rather than the predicate alone is deliberate.
/// A raw-byte budget on the `Expr` cannot see base64 expansion, the file-path
/// list, or the projection, and the earlier 3 MiB raw budget admitted tickets
/// that measured 8-9 MB on the wire.
///
/// With `metrics`, records the shipped ticket's size and the time spent
/// encoding it, both attempts included (#809).
fn encode_ticket_within_limit(
    task: &ScanTask,
    worker_url: &str,
    max_message_bytes: usize,
    ticket_predicate_dropped: &datafusion::physical_plan::metrics::Count,
    metrics: Option<&sqe_metrics::MetricsRegistry>,
) -> Result<Vec<u8>, DataFusionError> {
    let started = metrics.map(|_| std::time::Instant::now());
    let encoded = encode_ticket_unmeasured(
        task,
        worker_url,
        max_message_bytes,
        ticket_predicate_dropped,
    )?;
    if let (Some(m), Some(started)) = (metrics, started) {
        m.scan_ticket_encode_seconds
            .observe(started.elapsed().as_secs_f64());
        m.scan_ticket_bytes.observe(encoded.len() as f64);
    }
    Ok(encoded)
}

fn encode_ticket_unmeasured(
    task: &ScanTask,
    worker_url: &str,
    max_message_bytes: usize,
    ticket_predicate_dropped: &datafusion::physical_plan::metrics::Count,
) -> Result<Vec<u8>, DataFusionError> {
    let ticket_bytes = task
        .to_bytes()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let budget = max_message_bytes.saturating_sub(TICKET_FRAMING_HEADROOM_BYTES);
    if ticket_bytes.len() <= budget || task.predicate_proto.is_none() {
        return Ok(ticket_bytes);
    }

    let mut stripped = task.clone();
    stripped.predicate_proto = None;
    let fallback = stripped
        .to_bytes()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    ticket_predicate_dropped.add(1);
    warn!(
        fragment_id = %task.fragment_id,
        worker = %worker_url,
        ticket_bytes = ticket_bytes.len(),
        stripped_bytes = fallback.len(),
        max_message_bytes,
        "ticket exceeds the gRPC message ceiling; dropping the pushed predicate \
         and shipping the fragment unfiltered"
    );
    Ok(fallback)
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_to_worker(
    task: &ScanTask,
    worker_url: &str,
    pool: Option<&ChannelPool>,
    parent_cx: &opentelemetry::Context,
    worker_secret: &str,
    ticket_signer: Option<&sqe_core::ticket_signing::TicketSigner>,
    ticket_ttl: std::time::Duration,
    limits: &DispatchLimits,
    ticket_predicate_dropped: &datafusion::physical_plan::metrics::Count,
    metrics: Option<&sqe_metrics::MetricsRegistry>,
) -> Result<FlightRecordBatchStream, DispatchError> {
    // Finding 601: bind the ticket to this worker and to a deadline before
    // signing, so a captured ticket cannot be replayed elsewhere or later.
    let mut task = task.clone();
    task.audience = worker_url.to_string();
    task.expires_at = chrono::Utc::now().timestamp() + ticket_ttl.as_secs() as i64;
    let task = &task;
    let ticket_bytes = encode_ticket_within_limit(
        task,
        worker_url,
        limits.max_message_bytes,
        ticket_predicate_dropped,
        metrics,
    )
    .map_err(DispatchError::Ticket)?;

    let channel = match pool {
        Some(pool) => pool.get(worker_url).await.map_err(|e| {
            pool.invalidate(worker_url);
            DispatchError::Transport(format!("Failed to connect to worker {worker_url}: {e}"))
        })?,
        // No pool: build a one-off channel. The HTTP/2 windows come from
        // `GrpcTransportConfig::default()` rather than the deployment's
        // config, which is not threaded this deep. Production always passes a
        // pool, so this is the test/local path; the defaults are the tuned
        // values, never tonic's 64 KB.
        None => crate::transport::apply_grpc_transport_endpoint(
            tonic::transport::Endpoint::new(worker_url.to_string()).map_err(|e| {
                DispatchError::Transport(format!(
                    "Failed to create endpoint for worker {worker_url}: {e}"
                ))
            })?,
            &sqe_core::config::GrpcTransportConfig::default(),
        )
        .connect_timeout(limits.connect_timeout)
        .timeout(limits.rpc_timeout)
        .connect()
        .await
        .map_err(|e| {
            DispatchError::Transport(format!("Failed to connect to worker {worker_url}: {e}"))
        })?,
    };
    // Match the worker's own inbound ceiling on the response direction: a
    // large Flight batch coming back must not die on tonic's 4 MiB default.
    let mut client =
        FlightServiceClient::new(channel).max_decoding_message_size(limits.max_message_bytes);

    // Sign the exact ticket bytes (#206) before they are moved into the Ticket.
    // Signing the wire bytes rather than a re-serialized struct guarantees the
    // tag covers precisely what the worker decodes: file paths, credentials,
    // and the #233 predicate and limit fields, with no canonicalization gap.
    let scan_signature = match ticket_signer {
        Some(signer) => Some(signer.sign(&ticket_bytes)),
        None => sign_ticket(worker_secret, &ticket_bytes),
    };

    let ticket = Ticket::new(ticket_bytes);
    let mut request = tonic::Request::new(ticket);

    if !worker_secret.is_empty() {
        let value = worker_secret.parse().map_err(|e| {
            DispatchError::Ticket(DataFusionError::Execution(format!(
                "worker_secret cannot be encoded as a metadata header value: {e}"
            )))
        })?;
        request.metadata_mut().insert(WORKER_SECRET_HEADER, value);

        if let Some(sig) = scan_signature {
            let sig_value = sig.parse().map_err(|e| {
                DispatchError::Ticket(DataFusionError::Execution(format!(
                    "scan signature cannot be encoded as a metadata header value: {e}"
                )))
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
    let response = tokio::time::timeout(limits.rpc_timeout, client.do_get(request))
        .await
        .map_err(|_| {
            if let Some(pool) = pool {
                pool.invalidate(worker_url);
            }
            DispatchError::Transport(format!(
                "Worker {worker_url} do_get exceeded {}s rpc timeout",
                limits.rpc_timeout.as_secs()
            ))
        })?
        .map_err(|e| {
            // Transport-shaped codes drop the pooled channel; the Status
            // itself travels on so the retry loop can decode the fault
            // (finding 602) instead of reading a message.
            if let Some(pool) = pool {
                if matches!(
                    e.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) {
                    pool.invalidate(worker_url);
                }
            }
            DispatchError::Status(e)
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
/// - `outcome` — [`FragmentOutcome::Finished`] at EOF, `Failed` on the first
///   error, `Cancelled` when the stream is dropped before either
/// - `elapsed_ms` — wall-clock time from the start of `execute()` to completion
/// - `output_rows` — total number of rows produced across all batches
struct CallbackStream {
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    fragment_id: String,
    callback: Option<FragmentCallback>,
    start: std::time::Instant,
    output_rows: usize,
    /// Once-flag: whether the teardown (callback + credential unregister) has
    /// already fired. Shared by the poll path (EOF / error) and `Drop`
    /// (cancellation) so the callback -- and therefore the worker-load
    /// reservation release behind it -- runs exactly once. Atomic `swap` makes
    /// the claim unambiguous even though both paths hold `&mut self`.
    fired: std::sync::atomic::AtomicBool,
    /// COORD-02: credential tracker to unregister this fragment from on
    /// completion. The success path used to leak an entry per finished fragment
    /// (register on dispatch, unregister only on the all-attempts-failed path),
    /// growing the tracker map for the life of the process. Firing the
    /// unregister here -- on the same exactly-once teardown that fires the
    /// callback -- covers success and error completions.
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    /// Finding 602: the fault behind the terminal error, written by the
    /// fragment driver before it yields that error, so `Failed` carries it.
    last_fault: Arc<Mutex<Option<FragmentFault>>>,
}

impl CallbackStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        fragment_id: String,
        callback: Option<FragmentCallback>,
        start: std::time::Instant,
        credential_tracker: Option<Arc<CredentialRefreshTracker>>,
        last_fault: Arc<Mutex<Option<FragmentFault>>>,
    ) -> Self {
        Self {
            inner,
            fragment_id,
            callback,
            start,
            output_rows: 0,
            fired: std::sync::atomic::AtomicBool::new(false),
            credential_tracker,
            last_fault,
        }
    }

    /// The fault the driver recorded for the terminal error, `Unknown` when
    /// the error did not come through the driver.
    fn terminal_fault(&self) -> FragmentFault {
        self.last_fault
            .lock()
            .ok()
            .and_then(|slot| *slot)
            .unwrap_or(FragmentFault::Unknown)
    }

    /// Claim the exactly-once teardown. Returns `true` for the first caller
    /// only.
    fn claim_teardown(&self) -> bool {
        !self.fired.swap(true, std::sync::atomic::Ordering::AcqRel)
    }

    /// Run the exactly-once teardown for `outcome`: fire the fragment callback
    /// (which releases the worker-load reservation on the dispatcher side) and
    /// unregister the credential-tracker entry (COORD-02). No-op after the
    /// first call.
    fn finish(&self, outcome: FragmentOutcome) {
        if !self.claim_teardown() {
            return;
        }
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        if let Some(cb) = &self.callback {
            cb(&self.fragment_id, outcome, elapsed_ms, self.output_rows);
        }
        self.unregister_credentials();
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
                // Stream exhausted: callback with Finished, release the
                // credential-tracker entry (COORD-02).
                self.finish(FragmentOutcome::Finished);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                // Stream produced an error: callback with Failed (carrying the
                // driver's fault, finding 602), release the credential-tracker
                // entry (COORD-02).
                let fault = self.terminal_fault();
                self.finish(FragmentOutcome::Failed(fault));
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
    /// Finding 408: the callback fires too, with the distinct
    /// [`FragmentOutcome::Cancelled`], so the dispatcher releases the
    /// worker-load reservation as soon as the stream is dropped instead of
    /// holding it until the plan `Arc` goes away. The callback used to be
    /// skipped here because its only non-success outcome was `Failed`, which
    /// would have mislabelled a clean stop; the tri-state outcome removes that
    /// reason. The `fired` once-flag keeps exactly-once semantics with the
    /// poll path, so a stream that already reached EOF or an error does not
    /// report a second outcome (double-release) from `Drop`.
    fn drop(&mut self) {
        self.finish(FragmentOutcome::Cancelled);
    }
}

/// Pass-through node recording that an Iceberg scan did NOT distribute, and
/// why (finding 402).
///
/// The Hive path has had this since cycle 5 (`HiveLocalScanExec`): a silent
/// fallback is indistinguishable from success, so the policy decision that
/// kept a scan on the coordinator becomes a plan node whose `Display` carries
/// the reason. It changes nothing about the data: `execute` delegates to the
/// wrapped scan and the properties are the scan's own. Only used when a worker
/// registry exists; a single-node coordinator has nothing to explain.
#[derive(Debug)]
pub struct CoordinatorLocalScanExec {
    input: Arc<dyn ExecutionPlan>,
    reason: String,
}

impl CoordinatorLocalScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, reason: impl Into<String>) -> Self {
        Self {
            input,
            reason: reason.into(),
        }
    }

    /// Why the scan stayed on the coordinator (the `scheduler_decisions`
    /// `reason` label value). Production reads it through `DisplayAs`.
    #[cfg(test)]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl DisplayAs for CoordinatorLocalScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            // The reason belongs in the DEFAULT format: someone asking why the
            // cluster is idle runs plain EXPLAIN.
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CoordinatorLocalScanExec: coordinator-local, reason={}",
                    self.reason
                )
            }
            DisplayFormatType::TreeRender => write!(f, "CoordinatorLocalScanExec"),
        }
    }
}

impl ExecutionPlan for CoordinatorLocalScanExec {
    fn name(&self) -> &str {
        "CoordinatorLocalScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Owns no expressions; the wrapped scan is a child and is visited in its
    /// own right.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Keep the reason across a rebuild; dropping it would make the marker
        // vanish the first time a rule rebuilt the plan.
        let child = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal(
                "CoordinatorLocalScanExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(CoordinatorLocalScanExec::new(
            child,
            self.reason.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    // No `metrics()`: the wrapped scan is a child and reports its own. Passing
    // them through counted every byte twice in plan walks and EXPLAIN ANALYZE.
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
            let cols: Vec<Arc<dyn arrow_array::Array>> = names
                .iter()
                .enumerate()
                .map(|(i, _)| col(i as i64))
                .collect();
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
            let c = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
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
        ScanTask::builder(
            id,
            vec![],
            sqe_planner::ScanStorage {
                s3_allow_http: true,
                ..Default::default()
            },
        )
        .build()
    }

    /// An oversized ticket must lose its predicate, not its fragment. The
    /// worker enforces the same ceiling, so sending it buys "decoded message
    /// length too large"; shipping unfiltered is correct because the
    /// coordinator's FilterExec stays above the scan (#432).
    #[test]
    fn oversized_ticket_sheds_its_predicate_and_still_dispatches() {
        let metrics = ExecutionPlanMetricsSet::new();
        let dropped = MetricBuilder::new(&metrics).counter("ticket_predicate_dropped_oversize", 0);

        let mut task = make_task("frag-big");
        task.predicate_proto = Some(vec![7u8; 256 * 1024]);
        let limit = 64 * 1024;

        let registry = sqe_metrics::MetricsRegistry::new().unwrap();
        let bytes =
            encode_ticket_within_limit(&task, "http://w1:50052", limit, &dropped, Some(&registry))
                .unwrap();

        assert_eq!(dropped.value(), 1, "the drop must be counted, not silent");
        // #809: the shipped (stripped) ticket is what gets measured, once.
        assert_eq!(registry.scan_ticket_bytes.get_sample_count(), 1);
        assert_eq!(
            registry.scan_ticket_bytes.get_sample_sum(),
            bytes.len() as f64
        );
        assert_eq!(registry.scan_ticket_encode_seconds.get_sample_count(), 1);
        assert!(
            bytes.len() <= limit,
            "stripped ticket is {} bytes, still over the {limit} ceiling",
            bytes.len()
        );
        let decoded = ScanTask::from_bytes(&bytes).unwrap();
        assert!(decoded.predicate_proto.is_none());
        assert_eq!(
            decoded.fragment_id, "frag-big",
            "the fragment itself must survive"
        );
    }

    /// The common case is untouched: a ticket under the ceiling keeps its
    /// predicate and increments nothing.
    #[test]
    fn ticket_within_the_ceiling_keeps_its_predicate() {
        let metrics = ExecutionPlanMetricsSet::new();
        let dropped = MetricBuilder::new(&metrics).counter("ticket_predicate_dropped_oversize", 0);

        let mut task = make_task("frag-small");
        task.predicate_proto = Some(vec![3u8; 512]);

        let registry = sqe_metrics::MetricsRegistry::new().unwrap();
        let bytes = encode_ticket_within_limit(
            &task,
            "http://w1:50052",
            16 * 1024 * 1024,
            &dropped,
            Some(&registry),
        )
        .unwrap();

        assert_eq!(dropped.value(), 0);
        // #809: one sample per encoded ticket, sized as shipped.
        assert_eq!(registry.scan_ticket_bytes.get_sample_count(), 1);
        assert!(registry.scan_ticket_bytes.get_sample_sum() > 512.0);
        assert_eq!(
            registry.scan_ticket_bytes.get_sample_sum(),
            bytes.len() as f64
        );
        assert_eq!(registry.scan_ticket_encode_seconds.get_sample_count(), 1);
        assert_eq!(
            ScanTask::from_bytes(&bytes).unwrap().predicate_proto,
            Some(vec![3u8; 512])
        );
    }

    /// Without a registry the encode path records nothing and the ticket is
    /// byte-identical to the measured one (#809).
    #[test]
    fn ticket_encoding_without_metrics_is_unchanged() {
        let metrics = ExecutionPlanMetricsSet::new();
        let dropped = MetricBuilder::new(&metrics).counter("ticket_predicate_dropped_oversize", 0);

        let mut task = make_task("frag-plain");
        task.predicate_proto = Some(vec![3u8; 512]);

        let registry = sqe_metrics::MetricsRegistry::new().unwrap();
        let measured = encode_ticket_within_limit(
            &task,
            "http://w1:50052",
            16 * 1024 * 1024,
            &dropped,
            Some(&registry),
        )
        .unwrap();
        let plain =
            encode_ticket_within_limit(&task, "http://w1:50052", 16 * 1024 * 1024, &dropped, None)
                .unwrap();

        assert_eq!(measured, plain);
        assert_eq!(registry.scan_ticket_bytes.get_sample_count(), 1);
        assert_eq!(
            ScanTask::from_bytes(&plain).unwrap().predicate_proto,
            Some(vec![3u8; 512])
        );
    }

    /// A ticket over the ceiling with nothing to shed is passed through rather
    /// than failed here: the dispatch error is the honest signal, and the scan
    /// still has retry and local fallback below it.
    #[test]
    fn oversized_ticket_without_a_predicate_is_not_rewritten() {
        let metrics = ExecutionPlanMetricsSet::new();
        let dropped = MetricBuilder::new(&metrics).counter("ticket_predicate_dropped_oversize", 0);

        let mut task = make_task("frag-paths");
        task.data_file_paths = (0..2_000)
            .map(|i| format!("s3://bucket/very/long/prefix/that/repeats/file-{i}.parquet"))
            .collect();

        let bytes =
            encode_ticket_within_limit(&task, "http://w1:50052", 4 * 1024, &dropped, None).unwrap();

        assert_eq!(dropped.value(), 0);
        assert!(bytes.len() > 4 * 1024);
    }

    #[test]
    fn test_distributed_scan_exec_properties() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let task = ScanTask::builder(
            "frag-001",
            vec!["s3://bucket/file.parquet".to_string()],
            sqe_planner::ScanStorage {
                s3_allow_http: true,
                ..Default::default()
            },
        )
        .build();

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
        assert!(
            err_msg.contains("out of range"),
            "unexpected error: {err_msg}"
        );
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
            Arc::new(Mutex::new(None)),
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

        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::once(async {
                Err(DataFusionError::Execution("boom".into()))
            }));
        let mut stream = CallbackStream::new(
            inner,
            fragment_id.clone(),
            None,
            std::time::Instant::now(),
            Some(Arc::clone(&tracker)),
            Arc::new(Mutex::new(None)),
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
            Arc::new(Mutex::new(None)),
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

    /// Finding 408: dropping the stream before EOF fires the fragment callback
    /// exactly once with `Cancelled`, so the dispatcher's worker-load
    /// reservation is released at drop time (LIMIT / cancellation) rather than
    /// when the plan `Arc` finally goes away.
    #[tokio::test]
    async fn finding408_callback_stream_releases_reservation_on_drop() {
        use std::sync::Mutex;

        let load = crate::worker_registry::WorkerLoadTracker::new();
        let guard = load.reserve_load("http://w1:50052", 500);
        assert_eq!(load.in_flight_load("http://w1:50052"), 500);

        let reservations: Arc<dashmap::DashMap<String, crate::worker_registry::ReservationGuard>> =
            Arc::new(dashmap::DashMap::new());
        reservations.insert("frag-408".to_string(), guard);

        let outcomes: Arc<Mutex<Vec<FragmentOutcome>>> = Arc::new(Mutex::new(Vec::new()));
        let cb: FragmentCallback = {
            let reservations = Arc::clone(&reservations);
            let outcomes = Arc::clone(&outcomes);
            Arc::new(move |task_id, outcome, _elapsed, _rows| {
                let _ = reservations.remove(task_id);
                outcomes.lock().unwrap().push(outcome);
            })
        };

        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::pending());
        let stream = CallbackStream::new(
            inner,
            "frag-408".to_string(),
            Some(cb),
            std::time::Instant::now(),
            None,
            Arc::new(Mutex::new(None)),
        );
        drop(stream);

        assert_eq!(*outcomes.lock().unwrap(), vec![FragmentOutcome::Cancelled]);
        assert!(
            reservations.is_empty(),
            "reservation guard must be removed on drop"
        );
        assert_eq!(
            load.in_flight_load("http://w1:50052"),
            0,
            "worker load reserved for the fragment must be released when the stream drops"
        );
    }

    /// Finding 408: a stream that already completed (EOF) must not report a
    /// second outcome from `Drop` -- the once-flag protects against a
    /// double-release of the reservation.
    #[tokio::test]
    async fn finding408_callback_fires_once_when_eof_then_dropped() {
        use std::sync::Mutex;

        let outcomes: Arc<Mutex<Vec<FragmentOutcome>>> = Arc::new(Mutex::new(Vec::new()));
        let cb: FragmentCallback = {
            let outcomes = Arc::clone(&outcomes);
            Arc::new(move |_task_id, outcome, _elapsed, _rows| {
                outcomes.lock().unwrap().push(outcome);
            })
        };
        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            Box::pin(futures::stream::empty());
        let mut stream = CallbackStream::new(
            inner,
            "frag-408-eof".to_string(),
            Some(cb),
            std::time::Instant::now(),
            None,
            Arc::new(Mutex::new(None)),
        );
        assert!(stream.next().await.is_none());
        drop(stream);

        assert_eq!(*outcomes.lock().unwrap(), vec![FragmentOutcome::Finished]);
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
        assert!(
            buf.contains("max_retries=3"),
            "display should contain max_retries: {buf}"
        );
    }

    /// Helper to use DisplayAs in tests.
    struct DisplayWrapper<'a>(&'a DistributedScanExec);
    impl fmt::Display for DisplayWrapper<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_as(DisplayFormatType::Default, f)
        }
    }

    // ---- Retry / fault-policy tests (findings 402 and 602) ----

    use arrow_array::Int64Array;
    use std::collections::{HashMap, VecDeque};

    /// What one scripted dispatch does.
    #[derive(Debug)]
    enum Scripted {
        /// `do_get` itself fails.
        Refuse(DispatchError),
        /// `do_get` succeeds and the stream yields these items in order.
        Stream(Vec<Result<RecordBatch, FlightError>>),
    }

    /// The mock worker: a per-URL queue of outcomes, consumed one per
    /// dispatch, plus the order in which workers were asked.
    #[derive(Debug, Default)]
    struct ScriptedDispatcher {
        scripts: Mutex<HashMap<String, VecDeque<Scripted>>>,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedDispatcher {
        fn script(self, worker: &str, outcome: Scripted) -> Self {
            self.scripts
                .lock()
                .unwrap()
                .entry(worker.to_string())
                .or_default()
                .push_back(outcome);
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl FragmentDispatcher for ScriptedDispatcher {
        fn dispatch(
            &self,
            _task: ScanTask,
            worker_url: String,
        ) -> Pin<Box<dyn Future<Output = Result<WorkerStream, DispatchError>> + Send>> {
            self.calls.lock().unwrap().push(worker_url.clone());
            let next = self
                .scripts
                .lock()
                .unwrap()
                .get_mut(&worker_url)
                .and_then(|q| q.pop_front());
            Box::pin(async move {
                match next {
                    Some(Scripted::Refuse(e)) => Err(e),
                    Some(Scripted::Stream(items)) => {
                        let s: WorkerStream = Box::pin(futures::stream::iter(items));
                        Ok(s)
                    }
                    None => Err(DispatchError::Transport(format!(
                        "no scripted outcome left for {worker_url}"
                    ))),
                }
            })
        }
    }

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn one_row(v: i64) -> RecordBatch {
        RecordBatch::try_new(id_schema(), vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn tonic_err(status: tonic::Status) -> Result<RecordBatch, FlightError> {
        Err(FlightError::Tonic(Box::new(status)))
    }

    async fn two_healthy_workers() -> Arc<WorkerRegistry> {
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        registry.mark_healthy("http://w1:50052").await;
        registry.mark_healthy("http://w2:50052").await;
        registry
    }

    /// Build a one-partition exec targeting w1 with the scripted mock worker
    /// and a callback recorder.
    fn scripted_exec(
        registry: &Arc<WorkerRegistry>,
        dispatcher: &Arc<ScriptedDispatcher>,
        max_retries: u32,
        outcomes: &Arc<Mutex<Vec<FragmentOutcome>>>,
    ) -> DistributedScanExec {
        let cb: FragmentCallback = {
            let outcomes = Arc::clone(outcomes);
            Arc::new(move |_id, outcome, _ms, _rows| outcomes.lock().unwrap().push(outcome))
        };
        DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            id_schema(),
        )
        .with_worker_registry(Arc::clone(registry))
        .with_max_retries(max_retries)
        .with_dispatcher(Arc::clone(dispatcher) as Arc<dyn FragmentDispatcher>)
        .with_fragment_callback(cb)
    }

    async fn collect(exec: &DistributedScanExec) -> Vec<DFResult<RecordBatch>> {
        exec.execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .collect()
            .await
    }

    fn sqe_code(err: &DataFusionError) -> sqe_core::SqeErrorCode {
        // Same conversion the query path applies before auditing.
        let cloned = DataFusionError::Execution(err.to_string());
        match err {
            DataFusionError::External(inner) => inner
                .downcast_ref::<SqeError>()
                .map(|e| e.error_code())
                .unwrap_or_else(|| SqeError::from(cloned).error_code()),
            _ => SqeError::from(cloned).error_code(),
        }
    }

    /// 402 acceptance: a stream that errors after ZERO batches is re-dispatched
    /// to a second healthy worker and the query succeeds; the first worker is
    /// marked unhealthy (the fault was `worker_internal`).
    #[tokio::test]
    async fn midstream_error_before_first_batch_retries_on_second_worker() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Stream(vec![tonic_err(tonic::Status::internal("pod evicted"))]),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(7))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 1, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 1, "{results:?}");
        let batch = results[0].as_ref().expect("retry on w2 must succeed");
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            7
        );
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052", "http://w2:50052"]);
        assert_eq!(
            registry.healthy_workers().await,
            vec!["http://w2:50052"],
            "w1 evicted, w2 kept"
        );
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![
                FragmentOutcome::Retrying(FragmentFault::Worker(WorkerFault::WorkerInternal)),
                FragmentOutcome::Finished,
            ]
        );
    }

    /// 402 acceptance: a stream that errors after ONE batch cannot be retried
    /// (the consumer holds a prefix): the query fails, the worker is still
    /// marked unhealthy, and the callback fires `Failed` with the fault.
    #[tokio::test]
    async fn midstream_error_after_one_batch_fails_query_and_marks_worker_unhealthy() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Stream(vec![
                        Ok(one_row(1)),
                        tonic_err(tonic::Status::internal("pod evicted")),
                    ]),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(2))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 2, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 2, "{results:?}");
        assert!(results[0].is_ok());
        assert!(results[1].is_err(), "query must fail, not retry with a prefix out");
        assert_eq!(
            dispatcher.calls(),
            vec!["http://w1:50052"],
            "no re-dispatch after batches were yielded"
        );
        assert_eq!(registry.healthy_workers().await, vec!["http://w2:50052"]);
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![FragmentOutcome::Failed(FragmentFault::Worker(
                WorkerFault::WorkerInternal
            ))]
        );
    }

    /// 602 acceptance: `PermissionDenied` is the user's problem. The registry
    /// is untouched, nothing is retried, and the error carries
    /// `SqeErrorCode::AccessDenied`.
    #[tokio::test]
    async fn permission_denied_leaves_registry_unchanged_and_yields_access_denied() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Status(tonic::Status::permission_denied(
                        "S3 AccessDenied: token expired",
                    ))),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(2))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 2, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 1);
        let err = results[0].as_ref().expect_err("must fail");
        assert_eq!(sqe_code(err), sqe_core::SqeErrorCode::AccessDenied, "{err}");
        assert_eq!(
            SqeError::from(DataFusionError::External(Box::new(SqeError::with_code(
                sqe_core::SqeErrorCode::AccessDenied,
                "x"
            ))))
            .error_code(),
            sqe_core::SqeErrorCode::AccessDenied,
            "the query path's From<DataFusionError> must keep the fault code for the audit record"
        );
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052"], "no retry");
        let mut healthy = registry.healthy_workers().await;
        healthy.sort();
        assert_eq!(
            healthy,
            vec!["http://w1:50052", "http://w2:50052"],
            "a user fault must not evict a worker"
        );
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![FragmentOutcome::Failed(FragmentFault::Worker(
                WorkerFault::PrincipalDenied
            ))]
        );
    }

    /// 602 acceptance: `ResourceExhausted` from w1 retries on w2 and marks w1
    /// unhealthy. Uses the lossless `x-sqe-fault` header encoding.
    #[tokio::test]
    async fn resource_exhausted_retries_elsewhere_and_marks_worker_unhealthy() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Status(
                        WorkerFault::WorkerResourceExhausted.into_status("memory pool full"),
                    )),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(2))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 1, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok(), "{results:?}");
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052", "http://w2:50052"]);
        assert_eq!(registry.healthy_workers().await, vec!["http://w2:50052"]);
        assert_eq!(
            outcomes.lock().unwrap()[0],
            FragmentOutcome::Retrying(FragmentFault::Worker(
                WorkerFault::WorkerResourceExhausted
            ))
        );
    }

    /// 602: `SourceThrottled` backs off and retries the SAME worker; moving it
    /// would only move the pressure. Registry untouched.
    #[tokio::test]
    async fn source_throttled_retries_same_worker_without_touching_registry() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Status(
                        WorkerFault::SourceThrottled.into_status("SlowDown"),
                    )),
                )
                .script("http://w1:50052", Scripted::Stream(vec![Ok(one_row(1))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 1, &outcomes);

        let results = collect(&exec).await;
        assert!(results.len() == 1 && results[0].is_ok(), "{results:?}");
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052", "http://w1:50052"]);
        assert_eq!(registry.healthy_workers().await.len(), 2);
    }

    /// 602: `TaskInvalid` (bad ticket) fails fast: no retry, no eviction, the
    /// error is `InvalidArguments`.
    #[tokio::test]
    async fn task_invalid_fails_fast_without_retry() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Status(tonic::Status::invalid_argument(
                        "unsupported ScanTask version 9",
                    ))),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(2))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 2, &outcomes);

        let results = collect(&exec).await;
        let err = results[0].as_ref().expect_err("must fail");
        assert_eq!(sqe_code(err), sqe_core::SqeErrorCode::InvalidArguments);
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052"]);
        assert_eq!(registry.healthy_workers().await.len(), 2);
    }

    /// Retries exhausted on worker faults: the last error surfaces, both
    /// workers are evicted, and the callback reports `Retrying` then `Failed`.
    #[tokio::test]
    async fn retries_exhausted_surfaces_error_and_callback_reports_failed() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Transport("connection refused".into())),
                )
                .script(
                    "http://w2:50052",
                    Scripted::Refuse(DispatchError::Transport("connection refused".into())),
                ),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 1, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        assert!(registry.healthy_workers().await.is_empty());
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![
                FragmentOutcome::Retrying(FragmentFault::Transport),
                FragmentOutcome::Failed(FragmentFault::Transport),
            ]
        );
    }

    /// The transport fallback: `Unavailable` decodes to no fault and takes the
    /// evict-and-retry path (a dead connection is not a throttle).
    #[tokio::test]
    async fn unavailable_status_is_transport_not_throttle() {
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script(
                    "http://w1:50052",
                    Scripted::Refuse(DispatchError::Status(tonic::Status::unavailable(
                        "transport error",
                    ))),
                )
                .script("http://w2:50052", Scripted::Stream(vec![Ok(one_row(2))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 1, &outcomes);

        let results = collect(&exec).await;
        assert!(results[0].is_ok(), "{results:?}");
        assert_eq!(dispatcher.calls(), vec!["http://w1:50052", "http://w2:50052"]);
        assert_eq!(registry.healthy_workers().await, vec!["http://w2:50052"]);
    }

    /// 402: the local-reason marker prints its reason in the default EXPLAIN
    /// format and delegates everything else to the wrapped scan.
    #[test]
    fn coordinator_local_scan_exec_displays_reason() {
        use datafusion::physical_plan::displayable;
        let inner: Arc<dyn ExecutionPlan> = Arc::new(DistributedScanExec::new(
            vec![make_task("f1")],
            vec!["http://w1:50052".to_string()],
            id_schema(),
        ));
        let marked: Arc<dyn ExecutionPlan> = Arc::new(CoordinatorLocalScanExec::new(
            Arc::clone(&inner),
            "below_file_threshold",
        ));
        let text = format!("{}", displayable(marked.as_ref()).indent(true));
        assert!(
            text.contains("CoordinatorLocalScanExec: coordinator-local, reason=below_file_threshold"),
            "{text}"
        );
        assert_eq!(marked.children().len(), 1);
        assert_eq!(
            marked.output_partitioning().partition_count(),
            inner.output_partitioning().partition_count()
        );
        let rebuilt = Arc::clone(&marked)
            .replace_children(
                vec![inner],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        assert_eq!(
            rebuilt
                .downcast_ref::<CoordinatorLocalScanExec>()
                .unwrap()
                .reason(),
            "below_file_threshold",
            "reason survives a rebuild"
        );
    }

    #[tokio::test]
    async fn test_retry_marks_worker_unhealthy() {
        let registry = Arc::new(WorkerRegistry::new(vec!["http://w1:50052".to_string()]));
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
    async fn metrics_populated_after_execution() {
        // Drive a partition through a scripted worker that ships one row and
        // assert the exec's metrics carry the rows that flowed through the
        // stream, so passive profiles and EXPLAIN ANALYZE show real numbers
        // on the DistributedScanExec row.
        let registry = two_healthy_workers().await;
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script("http://w1:50052", Scripted::Stream(vec![Ok(one_row(42))])),
        );
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let exec = scripted_exec(&registry, &dispatcher, 0, &outcomes);

        let results = collect(&exec).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let metrics = exec
            .metrics()
            .expect("metrics must be Some after execution");
        assert_eq!(
            metrics.output_rows(),
            Some(1),
            "baseline metrics must record the one row"
        );
        assert!(
            metrics.elapsed_compute().is_some(),
            "elapsed_compute must be recorded"
        );
        assert_eq!(*outcomes.lock().unwrap(), vec![FragmentOutcome::Finished]);
    }

    #[tokio::test]
    async fn test_no_fallback_returns_error() {
        let registry = Arc::new(WorkerRegistry::new(vec!["http://w1:50052".to_string()]));
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

    /// Two partitions failing at once each surface their own error; there is
    /// no per-fragment local fallback to paper over a dead cluster.
    #[tokio::test]
    async fn test_concurrent_two_partitions_fail_independently() {
        let registry = two_healthy_workers().await;
        let refuse = || {
            Scripted::Refuse(DispatchError::Transport(
                "connection refused".to_string(),
            ))
        };
        let dispatcher = Arc::new(
            ScriptedDispatcher::default()
                .script("http://w1:50052", refuse())
                .script("http://w1:50052", refuse())
                .script("http://w2:50052", refuse())
                .script("http://w2:50052", refuse()),
        );
        let exec = Arc::new(
            DistributedScanExec::new(
                vec![make_task("f1"), make_task("f2")],
                vec!["http://w1:50052".to_string(), "http://w2:50052".to_string()],
                id_schema(),
            )
            .with_worker_registry(registry.clone())
            .with_max_retries(1)
            .with_dispatcher(Arc::clone(&dispatcher) as Arc<dyn FragmentDispatcher>),
        );
        let context = Arc::new(TaskContext::default());
        let (exec0, ctx0, exec1, ctx1) = (exec.clone(), context.clone(), exec.clone(), context);
        let (res0, res1) = tokio::join!(
            async move { exec0.execute(0, ctx0).unwrap().collect::<Vec<_>>().await },
            async move { exec1.execute(1, ctx1).unwrap().collect::<Vec<_>>().await }
        );
        assert!(res0.len() == 1 && res0[0].is_err(), "{res0:?}");
        assert!(res1.len() == 1 && res1[0].is_err(), "{res1:?}");
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn terminate_on_error_drops_trailing_oks() {
        // Construct a stream Ok, Ok, Err, Ok, Ok and assert the wrapper
        // surfaces Ok, Ok, Err only.
        use arrow_array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let s = schema.clone();
        let b1 =
            RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let b2 =
            RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![2]))]).unwrap();
        let b3 =
            RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![3]))]).unwrap();
        let b4 =
            RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![4]))]).unwrap();
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

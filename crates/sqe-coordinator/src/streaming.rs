//! End-to-end streaming execution path for SELECT queries.
//!
//! Wraps a DataFusion [`SendableRecordBatchStream`] in a per-query tracker
//! that finalizes metrics, the query history, audit log, and concurrency
//! permit when the stream is fully drained, errors, or is dropped by the
//! client. No [`RecordBatch`]es are buffered on the coordinator -- each
//! batch flows straight from DataFusion into the Flight encoder and onto
//! the gRPC wire.
//!
//! This is the piece that lets SQE survive very large result sets. The
//! old path accumulated every batch into a `Vec<RecordBatch>` before
//! returning, which lived outside the DataFusion memory pool and so could
//! not be spilled. For a 20M+ row result (e.g. TPC-E `trade_result` at
//! SF1) that buffer grew past `memory_limit` and the OS killed the
//! coordinator. With the streaming path the coordinator only ever holds
//! a handful of batches in flight, so the OOM risk disappears regardless
//! of result cardinality; spill-to-disk in DataFusion's operators takes
//! care of intermediate state.
//!
//! Ordering guarantees: the stream preserves DataFusion's per-partition
//! order. If the client requires a globally ordered result, the physical
//! plan must contain a `SortPreservingMergeExec` (or an `OrderBy`). This
//! module does not re-order batches.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan, RecordBatchStream};
use futures::Stream;
use sqe_core::ProfileMode;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::query_handler::{aggregate_spill_metrics, extract_plan_metrics};
use crate::query_tracker::QueryTracker;

/// Hard cap on a rendered profile. Deep plans with long predicate lists can
/// blow up the tree text; 64 KiB keeps the log event and the tracker record
/// bounded.
const MAX_PROFILE_BYTES: usize = 64 * 1024;

/// Render a passive query profile: a header (total elapsed, output rows,
/// unpushed-scan flag) followed by the executed plan tree with the
/// per-operator metrics DataFusion populated during normal execution.
fn render_query_profile(plan: &Arc<dyn ExecutionPlan>, elapsed_ms: u64, rows: usize) -> String {
    let tree = DisplayableExecutionPlan::with_metrics(plan.as_ref())
        .indent(true)
        .to_string();

    let mut unpushed = 0usize;
    let mut tables: Vec<String> = Vec::new();
    collect_unpushed_scans(plan, &mut unpushed, &mut tables);
    let tables_suffix = if tables.is_empty() {
        String::new()
    } else {
        format!(" unpushed_tables=[{}]", tables.join(", "))
    };

    truncate_profile(format!(
        "elapsed_ms={elapsed_ms} output_rows={rows} unpushed_scans={unpushed}{tables_suffix}\n{tree}"
    ))
}

/// Cap a rendered profile at [`MAX_PROFILE_BYTES`], cutting on a char
/// boundary so multibyte content cannot panic the truncation.
fn truncate_profile(mut profile: String) -> String {
    if profile.len() > MAX_PROFILE_BYTES {
        let mut cut = MAX_PROFILE_BYTES;
        while !profile.is_char_boundary(cut) {
            cut -= 1;
        }
        profile.truncate(cut);
        profile.push_str("\n... [profile truncated at 64 KiB]");
    }
    profile
}

/// Count scan nodes whose display carries an empty pushed-down predicate
/// (`IcebergScanExec` renders `predicate=[]` when nothing was pushed): each
/// one is a full table scan. Table names are scraped from the same display
/// line when present (`table=<name>,`).
fn collect_unpushed_scans(
    node: &Arc<dyn ExecutionPlan>,
    count: &mut usize,
    tables: &mut Vec<String>,
) {
    let line = displayable(node.as_ref()).one_line().to_string();
    if line.contains("predicate=[]") {
        *count += 1;
        if let Some(rest) = line.split("table=").nth(1) {
            if let Some(name) = rest.split(',').next() {
                tables.push(name.trim().to_string());
            }
        }
    }
    for child in node.children() {
        collect_unpushed_scans(child, count, tables);
    }
}

/// State captured at query start that [`TrackedRecordBatchStream`] uses
/// to finalize tracker / metrics / audit exactly once when the stream
/// terminates (clean EOF, error, or client drop).
///
/// All fields are either owned values or cheap `Arc` clones so the
/// finalizer can outlive the spawning request without holding a borrow
/// on `QueryHandler`.
pub struct StreamFinalizer {
    pub tracker: Arc<QueryTracker>,
    pub metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    pub audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    pub query_id: uuid::Uuid,
    pub username: String,
    pub session_id: String,
    pub sql: String,
    pub kind_name: String,
    pub plan: Arc<dyn ExecutionPlan>,
    pub runtime: Arc<RuntimeEnv>,
    pub start: Instant,
    pub slow_query_threshold_secs: u64,
    pub sql_length: usize,
    pub tables_touched: Vec<String>,
    /// Policy-decision summary from the enforcer for this query. Copied into the
    /// audit entry on stream finalization so a masked / filtered / denied SELECT
    /// (the primary Flight SQL path) records what policy did, not a bare
    /// `status:"success"` with zero rows.
    pub policy_summary: sqe_policy::PolicySummary,
    /// Passive profiling mode (`[query] query_profile`). When a profile is
    /// due, the executed plan tree is rendered with its populated metrics,
    /// logged under the `query_profile` target, and stored on the tracker
    /// record.
    pub profile_mode: ProfileMode,
    /// Identity of the authenticated user who issued this query. Carried into
    /// the canonical `AuditEvent` actor field so the SIEM record contains
    /// the full structured identity rather than a bare username string.
    pub actor: sqe_metrics::audit::Actor,
    /// Structured resource list extracted from the logical plan before it was
    /// handed to DataFusion for optimization. Each entry names a catalog
    /// table or view the query touches. Threaded here from `open_stream` (the
    /// only place the logical plan is in scope) so the streaming finalizer
    /// can emit a complete `AuditEvent` without re-planning.
    pub resources: Vec<sqe_metrics::audit::Resource>,
    /// Source IP of the Flight SQL client that submitted this query. Threaded
    /// from the `execute_stream` parameter so every streaming audit event
    /// (success, error, cancel) carries the originating address.
    pub client_ip: Option<String>,
}

impl StreamFinalizer {
    /// Map the policy decision summary into the canonical audit type.
    ///
    /// `PolicySummary` lives in `sqe-policy`; `PolicyAudit` lives in
    /// `sqe-metrics`. They carry the same fields but the crate boundary
    /// means we convert here rather than deriving `From` across the boundary.
    fn policy_summary_to_audit(&self) -> sqe_metrics::audit::PolicyAudit {
        sqe_metrics::audit::PolicyAudit {
            row_filters_applied: self.policy_summary.row_filters_applied,
            columns_masked: self.policy_summary.columns_masked.clone(),
            columns_restricted: self.policy_summary.columns_restricted.clone(),
            denied: self.policy_summary.denied,
        }
    }

    /// Render, log, and store the per-operator profile for this query.
    /// Called from the success and error finalization paths once the
    /// decision to profile has been made.
    fn capture_profile(&self, elapsed_ms: u64, rows: usize) {
        let profile = render_query_profile(&self.plan, elapsed_ms, rows);
        warn!(
            target: "query_profile",
            query_id = %self.query_id,
            elapsed_ms,
            "query profile\n{profile}"
        );
        self.tracker.set_profile(&self.query_id, profile);
    }

    fn record_spill_metrics(&self) {
        let Some(ref metrics) = self.metrics else {
            return;
        };
        let (sort_spill_count, sort_spill_bytes, join_spill_count, join_spill_bytes) =
            aggregate_spill_metrics(&self.plan);
        if sort_spill_count > 0 {
            metrics.sort_spill_count.inc_by(sort_spill_count as f64);
            metrics.sort_spill_bytes.inc_by(sort_spill_bytes as f64);
        }
        if join_spill_count > 0 {
            metrics.join_spill_count.inc_by(join_spill_count as f64);
            metrics.join_spill_bytes.inc_by(join_spill_bytes as f64);
        }
    }

    fn on_success(self, rows: usize) {
        let duration = self.start.elapsed();
        let execution_ms = duration.as_millis() as u64;

        let mut pm = extract_plan_metrics(&self.plan);
        pm.peak_memory_bytes = crate::memory::used_bytes(&self.runtime.memory_pool) as u64;

        self.tracker.complete(
            &self.query_id,
            rows,
            execution_ms,
            self.tables_touched.clone(),
            pm.bytes_scanned,
            pm.rows_scanned,
            pm.spill_bytes,
            pm.peak_memory_bytes,
        );

        if let Some(ref metrics) = self.metrics {
            metrics
                .query_count
                .with_label_values(&["success", &self.kind_name, ""])
                .inc();
            metrics
                .query_duration
                .with_label_values(&[&self.kind_name])
                .observe(duration.as_secs_f64());
            metrics.rows_returned.inc_by(rows as f64);
            if rows > 0 {
                // Streaming: duration still approximates total time to last
                // row, not time-to-first-row. A dedicated first-batch probe
                // can refine this later.
                metrics.time_to_first_row.observe(duration.as_secs_f64());
            }
        }
        self.record_spill_metrics();

        if let Some(ref audit) = self.audit {
            let policy = self.policy_summary_to_audit();
            let event = sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::Query,
                actor: self.actor.clone(),
                outcome: sqe_metrics::audit::Outcome::Success,
                resources: self.resources.clone(),
                policy: Some(policy),
                timing: Some(sqe_metrics::audit::Timing {
                    duration_ms: execution_ms,
                    ..Default::default()
                }),
                stats: Some(sqe_metrics::audit::QueryStats {
                    rows_returned: rows,
                    ..Default::default()
                }),
                query: Some(sqe_metrics::audit::QueryInfo {
                    text: Some(self.sql.clone()),
                    query_hash: sqe_metrics::audit::query_hash(&self.sql),
                    statement_type: "query".to_string(),
                }),
                session_id: Some(self.session_id.clone()),
                client_ip: self.client_ip.clone(),
                integrity: Default::default(),
            };
            audit.log_event(event);
        }

        let elapsed_secs = duration.as_secs();
        if self.slow_query_threshold_secs > 0 && elapsed_secs >= self.slow_query_threshold_secs {
            warn!(
                query_id = %self.query_id,
                username = %self.username,
                elapsed_secs,
                sql_length = self.sql_length,
                rows_returned = rows,
                "Slow streaming query detected"
            );
        }

        let profile_due = match self.profile_mode {
            ProfileMode::Off => false,
            ProfileMode::All => true,
            ProfileMode::Slow => {
                self.slow_query_threshold_secs > 0
                    && elapsed_secs >= self.slow_query_threshold_secs
            }
        };
        if profile_due {
            self.capture_profile(execution_ms, rows);
        }
    }

    fn on_error(self, rows: usize, err: &sqe_core::SqeError) {
        let duration = self.start.elapsed();
        self.tracker.failed(&self.query_id, err);

        if let Some(ref metrics) = self.metrics {
            metrics
                .query_count
                .with_label_values(&["error", &self.kind_name, err.error_code().name()])
                .inc();
            metrics
                .query_duration
                .with_label_values(&[&self.kind_name])
                .observe(duration.as_secs_f64());
        }
        self.record_spill_metrics();

        if let Some(ref audit) = self.audit {
            let policy = self.policy_summary_to_audit();
            let event = sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::Query,
                actor: self.actor.clone(),
                outcome: sqe_metrics::audit::Outcome::Failure {
                    error_type: Some(err.error_code().trino_error_type().to_string()),
                    error_code: Some(err.error_code().name().to_string()),
                    message: Some(err.client_message()),
                },
                resources: self.resources.clone(),
                policy: Some(policy),
                timing: Some(sqe_metrics::audit::Timing {
                    duration_ms: duration.as_millis() as u64,
                    ..Default::default()
                }),
                stats: Some(sqe_metrics::audit::QueryStats {
                    rows_returned: rows,
                    ..Default::default()
                }),
                query: Some(sqe_metrics::audit::QueryInfo {
                    text: Some(self.sql.clone()),
                    query_hash: sqe_metrics::audit::query_hash(&self.sql),
                    statement_type: "query".to_string(),
                }),
                session_id: Some(self.session_id.clone()),
                client_ip: self.client_ip.clone(),
                integrity: Default::default(),
            };
            audit.log_event(event);
        }

        // Failures always profile when the feature is on at all: a failed
        // query is exactly the case where per-operator evidence is wanted.
        if self.profile_mode != ProfileMode::Off {
            self.capture_profile(duration.as_millis() as u64, rows);
        }
    }

    fn on_cancel(self, rows: usize) {
        let duration = self.start.elapsed();
        self.tracker.canceled(&self.query_id);

        if let Some(ref metrics) = self.metrics {
            metrics
                .query_count
                .with_label_values(&["cancelled", &self.kind_name, "QUERY_CANCELLED"])
                .inc();
            metrics
                .query_duration
                .with_label_values(&[&self.kind_name])
                .observe(duration.as_secs_f64());
        }

        if let Some(ref audit) = self.audit {
            let policy = self.policy_summary_to_audit();
            let event = sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::Query,
                actor: self.actor.clone(),
                // Cancellation is a client-driven interruption, not a server
                // error. We map it to Failure with a dedicated error code so
                // SIEM tools can distinguish cancelled-by-client from
                // failed-with-error. The legacy path emitted status:"cancelled".
                outcome: sqe_metrics::audit::Outcome::Failure {
                    error_type: None,
                    error_code: Some("QUERY_CANCELLED".to_string()),
                    message: Some("Query was cancelled by the client".to_string()),
                },
                resources: self.resources.clone(),
                policy: Some(policy),
                timing: Some(sqe_metrics::audit::Timing {
                    duration_ms: duration.as_millis() as u64,
                    ..Default::default()
                }),
                stats: Some(sqe_metrics::audit::QueryStats {
                    rows_returned: rows,
                    ..Default::default()
                }),
                query: Some(sqe_metrics::audit::QueryInfo {
                    text: Some(self.sql.clone()),
                    query_hash: sqe_metrics::audit::query_hash(&self.sql),
                    statement_type: "query".to_string(),
                }),
                session_id: Some(self.session_id.clone()),
                client_ip: self.client_ip.clone(),
                integrity: Default::default(),
            };
            audit.log_event(event);
        }
    }
}

/// Wraps a DataFusion record batch stream so the coordinator can observe
/// completion and release per-query resources.
///
/// The wrapper never buffers more than the one batch currently in flight.
/// When the underlying stream terminates (either cleanly, with an error,
/// or because the client dropped the Flight response) the attached
/// [`StreamFinalizer`] is consumed exactly once to record the outcome.
///
/// A held [`OwnedSemaphorePermit`] keeps the concurrency counter honest:
/// the permit is released on drop, which happens after the whole stream
/// has been consumed, so long-running streams continue to count against
/// `max_concurrent_queries` for their full lifetime.
pub struct TrackedRecordBatchStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    finalizer: Option<StreamFinalizer>,
    rows_so_far: usize,
    /// Concurrency permits held for the lifetime of the stream. Stored as a
    /// vec so the streaming path can carry both a per-user permit and a
    /// global permit without duplicating fields; both drop when the stream
    /// drops, releasing the slots back to their respective semaphores.
    _permits: Vec<OwnedSemaphorePermit>,
    /// Opaque teardown handle whose Drop runs when the stream completes
    /// (clean EOF, error, or client cancel). Used by time-travel pinned
    /// providers (#44) to deregister the session-context alias once the
    /// query finishes, so subsequent SQL in the same session sees HEAD.
    _teardown: Option<Box<dyn std::any::Any + Send>>,
    /// Per-user memory reservation released when the stream drops.
    _per_user_reservation: Option<crate::memory::PerUserReservation>,
    cancel_token: Option<CancellationToken>,
    /// Set once the cancel token has fired so subsequent polls short-circuit
    /// without re-running the finalizer.
    cancelled: bool,
    /// Maximum time the stream may sit without producing a batch before it
    /// is aborted and its concurrency permit is released. None disables the
    /// guard. Without this an idle gRPC client can pin every slot in
    /// `max_concurrent_queries` by holding open Flight streams without
    /// draining them. Issue #75.
    idle_timeout: Option<Duration>,
    /// Pending idle deadline. Pinned so it can be polled in place each call.
    idle_sleep: Option<Pin<Box<Sleep>>>,
}

impl TrackedRecordBatchStream {
    /// Construct a new tracked stream. The schema is read from the inner
    /// stream so callers cannot accidentally supply a mismatched one.
    pub fn new(
        inner: SendableRecordBatchStream,
        finalizer: StreamFinalizer,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            finalizer: Some(finalizer),
            rows_so_far: 0,
            _permits: permit.into_iter().collect(),
            _teardown: None,
            _per_user_reservation: None,
            cancel_token: None,
            cancelled: false,
            idle_timeout: None,
            idle_sleep: None,
        }
    }

    /// Construct a tracked stream that also observes a cancellation token.
    /// When the token fires, `poll_next` returns `None` after invoking the
    /// finalizer's `on_cancel` path exactly once.
    pub fn with_cancel_token(
        inner: SendableRecordBatchStream,
        finalizer: StreamFinalizer,
        permit: Option<OwnedSemaphorePermit>,
        cancel_token: CancellationToken,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            finalizer: Some(finalizer),
            rows_so_far: 0,
            _permits: permit.into_iter().collect(),
            _teardown: None,
            _per_user_reservation: None,
            cancel_token: Some(cancel_token),
            cancelled: false,
            idle_timeout: None,
            idle_sleep: None,
        }
    }

    /// Construct a tracked stream carrying multiple permits (per-user and
    /// global). Useful when admission control involves layered semaphores.
    pub fn with_permits_and_cancel_token(
        inner: SendableRecordBatchStream,
        finalizer: StreamFinalizer,
        permits: Vec<OwnedSemaphorePermit>,
        cancel_token: CancellationToken,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            finalizer: Some(finalizer),
            rows_so_far: 0,
            _permits: permits,
            _teardown: None,
            _per_user_reservation: None,
            cancel_token: Some(cancel_token),
            cancelled: false,
            idle_timeout: None,
            idle_sleep: None,
        }
    }

    /// Variant that also carries a per-user memory reservation. The
    /// reservation is released when the stream drops, freeing the user's
    /// share of the per-user memory budget.
    pub fn with_permits_reservation_and_cancel_token(
        inner: SendableRecordBatchStream,
        finalizer: StreamFinalizer,
        permits: Vec<OwnedSemaphorePermit>,
        reservation: Option<crate::memory::PerUserReservation>,
        cancel_token: CancellationToken,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            finalizer: Some(finalizer),
            rows_so_far: 0,
            _permits: permits,
            _teardown: None,
            _per_user_reservation: reservation,
            cancel_token: Some(cancel_token),
            cancelled: false,
            idle_timeout: None,
            idle_sleep: None,
        }
    }

    /// Attach an opaque teardown handle. The handle's Drop runs when this
    /// stream is dropped (after EOF, error, or cancel). The handle type is
    /// erased so this module stays free of dependencies on individual
    /// cleanup guards.
    pub fn with_teardown<T: std::any::Any + Send + 'static>(mut self, t: T) -> Self {
        self._teardown = Some(Box::new(t));
        self
    }

    /// Enable the idle-timeout guard. The stream aborts (releasing its
    /// concurrency permit) when no batch is produced for `timeout`. Skips
    /// installation when `timeout` is zero. Issue #75.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        if !timeout.is_zero() {
            self.idle_timeout = Some(timeout);
            self.idle_sleep = Some(Box::pin(tokio::time::sleep(timeout)));
        }
        self
    }

    fn reset_idle_timer(&mut self) {
        if let (Some(timeout), Some(ref mut sleep)) = (self.idle_timeout, self.idle_sleep.as_mut())
        {
            sleep.as_mut().reset(tokio::time::Instant::now() + timeout);
        }
    }
}

impl Stream for TrackedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.cancelled {
            return Poll::Ready(None);
        }
        if let Some(ref token) = self.cancel_token {
            if token.is_cancelled() {
                self.cancelled = true;
                if let Some(f) = self.finalizer.take() {
                    f.on_cancel(self.rows_so_far);
                }
                return Poll::Ready(None);
            }
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.rows_so_far = self.rows_so_far.saturating_add(batch.num_rows());
                self.reset_idle_timer();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(f) = self.finalizer.take() {
                    let sqe_err = sqe_core::SqeError::Execution(format!(
                        "Query execution failed: {e}"
                    ));
                    f.on_error(self.rows_so_far, &sqe_err);
                }
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                if let Some(f) = self.finalizer.take() {
                    f.on_success(self.rows_so_far);
                }
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Idle-timeout guard. When no batch has been produced within
                // `idle_timeout` (stalled execution pipeline, or a client that
                // stopped draining), abort the stream so the held semaphore
                // permit cannot be pinned indefinitely. Issue #75.
                //
                // The abort must surface as an ERROR, not as end-of-stream:
                // ending with `Ready(None)` makes a stalled query
                // indistinguishable from a legitimately empty result. A
                // TPC-DS run recorded a stalled query as "0 rows, no error"
                // and the diff harness scored it as a row-count mismatch
                // instead of a failure (Trino reports the equivalent as
                // EXCEEDED_TIME_LIMIT).
                if let Some(ref mut sleep) = self.idle_sleep {
                    if sleep.as_mut().poll(cx).is_ready() {
                        warn!(
                            rows_so_far = self.rows_so_far,
                            "Stream idle-timeout reached, aborting query and releasing permits"
                        );
                        self.cancelled = true;
                        let timeout = self.idle_timeout.unwrap_or_default();
                        let msg = format!(
                            "Query aborted: produced no results for {}s (idle timeout). \
                             The execution pipeline stalled or the client stopped \
                             consuming the result stream.",
                            timeout.as_secs()
                        );
                        if let Some(f) = self.finalizer.take() {
                            f.on_error(
                                self.rows_so_far,
                                &sqe_core::SqeError::Execution(msg.clone()),
                            );
                        }
                        return Poll::Ready(Some(Err(DataFusionError::Execution(msg))));
                    }
                }
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for TrackedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for TrackedRecordBatchStream {
    fn drop(&mut self) {
        // Client dropped the response or the task panicked before the
        // stream was drained. Mark the query cancelled so the tracker
        // doesn't list it as still-running forever.
        if let Some(f) = self.finalizer.take() {
            f.on_cancel(self.rows_so_far);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use sqe_core::QueryHistoryConfig;

    use crate::query_tracker::QueryState;

    fn sample_batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let arr = Int64Array::from_iter_values(0..n);
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn test_tracker() -> Arc<QueryTracker> {
        Arc::new(QueryTracker::new(&QueryHistoryConfig {
            max_entries: 128,
            ttl_secs: 60,
        }))
    }

    fn test_finalizer(
        tracker: Arc<QueryTracker>,
        plan: Arc<dyn ExecutionPlan>,
        runtime: Arc<RuntimeEnv>,
    ) -> StreamFinalizer {
        StreamFinalizer {
            tracker,
            metrics: None,
            audit: None,
            query_id: uuid::Uuid::now_v7(),
            username: "test-user".to_string(),
            session_id: "test-session".to_string(),
            sql: "SELECT 1".to_string(),
            kind_name: "Query".to_string(),
            plan,
            runtime,
            start: Instant::now(),
            slow_query_threshold_secs: 0,
            sql_length: 8,
            tables_touched: Vec::new(),
            policy_summary: sqe_policy::PolicySummary::default(),
            profile_mode: ProfileMode::Off,
            actor: sqe_metrics::audit::Actor::default(),
            resources: Vec::new(),
            client_ip: None,
        }
    }

    fn fixed_stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let s = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(schema, s))
    }

    /// `extract_plan_metrics` walks a real execution plan tree; building
    /// one via `SessionContext` is the simplest way to get an
    /// `Arc<dyn ExecutionPlan>` without hand-crafting operators.
    async fn trivial_plan() -> (Arc<dyn ExecutionPlan>, SchemaRef, Arc<RuntimeEnv>) {
        let ctx = SessionContext::new();
        let df = ctx.sql("SELECT 1 AS x").await.unwrap();
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let plan = df.create_physical_plan().await.unwrap();
        let runtime = ctx.runtime_env();
        (plan, schema, runtime)
    }

    fn find_record(tracker: &QueryTracker, qid: uuid::Uuid) -> Arc<crate::query_tracker::QueryRecord> {
        tracker
            .records()
            .into_iter()
            .find(|r| r.query_id == qid)
            .expect("query record must exist for tracked id")
    }

    #[tokio::test]
    async fn tracked_stream_counts_rows_on_success() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(
            Arc::clone(&schema),
            vec![sample_batch(10), sample_batch(20), sample_batch(5)],
        );
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);

        let mut total = 0;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 35);

        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Finished);
        assert_eq!(record.output_rows, 35);
    }

    /// Task 2 conversion: a masked / filtered / denied SELECT records the
    /// policy-decision fields in its canonical audit event. The streaming
    /// finalizer copies `policy_summary` into `AuditEvent.policy` on success.
    ///
    /// Converted from the legacy flat `AuditEntry` assertions (which checked
    /// top-level `row_filters_applied`, `columns_masked`, `columns_restricted`,
    /// and `policy_denied`) to canonical `AuditEvent` assertions under the
    /// nested `policy` key. The `policy_denied` field was renamed `denied` in
    /// `PolicyAudit`. No logic changed: same policy summary, same stream, same
    /// finalizer path.
    #[tokio::test]
    async fn audit_entry_records_policy_summary_on_streaming_success() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let audit = Arc::new(
            sqe_metrics::audit::AuditLogger::new(path.to_str().unwrap()).unwrap(),
        );

        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        fin.audit = Some(Arc::clone(&audit));
        fin.policy_summary = sqe_policy::PolicySummary {
            row_filters_applied: 1,
            columns_masked: vec!["ssn".to_string()],
            columns_restricted: vec!["notes".to_string()],
            denied: false,
        };
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(Arc::clone(&schema), vec![sample_batch(3)]);
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
        while let Some(b) = stream.next().await {
            let _ = b.unwrap();
        }
        drop(stream);
        audit.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        // canonical AuditEvent discriminant
        assert_eq!(v["kind"], "query", "got: {v}");
        // policy fields nested under "policy" key (Task 2 migration)
        assert_eq!(v["policy"]["row_filters_applied"], 1, "got: {v}");
        assert_eq!(v["policy"]["columns_masked"], serde_json::json!(["ssn"]), "got: {v}");
        assert_eq!(v["policy"]["columns_restricted"], serde_json::json!(["notes"]), "got: {v}");
        assert_eq!(v["policy"]["denied"], false, "got: {v}");
        // legacy top-level field must no longer exist
        assert!(v.get("policy_denied").is_none() || v["policy_denied"].is_null(), "got: {v}");
        assert!(v.get("row_filters_applied").is_none() || v["row_filters_applied"].is_null(), "got: {v}");
    }

    #[tokio::test]
    async fn tracked_stream_marks_cancelled_on_early_drop() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(
            Arc::clone(&schema),
            vec![sample_batch(10), sample_batch(10), sample_batch(10)],
        );
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);

        let _first = stream.next().await.unwrap().unwrap();
        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Canceled);
    }

    #[tokio::test]
    async fn tracked_stream_marks_failed_on_error() {
        use datafusion::error::DataFusionError;

        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let s = futures::stream::iter(vec![
            Ok(sample_batch(3)),
            Err(DataFusionError::Execution("boom".to_string())),
        ]);
        let inner: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), s));
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);

        let _ok = stream.next().await.unwrap().unwrap();
        let err = stream.next().await.unwrap();
        assert!(err.is_err());
        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Failed);
    }

    #[tokio::test]
    async fn profile_mode_all_stores_profile_on_tracker() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        fin.profile_mode = ProfileMode::All;
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(Arc::clone(&schema), vec![sample_batch(7)]);
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Finished);
        let profile = record.profile.as_deref().expect("mode All must store a profile");
        assert!(
            profile.contains("Exec"),
            "profile must contain at least one operator name: {profile}"
        );
        assert!(
            profile.contains("elapsed_ms=") && profile.contains("unpushed_scans="),
            "profile must carry the summary header: {profile}"
        );
    }

    #[tokio::test]
    async fn profile_mode_off_leaves_profile_none() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(Arc::clone(&schema), vec![sample_batch(7)]);
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Finished);
        assert!(record.profile.is_none(), "mode Off must not store a profile");
    }

    #[tokio::test]
    async fn profile_captured_on_error_when_mode_slow() {
        use datafusion::error::DataFusionError;

        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        // Slow mode with a threshold the query never reaches: the error
        // path must still profile (failures always leave evidence).
        fin.profile_mode = ProfileMode::Slow;
        fin.slow_query_threshold_secs = 3600;
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let s = futures::stream::iter(vec![
            Ok(sample_batch(3)),
            Err(DataFusionError::Execution("boom".to_string())),
        ]);
        let inner: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), s));
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);

        let _ok = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.unwrap().is_err());
        drop(stream);

        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Failed);
        assert!(
            record.profile.is_some(),
            "failed query must carry a profile when mode != Off"
        );
    }

    #[tokio::test]
    async fn render_query_profile_carries_header() {
        let (plan, _schema, _runtime) = trivial_plan().await;
        let profile = render_query_profile(&plan, 12, 34);
        assert!(profile.starts_with("elapsed_ms=12 output_rows=34 unpushed_scans="));
        assert!(profile.contains("Exec"), "tree must list operators: {profile}");
    }

    #[test]
    fn truncate_profile_caps_at_64kib_on_char_boundary() {
        // Multibyte payload larger than the cap: must not panic, must not
        // exceed the cap plus the truncation marker.
        let big = "λ".repeat(MAX_PROFILE_BYTES);
        let out = truncate_profile(big);
        assert!(out.len() <= MAX_PROFILE_BYTES + 64);
        assert!(out.ends_with("[profile truncated at 64 KiB]"));

        // Small profiles pass through untouched.
        let small = "elapsed_ms=1\nProjectionExec".to_string();
        assert_eq!(truncate_profile(small.clone()), small);
    }

    /// Task 4 TDD guard: `StreamFinalizer` carries `client_ip` into the success
    /// audit event.
    ///
    /// Builds a `StreamFinalizer` with `client_ip: Some("10.9.9.9".into())`,
    /// wires it to a tempfile `AuditLogger`, drives the success finalize path,
    /// and asserts the written JSONL line has `client_ip == "10.9.9.9"`.
    ///
    /// RED before the field and emit-branch are added (emits `null`).
    /// GREEN after implementation.
    #[tokio::test]
    async fn streaming_finalizer_carries_client_ip_to_audit_event() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let audit = Arc::new(
            sqe_metrics::audit::AuditLogger::new(path.to_str().unwrap()).unwrap(),
        );

        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        fin.audit = Some(Arc::clone(&audit));
        fin.client_ip = Some("10.9.9.9".to_string());
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let inner = fixed_stream(Arc::clone(&schema), vec![sample_batch(1)]);
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
        while let Some(b) = stream.next().await {
            let _ = b.unwrap();
        }
        drop(stream);
        audit.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(
            v["client_ip"].as_str(),
            Some("10.9.9.9"),
            "streaming audit event must carry client_ip; got: {v}"
        );
    }

    /// `StreamFinalizer` carries `client_ip` into the audit event on the
    /// error finalize path (`on_error`).
    ///
    /// Builds a finalizer with `client_ip: Some("10.9.9.9".into())`, wires an
    /// `AuditLogger`, drives the error path via a stream that emits one error,
    /// and asserts the written JSONL line has `client_ip == "10.9.9.9"` and
    /// `outcome.error_code` set (indicating failure).
    #[tokio::test]
    async fn streaming_finalizer_carries_client_ip_to_audit_event_on_error() {
        use datafusion::error::DataFusionError;

        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit_error.jsonl");
        let audit = Arc::new(
            sqe_metrics::audit::AuditLogger::new(path.to_str().unwrap()).unwrap(),
        );

        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        fin.audit = Some(Arc::clone(&audit));
        fin.client_ip = Some("10.9.9.9".to_string());
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        let s = futures::stream::iter(vec![
            Err(DataFusionError::Execution("boom".to_string())),
        ]);
        let inner: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), s));
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);

        let err = stream.next().await.unwrap();
        assert!(err.is_err(), "stream must yield the error");
        drop(stream);
        audit.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(
            v["client_ip"].as_str(),
            Some("10.9.9.9"),
            "error audit event must carry client_ip; got: {v}"
        );
        // The JSONL serializer flattens outcome: status + error_code at top level.
        assert_eq!(
            v["status"].as_str(),
            Some("failure"),
            "error audit event must have status == failure; got: {v}"
        );
        assert!(
            v["error_code"].is_string(),
            "error audit event must have a top-level error_code; got: {v}"
        );
    }

    /// `StreamFinalizer` carries `client_ip` into the audit event on the
    /// cancel finalize path (`on_cancel`).
    ///
    /// Builds a finalizer with `client_ip: Some("10.9.9.9".into())`, wires an
    /// `AuditLogger`, drives the cancel path by dropping the stream mid-flight,
    /// and asserts the written JSONL line has `client_ip == "10.9.9.9"` and
    /// `outcome.error_code == "QUERY_CANCELLED"`.
    #[tokio::test]
    async fn streaming_finalizer_carries_client_ip_to_audit_event_on_cancel() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit_cancel.jsonl");
        let audit = Arc::new(
            sqe_metrics::audit::AuditLogger::new(path.to_str().unwrap()).unwrap(),
        );

        let mut fin = test_finalizer(Arc::clone(&tracker), plan, runtime);
        fin.audit = Some(Arc::clone(&audit));
        fin.client_ip = Some("10.9.9.9".to_string());
        let qid = fin.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        // Drop the stream after one batch to trigger the cancel path.
        let inner = fixed_stream(Arc::clone(&schema), vec![sample_batch(1), sample_batch(1)]);
        let mut stream = TrackedRecordBatchStream::new(inner, fin, None);
        let _first = stream.next().await.unwrap().unwrap();
        drop(stream);
        audit.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(
            v["client_ip"].as_str(),
            Some("10.9.9.9"),
            "cancel audit event must carry client_ip; got: {v}"
        );
        // The JSONL serializer flattens outcome: status + error_code at top level.
        assert_eq!(
            v["status"].as_str(),
            Some("failure"),
            "cancel audit event must have status == failure; got: {v}"
        );
        assert_eq!(
            v["error_code"].as_str(),
            Some("QUERY_CANCELLED"),
            "cancel audit event must have error_code == QUERY_CANCELLED; got: {v}"
        );
    }

    #[tokio::test]
    async fn idle_timeout_surfaces_error_not_empty_success() {
        let (plan, schema, runtime) = trivial_plan().await;
        let tracker = test_tracker();
        let finalizer = test_finalizer(tracker.clone(), plan, runtime);
        let qid = finalizer.query_id;
        tracker.start(qid, "test-user", None, "SELECT 1", "test-session", None, vec![]);

        // Inner stream never produces a batch: a stalled execution pipeline.
        let pending = futures::stream::pending::<Result<RecordBatch, DataFusionError>>();
        let inner: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, pending));

        let mut stream = TrackedRecordBatchStream::new(inner, finalizer, None)
            .with_idle_timeout(Duration::from_millis(50));

        // The client must receive an explicit error. Ending the stream with
        // a clean EOF here would present a stalled query as a legitimately
        // empty result (observed in a TPC-DS compare run as "0 rows, no
        // error" scored as a row diff).
        let item = stream.next().await.expect("stream must yield an item");
        let err = item.expect_err("idle timeout must surface as an error");
        assert!(
            err.to_string().contains("idle timeout"),
            "unexpected error: {err}"
        );

        // After the abort the stream is terminated.
        assert!(stream.next().await.is_none());

        // And the tracker records a failure, not a cancel or success.
        let record = find_record(&tracker, qid);
        assert_eq!(record.state, QueryState::Failed);
    }
}

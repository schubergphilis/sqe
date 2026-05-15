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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use futures::Stream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::query_handler::{aggregate_spill_metrics, extract_plan_metrics};
use crate::query_tracker::QueryTracker;

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
}

impl StreamFinalizer {
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
            Vec::new(),
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
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: self.username.clone(),
                session_id: Some(self.session_id.clone()),
                query_hash: sqe_metrics::audit::query_hash(&self.sql),
                query_text: Some(self.sql.clone()),
                statement_type: self.kind_name.clone(),
                duration_ms: execution_ms,
                rows_returned: rows,
                status: "success".to_string(),
                client_ip: None,
            });
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
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: self.username.clone(),
                session_id: Some(self.session_id.clone()),
                query_hash: sqe_metrics::audit::query_hash(&self.sql),
                query_text: Some(self.sql.clone()),
                statement_type: self.kind_name.clone(),
                duration_ms: duration.as_millis() as u64,
                rows_returned: rows,
                status: "error".to_string(),
                client_ip: None,
            });
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
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: self.username.clone(),
                session_id: Some(self.session_id.clone()),
                query_hash: sqe_metrics::audit::query_hash(&self.sql),
                query_text: Some(self.sql.clone()),
                statement_type: self.kind_name.clone(),
                duration_ms: duration.as_millis() as u64,
                rows_returned: rows,
                status: "cancelled".to_string(),
                client_ip: None,
            });
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
            Poll::Pending => Poll::Pending,
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
}

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
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, TryStreamExt};
use tracing::{error, info, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use sqe_metrics::propagation::inject_trace_context;
use sqe_planner::ScanTask;

use crate::credential_refresh::CredentialRefreshTracker;
use crate::worker_registry::WorkerRegistry;

/// Default maximum number of retry attempts per fragment before giving up
/// or falling back to local execution.
const DEFAULT_MAX_RETRIES: u32 = 2;

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
    properties: PlanProperties,
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

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

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
        }
    }

    /// Set the credential expiry for the vended credentials in these scan tasks.
    ///
    /// When combined with a credential tracker, this enables the coordinator to
    /// detect when credentials are approaching expiry and push refreshed ones
    /// to workers.
    pub fn with_credential_expiry(mut self, expiry: DateTime<Utc>) -> Self {
        self.credential_expiry = Some(expiry);
        self
    }

    /// Set the credential refresh tracker for monitoring expiring credentials.
    pub fn with_credential_tracker(mut self, tracker: Arc<CredentialRefreshTracker>) -> Self {
        self.credential_tracker = Some(tracker);
        self
    }

    /// Set the worker registry for health tracking and failover.
    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    /// Set the maximum number of retry attempts per fragment.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the local executor for fallback when all workers are down.
    pub fn with_local_executor(mut self, executor: Arc<dyn LocalExecutor>) -> Self {
        self.local_executor = Some(executor);
        self
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec: workers={}, total_files={}, max_retries={}",
            self.worker_urls.len(),
            self.scan_tasks
                .iter()
                .map(|t| t.data_file_paths.len())
                .sum::<usize>(),
            self.max_retries,
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

    fn properties(&self) -> &PlanProperties {
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

        let task = self.scan_tasks[partition].clone();
        let initial_worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();
        let schema_for_stream = self.schema.clone();
        let credential_expiry = self.credential_expiry;
        let credential_tracker = self.credential_tracker.clone();
        let max_retries = self.max_retries;
        let worker_registry = self.worker_registry.clone();
        let local_executor = self.local_executor.clone();

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

                match dispatch_to_worker(&task, &current_worker_url, &parent_cx).await {
                    Ok(flight_stream) => {
                        let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                            Box::pin(
                                flight_stream
                                    .map_err(|e| DataFusionError::External(Box::new(e))),
                            );
                        return Ok(inner);
                    }
                    Err(e) => {
                        warn!(
                            fragment_id = %task.fragment_id,
                            worker = %current_worker_url,
                            error = %e,
                            attempt = attempt,
                            "Worker execution failed"
                        );

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

            // All remote attempts exhausted — clean up credential tracker
            if let Some(ref tracker) = credential_tracker {
                tracker.unregister(&task.fragment_id).await;
            }

            // Try local fallback
            if let Some(ref executor) = local_executor {
                warn!(
                    fragment_id = %task.fragment_id,
                    failed_workers = ?failed_workers,
                    "All workers failed, falling back to local execution"
                );
                let local_stream = executor.execute_local(&task, schema)?;
                let inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                    Box::pin(local_stream);
                return Ok(inner);
            }

            // No local fallback available — propagate the last error
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
        }))
    }
}

/// Dispatch a scan task to a single worker via Arrow Flight `do_get`.
///
/// Returns the `FlightRecordBatchStream` on success, or a `DataFusionError`
/// on connection/transport failure.
async fn dispatch_to_worker(
    task: &ScanTask,
    worker_url: &str,
    parent_cx: &opentelemetry::Context,
) -> Result<FlightRecordBatchStream, DataFusionError> {
    let ticket_bytes = task
        .to_bytes()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let channel = tonic::transport::Endpoint::new(worker_url.to_string())
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create endpoint for worker {worker_url}: {e}"
            ))
        })?
        .connect()
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to connect to worker {worker_url}: {e}"
            ))
        })?;
    let mut client = FlightServiceClient::new(channel);

    let ticket = Ticket::new(ticket_bytes);
    let mut request = tonic::Request::new(ticket);

    // Inject W3C TraceContext (traceparent/tracestate) into gRPC metadata
    inject_trace_context(parent_cx, request.metadata_mut());

    let response = client
        .do_get(request)
        .await
        .map_err(|e| {
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
}

impl Stream for DistributedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for DistributedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlanProperties;

    fn make_task(id: &str) -> ScanTask {
        ScanTask {
            fragment_id: id.to_string(),
            data_file_paths: vec![],
            projected_columns: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: true,
        }
    }

    #[test]
    fn test_distributed_scan_exec_properties() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let task = ScanTask {
            fragment_id: "frag-001".to_string(),
            data_file_paths: vec!["s3://bucket/file.parquet".to_string()],
            projected_columns: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: true,
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
}

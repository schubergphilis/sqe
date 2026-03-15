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
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, TryStreamExt};
use tracing::info;

use sqe_planner::ScanTask;

/// DataFusion `ExecutionPlan` that distributes scan work across workers.
///
/// Each partition maps to one worker. When DataFusion calls `execute(i)`,
/// the DistributedScanExec sends a `ScanTask` to worker[i] via Arrow Flight
/// `do_get` and returns the result stream.
#[derive(Debug)]
pub struct DistributedScanExec {
    scan_tasks: Vec<ScanTask>,
    worker_urls: Vec<String>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DistributedScanExec {
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
        }
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec: workers={}, total_files={}",
            self.worker_urls.len(),
            self.scan_tasks
                .iter()
                .map(|t| t.data_file_paths.len())
                .sum::<usize>(),
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
        let worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();

        info!(
            fragment_id = %task.fragment_id,
            worker = %worker_url,
            file_count = task.data_file_paths.len(),
            "Dispatching scan to worker"
        );

        let stream = futures::stream::once(async move {
            let ticket_bytes = task.to_bytes().map_err(|e| {
                DataFusionError::External(Box::new(e))
            })?;

            let mut client =
                FlightServiceClient::connect(worker_url.clone())
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to connect to worker {worker_url}: {e}"
                        ))
                    })?;

            let ticket = Ticket::new(ticket_bytes);
            let response = client
                .do_get(tonic::Request::new(ticket))
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

            Ok::<_, DataFusionError>(
                flight_stream.map_err(|e| DataFusionError::External(Box::new(e))),
            )
        })
        .try_flatten();

        Ok(Box::pin(DistributedRecordBatchStream {
            schema,
            inner: Box::pin(stream),
        }))
    }
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
}

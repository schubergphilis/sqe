use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use futures::{Stream, TryStreamExt};
use iceberg::table::Table;
use tracing::debug;

/// Custom DataFusion `ExecutionPlan` that scans an Iceberg table using
/// iceberg-rust's scan API. This replaces the `EmptyExec` placeholder
/// in `SqeTableProvider` and provides actual data reads from S3.
///
/// The table's `FileIO` (configured with the user's vended S3 credentials)
/// handles all data access -- no separate ObjectStore registration needed.
#[derive(Debug)]
pub struct IcebergScanExec {
    /// The Iceberg table to scan (contains FileIO with credentials).
    table: Table,
    /// Arrow schema for the scan output (after projection).
    projected_schema: SchemaRef,
    /// Column names to project (None = all columns).
    projection: Option<Vec<String>>,
    /// Cached plan properties.
    properties: PlanProperties,
    /// Execution metrics (elapsed time, output rows).
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergScanExec {
    /// Create a new Iceberg scan execution plan.
    pub fn new(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            table,
            projected_schema,
            projection,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Returns the underlying Iceberg table.
    pub fn table(&self) -> &Table {
        &self.table
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: table={}, projection={:?}",
            self.table.identifier(),
            self.projection,
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self) // no children to replace
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergScanExec only supports partition 0, got {partition}"
            )));
        }

        let table = self.table.clone();
        let schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let baseline = BaselineMetrics::new(&self.metrics, partition);

        debug!(
            table = %table.identifier(),
            "Executing IcebergScanExec"
        );

        // Build the scan lazily -- to_arrow() is async, execute() is sync.
        // We create a stream that initializes the scan on first poll.
        let stream = futures::stream::once(async move {
            let mut scan_builder = table.scan();

            // Apply column projection if specified
            if let Some(ref cols) = projection {
                scan_builder = scan_builder.select(cols.iter().map(|s| s.as_str()));
            }

            let scan = scan_builder
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let arrow_stream = scan
                .to_arrow()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok::<_, DataFusionError>(
                arrow_stream.map_err(|e| DataFusionError::External(Box::new(e))),
            )
        })
        .try_flatten();

        Ok(Box::pin(IcebergRecordBatchStream {
            schema,
            inner: Box::pin(stream),
            baseline,
        }))
    }
}

/// Wrapper stream that implements `RecordBatchStream` for DataFusion.
///
/// DataFusion requires that record batch streams implement both the
/// `Stream<Item = DFResult<RecordBatch>>` trait and the `RecordBatchStream`
/// trait (which adds a `schema()` method). This wrapper bridges the
/// iceberg-rust arrow stream to satisfy both requirements.
///
/// The `baseline` field records elapsed wall-clock time and output row counts
/// so that `EXPLAIN ANALYZE` and `EXPLAIN FULL` can report scan metrics.
struct IcebergRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    baseline: BaselineMetrics,
}

impl Stream for IcebergRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: We do not move any fields out of self. `inner` remains pinned
        // (accessed only through Pin::as_mut()). `baseline` is Unpin.
        // This split is needed because Pin<&mut T> does not allow splitting
        // borrows of distinct fields through its DerefMut impl.
        let s = unsafe { self.as_mut().get_unchecked_mut() };
        let poll = {
            let _timer = s.baseline.elapsed_compute().timer();
            s.inner.as_mut().poll_next(cx)
        };
        if let Poll::Ready(Some(Ok(ref batch))) = poll {
            s.baseline.record_output(batch.num_rows());
        }
        poll
    }
}

impl datafusion::physical_plan::RecordBatchStream for IcebergRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

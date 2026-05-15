//! DataFusion `TableProvider` for CDC incremental scans.
//!
//! Unlike [`crate::table_provider::SqeTableProvider`], which goes through
//! iceberg-rust's scan planner to collect files for a whole snapshot, this
//! provider consumes an explicit [`IncrementalPlan`] built by
//! [`crate::incremental_scan::plan_incremental`]. It reads only the files in
//! the plan, in the order the planner chose, and attaches the three CDC meta
//! columns per row from each [`IncrementalFile`].
//!
//! The provider owns the augmented Arrow schema (base + meta) so DataFusion
//! can plan projections that reference `_change_type`, `_change_ordinal`, or
//! `_commit_snapshot_id` directly.
//!
//! Design notes:
//!
//! - The scan executes serially over the file list. Parallelising the reads
//!   requires per-file streams; for the initial wiring we favour correctness
//!   and simple error paths. The range-scan path is not latency-critical
//!   compared to the time-travel path.
//! - Only data (`Insert`) files are read. Delete files in
//!   `plan.delete_files` represent row removals from files inside the range
//!   and are already reconciled by the planner. For V1 of this provider we
//!   emit only inserts: the `_change_type` column is always `"insert"` for
//!   rows returned here. Deletes appear as separate rows in a follow-up once
//!   the delete-reader pipeline materialises row IDs.
//! - Schema projection follows the same DataFusion convention as
//!   `SqeTableProvider`: `projection` contains indices into the augmented
//!   schema (base columns plus three meta columns).

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use tracing::debug;

use crate::incremental_scan::{
    ChangeKind, IncrementalFile, IncrementalPlan, attach_meta_columns, augment_schema_with_meta,
    CHANGE_ORDINAL_COLUMN, CHANGE_TYPE_COLUMN, COMMIT_SNAPSHOT_ID_COLUMN,
};

/// A TableProvider backed by an explicit [`IncrementalPlan`].
///
/// Registered transiently by the coordinator when it encounters
/// `FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`. Each query builds a
/// fresh provider because the plan is scoped to the snapshot range.
pub struct IncrementalTableProvider {
    /// Base (non-meta) Arrow schema as declared by the Iceberg table.
    base_schema: ArrowSchemaRef,
    /// Augmented schema with `_change_type`, `_change_ordinal`,
    /// `_commit_snapshot_id` appended.
    augmented_schema: ArrowSchemaRef,
    /// The plan to execute. Owned so the provider can be cloned cheaply via
    /// `Arc` wrapping by DataFusion.
    plan: Arc<IncrementalPlan>,
    /// Iceberg `FileIO` used to read Parquet bytes. `None` is accepted for
    /// unit tests that exercise construction without a storage layer; in
    /// that mode the scan returns an empty batch.
    file_io: Option<FileIO>,
}

impl std::fmt::Debug for IncrementalTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalTableProvider")
            .field("base_schema", &self.base_schema)
            .field("data_files", &self.plan.data_files.len())
            .field("delete_files", &self.plan.delete_files.len())
            .field("snapshots", &self.plan.snapshots_in_range.len())
            .field("file_io", &self.file_io.is_some())
            .finish()
    }
}

impl IncrementalTableProvider {
    /// Build a provider from the base schema, a resolved plan, and the table's
    /// `FileIO` (or `None` for tests).
    pub fn new(
        base_schema: ArrowSchemaRef,
        plan: IncrementalPlan,
        file_io: Option<FileIO>,
    ) -> Self {
        let augmented_schema = augment_schema_with_meta(&base_schema);
        Self {
            base_schema,
            augmented_schema,
            plan: Arc::new(plan),
            file_io,
        }
    }

    /// Return the augmented schema (base + 3 meta columns).
    pub fn augmented_schema(&self) -> ArrowSchemaRef {
        self.augmented_schema.clone()
    }

    /// Return a reference to the plan this provider will execute.
    pub fn plan(&self) -> &IncrementalPlan {
        &self.plan
    }
}

#[async_trait]
impl TableProvider for IncrementalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.augmented_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Resolve the final output schema based on the projection.
        let output_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.augmented_schema.field(i).clone())
                    .collect();
                Arc::new(ArrowSchema::new_with_metadata(
                    fields,
                    self.augmented_schema.metadata().clone(),
                ))
            }
            None => self.augmented_schema.clone(),
        };

        // Translate the projection (indices into the augmented schema) into:
        //   - indices into the BASE schema, for the Parquet reader
        //   - flags telling us which meta columns to emit in the output
        let mut base_indices: Vec<usize> = Vec::new();
        let mut meta = MetaCols::default();
        let base_cols = self.base_schema.fields().len();
        let requested: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.augmented_schema.fields().len()).collect(),
        };
        let mut output_order: Vec<OutputCol> = Vec::with_capacity(requested.len());
        for idx in requested {
            if idx < base_cols {
                base_indices.push(idx);
                output_order
                    .push(OutputCol::Base(base_indices.len() - 1));
                continue;
            }
            let name = self.augmented_schema.field(idx).name().as_str();
            match name {
                CHANGE_TYPE_COLUMN => {
                    meta.change_type = true;
                    output_order.push(OutputCol::ChangeType);
                }
                CHANGE_ORDINAL_COLUMN => {
                    meta.change_ordinal = true;
                    output_order.push(OutputCol::ChangeOrdinal);
                }
                COMMIT_SNAPSHOT_ID_COLUMN => {
                    meta.commit_snapshot = true;
                    output_order.push(OutputCol::CommitSnapshot);
                }
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "IncrementalTableProvider: unexpected projection column {other}"
                    )));
                }
            }
        }

        let exec = IncrementalScanExec::new(
            self.plan.clone(),
            self.base_schema.clone(),
            output_schema,
            base_indices,
            output_order,
            meta,
            self.file_io.clone(),
        );
        Ok(Arc::new(exec))
    }
}

/// Where each output column comes from when building the per-file batch.
#[derive(Debug, Clone, Copy)]
enum OutputCol {
    /// Index into the projected base columns (i.e. parquet-read order).
    Base(usize),
    ChangeType,
    ChangeOrdinal,
    CommitSnapshot,
}

/// Which CDC meta columns the caller asked for. Replaces three
/// correlated `bool` arguments where a caller could silently swap
/// `wants_change_type` and `wants_change_ordinal` (same type, no
/// warning) and emit values into the wrong column (issue #130).
#[derive(Debug, Clone, Copy, Default)]
struct MetaCols {
    change_type: bool,
    change_ordinal: bool,
    commit_snapshot: bool,
}

impl MetaCols {
    fn count(self) -> usize {
        self.change_type as usize
            + self.change_ordinal as usize
            + self.commit_snapshot as usize
    }

    fn all(self) -> bool {
        self.change_type && self.change_ordinal && self.commit_snapshot
    }
}

/// Physical execution node for an incremental scan.
///
/// Reads the listed data files one at a time using the table's `FileIO`,
/// decodes Parquet, projects base columns, and appends the requested meta
/// columns. Delete files are intentionally not streamed: the planner has
/// already reconciled them, and V1 of the provider emits inserts only.
struct IncrementalScanExec {
    plan: Arc<IncrementalPlan>,
    base_schema: ArrowSchemaRef,
    output_schema: ArrowSchemaRef,
    base_indices: Vec<usize>,
    output_order: Vec<OutputCol>,
    meta: MetaCols,
    file_io: Option<FileIO>,
    target_partitions: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IncrementalScanExec {
    fn new(
        plan: Arc<IncrementalPlan>,
        base_schema: ArrowSchemaRef,
        output_schema: ArrowSchemaRef,
        base_indices: Vec<usize>,
        output_order: Vec<OutputCol>,
        meta: MetaCols,
        file_io: Option<FileIO>,
    ) -> Self {
        let eq_props = EquivalenceProperties::new(output_schema.clone());
        let target_partitions: usize = 1;
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(target_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            plan,
            base_schema,
            output_schema,
            base_indices,
            output_order,
            meta,
            file_io,
            target_partitions,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Override the number of output partitions.
    ///
    /// Each partition reads a round-robin slice of the planned `data_files`
    /// list, allowing DataFusion to scan a wide change-log range in parallel.
    #[allow(dead_code)]
    fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        let n = target_partitions.max(1);
        self.target_partitions = n;
        let eq = self.properties.eq_properties.clone();
        self.properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            self.properties.emission_type,
            self.properties.boundedness,
        ));
        self
    }
}

impl std::fmt::Debug for IncrementalScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalScanExec")
            .field("files", &self.plan.data_files.len())
            .field("base_indices", &self.base_indices)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl DisplayAs for IncrementalScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IncrementalScanExec: files={}, snapshots={}",
            self.plan.data_files.len(),
            self.plan.snapshots_in_range.len()
        )
    }
}

impl ExecutionPlan for IncrementalScanExec {
    fn name(&self) -> &str {
        "IncrementalScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
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

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.target_partitions {
            return Err(DataFusionError::Internal(format!(
                "IncrementalScanExec partition {partition} out of range (target_partitions = {})",
                self.target_partitions
            )));
        }
        let total_partitions = self.target_partitions;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let output_schema = self.output_schema.clone();
        let base_schema = self.base_schema.clone();
        let base_indices = self.base_indices.clone();
        let output_order = self.output_order.clone();
        let meta = self.meta;
        let file_io = self.file_io.clone();
        let plan = self.plan.clone();

        // Build a stream over (file, meta) pairs, then flatten the Parquet
        // batches into the output. `file_io` being `None` shortcuts to an
        // empty stream.
        let output_schema_inner = output_schema.clone();
        let file_io_opt = file_io.clone();
        let data_files: Vec<_> = if total_partitions > 1 {
            plan.data_files
                .iter()
                .cloned()
                .enumerate()
                .filter(|(idx, _)| idx % total_partitions == partition)
                .map(|(_, f)| f)
                .collect()
        } else {
            plan.data_files.clone()
        };
        let gen_stream = futures::stream::once(async move {
            let Some(file_io) = file_io_opt else {
                // Empty output batch once, then EOS.
                let empty = RecordBatch::new_empty(output_schema_inner.clone());
                return Ok::<Vec<RecordBatch>, DataFusionError>(vec![empty]);
            };
            let mut all: Vec<RecordBatch> = Vec::new();
            for file in data_files.iter() {
                if !matches!(file.kind, ChangeKind::Insert) {
                    continue;
                }
                let batches = read_file_batches(
                    &file_io,
                    file,
                    &base_schema,
                    &base_indices,
                )
                .await
                .map_err(DataFusionError::External)?;
                for base_batch in batches {
                    let out = project_with_meta(
                        base_batch,
                        file,
                        &output_schema_inner,
                        &output_order,
                        meta,
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    all.push(out);
                }
            }
            Ok::<Vec<RecordBatch>, DataFusionError>(all)
        });
        let stream = gen_stream
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();

        Ok(Box::pin(IncrementalBatchStream {
            inner: Box::pin(stream),
            schema: output_schema,
            baseline,
        }))
    }
}

async fn read_file_batches(
    file_io: &FileIO,
    file: &IncrementalFile,
    _base_schema: &ArrowSchemaRef,
    base_indices: &[usize],
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
    debug!(path = %file.path, size = file.size_bytes, "IncrementalScan: reading file");
    let input = file_io.new_input(&file.path)?;
    let bytes = input.read().await?;
    let reader_opts = ArrowReaderOptions::new();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, reader_opts)?;
    let parquet_schema = builder.parquet_schema().clone();
    let mask = if base_indices.is_empty() {
        // COUNT(*) or meta-only: read the first column for row count only.
        ProjectionMask::roots(&parquet_schema, vec![0])
    } else {
        ProjectionMask::roots(&parquet_schema, base_indices.iter().copied())
    };
    let reader = builder
        .with_projection(mask)
        .with_batch_size(8192)
        .build()?;
    let mut out = Vec::new();
    for r in reader {
        out.push(r?);
    }
    Ok(out)
}

fn project_with_meta(
    base_batch: RecordBatch,
    file: &IncrementalFile,
    output_schema: &ArrowSchemaRef,
    output_order: &[OutputCol],
    meta: MetaCols,
) -> Result<RecordBatch, ArrowError> {
    // Build meta columns for this batch by attaching to a zero-column base.
    let n = base_batch.num_rows();

    // For the common case where the output_order simply lists all base cols
    // followed by the meta cols in canonical order, fall back to
    // `attach_meta_columns`. For arbitrary projection orders we build the
    // column list by hand using `output_order`.
    let expects_canonical = {
        let base_cols = base_batch.num_columns();
        output_order.len() == base_cols + meta.count()
            && output_order
                .iter()
                .take(base_cols)
                .enumerate()
                .all(|(i, c)| matches!(c, OutputCol::Base(k) if *k == i))
    };
    if expects_canonical && meta.all() {
        return attach_meta_columns(base_batch, file);
    }

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    let change_type: ArrayRef = Arc::new(StringArray::from(vec![file.kind.as_str(); n]));
    let change_ordinal: ArrayRef = Arc::new(Int64Array::from(vec![file.ordinal; n]));
    let commit_snapshot: ArrayRef = Arc::new(Int64Array::from(vec![file.snapshot_id; n]));

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(output_order.len());
    for out in output_order {
        match out {
            OutputCol::Base(k) => cols.push(base_batch.column(*k).clone()),
            OutputCol::ChangeType => cols.push(change_type.clone()),
            OutputCol::ChangeOrdinal => cols.push(change_ordinal.clone()),
            OutputCol::CommitSnapshot => cols.push(commit_snapshot.clone()),
        }
    }

    RecordBatch::try_new_with_options(
        output_schema.clone(),
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
}

/// Adapter that turns the async_stream-generated stream into a
/// `RecordBatchStream` with the required schema and metrics.
struct IncrementalBatchStream {
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    schema: ArrowSchemaRef,
    baseline: BaselineMetrics,
}

impl Stream for IncrementalBatchStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline.record_poll(poll)
    }
}

impl datafusion::physical_plan::RecordBatchStream for IncrementalBatchStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn provider_schema_includes_meta_columns() {
        let base = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let plan = IncrementalPlan::default();
        let provider = IncrementalTableProvider::new(base, plan, None);
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "val");
        assert_eq!(schema.field(2).name(), "_change_type");
        assert_eq!(schema.field(3).name(), "_change_ordinal");
        assert_eq!(schema.field(4).name(), "_commit_snapshot_id");
    }

    #[tokio::test]
    async fn empty_plan_scan_produces_empty_batch() {
        use datafusion::prelude::SessionContext;

        let base = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let plan = IncrementalPlan::default();
        let provider = Arc::new(IncrementalTableProvider::new(base, plan, None));

        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1); // count(*) always emits one row
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 0);
    }
}

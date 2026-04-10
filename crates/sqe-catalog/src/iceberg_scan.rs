use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table;
use tracing::{debug, info_span, warn};

use crate::pruning_stats::IcebergManifestStatistics;

#[derive(Debug)]
pub struct IcebergScanExec {
    table: Table,
    projected_schema: SchemaRef,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    df_filters: Vec<Expr>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    /// Optional Prometheus metrics registry for reporting file pruning,
    /// footer cache, and S3 I/O counters.
    #[allow(dead_code)]
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Optional snapshot ID for time travel queries.
    snapshot_id: Option<i64>,
}

impl IcebergScanExec {
    pub fn new(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>) -> Self {
        Self::new_with_filters(table, projected_schema, projection, predicates, vec![])
    }

    pub fn new_with_filters(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>, df_filters: Vec<Expr>) -> Self {
        Self::new_with_filters_and_metrics(table, projected_schema, projection, predicates, df_filters, None)
    }

    pub fn new_with_filters_and_metrics(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>, df_filters: Vec<Expr>, prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>) -> Self {
        // Sort order from Iceberg metadata.
        //
        // IMPORTANT: Iceberg sort order is a HINT about how files should be
        // written, NOT a guarantee that existing data files are sorted. Writers
        // (Spark, Trino, SQE CTAS) may not enforce sort order. Declaring
        // pre-sorted data when it isn't causes incorrect query results
        // (DataFusion skips the sort and uses SortPreservingMergeExec).
        //
        // We only declare sort order for identity-transform partition columns,
        // which ARE guaranteed to be clustered by Iceberg's file organization.
        // Non-partition sort columns emit a warning and are ignored.
        let eq_props = {
            let sort_order = table.metadata().default_sort_order();
            let iceberg_schema = table.metadata().current_schema();
            let partition_cols = {
                use iceberg::spec::Transform;
                let spec = table.metadata().default_partition_spec();
                spec.fields()
                    .iter()
                    .filter(|f| f.transform == Transform::Identity)
                    .filter_map(|f| iceberg_schema.field_by_id(f.source_id).map(|sf| sf.name.clone()))
                    .collect::<std::collections::HashSet<_>>()
            };

            match crate::sort_order::iceberg_sort_to_physical(sort_order, iceberg_schema, &projected_schema) {
                Some(sort_exprs) => {
                    // Only keep sort expressions for partition columns
                    let safe_exprs: Vec<_> = sort_exprs.into_iter().filter(|expr| {
                        let col_name = expr.expr.to_string();
                        if partition_cols.contains(&col_name) {
                            true
                        } else {
                            warn!(
                                table = %table.identifier(),
                                column = %col_name,
                                "Ignoring non-partition sort order -- data may not be physically sorted. \
                                 Use ORDER BY explicitly for correctness."
                            );
                            false
                        }
                    }).collect();

                    if safe_exprs.is_empty() {
                        EquivalenceProperties::new(projected_schema.clone())
                    } else {
                        crate::sort_order::equivalence_with_sort(projected_schema.clone(), safe_exprs)
                    }
                }
                None => EquivalenceProperties::new(projected_schema.clone()),
            }
        };
        let properties = PlanProperties::new(eq_props, Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded);
        Self { table, projected_schema, projection, predicates, df_filters, properties, metrics: ExecutionPlanMetricsSet::new(), prom_metrics, snapshot_id: None }
    }

    /// Set the snapshot ID for time travel queries.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn table(&self) -> &Table { &self.table }
    pub fn predicates(&self) -> Option<&Predicate> { self.predicates.as_ref() }
    pub fn df_filters(&self) -> &[Expr] { &self.df_filters }
    pub fn projection(&self) -> Option<&[String]> { self.projection.as_deref() }

    /// Returns the names of identity-transform partition columns from the
    /// Iceberg table's default partition spec.
    ///
    /// Bucket, truncate, date, and other derived transforms are excluded
    /// because they don't map directly to sortable column values.
    pub fn partition_column_names(&self) -> Vec<String> {
        use iceberg::spec::Transform;
        let spec = self.table.metadata().default_partition_spec();
        let schema = self.table.metadata().current_schema();
        spec.fields()
            .iter()
            .filter(|f| f.transform == Transform::Identity)
            .filter_map(|f| {
                schema
                    .field_by_id(f.source_id)
                    .map(|sf| sf.name.clone())
            })
            .collect()
    }

    pub async fn data_file_paths(&self) -> Result<Vec<String>, iceberg::Error> {
        let info = self.data_file_info().await?;
        Ok(info.into_iter().map(|(path, _)| path).collect())
    }

    pub async fn data_file_info(&self) -> Result<Vec<(String, u64)>, iceberg::Error> {
        let (result, _) = self.data_file_info_with_pruning_stats().await?;
        Ok(result)
    }

    pub async fn data_file_info_with_pruning_stats(&self) -> Result<(Vec<(String, u64)>, usize), iceberg::Error> {
        let mut sb = self.table.scan();
        if let Some(sid) = self.snapshot_id { sb = sb.snapshot_id(sid); }
        if let Some(ref cols) = self.projection { sb = sb.select(cols.iter().map(|s| s.as_str())); }
        if let Some(ref pred) = self.predicates { sb = sb.with_filter(pred.clone()); }
        let scan = sb.build()?;
        let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
        let mut result: Vec<(String, u64)> = tasks.iter().map(|t| (t.data_file_path().to_string(), t.length)).collect();
        let mut pruned_count = 0usize;
        if !self.df_filters.is_empty() {
            if let Ok(data_files) = self.collect_data_files().await {
                let planned: std::collections::HashSet<String> = result.iter().map(|(p, _)| p.clone()).collect();
                let relevant: Vec<DataFile> = data_files.into_iter().filter(|df| planned.contains(df.file_path())).collect();
                if !relevant.is_empty() {
                    let ischema = self.table.metadata().current_schema();
                    let (kept, pc) = Self::prune_data_files(relevant, &self.df_filters, &self.projected_schema, ischema);
                    pruned_count = pc;
                    if pruned_count > 0 {
                        debug!(pruned = pruned_count, remaining = kept.len(), "File-level min/max pruning");
                        // Increment Prometheus file pruning counter
                        if let Some(ref pm) = self.prom_metrics {
                            pm.files_pruned_minmax.inc_by(pruned_count as f64);
                        }
                        let kept_paths: std::collections::HashSet<String> = kept.iter().map(|df| df.file_path().to_string()).collect();
                        result.retain(|(path, _)| kept_paths.contains(path));
                    }
                }
            }
        }
        Ok((result, pruned_count))
    }

    pub async fn collect_data_files(&self) -> Result<Vec<DataFile>, iceberg::Error> {
        let metadata = self.table.metadata();
        let snapshot = if let Some(sid) = self.snapshot_id {
            match metadata.snapshot_by_id(sid) { Some(s) => s, None => return Ok(vec![]) }
        } else {
            match metadata.current_snapshot() { Some(s) => s, None => return Ok(vec![]) }
        };
        let manifest_list = snapshot.load_manifest_list(self.table.file_io(), metadata).await?;
        let mut data_files = Vec::new();
        for mf in manifest_list.entries() {
            let manifest = mf.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Deleted && entry.data_file().content_type() == DataContentType::Data {
                    data_files.push(entry.data_file().clone());
                }
            }
        }
        Ok(data_files)
    }

    pub fn prune_data_files(data_files: Vec<DataFile>, df_filters: &[Expr], schema: &SchemaRef, iceberg_schema: &iceberg::spec::Schema) -> (Vec<DataFile>, usize) {
        use datafusion::physical_expr::create_physical_expr;
        use datafusion::prelude::SessionContext;
        if data_files.is_empty() || df_filters.is_empty() { return (data_files, 0); }
        let combined = match df_filters.iter().cloned().reduce(|a, b| a.and(b)) { Some(e) => e, None => return (data_files, 0) };
        let df_schema = match datafusion::common::DFSchema::try_from(schema.as_ref().clone()) { Ok(s) => s, Err(e) => { warn!(error=%e, "DFSchema creation failed"); return (data_files, 0); } };
        let ctx = SessionContext::new();
        let state = ctx.state();
        let physical_expr = match create_physical_expr(&combined, &df_schema, state.execution_props()) { Ok(e) => e, Err(e) => { warn!(error=%e, "Physical expr failed"); return (data_files, 0); } };
        let pruning_pred: PruningPredicate = match PruningPredicate::try_new(physical_expr, schema.clone()) { Ok(p) => p, Err(e) => { warn!(error=%e, "PruningPredicate failed"); return (data_files, 0); } };
        let stats = IcebergManifestStatistics::new(data_files.clone(), schema.clone(), iceberg_schema);
        match pruning_pred.prune(&stats) {
            Ok(flags) => {
                let total = data_files.len();
                let kept: Vec<DataFile> = data_files.into_iter().zip(flags).filter_map(|(df, keep)| if keep { Some(df) } else { None }).collect();
                let pruned = total - kept.len();
                debug!(total_files = total, kept_files = kept.len(), pruned_files = pruned, "File-level min/max pruning applied");
                (kept, pruned)
            }
            Err(e) => { warn!(error=%e, "PruningPredicate eval failed"); (data_files, 0) }
        }
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IcebergScanExec: table={}, projection={:?}, predicate=[{}], df_filters={}, snapshot_id={:?}", self.table.identifier(), self.projection, self.predicates.as_ref().map_or(String::new(), |p| format!("{p}")), self.df_filters.len(), self.snapshot_id)
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str { "IcebergScanExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.projected_schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> { Ok(self) }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let span = info_span!("iceberg_scan", table=%self.table.identifier(), partition=partition, predicates=?self.predicates);
        let _guard = span.enter();
        if partition != 0 { return Err(DataFusionError::Internal(format!("IcebergScanExec only supports partition 0, got {partition}"))); }
        let table = self.table.clone();
        let schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let predicates = self.predicates.clone();
        let snapshot_id = self.snapshot_id;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let _files_pruned_minmax = MetricBuilder::new(&self.metrics).counter("files_pruned_minmax", partition);
        debug!(table=%table.identifier(), predicates=?predicates, snapshot_id=?snapshot_id, "Executing IcebergScanExec");

        // For time-travel: check the specified snapshot exists; for current: check current snapshot.
        let has_snapshot = if let Some(sid) = snapshot_id {
            table.metadata().snapshot_by_id(sid).is_some()
        } else {
            table.metadata().current_snapshot().is_some()
        };
        if !has_snapshot {
            let empty_batch = RecordBatch::new_empty(schema.clone());
            let stream = futures::stream::once(async move { Ok::<_, DataFusionError>(empty_batch) });
            return Ok(Box::pin(IcebergRecordBatchStream { schema, inner: Box::pin(stream), baseline }));
        }
        let stream = futures::stream::once(async move {
            let mut sb = table.scan();
            if let Some(sid) = snapshot_id { sb = sb.snapshot_id(sid); }
            if let Some(ref cols) = projection { sb = sb.select(cols.iter().map(|s| s.as_str())); }
            if let Some(pred) = predicates { sb = sb.with_filter(pred); }
            let scan = sb.build().map_err(|e| DataFusionError::External(Box::new(e)))?;
            let arrow_stream = scan.to_arrow().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok::<_, DataFusionError>(arrow_stream.map_err(|e| DataFusionError::External(Box::new(e))))
        }).try_flatten();
        Ok(Box::pin(IcebergRecordBatchStream { schema, inner: Box::pin(stream), baseline }))
    }
}

struct IcebergRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    baseline: BaselineMetrics,
}

impl Stream for IcebergRecordBatchStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = { let _timer = this.baseline.elapsed_compute().timer(); this.inner.as_mut().poll_next(cx) };
        if let Poll::Ready(Some(Ok(ref batch))) = poll { this.baseline.record_output(batch.num_rows()); }
        poll
    }
}

impl datafusion::physical_plan::RecordBatchStream for IcebergRecordBatchStream {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}

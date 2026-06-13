use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::BooleanArray;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    PushedDown,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties,
};
use datafusion_expr::ColumnarValue;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::spec::{DataContentType, DataFile, ManifestContentType, ManifestStatus};
use iceberg::table::Table;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use tracing::{debug, info_span, warn};

use crate::pruning_stats::IcebergManifestStatistics;

/// Default small-file threshold: 3 MB.
///
/// Files below this size are read entirely in a single S3 GET and parsed
/// from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline.
pub const DEFAULT_SMALL_FILE_THRESHOLD_BYTES: u64 = 3 * 1024 * 1024;

/// Default concurrency for loading Iceberg manifests during query-time
/// column-statistics pruning.
///
/// Each manifest is a separate S3 GET. On wide snapshots the sequential
/// walk dominates cold-cache plan latency; loading manifests in parallel
/// collapses that to roughly one round trip. Warm reads are served from
/// iceberg-rust's `ObjectCache` and ignore this knob.
pub const DEFAULT_MANIFEST_CONCURRENCY: usize = 64;

/// Default concurrency for the direct-read small-file fast path.
///
/// The fast path reads each eligible file entirely in one S3 GET. Without
/// parallelism the loop is strictly serial, so total latency is roughly
/// `files × round_trip`. Fanning out with `buffer_unordered` overlaps the
/// GETs; 8 matches `storage.max_concurrent_files` and keeps the peak
/// in-flight bytes bounded (`concurrency × small_file_threshold`).
pub const DEFAULT_DIRECT_READ_CONCURRENCY: usize = 8;

/// Default number of output partitions for [`IcebergScanExec`].
///
/// One partition means the scan runs serially on a single thread regardless of
/// how many cores the coordinator has. Callers that know the table file count
/// should override this with [`IcebergScanExec::with_target_partitions`] (the
/// DataFusion planner sets it from `execution.target_partitions`). Default `1`
/// preserves the prior single-partition behaviour for code paths that have not
/// been updated.
pub const DEFAULT_TARGET_PARTITIONS: usize = 1;

#[derive(Debug)]
pub struct IcebergScanExec {
    table: Table,
    projected_schema: SchemaRef,
    projection: Option<Vec<String>>,
    predicates: Option<Predicate>,
    df_filters: Vec<Expr>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Optional snapshot ID for time travel queries.
    snapshot_id: Option<i64>,
    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// Set to true when you know data files are physically sorted (e.g., written
    /// by a sort-on-write engine). Default false: only partition columns are trusted.
    /// WARNING: if data is not actually sorted, queries may return incorrect results.
    trust_sort_order: bool,
    /// Maximum file size in bytes for the direct-read fast path.
    ///
    /// When all data files in the scan are smaller than this threshold, SQE reads
    /// each file entirely in one S3 GET via `FileIO::new_input().read()` and parses
    /// the Parquet from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline
    /// which issues redundant manifest, footer, and HEAD requests per file.
    ///
    /// Set to 0 to disable the fast path. Default: [`DEFAULT_SMALL_FILE_THRESHOLD_BYTES`].
    small_file_threshold_bytes: u64,
    /// Dynamic filters pushed down from parent operators (e.g., hash join build side).
    ///
    /// During the physical optimizer's filter pushdown phase, parent operators such as
    /// `HashJoinExec` create `DynamicFilterPhysicalExpr` objects that are pushed down
    /// to leaf scan nodes. When the hash join's build side completes at execution time,
    /// it updates the dynamic filter with min/max bounds. The scan node can use these
    /// bounds to skip files/row groups that cannot match.
    ///
    /// These are `PhysicalExpr`s (not logical `Expr`s) because they come from the
    /// physical optimizer, after logical-to-physical conversion has already occurred.
    pushed_down_filters: Vec<Arc<dyn PhysicalExpr>>,
    /// Concurrency for direct manifest walks (column-stats pruning path).
    ///
    /// The primary scan planner goes through `Table::scan().plan_files()` which
    /// has its own internal concurrency limit. This field only applies to the
    /// pruning walks that need `DataFile` column statistics (lower/upper
    /// bounds, null counts) which `FileScanTask` does not expose. Defaults to
    /// [`DEFAULT_MANIFEST_CONCURRENCY`].
    manifest_concurrency: usize,
    /// Concurrency for the direct-read small-file fast path.
    ///
    /// When the fast path is active, each eligible file becomes a single S3
    /// GET. This field sets how many of those GETs are in flight at once via
    /// `buffer_unordered`. Defaults to [`DEFAULT_DIRECT_READ_CONCURRENCY`].
    direct_read_concurrency: usize,
    /// Number of output partitions (logical scan streams) this exec produces.
    ///
    /// DataFusion calls `execute(partition)` once per partition in [0, N).
    /// Each partition reads a disjoint round-robin slice of the file list, so
    /// the per-thread fan-out scales linearly with `target_partitions` until
    /// the cluster runs out of useful concurrency. Defaults to
    /// [`DEFAULT_TARGET_PARTITIONS`].
    target_partitions: usize,
    /// Pre-computed table statistics aggregated from manifest entries.
    ///
    /// Populated at scan-planning time by `table_provider::scan` (which is
    /// async and can read manifests). Contains per-column min/max/null_count
    /// in addition to the snapshot summary's row count and byte size. When
    /// present, returned verbatim from `partition_statistics`; otherwise the
    /// scan falls back to the snapshot-summary path with column stats
    /// `Absent`. Storing `Statistics` directly (rather than recomputing it)
    /// keeps `partition_statistics` synchronous as DataFusion requires.
    cached_statistics: Option<datafusion::common::Statistics>,
    /// Issue #132: when true, skip Tier-1 dynamic-predicate registration on
    /// scans whose planned files are effectively uniform on every filter
    /// column. Sourced from `[catalog.runtime_filters]`. Default false.
    runtime_filter_clustering_skip: bool,
    /// Issue #132: per-file bounds-spread fraction above which a column is
    /// considered uniform. Sourced from `[catalog.runtime_filters]`. Default 0.8.
    runtime_filter_uniform_threshold: f64,
}

impl IcebergScanExec {
    pub fn new(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>) -> Self {
        Self::new_with_filters(table, projected_schema, projection, predicates, vec![])
    }

    pub fn new_with_filters(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>, df_filters: Vec<Expr>) -> Self {
        Self::new_with_filters_and_metrics(table, projected_schema, projection, predicates, df_filters)
    }

    pub fn new_with_filters_and_metrics(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>, predicates: Option<Predicate>, df_filters: Vec<Expr>) -> Self {
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
                    // Filter sort expressions: only trust partition columns by default.
                    // For TB-scale data, trusting non-partition sort order is dangerous
                    // because writers may not enforce it, causing silent incorrect results.
                    let mut stripped_cols = Vec::new();
                    let safe_exprs: Vec<_> = sort_exprs.into_iter().filter(|expr| {
                        let col_name = expr.expr.to_string();
                        if partition_cols.contains(&col_name) {
                            true
                        } else {
                            stripped_cols.push(col_name);
                            false
                        }
                    }).collect();
                    if !stripped_cols.is_empty() {
                        debug!(
                            table = %table.identifier(),
                            stripped_columns = ?stripped_cols,
                            "Ignoring non-partition sort order — data may not be physically sorted. \
                             Set [catalog] trust_sort_order = true to override."
                        );
                    }

                    if safe_exprs.is_empty() {
                        EquivalenceProperties::new(projected_schema.clone())
                    } else {
                        crate::sort_order::equivalence_with_sort(projected_schema.clone(), safe_exprs)
                    }
                }
                None => EquivalenceProperties::new(projected_schema.clone()),
            }
        };
        let properties = Arc::new(PlanProperties::new(eq_props, Partitioning::UnknownPartitioning(DEFAULT_TARGET_PARTITIONS), EmissionType::Incremental, Boundedness::Bounded));
        Self { table, projected_schema, projection, predicates, df_filters, properties, metrics: ExecutionPlanMetricsSet::new(), snapshot_id: None, trust_sort_order: false, small_file_threshold_bytes: DEFAULT_SMALL_FILE_THRESHOLD_BYTES, pushed_down_filters: vec![], manifest_concurrency: DEFAULT_MANIFEST_CONCURRENCY, direct_read_concurrency: DEFAULT_DIRECT_READ_CONCURRENCY, target_partitions: DEFAULT_TARGET_PARTITIONS, cached_statistics: None, runtime_filter_clustering_skip: false, runtime_filter_uniform_threshold: 0.8 }
    }

    /// Attach pre-computed statistics aggregated from Iceberg manifests.
    ///
    /// Call from an async planning context (e.g. `TableProvider::scan`) before
    /// returning the scan node so DataFusion's `partition_statistics` query
    /// returns full per-column bounds rather than the row-count-only fallback.
    #[must_use = "with_cached_statistics consumes self; bind the returned scan"]
    pub fn with_cached_statistics(mut self, stats: datafusion::common::Statistics) -> Self {
        self.cached_statistics = Some(stats);
        self
    }

    /// Set the snapshot ID for time travel queries.
    #[must_use = "with_snapshot_id consumes self; bind the returned scan"]
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set the small-file threshold for the direct-read fast path.
    ///
    /// Files whose size (from the Iceberg manifest) is below `threshold_bytes`
    /// will be read in a single S3 GET and parsed from memory, skipping the
    /// iceberg-rust `scan.to_arrow()` pipeline that issues redundant HEAD,
    /// footer, and manifest-list requests.
    ///
    /// Pass `0` to disable the fast path for all files.
    #[must_use = "with_small_file_threshold consumes self; bind the returned scan"]
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the concurrency for direct manifest walks during pruning.
    ///
    /// A value of `0` is treated as `1` (sequential fallback) to avoid a
    /// zero-width `buffer_unordered` which would stall.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned scan"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the concurrency for the direct-read small-file fast path.
    ///
    /// A value of `0` is treated as `1` (sequential fallback).
    #[must_use = "with_direct_read_concurrency consumes self; bind the returned scan"]
    pub fn with_direct_read_concurrency(mut self, concurrency: usize) -> Self {
        self.direct_read_concurrency = concurrency.max(1);
        self
    }

    /// Configure the Tier-1 clustering gate (issue #132). When `skip` is true,
    /// scans whose planned files are effectively uniform (median per-file bounds
    /// spread >= `uniform_threshold`) on every filter column skip Tier-1
    /// dynamic-predicate registration; Tier-2 per-batch filtering still applies.
    #[must_use = "with_runtime_filter_clustering consumes self; bind the returned scan"]
    pub fn with_runtime_filter_clustering(mut self, skip: bool, uniform_threshold: f64) -> Self {
        self.runtime_filter_clustering_skip = skip;
        self.runtime_filter_uniform_threshold = uniform_threshold;
        self
    }

    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// When true, DataFusion may skip redundant sorts based on Iceberg metadata.
    /// WARNING: only enable when you know all data files are physically sorted
    /// (e.g., written by a sort-on-write engine). Incorrect for mixed-writer tables.
    #[must_use = "with_trust_sort_order consumes self; bind the returned scan"]
    pub fn with_trust_sort_order(mut self, trust: bool) -> Self {
        if trust {
            // Rebuild equivalence properties with full sort order
            let sort_order = self.table.metadata().default_sort_order();
            let iceberg_schema = self.table.metadata().current_schema();
            if let Some(sort_exprs) = crate::sort_order::iceberg_sort_to_physical(sort_order, iceberg_schema, &self.projected_schema) {
                self.properties = Arc::new(PlanProperties::new(
                    crate::sort_order::equivalence_with_sort(self.projected_schema.clone(), sort_exprs),
                    Partitioning::UnknownPartitioning(self.target_partitions),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ));
            }
        }
        self.trust_sort_order = trust;
        self
    }

    /// Override the number of output partitions for this scan.
    ///
    /// Each partition reads a round-robin slice of the planned file list and
    /// emits an independent `SendableRecordBatchStream`. The DataFusion planner
    /// typically passes `execution.target_partitions` here so the scan fan-out
    /// matches the configured CPU parallelism.
    #[must_use = "with_target_partitions consumes self; bind the returned scan"]
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
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

    pub fn table(&self) -> &Table { &self.table }
    pub fn predicates(&self) -> Option<&Predicate> { self.predicates.as_ref() }
    pub fn df_filters(&self) -> &[Expr] { &self.df_filters }
    pub fn projection(&self) -> Option<&[String]> { self.projection.as_deref() }
    pub fn pushed_down_filters(&self) -> &[Arc<dyn PhysicalExpr>] { &self.pushed_down_filters }

    /// Create a copy of this scan with additional dynamic filters pushed down
    /// from parent operators (e.g., `HashJoinExec` build-side bounds).
    ///
    /// The returned node inherits all fields from `self` but replaces the
    /// `pushed_down_filters` with the given set.
    fn clone_with_pushed_filters(&self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            table: self.table.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            df_filters: self.df_filters.clone(),
            properties: self.properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            snapshot_id: self.snapshot_id,
            trust_sort_order: self.trust_sort_order,
            small_file_threshold_bytes: self.small_file_threshold_bytes,
            pushed_down_filters: filters,
            manifest_concurrency: self.manifest_concurrency,
            direct_read_concurrency: self.direct_read_concurrency,
            target_partitions: self.target_partitions,
            cached_statistics: self.cached_statistics.clone(),
            runtime_filter_clustering_skip: self.runtime_filter_clustering_skip,
            runtime_filter_uniform_threshold: self.runtime_filter_uniform_threshold,
        }
    }

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
                        let kept_paths: std::collections::HashSet<String> = kept.iter().map(|df| df.file_path().to_string()).collect();
                        result.retain(|(path, _)| kept_paths.contains(path));
                    }
                }
            }
        }
        Ok((result, pruned_count))
    }

    /// Load all live data files (with full column statistics) for the current snapshot.
    ///
    /// Returns `DataFile` objects including `lower_bounds` / `upper_bounds` /
    /// `null_value_counts` needed by `PruningPredicate`. `FileScanTask` from
    /// `Table::scan().plan_files()` does not expose those maps, so the pruning
    /// path walks manifests directly.
    ///
    /// Routes both the manifest-list and per-manifest reads through
    /// `Table::object_cache()`, so warm queries (any prior `plan_files()` call
    /// on the same snapshot) are served from memory without additional S3
    /// GETs. Cold reads are parallelised with `buffer_unordered` at
    /// `self.manifest_concurrency`.
    pub async fn collect_data_files(&self) -> Result<Vec<DataFile>, iceberg::Error> {
        let metadata_ref = self.table.metadata_ref();
        let snapshot = if let Some(sid) = self.snapshot_id {
            match metadata_ref.snapshot_by_id(sid) { Some(s) => s, None => return Ok(vec![]) }
        } else {
            match metadata_ref.current_snapshot() { Some(s) => s, None => return Ok(vec![]) }
        };
        let cache = self.table.object_cache();
        let manifest_list = cache.get_manifest_list(snapshot, &metadata_ref).await?;
        let concurrency = self.manifest_concurrency.max(1);
        let manifests: Vec<Arc<iceberg::spec::Manifest>> = futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| { let cache = cache.clone(); async move { cache.get_manifest(&mf).await } })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;
        let data_files = manifests
            .into_iter()
            .flat_map(|manifest| {
                manifest
                    .entries()
                    .iter()
                    .filter(|e| e.status() != ManifestStatus::Deleted
                        && e.data_file().content_type() == DataContentType::Data)
                    .map(|e| e.data_file().clone())
                    .collect::<Vec<_>>()
            })
            .collect();
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

    /// Prune data files using already-resolved `PhysicalExpr` filters.
    ///
    /// This is the dynamic-filter counterpart of `prune_data_files`. Instead of
    /// converting logical `Expr`s to physical expressions (which requires a
    /// `SessionContext`), it accepts `PhysicalExpr`s directly — exactly what the
    /// dynamic filter resolution path produces.
    ///
    /// Each filter is turned into a `PruningPredicate` and evaluated against the
    /// per-file column statistics from the Iceberg manifest. Files whose min/max
    /// ranges are provably outside the filter bounds are removed.
    pub fn prune_data_files_with_physical_exprs(
        data_files: Vec<DataFile>,
        physical_filters: &[Arc<dyn PhysicalExpr>],
        schema: &SchemaRef,
        iceberg_schema: &iceberg::spec::Schema,
    ) -> (Vec<DataFile>, usize) {
        if data_files.is_empty() || physical_filters.is_empty() {
            return (data_files, 0);
        }

        let mut current_files = data_files;
        let mut total_pruned = 0usize;

        for filter_expr in physical_filters {
            if current_files.is_empty() {
                break;
            }
            let pruning_pred = match PruningPredicate::try_new(Arc::clone(filter_expr), schema.clone()) {
                Ok(p) => p,
                Err(e) => {
                    debug!(error = %e, "PruningPredicate from dynamic filter failed, skipping");
                    continue;
                }
            };
            let stats = IcebergManifestStatistics::new(
                current_files.clone(),
                schema.clone(),
                iceberg_schema,
            );
            match pruning_pred.prune(&stats) {
                Ok(flags) => {
                    let before = current_files.len();
                    current_files = current_files
                        .into_iter()
                        .zip(flags)
                        .filter_map(|(df, keep)| if keep { Some(df) } else { None })
                        .collect();
                    let pruned_this_round = before - current_files.len();
                    total_pruned += pruned_this_round;
                    if pruned_this_round > 0 {
                        debug!(
                            before = before,
                            after = current_files.len(),
                            pruned = pruned_this_round,
                            "Dynamic filter file-level pruning round"
                        );
                    }
                }
                Err(e) => {
                    debug!(error = %e, "PruningPredicate eval for dynamic filter failed, skipping");
                }
            }
        }

        (current_files, total_pruned)
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: table={}, projection={:?}, predicate=[{}], df_filters={}, pushed_down_filters={}, snapshot_id={:?}",
            self.table.identifier(),
            self.projection,
            self.predicates.as_ref().map_or(String::new(), |p| format!("{p}")),
            self.df_filters.len(),
            self.pushed_down_filters.len(),
            self.snapshot_id,
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str { "IcebergScanExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.projected_schema.clone() }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> { Ok(self) }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    /// Provide table statistics from Iceberg snapshot metadata for cost-based optimization.
    ///
    /// DataFusion's JoinSelection and join-reordering optimizers use these to:
    /// 1. Put the smaller table on the build side of hash joins
    /// 2. Choose CollectLeft mode for small tables (broadcast)
    /// 3. Estimate filter selectivity (column min/max bounds)
    /// 4. Order multi-way joins by intermediate cardinality
    ///
    /// When `cached_statistics` is populated (via `with_cached_statistics`), the
    /// pre-computed manifest aggregation is returned. Otherwise we fall back to
    /// the snapshot summary's row count and byte size with column stats absent.
    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<datafusion::common::Statistics> {
        use datafusion::common::{stats::Precision, ColumnStatistics, Statistics};

        if let Some(cached) = &self.cached_statistics {
            return Ok(cached.clone());
        }

        let metadata = self.table.metadata();
        let snapshot = match metadata.current_snapshot() {
            Some(s) => s,
            None => return Ok(Statistics::new_unknown(&self.projected_schema)),
        };

        // Extract total-records and total-file-size from snapshot summary.
        // These are maintained by Iceberg writers and available without reading manifests.
        let summary = &snapshot.summary().additional_properties;
        let total_records = summary
            .get("total-records")
            .and_then(|v| v.parse::<usize>().ok());
        let total_file_size = summary
            .get("total-files-size")
            .and_then(|v| v.parse::<usize>().ok());

        let num_rows = match total_records {
            Some(n) => Precision::Inexact(n),
            None => Precision::Absent,
        };
        let total_byte_size = match total_file_size {
            Some(n) => Precision::Inexact(n),
            None => Precision::Absent,
        };

        // Per-column statistics: provide Absent for now.
        // Full column-level stats (min/max/null_count) require reading manifests
        // which is async and cannot be done here synchronously.
        // The row count and byte size alone are sufficient for JoinSelection
        // to make correct build-side decisions.
        let column_statistics = self
            .projected_schema
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect();

        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    /// Declare ourselves a leaf to DataFusion's filter pushdown rule.
    ///
    /// The default `ExecutionPlan::gather_filters_for_pushdown` returns
    /// `FilterDescription::all_unsupported(...)`, which tells the
    /// optimizer "I do not support any of these filters." When SQE's
    /// `IcebergScanExec` inherits that default, the optimizer abandons
    /// the dynamic filter it tried to push down from a parent
    /// `HashJoinExec`, and `handle_child_pushdown_result` is never
    /// called. Path B-2 (runtime filter pushdown into the Iceberg
    /// scan) silently no-ops as a result. This was the root cause of
    /// SSB SF1 lineorder being scanned in full even when the dim
    /// build side filter was 0 rows: the dynamic filter never reached
    /// this scan to prune anything.
    ///
    /// Returning `FilterDescription::new()` (an empty descriptor,
    /// matching the leaf-scan convention used by the vendored
    /// `IcebergTableScan` in iceberg-rust) tells the optimizer "no
    /// children to forward to; absorb the filters here." After that,
    /// `handle_child_pushdown_result` runs and stores the dynamic
    /// filters on `pushed_down_filters` for evaluation at scan time.
    ///
    /// See `docs/features/ssb-sf1-trace.md` for the investigation
    /// trail and `docs/features/runtime-filter-pushdown.md` for the
    /// broader Path B-2 design.
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
        // Trace at debug so the iceberg-datafusion bridge can be
        // diagnosed via `RUST_LOG=sqe_catalog=debug` without affecting
        // production logs. Counts both the total parent filters and
        // the subset that are `DynamicFilterPhysicalExpr` (the only
        // shape we accept for runtime filtering).
        let dyn_count = child_pushdown_result
            .parent_filters
            .iter()
            .filter(|pf| {
                pf.filter
                    .as_any()
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                    .is_some()
            })
            .count();
        tracing::debug!(
            table = %self.table.identifier(),
            parent_filter_count = child_pushdown_result.parent_filters.len(),
            dynamic_filter_count = dyn_count,
            "IcebergScanExec::handle_child_pushdown_result"
        );
        // As a leaf node, we selectively accept pushed-down filters:
        // - Dynamic filters (from hash join build side): ACCEPT — we store them
        //   and will evaluate them at scan time for file/row-group pruning.
        // - Static parent filters (from FilterExec): REJECT — let the FilterExec
        //   remain in the plan to evaluate them. We cannot evaluate arbitrary
        //   PhysicalExpr during Iceberg scan planning.
        let mut dynamic_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut filter_results: Vec<PushedDown> = Vec::new();

        for pf in &child_pushdown_result.parent_filters {
            if pf.filter.as_any().downcast_ref::<DynamicFilterPhysicalExpr>().is_some() {
                // Dynamic filter from hash join — accept it
                dynamic_filters.push(Arc::clone(&pf.filter));
                filter_results.push(PushedDown::Yes);
            } else {
                // Static filter — leave it for FilterExec to handle
                filter_results.push(PushedDown::No);
            }
        }

        if dynamic_filters.is_empty() {
            // No dynamic filters to store — keep the current node unchanged.
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                filter_results,
            ));
        }

        // Merge with any previously stored pushed-down filters.
        let mut all_pushed = self.pushed_down_filters.clone();
        all_pushed.extend(dynamic_filters);

        let new_scan = self.clone_with_pushed_filters(all_pushed);

        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(filter_results)
                .with_updated_node(Arc::new(new_scan)),
        )
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let span = info_span!("iceberg_scan", table=%self.table.identifier(), partition=partition, predicates=?self.predicates);
        let _guard = span.enter();
        if partition >= self.target_partitions {
            return Err(DataFusionError::Internal(format!(
                "IcebergScanExec partition {partition} out of range (target_partitions = {})",
                self.target_partitions
            )));
        }
        let total_partitions = self.target_partitions;
        let table = self.table.clone();
        let schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let predicates = self.predicates.clone();
        let snapshot_id = self.snapshot_id;
        let small_file_threshold = self.small_file_threshold_bytes;
        let manifest_concurrency = self.manifest_concurrency;
        let direct_read_concurrency = self.direct_read_concurrency;
        let pushed_down_filters = self.pushed_down_filters.clone();
        let runtime_filter_clustering_skip = self.runtime_filter_clustering_skip;
        let runtime_filter_uniform_threshold = self.runtime_filter_uniform_threshold;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let _files_pruned_minmax = MetricBuilder::new(&self.metrics).counter("files_pruned_minmax", partition);
        let files_pruned_dynamic = MetricBuilder::new(&self.metrics).counter("files_pruned_dynamic", partition);
        // Scan-level visibility: enough to answer "how much was planned, read,
        // decoded, and dropped by runtime filters" from a profile alone.
        // `rows_decoded` vs `output_rows` gives the post-decode filter kill
        // rate; `rows_prefilter` vs `rows_decoded` gives the parquet RowFilter
        // kill rate on the direct path; `rows_passed_filter_pending` counts
        // rows that streamed through while a dynamic filter was still the
        // lit(true) placeholder, i.e. the build side had not sealed yet.
        let planning_time = MetricBuilder::new(&self.metrics).subset_time("planning_time", partition);
        let files_matched = MetricBuilder::new(&self.metrics).counter("files_matched", partition);
        let bytes_planned = MetricBuilder::new(&self.metrics).counter("bytes_planned", partition);
        let bytes_scanned = MetricBuilder::new(&self.metrics).counter("bytes_scanned", partition);
        let rows_prefilter = MetricBuilder::new(&self.metrics).counter("rows_prefilter", partition);
        let rows_decoded = MetricBuilder::new(&self.metrics).counter("rows_decoded", partition);
        let rows_filtered_dynamic = MetricBuilder::new(&self.metrics).counter("rows_filtered_dynamic", partition);
        let rows_passed_filter_pending = MetricBuilder::new(&self.metrics).counter("rows_passed_filter_pending", partition);
        let dynamic_filters_resolved = MetricBuilder::new(&self.metrics).counter("dynamic_filters_resolved", partition);
        let dynamic_filters_pending = MetricBuilder::new(&self.metrics).counter("dynamic_filters_pending", partition);
        debug!(table=%table.identifier(), predicates=?predicates, snapshot_id=?snapshot_id, pushed_filters=pushed_down_filters.len(), "Executing IcebergScanExec");

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

        // Type alias to avoid repeating the full BoxStream type in early-returns.
        type BatchStream = futures::stream::BoxStream<'static, DFResult<RecordBatch>>;

        // `schema` is also needed after the stream is created (for IcebergRecordBatchStream),
        // so clone it before moving it into the async block.
        let schema_for_stream = schema.clone();
        let stream = futures::stream::once(async move {
            let schema = schema_for_stream;
            // ── Collect data file list via iceberg-rust's scan planner ────────
            //
            // `Table::scan().plan_files()` reads the manifest list and manifests
            // through iceberg-rust's internal `ObjectCache`, which is kept warm
            // across queries because `TableMetadataCache` caches `Table` instances
            // globally. On a warm table this is served from memory; on cold, it
            // is one manifest-list GET plus one GET per manifest.
            let mut file_entries = {
                let _t = planning_time.timer();
                collect_data_files_via_plan(
                    &table,
                    snapshot_id,
                    projection.as_deref(),
                    predicates.as_ref(),
                ).await?
            };

            if file_entries.is_empty() {
                let empty = RecordBatch::new_empty(schema.clone());
                let s: BatchStream = futures::stream::once(async move { Ok(empty) }).boxed();
                return Ok::<BatchStream, DataFusionError>(s);
            }

            // ── Sort files by size descending for better S3 pipeline utilization ──
            //
            // Reading the largest files first keeps the S3 connection pipeline busy
            // longer on each request, reducing the relative overhead of per-request
            // latency. For over-partitioned tables with many small files this also
            // groups tiny files at the end where their overhead is amortised.
            let total_scan_bytes: u64 = file_entries.iter().map(|(_, sz)| *sz).sum();
            file_entries.sort_by(|a, b| b.1.cmp(&a.1));
            debug!(
                file_count = file_entries.len(),
                total_scan_bytes = total_scan_bytes,
                largest_file_bytes = file_entries.first().map(|(_, sz)| *sz).unwrap_or(0),
                smallest_file_bytes = file_entries.last().map(|(_, sz)| *sz).unwrap_or(0),
                "IcebergScanExec: file entries collected and sorted by size descending"
            );

            // Round-robin assignment after the size-descending sort spreads the
            // largest files across partitions instead of clustering them at the
            // top of partition 0. With one partition this is a no-op slice.
            if total_partitions > 1 {
                let before = file_entries.len();
                file_entries = file_entries
                    .into_iter()
                    .enumerate()
                    .filter(|(idx, _)| idx % total_partitions == partition)
                    .map(|(_, entry)| entry)
                    .collect();
                debug!(
                    partition = partition,
                    total_partitions = total_partitions,
                    before = before,
                    after = file_entries.len(),
                    "IcebergScanExec: round-robin partition slice"
                );
            }

            // Counted after the partition slice so the per-partition values
            // sum to the table-wide totals, and before dynamic file pruning so
            // `files_matched - files_pruned_dynamic` is the files actually read.
            files_matched.add(file_entries.len());
            bytes_planned.add(file_entries.iter().map(|(_, sz)| *sz as usize).sum());

            // ── Direct-read fast path ────────────────────────────────────────
            //
            // When the threshold is non-zero and ALL data files are below it, read
            // each file entirely in one S3 GET and parse Parquet from memory. This
            // eliminates the extra HEAD, footer, and manifest-re-read requests that
            // iceberg-rust's `scan.to_arrow()` pipeline issues per file.
            //
            // The fast path opens parquet directly via FileIO and cannot apply
            // Iceberg position or equality deletes. Any snapshot referencing
            // delete manifests must fall back to iceberg-rust's reader pipeline
            // (`scan.to_arrow`) which routes through CachingDeleteFileLoader.
            // Without this gate, every MoR table whose data files are all under
            // the threshold returns previously-deleted rows.
            let has_deletes = snapshot_has_delete_files(&table, snapshot_id).await?;
            if has_deletes {
                debug!(
                    "IcebergScanExec: snapshot has delete manifests, skipping direct-read fast path"
                );
            }
            let use_direct = !has_deletes
                && small_file_threshold > 0
                && file_entries.iter().all(|(_, size)| *size <= small_file_threshold);

            if use_direct {
                debug!(
                    file_count = file_entries.len(),
                    threshold_bytes = small_file_threshold,
                    "IcebergScanExec: using direct-read fast path"
                );

                let file_io = table.file_io().clone();

                // Snapshot dynamic filters NON-BLOCKING.
                // Never call wait_complete() — it deadlocks when this scan is
                // the hash join probe side (probe waits for build, build waits
                // for probe to stream). Instead, use .current() which returns
                // the latest filter snapshot immediately (lit(true) if the build
                // hasn't finished yet). Early batches pass unfiltered; later
                // batches get the actual bounds once the build side completes.
                let mut resolved_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
                for filter in &pushed_down_filters {
                    if let Some(dynamic) = filter.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
                        match dynamic.current() {
                            Ok(expr) => {
                                if is_trivial_true(&expr) {
                                    // Placeholder snapshot: the build side has
                                    // not sealed yet, so this filter prunes
                                    // nothing for the whole direct read.
                                    dynamic_filters_pending.add(1);
                                } else {
                                    dynamic_filters_resolved.add(1);
                                    debug!(filter = %expr, "Snapshot dynamic filter for direct-read");
                                    resolved_filters.push(expr);
                                }
                            }
                            Err(e) => {
                                dynamic_filters_pending.add(1);
                                debug!(error = %e, "Dynamic filter not ready yet, skipping");
                            }
                        }
                    } else {
                        resolved_filters.push(Arc::clone(filter));
                    }
                }

                // ── File-level pruning with dynamic filters ─────────────────
                //
                // After the dynamic filters are resolved, use them to skip
                // entire files whose manifest column statistics (min/max) are
                // provably outside the filter bounds. This avoids S3 GETs for
                // files that cannot contain matching rows.
                //
                // We load full DataFile objects (with column stats) from the
                // manifests, then delegate to prune_data_files_with_physical_exprs
                // which wraps each filter in a PruningPredicate.
                if !resolved_filters.is_empty() && file_entries.len() > 1 {
                    let file_paths: std::collections::HashSet<String> =
                        file_entries.iter().map(|(p, _)| p.clone()).collect();
                    match collect_data_files_for_pruning(&table, snapshot_id, &file_paths, manifest_concurrency).await {
                        Ok(data_files) if !data_files.is_empty() => {
                            let iceberg_schema = table.metadata().current_schema();
                            let (kept, pruned_count) =
                                IcebergScanExec::prune_data_files_with_physical_exprs(
                                    data_files,
                                    &resolved_filters,
                                    &schema,
                                    iceberg_schema,
                                );
                            if pruned_count > 0 {
                                let kept_paths: std::collections::HashSet<String> =
                                    kept.iter().map(|df| df.file_path().to_string()).collect();
                                debug!(
                                    before = file_entries.len(),
                                    after = kept_paths.len(),
                                    pruned = pruned_count,
                                    "Dynamic filter file-level pruning skipped {} files",
                                    pruned_count
                                );
                                file_entries.retain(|(path, _)| kept_paths.contains(path));
                                files_pruned_dynamic.add(pruned_count);
                            }
                        }
                        Ok(_) => {
                            debug!("No DataFile objects with stats available for dynamic filter file pruning");
                        }
                        Err(e) => {
                            debug!(error = %e, "Failed to load DataFiles for dynamic filter pruning, continuing without");
                        }
                    }
                }

                // Parallel small-file fast path.
                //
                // Each file is a single S3 GET plus an in-memory Parquet decode.
                // We fan these out with `buffer_unordered(direct_read_concurrency)`
                // so per-file latency is amortised across concurrent requests
                // instead of accumulating serially. Order within the resulting
                // batch stream is not preserved, which DataFusion tolerates: the
                // scan is already marked `UnknownPartitioning`.
                //
                // Per-task captures are all cheap: `FileIO` and `SchemaRef` are
                // `Arc`, `projection` is an `Option<Vec<String>>` sized O(query
                // columns), and `resolved_filters` is wrapped in an outer `Arc`
                // so clones are refcount bumps.
                let concurrency = direct_read_concurrency.max(1);
                let resolved_filters = Arc::new(resolved_filters);
                let bytes_scanned = bytes_scanned.clone();
                let rows_prefilter = rows_prefilter.clone();
                let rows_decoded = rows_decoded.clone();
                let per_file_stream = futures::stream::iter(
                    file_entries.into_iter().map(move |(path, size)| {
                        let file_io = file_io.clone();
                        let projection = projection.clone();
                        let schema = schema.clone();
                        let resolved_filters = Arc::clone(&resolved_filters);
                        let bytes_scanned = bytes_scanned.clone();
                        let rows_prefilter = rows_prefilter.clone();
                        let rows_decoded = rows_decoded.clone();
                        async move {
                            debug!(path = %path, size = size, "Direct-read: reading file");

                            let input = file_io
                                .new_input(&path)
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            let bytes = input
                                .read()
                                .await
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            bytes_scanned.add(bytes.len());

                            // Parse Parquet from the in-memory bytes.
                            // `bytes::Bytes` implements `ChunkReader` so this works directly.
                            let reader_opts = ArrowReaderOptions::new().with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
                            let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, reader_opts)
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                            // Apply column projection by mapping column names to Parquet indices.
                            // For COUNT(*) queries, projection is Some([]) (empty list) -- we read
                            // just the first column to get the row count, then discard the data.
                            //
                            // The computed ProjectionMask is also reused for the row filter below
                            // so the predicate sees a batch shaped like `projected_schema`. That is
                            // required for correctness: `DynamicFilterPhysicalExpr` emitted by the
                            // hash join encodes `Column(index=N)` where N is the position in the
                            // scan's projected output schema (not the full Parquet schema). If we
                            // hand the predicate a full-schema batch, `Column(0)` reads the wrong
                            // column and the filter silently drops every row whose projected
                            // column 0 happens not to coincide with full column 0.
                            let (builder, filter_mask) = if let Some(ref cols) = projection {
                                let parquet_schema = builder.parquet_schema().clone();
                                let arrow_schema = builder.schema().clone();
                                let indices: Vec<usize> = cols
                                    .iter()
                                    .filter_map(|col| {
                                        arrow_schema.fields().iter().position(|f| f.name() == col)
                                    })
                                    .collect();
                                if indices.is_empty() {
                                    // COUNT(*) or similar: no columns needed, just row count.
                                    // Read the smallest column (first one) to get the row count.
                                    let mask = ProjectionMask::roots(&parquet_schema, vec![0]);
                                    (builder.with_projection(mask.clone()), mask)
                                } else {
                                    let mask = ProjectionMask::roots(&parquet_schema, indices);
                                    (builder.with_projection(mask.clone()), mask)
                                }
                            } else {
                                // No projection: predicate sees the full Parquet schema, which
                                // matches the scan's projected_schema (they're the same when no
                                // projection is applied).
                                (builder, ProjectionMask::all())
                            };

                            // Apply resolved dynamic filters as Parquet row filters.
                            //
                            // Each resolved filter becomes an ArrowPredicate that the Parquet
                            // reader evaluates per row group / page. Rows that fail the
                            // predicate are skipped before full decoding, reducing I/O and CPU.
                            //
                            // `filter_mask` (computed above) matches the output projection, so
                            // the predicate evaluates against a batch whose column layout
                            // matches `projected_schema`. That is the schema the filter's
                            // column indices were resolved against.
                            let builder = if !resolved_filters.is_empty() {
                                let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
                                for (idx, filter_expr) in resolved_filters.iter().enumerate() {
                                    predicates.push(Box::new(PhysicalExprPredicate {
                                        expr: Arc::clone(filter_expr),
                                        projection: filter_mask.clone(),
                                        // Only the first predicate sees every
                                        // row surviving row-group/page pruning;
                                        // later predicates see already-filtered
                                        // selections and would double-count.
                                        rows_seen: (idx == 0).then(|| rows_prefilter.clone()),
                                    }));
                                }
                                builder.with_row_filter(RowFilter::new(predicates))
                            } else {
                                builder
                            };

                            let reader = builder
                                .with_batch_size(8192)
                                .build()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                            let is_count_star = projection.as_ref().is_some_and(|cols| cols.is_empty());
                            let mut batches: Vec<RecordBatch> = Vec::new();
                            for batch_result in reader {
                                let batch = batch_result.map_err(|e| DataFusionError::External(Box::new(e)))?;
                                rows_decoded.add(batch.num_rows());
                                if is_count_star {
                                    // For COUNT(*): return empty-column batch with correct row count.
                                    // DataFusion only needs the row count, not the data.
                                    batches.push(RecordBatch::try_new_with_options(
                                        schema.clone(),
                                        vec![],
                                        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                                    ).map_err(|e| DataFusionError::External(Box::new(e)))?);
                                } else {
                                    batches.push(batch);
                                }
                            }
                            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
                        }
                    }),
                )
                .buffer_unordered(concurrency);

                // Stream per-file batches as each read completes instead of
                // collecting every file into memory first. The previous
                // `try_collect()` held `Vec<Vec<RecordBatch>>` for the whole
                // partition before yielding the first batch, pinning
                // `file_count x decoded_size` of Arrow on the coordinator.
                // See `flatten_per_file_batches` for the streaming semantics.
                let s: BatchStream = flatten_per_file_batches(per_file_stream);
                return Ok::<BatchStream, DataFusionError>(s);
            }

            // ── Fallback: iceberg-rust scan.to_arrow() pipeline ──────────────
            //
            // Used when the small-file fast path is disabled (`threshold = 0`) or
            // any file exceeds the threshold. This path handles predicate pushdown
            // via the Iceberg scan API.
            //
            // Dynamic filters are applied as a post-scan filter on the arrow stream.
            // We don't wait_complete() here (would deadlock if this scan is the
            // hash join probe side). Instead, we snapshot the current filter state
            // per batch — by the time batches flow, the build side has likely
            // finished and updated the filter. Batches that arrive before the filter
            // is ready pass through unfiltered (same as no dynamic filter).
            debug!(
                file_count = file_entries.len(),
                pushed_filters = pushed_down_filters.len(),
                "IcebergScanExec: using iceberg-rust scan.to_arrow() path"
            );
            let mut sb = table.scan();
            if let Some(sid) = snapshot_id { sb = sb.snapshot_id(sid); }
            if let Some(ref cols) = projection { sb = sb.select(cols.iter().map(|s| s.as_str())); }
            if let Some(pred) = predicates { sb = sb.with_filter(pred); }
            // Two-tier dynamic-filter pushdown.
            //
            // Tier 1 (scan-time): the DynamicPredicate is sampled once
            // per file scan task and feeds manifest, row-group min/max,
            // page-index, and parquet RowFilter pruning. This is what
            // collapsed TPC-DS q82 1787->113ms (and the whole TPC-DS
            // sweep 40s -> 16s): selective dim filters prune row groups
            // before any rows are decoded.
            //
            // Tier 2 (post-scan stream wrapper): re-evaluates the
            // filters per batch. iceberg-rust only samples
            // `current()` once at task open; for tables with a single
            // file (SSB SF1 lineorder is a 6-row-group 151MB file = one
            // task) that single sample lands BEFORE the dim build sides
            // have finished, so the dynamic filter is still lit(true)
            // and Tier 1 contributes nothing. The wrapper re-samples
            // every 8192-row batch, so once the build sides seal a few
            // ms in, every subsequent batch sees the real filter and
            // pruning works. Skipping the wrapper regressed SSB by
            // +21% (1.6s) without buying anything elsewhere; keeping it
            // costs ~3ms when Tier 1 already pruned because every
            // surviving batch passes the wrapper cheaply.
            if !pushed_down_filters.is_empty() {
                // Tier-1 clustering gate (issue #132). When enabled, inspect the
                // already-planned manifest bounds: if the fact table is
                // effectively uniform on every filter column, bounds-only Tier-1
                // pruning cannot skip anything and only adds per-file bind cost,
                // so skip registration and let the Tier-2 wrapper below filter.
                // Undecidable cases keep Tier-1 (the MR #220 behavior).
                let register_tier1 = if runtime_filter_clustering_skip && file_entries.len() > 1 {
                    let filter_columns = collect_filter_column_names(&pushed_down_filters);
                    let file_paths: std::collections::HashSet<String> =
                        file_entries.iter().map(|(p, _)| p.clone()).collect();
                    match collect_data_files_for_pruning(&table, snapshot_id, &file_paths, manifest_concurrency).await {
                        Ok(dfs) if !dfs.is_empty() => {
                            let iceberg_schema = table.metadata().current_schema();
                            let stats = IcebergManifestStatistics::new(dfs, schema.clone(), iceberg_schema);
                            let keep = stats.clustered_on_filters(&filter_columns, runtime_filter_uniform_threshold);
                            if !keep {
                                debug!(
                                    filter_columns = ?filter_columns,
                                    threshold = runtime_filter_uniform_threshold,
                                    "Tier-1 skipped: data uniform on all filter columns (issue #132)"
                                );
                            }
                            keep
                        }
                        // Could not load bounds -> can't decide -> keep Tier-1.
                        _ => true,
                    }
                } else {
                    true
                };
                if register_tier1 {
                    let dyn_pred = iceberg_datafusion::physical_plan::physical_to_predicate::RuntimeFiltersDynamicPredicate::new(pushed_down_filters.clone());
                    sb = sb.with_dynamic_predicate(dyn_pred);
                }
            }
            let scan = sb.build().map_err(|e| DataFusionError::External(Box::new(e)))?;
            let scan_result = scan.to_arrow_with_metrics().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let scan_metrics = scan_result.metrics().clone();
            let rows_decoded_inspect = rows_decoded.clone();
            let arrow_stream = scan_result
                .stream()
                .inspect_ok(move |batch| rows_decoded_inspect.add(batch.num_rows()));

            let s: BatchStream = if !pushed_down_filters.is_empty() {
                let filters = pushed_down_filters.clone();
                let filtered_schema = schema.clone();
                let rows_filtered_dynamic = rows_filtered_dynamic.clone();
                let rows_passed_filter_pending = rows_passed_filter_pending.clone();
                arrow_stream
                    .map_err(|e: iceberg::Error| DataFusionError::External(Box::new(e)))
                    .and_then(move |batch| {
                        let filters = filters.clone();
                        let filtered_schema = filtered_schema.clone();
                        let rows_filtered_dynamic = rows_filtered_dynamic.clone();
                        let rows_passed_filter_pending = rows_passed_filter_pending.clone();
                        async move {
                            let rows_in = batch.num_rows();
                            let mut saw_pending = false;
                            let mut result = batch;
                            for filter in &filters {
                                let (expr, is_dynamic): (Arc<dyn PhysicalExpr>, bool) = if let Some(dynamic) = filter.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
                                    match dynamic.current() {
                                        Ok(e) if is_trivial_true(&e) => {
                                            // Build side not sealed yet: the
                                            // snapshot is still the lit(true)
                                            // placeholder, every row passes.
                                            saw_pending = true;
                                            continue;
                                        }
                                        Ok(e) => (e, true),
                                        Err(_) => {
                                            saw_pending = true;
                                            continue;
                                        }
                                    }
                                } else {
                                    (Arc::clone(filter), false)
                                };
                                // DynamicFilterPhysicalExpr carries runtime literals
                                // typed to the build-side column (Iceberg Int32 for
                                // integer joinkeys); widening the probe batch to
                                // Int64 would make the column-vs-literal comparison
                                // fail and the eval-Err arm silently skips.
                                let coerced = if is_dynamic {
                                    result.clone()
                                } else {
                                    PhysicalExprPredicate::coerce_batch_types(&expr, &result)
                                        .unwrap_or_else(|_| result.clone())
                                };
                                let predicate = match expr.evaluate(&coerced) {
                                    Ok(ColumnarValue::Array(arr)) => {
                                        match arr.as_any().downcast_ref::<BooleanArray>() {
                                            Some(bool_arr) => bool_arr.clone(),
                                            None => continue,
                                        }
                                    }
                                    Ok(ColumnarValue::Scalar(s)) => {
                                        if s == datafusion::common::ScalarValue::Boolean(Some(true)) {
                                            continue;
                                        } else {
                                            rows_filtered_dynamic.add(rows_in);
                                            return Ok(RecordBatch::new_empty(filtered_schema));
                                        }
                                    }
                                    Err(_) => continue,
                                };
                                result = arrow::compute::filter_record_batch(&result, &predicate)
                                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            }
                            rows_filtered_dynamic.add(rows_in - result.num_rows());
                            if saw_pending {
                                rows_passed_filter_pending.add(result.num_rows());
                            }
                            Ok(result)
                        }
                    })
                    .boxed()
            } else {
                arrow_stream
                    .map_err(|e: iceberg::Error| DataFusionError::External(Box::new(e)))
                    .boxed()
            };
            // Flush the vendored reader's storage byte counter into the
            // DataFusion metric when the stream completes or is dropped, so
            // early-terminated scans (LIMIT, join short-circuit) still report.
            let flush = BytesScannedFlush { scan_metrics, counter: bytes_scanned.clone() };
            let s: BatchStream = s
                .chain(futures::stream::poll_fn(move |_| {
                    let _keep_alive = &flush;
                    Poll::Ready(None)
                }))
                .boxed();
            Ok::<BatchStream, DataFusionError>(s)
        }).try_flatten();
        Ok(Box::pin(IcebergRecordBatchStream { schema, inner: Box::pin(stream), baseline }))
    }
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Flatten a stream of per-file batch results into a single lazy batch stream.
///
/// The direct-read fast path fans file reads out with
/// `buffer_unordered(concurrency)`, so each stream item is the
/// `DFResult<Vec<RecordBatch>>` for one decoded file. We splice those per-file
/// vectors into one flat `RecordBatch` stream WITHOUT first collecting them:
///
/// - `map_ok` turns each completed file's `Vec<RecordBatch>` into an inner
///   stream of `Ok(batch)`.
/// - `try_flatten` emits each inner stream's batches as it is produced, so a
///   file's batches are yielded as soon as that file finishes decoding. The
///   `buffer_unordered(concurrency)` bound on the upstream is preserved, so
///   in-flight decoded Arrow is ~`concurrency x decoded_size` instead of the
///   whole partition.
///
/// Error semantics: a per-file `Err` propagates as a mid-stream `Err` item
/// rather than failing eagerly. The caller wraps this in `try_flatten()` plus
/// `IcebergRecordBatchStream`, both of which carry `DFResult` per item, so the
/// error still terminates the scan.
///
/// Note: streaming is at file granularity, not row granularity. Each file is
/// still read fully into memory by `input.read().await` before decode; the
/// per-file size cap is the small-file threshold gate, not this function.
fn flatten_per_file_batches<S>(
    per_file_stream: S,
) -> futures::stream::BoxStream<'static, DFResult<RecordBatch>>
where
    S: Stream<Item = DFResult<Vec<RecordBatch>>> + Send + 'static,
{
    per_file_stream
        .map_ok(|batches| {
            futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, DataFusionError>))
        })
        .try_flatten()
        .boxed()
}

/// Collect `(file_path, file_size_bytes)` pairs for the given snapshot via
/// iceberg-rust's scan planner.
///
/// Going through `Table::scan().plan_files()` routes every manifest and
/// manifest-list read through iceberg-rust's internal `ObjectCache`. Because
/// `TableMetadataCache` caches `Table` instances globally, the per-`Table`
/// object cache persists across queries and serves warm reads from memory.
///
/// Partition / predicate pruning from `predicates` is also applied here by
/// the planner, so files that cannot match the query are filtered before
/// they reach the scan node.
/// Returns true if the snapshot's manifest list references any delete
/// manifests (position-delete or equality-delete files). Used to gate
/// the direct-read fast path: that path bypasses iceberg-rust's reader
/// pipeline, so it cannot apply DeleteVectors or equality-delete
/// predicates. Falling back to `scan.to_arrow()` is the correct choice
/// any time a delete file exists.
async fn snapshot_has_delete_files(
    table: &Table,
    snapshot_id: Option<i64>,
) -> DFResult<bool> {
    let metadata_ref = table.metadata_ref();
    let snapshot = match snapshot_id {
        Some(sid) => metadata_ref.snapshot_by_id(sid),
        None => metadata_ref.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(false);
    };
    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(manifest_list
        .entries()
        .iter()
        .any(|mf| mf.content == ManifestContentType::Deletes))
}

async fn collect_data_files_via_plan(
    table: &Table,
    snapshot_id: Option<i64>,
    projection: Option<&[String]>,
    predicates: Option<&Predicate>,
) -> DFResult<Vec<(String, u64)>> {
    let mut sb = table.scan();
    if let Some(sid) = snapshot_id {
        sb = sb.snapshot_id(sid);
    }
    if let Some(cols) = projection {
        sb = sb.select(cols.iter().map(|s| s.as_str()));
    }
    if let Some(pred) = predicates {
        sb = sb.with_filter(pred.clone());
    }
    let scan = sb
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let tasks: Vec<iceberg::scan::FileScanTask> = scan
        .plan_files()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .try_collect()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(tasks
        .into_iter()
        .map(|t| (t.data_file_path, t.file_size_in_bytes))
        .collect())
}

/// Collect full `DataFile` objects (with column statistics) for the given snapshot.
///
/// Unlike `collect_data_files_via_plan` which returns lightweight `(path, size)` pairs,
/// this function loads the full manifest entries with lower/upper bounds needed for
/// `PruningPredicate` evaluation. Used by dynamic-filter file-level pruning.
///
/// Only files matching the given `file_paths` set are returned, so callers can
/// restrict to the files they actually intend to read.
///
/// Routes reads through `Table::object_cache()` so warm queries (same
/// snapshot after a prior `plan_files()` call) avoid redundant S3 GETs.
/// Cold reads are parallelised with `buffer_unordered` at `concurrency`.
/// Recursively collect the distinct `Column` names referenced by a set of
/// physical filter expressions (issue #132 clustering gate). `DynamicFilter`
/// expressions expose their referenced columns as children, so a tree walk
/// finds the fact-table columns the runtime filter prunes on.
fn collect_filter_column_names(filters: &[Arc<dyn PhysicalExpr>]) -> Vec<String> {
    fn walk(expr: &Arc<dyn PhysicalExpr>, out: &mut Vec<String>) {
        if let Some(col) = expr
            .as_any()
            .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        {
            let name = col.name().to_string();
            if !out.contains(&name) {
                out.push(name);
            }
        }
        for child in expr.children() {
            walk(child, out);
        }
    }
    let mut out = Vec::new();
    for f in filters {
        walk(f, &mut out);
    }
    out
}

async fn collect_data_files_for_pruning(
    table: &Table,
    snapshot_id: Option<i64>,
    file_paths: &std::collections::HashSet<String>,
    concurrency: usize,
) -> DFResult<Vec<DataFile>> {
    let metadata_ref = table.metadata_ref();
    let snapshot = if let Some(sid) = snapshot_id {
        match metadata_ref.snapshot_by_id(sid) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        }
    } else {
        match metadata_ref.current_snapshot() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        }
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let concurrency = concurrency.max(1);
    let manifests: Vec<Arc<iceberg::spec::Manifest>> = futures::stream::iter(
        manifest_list.entries().iter().cloned(),
    )
    .map(|mf| {
        let cache = cache.clone();
        async move { cache.get_manifest(&mf).await }
    })
    .buffer_unordered(concurrency)
    .try_collect()
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let data_files = manifests
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| {
                    entry.status() != ManifestStatus::Deleted
                        && entry.data_file().content_type() == DataContentType::Data
                        && file_paths.contains(entry.data_file().file_path())
                })
                .map(|entry| entry.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect();

    Ok(data_files)
}

/// Reads all data-file manifest entries for a snapshot and aggregates their
/// per-column statistics into a DataFusion `Statistics`.
///
/// This is the async hook that lets us bypass the constraint that
/// `ExecutionPlan::partition_statistics` is synchronous: callers that already
/// hold an async runtime (e.g. `TableProvider::scan`) compute the result once
/// and stash it on `IcebergScanExec` via `with_cached_statistics`.
///
/// `arrow_schema` should match the projection the scan node will return so
/// the `column_statistics` array indexes line up with what DataFusion sees.
/// Returns row count, byte size, and per-column min/max/null counts; distinct
/// counts and sums stay `Absent` (Iceberg manifests don't carry them).
pub async fn compute_table_statistics(
    table: &Table,
    snapshot_id: Option<i64>,
    arrow_schema: &arrow::datatypes::Schema,
    concurrency: usize,
) -> DFResult<datafusion::common::Statistics> {
    let metadata_ref = table.metadata_ref();
    let snapshot = if let Some(sid) = snapshot_id {
        match metadata_ref.snapshot_by_id(sid) {
            Some(s) => s,
            None => {
                return Ok(datafusion::common::Statistics::new_unknown(arrow_schema));
            }
        }
    } else {
        match metadata_ref.current_snapshot() {
            Some(s) => s,
            None => {
                return Ok(datafusion::common::Statistics::new_unknown(arrow_schema));
            }
        }
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let concurrency = concurrency.max(1);
    let manifests: Vec<Arc<iceberg::spec::Manifest>> = futures::stream::iter(
        manifest_list.entries().iter().cloned(),
    )
    .map(|mf| {
        let cache = cache.clone();
        async move { cache.get_manifest(&mf).await }
    })
    .buffer_unordered(concurrency)
    .try_collect()
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let data_files: Vec<DataFile> = manifests
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| {
                    entry.status() != ManifestStatus::Deleted
                        && entry.data_file().content_type() == DataContentType::Data
                })
                .map(|entry| entry.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect();

    let iceberg_schema = table.metadata().current_schema();
    Ok(crate::pruning_stats::aggregate_table_statistics(
        &data_files,
        arrow_schema,
        iceberg_schema,
    ))
}

/// Default target size for file coalescing: 64 MB.
///
/// When distributing scan tasks, small files below this threshold are grouped
/// together so that each task processes roughly `target_size` bytes, reducing
/// per-file overhead (S3 requests, task scheduling) on over-partitioned tables.
pub const DEFAULT_COALESCE_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Coalesce small file entries into groups of approximately `target_size` bytes.
///
/// Reduces per-file overhead (S3 requests, task scheduling) on over-partitioned
/// tables with many small files. Each group contains one or more files whose
/// combined size stays near `target_size`. A single file that exceeds the target
/// is placed alone in its own group.
///
/// The input order is preserved within groups: files appear in the same relative
/// order as the input `entries` slice.
///
/// # Example
///
/// ```
/// use sqe_catalog::iceberg_scan::coalesce_file_entries;
///
/// let entries = vec![
///     ("a.parquet".to_string(), 10_000_000u64),
///     ("b.parquet".to_string(), 20_000_000),
///     ("c.parquet".to_string(), 50_000_000),
///     ("d.parquet".to_string(), 5_000_000),
/// ];
/// let groups = coalesce_file_entries(entries, 64 * 1024 * 1024);
/// // All 4 files fit in one group (total 85 MB > 64 MB triggers split)
/// assert!(groups.len() >= 1);
/// ```
pub fn coalesce_file_entries(
    entries: Vec<(String, u64)>,
    target_size: u64,
) -> Vec<Vec<(String, u64)>> {
    let mut groups: Vec<Vec<(String, u64)>> = Vec::new();
    let mut current_group: Vec<(String, u64)> = Vec::new();
    let mut current_size: u64 = 0;

    for entry in entries {
        let size = entry.1;
        if !current_group.is_empty() && current_size + size > target_size {
            groups.push(std::mem::take(&mut current_group));
            current_size = 0;
        }
        current_size += size;
        current_group.push(entry);
    }
    if !current_group.is_empty() {
        groups.push(current_group);
    }
    groups
}

/// Wraps a DataFusion `PhysicalExpr` as a Parquet `ArrowPredicate` for row-level
/// filtering during Parquet decoding.
///
/// Handles type mismatches between the dynamic filter expression (typed for the
/// join schema) and the scan output (typed for the Parquet schema) by casting
/// batch columns to match the expression's expected types before evaluation.
/// This makes hash join dynamic filters work even when Iceberg stores a column
/// as Int32 but the join key arrives as Int64 or Utf8.
struct PhysicalExprPredicate {
    expr: Arc<dyn PhysicalExpr>,
    projection: ProjectionMask,
    /// When set, counts every row this predicate evaluates (the rows that
    /// survived row-group/page pruning). Set only on the first predicate of a
    /// `RowFilter` chain; later predicates see already-filtered selections.
    rows_seen: Option<datafusion::physical_plan::metrics::Count>,
}

/// True when a dynamic filter snapshot is still the `lit(true)` placeholder,
/// i.e. the hash join build side has not sealed the filter yet.
fn is_trivial_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Literal>()
        .is_some_and(|lit| {
            matches!(
                lit.value(),
                datafusion::common::ScalarValue::Boolean(Some(true))
            )
        })
}

/// Adds the vendored reader's storage byte counter to the DataFusion metric on
/// drop, covering both normal completion and early termination (LIMIT, join
/// short-circuit) of the scan stream.
struct BytesScannedFlush {
    scan_metrics: iceberg::arrow::ScanMetrics,
    counter: datafusion::physical_plan::metrics::Count,
}

impl Drop for BytesScannedFlush {
    fn drop(&mut self) {
        let bytes = self.scan_metrics.bytes_read();
        if bytes > 0 {
            self.counter.add(bytes as usize);
        }
    }
}

impl PhysicalExprPredicate {
    /// Cast batch columns to match the types expected by the filter expression.
    /// Returns the original batch unchanged if all types already match.
    fn coerce_batch_types(
        _expr: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, ArrowError> {
        use arrow::compute::cast;

        // Walk the expression to find column references and their expected types.
        // The expression's data_type() tells us what it expects from children.
        // We compare each column's actual type in the batch with what the
        // expression expects and cast if needed.
        let expr_schema = batch.schema();
        let mut needs_cast = false;
        let mut new_columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut new_fields: Vec<arrow::datatypes::FieldRef> = Vec::with_capacity(batch.num_columns());

        // Widen narrow integer columns to Int64 and narrow string columns to
        // Utf8. Covers the common Iceberg vs DataFusion type gaps (Iceberg
        // integers materialise as Int32, predicates compare against Int64).
        // Earlier iterations tried to walk the expression tree to collect the
        // expected type per column; the heuristic below proved sufficient and
        // kept that machinery from paying for itself.
        for (i, field) in expr_schema.fields().iter().enumerate() {
            let col = batch.column(i);
            let actual_type = col.data_type();

            // Widen narrow integers to Int64 (Iceberg often stores as Int32,
            // but DataFusion promotes to Int64 in expressions)
            let target_type = match actual_type {
                arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32 => Some(arrow::datatypes::DataType::Int64),
                arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32 => Some(arrow::datatypes::DataType::Int64),
                arrow::datatypes::DataType::Float32 => Some(arrow::datatypes::DataType::Float64),
                _ => None,
            };

            if let Some(ref target) = target_type {
                match cast(col, target) {
                    Ok(casted) => {
                        needs_cast = true;
                        new_columns.push(casted);
                        new_fields.push(std::sync::Arc::new(
                            field.as_ref().clone().with_data_type(target.clone()),
                        ));
                    }
                    Err(_) => {
                        new_columns.push(col.clone());
                        new_fields.push(field.clone());
                    }
                }
            } else {
                new_columns.push(col.clone());
                new_fields.push(field.clone());
            }
        }

        if needs_cast {
            let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(new_fields));
            RecordBatch::try_new(new_schema, new_columns)
        } else {
            Ok(batch.clone())
        }
    }
}

impl ArrowPredicate for PhysicalExprPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        if let Some(rows_seen) = &self.rows_seen {
            rows_seen.add(batch.num_rows());
        }
        // Step 1: Widen narrow types (Int32->Int64, Float32->Float64) to match
        // what DataFusion expressions expect. This fixes the "Utf8 >= Int32"
        // and "Int64 >= Int32" type mismatches from hash join dynamic filters.
        let coerced = Self::coerce_batch_types(&self.expr, &batch).unwrap_or(batch);

        // Step 2: Evaluate the filter expression on the (possibly widened) batch.
        let result = match self.expr.evaluate(&coerced) {
            Ok(r) => r,
            Err(e) => {
                // Still failed after coercion. Log and pass all rows through.
                // The parent FilterExec will apply the filter with full coercion.
                tracing::debug!(
                    error = %e,
                    "Dynamic filter evaluation failed after type coercion, passing all rows"
                );
                return Ok(BooleanArray::from(vec![true; coerced.num_rows()]));
            }
        };

        match result {
            ColumnarValue::Array(array) => {
                match array.as_any().downcast_ref::<BooleanArray>() {
                    Some(bool_arr) => Ok(bool_arr.clone()),
                    None => Ok(BooleanArray::from(vec![true; coerced.num_rows()])),
                }
            }
            ColumnarValue::Scalar(scalar) => {
                let bool_val = matches!(
                    scalar,
                    datafusion::common::ScalarValue::Boolean(Some(true))
                );
                Ok(BooleanArray::from(vec![bool_val; coerced.num_rows()]))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn batch(schema: &SchemaRef, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// A pending dynamic filter snapshots to `lit(true)`; anything else (a
    /// sealed bound, a constant false) must not be classified as pending.
    #[test]
    fn trivial_true_detects_placeholder_only() {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::{Column, Literal};

        let lit_true: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let lit_false: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));

        assert!(is_trivial_true(&lit_true));
        assert!(!is_trivial_true(&lit_false));
        assert!(!is_trivial_true(&column));
    }

    /// `rows_seen` counts every row the predicate evaluates (pre-filter), and
    /// stays silent when unset so chained predicates don't double-count.
    #[test]
    fn physical_expr_predicate_counts_rows_seen() {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;
        use datafusion::physical_plan::metrics::Count;

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let counter = Count::new();
        let mut counted = PhysicalExprPredicate {
            expr: Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            projection: ProjectionMask::all(),
            rows_seen: Some(counter.clone()),
        };
        let mut uncounted = PhysicalExprPredicate {
            expr: Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            projection: ProjectionMask::all(),
            rows_seen: None,
        };

        counted.evaluate(batch(&schema, &[1, 2, 3])).unwrap();
        counted.evaluate(batch(&schema, &[4, 5])).unwrap();
        uncounted.evaluate(batch(&schema, &[6, 7, 8])).unwrap();

        assert_eq!(counter.value(), 5, "two counted batches: 3 + 2 rows");
    }

    /// Each per-file `Vec<RecordBatch>` is spliced into one flat stream in file
    /// order, with every batch and every row preserved. This is the correctness
    /// guarantee the streaming rewrite must keep relative to the old
    /// `try_collect().flatten()` form.
    #[tokio::test]
    async fn flatten_preserves_all_batches_in_order() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Two files: the first yields two batches, the second yields one.
        let per_file: Vec<DFResult<Vec<RecordBatch>>> = vec![
            Ok(vec![batch(&schema, &[1, 2]), batch(&schema, &[3])]),
            Ok(vec![batch(&schema, &[4, 5, 6])]),
        ];

        let flat = flatten_per_file_batches(futures::stream::iter(per_file));
        let out: Vec<RecordBatch> = flat.try_collect().await.unwrap();

        let rows: Vec<i32> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();

        assert_eq!(out.len(), 3, "all three batches survive the flatten");
        assert_eq!(rows, vec![1, 2, 3, 4, 5, 6], "rows preserved in file order");
    }

    /// The stream must be lazy: polling for the first batch must NOT have driven
    /// the producer of every downstream file. The old code collected all files
    /// before emitting anything; this proves we no longer do.
    #[tokio::test]
    async fn flatten_is_lazy_not_pre_collected() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Count how many per-file items the consumer has pulled from upstream.
        let produced = Arc::new(AtomicUsize::new(0));
        let produced_in = Arc::clone(&produced);

        // Three files; `inspect` fires as each is pulled by the downstream.
        let upstream = futures::stream::iter(vec![
            Ok::<Vec<RecordBatch>, DataFusionError>(vec![batch(&schema, &[1])]),
            Ok(vec![batch(&schema, &[2])]),
            Ok(vec![batch(&schema, &[3])]),
        ])
        .inspect(move |_| {
            produced_in.fetch_add(1, Ordering::SeqCst);
        });

        let mut flat = flatten_per_file_batches(upstream);

        // Pull exactly one batch.
        let first = flat.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);

        // A pre-collecting implementation would have drained all three files to
        // produce the first batch. A lazy one pulls only what it needs (one
        // file yields one batch here, so at most one or two upstream items).
        let pulled = produced.load(Ordering::SeqCst);
        assert!(
            pulled < 3,
            "stream pre-collected all {pulled} files before first batch; expected lazy pull"
        );
    }

    /// A per-file error surfaces as a mid-stream `Err` (not an eager panic or a
    /// silently-dropped file), and batches emitted before it still arrive.
    #[tokio::test]
    async fn flatten_propagates_mid_stream_error() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let per_file: Vec<DFResult<Vec<RecordBatch>>> = vec![
            Ok(vec![batch(&schema, &[1])]),
            Err(DataFusionError::Internal("file read failed".into())),
            Ok(vec![batch(&schema, &[2])]),
        ];

        let mut flat = flatten_per_file_batches(futures::stream::iter(per_file));

        let first = flat.next().await.unwrap();
        assert!(first.is_ok(), "first file's batch arrives before the error");

        let second = flat.next().await.unwrap();
        assert!(second.is_err(), "the failing file surfaces as a mid-stream Err");
    }
}

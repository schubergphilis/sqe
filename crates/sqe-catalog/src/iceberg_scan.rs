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
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use tracing::{debug, info_span, warn};

use crate::manifest_cache::{ManifestCache, ManifestEntryData};
use crate::pruning_stats::IcebergManifestStatistics;

/// Default small-file threshold: 3 MB.
///
/// Files below this size are read entirely in a single S3 GET and parsed
/// from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline.
pub const DEFAULT_SMALL_FILE_THRESHOLD_BYTES: u64 = 3 * 1024 * 1024;

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
    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// Set to true when you know data files are physically sorted (e.g., written
    /// by a sort-on-write engine). Default false: only partition columns are trusted.
    /// WARNING: if data is not actually sorted, queries may return incorrect results.
    trust_sort_order: bool,
    /// Optional shared manifest file cache. When set, parsed manifest entries are
    /// served from the cache on warm queries, avoiding S3 round-trips per manifest.
    /// Immutability of Iceberg manifest files makes this safe without a TTL.
    manifest_cache: Option<ManifestCache>,
    /// Maximum file size in bytes for the direct-read fast path.
    ///
    /// When all data files in the scan are smaller than this threshold, SQE reads
    /// each file entirely in one S3 GET via `FileIO::new_input().read()` and parses
    /// the Parquet from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline
    /// which issues redundant manifest, footer, and HEAD requests per file.
    ///
    /// Set to 0 to disable the fast path. Default: [`DEFAULT_SMALL_FILE_THRESHOLD_BYTES`].
    small_file_threshold_bytes: u64,
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
                    // Filter sort expressions: only trust partition columns by default.
                    // For TB-scale data, trusting non-partition sort order is dangerous
                    // because writers may not enforce it, causing silent incorrect results.
                    let safe_exprs: Vec<_> = sort_exprs.into_iter().filter(|expr| {
                        let col_name = expr.expr.to_string();
                        if partition_cols.contains(&col_name) {
                            true
                        } else {
                            warn!(
                                table = %table.identifier(),
                                column = %col_name,
                                "Ignoring non-partition sort order -- data may not be physically sorted. \
                                 Set [catalog] trust_sort_order = true to trust Iceberg sort metadata."
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
        Self { table, projected_schema, projection, predicates, df_filters, properties, metrics: ExecutionPlanMetricsSet::new(), prom_metrics, snapshot_id: None, trust_sort_order: false, manifest_cache: None, small_file_threshold_bytes: DEFAULT_SMALL_FILE_THRESHOLD_BYTES }
    }

    /// Attach a shared manifest file cache for warm-query acceleration.
    ///
    /// When set, `collect_data_files()` checks the cache before fetching each
    /// manifest from S3. Safe without a TTL because Iceberg manifest files are
    /// immutable by specification.
    pub fn with_manifest_cache(mut self, cache: ManifestCache) -> Self {
        self.manifest_cache = Some(cache);
        self
    }

    /// Set the snapshot ID for time travel queries.
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
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Trust Iceberg sort order metadata for ALL columns, not just partition keys.
    /// When true, DataFusion may skip redundant sorts based on Iceberg metadata.
    /// WARNING: only enable when you know all data files are physically sorted
    /// (e.g., written by a sort-on-write engine). Incorrect for mixed-writer tables.
    pub fn with_trust_sort_order(mut self, trust: bool) -> Self {
        if trust {
            // Rebuild equivalence properties with full sort order
            let sort_order = self.table.metadata().default_sort_order();
            let iceberg_schema = self.table.metadata().current_schema();
            if let Some(sort_exprs) = crate::sort_order::iceberg_sort_to_physical(sort_order, iceberg_schema, &self.projected_schema) {
                self.properties = PlanProperties::new(
                    crate::sort_order::equivalence_with_sort(self.projected_schema.clone(), sort_exprs),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                );
            }
        }
        self.trust_sort_order = trust;
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
            // Populate the cache on first load; subsequent callers that only need
            // file paths + sizes (not full column stats) can use data_file_info()
            // which is backed by iceberg's plan_files() API.
            //
            // Note: collect_data_files() is used by the min/max pruning path which
            // needs full DataFile objects with column statistics. Therefore we always
            // load the manifest from S3 here. The cache provides a fast-path skip
            // only for manifests known to be empty (contains no live data files).
            if let Some(ref mc) = self.manifest_cache {
                if let Some(cached_entries) = mc.get(&mf.manifest_path) {
                    if cached_entries.is_empty() {
                        // Skip S3 fetch for known-empty manifests.
                        debug!(manifest = %mf.manifest_path, "Manifest cache: skip empty manifest");
                        continue;
                    }
                    // Non-empty: fall through to load full DataFile for pruning stats.
                }
            }

            let manifest = mf.load_manifest(self.table.file_io()).await?;

            // Populate the cache so data_file_info() and empty-manifest skipping
            // can benefit on warm queries.
            if let Some(ref mc) = self.manifest_cache {
                if mc.get(&mf.manifest_path).is_none() {
                    let cache_entries: Vec<ManifestEntryData> = manifest
                        .entries()
                        .iter()
                        .filter(|e| {
                            e.status() != ManifestStatus::Deleted
                                && e.data_file().content_type() == DataContentType::Data
                        })
                        .map(|e| {
                            let df = e.data_file();
                            ManifestEntryData {
                                file_path: df.file_path().to_string(),
                                file_size: df.file_size_in_bytes(),
                                record_count: df.record_count(),
                                content_type: df.content_type(),
                                status: e.status(),
                            }
                        })
                        .collect();
                    debug!(
                        manifest = %mf.manifest_path,
                        entries = cache_entries.len(),
                        "Manifest cache: populated"
                    );
                    mc.insert(mf.manifest_path.clone(), cache_entries);
                }
            }

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
        let manifest_cache = self.manifest_cache.clone();
        let small_file_threshold = self.small_file_threshold_bytes;
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

        // Type alias to avoid repeating the full BoxStream type in early-returns.
        type BatchStream = futures::stream::BoxStream<'static, DFResult<RecordBatch>>;

        // `schema` is also needed after the stream is created (for IcebergRecordBatchStream),
        // so clone it before moving it into the async block.
        let schema_for_stream = schema.clone();
        let stream = futures::stream::once(async move {
            let schema = schema_for_stream;
            // ── Collect data file list from our cache-backed path ────────────
            //
            // `collect_data_files_cached` reads the snapshot's manifest list (1 S3 GET
            // on first call) and then serves each manifest's entries from the
            // ManifestCache, avoiding redundant S3 fetches on warm queries.
            let file_entries = collect_data_files_cached(&table, snapshot_id, manifest_cache.as_ref()).await?;

            if file_entries.is_empty() {
                let empty = RecordBatch::new_empty(schema.clone());
                let s: BatchStream = futures::stream::once(async move { Ok(empty) }).boxed();
                return Ok::<BatchStream, DataFusionError>(s);
            }

            // ── Direct-read fast path ────────────────────────────────────────
            //
            // When the threshold is non-zero and ALL data files are below it, read
            // each file entirely in one S3 GET and parse Parquet from memory. This
            // eliminates the extra HEAD, footer, and manifest-re-read requests that
            // iceberg-rust's `scan.to_arrow()` pipeline issues per file.
            let use_direct = small_file_threshold > 0
                && file_entries.iter().all(|(_, size)| *size <= small_file_threshold);

            if use_direct {
                debug!(
                    file_count = file_entries.len(),
                    threshold_bytes = small_file_threshold,
                    "IcebergScanExec: using direct-read fast path"
                );

                let file_io = table.file_io().clone();
                let mut all_batches: Vec<RecordBatch> = Vec::new();

                for (path, size) in &file_entries {
                    debug!(path = %path, size = size, "Direct-read: reading file");

                    let input = file_io
                        .new_input(path)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let bytes = input
                        .read()
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    // Parse Parquet from the in-memory bytes.
                    // `bytes::Bytes` implements `ChunkReader` so this works directly.
                    let reader_opts = ArrowReaderOptions::new().with_page_index(true);
                    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, reader_opts)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    // Apply column projection by mapping column names to Parquet indices.
                    let builder = if let Some(ref cols) = projection {
                        let parquet_schema = builder.parquet_schema().clone();
                        let arrow_schema = builder.schema().clone();
                        let indices: Vec<usize> = cols
                            .iter()
                            .filter_map(|col| {
                                arrow_schema.fields().iter().position(|f| f.name() == col)
                            })
                            .collect();
                        if indices.is_empty() {
                            builder
                        } else {
                            let mask = ProjectionMask::roots(&parquet_schema, indices);
                            builder.with_projection(mask)
                        }
                    } else {
                        builder
                    };

                    let reader = builder
                        .with_batch_size(8192)
                        .build()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    for batch_result in reader {
                        all_batches.push(
                            batch_result.map_err(|e| DataFusionError::External(Box::new(e)))?,
                        );
                    }
                }

                debug!(batch_count = all_batches.len(), "Direct-read: scan complete");
                let s: BatchStream = futures::stream::iter(
                    all_batches.into_iter().map(Ok::<RecordBatch, DataFusionError>)
                ).boxed();
                return Ok::<BatchStream, DataFusionError>(s);
            }

            // ── Fallback: iceberg-rust scan.to_arrow() pipeline ──────────────
            //
            // Used when the small-file fast path is disabled (`threshold = 0`) or
            // any file exceeds the threshold. This is the original path and handles
            // predicate pushdown via the Iceberg scan API.
            debug!(
                file_count = file_entries.len(),
                "IcebergScanExec: using iceberg-rust scan.to_arrow() path"
            );
            let mut sb = table.scan();
            if let Some(sid) = snapshot_id { sb = sb.snapshot_id(sid); }
            if let Some(ref cols) = projection { sb = sb.select(cols.iter().map(|s| s.as_str())); }
            if let Some(pred) = predicates { sb = sb.with_filter(pred); }
            let scan = sb.build().map_err(|e| DataFusionError::External(Box::new(e)))?;
            let arrow_stream = scan.to_arrow().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let s: BatchStream = arrow_stream
                .map_err(|e: iceberg::Error| DataFusionError::External(Box::new(e)))
                .boxed();
            Ok::<BatchStream, DataFusionError>(s)
        }).try_flatten();
        Ok(Box::pin(IcebergRecordBatchStream { schema, inner: Box::pin(stream), baseline }))
    }
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Collect the set of live data files for the given snapshot using
/// the ManifestCache where possible.
///
/// Returns `(file_path, file_size_bytes)` pairs.  The function loads the
/// snapshot's manifest *list* from S3 (one unavoidable GET on the first query
/// for a given snapshot) and then serves each individual manifest's entries
/// from the cache, avoiding per-manifest S3 fetches on warm queries.
async fn collect_data_files_cached(
    table: &Table,
    snapshot_id: Option<i64>,
    manifest_cache: Option<&ManifestCache>,
) -> DFResult<Vec<(String, u64)>> {
    let metadata = table.metadata();
    let snapshot = if let Some(sid) = snapshot_id {
        match metadata.snapshot_by_id(sid) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        }
    } else {
        match metadata.current_snapshot() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        }
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut file_entries: Vec<(String, u64)> = Vec::new();

    for mf in manifest_list.entries() {
        // Fast path: serve from cache when available.
        if let Some(mc) = manifest_cache {
            if let Some(cached) = mc.get(&mf.manifest_path) {
                for entry in cached.iter() {
                    if entry.status != ManifestStatus::Deleted
                        && entry.content_type == DataContentType::Data
                    {
                        file_entries.push((entry.file_path.clone(), entry.file_size));
                    }
                }
                continue; // Cache hit — skip S3 read for this manifest.
            }
        }

        // Cache miss (or no cache): load manifest from S3.
        let manifest = mf
            .load_manifest(table.file_io())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Populate the cache so subsequent calls are served from memory.
        if let Some(mc) = manifest_cache {
            if mc.get(&mf.manifest_path).is_none() {
                let cache_entries: Vec<ManifestEntryData> = manifest
                    .entries()
                    .iter()
                    .filter(|e| {
                        e.status() != ManifestStatus::Deleted
                            && e.data_file().content_type() == DataContentType::Data
                    })
                    .map(|e| {
                        let df = e.data_file();
                        ManifestEntryData {
                            file_path: df.file_path().to_string(),
                            file_size: df.file_size_in_bytes(),
                            record_count: df.record_count(),
                            content_type: df.content_type(),
                            status: e.status(),
                        }
                    })
                    .collect();
                debug!(
                    manifest = %mf.manifest_path,
                    entries = cache_entries.len(),
                    "collect_data_files_cached: populated manifest cache"
                );
                mc.insert(mf.manifest_path.clone(), cache_entries);
            }
        }

        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Deleted
                && entry.data_file().content_type() == DataContentType::Data
            {
                let df = entry.data_file();
                file_entries.push((df.file_path().to_string(), df.file_size_in_bytes()));
            }
        }
    }

    Ok(file_entries)
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

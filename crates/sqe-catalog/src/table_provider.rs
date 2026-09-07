use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::stats::Precision;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::statistics::StatisticsRequest;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use tracing::{debug, warn};

use crate::expr_to_predicate;

/// DataFusion `TableProvider` that wraps an Iceberg `Table`.
///
/// This provider converts the Iceberg table schema to an Arrow schema and
/// makes the table queryable through DataFusion's SQL engine. The scan
/// method returns an `IcebergScanExec` that reads data from Iceberg tables
/// via iceberg-rust's scan API and S3/FileIO.
///
/// Note: We implement our own `TableProvider` rather than using
/// `iceberg-datafusion::IcebergTableProvider` to retain the per-user Table
/// object (with vended S3 credentials). For catalog-backed access,
/// `iceberg-datafusion::IcebergTableProvider` can be used via `SessionCatalogBridge`.
#[derive(Debug, Clone)]
pub struct SqeTableProvider {
    /// The underlying Iceberg table.
    table: Table,
    /// Arrow schema derived from the Iceberg table's current schema.
    schema: ArrowSchemaRef,
    /// Optional Prometheus metrics for file pruning and S3 I/O counters.
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Optional snapshot ID for time travel queries.
    snapshot_id: Option<i64>,
    /// Trust Iceberg sort order for all columns (not just partition keys).
    trust_sort_order: bool,
    /// Small-file threshold in bytes for the direct-read fast path.
    small_file_threshold_bytes: u64,
    /// Concurrency for direct manifest walks during pruning.
    manifest_concurrency: usize,
    /// In-flight prefetch concurrency for the direct-read small-file fast path.
    /// Wired through to `IcebergScanExec::direct_read_concurrency`.
    prefetch_concurrency: usize,
    /// Issue #132: Tier-1 dynamic-filter clustering gate, from
    /// `[catalog.runtime_filters]`. When `skip` is true, scans whose files are
    /// uniform on every filter column skip Tier-1 registration.
    runtime_filter_clustering_skip: bool,
    runtime_filter_uniform_threshold: f64,
    /// Bounded wait (ms) at scan open for pending dynamic filters, from
    /// `[catalog.runtime_filters] wait_ms`.
    runtime_filter_wait_ms: u64,
    /// Issue #369: bloom-filter row-group probing of sealed runtime
    /// filters, from `[catalog.runtime_filters] bloom_probe` /
    /// `bloom_max_values`.
    runtime_filter_bloom_probe: bool,
    runtime_filter_bloom_max_values: usize,
    /// Fetch/decode pipelining depth (sqe#scan-fetch-pipeline), from `[catalog]
    /// scan_fetch_ahead`. `None` = scan default (3x cores); `Some(0)` =
    /// staging disabled; `Some(n)` = absolute depth.
    scan_fetch_ahead: Option<usize>,
}

impl SqeTableProvider {
    /// Create a new table provider from an Iceberg table at its current
    /// snapshot and current schema.
    ///
    /// Converts the Iceberg schema to an Arrow schema so DataFusion can
    /// understand the table structure. For time travel use
    /// [`Self::try_new_at_snapshot`] (or `try_new(..).with_snapshot_id(..)`,
    /// which re-derives the schema from that snapshot).
    pub async fn try_new(table: Table) -> sqe_core::Result<Self> {
        Self::try_new_inner(table, None)
    }

    /// Create a provider pinned to `snapshot_id`, declaring the schema that
    /// snapshot was written with rather than the table's current schema
    /// (architecture review 312). Columns added since the snapshot are
    /// absent; columns dropped since are present. Falls back to the current
    /// schema only when the snapshot carries no `schema-id`.
    pub async fn try_new_at_snapshot(table: Table, snapshot_id: i64) -> sqe_core::Result<Self> {
        Self::try_new_inner(table, Some(snapshot_id))
    }

    fn try_new_inner(table: Table, snapshot_id: Option<i64>) -> sqe_core::Result<Self> {
        let table_name = table.identifier().name().to_string();
        debug!(table = %table_name, snapshot_id, "Creating SqeTableProvider");

        let schema = Self::arrow_schema_for(&table, snapshot_id)?;

        Ok(Self {
            table,
            schema,
            prom_metrics: None,
            trust_sort_order: false,
            snapshot_id,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            runtime_filter_clustering_skip: false,
            runtime_filter_uniform_threshold: 0.8,
            runtime_filter_wait_ms: crate::iceberg_scan::DEFAULT_RUNTIME_FILTER_WAIT_MS,
            runtime_filter_bloom_probe: true,
            runtime_filter_bloom_max_values: 65536,
            scan_fetch_ahead: None,
        })
    }

    /// Attach Prometheus metrics for file pruning and S3 I/O.
    #[must_use = "with_metrics consumes self; bind the returned provider"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    ///
    /// Files below this size are read entirely in a single S3 GET and parsed
    /// from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline.
    #[must_use = "with_small_file_threshold consumes self; bind the returned provider"]
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned provider"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the in-flight prefetch concurrency for the direct-read small-file
    /// fast path. Maps to `IcebergScanExec::direct_read_concurrency` and is
    /// fed from `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.prefetch_concurrency = concurrency.max(1);
        self
    }

    /// Configure the Tier-1 dynamic-filter clustering gate (issue #132) from
    /// `[catalog.runtime_filters]`. Threads through to `IcebergScanExec`.
    #[must_use = "with_runtime_filter_clustering consumes self; bind the returned provider"]
    pub fn with_runtime_filter_clustering(mut self, skip: bool, uniform_threshold: f64) -> Self {
        self.runtime_filter_clustering_skip = skip;
        self.runtime_filter_uniform_threshold = uniform_threshold;
        self
    }

    /// Bounded wait (ms) at scan open for pending dynamic filters, from
    /// `[catalog.runtime_filters] wait_ms`. Threads through to `IcebergScanExec`.
    #[must_use = "with_runtime_filter_wait_ms consumes self; bind the returned provider"]
    pub fn with_runtime_filter_wait_ms(mut self, wait_ms: u64) -> Self {
        self.runtime_filter_wait_ms = wait_ms;
        self
    }

    /// Bloom-filter (SBBF) row-group probing of sealed runtime filters
    /// (issue #369), from `[catalog.runtime_filters]`. Threads through to
    /// `IcebergScanExec`.
    #[must_use = "with_runtime_filter_bloom consumes self; bind the returned provider"]
    pub fn with_runtime_filter_bloom(mut self, enabled: bool, max_values: usize) -> Self {
        self.runtime_filter_bloom_probe = enabled;
        self.runtime_filter_bloom_max_values = max_values;
        self
    }

    /// Fetch/decode pipelining depth (sqe#scan-fetch-pipeline), from `[catalog]
    /// scan_fetch_ahead`. Threads through to `IcebergScanExec`. `None`
    /// keeps the scan's default (3x cores); `Some(0)` disables staging.
    #[must_use = "with_scan_fetch_ahead consumes self; bind the returned provider"]
    pub fn with_scan_fetch_ahead(mut self, fetch_ahead: Option<usize>) -> Self {
        self.scan_fetch_ahead = fetch_ahead;
        self
    }

    /// Pin this provider to a specific Iceberg snapshot for time travel queries.
    ///
    /// Re-derives the declared Arrow schema from the snapshot's `schema-id`
    /// so DataFusion plans against the shape the data had, not the shape the
    /// table has now (architecture review 312). Apply it right after
    /// `try_new`, before any projection is taken from `schema()`; the
    /// builder order `try_new(..).with_snapshot_id(..)` is the supported
    /// one, and [`Self::try_new_at_snapshot`] does both in one step.
    #[must_use = "with_snapshot_id consumes self; bind the returned provider"]
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        match Self::arrow_schema_for(&self.table, Some(snapshot_id)) {
            Ok(schema) => self.schema = schema,
            Err(e) => warn!(
                table = %self.table.identifier(),
                snapshot_id,
                error = %e,
                "Could not convert snapshot schema; time-travel provider keeps the current schema"
            ),
        }
        self
    }

    /// Arrow schema for `snapshot_id` (the snapshot's own schema; the table's
    /// current schema when `None` or when the snapshot has no schema id).
    fn arrow_schema_for(
        table: &Table,
        snapshot_id: Option<i64>,
    ) -> sqe_core::Result<ArrowSchemaRef> {
        let iceberg_schema = crate::iceberg_scan::snapshot_schema(table, snapshot_id);
        let schema = schema_to_arrow_schema(&iceberg_schema).map_err(|e| {
            sqe_core::SqeError::catalog_src(
                format!(
                    "Failed to convert Iceberg schema to Arrow for {}: {e}",
                    table.identifier().name()
                ),
                e,
            )
        })?;
        Ok(Arc::new(schema))
    }

    /// Trust Iceberg sort order for all columns, not just partition keys.
    /// Only enable when data files are known to be physically sorted.
    #[must_use = "with_trust_sort_order consumes self; bind the returned provider"]
    pub fn with_trust_sort_order(mut self, trust: bool) -> Self {
        self.trust_sort_order = trust;
        self
    }

    /// Returns a reference to the underlying Iceberg table.
    pub fn iceberg_table(&self) -> &Table {
        &self.table
    }
}

impl SqeTableProvider {
    fn schema_impl(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type_impl(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown_impl(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_predicate::is_filter_pushdown_supported(f) {
                    // Inexact: DataFusion must still evaluate the filter after
                    // scanning because Iceberg predicate pushdown only prunes
                    // manifests and row-groups — it does not guarantee per-row
                    // correctness for all expression types.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan_impl(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
        statistics_requests: &[StatisticsRequest],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Convert projection indices to column names for iceberg-rust's scan API
        let projected_columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        // Determine the projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new_with_metadata(
                    fields,
                    self.schema.metadata().clone(),
                ))
            }
            None => self.schema.clone(),
        };

        // Convert DataFusion filter expressions to an Iceberg predicate
        let predicates = expr_to_predicate::convert_filters_to_predicate(filters);
        if let Some(ref pred) = predicates {
            debug!(predicate = %pred, "Pushing predicate down to Iceberg scan");
        }

        let mut exec = crate::iceberg_scan::IcebergScanExec::new_with_filters_and_metrics(
            self.table.clone(),
            projected_schema.clone(),
            projected_columns,
            predicates,
            filters.to_vec(),
        );
        if let Some(sid) = self.snapshot_id {
            exec = exec.with_snapshot_id(sid);
        }
        if self.trust_sort_order {
            exec = exec.with_trust_sort_order(true);
        }
        // NOTE: Do NOT auto-wire `target_partitions` here. Setting it to
        // `state.config_options().execution.target_partitions` causes
        // `IcebergScanExec` to advertise `Partitioning::UnknownPartitioning(N)`,
        // which is the worst possible signal for DataFusion's EnforceDistribution
        // rule -- it cannot promote the downstream HashJoin to `Partitioned`
        // mode (which needs `HashPartitioning` on the join key), so the planner
        // falls back to `CollectLeft` and inserts `CoalescePartitionsExec`
        // immediately above the scan to gather the N streams back into 1. Net
        // effect: parallel I/O, then immediate serialisation, then a
        // single-threaded hash build that is also fragmented into many tiny
        // round-robin batches. tpcds q72 SF1 regressed 5-6x (~17s -> ~100s)
        // until the wiring was removed; see issue #131 and the 2026-08-15
        // audit follow-up #414.
        //
        // The parallelism arrived anyway, from the other end: the coordinator's
        // `ParallelProbeScanRule` / `ParallelScanRule` bump scan partition counts
        // AFTER planning, so the join mode is already decided and no build side is
        // coalesced. Both ship on by default (since 2026-07-08). That is why this
        // wiring stays off rather than waiting on a cost gate: the cost gate would
        // have to re-derive a join decision the post-planning rules never disturb.
        //
        // The `with_target_partitions` setter on IcebergScanExec is kept so a
        // follow-up can re-introduce parallel scan once a proper hash-aware
        // exchange (Doris-style Local Shuffle, or a planner rule that injects
        // `RepartitionExec(Hash, ...)` ahead of the join) is in place.
        let _ = state; // intentionally unused now; see note above
        exec = exec
            .with_small_file_threshold(self.small_file_threshold_bytes)
            .with_manifest_concurrency(self.manifest_concurrency)
            .with_direct_read_concurrency(self.prefetch_concurrency)
            .with_runtime_filter_clustering(
                self.runtime_filter_clustering_skip,
                self.runtime_filter_uniform_threshold,
            )
            .with_runtime_filter_wait_ms(self.runtime_filter_wait_ms)
            .with_runtime_filter_bloom(
                self.runtime_filter_bloom_probe,
                self.runtime_filter_bloom_max_values,
            );
        if let Some(fetch_ahead) = self.scan_fetch_ahead {
            exec = exec.with_scan_fetch_ahead(fetch_ahead);
        }

        // Pre-compute per-column min/max/null_count from manifest entries so
        // DataFusion's join-order optimizer sees real selectivity and picks
        // sensible build sides. Falls back to row-count-only stats if the
        // manifest read fails — incomplete stats are better than blocking
        // the scan, and the existing fallback path stays correct.
        //
        // Served through the per-snapshot cache (review 308): the manifest
        // walk runs once per snapshot per process; every later scan of the
        // same snapshot is a lookup. The walk time is exported as
        // `sqe_catalog_manifest_walk_seconds{outcome}` so operators can see
        // what planning pays on cold snapshots.
        let walk_started = std::time::Instant::now();
        match crate::iceberg_scan::compute_table_statistics_cached(
            &self.table,
            self.snapshot_id,
            projected_schema.as_ref(),
            self.manifest_concurrency,
        )
        .await
        {
            Ok((mut stats, cache_hit)) => {
                self.observe_manifest_walk(
                    if cache_hit { "cache_hit" } else { "ok" },
                    walk_started,
                );
                self.apply_requested_puffin_ndv(
                    &mut stats,
                    projected_schema.as_ref(),
                    statistics_requests,
                );
                exec = exec.with_cached_statistics(stats);
            }
            Err(e) => {
                self.observe_manifest_walk("err", walk_started);
                // A slow or failing manifest read used to be invisible at
                // `debug!`; it is a planning-latency event worth noticing.
                warn!(
                    error = %e,
                    table = %self.table.identifier(),
                    elapsed_ms = walk_started.elapsed().as_millis() as u64,
                    "Failed to pre-compute manifest column stats; using row-count fallback"
                );
            }
        }

        Ok(Arc::new(exec))
    }

    /// Record one manifest-walk observation when metrics are attached.
    fn observe_manifest_walk(&self, outcome: &str, started: std::time::Instant) {
        if let Some(m) = &self.prom_metrics {
            m.catalog_manifest_walk_seconds
                .with_label_values(&[outcome])
                .observe(started.elapsed().as_secs_f64());
        }
    }

    fn apply_requested_puffin_ndv(
        &self,
        stats: &mut datafusion::common::Statistics,
        projected_schema: &arrow::datatypes::Schema,
        requests: &[StatisticsRequest],
    ) {
        let requested: std::collections::HashSet<&str> = requests
            .iter()
            .filter_map(|request| match request {
                StatisticsRequest::DistinctCount(column) => Some(column.name.as_str()),
                _ => None,
            })
            .collect();
        if requested.is_empty() {
            return;
        }

        let metadata = self.table.metadata();
        let snapshot = self
            .snapshot_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .or_else(|| metadata.current_snapshot());
        let Some(snapshot) = snapshot else {
            return;
        };
        let snapshot_id = snapshot.snapshot_id();
        // Process-global, keyed by (table uuid, snapshot id): shared across
        // every provider built for this table (review 307).
        let ndv_by_field_id = crate::puffin_stats::PUFFIN_NDV_CACHE.for_table_snapshot(
            metadata.uuid(),
            snapshot_id,
            metadata.statistics_for_snapshot(snapshot_id),
        );
        let Ok(iceberg_schema) = snapshot.schema(metadata) else {
            return;
        };

        for (index, field) in projected_schema.fields().iter().enumerate() {
            if !requested.contains(field.name().as_str()) {
                continue;
            }
            let Some(iceberg_field) = iceberg_schema.field_by_name(field.name()) else {
                continue;
            };
            let Some(&ndv) = ndv_by_field_id.get(&iceberg_field.id) else {
                continue;
            };
            let ndv = stats
                .num_rows
                .get_value()
                .map_or(ndv, |row_count| ndv.min(*row_count));
            if let Some(column_stats) = stats.column_statistics.get_mut(index) {
                column_stats.distinct_count = Precision::Inexact(ndv);
            }
        }
    }
}

#[async_trait]
impl TableProvider for SqeTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema_impl()
    }

    fn table_type(&self) -> TableType {
        self.table_type_impl()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.supports_filters_pushdown_impl(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.scan_impl(state, projection, filters, limit, &[]).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        let projection = args.projection().map(<[_]>::to_vec);
        let plan = self
            .scan_impl(
                state,
                projection.as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
                args.statistics_requests(),
            )
            .await?;
        Ok(ScanResult::new(plan))
    }
}

/// Architecture review 312: a time-travel provider declares the snapshot's
/// schema, not the table's current one.
#[cfg(test)]
mod snapshot_schema_tests {
    use iceberg::{NamespaceIdent, TableIdent};

    use super::SqeTableProvider;

    const SNAP_ONLY_X: i64 = 100;
    const SNAP_X_AND_Y: i64 = 200;
    const SNAP_ONLY_Y: i64 = 300;

    /// Three schemas and three snapshots: `x` -> `x, y` (ADD COLUMN y) ->
    /// `y` (DROP COLUMN x). The current snapshot is the last one.
    fn evolved_table() -> iceberg::table::Table {
        const META_V2: &str = r#"{
            "format-version": 2,
            "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 3,
            "last-updated-ms": 3,
            "last-column-id": 2,
            "current-schema-id": 2,
            "schemas": [
                {"schema-id": 0, "type": "struct", "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"}
                ]},
                {"schema-id": 1, "type": "struct", "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": false, "type": "string"}
                ]},
                {"schema-id": 2, "type": "struct", "fields": [
                    {"id": 2, "name": "y", "required": false, "type": "string"}
                ]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 300,
            "snapshots": [
                {"snapshot-id": 100, "sequence-number": 1, "timestamp-ms": 1, "schema-id": 0,
                 "manifest-list": "s3://bucket/test/location/snap-100.avro",
                 "summary": {"operation": "append"}},
                {"snapshot-id": 200, "sequence-number": 2, "timestamp-ms": 2, "schema-id": 1,
                 "parent-snapshot-id": 100,
                 "manifest-list": "s3://bucket/test/location/snap-200.avro",
                 "summary": {"operation": "append"}},
                {"snapshot-id": 300, "sequence-number": 3, "timestamp-ms": 3, "schema-id": 2,
                 "parent-snapshot-id": 200,
                 "manifest-list": "s3://bucket/test/location/snap-300.avro",
                 "summary": {"operation": "append"}}
            ],
            "snapshot-log": [],
            "metadata-log": []
        }"#;
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(META_V2).unwrap();
        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        iceberg::table::Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "t".to_string(),
            ))
            .file_io(file_io)
            .disable_cache()
            .build()
            .unwrap()
    }

    fn column_names(provider: &SqeTableProvider) -> Vec<String> {
        provider
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[tokio::test]
    async fn current_provider_declares_the_current_schema() {
        let provider = SqeTableProvider::try_new(evolved_table()).await.unwrap();
        assert_eq!(column_names(&provider), ["y"]);
    }

    /// ADD COLUMN then time travel: the added column must be absent.
    #[tokio::test]
    async fn with_snapshot_id_hides_columns_added_after_the_snapshot() {
        let provider = SqeTableProvider::try_new(evolved_table())
            .await
            .unwrap()
            .with_snapshot_id(SNAP_ONLY_X);
        assert_eq!(column_names(&provider), ["x"]);
        assert_eq!(provider.snapshot_id, Some(SNAP_ONLY_X));
    }

    /// DROP COLUMN then time travel: the dropped column must be present.
    #[tokio::test]
    async fn with_snapshot_id_keeps_columns_dropped_after_the_snapshot() {
        let provider = SqeTableProvider::try_new(evolved_table())
            .await
            .unwrap()
            .with_snapshot_id(SNAP_X_AND_Y);
        assert_eq!(column_names(&provider), ["x", "y"]);
    }

    #[tokio::test]
    async fn try_new_at_snapshot_matches_with_snapshot_id() {
        let table = evolved_table();
        let direct = SqeTableProvider::try_new_at_snapshot(table.clone(), SNAP_X_AND_Y)
            .await
            .unwrap();
        let chained = SqeTableProvider::try_new(table)
            .await
            .unwrap()
            .with_snapshot_id(SNAP_X_AND_Y);
        assert_eq!(direct.schema, chained.schema);
        let iceberg_schema =
            crate::iceberg_scan::snapshot_schema(&direct.table, Some(SNAP_X_AND_Y));
        let expected = iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap();
        assert_eq!(direct.schema.as_ref(), &expected);
    }

    /// Pinning the current snapshot, or an id the metadata does not know,
    /// falls back to the current schema instead of failing.
    #[tokio::test]
    async fn unknown_or_current_snapshot_falls_back_to_current_schema() {
        let provider = SqeTableProvider::try_new(evolved_table())
            .await
            .unwrap()
            .with_snapshot_id(SNAP_ONLY_Y);
        assert_eq!(column_names(&provider), ["y"]);
        let provider = SqeTableProvider::try_new(evolved_table())
            .await
            .unwrap()
            .with_snapshot_id(999);
        assert_eq!(column_names(&provider), ["y"]);
    }
}

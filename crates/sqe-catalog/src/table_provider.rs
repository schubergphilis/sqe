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
use crate::iceberg_scan::ScanTuning;

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
    /// The scan tuning knobs, applied to every `IcebergScanExec` this
    /// provider builds (#575).
    tuning: ScanTuning,
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
            tuning: ScanTuning::default(),
        })
    }

    /// Replace every scan tuning knob at once (#575). This is the path the
    /// catalog and schema providers use; the per-knob setters below are kept
    /// for callers that adjust one value.
    #[must_use = "with_tuning consumes self; bind the returned provider"]
    pub fn with_tuning(mut self, tuning: ScanTuning) -> Self {
        self.tuning = tuning.normalized();
        self
    }

    /// The tuning every scan of this table runs with.
    pub fn tuning(&self) -> &ScanTuning {
        &self.tuning
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
        self.tuning.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned provider"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.tuning.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the in-flight prefetch concurrency for the direct-read small-file
    /// fast path. Maps to `IcebergScanExec::direct_read_concurrency` and is
    /// fed from `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.tuning.prefetch_concurrency = concurrency.max(1);
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

    /// Boxed at the definition (compile time, see
    /// docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the
    /// coroutine below holds `&[Expr]` across its awaits, and proving it
    /// `Send` walks DataFusion's whole `Expr` type graph. Done once here;
    /// `scan` hands this future through without a coroutine of its own.
    fn scan_impl<'a>(
        &'a self,
        state: &'a dyn Session,
        projection: Option<&'a [usize]>,
        filters: &'a [Expr],
        limit: Option<usize>,
        statistics_requests: &'a [StatisticsRequest],
    ) -> futures::future::BoxFuture<'a, DFResult<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.scan_impl_inner(state, projection, filters, limit, statistics_requests))
    }

    async fn scan_impl_inner(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
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
        exec = exec.with_tuning(&self.tuning);

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
            self.tuning.manifest_concurrency,
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

    // `async_trait`'s desugared form: no coroutine of its own, the boxed
    // `scan_impl` future is handed straight through (compile time; see the
    // note on `scan_impl`).
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> futures::future::BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.scan_impl(state, projection.map(Vec::as_slice), filters, limit, &[])
    }

    // Same desugared form as `scan`: `ScanArgs` hands out its borrows with
    // the outer `'a`, so the boxed `scan_impl` future can be built here and
    // mapped, with no coroutine that would hold the `&[Expr]` again.
    fn scan_with_args<'a, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        args: ScanArgs<'a>,
    ) -> futures::future::BoxFuture<'async_trait, DFResult<ScanResult>>
    where
        'a: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        use futures::TryFutureExt;
        Box::pin(
            self.scan_impl(
                state,
                args.projection(),
                args.filters().unwrap_or(&[]),
                args.limit(),
                args.statistics_requests(),
            )
            .map_ok(ScanResult::new),
        )
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
        let file_io = iceberg::io::FileIO::new_with_fs();
        iceberg::table::Table::builder()
            .runtime(iceberg::Runtime::current())
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

    /// #575: the nine scan knobs travel as one struct. A non-default value
    /// set on the provider is what the scan node reports it runs with, and
    /// zero concurrencies clamp to one instead of stalling a zero-width
    /// `buffer_unordered`.
    #[tokio::test]
    async fn scan_tuning_reaches_the_scan_exec_unchanged() {
        use crate::iceberg_scan::{IcebergScanExec, ScanTuning};

        let tuning = ScanTuning {
            small_file_threshold_bytes: 1,
            manifest_concurrency: 3,
            prefetch_concurrency: 5,
            runtime_filter_clustering_skip: true,
            runtime_filter_uniform_threshold: 0.42,
            runtime_filter_wait_ms: 7,
            runtime_filter_bloom_probe: false,
            runtime_filter_bloom_max_values: 11,
            scan_fetch_ahead: Some(13),
        };
        let provider = SqeTableProvider::try_new(evolved_table())
            .await
            .unwrap()
            .with_tuning(tuning.clone());
        assert_eq!(provider.tuning(), &tuning);

        let exec = IcebergScanExec::new(evolved_table(), provider.schema.clone(), None, None)
            .with_tuning(provider.tuning());
        assert_eq!(exec.tuning(), tuning);

        let clamped = ScanTuning {
            manifest_concurrency: 0,
            prefetch_concurrency: 0,
            ..ScanTuning::default()
        }
        .normalized();
        assert_eq!(clamped.manifest_concurrency, 1);
        assert_eq!(clamped.prefetch_concurrency, 1);
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

/// `scan_impl` attaches manifest-derived statistics to the scan node
/// (architecture review 313), against real manifests from
/// `crate::test_fixtures::fake_iceberg_table`.
#[cfg(test)]
mod scan_statistics_tests {
    use std::sync::Arc;

    use datafusion::common::stats::Precision;
    use datafusion::common::{Column, ScalarValue, Statistics};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::statistics::StatisticsRequest;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{col, lit, SessionContext};

    use super::SqeTableProvider;
    use crate::iceberg_scan::IcebergScanExec;
    use crate::test_fixtures::{
        fake_iceberg_table, fake_snapshotless_table, FAKE_NAME_NULLS, FAKE_TOTAL_BYTES,
        FAKE_TOTAL_ROWS,
    };

    /// The statistics DataFusion's planner would see for the scan node,
    /// through the same `StatisticsContext` entry point the optimizer uses.
    fn statistics(plan: &Arc<dyn ExecutionPlan>) -> Arc<Statistics> {
        let exec = plan
            .as_ref()
            .downcast_ref::<IcebergScanExec>()
            .expect("scan returns an IcebergScanExec");
        StatisticsContext::new()
            .compute(exec, &StatisticsArgs::new())
            .expect("statistics")
    }

    /// An unfiltered, unprojected scan reports the exact row count, the byte
    /// size and per-column bounds the manifests carry.
    #[tokio::test]
    async fn scan_attaches_manifest_statistics_to_the_exec() {
        let dir = tempfile::tempdir().unwrap();
        let provider = SqeTableProvider::try_new(fake_iceberg_table(dir.path(), false).await)
            .await
            .unwrap();
        let ctx = SessionContext::new();

        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let stats = statistics(&plan);

        assert_eq!(stats.num_rows, Precision::Exact(FAKE_TOTAL_ROWS));
        assert_eq!(stats.total_byte_size, Precision::Inexact(FAKE_TOTAL_BYTES));
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Int64(Some(60)))
        );
        assert_eq!(
            stats.column_statistics[1].null_count,
            Precision::Inexact(FAKE_NAME_NULLS)
        );
    }

    /// The attached statistics are indexed by the projected schema.
    #[tokio::test]
    async fn projected_scan_statistics_follow_the_projection() {
        let dir = tempfile::tempdir().unwrap();
        let provider = SqeTableProvider::try_new(fake_iceberg_table(dir.path(), false).await)
            .await
            .unwrap();
        let ctx = SessionContext::new();

        let plan = provider
            .scan(&ctx.state(), Some(&vec![1]), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "name");
        let stats = statistics(&plan);

        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(
            stats.column_statistics[0].null_count,
            Precision::Inexact(FAKE_NAME_NULLS)
        );
        assert_eq!(stats.num_rows, Precision::Exact(FAKE_TOTAL_ROWS));
    }

    /// A row-reducing filter turns the snapshot-wide row count inexact so
    /// `COUNT(*)` can never be answered from the unfiltered total.
    #[tokio::test]
    async fn filtered_scan_degrades_the_row_count_to_inexact() {
        let dir = tempfile::tempdir().unwrap();
        let provider = SqeTableProvider::try_new(fake_iceberg_table(dir.path(), false).await)
            .await
            .unwrap();
        let ctx = SessionContext::new();

        let plan = provider
            .scan(&ctx.state(), None, &[col("id").gt(lit(40i64))], None)
            .await
            .unwrap();
        let stats = statistics(&plan);

        assert_eq!(stats.num_rows, Precision::Inexact(FAKE_TOTAL_ROWS));
        assert_eq!(stats.total_byte_size, Precision::Inexact(FAKE_TOTAL_BYTES));
    }

    /// A `DistinctCount` request on a table without Puffin sketches leaves
    /// the NDV absent and everything else untouched.
    #[tokio::test]
    async fn distinct_count_request_without_puffin_leaves_ndv_absent() {
        let dir = tempfile::tempdir().unwrap();
        let provider = SqeTableProvider::try_new(fake_iceberg_table(dir.path(), false).await)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let requests = [StatisticsRequest::DistinctCount(Arc::new(
            Column::from_name("id"),
        ))];

        let plan = provider
            .scan_impl(&ctx.state(), None, &[], None, &requests)
            .await
            .unwrap();
        let stats = statistics(&plan);

        assert_eq!(stats.num_rows, Precision::Exact(FAKE_TOTAL_ROWS));
        assert_eq!(stats.column_statistics[0].distinct_count, Precision::Absent);
    }

    /// The manifest walk is observed once as `ok` on the first scan and as
    /// `cache_hit` on the second scan of the same snapshot.
    #[tokio::test]
    async fn manifest_walk_is_observed_in_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let provider = SqeTableProvider::try_new(fake_iceberg_table(dir.path(), false).await)
            .await
            .unwrap()
            .with_metrics(Arc::clone(&metrics));
        let ctx = SessionContext::new();

        provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        provider.scan(&ctx.state(), None, &[], None).await.unwrap();

        let walks = &metrics.catalog_manifest_walk_seconds;
        assert_eq!(walks.with_label_values(&["ok"]).get_sample_count(), 1);
        assert_eq!(
            walks.with_label_values(&["cache_hit"]).get_sample_count(),
            1
        );
        assert_eq!(walks.with_label_values(&["err"]).get_sample_count(), 0);
    }

    /// A table without a snapshot scans with unknown statistics rather than
    /// failing planning.
    #[tokio::test]
    async fn snapshotless_table_scans_with_unknown_statistics() {
        let provider = SqeTableProvider::try_new(fake_snapshotless_table())
            .await
            .unwrap();
        let ctx = SessionContext::new();

        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let stats = statistics(&plan);

        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.column_statistics.len(), 2);
    }
}

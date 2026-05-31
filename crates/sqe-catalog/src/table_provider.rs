use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use tracing::debug;

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
    /// Per-user bearer attached by the ballista logical codec on decode
    /// (scheduler side). `None` on the coordinator's normal registration.
    bearer: Option<Arc<str>>,
}

impl SqeTableProvider {
    /// Create a new table provider from an Iceberg table.
    ///
    /// Converts the Iceberg schema to an Arrow schema so DataFusion can
    /// understand the table structure.
    pub async fn try_new(table: Table) -> sqe_core::Result<Self> {
        let table_name = table.identifier().name().to_string();
        debug!(table = %table_name, "Creating SqeTableProvider");

        let schema = schema_to_arrow_schema(table.metadata().current_schema()).map_err(|e| {
            sqe_core::SqeError::Catalog(format!(
                "Failed to convert Iceberg schema to Arrow for {table_name}: {e}"
            ))
        })?;

        Ok(Self {
            table,
            schema: Arc::new(schema),
            prom_metrics: None,
            trust_sort_order: false,
            snapshot_id: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            bearer: None,
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

    /// Pin this provider to a specific Iceberg snapshot for time travel queries.
    #[must_use = "with_snapshot_id consumes self; bind the returned provider"]
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Trust Iceberg sort order for all columns, not just partition keys.
    /// Only enable when data files are known to be physically sorted.
    #[must_use = "with_trust_sort_order consumes self; bind the returned provider"]
    pub fn with_trust_sort_order(mut self, trust: bool) -> Self {
        self.trust_sort_order = trust;
        self
    }

    /// Attach a per-user bearer (ballista scheduler decode path only).
    ///
    /// On the normal coordinator registration path this is never called and the
    /// field stays `None`. The ballista logical codec sets it on decode so the
    /// bearer travels coordinator -> worker with the plan fragment.
    #[must_use = "with_bearer consumes self; bind the returned provider"]
    pub fn with_bearer(mut self, bearer: Option<Arc<str>>) -> Self {
        self.bearer = bearer;
        self
    }

    /// Returns a reference to the underlying Iceberg table.
    pub fn iceberg_table(&self) -> &Table {
        &self.table
    }
}

#[async_trait]
impl TableProvider for SqeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
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
        // until the wiring was removed; see issue #131.
        //
        // The `with_target_partitions` setter on IcebergScanExec is kept so a
        // follow-up can re-introduce parallel scan once a proper hash-aware
        // exchange (Doris-style Local Shuffle, or a planner rule that injects
        // `RepartitionExec(Hash, ...)` ahead of the join) is in place.
        let _ = state; // intentionally unused now; see note above
        exec = exec
            .with_small_file_threshold(self.small_file_threshold_bytes)
            .with_manifest_concurrency(self.manifest_concurrency)
            .with_direct_read_concurrency(self.prefetch_concurrency);

        // Pre-compute per-column min/max/null_count from manifest entries so
        // DataFusion's join-order optimizer sees real selectivity and picks
        // sensible build sides. Falls back to row-count-only stats if the
        // manifest read fails — incomplete stats are better than blocking
        // the scan, and the existing fallback path stays correct.
        match crate::iceberg_scan::compute_table_statistics(
            &self.table,
            self.snapshot_id,
            projected_schema.as_ref(),
            self.manifest_concurrency,
        )
        .await
        {
            Ok(stats) => {
                exec = exec.with_cached_statistics(stats);
            }
            Err(e) => {
                debug!(
                    error = %e,
                    table = %self.table.identifier(),
                    "Failed to pre-compute manifest column stats; using row-count fallback"
                );
            }
        }

        let exec = exec.with_bearer(self.bearer.clone());

        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::TableMetadata;
    use iceberg::{NamespaceIdent, TableIdent};

    /// Build a minimal `SqeTableProvider` from inline JSON metadata.
    /// No snapshots, no I/O performed; only the schema is needed for this test.
    async fn make_test_provider() -> SqeTableProvider {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
            "location": "file:///tmp/test-table",
            "last-sequence-number": 0,
            "last-updated-ms": 1600000000000,
            "last-column-id": 1,
            "schemas": [{"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":false,"type":"long"}]}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id":0,"fields":[]}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "sort-orders": [{"order-id":0,"fields":[]}],
            "default-sort-order-id": 0,
            "refs": {}
        }"#;
        let metadata: TableMetadata =
            serde_json::from_str(metadata_json).expect("test metadata must parse");
        let file_io = FileIOBuilder::new_fs_io().build().expect("fs FileIO");
        let ident = TableIdent::new(
            NamespaceIdent::new("test_ns".to_string()),
            "test_table".to_string(),
        );
        let table = iceberg::table::Table::builder()
            .file_io(file_io)
            .identifier(ident)
            .metadata(Arc::new(metadata))
            .build()
            .expect("test Table");
        SqeTableProvider::try_new(table)
            .await
            .expect("SqeTableProvider must build from test table")
    }

    #[tokio::test]
    async fn scan_propagates_bearer_to_iceberg_scan_exec() {
        let provider = make_test_provider()
            .await
            .with_bearer(Some(Arc::from("u-tok")));
        let ctx = SessionContext::new();
        let state = ctx.state();
        let exec = provider.scan(&state, None, &[], None).await.unwrap();
        let scan = exec
            .as_any()
            .downcast_ref::<crate::iceberg_scan::IcebergScanExec>()
            .expect("scan() must return an IcebergScanExec");
        assert_eq!(scan.bearer(), Some("u-tok"));
    }
}

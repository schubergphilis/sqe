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
use crate::manifest_cache::ManifestCache;

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
    /// Optional shared manifest file cache passed down to IcebergScanExec.
    manifest_cache: Option<ManifestCache>,
    /// Small-file threshold in bytes for the direct-read fast path.
    small_file_threshold_bytes: u64,
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
            manifest_cache: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
        })
    }

    /// Attach a shared manifest cache to accelerate warm queries.
    ///
    /// When set, `IcebergScanExec` will serve manifest entries from the cache
    /// on repeated scans, avoiding S3 fetches for immutable manifest files.
    pub fn with_manifest_cache(mut self, cache: ManifestCache) -> Self {
        self.manifest_cache = Some(cache);
        self
    }

    /// Attach Prometheus metrics for file pruning and S3 I/O.
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    ///
    /// Files below this size are read entirely in a single S3 GET and parsed
    /// from memory, bypassing iceberg-rust's `scan.to_arrow()` pipeline.
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Pin this provider to a specific Iceberg snapshot for time travel queries.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Trust Iceberg sort order for all columns, not just partition keys.
    /// Only enable when data files are known to be physically sorted.
    pub fn with_trust_sort_order(mut self, trust: bool) -> Self {
        self.trust_sort_order = trust;
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
        _state: &dyn Session,
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
            projected_schema,
            projected_columns,
            predicates,
            filters.to_vec(),
            self.prom_metrics.clone(),
        );
        if let Some(sid) = self.snapshot_id {
            exec = exec.with_snapshot_id(sid);
        }
        if self.trust_sort_order {
            exec = exec.with_trust_sort_order(true);
        }
        if let Some(ref mc) = self.manifest_cache {
            exec = exec.with_manifest_cache(mc.clone());
        }
        exec = exec.with_small_file_threshold(self.small_file_threshold_bytes);
        Ok(Arc::new(exec))
    }
}

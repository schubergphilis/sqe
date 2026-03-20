use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use tracing::debug;

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
        })
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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
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

        Ok(Arc::new(crate::iceberg_scan::IcebergScanExec::new(
            self.table.clone(),
            projected_schema,
            projected_columns,
        )))
    }
}

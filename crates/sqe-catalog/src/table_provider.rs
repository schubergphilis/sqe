use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use tracing::debug;

/// DataFusion `TableProvider` that wraps an Iceberg `Table`.
///
/// This provider converts the Iceberg table schema to an Arrow schema and
/// makes the table queryable through DataFusion's SQL engine. The scan
/// implementation currently returns an empty execution plan; actual data
/// reading will be implemented when the query execution pipeline is built.
///
/// Note: We implement our own `TableProvider` rather than using
/// `iceberg-datafusion::IcebergTableProvider` because iceberg-datafusion 0.5.x
/// depends on datafusion 47, which is incompatible with our datafusion 49.
/// Since iceberg 0.5.x uses arrow 55 (same as our workspace), the schema
/// types are fully compatible.
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
        // Determine the projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.schema.clone(),
        };

        // TODO: Implement actual Iceberg scan using table.scan().build().to_arrow()
        // For now, return an empty result set with the correct schema.
        // This will be replaced with a proper IcebergScan execution plan
        // when the query pipeline (sqe-coordinator) is implemented.
        Ok(Arc::new(EmptyExec::new(projected_schema)))
    }
}

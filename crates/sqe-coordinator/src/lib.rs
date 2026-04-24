pub mod adaptive_sort;
pub mod catalog_ops;
pub mod maintenance;
pub mod tls;
pub mod codec;
pub mod credential_refresh;
pub mod distributed_scan;
pub mod explain;
pub mod flight_sql;
pub mod flight_sql_helpers;
pub mod memory;
pub mod mode;
pub mod query_handler;
pub mod runtime;
pub mod session_context;
pub mod query_cache;
pub mod query_tracker;
pub mod rate_limiter;
pub mod scheduler;
pub mod streaming;
pub mod session_manager;
pub mod worker_registry;
pub mod write_handler;
pub mod writer;

pub use mode::Mode;
pub use query_handler::QueryHandler;
pub use session_manager::SessionManager;

/// Test-only re-exports used by integration tests under `tests/`.
///
/// Kept behind a sentinel name so accidental use in production code
/// stands out in review.
#[doc(hidden)]
pub mod __test_support {
    use iceberg::spec::Schema as IcebergSchema;
    use sqe_core::Result;

    pub fn sql_type_to_arrow_public(
        sql_type: &sqlparser::ast::DataType,
    ) -> Result<arrow_schema::DataType> {
        crate::write_handler::sql_type_to_arrow(sql_type)
    }

    /// Build an Iceberg schema from a parsed `CREATE TABLE`, applying DEFAULT
    /// literals and preserving nanosecond timestamp mappings.
    pub fn build_iceberg_schema_with_defaults(
        ct: &sqlparser::ast::CreateTable,
    ) -> Result<IcebergSchema> {
        use arrow_schema::{Field, Schema as ArrowSchema};

        let arrow_fields: Vec<Field> = ct
            .columns
            .iter()
            .map(|col| {
                let arrow_type = sql_type_to_arrow_public(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(Field::new(col.name.value.clone(), arrow_type, nullable))
            })
            .collect::<Result<Vec<_>>>()?;
        let arrow_schema = ArrowSchema::new(arrow_fields);
        crate::write_handler::arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns)
    }

    /// Report whether a `CREATE TABLE` would require Iceberg format-version 3.
    pub fn needs_v3(ct: &sqlparser::ast::CreateTable) -> Result<bool> {
        let schema = build_iceberg_schema_with_defaults(ct)?;
        Ok(crate::write_handler::requires_v3_features(&ct.columns, &schema))
    }
}

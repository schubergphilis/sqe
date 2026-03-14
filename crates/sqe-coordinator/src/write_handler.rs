use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::arrow_type_to_type;
use iceberg::spec::{NestedField, Schema as IcebergSchema};
use iceberg::transaction::Transaction;
use iceberg::{Catalog, TableCreation, TableIdent};
use sqlparser::ast::Statement;
use tracing::info;

use sqe_catalog::SessionCatalog;
use sqe_core::{Session, SqeConfig, SqeError};

use crate::catalog_ops::parse_table_ref;
use crate::writer::write_data_files;

/// Handles write operations: CTAS (CREATE TABLE AS SELECT) and INSERT INTO SELECT.
///
/// Write handlers receive already-executed RecordBatches from the query pipeline
/// and persist them as Iceberg data files via Parquet, then commit the changes
/// through the Iceberg REST catalog.
pub struct WriteHandler {
    config: SqeConfig,
}

impl WriteHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self { config }
    }

    /// Handle CREATE TABLE [OR REPLACE] ns.table AS SELECT ...
    ///
    /// The caller has already executed the inner SELECT and provides the result
    /// batches. This method:
    /// 1. Extracts the table name from the CTAS statement
    /// 2. Converts the Arrow schema to an Iceberg schema
    /// 3. Creates the table in the catalog
    /// 4. Writes RecordBatches as Parquet data files
    /// 5. Commits the data files via a fast-append transaction
    pub async fn handle_ctas(
        &self,
        session: &Session,
        stmt: &Statement,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_name, _or_replace, arrow_schema) = match stmt {
            Statement::CreateTable(ct) => {
                if ct.query.is_none() {
                    return Err(SqeError::Execution(
                        "CTAS statement has no SELECT query".into(),
                    ));
                }

                // Get the Arrow schema from the first batch, or return early if empty
                let schema = if let Some(batch) = batches.first() {
                    batch.schema()
                } else {
                    return Err(SqeError::Execution(
                        "CTAS query returned no results — cannot infer schema".into(),
                    ));
                };

                (&ct.name, ct.or_replace, schema)
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Executing CTAS"
        );

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        // Create the catalog bridge for this session
        let catalog = self.create_catalog_bridge(session).await?;

        // Create the table in the catalog
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .build();

        let _created_table = catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        // Load the table back (needed for the writer infrastructure which reads
        // table metadata for location generation, file IO, etc.)
        let table_ident = TableIdent::new(namespace, name);
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load created table: {e}")))?;

        // Write data files (skip if no data)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows > 0 {
            let data_files = write_data_files(&table, batches, "ctas").await?;

            if !data_files.is_empty() {
                // Commit data files via fast-append transaction
                let tx = Transaction::new(&table);
                let mut action = tx.fast_append(None, vec![]).map_err(|e| {
                    SqeError::Execution(format!("Failed to create fast append action: {e}"))
                })?;

                action.add_data_files(data_files).map_err(|e| {
                    SqeError::Execution(format!("Failed to add data files: {e}"))
                })?;

                let tx = action.apply().await.map_err(|e| {
                    SqeError::Execution(format!("Failed to apply fast append: {e}"))
                })?;

                tx.commit(catalog.as_ref()).await.map_err(|e| {
                    SqeError::Execution(format!("Failed to commit CTAS transaction: {e}"))
                })?;
            }

            info!(
                table = %table_ident,
                total_rows,
                "CTAS committed successfully"
            );
        } else {
            info!(
                table = %table_ident,
                "CTAS created empty table (no data to write)"
            );
        }

        Ok(vec![]) // DDL success, no result rows
    }

    /// Handle INSERT INTO ns.table SELECT ...
    ///
    /// The caller has already executed the SELECT and provides the result
    /// batches. This method:
    /// 1. Extracts the target table name from the INSERT statement
    /// 2. Loads the existing table from the catalog
    /// 3. Writes RecordBatches as Parquet data files
    /// 4. Commits the data files via a fast-append transaction
    pub async fn handle_insert(
        &self,
        session: &Session,
        stmt: &Statement,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_name = match stmt {
            Statement::Insert(ins) => &ins.table_name,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected Insert statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            username = %session.user.username,
            table = %table_ident,
            total_rows,
            "Executing INSERT INTO SELECT"
        );

        if total_rows == 0 {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            return Ok(vec![]);
        }

        // Create the catalog bridge and load the existing table
        let catalog = self.create_catalog_bridge(session).await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        // Write data files
        let data_files = write_data_files(&table, batches, "insert").await?;

        if !data_files.is_empty() {
            // Commit via fast-append
            let tx = Transaction::new(&table);
            let mut action = tx.fast_append(None, vec![]).map_err(|e| {
                SqeError::Execution(format!("Failed to create fast append action: {e}"))
            })?;

            action.add_data_files(data_files).map_err(|e| {
                SqeError::Execution(format!("Failed to add data files: {e}"))
            })?;

            let tx = action.apply().await.map_err(|e| {
                SqeError::Execution(format!("Failed to apply fast append: {e}"))
            })?;

            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit INSERT transaction: {e}"))
            })?;

            info!(
                table = %table_ident,
                total_rows,
                "INSERT INTO committed successfully"
            );
        }

        Ok(vec![]) // DML success, no result rows
    }

    /// Create a `SessionCatalogBridge` (which implements `iceberg::Catalog`)
    /// for the given session.
    async fn create_catalog_bridge(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
            )
            .await?,
        );

        Ok(session_catalog.as_catalog())
    }
}

/// Convert an Arrow schema to an Iceberg schema.
///
/// Arrow schemas from DataFusion queries do not carry Parquet field-id metadata,
/// so we cannot use `iceberg::arrow::arrow_schema_to_schema` directly (it
/// requires the `PARQUET_FIELD_ID` key). Instead, we convert each Arrow field
/// individually using `arrow_type_to_type` and assign sequential field IDs
/// starting from 1.
fn arrow_schema_to_iceberg(arrow_schema: &ArrowSchema) -> sqe_core::Result<IcebergSchema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields().len());

    for (idx, arrow_field) in arrow_schema.fields().iter().enumerate() {
        let field_id = (idx + 1) as i32;
        let iceberg_type = arrow_type_to_type(arrow_field.data_type()).map_err(|e| {
            SqeError::Execution(format!(
                "Cannot convert Arrow type {:?} for field '{}' to Iceberg type: {e}",
                arrow_field.data_type(),
                arrow_field.name()
            ))
        })?;

        let field = if arrow_field.is_nullable() {
            NestedField::optional(field_id, arrow_field.name(), iceberg_type)
        } else {
            NestedField::required(field_id, arrow_field.name(), iceberg_type)
        };

        fields.push(Arc::new(field));
    }

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| SqeError::Execution(format!("Failed to build Iceberg schema: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    #[test]
    fn test_arrow_schema_to_iceberg_basic() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert!(fields[0].required);
        assert_eq!(fields[1].name, "name");
        assert!(!fields[1].required);
        assert_eq!(fields[2].name, "value");
        assert!(!fields[2].required);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_empty() {
        let arrow_schema = ArrowSchema::empty();
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 0);
    }
}

use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::prelude::SessionContext as DFSessionContext;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::arrow_type_to_type;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestStatus, NestedField, Schema as IcebergSchema,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableCreation, TableIdent};
use sqlparser::ast::Statement;
use tracing::info;

use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use tracing::instrument;

use crate::catalog_ops::parse_table_ref;
use crate::writer::{
    parse_parquet_compression, write_data_files_streaming_with_metrics,
    write_data_files_with_metrics, write_position_delete_files,
};

/// Build a single-row RecordBatch reporting affected row count.
/// Matches Trino's DML response which returns the update count.
fn affected_rows_batch(count: usize) -> Vec<RecordBatch> {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("rows_affected", DataType::Int64, false),
    ]));
    match RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![count as i64]))]) {
        Ok(batch) => vec![batch],
        Err(_) => vec![],
    }
}

/// Handles write operations: CTAS (CREATE TABLE AS SELECT) and INSERT INTO SELECT.
///
/// Write handlers receive already-executed RecordBatches from the query pipeline
/// and persist them as Iceberg data files via Parquet, then commit the changes
/// through the Iceberg REST catalog.
pub struct WriteHandler {
    config: SqeConfig,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Shared global table metadata cache. Used so write-path SessionCatalog
    /// instances hit the warm cache and invalidate the right entry on commit.
    table_cache: Option<TableMetadataCache>,
}

impl WriteHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self { config, metrics: None, table_cache: None }
    }

    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a global table metadata cache shared across all sessions.
    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Return the Parquet compression codec from config.
    fn compression(&self) -> parquet::basic::Compression {
        parse_parquet_compression(&self.config.catalog.parquet_compression)
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
    #[instrument(skip(self, session, stmt, batches), fields(username = %session.user.username))]
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

                // Get the Arrow schema from the first batch. The caller guarantees
                // at least one batch is present (possibly with 0 rows).
                let schema = if let Some(batch) = batches.first() {
                    batch.schema()
                } else {
                    return Err(SqeError::Execution(
                        "CTAS query returned no batches — cannot infer schema".into(),
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
            .format_version(self.format_version())
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
            let data_files = write_data_files_with_metrics(&table, batches, "ctas", self.metrics.as_ref(), self.compression()).await?;

            if !data_files.is_empty() {
                // Commit data files via fast-append transaction
                let tx = Transaction::new(&table);
                let action = tx.fast_append().add_data_files(data_files);

                let tx = action.apply(tx).map_err(|e| {
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

    /// Handle CREATE TABLE [OR REPLACE] ns.table AS SELECT ... — streaming variant.
    ///
    /// Instead of buffering the full SELECT result in memory, this method:
    /// 1. Plans the SELECT via DataFusion and derives the Iceberg schema from the
    ///    DataFrame schema (before execution — no data buffered yet).
    /// 2. Creates the table in the catalog.
    /// 3. Streams batches directly to the Parquet writer via `df.execute_stream()`.
    /// 4. Commits the data files via a fast-append transaction.
    ///
    /// Peak memory is O(batch_size) instead of O(total_rows). Critical for large
    /// CTAS loads (SF1 lineorder, store_sales, etc.) that OOM with `df.collect()`.
    #[instrument(skip(self, session, stmt, ctx, select_sql), fields(username = %session.user.username))]
    pub async fn handle_ctas_streaming(
        &self,
        session: &Session,
        stmt: &Statement,
        ctx: &DFSessionContext,
        select_sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_name, _or_replace) = match stmt {
            Statement::CreateTable(ct) => {
                if ct.query.is_none() {
                    return Err(SqeError::Execution(
                        "CTAS statement has no SELECT query".into(),
                    ));
                }
                (&ct.name, ct.or_replace)
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;

        // Plan the SELECT without executing it — gives us the output schema cheaply.
        let df = ctx
            .sql(select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            "Executing CTAS (streaming)"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .format_version(self.format_version())
            .build();

        let _created_table = catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        let table_ident = TableIdent::new(namespace, name);
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load created table: {e}")))?;

        // Execute the SELECT and stream batches directly to the Parquet writer.
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to start execution stream: {e}")))?;

        let (data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "ctas",
            self.metrics.as_ref(),
            self.compression(),
        )
        .await?;

        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply fast append: {e}"))
            })?;
            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit CTAS transaction: {e}"))
            })?;

            info!(
                table = %table_ident,
                total_rows,
                "CTAS committed successfully (streaming)"
            );
        } else {
            info!(
                table = %table_ident,
                "CTAS created empty table (no data to write)"
            );
        }

        Ok(vec![])
    }

    /// Handle INSERT INTO ns.table SELECT ... — streaming variant.
    ///
    /// Streams batches from the SELECT directly to the Parquet writer without
    /// buffering the full result set. Peak memory is O(batch_size).
    #[instrument(skip(self, session, stmt, ctx, select_sql), fields(username = %session.user.username))]
    pub async fn handle_insert_streaming(
        &self,
        session: &Session,
        stmt: &Statement,
        ctx: &DFSessionContext,
        select_sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_name = match stmt {
            Statement::Insert(ins) => match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => name,
                other => {
                    return Err(SqeError::Execution(format!(
                        "INSERT INTO table functions not supported: {other}"
                    )));
                }
            },
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected Insert statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            "Executing INSERT INTO SELECT (streaming)"
        );

        let catalog = self.create_catalog_bridge(session).await?;
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        let df = ctx
            .sql(select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to start execution stream: {e}")))?;

        let (data_files, total_rows) = write_data_files_streaming_with_metrics(
            &table,
            stream,
            "insert",
            self.metrics.as_ref(),
            self.compression(),
        )
        .await?;

        if total_rows == 0 {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            return Ok(vec![]);
        }

        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply fast append: {e}"))
            })?;
            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit INSERT transaction: {e}"))
            })?;

            info!(
                table = %table_ident,
                total_rows,
                "INSERT INTO committed successfully (streaming)"
            );
        }

        Ok(vec![])
    }

    /// Handle CREATE TABLE [IF NOT EXISTS] ns.table (column definitions)
    ///
    /// Creates an empty Iceberg table from explicit column definitions.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn handle_create_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CreateTable statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(&ct.name)?;

        // Convert SQL column definitions to Arrow schema
        let arrow_fields: Vec<arrow_schema::Field> = ct
            .columns
            .iter()
            .map(|col| {
                let arrow_type = sql_type_to_arrow(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(arrow_schema::Field::new(col.name.value.clone(), arrow_type, nullable))
            })
            .collect::<sqe_core::Result<Vec<_>>>()?;

        if arrow_fields.is_empty() {
            return Err(SqeError::Execution(
                "CREATE TABLE requires at least one column definition".into(),
            ));
        }

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)?;

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            columns = arrow_schema.fields().len(),
            "Creating empty table"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        if ct.if_not_exists {
            let table_ident = TableIdent::new(namespace.clone(), name.clone());
            if catalog.load_table(&table_ident).await.is_ok() {
                info!(table = %table_ident, "Table already exists, skipping (IF NOT EXISTS)");
                return Ok(vec![]);
            }
        }

        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .format_version(self.format_version())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create table: {e}")))?;

        info!(
            namespace = %namespace,
            table = %name,
            "Table created successfully"
        );

        Ok(vec![])
    }

    /// Handle INSERT INTO ns.table SELECT ...
    ///
    /// The caller has already executed the SELECT and provides the result
    /// batches. This method:
    /// 1. Extracts the target table name from the INSERT statement
    /// 2. Loads the existing table from the catalog
    /// 3. Writes RecordBatches as Parquet data files
    /// 4. Commits the data files via a fast-append transaction
    #[instrument(skip(self, session, stmt, batches), fields(username = %session.user.username))]
    pub async fn handle_insert(
        &self,
        session: &Session,
        stmt: &Statement,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_name = match stmt {
            Statement::Insert(ins) => match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => name,
                other => {
                    return Err(SqeError::Execution(format!(
                        "INSERT INTO table functions not supported: {other}"
                    )));
                }
            },
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
        let data_files = write_data_files_with_metrics(&table, batches, "insert", self.metrics.as_ref(), self.compression()).await?;

        if !data_files.is_empty() {
            // Commit via fast-append
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            let tx = action.apply(tx).map_err(|e| {
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

        Ok(affected_rows_batch(total_rows)) // DML success with affected row count
    }

    /// Handle a Flight SQL DoPut ingest — write streamed Arrow batches to an Iceberg table.
    pub async fn handle_ingest(
        &self,
        session: &Session,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<usize> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if total_rows == 0 {
            return Ok(0);
        }

        // Parse "catalog.schema.table" or "schema.table"
        let parts: Vec<&str> = table_name.split('.').collect();
        let (namespace_str, name) = match parts.as_slice() {
            [ns, tbl] => (*ns, (*tbl).to_string()),
            [_cat, ns, tbl] => (*ns, (*tbl).to_string()),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Invalid table name for ingest: {table_name}"
                )));
            }
        };

        let namespace = iceberg::NamespaceIdent::new(namespace_str.to_string());
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            total_rows,
            "Executing DoPut ingest"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        let data_files = write_data_files_with_metrics(&table, batches, "ingest", self.metrics.as_ref(), self.compression()).await?;

        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply fast append: {e}"))
            })?;
            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit ingest transaction: {e}"))
            })?;

            info!(table = %table_ident, total_rows, "DoPut ingest committed successfully");
        }

        Ok(total_rows)
    }

    /// Handle DELETE FROM ns.table [WHERE ...]
    ///
    /// Uses Copy-on-Write: reads all data files, filters out rows matching
    /// the WHERE predicate, writes new files with surviving rows, and
    /// atomically swaps via rewrite_files().
    ///
    /// Without a WHERE clause, this is a truncate: commits a rewrite that
    /// removes all data files.
    #[instrument(skip(self, session, stmt, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_delete(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DELETE statement, got: {other}"
                )));
            }
        };

        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => tables,
            sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
        };
        let table_factor_name = match &tables[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in DELETE, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_factor_name)?;
        let table_ident = TableIdent::new(namespace, name);

        let table = catalog.load_table(&table_ident).await?;

        // Get all data files from current snapshot via manifest entries
        let old_data_files = self.collect_data_files(&table).await?;

        if old_data_files.is_empty() {
            info!(table = %table_ident, "DELETE: table has no data files, nothing to delete");
            return Ok(vec![]);
        }

        let where_clause = &delete.selection;

        // No WHERE = truncate: remove all files, add none
        if where_clause.is_none() {
            info!(table = %table_ident, file_count = old_data_files.len(), "DELETE: truncating table (no WHERE clause)");
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_files(old_data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
            })?;
            tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit truncate: {e}"))
            })?;
            info!(table = %table_ident, "DELETE: table truncated successfully");
            return Ok(vec![]);
        }

        // WHERE clause present: CoW rewrite
        let raw_where = format!("{}", where_clause.as_ref().unwrap());
        // Rewrite IN (subquery) → IN (literal_list) before per-file evaluation.
        // DataFusion's physical planner rejects InSubquery in DML context.
        let where_sql = self.rewrite_in_subquery_where(&raw_where, ctx).await?;
        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            where_clause = %where_sql,
            "DELETE: CoW rewrite"
        );

        let mut new_data_files = Vec::new();
        let mut total_deleted = 0usize;

        for data_file in &old_data_files {
            let file_path = data_file.file_path();
            let batches = self.read_parquet_via_table(&table, file_path).await?;

            if batches.is_empty() {
                continue;
            }

            // Evaluate WHERE predicate against each batch, keep rows that do NOT match
            let mut surviving_batches = Vec::new();
            for batch in &batches {
                let filtered =
                    self.filter_batch_negate(ctx, batch, &where_sql, &table_ident)
                        .await?;
                total_deleted += batch.num_rows() - filtered.num_rows();
                if filtered.num_rows() > 0 {
                    surviving_batches.push(filtered);
                }
            }

            // Write surviving rows as new data files (skip if all rows deleted)
            if !surviving_batches.is_empty() {
                let new_files = write_data_files_with_metrics(&table, surviving_batches, "delete", self.metrics.as_ref(), self.compression()).await?;
                new_data_files.extend(new_files);
            }
        }

        info!(
            table = %table_ident,
            deleted_rows = total_deleted,
            old_files = old_data_files.len(),
            new_files = new_data_files.len(),
            "DELETE: committing CoW rewrite"
        );

        // Atomic commit: remove old files, add new files
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .add_data_files(new_data_files)
            .delete_files(old_data_files);
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply DELETE rewrite: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            SqeError::Execution(format!("Failed to commit DELETE: {e}"))
        })?;

        info!(table = %table_ident, deleted_rows = total_deleted, "DELETE committed successfully");
        Ok(affected_rows_batch(total_deleted))
    }

    /// Handle DELETE FROM using Merge-on-Read (position deletes).
    ///
    /// Instead of rewriting data files (CoW), this method writes position delete files
    /// that mark specific row positions for deletion. This is more efficient for small
    /// deletes on large tables — the cost is O(deleted rows) vs O(total rows) for CoW —
    /// but increases read amplification until the table is compacted.
    ///
    /// The position delete files are committed via `FastAppendAction`, which auto-routes
    /// `DataFile`s with `content_type = PositionDeletes` into the delete manifest.
    ///
    /// Without a WHERE clause this falls back to the CoW truncate path (same as
    /// `handle_delete`), since there is no efficiency benefit to writing delete files
    /// for every row.
    #[instrument(skip(self, session, stmt, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_delete_mor(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let delete = match stmt {
            Statement::Delete(d) => d,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DELETE statement, got: {other}"
                )));
            }
        };

        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => tables,
            sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
        };
        let table_factor_name = match &tables[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in DELETE, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_factor_name)?;
        let table_ident = TableIdent::new(namespace, name);

        let table = catalog.load_table(&table_ident).await?;

        let old_data_files = self.collect_data_files(&table).await?;

        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR DELETE: table has no data files, nothing to delete");
            return Ok(vec![]);
        }

        let where_clause = &delete.selection;

        // No WHERE clause: fall back to CoW truncate (remove all files atomically).
        // Writing a position delete for every row would be wasteful and serves no purpose.
        if where_clause.is_none() {
            info!(
                table = %table_ident,
                file_count = old_data_files.len(),
                "MoR DELETE: no WHERE clause, truncating table via CoW"
            );
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_files(old_data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
            })?;
            tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit truncate: {e}"))
            })?;
            info!(table = %table_ident, "MoR DELETE: table truncated successfully");
            return Ok(vec![]);
        }

        let raw_where = format!("{}", where_clause.as_ref().unwrap());
        // Rewrite IN (subquery) → IN (literal_list) before per-file evaluation.
        // DataFusion's physical planner rejects InSubquery in DML context.
        let where_sql = self.rewrite_in_subquery_where(&raw_where, ctx).await?;
        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            where_clause = %where_sql,
            "MoR DELETE: collecting row positions to delete"
        );

        // Scan each data file and collect (file_path, row_position) pairs for matching rows.
        let mut position_deletes: Vec<(String, i64)> = Vec::new();

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self.read_parquet_via_table(&table, &file_path).await?;

            if batches.is_empty() {
                continue;
            }

            // Row positions are 0-based and contiguous across all batches in the file.
            let mut row_offset: i64 = 0;
            for batch in &batches {
                let match_mask = self
                    .filter_batch_match(ctx, batch, &where_sql, &table_ident)
                    .await?;

                for row_idx in 0..batch.num_rows() {
                    if match_mask.value(row_idx) {
                        position_deletes.push((file_path.clone(), row_offset + row_idx as i64));
                    }
                }
                row_offset += batch.num_rows() as i64;
            }
        }

        if position_deletes.is_empty() {
            info!(table = %table_ident, "MoR DELETE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        let deleted_count = position_deletes.len();
        info!(
            table = %table_ident,
            delete_count = deleted_count,
            "MoR DELETE: writing position delete files"
        );

        // Write position delete files (sorted by (file_path, pos) inside the helper).
        let delete_files = write_position_delete_files(&table, position_deletes, self.compression()).await?;

        // Commit: append position delete files. FastAppendAction auto-routes DataFiles
        // with content_type=PositionDeletes into the delete manifest entry.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(delete_files);
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply MoR DELETE fast-append: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            SqeError::Execution(format!("Failed to commit MoR DELETE: {e}"))
        })?;

        info!(table = %table_ident, deleted_rows = deleted_count, "MoR DELETE committed successfully");
        Ok(affected_rows_batch(deleted_count))
    }

    /// Handle UPDATE ns.table SET col = expr [WHERE ...]
    ///
    /// Uses Copy-on-Write: reads all data files, applies SET assignments to
    /// rows matching WHERE, writes new files, atomically swaps.
    #[instrument(skip(self, session, stmt, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_update(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (table_factor, assignments, selection) = match stmt {
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => (table, assignments, selection),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected UPDATE statement, got: {other}"
                )));
            }
        };

        let table_name = match &table_factor.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in UPDATE, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        let table = catalog.load_table(&table_ident).await?;

        // Get all data files
        let old_data_files = self.collect_data_files(&table).await?;

        if old_data_files.is_empty() {
            info!(table = %table_ident, "UPDATE: table has no data files");
            return Ok(vec![]);
        }

        // Build the SET clause as SQL CASE expressions for a SELECT rewrite
        // UPDATE t SET col1 = expr1, col2 = expr2 WHERE cond
        // becomes:
        // SELECT CASE WHEN cond THEN expr1 ELSE col1 END AS col1,
        //        CASE WHEN cond THEN expr2 ELSE col2 END AS col2,
        //        col3, col4, ...  (unchanged columns)
        // FROM t
        let raw_where = selection
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        // Rewrite IN (subquery) → IN (literal_list) before per-file evaluation.
        // DataFusion's physical planner rejects InSubquery in DML context.
        let where_sql = self.rewrite_in_subquery_where(&raw_where, ctx).await?;

        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            assignments = assignments.len(),
            where_clause = %where_sql,
            "UPDATE: CoW rewrite"
        );

        let mut new_data_files = Vec::new();
        let mut total_updated = 0usize;

        for data_file in &old_data_files {
            let file_path = data_file.file_path();
            let batches = self.read_parquet_via_table(&table, file_path).await?;

            if batches.is_empty() {
                continue;
            }

            let mut rewritten_batches = Vec::new();

            for batch in &batches {
                let rewritten = self
                    .apply_update(ctx, batch, assignments, &where_sql, &table_ident)
                    .await?;
                rewritten_batches.push(rewritten);
            }

            // Count updated rows by comparing before/after
            for batch in &batches {
                let count = self
                    .count_matching_rows(ctx, batch, &where_sql, &table_ident)
                    .await?;
                total_updated += count;
            }

            let new_files = write_data_files_with_metrics(&table, rewritten_batches, "update", self.metrics.as_ref(), self.compression()).await?;
            new_data_files.extend(new_files);
        }

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            old_files = old_data_files.len(),
            new_files = new_data_files.len(),
            "UPDATE: committing CoW rewrite"
        );

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .add_data_files(new_data_files)
            .delete_files(old_data_files);
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply UPDATE rewrite: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            SqeError::Execution(format!("Failed to commit UPDATE: {e}"))
        })?;

        info!(table = %table_ident, updated_rows = total_updated, "UPDATE committed successfully");
        Ok(affected_rows_batch(total_updated))
    }

    /// Handle MERGE INTO target USING source ON condition WHEN ...
    ///
    /// Uses Copy-on-Write: reads all target data files, performs a FULL OUTER
    /// JOIN with the provided source batches to classify rows as matched /
    /// not-matched / target-only, applies the appropriate MERGE actions via
    /// CASE WHEN SQL expressions, writes new data files, and atomically swaps
    /// via rewrite_files().
    ///
    /// The caller is responsible for executing the source query and providing
    /// the result batches. This follows the same pattern as `handle_ctas` and
    /// `handle_insert`.
    #[instrument(skip(self, session, stmt, source_batches, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_merge(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::{MergeAction, MergeClauseKind, MergeInsertKind, TableFactor};

        let (table_factor, source_factor, on_expr, clauses) = match stmt {
            Statement::Merge {
                table,
                source,
                on,
                clauses,
                ..
            } => (table, source, on, clauses),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected MERGE statement, got: {other}"
                )));
            }
        };

        // Extract target table name and optional alias
        let (target_table_name, target_alias) = match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                (name, alias_str)
            }
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected table name in MERGE target, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(target_table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        // Extract source alias (needed for column references in the JOIN)
        let source_alias = match source_factor {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };

        let on_sql = format!("{on_expr}");

        info!(
            username = %session.user.username,
            table = %table_ident,
            on_condition = %on_sql,
            clause_count = clauses.len(),
            "Executing MERGE INTO"
        );

        // Load target table and read all data files
        let table = catalog.load_table(&table_ident).await?;

        let old_data_files = self.collect_data_files(&table).await?;

        // Read all target batches into memory
        let mut target_batches: Vec<RecordBatch> = Vec::new();
        for data_file in &old_data_files {
            let file_path = data_file.file_path();
            let batches = self.read_parquet_via_table(&table, file_path).await?;
            target_batches.extend(batches);
        }

        // Get the target schema from existing data (or table metadata if empty)
        let target_schema = if let Some(first) = target_batches.first() {
            first.schema()
        } else {
            // Empty table — get the schema from the Iceberg table metadata
            let iceberg_schema = table.metadata().current_schema();
            let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
                .map_err(|e| {
                    SqeError::Execution(format!(
                        "Failed to convert Iceberg schema to Arrow: {e}"
                    ))
                })?;
            Arc::new(arrow_schema)
        };

        let target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Use the target alias (or a default) for the merge MemTable names
        let t_alias = target_alias
            .clone()
            .unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias
            .clone()
            .unwrap_or_else(|| "s".to_string());
        let target_table_ref = "__merge_target".to_string();
        let source_table_ref = "__merge_source".to_string();
        let qualified_target_ref = format!("datafusion.public.{target_table_ref}");
        let qualified_source_ref = format!("datafusion.public.{source_table_ref}");

        // Register target data as a MemTable in the full session context
        // (which has all catalog tables registered for cross-table subqueries)
        let target_mem = if target_batches.is_empty() {
            datafusion::datasource::MemTable::try_new(
                target_schema.clone(),
                vec![],
            )
        } else {
            datafusion::datasource::MemTable::try_new(
                target_schema.clone(),
                vec![target_batches],
            )
        }
        .map_err(|e| SqeError::Execution(format!("Failed to create target MemTable: {e}")))?;
        ctx
            .register_table(&qualified_target_ref, Arc::new(target_mem))
            .map_err(|e| {
                SqeError::Execution(format!("Failed to register target MemTable: {e}"))
            })?;

        // Use the pre-executed source batches (caller handles source query execution)
        if source_batches.is_empty() {
            info!(table = %table_ident, "MERGE: source returned no data, nothing to merge");
            return Ok(vec![]);
        }

        let source_schema = source_batches[0].schema();

        // Register source data as a MemTable
        let source_mem = datafusion::datasource::MemTable::try_new(
            source_schema.clone(),
            vec![source_batches],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to create source MemTable: {e}")))?;
        ctx
            .register_table(&qualified_source_ref, Arc::new(source_mem))
            .map_err(|e| {
                SqeError::Execution(format!("Failed to register source MemTable: {e}"))
            })?;

        // Rewrite the ON condition to use our MemTable names instead of aliases
        let on_rewritten = on_sql
            .replace(&format!("{t_alias}."), &format!("{target_table_ref}."))
            .replace(&format!("{s_alias}."), &format!("{source_table_ref}."));

        // Build a key column from the ON condition for matched/unmatched detection.
        // We need a column from the target side that we can check IS NULL / IS NOT NULL
        // to determine match status. Use the first target column as a sentinel.
        let target_sentinel = format!("{target_table_ref}.\"{}\"", target_columns[0]);

        // Also get a source sentinel for detecting not-matched rows
        let source_columns: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let source_sentinel = format!("{source_table_ref}.\"{}\"", source_columns[0]);

        // Classify clauses
        let mut matched_update: Option<&[sqlparser::ast::Assignment]> = None;
        let mut matched_delete = false;
        let mut not_matched_insert: Option<(&[sqlparser::ast::Ident], &MergeInsertKind)> = None;

        for clause in clauses {
            match (&clause.clause_kind, &clause.action) {
                (MergeClauseKind::Matched, MergeAction::Update { assignments }) => {
                    matched_update = Some(assignments);
                }
                (MergeClauseKind::Matched, MergeAction::Delete) => {
                    matched_delete = true;
                }
                (
                    MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
                    MergeAction::Insert(insert_expr),
                ) => {
                    not_matched_insert = Some((&insert_expr.columns, &insert_expr.kind));
                }
                (MergeClauseKind::NotMatchedBySource, MergeAction::Delete) => {
                    // Not-matched-by-source DELETE means remove target-only rows
                    // This is handled below by omitting target-only rows from the output
                    // For now, we don't support this clause
                    return Err(SqeError::NotImplemented(
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE is not yet supported".to_string(),
                    ));
                }
                _ => {
                    return Err(SqeError::NotImplemented(format!(
                        "Unsupported MERGE clause combination: {:?} / {:?}",
                        clause.clause_kind, clause.action
                    )));
                }
            }
        }

        // Build the SELECT query that implements the MERGE logic
        // Uses FULL OUTER JOIN to classify rows into:
        //   - matched (both target and source present): apply UPDATE or DELETE
        //   - not-matched (source only): apply INSERT
        //   - target-only (target only, no source match): pass through

        let column_exprs: Vec<String> = if matched_delete {
            // WHEN MATCHED THEN DELETE:
            // - Matched rows are excluded (filtered out via WHERE)
            // - Not-matched rows are inserted (if clause present)
            // - Target-only rows pass through
            //
            // We use a WHERE clause to exclude matched rows instead of CASE
            target_columns
                .iter()
                .map(|col| {
                    if let Some((insert_cols, insert_kind)) = &not_matched_insert {
                        let insert_expr = self.resolve_insert_expr(
                            col,
                            insert_cols,
                            insert_kind,
                            &source_table_ref,
                            &source_columns,
                            &s_alias,
                            &t_alias,
                            &target_table_ref,
                        );
                        format!(
                            "CASE \
                               WHEN {source_sentinel} IS NOT NULL AND {target_sentinel} IS NOT NULL THEN NULL \
                               WHEN {target_sentinel} IS NULL THEN {insert_expr} \
                               ELSE {target_table_ref}.\"{col}\" \
                             END AS \"{col}\""
                        )
                    } else {
                        format!(
                            "CASE \
                               WHEN {source_sentinel} IS NOT NULL AND {target_sentinel} IS NOT NULL THEN NULL \
                               ELSE {target_table_ref}.\"{col}\" \
                             END AS \"{col}\""
                        )
                    }
                })
                .collect()
        } else {
            // WHEN MATCHED THEN UPDATE (and optionally WHEN NOT MATCHED THEN INSERT):
            target_columns
                .iter()
                .map(|col| {
                    let update_expr = if let Some(assignments) = &matched_update {
                        self.resolve_update_expr(
                            col,
                            assignments,
                            &target_table_ref,
                            &source_table_ref,
                            &t_alias,
                            &s_alias,
                        )
                    } else {
                        format!("{target_table_ref}.\"{col}\"")
                    };

                    let insert_expr = if let Some((insert_cols, insert_kind)) = &not_matched_insert
                    {
                        self.resolve_insert_expr(
                            col,
                            insert_cols,
                            insert_kind,
                            &source_table_ref,
                            &source_columns,
                            &s_alias,
                            &t_alias,
                            &target_table_ref,
                        )
                    } else {
                        "NULL".to_string()
                    };

                    format!(
                        "CASE \
                           WHEN {target_sentinel} IS NOT NULL AND {source_sentinel} IS NOT NULL THEN {update_expr} \
                           WHEN {target_sentinel} IS NULL THEN {insert_expr} \
                           ELSE {target_table_ref}.\"{col}\" \
                         END AS \"{col}\""
                    )
                })
                .collect()
        };

        let select_sql = format!(
            "SELECT {} FROM {qualified_target_ref} AS {target_table_ref} FULL OUTER JOIN {qualified_source_ref} AS {source_table_ref} ON {on_rewritten}",
            column_exprs.join(", ")
        );

        info!(
            table = %table_ident,
            merge_sql = %select_sql,
            "MERGE: executing merge query"
        );

        let df = ctx.sql(&select_sql).await.map_err(|e| {
            SqeError::Execution(format!("Failed to plan MERGE query: {e}"))
        })?;
        let mut result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to execute MERGE query: {e}"))
        })?;

        // Deregister temp tables to avoid polluting the shared session context
        let _ = ctx.deregister_table(&qualified_target_ref);
        let _ = ctx.deregister_table(&qualified_source_ref);

        // For WHEN MATCHED THEN DELETE: filter out the rows where all columns are NULL
        // (these are the matched rows we set to NULL above)
        if matched_delete {
            let mut filtered_batches = Vec::new();
            for batch in &result_batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                // A row is a "deleted matched" row if all columns are NULL
                // (we set them to NULL for matched rows in the CASE expression).
                // Filter: keep rows where at least one column is NOT NULL.
                let mut keep = vec![true; batch.num_rows()];
                for (row, flag) in keep.iter_mut().enumerate() {
                    // Check if ALL columns are null (this is a deleted matched row)
                    let all_null = (0..batch.num_columns()).all(|c| batch.column(c).is_null(row));
                    if all_null {
                        *flag = false;
                    }
                }
                let keep_arr =
                    arrow::array::BooleanArray::from(keep);
                let filtered = filter_record_batch(batch, &keep_arr).map_err(|e| {
                    SqeError::Execution(format!("Failed to filter MERGE DELETE results: {e}"))
                })?;
                if filtered.num_rows() > 0 {
                    filtered_batches.push(filtered);
                }
            }
            result_batches = filtered_batches;
        }

        // Write new data files from the merged results
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        let new_data_files = if total_rows > 0 {
            write_data_files_with_metrics(&table, result_batches, "merge", self.metrics.as_ref(), self.compression()).await?
        } else {
            vec![]
        };

        info!(
            table = %table_ident,
            old_files = old_data_files.len(),
            new_files = new_data_files.len(),
            total_rows,
            "MERGE: committing CoW rewrite"
        );

        // Atomic commit: remove all old files, add new merged files
        if old_data_files.is_empty() && new_data_files.is_empty() {
            info!(table = %table_ident, "MERGE: no changes to commit");
            return Ok(vec![]);
        }

        let tx = Transaction::new(&table);
        let mut action = tx.rewrite_files();
        if !new_data_files.is_empty() {
            action = action.add_data_files(new_data_files);
        }
        if !old_data_files.is_empty() {
            action = action.delete_files(old_data_files);
        }
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply MERGE rewrite: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            SqeError::Execution(format!("Failed to commit MERGE: {e}"))
        })?;

        info!(table = %table_ident, total_rows, "MERGE committed successfully");
        Ok(affected_rows_batch(total_rows))
    }

    /// Resolve an UPDATE SET expression for a single column in the MERGE context.
    ///
    /// Rewrites alias references (e.g., `t.col` or `s.col`) to point to the
    /// MemTable names used in the FULL OUTER JOIN.
    fn resolve_update_expr(
        &self,
        col: &str,
        assignments: &[sqlparser::ast::Assignment],
        target_table_ref: &str,
        source_table_ref: &str,
        t_alias: &str,
        s_alias: &str,
    ) -> String {
        for a in assignments {
            let col_name = match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                    // Could be "t.col" or just "col"
                    let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
                    parts.last().cloned().unwrap_or_default()
                }
                sqlparser::ast::AssignmentTarget::Tuple(names) => {
                    names.first().map(|n| {
                        let parts: Vec<String> = n.0.iter().map(|i| i.value.clone()).collect();
                        parts.last().cloned().unwrap_or_default()
                    }).unwrap_or_default()
                }
            };
            if col_name == col {
                let expr_sql = format!("{}", a.value);
                // Rewrite alias references to MemTable names
                return expr_sql
                    .replace(&format!("{t_alias}."), &format!("{target_table_ref}."))
                    .replace(&format!("{s_alias}."), &format!("{source_table_ref}."));
            }
        }
        // Column not in SET assignments — pass through from target
        format!("{target_table_ref}.\"{col}\"")
    }

    /// Resolve an INSERT expression for a single column in the MERGE context.
    ///
    /// Maps the INSERT column list + VALUES to find the expression for the
    /// given target column. Rewrites alias references (e.g., `s.col`) to
    /// use the MemTable name.
    #[allow(clippy::too_many_arguments)]
    fn resolve_insert_expr(
        &self,
        col: &str,
        insert_columns: &[sqlparser::ast::Ident],
        insert_kind: &sqlparser::ast::MergeInsertKind,
        source_table_ref: &str,
        source_columns: &[String],
        s_alias: &str,
        t_alias: &str,
        target_table_ref: &str,
    ) -> String {
        use sqlparser::ast::MergeInsertKind;

        let rewrite_aliases = |expr: String| -> String {
            expr.replace(&format!("{s_alias}."), &format!("{source_table_ref}."))
                .replace(&format!("{t_alias}."), &format!("{target_table_ref}."))
        };

        match insert_kind {
            MergeInsertKind::Values(values) => {
                if insert_columns.is_empty() {
                    // No explicit column list — positional mapping by source column name.
                    if let Some(row) = values.rows.first() {
                        if let Some(idx) = source_columns.iter().position(|sc| sc == col) {
                            if idx < row.len() {
                                return rewrite_aliases(format!("{}", row[idx]));
                            }
                        }
                        return "NULL".to_string();
                    }
                    "NULL".to_string()
                } else {
                    // Explicit column list — find the column position
                    if let Some(pos) = insert_columns.iter().position(|c| c.value == col) {
                        if let Some(row) = values.rows.first() {
                            if pos < row.len() {
                                return rewrite_aliases(format!("{}", row[pos]));
                            }
                        }
                    }
                    "NULL".to_string()
                }
            }
            MergeInsertKind::Row => {
                // INSERT ROW: use the source column with the same name
                if source_columns.contains(&col.to_string()) {
                    format!("{source_table_ref}.\"{col}\"")
                } else {
                    "NULL".to_string()
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // CoW helper methods
    // -------------------------------------------------------------------------

    /// Collect all current DataFile objects from the table's manifest entries.
    ///
    /// Reads the current snapshot's manifest list, loads each manifest, and
    /// collects all data file entries that are Added or Existing (not Deleted).
    ///
    /// Routes reads through `Table::object_cache()` so warm CoW operations
    /// avoid redundant S3 GETs. Cold reads are parallelised with
    /// `buffer_unordered` at `config.catalog.manifest_concurrency`.
    async fn collect_data_files(
        &self,
        table: &IcebergTable,
    ) -> sqe_core::Result<Vec<DataFile>> {
        let metadata_ref = table.metadata_ref();
        let snapshot = match metadata_ref.current_snapshot() {
            Some(s) => s,
            None => return Ok(vec![]), // no snapshot = empty table
        };

        let cache = table.object_cache();
        let manifest_list = cache
            .get_manifest_list(snapshot, &metadata_ref)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

        let concurrency = self.config.catalog.manifest_concurrency.max(1);
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
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

        let data_files = manifests
            .into_iter()
            .flat_map(|manifest| {
                manifest
                    .entries()
                    .iter()
                    .filter(|entry| {
                        // Only include live data files (Added or Existing), skip Deleted
                        entry.status() != ManifestStatus::Deleted
                            && entry.data_file().content_type() == DataContentType::Data
                    })
                    .map(|entry| entry.data_file().clone())
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(data_files)
    }

    /// Read all RecordBatches from a Parquet data file using the table's FileIO.
    ///
    /// Uses iceberg-rust's scan infrastructure to read a single file via the
    /// table's already-configured FileIO (which handles S3 credentials, region, etc.).
    async fn read_parquet_via_table(
        &self,
        table: &IcebergTable,
        file_path: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let file_io = table.file_io();
        let input = file_io
            .new_input(file_path)
            .map_err(|e| SqeError::Execution(format!("Failed to open file '{file_path}': {e}")))?;

        let input_file = input
            .read()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to read file '{file_path}': {e}")))?;

        let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(input_file)
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to create Parquet reader for '{file_path}': {e}"
                ))
            })?;

        let reader = reader.build().map_err(|e| {
            SqeError::Execution(format!(
                "Failed to build Parquet reader for '{file_path}': {e}"
            ))
        })?;

        let batches: Vec<RecordBatch> = reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
            })?;

        Ok(batches)
    }

    /// Evaluate a WHERE clause against a RecordBatch and return rows that do NOT match.
    /// Used for DELETE: we keep the rows that don't match the WHERE predicate.
    async fn filter_batch_negate(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<RecordBatch> {
        use arrow::compute::not;
        use datafusion::arrow::array::BooleanArray;

        // Register the batch as a temporary table so DataFusion can evaluate the predicate
        let table_name = format!("__delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table = datafusion::datasource::MemTable::try_new(
            batch.schema(),
            vec![vec![batch.clone()]],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(format!("datafusion.public.{table_name}"), Arc::new(mem_table))
            .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Execute: SELECT <where_clause> AS __match FROM __delete_<table>
        // Alias the scratch table to the original target name (see apply_update
        // for rationale) so correlated subqueries inside the WHERE clause can
        // reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({where_sql}) AS BOOLEAN) AS __match FROM datafusion.public.{table_name} AS \"{orig_name}\""
        );
        let df = ctx.sql(&eval_sql).await.map_err(|e| {
            SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}"))
        })?;
        let result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}"))
        })?;

        // Deregister temp table
        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        if result_batches.is_empty() || result_batches[0].num_rows() == 0 {
            return Ok(batch.clone());
        }

        // Build a boolean mask: NOT <predicate> (rows to keep)
        let mask_batch = &result_batches[0];
        let match_col = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                SqeError::Execution("WHERE evaluation did not produce a boolean column".into())
            })?;
        let negated = not(match_col)
            .map_err(|e| SqeError::Execution(format!("Failed to negate WHERE mask: {e}")))?;

        // Apply the mask to the original batch
        filter_record_batch(batch, &negated)
            .map_err(|e| SqeError::Execution(format!("Failed to filter batch: {e}")))
    }

    /// Evaluate a WHERE clause against a RecordBatch and return a BooleanArray indicating
    /// which rows MATCH the predicate (i.e., rows to be deleted in a MoR DELETE).
    ///
    /// Unlike `filter_batch_negate`, this returns the raw match mask rather than the
    /// filtered batch, so the caller can record which row positions matched.
    async fn filter_batch_match(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<arrow_array::BooleanArray> {
        use arrow_array::BooleanArray;

        let table_name = format!("__mor_delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table = datafusion::datasource::MemTable::try_new(
            batch.schema(),
            vec![vec![batch.clone()]],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(format!("datafusion.public.{table_name}"), Arc::new(mem_table))
            .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Alias the scratch table to the original target name so correlated
        // subqueries inside WHERE can reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({where_sql}) AS BOOLEAN) AS __match FROM datafusion.public.{table_name} AS \"{orig_name}\""
        );
        let df = ctx.sql(&eval_sql).await.map_err(|e| {
            SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}"))
        })?;
        let result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}"))
        })?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        if result_batches.is_empty() || result_batches[0].num_rows() == 0 {
            // No rows matched
            return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
        }

        let mask_batch = &result_batches[0];
        let match_col = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                SqeError::Execution("WHERE evaluation did not produce a boolean column".into())
            })?
            .clone();

        Ok(match_col)
    }

    /// Apply UPDATE SET assignments to a RecordBatch using DataFusion SQL evaluation.
    ///
    /// For each column, generates CASE WHEN <where> THEN <new_value> ELSE <old_value> END.
    /// Unchanged columns pass through directly.
    async fn apply_update(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        assignments: &[sqlparser::ast::Assignment],
        where_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<RecordBatch> {
        let table_name = format!("__update_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table = datafusion::datasource::MemTable::try_new(
            batch.schema(),
            vec![vec![batch.clone()]],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(format!("datafusion.public.{table_name}"), Arc::new(mem_table))
            .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Build assignment map: column_name -> expression_sql
        let mut assignment_map = std::collections::HashMap::new();
        for a in assignments {
            let col_name = match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(name) => format!("{name}"),
                sqlparser::ast::AssignmentTarget::Tuple(names) => {
                    // Tuple assignment (a, b) = ... — take first for simplicity
                    names.first().map(|n| format!("{n}")).unwrap_or_default()
                }
            };
            let expr_sql = format!("{}", a.value);
            assignment_map.insert(col_name, expr_sql);
        }

        // Build SELECT with CASE expressions for assigned columns
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let col = f.name().clone();
                if let Some(expr) = assignment_map.get(&col) {
                    format!(
                        "CASE WHEN ({where_sql}) THEN ({expr}) ELSE \"{col}\" END AS \"{col}\""
                    )
                } else {
                    format!("\"{col}\"")
                }
            })
            .collect();

        // Alias the scratch table back to the UPDATE target's original name so
        // correlated subqueries inside the SET expression can reference it.
        // e.g. `SET x = (SELECT ... WHERE ... = holding_summary.hs_ca_id)` needs
        // `holding_summary` to be in scope; without the alias DataFusion only
        // sees `__update_holding_summary` and fails to resolve the correlation.
        let select_sql = format!(
            "SELECT {cols} FROM datafusion.public.{table_name} AS \"{orig_name}\"",
            cols = columns.join(", "),
        );
        let df = ctx.sql(&select_sql).await.map_err(|e| {
            SqeError::Execution(format!("Failed to evaluate UPDATE: {e}"))
        })?;
        let result_batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to collect UPDATE results: {e}"))
        })?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        // Return the first (and only) result batch
        result_batches.into_iter().next().ok_or_else(|| {
            SqeError::Execution("UPDATE produced no output batches".to_string())
        })
    }

    /// Count rows matching a WHERE clause in a batch.
    async fn count_matching_rows(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<usize> {
        let table_name = format!("__count_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table = datafusion::datasource::MemTable::try_new(
            batch.schema(),
            vec![vec![batch.clone()]],
        )
        .map_err(|e| SqeError::Execution(format!("MemTable error: {e}")))?;
        ctx.register_table(format!("datafusion.public.{table_name}"), Arc::new(mem_table))
            .map_err(|e| SqeError::Execution(format!("Register error: {e}")))?;

        // Alias the scratch table to the original target name (see apply_update
        // for rationale) — allows `tablename.col` references in WHERE subqueries
        // to resolve correctly.
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM datafusion.public.{table_name} AS \"{orig_name}\" WHERE {where_sql}"
        );
        let df = ctx.sql(&sql).await.map_err(|e| {
            SqeError::Execution(format!("Count query failed: {e}"))
        })?;
        let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Count collect failed: {e}"))
        })?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        let count = batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
            })
            .map(|a| a.value(0) as usize)
            .unwrap_or(0);
        Ok(count)
    }

    /// Rewrite `IN (SELECT ...)` to `IN (val1, val2, ...)` in a WHERE-clause string.
    ///
    /// DataFusion's physical planner rejects `InSubquery` in UPDATE/DELETE context
    /// with "Physical plan does not support logical expression InSubquery(...)".
    /// SELECT works fine; only DML fails.
    ///
    /// Workaround: before executing DML, detect any `IN (subquery)` in the WHERE
    /// clause, execute each subquery as a standalone SELECT (using the same session
    /// context / Iceberg snapshot), collect the result values, and rewrite the
    /// WHERE clause to use `IN (literal_list)`.  The rewritten SQL is semantically
    /// identical and DataFusion handles it without issues.
    ///
    /// Returns the original string unchanged when no subqueries are present, so
    /// the fast path has zero overhead.
    async fn rewrite_in_subquery_where(
        &self,
        where_sql: &str,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<String> {
        use sqlparser::ast::{Expr, Value as SqlValue};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Quick bail-out: most WHERE clauses don't contain subqueries.
        if !where_sql.to_uppercase().contains("SELECT") {
            return Ok(where_sql.to_string());
        }

        // Parse the WHERE expression by wrapping it in a dummy SELECT.
        let dummy_sql = format!("SELECT * FROM __dummy WHERE {where_sql}");
        let mut stmts = Parser::parse_sql(&GenericDialect {}, &dummy_sql)
            .map_err(|e| SqeError::Execution(format!("IN-subquery rewrite parse error: {e}")))?;

        let where_expr = match stmts.first_mut() {
            Some(sqlparser::ast::Statement::Query(q)) => {
                match q.body.as_mut() {
                    sqlparser::ast::SetExpr::Select(sel) => sel.selection.take(),
                    _ => return Ok(where_sql.to_string()),
                }
            }
            _ => return Ok(where_sql.to_string()),
        };

        let mut expr = match where_expr {
            Some(e) => e,
            None => return Ok(where_sql.to_string()),
        };

        // Collect all InSubquery occurrences and replace them with sentinel
        // placeholder expressions. We store the extracted subquery SQL strings
        // and the `negated` flag so we can execute them asynchronously.
        let mut subqueries: Vec<(String, bool)> = Vec::new();
        collect_and_replace_in_subqueries(&mut expr, &mut subqueries);

        if subqueries.is_empty() {
            return Ok(where_sql.to_string());
        }

        // Execute each collected subquery and gather the literal value lists.
        //
        // `value_lists` is Vec<rows> where each row is Vec<column_values>.
        // For single-column IN subqueries each row has exactly one element.
        // For multi-column (tuple) IN subqueries each row has N elements,
        // one per column of the subquery result, enabling the OR-of-ANDs rewrite.
        let mut value_lists: Vec<Vec<Vec<Expr>>> = Vec::with_capacity(subqueries.len());
        for (subquery_sql, _negated) in &subqueries {
            let df = ctx.sql(subquery_sql).await.map_err(|e| {
                SqeError::Execution(format!(
                    "IN-subquery execution failed for `{subquery_sql}`: {e}"
                ))
            })?;
            let batches = df.collect().await.map_err(|e| {
                SqeError::Execution(format!(
                    "IN-subquery collect failed for `{subquery_sql}`: {e}"
                ))
            })?;

            let mut rows: Vec<Vec<Expr>> = Vec::new();
            for batch in &batches {
                let num_cols = batch.num_columns();
                'row: for row in 0..batch.num_rows() {
                    let mut col_literals: Vec<Expr> = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let col = batch.column(col_idx);
                        // Skip entire row if any column is NULL.
                        if col.is_null(row) {
                            continue 'row;
                        }
                        let val_str = arrow::util::display::array_value_to_string(col, row)
                            .unwrap_or_default();
                        let literal = match col.data_type() {
                            arrow::datatypes::DataType::Int8
                            | arrow::datatypes::DataType::Int16
                            | arrow::datatypes::DataType::Int32
                            | arrow::datatypes::DataType::Int64
                            | arrow::datatypes::DataType::UInt8
                            | arrow::datatypes::DataType::UInt16
                            | arrow::datatypes::DataType::UInt32
                            | arrow::datatypes::DataType::UInt64 => {
                                Expr::Value(SqlValue::Number(val_str, false))
                            }
                            arrow::datatypes::DataType::Float32
                            | arrow::datatypes::DataType::Float64 => {
                                Expr::Value(SqlValue::Number(val_str, false))
                            }
                            _ => Expr::Value(SqlValue::SingleQuotedString(val_str)),
                        };
                        col_literals.push(literal);
                    }
                    rows.push(col_literals);
                }
            }
            value_lists.push(rows);
        }

        // Second pass: substitute the sentinel placeholders with InList / Boolean
        // (single-column) or OR-of-ANDs (multi-column / tuple IN).
        let mut idx = 0usize;
        substitute_in_subquery_placeholders(&mut expr, &subqueries, &value_lists, &mut idx);

        let rewritten = format!("{expr}");
        tracing::info!(
            original = %where_sql,
            rewritten = %&rewritten[..rewritten.len().min(300)],
            subquery_count = subqueries.len(),
            "Rewrote IN (subquery) to literal predicate(s) for DML WHERE clause"
        );
        Ok(rewritten)
    }

    fn format_version(&self) -> FormatVersion {
        match self.config.catalog.default_table_format_version {
            3 => FormatVersion::V3,
            1 => FormatVersion::V1,
            _ => FormatVersion::V2,
        }
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
                self.table_cache.clone(),
                None, None,
            )
            .await?,
        );

        // Warm up the REST catalog by listing namespaces. The RisingWave fork's
        // RestCatalog requires this initial API call to bootstrap its internal
        // session state before load_table works correctly.
        let _ = session_catalog.list_namespaces().await;

        Ok(session_catalog.as_catalog())
    }
}

// ---------------------------------------------------------------------------
// IN-subquery rewrite helpers (free functions, sync, no async_recursion needed)
// ---------------------------------------------------------------------------

/// Walk `expr` recursively, replacing every `InSubquery { expr, subquery, negated }` node
/// with a sentinel `Expr::Value(Value::Placeholder("?N"))`.
///
/// The extracted `(subquery_sql, negated)` tuples are pushed into `out` in the
/// order they are encountered (depth-first, left-to-right), which must match
/// the order used by `substitute_in_subquery_placeholders`.
fn collect_and_replace_in_subqueries(
    expr: &mut sqlparser::ast::Expr,
    out: &mut Vec<(String, bool)>,
) {
    use sqlparser::ast::{Expr, Value as SqlValue};

    match expr {
        Expr::InSubquery { expr: inner, subquery, negated } => {
            let subquery_sql = format!("SELECT * FROM ({subquery}) AS __sq");
            let neg = *negated;
            out.push((subquery_sql, neg));
            let idx = out.len() - 1;
            // Replace this node with a sentinel placeholder.
            // We keep `inner` to use later; stash its current value.
            let inner_box = std::mem::replace(inner, Box::new(Expr::Value(SqlValue::Null)));
            *expr = Expr::Nested(Box::new(Expr::InList {
                expr: inner_box,
                list: vec![Expr::Value(SqlValue::Placeholder(format!("?{idx}")))],
                negated: neg,
            }));
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_and_replace_in_subqueries(left, out);
            collect_and_replace_in_subqueries(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_and_replace_in_subqueries(inner, out);
        }
        Expr::Nested(inner) => {
            collect_and_replace_in_subqueries(inner, out);
        }
        Expr::Between { expr: e, low, high, .. } => {
            collect_and_replace_in_subqueries(e, out);
            collect_and_replace_in_subqueries(low, out);
            collect_and_replace_in_subqueries(high, out);
        }
        _ => {}
    }
}

/// Walk `expr` and substitute every sentinel `InList` with a single `Placeholder("?N")`
/// list element with the real expression (using the collected values) or a `Boolean`
/// constant when the value list is empty.
///
/// For single-column IN subqueries the sentinel's `col_expr` is a plain column reference
/// and the result is `col IN (v1, v2, ...)` (or `NOT IN`).
///
/// For multi-column (tuple) IN subqueries the sentinel's `col_expr` is `Expr::Tuple`
/// and the result is an OR chain of AND conditions:
///   `(col1=v1 AND col2=v2) OR (col1=v3 AND col2=v4) OR ...`
/// This avoids DataFusion's lack of support for tuple-IN in DML context.
///
/// `idx` tracks which entry in `subqueries`/`value_lists` we are currently visiting.
/// Must be called with the same `expr` that `collect_and_replace_in_subqueries` modified.
#[allow(clippy::only_used_in_recursion)]
fn substitute_in_subquery_placeholders(
    expr: &mut sqlparser::ast::Expr,
    subqueries: &[(String, bool)],
    value_lists: &[Vec<Vec<sqlparser::ast::Expr>>],
    idx: &mut usize,
) {
    use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};

    match expr {
        // Sentinel pattern: Nested(InList { list: [Placeholder("?N")], .. })
        Expr::Nested(inner) => {
            if let Expr::InList { list, negated, expr: col_expr } = inner.as_mut() {
                if list.len() == 1 {
                    if let Expr::Value(SqlValue::Placeholder(p)) = &list[0] {
                        if p.starts_with('?') {
                            let current_idx = *idx;
                            *idx += 1;
                            let rows = &value_lists[current_idx];
                            let neg = *negated;
                            let col =
                                std::mem::replace(col_expr, Box::new(Expr::Value(SqlValue::Null)));

                            if rows.is_empty() {
                                // IN () → FALSE; NOT IN () → TRUE
                                *expr = Expr::Value(SqlValue::Boolean(!neg));
                                return;
                            }

                            // Determine whether this is a tuple IN by checking if the
                            // sentinel's col_expr (now in `col`) is an Expr::Tuple.
                            if let Expr::Tuple(col_refs) = col.as_ref() {
                                // Multi-column (tuple) IN rewrite:
                                // (col1, col2) IN (SELECT c1, c2 ...) becomes
                                // (col1=v1 AND col2=v2) OR (col1=v3 AND col2=v4) OR ...
                                // NOT IN becomes the negation of that OR chain.
                                let col_refs = col_refs.clone();
                                let mut or_chain: Option<Expr> = None;
                                for row in rows {
                                    // Build AND chain for this row: col1=v1 AND col2=v2 ...
                                    let mut and_chain: Option<Expr> = None;
                                    for (col_ref, val) in col_refs.iter().zip(row.iter()) {
                                        let eq = Expr::BinaryOp {
                                            left: Box::new(col_ref.clone()),
                                            op: BinaryOperator::Eq,
                                            right: Box::new(val.clone()),
                                        };
                                        and_chain = Some(match and_chain {
                                            Some(prev) => Expr::BinaryOp {
                                                left: Box::new(prev),
                                                op: BinaryOperator::And,
                                                right: Box::new(eq),
                                            },
                                            None => eq,
                                        });
                                    }
                                    if let Some(and_expr) = and_chain {
                                        or_chain = Some(match or_chain {
                                            Some(prev) => Expr::BinaryOp {
                                                left: Box::new(prev),
                                                op: BinaryOperator::Or,
                                                right: Box::new(Expr::Nested(Box::new(and_expr))),
                                            },
                                            None => and_expr,
                                        });
                                    }
                                }
                                let or_expr = or_chain
                                    .unwrap_or(Expr::Value(SqlValue::Boolean(false)));
                                *expr = if neg {
                                    // NOT IN → wrap in NOT
                                    Expr::UnaryOp {
                                        op: sqlparser::ast::UnaryOperator::Not,
                                        expr: Box::new(Expr::Nested(Box::new(or_expr))),
                                    }
                                } else {
                                    Expr::Nested(Box::new(or_expr))
                                };
                            } else {
                                // Single-column IN: flatten rows to a value list and use InList.
                                let flat: Vec<Expr> =
                                    rows.iter().filter_map(|r| r.first().cloned()).collect();
                                *expr = Expr::InList {
                                    expr: col,
                                    list: flat,
                                    negated: neg,
                                };
                            }
                            return;
                        }
                    }
                }
            }
            substitute_in_subquery_placeholders(inner, subqueries, value_lists, idx);
        }
        Expr::BinaryOp { left, right, .. } => {
            substitute_in_subquery_placeholders(left, subqueries, value_lists, idx);
            substitute_in_subquery_placeholders(right, subqueries, value_lists, idx);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            substitute_in_subquery_placeholders(inner, subqueries, value_lists, idx);
        }
        Expr::Between { expr: e, low, high, .. } => {
            substitute_in_subquery_placeholders(e, subqueries, value_lists, idx);
            substitute_in_subquery_placeholders(low, subqueries, value_lists, idx);
            substitute_in_subquery_placeholders(high, subqueries, value_lists, idx);
        }
        _ => {}
    }
}

/// Convert an Arrow schema to an Iceberg schema.
///
/// Arrow schemas from DataFusion queries do not carry Parquet field-id metadata,
/// so we cannot use `iceberg::arrow::arrow_schema_to_schema` directly (it
/// requires the `PARQUET_FIELD_ID` key). Instead, we convert each Arrow field
/// individually using `arrow_type_to_type` and assign sequential field IDs
/// starting from 1.
/// Convert a sqlparser SQL data type to an Arrow DataType.
pub(crate) fn sql_type_to_arrow(sql_type: &sqlparser::ast::DataType) -> sqe_core::Result<arrow_schema::DataType> {
    use arrow_schema::DataType;
    use sqlparser::ast::DataType as SqlType;

    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::TinyInt(_) | SqlType::Int8(_) => Ok(DataType::Int8),
        SqlType::SmallInt(_) | SqlType::Int16 => Ok(DataType::Int16),
        SqlType::Int(_) | SqlType::Integer(_) | SqlType::Int32 => Ok(DataType::Int32),
        SqlType::BigInt(_) | SqlType::Int64 => Ok(DataType::Int64),
        SqlType::Float(_) | SqlType::Real => Ok(DataType::Float32),
        SqlType::Double(_) | SqlType::DoublePrecision => Ok(DataType::Float64),
        SqlType::Varchar(_) | SqlType::CharVarying(_) | SqlType::Text | SqlType::String(_) => {
            Ok(DataType::Utf8)
        }
        SqlType::Char(_) | SqlType::Character(_) => Ok(DataType::Utf8),
        SqlType::Binary(_) | SqlType::Varbinary(_) | SqlType::Bytea => Ok(DataType::Binary),
        SqlType::Date => Ok(DataType::Date32),
        SqlType::Timestamp(precision, _tz_info) => {
            let p = precision.unwrap_or(6);
            match p {
                0..=3 => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)),
                4..=6 => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
                _ => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)),
            }
        }
        SqlType::Decimal(info) | SqlType::Numeric(info) => {
            let (precision, scale) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p, *s),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p, 0),
                sqlparser::ast::ExactNumberInfo::None => (38, 10),
            };
            Ok(DataType::Decimal128(precision as u8, scale as i8))
        }
        other => Err(SqeError::NotImplemented(format!(
            "SQL type not supported for CREATE TABLE: {other}"
        ))),
    }
}

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
    use arrow_schema::{DataType, Field, TimeUnit};
    use sqlparser::ast::{DataType as SqlType, ExactNumberInfo, TimezoneInfo};

    // -------------------------------------------------------------------------
    // arrow_schema_to_iceberg tests
    // -------------------------------------------------------------------------

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

    #[test]
    fn test_arrow_schema_to_iceberg_field_ids_are_sequential() {
        // Field IDs must start at 1 and be sequential (Iceberg spec requirement).
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[2].id, 3);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_nullable_flags() {
        // Nullable Arrow fields → optional Iceberg fields (required == false).
        // Non-nullable Arrow fields → required Iceberg fields (required == true).
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("required_col", DataType::Int64, false),
            Field::new("optional_col", DataType::Utf8, true),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert!(fields[0].required, "non-nullable Arrow field should map to required Iceberg field");
        assert!(!fields[1].required, "nullable Arrow field should map to optional Iceberg field");
    }

    #[test]
    fn test_arrow_schema_to_iceberg_all_required() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        for field in &fields {
            assert!(field.required, "all fields should be required");
        }
    }

    #[test]
    fn test_arrow_schema_to_iceberg_wide_schema() {
        // Verify that a schema with many fields produces the correct count and IDs.
        let columns: Vec<Field> = (0..20)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int64, i % 2 == 0))
            .collect();
        let count = columns.len();
        let arrow_schema = ArrowSchema::new(columns);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), count);
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(field.id, (i + 1) as i32);
            assert_eq!(field.name, format!("col_{i}"));
        }
    }

    #[test]
    fn test_arrow_schema_to_iceberg_various_numeric_types() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("i8_col", DataType::Int8, true),
            Field::new("i16_col", DataType::Int16, true),
            Field::new("i32_col", DataType::Int32, true),
            Field::new("i64_col", DataType::Int64, true),
            Field::new("f32_col", DataType::Float32, true),
            Field::new("f64_col", DataType::Float64, true),
        ]);

        // Should convert without error; all numeric types are supported by Iceberg.
        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 6);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_temporal_types() {
        // Iceberg only supports Microsecond precision for timestamps (not Millisecond or
        // Nanosecond). This test verifies the supported subset converts cleanly.
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("date_col", DataType::Date32, true),
            Field::new(
                "ts_us_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 2);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_millisecond_timestamp_is_unsupported() {
        // The underlying iceberg-rust library rejects Timestamp(Millisecond) — this is a
        // known limitation and the error path must be exercised rather than silently
        // producing a wrong schema.
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "ts_ms_col",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let result = arrow_schema_to_iceberg(&arrow_schema);
        assert!(
            result.is_err(),
            "Timestamp(Millisecond) should not convert to Iceberg"
        );
    }

    #[test]
    fn test_arrow_schema_to_iceberg_decimal_type() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("amount", DataType::Decimal128(18, 4), false),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "amount");
        assert!(fields[0].required);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_binary_type() {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("payload", DataType::Binary, true),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 1);
    }

    // -------------------------------------------------------------------------
    // sql_type_to_arrow tests (private fn accessed via super::)
    // -------------------------------------------------------------------------

    #[test]
    fn test_sql_type_to_arrow_boolean() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Boolean).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn test_sql_type_to_arrow_integer_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::TinyInt(None)).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int8(None)).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::SmallInt(None)).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int16).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Integer(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int32).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::BigInt(None)).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int64).unwrap(),
            DataType::Int64
        );
    }

    #[test]
    fn test_sql_type_to_arrow_float_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Float(None)).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Real).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Double(sqlparser::ast::ExactNumberInfo::None)).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::DoublePrecision).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_sql_type_to_arrow_string_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Varchar(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Text).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Char(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Character(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::CharVarying(None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::String(None)).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_sql_type_to_arrow_binary_variants() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Binary(None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Varbinary(None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Bytea).unwrap(),
            DataType::Binary
        );
    }

    #[test]
    fn test_sql_type_to_arrow_date() {
        assert_eq!(
            sql_type_to_arrow(&SqlType::Date).unwrap(),
            DataType::Date32
        );
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_default_precision() {
        // No precision → defaults to 6 → Microsecond
        let result = sql_type_to_arrow(&SqlType::Timestamp(None, TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_low_precision() {
        // Precision 0-3 → Millisecond
        for p in 0u64..=3 {
            let result =
                sql_type_to_arrow(&SqlType::Timestamp(Some(p), TimezoneInfo::None)).unwrap();
            assert_eq!(
                result,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                "precision {p} should map to Millisecond"
            );
        }
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_mid_precision() {
        // Precision 4-6 → Microsecond
        for p in 4u64..=6 {
            let result =
                sql_type_to_arrow(&SqlType::Timestamp(Some(p), TimezoneInfo::None)).unwrap();
            assert_eq!(
                result,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "precision {p} should map to Microsecond"
            );
        }
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_high_precision() {
        // Precision 7+ → Nanosecond
        let result =
            sql_type_to_arrow(&SqlType::Timestamp(Some(9), TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_full() {
        let result = sql_type_to_arrow(&SqlType::Decimal(
            ExactNumberInfo::PrecisionAndScale(18, 4),
        ))
        .unwrap();
        assert_eq!(result, DataType::Decimal128(18, 4));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_precision_only() {
        let result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::Precision(10))).unwrap();
        // Scale defaults to 0
        assert_eq!(result, DataType::Decimal128(10, 0));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_no_info() {
        let result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::None)).unwrap();
        // Defaults to Decimal128(38, 10)
        assert_eq!(result, DataType::Decimal128(38, 10));
    }

    #[test]
    fn test_sql_type_to_arrow_numeric_alias() {
        // NUMERIC is the same as DECIMAL in the implementation.
        let decimal_result = sql_type_to_arrow(&SqlType::Decimal(
            ExactNumberInfo::PrecisionAndScale(12, 2),
        ))
        .unwrap();
        let numeric_result = sql_type_to_arrow(&SqlType::Numeric(
            ExactNumberInfo::PrecisionAndScale(12, 2),
        ))
        .unwrap();
        assert_eq!(decimal_result, numeric_result);
    }

    #[test]
    fn test_sql_type_to_arrow_unsupported_returns_err() {
        // JSON is not in the supported set — must return a NotImplemented error.
        let result = sql_type_to_arrow(&SqlType::JSON);
        assert!(result.is_err());
        let err = result.unwrap_err();
        // The error should be a NotImplemented variant.
        assert!(
            matches!(err, sqe_core::SqeError::NotImplemented(_)),
            "expected NotImplemented, got: {err:?}"
        );
    }

    // -------------------------------------------------------------------------
    // handle_ingest table-name parsing (pure logic, no catalog required)
    // -------------------------------------------------------------------------

    /// The ingest name-parsing logic is embedded in `handle_ingest`. We test it
    /// by extracting the equivalent logic as a free function here so we can unit
    /// test the three cases (valid 2-part, valid 3-part, invalid) without
    /// needing a real catalog connection.
    fn parse_ingest_table_name(table_name: &str) -> sqe_core::Result<(String, String)> {
        let parts: Vec<&str> = table_name.split('.').collect();
        match parts.as_slice() {
            [ns, tbl] => Ok(((*ns).to_string(), (*tbl).to_string())),
            [_cat, ns, tbl] => Ok(((*ns).to_string(), (*tbl).to_string())),
            _ => Err(sqe_core::SqeError::Execution(format!(
                "Invalid table name for ingest: {table_name}"
            ))),
        }
    }

    #[test]
    fn test_ingest_table_name_two_part() {
        let (ns, tbl) = parse_ingest_table_name("my_schema.my_table").unwrap();
        assert_eq!(ns, "my_schema");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_ingest_table_name_three_part_catalog() {
        // "catalog.schema.table" — catalog is discarded, schema + table kept.
        let (ns, tbl) = parse_ingest_table_name("my_catalog.my_schema.my_table").unwrap();
        assert_eq!(ns, "my_schema");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_ingest_table_name_single_part_is_error() {
        let result = parse_ingest_table_name("just_a_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_table_name_four_part_is_error() {
        // More than three parts is also invalid.
        let result = parse_ingest_table_name("a.b.c.d");
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_table_name_empty_string_is_error() {
        // An empty string yields a single empty segment → invalid.
        let result = parse_ingest_table_name("");
        assert!(result.is_err());
    }
}

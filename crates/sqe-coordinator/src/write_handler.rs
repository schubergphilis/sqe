use std::sync::atomic::{AtomicU64, Ordering};
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

use sqe_catalog::puffin_stats::{
    puffin_stats_enabled, write_puffin_sidecar,
};
use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::table_properties::{
    WriteMode, resolve_delete_mode, resolve_merge_mode, resolve_update_mode,
};
use sqe_core::{Session, SqeConfig, SqeError};
use tracing::instrument;

use crate::catalog_ops::parse_table_ref;
use crate::writer::{
    parse_parquet_compression, write_data_files_streaming_with_metrics,
    write_data_files_with_metrics, write_equality_delete_files, write_position_delete_files,
};

/// Build a single-row RecordBatch reporting affected row count.
/// Matches Trino's DML response which returns the update count.
fn affected_rows_batch(count: usize) -> Vec<RecordBatch> {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "rows_affected",
        DataType::Int64,
        false,
    )]));
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
        Self {
            config,
            metrics: None,
            table_cache: None,
        }
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

    /// Emit a Puffin NDV sidecar for the most recent snapshot, if opted in.
    ///
    /// Runs after a successful append-style commit. The caller reloads the
    /// table to get the new snapshot; we then build theta sketches from the
    /// just-written batches and register the sidecar via
    /// [`UpdateStatisticsAction`]. A failure here is logged and swallowed:
    /// the data commit has already succeeded, so losing statistics is not
    /// worth rolling back for.
    async fn maybe_emit_puffin_sidecar(
        &self,
        catalog: &Arc<dyn Catalog>,
        table_ident: &TableIdent,
        batches_for_stats: &[RecordBatch],
    ) {
        // Reload the table post-commit so we see the new snapshot id and
        // sequence number.
        let table = match catalog.load_table(table_ident).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: could not reload table for sidecar");
                return;
            }
        };
        if !puffin_stats_enabled(table.metadata().properties()) {
            return;
        }
        let Some(snapshot_id) = table.metadata().current_snapshot_id() else {
            // No snapshot yet — nothing to attach the sidecar to.
            return;
        };
        let sequence_number = table.metadata().last_sequence_number();

        let metadata_location = match table.metadata_location() {
            Some(p) => p,
            None => {
                tracing::warn!("puffin: no metadata_location on loaded table");
                return;
            }
        };
        let base_dir = metadata_location
            .rsplit_once('/')
            .map(|(head, _tail)| head)
            .unwrap_or(metadata_location)
            .to_string();

        let stats_file = match write_puffin_sidecar(
            table.file_io(),
            &base_dir,
            table.metadata().current_schema(),
            batches_for_stats,
            snapshot_id,
            sequence_number,
        )
        .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: sidecar write failed");
                return;
            }
        };

        // Reload fresh for the stats transaction so our view of the metadata
        // is current (avoid committing against stale base metadata).
        let table = match catalog.load_table(table_ident).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: reload for stats tx failed");
                return;
            }
        };
        let tx = Transaction::new(&table);
        let action = tx.update_statistics().set_statistics(stats_file);
        let tx = match action.apply(tx) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "puffin: apply update_statistics failed");
                return;
            }
        };
        if let Err(e) = tx.commit(catalog.as_ref()).await {
            tracing::warn!(error = %e, "puffin: commit update_statistics failed");
        }
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
        let create_format_version = self.format_version();
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .format_version(create_format_version)
            .properties(format_version_properties(create_format_version))
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
            // Clone batches cheaply for the Puffin sidecar when the table has
            // opted in. RecordBatch clones are Arc bumps, not data copies.
            let stats_snapshot: Option<Vec<RecordBatch>> =
                puffin_stats_enabled(table.metadata().properties())
                    .then(|| batches.clone());

            let data_files = write_data_files_with_metrics(
                &table,
                batches,
                "ctas",
                self.metrics.as_ref(),
                self.compression(),
            )
            .await?;

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

                if let Some(stats_batches) = stats_snapshot {
                    self.maybe_emit_puffin_sidecar(&catalog, &table_ident, &stats_batches)
                        .await;
                }
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

        let create_format_version = self.format_version();
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .format_version(create_format_version)
            .properties(format_version_properties(create_format_version))
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
            let tx = action
                .apply(tx)
                .map_err(|e| SqeError::Execution(format!("Failed to apply fast append: {e}")))?;
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
            let tx = action
                .apply(tx)
                .map_err(|e| SqeError::Execution(format!("Failed to apply fast append: {e}")))?;
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
    /// Honours V3 features (nanosecond timestamps, column defaults) by
    /// auto-bumping the format version.
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

        if ct.columns.is_empty() {
            return Err(SqeError::Execution(
                "CREATE TABLE requires at least one column definition".into(),
            ));
        }

        // Convert SQL column definitions to Arrow schema.
        let arrow_fields: Vec<arrow_schema::Field> = ct
            .columns
            .iter()
            .map(|col| {
                let arrow_type = sql_type_to_arrow(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(arrow_schema::Field::new(
                    col.name.value.clone(),
                    arrow_type,
                    nullable,
                ))
            })
            .collect::<sqe_core::Result<Vec<_>>>()?;

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg_schema = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns)?;

        // Decide the format version. V3 features auto-upgrade the table;
        // otherwise fall back to the configured default (normally V2).
        let needs_v3 = requires_v3_features(&ct.columns, &iceberg_schema);
        let format_version = if needs_v3 {
            FormatVersion::V3
        } else {
            self.format_version()
        };

        info!(
            username = %session.user.username,
            namespace = %namespace,
            table = %name,
            columns = arrow_schema.fields().len(),
            format_version = ?format_version,
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

        // Merge in user-specified TBLPROPERTIES / WITH options so Polaris
        // stores them alongside the format-version directive. Without this
        // step CREATE TABLE silently drops every property the user typed.
        let mut props = format_version_properties(format_version);
        merge_user_table_properties(&mut props, &ct.table_properties);
        merge_user_table_properties(&mut props, &ct.with_options);

        // Translate any PARTITIONED BY (...) clause into an Iceberg
        // UnboundPartitionSpec. Identity transforms cover bare column
        // refs; year/month/day/hour/bucket/truncate/void cover the
        // standard hidden-partitioning transforms.
        let partition_spec = build_partition_spec(
            ct.partition_by.as_deref(),
            &iceberg_schema,
        )?;

        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .format_version(format_version)
            .properties(props)
            .partition_spec_opt(partition_spec)
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

        // Clone batches for a Puffin sidecar only when the table opted in.
        let stats_snapshot: Option<Vec<RecordBatch>> =
            puffin_stats_enabled(table.metadata().properties()).then(|| batches.clone());

        // Write data files
        let data_files = write_data_files_with_metrics(
            &table,
            batches,
            "insert",
            self.metrics.as_ref(),
            self.compression(),
        )
        .await?;

        if !data_files.is_empty() {
            // Commit via fast-append
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            let tx = action
                .apply(tx)
                .map_err(|e| SqeError::Execution(format!("Failed to apply fast append: {e}")))?;

            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit INSERT transaction: {e}"))
            })?;

            if let Some(stats_batches) = stats_snapshot {
                self.maybe_emit_puffin_sidecar(&catalog, &table_ident, &stats_batches)
                    .await;
            }

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

        let data_files = write_data_files_with_metrics(
            &table,
            batches,
            "ingest",
            self.metrics.as_ref(),
            self.compression(),
        )
        .await?;

        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action
                .apply(tx)
                .map_err(|e| SqeError::Execution(format!("Failed to apply fast append: {e}")))?;
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
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit truncate: {e}")))?;
            info!(table = %table_ident, "DELETE: table truncated successfully");
            return Ok(vec![]);
        }

        // WHERE clause present: CoW rewrite
        let raw_where = format!("{}", where_clause.as_ref().unwrap());
        // Lift any `IN (subquery)` expressions out of the WHERE into materialised
        // scratch MemTables joined via LEFT JOIN. The cleanup guard must outlive
        // every per-batch evaluator call below; `_in_subq_guard`'s Drop runs at
        // the end of this handler and deregisters the scratch tables.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;
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
                let filtered = self
                    .filter_batch_negate(ctx, batch, &where_sql, &joins_sql, &table_ident)
                    .await?;
                total_deleted += batch.num_rows() - filtered.num_rows();
                if filtered.num_rows() > 0 {
                    surviving_batches.push(filtered);
                }
            }

            // Write surviving rows as new data files (skip if all rows deleted)
            if !surviving_batches.is_empty() {
                let new_files = write_data_files_with_metrics(
                    &table,
                    surviving_batches,
                    "delete",
                    self.metrics.as_ref(),
                    self.compression(),
                )
                .await?;
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
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("Failed to apply DELETE rewrite: {e}")))?;
        tx.commit(catalog.as_catalog().as_ref())
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to commit DELETE: {e}")))?;

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
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit truncate: {e}")))?;
            info!(table = %table_ident, "MoR DELETE: table truncated successfully");
            return Ok(vec![]);
        }

        let raw_where = format!("{}", where_clause.as_ref().unwrap());
        // Lift any `IN (subquery)` expressions; see `handle_delete` for details.
        // The guard must outlive the per-batch loop below.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;
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
                    .filter_batch_match(ctx, batch, &where_sql, &joins_sql, &table_ident)
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
        let delete_files =
            write_position_delete_files(&table, position_deletes, self.compression()).await?;

        // Commit: append position delete files. FastAppendAction auto-routes DataFiles
        // with content_type=PositionDeletes into the delete manifest entry.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(delete_files);
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply MoR DELETE fast-append: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref())
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to commit MoR DELETE: {e}")))?;

        info!(table = %table_ident, deleted_rows = deleted_count, "MoR DELETE committed successfully");
        Ok(affected_rows_batch(deleted_count))
    }

    /// Handle DELETE FROM using Merge-on-Read with equality deletes.
    ///
    /// Phase E, tasks 6.7 and 6.8. Commits an equality-delete file that names
    /// the table's declared identifier fields (primary key). Downstream readers
    /// exclude any row where those fields match one of the emitted values.
    ///
    /// Advantages over position deletes:
    ///
    /// - Snapshot-stable: rows added later that match the equality keys are
    ///   also excluded without writing new deletes.
    /// - Compact: one delete file per batch of keys, regardless of how many
    ///   data files those keys span.
    ///
    /// The file is committed via `RowDeltaAction` so the operation classifies
    /// as `Overwrite` with `added-delete-files=1` in the snapshot summary,
    /// matching Java Iceberg and Spark semantics.
    #[instrument(skip(self, session, stmt, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_delete_equality(
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

        // Equality deletes require declared identifier-field-ids (primary key).
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "table {table_ident} has no identifier-field-ids; equality-delete path requires a primary key"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "equality DELETE: table has no data files");
            return Ok(vec![]);
        }

        let where_clause = &delete.selection;
        // DELETE without WHERE clause: falling back to CoW truncate as
        // emitting an empty equality-delete file serves no purpose.
        if where_clause.is_none() {
            info!(
                table = %table_ident,
                file_count = old_data_files.len(),
                "equality DELETE: no WHERE clause, falling back to CoW truncate"
            );
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_files(old_data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply truncate transaction: {e}"))
            })?;
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit truncate: {e}")))?;
            return Ok(vec![]);
        }

        let raw_where = format!("{}", where_clause.as_ref().unwrap());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;
        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            where_clause = %where_sql,
            equality_ids = ?identifier_field_ids,
            "equality DELETE: scanning for matching rows"
        );

        // Scan every data file and collect rows where WHERE matches. Equality
        // deletes need only the identifier columns, so we keep the full batch
        // for now and let the writer project downstream.
        let mut key_batches: Vec<RecordBatch> = Vec::new();
        let mut total_matched: usize = 0;

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self.read_parquet_via_table(&table, &file_path).await?;
            if batches.is_empty() {
                continue;
            }
            for batch in batches {
                let match_mask = self
                    .filter_batch_match(ctx, &batch, &where_sql, &joins_sql, &table_ident)
                    .await?;
                let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
                    SqeError::Execution(format!("failed to filter match rows: {e}"))
                })?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                total_matched += filtered.num_rows();
                key_batches.push(filtered);
            }
        }

        if total_matched == 0 {
            info!(table = %table_ident, "equality DELETE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        let delete_files = write_equality_delete_files(
            &table,
            key_batches,
            identifier_field_ids,
            self.compression(),
        )
        .await?;

        // Commit via RowDeltaAction: this emits Operation::Overwrite with
        // added-delete-files > 0 and no removed/added data files. The
        // SnapshotProducer's added-delete-files summary key mirrors Spark.
        let tx = Transaction::new(&table);
        let snapshot_id = table.metadata().current_snapshot_id();
        let mut action = tx.row_delta().add_delete_files(delete_files);
        if let Some(snap) = snapshot_id {
            action = action.validate_from_snapshot(snap);
        }
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply RowDelta transaction: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit equality DELETE: {e}"))
            }
        })?;

        info!(
            table = %table_ident,
            deleted_rows = total_matched,
            "equality DELETE committed successfully"
        );
        Ok(affected_rows_batch(total_matched))
    }

    /// Dispatch a DELETE statement to CoW, MoR position deletes, or MoR
    /// equality deletes based on the target table's `write.delete.mode`
    /// property (Phase E, task 6.8).
    ///
    /// Semantics:
    ///
    /// - `copy-on-write` (default): rewrite data files. Backward compatible.
    /// - `merge-on-read`: pick equality deletes when the table declares a
    ///   primary key (identifier-field-ids), otherwise fall back to position
    ///   deletes.
    pub async fn handle_delete_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Peek at the target table to read its properties. Any parse or
        // load error falls through to the default CoW path, which surfaces
        // the error at that point.
        let delete = match stmt {
            Statement::Delete(d) => d,
            _ => return self.handle_delete(session, stmt, catalog, ctx).await,
        };
        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(t) => t,
            sqlparser::ast::FromTable::WithoutKeyword(t) => t,
        };
        let table_factor_name = match tables.first().map(|t| &t.relation) {
            Some(sqlparser::ast::TableFactor::Table { name, .. }) => name,
            _ => return self.handle_delete(session, stmt, catalog, ctx).await,
        };

        let Ok((namespace, name)) = parse_table_ref(table_factor_name) else {
            return self.handle_delete(session, stmt, catalog, ctx).await;
        };
        let table_ident = TableIdent::new(namespace, name);
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self.handle_delete(session, stmt, catalog, ctx).await;
        };

        let mode = resolve_delete_mode(table.metadata().properties())?;

        match mode {
            WriteMode::MergeOnRead => {
                // Prefer equality deletes when the table declares a PK.
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "DELETE dispatch: MoR + equality deletes"
                    );
                    self.handle_delete_equality(session, stmt, catalog, ctx).await
                } else {
                    info!(
                        table = %table_ident,
                        "DELETE dispatch: MoR + position deletes (no PK declared)"
                    );
                    self.handle_delete_mor(session, stmt, catalog, ctx).await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "DELETE dispatch: CoW");
                self.handle_delete(session, stmt, catalog, ctx).await
            }
        }
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
        // Lift any `IN (subquery)` expressions; see `handle_delete` for details.
        // The guard must outlive the per-batch loop below.
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;

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
                    .apply_update(
                        ctx,
                        batch,
                        assignments,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                    )
                    .await?;
                rewritten_batches.push(rewritten);
            }

            // Count updated rows by comparing before/after
            for batch in &batches {
                let count = self
                    .count_matching_rows(ctx, batch, &where_sql, &joins_sql, &table_ident)
                    .await?;
                total_updated += count;
            }

            let new_files = write_data_files_with_metrics(
                &table,
                rewritten_batches,
                "update",
                self.metrics.as_ref(),
                self.compression(),
            )
            .await?;
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
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("Failed to apply UPDATE rewrite: {e}")))?;
        tx.commit(catalog.as_catalog().as_ref())
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to commit UPDATE: {e}")))?;

        info!(table = %table_ident, updated_rows = total_updated, "UPDATE committed successfully");
        Ok(affected_rows_batch(total_updated))
    }

    /// Dispatch an UPDATE statement to CoW or MoR based on
    /// `write.update.mode` (Phase H, task 9.4).
    ///
    /// - `copy-on-write` (default): rewrite affected data files in place.
    /// - `merge-on-read`: fall through to MoR only when the table declares
    ///   identifier-field-ids (primary key). Without a PK we cannot emit an
    ///   equality delete for the old row, so we fall back to CoW with a log
    ///   entry rather than fail.
    pub async fn handle_update_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Peek at the target table to read its properties.
        let update_table = match stmt {
            Statement::Update { table, .. } => table,
            _ => return self.handle_update(session, stmt, catalog, ctx).await,
        };
        let table_factor_name = match &update_table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            _ => return self.handle_update(session, stmt, catalog, ctx).await,
        };
        let Ok((namespace, name)) = parse_table_ref(table_factor_name) else {
            return self.handle_update(session, stmt, catalog, ctx).await;
        };
        let table_ident = TableIdent::new(namespace, name);
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self.handle_update(session, stmt, catalog, ctx).await;
        };

        let mode = resolve_update_mode(table.metadata().properties())?;

        match mode {
            WriteMode::MergeOnRead => {
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "UPDATE dispatch: MoR + equality deletes"
                    );
                    self.handle_update_equality(session, stmt, catalog, ctx).await
                } else {
                    info!(
                        table = %table_ident,
                        "UPDATE dispatch: MoR requested but no PK; falling back to CoW"
                    );
                    self.handle_update(session, stmt, catalog, ctx).await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "UPDATE dispatch: CoW");
                self.handle_update(session, stmt, catalog, ctx).await
            }
        }
    }

    /// Handle UPDATE in Merge-on-Read mode.
    ///
    /// For each matched row we emit two records:
    ///
    /// 1. A row in a new data file carrying the UPDATE'd values.
    /// 2. A row in an equality-delete file carrying the old primary-key
    ///    values so the pre-update row is hidden at scan time.
    ///
    /// Both are committed atomically via `RowDeltaAction`. Unmatched rows
    /// in existing data files are left alone: no file rewrite. The SF100
    /// `trade_result_update_holding` pattern benefits here because the
    /// working set is the small set of matched rows, not every file in
    /// the partition.
    #[instrument(skip(self, session, stmt, catalog, ctx), fields(username = %session.user.username))]
    pub async fn handle_update_equality(
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

        // MoR UPDATE requires declared identifier-field-ids (primary key)
        // so we can emit an equality delete for the old row. Without a PK
        // the dispatcher falls back to CoW; reaching this function without
        // a PK is a caller bug.
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "MoR UPDATE on {table_ident} requires identifier-field-ids (primary key)"
            )));
        }

        let old_data_files = self.collect_data_files(&table).await?;
        if old_data_files.is_empty() {
            info!(table = %table_ident, "MoR UPDATE: table has no data files");
            return Ok(vec![]);
        }

        let raw_where = selection
            .as_ref()
            .map(|w| format!("{w}"))
            .unwrap_or_else(|| "TRUE".to_string());
        let (where_sql, joins_sql, _in_subq_guard) =
            self.lift_in_subqueries(&raw_where, ctx).await?;

        info!(
            table = %table_ident,
            file_count = old_data_files.len(),
            assignments = assignments.len(),
            where_clause = %where_sql,
            equality_ids = ?identifier_field_ids,
            "MoR UPDATE: scanning for matching rows"
        );

        // For each data file, find the matched rows twice:
        //   - once with the UPDATE applied, projected into a new data file
        //   - once as the raw matched rows, projected into an equality
        //     delete file keyed on identifier-field-ids
        //
        // The CoW `apply_update` helper returns a per-batch full rewrite
        // (matched rows get new values, others pass through). For MoR we
        // only want the matched rows, so we filter after apply_update.
        let mut new_row_batches: Vec<RecordBatch> = Vec::new();
        let mut key_batches: Vec<RecordBatch> = Vec::new();
        let mut total_updated: usize = 0;

        for data_file in &old_data_files {
            let file_path = data_file.file_path().to_string();
            let batches = self.read_parquet_via_table(&table, &file_path).await?;
            if batches.is_empty() {
                continue;
            }
            for batch in batches {
                let match_mask = self
                    .filter_batch_match(ctx, &batch, &where_sql, &joins_sql, &table_ident)
                    .await?;
                // Skip files with zero matches: no new data rows, no
                // equality deletes. Leaving them alone is the point of MoR.
                let matched_count = match_mask.true_count();
                if matched_count == 0 {
                    continue;
                }
                total_updated += matched_count;

                // Old PKs for the equality delete. Filter the original
                // batch by the match mask; the equality-delete writer
                // projects identifier columns from the Iceberg schema.
                let old_keys = filter_record_batch(&batch, &match_mask).map_err(|e| {
                    SqeError::Execution(format!("failed to filter match rows: {e}"))
                })?;
                if old_keys.num_rows() > 0 {
                    key_batches.push(old_keys);
                }

                // New values for the data file. `apply_update` produces a
                // full-batch rewrite with CASE WHEN where THEN new ELSE
                // old END, then we filter to only the matched rows so we
                // do not re-write the unchanged ones.
                let full_rewrite = self
                    .apply_update(
                        ctx,
                        &batch,
                        assignments,
                        &where_sql,
                        &joins_sql,
                        &table_ident,
                    )
                    .await?;
                let new_rows =
                    filter_record_batch(&full_rewrite, &match_mask).map_err(|e| {
                        SqeError::Execution(format!("failed to filter updated rows: {e}"))
                    })?;
                if new_rows.num_rows() > 0 {
                    new_row_batches.push(new_rows);
                }
            }
        }

        if total_updated == 0 {
            info!(table = %table_ident, "MoR UPDATE: no matching rows, nothing to commit");
            return Ok(vec![]);
        }

        // Write the data file with the new values and the equality delete
        // file with the old keys. Both go into one RowDelta commit.
        let new_data_files = write_data_files_with_metrics(
            &table,
            new_row_batches,
            "update-mor",
            self.metrics.as_ref(),
            self.compression(),
        )
        .await?;

        let delete_files = write_equality_delete_files(
            &table,
            key_batches,
            identifier_field_ids,
            self.compression(),
        )
        .await?;

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            new_data_files = new_data_files.len(),
            equality_delete_files = delete_files.len(),
            "MoR UPDATE: committing row delta"
        );

        let tx = Transaction::new(&table);
        let snapshot_id = table.metadata().current_snapshot_id();
        let mut action = tx
            .row_delta()
            .add_data_files(new_data_files)
            .add_delete_files(delete_files);
        if let Some(snap) = snapshot_id {
            action = action.validate_from_snapshot(snap);
        }
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply MoR UPDATE row delta: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit MoR UPDATE: {e}"))
            }
        })?;

        info!(
            table = %table_ident,
            updated_rows = total_updated,
            "MoR UPDATE committed successfully"
        );
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
            let arrow_schema =
                iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|e| {
                    SqeError::Execution(format!("Failed to convert Iceberg schema to Arrow: {e}"))
                })?;
            Arc::new(arrow_schema)
        };

        let target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Use the target alias (or a default) for the merge MemTable names
        let t_alias = target_alias.clone().unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias.clone().unwrap_or_else(|| "s".to_string());
        let target_table_ref = "__merge_target".to_string();
        let source_table_ref = "__merge_source".to_string();
        let qualified_target_ref = format!("datafusion.public.{target_table_ref}");
        let qualified_source_ref = format!("datafusion.public.{source_table_ref}");

        // Register target data as a MemTable in the full session context
        // (which has all catalog tables registered for cross-table subqueries)
        let target_mem = if target_batches.is_empty() {
            datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![])
        } else {
            datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![target_batches])
        }
        .map_err(|e| SqeError::Execution(format!("Failed to create target MemTable: {e}")))?;
        ctx.register_table(&qualified_target_ref, Arc::new(target_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register target MemTable: {e}")))?;

        // Use the pre-executed source batches (caller handles source query execution)
        if source_batches.is_empty() {
            info!(table = %table_ident, "MERGE: source returned no data, nothing to merge");
            return Ok(vec![]);
        }

        let source_schema = source_batches[0].schema();

        // Register source data as a MemTable
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), vec![source_batches])
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&qualified_source_ref, Arc::new(source_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register source MemTable: {e}")))?;

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

        let df = ctx
            .sql(&select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to plan MERGE query: {e}")))?;
        let mut result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to execute MERGE query: {e}")))?;

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
                let keep_arr = arrow::array::BooleanArray::from(keep);
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
            write_data_files_with_metrics(
                &table,
                result_batches,
                "merge",
                self.metrics.as_ref(),
                self.compression(),
            )
            .await?
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
        let tx = action
            .apply(tx)
            .map_err(|e| SqeError::Execution(format!("Failed to apply MERGE rewrite: {e}")))?;
        tx.commit(catalog.as_catalog().as_ref())
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to commit MERGE: {e}")))?;

        info!(table = %table_ident, total_rows, "MERGE committed successfully");
        Ok(affected_rows_batch(total_rows))
    }

    /// Dispatch MERGE to CoW or MoR based on `write.merge.mode`
    /// (Phase H, task 9.7).
    ///
    /// - `copy-on-write` (default): rewrite all target files via
    ///   `handle_merge` (pre-existing behaviour).
    /// - `merge-on-read`: route to `handle_merge_equality` when the table
    ///   declares a primary key. Without a PK we fall back to CoW because
    ///   the MATCHED clauses need old-row keys for the equality delete.
    pub async fn handle_merge_dispatch(
        &self,
        session: &Session,
        stmt: &Statement,
        source_batches: Vec<RecordBatch>,
        catalog: Arc<SessionCatalog>,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Peek at the target table to read its properties.
        let merge_table = match stmt {
            Statement::Merge { table, .. } => table,
            _ => {
                return self
                    .handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await;
            }
        };
        let target_name = match merge_table {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            _ => {
                return self
                    .handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await;
            }
        };
        let Ok((namespace, name)) = parse_table_ref(target_name) else {
            return self
                .handle_merge(session, stmt, source_batches, catalog, ctx)
                .await;
        };
        let table_ident = TableIdent::new(namespace, name);
        let Ok(table) = catalog.load_table(&table_ident).await else {
            return self
                .handle_merge(session, stmt, source_batches, catalog, ctx)
                .await;
        };

        let mode = resolve_merge_mode(table.metadata().properties())?;
        match mode {
            WriteMode::MergeOnRead => {
                let has_ids = table
                    .metadata()
                    .current_schema()
                    .identifier_field_ids()
                    .next()
                    .is_some();
                if has_ids {
                    info!(
                        table = %table_ident,
                        "MERGE dispatch: MoR + equality deletes"
                    );
                    self.handle_merge_equality(session, stmt, source_batches, catalog, ctx)
                        .await
                } else {
                    info!(
                        table = %table_ident,
                        "MERGE dispatch: MoR requested but no PK; falling back to CoW"
                    );
                    self.handle_merge(session, stmt, source_batches, catalog, ctx)
                        .await
                }
            }
            WriteMode::CopyOnWrite => {
                info!(table = %table_ident, "MERGE dispatch: CoW");
                self.handle_merge(session, stmt, source_batches, catalog, ctx)
                    .await
            }
        }
    }

    /// Handle MERGE INTO in Merge-on-Read mode.
    ///
    /// The three MERGE clause branches map onto RowDelta inputs:
    ///
    /// - `WHEN MATCHED THEN UPDATE`: emit a data file row with the new
    ///   values and an equality-delete row with the matched target's PK.
    /// - `WHEN MATCHED THEN DELETE`: emit an equality-delete row only.
    /// - `WHEN NOT MATCHED THEN INSERT`: emit a data file row only.
    ///
    /// All outputs commit in one `RowDeltaAction`. Target rows that have
    /// no matching source row pass through untouched: no rewrite, no
    /// delete.
    #[instrument(
        skip(self, session, stmt, source_batches, catalog, ctx),
        fields(username = %session.user.username)
    )]
    pub async fn handle_merge_equality(
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

        let source_alias = match source_factor {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };
        let on_sql = format!("{on_expr}");

        let table = catalog.load_table(&table_ident).await?;
        let identifier_field_ids: Vec<i32> = table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect();
        if identifier_field_ids.is_empty() {
            return Err(SqeError::Execution(format!(
                "MoR MERGE on {table_ident} requires identifier-field-ids (primary key)"
            )));
        }

        // Collect target batches for the JOIN. Unlike CoW we do not need
        // to rewrite them; the RowDelta only touches matched rows.
        let old_data_files = self.collect_data_files(&table).await?;
        let mut target_batches: Vec<RecordBatch> = Vec::new();
        for data_file in &old_data_files {
            let file_path = data_file.file_path();
            let batches = self.read_parquet_via_table(&table, file_path).await?;
            target_batches.extend(batches);
        }

        // Resolve schema from the existing data or the Iceberg metadata.
        let target_schema = if let Some(first) = target_batches.first() {
            first.schema()
        } else {
            let iceberg_schema = table.metadata().current_schema();
            let arrow_schema =
                iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|e| {
                    SqeError::Execution(format!("Failed to convert Iceberg schema to Arrow: {e}"))
                })?;
            Arc::new(arrow_schema)
        };
        let target_columns: Vec<String> = target_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let t_alias = target_alias.clone().unwrap_or_else(|| "t".to_string());
        let s_alias = source_alias.clone().unwrap_or_else(|| "s".to_string());
        let target_ref = "__merge_mor_target".to_string();
        let source_ref = "__merge_mor_source".to_string();
        let q_target = format!("datafusion.public.{target_ref}");
        let q_source = format!("datafusion.public.{source_ref}");

        let target_mem = if target_batches.is_empty() {
            datafusion::datasource::MemTable::try_new(target_schema.clone(), vec![])
        } else {
            datafusion::datasource::MemTable::try_new(
                target_schema.clone(),
                vec![target_batches.clone()],
            )
        }
        .map_err(|e| SqeError::Execution(format!("Failed to create target MemTable: {e}")))?;
        ctx.register_table(&q_target, Arc::new(target_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register target MemTable: {e}")))?;

        if source_batches.is_empty() {
            info!(table = %table_ident, "MoR MERGE: source returned no data, nothing to merge");
            let _ = ctx.deregister_table(&q_target);
            return Ok(vec![]);
        }
        let source_schema = source_batches[0].schema();
        let source_mem =
            datafusion::datasource::MemTable::try_new(source_schema.clone(), vec![source_batches])
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to create source MemTable: {e}"))
                })?;
        ctx.register_table(&q_source, Arc::new(source_mem))
            .map_err(|e| SqeError::Execution(format!("Failed to register source MemTable: {e}")))?;

        let source_columns: Vec<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let on_rewritten = on_sql
            .replace(&format!("{t_alias}."), &format!("{target_ref}."))
            .replace(&format!("{s_alias}."), &format!("{source_ref}."));

        // Classify MERGE clauses.
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

        info!(
            table = %table_ident,
            on_condition = %on_sql,
            matched_update = matched_update.is_some(),
            matched_delete,
            not_matched_insert = not_matched_insert.is_some(),
            "MoR MERGE: planning row delta"
        );

        let mut new_data_batches: Vec<RecordBatch> = Vec::new();
        let mut equality_delete_batches: Vec<RecordBatch> = Vec::new();
        let mut updated_rows: usize = 0;
        let mut deleted_rows: usize = 0;
        let mut inserted_rows: usize = 0;

        // MATCHED UPDATE: INNER JOIN of target + source, emit new row per
        // match plus an equality-delete row for the old target PK.
        if let Some(assignments) = matched_update {
            let update_cols: Vec<String> = target_columns
                .iter()
                .map(|col| {
                    let expr = self.resolve_update_expr(
                        col,
                        assignments,
                        &target_ref,
                        &source_ref,
                        &t_alias,
                        &s_alias,
                    );
                    format!("{expr} AS \"{col}\"")
                })
                .collect();
            let new_sql = format!(
                "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten}",
                update_cols.join(", ")
            );
            let df = ctx
                .sql(&new_sql)
                .await
                .map_err(|e| SqeError::Execution(format!("MoR MERGE UPDATE plan failed: {e}")))?;
            let batches = df.collect().await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE UPDATE execution failed: {e}"))
            })?;
            for batch in batches {
                updated_rows += batch.num_rows();
                if batch.num_rows() > 0 {
                    new_data_batches.push(batch);
                }
            }

            // Old target rows for the equality delete. The writer projects
            // identifier columns, so we select all target columns for the
            // matched rows.
            let old_cols: Vec<String> = target_columns
                .iter()
                .map(|col| format!("{target_ref}.\"{col}\" AS \"{col}\""))
                .collect();
            let old_sql = format!(
                "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten}",
                old_cols.join(", ")
            );
            let df = ctx.sql(&old_sql).await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE old-key plan failed: {e}"))
            })?;
            let batches = df.collect().await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE old-key execution failed: {e}"))
            })?;
            for batch in batches {
                if batch.num_rows() > 0 {
                    equality_delete_batches.push(batch);
                }
            }
        }

        // MATCHED DELETE: emit equality-delete rows only, no new data file.
        if matched_delete {
            let old_cols: Vec<String> = target_columns
                .iter()
                .map(|col| format!("{target_ref}.\"{col}\" AS \"{col}\""))
                .collect();
            let del_sql = format!(
                "SELECT {} FROM {q_target} INNER JOIN {q_source} ON {on_rewritten}",
                old_cols.join(", ")
            );
            let df = ctx.sql(&del_sql).await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE DELETE plan failed: {e}"))
            })?;
            let batches = df.collect().await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE DELETE execution failed: {e}"))
            })?;
            for batch in batches {
                deleted_rows += batch.num_rows();
                if batch.num_rows() > 0 {
                    equality_delete_batches.push(batch);
                }
            }
        }

        // NOT MATCHED INSERT: LEFT ANTI JOIN from source to target.
        if let Some((insert_cols, insert_kind)) = not_matched_insert {
            let insert_exprs: Vec<String> = target_columns
                .iter()
                .map(|col| {
                    let expr = self.resolve_insert_expr(
                        col,
                        insert_cols,
                        insert_kind,
                        &source_ref,
                        &source_columns,
                        &s_alias,
                        &t_alias,
                        &target_ref,
                    );
                    format!("{expr} AS \"{col}\"")
                })
                .collect();
            // A source row is "not matched" when the JOIN on the ON
            // condition does not find a target row. LEFT ANTI JOIN gives
            // that directly.
            let insert_sql = format!(
                "SELECT {} FROM {q_source} WHERE NOT EXISTS \
                 (SELECT 1 FROM {q_target} WHERE {on_rewritten})",
                insert_exprs.join(", ")
            );
            let df = ctx.sql(&insert_sql).await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE INSERT plan failed: {e}"))
            })?;
            let batches = df.collect().await.map_err(|e| {
                SqeError::Execution(format!("MoR MERGE INSERT execution failed: {e}"))
            })?;
            for batch in batches {
                inserted_rows += batch.num_rows();
                if batch.num_rows() > 0 {
                    new_data_batches.push(batch);
                }
            }
        }

        let _ = ctx.deregister_table(&q_target);
        let _ = ctx.deregister_table(&q_source);

        let total_touched = updated_rows + deleted_rows + inserted_rows;
        if total_touched == 0 {
            info!(table = %table_ident, "MoR MERGE: no matched or not-matched rows");
            return Ok(vec![]);
        }

        let new_data_files = if !new_data_batches.is_empty() {
            write_data_files_with_metrics(
                &table,
                new_data_batches,
                "merge-mor",
                self.metrics.as_ref(),
                self.compression(),
            )
            .await?
        } else {
            vec![]
        };

        let delete_files = if !equality_delete_batches.is_empty() {
            write_equality_delete_files(
                &table,
                equality_delete_batches,
                identifier_field_ids,
                self.compression(),
            )
            .await?
        } else {
            vec![]
        };

        info!(
            table = %table_ident,
            updated_rows,
            deleted_rows,
            inserted_rows,
            new_data_files = new_data_files.len(),
            equality_delete_files = delete_files.len(),
            "MoR MERGE: committing row delta"
        );

        let tx = Transaction::new(&table);
        let snapshot_id = table.metadata().current_snapshot_id();
        let mut action = tx.row_delta();
        if !new_data_files.is_empty() {
            action = action.add_data_files(new_data_files);
        }
        if !delete_files.is_empty() {
            action = action.add_delete_files(delete_files);
        }
        if let Some(snap) = snapshot_id {
            action = action.validate_from_snapshot(snap);
        }
        let tx = action.apply(tx).map_err(|e| {
            SqeError::Execution(format!("Failed to apply MoR MERGE row delta: {e}"))
        })?;
        tx.commit(catalog.as_catalog().as_ref()).await.map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("stale snapshot") || msg.contains("rowdelta conflict") {
                SqeError::Catalog(format!("commit conflict: {e}"))
            } else {
                SqeError::Execution(format!("Failed to commit MoR MERGE: {e}"))
            }
        })?;

        info!(
            table = %table_ident,
            updated_rows,
            deleted_rows,
            inserted_rows,
            "MoR MERGE committed successfully"
        );
        Ok(affected_rows_batch(total_touched))
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
                sqlparser::ast::AssignmentTarget::Tuple(names) => names
                    .first()
                    .map(|n| {
                        let parts: Vec<String> = n.0.iter().map(|i| i.value.clone()).collect();
                        parts.last().cloned().unwrap_or_default()
                    })
                    .unwrap_or_default(),
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
    async fn collect_data_files(&self, table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
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
        let manifests: Vec<Arc<iceberg::spec::Manifest>> =
            futures::stream::iter(manifest_list.entries().iter().cloned())
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

        let batches: Vec<RecordBatch> =
            reader
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to read Parquet file '{file_path}': {e}"))
                })?;

        Ok(batches)
    }

    /// Evaluate a WHERE clause against a RecordBatch and return rows that do NOT match.
    /// Used for DELETE: we keep the rows that don't match the WHERE predicate.
    ///
    /// `joins_sql` is a concatenation of `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`] and is spliced into the outer SELECT's
    /// FROM clause immediately after the aliased target. Pass an empty string
    /// when no lifted joins are needed.
    async fn filter_batch_negate(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<RecordBatch> {
        use arrow::compute::not;
        use datafusion::arrow::array::BooleanArray;

        // Register the batch as a temporary table so DataFusion can evaluate the predicate
        let table_name = format!("__delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Execute: SELECT <where_clause> AS __match FROM __delete_<table>
        // Alias the scratch table to the original target name (see apply_update
        // for rationale) so correlated subqueries inside the WHERE clause can
        // reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({where_sql}) AS BOOLEAN) AS __match \
             FROM datafusion.public.{table_name} AS \"{orig_name}\"{joins_sql}"
        );
        let df = ctx
            .sql(&eval_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}")))?;

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
    ///
    /// `joins_sql` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`]; see `filter_batch_negate` for details.
    async fn filter_batch_match(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<arrow_array::BooleanArray> {
        use arrow_array::BooleanArray;

        let table_name = format!("__mor_delete_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Alias the scratch table to the original target name so correlated
        // subqueries inside WHERE can reference `tablename.col`.
        let eval_sql = format!(
            "SELECT CAST(({where_sql}) AS BOOLEAN) AS __match \
             FROM datafusion.public.{table_name} AS \"{orig_name}\"{joins_sql}"
        );
        let df = ctx
            .sql(&eval_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate WHERE clause: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect WHERE evaluation: {e}")))?;

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
    ///
    /// `in_subquery_joins` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`] and is appended to the outer SELECT's FROM
    /// clause after any decorrelator-generated joins. Pass an empty string when
    /// no lifted joins are needed.
    async fn apply_update(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        assignments: &[sqlparser::ast::Assignment],
        where_sql: &str,
        in_subquery_joins: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<RecordBatch> {
        let table_name = format!("__update_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("Failed to create MemTable: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Failed to register temp table: {e}")))?;

        // Best-effort decorrelation of any `ScalarSubquery` nodes in the SET
        // expressions. DataFusion's physical planner cannot compile scalar
        // subqueries that survive inside a CASE WHEN ... THEN (subquery) ELSE
        // col END projection, so we rewrite recognised correlated-equality
        // shapes into LEFT JOINs at the outer FROM. Shapes we don't recognise
        // are left alone and will surface DataFusion's original error — no
        // change in behaviour for them.
        let (decorrelated, extra_joins) = decorrelate_scalar_subqueries(assignments, orig_name);
        let mut assignment_map = std::collections::HashMap::new();
        for d in &decorrelated {
            assignment_map.insert(d.col_name.clone(), d.expr_sql.clone());
        }

        // Build SELECT with CASE expressions for assigned columns
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let col = f.name().clone();
                if let Some(expr) = assignment_map.get(&col) {
                    format!("CASE WHEN ({where_sql}) THEN ({expr}) ELSE \"{col}\" END AS \"{col}\"")
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
        //
        // Two join sources get appended to the outer FROM clause:
        //   1. `extra_joins` from the decorrelator above — these provide the
        //      `__corrN.__val` columns substituted into the SET expressions.
        //   2. `in_subquery_joins` from `lift_in_subqueries` — these provide the
        //      `__sqN.__matched` flags referenced from the rewritten WHERE.
        // Decorrelator joins come first so any columns they introduce are in
        // scope for the IN-subquery join ON clauses (not currently exercised,
        // but preserves a consistent ordering).
        let joins_sql = if extra_joins.is_empty() {
            in_subquery_joins.to_string()
        } else {
            format!(" {}{}", extra_joins.join(" "), in_subquery_joins)
        };
        let select_sql = format!(
            "SELECT {cols} FROM datafusion.public.{table_name} AS \"{orig_name}\"{joins}",
            cols = columns.join(", "),
            joins = joins_sql,
        );
        let df = ctx
            .sql(&select_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to evaluate UPDATE: {e}")))?;
        let result_batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to collect UPDATE results: {e}")))?;

        let _ = ctx.deregister_table(format!("datafusion.public.{table_name}"));

        // Return the first (and only) result batch
        result_batches
            .into_iter()
            .next()
            .ok_or_else(|| SqeError::Execution("UPDATE produced no output batches".to_string()))
    }

    /// Count rows matching a WHERE clause in a batch.
    ///
    /// `joins_sql` carries the `LEFT JOIN ...` clauses produced by
    /// [`Self::lift_in_subqueries`]; see `filter_batch_negate` for details.
    async fn count_matching_rows(
        &self,
        ctx: &DFSessionContext,
        batch: &RecordBatch,
        where_sql: &str,
        joins_sql: &str,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<usize> {
        let table_name = format!("__count_{}", table_ident.name());
        let orig_name = table_ident.name();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                .map_err(|e| SqeError::Execution(format!("MemTable error: {e}")))?;
        ctx.register_table(
            format!("datafusion.public.{table_name}"),
            Arc::new(mem_table),
        )
        .map_err(|e| SqeError::Execution(format!("Register error: {e}")))?;

        // Alias the scratch table to the original target name (see apply_update
        // for rationale) — allows `tablename.col` references in WHERE subqueries
        // to resolve correctly.
        let sql = format!(
            "SELECT COUNT(*) AS cnt \
             FROM datafusion.public.{table_name} AS \"{orig_name}\"{joins_sql} \
             WHERE {where_sql}"
        );
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| SqeError::Execution(format!("Count query failed: {e}")))?;
        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Count collect failed: {e}")))?;

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

    /// Lift every `IN (subquery)` in `where_sql` into a LEFT JOIN over a
    /// pre-materialised DISTINCT keyset.
    ///
    /// Replaces the old literal-inlining rewriter. The old rewriter materialised
    /// each subquery into an `IN (v1, v2, ...)` or OR-of-ANDs list, producing
    /// O(N) plan text for N matching rows. TPC-E SF10 at 34,496 tuples crashed
    /// the coordinator with a stack overflow (see the regression test at
    /// `tests/in_subquery_or_stack_overflow.rs` for the failure mode).
    ///
    /// The new approach:
    ///
    /// 1. Execute each subquery once, projecting its columns as `__col0..__colK`
    ///    plus a constant `TRUE AS __matched` column, with NULL rows dropped and
    ///    DISTINCT applied.
    /// 2. Register the result as a scratch `MemTable` named `__sqe_in_subq_<id>`
    ///    where `<id>` is a process-global monotonic counter.
    /// 3. Emit a `LEFT JOIN` clause against the scratch table on the LHS columns.
    /// 4. Replace the original `IN (subquery)` node with
    ///    `COALESCE("__sq<alias_id>"."__matched", FALSE)`, wrapped in `NOT` for
    ///    `NOT IN`.
    ///
    /// Returns:
    /// - The rewritten WHERE string (O(1) in subquery cardinality).
    /// - A concatenated JOIN clause string to append to the outer SELECT's FROM.
    /// - An RAII guard that deregisters every scratch table on drop.
    ///
    /// Fast path: if `where_sql` contains no `SELECT` token, returns
    /// `(where_sql.to_string(), "", empty_guard)` with zero overhead. Matches
    /// the fast-path behaviour of the old rewriter.
    ///
    /// NULL semantics: rows where any matcher column is NULL are dropped from
    /// the scratch keyset, and outer rows with NULL in matcher columns do not
    /// match. This preserves the behaviour of the old rewriter (which skipped
    /// NULL subquery rows at the Rust level) and is a deliberate deviation from
    /// strict SQL `IN`/`NOT IN` semantics in the presence of NULLs.
    async fn lift_in_subqueries(
        &self,
        where_sql: &str,
        ctx: &DFSessionContext,
    ) -> sqe_core::Result<(String, String, InSubqueryCleanup)> {
        lift_in_subqueries(where_sql, ctx).await
    }
}

// ---------------------------------------------------------------------------
// Free-function form of the IN-subquery lifter.
//
// `WriteHandler::lift_in_subqueries` is a thin shim that delegates here. The
// method has no `self` dependencies; keeping the implementation free-standing
// lets integration tests drive it directly without constructing a full
// `SqeConfig` + `WriteHandler`.
// ---------------------------------------------------------------------------

/// See [`WriteHandler::lift_in_subqueries`] for semantics. Exposed for the
/// integration test in `tests/in_subquery_view_rewrite.rs`.
#[doc(hidden)]
pub async fn lift_in_subqueries(
    where_sql: &str,
    ctx: &DFSessionContext,
) -> sqe_core::Result<(String, String, InSubqueryCleanup)> {
    use datafusion::datasource::MemTable;
    use sqlparser::ast::SetExpr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    // Fast path: most WHERE clauses don't contain subqueries.
    if !where_sql.to_uppercase().contains("SELECT") {
        return Ok((
            where_sql.to_string(),
            String::new(),
            InSubqueryCleanup::empty(ctx),
        ));
    }

    // Parse the WHERE expression by wrapping it in a dummy SELECT.
    let dummy_sql = format!("SELECT * FROM __dummy WHERE {where_sql}");
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &dummy_sql)
        .map_err(|e| SqeError::Execution(format!("IN-subquery lift parse error: {e}")))?;

    let where_expr_opt = match stmts.first_mut() {
        Some(Statement::Query(q)) => match q.body.as_mut() {
            SetExpr::Select(sel) => sel.selection.take(),
            _ => {
                return Ok((
                    where_sql.to_string(),
                    String::new(),
                    InSubqueryCleanup::empty(ctx),
                ));
            }
        },
        _ => {
            return Ok((
                where_sql.to_string(),
                String::new(),
                InSubqueryCleanup::empty(ctx),
            ));
        }
    };

    let mut expr = match where_expr_opt {
        Some(e) => e,
        None => {
            return Ok((
                where_sql.to_string(),
                String::new(),
                InSubqueryCleanup::empty(ctx),
            ));
        }
    };

    // Walk the AST and replace every `InSubquery` node with a sentinel
    // identifier. The sentinel is a plain `Expr::Identifier` containing a
    // unique token; text-substitution at the end swaps each token for the
    // real COALESCE expression. Using a sentinel keeps the rewritten AST
    // depth O(1) in subquery cardinality and avoids the sqlparser-Display
    // trick that caused the old rewriter's stack overflow.
    let mut found: Vec<LiftedSubquery> = Vec::new();
    collect_and_sentinel_in_subqueries(&mut expr, &mut found);

    if found.is_empty() {
        return Ok((
            where_sql.to_string(),
            String::new(),
            InSubqueryCleanup::empty(ctx),
        ));
    }

    let mut joins: Vec<String> = Vec::with_capacity(found.len());
    let mut scratch_names: Vec<String> = Vec::with_capacity(found.len());
    let mut replacements: Vec<(String, String)> = Vec::with_capacity(found.len());

    for (alias_idx, lifted) in found.into_iter().enumerate() {
        let LiftedSubquery {
            lhs_cols,
            subquery_text,
            negated,
            sentinel,
        } = lifted;
        let num_cols = lhs_cols.len();

        // Preflight: get the subquery's output column names so we can alias
        // them positionally as __col0..__colN. Column names in the
        // subquery's projection may be arbitrary (e.g. `t.t_ca_id`); we do
        // not depend on them beyond this preflight.
        let preflight_sql = format!("SELECT * FROM ({subquery_text}) AS __sq");
        let pre_df = ctx.sql(&preflight_sql).await.map_err(|e| {
            SqeError::Execution(format!(
                "IN-subquery preflight failed for `{preflight_sql}`: {e}"
            ))
        })?;
        let subq_cols: Vec<String> = pre_df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        drop(pre_df);

        if subq_cols.len() != num_cols {
            return Err(SqeError::Execution(format!(
                "IN-subquery arity mismatch: LHS has {num_cols} column(s), \
                     subquery returns {} column(s)",
                subq_cols.len()
            )));
        }

        // Build the scratch-materialiser SQL. Using `__sq."<name>"` avoids
        // ambiguity when the subquery contains joins whose output column
        // names collide; wrapping in `FROM ({subquery}) AS __sq` flattens
        // the projection to a single relation.
        let projections: Vec<String> = subq_cols
            .iter()
            .enumerate()
            .map(|(i, name)| {
                format!(
                    "\"__sq\".\"{esc_name}\" AS \"__col{i}\"",
                    esc_name = name.replace('"', "\"\""),
                )
            })
            .collect();
        let null_filters: Vec<String> = subq_cols
            .iter()
            .map(|name| {
                format!(
                    "\"__sq\".\"{esc_name}\" IS NOT NULL",
                    esc_name = name.replace('"', "\"\""),
                )
            })
            .collect();
        let materialiser_sql = format!(
            "SELECT DISTINCT {projs}, TRUE AS \"__matched\" \
                 FROM ({sub}) AS __sq \
                 WHERE {filters}",
            projs = projections.join(", "),
            sub = subquery_text,
            filters = null_filters.join(" AND "),
        );

        // Execute the materialiser and wrap its output into a MemTable.
        // The DataFrame's schema is taken before `collect()` consumes it so
        // we can build the MemTable even when the subquery returns zero
        // rows (in which case the scratch table has the right shape but no
        // data; every LEFT JOIN lookup misses and `COALESCE(..., FALSE)`
        // evaluates to FALSE — i.e. `IN (empty)` is FALSE and `NOT IN
        // (empty)` is TRUE, matching the old rewriter's behaviour).
        let mat_df = ctx.sql(&materialiser_sql).await.map_err(|e| {
            SqeError::Execution(format!("IN-subquery materialiser plan failed: {e}"))
        })?;
        let schema: Arc<ArrowSchema> = Arc::new(mat_df.schema().as_arrow().clone());
        let batches = mat_df.collect().await.map_err(|e| {
            SqeError::Execution(format!("IN-subquery materialiser execution failed: {e}"))
        })?;

        let mem = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            SqeError::Execution(format!(
                "Failed to build MemTable for IN-subquery keyset: {e}"
            ))
        })?;

        // Register the scratch MemTable under the built-in `datafusion.public`
        // catalog/schema rather than the session's default. In production the
        // DML handler hands us a `SessionContext` whose default catalog is the
        // Iceberg catalog, whose `SchemaProvider` rejects `register_table` with
        // "schema provider does not support registering tables". The other
        // scratch-table call sites in this file (`filter_batch_negate`,
        // `filter_batch_match`, `apply_update`, `count_matching_rows`) already
        // use this qualified path for the same reason — see commit 725a47c
        // "fix: register DML temp tables in datafusion catalog, not Iceberg
        // catalog". Keep register, JOIN reference, and deregister in lockstep
        // so the Drop impl on `InSubqueryCleanup` releases the same table it
        // registered.
        let counter_id = IN_SUBQUERY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let scratch_name = format!("__sqe_in_subq_{counter_id}");
        let qualified_scratch = format!("datafusion.public.{scratch_name}");
        ctx.register_table(qualified_scratch.as_str(), Arc::new(mem))
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to register IN-subquery scratch MemTable: {e}"
                ))
            })?;
        scratch_names.push(qualified_scratch.clone());

        // Build the LEFT JOIN clause and the replacement expression text.
        // The per-statement alias `__sqN` is bounded by `found.len()` and
        // keeps the JOIN's ON clause readable in debug logs. The scratch
        // table is referenced through its fully-qualified catalog path to
        // match the registration above; a bare name would resolve against
        // the session's default catalog (Iceberg) and fail at plan time.
        let alias = format!("__sq{alias_idx}");
        let on_clauses: Vec<String> = (0..num_cols)
            .map(|i| format!("\"{alias}\".\"__col{i}\" = {lhs}", lhs = lhs_cols[i],))
            .collect();
        let join_clause = format!(
            " LEFT JOIN datafusion.public.\"{scratch_name}\" AS \"{alias}\" ON {on}",
            on = on_clauses.join(" AND "),
        );
        joins.push(join_clause);

        let coalesce_sql = format!("COALESCE(\"{alias}\".\"__matched\", FALSE)");
        let replacement = if negated {
            format!("(NOT {coalesce_sql})")
        } else {
            coalesce_sql
        };
        replacements.push((sentinel, replacement));
    }

    // Stringify the modified WHERE AST. Depth is O(1) in subquery
    // cardinality because every `InSubquery` node is now a single
    // `Expr::Identifier` sentinel.
    let mut rewritten = format!("{expr}");
    for (sentinel, replacement) in &replacements {
        rewritten = rewritten.replace(sentinel, replacement);
    }

    tracing::info!(
        original = %&where_sql[..where_sql.len().min(200)],
        rewritten = %&rewritten[..rewritten.len().min(200)],
        subquery_count = replacements.len(),
        "Lifted IN (subquery) into LEFT JOIN(s) for DML WHERE clause"
    );

    Ok((
        rewritten,
        joins.join(""),
        InSubqueryCleanup {
            ctx: ctx.clone(),
            scratch_tables: scratch_names,
        },
    ))
}

impl WriteHandler {
    fn format_version(&self) -> FormatVersion {
        match self.config.catalog.default_table_format_version {
            3 => FormatVersion::V3,
            1 => FormatVersion::V1,
            _ => FormatVersion::V2,
        }
    }

    /// Create a `SessionCatalogBridge` (which implements `iceberg::Catalog`)
    /// for the given session.
    async fn create_catalog_bridge(&self, session: &Session) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
                self.table_cache.clone(),
                None,
                None,
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
// IN-subquery view-lift: RAII cleanup + AST sentinelisation
// ---------------------------------------------------------------------------

/// Process-global monotonic counter for naming scratch MemTables registered
/// by [`WriteHandler::lift_in_subqueries`]. A plain atomic is sufficient:
/// DML statements across sessions never share scratch tables, and the counter
/// only has to guarantee distinctness, not ordering.
static IN_SUBQUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII handle that deregisters a set of scratch MemTables on drop.
///
/// Returned by [`WriteHandler::lift_in_subqueries`] and bound by the DML
/// handler so it outlives the per-batch SELECT loop. On drop, every
/// registered scratch table is deregistered from the session context.
///
/// `scratch_tables` stores the fully-qualified `datafusion.public.<name>`
/// path used at registration, so `deregister_table` resolves to the same
/// slot regardless of what the session's default catalog is (in production
/// that default is the Iceberg catalog).
///
/// Deregister errors are logged at `warn!` and swallowed: matches the
/// existing scratch-table cleanup behaviour inside `filter_batch_negate`,
/// `filter_batch_match`, `apply_update`, and `count_matching_rows` (see the
/// `let _ = ctx.deregister_table(...)` calls in this file).
#[doc(hidden)]
pub struct InSubqueryCleanup {
    ctx: DFSessionContext,
    /// Fully-qualified `datafusion.public.<scratch_name>` paths that Drop
    /// will deregister. Storing the qualified form (not the bare name)
    /// mirrors the registration path in `lift_in_subqueries`.
    scratch_tables: Vec<String>,
}

impl InSubqueryCleanup {
    /// Build a cleanup guard that holds no scratch tables. Used on fast-path
    /// returns from `lift_in_subqueries` where no subqueries were found.
    fn empty(ctx: &DFSessionContext) -> Self {
        Self {
            ctx: ctx.clone(),
            scratch_tables: Vec::new(),
        }
    }
}

impl Drop for InSubqueryCleanup {
    fn drop(&mut self) {
        for name in &self.scratch_tables {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "in-subquery scratch deregister failed"
                );
            }
        }
    }
}

/// One `IN (subquery)` occurrence that the lifter needs to materialise.
struct LiftedSubquery {
    /// LHS columns as SQL text. For `col IN (...)` this has length 1; for
    /// `(c1, c2) IN (...)` it has length 2. The strings are used verbatim in
    /// the JOIN ON clause, so qualified references like `target.col` are
    /// preserved.
    lhs_cols: Vec<String>,
    /// Text of the parenthesised subquery (without outer `IN (` wrapper).
    subquery_text: String,
    /// Whether the original expression was `NOT IN`.
    negated: bool,
    /// Unique sentinel token substituted into the WHERE string. The lifter
    /// replaces the `Expr::InSubquery` AST node with `Expr::Identifier(sentinel)`
    /// before stringifying; after stringification the sentinel is swapped for
    /// the real `COALESCE(...)` expression text.
    sentinel: String,
}

/// Walk `expr` recursively. For every `InSubquery { expr, subquery, negated }`
/// found, collect its LHS column list, subquery text, and a unique sentinel
/// token, then replace the node with `Expr::Identifier(sentinel)` (no wrap:
/// the `negated` flag is encoded in the replacement text that gets
/// substituted later, not in the AST).
///
/// The sentinel token uses a `__SQE_IN_SUBQ_SENTINEL_<idx>__` form that does
/// not appear in user SQL and is stable across `Display` impls.
fn collect_and_sentinel_in_subqueries(
    expr: &mut sqlparser::ast::Expr,
    out: &mut Vec<LiftedSubquery>,
) {
    use sqlparser::ast::{Expr, Ident};

    match expr {
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let neg = *negated;
            let subquery_text = format!("{subquery}");
            let lhs_cols: Vec<String> = match inner.as_ref() {
                Expr::Tuple(items) => items.iter().map(|e| format!("{e}")).collect(),
                other => vec![format!("{other}")],
            };
            let idx = out.len();
            let sentinel = format!("__SQE_IN_SUBQ_SENTINEL_{idx}__");
            out.push(LiftedSubquery {
                lhs_cols,
                subquery_text,
                negated: neg,
                sentinel: sentinel.clone(),
            });
            *expr = Expr::Identifier(Ident::new(sentinel));
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_and_sentinel_in_subqueries(left, out);
            collect_and_sentinel_in_subqueries(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_and_sentinel_in_subqueries(inner, out);
        }
        Expr::Nested(inner) => {
            collect_and_sentinel_in_subqueries(inner, out);
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_and_sentinel_in_subqueries(e, out);
            collect_and_sentinel_in_subqueries(low, out);
            collect_and_sentinel_in_subqueries(high, out);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_and_sentinel_in_subqueries(op, out);
            }
            for c in conditions {
                collect_and_sentinel_in_subqueries(c, out);
            }
            for r in results {
                collect_and_sentinel_in_subqueries(r, out);
            }
            if let Some(e) = else_result {
                collect_and_sentinel_in_subqueries(e, out);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Correlated ScalarSubquery decorrelator (UPDATE SET)
// ---------------------------------------------------------------------------
//
// DataFusion's physical planner does not compile `Expr::ScalarSubquery` that
// survives into DML. The `ScalarSubqueryToJoin` optimizer rule rewrites most
// projection-scoped scalar subqueries, but it does not reach subqueries buried
// inside a `CASE WHEN cond THEN expr ELSE col END` — which is the exact shape
// `apply_update` generates from an `UPDATE ... SET col = <expr>` statement.
//
// We decorrelate here at the sqlparser-AST level for a narrow shape:
//
//     SET col = <expr_with>(SELECT <scalar>
//                           FROM <tables>
//                           WHERE <target>.k1 = <sub_alias>.k1'
//                             AND <target>.k2 = <sub_alias>.k2'
//                             AND <local_preds>
//                           [LIMIT 1])
//
// Rewritten to:
//
//     SET col = <expr_with> "__corrN"."__val"
//     (+ LEFT JOIN (
//          SELECT <sub_alias>.k1' AS __k0, <sub_alias>.k2' AS __k1,
//                 MAX(<scalar>)    AS __val
//          FROM <tables>
//          WHERE <local_preds>
//          GROUP BY <sub_alias>.k1', <sub_alias>.k2'
//        ) AS "__corrN"
//        ON __corrN.__k0 = <target>.k1 AND __corrN.__k1 = <target>.k2)
//
// The `MAX` aggregate is an approximation of `LIMIT 1`: any one value per
// correlation group. For UPDATE statements that expect a specific row
// (ORDER BY + LIMIT 1), MAX may pick a different row, so behaviour differs
// from e.g. PostgreSQL's UPDATE FROM JOIN semantics. Use only when the
// subquery is a simple correlated lookup.
//
// Any shape not matching the above (non-equality correlation, non-scalar
// projection, nested subqueries, no correlation at all) leaves the
// assignment unchanged — current behaviour then surfaces DataFusion's clear
// "ScalarSubquery not implemented" error.

/// Returned by [`decorrelate_scalar_subqueries`] for each SET assignment.
pub(crate) struct DecorrelatedAssignment {
    /// Target column name.
    pub col_name: String,
    /// Assignment RHS expression (with correlated scalar subqueries replaced
    /// by `"__corrN"."__val"` column references if decorrelation succeeded).
    pub expr_sql: String,
}

/// Walks the `assignments`, attempts to decorrelate every correlated
/// ScalarSubquery it finds, and returns the rewritten assignments plus the
/// LEFT JOIN clauses to append to the outer SELECT's FROM.
pub(crate) fn decorrelate_scalar_subqueries(
    assignments: &[sqlparser::ast::Assignment],
    target_name: &str,
) -> (Vec<DecorrelatedAssignment>, Vec<String>) {
    use sqlparser::ast::AssignmentTarget;

    let mut out_assignments: Vec<DecorrelatedAssignment> = Vec::with_capacity(assignments.len());
    let mut joins: Vec<String> = Vec::new();
    let mut next_idx = 0usize;

    for a in assignments {
        let col_name = match &a.target {
            AssignmentTarget::ColumnName(name) => format!("{name}"),
            AssignmentTarget::Tuple(names) => {
                names.first().map(|n| format!("{n}")).unwrap_or_default()
            }
        };
        let mut expr = a.value.clone();
        rewrite_subqueries_in_expr(&mut expr, target_name, &mut next_idx, &mut joins);
        out_assignments.push(DecorrelatedAssignment {
            col_name,
            expr_sql: format!("{expr}"),
        });
    }

    (out_assignments, joins)
}

/// Recursively walk an Expr looking for `Expr::Subquery(Box<Query>)` nodes and
/// try to decorrelate each. On success, the node is replaced with a compound
/// identifier pointing at the joined lookup column; on failure, the node is
/// left untouched so the caller's current error path still fires.
fn rewrite_subqueries_in_expr(
    expr: &mut sqlparser::ast::Expr,
    target_name: &str,
    next_idx: &mut usize,
    joins: &mut Vec<String>,
) {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Subquery(q) => {
            if let Some((join_sql, alias)) = try_decorrelate_query(q, target_name, *next_idx) {
                joins.push(join_sql);
                *expr = Expr::CompoundIdentifier(vec![
                    sqlparser::ast::Ident::with_quote('"', alias),
                    sqlparser::ast::Ident::with_quote('"', "__val".to_string()),
                ]);
                *next_idx += 1;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_subqueries_in_expr(left, target_name, next_idx, joins);
            rewrite_subqueries_in_expr(right, target_name, next_idx, joins);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            rewrite_subqueries_in_expr(inner, target_name, next_idx, joins);
        }
        Expr::Nested(inner) => {
            rewrite_subqueries_in_expr(inner, target_name, next_idx, joins);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                rewrite_subqueries_in_expr(op, target_name, next_idx, joins);
            }
            for c in conditions {
                rewrite_subqueries_in_expr(c, target_name, next_idx, joins);
            }
            for r in results {
                rewrite_subqueries_in_expr(r, target_name, next_idx, joins);
            }
            if let Some(e) = else_result {
                rewrite_subqueries_in_expr(e, target_name, next_idx, joins);
            }
        }
        Expr::Function(_) => {
            // Most function arguments are expressions; the full sqlparser
            // walker is complex. For now we skip — correlated subqueries
            // buried inside function calls remain un-decorrelated and surface
            // DataFusion's original error, which is an acceptable fallback.
        }
        _ => {}
    }
}

/// Attempt to decorrelate a single scalar subquery against `target_name`.
/// Returns `(join_sql, alias)` when the shape matches, or `None` to skip.
fn try_decorrelate_query(
    q: &sqlparser::ast::Query,
    target_name: &str,
    idx: usize,
) -> Option<(String, String)> {
    use sqlparser::ast::{Expr, SelectItem, SetExpr};

    // Only plain SELECT (no UNION, no VALUES).
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => return None,
    };

    // Require a single unnamed scalar projection.
    if select.projection.len() != 1 {
        return None;
    }
    let scalar_expr_sql = match &select.projection[0] {
        SelectItem::UnnamedExpr(e) => format!("{e}"),
        SelectItem::ExprWithAlias { expr, .. } => format!("{expr}"),
        _ => return None,
    };

    // Need a WHERE clause — correlation has to live somewhere.
    let where_expr = select.selection.as_ref()?;

    // Partition the WHERE conjuncts into correlation predicates (equality
    // between a target-table column and a subquery-side column) and local
    // predicates (everything else).
    let mut conjuncts: Vec<&Expr> = Vec::new();
    collect_and_conjuncts(where_expr, &mut conjuncts);

    let mut correlation: Vec<(String, String)> = Vec::new();
    let mut local_preds: Vec<String> = Vec::new();
    for c in conjuncts {
        if let Some((t_col, s_col)) = extract_correlation_eq(c, target_name) {
            correlation.push((t_col, s_col));
        } else {
            local_preds.push(format!("{c}"));
        }
    }
    if correlation.is_empty() {
        return None;
    }

    // Rebuild FROM clause verbatim (sqlparser's Display handles joins).
    let from_sql = select
        .from
        .iter()
        .map(|t| format!("{t}"))
        .collect::<Vec<_>>()
        .join(", ");
    if from_sql.is_empty() {
        return None;
    }

    let alias = format!("__corr{idx}");
    let select_cols: Vec<String> = correlation
        .iter()
        .enumerate()
        .map(|(i, (_, s))| format!("({s}) AS __k{i}"))
        .collect();
    let group_by: Vec<String> = correlation.iter().map(|(_, s)| s.clone()).collect();

    let where_sql = if local_preds.is_empty() {
        "TRUE".to_string()
    } else {
        local_preds.join(" AND ")
    };

    let decorr_sql = format!(
        "SELECT {cols}, MAX({scalar}) AS __val FROM {from} WHERE {where_} GROUP BY {group_by}",
        cols = select_cols.join(", "),
        scalar = scalar_expr_sql,
        from = from_sql,
        where_ = where_sql,
        group_by = group_by.join(", "),
    );

    let on_clauses: Vec<String> = correlation
        .iter()
        .enumerate()
        .map(|(i, (t_col, _))| format!("\"{alias}\".__k{i} = \"{target_name}\".{t_col}"))
        .collect();

    let join_sql = format!(
        "LEFT JOIN ({sub}) AS \"{alias}\" ON {on}",
        sub = decorr_sql,
        on = on_clauses.join(" AND "),
    );

    Some((join_sql, alias))
}

/// Flatten `a AND b AND c ...` into `[a, b, c]`.
fn collect_and_conjuncts<'a>(
    expr: &'a sqlparser::ast::Expr,
    out: &mut Vec<&'a sqlparser::ast::Expr>,
) {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_conjuncts(left, out);
            collect_and_conjuncts(right, out);
        }
        Expr::Nested(inner) => collect_and_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// If `pred` is `<target>.col = <alias>.col'` (or the reversed form), return
/// `Some((<target>.col, <alias>.col'))`. The `<target>.col` is returned as
/// just the column name (outer target alias is the UPDATE target itself and
/// applied by the caller when building the ON clause).
fn extract_correlation_eq(
    pred: &sqlparser::ast::Expr,
    target_name: &str,
) -> Option<(String, String)> {
    use sqlparser::ast::{BinaryOperator, Expr};

    let (left, right) = match pred {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => (left.as_ref(), right.as_ref()),
        Expr::Nested(inner) => return extract_correlation_eq(inner, target_name),
        _ => return None,
    };

    let left_ref = compound_ident_parts(left);
    let right_ref = compound_ident_parts(right);
    match (left_ref, right_ref) {
        (Some((lq, lc)), Some((rq, rc))) if lq == target_name && rq != target_name => {
            Some((lc, format!("{rq}.{rc}")))
        }
        (Some((lq, lc)), Some((rq, rc))) if rq == target_name && lq != target_name => {
            Some((rc, format!("{lq}.{lc}")))
        }
        _ => None,
    }
}

/// If `expr` is a two-part compound identifier `a.b`, return `(a, b)`.
fn compound_ident_parts(expr: &sqlparser::ast::Expr) -> Option<(String, String)> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((parts[0].value.clone(), parts[1].value.clone()))
        }
        _ => None,
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
pub(crate) fn sql_type_to_arrow(
    sql_type: &sqlparser::ast::DataType,
) -> sqe_core::Result<arrow_schema::DataType> {
    use arrow_schema::DataType;
    use sqe_sql::{detect_ns_timestamp, NsTimestamp};
    use sqlparser::ast::DataType as SqlType;

    // V3 nanosecond timestamps arrive as `DataType::Custom` from sqlparser.
    // Route them through the sqe-sql helper so the mapping stays in one place.
    if let Some(kind) = detect_ns_timestamp(sql_type) {
        return Ok(match kind {
            NsTimestamp::WithoutTz => {
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }
            NsTimestamp::WithTz => DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("UTC".into()),
            ),
        });
    }

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
        SqlType::Timestamp(precision, tz_info) => {
            let p = precision.unwrap_or(6);
            let tz = if sqe_sql::is_tz_variant(tz_info) {
                Some("UTC".into())
            } else {
                None
            };
            match p {
                0..=3 => Ok(DataType::Timestamp(
                    arrow_schema::TimeUnit::Millisecond,
                    tz,
                )),
                4..=6 => Ok(DataType::Timestamp(
                    arrow_schema::TimeUnit::Microsecond,
                    tz,
                )),
                _ => Ok(DataType::Timestamp(
                    arrow_schema::TimeUnit::Nanosecond,
                    tz,
                )),
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

/// Build an Iceberg schema from an Arrow schema, applying column DEFAULTs.
///
/// For each column def with a `DEFAULT` option, extracts the literal and
/// sets both `initial_default` and `write_default` on the `NestedField`.
/// `initial_default` fills existing rows in case of retroactive `ADD COLUMN`;
/// `write_default` applies to new rows when no value is provided.
pub(crate) fn arrow_schema_to_iceberg_with_defaults(
    arrow_schema: &ArrowSchema,
    column_defs: &[sqlparser::ast::ColumnDef],
) -> sqe_core::Result<IcebergSchema> {
    use sqe_sql::{extract_default_literal, DefaultLiteral};

    if arrow_schema.fields().len() != column_defs.len() {
        return Err(SqeError::Execution(format!(
            "Schema field count ({}) does not match column definition count ({})",
            arrow_schema.fields().len(),
            column_defs.len()
        )));
    }

    let mut fields = Vec::with_capacity(arrow_schema.fields().len());

    for (idx, (arrow_field, col_def)) in arrow_schema
        .fields()
        .iter()
        .zip(column_defs.iter())
        .enumerate()
    {
        let field_id = (idx + 1) as i32;
        let iceberg_type = arrow_type_to_type(arrow_field.data_type()).map_err(|e| {
            SqeError::Execution(format!(
                "Cannot convert Arrow type {:?} for field '{}' to Iceberg type: {e}",
                arrow_field.data_type(),
                arrow_field.name()
            ))
        })?;

        let mut field = if arrow_field.is_nullable() {
            NestedField::optional(field_id, arrow_field.name(), iceberg_type.clone())
        } else {
            NestedField::required(field_id, arrow_field.name(), iceberg_type.clone())
        };

        // Pull the DEFAULT from the column def (if any) and lift it into
        // an iceberg Literal compatible with the target type.
        let default_expr = col_def.options.iter().find_map(|o| match &o.option {
            sqlparser::ast::ColumnOption::Default(expr) => Some(expr),
            _ => None,
        });

        if let Some(expr) = default_expr {
            let sql_literal = extract_default_literal(expr).map_err(|e| {
                SqeError::Execution(format!(
                    "Invalid DEFAULT for column '{}': {e}",
                    col_def.name.value
                ))
            })?;

            if let Some(iceberg_literal) = default_to_iceberg_literal(&sql_literal, &iceberg_type)
            {
                field = field
                    .with_initial_default(iceberg_literal.clone())
                    .with_write_default(iceberg_literal);
            } else if !matches!(sql_literal, DefaultLiteral::Null) {
                return Err(SqeError::Execution(format!(
                    "DEFAULT literal for column '{}' is not compatible with type {:?}",
                    col_def.name.value, iceberg_type
                )));
            }
            // DefaultLiteral::Null is a no-op: NULL is already the absent default.
        }

        fields.push(Arc::new(field));
    }

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| SqeError::Execution(format!("Failed to build Iceberg schema: {e}")))
}

/// Convert a SQL-surface default literal into an Iceberg `Literal`.
///
/// Returns `None` if the combination of SQL literal and target Iceberg
/// type is not representable. The caller decides whether that is a
/// hard error or a silent NULL.
pub(crate) fn default_to_iceberg_literal(
    sql_literal: &sqe_sql::DefaultLiteral,
    target: &iceberg::spec::Type,
) -> Option<iceberg::spec::Literal> {
    use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type};
    use sqe_sql::DefaultLiteral;

    let prim = match target {
        Type::Primitive(p) => p,
        // Struct/list/map defaults are not in scope.
        _ => return None,
    };

    match (sql_literal, prim) {
        (DefaultLiteral::Null, _) => None,
        (DefaultLiteral::Int(i), PrimitiveType::Int) => Some(Literal::int(*i as i32)),
        (DefaultLiteral::Int(i), PrimitiveType::Long) => Some(Literal::long(*i)),
        (DefaultLiteral::Int(i), PrimitiveType::Float) => Some(Literal::float(*i as f32)),
        (DefaultLiteral::Int(i), PrimitiveType::Double) => Some(Literal::double(*i as f64)),
        (DefaultLiteral::Float(f), PrimitiveType::Float) => Some(Literal::float(*f as f32)),
        (DefaultLiteral::Float(f), PrimitiveType::Double) => Some(Literal::double(*f)),
        (DefaultLiteral::Bool(b), PrimitiveType::Boolean) => Some(Literal::bool(*b)),
        (DefaultLiteral::String(s), PrimitiveType::String) => Some(Literal::string(s)),
        // Fall back: wrap string-like literals as strings.
        (DefaultLiteral::String(s), _) => {
            Some(Literal::Primitive(PrimitiveLiteral::String(s.clone())))
        }
        _ => None,
    }
}

/// Decide whether a CREATE TABLE definition requires Iceberg format-version 3.
///
/// Triggers V3 when any column uses a V3-only SQL type (nanosec timestamps)
/// or when any column carries a write-default through the Iceberg schema.
pub(crate) fn requires_v3_features(
    column_defs: &[sqlparser::ast::ColumnDef],
    iceberg_schema: &IcebergSchema,
) -> bool {
    use sqe_sql::is_v3_only_type;

    // Nanosecond timestamps and other V3-only SQL types trigger V3.
    if column_defs.iter().any(|c| is_v3_only_type(&c.data_type)) {
        return true;
    }

    // A write_default or initial_default means V3 too.
    iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .any(|f| f.write_default.is_some() || f.initial_default.is_some())
}

/// Build the table-properties map that signals the desired Iceberg format
/// version to a REST catalog (Polaris, Tabular, etc).
///
/// The Iceberg REST `CreateTableRequest` has no dedicated `format-version`
/// field; the spec uses the reserved `format-version` table property to
/// communicate the version at create time. iceberg-rust currently does not
/// auto-translate `TableCreation.format_version` into this property, so we
/// set it explicitly.
pub(crate) fn format_version_properties(
    format_version: FormatVersion,
) -> std::collections::HashMap<String, String> {
    let mut props = std::collections::HashMap::new();
    let value = match format_version {
        FormatVersion::V1 => "1",
        FormatVersion::V2 => "2",
        FormatVersion::V3 => "3",
    };
    props.insert("format-version".to_string(), value.to_string());
    props
}

/// Fold a `Vec<SqlOption>` (from sqlparser-rs `TBLPROPERTIES (...)` or
/// `WITH (...)` clauses) into a property HashMap.
///
/// Only `KeyValue` options are materialised. Existing entries in `props`
/// (typically `format-version` set by the SQE auto-upgrade path) are
/// preserved when the user did not explicitly set them; an explicit
/// user-supplied value wins so callers can pin a different format version.
pub(crate) fn merge_user_table_properties(
    props: &mut std::collections::HashMap<String, String>,
    options: &[sqlparser::ast::SqlOption],
) {
    use sqlparser::ast::SqlOption;
    for opt in options {
        if let SqlOption::KeyValue { key, value } = opt {
            let k = key.value.clone();
            let v = sql_expr_to_property_string(value);
            props.insert(k, v);
        }
    }
}

/// Turn a sqlparser `Expr` value used in TBLPROPERTIES / WITH into the
/// raw string Iceberg expects. We trim surrounding string-literal quotes
/// so `'merge-on-read'` becomes `merge-on-read` rather than `'merge-on-read'`.
fn sql_expr_to_property_string(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(v) => match v {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.clone(),
            Value::Boolean(b) => b.to_string(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Translate a sqlparser `PARTITIONED BY (...)` clause into an Iceberg
/// [`UnboundPartitionSpec`]. Supports the six standard transforms:
///
/// | SQL              | Transform              |
/// |------------------|------------------------|
/// | `col`            | `Identity`             |
/// | `year(col)`      | `Year`                 |
/// | `month(col)`     | `Month`                |
/// | `day(col)`       | `Day`                  |
/// | `hour(col)`      | `Hour`                 |
/// | `bucket(N, col)` | `Bucket(N)`            |
/// | `truncate(L, col)` | `Truncate(L)`        |
///
/// The partition field name is auto-derived following the Iceberg
/// convention: `<col>` for identity, `<col>_<transform>` for time
/// transforms, `<col>_bucket_<N>` for bucket, `<col>_trunc_<L>` for
/// truncate. Source column ids come from the table's iceberg schema.
///
/// Returns `Ok(None)` when no `PARTITIONED BY` clause is present so
/// callers can pass the result to `TableCreation::builder().partition_spec_opt()`
/// directly.
pub(crate) fn build_partition_spec(
    partition_by: Option<&sqlparser::ast::Expr>,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Option<iceberg::spec::UnboundPartitionSpec>> {
    use iceberg::spec::{UnboundPartitionField, UnboundPartitionSpec};
    use sqlparser::ast::Expr;

    let Some(expr) = partition_by else {
        return Ok(None);
    };

    // The PARTITIONED BY clause may be a single expression or a tuple
    // of expressions. sqlparser models multi-column partitioning as
    // `Expr::Tuple(Vec<Expr>)`; single-column as the bare expression.
    let exprs: Vec<&Expr> = match expr {
        Expr::Tuple(items) => items.iter().collect(),
        Expr::Nested(inner) => match inner.as_ref() {
            Expr::Tuple(items) => items.iter().collect(),
            other => vec![other],
        },
        other => vec![other],
    };

    // Iceberg V2 expects partition field ids to be unique across all
    // partition specs. By spec, fresh specs start their field ids at
    // 1000 and increment by 1 per field. iceberg-rust's UnboundPartitionSpec
    // serializes `field-id: null` when None, which Polaris's REST
    // endpoint rejects. Assigning the standard 1000+ ids up front keeps
    // the wire format compatible.
    let mut fields = Vec::with_capacity(exprs.len());
    let mut next_field_id: i32 = 1000;
    for partition_expr in exprs {
        let (source_name, target_name, transform) =
            parse_partition_transform(partition_expr)?;
        let source_id = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == source_name)
            .map(|f| f.id)
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "PARTITIONED BY references unknown column '{source_name}'"
                ))
            })?;
        fields.push(UnboundPartitionField {
            source_id,
            field_id: Some(next_field_id),
            name: target_name,
            transform,
        });
        next_field_id += 1;
    }

    let spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(fields)
        .map_err(|e| SqeError::Execution(format!("Invalid PARTITIONED BY clause: {e}")))?
        .build();

    Ok(Some(spec))
}

/// Parse a single partition expression into `(source_column, target_name, Transform)`.
fn parse_partition_transform(
    expr: &sqlparser::ast::Expr,
) -> sqe_core::Result<(String, String, iceberg::spec::Transform)> {
    use iceberg::spec::Transform;
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value};

    match expr {
        // Bare column name = identity transform.
        Expr::Identifier(ident) => Ok((
            ident.value.clone(),
            ident.value.clone(),
            Transform::Identity,
        )),
        // Compound identifier (e.g. `t.col`) — take the last segment.
        Expr::CompoundIdentifier(parts) => {
            let name = parts
                .last()
                .map(|p| p.value.clone())
                .ok_or_else(|| {
                    SqeError::Execution(
                        "PARTITIONED BY: empty compound identifier".to_string(),
                    )
                })?;
            Ok((name.clone(), name, Transform::Identity))
        }
        // Function call: year(col), bucket(N, col), truncate(L, col), etc.
        Expr::Function(func) => {
            let fn_name = func
                .name
                .0
                .last()
                .map(|id| id.value.to_ascii_lowercase())
                .ok_or_else(|| {
                    SqeError::Execution(
                        "PARTITIONED BY: function call without a name".to_string(),
                    )
                })?;
            let args = match &func.args {
                FunctionArguments::List(list) => &list.args,
                _ => {
                    return Err(SqeError::Execution(format!(
                        "PARTITIONED BY: unsupported argument form for {fn_name}()"
                    )));
                }
            };

            // Extract the bare-expr arguments out of FunctionArg.
            let bare_args: Vec<&Expr> = args
                .iter()
                .filter_map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect();

            // Helper closures.
            let column_name = |arg: &Expr| -> sqe_core::Result<String> {
                match arg {
                    Expr::Identifier(id) => Ok(id.value.clone()),
                    Expr::CompoundIdentifier(parts) => parts
                        .last()
                        .map(|p| p.value.clone())
                        .ok_or_else(|| {
                            SqeError::Execution(format!(
                                "PARTITIONED BY {fn_name}(): empty compound identifier"
                            ))
                        }),
                    other => Err(SqeError::Execution(format!(
                        "PARTITIONED BY {fn_name}(): expected column name, got {other}"
                    ))),
                }
            };
            let int_arg = |arg: &Expr| -> sqe_core::Result<u32> {
                match arg {
                    Expr::Value(v) => match v {
                        Value::Number(n, _) => n.parse::<u32>().map_err(|e| {
                            SqeError::Execution(format!(
                                "PARTITIONED BY {fn_name}(): integer parameter '{n}': {e}"
                            ))
                        }),
                        other => Err(SqeError::Execution(format!(
                            "PARTITIONED BY {fn_name}(): expected integer, got {other}"
                        ))),
                    },
                    other => Err(SqeError::Execution(format!(
                        "PARTITIONED BY {fn_name}(): expected integer literal, got {other}"
                    ))),
                }
            };

            match fn_name.as_str() {
                "year" | "years" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY year(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_year"), Transform::Year))
                }
                "month" | "months" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY month(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_month"), Transform::Month))
                }
                "day" | "days" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY day(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_day"), Transform::Day))
                }
                "hour" | "hours" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY hour(): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_hour"), Transform::Hour))
                }
                "bucket" => {
                    if bare_args.len() != 2 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY bucket(N, col): expected exactly two arguments".into(),
                        ));
                    }
                    let n = int_arg(bare_args[0])?;
                    let col = column_name(bare_args[1])?;
                    Ok((
                        col.clone(),
                        format!("{col}_bucket_{n}"),
                        Transform::Bucket(n),
                    ))
                }
                "truncate" => {
                    if bare_args.len() != 2 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY truncate(L, col): expected exactly two arguments".into(),
                        ));
                    }
                    let l = int_arg(bare_args[0])?;
                    let col = column_name(bare_args[1])?;
                    Ok((
                        col.clone(),
                        format!("{col}_trunc_{l}"),
                        Transform::Truncate(l),
                    ))
                }
                "void" => {
                    if bare_args.len() != 1 {
                        return Err(SqeError::Execution(
                            "PARTITIONED BY void(col): expected exactly one column".into(),
                        ));
                    }
                    let col = column_name(bare_args[0])?;
                    Ok((col.clone(), format!("{col}_null"), Transform::Void))
                }
                other => Err(SqeError::Execution(format!(
                    "PARTITIONED BY: unsupported transform '{other}'. \
                     Supported: identity (bare column), year, month, day, hour, \
                     bucket(N, col), truncate(L, col), void."
                ))),
            }
        }
        other => Err(SqeError::Execution(format!(
            "PARTITIONED BY: unsupported expression form: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit};
    use sqlparser::ast::{DataType as SqlType, ExactNumberInfo, TimezoneInfo};

    // -------------------------------------------------------------------------
    // build_partition_spec JSON shape (for catalog interop debugging)
    // -------------------------------------------------------------------------

    #[test]
    fn build_partition_spec_emits_polaris_compatible_json() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use std::sync::Arc;

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let dialect = sqlparser::dialect::GenericDialect {};
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql("region")
            .unwrap();
        let expr = parser.parse_expr().unwrap();
        let spec = build_partition_spec(Some(&expr), &schema).unwrap().unwrap();
        let json = serde_json::to_string_pretty(&spec).unwrap();
        eprintln!("PARTITION SPEC JSON:\n{json}");
        // The spec must serialize with kebab-case keys that Polaris recognises.
        assert!(json.contains("\"source-id\": 2"), "json: {json}");
        assert!(json.contains("\"transform\": \"identity\""), "json: {json}");
        assert!(json.contains("\"name\": \"region\""), "json: {json}");
    }

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

        assert!(
            fields[0].required,
            "non-nullable Arrow field should map to required Iceberg field"
        );
        assert!(
            !fields[1].required,
            "nullable Arrow field should map to optional Iceberg field"
        );
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
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 4),
            false,
        )]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg_schema.as_struct().fields().to_vec();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "amount");
        assert!(fields[0].required);
    }

    #[test]
    fn test_arrow_schema_to_iceberg_binary_type() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("payload", DataType::Binary, true)]);

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
        assert_eq!(sql_type_to_arrow(&SqlType::Int16).unwrap(), DataType::Int16);
        assert_eq!(
            sql_type_to_arrow(&SqlType::Int(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            sql_type_to_arrow(&SqlType::Integer(None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Int32).unwrap(), DataType::Int32);
        assert_eq!(
            sql_type_to_arrow(&SqlType::BigInt(None)).unwrap(),
            DataType::Int64
        );
        assert_eq!(sql_type_to_arrow(&SqlType::Int64).unwrap(), DataType::Int64);
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
        assert_eq!(sql_type_to_arrow(&SqlType::Text).unwrap(), DataType::Utf8);
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
        assert_eq!(sql_type_to_arrow(&SqlType::Date).unwrap(), DataType::Date32);
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
        let result = sql_type_to_arrow(&SqlType::Timestamp(Some(9), TimezoneInfo::None)).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_timestamp_ns_custom_type() {
        // TIMESTAMP_NS(9) lands in sqlparser as DataType::Custom.
        use sqlparser::ast::ObjectName;

        let custom = SqlType::Custom(
            ObjectName(vec![sqlparser::ast::Ident::new("TIMESTAMP_NS")]),
            vec!["9".to_string()],
        );
        let result = sql_type_to_arrow(&custom).unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Nanosecond, None));

        // Lowercase should map to the same type (identifiers are case-insensitive).
        let custom_lower = SqlType::Custom(
            ObjectName(vec![sqlparser::ast::Ident::new("timestamp_ns")]),
            vec!["9".to_string()],
        );
        let result_lower = sql_type_to_arrow(&custom_lower).unwrap();
        assert_eq!(result_lower, DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_sql_type_to_arrow_timestamptz_ns_custom_type() {
        use sqlparser::ast::ObjectName;

        let custom = SqlType::Custom(
            ObjectName(vec![sqlparser::ast::Ident::new("TIMESTAMPTZ_NS")]),
            vec!["9".to_string()],
        );
        let result = sql_type_to_arrow(&custom).unwrap();
        assert_eq!(
            result,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_sql_type_to_arrow_ns_via_parser() {
        // Parse CREATE TABLE and feed the resulting column type through the
        // conversion. Locks behaviour across the full parser -> mapper path.
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "CREATE TABLE events (ts TIMESTAMP_NS(9), utcts TIMESTAMPTZ_NS(9))";
        let stmt = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(
            sql_type_to_arrow(&ct.columns[0].data_type).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            sql_type_to_arrow(&ct.columns[1].data_type).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_arrow_to_iceberg_preserves_nanosec() {
        // Nanosecond Arrow timestamps must map to Iceberg TimestampNs / TimestamptzNs.
        use arrow_schema::Field;
        use iceberg::spec::{PrimitiveType, Type};

        let arrow_schema = ArrowSchema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "utcts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]);
        let iceberg = arrow_schema_to_iceberg(&arrow_schema).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();
        assert!(matches!(
            *fields[0].field_type,
            Type::Primitive(PrimitiveType::TimestampNs)
        ));
        assert!(matches!(
            *fields[1].field_type,
            Type::Primitive(PrimitiveType::TimestamptzNs)
        ));
    }

    // ------------------------------------------------------------------
    // DEFAULT literal handling and format-version gating
    // ------------------------------------------------------------------

    fn parse_create_table(sql: &str) -> sqlparser::ast::CreateTable {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .expect("sql parses")
            .into_iter()
            .next()
            .expect("one statement");
        match stmt {
            sqlparser::ast::Statement::CreateTable(ct) => ct,
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn test_default_string_sets_write_default() {
        use iceberg::spec::{Literal, PrimitiveLiteral};

        let ct = parse_create_table(
            "CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')",
        );
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();

        let status = fields.iter().find(|f| f.name == "status").unwrap();
        assert!(
            matches!(
                status.write_default.as_ref(),
                Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
            ),
            "write_default should be 'pending', got {:?}",
            status.write_default
        );
        assert!(
            matches!(
                status.initial_default.as_ref(),
                Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
            ),
            "initial_default should be 'pending', got {:?}",
            status.initial_default
        );
    }

    #[test]
    fn test_default_integer_on_bigint() {
        use iceberg::spec::{Literal, PrimitiveLiteral};

        let ct = parse_create_table("CREATE TABLE t (n BIGINT DEFAULT 42)");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let iceberg = arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap();
        let fields: Vec<_> = iceberg.as_struct().fields().to_vec();

        assert!(matches!(
            fields[0].write_default.as_ref(),
            Some(Literal::Primitive(PrimitiveLiteral::Long(42)))
        ));
    }

    #[test]
    fn test_default_function_rejected_with_clear_error() {
        let ct = parse_create_table(
            "CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())",
        );
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let err =
            arrow_schema_to_iceberg_with_defaults(&arrow_schema, &ct.columns).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("current_timestamp"),
            "error should name rejected function: {msg}"
        );
        assert!(
            msg.contains("Accepted forms"),
            "error should list accepted forms: {msg}"
        );
    }

    #[test]
    fn test_requires_v3_on_nanosec_timestamp() {
        let ct = parse_create_table("CREATE TABLE t (ts TIMESTAMP_NS(9))");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg = arrow_schema_to_iceberg_with_defaults(
            &ArrowSchema::new(arrow_fields),
            &ct.columns,
        )
        .unwrap();
        assert!(requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_requires_v3_on_write_default() {
        let ct = parse_create_table(
            "CREATE TABLE t (id BIGINT, status STRING DEFAULT 'pending')",
        );
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg = arrow_schema_to_iceberg_with_defaults(
            &ArrowSchema::new(arrow_fields),
            &ct.columns,
        )
        .unwrap();
        assert!(requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_does_not_require_v3_when_v2_only() {
        let ct = parse_create_table("CREATE TABLE t (id BIGINT, name STRING)");
        let arrow_fields: Vec<_> = ct
            .columns
            .iter()
            .map(|c| {
                let ty = sql_type_to_arrow(&c.data_type).unwrap();
                Field::new(c.name.value.clone(), ty, true)
            })
            .collect();
        let iceberg = arrow_schema_to_iceberg_with_defaults(
            &ArrowSchema::new(arrow_fields),
            &ct.columns,
        )
        .unwrap();
        assert!(!requires_v3_features(&ct.columns, &iceberg));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_full() {
        let result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(18, 4)))
                .unwrap();
        assert_eq!(result, DataType::Decimal128(18, 4));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_precision_only() {
        let result = sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::Precision(10))).unwrap();
        // Scale defaults to 0
        assert_eq!(result, DataType::Decimal128(10, 0));
    }

    #[test]
    fn test_sql_type_to_arrow_decimal_no_info() {
        let result = sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::None)).unwrap();
        // Defaults to Decimal128(38, 10)
        assert_eq!(result, DataType::Decimal128(38, 10));
    }

    #[test]
    fn test_sql_type_to_arrow_numeric_alias() {
        // NUMERIC is the same as DECIMAL in the implementation.
        let decimal_result =
            sql_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(12, 2)))
                .unwrap();
        let numeric_result =
            sql_type_to_arrow(&SqlType::Numeric(ExactNumberInfo::PrecisionAndScale(12, 2)))
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

    // ---------------------------------------------------------------------
    // Correlated ScalarSubquery decorrelator tests
    // ---------------------------------------------------------------------

    fn parse_update(sql: &str) -> Vec<sqlparser::ast::Assignment> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse UPDATE");
        match stmts.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Update { assignments, .. } => assignments,
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn decorrelator_rewrites_simple_correlated_subquery() {
        let sql = "\
UPDATE holding_summary \
SET hs_qty = hs_qty + ( \
    SELECT t.t_qty FROM trade t \
    WHERE t.t_ca_id = holding_summary.hs_ca_id \
      AND t.t_st_id = 'PNDG' \
    LIMIT 1 \
)";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "holding_summary");
        assert_eq!(rewritten.len(), 1);
        assert_eq!(rewritten[0].col_name, "hs_qty");
        // The scalar subquery must have been replaced with a column reference
        // into the joined lookup.
        assert!(
            rewritten[0].expr_sql.contains("\"__corr0\".\"__val\""),
            "unexpected rewritten expr: {}",
            rewritten[0].expr_sql
        );
        assert_eq!(joins.len(), 1);
        assert!(joins[0].contains("LEFT JOIN"));
        assert!(joins[0].contains("GROUP BY t.t_ca_id"));
        assert!(joins[0].contains("MAX(t.t_qty)"));
        assert!(joins[0].contains("t.t_st_id = 'PNDG'"));
        assert!(joins[0].contains("\"__corr0\".__k0 = \"holding_summary\".hs_ca_id"));
    }

    #[test]
    fn decorrelator_handles_two_correlation_keys() {
        let sql = "\
UPDATE holding_summary \
SET hs_qty = ( \
    SELECT t.t_qty FROM trade t \
    WHERE t.t_ca_id = holding_summary.hs_ca_id \
      AND t.t_s_symb = holding_summary.hs_s_symb \
)";
        let assignments = parse_update(sql);
        let (_, joins) = decorrelate_scalar_subqueries(&assignments, "holding_summary");
        assert_eq!(joins.len(), 1);
        assert!(joins[0].contains("GROUP BY t.t_ca_id, t.t_s_symb"));
        assert!(joins[0].contains("__k0 = \"holding_summary\".hs_ca_id"));
        assert!(joins[0].contains("__k1 = \"holding_summary\".hs_s_symb"));
    }

    #[test]
    fn decorrelator_skips_when_no_correlation() {
        // Subquery with no reference to the UPDATE target — leave as-is.
        let sql = "\
UPDATE customer \
SET c_balance = ( \
    SELECT MAX(trade_price) FROM trade WHERE t_st_id = 'PNDG' \
)";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "customer");
        assert!(joins.is_empty(), "should not emit joins: {:?}", joins);
        // Expression should still contain the subquery unchanged.
        assert!(
            rewritten[0].expr_sql.contains("SELECT"),
            "subquery should remain: {}",
            rewritten[0].expr_sql
        );
    }

    #[test]
    fn decorrelator_skips_when_no_subquery() {
        let sql = "UPDATE district SET d_ytd = d_ytd + 2500.00 WHERE d_id = 1";
        let assignments = parse_update(sql);
        let (rewritten, joins) = decorrelate_scalar_subqueries(&assignments, "district");
        assert!(joins.is_empty());
        assert_eq!(rewritten[0].expr_sql, "d_ytd + 2500.00");
    }

    #[test]
    fn decorrelator_skips_non_equality_correlation() {
        // Correlation via `>` — we only decorrelate equality shapes.
        let sql = "\
UPDATE t \
SET c = ( \
    SELECT x FROM s WHERE s.k > t.k \
)";
        let assignments = parse_update(sql);
        let (_, joins) = decorrelate_scalar_subqueries(&assignments, "t");
        assert!(joins.is_empty(), "non-eq correlation should be left alone");
    }
}

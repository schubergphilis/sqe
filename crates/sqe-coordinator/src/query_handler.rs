use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use tracing::{debug, info};

use sqlparser::ast::Statement;
use sqe_catalog::{SessionCatalog, SqeCatalogProvider};
use sqe_core::{Session, SqeConfig, SqeError};
use sqe_policy::PolicyEnforcer;
use sqe_sql::{parse_and_classify, StatementKind};

use crate::catalog_ops::CatalogOps;
use crate::write_handler::WriteHandler;

/// Handles query execution by routing parsed SQL through the appropriate
/// pipeline: DataFusion for queries, catalog metadata for SHOW commands,
/// and policy enforcement for all plans.
pub struct QueryHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    config: SqeConfig,
    catalog_ops: CatalogOps,
    write_handler: WriteHandler,
    #[allow(dead_code)] // Wired in Chunk 3 for distributed execution; query routing TBD
    worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
}

impl QueryHandler {
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    ) -> Self {
        let catalog_ops = CatalogOps::new(config.clone());
        let write_handler = WriteHandler::new(config.clone());
        Self {
            policy_enforcer,
            config,
            catalog_ops,
            write_handler,
            worker_registry,
            metrics,
            audit,
        }
    }

    /// Check if distributed execution should be used for a query.
    #[allow(dead_code)] // Will be used when distributed query routing is wired in
    async fn should_distribute(&self) -> bool {
        if let Some(ref registry) = self.worker_registry {
            !registry.healthy_workers().await.is_empty()
        } else {
            false
        }
    }

    /// Execute a SQL statement for the given session and return collected RecordBatches.
    #[tracing::instrument(skip(self, session, sql), fields(username = %session.user.username, statement_type))]
    pub async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        info!(
            username = %session.user.username,
            sql_length = sql.len(),
            "Executing query"
        );

        let start = std::time::Instant::now();
        let kind = parse_and_classify(sql)?;
        let kind_name = kind.name().to_string();
        tracing::Span::current().record("statement_type", &kind_name.as_str());

        let result = match kind {
            StatementKind::Query(_) => self.execute_query(session, sql).await,

            StatementKind::ShowCatalogs => self.handle_show_catalogs(session).await,

            StatementKind::ShowSchemas(_filter) => {
                self.handle_show_schemas(session).await
            }

            StatementKind::ShowTables(filter) => {
                self.handle_show_tables(session, &filter).await
            }

            StatementKind::Utility(stmt) => {
                if let sqlparser::ast::Statement::Explain { statement, .. } = *stmt {
                    self.handle_explain(session, &statement.to_string()).await
                } else {
                    Err(SqeError::NotImplemented(format!(
                        "Utility statement not supported: {stmt}"
                    )))
                }
            }

            StatementKind::Policy(_) => Err(SqeError::NotImplemented(
                "Policy management not configured".to_string(),
            )),

            StatementKind::Drop(stmt) => {
                self.catalog_ops.drop_table(session, &stmt).await?;
                Ok(vec![])
            }
            StatementKind::Rename(stmt) => {
                self.catalog_ops.rename_table(session, &stmt).await?;
                Ok(vec![])
            }
            StatementKind::CreateView(stmt) => {
                self.handle_create_view(session, &stmt).await?;
                Ok(vec![])
            }
            StatementKind::DropView(stmt) => {
                self.catalog_ops.drop_view(session, &stmt).await?;
                Ok(vec![])
            }

            StatementKind::Ctas(stmt) => {
                if let Statement::CreateTable(ref ct) = *stmt {
                    if let Some(ref query) = ct.query {
                        let select_sql = format!("{query}");
                        let batches = self.execute_query(session, &select_sql).await?;
                        self.write_handler.handle_ctas(session, &stmt, batches).await
                    } else {
                        Err(SqeError::Execution("CTAS without SELECT query".into()))
                    }
                } else {
                    Err(SqeError::Execution("Expected CreateTable statement".into()))
                }
            }

            StatementKind::Insert(stmt) => {
                if let Statement::Insert(ref ins) = *stmt {
                    let select_sql = ins
                        .source
                        .as_ref()
                        .map(|q| format!("{q}"))
                        .ok_or_else(|| {
                            SqeError::Execution("INSERT without SELECT source".into())
                        })?;
                    let batches = self.execute_query(session, &select_sql).await?;
                    self.write_handler
                        .handle_insert(session, &stmt, batches)
                        .await
                } else {
                    Err(SqeError::Execution("Expected Insert statement".into()))
                }
            }

            StatementKind::Delete(_) => Err(SqeError::NotImplemented(
                "DELETE FROM requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
            )),
            StatementKind::Merge(_) => Err(SqeError::NotImplemented(
                "MERGE INTO requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
            )),
        };

        // Record metrics and audit
        let duration = start.elapsed();
        let status = if result.is_ok() { "success" } else { "error" };
        let rows: usize = result
            .as_ref()
            .map(|b| b.iter().map(|r| r.num_rows()).sum())
            .unwrap_or(0);

        if let Some(ref metrics) = self.metrics {
            metrics
                .query_count
                .with_label_values(&[status, &kind_name])
                .inc();
            metrics
                .query_duration
                .with_label_values(&[&kind_name])
                .observe(duration.as_secs_f64());
            metrics.rows_returned.inc_by(rows as f64);
        }

        if let Some(ref audit) = self.audit {
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: session.user.username.clone(),
                query_text: sql.to_string(),
                statement_type: kind_name,
                duration_ms: duration.as_millis() as u64,
                rows_returned: rows,
                status: status.to_string(),
            });
        }

        result
    }

    /// Plan a SQL query and return only its schema, without executing it.
    pub async fn get_schema(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<SchemaRef> {
        let ctx = self.create_session_context(session).await?;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        Ok(Arc::new(df.schema().as_arrow().clone()))
    }

    /// Execute a SELECT query through DataFusion with the user's catalog.
    #[tracing::instrument(skip(self, session, sql), fields(username = %session.user.username))]
    async fn execute_query(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let ctx = self.create_session_context(session).await?;

        // Plan the query via DataFusion's SQL planner
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        // Get the logical plan and run policy enforcement
        let plan = df.logical_plan().clone();
        let enforced_plan = self
            .policy_enforcer
            .evaluate(&session.user, plan)
            .await?;

        debug!("Policy-enforced plan: {:?}", enforced_plan);

        // Create a new DataFrame from the enforced plan and execute
        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

        let batches = enforced_df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

        info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Query execution complete"
        );

        Ok(batches)
    }

    /// Create a DataFusion SessionContext with the user's Polaris catalog registered.
    #[tracing::instrument(skip(self, session), fields(username = %session.user.username))]
    async fn create_session_context(
        &self,
        session: &Session,
    ) -> sqe_core::Result<SessionContext> {
        let ctx = SessionContext::new();

        // Create a per-session catalog connected to Polaris with the user's bearer token
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
            )
            .await?,
        );

        // Create the DataFusion CatalogProvider from the session catalog
        let catalog_provider = SqeCatalogProvider::try_new(
            session_catalog,
            self.config.storage.clone(),
            self.config.catalog.warehouse.clone(),
        )
        .await?;

        // Register the catalog with the warehouse name
        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));

        debug!(
            catalog_name = %catalog_name,
            username = %session.user.username,
            "Registered session catalog in DataFusion context"
        );

        Ok(ctx)
    }

    /// Handle SHOW CATALOGS by returning the configured warehouse name.
    async fn handle_show_catalogs(
        &self,
        _session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default"
        } else {
            &self.config.catalog.warehouse
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        builder.append_value(catalog_name);
        let array: ArrayRef = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array])
            .map_err(|e| SqeError::Execution(format!("Failed to create result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Handle SHOW SCHEMAS by listing namespaces from the Polaris catalog.
    async fn handle_show_schemas(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let session_catalog = SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
        )
        .await?;

        let namespaces = session_catalog.list_namespaces().await?;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "schema_name",
            DataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        for ns in &namespaces {
            // Namespace is a list of parts, join with "."
            let name: Vec<&str> = ns.as_ref().iter().map(|s| s.as_str()).collect();
            builder.append_value(name.join("."));
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array])
            .map_err(|e| SqeError::Execution(format!("Failed to create result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Handle SHOW TABLES by listing tables in a namespace from the Polaris catalog.
    async fn handle_show_tables(
        &self,
        session: &Session,
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let session_catalog = SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
        )
        .await?;

        // If a filter is provided, use it as the namespace; otherwise list all namespaces
        let namespaces = if filter.is_empty() {
            session_catalog.list_namespaces().await?
        } else {
            // Parse the filter — strip any "IN" prefix that sqlparser may add
            let ns_name = filter
                .trim()
                .trim_start_matches("IN")
                .trim()
                .to_string();
            if ns_name.is_empty() {
                session_catalog.list_namespaces().await?
            } else {
                vec![iceberg::NamespaceIdent::new(ns_name)]
            }
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
        ]));

        let mut ns_builder = StringBuilder::new();
        let mut table_builder = StringBuilder::new();

        for ns in &namespaces {
            match session_catalog.list_tables(ns).await {
                Ok(tables) => {
                    let ns_name: Vec<&str> =
                        ns.as_ref().iter().map(|s| s.as_str()).collect();
                    let ns_str = ns_name.join(".");
                    for table in &tables {
                        ns_builder.append_value(&ns_str);
                        table_builder.append_value(table.name());
                    }
                }
                Err(e) => {
                    debug!(
                        namespace = ?ns,
                        error = %e,
                        "Failed to list tables in namespace, skipping"
                    );
                }
            }
        }

        let ns_array: ArrayRef = Arc::new(ns_builder.finish());
        let table_array: ArrayRef = Arc::new(table_builder.finish());

        let batch = RecordBatch::try_new(schema, vec![ns_array, table_array])
            .map_err(|e| SqeError::Execution(format!("Failed to create result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Handle CREATE VIEW by inferring the output schema from the SELECT query
    /// and then calling the Polaris REST API to create the view.
    async fn handle_create_view(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let query = match stmt {
            Statement::CreateView { query, .. } => query,
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CREATE VIEW statement, got: {other}"
                )));
            }
        };

        // Infer the output schema by planning the SELECT query through DataFusion
        let select_sql = format!("{query}");
        let schema = self.get_schema(session, &select_sql).await?;

        // Convert Arrow schema to Iceberg REST API schema format
        let schema_json = arrow_schema_to_iceberg_json(&schema);

        self.catalog_ops
            .create_view(session, stmt, &schema_json)
            .await
    }

    /// Handle EXPLAIN by planning the inner statement and returning the plan as text.
    async fn handle_explain(
        &self,
        session: &Session,
        inner_sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let ctx = self.create_session_context(session).await?;

        let explain_sql = format!("EXPLAIN {inner_sql}");
        let df = ctx
            .sql(&explain_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN planning failed: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN execution failed: {e}")))?;

        Ok(batches)
    }
}

/// Convert an Arrow `SchemaRef` to Iceberg REST API schema JSON format.
///
/// Produces a JSON object like:
/// ```json
/// {
///   "type": "struct",
///   "schema-id": 0,
///   "fields": [
///     { "id": 1, "name": "col", "required": false, "type": "string" },
///     ...
///   ]
/// }
/// ```
fn arrow_schema_to_iceberg_json(schema: &SchemaRef) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            serde_json::json!({
                "id": i + 1,
                "name": field.name(),
                "required": !field.is_nullable(),
                "type": arrow_type_to_iceberg(field.data_type()),
            })
        })
        .collect();

    serde_json::json!({
        "type": "struct",
        "schema-id": 0,
        "fields": fields,
    })
}

/// Map an Arrow `DataType` to an Iceberg type string.
fn arrow_type_to_iceberg(dt: &DataType) -> serde_json::Value {
    match dt {
        DataType::Boolean => serde_json::json!("boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => serde_json::json!("int"),
        DataType::Int64 => serde_json::json!("long"),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => serde_json::json!("int"),
        DataType::UInt64 => serde_json::json!("long"),
        DataType::Float16 | DataType::Float32 => serde_json::json!("float"),
        DataType::Float64 => serde_json::json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => serde_json::json!("string"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            serde_json::json!("binary")
        }
        DataType::Date32 | DataType::Date64 => serde_json::json!("date"),
        DataType::Timestamp(_, _) => serde_json::json!("timestamptz"),
        DataType::Time32(_) | DataType::Time64(_) => serde_json::json!("time"),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            serde_json::json!(format!("decimal({p},{s})"))
        }
        DataType::FixedSizeBinary(len) => serde_json::json!(format!("fixed[{len}]")),
        _ => serde_json::json!("string"), // fallback
    }
}

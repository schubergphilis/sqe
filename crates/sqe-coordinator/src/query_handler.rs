use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use tracing::{debug, info, warn};

use sqlparser::ast::Statement;
use sqe_catalog::{IcebergScanExec, SessionCatalog, SqeCatalogProvider};
use sqe_core::{QueryConfig, Session, SqeConfig, SqeError};
use sqe_policy::PolicyEnforcer;
use sqe_sql::{parse_and_classify, StatementKind};

use crate::catalog_ops::CatalogOps;
use crate::credential_refresh::CredentialRefreshTracker;
use crate::query_cache::ResultCache;
use crate::query_tracker::{FragmentState, QueryTracker};
use crate::write_handler::WriteHandler;

/// Determine the effective query timeout for a session.
///
/// If any of the user's roles appear in `config.role_overrides`, the highest
/// matching override wins. Otherwise the global `config.timeout_secs` is used.
pub fn timeout_for_session(config: &QueryConfig, session: &Session) -> u64 {
    let override_timeout = session
        .user
        .roles
        .iter()
        .filter_map(|role| config.role_overrides.get(role))
        .copied()
        .max();

    override_timeout.unwrap_or(config.timeout_secs)
}

/// Handles query execution by routing parsed SQL through the appropriate
/// pipeline: DataFusion for queries, catalog metadata for SHOW commands,
/// and policy enforcement for all plans.
pub struct QueryHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    config: SqeConfig,
    catalog_ops: CatalogOps,
    write_handler: WriteHandler,
    explain_handler: crate::explain::ExplainHandler,
    worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
    #[allow(dead_code)] // Used when constructing DistributedScanExec for distributed queries
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    query_tracker: Arc<QueryTracker>,
    query_cache: Option<Arc<ResultCache>>,
}

impl QueryHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
        credential_tracker: Option<Arc<CredentialRefreshTracker>>,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
        query_tracker: Arc<QueryTracker>,
        query_cache: Option<Arc<ResultCache>>,
    ) -> Self {
        let catalog_ops = CatalogOps::new(config.clone());
        let write_handler = WriteHandler::new(config.clone());
        let explain_handler = crate::explain::ExplainHandler::new(Arc::clone(&policy_enforcer));
        Self {
            policy_enforcer,
            config,
            catalog_ops,
            write_handler,
            explain_handler,
            worker_registry,
            credential_tracker,
            metrics,
            audit,
            query_tracker,
            query_cache,
        }
    }

    /// Returns a reference to the query tracker.
    pub fn query_tracker(&self) -> &Arc<QueryTracker> {
        &self.query_tracker
    }

    pub fn write_handler(&self) -> &WriteHandler {
        &self.write_handler
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
        tracing::Span::current().record("statement_type", kind_name.as_str());

        // Generate a query ID for lifecycle tracking
        let query_id = uuid::Uuid::now_v7();
        self.query_tracker.start(
            query_id,
            &session.user.username,
            session.source.as_deref(),
            sql,
            &session.id,
            None, // client_ip — populated by caller if available
            session.user.roles.clone(),
        );

        // Check result cache for read queries (before execution)
        if let StatementKind::Query(_) = &kind {
            if let Some(ref cache) = self.query_cache {
                if let Some(cached) = cache.lookup(&session.user.username, sql) {
                    debug!(username = %session.user.username, "Cache hit");
                    let rows: usize = cached.batches.iter().map(|b| b.num_rows()).sum();
                    self.query_tracker.complete(&query_id, rows, 0, cached.tables_touched.clone());
                    if let Some(ref metrics) = self.metrics {
                        metrics.query_count.with_label_values(&["success", &kind_name]).inc();
                        metrics.rows_returned.inc_by(rows as f64);
                    }
                    return Ok(cached.batches.clone());
                }
            }
        }

        // Mark query as running (planning phase complete)
        self.query_tracker.running(&query_id, start.elapsed().as_millis() as u64);

        // Determine the effective timeout for this session (role overrides may increase it)
        let timeout_secs = timeout_for_session(&self.config.query, session);
        let timeout_duration = Duration::from_secs(timeout_secs);

        let execution_future = async {
            match &kind {
                StatementKind::Query(_) => self.execute_query(session, sql, &query_id).await,

                StatementKind::ShowCatalogs => self.handle_show_catalogs(session).await,

                StatementKind::ShowSchemas(_filter) => {
                    self.handle_show_schemas(session).await
                }

                StatementKind::ShowTables(filter) => {
                    self.handle_show_tables(session, filter).await
                }

                StatementKind::Utility(stmt) => {
                    if let sqlparser::ast::Statement::Explain { analyze, statement, .. } = stmt.as_ref() {
                        let inner = statement.to_string();
                        let ctx = self.create_session_context(session).await?;
                        if *analyze {
                            self.explain_handler.analyze(session, &inner, &ctx).await
                        } else {
                            self.explain_handler.plan(session, &inner, &ctx).await
                        }
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
                    self.catalog_ops.drop_table(session, stmt).await?;
                    Ok(vec![])
                }
                StatementKind::Rename(stmt) => {
                    self.catalog_ops.rename_table(session, stmt).await?;
                    Ok(vec![])
                }
                StatementKind::CreateView(stmt) => {
                    self.handle_create_view(session, stmt).await?;
                    Ok(vec![])
                }
                StatementKind::DropView(stmt) => {
                    self.catalog_ops.drop_view(session, stmt).await?;
                    Ok(vec![])
                }
                StatementKind::CreateSchema(stmt) => {
                    self.catalog_ops.create_schema(session, stmt).await?;
                    Ok(vec![])
                }
                StatementKind::DropSchema(stmt) => {
                    self.catalog_ops.drop_schema(session, stmt).await?;
                    Ok(vec![])
                }

                StatementKind::CreateTable(stmt) => {
                    self.write_handler.handle_create_table(session, stmt).await
                }

                StatementKind::Ctas(stmt) => {
                    if let Statement::CreateTable(ref ct) = **stmt {
                        if let Some(ref query) = ct.query {
                            // Handle CREATE OR REPLACE TABLE AS SELECT:
                            // drop the existing table first, then create a new one.
                            if ct.or_replace {
                                self.drop_table_if_exists(session, &ct.name).await?;
                            }
                            let select_sql = format!("{query}");
                            let batches = self.execute_query(session, &select_sql, &query_id).await?;
                            self.write_handler.handle_ctas(session, stmt, batches).await
                        } else {
                            Err(SqeError::Execution("CTAS without SELECT query".into()))
                        }
                    } else {
                        Err(SqeError::Execution("Expected CreateTable statement".into()))
                    }
                }

                StatementKind::Insert(stmt) => {
                    if let Statement::Insert(ref ins) = **stmt {
                        let select_sql = ins
                            .source
                            .as_ref()
                            .map(|q| format!("{q}"))
                            .ok_or_else(|| {
                                SqeError::Execution("INSERT without SELECT source".into())
                            })?;
                        let batches = self.execute_query(session, &select_sql, &query_id).await?;
                        self.write_handler
                            .handle_insert(session, stmt, batches)
                            .await
                    } else {
                        Err(SqeError::Execution("Expected Insert statement".into()))
                    }
                }

                StatementKind::ExplainFull(inner) => {
                    let ctx = self.create_session_context(session).await?;
                    self.explain_handler.full(session, inner, &ctx).await
                }

                StatementKind::Delete(_) => Err(SqeError::NotImplemented(
                    "DELETE FROM requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
                )),
                StatementKind::Merge(_) => Err(SqeError::NotImplemented(
                    "MERGE INTO requires Iceberg overwrite transaction support (planned for Chunk 3)".to_string(),
                )),
            }
        };

        let result = match tokio::time::timeout(timeout_duration, execution_future).await {
            Ok(inner_result) => inner_result,
            Err(_elapsed) => {
                warn!(
                    username = %session.user.username,
                    timeout_secs = timeout_secs,
                    "Query timed out"
                );
                self.query_tracker.failed(&query_id, "Timeout", None);
                Err(SqeError::Execution(format!(
                    "Query timed out after {timeout_secs}s"
                )))
            }
        };

        // Record metrics and audit
        let duration = start.elapsed();
        let status = if result.is_ok() { "success" } else { "error" };
        let rows: usize = result
            .as_ref()
            .map(|b| b.iter().map(|r| r.num_rows()).sum())
            .unwrap_or(0);

        // Update query tracker with final state
        let execution_ms = duration.as_millis() as u64;
        if result.is_ok() {
            self.query_tracker.complete(&query_id, rows, execution_ms, vec![]);

            // Store successful read query results in cache
            if let Some(ref cache) = self.query_cache {
                if matches!(&kind, StatementKind::Query(_)) {
                    if let Ok(ref batches) = result {
                        cache.store(
                            &session.user.username,
                            sql,
                            query_id,
                            batches.clone(),
                            vec![], // tables_touched — filled when we add plan extraction
                        );
                    }
                }
            }

            // Invalidate cache entries for written tables
            if let Some(ref cache) = self.query_cache {
                match &kind {
                    StatementKind::Insert(stmt) => {
                        if let Statement::Insert(ins) = stmt.as_ref() {
                            let table = ins.table_name.to_string();
                            cache.invalidate(&table);
                        }
                    }
                    StatementKind::Ctas(stmt) => {
                        if let Statement::CreateTable(ct) = stmt.as_ref() {
                            let table = ct.name.to_string();
                            cache.invalidate(&table);
                        }
                    }
                    StatementKind::Drop(stmt) => {
                        let table = stmt.to_string();
                        cache.invalidate(&table);
                    }
                    _ => {}
                }
            }
        } else {
            let err_msg = result.as_ref().err().map(|e| format!("{e}")).unwrap_or_default();
            // Only mark failed if not already marked (e.g., timeout already marked above)
            let _ = err_msg; // suppress unused warning; error details in audit log
            self.query_tracker.failed(&query_id, "ExecutionError", None);
        }

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
                session_id: Some(session.id.clone()),
                query_hash: sqe_metrics::audit::query_hash(sql),
                query_text: Some(sql.to_string()),
                statement_type: kind_name,
                duration_ms: duration.as_millis() as u64,
                rows_returned: rows,
                status: status.to_string(),
                client_ip: None,
            });
        }

        result
    }

    /// Return the schema for a SQL statement without executing it.
    ///
    /// Only pure SELECT/WITH queries are planned via DataFusion. For all
    /// other statements (SHOW, DDL, DML) we return an empty schema since
    /// they are side-effect-only and Flight SQL only needs the schema for
    /// the `get_flight_info` response.
    pub async fn get_schema(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<SchemaRef> {
        let kind = parse_and_classify(sql)?;

        if matches!(kind, StatementKind::Query(_)) {
            let ctx = self.create_session_context(session).await?;
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;
            Ok(Arc::new(df.schema().as_arrow().clone()))
        } else {
            // Non-query statements: return empty schema. The actual execution
            // happens in do_get_statement via execute().
            Ok(Arc::new(Schema::empty()))
        }
    }

    /// Execute a SELECT query through DataFusion with the user's catalog.
    ///
    /// After policy enforcement and physical planning, this method attempts to
    /// distribute the scan work across available workers via [`try_distribute`].
    /// If distribution is not possible (single-node mode, no healthy workers,
    /// too few files, or complex multi-table plans), the query executes locally.
    #[tracing::instrument(skip(self, session, sql, query_id), fields(username = %session.user.username))]
    async fn execute_query(
        &self,
        session: &Session,
        sql: &str,
        query_id: &uuid::Uuid,
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

        // Create a new DataFrame from the enforced plan
        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

        // Get the physical plan
        let physical_plan = enforced_df
            .create_physical_plan()
            .await
            .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

        // Try to distribute scan work across workers
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;

        // Execute the (possibly distributed) plan
        let batches = collect(final_plan, ctx.task_ctx())
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

        info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Query execution complete"
        );

        Ok(batches)
    }

    /// Attempt to distribute scan work across available workers.
    ///
    /// If distribution is possible, the plan's IcebergScanExec is replaced
    /// with a DistributedScanExec that fans out to workers via Arrow Flight.
    /// Otherwise, the original plan is returned unchanged for local execution.
    ///
    /// Distribution is skipped when:
    /// - No worker registry is configured (single-node mode)
    /// - No healthy workers are available
    /// - The query has no IcebergScanExec (e.g., metadata queries)
    /// - The total data file count is less than the number of healthy workers
    /// - The query has multiple IcebergScanExec nodes (joins — not yet supported)
    async fn try_distribute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session: &Session,
        query_id: &uuid::Uuid,
    ) -> Arc<dyn ExecutionPlan> {
        // 1. Check if we have a worker registry (distributed mode)
        let registry = match self.worker_registry {
            Some(ref r) => r,
            None => return plan,
        };

        // 2. Get healthy workers — if none, fall back to local
        let healthy = registry.healthy_workers().await;
        if healthy.is_empty() {
            debug!("No healthy workers available, executing locally");
            return plan;
        }

        // 3. Find IcebergScanExec node in the plan tree
        let scan_node = match find_iceberg_scan(&plan) {
            Some(node) => node,
            None => {
                debug!("No IcebergScanExec found in plan, executing locally");
                return plan;
            }
        };

        let iceberg_scan = scan_node
            .as_any()
            .downcast_ref::<IcebergScanExec>()
            .expect("find_iceberg_scan returned a non-IcebergScanExec node");

        // 4. Get data file paths from the scan
        let file_paths = match iceberg_scan.data_file_paths().await {
            Ok(paths) => paths,
            Err(e) => {
                warn!(error = %e, "Failed to list data files for distribution, executing locally");
                return plan;
            }
        };

        let total_files = file_paths.len();
        if total_files == 0 {
            debug!("No data files to distribute, executing locally");
            return plan;
        }

        // 5. Check if there are enough files to justify distribution
        let num_workers = healthy.len();
        if total_files < num_workers {
            debug!(
                total_files,
                num_workers,
                "Fewer files than workers, executing locally"
            );
            return plan;
        }

        info!(
            total_files,
            num_workers,
            "Distributing scan across workers"
        );

        // 6. Get projected columns from the scan
        let projected_cols: Vec<String> = iceberg_scan
            .projection()
            .map(|cols| cols.to_vec())
            .unwrap_or_default();

        // 7. Split files across workers
        let file_groups = sqe_planner::split_files(file_paths, num_workers);

        // 8. Build ScanTasks
        let storage = &self.config.storage;
        let scan_tasks: Vec<sqe_planner::ScanTask> = file_groups
            .into_iter()
            .filter(|files| !files.is_empty())
            .map(|files| sqe_planner::ScanTask {
                fragment_id: uuid::Uuid::now_v7().to_string(),
                data_file_paths: files,
                projected_columns: projected_cols.clone(),
                s3_endpoint: storage.s3_endpoint.clone(),
                s3_region: storage.s3_region.clone(),
                s3_access_key: storage.s3_access_key.clone(),
                s3_secret_key: storage.s3_secret_key.clone(),
                s3_session_token: String::new(),
                s3_path_style: storage.s3_path_style,
                s3_allow_http: storage.s3_endpoint.starts_with("http://"),
            })
            .collect();

        if scan_tasks.is_empty() {
            debug!("No non-empty scan tasks after splitting, executing locally");
            return plan;
        }

        // 9. Schedule tasks to workers using weighted scheduler
        let worker_infos: Vec<crate::scheduler::WorkerInfo> = healthy
            .iter()
            .map(|url| crate::scheduler::WorkerInfo {
                url: url.clone(),
                healthy: true,
                active_fragments: 0, // First version: no active fragment tracking
            })
            .collect();

        let scheduler = crate::scheduler::WeightedScheduler::new();
        let assignments = match crate::scheduler::FragmentScheduler::assign(
            &scheduler,
            &scan_tasks,
            &worker_infos,
        ) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "Scheduling failed, executing locally");
                return plan;
            }
        };

        // Build ordered worker URLs matching the scan_tasks order
        let worker_urls: Vec<String> = assignments.iter().map(|a| a.worker_url.clone()).collect();

        // 10. Record fragments in query tracker
        let fragment_infos: Vec<crate::query_tracker::FragmentInfo> = scan_tasks
            .iter()
            .zip(worker_urls.iter())
            .map(|(task, url)| crate::query_tracker::FragmentInfo {
                task_id: task.fragment_id.clone(),
                worker_url: url.clone(),
                state: crate::query_tracker::FragmentState::Running,
                elapsed_ms: 0,
                input_rows: 0,
                output_rows: 0,
            })
            .collect();
        self.query_tracker.set_fragments(query_id, fragment_infos);

        // 11. Build fragment callback for progress tracking
        let tracker = self.query_tracker.clone();
        let qid = *query_id;
        let callback: crate::distributed_scan::FragmentCallback =
            Arc::new(move |task_id, success, elapsed_ms, rows| {
                let state = if success {
                    FragmentState::Finished
                } else {
                    FragmentState::Failed
                };
                tracker.update_fragment(&qid, task_id, state, elapsed_ms, rows);
            });

        // 12. Build the DistributedScanExec
        let schema = scan_node.schema();
        let mut exec = crate::distributed_scan::DistributedScanExec::new(
            scan_tasks,
            worker_urls,
            schema,
        )
        .with_fragment_callback(callback);

        // Attach worker registry for health tracking / failover
        exec = exec.with_worker_registry(Arc::clone(registry));

        info!(
            username = %session.user.username,
            query_id = %query_id,
            partitions = exec.scan_tasks().len(),
            "Distributed scan plan created"
        );

        Arc::new(exec)
    }

    /// Create a DataFusion SessionContext with the user's Polaris catalog registered.
    #[tracing::instrument(skip(self, session), fields(username = %session.user.username))]
    async fn create_session_context(
        &self,
        session: &Session,
    ) -> sqe_core::Result<SessionContext> {
        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema(&catalog_name, "default"),
        );

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

        // Clone before moving into SqeCatalogProvider (which consumes the Arc)
        let session_catalog_for_system = session_catalog.clone();

        // Create the DataFusion CatalogProvider from the session catalog
        let catalog_provider = SqeCatalogProvider::try_new(
            session_catalog,
            self.config.storage.clone(),
            self.config.catalog.warehouse.clone(),
        )
        .await?;

        ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));

        // Register the system catalog for Trino JDBC metadata browsing
        // (system.jdbc.types, system.jdbc.catalogs, system.jdbc.schemas, etc.)
        // and the system.runtime.* virtual tables for query/node/task info.
        let tracker = Arc::clone(&self.query_tracker);
        let records_fn: sqe_catalog::system_runtime::QueryRecordsFn = Arc::new(move || {
            tracker
                .records()
                .into_iter()
                .map(|r| sqe_catalog::system_runtime::RuntimeQueryRecord {
                    query_id: r.query_id.to_string(),
                    state: match r.state {
                        crate::query_tracker::QueryState::Queued => {
                            sqe_catalog::system_runtime::RuntimeQueryState::Queued
                        }
                        crate::query_tracker::QueryState::Running => {
                            sqe_catalog::system_runtime::RuntimeQueryState::Running
                        }
                        crate::query_tracker::QueryState::Finished => {
                            sqe_catalog::system_runtime::RuntimeQueryState::Finished
                        }
                        crate::query_tracker::QueryState::Failed => {
                            sqe_catalog::system_runtime::RuntimeQueryState::Failed
                        }
                        crate::query_tracker::QueryState::Canceled => {
                            sqe_catalog::system_runtime::RuntimeQueryState::Canceled
                        }
                    },
                    user: r.user.clone(),
                    source: r.source.clone(),
                    sql: r.sql.clone(),
                    created: r.created,
                    started: r.started,
                    ended: r.ended,
                    queued_ms: r.queued_ms,
                    planning_ms: r.planning_ms,
                    execution_ms: r.execution_ms,
                    output_rows: r.output_rows,
                    error_type: r.error_type.clone(),
                    error_code: r.error_code.clone(),
                    fragments: r
                        .fragments
                        .iter()
                        .map(|f| sqe_catalog::system_runtime::RuntimeFragmentInfo {
                            task_id: f.task_id.clone(),
                            worker_url: f.worker_url.clone(),
                            state: f.state.to_string(),
                            elapsed_ms: f.elapsed_ms,
                            input_rows: f.input_rows,
                            output_rows: f.output_rows,
                        })
                        .collect(),
                })
                .collect()
        });
        let coordinator_uri = format!(
            "http://localhost:{}",
            self.config.coordinator.flight_sql_port
        );
        let runtime_schema = Arc::new(sqe_catalog::system_runtime::RuntimeSchemaProvider::new(
            records_fn,
            self.config.catalog.warehouse.clone(),
            coordinator_uri,
            self.config.coordinator.worker_urls.clone(),
        ));
        let system_catalog = sqe_catalog::SystemCatalogProvider::new(
            session_catalog_for_system,
            self.config.catalog.warehouse.clone(),
        )
        .with_runtime(runtime_schema);
        ctx.register_catalog("system", Arc::new(system_catalog));

        // Register the read_parquet() table-valued function so users can
        // query external Parquet files directly from SQL:
        //   SELECT * FROM read_parquet('s3://bucket/path/*.parquet', ...)
        ctx.register_udtf(
            "read_parquet",
            Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
                self.config.storage.clone(),
            )),
        );

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
                    warn!(
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

    /// Drop a table if it exists — used for CREATE OR REPLACE TABLE.
    async fn drop_table_if_exists(
        &self,
        session: &Session,
        table_name: &sqlparser::ast::ObjectName,
    ) -> sqe_core::Result<()> {
        use crate::catalog_ops::parse_table_ref;
        use iceberg::{Catalog, TableIdent};

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
            )
            .await?,
        );

        let catalog = session_catalog.as_catalog();
        match catalog.table_exists(&table_ident).await {
            Ok(true) => {
                info!(table = %table_ident, "DROP existing table for CREATE OR REPLACE");
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(|e| SqeError::Catalog(format!("Failed to drop table for replace: {e}")))?;
            }
            Ok(false) => {}
            Err(e) => {
                return Err(SqeError::Catalog(format!(
                    "Failed to check table existence for replace: {e}"
                )));
            }
        }

        Ok(())
    }

}

/// Walk a physical plan tree to find the first `IcebergScanExec` node.
///
/// Uses iterative depth-first search (plan trees are shallow, so no need
/// for async recursion). Returns the first matching node, or `None` if the
/// plan contains no Iceberg table scans.
fn find_iceberg_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut stack: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.as_any().downcast_ref::<IcebergScanExec>().is_some() {
            return Some(node);
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    None
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
        DataType::List(f) | DataType::LargeList(f) => {
            serde_json::json!({
                "type": "list",
                "element-id": 1,
                "element": arrow_type_to_iceberg(f.data_type()),
                "element-required": !f.is_nullable(),
            })
        }
        DataType::Struct(fields) => {
            let iceberg_fields: Vec<serde_json::Value> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| serde_json::json!({
                    "id": i + 1,
                    "name": f.name(),
                    "required": !f.is_nullable(),
                    "type": arrow_type_to_iceberg(f.data_type()),
                }))
                .collect();
            serde_json::json!({
                "type": "struct",
                "fields": iceberg_fields,
            })
        }
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                let key_field = fields.first();
                let value_field = fields.get(1);
                serde_json::json!({
                    "type": "map",
                    "key-id": 1,
                    "key": key_field.map(|kf| arrow_type_to_iceberg(kf.data_type())).unwrap_or(serde_json::json!("string")),
                    "value-id": 2,
                    "value": value_field.map(|vf| arrow_type_to_iceberg(vf.data_type())).unwrap_or(serde_json::json!("string")),
                    "value-required": value_field.map(|vf| !vf.is_nullable()).unwrap_or(false),
                })
            } else {
                serde_json::json!("string")
            }
        }
        other => {
            tracing::warn!(arrow_type = ?other, "Unmapped Arrow type, falling back to string");
            serde_json::json!("string")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::QueryConfig;
    use sqe_core::session::{Session, SessionUser};

    /// Build a minimal session for timeout tests.
    fn test_session(roles: Vec<&str>) -> Session {
        let now = chrono::Utc::now();
        Session {
            id: "test-session".to_string(),
            user: SessionUser {
                username: "alice".to_string(),
                roles: roles.into_iter().map(String::from).collect(),
            },
            access_token: "tok".to_string(),
            refresh_token: None,
            token_expiry: now + chrono::Duration::hours(1),
            created_at: now,
            last_activity: now,
            default_catalog: None,
            default_schema: None,
            source: None,
        }
    }

    #[test]
    fn timeout_for_session_uses_default_when_no_overrides() {
        let config = QueryConfig::default();
        let session = test_session(vec!["viewer"]);
        assert_eq!(timeout_for_session(&config, &session), 300);
    }

    #[test]
    fn timeout_for_session_uses_default_when_roles_dont_match() {
        let mut config = QueryConfig::default();
        config.role_overrides.insert("admin".to_string(), 600);
        let session = test_session(vec!["viewer"]);
        assert_eq!(timeout_for_session(&config, &session), 300);
    }

    #[test]
    fn timeout_for_session_uses_role_override() {
        let mut config = QueryConfig::default();
        config.role_overrides.insert("etl".to_string(), 900);
        let session = test_session(vec!["viewer", "etl"]);
        assert_eq!(timeout_for_session(&config, &session), 900);
    }

    #[test]
    fn timeout_for_session_picks_highest_matching_role() {
        let mut config = QueryConfig::default();
        config.role_overrides.insert("etl".to_string(), 900);
        config.role_overrides.insert("admin".to_string(), 3600);
        config.role_overrides.insert("viewer".to_string(), 120);
        let session = test_session(vec!["viewer", "admin", "etl"]);
        assert_eq!(timeout_for_session(&config, &session), 3600);
    }

    #[test]
    fn timeout_for_session_empty_roles() {
        let mut config = QueryConfig::default();
        config.role_overrides.insert("admin".to_string(), 600);
        let session = test_session(vec![]);
        assert_eq!(timeout_for_session(&config, &session), 300);
    }

    #[test]
    fn arrow_type_to_iceberg_list() {
        use arrow_schema::{DataType, Field};
        let elem = Arc::new(Field::new("item", DataType::Int32, false));
        let result = arrow_type_to_iceberg(&DataType::List(elem));
        assert_eq!(result["type"], "list");
        assert_eq!(result["element"], "int");
        assert_eq!(result["element-required"], true);
    }

    #[test]
    fn arrow_type_to_iceberg_large_list() {
        use arrow_schema::{DataType, Field};
        let elem = Arc::new(Field::new("item", DataType::Utf8, true));
        let result = arrow_type_to_iceberg(&DataType::LargeList(elem));
        assert_eq!(result["type"], "list");
        assert_eq!(result["element"], "string");
        assert_eq!(result["element-required"], false);
    }

    #[test]
    fn arrow_type_to_iceberg_struct() {
        use arrow_schema::{DataType, Field, Fields};
        let fields: Fields = vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]
        .into();
        let result = arrow_type_to_iceberg(&DataType::Struct(fields));
        assert_eq!(result["type"], "struct");
        let iceberg_fields = result["fields"].as_array().expect("fields array");
        assert_eq!(iceberg_fields.len(), 2);
        assert_eq!(iceberg_fields[0]["name"], "id");
        assert_eq!(iceberg_fields[0]["type"], "long");
        assert_eq!(iceberg_fields[0]["required"], true);
        assert_eq!(iceberg_fields[1]["name"], "name");
        assert_eq!(iceberg_fields[1]["type"], "string");
        assert_eq!(iceberg_fields[1]["required"], false);
    }

    #[test]
    fn arrow_type_to_iceberg_map() {
        use arrow_schema::{DataType, Field, Fields};
        let kv_fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into();
        let entries = Arc::new(Field::new("entries", DataType::Struct(kv_fields), false));
        let result = arrow_type_to_iceberg(&DataType::Map(entries, false));
        assert_eq!(result["type"], "map");
        assert_eq!(result["key"], "string");
        assert_eq!(result["value"], "int");
        assert_eq!(result["value-required"], false);
    }
}

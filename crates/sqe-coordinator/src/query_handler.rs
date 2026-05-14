use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::TryStreamExt;
use datafusion::prelude::SessionContext;
use tracing::{debug, info, warn, Span};

use sqlparser::ast::{Statement, TableFactor};
use sqe_catalog::{IcebergScanExec, SessionCatalog};
use sqe_core::{QueryConfig, SecretStore, Session, SortMode, SqeConfig, SqeError};

use crate::adaptive_sort;
use sqe_policy::{PolicyEnforcer, PolicyStore};
use sqe_policy::grants::{
    AccessCheck, GrantBackend, GrantFilter, GrantStatement, Grantee, RevokeStatement,
};
use sqe_sql::{parse_and_classify, StatementKind};

use crate::catalog_ops::CatalogOps;
use crate::credential_refresh::CredentialRefreshTracker;
use crate::maintenance::MaintenanceHandler;
use crate::query_cache::ResultCache;
use crate::query_tracker::{FragmentState, QueryTracker};
use crate::runtime_catalog::RuntimeCatalogRegistry;
use crate::write_handler::WriteHandler;

/// Per-query resource metrics extracted from the executed physical plan tree.
///
/// `pub(crate)` so the streaming finalizer can construct and inspect
/// these alongside the existing `execute()` path.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlanMetrics {
    pub(crate) bytes_scanned: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) spill_bytes: u64,
    pub(crate) peak_memory_bytes: u64,
}

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
    /// Optional policy store for filtering restricted columns in information_schema.
    policy_store: Option<Arc<dyn PolicyStore>>,
    config: SqeConfig,
    catalog_ops: CatalogOps,
    write_handler: WriteHandler,
    maintenance_handler: MaintenanceHandler,
    explain_handler: crate::explain::ExplainHandler,
    worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
    #[allow(dead_code)] // Used when constructing DistributedScanExec for distributed queries
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    /// Optional OpenLineage observer. When set and the statement kind is
    /// emit-eligible (see `should_emit`), `execute()` calls
    /// `on_query_start` / `on_query_complete` / `on_query_fail` around the
    /// dispatch.
    lineage: Option<Arc<dyn sqe_lineage::LineageObserver>>,
    query_tracker: Arc<QueryTracker>,
    query_cache: Option<Arc<ResultCache>>,
    /// Semaphore limiting concurrent query execution.
    query_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Shared DataFusion runtime with FairSpillPool memory management.
    /// Built once at startup and reused across all queries.
    runtime: Arc<RuntimeEnv>,
    /// Shared global table metadata cache. Persists across sessions and queries so
    /// that repeated `load_table()` calls skip the Polaris REST round-trip.
    table_cache: Option<sqe_catalog::TableMetadataCache>,
    /// Pluggable backend for GRANT/REVOKE/SHOW GRANTS operations.
    /// Initialized by the caller based on `[access_control]` config.
    grant_backend: Option<Arc<dyn GrantBackend>>,
    /// Session manager used for `SET WRITE_BRANCH` mutations.
    /// Optional so in-process tests can construct a QueryHandler without it.
    session_manager: Option<Arc<crate::session_manager::SessionManager>>,
    /// Process-local registry of catalogs added via SQL `ATTACH`.
    /// Default-constructed registries are empty and impose no behaviour
    /// change for callers that never issue `ATTACH`. Cloning shares the
    /// underlying map so multiple handlers (Flight SQL, Trino) see the
    /// same attached set.
    runtime_catalogs: RuntimeCatalogRegistry,
    /// Process-global in-memory secret store for `CREATE SECRET`. Same
    /// rationale as `runtime_catalogs`: a default-constructed store is
    /// empty and acts as a no-op until SQL populates it.
    secrets: SecretStore,
}

impl QueryHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        policy_store: Option<Arc<dyn PolicyStore>>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
        credential_tracker: Option<Arc<CredentialRefreshTracker>>,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
        query_tracker: Arc<QueryTracker>,
        query_cache: Option<Arc<ResultCache>>,
        grant_backend: Option<Arc<dyn GrantBackend>>,
        lineage: Option<Arc<dyn sqe_lineage::LineageObserver>>,
        runtime_catalogs: RuntimeCatalogRegistry,
        secrets: SecretStore,
    ) -> sqe_core::Result<Self> {
        let catalog_ops = CatalogOps::new(config.clone());
        let mut write_handler = WriteHandler::new(config.clone());
        if let Some(ref m) = metrics {
            write_handler = write_handler.with_metrics(Arc::clone(m));
        }
        write_handler = write_handler.with_policy_enforcer(Arc::clone(&policy_enforcer));
        let mut maintenance_handler = MaintenanceHandler::new(config.clone());
        if let Some(ref a) = audit {
            maintenance_handler = maintenance_handler.with_audit(Arc::clone(a));
        }
        // Wire the query tracker in as a history callback so
        // `suggest_bloom_filter_columns` can walk recent SQL texts.
        {
            let tracker = Arc::clone(&query_tracker);
            let f: crate::maintenance::QueryHistoryFn = Arc::new(move || {
                tracker
                    .records()
                    .into_iter()
                    .map(|rec| rec.sql.clone())
                    .collect()
            });
            maintenance_handler = maintenance_handler.with_query_history(f);
        }
        let explain_handler = crate::explain::ExplainHandler::new(Arc::clone(&policy_enforcer));
        let query_semaphore = if config.query.max_concurrent_queries > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(config.query.max_concurrent_queries)))
        } else {
            None
        };

        // Build shared DataFusion runtime with FairSpillPool for memory management
        // and optional spill-to-disk. This is built once and shared across all queries.
        let runtime = crate::runtime::build_coordinator_runtime(&config.coordinator)
            .map_err(|e| sqe_core::SqeError::Config(format!("Failed to build runtime: {e}")))?;

        Ok(Self {
            policy_enforcer,
            policy_store,
            config,
            catalog_ops,
            write_handler,
            maintenance_handler,
            explain_handler,
            worker_registry,
            credential_tracker,
            metrics,
            audit,
            lineage,
            query_tracker,
            query_cache,
            query_semaphore,
            runtime,
            table_cache: None,
            grant_backend,
            session_manager: None,
            runtime_catalogs,
            secrets,
        })
    }

    /// Attach the session manager so `SET WRITE_BRANCH` can mutate session state.
    pub fn with_session_manager(
        mut self,
        manager: Arc<crate::session_manager::SessionManager>,
    ) -> Self {
        self.session_manager = Some(manager);
        self
    }

    /// Attach a global table metadata cache shared across all sessions and queries.
    ///
    /// The cache is created once at coordinator startup and passed here so that
    /// every `SessionCatalog` constructed during query execution shares the same
    /// backing store. This eliminates the per-query Polaris REST round-trip for
    /// tables that have already been loaded within the TTL window.
    ///
    /// Propagates the cache into the sub-handlers (`CatalogOps`, `WriteHandler`)
    /// so that DDL and write paths also share the same global cache.
    pub fn with_table_cache(mut self, cache: sqe_catalog::TableMetadataCache) -> Self {
        self.catalog_ops = self.catalog_ops.with_table_cache(cache.clone());
        self.write_handler = self.write_handler.with_table_cache(cache.clone());
        self.maintenance_handler = self.maintenance_handler.with_table_cache(cache.clone());
        self.table_cache = Some(cache);
        self
    }

    /// Returns a reference to the query tracker.
    pub fn query_tracker(&self) -> &Arc<QueryTracker> {
        &self.query_tracker
    }

    /// Returns a reference to the shared DataFusion runtime.
    ///
    /// The runtime contains the [`FairSpillPool`] memory pool shared across
    /// all queries. Use this to check memory usage for admission control.
    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    pub fn write_handler(&self) -> &WriteHandler {
        &self.write_handler
    }

    /// Return the cached per-session [`SessionCatalog`] for the given session.
    ///
    /// This is the same catalog instance that backs the user's queries —
    /// it's built once on a SessionContext cache miss and reused thereafter.
    /// Metadata paths (Flight `do_get_tables`, `SHOW SCHEMAS`, `SHOW TABLES`)
    /// use this to avoid rebuilding the catalog wrapping (token hash, prop
    /// map, HTTP client) every call.
    pub async fn session_catalog(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
        let (_ctx, catalog) = self.create_session_context(session).await?;
        Ok(catalog)
    }

    /// List `(namespace, table_name)` pairs reachable by `session` across the
    /// default Iceberg catalog. Used by Flight SQL `do_get_tables` and the
    /// JDBC `DatabaseMetaData.getTables` path; bypasses the SQL planner so
    /// 500-table warehouses no longer trigger 500 planner invocations
    /// (issue #7), removes the catalog-name → SQL-string concatenation that
    /// enabled SQL injection by federated catalogs returning crafted names
    /// (issue #9), and reuses the cached `SessionCatalog` instead of
    /// rebuilding it (issue #15).
    ///
    /// Namespaces are listed sequentially (one Polaris call) and the per-
    /// namespace `list_tables` fans out with bounded concurrency. Views are
    /// included when the catalog backend supports them; unsupported backends
    /// (non-REST) are skipped silently per the existing schema-provider
    /// contract.
    pub async fn list_metadata_tables(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<(String, String)>> {
        use futures::StreamExt;

        let catalog = self.session_catalog(session).await?;
        let namespaces = catalog.list_namespaces().await?;

        // Bounded concurrency keeps load on Polaris predictable while
        // shaving wall-clock for warehouses with many schemas.
        const MAX_INFLIGHT: usize = 16;

        let entries: Vec<(String, Vec<String>)> = futures::stream::iter(namespaces.into_iter())
            .map(|ns| {
                let catalog = Arc::clone(&catalog);
                async move {
                    let ns_label: String = ns
                        .as_ref()
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(".");

                    let mut names: Vec<String> = match catalog.list_tables(&ns).await {
                        Ok(tables) => tables.into_iter().map(|t| t.name().to_string()).collect(),
                        Err(e) => {
                            warn!(
                                namespace = %ns_label,
                                error = %e,
                                "list_metadata_tables: failed to list tables, skipping"
                            );
                            Vec::new()
                        }
                    };

                    // Views: REST-only. Non-REST backends return an error
                    // which we swallow — same shape as schema_provider.
                    if let Ok(views) = catalog.list_views(&ns).await {
                        names.extend(views);
                    }

                    (ns_label, names)
                }
            })
            .buffer_unordered(MAX_INFLIGHT)
            .collect()
            .await;

        let mut pairs: Vec<(String, String)> = Vec::new();
        for (ns_label, names) in entries {
            for name in names {
                pairs.push((ns_label.clone(), name));
            }
        }
        // Sorted output for stable JDBC client display.
        pairs.sort();
        Ok(pairs)
    }

    /// Reject a coordinator-wide DDL statement when the session does not
    /// hold an admin role. `statement` is the SQL verb (e.g. "ATTACH",
    /// "CREATE SECRET") and lands in the audit log + the error message
    /// returned to the client so operators can see which statement was
    /// denied without dumping the full SQL text.
    ///
    /// The role allowlist comes from `[auth] admin_roles` (default:
    /// `["service_admin", "catalog_admin"]`). An empty allowlist
    /// fails closed for every caller — operators must populate it
    /// before any admin statement succeeds. Issue #3.
    fn require_admin(&self, session: &Session, statement: &str) -> sqe_core::Result<()> {
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(());
        }
        warn!(
            username = %session.user.username,
            roles = ?session.user.roles,
            statement = statement,
            "denied: caller lacks admin role required for coordinator-wide DDL"
        );
        // Build the message so `classify_catalog_error` maps it to
        // `SqeErrorCode::AccessDenied` (and thus gRPC PermissionDenied)
        // rather than AuthenticationFailed — the caller is authenticated,
        // just not authorised. The "403 Forbidden" prefix is what the
        // substring classifier looks for.
        Err(SqeError::Catalog(format!(
            "403 Forbidden: {statement} requires one of admin roles {:?}; \
             caller has roles {:?}",
            self.config.auth.admin_roles, session.user.roles
        )))
    }

    /// Compute the set of catalog names that the coordinator will
    /// register on every session context. The pre-flight unknown-
    /// qualifier check compares the leading component of any 3-part
    /// identifier against this set.
    ///
    /// Sources:
    /// 1. `config.flattened_catalogs()` - the legacy `[catalog]` block
    ///    plus any `[catalogs.<name>]` map entries. This matches the
    ///    list `session_context::create_session_context` registers.
    /// 2. The two coordinator-registered system catalogs `system`
    ///    (Trino JDBC metadata + `system.runtime.*`) and `datafusion`
    ///    (in-memory scratch catalog used by the IN-subquery
    ///    rewriter).
    ///
    /// Returned in stable sorted order so the error message reads the
    /// same across runs.
    fn known_catalog_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .config
            .flattened_catalogs()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        names.push("system".to_string());
        names.push("datafusion".to_string());
        names.extend(self.runtime_catalogs.list());
        names.sort();
        names.dedup();
        names
    }

    /// Execute a SQL statement for the given session and return collected RecordBatches.
    #[tracing::instrument(
        skip(self, session, sql),
        fields(
            db.system.name = "sqe",
            db.operation.name = tracing::field::Empty,
            db.namespace = tracing::field::Empty,
            db.collection.name = tracing::field::Empty,
            username = %session.user.username,
            statement_type,
        ),
        name = "execute",
    )]
    pub async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Memory pressure admission control: reject new queries when the
        // coordinator's FairSpillPool is >95% utilized (Red).
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        if let Some(ref metrics) = self.metrics {
            metrics
                .coordinator_memory_pressure
                .set(pressure.as_gauge());
            metrics
                .coordinator_memory_used_bytes
                .set(crate::memory::used_bytes(&self.runtime.memory_pool) as f64);
            metrics
                .coordinator_memory_limit_bytes
                .set(crate::memory::limit_bytes(&self.runtime.memory_pool) as f64);
        }
        if !pressure.admits_new_query() {
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                "Rejecting query due to memory pressure"
            );
            let sort_cols = extract_order_by_columns(sql);
            return Err(SqeError::Execution(
                adaptive_sort::format_pressure_rejection(&sort_cols, pressure),
            ));
        }

        // Backpressure: reject if too many concurrent queries
        let _permit = if let Some(ref sem) = self.query_semaphore {
            match sem.try_acquire() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(SqeError::Execution(format!(
                        "Too many concurrent queries ({} active). Please retry later.",
                        self.config.query.max_concurrent_queries
                    )));
                }
            }
        } else {
            None
        };

        let start = std::time::Instant::now();
        // Pre-strip `FOR INCREMENTAL BETWEEN SNAPSHOT` before classification.
        // sqlparser does not model the clause, so the classifier's
        // `Parser::parse_sql` would reject it. The downstream
        // `handle_incremental` call re-extracts the specs from the original
        // SQL and registers the providers against the same session context.
        let (classify_sql, _incremental_specs) = sqe_sql::extract_incremental_spec(sql)?;
        // Also strip `FOR VERSION AS OF`. sqlparser-rs models
        // `FOR SYSTEM_TIME AS OF` natively but not VERSION; the time-travel
        // pre-parser handles VERSION later, but the classifier runs first
        // and would otherwise reject the query.
        let (classify_sql, _version_specs) =
            sqe_sql::extract_time_travel_spec(&classify_sql)?;
        // Rewrite Hive/Spark/Trino-style `PARTITIONED BY (...)` into
        // sqlparser-friendly `PARTITION BY (...)`. sqlparser's native
        // PARTITIONED BY expects column definitions; we want Iceberg
        // transforms (year/month/day/hour/bucket/truncate/identity)
        // which fit the BigQuery-style `PARTITION BY <expr>` shape.
        let classify_sql = sqe_sql::normalize_partitioned_by(&classify_sql);
        let kind = parse_and_classify(&classify_sql)?;
        let kind_name = kind.name().to_string();

        // Pre-flight: when a 3-part identifier names a catalog that the
        // coordinator has not registered, fail fast with a clear error
        // instead of letting DataFusion silently fall back to the
        // session-default catalog. The silent fallback produces
        // confusing "namespace does not exist" errors against the
        // wrong warehouse and was the original symptom of issue #1.
        if let Some(stmt) = kind.statement() {
            let qualifiers = sqe_sql::extract_catalog_qualifiers(stmt);
            if !qualifiers.is_empty() {
                let known = self.known_catalog_names();
                if let Some(unknown) =
                    qualifiers.iter().find(|q| !known.iter().any(|k| k == *q))
                {
                    return Err(SqeError::Catalog(format!(
                        "unknown catalog '{}' in 3-part identifier; configured \
                         catalogs are {:?}. Use TOML `[catalogs.<name>]` to declare \
                         additional catalogs, or `ATTACH` at runtime once that lands.",
                        unknown, known
                    )));
                }
            }
        }

        let span = Span::current();
        span.record("statement_type", kind_name.as_str());
        span.record("db.operation.name", kind_name.as_str());

        // Best-effort: extract schema and table names for OTel conventions.
        if let Some((schema_name, table_name)) = extract_otel_table_info(&kind) {
            if let Some(ref s) = schema_name {
                span.record("db.namespace", s.as_str());
            }
            if let Some(ref t) = table_name {
                span.record("db.collection.name", t.as_str());
            }
        }

        // Generate a query ID for lifecycle tracking
        let query_id = uuid::Uuid::now_v7();
        // Wall-clock start timestamp for OpenLineage. The Instant `start`
        // already captured monotonic time but OL events need RFC3339 timestamps.
        let ol_started_at = chrono::Utc::now();
        info!(
            query_id = %query_id,
            username = %session.user.username,
            sql_length = sql.len(),
            "Executing query"
        );
        self.query_tracker.start(
            query_id,
            &session.user.username,
            session.source.as_deref(),
            sql,
            &session.id,
            None, // client_ip — populated by caller if available
            session.user.roles.clone(),
        );

        // OpenLineage: emit START event. The observer is sync and best-effort;
        // failures inside the observer (full channel, etc.) increment a metric
        // but do not affect query execution.
        let ol_emit = self.lineage.is_some()
            && should_emit(&kind, &self.config.metrics.openlineage);
        if ol_emit {
            if let Some(ref obs) = self.lineage {
                obs.on_query_start(sqe_lineage::QueryStartCtx {
                    run_id: query_id,
                    job_namespace: self.config.metrics.openlineage.job_namespace.clone(),
                    sql: sql.to_string(),
                    user: sqe_lineage::UserCtx {
                        username: session.user.username.clone(),
                        bearer: Some(session.access_token.clone()),
                    },
                    session_id: session.id.clone(),
                    started_at: ol_started_at,
                    statement_kind: kind_name.clone(),
                });
            }
        }

        // Check result cache for read queries (before execution)
        if let StatementKind::Query(_) = &kind {
            if let Some(ref cache) = self.query_cache {
                if let Some(cached) = cache.lookup(&session.user.username, sql) {
                    debug!(username = %session.user.username, "Cache hit");
                    let rows: usize = cached.batches.iter().map(|b| b.num_rows()).sum();
                    self.query_tracker.complete(&query_id, rows, 0, cached.tables_touched.clone(), 0, 0, 0, 0);
                    if let Some(ref metrics) = self.metrics {
                        metrics.query_count.with_label_values(&["success", &kind_name]).inc();
                        metrics.rows_returned.inc_by(rows as f64);
                    }
                    // OpenLineage: cache hits still get a COMPLETE event so the
                    // run shows up end-to-end. The plan field is None because
                    // the post-policy plan was not re-derived on the fast path.
                    if ol_emit {
                        if let Some(ref obs) = self.lineage {
                            let ol_ended_at = chrono::Utc::now();
                            obs.on_query_complete(sqe_lineage::QueryCompleteCtx {
                                run_id: query_id,
                                job_namespace: self.config.metrics.openlineage.job_namespace.clone(),
                                sql: sql.to_string(),
                                user: sqe_lineage::UserCtx {
                                    username: session.user.username.clone(),
                                    bearer: Some(session.access_token.clone()),
                                },
                                session_id: session.id.clone(),
                                started_at: ol_started_at,
                                ended_at: ol_ended_at,
                                duration: ol_ended_at
                                    .signed_duration_since(ol_started_at)
                                    .to_std()
                                    .unwrap_or_default(),
                                statement_kind: kind_name.clone(),
                                rows_returned: rows,
                                plan: None,
                            });
                        }
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

        let plan_metrics = Arc::new(Mutex::new(PlanMetrics::default()));
        // Slot for the lineage observer's plan capture. `execute_query`
        // populates this after policy enforcement; non-Query branches leave
        // it as None and the observer falls back to plan-less inputs/outputs.
        let mut captured_plan: Option<sqe_lineage::PlanOrHint> = None;
        let execution_future = async {
            match &kind {
                StatementKind::Query(_) => self.execute_query(session, sql, &query_id, &plan_metrics, &mut captured_plan).await,

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
                        let (ctx, _) = self.create_session_context(session).await?;
                        if *analyze {
                            self.explain_handler.analyze(session, &inner, &ctx).await
                        } else {
                            self.explain_handler.plan(session, &inner, &ctx).await
                        }
                    } else if let sqlparser::ast::Statement::ShowColumns {
                        show_options, ..
                    } = stmt.as_ref()
                    {
                        // Trino: SHOW COLUMNS FROM ns.table -> rewrite as a
                        // query against information_schema.columns. Same pattern
                        // as handle_show_create_table; the Trino output has
                        // four columns (Column, Type, Extra, Comment), but we
                        // emit (column_name, data_type, is_nullable) for now.
                        // dbt and most BI clients use this query for schema
                        // inspection and only need the first two columns.
                        self.handle_show_columns(session, show_options).await
                    } else {
                        Err(SqeError::NotImplemented(format!(
                            "Utility statement not supported: {stmt}"
                        )))
                    }
                }

                StatementKind::Grant(ref stmt) => self.handle_grant(session, stmt).await,
                StatementKind::Revoke(ref stmt) => self.handle_revoke(session, stmt).await,
                StatementKind::ShowGrants(ref target) => self.handle_show_grants(session, target).await,
                StatementKind::ShowEffectiveGrants(ref user) => self.handle_show_effective_grants(session, user).await,
                StatementKind::CheckAccess(ref params) => self.handle_check_access(session, params).await,

                // DDL/DML invalidation scope (issue #11): a 50 ms SessionContext
                // rebuild on cache miss multiplied by every active user's next
                // query is a real cost on multi-tenant deployments running dbt.
                // Most table mutations only need the writer's cache flushed;
                // other users will refresh on their normal 5-minute TTL. Cross-
                // user invalidation is reserved for changes that affect the
                // schema/catalog name list (RENAME, CREATE/DROP SCHEMA,
                // ATTACH/DETACH).
                StatementKind::Drop(stmt) => {
                    self.catalog_ops.drop_table(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::Rename(stmt) => {
                    // Cross-user: the table name itself changes; other users'
                    // SessionContexts hold the old name in their catalog
                    // provider's cached namespace listings.
                    self.catalog_ops.rename_table(session, stmt).await?;
                    crate::session_context::invalidate_all_session_caches();
                    Ok(vec![])
                }
                StatementKind::AlterSchema(stmt) => {
                    self.catalog_ops.alter_table_schema(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::AlterTableProps(stmt) => {
                    self.catalog_ops.set_table_properties(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::RefDdl(ddl) => {
                    self.catalog_ops.apply_ref_ddl(session, ddl).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::PartitionEvolution(evolution) => {
                    self.catalog_ops
                        .apply_partition_evolution(session, evolution)
                        .await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::SetWriteBranch(ref branch) => {
                    if let Some(ref manager) = self.session_manager {
                        let updated = manager.set_write_branch(&session.id, branch.clone());
                        if !updated {
                            return Err(SqeError::Execution(
                                "SET WRITE_BRANCH: session not found in manager".into(),
                            ));
                        }
                        tracing::info!(
                            username = %session.user.username,
                            branch = ?branch,
                            "SET WRITE_BRANCH applied"
                        );
                    } else {
                        tracing::debug!(
                            "SET WRITE_BRANCH requested but session manager is not wired; \
                             the value is ignored in this mode"
                        );
                    }
                    Ok(vec![])
                }
                StatementKind::CreateView(stmt) => {
                    self.handle_create_view(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::DropView(stmt) => {
                    self.catalog_ops.drop_view(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    Ok(vec![])
                }
                StatementKind::CreateSchema(stmt) => {
                    // Cross-user: a new namespace appears in the catalog
                    // listing, which other users' SessionContexts cache.
                    self.catalog_ops.create_schema(session, stmt).await?;
                    crate::session_context::invalidate_all_session_caches();
                    Ok(vec![])
                }
                StatementKind::DropSchema(stmt) => {
                    // Cross-user: the namespace disappears for everyone.
                    self.catalog_ops.drop_schema(session, stmt).await?;
                    crate::session_context::invalidate_all_session_caches();
                    Ok(vec![])
                }

                StatementKind::CreateTable(stmt) => {
                    let result = self.write_handler.handle_create_table(session, stmt).await;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    result
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
                            let (ctx, _) = self.create_session_context(session).await?;
                            let result = self.write_handler
                                .handle_ctas_streaming(
                                    session,
                                    stmt,
                                    &ctx,
                                    &select_sql,
                                    &mut captured_plan,
                                )
                                .await;
                            crate::session_context::invalidate_session_cache(&session.user.username).await;
                            result
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
                        let (ctx, _) = self.create_session_context(session).await?;
                        self.write_handler
                            .handle_insert_streaming(
                                session,
                                stmt,
                                &ctx,
                                &select_sql,
                                &mut captured_plan,
                            )
                            .await
                    } else {
                        Err(SqeError::Execution("Expected Insert statement".into()))
                    }
                }

                StatementKind::ExplainFull(inner) => {
                    let (ctx, _) = self.create_session_context(session).await?;
                    self.explain_handler.full(session, inner, &ctx).await
                }

                StatementKind::Delete(stmt) => {
                    let (ctx, session_catalog) = self.create_session_context(session).await?;
                    // Dispatch on `write.delete.mode` table property. Default CoW
                    // matches prior behaviour; `merge-on-read` routes to position
                    // or equality deletes depending on declared primary key.
                    self.write_handler
                        .handle_delete_dispatch(
                            session,
                            stmt,
                            session_catalog,
                            &ctx,
                            &mut captured_plan,
                        )
                        .await
                }

                StatementKind::Update(stmt) => {
                    let (ctx, session_catalog) = self.create_session_context(session).await?;
                    // Dispatch on `write.update.mode` table property. Default CoW
                    // matches prior behaviour; `merge-on-read` routes to the
                    // equality-delete path when the table has a declared PK.
                    self.write_handler
                        .handle_update_dispatch(
                            session,
                            stmt,
                            session_catalog,
                            &ctx,
                            &mut captured_plan,
                        )
                        .await
                }

                // Transaction stubs — no-ops for JDBC tools that use setAutoCommit(false)
                StatementKind::Begin | StatementKind::Commit | StatementKind::Rollback => {
                    tracing::debug!("Transaction stubs: BEGIN/COMMIT/ROLLBACK are no-ops");
                    Ok(vec![])
                }

                // USE catalog.schema — session context switching stub.
                // The actual session mutation happens in the Trino HTTP layer via
                // X-Trino-Set-Catalog / X-Trino-Set-Schema response headers.
                StatementKind::Use(ref target) => {
                    tracing::info!("USE {target} — session catalog/schema switch (no-op at engine level)");
                    Ok(vec![])
                }

                // SHOW CREATE TABLE — reconstruct DDL from information_schema
                StatementKind::ShowCreateTable(ref stmt) => {
                    let (ctx, session_catalog) = self.create_session_context(session).await?;
                    self.handle_show_create_table(session, stmt, &ctx, &session_catalog)
                        .await
                }

                // TRUNCATE TABLE — rewrite as DELETE FROM (no WHERE clause)
                StatementKind::Truncate(ref table_name) => {
                    tracing::info!("TRUNCATE TABLE {table_name} → DELETE FROM {table_name}");
                    let delete_sql = format!("DELETE FROM {table_name}");
                    let delete_kind = sqe_sql::parse_and_classify(&delete_sql)?;
                    if let StatementKind::Delete(delete_stmt) = delete_kind {
                        let (ctx, session_catalog) = self.create_session_context(session).await?;
                        // Route through dispatch so OL lineage capture covers
                        // TRUNCATE the same way it covers a normal DELETE.
                        self.write_handler
                            .handle_delete_dispatch(
                                session,
                                &delete_stmt,
                                session_catalog,
                                &ctx,
                                &mut captured_plan,
                            )
                            .await
                    } else {
                        Err(SqeError::Execution("Failed to rewrite TRUNCATE as DELETE".into()))
                    }
                }

                // CALL procedure — not supported
                StatementKind::Call(_) => {
                    Err(SqeError::NotImplemented(
                        "CALL is not supported. SQE does not have stored procedures. \
                         Use SQL statements directly instead.".into(),
                    ))
                }

                // CALL system.<maintenance procedure>(...) - Iceberg
                // compaction, snapshot expiry, orphan file removal, manifest
                // rewrite. These procedures write new snapshots; the cached
                // SessionContext in SESSION_CONTEXT_CACHE holds DataFusion
                // TVF MemTables built from the pre-call snapshot, so any
                // follow-up SELECT ... FROM table_files(...) / table_snapshots(...)
                // in the same session would serve stale rows. Invalidate after
                // the call completes so the next query rebuilds the context
                // against the fresh Polaris metadata.
                StatementKind::Procedure(ref call) => {
                    let _ = call; // table_ref kept for future per-table invalidation
                    let result = self.maintenance_handler.handle(session, call).await;

                    // Maintenance procedures rewrite the table's snapshot
                    // history. Two caches must drop their entries before the
                    // next read or the user sees stale results:
                    //
                    // 1. SESSION_CONTEXT_CACHE keeps a per-user DataFusion
                    //    SessionContext. Its registered table_files /
                    //    table_snapshots TVFs return MemTables built from the
                    //    pre-rewrite metadata. moka's remove + flush of
                    //    pending tasks drops the entry immediately.
                    // 2. ResultCache (query_cache) keys by SQL text. The
                    //    per-table invalidation does NOT cover queries that
                    //    referenced the table through a TVF: the TableScan
                    //    carries the function name (`table_files`) rather
                    //    than the Iceberg identifier. Nuke the whole result
                    //    cache after a procedure: maintenance procedures are
                    //    rare and the cache rebuilds cheaply on next read.
                    crate::session_context::invalidate_session_cache(
                        &session.user.username,
                    ).await;
                    if let Some(ref qcache) = self.query_cache {
                        qcache.invalidate_all();
                    }
                    result
                }

                // COMMENT ON TABLE/COLUMN — store as Iceberg table property
                StatementKind::Comment(ref stmt) => {
                    let (_, session_catalog) = self.create_session_context(session).await?;
                    self.handle_comment_on(session, stmt, &session_catalog).await
                }

                // SHOW STATS FOR table — return snapshot summary stats
                StatementKind::ShowStats(ref table_name) => {
                    let (_, session_catalog) = self.create_session_context(session).await?;
                    self.handle_show_stats(session, table_name, &session_catalog).await
                }

                StatementKind::Merge(stmt) => {
                    // Extract source SQL from the MERGE statement and execute it
                    // to get the source batches, then pass them to the write handler.
                    let source_sql = if let Statement::Merge { source, .. } = stmt.as_ref() {
                        match source {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                format!("SELECT * FROM {name}")
                            }
                            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                                format!("{subquery}")
                            }
                            other => {
                                return Err(SqeError::Execution(format!(
                                    "Unsupported MERGE source: {other}"
                                )));
                            }
                        }
                    } else {
                        return Err(SqeError::Execution(
                            "Expected MERGE statement".into(),
                        ));
                    };
                    let (ctx, session_catalog) = self.create_session_context(session).await?;
                    // Capture the MERGE source plan from `execute_query`. The
                    // lineage for a MERGE event should describe the source's
                    // TableScans as inputs and the target as the output, so
                    // the write handler re-wraps this source plan as a
                    // synthetic INSERT against the target.
                    let mut merge_source_plan: Option<sqe_lineage::PlanOrHint> = None;
                    let source_batches = self
                        .execute_query(session, &source_sql, &query_id, &plan_metrics, &mut merge_source_plan)
                        .await?;
                    let merge_source_logical = match merge_source_plan {
                        Some(sqe_lineage::PlanOrHint::Plan(p)) => Some(*p),
                        _ => None,
                    };
                    // Dispatch on `write.merge.mode` table property. Default CoW
                    // matches prior behaviour; `merge-on-read` routes to the
                    // equality-delete path when the target has a declared PK.
                    self.write_handler
                        .handle_merge_dispatch(
                            session,
                            stmt,
                            source_batches,
                            session_catalog,
                            &ctx,
                            merge_source_logical,
                            &mut captured_plan,
                        )
                        .await
                }

                // Coordinator-wide DDL — ATTACH / DETACH mount and unmount
                // catalog backends, CREATE / DROP SECRET mutate the in-memory
                // credential store, SHOW SECRETS exposes the inventory.
                // Every authenticated session could run these before #3.
                // Now gated behind the `[auth] admin_roles` allowlist; the
                // helper returns PermissionDenied early when the caller is
                // not an admin.
                StatementKind::Attach(stmt) => {
                    self.require_admin(session, "ATTACH")?;
                    self.handle_attach(stmt).await
                }
                StatementKind::Detach(stmt) => {
                    self.require_admin(session, "DETACH")?;
                    self.handle_detach(stmt)
                }
                StatementKind::CreateSecret(stmt) => {
                    self.require_admin(session, "CREATE SECRET")?;
                    self.handle_create_secret(stmt)
                }
                StatementKind::DropSecret(stmt) => {
                    self.require_admin(session, "DROP SECRET")?;
                    self.handle_drop_secret(stmt)
                }
                StatementKind::ShowSecrets => {
                    self.require_admin(session, "SHOW SECRETS")?;
                    self.handle_show_secrets()
                }
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
                let timeout_error = SqeError::Execution(format!(
                    "Query timed out after {timeout_secs}s"
                ));
                self.query_tracker.failed(&query_id, &timeout_error);
                Err(timeout_error)
            }
        };

        // Auth-failure recovery (issue #20). When the catalog rejected our
        // bearer mid-query — typically Polaris-side expiry crossing the
        // REST_CATALOG_CACHE TTL boundary on a long-running dbt run — the
        // cached `RestCatalog` and `SessionContext` both still carry the
        // stale token. Drop them now so the very next query rebuilds with
        // whatever fresh bearer the session has (background refresh or
        // re-auth from the client).
        if let Err(ref err) = result {
            if matches!(
                err.error_code(),
                sqe_core::SqeErrorCode::AuthenticationFailed
                    | sqe_core::SqeErrorCode::AccessDenied
            ) {
                warn!(
                    username = %session.user.username,
                    error_code = ?err.error_code(),
                    "Auth failure on catalog; evicting REST_CATALOG_CACHE and SESSION_CONTEXT_CACHE for the user"
                );
                sqe_catalog::invalidate_rest_catalog_cache_all().await;
                crate::session_context::invalidate_session_cache(&session.user.username).await;
            }
        }

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
            let pm = plan_metrics.lock().unwrap_or_else(|e| e.into_inner()).clone();
            self.query_tracker.complete(
                &query_id,
                rows,
                execution_ms,
                vec![],
                pm.bytes_scanned,
                pm.rows_scanned,
                pm.spill_bytes,
                pm.peak_memory_bytes,
            );

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
                            let table = ins.table.to_string();
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
                    StatementKind::Delete(stmt) => {
                        if let Statement::Delete(del) = stmt.as_ref() {
                            let tables = match &del.from {
                                sqlparser::ast::FromTable::WithFromKeyword(t)
                                | sqlparser::ast::FromTable::WithoutKeyword(t) => t,
                            };
                            if let Some(first) = tables.first() {
                                let table = first.relation.to_string();
                                cache.invalidate(&table);
                            }
                        }
                    }
                    StatementKind::Update(stmt) => {
                        if let Statement::Update { table, .. } = stmt.as_ref() {
                            let table_name = table.relation.to_string();
                            cache.invalidate(&table_name);
                        }
                    }
                    StatementKind::Merge(stmt) => {
                        if let Statement::Merge { table, .. } = stmt.as_ref() {
                            let table_name = table.to_string();
                            cache.invalidate(&table_name);
                        }
                    }
                    _ => {}
                }
            }
        } else if let Err(ref e) = result {
            // Only mark failed if not already marked (e.g., timeout already marked above)
            self.query_tracker.failed(&query_id, e);
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

            // Record time-to-first-row for successful queries that returned rows.
            // In the current collect()-based model this equals total execution time;
            // when streaming is wired in it will reflect true first-row latency.
            if status == "success" && rows > 0 {
                metrics.time_to_first_row.observe(duration.as_secs_f64());
            }
        }

        // OpenLineage: emit COMPLETE on success or FAIL on error. The
        // captured plan (Some when execute_query ran; None for DDL/DML branches
        // that did not populate it) flows into the observer so the lineage
        // extractor can build inputs/outputs/columnLineage facets.
        if ol_emit {
            if let Some(ref obs) = self.lineage {
                let ol_ended_at = chrono::Utc::now();
                let ol_duration = ol_ended_at
                    .signed_duration_since(ol_started_at)
                    .to_std()
                    .unwrap_or_default();
                match &result {
                    Ok(_) => {
                        obs.on_query_complete(sqe_lineage::QueryCompleteCtx {
                            run_id: query_id,
                            job_namespace: self.config.metrics.openlineage.job_namespace.clone(),
                            sql: sql.to_string(),
                            user: sqe_lineage::UserCtx {
                                username: session.user.username.clone(),
                                bearer: Some(session.access_token.clone()),
                            },
                            session_id: session.id.clone(),
                            started_at: ol_started_at,
                            ended_at: ol_ended_at,
                            duration: ol_duration,
                            statement_kind: kind_name.clone(),
                            rows_returned: rows,
                            plan: captured_plan.take(),
                        });
                    }
                    Err(e) => {
                        obs.on_query_fail(sqe_lineage::QueryFailCtx {
                            run_id: query_id,
                            job_namespace: self.config.metrics.openlineage.job_namespace.clone(),
                            sql: sql.to_string(),
                            user: sqe_lineage::UserCtx {
                                username: session.user.username.clone(),
                                bearer: Some(session.access_token.clone()),
                            },
                            session_id: session.id.clone(),
                            started_at: ol_started_at,
                            ended_at: ol_ended_at,
                            duration: ol_duration,
                            statement_kind: kind_name.clone(),
                            error_message: e.to_string(),
                            plan: captured_plan.take(),
                        });
                    }
                }
            }
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

        // Slow query warning
        let elapsed_secs = duration.as_secs();
        if self.config.query.slow_query_threshold_secs > 0
            && elapsed_secs >= self.config.query.slow_query_threshold_secs
        {
            warn!(
                query_id = %query_id,
                username = %session.user.username,
                elapsed_secs = elapsed_secs,
                sql_length = sql.len(),
                "Slow query detected"
            );
        }

        result
    }

    /// Plan and open a streaming SELECT without buffering the result.
    ///
    /// Returns `(schema, stream)`. Batches are yielded from DataFusion to
    /// the caller one at a time; the coordinator never accumulates the
    /// full result set. Memory-bound intermediate state (hash joins,
    /// sorts, group-bys) is handled by DataFusion's spill-aware operators
    /// against the shared [`FairSpillPool`], so arbitrarily large result
    /// sets can flow through without the coordinator being killed by the
    /// OS for exceeding its memory limit.
    ///
    /// The returned stream is wrapped in a [`crate::streaming::TrackedRecordBatchStream`]
    /// that records query tracker / metrics / audit state when the stream
    /// ends (clean EOF, error, or client drop). A held concurrency permit
    /// keeps the query counted against `max_concurrent_queries` for the
    /// full streaming lifetime.
    ///
    /// Only [`StatementKind::Query`] is supported. Any other statement
    /// returns [`SqeError::NotImplemented`]; the caller should fall back
    /// to [`Self::execute`] in that case. DML that materializes a result
    /// set (INSERT, MERGE, DELETE, UPDATE) still goes through the
    /// buffered path because the write handlers consume a `Vec<RecordBatch>`.
    #[tracing::instrument(
        skip(self, session, sql),
        fields(
            db.system.name = "sqe",
            username = %session.user.username,
            sql_length = sql.len(),
        ),
        name = "execute_stream",
    )]
    pub async fn execute_stream(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<(SchemaRef, SendableRecordBatchStream)> {
        // --- Admission control -------------------------------------------------
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        if let Some(ref metrics) = self.metrics {
            metrics.coordinator_memory_pressure.set(pressure.as_gauge());
            metrics
                .coordinator_memory_used_bytes
                .set(crate::memory::used_bytes(&self.runtime.memory_pool) as f64);
            metrics
                .coordinator_memory_limit_bytes
                .set(crate::memory::limit_bytes(&self.runtime.memory_pool) as f64);
        }
        if !pressure.admits_new_query() {
            warn!(
                pressure = %pressure,
                username = %session.user.username,
                "Rejecting streaming query due to memory pressure"
            );
            let sort_cols = extract_order_by_columns(sql);
            return Err(SqeError::Execution(
                adaptive_sort::format_pressure_rejection(&sort_cols, pressure),
            ));
        }

        // Acquire owned concurrency permit so it can be moved into the
        // stream wrapper and released when the client finishes draining.
        let permit = if let Some(ref sem) = self.query_semaphore {
            match Arc::clone(sem).try_acquire_owned() {
                Ok(p) => Some(p),
                Err(_) => {
                    return Err(SqeError::Execution(format!(
                        "Too many concurrent queries ({} active). Please retry later.",
                        self.config.query.max_concurrent_queries
                    )));
                }
            }
        } else {
            None
        };

        // --- Classify ---------------------------------------------------------
        // See execute() for why we pre-strip `FOR INCREMENTAL BETWEEN` before
        // handing SQL to sqlparser-rs.
        let (classify_sql, _incremental_specs) = sqe_sql::extract_incremental_spec(sql)?;
        // Also strip `FOR VERSION AS OF`. sqlparser-rs models
        // `FOR SYSTEM_TIME AS OF` natively but not VERSION; the time-travel
        // pre-parser handles VERSION later, but the classifier runs first
        // and would otherwise reject the query.
        let (classify_sql, _version_specs) =
            sqe_sql::extract_time_travel_spec(&classify_sql)?;
        // Rewrite Hive/Spark/Trino-style `PARTITIONED BY (...)` into
        // sqlparser-friendly `PARTITION BY (...)`. sqlparser's native
        // PARTITIONED BY expects column definitions; we want Iceberg
        // transforms (year/month/day/hour/bucket/truncate/identity)
        // which fit the BigQuery-style `PARTITION BY <expr>` shape.
        let classify_sql = sqe_sql::normalize_partitioned_by(&classify_sql);
        let kind = parse_and_classify(&classify_sql)?;
        let kind_name = kind.name().to_string();
        if !matches!(kind, StatementKind::Query(_)) {
            return Err(SqeError::NotImplemented(
                "execute_stream only supports SELECT queries; \
                 use execute() for DML and metadata statements"
                    .into(),
            ));
        }

        // Pre-flight: same unknown-catalog-qualifier check as execute().
        // See `execute()` for the rationale.
        if let Some(stmt) = kind.statement() {
            let qualifiers = sqe_sql::extract_catalog_qualifiers(stmt);
            if !qualifiers.is_empty() {
                let known = self.known_catalog_names();
                if let Some(unknown) =
                    qualifiers.iter().find(|q| !known.iter().any(|k| k == *q))
                {
                    return Err(SqeError::Catalog(format!(
                        "unknown catalog '{}' in 3-part identifier; configured \
                         catalogs are {:?}. Use TOML `[catalogs.<name>]` to declare \
                         additional catalogs, or `ATTACH` at runtime once that lands.",
                        unknown, known
                    )));
                }
            }
        }

        // --- Start tracker ----------------------------------------------------
        let start = std::time::Instant::now();
        let query_id = uuid::Uuid::now_v7();
        info!(
            query_id = %query_id,
            username = %session.user.username,
            sql_length = sql.len(),
            "Starting streaming query"
        );
        self.query_tracker.start(
            query_id,
            &session.user.username,
            session.source.as_deref(),
            sql,
            &session.id,
            None,
            session.user.roles.clone(),
        );

        // --- Plan + open DataFusion stream -----------------------------------
        match self.open_stream(session, sql, &query_id, start).await {
            Ok((schema, inner_stream, final_plan)) => {
                let finalizer = crate::streaming::StreamFinalizer {
                    tracker: Arc::clone(&self.query_tracker),
                    metrics: self.metrics.clone(),
                    audit: self.audit.clone(),
                    query_id,
                    username: session.user.username.clone(),
                    session_id: session.id.clone(),
                    sql: sql.to_string(),
                    kind_name,
                    plan: final_plan,
                    runtime: Arc::clone(&self.runtime),
                    start,
                    slow_query_threshold_secs: self.config.query.slow_query_threshold_secs,
                    sql_length: sql.len(),
                };

                let tracked = crate::streaming::TrackedRecordBatchStream::new(
                    inner_stream,
                    finalizer,
                    permit,
                );
                let boxed: SendableRecordBatchStream = Box::pin(tracked);
                Ok((schema, boxed))
            }
            Err(e) => {
                // Planning failure: mark failed and return. The permit
                // (held in this function's scope) drops here and the
                // semaphore slot is released.
                self.query_tracker.failed(&query_id, &e);
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .query_count
                        .with_label_values(&["error", &kind_name])
                        .inc();
                    metrics
                        .query_duration
                        .with_label_values(&[&kind_name])
                        .observe(start.elapsed().as_secs_f64());
                }
                Err(e)
            }
        }
    }

    /// Build the physical plan and open a DataFusion record-batch stream.
    ///
    /// This is the streaming counterpart to [`Self::execute_query`]: it
    /// reproduces the same planning, policy-enforcement, physical-plan
    /// building, star-schema reorder, adaptive-sort, and `try_distribute`
    /// steps, but returns the opened [`SendableRecordBatchStream`] plus
    /// the final plan instead of draining into a `Vec<RecordBatch>`.
    async fn open_stream(
        &self,
        session: &Session,
        sql: &str,
        query_id: &uuid::Uuid,
        start: std::time::Instant,
    ) -> sqe_core::Result<(
        SchemaRef,
        SendableRecordBatchStream,
        Arc<dyn ExecutionPlan>,
    )> {
        let (ctx, session_catalog) = self.create_session_context(session).await?;

        let sql = self.handle_incremental(sql, &ctx, &session_catalog).await?;
        let sql = self.handle_time_travel(&sql, &ctx, &session_catalog).await?;
        let sql = sql.as_str();

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;
        let plan = df.logical_plan().clone();
        let enforced_plan = self.policy_enforcer.evaluate(&session.user, plan).await?;
        debug!("Policy-enforced plan (streaming): {:?}", enforced_plan);

        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;
        let physical_plan = enforced_df
            .create_physical_plan()
            .await
            .map_err(|e| SqeError::Execution(format!("Physical plan creation failed: {e}")))?;

        // Star-schema join reorder
        let physical_plan = if self.config.query.star_schema_reorder {
            let rule = sqe_planner::StarSchemaReorderRule::new(
                self.config.query.star_schema_min_ratio,
            );
            match rule.optimize(
                physical_plan.clone(),
                &datafusion::config::ConfigOptions::new(),
            ) {
                Ok(optimized) => optimized,
                Err(e) => {
                    debug!(error = %e, "Star-schema join reorder failed, using original plan");
                    physical_plan
                }
            }
        } else {
            physical_plan
        };

        // Adaptive sort stripping
        let sort_mode = SortMode::parse(&self.config.query.sort_mode);
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        let (physical_plan, sort_decisions) = adaptive_sort::apply_adaptive_sort(
            physical_plan,
            sort_mode,
            pressure,
            self.metrics.as_ref(),
        );
        if let Some(warning) = adaptive_sort::format_sort_warning(&sort_decisions, sort_mode) {
            debug!(warning = %warning, "Adaptive sort stripping applied (streaming)");
        }

        // Distribute scan across workers if possible
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;

        // Planning complete — promote tracker to Running
        self.query_tracker
            .running(query_id, start.elapsed().as_millis() as u64);

        let schema = final_plan.schema();
        let stream = execute_stream(Arc::clone(&final_plan), ctx.task_ctx())
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

        Ok((schema, stream, final_plan))
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
        // Pre-strip FOR INCREMENTAL so sqlparser can tokenise the statement.
        let (classify_sql, _incremental_specs) = sqe_sql::extract_incremental_spec(sql)?;
        // Also strip `FOR VERSION AS OF`. sqlparser-rs models
        // `FOR SYSTEM_TIME AS OF` natively but not VERSION; the time-travel
        // pre-parser handles VERSION later, but the classifier runs first
        // and would otherwise reject the query.
        let (classify_sql, _version_specs) =
            sqe_sql::extract_time_travel_spec(&classify_sql)?;
        // Rewrite Hive/Spark/Trino-style `PARTITIONED BY (...)` into
        // sqlparser-friendly `PARTITION BY (...)`. sqlparser's native
        // PARTITIONED BY expects column definitions; we want Iceberg
        // transforms (year/month/day/hour/bucket/truncate/identity)
        // which fit the BigQuery-style `PARTITION BY <expr>` shape.
        let classify_sql = sqe_sql::normalize_partitioned_by(&classify_sql);
        let kind = parse_and_classify(&classify_sql)?;

        if matches!(kind, StatementKind::Query(_)) {
            let (ctx, session_catalog) = self.create_session_context(session).await?;
            // Register incremental providers so the planner can resolve the
            // tables with their augmented schemas. The FOR INCREMENTAL clause
            // must be stripped before handle_time_travel sees it because that
            // step parses the SQL with sqlparser.
            let sql_for_plan = self
                .handle_incremental(sql, &ctx, &session_catalog)
                .await?;
            let sql_for_plan = self
                .handle_time_travel(&sql_for_plan, &ctx, &session_catalog)
                .await?;
            let df = ctx
                .sql(&sql_for_plan)
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
    ///
    /// `plan_out` is an out-parameter that, when present, receives the
    /// post-policy logical plan as a [`sqe_lineage::PlanOrHint::Plan`] so the
    /// caller can pass it to the OpenLineage observer's complete/fail hook.
    /// Plan capture happens after policy enforcement so the emitted lineage
    /// reflects the user's *enforced* view, not the raw user query.
    #[tracing::instrument(skip(self, session, sql, query_id, plan_metrics, plan_out), fields(username = %session.user.username))]
    async fn execute_query(
        &self,
        session: &Session,
        sql: &str,
        query_id: &uuid::Uuid,
        plan_metrics: &Arc<Mutex<PlanMetrics>>,
        plan_out: &mut Option<sqe_lineage::PlanOrHint>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (ctx, session_catalog) = self.create_session_context(session).await?;

        // Pre-process incremental (CDC) first. The FOR INCREMENTAL BETWEEN
        // clause is not modelled by sqlparser-rs, so the downstream
        // handle_time_travel call (which parses SQL) would fail if we left it
        // in place. handle_incremental strips the clause text and registers
        // an IncrementalTableProvider per target table.
        let sql = self.handle_incremental(sql, &ctx, &session_catalog).await?;
        // Pre-process time travel: detect FOR SYSTEM_TIME AS OF, resolve
        // snapshot IDs, register snapshot-specific table providers, and
        // strip the temporal clause.
        let sql = self.handle_time_travel(&sql, &ctx, &session_catalog).await?;
        // Pre-process Trino-compat AST patterns DataFusion does not natively
        // recognize. Today this only rewrites `CAST(v AS JSON)` to
        // `to_json(v)`; the rewriter is a no-op when the input does not
        // contain `as json` (case-insensitive). Errors during parse fall
        // through as the original string so DataFusion produces its own
        // error message.
        let sql = sqe_sql::rewrite_trino_compat(&sql);
        let sql = sql.as_str();

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

        // Capture the enforced plan for OpenLineage extraction. Cloning into
        // a Box keeps the lineage path independent of DataFusion's
        // execute_logical_plan consumption pattern below.
        *plan_out = Some(sqe_lineage::PlanOrHint::Plan(Box::new(enforced_plan.clone())));

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

        // Apply star-schema join reordering: reorder inner equi-join chains so
        // small dimension tables are joined first and the large fact table is
        // probed last.
        let physical_plan = if self.config.query.star_schema_reorder {
            let rule = sqe_planner::StarSchemaReorderRule::new(
                self.config.query.star_schema_min_ratio,
            );
            match rule.optimize(physical_plan.clone(), &datafusion::config::ConfigOptions::new()) {
                Ok(optimized) => optimized,
                Err(e) => {
                    debug!(
                        error = %e,
                        "Star-schema join reorder failed, using original plan"
                    );
                    physical_plan
                }
            }
        } else {
            physical_plan
        };

        // Apply adaptive sort stripping based on sort_mode config and memory pressure.
        let sort_mode = SortMode::parse(&self.config.query.sort_mode);
        let pressure = crate::memory::check_pressure(&self.runtime.memory_pool);
        let (physical_plan, sort_decisions) = adaptive_sort::apply_adaptive_sort(
            physical_plan,
            sort_mode,
            pressure,
            self.metrics.as_ref(),
        );
        if let Some(warning) = adaptive_sort::format_sort_warning(&sort_decisions, sort_mode) {
            debug!(warning = %warning, "Adaptive sort stripping applied");
        }

        // Try to distribute scan work across workers
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;

        // Execute the (possibly distributed) plan.
        //
        // We drive the stream manually instead of using DataFusion's `collect()`
        // so the row-count limit can fire BEFORE the entire result set is
        // materialised. `collect()` buffers every batch into memory first and
        // only returns when the plan is exhausted. For an unbounded or
        // mis-shaped query (e.g. a 20M-row fact-to-dimension cross product) the
        // buffered `Vec<RecordBatch>` could grow to many GB and trigger an
        // OS-level OOM kill. Streaming and counting as we go lets the
        // coordinator reject oversized queries with a clean error and keeps the
        // process alive for other concurrent requests.
        //
        // Two caps apply:
        //   1. The user-configured `query.max_result_rows` (0 means no explicit
        //      cap, but see #2).
        //   2. A hard memory-derived ceiling: `memory_limit / 256` rows, assuming
        //      a conservative 256-byte average row size. This fires even when
        //      the config disables #1. The coordinator must never be one query
        //      away from the OS SIGKILLing the process for crossing the memory
        //      line, so we always enforce something lower than total memory.
        let output_schema = final_plan.schema();
        let configured_max = self.config.query.max_result_rows;
        let memory_ceiling: usize = {
            let bytes = crate::memory::limit_bytes(&self.runtime.memory_pool);
            if bytes == 0 { 0 } else { bytes / 256 }
        };
        let effective_max: usize = match (configured_max, memory_ceiling) {
            (0, 0) => 0, // neither cap configured. Falls back to OS behaviour.
            (0, m) => m,
            (c, 0) => c,
            (c, m) => c.min(m),
        };
        let mut stream = execute_stream(final_plan.clone(), ctx.task_ctx())
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut rows_so_far: usize = 0;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?
        {
            rows_so_far = rows_so_far.saturating_add(batch.num_rows());
            if effective_max > 0 && rows_so_far > effective_max {
                let reason = if configured_max > 0 && rows_so_far > configured_max {
                    format!("configured max_result_rows={configured_max}")
                } else {
                    format!(
                        "memory-derived ceiling={effective_max} (memory_limit / 256 bytes-per-row)"
                    )
                };
                return Err(SqeError::Execution(format!(
                    "Query result exceeds maximum allowed rows ({rows_so_far} > {effective_max}). \
                     Reason: {reason}. Use LIMIT to reduce output or raise the limit in config."
                )));
            }
            batches.push(batch);
        }

        // Aggregate spill metrics from the executed plan tree
        if let Some(ref metrics) = self.metrics {
            let (sort_spill_count, sort_spill_bytes, join_spill_count, join_spill_bytes) =
                aggregate_spill_metrics(&final_plan);
            if sort_spill_count > 0 {
                metrics.sort_spill_count.inc_by(sort_spill_count as f64);
                metrics.sort_spill_bytes.inc_by(sort_spill_bytes as f64);
                debug!(sort_spill_count, sort_spill_bytes, "Sort spill detected");
            }
            if join_spill_count > 0 {
                metrics.join_spill_count.inc_by(join_spill_count as f64);
                metrics.join_spill_bytes.inc_by(join_spill_bytes as f64);
                debug!(join_spill_count, join_spill_bytes, "Join spill detected");
            }
        }

        // Extract per-query resource metrics from the executed plan tree and
        // store them so that the caller (execute()) can record them on the
        // QueryTracker when the query completes.
        {
            let mut extracted = extract_plan_metrics(&final_plan);
            // Snapshot the memory pool usage as a best-effort proxy for peak
            // memory. This is the pool-wide value (shared across concurrent
            // queries), so it overestimates per-query usage, but it's the best
            // signal available without per-query reservation tracking.
            extracted.peak_memory_bytes =
                crate::memory::used_bytes(&self.runtime.memory_pool) as u64;
            if let Ok(mut pm) = plan_metrics.lock() {
                *pm = extracted;
            }
        }

        // Ensure we always return at least one batch so callers can infer the
        // output schema (e.g. CTAS with WHERE false that returns zero rows).
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(output_schema));
        }

        // `rows_so_far` was tracked during streaming above, but we recompute
        // here because the empty-batch fallback above may have appended a
        // zero-row schema batch.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            batch_count = batches.len(),
            total_rows = total_rows,
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
    /// - The scan is below the configured file-count threshold (`distribution_file_threshold`)
    /// - The estimated scan size is below the configured byte threshold (`distribution_threshold`)
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

        let iceberg_scan = match scan_node.as_any().downcast_ref::<IcebergScanExec>() {
            Some(s) => s,
            None => {
                tracing::warn!("find_iceberg_scan returned unexpected node type, falling back to local");
                return plan;
            }
        };

        // 4. Get data file paths and sizes from the scan manifest metadata
        let file_info = match iceberg_scan.data_file_info().await {
            Ok(info) => info,
            Err(e) => {
                warn!(error = %e, "Failed to list data files for distribution, executing locally");
                return plan;
            }
        };

        let total_files = file_info.len();
        if total_files == 0 {
            debug!("No data files to distribute, executing locally");
            return plan;
        }

        // 5. Check if the scan is large enough to benefit from distribution.
        // Use the configured file-count threshold as a fast proxy (no file size
        // metadata needed at this point).
        let file_threshold = self.config.query.distribution_file_threshold;
        if file_threshold > 0 && total_files < file_threshold {
            debug!(
                total_files,
                threshold = file_threshold,
                "Scan below distribution file threshold — executing locally"
            );
            if let Some(ref metrics) = self.metrics {
                metrics.scheduler_decisions.with_label_values(&["local"]).inc();
            }
            return plan;
        }

        // Also check the byte-size threshold using real sizes from the manifest.
        let distribution_threshold = sqe_core::parse_memory_limit(
            &self.config.query.distribution_threshold
        ).unwrap_or(128 * 1024 * 1024);

        if distribution_threshold > 0 {
            let total_bytes: u64 = file_info.iter().map(|(_, size)| size).sum();
            if total_bytes < distribution_threshold as u64 {
                debug!(
                    total_bytes,
                    threshold = distribution_threshold,
                    total_files,
                    "Scan below distribution byte threshold — executing locally"
                );
                if let Some(ref metrics) = self.metrics {
                    metrics.scheduler_decisions.with_label_values(&["local"]).inc();
                }
                return plan;
            }
        }

        // 6. Check if there are enough files to justify distribution
        let num_workers = healthy.len();
        if total_files < num_workers {
            debug!(
                total_files,
                num_workers,
                "Fewer files than workers, executing locally"
            );
            if let Some(ref metrics) = self.metrics {
                metrics.scheduler_decisions.with_label_values(&["local"]).inc();
            }
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

        // 7. Split (path, size) pairs into size-balanced bins using bin-packing.
        // target_size_bytes: read from config or fall back to 256 MiB.
        // max_bins: allow up to 3 tasks per worker so work is evenly spread
        // even when file sizes vary widely.
        let target_size_bytes = sqe_core::parse_memory_limit(
            &self.config.query.target_task_size
        ).unwrap_or(256 * 1024 * 1024) as u64;
        let max_bins = num_workers * 3;
        let file_groups = sqe_planner::bin_pack_files(file_info, target_size_bytes, max_bins);

        // 8. Build ScanTasks — paths and sizes are parallel vecs within each group
        let storage = &self.config.storage;
        let scan_tasks: Vec<sqe_planner::ScanTask> = file_groups
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|group| {
                let (data_file_paths, file_sizes_bytes): (Vec<String>, Vec<u64>) =
                    group.into_iter().unzip();
                sqe_planner::ScanTask {
                    fragment_id: uuid::Uuid::now_v7().to_string(),
                    data_file_paths,
                    file_sizes_bytes,
                    projected_columns: projected_cols.clone(),
                    s3_endpoint: storage.s3_endpoint.clone(),
                    s3_region: storage.s3_region.clone(),
                    s3_access_key: storage.s3_access_key.clone(),
                    s3_secret_key: storage.s3_secret_key.clone(),
                    s3_session_token: String::new(),
                    s3_path_style: storage.s3_path_style,
                    s3_allow_http: storage.s3_endpoint.starts_with("http://"),
                }
            })
            .collect();

        if scan_tasks.is_empty() {
            debug!("No non-empty scan tasks after splitting, executing locally");
            if let Some(ref metrics) = self.metrics {
                metrics.scheduler_decisions.with_label_values(&["local"]).inc();
            }
            return plan;
        }

        if let Some(ref metrics) = self.metrics {
            metrics.scheduler_decisions.with_label_values(&["distributed"]).inc();
            metrics.scheduler_task_count.observe(scan_tasks.len() as f64);
            for task in &scan_tasks {
                let size_mb = task.file_sizes_bytes.iter().sum::<u64>() as f64 / (1024.0 * 1024.0);
                metrics.scheduler_task_size_mb.observe(size_mb);
            }
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

        // 11. Build fragment callback for progress tracking and straggler detection
        let tracker = self.query_tracker.clone();
        let qid = *query_id;
        let callback_metrics = self.metrics.clone();
        let callback: crate::distributed_scan::FragmentCallback =
            Arc::new(move |task_id, success, elapsed_ms, rows| {
                let state = if success {
                    FragmentState::Finished
                } else {
                    FragmentState::Failed
                };
                tracker.update_fragment(&qid, task_id, state, elapsed_ms, rows);

                // Once all fragments are done, emit a summary and check for stragglers.
                if let Some(timings) = tracker.all_fragments_done(&qid) {
                    let durations: Vec<u64> = timings.iter().map(|(_, _, ms)| *ms).collect();
                    let total_ms: u64 = durations.iter().sum();
                    let max_ms = *durations.iter().max().unwrap_or(&0);
                    let min_ms = *durations.iter().min().unwrap_or(&0);

                    tracing::info!(
                        query_id = %qid,
                        fragment_count = durations.len(),
                        total_ms,
                        max_ms,
                        min_ms,
                        "Distributed scan complete"
                    );

                    // Straggler detection: warn when any fragment took >3× the median.
                    if durations.len() >= 2 {
                        let mut sorted = durations.clone();
                        sorted.sort_unstable();
                        let median = sorted[sorted.len() / 2];
                        let threshold = median.saturating_mul(3);

                        if median > 0 {
                            for (i, (frag_id, worker_url, duration)) in timings.iter().enumerate() {
                                if *duration > threshold {
                                    let ratio = duration / median.max(1);
                                    tracing::warn!(
                                        query_id = %qid,
                                        fragment_index = i,
                                        fragment_id = %frag_id,
                                        duration_ms = duration,
                                        median_ms = median,
                                        ratio,
                                        worker = %worker_url,
                                        "Straggler detected: fragment took {}x the median",
                                        ratio,
                                    );
                                    if let Some(ref metrics) = callback_metrics {
                                        metrics.scheduler_stragglers.inc();
                                    }
                                }
                            }
                        }
                    }
                }
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

        let dist_scan: Arc<dyn ExecutionPlan> = Arc::new(exec);

        // Replace the IcebergScanExec leaf within the plan tree, keeping
        // all parent nodes (filter, aggregate, sort, projection) intact.
        replace_scan_in_plan(&plan, &scan_node, dist_scan)
    }

    /// Create a DataFusion SessionContext with the user's Polaris catalog registered.
    ///
    /// Delegates to [`crate::session_context::create_session_context`] which
    /// owns the full setup logic, keeping this file focused on query routing.
    async fn create_session_context(
        &self,
        session: &Session,
    ) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
        crate::session_context::create_session_context(
            &self.config,
            session,
            self.policy_store.as_ref(),
            &self.query_tracker,
            Some(&self.runtime),
            self.metrics.as_ref(),
            self.table_cache.as_ref(),
            &self.runtime_catalogs,
        )
        .await
    }

    /// Handle SHOW CREATE TABLE by querying information_schema and reconstructing DDL.
    async fn handle_show_create_table(
        &self,
        _session: &Session,
        stmt: &Statement,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use arrow_array::StringArray as ArrowStringArray;

        // Extract object name from the ShowCreate statement
        let table_name = match stmt {
            Statement::ShowCreate { obj_name, .. } => obj_name.to_string(),
            _ => {
                return Err(SqeError::Execution(
                    "Expected ShowCreate statement".into(),
                ))
            }
        };

        // Query information_schema.columns for the table's column definitions.
        // Use only the last part of a qualified name for the WHERE filter.
        let bare_name = table_name.split('.').next_back().unwrap_or(&table_name);
        let col_sql = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_name = '{bare_name}' \
             ORDER BY ordinal_position"
        );

        let df = ctx.sql(&col_sql).await.map_err(|e| {
            SqeError::Execution(format!("Failed to query column metadata: {e}"))
        })?;
        let batches = df.collect().await.map_err(|e| {
            SqeError::Execution(format!("Failed to collect column metadata: {e}"))
        })?;

        // Reconstruct CREATE TABLE DDL
        let mut ddl = format!("CREATE TABLE {table_name} (\n");
        let mut first = true;
        for batch in &batches {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| SqeError::Execution("Unexpected column_name type".into()))?;
            let types = batch
                .column(1)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| SqeError::Execution("Unexpected data_type type".into()))?;
            let nullables = batch
                .column(2)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| SqeError::Execution("Unexpected is_nullable type".into()))?;

            for i in 0..batch.num_rows() {
                if !first {
                    ddl.push_str(",\n");
                }
                first = false;
                let col_name = names.value(i);
                let col_type = types.value(i);
                let nullable = nullables.value(i);
                ddl.push_str(&format!("   {col_name} {col_type}"));
                if nullable == "NO" {
                    ddl.push_str(" NOT NULL");
                }
            }
        }
        ddl.push_str("\n)");

        // Append TBLPROPERTIES (k = 'v', ...) for any user-set properties.
        // The Iceberg metadata exposes the property map; we filter out the
        // reserved keys that Iceberg manages internally so the round-trip
        // matches what the user actually set in CREATE TABLE.
        let parts: Vec<&str> = table_name.split('.').collect();
        let (ns, bare) = match parts.len() {
            1 => ("default", parts[0]),
            2 => (parts[0], parts[1]),
            3 => (parts[1], parts[2]),
            _ => ("default", parts[parts.len() - 1]),
        };
        let ns_ident = iceberg::NamespaceIdent::new(ns.to_string());
        let table_ident = iceberg::TableIdent::new(ns_ident, bare.to_string());
        if let Ok(table) = session_catalog.load_table(&table_ident).await {
            let props = table.metadata().properties();
            // Iceberg-internal property keys we never want to surface.
            const SUPPRESSED_PREFIXES: &[&str] = &[
                "current-",
                "snapshot-count",
                "last-",
                "uuid",
                "format-version",
                "default-",
                "owner",
            ];
            let mut user_props: Vec<(&String, &String)> = props
                .iter()
                .filter(|(k, _)| {
                    !SUPPRESSED_PREFIXES
                        .iter()
                        .any(|pre| k.starts_with(pre))
                })
                .collect();
            user_props.sort_by(|a, b| a.0.cmp(b.0));
            if !user_props.is_empty() {
                ddl.push_str("\nTBLPROPERTIES (\n");
                let last = user_props.len() - 1;
                for (i, (k, v)) in user_props.iter().enumerate() {
                    ddl.push_str(&format!("   '{k}' = '{v}'"));
                    if i != last {
                        ddl.push(',');
                    }
                    ddl.push('\n');
                }
                ddl.push(')');
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "Create Table",
            DataType::Utf8,
            false,
        )]));
        let result = RecordBatch::try_new(
            schema,
            vec![Arc::new(ArrowStringArray::from(vec![ddl.as_str()])) as ArrayRef],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        Ok(vec![result])
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
    ///
    /// Reuses the cached `Arc<SessionCatalog>` from `create_session_context`
    /// instead of rebuilding the wrapping on every call (issue #15).
    async fn handle_show_schemas(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let session_catalog = self.session_catalog(session).await?;

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

    /// Handle `SHOW COLUMNS FROM ns.table` (Trino syntax) by translating
    /// into a query against `information_schema.columns`. Same pattern as
    /// `handle_show_create_table`'s column lookup.
    ///
    /// Trino's full `SHOW COLUMNS` output has four columns (Column, Type,
    /// Extra, Comment); SQE returns (column_name, data_type, is_nullable),
    /// which is the subset dbt and BI clients use for schema inspection.
    async fn handle_show_columns(
        &self,
        session: &Session,
        show_options: &sqlparser::ast::ShowStatementOptions,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // sqlparser groups the table identifier under
        // `show_in.parent_name`. Both `show_in` and `parent_name` are
        // optional in the AST; missing either means the query did not
        // name a table to inspect.
        let table_name = show_options
            .show_in
            .as_ref()
            .and_then(|si| si.parent_name.as_ref())
            .map(|n| n.to_string())
            .ok_or_else(|| {
                SqeError::Execution(
                    "SHOW COLUMNS requires `FROM <table>` (or `IN <table>`)".to_string(),
                )
            })?;

        // Use only the bare table name for the WHERE filter; information_schema
        // queries collapse to the leaf table name.
        let bare_name = table_name.split('.').next_back().unwrap_or(&table_name);
        let col_sql = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_name = '{bare_name}' \
             ORDER BY ordinal_position"
        );

        let (ctx, _) = self.create_session_context(session).await?;
        let df = ctx.sql(&col_sql).await.map_err(|e| {
            SqeError::Execution(format!(
                "SHOW COLUMNS failed to query information_schema: {e}"
            ))
        })?;
        df.collect().await.map_err(|e| {
            SqeError::Execution(format!(
                "SHOW COLUMNS failed to collect column metadata: {e}"
            ))
        })
    }

    /// Handle SHOW TABLES by listing tables in a namespace from the Polaris catalog.
    ///
    /// Reuses the cached `Arc<SessionCatalog>` from `create_session_context`
    /// instead of re-running `SessionCatalog::for_session` (which re-hashes
    /// the bearer and rebuilds the props map on every call — issue #15).
    async fn handle_show_tables(
        &self,
        session: &Session,
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let session_catalog = self.session_catalog(session).await?;

        // If a filter is provided, use it as the namespace; otherwise list all namespaces
        let ns_name = parse_show_tables_namespace(filter);
        let namespaces = match ns_name {
            None => session_catalog.list_namespaces().await?,
            Some(name) => vec![iceberg::NamespaceIdent::new(name)],
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

    /// Pre-process SQL for time travel: detect `FOR SYSTEM_TIME AS OF`, resolve
    /// timestamps to snapshot IDs, register snapshot-specific table providers,
    /// and return the rewritten SQL with the temporal clause stripped.
    ///
    /// Also handles `FOR VERSION AS OF <snapshot_id_or_ref>` via a pre-scan
    /// because sqlparser-rs 0.53 does not model that variant.
    ///
    /// When no time travel is found the original SQL is returned unchanged.
    async fn handle_time_travel(
        &self,
        sql: &str,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<String> {
        use sqlparser::ast::SetExpr;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // First, pre-scan and resolve FOR VERSION AS OF. This path hides the
        // clause from sqlparser and registers snapshot-pinned providers
        // under aliases in the writable `datafusion.public` schema. The
        // SQL is rewritten so the original table reference (which lives
        // in the read-only Iceberg schema provider) is replaced with the
        // alias.
        let (mut rewritten_for_version, version_specs) =
            sqe_sql::extract_time_travel_spec(sql)?;
        let mut version_resolved = !version_specs.is_empty();
        if version_resolved {
            for spec in &version_specs {
                let alias = Self::apply_version_spec(spec, ctx, session_catalog).await?;
                rewritten_for_version =
                    replace_table_reference(&rewritten_for_version, &spec.table, &alias);
            }
        }
        let sql_for_ast = if version_resolved {
            rewritten_for_version
        } else {
            sql.to_string()
        };

        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, &sql_for_ast)
            .map_err(|e| SqeError::Execution(format!("Parse error in time travel detection: {e}")))?;

        if statements.is_empty() {
            return Ok(sql_for_ast);
        }

        let stmt = &mut statements[0];
        let mut found_time_travel = false;

        if let sqlparser::ast::Statement::Query(ref mut query) = stmt {
            if let SetExpr::Select(ref mut select) = *query.body {
                for twj in &mut select.from {
                    if Self::process_time_travel_table_factor(
                        &mut twj.relation,
                        ctx,
                        session_catalog,
                    ).await? {
                        found_time_travel = true;
                    }
                    for join in &mut twj.joins {
                        if Self::process_time_travel_table_factor(
                            &mut join.relation,
                            ctx,
                            session_catalog,
                        ).await? {
                            found_time_travel = true;
                        }
                    }
                }
            }
        }

        if found_time_travel {
            Ok(statements[0].to_string())
        } else if version_resolved {
            Ok(sql_for_ast)
        } else {
            version_resolved = false;
            let _ = version_resolved; // silence unused warning if flow changes
            Ok(sql.to_string())
        }
    }

    /// Resolve a parsed `TimeTravelSpec` (from `FOR VERSION AS OF`) against
    /// table metadata and register a snapshot-pinned provider.
    ///
    /// - `VersionRef::SnapshotId(id)` pins to that specific snapshot.
    /// - `VersionRef::Named(name)` looks up `name` in the table's refs. If both
    ///   a tag and a branch exist with the same name, the tag wins and a
    ///   warning is logged.
    async fn apply_version_spec(
        spec: &sqe_sql::TimeTravelSpec,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<String> {
        use iceberg::spec::SnapshotRetention;
        use sqe_sql::VersionRef;

        let parts: Vec<&str> = spec.table.split('.').collect();
        let (namespace, bare_table) = match parts.len() {
            1 => ("default", parts[0]),
            2 => (parts[0], parts[1]),
            3 => (parts[1], parts[2]),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Unsupported table name format for time travel: {}",
                    spec.table
                )));
            }
        };

        let ns_ident = iceberg::NamespaceIdent::new(namespace.to_string());
        let table_ident = iceberg::TableIdent::new(ns_ident, bare_table.to_string());
        let iceberg_table = session_catalog.load_table(&table_ident).await?;
        let metadata = iceberg_table.metadata();

        let snapshot_id = match &spec.version {
            VersionRef::SnapshotId(id) => {
                if metadata.snapshot_by_id(*id).is_none() {
                    return Err(SqeError::Execution(format!(
                        "FOR VERSION AS OF {id}: snapshot id not found on {}",
                        spec.table
                    )));
                }
                *id
            }
            VersionRef::Named(name) => {
                match metadata.reference_by_name(name) {
                    None => {
                        return Err(SqeError::Execution(format!(
                            "FOR VERSION AS OF '{name}': no branch or tag with that name on {}",
                            spec.table
                        )));
                    }
                    Some(reference) => {
                        // If another ref with the same name exists with a different
                        // retention kind, prefer the tag. Iceberg uses a single
                        // namespace for branch and tag names, so this only applies
                        // in migration scenarios where someone creates a tag that
                        // shadows a branch (or vice versa).
                        if matches!(reference.retention, SnapshotRetention::Branch { .. }) {
                            tracing::debug!(
                                table = %spec.table,
                                ref_name = %name,
                                snapshot_id = reference.snapshot_id,
                                "FOR VERSION AS OF resolved to branch"
                            );
                        } else {
                            tracing::debug!(
                                table = %spec.table,
                                ref_name = %name,
                                snapshot_id = reference.snapshot_id,
                                "FOR VERSION AS OF resolved to tag"
                            );
                        }
                        reference.snapshot_id
                    }
                }
            }
        };

        tracing::info!(
            table = %spec.table,
            snapshot_id,
            "Time travel (FOR VERSION AS OF): pinned snapshot"
        );

        let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
            .await?
            .with_snapshot_id(snapshot_id);

        // Register under a unique alias in `datafusion.public` (a
        // MemoryCatalog schema that supports dynamic registration).
        // The Iceberg schema provider that owns the original table name
        // is read-only.
        let alias = format!(
            "__sqe_ver_{}_{}",
            bare_table,
            uuid::Uuid::now_v7().simple()
        );
        let qualified = format!("datafusion.public.{alias}");
        ctx.register_table(qualified.as_str(), Arc::new(provider))
            .map_err(|e| SqeError::Execution(format!(
                "Failed to register time-travel provider for {bare_table}: {e}"
            )))?;

        Ok(qualified)
    }

    /// Process a single `TableFactor` for `FOR SYSTEM_TIME AS OF`.
    ///
    /// When a time travel clause is found:
    /// 1. Resolves the timestamp to a snapshot ID
    /// 2. Loads the Iceberg table and creates a snapshot-pinned provider
    /// 3. Registers it in the DataFusion context
    /// 4. Strips the `version` field so DataFusion doesn't see it
    ///
    /// Returns `true` if a time travel clause was processed.
    async fn process_time_travel_table_factor(
        relation: &mut TableFactor,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<bool> {
        use sqlparser::ast::TableVersion;

        if let TableFactor::Table { ref name, ref mut version, .. } = relation {
            if let Some(TableVersion::ForSystemTimeAsOf(ref expr)) = version {
                let table_name = name.to_string();
                let timestamp_ms = resolve_timestamp_expr(expr)?;

                // Extract namespace and table from the (possibly qualified) name
                let parts: Vec<&str> = table_name.split('.').collect();
                let (namespace, bare_table) = match parts.len() {
                    1 => ("default", parts[0]),
                    2 => (parts[0], parts[1]),
                    3 => (parts[1], parts[2]), // catalog.schema.table — skip catalog
                    _ => return Err(SqeError::Execution(format!(
                        "Unsupported table name format for time travel: {table_name}"
                    ))),
                };

                let ns_ident = iceberg::NamespaceIdent::new(namespace.to_string());
                let table_ident = iceberg::TableIdent::new(ns_ident, bare_table.to_string());
                let iceberg_table = session_catalog.load_table(&table_ident).await?;

                let snapshot_id = find_snapshot_at_timestamp(iceberg_table.metadata(), timestamp_ms)?;

                tracing::info!(
                    table = %table_name,
                    timestamp_ms,
                    snapshot_id,
                    "Time travel: resolved timestamp to snapshot"
                );

                // Build a snapshot-pinned SqeTableProvider and register it
                let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
                    .await?
                    .with_snapshot_id(snapshot_id);

                ctx.register_table(bare_table, Arc::new(provider))
                    .map_err(|e| SqeError::Execution(format!(
                        "Failed to register time-travel provider for {bare_table}: {e}"
                    )))?;

                // Strip the temporal clause so DataFusion doesn't reject it
                *version = None;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Pre-process SQL for CDC incremental scans. Detect
    /// `FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`, resolve the range
    /// against table metadata, plan the file list with delete reconciliation,
    /// and register a transient `IncrementalTableProvider` for each target
    /// table. Returns the SQL with the clause stripped and each target table
    /// reference rewritten to a qualified `datafusion.public.<alias>` name so
    /// the planner resolves to the freshly registered provider.
    ///
    /// When no incremental clause is found, the original SQL is returned
    /// unchanged.
    async fn handle_incremental(
        &self,
        sql: &str,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<String> {
        let (mut rewritten, specs) = sqe_sql::extract_incremental_spec(sql)?;
        if specs.is_empty() {
            return Ok(sql.to_string());
        }
        for spec in &specs {
            let alias = Self::apply_incremental_spec(spec, ctx, session_catalog).await?;
            // Rewrite the table reference (e.g. `ns.events`) to the qualified
            // MemoryCatalog path (`datafusion.public.<alias>`). The replacement
            // is whole-token so `events.id` inside a SELECT list is preserved
            // because it doesn't match the qualified form.
            rewritten = replace_table_reference(&rewritten, &spec.table, &alias);
        }
        tracing::info!(
            specs = specs.len(),
            "Handled FOR INCREMENTAL BETWEEN SNAPSHOT clauses"
        );
        Ok(rewritten)
    }

    /// Resolve one `IncrementalSpec` against the catalog, register an
    /// `IncrementalTableProvider` in the in-memory `datafusion.public` schema,
    /// and return the fully qualified name the caller should substitute into
    /// the SQL.
    async fn apply_incremental_spec(
        spec: &sqe_sql::IncrementalSpec,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<String> {
        use iceberg::arrow::schema_to_arrow_schema;
        use std::collections::HashMap;

        let parts: Vec<&str> = spec.table.split('.').collect();
        let (namespace, bare_table) = match parts.len() {
            1 => ("default", parts[0]),
            2 => (parts[0], parts[1]),
            3 => (parts[1], parts[2]),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Unsupported table name format for incremental scan: {}",
                    spec.table
                )));
            }
        };

        let ns_ident = iceberg::NamespaceIdent::new(namespace.to_string());
        let table_ident = iceberg::TableIdent::new(ns_ident, bare_table.to_string());
        let iceberg_table = session_catalog.load_table(&table_ident).await?;

        // Plan the range. `plan_incremental` also calls `resolve_range`
        // internally, so range validation happens once.
        let mut plan = sqe_catalog::incremental_scan::plan_incremental(
            &iceberg_table,
            spec.start,
            spec.end,
        )
        .await?;

        // Reconcile in-range deletes. Without a referenced-data-file map
        // (position deletes currently do not expose one through
        // `plan_incremental`), we default every delete file to "no reference"
        // which keeps equality deletes intact. This is the same defensive
        // default the helper uses in its unit tests.
        let empty_refs: HashMap<String, Option<String>> = HashMap::new();
        let kept_deletes =
            sqe_catalog::incremental_scan::reconcile_in_range_deletes(
                &plan.data_files,
                std::mem::take(&mut plan.delete_files),
                &empty_refs,
            );
        plan.delete_files = kept_deletes;

        tracing::info!(
            table = %spec.table,
            start = spec.start,
            end = spec.end,
            snapshots = plan.snapshots_in_range.len(),
            data_files = plan.data_files.len(),
            delete_files = plan.delete_files.len(),
            "Incremental scan: planned range"
        );

        let base_schema = schema_to_arrow_schema(iceberg_table.metadata().current_schema())
            .map_err(|e| {
                SqeError::Catalog(format!(
                    "Failed to convert Iceberg schema to Arrow for {}: {e}",
                    spec.table
                ))
            })?;

        let file_io = iceberg_table.file_io().clone();
        let provider =
            sqe_catalog::incremental_provider::IncrementalTableProvider::new(
                Arc::new(base_schema),
                plan,
                Some(file_io),
            );

        // Register under a unique alias in the `datafusion.public` schema,
        // which is a MemoryCatalogProvider and supports dynamic registration.
        // The alias embeds a short uuid to avoid collisions when the same
        // table is referenced multiple times in one statement.
        let alias = format!(
            "__sqe_incr_{}_{}",
            bare_table,
            uuid::Uuid::now_v7().simple()
        );
        let qualified = format!("datafusion.public.{alias}");
        ctx.register_table(qualified.as_str(), Arc::new(provider))
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to register incremental provider for {bare_table}: {e}"
                ))
            })?;

        Ok(qualified)
    }

    // ── Access control handlers ──────────────────────────────────────────

    /// Return the grant backend or a `NotImplemented` error.
    fn require_grant_backend(&self) -> sqe_core::Result<&dyn GrantBackend> {
        self.grant_backend
            .as_deref()
            .ok_or_else(|| {
                SqeError::NotImplemented(
                    "Access control is not configured. Set [access_control] backend and url in the config."
                        .to_string(),
                )
            })
    }

    /// Extract a `GrantStatement` from a sqlparser `Statement::Grant` or `Statement::Revoke`.
    fn extract_grant_statement(stmt: &Statement) -> sqe_core::Result<GrantStatement> {
        let (privileges, objects, grantees) = match stmt {
            Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } => (privileges, objects, grantees),
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => (privileges, objects, grantees),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected GRANT/REVOKE statement, got: {other}"
                )));
            }
        };

        let privilege = format!("{privileges}");

        let (catalog, namespace, table) = match objects {
            sqlparser::ast::GrantObjects::Tables(tables) if !tables.is_empty() => {
                let name = &tables[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, None, Some(parts[0].clone())),
                    2 => (None, Some(parts[0].clone()), Some(parts[1].clone())),
                    3 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some(parts[2].clone()),
                    ),
                    _ => (None, None, Some(name.to_string())),
                }
            }
            sqlparser::ast::GrantObjects::Schemas(schemas) if !schemas.is_empty() => {
                let name = &schemas[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            sqlparser::ast::GrantObjects::AllTablesInSchema { schemas }
                if !schemas.is_empty() =>
            {
                let name = &schemas[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            _ => (None, None, None),
        };

        let raw_grantee = grantees.first().ok_or_else(|| {
            SqeError::Execution("GRANT/REVOKE requires at least one grantee".to_string())
        })?;

        // Extract the raw grantee name without surrounding quotes.
        // GranteeName::ObjectName.to_string() would include quotes for quoted identifiers
        // such as `TO ROLE "analysts"`. We want the bare value instead.
        // In sqlparser 0.54, ObjectName is Vec<Ident>; each Ident.value is the raw string.
        let grantee_name = match raw_grantee.name.as_ref() {
            Some(sqlparser::ast::GranteeName::ObjectName(obj)) => obj
                .0
                .iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
            Some(other) => other.to_string(),
            None => String::new(),
        };

        let grantee = match &raw_grantee.grantee_type {
            sqlparser::ast::GranteesType::User => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::None => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::Role => Grantee::Role(grantee_name),
            sqlparser::ast::GranteesType::Group => Grantee::Group(grantee_name),
            sqlparser::ast::GranteesType::DatabaseRole => Grantee::Role(grantee_name),
            other => {
                return Err(SqeError::NotImplemented(format!(
                    "Unsupported grantee type: {other:?}. Use USER, ROLE, or GROUP"
                )));
            }
        };

        Ok(GrantStatement {
            privilege,
            catalog,
            namespace,
            table,
            grantee,
        })
    }

    /// Handle GRANT by delegating to the configured grant backend.
    async fn handle_grant(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        backend.grant(&session.access_token, &grant_stmt).await?;
        Ok(vec![])
    }

    /// Handle REVOKE by delegating to the configured grant backend.
    async fn handle_revoke(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        let revoke_stmt = RevokeStatement {
            privilege: grant_stmt.privilege,
            catalog: grant_stmt.catalog,
            namespace: grant_stmt.namespace,
            table: grant_stmt.table,
            grantee: grant_stmt.grantee,
        };
        backend.revoke(&session.access_token, &revoke_stmt).await?;
        Ok(vec![])
    }

    /// Handle SHOW GRANTS by delegating to the configured grant backend.
    async fn handle_show_grants(
        &self,
        session: &Session,
        target: &sqe_sql::ShowGrantsTarget,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;

        let filter = match target {
            sqe_sql::ShowGrantsTarget::OnResource {
                catalog,
                namespace,
                table,
            } => GrantFilter::OnResource {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
            },
            sqe_sql::ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            } => {
                let grantee = match grantee_type.to_uppercase().as_str() {
                    "ROLE" => Grantee::Role(grantee_name.clone()),
                    "GROUP" => Grantee::Group(grantee_name.clone()),
                    _ => Grantee::User(grantee_name.clone()),
                };
                GrantFilter::ToGrantee(grantee)
            }
        };

        let entries = backend.show_grants(&session.access_token, &filter).await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Handle SHOW EFFECTIVE GRANTS by delegating to the configured grant backend.
    async fn handle_show_effective_grants(
        &self,
        session: &Session,
        user: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let entries = backend.show_effective(&session.access_token, user).await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Handle CHECK ACCESS by delegating to the configured grant backend.
    async fn handle_check_access(
        &self,
        session: &Session,
        params: &sqe_sql::CheckAccessParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;

        let check = AccessCheck {
            user: params.user.clone(),
            privilege: params.privilege.clone(),
            catalog: params.catalog.clone(),
            namespace: params.namespace.clone(),
            table: params.table.clone(),
        };

        let resp = backend.check_access(&session.access_token, &check).await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("allowed", DataType::Boolean, false),
            Field::new("reason", DataType::Utf8, true),
        ]));

        let allowed_array: ArrayRef = Arc::new(BooleanArray::from(vec![resp.allowed]));
        let mut reason_builder = StringBuilder::new();
        match resp.reason {
            Some(ref r) => reason_builder.append_value(r),
            None => reason_builder.append_null(),
        }
        let reason_array: ArrayRef = Arc::new(reason_builder.finish());

        let batch = RecordBatch::try_new(schema, vec![allowed_array, reason_array])
            .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Convert a list of `GrantEntry` values into a `RecordBatch` for the client.
    fn grants_to_record_batch(
        entries: &[sqe_policy::grants::GrantEntry],
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("privilege", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("grantee_type", DataType::Utf8, false),
            Field::new("grantee_name", DataType::Utf8, false),
            Field::new("effect", DataType::Utf8, false),
            Field::new("granted_by", DataType::Utf8, true),
            Field::new("granted_at", DataType::Utf8, true),
        ]));

        let mut priv_builder = StringBuilder::new();
        let mut resource_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut effect_builder = StringBuilder::new();
        let mut by_builder = StringBuilder::new();
        let mut at_builder = StringBuilder::new();

        for entry in entries {
            priv_builder.append_value(&entry.privilege);
            resource_builder.append_value(&entry.resource);
            type_builder.append_value(&entry.grantee_type);
            name_builder.append_value(&entry.grantee_name);
            effect_builder.append_value(&entry.effect);
            match entry.granted_by {
                Some(ref v) => by_builder.append_value(v),
                None => by_builder.append_null(),
            }
            match entry.granted_at {
                Some(ref v) => at_builder.append_value(v),
                None => at_builder.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(priv_builder.finish()) as ArrayRef,
                Arc::new(resource_builder.finish()) as ArrayRef,
                Arc::new(type_builder.finish()) as ArrayRef,
                Arc::new(name_builder.finish()) as ArrayRef,
                Arc::new(effect_builder.finish()) as ArrayRef,
                Arc::new(by_builder.finish()) as ArrayRef,
                Arc::new(at_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Handle `COMMENT ON TABLE/COLUMN` by storing the comment as an Iceberg table property.
    ///
    /// - `COMMENT ON TABLE t IS 'text'` → sets property `"comment"` = text
    /// - `COMMENT ON COLUMN t.col IS 'text'` → sets property `"comment.<col>"` = text
    /// - `IS NULL` removes the comment (stores empty string)
    async fn handle_comment_on(
        &self,
        session: &Session,
        stmt: &Statement,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqlparser::ast::CommentObject;
        use iceberg::TableIdent;
        use crate::catalog_ops::parse_table_ref;

        let (object_type, object_name, comment_text) = match stmt {
            Statement::Comment {
                object_type,
                object_name,
                comment,
                ..
            } => (object_type, object_name, comment),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected COMMENT statement, got: {other}"
                )));
            }
        };

        // For COLUMN comments the object_name is "table.column" — split off the column part.
        let (table_ref_parts, prop_key) = match object_type {
            CommentObject::Table => {
                // object_name is the table name
                (object_name.clone(), "comment".to_string())
            }
            CommentObject::Column => {
                // object_name is table.column — last ident is the column name
                let parts: Vec<_> = object_name.0.iter().collect();
                if parts.len() < 2 {
                    return Err(SqeError::Execution(
                        "COMMENT ON COLUMN requires table.column format".to_string(),
                    ));
                }
                let col_name = parts.last().map(|i| i.value.clone()).unwrap_or_default();
                let table_parts = sqlparser::ast::ObjectName(
                    object_name.0[..object_name.0.len() - 1].to_vec(),
                );
                (table_parts, format!("comment.{col_name}"))
            }
            other => {
                return Err(SqeError::NotImplemented(format!(
                    "COMMENT ON {other} is not supported; only TABLE and COLUMN are supported"
                )));
            }
        };

        let (namespace, table_name) = parse_table_ref(&table_ref_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);

        let comment_value = comment_text.clone().unwrap_or_default();

        tracing::info!(
            username = %session.user.username,
            table = %table_ident,
            property = %prop_key,
            "COMMENT ON — storing as Iceberg table property"
        );

        let updates = vec![iceberg::TableUpdate::SetProperties {
            updates: std::collections::HashMap::from([(prop_key, comment_value)]),
        }];

        session_catalog
            .commit_schema_update(&table_ident, updates, vec![])
            .await?;

        Ok(vec![])
    }

    /// Handle `SHOW STATS FOR <table>` by reading the current snapshot summary.
    ///
    /// Returns a single-row RecordBatch with columns:
    /// - `column_name`   — `"<all columns>"` (aggregate row)
    /// - `row_count`     — total-records from snapshot summary
    /// - `data_file_count` — total-data-files from snapshot summary
    /// - `total_size`    — total-files-size from snapshot summary (bytes)
    async fn handle_show_stats(
        &self,
        session: &Session,
        table_name: &str,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use iceberg::{NamespaceIdent, TableIdent};

        // Parse "schema.table" or "table"
        let parts: Vec<&str> = table_name.splitn(3, '.').collect();
        let (namespace, bare_table) = match parts.len() {
            1 => ("default", parts[0]),
            2 => (parts[0], parts[1]),
            _ => (parts[1], parts[2]), // catalog.schema.table
        };

        let ns_ident = NamespaceIdent::new(namespace.to_string());
        let table_ident = TableIdent::new(ns_ident, bare_table.to_string());

        let table = session_catalog.load_table(&table_ident).await?;
        let metadata = table.metadata();

        // Extract stats from the current snapshot summary (empty table has no snapshot)
        let (row_count, file_count, total_size) = if let Some(snapshot) = metadata.current_snapshot() {
            let summary = snapshot.summary();
            let props = &summary.additional_properties;
            let rows = props
                .get("total-records")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let files = props
                .get("total-data-files")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let size = props
                .get("total-files-size")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            (rows, files, size)
        } else {
            (0_i64, 0_i64, 0_i64)
        };

        tracing::info!(
            username = %session.user.username,
            table = %table_ident,
            row_count,
            file_count,
            total_size,
            "SHOW STATS FOR — returning snapshot summary"
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, true),
            Field::new("data_file_count", DataType::Int64, true),
            Field::new("total_size", DataType::Int64, true),
        ]));

        let mut name_builder = StringBuilder::new();
        name_builder.append_value("<all columns>");
        let name_array: ArrayRef = Arc::new(name_builder.finish());
        let row_array: ArrayRef = Arc::new(Int64Array::from(vec![row_count]));
        let file_array: ArrayRef = Arc::new(Int64Array::from(vec![file_count]));
        let size_array: ArrayRef = Arc::new(Int64Array::from(vec![total_size]));

        let batch = RecordBatch::try_new(schema, vec![name_array, row_array, file_array, size_array])
            .map_err(|e| SqeError::Execution(format!("Failed to build SHOW STATS result: {e}")))?;

        Ok(vec![batch])
    }

    // -----------------------------------------------------------------------
    // ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS handlers
    // -----------------------------------------------------------------------

    async fn handle_attach(
        &self,
        stmt: &sqe_sql::AttachStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.runtime_catalogs
            .attach(stmt, &self.secrets)
            .await
            .map_err(SqeError::Execution)?;
        crate::session_context::invalidate_all_session_caches();
        info!(catalog = %stmt.name, kind = %stmt.kind.name(), "ATTACH complete");
        Ok(vec![])
    }

    fn handle_detach(
        &self,
        stmt: &sqe_sql::DetachStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.runtime_catalogs
            .detach(&stmt.name)
            .map_err(SqeError::Execution)?;
        crate::session_context::invalidate_all_session_caches();
        info!(catalog = %stmt.name, "DETACH complete");
        Ok(vec![])
    }

    fn handle_create_secret(
        &self,
        stmt: &sqe_sql::CreateSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqe_core::Secret;
        use sqe_sql::SecretKind;

        let opts = &stmt.options;
        let get_str = |key: &str| -> Result<String, SqeError> {
            opts.get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| SqeError::Execution(format!(
                    "CREATE SECRET: missing required option {key} for {:?} secret",
                    stmt.kind.name()
                )))
        };
        let get_opt = |key: &str| -> Option<String> {
            opts.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        let secret = match stmt.kind {
            SecretKind::Aws => Secret::Aws {
                access_key: get_opt("ACCESS_KEY_ID"),
                secret_key: get_opt("SECRET_ACCESS_KEY"),
                session_token: get_opt("SESSION_TOKEN"),
                region: get_opt("REGION"),
                profile: get_opt("PROFILE"),
            },
            SecretKind::Bearer => Secret::Bearer {
                token: get_str("TOKEN")?,
            },
            SecretKind::Basic => Secret::Basic {
                username: get_str("USERNAME")?,
                password: get_str("PASSWORD")?,
            },
        };

        self.secrets
            .create(&stmt.name, secret)
            .map_err(SqeError::Execution)?;
        info!(name = %stmt.name, kind = %stmt.kind.name(), "CREATE SECRET complete");
        Ok(vec![])
    }

    fn handle_drop_secret(
        &self,
        stmt: &sqe_sql::DropSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let in_use = self.runtime_catalogs.referenced_secrets(&stmt.name);
        self.secrets
            .drop_secret(&stmt.name, &in_use)
            .map_err(SqeError::Execution)?;
        info!(name = %stmt.name, "DROP SECRET complete");
        Ok(vec![])
    }

    fn handle_show_secrets(&self) -> sqe_core::Result<Vec<RecordBatch>> {
        let listed = self.secrets.list();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
        ]));

        let mut name_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        for (name, type_name) in &listed {
            name_b.append_value(name);
            type_b.append_value(type_name);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(name_b.finish()) as ArrayRef, Arc::new(type_b.finish()) as ArrayRef],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build SHOW SECRETS result: {e}")))?;

        Ok(vec![batch])
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
            SessionCatalog::for_session(
                &self.config,
                self.table_cache.clone(),
                &session.access_token,
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

/// Replace whole-token occurrences of `needle` (case insensitive) in `sql` with `replacement`.
///
/// Used to rewrite a table reference like `ns.t` to `datafusion.public.alias` after the incremental pre-parser has run.
///
/// The match is strict: `needle` must be preceded and followed by a character that cannot appear in a SQL identifier (whitespace, punctuation, or start / end of string). This prevents spurious matches when `needle` appears as a substring of a longer identifier.
fn replace_table_reference(sql: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return sql.to_string();
    }
    let sql_upper = sql.to_uppercase();
    let needle_upper = needle.to_uppercase();
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    while let Some(pos) = sql_upper[cursor..].find(needle_upper.as_str()) {
        let abs = cursor + pos;
        // Verify word boundary. The character preceding `abs` must not be an
        // identifier character, and the character after `abs + needle.len()`
        // must also not be an identifier character (letter, digit, `_`, `.`).
        let pre_ok = abs == 0
            || !is_ident_char(sql.as_bytes()[abs - 1]);
        let end = abs + needle.len();
        let post_ok = end == sql.len() || !is_ident_char(sql.as_bytes()[end]);
        if pre_ok && post_ok {
            out.push_str(&sql[cursor..abs]);
            out.push_str(replacement);
            cursor = end;
        } else {
            // Advance by one byte past the start of this match so we can keep
            // searching for later occurrences.
            out.push_str(&sql[cursor..=abs]);
            cursor = abs + 1;
        }
    }
    out.push_str(&sql[cursor..]);
    out
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'.'
}

/// Resolve a SQL timestamp expression to epoch milliseconds.
///
/// Supports:
/// - `TIMESTAMP '2026-01-01 00:00:00'` (TypedString)
/// - `'2026-01-01'` (bare string literal)
/// - `CAST('...' AS TIMESTAMP)` (Cast)
/// - Raw integer literals (treated as epoch ms directly)
fn resolve_timestamp_expr(expr: &sqlparser::ast::Expr) -> sqe_core::Result<i64> {
    use sqlparser::ast::{Expr, Value};

    match expr {
        Expr::TypedString { value, .. } => {
            parse_timestamp_str(value)
        }
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => {
            parse_timestamp_str(s)
        }
        Expr::Value(Value::Number(n, _)) => {
            n.parse::<i64>().map_err(|_| SqeError::Execution(
                format!("Cannot parse time travel integer expression: {n}")
            ))
        }
        Expr::Cast { expr: inner, .. } => {
            resolve_timestamp_expr(inner)
        }
        other => Err(SqeError::Execution(format!(
            "Unsupported time travel expression: {other}. \
             Use TIMESTAMP '2026-01-01 00:00:00' or epoch milliseconds."
        ))),
    }
}

/// Parse a timestamp string into epoch milliseconds.
///
/// Tries common formats: `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DDTHH:MM:SS`, `YYYY-MM-DD`.
fn parse_timestamp_str(s: &str) -> sqe_core::Result<i64> {
    // Try as raw epoch milliseconds
    if let Ok(n) = s.parse::<i64>() {
        return Ok(n);
    }
    // Try ISO datetime with space separator
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp_millis());
    }
    // Try ISO datetime with T separator
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt.and_utc().timestamp_millis());
    }
    // Try date-only (midnight)
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(dt) = d.and_hms_opt(0, 0, 0) {
            return Ok(dt.and_utc().timestamp_millis());
        }
    }
    Err(SqeError::Execution(format!(
        "Cannot parse time travel timestamp '{s}'. \
         Use format 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD', or epoch milliseconds."
    )))
}

/// Find the latest Iceberg snapshot with `timestamp_ms <= target_ms`.
///
/// Returns an error when the table has no snapshot at or before the given timestamp.
fn find_snapshot_at_timestamp(
    metadata: &iceberg::spec::TableMetadata,
    target_ms: i64,
) -> sqe_core::Result<i64> {
    let mut best: Option<(i64, i64)> = None; // (snapshot_id, timestamp_ms)

    for snap in metadata.snapshots() {
        let snap_ts = snap.timestamp_ms();
        if snap_ts <= target_ms && (best.is_none() || snap_ts > best.unwrap().1) {
            best = Some((snap.snapshot_id(), snap_ts));
        }
    }

    best.map(|(id, _)| id).ok_or_else(|| {
        SqeError::Execution(format!(
            "No Iceberg snapshot found at or before timestamp {}ms. \
             The table may not have existed yet at that point in time.",
            target_ms
        ))
    })
}

/// Best-effort extraction of ORDER BY column names from SQL text.
fn extract_order_by_columns(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    if let Some(idx) = upper.rfind("ORDER BY") {
        let after = &sql[idx + 8..];
        let end = after
            .find([')' , ';'])
            .or_else(|| {
                let u = after.to_uppercase();
                u.find("LIMIT").or_else(|| u.find("OFFSET")).or_else(|| u.find("FETCH"))
            })
            .unwrap_or(after.len());
        let cols_str = &after[..end];
        cols_str
            .split(',')
            .map(|s| s.split_whitespace().next().unwrap_or("").to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![]
    }
}

/// Walk a physical plan tree and extract per-query resource metrics.
///
/// Sums `OutputBytes` and `output_rows` on scan nodes (`IcebergScanExec`,
/// `ParquetExec`, `CsvExec`) to approximate bytes/rows scanned, and sums
/// `spilled_bytes` across all nodes. `peak_memory_bytes` is left at 0 here
/// and filled by the caller from the runtime memory pool snapshot.
pub(crate) fn extract_plan_metrics(plan: &Arc<dyn ExecutionPlan>) -> PlanMetrics {
    use datafusion::physical_plan::metrics::MetricValue;

    let mut bytes_scanned: u64 = 0;
    let mut rows_scanned: u64 = 0;
    let mut spill_bytes: u64 = 0;

    let mut stack: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        let name = node.name();
        let is_scan = name.contains("Scan")
            || name.contains("Parquet")
            || name.contains("Csv");

        if let Some(metrics) = node.metrics() {
            // Scan nodes: accumulate bytes/rows scanned
            if is_scan {
                if let Some(ob) = metrics.sum(|m| matches!(m.value(), MetricValue::OutputBytes(_))) {
                    bytes_scanned += ob.as_usize() as u64;
                }
                if let Some(or) = metrics.output_rows() {
                    rows_scanned += or as u64;
                }
            }

            // All nodes: accumulate spill bytes
            if let Some(sb) = metrics.spilled_bytes() {
                spill_bytes += sb as u64;
            }
        }

        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }

    PlanMetrics {
        bytes_scanned,
        rows_scanned,
        spill_bytes,
        peak_memory_bytes: 0,
    }
}

/// Walk a physical plan tree and aggregate spill metrics from all operators.
///
/// Returns `(sort_spill_count, sort_spill_bytes, join_spill_count, join_spill_bytes)`.
///
/// Sort operators (SortExec, SortPreservingMergeExec) contribute to sort spill
/// metrics, while join operators (HashJoinExec, SortMergeJoinExec,
/// NestedLoopJoinExec) contribute to join spill metrics. DataFusion's
/// `MetricsSet` provides `spill_count()` and `spilled_bytes()` on each
/// operator after execution.
pub(crate) fn aggregate_spill_metrics(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize, usize) {
    let mut sort_spill_count: usize = 0;
    let mut sort_spill_bytes: usize = 0;
    let mut join_spill_count: usize = 0;
    let mut join_spill_bytes: usize = 0;

    let mut stack: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        let name = node.name();
        if let Some(metrics) = node.metrics() {
            let sc = metrics.spill_count().unwrap_or(0);
            let sb = metrics.spilled_bytes().unwrap_or(0);

            if sc > 0 || sb > 0 {
                let is_sort = name.contains("Sort");
                let is_join = name.contains("Join");
                if is_sort {
                    sort_spill_count += sc;
                    sort_spill_bytes += sb;
                } else if is_join {
                    join_spill_count += sc;
                    join_spill_bytes += sb;
                } else {
                    // Unknown operator that spills — attribute to sort as default
                    sort_spill_count += sc;
                    sort_spill_bytes += sb;
                }
            }
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }

    (sort_spill_count, sort_spill_bytes, join_spill_count, join_spill_bytes)
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

/// Replace a specific scan node in a physical plan tree with a new node,
/// keeping all parent nodes (filter, aggregate, sort, projection) intact.
///
/// Walks the tree recursively. When the target node is found (by Arc pointer
/// equality), it's replaced with the replacement. All ancestor nodes are
/// rebuilt via `with_new_children()` to incorporate the change.
fn replace_scan_in_plan(
    plan: &Arc<dyn ExecutionPlan>,
    target: &Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    // Check if this node IS the target (pointer equality)
    if Arc::ptr_eq(plan, target) {
        return replacement;
    }

    let children = plan.children();
    if children.is_empty() {
        // Leaf node that isn't the target — return as-is
        return Arc::clone(plan);
    }

    // Recurse into children and rebuild if any changed
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|child| replace_scan_in_plan(child, target, Arc::clone(&replacement)))
        .collect();

    // Check if any child actually changed (avoid unnecessary cloning)
    let changed = new_children
        .iter()
        .zip(children.iter())
        .any(|(new, old)| !Arc::ptr_eq(new, old));

    if changed {
        plan.clone().with_new_children(new_children)
            .unwrap_or_else(|_| Arc::clone(plan))
    } else {
        Arc::clone(plan)
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

/// Best-effort extraction of schema and table names from a classified SQL statement
/// for OTel `db.namespace` and `db.collection.name` attributes.
fn extract_otel_table_info(kind: &StatementKind) -> Option<(Option<String>, Option<String>)> {
    let from_object_name = |name: &sqlparser::ast::ObjectName| -> (Option<String>, Option<String>) {
        let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
        match parts.len() {
            1 => (None, Some(parts[0].clone())),
            2 => (Some(parts[0].clone()), Some(parts[1].clone())),
            3 => (Some(parts[1].clone()), Some(parts[2].clone())),
            _ => (None, None),
        }
    };

    fn from_table_tables(ft: &sqlparser::ast::FromTable) -> Option<&Vec<sqlparser::ast::TableWithJoins>> {
        match ft {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => Some(tables),
            sqlparser::ast::FromTable::WithoutKeyword(tables) => Some(tables),
        }
    }

    match kind {
        StatementKind::Query(stmt) => {
            if let Statement::Query(query) = stmt.as_ref() {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(first_from) = select.from.first() {
                        if let TableFactor::Table { name, .. } = &first_from.relation {
                            return Some(from_object_name(name));
                        }
                    }
                }
            }
            None
        }
        StatementKind::Insert(stmt) => {
            if let Statement::Insert(insert) = stmt.as_ref() {
                if let sqlparser::ast::TableObject::TableName(name) = &insert.table {
                    return Some(from_object_name(name));
                }
            }
            None
        }
        StatementKind::Delete(stmt) => {
            if let Statement::Delete(delete) = stmt.as_ref() {
                if let Some(tables) = from_table_tables(&delete.from) {
                    if let Some(first_from) = tables.first() {
                        if let TableFactor::Table { name, .. } = &first_from.relation {
                            return Some(from_object_name(name));
                        }
                    }
                }
            }
            None
        }
        StatementKind::CreateTable(stmt) | StatementKind::Ctas(stmt) | StatementKind::Drop(stmt) => {
            if let Statement::CreateTable(ct) = stmt.as_ref() {
                return Some(from_object_name(&ct.name));
            }
            if let Statement::Drop { names, .. } = stmt.as_ref() {
                if let Some(first) = names.first() {
                    return Some(from_object_name(first));
                }
            }
            None
        }
        _ => None,
    }
}

/// Decide whether a statement should produce OpenLineage events.
///
/// - SELECT (`Query`): opt-in via `cfg.emit_selects` -- read-only queries
///   are noisy and lineage tools rarely need them.
/// - Maintenance procedures (`Procedure`): never emit. CALL system.optimize,
///   expire_snapshots, etc. mutate snapshot history but produce no
///   user-visible inputs/outputs that matter to lineage.
/// - Everything else (DML writes, DDL, SHOW commands, transactions, USE,
///   GRANT/REVOKE): always emit. Sinks decide what to do with metadata events.
fn should_emit(kind: &StatementKind, cfg: &sqe_core::config::OpenLineageConfig) -> bool {
    match kind {
        StatementKind::Query(_) => cfg.emit_selects,
        StatementKind::Procedure(_) => false,
        _ => true,
    }
}

/// Extract the target namespace name from sqlparser's `ShowStatementIn`
/// stringification, returned by `StatementKind::ShowTables(filter)`.
///
/// Returns `None` when the filter is empty (caller should list every
/// namespace) and `Some(name)` for a single-segment namespace.
///
/// Handles every shape sqlparser can render for `SHOW TABLES`:
///
/// - `""` (no filter)              -> `None`
/// - `IN analytics_db`              -> `Some("analytics_db")`
/// - `FROM analytics_db`            -> `Some("analytics_db")`
/// - `IN "analytics_db"`            -> `Some("analytics_db")` (strips wrapping quotes)
/// - `IN main_warehouse.analytics_db`        -> `Some("analytics_db")` (drops catalog qualifier)
/// - `IN "main_warehouse"."analytics_db"`    -> `Some("analytics_db")` (both)
///
/// The Polaris namespace API stores names verbatim. Earlier code did
/// `trim_start_matches("IN")` and used the result as-is, which made
/// `SHOW TABLES IN "analytics_db"` look up a namespace literally named
/// `"analytics_db"` (with the quote characters in it). The Flight SQL
/// `do_get_tables` handler emits the quoted form, so DBeaver's database
/// tree showed every schema as empty.
fn parse_show_tables_namespace(filter: &str) -> Option<String> {
    let raw = filter.trim();
    if raw.is_empty() {
        return None;
    }
    // sqlparser renders the IN/FROM keyword followed by a space.
    // Match either prefix; fall through to the raw input if neither
    // is present (some callers pass the bare namespace already).
    let after_kw = raw
        .strip_prefix("IN ")
        .or_else(|| raw.strip_prefix("FROM "))
        .or_else(|| raw.strip_prefix("in "))
        .or_else(|| raw.strip_prefix("from "))
        .unwrap_or(raw)
        .trim();
    if after_kw.is_empty() {
        return None;
    }
    // Take the last dotted segment so a `cat.schema` qualifier reduces
    // to `schema`. Polaris namespaces are single-segment in this
    // codebase; a multi-level Iceberg namespace would still resolve
    // through the trailing segment which matches the historical
    // behaviour for unqualified inputs.
    let last_segment = after_kw.rsplit('.').next().unwrap_or(after_kw).trim();
    // Strip wrapping double quotes if present.
    let unquoted = last_segment
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(last_segment);
    if unquoted.is_empty() {
        None
    } else {
        Some(unquoted.to_string())
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
            write_branch: None,
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

    // ── parse_show_tables_namespace ──────────────────────────────
    // Regression coverage for the "DBeaver shows empty schemas" bug:
    // sqlparser stringifies `SHOW TABLES IN "analytics_db"` as
    // `IN "analytics_db"`, and the old code stripped the keyword but
    // not the surrounding quotes. The Polaris namespace lookup was
    // then asking for a namespace literally named `"analytics_db"`.

    #[test]
    fn parse_show_tables_namespace_empty_returns_none() {
        assert_eq!(parse_show_tables_namespace(""), None);
        assert_eq!(parse_show_tables_namespace("   "), None);
    }

    #[test]
    fn parse_show_tables_namespace_unquoted() {
        assert_eq!(
            parse_show_tables_namespace("IN analytics_db"),
            Some("analytics_db".to_string())
        );
        assert_eq!(
            parse_show_tables_namespace("FROM analytics_db"),
            Some("analytics_db".to_string())
        );
    }

    #[test]
    fn parse_show_tables_namespace_quoted_strips_quotes() {
        assert_eq!(
            parse_show_tables_namespace(r#"IN "analytics_db""#),
            Some("analytics_db".to_string())
        );
        assert_eq!(
            parse_show_tables_namespace(r#"FROM "analytics_db""#),
            Some("analytics_db".to_string())
        );
    }

    #[test]
    fn parse_show_tables_namespace_qualified_drops_catalog() {
        assert_eq!(
            parse_show_tables_namespace("IN main_warehouse.analytics_db"),
            Some("analytics_db".to_string())
        );
    }

    #[test]
    fn parse_show_tables_namespace_quoted_qualified() {
        assert_eq!(
            parse_show_tables_namespace(r#"IN "main_warehouse"."analytics_db""#),
            Some("analytics_db".to_string())
        );
    }

    #[test]
    fn parse_show_tables_namespace_bare_name_no_keyword() {
        // Some callers pass the bare namespace already.
        assert_eq!(
            parse_show_tables_namespace("analytics_db"),
            Some("analytics_db".to_string())
        );
        assert_eq!(
            parse_show_tables_namespace(r#""analytics_db""#),
            Some("analytics_db".to_string())
        );
    }

    #[test]
    fn parse_show_tables_namespace_lowercase_keyword() {
        // sqlparser usually upper-cases but defend against either form.
        assert_eq!(
            parse_show_tables_namespace("in analytics_db"),
            Some("analytics_db".to_string())
        );
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

    // ── extract_grant_statement tests ──────────────────────────────────

    #[test]
    fn extract_grant_statement_basic_table() {
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON my_catalog.my_schema.my_table TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert_eq!(stmt.catalog.as_deref(), Some("my_catalog"));
        assert_eq!(stmt.namespace.as_deref(), Some("my_schema"));
        assert_eq!(stmt.table.as_deref(), Some("my_table"));
        assert!(matches!(stmt.grantee, Grantee::User(ref n) if n == "alice"));
    }

    #[test]
    fn extract_grant_statement_role_grantee() {
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = r#"GRANT SELECT ON my_schema.my_table TO ROLE "analysts""#;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert!(matches!(stmt.grantee, Grantee::Role(ref n) if n == "analysts"));
    }

    #[test]
    fn extract_grant_statement_group_grantee() {
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = r#"GRANT INSERT ON my_table TO GROUP "SG-Risk""#;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "INSERT");
        assert!(matches!(stmt.grantee, Grantee::Group(ref n) if n == "SG-Risk"));
    }

    #[test]
    fn extract_grant_statement_bare_identifier_defaults_to_user() {
        // sqlparser 0.54 parses `TO alice` as GranteesType::None (bare identifier,
        // no explicit ROLE/USER prefix). We default bare identifiers to User.
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON t TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert!(matches!(stmt.grantee, Grantee::User(ref n) if n == "alice"));
    }

    #[test]
    fn extract_grant_statement_rejects_non_grant() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "SELECT 1";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let result = QueryHandler::extract_grant_statement(&stmts[0]);

        assert!(result.is_err(), "Should reject non-GRANT/REVOKE statements");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Expected GRANT/REVOKE"),
            "Error message should mention GRANT/REVOKE, got: {err_msg}"
        );
    }

    #[test]
    fn extract_grant_statement_handles_revoke_three_part_name() {
        // Belt-and-suspenders: the GRANT and REVOKE arms share downstream code,
        // but this test guards against future divergence and also exercises
        // the 3-part identifier path (catalog.namespace.table) in one shot.
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "REVOKE SELECT ON prod.analytics.events FROM alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert_eq!(stmt.catalog.as_deref(), Some("prod"));
        assert_eq!(stmt.namespace.as_deref(), Some("analytics"));
        assert_eq!(stmt.table.as_deref(), Some("events"));
        assert!(matches!(stmt.grantee, Grantee::User(ref n) if n == "alice"));
    }

    /// DENY is not a standard SQL keyword recognized by sqlparser.
    /// It cannot be parsed as a Statement::Grant or Statement::Revoke.
    /// SQE would need custom pre-scan logic to handle DENY syntax.
    /// This test documents the current behavior: DENY is a parse error.
    #[test]
    fn deny_is_not_parseable_by_sqlparser() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "DENY SELECT ON my_table TO alice";
        let result = Parser::parse_sql(&GenericDialect {}, sql);

        assert!(
            result.is_err(),
            "DENY should not parse as valid SQL in sqlparser 0.54"
        );
    }

    // ── should_emit (OpenLineage gating) tests ─────────────────────────

    fn ol_cfg(emit_selects: bool) -> sqe_core::config::OpenLineageConfig {
        sqe_core::config::OpenLineageConfig {
            emit_selects,
            ..sqe_core::config::OpenLineageConfig::default()
        }
    }

    #[test]
    fn should_emit_select_respects_emit_selects_flag() {
        let kind = parse_and_classify("SELECT 1").expect("parse SELECT");
        assert!(matches!(kind, StatementKind::Query(_)));
        assert!(!should_emit(&kind, &ol_cfg(false)));
        assert!(should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_maintenance_procedure_never_emits() {
        // CALL system.rewrite_data_files is classified as Procedure, the
        // maintenance variant. Lineage events for snapshot rewrites add
        // noise without a meaningful input/output set.
        let kind = parse_and_classify("CALL system.rewrite_data_files(table => 'ns.t')")
            .expect("parse CALL");
        assert!(matches!(kind, StatementKind::Procedure(_)));
        assert!(!should_emit(&kind, &ol_cfg(false)));
        assert!(!should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_dml_always_emits_regardless_of_emit_selects() {
        let kind = parse_and_classify("INSERT INTO t VALUES (1)").expect("parse INSERT");
        assert!(matches!(kind, StatementKind::Insert(_)));
        assert!(should_emit(&kind, &ol_cfg(false)));
        assert!(should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_ddl_always_emits() {
        let kind = parse_and_classify("CREATE TABLE t (id INT)").expect("parse CREATE TABLE");
        assert!(matches!(kind, StatementKind::CreateTable(_)));
        assert!(should_emit(&kind, &ol_cfg(false)));
    }
}

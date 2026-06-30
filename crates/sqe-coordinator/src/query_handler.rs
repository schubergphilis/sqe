use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, BooleanArray, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::JoinType;
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
use sqe_sql::StatementKind;

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
    /// Semaphore limiting global concurrent query execution.
    query_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Per-user concurrency semaphores. Lazily created on first query per
    /// username. Each entry caps how many simultaneous queries one user can
    /// hold against the global pool, preventing a single tenant from
    /// monopolising `query_semaphore`.
    per_user_semaphores: Arc<dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// Tracks per-user reserved memory bytes for admission. Reservations are
    /// recorded at admit time and released when the streaming result wrapper
    /// drops, so a single user submitting many memory-hungry queries gets
    /// rejected with a per-user pressure error before they drag the global
    /// FairSpillPool into the red-band.
    per_user_memory: Arc<crate::memory::PerUserMemoryRegistry>,
    /// Cached parse of `config.query.per_user_memory_budget`. `0` disables
    /// the per-user cap.
    per_user_memory_budget_bytes: usize,
    /// Cached parse of `config.query.max_query_memory`. Used as the
    /// per-query reservation increment against the per-user budget.
    per_query_memory_bytes: usize,
    /// Cached parse of `config.query.query_profile`. Parsed once at startup
    /// so an unknown value warns once, not per query.
    profile_mode: sqe_core::ProfileMode,
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
    /// Cross-query in-flight fragment counter per worker URL. Read by the
    /// scheduler as initial load so concurrent queries don't both pick the
    /// same idle worker; incremented on assignment, decremented on fragment
    /// completion via [`ReservationGuard`].
    worker_load: Arc<crate::worker_registry::WorkerLoadTracker>,
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
        let runtime = crate::runtime::build_coordinator_runtime(&config.coordinator, &config.storage)
            .map_err(|e| sqe_core::SqeError::Config(format!("Failed to build runtime: {e}")))?;

        let per_user_memory_budget_bytes = if config.query.per_user_memory_budget == "0" {
            0
        } else {
            sqe_core::parse_memory_limit(&config.query.per_user_memory_budget).map_err(|e| {
                sqe_core::SqeError::Config(format!(
                    "Invalid per_user_memory_budget '{}': {e}",
                    config.query.per_user_memory_budget
                ))
            })? as usize
        };
        let per_query_memory_bytes = if config.query.max_query_memory == "0" {
            0
        } else {
            sqe_core::parse_memory_limit(&config.query.max_query_memory).map_err(|e| {
                sqe_core::SqeError::Config(format!(
                    "Invalid max_query_memory '{}': {e}",
                    config.query.max_query_memory
                ))
            })? as usize
        };

        let profile_mode = sqe_core::ProfileMode::parse(&config.query.query_profile);

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
            per_user_semaphores: Arc::new(dashmap::DashMap::new()),
            per_user_memory: Arc::new(crate::memory::PerUserMemoryRegistry::new()),
            per_user_memory_budget_bytes,
            per_query_memory_bytes,
            profile_mode,
            runtime,
            table_cache: None,
            grant_backend,
            session_manager: None,
            runtime_catalogs,
            secrets,
            worker_load: Arc::new(crate::worker_registry::WorkerLoadTracker::new()),
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
    #[must_use = "with_table_cache consumes self; bind the returned QueryHandler"]
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

    /// Returns the audit logger, if one was wired at construction time.
    ///
    /// Used by `SqeFlightSqlService` to emit `AuditKind::Auth` events from the
    /// auth path without duplicating the logger reference.
    pub fn audit(&self) -> Option<&Arc<sqe_metrics::audit::AuditLogger>> {
        self.audit.as_ref()
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

    /// Resolve the catalog whose metadata a `SHOW SCHEMAS/TABLES FROM <catalog>`
    /// should read. When `catalog` names a non-default warehouse and
    /// `catalog_discovery = polaris-auto`, discover THAT catalog via Polaris
    /// (same resolution the write path uses), so `SHOW SCHEMAS FROM ws_team_a`
    /// lists `ws_team_a`'s namespaces rather than the default warehouse's. An
    /// unqualified SHOW (or a reference to the default warehouse) uses the
    /// default session catalog as before.
    async fn show_catalog(
        &self,
        session: &Session,
        catalog: Option<&str>,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
        // An explicit catalog in the SHOW statement wins; otherwise fall back to
        // the session catalog (the connection's X-Trino-Catalog / Flight
        // catalog). Without this, `SHOW TABLES` / `SHOW TABLES FROM <schema>`
        // and `SHOW SCHEMAS` (no catalog qualifier) resolved against the default
        // warehouse and ignored the session catalog, so a BI client syncing
        // against a polaris-auto-discovered catalog saw 0 tables -- while the
        // SELECT path worked because its explicit 3-part name triggered
        // discovery. This aligns the SHOW path with the SELECT path. (#6/#2)
        let catalog = catalog.or(session.default_catalog.as_deref());
        if let Some(cat) = catalog {
            if cat != self.config.catalog.warehouse
                && self.config.query.catalog_discovery
                    == sqe_core::config::CatalogDiscovery::PolarisAuto
            {
                return crate::session_context::discover_session_catalog(
                    cat,
                    &self.config,
                    session,
                    self.table_cache.as_ref(),
                )
                .await
                .ok_or_else(|| {
                    sqe_core::SqeError::Catalog(format!(
                        "Unknown catalog '{cat}': not resolvable via Polaris \
                         (nonexistent or not authorized for this user)"
                    ))
                });
            }
        }
        self.session_catalog(session).await
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

    /// Resolve any unknown 3-part catalog qualifiers in `stmt` against the
    /// caller's session ctx, lazily discovering Polaris warehouses when
    /// `[query] catalog_discovery = polaris-auto`. Errors with the standard
    /// "unknown catalog" message if a qualifier still can't be resolved.
    /// Shared by `execute()` and `execute_stream()`.
    async fn preflight_resolve_catalogs(
        &self,
        stmt: &Statement,
        session: &Session,
    ) -> Result<(), SqeError> {
        let qualifiers = sqe_sql::extract_catalog_qualifiers(stmt);
        if qualifiers.is_empty() {
            return Ok(());
        }

        // First pass: build the "known" set from config + attached catalogs
        // WITHOUT contacting Polaris. This preserves static-mode behavior: an
        // unknown qualifier under `catalog_discovery = static` fails fast with
        // no network IO (the pre-flight check fires before any connection).
        let mut known: std::collections::HashSet<String> = self
            .config
            .flattened_catalogs()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        known.insert("system".to_string());
        known.insert("datafusion".to_string());
        known.extend(self.runtime_catalogs.list().map_err(SqeError::Catalog)?);

        // Fast path: every qualifier is statically known — done, no Polaris.
        if qualifiers.iter().all(|q| known.contains(q)) {
            return Ok(());
        }

        // Static mode: an unknown qualifier is an error. Never probe Polaris.
        if self.config.query.catalog_discovery
            != sqe_core::config::CatalogDiscovery::PolarisAuto
        {
            let unknown = qualifiers
                .iter()
                .find(|q| !known.contains(*q))
                .expect("at least one unknown qualifier (the all() check failed)");
            let mut names: Vec<String> = known.into_iter().collect();
            names.sort();
            return Err(SqeError::Catalog(format!(
                "unknown catalog '{}' in 3-part identifier; configured \
                 catalogs are {:?}. Declare it via TOML `[catalogs.<name>]`, \
                 `ATTACH` it, or enable `[query] catalog_discovery = \"polaris-auto\"`.",
                unknown, names
            )));
        }

        // PolarisAuto + at least one unknown qualifier: now build the session
        // ctx (authoritative — it includes any catalog discovered earlier in
        // this session, so a second reference never re-probes Polaris) and
        // lazily discover the unknown warehouses.
        let (ctx, _) = self.create_session_context(session).await?;
        let mut known: std::collections::HashSet<String> =
            ctx.catalog_names().into_iter().collect();
        known.insert("system".to_string());
        known.insert("datafusion".to_string());

        for q in &qualifiers {
            if known.contains(q) {
                continue;
            }
            if let Some((provider, _session_catalog)) =
                crate::session_context::discover_catalog_provider(
                    q,
                    &self.config,
                    session,
                    self.table_cache.as_ref(),
                    self.policy_store.as_ref(),
                    self.metrics.as_ref(),
                )
                .await
            {
                ctx.register_catalog(q.clone(), std::sync::Arc::new(provider));
                known.insert(q.clone());
                tracing::info!(catalog = %q, "catalog discovery: registered Polaris warehouse for session");
            }
        }

        if let Some(unknown) = qualifiers.iter().find(|q| !known.contains(*q)) {
            let mut names: Vec<String> = known.into_iter().collect();
            names.sort();
            return Err(SqeError::Catalog(format!(
                "unknown catalog '{}' in 3-part identifier; configured \
                 catalogs are {:?}. Declare it via TOML `[catalogs.<name>]`, \
                 `ATTACH` it, or enable `[query] catalog_discovery = \"polaris-auto\"`.",
                unknown, names
            )));
        }
        Ok(())
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
        client_ip: Option<String>,
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

        // Backpressure: per-user gate first, then global gate. The per-user
        // gate prevents one tenant from holding every global permit while
        // legitimately under their submission rate limit. We acquire owned
        // permits because the streaming path holds them for the result-
        // stream lifetime, not the synchronous execute() call.
        let _per_user_permit = if self.config.query.max_concurrent_per_user > 0 {
            let username = session.user.username.clone();
            let sem = self
                .per_user_semaphores
                .entry(username.clone())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        self.config.query.max_concurrent_per_user,
                    ))
                })
                .clone();
            match sem.try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(SqeError::Execution(format!(
                        "Too many concurrent queries for user '{}' ({} active). Please retry later.",
                        username, self.config.query.max_concurrent_per_user
                    )));
                }
            }
        } else {
            None
        };

        // Per-user memory reservation. Reject when this user's in-flight
        // queries would push past their share of the global pool, before
        // the global red-band check fires for everyone else.
        let _per_user_mem_reservation = if self.per_user_memory_budget_bytes > 0
            && self.per_query_memory_bytes > 0
        {
            let username = session.user.username.clone();
            match self.per_user_memory.try_reserve(
                &username,
                self.per_query_memory_bytes,
                self.per_user_memory_budget_bytes,
            ) {
                Some(r) => Some(r),
                None => {
                    let used = self.per_user_memory.used_bytes(&username);
                    return Err(SqeError::Execution(format!(
                        "Per-user memory budget exceeded for '{}': {} bytes reserved, \
                         limit {} bytes. Wait for in-flight queries to complete.",
                        username, used, self.per_user_memory_budget_bytes
                    )));
                }
            }
        } else {
            None
        };

        let _permit = if let Some(ref sem) = self.query_semaphore {
            match sem.clone().try_acquire_owned() {
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
        // Run the pre-parse pipeline: strip FOR INCREMENTAL / FOR VERSION AS OF
        // (sqlparser does not model them) and rewrite Hive-style
        // PARTITIONED BY into sqlparser-friendly PARTITION BY. The returned
        // ClassifiableSql proves at the type level that the input is safe
        // to hand to the classifier (issue #117).
        let kind = sqe_sql::parse_and_classify_typed(
            &sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql))?,
        )?;
        let kind_name = kind.name().to_string();

        // Pre-flight: when a 3-part identifier names a catalog that the
        // coordinator has not registered, fail fast with a clear error
        // instead of letting DataFusion silently fall back to the
        // session-default catalog. The silent fallback produces
        // confusing "namespace does not exist" errors against the
        // wrong warehouse and was the original symptom of issue #1.
        // Lazily discovers Polaris warehouses when catalog_discovery = polaris-auto.
        if let Some(stmt) = kind.statement() {
            self.preflight_resolve_catalogs(stmt, session).await?;
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
        let cancel_token = self.query_tracker.start(
            query_id,
            &session.user.username,
            session.source.as_deref(),
            sql,
            &session.id,
            client_ip.as_deref(),
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
                        bearer: Some(session.access_token().clone()),
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
                        metrics
                            .query_count
                            .with_label_values(&["success", &kind_name, ""])
                            .inc();
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
                                    bearer: Some(session.access_token().clone()),
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
        // Slot for the policy-decision summary. `execute_query` populates this
        // after policy enforcement; non-Query branches leave it None (the audit
        // entry then records the all-zero, not-denied default).
        let mut policy_summary: Option<sqe_policy::PolicySummary> = None;
        let execution_future = async {
            match &kind {
                StatementKind::Query(_) => self.execute_query(session, sql, &query_id, &plan_metrics, &mut captured_plan, &mut policy_summary).await,

                StatementKind::ShowCatalogs => self.handle_show_catalogs(session).await,

                StatementKind::ShowSchemas(filter) => {
                    self.handle_show_schemas(session, filter).await
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
                    } else if let sqlparser::ast::Statement::ExplainTable {
                        describe_alias,
                        table_name,
                        ..
                    } = stmt.as_ref()
                    {
                        // DESCRIBE / DESC <table> -> same column metadata as
                        // SHOW COLUMNS. EXPLAIN TABLE (a different alias) is not
                        // a column-listing request, so it is not redirected.
                        use sqlparser::ast::DescribeAlias;
                        if matches!(describe_alias, DescribeAlias::Describe | DescribeAlias::Desc) {
                            self.columns_for_table(session, &table_name.to_string()).await
                        } else {
                            Err(SqeError::NotImplemented(format!(
                                "Utility statement not supported: {stmt}"
                            )))
                        }
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
                StatementKind::ShowEffectivePolicy(ref params) => {
                    self.handle_show_effective_policy(session, params).await
                }
                StatementKind::ShowTags(ref table) => self.handle_show_tags(session, table).await,

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
                    crate::session_context::invalidate_all_session_caches().await;
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
                    // Flush cached tag policies so a SET TBLPROPERTIES on sqe.column-tags
                    // takes effect on the next query rather than after the cache TTL.
                    // Defense-in-depth: RangerStore::resolve_tags re-fetches the bundle
                    // on every call regardless, so the tag path is not TTL-lagged.
                    self.invalidate_policy_cache();
                    Ok(vec![])
                }
                StatementKind::SetTags(stmt) => {
                    self.catalog_ops.set_column_tags(session, stmt).await?;
                    crate::session_context::invalidate_session_cache(&session.user.username).await;
                    // Tag->column associations changed; flush cached tag policies so
                    // the next query re-resolves masks against the new tags.
                    self.invalidate_policy_cache();
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
                    crate::session_context::invalidate_all_session_caches().await;
                    Ok(vec![])
                }
                StatementKind::DropSchema(stmt) => {
                    // Cross-user: the namespace disappears for everyone.
                    self.catalog_ops.drop_schema(session, stmt).await?;
                    crate::session_context::invalidate_all_session_caches().await;
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
                            let select_sql = sqe_sql::rewrite_named_tvf_args(&format!("{query}"));
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
                            .map(|q| sqe_sql::rewrite_named_tvf_args(&format!("{q}")))
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

                // SHOW STATS FOR table — Trino per-column stats result set
                StatementKind::ShowStats(ref table_name) => {
                    self.handle_show_stats(session, table_name).await
                }

                StatementKind::Merge(stmt) => {
                    // Extract source SQL from the MERGE statement and execute it
                    // to get the source batches, then pass them to the write handler.
                    let source_sql = if let Statement::Merge(merge) = stmt.as_ref() {
                        match &merge.source {
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
                    // The MERGE source's policy summary is recorded against the
                    // outer MERGE statement's audit entry via `policy_summary`.
                    let source_batches = self
                        .execute_query(session, &source_sql, &query_id, &plan_metrics, &mut merge_source_plan, &mut policy_summary)
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
                    self.handle_detach(stmt).await
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

        // Race the execution against:
        //   - the configured query timeout
        //   - the per-query cancellation token (admin CancelQuery action)
        // The first one to fire wins. Cancellation skips the failed() write
        // because the tracker's cancel() already transitioned to Canceled.
        let result = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                warn!(
                    query_id = %query_id,
                    username = %session.user.username,
                    "Query cancelled by admin or client"
                );
                Err(SqeError::Execution("Query cancelled".to_string()))
            }
            inner = tokio::time::timeout(timeout_duration, execution_future) => {
                match inner {
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
                }
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

        let execution_ms = duration.as_millis() as u64;
        let tt_for_complete: Vec<String> = match captured_plan.as_ref() {
            Some(sqe_lineage::PlanOrHint::Plan(p)) => {
                sqe_lineage::extract::extract_table_names(p.as_ref())
            }
            _ => Vec::new(),
        };
        // Hoist plan metrics so they are in scope at the audit emit site below,
        // regardless of whether the query succeeded or failed.
        let pm = plan_metrics.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if result.is_ok() {
            self.query_tracker.complete(
                &query_id,
                rows,
                execution_ms,
                tt_for_complete.clone(),
                pm.bytes_scanned,
                pm.rows_scanned,
                pm.spill_bytes,
                pm.peak_memory_bytes,
            );

            if let Some(ref cache) = self.query_cache {
                if matches!(&kind, StatementKind::Query(_)) {
                    if let Ok(ref batches) = result {
                        cache.store(
                            &session.user.username,
                            sql,
                            query_id,
                            batches.clone(),
                            tt_for_complete.clone(),
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
                        if let Statement::Update(update) = stmt.as_ref() {
                            let table_name = update.table.relation.to_string();
                            cache.invalidate(&table_name);
                        }
                    }
                    StatementKind::Merge(stmt) => {
                        if let Statement::Merge(merge) = stmt.as_ref() {
                            let table_name = merge.table.to_string();
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
            let error_code = match &result {
                Err(e) => e.error_code().name(),
                Ok(_) => "",
            };
            metrics
                .query_count
                .with_label_values(&[status, &kind_name, error_code])
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

        // Resolve resources from the plan using structured TableReferences so
        // audit entries carry fully-qualified `catalog.ns.table` names.
        // `session.default_catalog` fills missing catalog components for Bare
        // and Partial references; when absent, `config.query.default_catalog`
        // is the fallback.
        //
        // NOTE: the streaming path (execute_stream, ~line 1777) still uses
        // `sqe_lineage::extract::extract_table_names` for the tracker/cache
        // path. Upgrading that site is a follow-on task (its `tables_touched`
        // feeds query_cache invalidation, so a format change there needs a
        // separate review).
        // `effective_catalog_buf` owns the resolved catalog string when the
        // session does not carry an explicit default. The borrow `default_catalog`
        // points into either the session string or this buffer; keeping them
        // separate makes the lifetime explicit rather than hiding it behind an
        // underscore-prefixed "unused" variable.
        let effective_catalog_buf: Option<String> = if session.default_catalog.is_none() {
            Some(self.config.resolve_default_catalog())
        } else {
            None
        };
        let default_catalog: Option<&str> = session
            .default_catalog
            .as_deref()
            .or(effective_catalog_buf.as_deref());
        let audit_resources: Vec<sqe_metrics::audit::Resource> = match captured_plan.as_ref() {
            Some(sqe_lineage::PlanOrHint::Plan(p)) => {
                crate::audit_resources::resources_from_plan(p.as_ref(), default_catalog)
            }
            _ => Vec::new(),
        };
        let tables_touched: Vec<String> = audit_resources.iter().map(|r| r.fqn()).collect();

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
                                bearer: Some(session.access_token().clone()),
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
                                bearer: Some(session.access_token().clone()),
                            },
                            session_id: session.id.clone(),
                            started_at: ol_started_at,
                            ended_at: ol_ended_at,
                            duration: ol_duration,
                            statement_kind: kind_name.clone(),
                            // SQL-06: emit the SANITIZED client message to the
                            // lineage sink, not the raw error. `e.to_string()`
                            // can carry file paths, S3 URIs, schema/column names,
                            // partition values, and literal data fragments that
                            // the error-sanitization layer suppresses from the
                            // client. Lineage sinks (JSONL/HTTP/Marquez) are a
                            // different trust boundary, so they get the same
                            // sanitized text the SQL client sees. Mirrors
                            // query_tracker.rs:214.
                            error_message: e.client_message(),
                            plan: captured_plan.take(),
                        });
                    }
                }
            }
        }

        if let Some(ref audit) = self.audit {
            // Route SELECT/DQL (StatementKind::Query) through the canonical
            // AuditEvent path so the event carries a fully-populated Actor,
            // structured Resource list, and typed policy/timing/stats blocks.
            //
            // GRANT/REVOKE are routed through the canonical AuditKind::Grant
            // path (Task 14). The raw SQL text in query.text carries grantee
            // info; the worker thread's redact_pii pass sanitises it before
            // chain stamping. No secrets travel in GRANT/REVOKE SQL so the
            // redacted-legacy path is not required for these statement kinds.
            //
            // Secret-bearing kinds (CREATE/DROP/SHOW SECRET, ATTACH, DETACH)
            // stay on the legacy log(&AuditEntry) path: it applies PII redaction
            // before writing, which is critical for statements that carry bearer
            // tokens or credentials in their SQL text. All other DDL/DML/admin
            // kinds emit canonical AuditEvents via log_event (the worker thread
            // redacts query text there too); DML maps to Query, the rest to
            // AdminDdl.
            if matches!(&kind, StatementKind::Query(_)) {
                let ps = policy_summary.unwrap_or_default();

                let outcome = match &result {
                    Ok(_) => sqe_metrics::audit::Outcome::Success,
                    Err(e) => sqe_metrics::audit::Outcome::Failure {
                        error_type: Some(e.error_code().trino_error_type().to_string()),
                        error_code: Some(e.error_code().name().to_string()),
                        message: Some(e.client_message()),
                    },
                };

                let policy = if ps.row_filters_applied > 0
                    || !ps.columns_masked.is_empty()
                    || !ps.columns_restricted.is_empty()
                    || ps.denied
                {
                    Some(sqe_metrics::audit::PolicyAudit {
                        row_filters_applied: ps.row_filters_applied,
                        columns_masked: ps.columns_masked,
                        columns_restricted: ps.columns_restricted,
                        denied: ps.denied,
                    })
                } else {
                    None
                };

                let actor = sqe_metrics::audit::Actor::from_parts(
                    session.user.username.clone(),
                    session.user.subject.clone(),
                    session.user.email.clone(),
                    session.user.roles.clone(),
                    session.user.groups.clone(),
                );

                let event = sqe_metrics::audit::AuditEvent {
                    time: chrono::Utc::now(),
                    kind: sqe_metrics::audit::AuditKind::Query,
                    actor,
                    outcome,
                    resources: audit_resources,
                    policy,
                    timing: Some(sqe_metrics::audit::Timing {
                        duration_ms: duration.as_millis() as u64,
                        // queued_ms and planning_ms are tracked by QueryTracker
                        // but no per-id getter is exposed at this call site.
                        // They default to 0 here; a follow-up can expose a
                        // QueryTracker::timing_for(id) if needed.
                        queued_ms: 0,
                        planning_ms: 0,
                        execution_ms,
                    }),
                    stats: Some(sqe_metrics::audit::QueryStats {
                        rows_returned: rows,
                        bytes_scanned: pm.bytes_scanned,
                        rows_scanned: pm.rows_scanned,
                        spill_bytes: pm.spill_bytes,
                        peak_memory_bytes: pm.peak_memory_bytes,
                    }),
                    query: Some(sqe_metrics::audit::QueryInfo {
                        // Pass sql text directly. The worker thread always applies
                        // redact_pii to query.text before chain stamping and writing.
                        // When GDPR config is active, GDPR-tag masking runs additionally
                        // via apply_gdpr_masking (which calls redact_pii internally).
                        // Do NOT redact here: caller-side redaction would double-apply.
                        text: Some(sql.to_string()),
                        query_hash: sqe_metrics::audit::query_hash(sql),
                        statement_type: kind_name,
                    }),
                    session_id: Some(session.id.clone()),
                    client_ip: client_ip.clone(),
                    integrity: sqe_metrics::audit::Integrity::default(),
                };
                audit.log_event(event);
            } else if matches!(&kind, StatementKind::Grant(_) | StatementKind::Revoke(_)) {
                // Canonical path for GRANT/REVOKE (Task 14, OCSF Account Change 3001).
                // Build a Resource from the parsed grant statement so the target object
                // is structured. The raw SQL travels in query.text where the worker
                // thread's redact_pii pass handles sanitisation.
                let grant_resources = kind
                    .statement()
                    .and_then(|ast_stmt| Self::extract_grant_statement(ast_stmt).ok())
                    .map(|gs| {
                        let name = gs.table
                            .clone()
                            .or_else(|| gs.namespace.clone())
                            .or_else(|| gs.catalog.clone())
                            .unwrap_or_else(|| "*".to_string());
                        vec![sqe_metrics::audit::Resource {
                            catalog: gs.catalog,
                            namespace: gs.namespace
                                .map(|n| vec![n])
                                .unwrap_or_default(),
                            name,
                            object_type: sqe_metrics::audit::ObjectType::Table,
                        }]
                    })
                    .unwrap_or_default();

                let grant_outcome = match &result {
                    Ok(_) => sqe_metrics::audit::Outcome::Success,
                    Err(e) => sqe_metrics::audit::Outcome::Failure {
                        error_type: Some(e.error_code().trino_error_type().to_string()),
                        error_code: Some(e.error_code().name().to_string()),
                        message: Some(e.client_message()),
                    },
                };

                let grant_actor = sqe_metrics::audit::Actor::from_parts(
                    session.user.username.clone(),
                    session.user.subject.clone(),
                    session.user.email.clone(),
                    session.user.roles.clone(),
                    session.user.groups.clone(),
                );

                let grant_event = sqe_metrics::audit::AuditEvent {
                    time: chrono::Utc::now(),
                    kind: sqe_metrics::audit::AuditKind::Grant,
                    actor: grant_actor,
                    outcome: grant_outcome,
                    resources: grant_resources,
                    policy: None,
                    timing: Some(sqe_metrics::audit::Timing {
                        duration_ms: duration.as_millis() as u64,
                        queued_ms: 0,
                        planning_ms: 0,
                        execution_ms: 0,
                    }),
                    stats: None,
                    query: Some(sqe_metrics::audit::QueryInfo {
                        text: Some(sql.to_string()),
                        query_hash: sqe_metrics::audit::query_hash(sql),
                        statement_type: kind_name,
                    }),
                    session_id: Some(session.id.clone()),
                    client_ip: client_ip.clone(),
                    integrity: sqe_metrics::audit::Integrity::default(),
                };
                audit.log_event(grant_event);
            } else if matches!(
                &kind,
                StatementKind::CreateSecret(_)
                    | StatementKind::DropSecret(_)
                    | StatementKind::ShowSecrets
                    | StatementKind::Attach(_)
                    | StatementKind::Detach(_)
            ) {
                // Secret-bearing statements stay on the redacted legacy path.
                // PII redaction runs inside log() before the entry is written.
                // Never route these through log_event: the legacy path is the
                // established redaction contract for bearer tokens and catalog
                // credentials embedded in SQL text.
                let ps = policy_summary.unwrap_or_default();
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
                    tables_touched: tables_touched.clone(),
                    row_filters_applied: ps.row_filters_applied,
                    columns_masked: ps.columns_masked,
                    columns_restricted: ps.columns_restricted,
                    policy_denied: ps.denied,
                });
            } else {
                // Canonical path for DDL, DML, and admin statements.
                // Non-secret SQL travels through log_event; the worker thread
                // applies redact_pii before chain-stamping and writing.
                let audit_kind = if matches!(
                    &kind,
                    StatementKind::Insert(_)
                        | StatementKind::Ctas(_)
                        | StatementKind::Merge(_)
                        | StatementKind::Delete(_)
                        | StatementKind::Update(_)
                        | StatementKind::Truncate(_)
                ) {
                    sqe_metrics::audit::AuditKind::Query
                } else {
                    sqe_metrics::audit::AuditKind::AdminDdl
                };

                let ddl_outcome = match &result {
                    Ok(_) => sqe_metrics::audit::Outcome::Success,
                    Err(e) => sqe_metrics::audit::Outcome::Failure {
                        error_type: Some(e.error_code().trino_error_type().to_string()),
                        error_code: Some(e.error_code().name().to_string()),
                        message: Some(e.client_message()),
                    },
                };

                let ddl_actor = sqe_metrics::audit::Actor::from_parts(
                    session.user.username.clone(),
                    session.user.subject.clone(),
                    session.user.email.clone(),
                    session.user.roles.clone(),
                    session.user.groups.clone(),
                );

                let ddl_event = sqe_metrics::audit::AuditEvent {
                    time: chrono::Utc::now(),
                    kind: audit_kind,
                    actor: ddl_actor,
                    outcome: ddl_outcome,
                    resources: audit_resources,
                    policy: None,
                    timing: Some(sqe_metrics::audit::Timing {
                        duration_ms: duration.as_millis() as u64,
                        queued_ms: 0,
                        planning_ms: 0,
                        execution_ms: 0,
                    }),
                    stats: None,
                    query: Some(sqe_metrics::audit::QueryInfo {
                        text: Some(sql.to_string()),
                        query_hash: sqe_metrics::audit::query_hash(sql),
                        statement_type: kind_name,
                    }),
                    session_id: Some(session.id.clone()),
                    client_ip: client_ip.clone(),
                    integrity: sqe_metrics::audit::Integrity::default(),
                };
                audit.log_event(ddl_event);
            }
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
        client_ip: Option<String>,
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

        // Acquire owned concurrency permits so they can be moved into the
        // stream wrapper and released when the client finishes draining.
        // The per-user permit is acquired first; if granted, the global
        // permit is acquired second. A user that holds N per-user permits
        // (each tied to an open stream) still counts N times against the
        // global ceiling.
        let mut permits: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
        if self.config.query.max_concurrent_per_user > 0 {
            let username = session.user.username.clone();
            let sem = self
                .per_user_semaphores
                .entry(username.clone())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        self.config.query.max_concurrent_per_user,
                    ))
                })
                .clone();
            match sem.try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => {
                    return Err(SqeError::Execution(format!(
                        "Too many concurrent queries for user '{}' ({} active). Please retry later.",
                        username, self.config.query.max_concurrent_per_user
                    )));
                }
            }
        }
        if let Some(ref sem) = self.query_semaphore {
            match Arc::clone(sem).try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(_) => {
                    return Err(SqeError::Execution(format!(
                        "Too many concurrent queries ({} active). Please retry later.",
                        self.config.query.max_concurrent_queries
                    )));
                }
            }
        }

        // Per-user memory reservation. Carried by the streaming wrapper so
        // it releases when the result stream drops, not when the planning
        // call returns. Without this, large multi-second streams would
        // appear to free their budget immediately on admission.
        let per_user_mem_reservation = if self.per_user_memory_budget_bytes > 0
            && self.per_query_memory_bytes > 0
        {
            let username = session.user.username.clone();
            match self.per_user_memory.try_reserve(
                &username,
                self.per_query_memory_bytes,
                self.per_user_memory_budget_bytes,
            ) {
                Some(r) => Some(r),
                None => {
                    let used = self.per_user_memory.used_bytes(&username);
                    return Err(SqeError::Execution(format!(
                        "Per-user memory budget exceeded for '{}': {} bytes reserved, \
                         limit {} bytes. Wait for in-flight queries to complete.",
                        username, used, self.per_user_memory_budget_bytes
                    )));
                }
            }
        } else {
            None
        };

        // --- Classify ---------------------------------------------------------
        // See execute() for the pipeline rationale. The typed wrapper proves
        // at compile time that the classifier sees normalized SQL only
        // (issue #117).
        let kind = sqe_sql::parse_and_classify_typed(
            &sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql))?,
        )?;
        let kind_name = kind.name().to_string();
        if !matches!(kind, StatementKind::Query(_)) {
            return Err(SqeError::NotImplemented(
                "execute_stream only supports SELECT queries; \
                 use execute() for DML and metadata statements"
                    .into(),
            ));
        }

        // Pre-flight: same unknown-catalog-qualifier check + lazy Polaris
        // discovery as execute(). See `preflight_resolve_catalogs` for the
        // rationale.
        if let Some(stmt) = kind.statement() {
            self.preflight_resolve_catalogs(stmt, session).await?;
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
        let cancel_token = self.query_tracker.start(
            query_id,
            &session.user.username,
            session.source.as_deref(),
            sql,
            &session.id,
            client_ip.as_deref(),
            session.user.roles.clone(),
        );

        match self.open_stream(session, sql, &query_id, start).await {
            Ok((schema, inner_stream, final_plan, tt_cleanup, tables_touched, policy_summary, audit_resources)) => {
                let actor = sqe_metrics::audit::Actor::from_parts(
                    session.user.username.clone(),
                    session.user.subject.clone(),
                    session.user.email.clone(),
                    session.user.roles.clone(),
                    session.user.groups.clone(),
                );
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
                    tables_touched,
                    policy_summary,
                    profile_mode: self.profile_mode,
                    actor,
                    resources: audit_resources,
                    client_ip: client_ip.clone(),
                };

                let tracked = crate::streaming::TrackedRecordBatchStream::with_permits_reservation_and_cancel_token(
                    inner_stream,
                    finalizer,
                    permits,
                    per_user_mem_reservation,
                    cancel_token,
                )
                .with_teardown(tt_cleanup)
                .with_idle_timeout(std::time::Duration::from_secs(
                    self.config.query.stream_idle_timeout_secs,
                ));
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
                        .with_label_values(&["error", &kind_name, e.error_code().name()])
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
        TimeTravelCleanup,
        Vec<String>,
        sqe_policy::PolicySummary,
        Vec<sqe_metrics::audit::Resource>,
    )> {
        let (ctx, session_catalog) = self.create_session_context(session).await?;

        let sql = self.handle_incremental(sql, &ctx, &session_catalog).await?;
        let (sql, tt_cleanup) =
            self.handle_time_travel(&sql, &ctx, session, &session_catalog).await?;
        // Apply Trino-compat AST rewrites before planning, matching the
        // execute() path. Today this matters for the empty-input ROLLUP /
        // CUBE / GROUPING SETS wrap that works around apache/datafusion#21570;
        // CAST(v AS JSON) -> to_json(v) and the $-suffix metadata-table
        // rewrite also apply here when present.
        let sql = sqe_sql::rewrite_trino_compat(&sql);
        let sql = sqe_sql::rewrite_named_tvf_args(&sql);
        let sql = sql.as_str();

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;
        let plan = df.logical_plan().clone();
        // Streaming path: the audit entry is recorded by the
        // TrackedRecordBatchStream finalizer (not the `execute` path), so carry
        // the policy summary out to the caller, which stows it on the
        // StreamFinalizer for the audit emission at stream end.
        let (enforced_plan, policy_summary) =
            self.policy_enforcer.evaluate(&session.user, plan).await?;
        debug!("Policy-enforced plan (streaming): {:?}", enforced_plan);
        // Compute the structured resource list from the logical plan now,
        // before it is consumed by `execute_logical_plan`. The streaming
        // finalizer needs these to emit a canonical `AuditEvent`; there is
        // no opportunity to recover the logical plan later in the pipeline.
        let effective_catalog_buf: Option<String> = if session.default_catalog.is_none() {
            Some(self.config.resolve_default_catalog())
        } else {
            None
        };
        let default_catalog_str: Option<&str> = session
            .default_catalog
            .as_deref()
            .or(effective_catalog_buf.as_deref());
        let audit_resources =
            crate::audit_resources::resources_from_plan(&enforced_plan, default_catalog_str);
        let tables_touched = sqe_lineage::extract::extract_table_names(&enforced_plan);

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

        // Probe-side scan parallelization (issue #235). Opt-in: bumps the
        // probe-side Iceberg scan of CollectLeft joins to N output partitions
        // so the fact-table decode runs across cores; build-side scans are
        // never touched (the q72 regression guard). Default off until validated
        // on a clean (non-swapping) benchmark rig. Uses the real session config
        // so EnforceDistribution sees the configured target_partitions.
        let physical_plan = if self.config.query.parallel_probe_scan {
            let state = ctx.state();
            let rule = crate::parallel_probe_scan::ParallelProbeScanRule::new();
            match rule.optimize(physical_plan.clone(), state.config_options()) {
                Ok(optimized) => optimized,
                Err(e) => {
                    debug!(error = %e, "Probe-side scan parallelization failed, using original plan");
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

        // Distribute scan across workers if possible.
        let final_plan = self.try_distribute(physical_plan, session, query_id).await;

        // Planning complete — promote tracker to Running
        self.query_tracker
            .running(query_id, start.elapsed().as_millis() as u64);

        // Remove dynamic filters from Iceberg self-joins (q95-class inlist blowup).
        let final_plan = strip_self_join_dynamic_filters(final_plan);
        let schema = final_plan.schema();
        let stream = execute_stream(Arc::clone(&final_plan), ctx.task_ctx())
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

        Ok((schema, stream, final_plan, tt_cleanup, tables_touched, policy_summary, audit_resources))
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
        // Run the pre-parse pipeline before handing to the classifier;
        // see `pipeline_types.rs` for the trust-boundary contract (issue #117).
        let kind = sqe_sql::parse_and_classify_typed(
            &sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql))?,
        )?;

        if matches!(kind, StatementKind::Query(_)) {
            // get_flight_info plans the result schema WITHOUT executing, so it
            // must mirror execute()/execute_stream() and discover Polaris
            // warehouses for the caller's token first. Otherwise a 3-part id
            // into a workspace catalog (ws_*, dedicated team catalogs) fails
            // "table not found" during the planning below, before do_get ->
            // execute_stream (which discovers) ever runs. preflight registers
            // the discovered warehouse on the per-token cached SessionContext
            // that create_session_context returns here, so it is visible to the
            // planner. kind.statement() is None for non-statement kinds (no-op).
            if let Some(stmt) = kind.statement() {
                self.preflight_resolve_catalogs(stmt, session).await?;
            }
            let (ctx, session_catalog) = self.create_session_context(session).await?;
            // Register incremental providers so the planner can resolve the
            // tables with their augmented schemas. The FOR INCREMENTAL clause
            // must be stripped before handle_time_travel sees it because that
            // step parses the SQL with sqlparser.
            let sql_for_plan = self
                .handle_incremental(sql, &ctx, &session_catalog)
                .await?;
            let (sql_for_plan, _tt_cleanup) = self
                .handle_time_travel(&sql_for_plan, &ctx, session, &session_catalog)
                .await?;
            let df = ctx
                .sql(&sql_for_plan)
                .await
                .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;
            // `_tt_cleanup` drops here, deregistering any pinned providers
            // that schema-resolution required. Schema-only paths never read
            // batches, so dropping right after `df.schema()` is fine.
            Ok(Arc::new(df.schema().as_arrow().clone()))
        } else {
            // Non-query statements: return empty schema. The actual execution
            // happens in do_get_statement via execute().
            Ok(Arc::new(Schema::empty()))
        }
    }

    /// Describe a prepared statement: its output columns (`DESCRIBE OUTPUT`) or
    /// its bind parameters (`DESCRIBE INPUT`). Returns a synthetic result set
    /// matching Trino's column layout. `prepared_sql` is the statement template
    /// with `?` placeholders; they are numbered to `$1..$N` so DataFusion can
    /// plan it (inferring the output schema and placeholder types) without
    /// bound values. (#3)
    pub async fn describe_prepared(
        &self,
        session: &Session,
        prepared_sql: &str,
        kind: sqe_trino_compat::protocol::DescribeKind,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqe_trino_compat::protocol::DescribeKind;
        let (numbered_sql, param_count) = sqe_core::number_placeholders(prepared_sql);
        match kind {
            DescribeKind::Output => {
                let schema = self.get_schema(session, &numbered_sql).await?;
                build_describe_output(&schema)
            }
            DescribeKind::Input => {
                let param_types = self
                    .prepared_parameter_types(session, &numbered_sql, param_count)
                    .await?;
                build_describe_input(&param_types)
            }
        }
    }

    /// Plan a (placeholder-numbered) statement and return the inferred type of
    /// each bind parameter in positional order. `None` for a parameter whose
    /// type DataFusion could not infer.
    async fn prepared_parameter_types(
        &self,
        session: &Session,
        numbered_sql: &str,
        param_count: usize,
    ) -> sqe_core::Result<Vec<Option<DataType>>> {
        let kind = sqe_sql::parse_and_classify_typed(
            &sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(numbered_sql))?,
        )?;
        if let Some(stmt) = kind.statement() {
            self.preflight_resolve_catalogs(stmt, session).await?;
        }
        let (ctx, _session_catalog) = self.create_session_context(session).await?;
        let df = ctx
            .sql(numbered_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;
        let types_map = df
            .logical_plan()
            .get_parameter_types()
            .map_err(|e| SqeError::Execution(format!("parameter type inference failed: {e}")))?;
        Ok((1..=param_count)
            .map(|i| types_map.get(&format!("${i}")).cloned().flatten())
            .collect())
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
        summary_out: &mut Option<sqe_policy::PolicySummary>,
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
        // strip the temporal clause. `_tt_cleanup` deregisters the pinned
        // providers when this function returns, so the bare table name
        // resolves to HEAD on the next query in the same session (#44).
        let (sql, _tt_cleanup) = self
            .handle_time_travel(&sql, &ctx, session, &session_catalog)
            .await?;
        // Pre-process Trino-compat AST patterns DataFusion does not natively
        // recognize. Today this only rewrites `CAST(v AS JSON)` to
        // `to_json(v)`; the rewriter is a no-op when the input does not
        // contain `as json` (case-insensitive). Errors during parse fall
        // through as the original string so DataFusion produces its own
        // error message.
        let sql = sqe_sql::rewrite_trino_compat(&sql);
        let sql = sqe_sql::rewrite_named_tvf_args(&sql);
        let sql = sql.as_str();

        // Plan the query via DataFusion's SQL planner
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        // Get the logical plan and run policy enforcement
        let plan = df.logical_plan().clone();
        let (enforced_plan, policy_summary) = self
            .policy_enforcer
            .evaluate(&session.user, plan)
            .await?;

        debug!("Policy-enforced plan: {:?}", enforced_plan);

        // Surface the policy summary to the audit log (out-param, same pattern as
        // `plan_out`). The caller copies the counts/names/denied flag into the
        // AuditEntry so a deny-all (zero rows) is distinguishable from a
        // legitimate empty result.
        *summary_out = Some(policy_summary);

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
        // Remove dynamic filters from Iceberg self-joins (q95-class inlist blowup).
        let final_plan = strip_self_join_dynamic_filters(final_plan);
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

        let iceberg_scan = match scan_node.downcast_ref::<IcebergScanExec>() {
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

        // 6. Get projected columns + Iceberg field IDs from the scan. Field
        // IDs (#43) let the worker project by the parquet PARQUET:field_id
        // metadata key, so RENAME COLUMN / ADD COLUMN against post-evolution
        // schema still resolves to the right parquet column in pre-evolution
        // files. Names stay as a fallback for old workers and files without
        // field IDs.
        //
        // Worker-side projection is safe again: the !327 failure ("number of
        // columns(N) must match number of fields(M)") was NOT a coordinator
        // schema mismatch -- `scan_node.schema()` is the projected schema, and
        // it is what DistributedScanExec advertises below. The real bug was the
        // worker's streaming path advertising `builder.schema()` (the full
        // parquet file schema) over Flight while shipping projected batches;
        // the coordinator-side Flight DECODE then failed before reassembly ever
        // ran. Fixed in sqe-worker::executor::open_parquet_stream (schema now
        // taken from the built, projected stream). The reassembly path keeps a
        // by-name safety net for order/width drift (workers emit projected
        // columns in parquet FILE order, which may differ from the plan's
        // projection order).
        let (projected_cols, projected_field_ids) = scan_task_projection(
            iceberg_scan.projection(),
            iceberg_scan.table().metadata().current_schema(),
        );

        // 6b. Push the scan predicate and (when safe) the query LIMIT into each
        // ScanTask (#233). Both are pure optimizations: the authoritative
        // FilterExec / GlobalLimitExec remain above DistributedScanExec (the
        // IcebergScanExec rejects static filter pushdown, so the FilterExec is
        // never elided), so a worker that double-filters or over-counts a
        // per-fragment limit cannot change results. Gated by config so the
        // pushdown can be disabled without a redeploy.
        let (predicate_proto, scan_limit): (Option<Vec<u8>>, Option<usize>) =
            if self.config.query.distributed_scan_pushdown {
                let pred = crate::scan_pushdown::serialize_scan_predicate(
                    iceberg_scan.df_filters(),
                );
                let lim = crate::scan_pushdown::extract_pushable_limit(&plan, &scan_node);
                if pred.is_some() || lim.is_some() {
                    debug!(
                        has_predicate = pred.is_some(),
                        limit = ?lim,
                        "Scan pushdown: populating ScanTask predicate/limit"
                    );
                }
                (pred, lim)
            } else {
                (None, None)
            };

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
                    projected_field_ids: projected_field_ids.clone(),
                    s3_endpoint: storage.s3_endpoint.clone(),
                    s3_region: storage.s3_region.clone(),
                    s3_access_key: storage.s3_access_key.clone(),
                    s3_secret_key: storage.s3_secret_key.expose().to_string(),
                    s3_session_token: String::new(),
                    s3_path_style: storage.s3_path_style,
                    s3_allow_http: storage.s3_endpoint.starts_with("http://"),
                    predicate_proto: predicate_proto.clone(),
                    limit: scan_limit,
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

        let worker_infos: Vec<crate::scheduler::WorkerInfo> = healthy
            .iter()
            .map(|url| crate::scheduler::WorkerInfo {
                url: url.clone(),
                healthy: true,
                active_fragments: self.worker_load.in_flight(url),
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

        let worker_urls: Vec<String> = assignments.iter().map(|a| a.worker_url.clone()).collect();

        let reservations: Arc<dashmap::DashMap<String, crate::worker_registry::ReservationGuard>> =
            Arc::new(dashmap::DashMap::new());
        for (task, url) in scan_tasks.iter().zip(worker_urls.iter()) {
            reservations.insert(task.fragment_id.clone(), self.worker_load.reserve(url));
        }

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

        let tracker = self.query_tracker.clone();
        let qid = *query_id;
        let callback_metrics = self.metrics.clone();
        let reservations_cb = reservations.clone();
        let callback: crate::distributed_scan::FragmentCallback =
            Arc::new(move |task_id, success, elapsed_ms, rows| {
                let _ = reservations_cb.remove(task_id);
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
        .with_fragment_callback(callback)
        .with_pushed_down_filters(iceberg_scan.pushed_down_filters().to_vec())
        .with_worker_secret(self.config.coordinator.worker_secret.expose().to_string())
        .with_timeouts(
            std::time::Duration::from_secs(self.config.coordinator.worker_connect_timeout_secs),
            std::time::Duration::from_secs(self.config.coordinator.worker_rpc_timeout_secs),
        );

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

        // Query information_schema.columns for the table's column definitions,
        // qualified by the table's catalog and schema so the lookup hits the
        // right catalog (a 3-part name, or a 2-part name under the session
        // catalog) instead of only the default. Without this the body came
        // back empty for tables outside the default catalog. (#2)
        let col_sql = info_schema_columns_query(&table_name);

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

    /// Handle SHOW CATALOGS by listing every catalog the session can reach:
    /// the configured catalogs plus the session's own (X-Trino-Catalog) Polaris
    /// warehouse, deduplicated. JDBC schema sync (DatabaseMetaData.getCatalogs)
    /// then sees workspace catalogs, not just the default warehouse. (#5)
    async fn handle_show_catalogs(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Configured catalogs first (default warehouse leads), then the session
        // catalog if it is not already in the list.
        let mut candidates: Vec<String> =
            self.config.flattened_catalogs().into_iter().map(|(n, _)| n).collect();
        if let Some(session_catalog) = session.default_catalog.as_deref() {
            candidates.push(session_catalog.to_string());
        }
        if candidates.is_empty() {
            candidates.push(self.config.catalog.warehouse.clone());
        }
        let mut catalog_names: Vec<String> = Vec::new();
        for name in candidates {
            let name = if name.is_empty() { "default".to_string() } else { name };
            if !catalog_names.contains(&name) {
                catalog_names.push(name);
            }
        }

        // Trino's `SHOW CATALOGS` column is named `Catalog`.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Catalog",
            DataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        for name in &catalog_names {
            builder.append_value(name);
        }
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
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // `SHOW SCHEMAS FROM <catalog>` must list that catalog's namespaces, not
        // the default warehouse's. Resolve the named catalog (discovered under
        // polaris-auto) so reads line up with the write path.
        let catalog = show_schemas_catalog(filter);
        let session_catalog = self.show_catalog(session, catalog.as_deref()).await?;

        // A principal that is not authorized to list this catalog's namespaces
        // gets an empty result, not a hard error -- so JDBC schema sync and
        // SHOW SCHEMAS skip catalogs the user can't see instead of aborting. (#5)
        let namespaces = match session_catalog.list_namespaces().await {
            Ok(ns) => ns,
            Err(e) if e.error_code() == sqe_core::SqeErrorCode::AccessDenied => Vec::new(),
            Err(e) => return Err(e),
        };

        // Trino's `SHOW SCHEMAS` column is named `Schema`.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Schema",
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
        self.columns_for_table(session, &table_name).await
    }

    /// Column metadata for a table via `information_schema.columns`, shared by
    /// `SHOW COLUMNS` and `DESCRIBE <table>`.
    async fn columns_for_table(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let col_sql = info_schema_columns_query(table_name);

        let (ctx, _) = self.create_session_context(session).await?;
        let df = ctx.sql(&col_sql).await.map_err(|e| {
            SqeError::Execution(format!(
                "failed to query information_schema.columns: {e}"
            ))
        })?;
        df.collect().await.map_err(|e| {
            SqeError::Execution(format!(
                "failed to collect column metadata: {e}"
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
        // `SHOW TABLES FROM <catalog>.<schema>` must list tables from that
        // catalog, not the default warehouse. Resolve the leading catalog
        // qualifier (discovered under polaris-auto); the schema part is parsed
        // separately by parse_show_tables_namespace.
        let catalog = show_tables_catalog(filter);
        let session_catalog = self.show_catalog(session, catalog.as_deref()).await?;

        // Explicit `FROM <schema>` wins; otherwise fall back to the session
        // schema (X-Trino-Schema) so bare `SHOW TABLES` lists that schema's
        // tables (Trino behavior). Only when neither is set list all
        // namespaces. (#7)
        let ns_name =
            parse_show_tables_namespace(filter).or_else(|| session.default_schema.clone());
        let namespaces = match ns_name {
            None => session_catalog.list_namespaces().await?,
            Some(name) => vec![iceberg::NamespaceIdent::new(name)],
        };

        // Trino's `SHOW TABLES` returns a SINGLE column named `Table` holding
        // bare table names (the schema is the FROM / session-schema context).
        // SQE previously returned two columns [namespace, table_name]; the
        // Trino / Starburst JDBC driver reads column 0 as the table name, so
        // the leading namespace column made every row collapse to the namespace
        // (e.g. Metabase schema sync saw one malformed `gold.` entry instead of
        // the real tables). Match Trino exactly: one `Table` column.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Table",
            DataType::Utf8,
            false,
        )]));

        let mut names: Vec<String> = Vec::new();
        for ns in &namespaces {
            match session_catalog.list_tables(ns).await {
                Ok(tables) => {
                    for table in &tables {
                        names.push(table.name().to_string());
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
        // Trino lists table names sorted.
        names.sort();

        let mut table_builder = StringBuilder::new();
        for name in &names {
            table_builder.append_value(name);
        }
        let table_array: ArrayRef = Arc::new(table_builder.finish());

        let batch = RecordBatch::try_new(schema, vec![table_array])
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
            Statement::CreateView(cv) => &cv.query,
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
    /// Returns the rewritten SQL plus a [`TimeTravelCleanup`] guard whose
    /// Drop deregisters every alias that was registered for this query. The
    /// caller must hold the guard across query execution so the pinned
    /// provider doesn't leak into subsequent SQL in the same session (#44).
    ///
    /// When no time travel is found the original SQL is returned unchanged
    /// and the guard is empty.
    /// Resolve which catalog a (possibly catalog-qualified) time-travel table
    /// reference loads against, mirroring the SELECT/SHOW resolution (#2/#6):
    /// an explicit `catalog.schema.table` uses that catalog; otherwise the
    /// session's bound default catalog (discovered under polaris-auto). The
    /// time-travel path previously loaded against the primary/config catalog,
    /// so a polaris-auto session querying a discovered catalog failed with
    /// "table does not exist" (#317). Falls back to `fallback` only when the
    /// reference names no catalog and the session has no default.
    async fn resolve_reference_catalog(
        &self,
        session: &Session,
        table: &str,
        fallback: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
        let explicit = explicit_catalog_component(table);
        if explicit.is_none() && session.default_catalog.is_none() {
            return Ok(Arc::clone(fallback));
        }
        self.show_catalog(session, explicit).await
    }

    async fn handle_time_travel(
        &self,
        sql: &str,
        ctx: &SessionContext,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<(String, TimeTravelCleanup)> {
        use sqlparser::ast::SetExpr;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let mut cleanup_aliases: Vec<String> = Vec::new();
        let prefetch_concurrency = self.config.storage.prefetch_concurrency;

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
                // Resolve the catalog the table lives in, like the SELECT/SHOW
                // paths do (#2/#6). A time-travel reference loads against the
                // session's bound catalog -- or, for a catalog-qualified name,
                // that catalog -- not the primary/config catalog. Using the
                // primary made polaris-auto sessions fail with "table does not
                // exist" (#317).
                let resolved_catalog = self
                    .resolve_reference_catalog(session, &spec.table, session_catalog)
                    .await?;
                let alias =
                    Self::apply_version_spec(spec, ctx, &resolved_catalog, prefetch_concurrency)
                        .await?;
                cleanup_aliases.push(alias.clone());
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
            return Ok((sql_for_ast, TimeTravelCleanup::new(ctx, cleanup_aliases)));
        }

        let stmt = &mut statements[0];
        let mut found_time_travel = false;
        let cleanup = &mut cleanup_aliases;

        if let sqlparser::ast::Statement::Query(ref mut query) = stmt {
            if let SetExpr::Select(ref mut select) = *query.body {
                for twj in &mut select.from {
                    if self.process_time_travel_table_factor(
                        &mut twj.relation,
                        ctx,
                        session,
                        session_catalog,
                        cleanup,
                        prefetch_concurrency,
                    ).await? {
                        found_time_travel = true;
                    }
                    for join in &mut twj.joins {
                        if self.process_time_travel_table_factor(
                            &mut join.relation,
                            ctx,
                            session,
                            session_catalog,
                            cleanup,
                            prefetch_concurrency,
                        ).await? {
                            found_time_travel = true;
                        }
                    }
                }
            }
        }

        let guard = TimeTravelCleanup::new(ctx, cleanup_aliases);
        if found_time_travel {
            Ok((statements[0].to_string(), guard))
        } else if version_resolved {
            Ok((sql_for_ast, guard))
        } else {
            version_resolved = false;
            let _ = version_resolved; // silence unused warning if flow changes
            Ok((sql.to_string(), guard))
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
        prefetch_concurrency: usize,
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
            VersionRef::Timestamp(raw) => {
                // `FOR TIMESTAMP AS OF` / `FOR SYSTEM_TIME AS OF`: parse the
                // captured argument (a `TIMESTAMP '...'` literal, a quoted
                // string, or epoch millis) with the same resolver the AST path
                // uses, then pick the latest snapshot at or before that time.
                use sqlparser::dialect::GenericDialect;
                use sqlparser::parser::Parser;
                let expr = Parser::new(&GenericDialect {})
                    .try_with_sql(raw)
                    .and_then(|mut p| p.parse_expr())
                    .map_err(|e| {
                        SqeError::Execution(format!(
                            "FOR TIMESTAMP AS OF: cannot parse timestamp '{raw}': {e}"
                        ))
                    })?;
                let target_ms = resolve_timestamp_expr(&expr)?;
                let snapshot_id = find_snapshot_at_timestamp(metadata, target_ms)?;
                tracing::info!(
                    table = %spec.table,
                    target_ms,
                    snapshot_id,
                    "Time travel (FOR TIMESTAMP AS OF): pinned snapshot"
                );
                snapshot_id
            }
        };

        tracing::info!(
            table = %spec.table,
            snapshot_id,
            "Time travel (FOR VERSION AS OF): pinned snapshot"
        );

        let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
            .await?
            .with_snapshot_id(snapshot_id)
            .with_prefetch_concurrency(prefetch_concurrency);

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
    /// 3. Registers it under a unique alias `datafusion.public.__sqe_tt_<table>_<uuid>`
    /// 4. Rewrites the `TableFactor` name to point at the alias and strips
    ///    the `version` field so DataFusion doesn't see it
    ///
    /// Returning the qualified alias under `datafusion.public` (#44):
    /// - Earlier versions registered under the bare table name, leaking the
    ///   pinned provider into the session for subsequent queries (silent
    ///   stale reads).
    /// - Qualified names also avoid collisions between two namespaces that
    ///   happen to share a bare table name (`ns_a.t` vs `ns_b.t`).
    ///
    /// The alias is pushed into `cleanup` so the caller can deregister it
    /// once query execution completes.
    async fn process_time_travel_table_factor(
        &self,
        relation: &mut TableFactor,
        ctx: &SessionContext,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
        cleanup: &mut Vec<String>,
        prefetch_concurrency: usize,
    ) -> sqe_core::Result<bool> {
        use sqlparser::ast::TableVersion;

        if let TableFactor::Table { ref mut name, ref mut version, .. } = relation {
            if let Some(TableVersion::ForSystemTimeAsOf(ref expr)) = version {
                let table_name = name.to_string();
                let timestamp_ms = resolve_timestamp_expr(expr)?;

                // Resolve the catalog the same way the SELECT/SHOW paths do, so
                // a polaris-auto session loads from its bound catalog rather
                // than the primary/config one (#317).
                let session_catalog = &self
                    .resolve_reference_catalog(session, &table_name, session_catalog)
                    .await?;

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
                // under a unique alias under datafusion.public.
                let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
                    .await?
                    .with_snapshot_id(snapshot_id)
                    .with_prefetch_concurrency(prefetch_concurrency);

                let alias = format!(
                    "__sqe_tt_{}_{}",
                    bare_table,
                    uuid::Uuid::now_v7().simple()
                );
                let qualified = format!("datafusion.public.{alias}");
                ctx.register_table(qualified.as_str(), Arc::new(provider))
                    .map_err(|e| SqeError::Execution(format!(
                        "Failed to register time-travel provider for {bare_table}: {e}"
                    )))?;
                cleanup.push(qualified.clone());

                // Rewrite the AST to point at the qualified alias so the
                // planner resolves it under the writable `datafusion.public`
                // schema. Splitting on `.` reconstructs the ObjectName as
                // three Idents (catalog / schema / table).
                *name = sqlparser::ast::ObjectName::from(
                    qualified
                        .split('.')
                        .map(|part| sqlparser::ast::Ident::new(part.to_string()))
                        .collect::<Vec<_>>(),
                );

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

    /// Flush the policy decision cache after a policy-mutating statement so
    /// the change takes effect on the next query instead of lingering until
    /// the cache TTL elapses (default 60s, `[policy] cache_ttl_secs`).
    /// Issue #207.
    ///
    /// No-op when no `PolicyStore` is wired (the current production default;
    /// the OPA enforcer is gated behind AUTH-01). When OPA IS wired, the
    /// SAME `Arc<dyn PolicyStore>` must back both the `PolicyPlanRewriter`
    /// enforcer on the read path AND this `policy_store` field, otherwise the
    /// flush hits a different cache than the one the read path consults.
    fn invalidate_policy_cache(&self) {
        if let Some(store) = &self.policy_store {
            store.invalidate_all();
        }
    }

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
        // sqlparser 0.62: ObjectName holds `Vec<ObjectNamePart>` rather than
        // `Vec<Ident>`; pull the bare identifier value out of each part.
        fn object_name_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
            name.0
                .iter()
                .filter_map(|p| p.as_ident())
                .map(|id| id.value.clone())
                .collect()
        }

        let (privileges, objects, grantees) = match stmt {
            Statement::Grant(g) => (&g.privileges, &g.objects, &g.grantees),
            Statement::Revoke(r) => (&r.privileges, &r.objects, &r.grantees),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected GRANT/REVOKE statement, got: {other}"
                )));
            }
        };

        let privilege = format!("{privileges}");

        let (catalog, namespace, table) = match objects {
            Some(sqlparser::ast::GrantObjects::Tables(tables)) if !tables.is_empty() => {
                let name = &tables[0];
                let parts: Vec<String> = object_name_parts(name);
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
            Some(sqlparser::ast::GrantObjects::Schemas(schemas)) if !schemas.is_empty() => {
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            Some(sqlparser::ast::GrantObjects::AllTablesInSchema { schemas })
                if !schemas.is_empty() =>
            {
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            Some(sqlparser::ast::GrantObjects::FutureTablesInSchema { schemas })
                if !schemas.is_empty() =>
            {
                // Ranger has no "future-only" resource; a table wildcard ("*")
                // covers existing and future tables in the schema. We document
                // this as the SQE meaning of ON FUTURE TABLES.
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), Some("*".to_string())),
                    2 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some("*".to_string()),
                    ),
                    _ => (None, Some(name.to_string()), Some("*".to_string())),
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
            Some(sqlparser::ast::GranteeName::ObjectName(obj)) => {
                object_name_parts(obj).join(".")
            }
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
        // Gate behind the admin allowlist BEFORE touching the backend
        // (issue #204). In production the Polaris backend swaps the caller's
        // token for a service token scoped PRINCIPAL_ROLE:ALL, so an
        // ungated GRANT let any authenticated user self-escalate. The check
        // sits here, ahead of `require_grant_backend`, so the service-token
        // path is unreachable for non-admins.
        self.require_admin(session, "GRANT")?;
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        backend.grant(session.access_token().expose(), &grant_stmt).await?;
        // Flush the policy cache only after the mutation succeeds so the new
        // grant is visible on the next query (issue #207).
        self.invalidate_policy_cache();
        Ok(vec![])
    }

    /// Handle REVOKE by delegating to the configured grant backend.
    async fn handle_revoke(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Same admin gate as GRANT (issue #204): REVOKE mutates grants under
        // the service token too.
        self.require_admin(session, "REVOKE")?;
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        let revoke_stmt = RevokeStatement {
            privilege: grant_stmt.privilege,
            catalog: grant_stmt.catalog,
            namespace: grant_stmt.namespace,
            table: grant_stmt.table,
            grantee: grant_stmt.grantee,
        };
        backend.revoke(session.access_token().expose(), &revoke_stmt).await?;
        // Flush the policy cache only after the mutation succeeds so the
        // revoked grant stops working immediately (issue #207).
        self.invalidate_policy_cache();
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

        let entries = backend.show_grants(session.access_token().expose(), &filter).await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Authorise a read-path grant introspection request (issue #260).
    ///
    /// A caller may always introspect THEIR OWN effective grants/access
    /// (legitimate self-service). Targeting another principal requires an
    /// admin role: in production the Polaris backend swaps the caller's bearer
    /// for a service token scoped PRINCIPAL_ROLE:ALL, so an ungated read let
    /// any authenticated user enumerate anyone's effective privileges. The
    /// check sits ahead of `require_grant_backend`, so the service-token path
    /// is unreachable for a non-admin targeting someone else.
    fn require_self_or_admin(
        &self,
        session: &Session,
        target_user: &str,
        statement: &str,
    ) -> sqe_core::Result<()> {
        if target_user == session.user.username {
            return Ok(());
        }
        self.require_admin(session, statement)
    }

    /// Handle SHOW EFFECTIVE GRANTS by delegating to the configured grant backend.
    async fn handle_show_effective_grants(
        &self,
        session: &Session,
        user: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Self-introspection is allowed; introspecting another principal
        // requires admin (issue #260). Gate BEFORE touching the backend so
        // the service-token path is unreachable for an unauthorised caller.
        self.require_self_or_admin(session, user, "SHOW EFFECTIVE GRANTS")?;
        let backend = self.require_grant_backend()?;
        let entries = backend.show_effective(session.access_token().expose(), user).await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Handle CHECK ACCESS by delegating to the configured grant backend.
    async fn handle_check_access(
        &self,
        session: &Session,
        params: &sqe_sql::CheckAccessParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Same self-or-admin gate as SHOW EFFECTIVE GRANTS (issue #260):
        // checking your own access is self-service, checking another
        // principal's is admin-only.
        self.require_self_or_admin(session, &params.user, "CHECK ACCESS")?;
        let backend = self.require_grant_backend()?;

        let check = AccessCheck {
            user: params.user.clone(),
            privilege: params.privilege.clone(),
            catalog: params.catalog.clone(),
            namespace: params.namespace.clone(),
            table: params.table.clone(),
        };

        let resp = backend.check_access(session.access_token().expose(), &check).await?;

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

    /// Handle `SHOW EFFECTIVE POLICY [FOR USER "u"] ON <table>`.
    ///
    /// Runs the SAME policy resolution the plan rewriter applies for (user,
    /// table): resolves the resource policy via the wired `PolicyStore`, then
    /// merges tag-derived masks and filters using the table's own
    /// `sqe.column-tags`. Returns a redacted, row-per-effect description.
    ///
    /// Gating mirrors SHOW EFFECTIVE GRANTS: self-introspection is always
    /// allowed; `FOR USER other` requires admin (`require_self_or_admin`).
    ///
    /// Redaction: only the mask *type* name is surfaced (never `Redact`'s
    /// replacement, `Custom`'s expression body, or any row-filter body) so the
    /// diagnostic cannot leak the literals embedded in a policy expression.
    async fn handle_show_effective_policy(
        &self,
        session: &Session,
        params: &sqe_sql::ShowEffectivePolicyParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Default target is the session user. `FOR USER other` is gated BEFORE
        // any catalog/policy work so an unauthorised caller cannot probe another
        // principal's policy (issue #260, same gate as SHOW EFFECTIVE GRANTS).
        let target_user = params.user.as_deref().unwrap_or(&session.user.username);
        self.require_self_or_admin(session, target_user, "SHOW EFFECTIVE POLICY")?;

        let store = self.policy_store.as_deref().ok_or_else(|| {
            SqeError::NotImplemented(
                "Policy enforcement is not configured. Set [policy] backend in the config."
                    .to_string(),
            )
        })?;

        // Load the table's column tags via the caller's token (catalog gates
        // read access). The returned TableIdent gives the policy key without a
        // second parse.
        let (table_ident, col_tags) =
            self.catalog_ops.load_column_tags(session, &params.table).await?;

        // Derive the (namespace, table) policy key the SAME way the rewriter
        // does (`resolve_policy_key`): the last DOT-separated component of the
        // namespace. `parse_table_ref` collapses a 3-part reference to a single
        // namespace component, but a quoted multi-level component (e.g.
        // `cat."ns1.ns2".tbl`) stays as the string "ns1.ns2"; the rewriter and
        // the write path both key on its last component ("ns2"), so we must too
        // or the diagnostic would resolve a different policy than enforcement.
        let namespace_raw = table_ident
            .namespace()
            .as_ref()
            .last()
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        let namespace = namespace_raw
            .rsplit('.')
            .next()
            .unwrap_or(&namespace_raw)
            .to_string();
        let table_name = table_ident.name().to_string();

        // The target user's roles are only known for the self case. For
        // `FOR USER other` we have no role lookup wired (the session holds only
        // the caller's roles), so role-based policy for another principal is
        // resolved with an empty role set. See the report's known-gaps note.
        let target = if params.user.is_none()
            || params.user.as_deref() == Some(session.user.username.as_str())
        {
            session.user.clone()
        } else {
            sqe_core::session::SessionUser {
                username: target_user.to_string(),
                roles: Vec::new(),
                subject: None,
                email: None,
                groups: Vec::new(),
            }
        };

        let policy = sqe_policy::plan_rewriter::resolve_effective_policy(
            store,
            &target,
            &table_name,
            &namespace,
            &col_tags,
        )
        .await;

        Self::effective_policy_to_record_batch(&policy)
    }

    /// Build the `SHOW EFFECTIVE POLICY` result batch from a `ResolvedPolicy`.
    ///
    /// Columns: `kind` (`row_filter` | `column_mask` | `column_restriction` |
    /// `denied`), `column` (null for row filters / denied), `detail` (redacted),
    /// `source` (best-effort origin; `policy` for now). Rows are emitted in a
    /// deterministic order: denied, then row filters, then masks (sorted by
    /// column), then restrictions (sorted).
    fn effective_policy_to_record_batch(
        policy: &sqe_policy::ResolvedPolicy,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, true),
            Field::new("detail", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
        ]));

        let mut kind_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut detail_b = StringBuilder::new();
        let mut source_b = StringBuilder::new();

        let mut push = |kind: &str, column: Option<&str>, detail: &str, source: &str| {
            kind_b.append_value(kind);
            match column {
                Some(c) => col_b.append_value(c),
                None => col_b.append_null(),
            }
            detail_b.append_value(detail);
            source_b.append_value(source);
        };

        // Deny-all sentinel: the resolver signals a fully-denied table by
        // pushing `lit(false)` into row_filters. Surface it as a single `denied`
        // row rather than counting it as a normal filter.
        let denied = policy
            .row_filters
            .iter()
            .any(|e| matches!(e, datafusion::logical_expr::Expr::Literal(sv, _) if sv == &datafusion::scalar::ScalarValue::Boolean(Some(false))));

        if denied {
            push("denied", None, "access denied (deny-all policy)", "policy");
        } else {
            let n = policy.row_filters.len();
            if n > 0 {
                // Never print the filter expression body: it can embed literals.
                push(
                    "row_filter",
                    None,
                    &format!("{n} row filter(s) applied"),
                    "policy",
                );
            }
        }

        // Column masks, sorted by column for deterministic output. Only the
        // mask TYPE name is surfaced (redaction).
        let mut masks: Vec<(&String, &sqe_policy::MaskType)> = policy.column_masks.iter().collect();
        masks.sort_by(|a, b| a.0.cmp(b.0));
        for (col, mask) in masks {
            push("column_mask", Some(col), Self::mask_type_name(mask), "policy");
        }

        // Restricted columns, sorted.
        let mut restricted = policy.restricted_columns.clone();
        restricted.sort();
        for col in &restricted {
            push("column_restriction", Some(col), "restricted", "policy");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(kind_b.finish()) as ArrayRef,
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(detail_b.finish()) as ArrayRef,
                Arc::new(source_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Redaction-safe label for a `MaskType`: the variant name only, never the
    /// `Redact` replacement string or the `Custom` expression body.
    fn mask_type_name(mask: &sqe_policy::MaskType) -> &'static str {
        match mask {
            sqe_policy::MaskType::Nullify => "Nullify",
            sqe_policy::MaskType::Redact(_) => "Redact",
            sqe_policy::MaskType::Hash => "Hash",
            sqe_policy::MaskType::Custom(_) => "Custom",
            sqe_policy::MaskType::PartialMask { .. } => "PartialMask",
            sqe_policy::MaskType::DateShowYear => "DateShowYear",
        }
    }

    /// Handle `SHOW TAGS ON <table>` — read back the `sqe.column-tags` property
    /// as (column, tag) rows. No extra SQE gate: `load_column_tags` loads the
    /// table with the caller's token, so the catalog enforces read access.
    /// Empty result when the table carries no tags.
    async fn handle_show_tags(
        &self,
        session: &Session,
        table: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (_table_ident, col_tags) = self.catalog_ops.load_column_tags(session, table).await?;
        Self::tags_to_record_batch(&col_tags)
    }

    /// Build the `SHOW TAGS` result batch: one row per (column, tag), sorted by
    /// (column, tag) for deterministic output.
    fn tags_to_record_batch(
        col_tags: &std::collections::HashMap<String, Vec<String>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("column", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
        ]));

        let mut rows: Vec<(&str, &str)> = Vec::new();
        for (col, tags) in col_tags {
            for tag in tags {
                rows.push((col.as_str(), tag.as_str()));
            }
        }
        rows.sort_unstable();

        let mut col_b = StringBuilder::new();
        let mut tag_b = StringBuilder::new();
        for (col, tag) in rows {
            col_b.append_value(col);
            tag_b.append_value(tag);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(tag_b.finish()) as ArrayRef,
            ],
        )
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
                let col_name = parts
                    .last()
                    .and_then(|p| p.as_ident())
                    .map(|i| i.value.clone())
                    .unwrap_or_default();
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

        let table_ident = parse_table_ref(&table_ref_parts)?;

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
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use iceberg::{NamespaceIdent, TableIdent};

        // Parse "table" / "schema.table" / "catalog.schema.table". Resolve the
        // catalog via show_catalog (explicit qualifier wins, else the session
        // catalog, with polaris-auto discovery) so SHOW STATS finds tables in
        // the caller's catalog instead of failing against the default
        // warehouse ("Failed to load table ... does not exist"). (#6)
        let parts: Vec<&str> = table_name.splitn(3, '.').collect();
        let (catalog, namespace, bare_table) = match parts.len() {
            1 => (None, "default", parts[0]),
            2 => (None, parts[0], parts[1]),
            _ => (Some(parts[0]), parts[1], parts[2]),
        };
        let session_catalog = self.show_catalog(session, catalog).await?;

        let ns_ident = NamespaceIdent::new(namespace.to_string());
        let table_ident = TableIdent::new(ns_ident, bare_table.to_string());
        let table = session_catalog.load_table(&table_ident).await?;
        let metadata = table.metadata();

        // Table-level row count from the current snapshot summary (None when the
        // table has never been written). Per-column stats (size, distinct, null
        // fraction, min/max) are not cheaply available from iceberg metadata, so
        // they are reported NULL -- the result SHAPE matches Trino so clients
        // and the CBO do not error.
        let row_count: Option<f64> = metadata.current_snapshot().and_then(|s| {
            s.summary()
                .additional_properties
                .get("total-records")
                .and_then(|v| v.parse::<f64>().ok())
        });

        let column_names: Vec<String> = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        build_show_stats_batch(&column_names, row_count)
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
        crate::session_context::invalidate_all_session_caches().await;
        info!(catalog = %stmt.name, kind = %stmt.kind.name(), "ATTACH complete");
        Ok(vec![])
    }

    async fn handle_detach(
        &self,
        stmt: &sqe_sql::DetachStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.runtime_catalogs
            .detach(&stmt.name)
            .map_err(SqeError::Execution)?;
        crate::session_context::invalidate_all_session_caches().await;
        info!(catalog = %stmt.name, "DETACH complete");
        Ok(vec![])
    }

    fn handle_create_secret(
        &self,
        stmt: &sqe_sql::CreateSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let secret = sqe_sql::build_secret_from_stmt(stmt)?;
        self.secrets.create(&stmt.name, secret)?;
        info!(name = %stmt.name, kind = %stmt.kind.name(), "CREATE SECRET complete");
        Ok(vec![])
    }

    fn handle_drop_secret(
        &self,
        stmt: &sqe_sql::DropSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let in_use = self
            .runtime_catalogs
            .referenced_secrets(&stmt.name)
            .map_err(SqeError::Catalog)?;
        self.secrets.drop_secret(&stmt.name, &in_use)?;
        info!(name = %stmt.name, "DROP SECRET complete");
        Ok(vec![])
    }

    fn handle_show_secrets(&self) -> sqe_core::Result<Vec<RecordBatch>> {
        let listed = self.secrets.list()?;

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
        use iceberg::Catalog;

        let table_ident = parse_table_ref(table_name)?;

        let session_catalog = Arc::new(
            SessionCatalog::for_session(
                &self.config,
                self.table_cache.clone(),
                session.access_token().expose(),
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

/// RAII guard that deregisters time-travel pinned providers when the query
/// completes (#44). The pinned provider must not survive the query that
/// asked for it; otherwise later SQL in the same session that references
/// the bare table name resolves to the pinned snapshot instead of HEAD,
/// silently serving stale data.
///
/// Each entry is the fully-qualified `datafusion.public.<alias>` path used
/// at registration, matching what `ctx.deregister_table` expects.
pub struct TimeTravelCleanup {
    ctx: SessionContext,
    qualified_aliases: Vec<String>,
}

impl TimeTravelCleanup {
    fn new(ctx: &SessionContext, qualified_aliases: Vec<String>) -> Self {
        Self {
            ctx: ctx.clone(),
            qualified_aliases,
        }
    }
}

impl Drop for TimeTravelCleanup {
    fn drop(&mut self) {
        for name in &self.qualified_aliases {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "time-travel pinned provider deregister failed"
                );
            }
        }
    }
}

/// Replace whole-token occurrences of `needle` (case insensitive) in `sql` with `replacement`.
///
/// Used to rewrite a table reference like `ns.t` to `datafusion.public.alias` after the incremental pre-parser has run.
///
/// The match is strict: `needle` must be preceded and followed by a character that cannot appear in a SQL identifier (whitespace, punctuation, or start / end of string). This prevents spurious matches when `needle` appears as a substring of a longer identifier.
/// The explicit catalog component of a dotted table reference, if present.
/// SQE models references as `[catalog.]schema.table` (single-level namespace),
/// so only a three-part name carries an explicit catalog; one- and two-part
/// names do not. Used to route time-travel reads to the right catalog (#317).
fn explicit_catalog_component(table: &str) -> Option<&str> {
    let parts: Vec<&str> = table.split('.').collect();
    match parts.as_slice() {
        [catalog, _schema, _table] => Some(catalog),
        _ => None,
    }
}

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
    use sqlparser::ast::{Expr, Value, ValueWithSpan};

    match expr {
        // sqlparser 0.62: TypedString is a tuple variant whose `value` is a
        // ValueWithSpan; pull the string out (non-string typed literals are
        // rejected by the catch-all arm below).
        Expr::TypedString(ts) => match ts.value.clone().into_string() {
            Some(s) => parse_timestamp_str(&s),
            None => Err(SqeError::Execution(format!(
                "Unsupported time travel expression: {expr}. \
                 Use TIMESTAMP '2026-01-01 00:00:00' or epoch milliseconds."
            ))),
        },
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ..
        }) => parse_timestamp_str(s),
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => {
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

/// Walk a physical plan tree and pick the `IcebergScanExec` worth
/// distributing: the LARGEST by estimated row count (falling back to byte
/// size, both from snapshot-summary statistics — no manifest I/O).
///
/// Only one scan per query is distributed, and first-match DFS picked an
/// arbitrary one in multi-join plans: SSB q4.x shipped a dimension table to
/// the workers while the 6M-row lineorder fact scan ran locally on the
/// coordinator. Returns `None` if the plan contains no Iceberg table scans.
/// Collect the identifiers of Iceberg tables scanned under `plan`, stopping at
/// any `AggregateExec` barrier.
///
/// The barrier matters: the q95-class inlist blowup happens only when a hash
/// join's build side is a **raw** fact scan (tens of thousands of distinct
/// keys). Year-over-year TPC-DS queries (q11, q64, ...) join the *same* table
/// via pre-aggregated CTEs (a SUM per customer/year) -- a small, selective
/// build that benefits from the dynamic filter and must not be stripped.
/// Treating `AggregateExec` as a barrier means a table only counts toward
/// self-join detection when it reaches the join without an aggregation in
/// between, which distinguishes q95's raw `web_sales ⋈ web_sales` from the
/// benign aggregated-CTE joins.
fn collect_raw_iceberg_table_idents(
    plan: &Arc<dyn ExecutionPlan>,
    out: &mut std::collections::HashSet<String>,
) {
    use datafusion::physical_plan::aggregates::AggregateExec;
    if plan.downcast_ref::<AggregateExec>().is_some() {
        return;
    }
    if let Some(scan) = plan.downcast_ref::<IcebergScanExec>() {
        out.insert(scan.table().identifier().to_string());
    }
    for child in plan.children() {
        collect_raw_iceberg_table_idents(child, out);
    }
}

/// Strip the runtime dynamic filter from any `Inner` `HashJoinExec` whose two
/// inputs both scan the **same** Iceberg table -- a self-join (e.g. TPC-DS
/// q95's `ws_wh` CTE).
///
/// DataFusion only attaches a dynamic (runtime) filter to `Inner` joins, and
/// below `hash_join_inlist_pushdown_max_distinct_values` it materializes that
/// filter as an IN-list of the build side's distinct keys. For a self-join the
/// build side carries tens of thousands of fact-table keys and the IN-list path
/// collapses the join into a materialized cross product (q95: 0.2s -> 17s at the
/// 65536 threshold SQE uses for SSB dimension pushdown). We rebuild only the
/// offending self-join node without its dynamic filter, via
/// [`HashJoinExec::try_new`] (which starts with `dynamic_filter: None`). Every
/// other join -- including the dimension/fact joins inside the same query that
/// legitimately prune via the key-set push-down -- is left untouched.
///
/// Detection keys on table *identity*, not cardinality, so it is scale-invariant
/// (SF1/SF10/SF100) and never fires on dimension/fact joins (different tables).
/// Removing a runtime filter only changes pruning, never results.
///
/// Note: this runs on the post-distribution coordinator plan; a self-join that
/// is ever distributed (its `IcebergScanExec` replaced by a distributed exec)
/// would no-op here, which is safe because workers do not raise the inlist
/// threshold above DataFusion's default.
fn strip_self_join_dynamic_filters(
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let original = Arc::clone(&plan);
    let result = plan.transform_up(|node| {
        let Some(hj) = node.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if *hj.join_type() != JoinType::Inner || hj.dynamic_filter_expr().is_none() {
            return Ok(Transformed::no(node));
        }
        let mut left = std::collections::HashSet::new();
        let mut right = std::collections::HashSet::new();
        collect_raw_iceberg_table_idents(hj.left(), &mut left);
        collect_raw_iceberg_table_idents(hj.right(), &mut right);
        if left.intersection(&right).next().is_none() {
            return Ok(Transformed::no(node));
        }
        let rebuilt = HashJoinExec::try_new(
            Arc::clone(hj.left()),
            Arc::clone(hj.right()),
            hj.on().to_vec(),
            hj.filter().cloned(),
            hj.join_type(),
            hj.projection.as_ref().map(|p| p.to_vec()),
            *hj.partition_mode(),
            hj.null_equality(),
            hj.null_aware,
        )?;
        debug!(
            on = ?hj.on(),
            "Stripped dynamic filter from Iceberg self-join HashJoinExec (q95-class)"
        );
        Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>))
    });
    match result {
        Ok(t) => t.data,
        Err(e) => {
            debug!(error = %e, "strip_self_join_dynamic_filters failed; using original plan");
            original
        }
    }
}

fn find_iceberg_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut scans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let mut stack: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.downcast_ref::<IcebergScanExec>().is_some() {
            scans.push(node);
            continue;
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    let size_of = |node: &Arc<dyn ExecutionPlan>| -> u64 {
        let stats = match node.partition_statistics(None) {
            Ok(s) => s,
            Err(_) => return 0,
        };
        match (stats.num_rows.get_value(), stats.total_byte_size.get_value()) {
            (Some(rows), _) => *rows as u64,
            (None, Some(bytes)) => *bytes as u64,
            (None, None) => 0,
        }
    };
    scans.into_iter().max_by_key(size_of)
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

/// Derive the worker-side projection for a `ScanTask` from the scan's
/// projection and the table's Iceberg schema.
///
/// Returns `(projected_columns, projected_field_ids)`:
/// - both empty when the scan is unprojected (`SELECT *`) or the projection
///   is empty (`COUNT(*)` -- workers return full columns, the reassembly
///   path drops them),
/// - field IDs only when EVERY projected column resolves to an Iceberg field
///   ID; a partial mapping would silently project the wrong columns on
///   post-evolution files, so incomplete IDs degrade to name-only projection.
///
/// Invariant (regression guard for !327): the names returned here are exactly
/// the field names of `IcebergScanExec::schema()` (its `projected_schema`),
/// which is the schema `DistributedScanExec` advertises. The worker projects
/// these columns and ships batches that match that schema (modulo column
/// order, which reassembly normalizes by name).
fn scan_task_projection(
    projection: Option<&[String]>,
    iceberg_schema: &iceberg::spec::Schema,
) -> (Vec<String>, Vec<i32>) {
    let projected_cols: Vec<String> = projection.map(<[String]>::to_vec).unwrap_or_default();
    if projected_cols.is_empty() {
        return (projected_cols, Vec::new());
    }
    let projected_field_ids: Vec<i32> = projected_cols
        .iter()
        .filter_map(|name| {
            iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.name == *name)
                .map(|f| f.id)
        })
        .collect();
    if projected_field_ids.len() == projected_cols.len() {
        (projected_cols, projected_field_ids)
    } else {
        (projected_cols, Vec::new())
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
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident())
            .map(|p| p.value.clone())
            .collect();
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

/// Build an `information_schema.columns` query for a (possibly qualified)
/// table name, qualifying by the table's catalog and schema when present.
///
/// A bare name queries the default catalog's information_schema (with the
/// session catalog as default, that is the caller's catalog). A 2-part
/// `schema.table` adds a `table_schema` filter; a 3-part
/// `catalog.schema.table` additionally qualifies the information_schema
/// reference with that catalog, so SHOW CREATE TABLE / SHOW COLUMNS resolve a
/// table outside the default catalog instead of returning an empty result.
///
/// All interpolated values are escaped (string literals double single-quotes;
/// the catalog identifier doubles double-quotes) to prevent injection. (#2)
fn info_schema_columns_query(table_name: &str) -> String {
    let parts: Vec<&str> = table_name.split('.').collect();
    let (catalog, schema, bare) = match parts.as_slice() {
        [t] => (None, None, *t),
        [s, t] => (None, Some(*s), *t),
        [c, s, t] => (Some(*c), Some(*s), *t),
        // More than 3 parts: take the last three as catalog.schema.table.
        [.., c, s, t] => (Some(*c), Some(*s), *t),
        [] => (None, None, table_name),
    };
    let esc_lit = |s: &str| s.replace('\'', "''");
    let from = match catalog {
        Some(c) => format!("\"{}\".information_schema.columns", c.replace('"', "\"\"")),
        None => "information_schema.columns".to_string(),
    };
    let mut where_clause = format!("table_name = '{}'", esc_lit(bare));
    if let Some(s) = schema {
        where_clause.push_str(&format!(" AND table_schema = '{}'", esc_lit(s)));
    }
    format!(
        "SELECT column_name, data_type, is_nullable \
         FROM {from} WHERE {where_clause} ORDER BY ordinal_position"
    )
}

/// Build Trino's `SHOW STATS FOR <table>` result set: one row per column
/// (name set, per-column stats NULL -- iceberg does not cheaply expose
/// data_size/distinct/null-fraction/min/max), plus a final summary row
/// (column_name NULL, row_count from the snapshot, or NULL when never
/// written). Columns: column_name, data_size, distinct_values_count,
/// nulls_fraction, row_count, low_value, high_value. (#6)
fn build_show_stats_batch(
    column_names: &[String],
    row_count: Option<f64>,
) -> sqe_core::Result<Vec<RecordBatch>> {
    use arrow_array::builder::Float64Builder;
    let schema = Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, true),
        Field::new("data_size", DataType::Float64, true),
        Field::new("distinct_values_count", DataType::Float64, true),
        Field::new("nulls_fraction", DataType::Float64, true),
        Field::new("row_count", DataType::Float64, true),
        Field::new("low_value", DataType::Utf8, true),
        Field::new("high_value", DataType::Utf8, true),
    ]));
    let mut name_b = StringBuilder::new();
    let mut data_size_b = Float64Builder::new();
    let mut distinct_b = Float64Builder::new();
    let mut nulls_b = Float64Builder::new();
    let mut row_count_b = Float64Builder::new();
    let mut low_b = StringBuilder::new();
    let mut high_b = StringBuilder::new();

    for name in column_names {
        name_b.append_value(name);
        data_size_b.append_null();
        distinct_b.append_null();
        nulls_b.append_null();
        row_count_b.append_null();
        low_b.append_null();
        high_b.append_null();
    }
    // Summary row.
    name_b.append_null();
    data_size_b.append_null();
    distinct_b.append_null();
    nulls_b.append_null();
    match row_count {
        Some(rc) => row_count_b.append_value(rc),
        None => row_count_b.append_null(),
    }
    low_b.append_null();
    high_b.append_null();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_b.finish()) as ArrayRef,
            Arc::new(data_size_b.finish()) as ArrayRef,
            Arc::new(distinct_b.finish()) as ArrayRef,
            Arc::new(nulls_b.finish()) as ArrayRef,
            Arc::new(row_count_b.finish()) as ArrayRef,
            Arc::new(low_b.finish()) as ArrayRef,
            Arc::new(high_b.finish()) as ArrayRef,
        ],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build SHOW STATS result: {e}")))?;
    Ok(vec![batch])
}

/// Build the `DESCRIBE OUTPUT` result set for a prepared statement's output
/// schema: one row per column, matching Trino's column layout
/// (Column Name, Catalog, Schema, Table, Type, Type Size, Aliased). Catalog /
/// Schema / Table are null (SQE does not track per-column source bindings);
/// Type is the Trino type name; Type Size is the fixed byte width where known.
fn build_describe_output(schema: &SchemaRef) -> sqe_core::Result<Vec<RecordBatch>> {
    use arrow_array::builder::{BooleanBuilder, Int64Builder};
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("Column Name", DataType::Utf8, false),
        Field::new("Catalog", DataType::Utf8, true),
        Field::new("Schema", DataType::Utf8, true),
        Field::new("Table", DataType::Utf8, true),
        Field::new("Type", DataType::Utf8, false),
        Field::new("Type Size", DataType::Int64, true),
        Field::new("Aliased", DataType::Boolean, false),
    ]));
    let mut name_b = StringBuilder::new();
    let mut cat_b = StringBuilder::new();
    let mut sch_b = StringBuilder::new();
    let mut tbl_b = StringBuilder::new();
    let mut type_b = StringBuilder::new();
    let mut size_b = Int64Builder::new();
    let mut aliased_b = BooleanBuilder::new();
    for field in schema.fields() {
        name_b.append_value(field.name());
        cat_b.append_null();
        sch_b.append_null();
        tbl_b.append_null();
        type_b.append_value(sqe_trino_compat::types::arrow_to_trino_type(field.data_type()));
        match field.data_type().primitive_width() {
            Some(w) => size_b.append_value(w as i64),
            None => size_b.append_null(),
        }
        aliased_b.append_value(false);
    }
    let batch = RecordBatch::try_new(
        out_schema.clone(),
        vec![
            Arc::new(name_b.finish()) as ArrayRef,
            Arc::new(cat_b.finish()) as ArrayRef,
            Arc::new(sch_b.finish()) as ArrayRef,
            Arc::new(tbl_b.finish()) as ArrayRef,
            Arc::new(type_b.finish()) as ArrayRef,
            Arc::new(size_b.finish()) as ArrayRef,
            Arc::new(aliased_b.finish()) as ArrayRef,
        ],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build DESCRIBE OUTPUT batch: {e}")))?;
    Ok(vec![batch])
}

/// Build the `DESCRIBE INPUT` result set: one row per bind parameter
/// (Position, Type), matching Trino's layout. Position is 0-based; Type is the
/// inferred Trino type name, or `unknown` when DataFusion could not infer it.
fn build_describe_input(param_types: &[Option<DataType>]) -> sqe_core::Result<Vec<RecordBatch>> {
    use arrow_array::builder::Int64Builder;
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("Position", DataType::Int64, false),
        Field::new("Type", DataType::Utf8, false),
    ]));
    let mut pos_b = Int64Builder::new();
    let mut type_b = StringBuilder::new();
    for (i, dt) in param_types.iter().enumerate() {
        pos_b.append_value(i as i64);
        match dt {
            Some(dt) => type_b.append_value(sqe_trino_compat::types::arrow_to_trino_type(dt)),
            None => type_b.append_value("unknown"),
        }
    }
    let batch = RecordBatch::try_new(
        out_schema.clone(),
        vec![
            Arc::new(pos_b.finish()) as ArrayRef,
            Arc::new(type_b.finish()) as ArrayRef,
        ],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build DESCRIBE INPUT batch: {e}")))?;
    Ok(vec![batch])
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
/// Strip the leading `IN `/`FROM ` keyword (sqlparser renders a `show_in` clause
/// as "FROM <x>" / "IN <x>") and return the trimmed remainder.
fn strip_show_in_keyword(filter: &str) -> &str {
    let raw = filter.trim();
    raw.strip_prefix("IN ")
        .or_else(|| raw.strip_prefix("FROM "))
        .or_else(|| raw.strip_prefix("in "))
        .or_else(|| raw.strip_prefix("from "))
        .unwrap_or(raw)
        .trim()
}

/// Strip a single pair of wrapping double quotes.
fn unquote_segment(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .unwrap_or(s)
}

/// Catalog named by `SHOW SCHEMAS FROM <catalog>`: the leading dotted segment.
/// `None` when there is no FROM/IN clause (lists the default catalog).
fn show_schemas_catalog(filter: &str) -> Option<String> {
    let after = strip_show_in_keyword(filter);
    if after.is_empty() {
        return None;
    }
    let first = after.split('.').next().unwrap_or(after).trim();
    let unq = unquote_segment(first).trim();
    (!unq.is_empty()).then(|| unq.to_string())
}

/// Catalog named by `SHOW TABLES FROM <catalog>.<schema>`: the leading segment
/// of a 2+ part filter. A 1-part filter names a schema in the current catalog,
/// so it carries no catalog qualifier and returns `None`.
fn show_tables_catalog(filter: &str) -> Option<String> {
    let after = strip_show_in_keyword(filter);
    let parts: Vec<&str> = after.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let unq = unquote_segment(parts[0].trim()).trim();
    (!unq.is_empty()).then(|| unq.to_string())
}

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
    use sqe_core::session::Session;

    #[test]
    fn explicit_catalog_component_only_for_three_part() {
        // Three-part name carries an explicit catalog; one/two-part do not.
        assert_eq!(
            explicit_catalog_component("ws_energy_co.gold.fct_revenue_monthly"),
            Some("ws_energy_co")
        );
        assert_eq!(explicit_catalog_component("gold.fct_revenue_monthly"), None);
        assert_eq!(explicit_catalog_component("fct_revenue_monthly"), None);
    }

    #[test]
    fn show_stats_batch_has_trino_shape() {
        use arrow_array::Array;
        let cols = vec!["id".to_string(), "name".to_string()];
        let batches = build_show_stats_batch(&cols, Some(42.0)).unwrap();
        let b = &batches[0];
        // Trino's 7-column layout.
        assert_eq!(
            b.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            vec![
                "column_name",
                "data_size",
                "distinct_values_count",
                "nulls_fraction",
                "row_count",
                "low_value",
                "high_value"
            ]
        );
        // One row per column + a summary row.
        assert_eq!(b.num_rows(), 3);
        let names = b.column(0).as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(names.value(0), "id");
        assert_eq!(names.value(1), "name");
        assert!(names.is_null(2), "summary row has NULL column_name");
        let rc = b.column(4).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap();
        assert!(rc.is_null(0) && rc.is_null(1), "per-column row_count is NULL");
        assert_eq!(rc.value(2), 42.0, "summary row carries row_count");
    }

    #[test]
    fn for_timestamp_raw_resolves_to_millis() {
        // Exercises the exact glue apply_version_spec's VersionRef::Timestamp
        // arm runs: parse the raw text captured by parse_timestamp_token into
        // an Expr, then resolve to epoch millis. The raw forms here match what
        // sqe_sql::parse_timestamp_token emits (verified in its own tests). (#5)
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let resolve = |raw: &str| -> i64 {
            let expr = Parser::new(&GenericDialect {})
                .try_with_sql(raw)
                .and_then(|mut p| p.parse_expr())
                .unwrap_or_else(|e| panic!("parse_expr('{raw}') failed: {e}"));
            resolve_timestamp_expr(&expr)
                .unwrap_or_else(|e| panic!("resolve('{raw}') failed: {e}"))
        };
        // `TIMESTAMP '...'` literal and a bare quoted date both resolve to
        // 2026-01-01 midnight UTC; they must agree.
        let ts_literal = resolve("TIMESTAMP '2026-01-01 00:00:00'");
        let bare_date = resolve("'2026-01-01'");
        assert_eq!(ts_literal, bare_date, "TIMESTAMP literal == bare date midnight");
        assert!(ts_literal > 0);
        // Epoch millis pass through unchanged.
        assert_eq!(resolve("1700000000000"), 1_700_000_000_000);
    }

    #[test]
    fn info_schema_columns_query_qualifies_by_catalog_and_schema() {
        // Bare name: default catalog's information_schema, no schema filter.
        let bare = info_schema_columns_query("fct_revenue_monthly");
        assert!(bare.contains("FROM information_schema.columns"));
        assert!(bare.contains("table_name = 'fct_revenue_monthly'"));
        assert!(!bare.contains("table_schema ="));

        // 2-part: adds a schema filter, default catalog.
        let two = info_schema_columns_query("gold.fct_revenue_monthly");
        assert!(two.contains("FROM information_schema.columns"));
        assert!(two.contains("table_name = 'fct_revenue_monthly'"));
        assert!(two.contains("table_schema = 'gold'"));

        // 3-part: qualifies the information_schema reference with the catalog.
        let three = info_schema_columns_query("ws_energy_co.gold.fct_revenue_monthly");
        assert!(three.contains("\"ws_energy_co\".information_schema.columns"));
        assert!(three.contains("table_schema = 'gold'"));
        assert!(three.contains("table_name = 'fct_revenue_monthly'"));
    }

    #[test]
    fn info_schema_columns_query_escapes_injection() {
        // Single quotes in the (client-supplied) name are escaped as SQL
        // string literals; double quotes in the catalog ident are doubled.
        let q = info_schema_columns_query("ev'il.sch'ema.ta'ble");
        assert!(q.contains("table_name = 'ta''ble'"));
        assert!(q.contains("table_schema = 'sch''ema'"));
        let q2 = info_schema_columns_query(r#"ca"t.s.t"#);
        assert!(q2.contains("\"ca\"\"t\".information_schema.columns"));
    }

    /// Build a minimal session for timeout tests.
    fn test_session(roles: Vec<&str>) -> Session {
        let now = chrono::Utc::now();
        Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("tok".to_string()),
            None,
            now + chrono::Duration::hours(1),
            roles.into_iter().map(String::from).collect(),
        )
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

    // ── SHOW EFFECTIVE POLICY / SHOW TAGS result builders ────────
    // The handlers gate + resolve against a live catalog/store, but the
    // RecordBatch-building helpers are pure and tested here with synthetic
    // input. Redaction: only the mask TYPE name appears in `detail`.

    fn string_col(batch: &RecordBatch, idx: usize) -> Vec<Option<String>> {
        use arrow_array::Array;
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("string column");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn effective_policy_batch_redacts_and_orders_rows() {
        let mut p = sqe_policy::ResolvedPolicy::default();
        p.row_filters.push(datafusion::logical_expr::col("region")
            .eq(datafusion::logical_expr::lit("EU")));
        p.column_masks
            .insert("ssn".to_string(), sqe_policy::MaskType::Hash);
        // Redact carries a replacement string that MUST NOT appear in output.
        p.column_masks.insert(
            "name".to_string(),
            sqe_policy::MaskType::Redact("SECRET_VALUE".to_string()),
        );
        p.restricted_columns.push("notes".to_string());

        let batches = QueryHandler::effective_policy_to_record_batch(&p).unwrap();
        let b = &batches[0];
        let kinds = string_col(b, 0);
        let cols = string_col(b, 1);
        let details = string_col(b, 2);

        // row_filter (no column), then masks sorted by column (name, ssn), then
        // restriction.
        assert_eq!(kinds[0].as_deref(), Some("row_filter"));
        assert_eq!(cols[0], None, "row filter has no column");
        assert_eq!(details[0].as_deref(), Some("1 row filter(s) applied"));

        assert_eq!(kinds[1].as_deref(), Some("column_mask"));
        assert_eq!(cols[1].as_deref(), Some("name"));
        assert_eq!(details[1].as_deref(), Some("Redact"));

        assert_eq!(kinds[2].as_deref(), Some("column_mask"));
        assert_eq!(cols[2].as_deref(), Some("ssn"));
        assert_eq!(details[2].as_deref(), Some("Hash"));

        assert_eq!(kinds[3].as_deref(), Some("column_restriction"));
        assert_eq!(cols[3].as_deref(), Some("notes"));
        assert_eq!(details[3].as_deref(), Some("restricted"));

        // No detail field may leak the Redact replacement literal.
        for d in details.iter().flatten() {
            assert!(!d.contains("SECRET_VALUE"), "mask body leaked: {d}");
        }
    }

    #[test]
    fn effective_policy_batch_deny_all_emits_single_denied_row() {
        let mut p = sqe_policy::ResolvedPolicy::default();
        p.row_filters.push(datafusion::logical_expr::lit(false));
        let batches = QueryHandler::effective_policy_to_record_batch(&p).unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 1, "deny-all collapses to one row");
        assert_eq!(string_col(b, 0)[0].as_deref(), Some("denied"));
    }

    #[test]
    fn effective_policy_batch_empty_policy_is_empty() {
        let p = sqe_policy::ResolvedPolicy::default();
        let batches = QueryHandler::effective_policy_to_record_batch(&p).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn tags_batch_sorted_one_row_per_pair() {
        let mut map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        map.insert("email".to_string(), vec!["PII".to_string(), "CONTACT".to_string()]);
        map.insert("id".to_string(), vec!["PK".to_string()]);

        let batches = QueryHandler::tags_to_record_batch(&map).unwrap();
        let b = &batches[0];
        let cols = string_col(b, 0);
        let tags = string_col(b, 1);
        assert_eq!(b.num_rows(), 3);
        // Sorted by (column, tag): (email, CONTACT), (email, PII), (id, PK).
        assert_eq!(cols[0].as_deref(), Some("email"));
        assert_eq!(tags[0].as_deref(), Some("CONTACT"));
        assert_eq!(cols[1].as_deref(), Some("email"));
        assert_eq!(tags[1].as_deref(), Some("PII"));
        assert_eq!(cols[2].as_deref(), Some("id"));
        assert_eq!(tags[2].as_deref(), Some("PK"));
    }

    #[test]
    fn tags_batch_empty_when_no_tags() {
        let map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let batches = QueryHandler::tags_to_record_batch(&map).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn mask_type_name_covers_all_variants() {
        use sqe_policy::MaskType;
        assert_eq!(QueryHandler::mask_type_name(&MaskType::Nullify), "Nullify");
        assert_eq!(
            QueryHandler::mask_type_name(&MaskType::Redact("x".into())),
            "Redact"
        );
        assert_eq!(QueryHandler::mask_type_name(&MaskType::Hash), "Hash");
        assert_eq!(
            QueryHandler::mask_type_name(&MaskType::Custom(datafusion::logical_expr::lit("e"))),
            "Custom"
        );
        assert_eq!(
            QueryHandler::mask_type_name(&MaskType::PartialMask {
                show_first: 0,
                show_last: 4,
                upper: 'x',
                lower: 'x',
                digit: 'x'
            }),
            "PartialMask"
        );
        assert_eq!(
            QueryHandler::mask_type_name(&MaskType::DateShowYear),
            "DateShowYear"
        );
    }

    // ── scan_task_projection ─────────────────────────────────────
    // Regression coverage for the !327 projected-distributed-scan bug:
    // the ScanTask's projected_columns must be exactly the field names of
    // the scan's projected schema (which DistributedScanExec advertises),
    // and field IDs are all-or-nothing.

    fn iceberg_schema_abc() -> iceberg::spec::Schema {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "b", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "c", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn scan_task_projection_matches_projected_schema_names_and_ids() {
        // The projection the IcebergScanExec carries IS its projected_schema
        // field-name list (they are built together in SqeTableProvider::scan),
        // so the ScanTask projection derived here agrees with the schema
        // DistributedScanExec advertises: no 16-vs-2 width mismatch.
        let proj = vec!["c".to_string(), "a".to_string()];
        let (cols, ids) = scan_task_projection(Some(&proj), &iceberg_schema_abc());
        assert_eq!(cols, vec!["c".to_string(), "a".to_string()]);
        assert_eq!(ids, vec![3, 1], "field IDs parallel the projected columns");
    }

    #[test]
    fn scan_task_projection_unprojected_scan_sends_nothing() {
        let (cols, ids) = scan_task_projection(None, &iceberg_schema_abc());
        assert!(cols.is_empty());
        assert!(ids.is_empty());
    }

    #[test]
    fn scan_task_projection_count_star_sends_nothing() {
        // COUNT(*) scans carry Some([]) — workers return full columns and the
        // reassembly path drops them (expected schema has 0 fields).
        let proj: Vec<String> = vec![];
        let (cols, ids) = scan_task_projection(Some(&proj), &iceberg_schema_abc());
        assert!(cols.is_empty());
        assert!(ids.is_empty());
    }

    #[test]
    fn scan_task_projection_incomplete_field_ids_degrade_to_names_only() {
        // A projected column missing from the Iceberg schema (should not
        // happen, but fail safe): partial IDs would project the wrong parquet
        // columns, so the IDs are dropped and the name fallback is used.
        let proj = vec!["a".to_string(), "ghost".to_string()];
        let (cols, ids) = scan_task_projection(Some(&proj), &iceberg_schema_abc());
        assert_eq!(cols, vec!["a".to_string(), "ghost".to_string()]);
        assert!(ids.is_empty(), "incomplete ID mapping must send no IDs");
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

    // ─── read-side catalog extraction (SHOW SCHEMAS/TABLES FROM <catalog>) ──
    #[test]
    fn show_schemas_catalog_extracts_from_clause() {
        assert_eq!(show_schemas_catalog("FROM ws_team_a"), Some("ws_team_a".to_string()));
        assert_eq!(show_schemas_catalog("IN ws_team_a"), Some("ws_team_a".to_string()));
    }

    #[test]
    fn show_schemas_catalog_none_when_unqualified() {
        assert_eq!(show_schemas_catalog(""), None);
    }

    #[test]
    fn show_tables_catalog_extracts_leading_segment() {
        // catalog.schema -> catalog
        assert_eq!(
            show_tables_catalog("FROM ws_team_a.dev_raw"),
            Some("ws_team_a".to_string())
        );
    }

    #[test]
    fn show_tables_catalog_none_for_bare_schema() {
        // a single segment is the schema in the current catalog, not a catalog
        assert_eq!(show_tables_catalog("FROM dev_raw"), None);
        assert_eq!(show_tables_catalog(""), None);
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
    fn extract_grant_statement_future_tables_in_schema() {
        use sqe_policy::grants::Grantee;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON FUTURE TABLES IN SCHEMA my_catalog.sales TO ROLE analyst";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert_eq!(stmt.catalog.as_deref(), Some("my_catalog"));
        assert_eq!(stmt.namespace.as_deref(), Some("sales"));
        assert_eq!(stmt.table.as_deref(), Some("*"));
        assert!(matches!(stmt.grantee, Grantee::Role(ref n) if n == "analyst"));
    }

    #[test]
    fn extract_grant_statement_future_tables_single_part_schema() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON FUTURE TABLES IN SCHEMA sales TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.catalog, None);
        assert_eq!(stmt.namespace.as_deref(), Some("sales"));
        assert_eq!(stmt.table.as_deref(), Some("*"));
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

    /// DENY is not a grant/revoke SQE understands. sqlparser 0.62 added a
    /// dedicated `Statement::Deny` parse (older versions errored at parse
    /// time), but SQE's classifier has no arm for it, so it is refused with a
    /// `NotImplemented` error rather than being silently accepted. This test
    /// documents that SQE's observable behavior is preserved: DENY is rejected,
    /// and in particular it is NOT classified as a Grant or Revoke.
    #[test]
    fn deny_is_rejected_by_sqe() {
        let sql = "DENY SELECT ON my_table TO alice";
        let result = sqe_sql::parse_and_classify(sql);

        assert!(
            result.is_err(),
            "DENY must be refused by SQE, got: {result:?}"
        );
        if let Ok(kind) = &result {
            assert!(
                !matches!(kind, sqe_sql::StatementKind::Grant(_) | sqe_sql::StatementKind::Revoke(_)),
                "DENY must never be treated as a Grant/Revoke, got: {kind:?}"
            );
        }
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
        let kind = sqe_sql::parse_and_classify("SELECT 1").expect("parse SELECT");
        assert!(matches!(kind, StatementKind::Query(_)));
        assert!(!should_emit(&kind, &ol_cfg(false)));
        assert!(should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_maintenance_procedure_never_emits() {
        // CALL system.rewrite_data_files is classified as Procedure, the
        // maintenance variant. Lineage events for snapshot rewrites add
        // noise without a meaningful input/output set.
        let kind = sqe_sql::parse_and_classify("CALL system.rewrite_data_files(table => 'ns.t')")
            .expect("parse CALL");
        assert!(matches!(kind, StatementKind::Procedure(_)));
        assert!(!should_emit(&kind, &ol_cfg(false)));
        assert!(!should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_dml_always_emits_regardless_of_emit_selects() {
        let kind = sqe_sql::parse_and_classify("INSERT INTO t VALUES (1)").expect("parse INSERT");
        assert!(matches!(kind, StatementKind::Insert(_)));
        assert!(should_emit(&kind, &ol_cfg(false)));
        assert!(should_emit(&kind, &ol_cfg(true)));
    }

    #[test]
    fn should_emit_ddl_always_emits() {
        let kind = sqe_sql::parse_and_classify("CREATE TABLE t (id INT)").expect("parse CREATE TABLE");
        assert!(matches!(kind, StatementKind::CreateTable(_)));
        assert!(should_emit(&kind, &ol_cfg(false)));
    }
}

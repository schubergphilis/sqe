use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::NamespaceIdent;
use moka::sync::Cache as SyncCache;
use tracing::{debug, error, warn};

use sqe_core::config::StorageConfig;

use crate::catalog_provider::SqeCatalogProvider;
use crate::rest_catalog::SessionCatalog;
use crate::table_provider::SqeTableProvider;

/// Refresh interval for a namespace's table-name snapshot. A snapshot older
/// than this is re-listed by the next async pre-plan refresh, so DDL by other
/// writers becomes visible within a few seconds without any planning lookup
/// paying a REST round trip.
const TABLE_NAMES_TTL_SECS: u64 = 5;

/// Last known table + view names of one namespace for one principal
/// (architecture review 806).
///
/// `SchemaProvider::table_names()` is synchronous and has no error channel.
/// It used to bridge onto the network from inside a DataFusion hook and turn
/// a Polaris 503 into "this schema has no tables". It now serves this
/// snapshot, which is refreshed by [`refresh_namespace_listing`] from the
/// coordinator's async pre-plan path. A failed refresh keeps the previous
/// `names` and records why in `stale_error`, so readers can tell "empty"
/// from "unavailable".
#[derive(Debug, Clone)]
pub struct NamespaceListing {
    /// Tables then views, as of the last successful refresh.
    pub names: Vec<String>,
    /// When the last refresh (successful or not) was attempted.
    pub checked_at: Instant,
    /// Error text of the most recent failed refresh, or `None` when `names`
    /// reflects the catalog as of `checked_at`.
    pub stale_error: Option<String>,
    /// True when `stale_error` came from an authorization failure rather
    /// than an outage. Denied is a legitimate (empty) answer; unavailable
    /// is not.
    pub denied: bool,
}

impl NamespaceListing {
    /// True when a refresh should run before the next planning pass.
    pub fn needs_refresh(&self) -> bool {
        self.checked_at.elapsed() >= Duration::from_secs(TABLE_NAMES_TTL_SECS)
    }

    /// True when the snapshot is stale because the catalog could not be
    /// reached (not because the principal was denied).
    pub fn unavailable(&self) -> bool {
        self.stale_error.is_some() && !self.denied
    }
}

/// Process-global table-name snapshots keyed
/// `{token_fingerprint}|{warehouse}|{namespace}`. Global rather than per
/// provider because the coordinator builds a fresh `SessionContext` (and so
/// fresh providers) per query; a per-provider cache never survived to the
/// next statement, which is why the old 5 s TTL was "only within a single
/// provider instance's lifetime" (review 307). The principal is part of the
/// key so one user's visible names never serve another.
static TABLE_NAMES_SNAPSHOTS: LazyLock<SyncCache<String, Arc<NamespaceListing>>> =
    LazyLock::new(|| {
        SyncCache::builder()
            .max_capacity(20_000)
            .time_to_idle(Duration::from_secs(3600))
            .build()
    });

fn listing_key(catalog: &SessionCatalog, warehouse: &str, namespace: &str) -> String {
    format!("{}|{warehouse}|{namespace}", catalog.token_fingerprint())
}

/// Snapshot for `(catalog principal, warehouse, namespace)`, if any.
pub fn cached_namespace_listing(
    catalog: &SessionCatalog,
    warehouse: &str,
    namespace: &str,
) -> Option<Arc<NamespaceListing>> {
    TABLE_NAMES_SNAPSHOTS.get(&listing_key(catalog, warehouse, namespace))
}

/// Drop the snapshot for one namespace so the next pre-plan refresh
/// re-lists it. Call after DDL that creates or drops a table/view.
pub fn invalidate_namespace_listing(catalog: &SessionCatalog, warehouse: &str, namespace: &str) {
    TABLE_NAMES_SNAPSHOTS.invalidate(&listing_key(catalog, warehouse, namespace));
}

/// Fold one refresh outcome into the previous snapshot: success replaces the
/// names, failure keeps them and records the error. Pure so it is testable
/// without a catalog; `a_bridge_failure_keeps_previous_snapshot` in
/// `catalog_provider.rs` is the same rule for namespaces.
pub(crate) fn merge_listing(
    previous: Option<&NamespaceListing>,
    outcome: Result<Vec<String>, (String, bool)>,
) -> NamespaceListing {
    match outcome {
        Ok(names) => NamespaceListing {
            names,
            checked_at: Instant::now(),
            stale_error: None,
            denied: false,
        },
        Err((error, denied)) => NamespaceListing {
            names: previous.map(|p| p.names.clone()).unwrap_or_default(),
            checked_at: Instant::now(),
            stale_error: Some(error),
            denied,
        },
    }
}

/// List tables and views of `namespace` with the caller's bearer and store
/// the result as the namespace's snapshot. On failure the previous names are
/// kept and the error recorded; the error is also returned so an async caller
/// can decide whether to surface it.
///
/// This is the only place the table-name snapshot touches the network, and
/// it is `async`: call it from the coordinator's pre-plan path
/// (`refresh_referenced_namespaces`) or from `SqeCatalogProvider::refresh_namespace`.
pub async fn refresh_namespace_listing(
    catalog: &SessionCatalog,
    warehouse: &str,
    namespace: &str,
) -> sqe_core::Result<Arc<NamespaceListing>> {
    let key = listing_key(catalog, warehouse, namespace);
    let previous = TABLE_NAMES_SNAPSHOTS.get(&key);
    let ns_ident = NamespaceIdent::new(namespace.to_string());

    let outcome = match catalog.list_tables(&ns_ident).await {
        Ok(tables) => {
            let mut names: Vec<String> = tables.iter().map(|t| t.name().to_string()).collect();
            // Views are additive: a failed view listing degrades to
            // tables-only rather than failing the whole snapshot, as before.
            match catalog.list_views(&ns_ident).await {
                Ok(views) => names.extend(views),
                Err(e) => {
                    warn!(namespace = %namespace, error = %e, "Failed to list views; snapshot carries tables only");
                }
            }
            Ok(names)
        }
        Err(e) => {
            let denied = matches!(
                e.error_code(),
                sqe_core::SqeErrorCode::AccessDenied | sqe_core::SqeErrorCode::AuthenticationFailed
            );
            Err((e.to_string(), denied, e))
        }
    };

    let (merged, err) = match outcome {
        Ok(names) => (merge_listing(previous.as_deref(), Ok(names)), None),
        Err((text, denied, e)) => (
            merge_listing(previous.as_deref(), Err((text, denied))),
            Some(e),
        ),
    };
    let merged = Arc::new(merged);
    TABLE_NAMES_SNAPSHOTS.insert(key, Arc::clone(&merged));
    match err {
        Some(e) => {
            error!(namespace = %namespace, error = %e, kept = merged.names.len(), "Failed to list tables; keeping previous snapshot");
            Err(e)
        }
        None => Ok(merged),
    }
}

/// Refresh the snapshots a statement is about to plan against, off the async
/// path (review 806). Refreshes the session default schema plus every
/// schema-qualified reference in `sql` that targets `warehouse`; schemas
/// whose snapshot is younger than [`TABLE_NAMES_TTL_SECS`] are skipped, so the
/// steady-state cost is two catalog calls per namespace per window, not per
/// query. Refresh failures are recorded in the snapshot, never propagated:
/// planning proceeds and `table()` reports the precise error for the table
/// it actually needs.
pub async fn refresh_referenced_namespaces(
    catalog: &SessionCatalog,
    warehouse: &str,
    sql: &str,
    default_schema: Option<&str>,
) {
    let mut namespaces: Vec<String> = Vec::new();
    if let Some(ns) = default_schema {
        namespaces.push(ns.to_string());
    }
    for (cat, schema) in sqe_sql::extract_schema_qualifiers_from_sql(sql) {
        if cat.as_deref().is_none_or(|c| c == warehouse) && !namespaces.contains(&schema) {
            namespaces.push(schema);
        }
    }
    for ns in namespaces {
        if ns.eq_ignore_ascii_case("information_schema") {
            continue;
        }
        let fresh =
            cached_namespace_listing(catalog, warehouse, &ns).is_some_and(|l| !l.needs_refresh());
        if fresh {
            continue;
        }
        // Errors are already folded into the snapshot with their reason.
        let _ = refresh_namespace_listing(catalog, warehouse, &ns).await;
    }
}

/// Return true if `name` is present in the namespace's table/view name set.
///
/// Pulled out of [`SqeSchemaProvider::table_exist`] so the existence check is
/// unit-testable without constructing a live `SessionCatalog` (which would
/// require a running Polaris). Issue #238.
fn name_exists_in(names: &[String], name: &str) -> bool {
    names.iter().any(|n| n == name)
}

/// A view lookup is valid only when the catalog confirms the table is absent.
/// Authorization and infrastructure failures must reach the client unchanged.
fn should_try_view_after_table_error(error: &sqe_core::SqeError) -> bool {
    error.error_code() == sqe_core::SqeErrorCode::TableNotFound
}

/// DataFusion `SchemaProvider` that maps an Iceberg namespace to a DataFusion schema.
///
/// Tables are loaded lazily when `table()` is called (async, so it may talk
/// to the catalog). `table_names()` is synchronous and serves the namespace's
/// last known snapshot; see [`NamespaceListing`].
#[derive(Debug)]
pub struct SqeSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    namespace: String,
    storage_config: StorageConfig,
    warehouse: String,
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Small-file threshold in bytes for the direct-read fast path.
    small_file_threshold_bytes: u64,
    /// Concurrency for direct manifest walks during pruning.
    manifest_concurrency: usize,
    /// Prefetch concurrency for the direct-read fast path, propagated to
    /// each `SqeTableProvider`.
    prefetch_concurrency: usize,
    /// Issue #132: Tier-1 dynamic-filter clustering gate, propagated to each
    /// `SqeTableProvider`.
    runtime_filter_clustering_skip: bool,
    runtime_filter_uniform_threshold: f64,
    /// Bounded wait (ms) at scan open for pending dynamic filters,
    /// propagated to each `SqeTableProvider`.
    runtime_filter_wait_ms: u64,
    /// Issue #369: bloom-filter row-group probing of sealed runtime
    /// filters, propagated to each `SqeTableProvider`.
    runtime_filter_bloom_probe: bool,
    runtime_filter_bloom_max_values: usize,
    /// Fetch/decode pipelining depth (sqe#scan-fetch-pipeline); see
    /// `SqeTableProvider::with_scan_fetch_ahead`.
    scan_fetch_ahead: Option<usize>,
    /// Namespace names the owning `SqeCatalogProvider` already resolved for
    /// this session. `plan_view` builds its planning catalog from these
    /// instead of re-listing Polaris (review 307).
    known_namespaces: Vec<String>,
    /// Set when the most recent `table_names()` served no fresh snapshot
    /// (none yet, or stale after a failed refresh) so `table_exist` does not
    /// treat an outage as "table missing".
    listing_failed: AtomicBool,
}

impl SqeSchemaProvider {
    /// Create a new schema provider for the given namespace.
    pub fn new(
        session_catalog: Arc<SessionCatalog>,
        namespace: String,
        storage_config: StorageConfig,
        warehouse: String,
    ) -> Self {
        Self {
            session_catalog,
            namespace,
            storage_config,
            warehouse,
            prom_metrics: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            runtime_filter_clustering_skip: false,
            runtime_filter_uniform_threshold: 0.8,
            runtime_filter_wait_ms: crate::iceberg_scan::DEFAULT_RUNTIME_FILTER_WAIT_MS,
            runtime_filter_bloom_probe: true,
            runtime_filter_bloom_max_values: 65536,
            scan_fetch_ahead: None,
            known_namespaces: Vec::new(),
            listing_failed: AtomicBool::new(false),
        }
    }

    /// Drop the cached `table_names()` snapshot for this namespace. Call after
    /// DDL that creates or drops a table/view so the next pre-plan refresh
    /// re-lists instead of waiting for the TTL.
    pub fn invalidate_table_names(&self) {
        invalidate_namespace_listing(&self.session_catalog, &self.warehouse, &self.namespace);
    }

    /// Re-list this namespace now (async). See [`refresh_namespace_listing`].
    pub async fn refresh_table_names(&self) -> sqe_core::Result<Arc<NamespaceListing>> {
        refresh_namespace_listing(&self.session_catalog, &self.warehouse, &self.namespace).await
    }

    /// The current snapshot, if one exists.
    pub fn cached_listing(&self) -> Option<Arc<NamespaceListing>> {
        cached_namespace_listing(&self.session_catalog, &self.warehouse, &self.namespace)
    }

    /// True when there is no snapshot or it is older than the TTL.
    pub fn needs_refresh(&self) -> bool {
        self.cached_listing().is_none_or(|l| l.needs_refresh())
    }

    /// Namespace names already resolved by the owning catalog provider, used
    /// by `plan_view` to avoid a fresh namespace listing.
    #[must_use = "with_known_namespaces consumes self; bind the returned provider"]
    pub fn with_known_namespaces(mut self, namespaces: Vec<String>) -> Self {
        self.known_namespaces = namespaces;
        self
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    #[must_use = "with_small_file_threshold consumes self; bind the returned provider"]
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned provider"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the prefetch concurrency propagated to every table provider's
    /// direct-read fast path. Sourced from `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.prefetch_concurrency = concurrency.max(1);
        self
    }

    /// Configure the Tier-1 dynamic-filter clustering gate (issue #132),
    /// propagated to every table provider.
    #[must_use = "with_runtime_filter_clustering consumes self; bind the returned provider"]
    pub fn with_runtime_filter_clustering(mut self, skip: bool, uniform_threshold: f64) -> Self {
        self.runtime_filter_clustering_skip = skip;
        self.runtime_filter_uniform_threshold = uniform_threshold;
        self
    }

    /// Bounded wait (ms) at scan open for pending dynamic filters,
    /// propagated to every table provider.
    #[must_use = "with_runtime_filter_wait_ms consumes self; bind the returned provider"]
    pub fn with_runtime_filter_wait_ms(mut self, wait_ms: u64) -> Self {
        self.runtime_filter_wait_ms = wait_ms;
        self
    }

    /// Bloom-filter (SBBF) row-group probing of sealed runtime filters
    /// (issue #369), propagated to every table provider.
    #[must_use = "with_runtime_filter_bloom consumes self; bind the returned provider"]
    pub fn with_runtime_filter_bloom(mut self, enabled: bool, max_values: usize) -> Self {
        self.runtime_filter_bloom_probe = enabled;
        self.runtime_filter_bloom_max_values = max_values;
        self
    }

    /// Fetch/decode pipelining depth (sqe#scan-fetch-pipeline), forwarded to each table
    /// provider. `None` keeps the scan default (3x cores); `Some(0)`
    /// disables staging.
    #[must_use = "with_scan_fetch_ahead consumes self; bind the returned provider"]
    pub fn with_scan_fetch_ahead(mut self, fetch_ahead: Option<usize>) -> Self {
        self.scan_fetch_ahead = fetch_ahead;
        self
    }

    /// Attach Prometheus metrics to propagate to table providers.
    #[must_use = "with_metrics consumes self; bind the returned provider"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }
}

#[async_trait]
impl SchemaProvider for SqeSchemaProvider {
    // `SchemaProvider::table_names()` is synchronous by design (returns
    // `Vec<String>`, not a `Future`) and has no error channel. It serves the
    // namespace's last known snapshot and never touches the network: the
    // snapshot is refreshed by `refresh_namespace_listing` from the
    // coordinator's async pre-plan path (review 806). A stale snapshot is
    // served with a warning; `listing_failed` lets `table_exist` treat it as
    // unknown rather than "absent".
    fn table_names(&self) -> Vec<String> {
        match self.cached_listing() {
            Some(listing) => {
                if let Some(err) = &listing.stale_error {
                    warn!(
                        namespace = %self.namespace,
                        error = %err,
                        denied = listing.denied,
                        age_secs = listing.checked_at.elapsed().as_secs(),
                        known = listing.names.len(),
                        "Serving last known table list; catalog listing is stale"
                    );
                }
                self.listing_failed
                    .store(listing.stale_error.is_some(), Ordering::Relaxed);
                listing.names.clone()
            }
            None => {
                debug!(
                    namespace = %self.namespace,
                    "No table-name snapshot yet; refresh_namespace_listing has not run for this namespace"
                );
                self.listing_failed.store(true, Ordering::Relaxed);
                Vec::new()
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Issue #238: derive existence from the cached `table_names()` set
        // instead of issuing a fresh `table_exists()` + `load_view_sql` round
        // trip per call. The snapshot is process-global and refreshed off the
        // async path, so repeated existence probes during one planning pass
        // (and across the statements of a dbt run) cost zero REST calls.
        let names = self.table_names();
        if self.listing_failed.load(Ordering::Relaxed) {
            // No usable snapshot must not look like "table does not exist".
            // Treat existence as unknown-true so DROP/CREATE IF EXISTS hit the
            // catalog and surface the real error instead of no-op.
            return true;
        }
        name_exists_in(&names, name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        debug!(
            namespace = %self.namespace,
            table = name,
            "Loading table/view via SqeSchemaProvider"
        );

        let ns_ident = NamespaceIdent::new(self.namespace.clone());
        let table_ident = iceberg::TableIdent::new(ns_ident.clone(), name.to_string());

        // First: try loading as a regular Iceberg table
        match self.session_catalog.load_table(&table_ident).await {
            Ok(table) => match SqeTableProvider::try_new(table).await {
                Ok(provider) => {
                    let provider = match self.prom_metrics {
                        Some(ref m) => provider.with_metrics(Arc::clone(m)),
                        None => provider,
                    };
                    let provider = provider
                        .with_small_file_threshold(self.small_file_threshold_bytes)
                        .with_manifest_concurrency(self.manifest_concurrency)
                        .with_prefetch_concurrency(self.prefetch_concurrency)
                        .with_runtime_filter_clustering(
                            self.runtime_filter_clustering_skip,
                            self.runtime_filter_uniform_threshold,
                        )
                        .with_runtime_filter_wait_ms(self.runtime_filter_wait_ms)
                        .with_scan_fetch_ahead(self.scan_fetch_ahead)
                        .with_runtime_filter_bloom(
                            self.runtime_filter_bloom_probe,
                            self.runtime_filter_bloom_max_values,
                        );
                    return Ok(Some(Arc::new(provider)));
                }
                Err(e) => {
                    error!(table = name, error = %e, "Failed to create table provider");
                }
            },
            Err(e) if should_try_view_after_table_error(&e) => {
                debug!(table = name, error = %e, "Not found as table, trying view");
            }
            Err(e) => {
                // Swallowing a Polaris 403 here turns AccessDenied into
                // DataFusion's misleading "table not found" response.
                return Err(DataFusionError::External(Box::new(e)));
            }
        }

        // Second: try loading as an Iceberg view. Views are a REST-only
        // surface; other backends have no view endpoint to consult.
        if !self.session_catalog.is_rest_backend() {
            return Ok(None);
        }
        match self.session_catalog.load_view_sql(&ns_ident, name).await {
            Ok(Some(sql)) => {
                debug!(view = name, sql = %sql, "Loaded view SQL, planning...");
                return self.plan_view(name, sql).await;
            }
            Ok(None) => {}
            Err(e) if e.is_not_found() => {
                debug!(view = name, error = %e, "View not found");
            }
            Err(e) => {
                // Same rule as the table path above: a 403, a 5xx, an open
                // breaker or an unsupported view dialect must reach the user
                // as itself, not as DataFusion's "table not found"
                // (architecture review 311).
                return Err(DataFusionError::External(Box::new(e)));
            }
        }

        Ok(None)
    }
}

impl SqeSchemaProvider {
    /// A catalog provider for view planning built from the calling session's
    /// already-resolved namespace list and scan settings: zero namespace
    /// probes (review 307). Reused for the same warehouse and for siblings
    /// (with their own session catalog and namespace list).
    fn planning_catalog_provider(
        &self,
        session_catalog: Arc<SessionCatalog>,
        warehouse: String,
        namespaces: Vec<String>,
    ) -> SqeCatalogProvider {
        let mut provider = SqeCatalogProvider::with_namespaces(
            session_catalog,
            self.storage_config.clone(),
            warehouse,
            namespaces,
        )
        .with_small_file_threshold(self.small_file_threshold_bytes)
        .with_manifest_concurrency(self.manifest_concurrency)
        .with_prefetch_concurrency(self.prefetch_concurrency)
        .with_runtime_filter_clustering(
            self.runtime_filter_clustering_skip,
            self.runtime_filter_uniform_threshold,
        )
        .with_runtime_filter_wait_ms(self.runtime_filter_wait_ms)
        .with_runtime_filter_bloom(
            self.runtime_filter_bloom_probe,
            self.runtime_filter_bloom_max_values,
        )
        .with_scan_fetch_ahead(self.scan_fetch_ahead);
        if let Some(m) = &self.prom_metrics {
            provider = provider.with_metrics(Arc::clone(m));
        }
        provider
    }

    /// Plan a view's SQL and wrap it in a DataFusion ViewTable.
    ///
    /// Creates a minimal SessionContext with the calling session's catalog
    /// (same `SessionCatalog`, same namespace snapshot) registered so the
    /// view's SQL can reference tables in the same warehouse without a
    /// single `list_namespaces` call. Sibling warehouses named by the view
    /// body are registered lazily, only when planning fails to resolve a
    /// reference.
    ///
    /// Boxed at the definition (compile time, see
    /// docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the
    /// coroutine holds a `DataFrame` (a `LogicalPlan`) across its awaits, and
    /// proving that `Send` walks DataFusion's whole plan graph. Done once
    /// here instead of again inside `SchemaProvider::table`.
    fn plan_view<'a>(
        &'a self,
        name: &'a str,
        sql: String,
    ) -> futures::future::BoxFuture<'a, DFResult<Option<Arc<dyn TableProvider>>>> {
        Box::pin(self.plan_view_inner(name, sql))
    }

    async fn plan_view_inner(
        &self,
        name: &str,
        sql: String,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let catalog_name = &self.warehouse;

        let mini_ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema(catalog_name, "default"),
        );

        // The view's own namespace is always resolvable even when the
        // owning provider's list was built before it existed.
        let mut namespaces = self.known_namespaces.clone();
        if !namespaces.contains(&self.namespace) {
            namespaces.push(self.namespace.clone());
        }
        mini_ctx.register_catalog(
            catalog_name,
            Arc::new(self.planning_catalog_provider(
                self.session_catalog.clone(),
                self.warehouse.clone(),
                namespaces,
            )),
        );

        let plan_err = |e: DataFusionError| {
            DataFusionError::External(format!("Failed to plan view '{name}' SQL: {e}").into())
        };

        let df = match mini_ctx.sql(&sql).await {
            Ok(df) => df,
            Err(first_err) => {
                // A view body may reference tables in OTHER catalogs (e.g. a
                // workspace ontology view in `ws_team_a.ontology` selecting
                // from `team_a_data.public.events`). Register every sibling
                // warehouse the view's SQL names, using the same user token
                // (Polaris/OPA still enforce access), and plan once more.
                // Failures are logged and skipped so the planner reports the
                // precise unresolved reference.
                let siblings: Vec<String> = sqe_sql::extract_catalog_qualifiers_from_sql(&sql)
                    .into_iter()
                    .filter(|s| s != catalog_name)
                    .collect();
                if siblings.is_empty() {
                    return Err(plan_err(first_err));
                }
                for sibling in siblings {
                    match self.session_catalog.for_sibling_warehouse(&sibling).await {
                        Ok(sc) => {
                            match SqeCatalogProvider::try_new(
                                Arc::new(sc),
                                self.storage_config.clone(),
                                sibling.clone(),
                            )
                            .await
                            {
                                Ok(p) => {
                                    mini_ctx.register_catalog(&sibling, Arc::new(p));
                                    tracing::info!(
                                        view = %name,
                                        catalog = %sibling,
                                        "view planning: registered sibling catalog referenced by view body"
                                    );
                                }
                                Err(e) => tracing::warn!(
                                    view = %name,
                                    catalog = %sibling,
                                    error = %e,
                                    "view planning: failed to build sibling catalog provider"
                                ),
                            }
                        }
                        Err(e) => tracing::warn!(
                            view = %name,
                            catalog = %sibling,
                            error = %e,
                            "view planning: failed to open sibling warehouse session"
                        ),
                    }
                }
                mini_ctx.sql(&sql).await.map_err(plan_err)?
            }
        };

        let plan = df.into_optimized_plan().map_err(|e| {
            datafusion::error::DataFusionError::External(
                format!("Failed to optimize view '{name}' plan: {e}").into(),
            )
        })?;

        Ok(Some(Arc::new(ViewTable::new(plan, Some(sql)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_exists_in_matches_tables_and_views() {
        let names = vec![
            "orders".to_string(),
            "customers".to_string(),
            "sales_view".to_string(),
        ];
        assert!(name_exists_in(&names, "orders"));
        assert!(name_exists_in(&names, "sales_view"));
        assert!(!name_exists_in(&names, "missing"));
        // Existence is case-sensitive and exact, mirroring catalog identifiers.
        assert!(!name_exists_in(&names, "Orders"));
        assert!(!name_exists_in(&[], "orders"));
    }

    #[test]
    fn view_fallback_is_limited_to_a_genuine_table_miss() {
        let missing =
            sqe_core::SqeError::Catalog("Failed to load table: HTTP 404 Not Found: orders".into());
        let denied = sqe_core::SqeError::sourced(
            sqe_core::SqeErrorCode::AccessDenied,
            "Access denied: Failed to load table",
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "403 Forbidden"),
        );

        assert!(should_try_view_after_table_error(&missing));
        assert!(!should_try_view_after_table_error(&denied));
    }

    /// Review 806: a failed refresh keeps the previous names and records the
    /// reason, so `SHOW TABLES` on a warm session during an outage answers
    /// with the last known list plus a warning, never with zero rows.
    #[test]
    fn failed_refresh_keeps_previous_names_and_records_error() {
        let previous = merge_listing(None, Ok(vec!["orders".into(), "customers".into()]));
        assert!(previous.stale_error.is_none());
        assert!(!previous.unavailable());

        let stale = merge_listing(
            Some(&previous),
            Err(("503 service unavailable".to_string(), false)),
        );
        assert_eq!(stale.names, vec!["orders", "customers"]);
        assert_eq!(
            stale.stale_error.as_deref(),
            Some("503 service unavailable")
        );
        assert!(stale.unavailable(), "non-auth failure is 'unavailable'");

        let denied = merge_listing(Some(&previous), Err(("403 forbidden".to_string(), true)));
        assert!(denied.denied);
        assert!(
            !denied.unavailable(),
            "denied is a legitimate answer, not an outage"
        );
    }

    /// A cold snapshot that fails on first refresh carries no names but is
    /// still distinguishable from an empty namespace.
    #[test]
    fn cold_failed_refresh_is_empty_but_flagged() {
        let l = merge_listing(None, Err(("timeout".to_string(), false)));
        assert!(l.names.is_empty());
        assert!(l.unavailable());
    }

    /// A freshly refreshed snapshot is inside the TTL; an old one is not.
    #[test]
    fn needs_refresh_follows_ttl() {
        let mut l = merge_listing(None, Ok(vec![]));
        assert!(!l.needs_refresh());
        l.checked_at = Instant::now() - Duration::from_secs(TABLE_NAMES_TTL_SECS + 1);
        assert!(l.needs_refresh());
    }
}

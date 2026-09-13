use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use futures::StreamExt;
use iceberg::NamespaceIdent;
use tracing::{debug, warn};

use sqe_core::config::StorageConfig;
use sqe_core::SessionUser;
use sqe_policy_api::PolicyStore;

use crate::iceberg_scan::ScanTuning;
use crate::rest_catalog::SessionCatalog;
use crate::schema_provider::SqeSchemaProvider;

/// Maximum in-flight namespace visibility probes per provider build. A
/// 30-namespace catalog costs ~4 round-trip waves, paid once per session
/// catalog construction, never per query.
const NAMESPACE_PROBE_CONCURRENCY: usize = 8;

/// Minimum interval between miss-triggered live namespace re-lists (#368).
/// A query probing a genuinely nonexistent schema must not turn into a
/// Polaris list-namespaces storm; one probe per window per provider is
/// enough to pick up externally committed namespaces.
const NAMESPACE_RELIST_COOLDOWN: Duration = Duration::from_secs(5);

/// Decide whether a namespace-listing failure at catalog construction should
/// degrade to an empty namespace list (catalog still registers, exposes nothing)
/// rather than aborting the whole session-context build.
///
/// Two cases degrade:
///
///   * **Forbidden (AccessDenied)** — always, on every path. The catalog stays
///     visible (SHOW CATALOGS / system.jdbc.catalogs) but exposes no namespaces
///     to this principal; per-operation checks still protect the data. (#5)
///   * **Unresolvable static anchor** — only when `degrade_unresolvable` is set.
///     A static `[catalogs.*]` anchor whose Polaris warehouse does not exist
///     ("Unable to find warehouse main_warehouse") must not take down the whole
///     data plane and block unrelated `ws_<slug>.*` catalogs. It registers empty
///     and self-heals via the miss-triggered re-list once the warehouse exists.
///     (#501)
///
/// Transient / infra faults (Polaris 5xx, timeouts, circuit-breaker open, rate
/// limiting, a rejected bearer) always propagate so the session build fails
/// loudly and clients retry, instead of the fault masquerading as an empty
/// catalog. The dynamic discovery path passes `degrade_unresolvable = false`,
/// keeping its documented Err -> None -> config-default fallback.
fn should_degrade_to_empty_namespaces(
    err: &sqe_core::SqeError,
    degrade_unresolvable: bool,
) -> bool {
    use sqe_core::SqeErrorCode;
    let code = err.error_code();
    if code == SqeErrorCode::AccessDenied {
        return true;
    }
    if !degrade_unresolvable {
        return false;
    }
    // Allow-list: only a missing warehouse/namespace may look empty. TLS
    // misconfig, deserialization bugs, and unknown codes propagate.
    matches!(
        code,
        SqeErrorCode::SchemaNotFound | SqeErrorCode::CatalogNotFound | SqeErrorCode::TableNotFound
    )
}

/// The cooldown gate in front of a miss-triggered namespace re-list
/// (`SqeCatalogProvider::refresh_namespace`): true when no re-list ran within
/// `cooldown`, and then arms the gate. A miss is exactly the moment the
/// snapshot is provably stale (#368: namespaces committed through the REST
/// API by external writers never fire our local DDL invalidation hooks), and
/// this is what keeps a nonexistent-schema probe loop from hammering the
/// catalog. A failed re-list leaves the previous snapshot untouched because
/// the caller only writes it on success.
pub(crate) fn relist_due(last_relist: &Mutex<Option<Instant>>, cooldown: Duration) -> bool {
    let mut last = last_relist.lock().unwrap_or_else(PoisonError::into_inner);
    if last.is_some_and(|at| at.elapsed() < cooldown) {
        return false;
    }
    *last = Some(Instant::now());
    true
}

/// Probe-filter a namespace list with bounded concurrency, preserving the
/// input order. `probe` answers "may this caller see the namespace?":
/// `Ok(true)` keeps the name, `Ok(false)` hides it (403), and `Err`
/// (timeout/5xx/other) fails the whole listing so an indeterminate
/// probe cannot leak the name (#401).
///
/// A single transient probe is retried once before the listing fails.
/// Catalogs with hundreds of namespaces otherwise turn one timeout into
/// a total metadata outage for every BI tool that lists schemas.
pub(crate) async fn filter_visible_namespaces<F, Fut>(
    namespaces: Vec<NamespaceIdent>,
    concurrency: usize,
    probe: F,
) -> sqe_core::Result<Vec<NamespaceIdent>>
where
    F: Fn(NamespaceIdent) -> Fut,
    Fut: std::future::Future<Output = sqe_core::Result<bool>>,
{
    let probe = std::sync::Arc::new(probe);
    let probed: Vec<(NamespaceIdent, sqe_core::Result<bool>)> = futures::stream::iter(namespaces)
        .map(|ns| {
            let probe = std::sync::Arc::clone(&probe);
            async move {
                let result = match probe(ns.clone()).await {
                    Ok(v) => Ok(v),
                    Err(_) => probe(ns.clone()).await,
                };
                (ns, result)
            }
        })
        .buffered(concurrency.max(1))
        .collect()
        .await;

    let mut visible = Vec::with_capacity(probed.len());
    for (ns, result) in probed {
        match result {
            Ok(true) => visible.push(ns),
            Ok(false) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(visible)
}

/// DataFusion `CatalogProvider` that bridges Iceberg namespaces to DataFusion schemas.
///
/// Each instance is tied to a user session via `SessionCatalog`, ensuring
/// that all catalog operations are authenticated with the user's bearer token.
///
/// Schema providers are created lazily when `schema()` is called, and namespace
/// listing is done synchronously from a cached snapshot taken at construction time.
pub struct SqeCatalogProvider {
    session_catalog: Arc<SessionCatalog>,
    storage_config: StorageConfig,
    warehouse: String,
    /// Cached namespace names, populated at construction time.
    /// This avoids async calls in the synchronous `schema_names()` method.
    /// A `schema()` miss refreshes the snapshot live (#368), so namespaces
    /// committed by external REST writers become visible without a restart.
    cached_namespaces: RwLock<Vec<String>>,
    /// Whether miss-triggered re-lists apply the same per-namespace
    /// visibility probes as construction. Must match, or a stale-miss
    /// refresh would resurface names the caller is not granted.
    namespace_visibility_filter: bool,
    /// Instant of the last miss-triggered re-list; gates the cooldown.
    namespace_relist_at: Mutex<Option<Instant>>,
    /// Optional policy store for filtering restricted columns in information_schema.
    policy_store: Option<Arc<dyn PolicyStore>>,
    /// Session user identity for policy resolution.
    session_user: Option<SessionUser>,
    /// Optional Prometheus metrics propagated to schema/table providers.
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// The scan tuning knobs, handed unchanged to every schema and table
    /// provider (#575). One struct, one set of defaults, one copy from
    /// config: a hop can no longer forget a knob.
    tuning: ScanTuning,
    /// One `SqeSchemaProvider` per namespace for the life of this provider,
    /// so repeated `schema()` lookups during a planning pass (DataFusion
    /// resolves each table reference separately) share one instance instead
    /// of rebuilding it per lookup (review 307). Table-name snapshots
    /// themselves are process-global; see `schema_provider.rs`.
    schema_providers: DashMap<String, Arc<SqeSchemaProvider>>,
}

impl std::fmt::Debug for SqeCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqeCatalogProvider")
            .field("warehouse", &self.warehouse)
            .field("cached_namespaces", &self.cached_namespace_names())
            .field("has_policy_store", &self.policy_store.is_some())
            .field("session_user", &self.session_user)
            .finish()
    }
}

impl SqeCatalogProvider {
    /// Create a new catalog provider, fetching and caching the namespace list.
    ///
    /// This performs an async call to list namespaces at construction time,
    /// so the synchronous `schema_names()` method can return results without blocking.
    pub async fn try_new(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
    ) -> sqe_core::Result<Self> {
        Self::try_new_with_policy(session_catalog, storage_config, warehouse, None, None).await
    }

    /// Create a new catalog provider with optional policy filtering for information_schema.
    ///
    /// When `policy_store` and `session_user` are provided, `information_schema.columns`
    /// will filter out columns restricted by the policy engine.
    ///
    /// Namespace visibility filtering defaults ON; use
    /// [`Self::try_new_with_options`] to control it from config.
    pub async fn try_new_with_policy(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
        policy_store: Option<Arc<dyn PolicyStore>>,
        session_user: Option<SessionUser>,
    ) -> sqe_core::Result<Self> {
        Self::try_new_with_options(
            session_catalog,
            storage_config,
            warehouse,
            policy_store,
            session_user,
            true,
            // Convenience/legacy constructor: keep the original hard-fail
            // semantics on an unresolvable catalog. Only the static-anchor loop
            // opts into degrade-to-empty via `build_catalog_provider`.
            false,
        )
        .await
    }

    /// Full-option constructor.
    ///
    /// When `namespace_visibility_filter` is true and the backend is
    /// REST/Polaris, each listed namespace is probed with the session's
    /// bearer (`get_namespace` → Polaris `LOAD_NAMESPACE_METADATA`) and
    /// names the caller is forbidden to load are dropped from the cached
    /// list. Every metadata surface — `SHOW SCHEMAS`,
    /// `information_schema.schemata`, Flight SQL `GetDbSchemas` — reads
    /// that one list, so they can never disagree. Probe failures other
    /// than 403 fail the listing (catalog unavailable) so a timeout or
    /// 5xx cannot leak a namespace name. Single-identity backends skip
    /// the probes entirely: there is no caller to scope the list to.
    pub async fn try_new_with_options(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
        policy_store: Option<Arc<dyn PolicyStore>>,
        session_user: Option<SessionUser>,
        namespace_visibility_filter: bool,
        degrade_unresolvable: bool,
    ) -> sqe_core::Result<Self> {
        // A failed namespace listing degrades to an empty list (catalog still
        // registers, exposes nothing) instead of aborting the whole session
        // context build, in two cases decided by
        // `should_degrade_to_empty_namespaces`:
        //   * the principal is forbidden to LIST (always), or (#5)
        //   * `degrade_unresolvable` is set and the catalog is an unresolvable
        //     static anchor whose warehouse does not exist (#501).
        // Transient/infra faults always propagate so the build fails loudly.
        let cached_namespaces = match Self::list_visible_namespace_names(
            Arc::clone(&session_catalog),
            namespace_visibility_filter,
        )
        .await
        {
            Ok(names) => names,
            Err(e) if should_degrade_to_empty_namespaces(&e, degrade_unresolvable) => {
                if e.error_code() == sqe_core::SqeErrorCode::AccessDenied {
                    debug!(
                        warehouse = %warehouse,
                        "principal not authorized to list namespaces; registering catalog with none visible"
                    );
                } else {
                    warn!(
                        warehouse = %warehouse,
                        error = %e,
                        "static anchor catalog is unresolvable; registering it with no namespaces so \
                         unrelated catalogs still serve queries (self-heals once the warehouse exists)"
                    );
                }
                Vec::new()
            }
            Err(e) => return Err(e),
        };

        debug!(
            namespace_count = cached_namespaces.len(),
            "Initialized SqeCatalogProvider"
        );

        Ok(Self {
            session_catalog,
            storage_config,
            warehouse,
            cached_namespaces: RwLock::new(cached_namespaces),
            namespace_visibility_filter,
            namespace_relist_at: Mutex::new(None),
            policy_store,
            session_user,
            prom_metrics: None,
            tuning: ScanTuning::default(),
            schema_providers: DashMap::new(),
        })
    }

    /// Replace every scan tuning knob at once (#575). Handed unchanged to
    /// every schema and table provider this catalog builds. Schema providers
    /// are cached per namespace with the tuning they were built with, so the
    /// cache is dropped here: a lookup after this call can never serve the
    /// old values.
    #[must_use = "with_tuning consumes self; bind the returned provider"]
    pub fn with_tuning(mut self, tuning: ScanTuning) -> Self {
        self.tuning = tuning.normalized();
        self.schema_providers.clear();
        self
    }

    /// The tuning every scan through this catalog runs with.
    pub fn tuning(&self) -> &ScanTuning {
        &self.tuning
    }

    /// Attach Prometheus metrics to be propagated to schema/table providers.
    #[must_use = "with_metrics consumes self; bind the returned provider"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }

    /// Set the prefetch concurrency for the direct-read fast path.
    /// Propagated to schema and table providers. Fed from
    /// `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.tuning.prefetch_concurrency = concurrency.max(1);
        self
    }

    /// Create a catalog provider with pre-populated namespace names.
    /// Useful when the namespace list is already known.
    pub fn with_namespaces(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
        namespaces: Vec<String>,
    ) -> Self {
        Self {
            session_catalog,
            storage_config,
            warehouse,
            cached_namespaces: RwLock::new(namespaces),
            namespace_visibility_filter: true,
            namespace_relist_at: Mutex::new(None),
            policy_store: None,
            session_user: None,
            prom_metrics: None,
            tuning: ScanTuning::default(),
            schema_providers: DashMap::new(),
        }
    }

    /// The session catalog this provider plans against.
    pub fn session_catalog(&self) -> &Arc<SessionCatalog> {
        &self.session_catalog
    }

    /// Drop the cached table-name snapshot for `namespace` after DDL that
    /// created or dropped a table/view, so the next pre-plan refresh
    /// re-lists it instead of waiting for the TTL.
    pub fn invalidate_table_names(&self, namespace: &str) {
        crate::schema_provider::invalidate_namespace_listing(
            &self.session_catalog,
            &self.warehouse,
            namespace,
        );
    }

    /// Async pre-plan refresh for one namespace (review 806): re-list the
    /// visible namespaces if `namespace` is not in the snapshot yet (the #368
    /// miss case, cooldown-gated), then refresh its table-name snapshot when
    /// it is older than the TTL. Errors are folded into the snapshot with
    /// their reason and returned for the caller to log.
    pub async fn refresh_namespace(&self, namespace: &str) -> sqe_core::Result<()> {
        let known = self
            .cached_namespaces
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|n| n == namespace);
        if !known && relist_due(&self.namespace_relist_at, NAMESPACE_RELIST_COOLDOWN) {
            let fresh = Self::list_visible_namespace_names(
                Arc::clone(&self.session_catalog),
                self.namespace_visibility_filter,
            )
            .await?;
            *self
                .cached_namespaces
                .write()
                .unwrap_or_else(PoisonError::into_inner) = fresh;
        }
        let provider = self.schema_provider(namespace);
        if provider.needs_refresh() {
            provider.refresh_table_names().await?;
        }
        Ok(())
    }

    /// Async pre-plan refresh for a statement that reads `information_schema`
    /// (issue #664). With `with_information_schema(true)` DataFusion's built-in
    /// provider answers every `information_schema` reference, qualified or
    /// not, and it enumerates this catalog through the synchronous
    /// `schema_names()` / `table_names()` hooks, which only serve snapshots.
    /// Such a statement names no namespace, so [`Self::refresh_namespace`]
    /// filled none of them, and a namespace the session had not touched
    /// listed no tables at all. Re-list the visible namespaces (cooldown-gated,
    /// so an externally created namespace shows up) and refresh every
    /// namespace's table snapshot that is older than its TTL, a bounded number
    /// at a time. Failures are folded into the snapshots with their reason
    /// and logged here; planning proceeds on the last known names, as on
    /// every other pre-plan path.
    pub async fn refresh_all_namespaces(&self) {
        if relist_due(&self.namespace_relist_at, NAMESPACE_RELIST_COOLDOWN) {
            match Self::list_visible_namespace_names(
                Arc::clone(&self.session_catalog),
                self.namespace_visibility_filter,
            )
            .await
            {
                Ok(fresh) => {
                    *self
                        .cached_namespaces
                        .write()
                        .unwrap_or_else(PoisonError::into_inner) = fresh;
                }
                Err(e) => debug!(
                    warehouse = %self.warehouse,
                    error = %e,
                    "namespace re-list before an information_schema read failed; keeping the snapshot"
                ),
            }
        }
        let stale: Vec<Arc<SqeSchemaProvider>> = self
            .cached_namespace_names()
            .iter()
            .map(|ns| self.schema_provider(ns))
            .filter(|p| p.needs_refresh())
            .collect();
        let warehouse = &self.warehouse;
        futures::stream::iter(stale)
            .for_each_concurrent(NAMESPACE_PROBE_CONCURRENCY, |provider| async move {
                if let Err(e) = provider.refresh_table_names().await {
                    debug!(
                        warehouse = %warehouse,
                        error = %e,
                        "table listing before an information_schema read failed; serving last known names"
                    );
                }
            })
            .await;
    }

    /// The cached per-namespace schema provider, built on first use.
    fn schema_provider(&self, name: &str) -> Arc<SqeSchemaProvider> {
        if let Some(existing) = self.schema_providers.get(name) {
            return Arc::clone(&existing);
        }
        let mut provider = SqeSchemaProvider::new(
            self.session_catalog.clone(),
            name.to_string(),
            self.storage_config.clone(),
            self.warehouse.clone(),
        )
        .with_known_namespaces(self.cached_namespace_names());
        if let Some(ref m) = self.prom_metrics {
            provider = provider.with_metrics(Arc::clone(m));
        }
        let provider = Arc::new(provider.with_tuning(self.tuning.clone()));
        Arc::clone(
            self.schema_providers
                .entry(name.to_string())
                .or_insert(provider)
                .value(),
        )
    }

    /// List namespace names visible to this session, applying the same
    /// per-namespace probes as construction when `visibility_filter` is on.
    /// Shared by the constructor snapshot and the miss-triggered re-list so
    /// the two can never disagree on what the caller may see.
    async fn list_visible_namespace_names(
        session_catalog: Arc<SessionCatalog>,
        visibility_filter: bool,
    ) -> sqe_core::Result<Vec<String>> {
        let mut namespaces = session_catalog.list_namespaces().await?;

        if visibility_filter && session_catalog.is_rest_backend() {
            let listed = namespaces.len();
            let catalog = &session_catalog;
            namespaces = filter_visible_namespaces(
                namespaces,
                NAMESPACE_PROBE_CONCURRENCY,
                |ns| async move { catalog.namespace_visible(&ns).await },
            )
            .await?;
            if namespaces.len() < listed {
                debug!(
                    listed,
                    visible = namespaces.len(),
                    "Namespace visibility filter hid ungranted namespace names"
                );
            }
        }

        Ok(namespaces
            .iter()
            .map(|ns| {
                ns.as_ref()
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(".")
            })
            .collect())
    }

    /// Snapshot of the cached namespace names.
    fn cached_namespace_names(&self) -> Vec<String> {
        self.cached_namespaces
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl CatalogProvider for SqeCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.cached_namespace_names();
        names.push("information_schema".to_string());
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "information_schema" {
            return Some(Arc::new(
                crate::info_schema::InformationSchemaProvider::new(
                    self.session_catalog.clone(),
                    self.warehouse.clone(),
                    self.policy_store.clone(),
                    self.session_user.clone(),
                )
                // schemata/tables/columns must derive from the same
                // (visibility-filtered) list SHOW SCHEMAS serves, not a
                // second unfiltered listNamespaces.
                .with_cached_namespaces(self.cached_namespace_names()),
            ));
        }

        // Synchronous and called from inside the planner, so this answers
        // from the namespace snapshot only (architecture review 806). The
        // #368 miss case, a namespace committed by an external REST writer
        // after this snapshot was taken, is resolved on the async path:
        // the coordinator calls `refresh_namespace` for every schema a
        // statement names before planning it, and that re-lists the catalog
        // (cooldown-gated) when the name is missing here.
        let known = self
            .cached_namespaces
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|n| n == name);
        if !known {
            debug!(schema = name, "Schema not found in cached namespaces");
            return None;
        }

        Some(self.schema_provider(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ns(name: &str) -> NamespaceIdent {
        NamespaceIdent::new(name.to_string())
    }

    /// Issue #664: an `information_schema` read enumerates this catalog
    /// through the synchronous hooks, which serve snapshots only. After
    /// `refresh_all_namespaces` every namespace has a table snapshot, and a
    /// namespace created after the provider was built (the second listing
    /// below) is in `schema_names()` too.
    #[tokio::test]
    async fn refresh_all_namespaces_fills_every_snapshot_and_relists() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const WAREHOUSE: &str = "test-warehouse";
        let server = MockServer::start().await;
        let json = |body: serde_json::Value| ResponseTemplate::new(200).set_body_json(body);
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(json(
                serde_json::json!({"overrides": {"prefix": WAREHOUSE}, "defaults": {}}),
            ))
            .mount(&server)
            .await;
        // First listing (provider construction): `sales` only. Every later
        // listing: `sales` and `hr`, as if `hr` was created externally.
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces"))
            .respond_with(json(serde_json::json!({"namespaces": [["sales"]]})))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces"))
            .respond_with(json(serde_json::json!({"namespaces": [["sales"], ["hr"]]})))
            .mount(&server)
            .await;
        for (namespace, table) in [("sales", "orders"), ("hr", "employees")] {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/v1/test-warehouse/namespaces/{namespace}/tables"
                )))
                .respond_with(json(serde_json::json!({
                    "identifiers": [{"namespace": [namespace], "name": table}]
                })))
                .mount(&server)
                .await;
        }
        // `sales` also has a view; `hr` answers 404 for views, which the
        // refresh degrades to tables-only.
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces/sales/views"))
            .respond_with(json(serde_json::json!({
                "identifiers": [{"namespace": ["sales"], "name": "v_orders"}]
            })))
            .mount(&server)
            .await;

        // A token no other test uses: the table-name snapshots are
        // process-global and keyed by the bearer's fingerprint.
        let token = format!("refresh-all-{}", uuid::Uuid::new_v4());
        let catalog = Arc::new(
            SessionCatalog::new(
                &server.uri(),
                WAREHOUSE,
                &token,
                &StorageConfig::default(),
                Some(crate::rest_catalog::TableMetadataCache::new(0)),
                None,
                None,
                false,
            )
            .await
            .expect("session catalog"),
        );
        let provider = SqeCatalogProvider::try_new_with_options(
            catalog,
            StorageConfig::default(),
            WAREHOUSE.to_string(),
            None,
            None,
            false,
            false,
        )
        .await
        .expect("provider");

        let names = provider.schema_names();
        assert!(names.iter().any(|n| n == "sales"));
        assert!(!names.iter().any(|n| n == "hr"), "hr is listed later");
        assert!(
            provider
                .schema("sales")
                .expect("sales")
                .table_names()
                .is_empty(),
            "no statement named `sales`, so its snapshot is cold"
        );

        provider.refresh_all_namespaces().await;

        let names = provider.schema_names();
        assert!(names.iter().any(|n| n == "hr"), "re-listed: {names:?}");
        let sales = provider.schema("sales").expect("sales");
        assert_eq!(
            sales.table_names(),
            vec!["orders".to_string(), "v_orders".to_string()]
        );
        assert_eq!(
            provider.schema("hr").expect("hr").table_names(),
            vec!["employees".to_string()]
        );

        // `information_schema.tables` asks each listed name for its type.
        // The snapshot answers: no `loadTable` round trip, and views are
        // reported as views (dbt decides DROP VIEW vs DROP TABLE on this).
        use datafusion::datasource::TableType;
        assert_eq!(
            sales.table_type("orders").await.expect("type"),
            Some(TableType::Base)
        );
        assert_eq!(
            sales.table_type("v_orders").await.expect("type"),
            Some(TableType::View)
        );
        let loads = server
            .received_requests()
            .await
            .expect("request log")
            .iter()
            .filter(|r| r.url.path().contains("/tables/") || r.url.path().contains("/views/"))
            .count();
        assert_eq!(loads, 0, "table_type must be answered from the snapshot");

        // #575: one tuning struct reaches every schema provider, including
        // the ones already cached before `with_tuning` ran.
        let tuning = ScanTuning {
            small_file_threshold_bytes: 1,
            manifest_concurrency: 3,
            prefetch_concurrency: 5,
            runtime_filter_clustering_skip: true,
            runtime_filter_uniform_threshold: 0.42,
            runtime_filter_wait_ms: 7,
            runtime_filter_bloom_probe: false,
            runtime_filter_bloom_max_values: 11,
            scan_fetch_ahead: Some(13),
        };
        let provider = provider.with_tuning(tuning.clone());
        assert_eq!(provider.tuning(), &tuning);
        assert_eq!(provider.schema_provider("hr").tuning(), &tuning);
        assert_eq!(provider.schema_provider("sales").tuning(), &tuning);
    }

    /// Mixed allow/deny probe results: only the allowed names survive, in
    /// the original listing order.
    #[tokio::test]
    async fn filter_keeps_allowed_names_in_order() {
        let input = vec![ns("public"), ns("limited"), ns("shared"), ns("secret")];
        let out = filter_visible_namespaces(input, 8, |n| async move {
            let name = n.as_ref().join(".");
            Ok(name != "limited" && name != "secret")
        })
        .await
        .unwrap();
        let names: Vec<String> = out.iter().map(|n| n.as_ref().join(".")).collect();
        assert_eq!(names, vec!["public", "shared"]);
    }

    /// All probes denied: a zero-grant caller gets an empty list (the
    /// provider appends information_schema after filtering, never probed).
    #[tokio::test]
    async fn filter_all_denied_yields_empty() {
        let input = vec![ns("a"), ns("b")];
        let out = filter_visible_namespaces(input, 8, |_| async move { Ok(false) })
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    /// Every namespace gets exactly one probe, even with a concurrency cap
    /// far below the list length, and a cap of 0 is clamped rather than
    /// wedging the stream.
    #[tokio::test]
    async fn filter_probes_each_namespace_once_under_cap() {
        let probes = AtomicUsize::new(0);
        let input: Vec<NamespaceIdent> = (0..20).map(|i| ns(&format!("ns{i}"))).collect();
        let out = filter_visible_namespaces(input, 0, |_| {
            probes.fetch_add(1, Ordering::SeqCst);
            async move { Ok(true) }
        })
        .await
        .unwrap();
        assert_eq!(out.len(), 20);
        assert_eq!(probes.load(Ordering::SeqCst), 20);
    }

    /// #401: a timeout/5xx probe must fail the listing, not keep the name.
    /// Mixed success + one indeterminate probe still fails — names are
    /// not listed when we cannot prove the probe worked.
    #[tokio::test]
    async fn visibility_filter_fails_listing_on_probe_error() {
        let input = vec![ns("public"), ns("flaky"), ns("shared")];
        let err = filter_visible_namespaces(input, 8, |n| async move {
            let name = n.as_ref().join(".");
            if name == "flaky" {
                Err(sqe_core::SqeError::Catalog(
                    "500 Internal Server Error".to_string(),
                ))
            } else {
                Ok(true)
            }
        })
        .await
        .expect_err("probe failure must fail the listing, not keep the name");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::CatalogUnavailable);
    }

    /// A single transient probe is retried once; the listing succeeds if
    /// the retry is Ok. Persistent errors still fail the listing.
    #[tokio::test]
    async fn visibility_filter_retries_transient_probe_once() {
        let flaky_tries = std::sync::Arc::new(AtomicUsize::new(0));
        let input = vec![ns("public"), ns("flaky")];
        let out = filter_visible_namespaces(input, 8, {
            let flaky_tries = std::sync::Arc::clone(&flaky_tries);
            move |n| {
                let name = n.as_ref().join(".");
                let flaky_tries = std::sync::Arc::clone(&flaky_tries);
                async move {
                    if name == "flaky" && flaky_tries.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(sqe_core::SqeError::Catalog(
                            "500 Internal Server Error".to_string(),
                        ));
                    }
                    Ok(true)
                }
            }
        })
        .await
        .expect("one retry must recover a single 5xx probe");
        assert_eq!(out.len(), 2);
        assert_eq!(
            flaky_tries.load(Ordering::SeqCst),
            2,
            "flaky namespace must have been probed twice"
        );
    }

    /// Repeated misses inside the cooldown window arm exactly one re-list;
    /// a nonexistent-schema probe loop cannot hammer the catalog.
    #[test]
    fn cooldown_suppresses_repeated_relists() {
        let last = Mutex::new(None);
        let due: Vec<bool> = (0..5)
            .map(|_| relist_due(&last, Duration::from_secs(60)))
            .collect();
        assert_eq!(due, vec![true, false, false, false, false]);
    }

    /// A cooldown of zero re-arms immediately: the gate is a rate limit,
    /// not a one-shot.
    #[test]
    fn zero_cooldown_rearms() {
        let last = Mutex::new(None);
        for _ in 0..3 {
            assert!(relist_due(&last, Duration::ZERO));
        }
    }

    /// A caller forbidden to LIST always degrades to an empty namespace list —
    /// on both the static and the discovery path — so the catalog still
    /// registers (visible in SHOW CATALOGS) but exposes nothing to this
    /// principal. (#5)
    #[test]
    fn access_denied_degrades_regardless_of_flag() {
        let forbidden = sqe_core::SqeError::Catalog("403 forbidden".to_string());
        assert_eq!(forbidden.error_code(), sqe_core::SqeErrorCode::AccessDenied);
        assert!(should_degrade_to_empty_namespaces(&forbidden, false));
        assert!(should_degrade_to_empty_namespaces(&forbidden, true));
    }

    /// #501: an unresolvable *static* anchor must degrade to empty so unrelated
    /// catalogs still serve queries — but only when the caller opted into anchor
    /// resilience (`degrade_unresolvable = true`). The discovery path keeps its
    /// Err -> None -> config-default fallback.
    ///
    /// The real runtime error: `load_config` renders the Polaris `/v1/config`
    /// 404 as "...unexpected status code, status: 404 Not Found, json:
    /// {...Unable to find warehouse...}", which `SessionCatalog::list_namespaces`
    /// wraps as "Failed to list namespaces: ...". That string carries both "not
    /// found" and "namespaces", so `classify_catalog_error` lands it on
    /// `SchemaNotFound`. The second case is a defensive one: a body with no
    /// "not found"/"404" marker classifies as the generic `CatalogError`. Both
    /// must degrade when flagged and propagate otherwise.
    #[test]
    fn absent_static_anchor_degrades_only_when_flagged() {
        let realistic_404 = sqe_core::SqeError::Catalog(
            "Failed to list namespaces: Unexpected => Received response with unexpected \
             status code, status: 404 Not Found, json: {\"error\":{\"message\":\"Unable to \
             find warehouse main_warehouse\"}}"
                .to_string(),
        );
        assert_eq!(
            realistic_404.error_code(),
            sqe_core::SqeErrorCode::SchemaNotFound
        );
        assert!(should_degrade_to_empty_namespaces(&realistic_404, true));
        assert!(!should_degrade_to_empty_namespaces(&realistic_404, false));

        let generic = sqe_core::SqeError::Catalog(
            "Failed to list namespaces: Unable to find warehouse main_warehouse".to_string(),
        );
        assert_eq!(generic.error_code(), sqe_core::SqeErrorCode::CatalogError);
        assert!(
            !should_degrade_to_empty_namespaces(&generic, true),
            "unexpected codes must propagate, not degrade to empty"
        );
        assert!(!should_degrade_to_empty_namespaces(&generic, false));
    }

    /// Transient/infra faults fail the session build loudly even for a static
    /// anchor: a Polaris outage or a bad bearer must surface as a retryable
    /// error, never masquerade as an empty catalog.
    #[test]
    fn transient_faults_never_degrade() {
        let unavailable = sqe_core::SqeError::Catalog("503 service unavailable".to_string());
        assert_eq!(
            unavailable.error_code(),
            sqe_core::SqeErrorCode::CatalogUnavailable
        );
        assert!(!should_degrade_to_empty_namespaces(&unavailable, true));

        let unauth = sqe_core::SqeError::Catalog("401 unauthorized".to_string());
        assert_eq!(
            unauth.error_code(),
            sqe_core::SqeErrorCode::AuthenticationFailed
        );
        assert!(!should_degrade_to_empty_namespaces(&unauth, true));
    }
}

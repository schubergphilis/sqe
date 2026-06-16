use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use futures::StreamExt;
use iceberg::NamespaceIdent;
use tracing::debug;

use sqe_core::config::StorageConfig;
use sqe_core::SessionUser;
use sqe_policy::PolicyStore;

use crate::rest_catalog::SessionCatalog;
use crate::schema_provider::SqeSchemaProvider;

/// Maximum in-flight namespace visibility probes per provider build. A
/// 30-namespace catalog costs ~4 round-trip waves, paid once per session
/// catalog construction, never per query.
const NAMESPACE_PROBE_CONCURRENCY: usize = 8;

/// Probe-filter a namespace list with bounded concurrency, preserving the
/// input order. `probe` answers "may this caller see the namespace?"; the
/// fail-open decision for indeterminate probe errors lives inside the
/// probe (see `SessionCatalog::namespace_visible`), so this function only
/// keeps or drops on the boolean.
pub(crate) async fn filter_visible_namespaces<F, Fut>(
    namespaces: Vec<NamespaceIdent>,
    concurrency: usize,
    probe: F,
) -> Vec<NamespaceIdent>
where
    F: Fn(NamespaceIdent) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    futures::stream::iter(namespaces)
        .map(|ns| {
            let visible = probe(ns.clone());
            async move { (ns, visible.await) }
        })
        .buffered(concurrency.max(1))
        .filter_map(|(ns, visible)| async move { visible.then_some(ns) })
        .collect()
        .await
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
    cached_namespaces: Vec<String>,
    /// Optional policy store for filtering restricted columns in information_schema.
    policy_store: Option<Arc<dyn PolicyStore>>,
    /// Session user identity for policy resolution.
    session_user: Option<SessionUser>,
    /// Optional Prometheus metrics propagated to schema/table providers.
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    /// Small-file threshold in bytes for the direct-read fast path.
    /// Propagated to schema and table providers.
    small_file_threshold_bytes: u64,
    /// Concurrency for direct manifest walks during pruning.
    /// Propagated to schema and table providers.
    manifest_concurrency: usize,
    /// Prefetch concurrency for the direct-read fast path. Sourced from
    /// `[storage] prefetch_concurrency` and propagated downstream.
    prefetch_concurrency: usize,
    /// Issue #132: Tier-1 dynamic-filter clustering gate, propagated downstream.
    runtime_filter_clustering_skip: bool,
    runtime_filter_uniform_threshold: f64,
    /// Bounded wait (ms) at scan open for pending dynamic filters,
    /// propagated downstream.
    runtime_filter_wait_ms: u64,
}

impl std::fmt::Debug for SqeCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqeCatalogProvider")
            .field("warehouse", &self.warehouse)
            .field("cached_namespaces", &self.cached_namespaces)
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
    /// than 403 fail open (the name stays; contents remain protected by
    /// the per-operation checks). Single-identity backends skip the
    /// probes entirely: there is no caller to scope the list to.
    pub async fn try_new_with_options(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
        policy_store: Option<Arc<dyn PolicyStore>>,
        session_user: Option<SessionUser>,
        namespace_visibility_filter: bool,
    ) -> sqe_core::Result<Self> {
        let mut namespaces = session_catalog.list_namespaces().await?;

        if namespace_visibility_filter && session_catalog.is_rest_backend() {
            let listed = namespaces.len();
            let catalog = &session_catalog;
            namespaces = filter_visible_namespaces(
                namespaces,
                NAMESPACE_PROBE_CONCURRENCY,
                |ns| async move { catalog.namespace_visible(&ns).await },
            )
            .await;
            if namespaces.len() < listed {
                debug!(
                    listed,
                    visible = namespaces.len(),
                    "Namespace visibility filter hid ungranted namespace names"
                );
            }
        }

        let cached_namespaces: Vec<String> = namespaces
            .iter()
            .map(|ns| ns.as_ref().iter().map(|s| s.as_str()).collect::<Vec<_>>().join("."))
            .collect();

        debug!(
            namespace_count = cached_namespaces.len(),
            "Initialized SqeCatalogProvider"
        );

        Ok(Self {
            session_catalog,
            storage_config,
            warehouse,
            cached_namespaces,
            policy_store,
            session_user,
            prom_metrics: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            runtime_filter_clustering_skip: false,
            runtime_filter_uniform_threshold: 0.8,
            runtime_filter_wait_ms: crate::iceberg_scan::DEFAULT_RUNTIME_FILTER_WAIT_MS,
        })
    }

    /// Attach Prometheus metrics to be propagated to schema/table providers.
    #[must_use = "with_metrics consumes self; bind the returned provider"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    /// Propagated to all schema and table providers.
    #[must_use = "with_small_file_threshold consumes self; bind the returned provider"]
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning. Propagated to schema and table providers.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned provider"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the prefetch concurrency for the direct-read fast path.
    /// Propagated to schema and table providers. Fed from
    /// `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.prefetch_concurrency = concurrency.max(1);
        self
    }

    /// Configure the Tier-1 dynamic-filter clustering gate (issue #132).
    /// Propagated to schema and table providers.
    #[must_use = "with_runtime_filter_clustering consumes self; bind the returned provider"]
    pub fn with_runtime_filter_clustering(mut self, skip: bool, uniform_threshold: f64) -> Self {
        self.runtime_filter_clustering_skip = skip;
        self.runtime_filter_uniform_threshold = uniform_threshold;
        self
    }

    /// Bounded wait (ms) at scan open for pending dynamic filters.
    /// Propagated to schema and table providers.
    #[must_use = "with_runtime_filter_wait_ms consumes self; bind the returned provider"]
    pub fn with_runtime_filter_wait_ms(mut self, wait_ms: u64) -> Self {
        self.runtime_filter_wait_ms = wait_ms;
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
            cached_namespaces: namespaces,
            policy_store: None,
            session_user: None,
            prom_metrics: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            runtime_filter_clustering_skip: false,
            runtime_filter_uniform_threshold: 0.8,
            runtime_filter_wait_ms: crate::iceberg_scan::DEFAULT_RUNTIME_FILTER_WAIT_MS,
        }
    }
}

impl CatalogProvider for SqeCatalogProvider {

    fn schema_names(&self) -> Vec<String> {
        let mut names = self.cached_namespaces.clone();
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
                .with_cached_namespaces(self.cached_namespaces.clone()),
            ));
        }

        if !self.cached_namespaces.contains(&name.to_string()) {
            debug!(schema = name, "Schema not found in cached namespaces");
            return None;
        }

        let mut provider = SqeSchemaProvider::new(
            self.session_catalog.clone(),
            name.to_string(),
            self.storage_config.clone(),
            self.warehouse.clone(),
        );
        if let Some(ref m) = self.prom_metrics {
            provider = provider.with_metrics(Arc::clone(m));
        }
        provider = provider.with_small_file_threshold(self.small_file_threshold_bytes);
        provider = provider.with_manifest_concurrency(self.manifest_concurrency);
        provider = provider.with_prefetch_concurrency(self.prefetch_concurrency);
        provider = provider.with_runtime_filter_clustering(
            self.runtime_filter_clustering_skip,
            self.runtime_filter_uniform_threshold,
        );
        provider = provider.with_runtime_filter_wait_ms(self.runtime_filter_wait_ms);

        Some(Arc::new(provider))

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ns(name: &str) -> NamespaceIdent {
        NamespaceIdent::new(name.to_string())
    }

    /// Mixed allow/deny probe results: only the allowed names survive, in
    /// the original listing order.
    #[tokio::test]
    async fn filter_keeps_allowed_names_in_order() {
        let input = vec![ns("public"), ns("limited"), ns("shared"), ns("secret")];
        let out = filter_visible_namespaces(input, 8, |n| async move {
            let name = n.as_ref().join(".");
            name != "limited" && name != "secret"
        })
        .await;
        let names: Vec<String> = out.iter().map(|n| n.as_ref().join(".")).collect();
        assert_eq!(names, vec!["public", "shared"]);
    }

    /// All probes denied: a zero-grant caller gets an empty list (the
    /// provider appends information_schema after filtering, never probed).
    #[tokio::test]
    async fn filter_all_denied_yields_empty() {
        let input = vec![ns("a"), ns("b")];
        let out = filter_visible_namespaces(input, 8, |_| async move { false }).await;
        assert!(out.is_empty());
    }

    /// Every namespace gets exactly one probe, even with a concurrency cap
    /// far below the list length, and a cap of 0 is clamped rather than
    /// wedging the stream.
    #[tokio::test]
    async fn filter_probes_each_namespace_once_under_cap() {
        let probes = AtomicUsize::new(0);
        let input: Vec<NamespaceIdent> =
            (0..20).map(|i| ns(&format!("ns{i}"))).collect();
        let out = filter_visible_namespaces(input, 0, |_| {
            probes.fetch_add(1, Ordering::SeqCst);
            async move { true }
        })
        .await;
        assert_eq!(out.len(), 20);
        assert_eq!(probes.load(Ordering::SeqCst), 20);
    }
}

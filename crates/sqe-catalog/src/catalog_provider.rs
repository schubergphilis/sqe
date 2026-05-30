use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use tracing::debug;

use sqe_core::config::StorageConfig;
use sqe_core::SessionUser;
use sqe_policy::PolicyStore;

use crate::rest_catalog::SessionCatalog;
use crate::schema_provider::SqeSchemaProvider;

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
    /// When true, `schema(name)` resolves the requested namespace directly
    /// instead of gating on `cached_namespaces`. Long-lived catalogs (the
    /// ballista cluster catalog, built once and reused) set this so a namespace
    /// created after construction still resolves; table existence is then
    /// decided live by the schema provider's `table()`. The per-statement
    /// coordinator catalog leaves it false: its snapshot is always fresh, and
    /// the guard preserves "schema not found" semantics. See ledger D12.
    live_schema_resolution: bool,
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
    pub async fn try_new_with_policy(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        warehouse: String,
        policy_store: Option<Arc<dyn PolicyStore>>,
        session_user: Option<SessionUser>,
    ) -> sqe_core::Result<Self> {
        let namespaces = session_catalog.list_namespaces().await?;
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
            live_schema_resolution: false,
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
            live_schema_resolution: false,
        }
    }

    /// Resolve `schema(name)` point lookups live (bypass the construction-time
    /// `cached_namespaces` snapshot). Set for long-lived catalogs that outlive
    /// DDL, such as the ballista cluster catalog. `schema_names()` enumeration
    /// still uses the snapshot. See ledger D12.
    #[must_use = "with_live_schema_resolution consumes self; bind the returned provider"]
    pub fn with_live_schema_resolution(mut self) -> Self {
        self.live_schema_resolution = true;
        self
    }
}

impl CatalogProvider for SqeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
                ),
            ));
        }

        // Long-lived catalogs (cluster catalog) resolve the requested namespace
        // directly so namespaces created after construction still resolve;
        // table existence is decided live by the schema provider (ledger D12).
        // Per-statement catalogs keep the snapshot guard ("schema not found").
        if !self.live_schema_resolution && !self.cached_namespaces.contains(&name.to_string()) {
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

        Some(Arc::new(provider))

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_catalog::SessionCatalog;
    use sqe_core::config::CatalogConfig;

    /// Build a `SessionCatalog` offline. `for_session_with` builds the REST
    /// catalog lazily (the `/v1/config` probe is memoized on first table use),
    /// so construction touches no network. `schema()` itself does no I/O (it
    /// only constructs a `SqeSchemaProvider`), so these assertions are offline.
    async fn offline_session_catalog() -> Arc<SessionCatalog> {
        let cat_cfg: CatalogConfig = serde_json::from_value(serde_json::json!({
            "catalog_url": "http://127.0.0.1:1/iceberg",
            "warehouse": "test",
        }))
        .expect("minimal CatalogConfig must deserialize");
        let storage = StorageConfig::default();
        Arc::new(
            SessionCatalog::for_session_with(&cat_cfg, &storage, None, "svc")
                .await
                .expect("offline SessionCatalog build (lazy REST catalog)"),
        )
    }

    /// The long-lived cluster catalog (ballista) is built once and reused; a
    /// namespace created afterward must still resolve. Snapshot mode (the
    /// per-statement coordinator default) gates point lookups on the cached
    /// list; live mode (the cluster catalog) resolves the requested namespace
    /// directly and lets table resolution decide existence. Regression guard
    /// for cutover ledger D12.
    #[tokio::test]
    async fn schema_point_lookup_respects_live_resolution_flag() {
        let sc = offline_session_catalog().await;
        let storage = StorageConfig::default();

        // Snapshot mode: the cached list omits "newns" -> rejected (current,
        // correct-for-per-statement behaviour, preserved).
        let snapshot_only = SqeCatalogProvider::with_namespaces(
            sc.clone(),
            storage.clone(),
            "test".to_string(),
            vec!["oldns".to_string()],
        );
        assert!(
            snapshot_only.schema("newns").is_none(),
            "snapshot mode must gate point lookups on the cached namespace list"
        );
        assert!(
            snapshot_only.schema("oldns").is_some(),
            "snapshot mode resolves cached namespaces"
        );

        // Live mode: the cluster catalog resolves a namespace absent from the
        // construction-time snapshot (the D12 fix).
        let live = SqeCatalogProvider::with_namespaces(
            sc,
            storage,
            "test".to_string(),
            vec!["oldns".to_string()],
        )
        .with_live_schema_resolution();
        assert!(
            live.schema("newns").is_some(),
            "live mode resolves namespaces created after the snapshot was taken"
        );
        assert!(
            live.schema("information_schema").is_some(),
            "live mode still serves information_schema"
        );
    }
}

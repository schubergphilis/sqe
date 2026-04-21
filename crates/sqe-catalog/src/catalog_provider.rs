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
        })
    }

    /// Attach Prometheus metrics to be propagated to schema/table providers.
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    /// Propagated to all schema and table providers.
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning. Propagated to schema and table providers.
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
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
        }
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

        Some(Arc::new(provider))

    }
}

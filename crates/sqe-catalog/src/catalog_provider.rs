use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use tracing::debug;

use sqe_core::config::StorageConfig;

use crate::rest_catalog::SessionCatalog;
use crate::schema_provider::SqeSchemaProvider;

/// DataFusion `CatalogProvider` that bridges Iceberg namespaces to DataFusion schemas.
///
/// Each instance is tied to a user session via `SessionCatalog`, ensuring
/// that all catalog operations are authenticated with the user's bearer token.
///
/// Schema providers are created lazily when `schema()` is called, and namespace
/// listing is done synchronously from a cached snapshot taken at construction time.
#[derive(Debug)]
pub struct SqeCatalogProvider {
    session_catalog: Arc<SessionCatalog>,
    storage_config: StorageConfig,
    /// Cached namespace names, populated at construction time.
    /// This avoids async calls in the synchronous `schema_names()` method.
    cached_namespaces: Vec<String>,
}

impl SqeCatalogProvider {
    /// Create a new catalog provider, fetching and caching the namespace list.
    ///
    /// This performs an async call to list namespaces at construction time,
    /// so the synchronous `schema_names()` method can return results without blocking.
    pub async fn try_new(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
    ) -> sqe_core::Result<Self> {
        let namespaces = session_catalog.list_namespaces().await?;
        let cached_namespaces: Vec<String> = namespaces
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        debug!(
            namespace_count = cached_namespaces.len(),
            "Initialized SqeCatalogProvider"
        );

        Ok(Self {
            session_catalog,
            storage_config,
            cached_namespaces,
        })
    }

    /// Create a catalog provider with pre-populated namespace names.
    /// Useful when the namespace list is already known.
    pub fn with_namespaces(
        session_catalog: Arc<SessionCatalog>,
        storage_config: StorageConfig,
        namespaces: Vec<String>,
    ) -> Self {
        Self {
            session_catalog,
            storage_config,
            cached_namespaces: namespaces,
        }
    }
}

impl CatalogProvider for SqeCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.cached_namespaces.clone()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if !self.cached_namespaces.contains(&name.to_string()) {
            debug!(schema = name, "Schema not found in cached namespaces");
            return None;
        }

        let provider = SqeSchemaProvider::new(
            self.session_catalog.clone(),
            name.to_string(),
            self.storage_config.clone(),
        );

        Some(Arc::new(provider))
    }
}

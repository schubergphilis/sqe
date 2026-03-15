use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{debug, error};

use sqe_core::config::StorageConfig;

use crate::rest_catalog::SessionCatalog;
use crate::table_provider::SqeTableProvider;

/// DataFusion `SchemaProvider` that maps an Iceberg namespace to a DataFusion schema.
///
/// Tables are loaded lazily when `table()` is called. The `table_names()` method
/// performs an async call via `tokio::task::block_in_place` to list tables from the
/// Iceberg catalog.
#[derive(Debug)]
pub struct SqeSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    namespace: String,
    /// Retained for future use with credential vending per-table.
    #[allow(dead_code)]
    storage_config: StorageConfig,
}

impl SqeSchemaProvider {
    /// Create a new schema provider for the given namespace.
    pub fn new(
        session_catalog: Arc<SessionCatalog>,
        namespace: String,
        storage_config: StorageConfig,
    ) -> Self {
        Self {
            session_catalog,
            namespace,
            storage_config,
        }
    }
}

#[async_trait]
impl SchemaProvider for SqeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // SchemaProvider::table_names() is synchronous, but listing tables requires
        // an async call. We use a best-effort approach: spawn a blocking task.
        let catalog = self.session_catalog.clone();
        let ns = self.namespace.clone();

        // Try to get the current tokio runtime handle to block on
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                error!(namespace = %ns, "No tokio runtime available for table_names()");
                return Vec::new();
            }
        };

        let ns_ident = NamespaceIdent::new(ns.clone());
        match tokio::task::block_in_place(|| handle.block_on(catalog.list_tables(&ns_ident))) {
            Ok(tables) => tables.iter().map(|t| t.name().to_string()).collect(),
            Err(e) => {
                error!(namespace = %ns, error = %e, "Failed to list tables");
                Vec::new()
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Use table_names for a simple existence check
        self.table_names().contains(&name.to_string())
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        debug!(
            namespace = %self.namespace,
            table = name,
            "Loading table via SqeSchemaProvider"
        );

        let ns_ident = NamespaceIdent::new(self.namespace.clone());
        let table_ident = iceberg::TableIdent::new(ns_ident, name.to_string());

        let table = match self.session_catalog.load_table(&table_ident).await {
            Ok(t) => t,
            Err(e) => {
                error!(
                    namespace = %self.namespace,
                    table = name,
                    error = %e,
                    "Failed to load table from catalog"
                );
                return Ok(None);
            }
        };

        match SqeTableProvider::try_new(table).await {
            Ok(provider) => Ok(Some(Arc::new(provider))),
            Err(e) => {
                error!(
                    namespace = %self.namespace,
                    table = name,
                    error = %e,
                    "Failed to create table provider"
                );
                Ok(None)
            }
        }
    }
}

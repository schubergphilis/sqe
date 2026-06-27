use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::rest_catalog::SessionCatalog;
use crate::system_jdbc::JdbcSchemaProvider;
use crate::system_metadata::MetadataSchemaProvider;
use crate::system_runtime::RuntimeSchemaProvider;

/// One catalog the session can reach, paired with the `SessionCatalog` used to
/// enumerate it. `system.jdbc.*` and `system.metadata.*` iterate these so JDBC
/// metadata browsing (`getCatalogs`/`getTables`/`getColumns`) sees every
/// reachable catalog -- the configured ones plus the session's own
/// (X-Trino-Catalog) Polaris warehouse -- not just the default. (#5)
#[derive(Clone)]
pub struct SystemCatalogEntry {
    pub name: String,
    pub catalog: Arc<SessionCatalog>,
}

impl std::fmt::Debug for SystemCatalogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemCatalogEntry").field("name", &self.name).finish()
    }
}

/// DataFusion `CatalogProvider` for the virtual `system` catalog.
///
/// Provides the following schemas:
///
/// - `jdbc`     — JDBC metadata tables required by Trino JDBC drivers
/// - `metadata` — catalog/table/schema property tables
/// - `runtime`  — live query, node, and task information (optional)
#[derive(Debug)]
pub struct SystemCatalogProvider {
    jdbc_schema: Arc<JdbcSchemaProvider>,
    metadata_schema: Arc<MetadataSchemaProvider>,
    runtime_schema: Option<Arc<RuntimeSchemaProvider>>,
}

impl SystemCatalogProvider {
    /// Build the `system` catalog over every reachable catalog. `entries[0]`
    /// is the primary/default warehouse (first in JDBC catalog listings);
    /// `entries` also include the session's own catalog so a JDBC client that
    /// connected with `X-Trino-Catalog=<ws>` enumerates `<ws>`.
    pub fn new(entries: Vec<SystemCatalogEntry>) -> Self {
        Self {
            jdbc_schema: Arc::new(JdbcSchemaProvider::new(entries.clone())),
            metadata_schema: Arc::new(MetadataSchemaProvider::new(entries)),
            runtime_schema: None,
        }
    }

    /// Set the runtime schema provider for `system.runtime.*` virtual tables.
    #[must_use = "with_runtime consumes self; bind the returned provider"]
    pub fn with_runtime(mut self, runtime: Arc<RuntimeSchemaProvider>) -> Self {
        self.runtime_schema = Some(runtime);
        self
    }
}

impl CatalogProvider for SystemCatalogProvider {

    fn schema_names(&self) -> Vec<String> {
        let mut names = vec!["jdbc".to_string(), "metadata".to_string()];
        if self.runtime_schema.is_some() {
            names.push("runtime".to_string());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "jdbc" => Some(self.jdbc_schema.clone()),
            "metadata" => Some(self.metadata_schema.clone()),
            "runtime" => self.runtime_schema.as_ref().map(|s| s.clone() as Arc<dyn SchemaProvider>),
            _ => None,
        }
    }
}

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::rest_catalog::SessionCatalog;
use crate::system_jdbc::JdbcSchemaProvider;
use crate::system_metadata::MetadataSchemaProvider;
use crate::system_runtime::RuntimeSchemaProvider;

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
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            jdbc_schema: Arc::new(JdbcSchemaProvider::new(
                session_catalog.clone(),
                warehouse.clone(),
            )),
            metadata_schema: Arc::new(MetadataSchemaProvider::new(session_catalog, warehouse)),
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

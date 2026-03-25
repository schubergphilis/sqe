use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::rest_catalog::SessionCatalog;
use crate::system_jdbc::JdbcSchemaProvider;

/// DataFusion `CatalogProvider` for the virtual `system` catalog.
///
/// Provides the `jdbc` schema containing JDBC metadata tables
/// required by Trino JDBC drivers for metadata browsing.
#[derive(Debug)]
pub struct SystemCatalogProvider {
    jdbc_schema: Arc<JdbcSchemaProvider>,
}

impl SystemCatalogProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            jdbc_schema: Arc::new(JdbcSchemaProvider::new(session_catalog, warehouse)),
        }
    }
}

impl CatalogProvider for SystemCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["jdbc".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "jdbc" {
            Some(self.jdbc_schema.clone())
        } else {
            None
        }
    }
}

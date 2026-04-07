use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::ViewTable;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::NamespaceIdent;
use tracing::{debug, error};

use sqe_core::config::StorageConfig;

use crate::catalog_provider::SqeCatalogProvider;
use crate::rest_catalog::SessionCatalog;
use crate::table_provider::SqeTableProvider;

/// DataFusion `SchemaProvider` that maps an Iceberg namespace to a DataFusion schema.
///
/// Tables are loaded lazily when `table()` is called. The `table_names()` method
/// performs an async call via `tokio::task::block_in_place` to list tables and views
/// from the Iceberg catalog.
#[derive(Debug)]
pub struct SqeSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    namespace: String,
    storage_config: StorageConfig,
    warehouse: String,
    prom_metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
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
        }
    }

    /// Attach Prometheus metrics to propagate to table providers.
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }
}

#[async_trait]
impl SchemaProvider for SqeSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // SAFETY NOTE: DataFusion's SchemaProvider::table_names() is synchronous by design
    // (returns Vec<String>, not a Future). Since our catalog is async (HTTP calls to
    // Polaris), we use `block_in_place` to bridge the sync-async gap. This yields the
    // current tokio thread first (avoiding deadlock with the current-thread runtime),
    // then blocks on the async call. This is a known DataFusion design constraint —
    // the SchemaProvider trait predates DataFusion's async catalog work. A future
    // DataFusion version may provide an async alternative.
    fn table_names(&self) -> Vec<String> {
        let catalog = self.session_catalog.clone();
        let ns = self.namespace.clone();

        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                error!(namespace = %ns, "No tokio runtime available for table_names()");
                return Vec::new();
            }
        };

        let ns_ident = NamespaceIdent::new(ns.clone());

        let tables =
            tokio::task::block_in_place(|| handle.block_on(catalog.list_tables(&ns_ident)));
        let mut names: Vec<String> = match tables {
            Ok(t) => t.iter().map(|t| t.name().to_string()).collect(),
            Err(e) => {
                error!(namespace = %ns, error = %e, "Failed to list tables");
                Vec::new()
            }
        };

        // Also include views
        let views =
            tokio::task::block_in_place(|| handle.block_on(catalog.list_views(&ns_ident)));
        match views {
            Ok(view_names) => names.extend(view_names),
            Err(e) => {
                error!(namespace = %ns, error = %e, "Failed to list views");
            }
        }

        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
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
            Ok(table) => {
                match SqeTableProvider::try_new(table).await {
                    Ok(provider) => {
                        let provider = match self.prom_metrics {
                            Some(ref m) => provider.with_metrics(Arc::clone(m)),
                            None => provider,
                        };
                        return Ok(Some(Arc::new(provider)));
                    }
                    Err(e) => {
                        error!(table = name, error = %e, "Failed to create table provider");
                    }
                }
            }
            Err(e) => {
                debug!(table = name, error = %e, "Not found as table, trying view");
            }
        }

        // Second: try loading as an Iceberg view
        match self.session_catalog.load_view_sql(&ns_ident, name).await {
            Ok(Some(sql)) => {
                debug!(view = name, sql = %sql, "Loaded view SQL, planning...");
                return self.plan_view(name, sql).await;
            }
            Ok(None) => {}
            Err(e) => {
                debug!(view = name, error = %e, "Failed to load view SQL");
            }
        }

        Ok(None)
    }
}

impl SqeSchemaProvider {
    /// Plan a view's SQL and wrap it in a DataFusion ViewTable.
    ///
    /// Creates a minimal SessionContext with the same catalog registered so that
    /// the view's SQL can reference tables in the same namespace.
    async fn plan_view(
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

        // Register the same catalog so the view's SQL can reference its tables
        let catalog_provider = SqeCatalogProvider::try_new(
            self.session_catalog.clone(),
            self.storage_config.clone(),
            self.warehouse.clone(),
        )
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::External(format!(
                "Failed to create catalog for view planning: {e}"
            ).into())
        })?;

        mini_ctx.register_catalog(catalog_name, Arc::new(catalog_provider));

        let df = mini_ctx.sql(&sql).await.map_err(|e| {
            datafusion::error::DataFusionError::External(
                format!("Failed to plan view '{name}' SQL: {e}").into(),
            )
        })?;

        let plan = df.into_optimized_plan().map_err(|e| {
            datafusion::error::DataFusionError::External(
                format!("Failed to optimize view '{name}' plan: {e}").into(),
            )
        })?;

        Ok(Some(Arc::new(ViewTable::new(plan, Some(sql)))))
    }
}

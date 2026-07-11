use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::ViewTable;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::NamespaceIdent;
use moka::sync::Cache as SyncCache;
use tracing::{debug, error};

use sqe_core::config::StorageConfig;

use crate::catalog_provider::SqeCatalogProvider;
use crate::rest_catalog::SessionCatalog;
use crate::table_provider::SqeTableProvider;

/// TTL for the per-namespace table_names() cache. Short so DDL becomes
/// visible within a few seconds without each planning lookup paying two
/// REST round trips against Polaris.
const TABLE_NAMES_TTL_SECS: u64 = 5;

/// Return true if `name` is present in the namespace's table/view name set.
///
/// Pulled out of [`SqeSchemaProvider::table_exist`] so the existence check is
/// unit-testable without constructing a live `SessionCatalog` (which would
/// require a running Polaris). Issue #238.
fn name_exists_in(names: &[String], name: &str) -> bool {
    names.iter().any(|n| n == name)
}

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
    /// Small-file threshold in bytes for the direct-read fast path.
    small_file_threshold_bytes: u64,
    /// Concurrency for direct manifest walks during pruning.
    manifest_concurrency: usize,
    /// Prefetch concurrency for the direct-read fast path, propagated to
    /// each `SqeTableProvider`.
    prefetch_concurrency: usize,
    /// Issue #132: Tier-1 dynamic-filter clustering gate, propagated to each
    /// `SqeTableProvider`.
    runtime_filter_clustering_skip: bool,
    runtime_filter_uniform_threshold: f64,
    /// Bounded wait (ms) at scan open for pending dynamic filters,
    /// propagated to each `SqeTableProvider`.
    runtime_filter_wait_ms: u64,
    /// Issue #369: bloom-filter row-group probing of sealed runtime
    /// filters, propagated to each `SqeTableProvider`.
    runtime_filter_bloom_probe: bool,
    runtime_filter_bloom_max_values: usize,
    /// Short-TTL cache of table_names() results so repeated planning
    /// lookups during a single dbt run do not pay two REST round trips
    /// per call.
    table_names_cache: SyncCache<String, Arc<Vec<String>>>,
}

impl SqeSchemaProvider {
    /// Create a new schema provider for the given namespace.
    pub fn new(
        session_catalog: Arc<SessionCatalog>,
        namespace: String,
        storage_config: StorageConfig,
        warehouse: String,
    ) -> Self {
        let table_names_cache = SyncCache::builder()
            .max_capacity(128)
            .time_to_live(Duration::from_secs(TABLE_NAMES_TTL_SECS))
            .build();
        Self {
            session_catalog,
            namespace,
            storage_config,
            warehouse,
            prom_metrics: None,
            small_file_threshold_bytes: crate::iceberg_scan::DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
            manifest_concurrency: crate::iceberg_scan::DEFAULT_MANIFEST_CONCURRENCY,
            prefetch_concurrency: crate::iceberg_scan::DEFAULT_DIRECT_READ_CONCURRENCY,
            runtime_filter_clustering_skip: false,
            runtime_filter_uniform_threshold: 0.8,
            runtime_filter_wait_ms: crate::iceberg_scan::DEFAULT_RUNTIME_FILTER_WAIT_MS,
            runtime_filter_bloom_probe: true,
            runtime_filter_bloom_max_values: 65536,
            table_names_cache,
        }
    }

    /// Drop any cached `table_names()` result for this namespace. Call after
    /// DDL that creates or drops a table/view so the next planning lookup
    /// sees the change without waiting for the TTL.
    pub fn invalidate_table_names(&self) {
        self.table_names_cache.invalidate(&self.namespace);
    }

    /// Set the small-file threshold (bytes) for the direct-read fast path.
    #[must_use = "with_small_file_threshold consumes self; bind the returned provider"]
    pub fn with_small_file_threshold(mut self, threshold_bytes: u64) -> Self {
        self.small_file_threshold_bytes = threshold_bytes;
        self
    }

    /// Set the per-scan concurrency used when walking manifests for
    /// column-statistics pruning.
    #[must_use = "with_manifest_concurrency consumes self; bind the returned provider"]
    pub fn with_manifest_concurrency(mut self, concurrency: usize) -> Self {
        self.manifest_concurrency = concurrency.max(1);
        self
    }

    /// Set the prefetch concurrency propagated to every table provider's
    /// direct-read fast path. Sourced from `[storage] prefetch_concurrency`.
    pub fn with_prefetch_concurrency(mut self, concurrency: usize) -> Self {
        self.prefetch_concurrency = concurrency.max(1);
        self
    }

    /// Configure the Tier-1 dynamic-filter clustering gate (issue #132),
    /// propagated to every table provider.
    #[must_use = "with_runtime_filter_clustering consumes self; bind the returned provider"]
    pub fn with_runtime_filter_clustering(mut self, skip: bool, uniform_threshold: f64) -> Self {
        self.runtime_filter_clustering_skip = skip;
        self.runtime_filter_uniform_threshold = uniform_threshold;
        self
    }

    /// Bounded wait (ms) at scan open for pending dynamic filters,
    /// propagated to every table provider.
    #[must_use = "with_runtime_filter_wait_ms consumes self; bind the returned provider"]
    pub fn with_runtime_filter_wait_ms(mut self, wait_ms: u64) -> Self {
        self.runtime_filter_wait_ms = wait_ms;
        self
    }

    /// Bloom-filter (SBBF) row-group probing of sealed runtime filters
    /// (issue #369), propagated to every table provider.
    #[must_use = "with_runtime_filter_bloom consumes self; bind the returned provider"]
    pub fn with_runtime_filter_bloom(mut self, enabled: bool, max_values: usize) -> Self {
        self.runtime_filter_bloom_probe = enabled;
        self.runtime_filter_bloom_max_values = max_values;
        self
    }

    /// Attach Prometheus metrics to propagate to table providers.
    #[must_use = "with_metrics consumes self; bind the returned provider"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.prom_metrics = Some(metrics);
        self
    }
}

#[async_trait]
impl SchemaProvider for SqeSchemaProvider {

    // SAFETY NOTE: DataFusion's SchemaProvider::table_names() is synchronous by design
    // (returns Vec<String>, not a Future). Since our catalog is async (HTTP calls to
    // Polaris), we use `block_in_place` to bridge the sync-async gap. This yields the
    // current tokio thread first (avoiding deadlock with the current-thread runtime),
    // then blocks on the async call. This is a known DataFusion design constraint —
    // the SchemaProvider trait predates DataFusion's async catalog work. A future
    // DataFusion version may provide an async alternative.
    fn table_names(&self) -> Vec<String> {
        if let Some(cached) = self.table_names_cache.get(&self.namespace) {
            return (*cached).clone();
        }

        let catalog = self.session_catalog.clone();
        let catalog_for_views = catalog.clone();
        let ns = self.namespace.clone();
        let ns_for_views = ns.clone();

        let ns_ident = NamespaceIdent::new(ns.clone());
        let ns_ident_views = ns_ident.clone();

        let tables = crate::runtime_bridge::block_on_compat(async move {
            catalog.list_tables(&ns_ident).await
        });
        let mut names: Vec<String> = match tables {
            Some(Ok(t)) => t.iter().map(|t| t.name().to_string()).collect(),
            Some(Err(e)) => {
                error!(namespace = %ns, error = %e, "Failed to list tables");
                Vec::new()
            }
            None => {
                error!(namespace = %ns, "No tokio runtime available for table_names()");
                return Vec::new();
            }
        };

        let views = crate::runtime_bridge::block_on_compat(async move {
            catalog_for_views.list_views(&ns_ident_views).await
        });
        match views {
            Some(Ok(view_names)) => names.extend(view_names),
            Some(Err(e)) => {
                error!(namespace = %ns_for_views, error = %e, "Failed to list views");
            }
            None => {}
        }

        self.table_names_cache
            .insert(self.namespace.clone(), Arc::new(names.clone()));
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        // Issue #238: derive existence from the cached `table_names()` set
        // instead of issuing a fresh `table_exists()` + `load_view_sql` round
        // trip per call. `table_names()` already merges tables and views and is
        // backed by the 5s `table_names_cache`, so repeated existence probes
        // during one planning pass (DataFusion may call `table_exist` then
        // `table_names` on the same provider, and a dbt run checks many
        // relations) cost zero extra REST calls once the namespace list is warm.
        // None of these probes steals a tokio worker thread via the
        // `block_in_place + block_on` bridge any more.
        //
        // Freshness: `SqeCatalogProvider::schema()` builds a fresh
        // `SqeSchemaProvider` (with an empty cache) per lookup, so a new query
        // re-lists from Polaris and sees DDL committed by an earlier query. The
        // 5s window only applies within a single provider instance's lifetime;
        // `invalidate_table_names()` is available to drop the cache earlier if a
        // future caller mutates and re-checks on the same provider.
        //
        // Tradeoff: a cold cache lists every table/view in the namespace once
        // (one `list_tables` + one `list_views`) rather than a single
        // `table_exists` HEAD. The dbt burst pattern warms the cache on the
        // first probe, so subsequent checks are free. Issue #238.
        name_exists_in(&self.table_names(), name)
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
                        let provider = provider
                            .with_small_file_threshold(self.small_file_threshold_bytes)
                            .with_manifest_concurrency(self.manifest_concurrency)
                            .with_prefetch_concurrency(self.prefetch_concurrency)
                            .with_runtime_filter_clustering(
                                self.runtime_filter_clustering_skip,
                                self.runtime_filter_uniform_threshold,
                            )
                            .with_runtime_filter_wait_ms(self.runtime_filter_wait_ms)
                            .with_runtime_filter_bloom(
                                self.runtime_filter_bloom_probe,
                                self.runtime_filter_bloom_max_values,
                            );
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

        // A view body may reference tables in OTHER catalogs (e.g. a workspace
        // ontology view in `ws_team_a.ontology` selecting from
        // `team_a_data.public.events`). Register every sibling warehouse the
        // view's SQL names, using the same user token — Polaris/OPA still
        // enforce access. Failures are logged and skipped so the planner
        // below reports the precise unresolved reference.
        for sibling in sqe_sql::extract_catalog_qualifiers_from_sql(&sql) {
            if &sibling == catalog_name {
                continue;
            }
            match self.session_catalog.for_sibling_warehouse(&sibling).await {
                Ok(sc) => {
                    match SqeCatalogProvider::try_new(
                        Arc::new(sc),
                        self.storage_config.clone(),
                        sibling.clone(),
                    )
                    .await
                    {
                        Ok(p) => {
                            mini_ctx.register_catalog(&sibling, Arc::new(p));
                            tracing::info!(
                                view = %name,
                                catalog = %sibling,
                                "view planning: registered sibling catalog referenced by view body"
                            );
                        }
                        Err(e) => tracing::warn!(
                            view = %name,
                            catalog = %sibling,
                            error = %e,
                            "view planning: failed to build sibling catalog provider"
                        ),
                    }
                }
                Err(e) => tracing::warn!(
                    view = %name,
                    catalog = %sibling,
                    error = %e,
                    "view planning: failed to open sibling warehouse session"
                ),
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn name_exists_in_matches_tables_and_views() {
        let names = vec![
            "orders".to_string(),
            "customers".to_string(),
            "sales_view".to_string(),
        ];
        assert!(name_exists_in(&names, "orders"));
        assert!(name_exists_in(&names, "sales_view"));
        assert!(!name_exists_in(&names, "missing"));
        // Existence is case-sensitive and exact, mirroring catalog identifiers.
        assert!(!name_exists_in(&names, "Orders"));
        assert!(!name_exists_in(&[], "orders"));
    }

    /// Issue #238: a warm `table_names_cache` serves repeated existence probes
    /// without recomputing the (in production, REST-backed) name list. We model
    /// the REST round trip with a counter that increments only on a cache miss;
    /// the second and third reads must not bump it.
    #[test]
    fn cached_table_names_avoid_recompute_on_repeated_existence_checks() {
        let cache: SyncCache<String, Arc<Vec<String>>> = SyncCache::builder()
            .max_capacity(128)
            .time_to_live(Duration::from_secs(TABLE_NAMES_TTL_SECS))
            .build();
        let ns = "analytics".to_string();
        let rest_calls = AtomicUsize::new(0);

        // Mirror table_names(): cache hit short-circuits, miss "lists" once.
        let names_for = |ns: &str| -> Arc<Vec<String>> {
            if let Some(cached) = cache.get(ns) {
                return cached;
            }
            rest_calls.fetch_add(1, Ordering::SeqCst);
            let names = Arc::new(vec!["orders".to_string(), "customers".to_string()]);
            cache.insert(ns.to_string(), names.clone());
            names
        };

        // First probe: cold cache, one "REST" list.
        assert!(name_exists_in(&names_for(&ns), "orders"));
        // Repeated probes during the same dbt burst: served from cache.
        assert!(name_exists_in(&names_for(&ns), "customers"));
        assert!(!name_exists_in(&names_for(&ns), "missing"));

        assert_eq!(
            rest_calls.load(Ordering::SeqCst),
            1,
            "table_exist should hit the catalog at most once while the cache is warm"
        );
    }
}

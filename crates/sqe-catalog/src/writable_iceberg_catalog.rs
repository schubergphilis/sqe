//! Writable `CatalogProvider` for embedded iceberg catalogs.
//!
//! `iceberg-datafusion::IcebergCatalogProvider` is read-only: its
//! `register_schema` falls through to the trait default which returns
//! `not_impl_err!("Registering new schemas is not supported")`. That
//! means `CREATE SCHEMA <iceberg_catalog>.<ns>` fails in DataFusion's
//! SQL pipeline before reaching the iceberg::Catalog API.
//!
//! This module wraps the same `Arc<dyn iceberg::Catalog>` and
//! implements the missing pieces. Read paths (`schema_names`,
//! `schema`) reuse `IcebergSchemaProvider` from upstream so existing
//! behaviour is preserved bit-for-bit. Write paths
//! (`register_schema`, `deregister_schema`) delegate to
//! `iceberg::Catalog::create_namespace` / `drop_namespace` via the
//! runtime-flavor-aware `crate::runtime_bridge::block_on_compat`
//! helper (issue #81 / #83). The earlier
//! `spawn_blocking + futures::executor::block_on(join)` pattern could
//! stall a worker waiting on the join future under busy schema DDL.
//!
//! `CREATE TABLE <cat>.<ns>.<table> (col TYPE, ...)` already works
//! because `IcebergSchemaProvider` from upstream implements
//! `register_table`. CTAS (`CREATE TABLE ... AS SELECT ...`) is still
//! out of scope: the upstream `register_table` rejects providers that
//! carry data, and the embedded mode does not yet have a Parquet
//! writer + iceberg-transaction commit pipeline.
//!
//! ## Why a custom CatalogProvider rather than a SQL interceptor?
//!
//! An earlier draft tried to parse incoming SQL with `sqlparser` and
//! route DDL targeting iceberg catalogs to the iceberg API directly.
//! That worked but operated at the wrong altitude: every other
//! engine in the DataFusion ecosystem expresses catalog semantics
//! through the `CatalogProvider` trait. Doing the same here means
//! `CREATE TABLE foo (id BIGINT)` works through DataFusion's normal
//! plan path, with whatever planner extensions and validation come
//! along — for free. Less code, fewer corner cases, no parser
//! dialect to maintain.

use std::sync::{Arc, OnceLock, Weak};

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_iceberg::IcebergCatalogProvider;
use iceberg::inspect::MetadataTableType;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::rest_catalog::SessionCatalog;

type SchemaMap = DashMap<String, Arc<dyn SchemaProvider>>;

/// A `CatalogProvider` over an iceberg::Catalog that supports both
/// reads and namespace-level writes.
#[derive(Debug)]
pub struct WritableIcebergCatalog {
    /// Underlying iceberg::Catalog. Cloned for every async write so
    /// long-lived references aren't held across await points in
    /// blocking contexts.
    catalog: Arc<dyn Catalog>,
    /// Cached schema providers, keyed by namespace name. Populated
    /// at construction time and updated on register/deregister.
    /// Lookups stay sync as DataFusion's `CatalogProvider::schema`
    /// signature requires.
    ///
    /// External writers (another process touching the same SQLite
    /// file, an `iceberg::Catalog::create_namespace` call from the
    /// DDL bypass path) won't be reflected without a rebuild. For
    /// embedded single-process workflows that's acceptable; the
    /// cluster path uses richer caching with TTL eviction.
    schemas: Arc<SchemaMap>,
    /// Where a name that is not a table is looked up as an Iceberg view;
    /// unset for a catalog without views. Shared with every schema
    /// provider in `schemas`, so [`Self::with_views`] reaches the ones
    /// already built.
    views: Arc<OnceLock<ViewSource>>,
}

impl WritableIcebergCatalog {
    /// The iceberg catalog behind this provider, for writes that commit
    /// through `sqe-write` rather than DataFusion's DML hooks.
    pub fn client(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }

    /// Resolve Iceberg views of this catalog through `session_catalog`, a
    /// REST `SessionCatalog` over the same server: a name that is not a
    /// table loads the view's SQL and plans it with this catalog
    /// registered as `name` (the session's name for it) and as the
    /// warehouse. The `iceberg::Catalog` trait has no view operations, so
    /// without this a view reads as a missing table.
    pub fn with_views(self, session_catalog: Arc<SessionCatalog>, name: &str) -> Self {
        let source = ViewSource {
            warehouse: session_catalog.warehouse().to_string(),
            session_catalog,
            name: name.to_string(),
            schemas: Arc::downgrade(&self.schemas),
            listed: DashMap::new(),
        };
        // `self` is new, so the cell is still empty.
        let _ = self.views.set(source);
        self
    }

    fn wrap(&self, namespace: &str, provider: Arc<dyn SchemaProvider>) -> Arc<dyn SchemaProvider> {
        Arc::new(BaseTablesOnly {
            inner: provider,
            catalog: Arc::clone(&self.catalog),
            namespace: namespace.to_string(),
            views: Arc::clone(&self.views),
        })
    }

    /// Build a writable catalog by snapshotting the current set of
    /// namespaces in `client`. New namespaces created later through
    /// this provider's `register_schema` are added to the cache; the
    /// caller is responsible for never sharing the underlying
    /// iceberg::Catalog across `WritableIcebergCatalog` instances if
    /// they want a consistent view of the namespace list.
    pub async fn try_new(client: Arc<dyn Catalog>) -> anyhow::Result<Self> {
        // Reuse the upstream provider's bootstrap path to get the
        // SchemaProvider builders right; we drop the wrapper itself
        // afterwards because we only need the underlying schemas.
        let upstream = IcebergCatalogProvider::try_new(client.clone())
            .await
            .map_err(|e| anyhow::anyhow!("IcebergCatalogProvider build failed: {e}"))?;
        let this = Self {
            catalog: client,
            schemas: Arc::new(DashMap::new()),
            views: Arc::new(OnceLock::new()),
        };
        for name in upstream.schema_names() {
            if let Some(provider) = upstream.schema(&name) {
                this.schemas
                    .insert(name.clone(), this.wrap(&name, provider));
            }
        }
        Ok(this)
    }
}

impl CatalogProvider for WritableIcebergCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|e| e.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|e| e.value().clone())
    }

    /// `CREATE SCHEMA <this_catalog>.<name>` lands here. We ignore
    /// the `_provider` argument because DataFusion passes a stub
    /// `MemorySchemaProvider`; the source of truth is the
    /// iceberg::Catalog. After the namespace is created we build a
    /// fresh `IcebergSchemaProvider` (via the upstream constructor
    /// path) and cache it so subsequent `CREATE TABLE` / `SELECT`
    /// resolve against the same handle.
    // `CREATE SCHEMA` through DataFusion's own DDL executor lands here. The
    // hook is synchronous and the Iceberg catalog is async; it used to
    // bridge with `runtime_bridge::block_on_compat` from inside the planner
    // (architecture review 806). SQE's statement paths (the coordinator's
    // `catalog_ops`, the embedded CLI's DDL interceptor) call
    // [`WritableIcebergCatalog::create_namespace`] on the async path
    // instead, so reaching this hook means a planner entry point handed
    // the statement to DataFusion unintercepted; say so rather than block.
    fn register_schema(
        &self,
        name: &str,
        _provider: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::Plan(format!(
            "CREATE SCHEMA {name}: this Iceberg catalog cannot be written through \
             DataFusion's synchronous register_schema hook; route the statement to \
             WritableIcebergCatalog::create_namespace on the async path (review 806)"
        )))
    }

    fn deregister_schema(
        &self,
        name: &str,
        _cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::Plan(format!(
            "DROP SCHEMA {name}: this Iceberg catalog cannot be written through \
             DataFusion's synchronous deregister_schema hook; route the statement to \
             WritableIcebergCatalog::drop_namespace on the async path (review 806)"
        )))
    }
}

impl WritableIcebergCatalog {
    /// `CREATE SCHEMA [IF NOT EXISTS] <name>` on this catalog, on the async
    /// path. After the namespace exists a fresh `IcebergSchemaProvider` is
    /// built (via the upstream constructor path) and cached so subsequent
    /// `CREATE TABLE` / `SELECT` resolve against the same handle. Duplicates
    /// are rejected explicitly unless `if_not_exists`: the iceberg catalog
    /// fails loud on create-after-create, and surfacing that as an error is
    /// more useful than silently swapping cache entries.
    pub async fn create_namespace(&self, name: &str, if_not_exists: bool) -> anyhow::Result<()> {
        if self.schemas.contains_key(name) {
            if if_not_exists {
                return Ok(());
            }
            anyhow::bail!("schema {name} already exists");
        }
        let ns = NamespaceIdent::new(name.to_string());
        self.catalog
            .create_namespace(&ns, std::collections::HashMap::new())
            .await
            .map_err(|e| anyhow::anyhow!("create_namespace({name}): {e}"))?;
        let upstream = IcebergCatalogProvider::try_new(self.catalog.clone())
            .await
            .map_err(|e| anyhow::anyhow!("rebuild after create_namespace: {e}"))?;
        let provider = upstream
            .schema(name)
            .ok_or_else(|| anyhow::anyhow!("schema {name} disappeared between create and read"))?;
        self.schemas
            .insert(name.to_string(), self.wrap(name, provider));
        Ok(())
    }

    /// Reload the cached schema provider of namespace `name` from the
    /// catalog. A table created outside DataFusion (the embedded CLI's
    /// `CREATE TABLE` through SQE's write handler) is not in the cached
    /// listing until this runs.
    pub async fn refresh_namespace(&self, name: &str) -> anyhow::Result<()> {
        let provider = IcebergCatalogProvider::schema_provider(
            self.catalog.clone(),
            NamespaceIdent::new(name.to_string()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("reload schema {name}: {e}"))?;
        self.schemas
            .insert(name.to_string(), self.wrap(name, provider));
        Ok(())
    }

    /// Re-list the Iceberg views of `namespace`, so `SHOW TABLES` and
    /// information_schema show them (`table_names` is synchronous and serves
    /// this snapshot). A no-op without [`Self::with_views`]; a server without
    /// the view endpoints lists none.
    pub async fn refresh_views(&self, namespace: &str) -> anyhow::Result<()> {
        let Some(source) = self.views.get() else {
            return Ok(());
        };
        let ns = NamespaceIdent::new(namespace.to_string());
        let views = match source.session_catalog.list_views(&ns).await {
            Ok(views) => views,
            Err(e) if e.is_not_found() || server_has_no_views(&e) => Vec::new(),
            Err(e) => anyhow::bail!("list views of {namespace}: {e}"),
        };
        source.listed.insert(namespace.to_string(), views);
        Ok(())
    }

    /// [`Self::refresh_views`] for every namespace, one request each. A
    /// namespace that fails does not stop the others; the first error is
    /// returned.
    pub async fn refresh_all_views(&self) -> anyhow::Result<()> {
        let mut first_err = None;
        for namespace in self.schema_names() {
            if let Err(e) = self.refresh_views(&namespace).await {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// `DROP SCHEMA [IF EXISTS] <name>` on this catalog, on the async path.
    /// `CASCADE` is not implemented: the iceberg catalog refuses to drop a
    /// non-empty namespace and SQE does not delete tables on its behalf.
    pub async fn drop_namespace(
        &self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> anyhow::Result<()> {
        if cascade {
            anyhow::bail!(
                "DROP SCHEMA ... CASCADE is not implemented for embedded iceberg catalogs"
            );
        }
        if !self.schemas.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            anyhow::bail!("schema {name} does not exist");
        }
        let ns = NamespaceIdent::new(name.to_string());
        self.catalog
            .drop_namespace(&ns)
            .await
            .map_err(|e| anyhow::anyhow!("drop_namespace({name}): {e}"))?;
        self.schemas.remove(name);
        if let Some(views) = self.views.get() {
            views.listed.remove(name);
        }
        Ok(())
    }
}

/// The upstream `IcebergSchemaProvider` lists `t$snapshots`, `t$manifests`
/// and `t$history` next to every table `t`, so `SHOW TABLES` and
/// `information_schema.tables` held four rows per table, all `BASE TABLE`.
/// Trino and Spark list base tables only. This wrapper drops the metadata
/// names from the listing; `t$snapshots` still resolves when queried.
///
/// It is also where a name that is not a table falls back to the catalog's
/// Iceberg views, when it has a [`ViewSource`].
#[derive(Debug)]
struct BaseTablesOnly {
    inner: Arc<dyn SchemaProvider>,
    /// The catalog `inner` reads, so a base table is scanned through SQE's
    /// own [`crate::table_provider::SqeTableProvider`] rather than the
    /// vendored `IcebergTableProvider`.
    catalog: Arc<dyn Catalog>,
    namespace: String,
    views: Arc<OnceLock<ViewSource>>,
}

/// The view side of a REST catalog: its `SessionCatalog` for the view
/// endpoints, and the names a view body may call the catalog by.
#[derive(Debug)]
struct ViewSource {
    session_catalog: Arc<SessionCatalog>,
    /// The catalog's name in this session (the `ATTACH ... AS` name).
    name: String,
    /// The warehouse, which a body written by another engine names.
    warehouse: String,
    /// The catalog's schemas, to plan a body against. Weak: every schema
    /// provider in the map holds this source.
    schemas: Weak<SchemaMap>,
    /// The views of each namespace as last listed by
    /// [`WritableIcebergCatalog::refresh_views`], for the synchronous
    /// `table_names` / `table_exist`. A view missing here still loads by
    /// name.
    listed: DashMap<String, Vec<String>>,
}

impl ViewSource {
    fn is_listed(&self, namespace: &str, name: &str) -> bool {
        self.listed
            .get(namespace)
            .is_some_and(|views| views.iter().any(|v| v == name))
    }
}

impl ViewSource {
    /// View `name` in `namespace` as a `ViewTable`, or `None` when there is
    /// no such view. Planned like the coordinator's (`view_planning_context`,
    /// `view_table`), against this catalog only: a body naming another
    /// catalog does not resolve.
    ///
    /// Boxed for the same reason as `SqeSchemaProvider::plan_view`: the
    /// future holds a `DataFrame` across awaits.
    fn view<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> futures::future::BoxFuture<'a, DFResult<Option<Arc<dyn TableProvider>>>> {
        let plan = async move {
            let ns = NamespaceIdent::new(namespace.to_string());
            let crate::rest_catalog::ViewBody {
                sql,
                default_schema,
            } = match self.session_catalog.load_view_body(&ns, name).await {
                Ok(Some(body)) => body,
                Ok(None) => return Ok(None),
                Err(e) if e.is_not_found() || server_has_no_views(&e) => return Ok(None),
                // A 403, a 5xx or an unsupported view dialect reaches the
                // user as itself, not as "table not found".
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            };
            let Some(schemas) = self.schemas.upgrade() else {
                return Ok(None);
            };
            let ctx = crate::schema_provider::view_planning_context(&self.name, &default_schema)?;
            let planning: Arc<dyn CatalogProvider> = Arc::new(ViewPlanningCatalog(schemas));
            ctx.register_catalog(&self.name, Arc::clone(&planning));
            if self.warehouse != self.name {
                ctx.register_catalog(&self.warehouse, planning);
            }
            let df = crate::file_tvf_prepare::plan_sql(&ctx, &sql)
                .await
                .map_err(|e| {
                    DataFusionError::External(
                        format!("Failed to plan view '{name}' SQL: {e}").into(),
                    )
                })?;
            crate::schema_provider::view_table(
                df,
                name,
                sql,
                &self.name,
                &default_schema,
                &[&self.warehouse],
            )
            .map(Some)
        };
        Box::pin(crate::schema_provider::plan_view_guarded(
            &self.warehouse,
            namespace,
            name,
            plan,
        ))
    }
}

/// A REST server without the view endpoints answers a view lookup with 400,
/// 405 or 501; for it every name is simply not a view.
fn server_has_no_views(e: &sqe_core::SqeError) -> bool {
    matches!(
        e,
        sqe_core::SqeError::CatalogHttp {
            status: 400 | 405 | 501,
            ..
        }
    )
}

/// The catalog a view body is planned against: the same schema providers
/// the session reads, so a view over a view resolves too.
#[derive(Debug)]
struct ViewPlanningCatalog(Arc<SchemaMap>);

impl CatalogProvider for ViewPlanningCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.0.iter().map(|e| e.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.0.get(name).map(|e| e.value().clone())
    }
}

fn is_metadata_table_name(name: &str) -> bool {
    name.split_once('$')
        .is_some_and(|(_, kind)| MetadataTableType::try_from(kind).is_ok())
}

#[async_trait::async_trait]
impl SchemaProvider for BaseTablesOnly {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .table_names()
            .into_iter()
            .filter(|name| !is_metadata_table_name(name))
            .collect();
        if let Some(listed) = self
            .views
            .get()
            .and_then(|views| views.listed.get(&self.namespace))
        {
            for view in listed.iter() {
                if !names.contains(view) {
                    names.push(view.clone());
                }
            }
        }
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // A base table reads through `SqeTableProvider`, the provider the
        // coordinator uses: its `IcebergScanExec` splits large files into
        // byte ranges and the parallel-scan rules fan it out, where the
        // vendored `IcebergTableScan` is always one partition and reloads
        // the table from the catalog on every scan. `$snapshots` and the
        // other metadata tables stay on the vendored provider.
        if !is_metadata_table_name(name) && self.inner.table_exist(name) {
            let ident = TableIdent::new(
                NamespaceIdent::new(self.namespace.clone()),
                name.to_string(),
            );
            let table = self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let provider = crate::table_provider::SqeTableProvider::try_new(table)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(Arc::new(provider)));
        }
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        }
        match self.views.get() {
            Some(views) => views.view(&self.namespace, name).await,
            None => Ok(None),
        }
    }

    async fn table_type(&self, name: &str) -> DFResult<Option<TableType>> {
        if let Some(kind) = self.inner.table_type(name).await? {
            return Ok(Some(kind));
        }
        // A listed view answers without loading and planning its body, so
        // information_schema does not plan every view it lists.
        if self
            .views
            .get()
            .is_some_and(|views| views.is_listed(&self.namespace, name))
        {
            return Ok(Some(TableType::View));
        }
        Ok(self.table(name).await?.map(|t| t.table_type()))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
            || self
                .views
                .get()
                .is_some_and(|views| views.is_listed(&self.namespace, name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::MemorySchemaProvider;
    use datafusion::datasource::empty::EmptyTable;

    #[tokio::test]
    async fn listing_drops_metadata_tables_but_they_still_resolve() {
        let inner = Arc::new(MemorySchemaProvider::new());
        let empty: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        for name in ["t", "t$snapshots", "t$manifests", "t$history", "cost$eur"] {
            inner
                .register_table(name.to_string(), empty.clone())
                .unwrap();
        }
        let wrapped = BaseTablesOnly {
            inner,
            catalog: memory_catalog().await,
            namespace: "ns".to_string(),
            views: Arc::new(OnceLock::new()),
        };
        let mut names = wrapped.table_names();
        names.sort();
        assert_eq!(names, vec!["cost$eur".to_string(), "t".to_string()]);
        assert!(wrapped.table_exist("t$snapshots"));
        assert!(wrapped.table("t$history").await.unwrap().is_some());
    }

    async fn memory_catalog() -> Arc<dyn Catalog> {
        use iceberg::CatalogBuilder;
        Arc::new(
            iceberg::memory::MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    [("warehouse".to_string(), "memory:///wh".to_string())].into(),
                )
                .await
                .unwrap(),
        )
    }

    /// A base table reads through `SqeTableProvider` (parallel, byte-range
    /// split scan), not the vendored one-partition `IcebergTableProvider`.
    #[tokio::test]
    async fn base_tables_read_through_sqe_table_provider() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use iceberg::TableCreation;

        let catalog = memory_catalog().await;
        let ns = NamespaceIdent::new("ns".into());
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .unwrap();
        let schema = IcebergSchema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("t".into())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        let writable = WritableIcebergCatalog::try_new(catalog).await.unwrap();
        let schema = writable.schema("ns").expect("namespace listed");
        let table = schema.table("t").await.unwrap().expect("table resolves");
        let any: &dyn std::any::Any = table.as_ref();
        assert!(
            any.downcast_ref::<crate::table_provider::SqeTableProvider>()
                .is_some(),
            "base table should be an SqeTableProvider"
        );
        assert!(schema.table("missing").await.unwrap().is_none());
    }
}

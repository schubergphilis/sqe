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

use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use iceberg::{Catalog, NamespaceIdent};
use iceberg_datafusion::IcebergCatalogProvider;

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
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl WritableIcebergCatalog {
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
        let schemas: DashMap<String, Arc<dyn SchemaProvider>> = DashMap::new();
        for name in upstream.schema_names() {
            if let Some(provider) = upstream.schema(&name) {
                schemas.insert(name, provider);
            }
        }
        Ok(Self {
            catalog: client,
            schemas,
        })
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
        self.schemas.insert(name.to_string(), provider);
        Ok(())
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
            anyhow::bail!("DROP SCHEMA ... CASCADE is not implemented for embedded iceberg catalogs");
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
        Ok(())
    }
}

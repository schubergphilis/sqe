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

use std::any::Any;
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
        Ok(Self { catalog: client, schemas })
    }
}

impl CatalogProvider for WritableIcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    fn register_schema(
        &self,
        name: &str,
        _provider: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        // Reject duplicates explicitly; the trait contract says we
        // could replace, but the iceberg::Catalog will fail-loud on
        // create-after-create and surfacing that error verbatim is
        // more useful than silently swapping cache entries.
        if self.schemas.contains_key(name) {
            return Err(DataFusionError::Execution(format!(
                "schema {name} already exists"
            )));
        }

        let catalog = self.catalog.clone();
        let ns_name = name.to_string();
        // The CatalogProvider trait method is sync; iceberg::Catalog is
        // async. crate::runtime_bridge::block_on_compat picks
        // block_in_place on multi-thread runtimes and an off-thread
        // block_on on current-thread. The earlier
        // spawn_blocking + futures::executor::block_on(join) pattern
        // could stall a worker waiting on the join future (issue #81).
        let ns_name_for_err = ns_name.clone();
        let provider = crate::runtime_bridge::block_on_compat(async move {
            let ns = NamespaceIdent::new(ns_name.clone());
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .map_err(|e| anyhow::anyhow!("create_namespace({ns_name}): {e}"))?;
            let upstream_catalog =
                iceberg_datafusion::IcebergCatalogProvider::try_new(catalog.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("rebuild after create_namespace: {e}"))?;
            upstream_catalog.schema(&ns_name).ok_or_else(|| {
                anyhow::anyhow!("schema {ns_name} disappeared between create and read")
            })
        })
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "register_schema({ns_name_for_err}): no tokio runtime available"
            ))
        })?
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.schemas.insert(name.to_string(), provider);
        Ok(None)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        if cascade {
            return Err(DataFusionError::NotImplemented(
                "DROP SCHEMA ... CASCADE is not implemented for embedded iceberg catalogs"
                    .to_string(),
            ));
        }
        if !self.schemas.contains_key(name) {
            // None signals "not present" per the trait doc.
            return Ok(None);
        }

        let catalog = self.catalog.clone();
        let ns_name = name.to_string();
        let ns_name_for_err = ns_name.clone();
        crate::runtime_bridge::block_on_compat(async move {
            let ns = NamespaceIdent::new(ns_name.clone());
            catalog
                .drop_namespace(&ns)
                .await
                .map_err(|e| anyhow::anyhow!("drop_namespace({ns_name}): {e}"))
        })
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "deregister_schema({ns_name_for_err}): no tokio runtime available"
            ))
        })?
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        Ok(self.schemas.remove(name).map(|(_, v)| v))
    }
}

//! Tag sources for Hive external tables, and the dispatcher in front of them.
//!
//! Without this module every Hive query is denied. `plan_rewriter.rs` calls
//! `TagSource::column_tags` for every non-reserved `TableScan` and turns
//! `None` into a `lit(false)` row filter, and `CacheTagSource` (the
//! Iceberg-backed source) returns `None` for anything absent from the
//! `TableMetadataCache`, which a Hive table always is: it is never inserted
//! there.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`

use std::collections::HashMap;
use std::sync::Arc;

use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::TagSource;

/// Reads `sqe.column_tags` from a cached Hive manifest.
///
/// `column_tags` is synchronous, so this can only answer from cache, never by
/// fetching. The ordering it relies on: plan rewriting runs after LogicalPlan
/// construction, which has already called `HiveSchemaProvider::table()` for
/// every scanned table and populated the manifest cache. A cache miss is
/// reported as unknown (`None`), never as no-tags: an entry that has not been
/// loaded yet is not the same as a table confirmed to carry no tags.
pub struct HiveTagSource {
    cache: Arc<HiveMetadataCache>,
}

impl HiveTagSource {
    pub fn new(cache: Arc<HiveMetadataCache>) -> Self {
        Self { cache }
    }
}

impl TagSource for HiveTagSource {
    fn column_tags(
        &self,
        _catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        // Hive databases are single-level, so the last namespace component is
        // the database name regardless of how many parts the caller passed.
        let database = namespace.last()?;
        let manifest = self.cache.get(database, table)?;
        Some(manifest.sqe.column_tags.clone().into_iter().collect())
    }
}

/// Routes `column_tags` by catalog name: a catalog registered in
/// `hive_catalogs` goes to `hive`, everything else goes to `fallback`.
///
/// `fallback` is the fail-closed Iceberg source in production wiring. An
/// unmatched or unknown catalog goes to `fallback` and this never returns
/// `Some(empty)` of its own accord: a permissive default in a dispatcher is
/// a silent mask bypass, and this is exactly where a future third catalog
/// kind would introduce one if the default here were anything but "ask the
/// fallback".
///
/// A `None` catalog (a two-part reference, `sales.orders`) also goes to
/// `fallback` -- but this is NOT because "no attached Hive root ever is"
/// `None` in some self-evident sense (an earlier version of this comment
/// claimed exactly that, and it was false: DataFusion resolves a two-part
/// reference against the session's default catalog at table-lookup time
/// while leaving the STORED `TableReference` on the scan node as `Partial`,
/// catalog `None`, so if a Hive root's name were ever the session default
/// catalog, this dispatcher would still see `None` here and route to
/// `fallback` regardless). It holds because `attach_configured_hive_roots`
/// (`runtime_catalog.rs`) and `handle_attach`'s `hive_external` branch
/// (`query_handler.rs`) both refuse to attach a Hive root whose name equals
/// `config.resolve_default_catalog()`. That refusal, not an assumption
/// about `None`, is what makes a `None` catalog here safe to always treat
/// as non-Hive: a two-part reference can only ever resolve against an
/// Iceberg default catalog, because a Hive one is never allowed to hold
/// that name.
///
/// `hive_catalogs` is read on every call, not cached at construction, so a
/// root attached after this router was built is routed correctly the very
/// next query.
pub struct CatalogRoutedTagSource {
    hive_catalogs: Arc<HiveCatalogs>,
    hive: Arc<dyn TagSource>,
    fallback: Arc<dyn TagSource>,
}

impl CatalogRoutedTagSource {
    pub fn new(
        hive_catalogs: Arc<HiveCatalogs>,
        hive: Arc<dyn TagSource>,
        fallback: Arc<dyn TagSource>,
    ) -> Self {
        Self {
            hive_catalogs,
            hive,
            fallback,
        }
    }
}

impl TagSource for CatalogRoutedTagSource {
    fn column_tags(
        &self,
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        if self.hive_catalogs.contains_name(catalog) {
            self.hive.column_tags(catalog, namespace, table)
        } else {
            self.fallback.column_tags(catalog, namespace, table)
        }
    }
}

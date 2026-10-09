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

/// Reads a cached Hive manifest's column tags through
/// `HiveManifest::column_tags`: the sidecar `sqe.column_tags` block plus the
/// `sqe.column-tags` table and column parameters a Glue catalog carries.
///
/// `column_tags` is synchronous, so this can only answer from cache, never by
/// fetching. The ordering it relies on: plan rewriting runs after LogicalPlan
/// construction, which has already called `HiveSchemaProvider::table()` for
/// every scanned table and populated the manifest cache. A cache miss is
/// reported as unknown (`None`), never as no-tags: an entry that has not been
/// loaded yet is not the same as a table confirmed to carry no tags. A tag
/// parameter that does not parse is unknown too.
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
        match manifest.column_tags() {
            Ok(tags) => Some(tags.into_iter().collect()),
            Err(e) => {
                tracing::warn!(
                    database = %database,
                    table = %table,
                    error = %e,
                    "hive tag_source: unreadable column tags, tag state UNKNOWN (fail closed)"
                );
                None
            }
        }
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
/// A `None` catalog also goes to `fallback`. DataFusion keeps the scan's
/// reference as written, so a two-part `sales.orders` carries no catalog
/// even when the session default is a Hive root (`USE <root>.<db>`). The
/// policy rewriter therefore qualifies every scan reference from the
/// planning context's `TableScope` before calling here, and such a
/// reference arrives with its catalog. A `None` that still reaches this
/// router came from a caller with no scope, and `fallback` (fail-closed)
/// is the safe answer for it. `attach_configured_hive_roots` and
/// `handle_attach`'s `hive_external` branch additionally refuse a Hive
/// root named like `config.resolve_default_catalog()`.
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

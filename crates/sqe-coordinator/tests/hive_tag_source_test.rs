//! Proves the two failure modes Task 7 exists to close:
//!
//! 1. Every Hive query is denied, because the Iceberg-backed `TagSource`
//!    (`CacheTagSource`) returns `None` for a table it never cached, and the
//!    rewriter turns `None` into a `lit(false)` row filter.
//! 2. A grant written for an Iceberg table governs a Hive table of the same
//!    name, because `resolve_policy_key` dropped the catalog.
//!
//! `CatalogRoutedTagSource` is exercised directly here (unit-shaped, no
//! LogicalPlan), matching the level `TagSource` itself is defined at.

use std::collections::HashMap;
use std::sync::Arc;

use sqe_catalog::hive::manifest::HiveManifest;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::TagSource;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// A stand-in for the Iceberg-backed source: unknown table means unknown tag
/// state, which the rewriter turns into a deny.
#[derive(Debug)]
struct AlwaysUnknown;
impl TagSource for AlwaysUnknown {
    fn column_tags(
        &self,
        _c: Option<&str>,
        _n: &[String],
        _t: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        None
    }
}

/// Build a `HiveManifest` the same way `CREATE EXTERNAL TABLE` would: parse a
/// minimal DDL string, extract the spec, then hand it to `HiveManifest::from_spec`.
fn manifest_for(database: &str, table: &str) -> HiveManifest {
    let sql = format!(
        "CREATE EXTERNAL TABLE {table} (id BIGINT) LOCATION 's3://bucket/{database}/{table}/'"
    );
    let stmts = Parser::parse_sql(&GenericDialect {}, &sql).expect("parse DDL");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract ok")
        .expect("is an external table");
    HiveManifest::from_spec(&spec, database, "test-owner", "2026-08-19T00:00:00Z")
}

/// A cache holding one manifest at `(database, table)`, and a `HiveCatalogs`
/// that already has `catalog` registered (the post-ATTACH state).
fn fixture_with_cached_manifest(
    catalog: &str,
    database: &str,
    table: &str,
) -> (Arc<HiveMetadataCache>, Arc<HiveCatalogs>) {
    let cache = Arc::new(HiveMetadataCache::new());
    cache.insert(database, table, manifest_for(database, table));
    let hive_catalogs = Arc::new(HiveCatalogs::new());
    hive_catalogs.insert(catalog);
    (cache, hive_catalogs)
}

/// Same cached manifest, but `catalog` is NOT yet registered (the pre-ATTACH
/// state), so a test can attach mid-flight and prove the router is live.
fn fixture_with_cached_manifest_unattached(
    _catalog: &str,
    database: &str,
    table: &str,
) -> (Arc<HiveMetadataCache>, Arc<HiveCatalogs>) {
    let cache = Arc::new(HiveMetadataCache::new());
    cache.insert(database, table, manifest_for(database, table));
    (cache, Arc::new(HiveCatalogs::new()))
}

/// A cached manifest whose `sqe.column_tags` carries one tagged column.
fn fixture_with_tagged_manifest(
    catalog: &str,
    database: &str,
    table: &str,
    column: &str,
    tag: &str,
) -> (Arc<HiveMetadataCache>, Arc<HiveCatalogs>) {
    let mut manifest = manifest_for(database, table);
    manifest
        .sqe
        .column_tags
        .insert(column.to_string(), vec![tag.to_string()]);
    let cache = Arc::new(HiveMetadataCache::new());
    cache.insert(database, table, manifest);
    let hive_catalogs = Arc::new(HiveCatalogs::new());
    hive_catalogs.insert(catalog);
    (cache, hive_catalogs)
}

#[test]
fn a_hive_table_is_answered_from_the_manifest_cache_not_denied() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    let got = routed.column_tags(Some("lake"), &["sales".to_string()], "orders");
    assert_eq!(
        got,
        Some(HashMap::new()),
        "an untagged Hive table is known-no-tags; None here would deny every Hive query"
    );
}

#[test]
fn a_hive_table_missing_from_the_cache_still_fails_closed() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "not_loaded"),
        None,
        "a cache miss is unknown tag state, same as the Iceberg source"
    );
}

#[test]
fn an_iceberg_table_still_reaches_the_iceberg_source() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(Some("iceberg"), &["hr".to_string()], "employees"),
        None
    );
}

#[test]
fn an_unknown_catalog_routes_to_the_fail_closed_source_not_to_empty() {
    // A permissive default in a dispatcher is a silent mask bypass.
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(None, &["sales".to_string()], "orders"),
        None
    );
    assert_eq!(
        routed.column_tags(Some("who_is_this"), &["sales".to_string()], "orders"),
        None
    );
}

#[test]
fn a_root_attached_after_the_rewriter_was_built_still_gets_the_hive_key() {
    // The regression guard for the staleness path. The enforcer is built once
    // process-wide; ATTACH happens later. If the set were snapshotted, this
    // key would come back as ("sales", "orders") and the table would inherit
    // any grant written for an Iceberg namespace named `sales`.
    let cats = Arc::new(HiveCatalogs::default());
    let r = datafusion::common::TableReference::full("lake", "sales", "orders");
    assert_eq!(
        sqe_policy::plan_rewriter::resolve_policy_key_for_test(&r, &cats),
        Some(("sales".to_string(), "orders".to_string())),
        "before ATTACH the root is unknown, so the Iceberg branch is correct"
    );
    cats.insert("lake");
    assert_eq!(
        sqe_policy::plan_rewriter::resolve_policy_key_for_test(&r, &cats),
        Some(("lake.sales".to_string(), "orders".to_string())),
        "after ATTACH the same reference must key under the root"
    );
}

#[test]
fn a_root_attached_after_construction_reaches_the_hive_tag_source() {
    let (cache, cats) = fixture_with_cached_manifest_unattached("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        cats.clone(),
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "orders"),
        None,
        "not attached yet, so the fail-closed source answers"
    );
    cats.insert("lake");
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "orders"),
        Some(HashMap::new()),
        "a router that snapshotted the set at construction would still deny here"
    );
}

#[test]
fn tags_recorded_in_the_manifest_are_returned() {
    let (cache, hive_catalogs) =
        fixture_with_tagged_manifest("lake", "sales", "orders", "ssn", "PII");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    let got = routed
        .column_tags(Some("lake"), &["sales".to_string()], "orders")
        .expect("known");
    assert_eq!(
        got.get("ssn").map(Vec::as_slice),
        Some(&["PII".to_string()][..])
    );
}

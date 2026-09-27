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
        sqe_policy::plan_rewriter::resolve_policy_key(&r, &cats),
        Some(("sales".to_string(), "orders".to_string())),
        "before ATTACH the root is unknown, so the Iceberg branch is correct"
    );
    cats.insert("lake");
    assert_eq!(
        sqe_policy::plan_rewriter::resolve_policy_key(&r, &cats),
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

// ─── Cycle 3: does a manifest tag actually MASK a Hive scan? ─────────────────
//
// Everything above is unit-shaped: it proves `CatalogRoutedTagSource` returns
// the right map. `sqe-policy`'s own rewriter tests prove the other half, that a
// tag becomes a mask -- but they prove it against a `FakeTagSource`. Neither
// side proves the COMPOSITION, and the composition is the thing cycle 3 is
// supposed to deliver.
//
// This test wires the real chain end to end: a tag planted in a real
// `HiveManifest`, read through the real `HiveTagSource` behind the real
// `CatalogRoutedTagSource`, into the real `PolicyPlanRewriter`, and then
// EXECUTES the rewritten plan and looks at the bytes. If this passes, the read
// side of cycle 3 is already built and the remaining work is the write path.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::Column;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use sqe_core::SessionUser;
use sqe_policy::{
    plan_rewriter::PolicyPlanRewriter, MaskType, PolicyEnforcer, PolicyStore, ResolvedPolicy,
    TagMaskSpec,
};
use std::collections::HashSet;

/// An `Expr`-bearing value behind a trait object: a compile firewall, keep it.
/// Held concretely, each `async_trait` method of the stores below re-proved
/// `Sync` through DataFusion's `Expr` graph (about 2.3 s per method, 9 s of
/// this test binary). Same device as `CachedPolicy` in sqe-policy.
struct Frozen<T>(Arc<dyn Fn() -> T + Send + Sync>);

impl<T: Clone + Send + Sync + 'static> Frozen<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(move || value.clone()))
    }
}

impl<T> Frozen<T> {
    fn get(&self) -> T {
        (self.0)()
    }
}

impl<T> std::fmt::Debug for Frozen<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Frozen(..)")
    }
}

/// Minimal store: no resource-level policy, one tag -> mask rule.
#[derive(Debug)]
struct OneTagMaskStore {
    tag: String,
    mask: Frozen<MaskType>,
    /// Tag-driven row filters, the other half of what cycle 3 promises.
    tag_filters: Frozen<Vec<Expr>>,
}

#[async_trait::async_trait]
impl PolicyStore for OneTagMaskStore {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    async fn resolve_tags(
        &self,
        _user: &SessionUser,
        _tags: &HashSet<String>,
    ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
        let mut masks = HashMap::new();
        masks.insert(self.tag.clone(), TagMaskSpec::Ready(self.mask.get()));
        (masks, self.tag_filters.get(), HashSet::new())
    }
}

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("ssn", DataType::Utf8, true),
    ]))
}

fn orders_batch() -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("111-11-1111"),
                Some("222-22-2222"),
                Some("333-33-3333"),
            ])),
        ],
    )
    .unwrap()
}

fn test_user() -> SessionUser {
    SessionUser {
        username: "alice".to_string(),
        roles: vec![],
        subject: None,
        email: None,
        groups: vec![],
    }
}

#[tokio::test]
async fn a_tag_in_the_hive_manifest_masks_the_column_in_an_executed_plan() {
    // Tag `ssn` as PII in a real manifest, cached the way a scan would have
    // cached it, under a catalog registered as a Hive root.
    let (cache, hive_catalogs) =
        fixture_with_tagged_manifest("lake", "sales", "orders", "ssn", "PII");

    let routed = Arc::new(
        sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
            hive_catalogs,
            Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
            // The Iceberg side stays fail-closed, so a mask that appears can only
            // have come from the Hive branch.
            Arc::new(AlwaysUnknown),
        ),
    );

    let mem = Arc::new(MemTable::try_new(orders_schema(), vec![vec![orders_batch()]]).unwrap());
    let tref = datafusion::common::TableReference::full("lake", "sales", "orders");
    let scan = LogicalPlanBuilder::scan(tref.clone(), provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "order_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let store = OneTagMaskStore {
        tag: "PII".to_string(),
        mask: Frozen::new(MaskType::Nullify),
        tag_filters: Frozen::new(vec![]),
    };
    let rewriter = PolicyPlanRewriter::new(Arc::new(store)).with_tag_source(routed);
    let (rewritten, _summary) = rewriter.evaluate(&test_user(), plan).await.unwrap();

    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("sales", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("lake", catalog);
    ctx.register_table(tref, mem).unwrap();
    let batches = ctx
        .execute_logical_plan(rewritten)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "a mask must not drop rows");

    let ssn = batches[0].column_by_name("ssn").expect("ssn projected");
    assert_eq!(
        ssn.null_count(),
        ssn.len(),
        "every ssn must be nullified: the PII tag came from the Hive manifest, \
         so the mask reached a Hive scan"
    );
}

#[tokio::test]
async fn a_tag_row_filter_also_reaches_a_hive_scan() {
    // The mask half is covered above. Cycle 3 promises row filters too, and
    // they travel a different field of the same `resolve_tags` triple, so
    // "masks work" is not evidence that filters do.
    let (cache, hive_catalogs) =
        fixture_with_tagged_manifest("lake", "sales", "orders", "ssn", "PII");
    let routed = Arc::new(
        sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
            hive_catalogs,
            Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
            Arc::new(AlwaysUnknown),
        ),
    );

    let mem = Arc::new(MemTable::try_new(orders_schema(), vec![vec![orders_batch()]]).unwrap());
    let tref = datafusion::common::TableReference::full("lake", "sales", "orders");
    let plan = LogicalPlanBuilder::scan(tref.clone(), provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();

    // Keep only order_id = 2, so a filter that never fires is visible as 3 rows.
    let store = OneTagMaskStore {
        tag: "PII".to_string(),
        mask: Frozen::new(MaskType::Nullify),
        tag_filters: Frozen::new(vec![
            datafusion::logical_expr::col("order_id").eq(datafusion::logical_expr::lit(2_i64))
        ]),
    };
    let rewriter = PolicyPlanRewriter::new(Arc::new(store)).with_tag_source(routed);
    let (rewritten, _summary) = rewriter.evaluate(&test_user(), plan).await.unwrap();

    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("sales", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("lake", catalog);
    ctx.register_table(tref, mem).unwrap();
    let batches = ctx
        .execute_logical_plan(rewritten)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "the tag row filter must reach the Hive scan; 3 rows means it never fired"
    );
}

#[tokio::test]
async fn a_resource_level_mask_needs_no_tag_at_all_on_a_hive_scan() {
    // The scoping question behind cycle 3: is tag-based masking a CAPABILITY
    // gap on Hive, or a convenience layer over something that already works?
    //
    // Here the manifest carries NO tags and the store returns NO tag masks.
    // The mask is resource-level, the kind cycle 1's grant-based policy already
    // resolves. If this passes, Hive column masking does not depend on cycle 3
    // at all, and cycle 3 only buys tag-DRIVEN masking.
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = Arc::new(
        sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
            hive_catalogs,
            Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
            Arc::new(AlwaysUnknown),
        ),
    );

    let mem = Arc::new(MemTable::try_new(orders_schema(), vec![vec![orders_batch()]]).unwrap());
    let tref = datafusion::common::TableReference::full("lake", "sales", "orders");
    let scan = LogicalPlanBuilder::scan(tref.clone(), provider_as_source(mem.clone()), None)
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::from(scan)
        .project(vec![
            Expr::Column(Column::new(Some(tref.clone()), "order_id")),
            Expr::Column(Column::new(Some(tref.clone()), "ssn")),
        ])
        .unwrap()
        .build()
        .unwrap();

    let mut column_masks = HashMap::new();
    column_masks.insert("ssn".to_string(), MaskType::Nullify);
    let store = ResourceMaskStore {
        policy: Frozen::new(ResolvedPolicy {
            row_filters: vec![],
            column_masks,
            restricted_columns: vec![],
        }),
    };
    let rewriter = PolicyPlanRewriter::new(Arc::new(store)).with_tag_source(routed);
    let (rewritten, _summary) = rewriter.evaluate(&test_user(), plan).await.unwrap();

    let ctx = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema("sales", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("lake", catalog);
    ctx.register_table(tref, mem).unwrap();
    let batches = ctx
        .execute_logical_plan(rewritten)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let ssn = batches[0].column_by_name("ssn").expect("ssn projected");
    assert_eq!(
        ssn.null_count(),
        ssn.len(),
        "an untagged Hive table must still honour a resource-level mask"
    );
}

/// Store with a resource-level policy and no tag rules whatsoever.
#[derive(Debug)]
struct ResourceMaskStore {
    policy: Frozen<ResolvedPolicy>,
}

#[async_trait::async_trait]
impl PolicyStore for ResourceMaskStore {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(self.policy.get())
    }

    async fn resolve_tags(
        &self,
        _user: &SessionUser,
        _tags: &HashSet<String>,
    ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
        (HashMap::new(), vec![], HashSet::new())
    }
}

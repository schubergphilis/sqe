#![cfg(feature = "hive-external")]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::Array;
use datafusion::prelude::{SessionConfig, SessionContext};

use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::provider::{HiveCatalogProvider, HiveMetadataCache};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

mod hive_fixture;
use hive_fixture::{partitioned_csv_root, register_hive_root};

#[tokio::test]
async fn reads_a_partitioned_csv_table_and_exposes_the_partition_column() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;

    let df = ctx
        .sql("SELECT id, amount, dt FROM lake.sales.orders ORDER BY id")
        .await
        .expect("plan");
    let batches = df.collect().await.expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "two partitions, three rows");

    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string();
    assert!(
        text.contains("2026-01-01"),
        "partition value must be projected: {text}"
    );
}

#[tokio::test]
async fn a_partition_predicate_prunes_to_one_partition() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;

    let batches = ctx
        .sql("SELECT id FROM lake.sales.orders WHERE dt = '2026-01-02'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);

    let plan = ctx
        .sql("EXPLAIN SELECT id FROM lake.sales.orders WHERE dt = '2026-01-02'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .expect("fmt")
        .to_string();
    assert!(
        !text.contains("dt=2026-01-01"),
        "the pruned partition must not appear in the scan: {text}"
    );
    // Not just "the excluded partition is absent" (which an empty, all-pruned
    // scan would also satisfy): the surviving partition must actually be the
    // requested one, or this proves nothing about which partition was kept.
    assert!(
        text.contains("dt=2026-01-02"),
        "the matching partition must appear in the scan: {text}"
    );
}

#[tokio::test]
async fn csv_binds_by_ordinal_so_a_renamed_column_keeps_its_position() {
    // The declared list is authoritative for CSV, and DataFusion's CsvFormat
    // never looks at the header text once an explicit schema is supplied. If
    // this bound by NAME instead (matching the declared column name against
    // the CSV header row) it would find no header field called `total`
    // (the file's header still literally reads `id,amount`) and this query
    // would either fail to resolve `total` at all or read the wrong values.
    // Binding by ordinal reads position 2 regardless of what the header
    // there says, which is what the manifest's `LOCATION` clause + Hive's own
    // "position, not name" CSV contract require.
    let f = hive_fixture::partitioned_csv_root_with_renamed_column().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT total FROM lake.sales.orders WHERE id = 1")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("fmt")
        .to_string();
    assert!(
        text.contains("10.5"),
        "ordinal binding must read field 2 under its declared name: {text}"
    );
}

#[tokio::test]
async fn show_tables_lists_the_manifest_backed_table() {
    let f = partitioned_csv_root().await;
    // `information_schema` is resolvable only when the session config turns
    // it on (default false); a plain `SessionContext::new()` would fail this
    // query with "table not found" regardless of whether the Hive provider
    // is wired up correctly.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("fmt")
        .to_string();
    assert!(
        text.contains("orders"),
        "information_schema must show the table: {text}"
    );
}

#[tokio::test]
async fn loading_a_table_populates_the_metadata_cache_before_returning() {
    // The TagSource in the policy path is synchronous, so it can only answer
    // from cache. This asserts the ordering it depends on.
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let provider = register_hive_root(&ctx, &f).await;
    assert!(
        provider.manifest_cache().get("sales", "orders").is_none(),
        "cold"
    );
    let _ = ctx
        .sql("SELECT 1 FROM lake.sales.orders LIMIT 1")
        .await
        .expect("plan");
    assert!(
        provider.manifest_cache().get("sales", "orders").is_some(),
        "planning must have loaded the manifest into cache"
    );
}

#[tokio::test]
async fn a_failed_table_build_still_leaves_the_manifest_cached() {
    // The happy-path test above cannot distinguish "insert before the
    // fallible build steps" from "insert as the last statement before Ok":
    // both look identical when nothing fails. This is the failure-path half
    // of that property. `root_with_unsupported_serde` loads a manifest that
    // `store.get()` accepts (it is well-formed and its location passes the
    // TVF allowlist) but whose SerDe class `resolve_format` rejects, so
    // `table()` fails AFTER the manifest is loaded and cached, not before.
    // If the cache insert in `HiveSchemaProvider::table` ever moved past
    // `resolve_format` (or any later fallible step), this test would start
    // failing on the second assertion while the query still correctly
    // errors on the first, which is exactly the gap a happy-path-only test
    // cannot see.
    let f = hive_fixture::root_with_unsupported_serde().await;
    let ctx = SessionContext::new();
    let provider = register_hive_root(&ctx, &f).await;
    assert!(
        provider.manifest_cache().get("sales", "orders").is_none(),
        "cold"
    );
    let result = ctx.sql("SELECT 1 FROM lake.sales.orders LIMIT 1").await;
    assert!(
        result.is_err(),
        "an unsupported SerDe class must fail table resolution: {result:?}"
    );
    assert!(
        provider.manifest_cache().get("sales", "orders").is_some(),
        "the manifest must still be cached even though table() errored \
         after loading it, not before"
    );
}

#[tokio::test]
async fn hive_default_partition_reads_as_null() {
    let f = hive_fixture::root_with_default_partition().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM lake.sales.orders WHERE dt IS NULL")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let n = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("count(*) always returns a row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count(*) is Int64")
        .value(0);
    assert_eq!(
        n, 2,
        "both __HIVE_DEFAULT_PARTITION__ rows must read as NULL"
    );
}

#[tokio::test]
async fn selecting_the_default_partition_column_returns_null_not_the_sentinel() {
    // A plain SELECT and an IS NULL filter must agree on what the value is.
    // Filter-rewriting alone made `WHERE dt IS NULL` return the right rows
    // without making the projected value itself NULL; this pins the other
    // half (nullify_default_partition_columns's ProjectionExec wrap).
    let f = hive_fixture::root_with_default_partition().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT dt FROM lake.sales.orders WHERE id = 4")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("the one matching row must be in some batch");
    let dt_col = batch.column(0);
    assert!(
        dt_col.is_null(0),
        "the value at the array level must be null, not the literal sentinel: {dt_col:?}"
    );
}

#[tokio::test]
async fn grouping_by_the_default_partition_column_folds_into_one_null_group() {
    // No WHERE filter: the fixture has two rows in `dt=2026-01-01`, one in
    // `dt=2026-01-02`, and two in the default partition. Grouping a single
    // default-partition row would fold into "one group" whether or not the
    // sentinel-to-NULL conversion worked at all (grouping one row by any key
    // is trivially one group); this needs two sentinel-valued rows that must
    // fold together, alongside two other groups that must stay separate, and
    // it checks all of that at the array level, not by pretty-printed text.
    let f = hive_fixture::root_with_default_partition().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT dt, count(*) AS n FROM lake.sales.orders GROUP BY dt")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    let mut null_group_count: Option<i64> = None;
    let mut other_group_counts: Vec<i64> = Vec::new();
    for batch in &batches {
        let dt = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("dt is Utf8");
        let n = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count(*) is Int64");
        for row in 0..batch.num_rows() {
            if dt.is_null(row) {
                assert!(
                    null_group_count.is_none(),
                    "must be exactly one NULL group, found a second"
                );
                null_group_count = Some(n.value(row));
            } else {
                assert_ne!(
                    dt.value(row),
                    "__HIVE_DEFAULT_PARTITION__",
                    "no group key must be the literal sentinel"
                );
                other_group_counts.push(n.value(row));
            }
        }
    }

    assert_eq!(
        null_group_count,
        Some(2),
        "both default-partition rows must fold into the one NULL group"
    );
    other_group_counts.sort_unstable();
    assert_eq!(
        other_group_counts,
        vec![1, 2],
        "the two real dt values must remain their own separate groups"
    );
}

/// Not exercised by any assertion above, but a schema built from the
/// declared columns and partition keys must put partition columns LAST
/// (Hive's own column order): a caller that projects `SELECT *` must see
/// `dt` after `amount`, not interleaved by whatever order a map iterated in.
#[tokio::test]
async fn select_star_orders_partition_columns_after_declared_columns() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let df = ctx
        .sql("SELECT * FROM lake.sales.orders ORDER BY id")
        .await
        .expect("plan");
    let names: Vec<String> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(names, vec!["id", "amount", "dt"], "got {names:?}");
}

// ---------------------------------------------------------------------------
// Object-store bucket collision guard (security review fix round)
// ---------------------------------------------------------------------------

/// A local, un-credentialed store built the same way `hive_fixture`'s
/// `local_store` does, but exposed here so this file can write two
/// manifests with different `sqe.secret` names without threading that
/// concern through the shared fixture module.
fn local_store_for_test(root: &str) -> HiveManifestStore {
    HiveManifestStore::new(
        datafusion::datasource::listing::ListingTableUrl::parse(root).expect("root url"),
        Arc::new(object_store::local::LocalFileSystem::new()),
        ManifestLimits::default(),
        TvfPolicy {
            allow_local_paths: true,
            ..TvfPolicy::default()
        },
        TvfCaller::trusted_local(),
    )
}

/// Two Hive tables under one warehouse root, both `file://` (no host
/// component, so both resolve to the SAME `object_store_bucket_key`,
/// `file://`, the exact collision `HiveMetadataCache::claim_object_store_bucket`
/// exists to catch), declared with DIFFERENT `sqe.secret` names. This
/// fixture bypasses `HiveDdlHandler::create_external_table` entirely (it
/// writes manifests directly, the same way `hive_fixture` does), so the
/// secret names never need to resolve against a real `SecretStore` --
/// only the manifest's declared name matters to the guard being tested.
async fn two_tables_one_bucket_different_secrets() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tmp");
    let root = format!("file://{}/wh", dir.path().display());

    for (table, data_sub, secret) in
        [("orders", "data-orders", "secret-a"), ("shipments", "data-shipments", "secret-b")]
    {
        let data_path = dir.path().join(data_sub);
        std::fs::create_dir_all(&data_path).expect("mkdir data");
        std::fs::write(data_path.join("part-0.csv"), "id\n1\n").expect("write csv");
        let data = format!("file://{}", data_path.display());
        let ddl = format!(
            "CREATE EXTERNAL TABLE sales.{table} (id BIGINT) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
             STORED AS TEXTFILE LOCATION '{data}' \
             TBLPROPERTIES ('skip.header.line.count' = '1', 'sqe.secret' = '{secret}')"
        );
        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
                .expect("parse");
        let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
            .expect("extract")
            .expect("external");
        let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-19T00:00:00Z");
        local_store_for_test(&root)
            .put(&m, false)
            .await
            .expect("put manifest");
    }
    (dir, root)
}

/// Two Hive tables in one bucket under different `sqe.secret` credentials
/// must be refused, naming both tables and both secrets, rather than
/// letting the second `register_object_store` silently overwrite the
/// first's entry in DataFusion's bucket-keyed registry -- which would make
/// a join across both tables read one of them under the other's
/// credentials.
///
/// `IgnoringFactory` below mirrors `hive_fixture`'s own `LocalFactory`: it
/// ignores `secret` entirely and always returns a fresh
/// `LocalFileSystem`. That is deliberate -- it proves this is
/// `HiveMetadataCache`'s new collision guard doing the refusing, not
/// `EngineHiveDataStoreFactory`'s credential resolution (which this test
/// does not exercise at all).
#[tokio::test]
async fn two_hive_tables_in_one_bucket_with_different_secrets_are_refused() {
    let (dir, root) = two_tables_one_bucket_different_secrets().await;
    let ctx = SessionContext::new();

    #[derive(Debug)]
    struct IgnoringFactory;
    impl sqe_catalog::hive::provider::HiveDataStoreFactory for IgnoringFactory {
        fn store_for(
            &self,
            _location: &str,
            _secret: Option<&str>,
        ) -> Result<Arc<dyn object_store::ObjectStore>, sqe_catalog::hive::HiveError> {
            Ok(Arc::new(object_store::local::LocalFileSystem::new()))
        }
    }

    let provider = Arc::new(HiveCatalogProvider::new(
        "lake".to_string(),
        Arc::new(local_store_for_test(&root)),
        Arc::new(IgnoringFactory),
        ctx.runtime_env(),
        Arc::new(HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    ctx.register_catalog("lake", provider);

    // The first table scans fine and claims the bucket for 'secret-a'.
    ctx.sql("SELECT id FROM lake.sales.orders")
        .await
        .expect("first table plans")
        .collect()
        .await
        .expect("first table scans");

    // Re-scanning the SAME table must stay fine (same owner, not a
    // collision) before the actual collision case is checked.
    ctx.sql("SELECT id FROM lake.sales.orders")
        .await
        .expect("re-scanning the same table must not be refused")
        .collect()
        .await
        .expect("re-scan collects");

    // The second table, same bucket, a DIFFERENT secret -> refused. The
    // error can surface from planning (`ctx.sql`) or from execution
    // (`collect`) depending on when DataFusion calls `SchemaProvider::table`;
    // check whichever branch actually produced it.
    let msg = match ctx.sql("SELECT id FROM lake.sales.shipments").await {
        Err(e) => e.to_string(),
        Ok(df) => df
            .collect()
            .await
            .expect_err("the second table's different-secret bucket claim must be refused")
            .to_string(),
    };
    assert!(msg.contains("orders"), "{msg}");
    assert!(msg.contains("shipments"), "{msg}");
    assert!(msg.contains("secret-a"), "{msg}");
    assert!(msg.contains("secret-b"), "{msg}");

    drop(dir);
}

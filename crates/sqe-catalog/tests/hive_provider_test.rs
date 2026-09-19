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

fn count_i64(batches: &[datafusion::arrow::array::RecordBatch]) -> i64 {
    batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a count(*) row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0)
}

#[tokio::test]
async fn indexed_read_ignores_files_outside_the_index() {
    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    // A third partition on disk that the index does not mention: an
    // indexed scan must omit it (staleness omits, never invents). A listing
    // scan would pick it up.
    std::fs::create_dir_all(f.dir.path().join("data/dt=2026-01-03")).unwrap();
    std::fs::write(
        f.dir.path().join("data/dt=2026-01-03/part-0.csv"),
        "id,amount\n99,99.0\n",
    )
    .unwrap();

    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect"),
    );
    assert_eq!(n, 3, "the unindexed dt=2026-01-03 partition must be omitted");
}

#[tokio::test]
async fn indexed_and_unindexed_null_sentinel_agree() {
    let unindexed = hive_fixture::root_with_default_partition().await;
    let indexed = hive_fixture::root_with_default_partition().await;
    hive_fixture::plant_orders_index(&indexed).await;
    // The planted index only covers 01-01 and 01-02; add the sentinel entry.
    {
        let store = {
            use sqe_catalog::hive::store::HiveManifestStore;
            HiveManifestStore::new(
                datafusion::datasource::listing::ListingTableUrl::parse(&indexed.root).unwrap(),
                Arc::new(object_store::local::LocalFileSystem::new()),
                sqe_catalog::hive::manifest::ManifestLimits::default(),
                TvfPolicy {
                    allow_local_paths: true,
                    ..TvfPolicy::default()
                },
                TvfCaller::trusted_local(),
            )
        };
        let mut m = store.get("sales", "orders").await.unwrap();
        let data = format!("file://{}", indexed.dir.path().join("data").display());
        if let Some(idx) = m.sqe.partition_index.as_mut() {
            let mut values = std::collections::BTreeMap::new();
            values.insert(
                "dt".into(),
                sqe_catalog::hive::manifest::HIVE_DEFAULT_PARTITION.into(),
            );
            idx.entries.push(sqe_catalog::hive::manifest::PartitionIndexEntry {
                values,
                location: format!("{data}/dt=__HIVE_DEFAULT_PARTITION__/"),
                file_count: 1,
                total_bytes: 40,
            });
        }
        store.put(&m, false).await.unwrap();
    }

    let ctx_u = SessionContext::new();
    register_hive_root(&ctx_u, &unindexed).await;
    let ctx_i = SessionContext::new();
    register_hive_root(&ctx_i, &indexed).await;
    for sql in [
        "SELECT count(*) FROM lake.sales.orders WHERE dt IS NULL",
        "SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-01'",
    ] {
        let nu = count_i64(
            &ctx_u
                .sql(sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        let ni = count_i64(
            &ctx_i
                .sql(sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        assert_eq!(nu, ni, "{sql}: indexed {ni} vs listing {nu}");
    }
}

#[tokio::test]
async fn indexed_equality_prunes_before_scan() {
    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-02'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
    let plan = ctx
        .sql("EXPLAIN SELECT id FROM lake.sales.orders WHERE dt = '2026-01-02'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();
    assert!(
        !text.contains("dt=2026-01-01"),
        "pruned partition must not be listed: {text}"
    );
}

#[tokio::test]
async fn index_metadata_supplies_size_statistics() {
    use datafusion::catalog::CatalogProvider;
    use datafusion::common::stats::Precision;

    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    let ctx = SessionContext::new();
    let provider = register_hive_root(&ctx, &f).await;
    let table = provider
        .schema("sales")
        .expect("sales")
        .table("orders")
        .await
        .expect("table()")
        .expect("orders");
    let stats = table.statistics().expect("size stats from the index");
    assert_eq!(stats.total_byte_size, Precision::Exact(150));
}

#[tokio::test]
async fn analyzed_row_counts_replace_size_derived_estimates() {
    use datafusion::catalog::CatalogProvider;
    use datafusion::common::stats::Precision;

    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    {
        let store = HiveManifestStore::new(
            datafusion::datasource::listing::ListingTableUrl::parse(&f.root).unwrap(),
            Arc::new(object_store::local::LocalFileSystem::new()),
            ManifestLimits::default(),
            TvfPolicy {
                allow_local_paths: true,
                ..TvfPolicy::default()
            },
            TvfCaller::trusted_local(),
        );
        let mut m = store.get("sales", "orders").await.unwrap();
        m.sqe.statistics = Some(serde_json::json!({
            "row_count": 3,
            "entries": []
        }));
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    let provider = register_hive_root(&ctx, &f).await;
    let table = provider
        .schema("sales")
        .unwrap()
        .table("orders")
        .await
        .unwrap()
        .unwrap();
    let stats = table.statistics().expect("analyzed stats");
    assert_eq!(stats.num_rows, Precision::Exact(3));
}

fn fixture_store(root: &str) -> HiveManifestStore {
    HiveManifestStore::new(
        datafusion::datasource::listing::ListingTableUrl::parse(root).unwrap(),
        Arc::new(object_store::local::LocalFileSystem::new()),
        ManifestLimits::default(),
        TvfPolicy {
            allow_local_paths: true,
            ..TvfPolicy::default()
        },
        TvfCaller::trusted_local(),
    )
}

#[tokio::test]
async fn out_of_prefix_index_entry_is_readable() {
    let f = partitioned_csv_root().await;
    let elsewhere = f.dir.path().join("elsewhere/dt=2026-09-01");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("part-0.csv"), "id,amount\n9,90.0\n").unwrap();
    hive_fixture::plant_orders_index(&f).await;
    {
        let store = fixture_store(&f.root);
        let mut m = store.get("sales", "orders").await.unwrap();
        if let Some(idx) = m.sqe.partition_index.as_mut() {
            let mut values = std::collections::BTreeMap::new();
            values.insert("dt".into(), "2026-09-01".into());
            idx.entries.push(sqe_catalog::hive::manifest::PartitionIndexEntry {
                values,
                location: format!("file://{}/", elsewhere.display()),
                file_count: 1,
                total_bytes: 20,
            });
        }
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-09-01'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
}

#[tokio::test]
async fn empty_index_scans_nothing_even_when_files_exist() {
    let f = partitioned_csv_root().await;
    {
        let store = fixture_store(&f.root);
        let mut m = store.get("sales", "orders").await.unwrap();
        m.sqe.partition_index = Some(sqe_catalog::hive::manifest::PartitionIndex {
            format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
            entries: vec![],
        });
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 0, "a present-but-empty index must not fall back to listing");
}

#[tokio::test]
async fn indexed_in_list_and_zero_match_queries() {
    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt IN ('2026-01-02', '1999-01-01')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
    let zero = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '1999-01-01'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(zero, 0);
}

#[tokio::test]
async fn int_partition_column_prunes_on_typed_equality() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    for (bucket, rows) in [(7, "1,1.0\n"), (42, "2,2.0\n3,3.0\n")] {
        let p = data.join(format!("bucket={bucket}"));
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("part-0.csv"), format!("id,amount\n{rows}")).unwrap();
    }
    let root = format!("file://{}/wh", dir.path().display());
    let loc = format!("file://{}", data.display());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (bucket int) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION '{loc}' TBLPROPERTIES ('skip.header.line.count' = '1')"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .unwrap();
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .unwrap()
        .unwrap();
    let mut m = HiveManifest::from_spec(&spec, "sales", "test", "t");
    m.sqe.partition_index = Some(sqe_catalog::hive::manifest::PartitionIndex {
        format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
        entries: [7, 42]
            .into_iter()
            .map(|b| {
                let mut values = std::collections::BTreeMap::new();
                values.insert("bucket".into(), b.to_string());
                sqe_catalog::hive::manifest::PartitionIndexEntry {
                    values,
                    location: format!("{loc}/bucket={b}/"),
                    file_count: 1,
                    total_bytes: 10,
                }
            })
            .collect(),
    });
    fixture_store(&root).put(&m, false).await.unwrap();
    let f = hive_fixture::Fixture { dir, root };
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE bucket = 42")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 2);
}

#[tokio::test]
async fn injected_projection_is_null_without_a_predicate_and_pins_with_one() {
    let f = partitioned_csv_root().await;
    // Files live under dt=...; an injected column with no context is the
    // sentinel, so point a copy of one partition there.
    let sentinel = f
        .dir
        .path()
        .join("data/tenant=__HIVE_DEFAULT_PARTITION__");
    std::fs::create_dir_all(&sentinel).unwrap();
    std::fs::write(sentinel.join("part-0.csv"), "id,amount\n1,1.0\n").unwrap();
    let acme = f.dir.path().join("data/tenant=acme");
    std::fs::create_dir_all(&acme).unwrap();
    std::fs::write(acme.join("part-0.csv"), "id,amount\n2,2.0\n").unwrap();
    {
        let store = fixture_store(&f.root);
        let mut m = store.get("sales", "orders").await.unwrap();
        m.partition_keys = vec![sqe_catalog::hive::manifest::GlueColumn {
            name: "tenant".into(),
            r#type: "string".into(),
            comment: None,
        }];
        m.parameters
            .insert("projection.enabled".into(), "true".into());
        m.parameters
            .insert("projection.tenant.type".into(), "injected".into());
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT tenant FROM lake.sales.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("row");
    assert!(
        batch.column(0).is_null(0),
        "injected without context is NULL: {:?}",
        batch.column(0)
    );
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE tenant = 'acme'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
}

fn cache_manifest() -> HiveManifest {
    HiveManifest {
        manifest_version: sqe_catalog::hive::manifest::MANIFEST_VERSION,
        table_type: "EXTERNAL_TABLE".to_string(),
        database_name: "sales".to_string(),
        name: "orders".to_string(),
        owner: None,
        created_at: None,
        description: None,
        view_original_text: None,
        partition_keys: vec![],
        parameters: Default::default(),
        storage_descriptor: sqe_catalog::hive::manifest::StorageDescriptor {
            location: "file:///tmp/x".to_string(),
            input_format: None,
            output_format: None,
            compressed: false,
            serde_info: Default::default(),
            columns: vec![],
            sort_columns: vec![],
        },
        sqe: Default::default(),
    }
}

#[test]
fn cache_without_etag_expires_after_ttl() {
    // Zero TTL: a store with no ETag must not serve a cached manifest.
    // Instant coarseness makes a 1ms-then-sleep test flake; Duration::ZERO
    // is the documented "always miss" bound.
    let cache = HiveMetadataCache::with_ttl(Duration::ZERO);
    cache.insert_versioned("sales", "orders", cache_manifest(), None);
    assert!(
        cache.get_if_fresh("sales", "orders", None).is_none(),
        "a zero TTL must miss immediately when the store has no ETag"
    );
}

#[test]
fn cache_without_etag_is_fresh_inside_a_long_ttl() {
    let cache = HiveMetadataCache::with_ttl(Duration::from_secs(60));
    cache.insert_versioned("sales", "orders", cache_manifest(), None);
    assert!(
        cache.get_if_fresh("sales", "orders", None).is_some(),
        "a long TTL with no ETag must hit"
    );
}

#[test]
fn cache_with_matching_etag_does_not_consult_ttl() {
    let cache = HiveMetadataCache::with_ttl(Duration::ZERO);
    cache.insert_versioned("sales", "orders", cache_manifest(), Some("etag-1".into()));
    assert!(
        cache
            .get_if_fresh("sales", "orders", Some("etag-1"))
            .is_some(),
        "ETag match is authoritative; TTL only applies when the store has no ETag"
    );
}

#[test]
fn cache_keys_are_database_and_table() {
    let cache = HiveMetadataCache::new();
    cache.insert("sales", "orders", cache_manifest());
    assert!(cache.get("sales", "orders").is_some());
    assert!(cache.get("sales", "other").is_none());
    assert!(cache.get("other", "orders").is_none());
}

#[tokio::test]
async fn out_of_band_rows_appear_after_repair() {
    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    std::fs::create_dir_all(f.dir.path().join("data/dt=2026-01-03")).unwrap();
    std::fs::write(
        f.dir.path().join("data/dt=2026-01-03/part-0.csv"),
        "id,amount\n99,99.0\n",
    )
    .unwrap();
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let before = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(before, 3);
    let store = fixture_store(&f.root);
    let mut m = store.get("sales", "orders").await.unwrap();
    sqe_catalog::hive::partition::msck_repair(&store, &mut m, 100)
        .await
        .unwrap();
    // A fresh context so we do not reuse a stale TableProvider.
    let ctx2 = SessionContext::new();
    register_hive_root(&ctx2, &f).await;
    let after = count_i64(
        &ctx2
            .sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(after, 4);
}

#[test]
fn changed_etag_reloads_the_index() {
    let cache = HiveMetadataCache::new();
    cache.insert_versioned("sales", "orders", cache_manifest(), Some("etag-1".into()));
    assert!(cache.get_if_fresh("sales", "orders", Some("etag-1")).is_some());
    assert!(
        cache.get_if_fresh("sales", "orders", Some("etag-2")).is_none(),
        "a changed ETag must miss so the caller reloads"
    );
    assert!(
        cache.get_if_fresh("sales", "orders", None).is_some(),
        "no-ETag caller still uses TTL (default 60s)"
    );
}

#[tokio::test]
async fn projected_enum_table_scans_computed_partitions() {
    let f = partitioned_csv_root().await;
    {
        let store = {
            HiveManifestStore::new(
                datafusion::datasource::listing::ListingTableUrl::parse(&f.root).unwrap(),
                Arc::new(object_store::local::LocalFileSystem::new()),
                ManifestLimits::default(),
                TvfPolicy {
                    allow_local_paths: true,
                    ..TvfPolicy::default()
                },
                TvfCaller::trusted_local(),
            )
        };
        let mut m = store.get("sales", "orders").await.unwrap();
        m.parameters
            .insert("projection.enabled".into(), "true".into());
        m.parameters
            .insert("projection.dt.type".into(), "enum".into());
        m.parameters
            .insert("projection.dt.values".into(), "2026-01-02".into());
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1, "only the projected dt=2026-01-02 partition");
    let zero = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '1999-01-01'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(zero, 0, "a value outside the enum scans nothing");
}

#[tokio::test]
async fn indexed_range_predicate_prunes() {
    let f = partitioned_csv_root().await;
    hive_fixture::plant_orders_index(&f).await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt >= '2026-01-02'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt <> '2026-01-01'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 1);
}

#[tokio::test]
async fn indexed_is_not_null_drops_the_sentinel_partition() {
    let f = hive_fixture::root_with_default_partition().await;
    hive_fixture::plant_orders_index(&f).await;
    {
        let store = fixture_store(&f.root);
        let mut m = store.get("sales", "orders").await.unwrap();
        let data = format!("file://{}", f.dir.path().join("data").display());
        if let Some(idx) = m.sqe.partition_index.as_mut() {
            let mut values = std::collections::BTreeMap::new();
            values.insert(
                "dt".into(),
                sqe_catalog::hive::manifest::HIVE_DEFAULT_PARTITION.into(),
            );
            idx.entries.push(sqe_catalog::hive::manifest::PartitionIndexEntry {
                values,
                location: format!("{data}/dt=__HIVE_DEFAULT_PARTITION__/"),
                file_count: 1,
                total_bytes: 40,
            });
        }
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let not_null = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt IS NOT NULL")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(not_null, 3, "two + one real partitions, not the sentinel");
    let is_null = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE dt IS NULL")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(is_null, 2);
}

#[tokio::test]
async fn composite_indexed_scan_prunes_on_one_key() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    for (dt, region, rows) in [
        ("2026-01-01", "eu", "1,1.0\n"),
        ("2026-01-01", "us", "2,2.0\n"),
        ("2026-01-02", "eu", "3,3.0\n"),
    ] {
        let p = data.join(format!("dt={dt}")).join(format!("region={region}"));
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("part-0.csv"), format!("id,amount\n{rows}")).unwrap();
    }
    let root = format!("file://{}/wh", dir.path().display());
    let loc = format!("file://{}", data.display());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string, region string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION '{loc}' TBLPROPERTIES ('skip.header.line.count' = '1')"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .unwrap();
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .unwrap()
        .unwrap();
    let mut m = HiveManifest::from_spec(&spec, "sales", "test", "t");
    m.sqe.partition_index = Some(sqe_catalog::hive::manifest::PartitionIndex {
        format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
        entries: [
            ("2026-01-01", "eu"),
            ("2026-01-01", "us"),
            ("2026-01-02", "eu"),
        ]
        .into_iter()
        .map(|(dt, region)| {
            let mut values = std::collections::BTreeMap::new();
            values.insert("dt".into(), dt.to_string());
            values.insert("region".into(), region.to_string());
            sqe_catalog::hive::manifest::PartitionIndexEntry {
                values,
                location: format!("{loc}/dt={dt}/region={region}/"),
                file_count: 1,
                total_bytes: 10,
            }
        })
        .collect(),
    });
    fixture_store(&root).put(&m, false).await.unwrap();
    let f = hive_fixture::Fixture { dir, root };
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders WHERE region = 'eu'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 2);
    let n = count_i64(
        &ctx.sql(
            "SELECT count(*) FROM lake.sales.orders \
             WHERE dt = '2026-01-01' AND region = 'us'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap(),
    );
    assert_eq!(n, 1);
}

// ---------------------------------------------------------------------------
// Table-load round trips: the manifest recheck window and cached data facts.
// ---------------------------------------------------------------------------

mod counting {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures::stream::BoxStream;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
    use sqe_catalog::hive::provider::HiveDataStoreFactory;
    use sqe_catalog::hive::source::HiveMetadataSource;
    use sqe_catalog::hive::store::HiveManifestStore;
    use sqe_catalog::hive::HiveError;

    /// Counts metastore round trips; every call is answered by the fixture's
    /// manifest store. `etags = false` behaves like Glue, which has no change
    /// token.
    pub struct CountingSource {
        inner: HiveManifestStore,
        etags: bool,
        pub etag_calls: AtomicUsize,
        pub get_calls: AtomicUsize,
    }

    impl CountingSource {
        pub fn new(inner: HiveManifestStore, etags: bool) -> Self {
            Self {
                inner,
                etags,
                etag_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
            }
        }
        pub fn etags(&self) -> usize {
            self.etag_calls.load(Ordering::SeqCst)
        }
        pub fn gets(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl HiveMetadataSource for CountingSource {
        async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
            self.inner.list_databases().await
        }
        async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
            self.inner.list_tables(database).await
        }
        async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get(database, table).await
        }
        async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
            self.etag_calls.fetch_add(1, Ordering::SeqCst);
            if !self.etags {
                // Still confirm the table exists, as Glue's `etag` path does
                // not: the store's answer is what tells the provider about a
                // dropped table.
                return self.inner.manifest_etag(database, table).await.map(|_| None);
            }
            self.inner.manifest_etag(database, table).await
        }
        fn limits(&self) -> &ManifestLimits {
            self.inner.limits()
        }
    }

    /// Counts data-prefix listings on the way to the local filesystem.
    #[derive(Debug)]
    pub struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        pub list_calls: AtomicUsize,
        /// How many upcoming `list` calls answer with an error instead of
        /// reaching the filesystem: a credential outage, scripted.
        pub fail_next_lists: AtomicUsize,
    }

    impl CountingStore {
        pub fn local() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                list_calls: AtomicUsize::new(0),
                fail_next_lists: AtomicUsize::new(0),
            })
        }
        pub fn lists(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
        fn take_failure(&self) -> bool {
            self.fail_next_lists
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            if self.take_failure() {
                return Box::pin(futures::stream::once(async {
                    Err(object_store::Error::Generic {
                        store: "S3",
                        source: "storage credentials expired or unavailable: 429".into(),
                    })
                }));
            }
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            opts: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[derive(Debug)]
    pub struct CountingFactory(pub Arc<CountingStore>);

    impl HiveDataStoreFactory for CountingFactory {
        fn store_for(
            &self,
            _location: &str,
            _secret: Option<&str>,
        ) -> Result<Arc<dyn ObjectStore>, HiveError> {
            Ok(Arc::clone(&self.0) as Arc<dyn ObjectStore>)
        }
    }
}

/// A `lake` catalog over the fixture whose metastore and data store count
/// their round trips.
async fn counting_root(
    f: &hive_fixture::Fixture,
    ctx: &SessionContext,
    cache: HiveMetadataCache,
    etags: bool,
) -> (
    Arc<HiveCatalogProvider>,
    Arc<counting::CountingSource>,
    Arc<counting::CountingStore>,
) {
    let source = Arc::new(counting::CountingSource::new(fixture_store(&f.root), etags));
    let store = counting::CountingStore::local();
    let provider = Arc::new(HiveCatalogProvider::with_source(
        "lake".to_string(),
        Arc::clone(&source) as Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>,
        Arc::new(counting::CountingFactory(Arc::clone(&store))),
        ctx.runtime_env(),
        Arc::new(cache),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;
    (provider, source, store)
}

async fn load_orders(
    provider: &HiveCatalogProvider,
) -> Arc<dyn datafusion::datasource::TableProvider> {
    use datafusion::catalog::CatalogProvider;
    provider
        .schema("sales")
        .expect("sales")
        .table("orders")
        .await
        .expect("table()")
        .expect("orders")
}

/// A statement resolves the same table several times (DataFusion's resolver,
/// `information_schema`, the tag source); inside the recheck window those
/// cost one metadata round trip in total, and the CSV codec probe and the
/// size-statistics listing run once per manifest version, not per lookup.
#[tokio::test]
async fn repeated_lookups_inside_the_recheck_window_pay_one_round_trip() {
    use datafusion::common::stats::Precision;

    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let (provider, source, store) =
        counting_root(&f, &ctx, HiveMetadataCache::new(), true).await;

    let first = load_orders(&provider).await;
    let first_size = first.statistics().expect("size stats").total_byte_size;
    assert!(
        matches!(first_size, Precision::Exact(n) if n > 0),
        "size-only statistics come from the listing: {first_size:?}"
    );
    assert_eq!(source.etags(), 1);
    assert_eq!(source.gets(), 1);
    assert_eq!(
        store.lists(),
        2,
        "one listing for the CSV codec, one for size statistics"
    );

    for _ in 0..3 {
        let again = load_orders(&provider).await;
        assert_eq!(
            again.statistics().expect("size stats").total_byte_size,
            first_size,
            "cached facts must yield the same statistics"
        );
    }
    assert_eq!(source.etags(), 1, "no HEAD inside the recheck window");
    assert_eq!(source.gets(), 1);
    assert_eq!(store.lists(), 2, "no relisting inside the TTL");
}

/// The queryability probe `describe_schema` runs per table is
/// `EXPLAIN SELECT * FROM t LIMIT 0`. Resolving the table is where the
/// format, SerDe, location and column types are checked; the zero limit is
/// eliminated before physical planning, so no scan is built and storage is
/// not listed. `LIMIT 1` builds the scan and lists the prefix on every call,
/// which is what made the probe cost half a second per Glue table and hang
/// with expired credentials.
#[tokio::test]
async fn explain_limit_zero_resolves_the_table_without_listing_storage() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let (provider, _source, store) =
        counting_root(&f, &ctx, HiveMetadataCache::new(), true).await;
    ctx.register_catalog("lake", provider.clone());
    // DataFusion's own listing (ListingTable::scan) goes through the
    // session runtime's object-store registry, not the provider's factory,
    // so count that path too by serving `file://` from the same counter.
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file:///").expect("url"),
        store.clone() as Arc<dyn object_store::ObjectStore>,
    );

    // Warm: the codec probe and the size-statistics listing, once.
    let _ = load_orders(&provider).await;
    let warm = store.lists();
    assert_eq!(warm, 2, "one listing for the CSV codec, one for size statistics");

    ctx.sql("EXPLAIN SELECT * FROM lake.sales.orders LIMIT 0")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("explain");
    assert_eq!(
        store.lists(),
        warm,
        "resolving and planning with LIMIT 0 must not list storage"
    );

    // The contrast: reading one row has to find the files, so it lists.
    // (This fixture carries a partition index, so its plan defers the
    // listing to execution; a Glue table without an index lists at plan
    // time, which is what `LIMIT 1` in the probe paid on every call.)
    let rows = ctx
        .sql("SELECT * FROM lake.sales.orders LIMIT 1")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("read");
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        store.lists() > warm,
        "reading lists storage: {} listings after {warm}",
        store.lists()
    );
}

/// `metadata_recheck_secs = 0` restores a HEAD per lookup; the data facts
/// still belong to the manifest version and are not re-listed while the
/// ETag matches.
#[tokio::test]
async fn a_zero_recheck_window_heads_every_lookup_but_keeps_the_listings() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let cache = HiveMetadataCache::new().with_recheck(Duration::ZERO);
    let (provider, source, store) = counting_root(&f, &ctx, cache, true).await;

    for _ in 0..3 {
        load_orders(&provider).await;
    }
    assert_eq!(source.etags(), 3, "every lookup compares the ETag");
    assert_eq!(source.gets(), 1, "an unchanged ETag never reloads");
    assert_eq!(store.lists(), 2, "facts follow the manifest version");
}

/// DDL and writes call `invalidate`; the next lookup must go back to the
/// source for everything, including the data facts, because a new manifest
/// version may point at a different location or codec.
#[tokio::test]
async fn invalidating_the_manifest_reloads_it_and_relists_the_data() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let cache = Arc::new(HiveMetadataCache::new());
    let source = Arc::new(counting::CountingSource::new(fixture_store(&f.root), true));
    let store = counting::CountingStore::local();
    let provider = Arc::new(HiveCatalogProvider::with_source(
        "lake".to_string(),
        Arc::clone(&source) as Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>,
        Arc::new(counting::CountingFactory(Arc::clone(&store))),
        ctx.runtime_env(),
        Arc::clone(&cache),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;

    load_orders(&provider).await;
    cache.invalidate("sales", "orders");
    load_orders(&provider).await;

    assert_eq!(source.etags(), 2);
    assert_eq!(source.gets(), 2, "an invalidated entry is reloaded");
    assert_eq!(store.lists(), 4, "facts were dropped with the entry");
}

/// A source without change tokens (Glue) refreshes on the cache TTL. Serving
/// from the cache must not extend that TTL: the previous implementation
/// re-inserted the entry on every hit, so a table queried continuously never
/// picked up a changed manifest.
#[tokio::test]
async fn an_etag_less_source_reloads_once_the_ttl_has_lapsed() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    // Generous margins on both sides of the TTL: a loaded CI runner spent
    // more than the 100 ms this test used to leave between "load again"
    // and "TTL lapses" (the 300 ms / 200 ms version failed on !980 with an
    // extra reload), so the in-TTL probe now sits 1.5 s clear of the edge
    // and the post-TTL probe 0.5 s past it.
    let ttl = Duration::from_millis(2_000);
    let cache = HiveMetadataCache::with_ttl(ttl);
    let (provider, source, _store) = counting_root(&f, &ctx, cache, false).await;

    let loaded_at = std::time::Instant::now();
    load_orders(&provider).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    load_orders(&provider).await;
    assert!(
        loaded_at.elapsed() < ttl,
        "test rig too slow to probe inside the TTL; nothing to conclude"
    );
    assert_eq!(source.gets(), 1, "inside the TTL the entry is served");
    let past_ttl = (ttl + Duration::from_millis(500)).saturating_sub(loaded_at.elapsed());
    tokio::time::sleep(past_ttl).await;
    load_orders(&provider).await;
    assert_eq!(
        source.gets(),
        2,
        "past the TTL the entry is reloaded, whatever was served in between"
    );
}

// ── Athena / Trino views stored in Glue (issue #667) ───────────────────

mod views {
    use std::collections::BTreeMap;

    use base64::Engine;

    use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits, StorageDescriptor};
    use sqe_catalog::hive::source::HiveMetadataSource;
    use sqe_catalog::hive::store::HiveManifestStore;
    use sqe_catalog::hive::HiveError;

    /// The fixture's `sales` database plus views the way Glue would return
    /// them: `VIRTUAL_VIEW`, empty storage descriptor, the definition in
    /// `view_original_text`.
    pub struct ViewSource {
        inner: HiveManifestStore,
        views: BTreeMap<String, HiveManifest>,
    }

    pub fn presto_payload(json: &str) -> String {
        format!(
            "/* Presto View: {} */",
            base64::engine::general_purpose::STANDARD.encode(json)
        )
    }

    pub fn view_manifest(name: &str, view_original_text: Option<&str>) -> HiveManifest {
        HiveManifest {
            manifest_version: sqe_catalog::hive::manifest::MANIFEST_VERSION,
            table_type: "VIRTUAL_VIEW".to_string(),
            database_name: "sales".to_string(),
            name: name.to_string(),
            owner: None,
            created_at: None,
            description: None,
            view_original_text: view_original_text.map(str::to_string),
            partition_keys: vec![],
            parameters: BTreeMap::from([("presto_view".to_string(), "true".to_string())]),
            storage_descriptor: StorageDescriptor {
                location: String::new(),
                input_format: None,
                output_format: None,
                compressed: false,
                serde_info: Default::default(),
                columns: vec![],
                sort_columns: vec![],
            },
            sqe: Default::default(),
        }
    }

    impl ViewSource {
        pub fn new(inner: HiveManifestStore, views: Vec<HiveManifest>) -> Self {
            Self {
                inner,
                views: views.into_iter().map(|m| (m.name.clone(), m)).collect(),
            }
        }
    }

    #[async_trait::async_trait]
    impl HiveMetadataSource for ViewSource {
        async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
            self.inner.list_databases().await
        }
        async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
            let mut names = self.inner.list_tables(database).await?;
            if database == "sales" {
                names.extend(self.views.keys().cloned());
            }
            Ok(names)
        }
        async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
            if database == "sales" {
                if let Some(v) = self.views.get(table) {
                    return Ok(v.clone());
                }
            }
            self.inner.get(database, table).await
        }
        async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
            if database == "sales" && self.views.contains_key(table) {
                return Ok(None);
            }
            self.inner.manifest_etag(database, table).await.map(|_| None)
        }
        fn limits(&self) -> &ManifestLimits {
            self.inner.limits()
        }
    }
}

/// A `lake` catalog over the fixture whose metastore also holds the given
/// Glue-style views.
async fn root_with_views(
    f: &hive_fixture::Fixture,
    ctx: &SessionContext,
    views: Vec<HiveManifest>,
) -> Arc<HiveCatalogProvider> {
    let source = Arc::new(views::ViewSource::new(fixture_store(&f.root), views));
    let provider = Arc::new(HiveCatalogProvider::with_source(
        "lake".to_string(),
        source as Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>,
        Arc::new(counting::CountingFactory(counting::CountingStore::local())),
        ctx.runtime_env(),
        Arc::new(HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;
    ctx.register_catalog("lake", provider.clone());
    provider
}

/// An Athena view is planned from its Presto payload and answers like a
/// table. Its SQL is Trino dialect with references relative to the view's
/// own database (`FROM orders`) and Athena's `awsdatacatalog` qualifier;
/// both resolve against this catalog.
#[tokio::test]
async fn athena_view_over_a_hive_table_is_queryable() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    let unqualified = views::view_manifest(
        "big_orders",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT id, amount FROM orders WHERE amount > 15",
                "catalog":"awsdatacatalog","schema":"sales",
                "columns":[{"name":"id","type":"bigint"},{"name":"amount","type":"double"}]}"#,
        )),
    );
    let athena_qualified = views::view_manifest(
        "order_count",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT count(*) AS n FROM awsdatacatalog.sales.orders",
                "catalog":"awsdatacatalog","schema":"sales"}"#,
        )),
    );
    let over_a_view = views::view_manifest(
        "big_order_count",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT count(*) AS n FROM \"sales\".\"big_orders\"",
                "catalog":"awsdatacatalog","schema":"sales"}"#,
        )),
    );
    root_with_views(&f, &ctx, vec![unqualified, athena_qualified, over_a_view]).await;

    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.big_orders")
            .await
            .expect("plan view")
            .collect()
            .await
            .expect("run view"),
    );
    assert_eq!(n, 2, "amount > 15 keeps 20.0 and 30.5");

    let n = count_i64(
        &ctx.sql("SELECT n FROM lake.sales.order_count")
            .await
            .expect("plan awsdatacatalog-qualified view")
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 3);

    let n = count_i64(
        &ctx.sql("SELECT n FROM lake.sales.big_order_count")
            .await
            .expect("plan view over view")
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 2);

    // information_schema reports it as a VIEW, the way clients decide
    // whether to offer DDL on it.
    let kind = ctx
        .sql(
            "SELECT table_type FROM lake.information_schema.tables \
             WHERE table_schema = 'sales' AND table_name = 'big_orders'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let kind = kind
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(kind, "VIEW");
}

/// Two Athena idioms from a live Glue catalog that hid their views. A Trino
/// lambda, `filter(arr, (tag) -> starts_with(tag, 'x'))`, only parses as a
/// lambda under the DuckDB dialect the engine session uses; a bare
/// `SessionConfig` read `->` as the JSON arrow and planned `tag` as a
/// column ("No field named tag"). And a correlated scalar subquery without
/// an aggregate, Athena's lookup idiom, is refused by DataFusion at plan
/// time; the front door wraps the projection in `max()`.
#[tokio::test]
async fn athena_views_with_a_lambda_and_a_lookup_subquery_are_queryable() {
    let f = partitioned_csv_root().await;
    // A plain context on purpose: the view must plan in its own session.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    let lambda_view = views::view_manifest(
        "tagged_orders",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT id, regexp_replace(element_at(filter(ARRAY['drt: dev-1', 'app: web'], (tag) -> starts_with(tag, 'drt:')), 1), '^drt:\\s*', '') AS device_id FROM orders",
                "catalog":"awsdatacatalog","schema":"sales"}"#,
        )),
    );
    let lookup_view = views::view_manifest(
        "orders_with_lookup",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT o.id, (SELECT i.amount FROM orders i WHERE i.id = o.id) AS looked_up FROM orders o",
                "catalog":"awsdatacatalog","schema":"sales"}"#,
        )),
    );
    root_with_views(&f, &ctx, vec![lambda_view, lookup_view]).await;

    let rows = ctx
        .sql("SELECT device_id FROM lake.sales.tagged_orders ORDER BY id LIMIT 1")
        .await
        .expect("plan the lambda view")
        .collect()
        .await
        .expect("run the lambda view");
    let device = rows
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    assert_eq!(device, "dev-1", "the lambda kept the drt: tag and the regexp stripped the prefix");

    let n = count_i64(
        &ctx.sql(
            "SELECT count(*) FROM lake.sales.orders_with_lookup WHERE looked_up IS NOT NULL",
        )
        .await
        .expect("plan the lookup view")
        .collect()
        .await
        .expect("run the lookup view"),
    );
    assert_eq!(n, 3, "every order looks itself up");
}

/// A view this release cannot plan is hidden with a reason in the log, not
/// reported as a table with a missing storage format, and not fatal to the
/// rest of the database.
#[tokio::test]
async fn unreadable_views_are_hidden_not_fatal() {
    use datafusion::catalog::CatalogProvider;

    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let hive_native = views::view_manifest("hive_native", Some("SELECT * FROM orders"));
    let no_text = views::view_manifest("no_text", None);
    let bad_sql = views::view_manifest(
        "bad_sql",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT * FROM orders_that_do_not_exist","schema":"sales"}"#,
        )),
    );
    let provider = root_with_views(&f, &ctx, vec![hive_native, no_text, bad_sql]).await;

    let sales = provider.schema("sales").expect("sales");
    for name in ["hive_native", "no_text", "bad_sql"] {
        assert!(
            sales.table(name).await.expect("hidden, not an error").is_none(),
            "{name} must be hidden"
        );
    }
    // The real table next to them is unaffected.
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 3);
}

/// The first live Glue catalog hid two of its three views (#667 follow-up).
/// Three Trino idioms from those bodies, each of which failed in the
/// original planning context: `date_parse` with a `%Y%m` format (chrono
/// refused the partial date), `trim(BOTH FROM x)` (sqlparser rejects the
/// form without a trim character) and `CROSS JOIN UNNEST(SEQUENCE(..)) t (p)`
/// (the DuckDB planner dialect has no UNNEST table factor). The view is
/// planned with the engine's Trino function set, through the same front
/// door a user statement takes, and with a generic-dialect retry.
#[tokio::test]
async fn athena_view_with_trino_idioms_plans() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let view = views::view_manifest(
        "orders_by_month",
        Some(&views::presto_payload(
            r#"{"originalSql":"SELECT id, trim(BOTH FROM CAST(id AS VARCHAR)) tid, DATE_PARSE(REPLACE(SUBSTR(dt, 1, 7), '-', ''), '%Y%m') period, t.m FROM orders CROSS JOIN UNNEST(SEQUENCE(DATE '2026-01-01', DATE '2026-03-01', INTERVAL '1' MONTH)) t (m) WHERE amount > 15",
                "catalog":"awsdatacatalog","schema":"sales"}"#,
        )),
    );
    root_with_views(&f, &ctx, vec![view]).await;

    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders_by_month")
            .await
            .expect("view with Trino idioms plans")
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 6, "2 orders over 15 x 3 months");

    let periods = ctx
        .sql("SELECT count(DISTINCT period) FROM lake.sales.orders_by_month WHERE tid = '2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(count_i64(&periods), 1, "date_parse('202601', '%Y%m') yields one period");
}

// ── Metadata round trips behind listings (issue #666) ──────────────────

mod bulk {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
    use sqe_catalog::hive::source::HiveMetadataSource;
    use sqe_catalog::hive::store::HiveManifestStore;
    use sqe_catalog::hive::HiveError;

    /// Behaves like Glue: no change token, and a listing that already
    /// carries every table's definition. Counts the per-table fetches the
    /// provider still makes.
    pub struct BulkSource {
        inner: HiveManifestStore,
        pub get_calls: AtomicUsize,
        pub list_calls: AtomicUsize,
    }

    impl BulkSource {
        pub fn new(inner: HiveManifestStore) -> Self {
            Self {
                inner,
                get_calls: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
            }
        }
        pub fn gets(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
        pub fn lists(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl HiveMetadataSource for BulkSource {
        async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
            self.inner.list_databases().await
        }
        async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_tables(database).await
        }
        async fn list_manifests(
            &self,
            database: &str,
        ) -> Result<Option<Vec<HiveManifest>>, HiveError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            let mut out = Vec::new();
            for name in self.inner.list_tables(database).await? {
                out.push(self.inner.get(database, &name).await?);
            }
            Ok(Some(out))
        }
        async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get(database, table).await
        }
        async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
            self.inner.manifest_etag(database, table).await.map(|_| None)
        }
        fn limits(&self) -> &ManifestLimits {
            self.inner.limits()
        }
    }
}

/// A source whose listing returns full definitions (Glue) warms the
/// manifest cache: after the pre-plan refresh, loading a listed table costs
/// no per-table fetch, and `information_schema` reading the whole database
/// costs one listing, not one fetch per table.
#[tokio::test]
async fn a_listing_with_definitions_serves_the_lookups_that_follow() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    let source = Arc::new(bulk::BulkSource::new(fixture_store(&f.root)));
    let provider = Arc::new(HiveCatalogProvider::with_source(
        "lake".to_string(),
        Arc::clone(&source) as Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>,
        Arc::new(counting::CountingFactory(counting::CountingStore::local())),
        ctx.runtime_env(),
        Arc::new(HiveMetadataCache::new()),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;
    ctx.register_catalog("lake", provider.clone());
    assert_eq!(source.lists(), 1, "one listing for the one database");

    load_orders(&provider).await;
    assert_eq!(
        source.gets(),
        0,
        "the listing already delivered the manifest; no per-table fetch"
    );

    let rows = ctx
        .sql("SELECT table_name, table_type FROM lake.information_schema.tables WHERE table_schema = 'sales'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 1);
    assert_eq!(source.gets(), 0, "information_schema is answered from the cache too");
}

/// `information_schema.tables` only needs each table's kind. Answering it
/// used to build every table, which listed every table's data prefix for
/// size statistics; the kind comes from the manifest alone.
#[tokio::test]
async fn table_type_does_not_list_the_data_prefix() {
    use datafusion::catalog::CatalogProvider;

    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let (provider, _source, store) =
        counting_root(&f, &ctx, HiveMetadataCache::new(), false).await;
    let sales = provider.schema("sales").expect("sales");

    let kind = sales.table_type("orders").await.expect("table_type");
    assert_eq!(kind, Some(datafusion::logical_expr::TableType::Base));
    assert_eq!(
        store.lists(),
        0,
        "the table's kind must not cost a data-prefix listing"
    );
    assert!(sales.table_type("nope").await.expect("table_type").is_none());

    // Building the table still lists, once, as before.
    load_orders(&provider).await;
    assert!(store.lists() >= 1);
}

// ── Metadata timeouts (issue #666) ─────────────────────────────────────

mod slow {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
    use sqe_catalog::hive::source::HiveMetadataSource;
    use sqe_catalog::hive::store::HiveManifestStore;
    use sqe_catalog::hive::HiveError;

    /// The fixture's metastore with a switch that makes every per-table call
    /// hang well past any timeout under test. Glue-shaped: no change token.
    pub struct SlowSource {
        inner: HiveManifestStore,
        pub stall: AtomicBool,
    }

    impl SlowSource {
        pub fn new(inner: HiveManifestStore) -> Self {
            Self {
                inner,
                stall: AtomicBool::new(false),
            }
        }
        async fn maybe_stall(&self) {
            if self.stall.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }

    #[async_trait::async_trait]
    impl HiveMetadataSource for SlowSource {
        async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
            self.inner.list_databases().await
        }
        async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
            self.maybe_stall().await;
            self.inner.list_tables(database).await
        }
        async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
            self.maybe_stall().await;
            self.inner.get(database, table).await
        }
        async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
            self.maybe_stall().await;
            self.inner.manifest_etag(database, table).await.map(|_| None)
        }
        fn limits(&self) -> &ManifestLimits {
            self.inner.limits()
        }
    }
}

/// A metastore that stops answering is cut at the timeout. A table seen
/// before is served from its last known manifest with a warning; a table
/// never seen fails with a named error instead of hanging the query.
#[tokio::test]
async fn a_stalled_metastore_serves_stale_metadata_or_fails_by_name() {
    use datafusion::catalog::CatalogProvider;
    use sqe_catalog::hive::source::TimeoutSource;

    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let slow = Arc::new(slow::SlowSource::new(fixture_store(&f.root)));
    let bounded: Arc<dyn sqe_catalog::hive::source::HiveMetadataSource> = Arc::new(
        TimeoutSource::new(
            Arc::clone(&slow) as Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>,
            Duration::from_millis(200),
        ),
    );
    // Recheck window zero: every lookup asks the source, so the stall is
    // hit on the second lookup rather than hidden by the recheck window.
    let cache = HiveMetadataCache::new().with_recheck(Duration::ZERO);
    let provider = Arc::new(HiveCatalogProvider::with_source(
        "lake".to_string(),
        bounded,
        Arc::new(counting::CountingFactory(counting::CountingStore::local())),
        ctx.runtime_env(),
        Arc::new(cache),
        Duration::from_secs(60),
    ));
    provider.refresh_all().await;
    ctx.register_catalog("lake", provider.clone());

    // Warm: the table is known.
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 3);

    slow.stall.store(true, std::sync::atomic::Ordering::SeqCst);

    // Known table: the stalled etag/get is cut and the cached manifest
    // answers, well inside the 30 s the source would have taken.
    let started = std::time::Instant::now();
    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .expect("stale metadata keeps the table queryable")
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 3);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the timeout must cut the stalled call: took {:?}",
        started.elapsed()
    );

    // Never-seen table: nothing stale to serve, so a named error.
    let sales = provider.schema("sales").expect("sales");
    let started = std::time::Instant::now();
    let err = sales
        .table("never_loaded")
        .await
        .expect_err("no stale entry: the timeout is an error");
    assert!(
        err.to_string().contains("catalog metadata timeout"),
        "error must name the timeout: {err}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));

    // A stalled listing keeps the previous snapshot instead of failing.
    let before = provider
        .schema("sales")
        .expect("sales")
        .table_names();
    let started = std::time::Instant::now();
    let _ = provider.refresh_tables("sales").await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        provider.schema("sales").expect("sales").table_names(),
        before,
        "a timed-out listing keeps the last known names"
    );
}

/// A limited scan over a projected table reads partitions lazily, newest
/// first, and stops at the limit. Before this, `scan_from_entries` built one
/// `ListingTable` scan per surviving partition at plan time, and building it
/// lists the prefix: `LIMIT 10` on a 1,241-day snapshot table paid 1,241
/// listings, twenty minutes, for one file's worth of rows. Here the two
/// projected partitions are read one at a time: `LIMIT 1` is satisfied by
/// the newest and never lists the older; `LIMIT 2` needs both; and an
/// unlimited `count(*)` still reads everything through the eager union.
#[tokio::test]
async fn a_limited_scan_reads_projected_partitions_newest_first_and_stops_at_the_limit() {
    let f = partitioned_csv_root().await;
    {
        let store = fixture_store(&f.root);
        let mut m = store.get("sales", "orders").await.unwrap();
        m.parameters
            .insert("projection.enabled".into(), "true".into());
        m.parameters
            .insert("projection.dt.type".into(), "enum".into());
        m.parameters.insert(
            "projection.dt.values".into(),
            "2026-01-01,2026-01-02".into(),
        );
        store.put(&m, false).await.unwrap();
    }
    let ctx = SessionContext::new();
    let (provider, _source, store) =
        counting_root(&f, &ctx, HiveMetadataCache::new(), true).await;
    ctx.register_catalog("lake", provider.clone());
    // DataFusion's own listing goes through the runtime's object-store
    // registry; serve `file://` from the same counter so it is counted too.
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file:///").expect("url"),
        store.clone() as Arc<dyn object_store::ObjectStore>,
    );
    let _ = load_orders(&provider).await;
    let warm = store.lists();

    let explain = ctx
        .sql("EXPLAIN SELECT id, dt FROM lake.sales.orders LIMIT 1")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("explain");
    let explain = datafusion::arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format")
        .to_string();
    assert!(
        explain.contains("LazyPartitionScanExec: partitions=2, limit=1"),
        "a limited scan over two partitions plans lazily: {explain}"
    );
    assert_eq!(
        store.lists(),
        warm,
        "planning the lazy scan lists nothing; listing is deferred to execution"
    );

    let one = ctx
        .sql("SELECT id, dt FROM lake.sales.orders LIMIT 1")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("read");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&one)
        .expect("format")
        .to_string();
    assert_eq!(one.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        text.contains("2026-01-02") && text.contains("| 3 "),
        "the newest partition answers a bare LIMIT: {text}"
    );
    let after_one = store.lists() - warm;
    assert_eq!(
        after_one, 1,
        "LIMIT 1 over two partitions lists exactly the one it reads"
    );

    let two = ctx
        .sql("SELECT id, dt FROM lake.sales.orders LIMIT 2")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("read");
    assert_eq!(two.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&two)
        .expect("format")
        .to_string();
    assert!(
        text.contains("2026-01-02") && text.contains("2026-01-01"),
        "a limit past the newest partition continues into the next: {text}"
    );
    assert!(
        store.lists() > warm + after_one,
        "LIMIT 2 lists again for the partition it continues into"
    );

    let n = count_i64(
        &ctx.sql("SELECT count(*) FROM lake.sales.orders")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(n, 3, "an unlimited scan still reads every partition");
}

/// A compression probe that fails to list (a credential outage, the SSO
/// portal answering 429 under a burst) must not be remembered as
/// "uncompressed": that guess was cached for a gzipped JSON table and every
/// read for the next minute failed inside the JSON parser. The failed probe
/// records no codec; the next lookup probes again and records the real one.
#[tokio::test]
async fn a_failed_compression_probe_is_not_recorded_as_a_fact() {
    use std::sync::atomic::Ordering;

    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let (provider, _source, store) =
        counting_root(&f, &ctx, HiveMetadataCache::new(), true).await;

    store.fail_next_lists.store(1, Ordering::SeqCst);
    let _ = load_orders(&provider).await;
    let facts = provider.manifest_cache().facts("sales", "orders");
    assert!(
        facts.as_ref().is_none_or(|f| f.compression.is_none()),
        "a failed probe records no codec: {:?}",
        facts.as_ref().and_then(|f| f.compression)
    );

    let _ = load_orders(&provider).await;
    let facts = provider
        .manifest_cache()
        .facts("sales", "orders")
        .expect("facts after a successful probe");
    assert!(
        facts.compression.is_some(),
        "the next lookup probes again and records the codec"
    );
}

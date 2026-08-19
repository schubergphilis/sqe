#![cfg(feature = "hive-external")]

use datafusion::prelude::{SessionConfig, SessionContext};

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
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("fmt")
        .to_string();
    assert!(
        text.contains('1'),
        "__HIVE_DEFAULT_PARTITION__ must read as NULL: {text}"
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

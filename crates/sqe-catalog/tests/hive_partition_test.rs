#![cfg(feature = "hive-external")]

use std::collections::BTreeMap;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::partition::{
    add_partition, analyze_table, drop_partition, list_partitions, msck_repair, PartitionSpec,
};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

fn store_for(dir: &std::path::Path) -> HiveManifestStore {
    store_for_limits(dir, ManifestLimits::default())
}

fn store_for_limits(dir: &std::path::Path, limits: ManifestLimits) -> HiveManifestStore {
    let url = datafusion::datasource::listing::ListingTableUrl::parse(format!(
        "file://{}",
        dir.display()
    ))
    .expect("url");
    let os: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(LocalFileSystem::new());
    HiveManifestStore::new(
        url,
        os,
        limits,
        TvfPolicy {
            allow_local_paths: true,
            ..TvfPolicy::default()
        },
        TvfCaller::trusted_local(),
    )
}

fn manifest(location: &str) -> HiveManifest {
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         STORED AS PARQUET LOCATION '{location}'"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    HiveManifest::from_spec(&spec, "sales", "jacob", "2026-08-19T00:00:00Z")
}

/// A table location with two partition directories, one file each.
async fn seeded_table(dir: &tempfile::TempDir) -> (HiveManifestStore, HiveManifest) {
    let data = dir.path().join("data");
    for p in [
        "data/dt=2026-01-01/f0.parquet",
        "data/dt=2026-01-02/f0.parquet",
        "data/dt=2026-01-02/f1.parquet",
    ] {
        let path = dir.path().join(p);
        std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
        std::fs::write(&path, b"xx").expect("write");
    }
    let store = store_for(dir.path());
    let m = manifest(&format!("file://{}", data.display()));
    (store, m)
}

#[tokio::test]
async fn add_partition_appends_one_entry_and_captures_stats() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    store.put(&m, false).await.expect("put");

    // An existing partition directory: stats must be captured from it.
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-02".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect("add");

    let back = store.get("sales", "orders").await.expect("get");
    let idx = back.sqe.partition_index.expect("index");
    assert_eq!(idx.entries.len(), 1);
    assert_eq!(idx.entries[0].file_count, 2);
    assert_eq!(idx.entries[0].total_bytes, 4);
    assert!(
        idx.entries[0].location.ends_with("dt=2026-01-02/"),
        "derived layout under the table location: {}",
        idx.entries[0].location
    );

    // A not-yet-existing directory is legal Hive behavior: zero files.
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-03".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect("add future partition");
    let back = store.get("sales", "orders").await.expect("get");
    let idx = back.sqe.partition_index.expect("index");
    assert_eq!(idx.entries.len(), 2);
    assert_eq!(idx.entries[1].file_count, 0);
}

#[test]
fn add_partition_rejects_an_undeclared_key() {
    let dir = tempfile::tempdir().expect("tmp");
    let rt = tokio::runtime::Runtime::new().expect("rt");
    rt.block_on(async {
        let (store, mut m) = seeded_table(&dir).await;
        let err = add_partition(
            &store,
            &mut m,
            &PartitionSpec {
                values: vec![("region".to_string(), "eu".to_string())],
                location: None,
            },
            false,
        )
        .await
        .expect_err("must reject");
        assert!(format!("{err:?}").contains("region"), "{err:?}");
    });
}

#[tokio::test]
async fn duplicate_add_errors_without_if_not_exists_and_noops_with_it() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    let spec = PartitionSpec {
        values: vec![("dt".to_string(), "2026-01-03".to_string())],
        location: None,
    };
    add_partition(&store, &mut m, &spec, false).await.expect("first");


    let err = add_partition(&store, &mut m, &spec, false)
        .await
        .expect_err("duplicate must error");
    assert!(format!("{err:?}").contains("already exists"), "{err:?}");

    add_partition(&store, &mut m, &spec, true).await.expect("no-op");
    let back = store.get("sales", "orders").await.expect("get");
    assert_eq!(
        back.sqe.partition_index.as_ref().expect("idx").entries.len(),
        1
    );
}

#[tokio::test]
async fn drop_partition_removes_only_the_entry() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    msck_repair(&store, &mut m, 100).await.expect("repair");
    let spec = PartitionSpec {
        values: vec![("dt".to_string(), "2026-01-02".to_string())],
        location: None,
    };
    drop_partition(&store, &mut m, &spec, false).await.expect("drop");

    let back = store.get("sales", "orders").await.expect("get");
    let idx = back.sqe.partition_index.expect("index");
    assert_eq!(idx.entries.len(), 1);
    assert_eq!(idx.entries[0].values.get("dt").map(String::as_str), Some("2026-01-01"));

    // Data still on disk.
    assert!(dir.path().join("data/dt=2026-01-02/f0.parquet").exists());

    // Absent without IF EXISTS errors; with it, no-op.
    let err = drop_partition(&store, &mut m, &spec, false)
        .await
        .expect_err("must error");
    assert!(format!("{err:?}").contains("does not exist"), "{err:?}");
    drop_partition(&store, &mut m, &spec, true).await.expect("no-op");
}

#[tokio::test]
async fn msck_repair_builds_the_whole_index_from_storage() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    let count = msck_repair(&store, &mut m, 100).await.expect("repair");
    assert_eq!(count, 2);

    let back = store.get("sales", "orders").await.expect("get");
    let idx = back.sqe.partition_index.expect("index");
    assert_eq!(idx.entries.len(), 2);
    let dt_0102 = idx
        .entries
        .iter()
        .find(|e| e.values.get("dt").map(String::as_str) == Some("2026-01-02"))
        .expect("entry");
    assert_eq!(dt_0102.file_count, 2, "two files in that partition");
    assert_eq!(dt_0102.total_bytes, 4);
}

#[tokio::test]
async fn msck_repair_skips_an_untypeable_segment_and_keeps_valid_neighbors() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    for p in [
        "data/dt=not-a-date/f0.parquet",
        "data/dt=2026-01-02/f0.parquet",
    ] {
        let path = dir.path().join(p);
        std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
        std::fs::write(&path, b"x").expect("write");
    }
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt date) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
            .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.expect("put");

    let count = msck_repair(&store, &mut m, 100).await.expect("repair skips junk");
    assert_eq!(count, 1);
    let idx = store
        .get("sales", "orders")
        .await
        .unwrap()
        .sqe
        .partition_index
        .unwrap();
    assert_eq!(idx.entries.len(), 1);
    assert_eq!(
        idx.entries[0].values.get("dt").map(String::as_str),
        Some("2026-01-02")
    );
}

#[tokio::test]
async fn msck_repair_enforces_the_cap_without_touching_the_old_index() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    // Seed an index with one entry first.
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "seeded".to_string())],
            location: Some(format!("file://{}/data/dt=seeded/", dir.path().display())),
        },
        false,
    )
    .await
    .expect("seed");

    let err = msck_repair(&store, &mut m, 1).await.expect_err("cap must bind");
    assert!(format!("{err:?}").contains("exceeding the cap"), "{err:?}");

    // The old index is untouched.
    let back = store.get("sales", "orders").await.expect("get");
    assert_eq!(
        back.sqe.partition_index.as_ref().expect("idx").entries.len(),
        1
    );
}

#[tokio::test]
async fn show_partitions_reads_the_index_then_falls_back_to_listing() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;

    // No index yet: listing fallback.
    let listed = list_partitions(&store, &m).await.expect("list");
    let specs: Vec<String> = listed.into_iter().map(|p| p.spec).collect();
    assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"]);

    // With an index: read from it, including an entry outside the prefix.
    m.sqe.partition_index =
        Some(sqe_catalog::hive::manifest::PartitionIndex {
            format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
            entries: vec![
                sqe_catalog::hive::manifest::PartitionIndexEntry {
                    values: BTreeMap::from([("dt".to_string(), "2026-05-01".to_string())]),
                    location: format!("file://{}", dir.path().join("elsewhere/").display()),
                    file_count: 0,
                    total_bytes: 0,
                },
                sqe_catalog::hive::manifest::PartitionIndexEntry {
                    values: BTreeMap::from([(
                        "dt".to_string(),
                        "__HIVE_DEFAULT_PARTITION__".to_string(),
                    )]),
                    location: format!("file://{}", dir.path().join("data/__HIVE_DEFAULT_PARTITION__/").display()),
                    file_count: 0,
                    total_bytes: 0,
                },
            ],
        });
    let from_index = list_partitions(&store, &m).await.expect("index");
    let specs: Vec<String> = from_index.into_iter().map(|p| p.spec).collect();
    assert_eq!(specs, vec!["dt=2026-05-01", "dt=__HIVE_DEFAULT_PARTITION__"]);
}

#[tokio::test]
async fn drop_partition_on_a_projected_table_errors_naming_projection() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "enum".into());
    m.parameters
        .insert("projection.dt.values".into(), "2026-01-01".into());
    store.put(&m, false).await.expect("put");
    let err = drop_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("projected DROP must fail");
    let msg = err.to_string();
    assert!(msg.contains("projection"), "{msg}");
}

#[tokio::test]
async fn add_partition_on_a_projected_table_errors_naming_projection() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "enum".into());
    m.parameters
        .insert("projection.dt.values".into(), "2026-01-01".into());
    store.put(&m, false).await.expect("put");
    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("projected ADD must fail");
    assert!(err.to_string().contains("projection"), "{err}");
}

#[tokio::test]
async fn add_partition_cap_leaves_the_index_unchanged() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut limits = ManifestLimits::default();
    limits.max_partition_index_entries = 1;
    let store = store_for_limits(dir.path(), limits);
    let mut m = manifest(&format!("file://{}", data.display()));
    store.put(&m, false).await.unwrap();
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "a".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect("first add fills the cap");
    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "b".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("second add must hit the cap");
    let msg = err.to_string();
    assert!(msg.contains("manifest_max_partition_index_entries") || msg.contains("cap"), "{msg}");
    assert_eq!(
        m.sqe.partition_index.as_ref().unwrap().entries.len(),
        1,
        "a failed write must restore the in-memory index"
    );
    let back = store.get("sales", "orders").await.unwrap();
    assert_eq!(back.sqe.partition_index.unwrap().entries.len(), 1);
}

#[tokio::test]
async fn drop_if_exists_on_a_table_with_no_index_is_a_noop() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    store.put(&m, false).await.unwrap();
    drop_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: None,
        },
        true,
    )
    .await
    .expect("IF EXISTS no-op with no index");
    assert!(store.get("sales", "orders").await.unwrap().sqe.partition_index.is_none());
}

#[tokio::test]
async fn add_rejects_a_slash_in_the_value_without_an_explicit_location() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026/01/02".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("slash would corrupt k=v/");
    assert!(err.to_string().contains('/'), "{err}");
}

#[tokio::test]
async fn add_and_drop_composite_partition_keys() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let part = data.join("dt=2026-01-01").join("region=eu");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("f0.parquet"), b"xx").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
         PARTITIONED BY (dt string, region string) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();

    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![
                ("dt".to_string(), "2026-01-01".to_string()),
                ("region".to_string(), "eu".to_string()),
            ],
            location: None,
        },
        false,
    )
    .await
    .expect("add composite");
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=2026-01-01/region=eu");

    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("missing region");
    assert!(err.to_string().contains("region"), "{err}");

    drop_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![
                ("dt".to_string(), "2026-01-01".to_string()),
                ("region".to_string(), "eu".to_string()),
            ],
            location: None,
        },
        false,
    )
    .await
    .expect("drop composite");
    assert!(list_partitions(&store, &m).await.unwrap().is_empty());
}

#[tokio::test]
async fn msck_on_an_empty_table_writes_an_empty_index() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let store = store_for(dir.path());
    let mut m = manifest(&format!("file://{}", data.display()));
    store.put(&m, false).await.unwrap();
    let n = msck_repair(&store, &mut m, 100).await.expect("repair empty");
    assert_eq!(n, 0);
    assert!(store
        .get("sales", "orders")
        .await
        .unwrap()
        .sqe
        .partition_index
        .unwrap()
        .entries
        .is_empty());
}

#[tokio::test]
async fn analyze_csv_counts_rows_minus_the_header() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let part = data.join("dt=2026-01-01");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("a.csv"), "id\n1\n2\n3\n").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: None,
        },
        false,
    )
    .await
    .unwrap();
    let stats = analyze_table(&store, &mut m).await.expect("analyze");
    assert_eq!(stats.row_count, 3, "header must not count as a row");
    assert_eq!(stats.entries[0].row_count, 3);
}

#[tokio::test]
async fn analyze_on_an_empty_partition_is_zero_not_absent() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "empty".to_string())],
            location: None,
        },
        false,
    )
    .await
    .unwrap();
    let stats = analyze_table(&store, &mut m).await.expect("analyze");
    let empty = stats
        .entries
        .iter()
        .find(|e| e.values.get("dt").map(String::as_str) == Some("empty"))
        .expect("empty entry");
    assert_eq!(empty.row_count, 0);
}

#[tokio::test]
async fn duplicate_add_leaves_the_in_memory_index_intact() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    let spec = PartitionSpec {
        values: vec![("dt".to_string(), "2026-01-03".to_string())],
        location: None,
    };
    add_partition(&store, &mut m, &spec, false)
        .await
        .expect("first");
    add_partition(&store, &mut m, &spec, false)
        .await
        .expect_err("duplicate");
    assert_eq!(
        m.sqe.partition_index.as_ref().expect("idx").entries.len(),
        1,
        "the in-memory manifest must still hold the original entry"
    );
    add_partition(&store, &mut m, &spec, true)
        .await
        .expect("IF NOT EXISTS");
    assert_eq!(m.sqe.partition_index.as_ref().expect("idx").entries.len(), 1);
}

#[tokio::test]
async fn if_not_exists_does_not_replace_the_original_location() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    let original = format!("file://{}/data/dt=2026-01-01/", dir.path().display());
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: Some(original.clone()),
        },
        false,
    )
    .await
    .expect("first");
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "2026-01-01".to_string())],
            location: Some(format!("file://{}/elsewhere/", dir.path().display())),
        },
        true,
    )
    .await
    .expect("no-op");
    let loc = m
        .sqe
        .partition_index
        .as_ref()
        .unwrap()
        .entries[0]
        .location
        .clone();
    assert_eq!(loc, original);
}

#[tokio::test]
async fn drop_if_exists_leaves_the_in_memory_index_intact() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    msck_repair(&store, &mut m, 100).await.expect("repair");
    drop_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "no-such".to_string())],
            location: None,
        },
        true,
    )
    .await
    .expect("IF EXISTS no-op");
    assert_eq!(
        m.sqe.partition_index.as_ref().expect("idx").entries.len(),
        2
    );
}

#[tokio::test]
async fn add_rejects_an_untypeable_date_value() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt date) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "not-a-date".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect_err("must reject");
    assert!(err.to_string().contains("not-a-date"), "{err}");
}

#[tokio::test]
async fn add_accepts_the_null_sentinel_for_any_type() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![(
                "dt".to_string(),
                "__HIVE_DEFAULT_PARTITION__".to_string(),
            )],
            location: None,
        },
        false,
    )
    .await
    .expect("sentinel");
    let listed = list_partitions(&store, &m).await.unwrap();
    assert!(
        listed.iter().any(|p| p.spec == "dt=__HIVE_DEFAULT_PARTITION__"),
        "{listed:?}"
    );
}

#[tokio::test]
async fn add_spec_key_order_does_not_matter() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(data.join("dt=2026-01-01").join("region=eu")).unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
         PARTITIONED BY (dt string, region string) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![
                ("region".to_string(), "eu".to_string()),
                ("dt".to_string(), "2026-01-01".to_string()),
            ],
            location: None,
        },
        false,
    )
    .await
    .expect("reversed key order");
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=2026-01-01/region=eu");
}

#[tokio::test]
async fn show_partitions_uses_declaration_order_not_key_sort() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(data.join("region=eu").join("dt=2026-01-01")).unwrap();
    std::fs::write(
        data.join("region=eu").join("dt=2026-01-01").join("f0.parquet"),
        b"xx",
    )
    .unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
         PARTITIONED BY (region string, dt string) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![
                ("region".to_string(), "eu".to_string()),
                ("dt".to_string(), "2026-01-01".to_string()),
            ],
            location: None,
        },
        false,
    )
    .await
    .expect("add");
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(
        listed[0].spec, "region=eu/dt=2026-01-01",
        "Athena renders PARTITIONED BY order, not alphabetical"
    );
    let loc = &m.sqe.partition_index.as_ref().unwrap().entries[0].location;
    assert!(
        loc.contains("region=eu/dt=2026-01-01"),
        "derived LOCATION must follow declaration order: {loc}"
    );
}

#[tokio::test]
async fn empty_index_show_partitions_is_empty_not_a_listing_fallback() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    m.sqe.partition_index = Some(sqe_catalog::hive::manifest::PartitionIndex {
        format_version: sqe_catalog::hive::manifest::PARTITION_INDEX_FORMAT_VERSION,
        entries: vec![],
    });
    let listed = list_partitions(&store, &m).await.unwrap();
    assert!(
        listed.is_empty(),
        "a present-but-empty index must not fall back to listing: {listed:?}"
    );
}

#[tokio::test]
async fn msck_composite_keys_follow_the_directory_layout() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let part = data.join("dt=2026-01-01").join("region=eu");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("f0.parquet"), b"xx").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
         PARTITIONED BY (dt string, region string) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    let n = msck_repair(&store, &mut m, 100).await.expect("repair");
    assert_eq!(n, 1);
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=2026-01-01/region=eu");
}

#[tokio::test]
async fn msck_skips_a_non_kv_directory_and_indexes_the_rest() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    store.put(&m, false).await.expect("put");
    let junk = dir.path().join("data/not-a-partition");
    std::fs::create_dir_all(&junk).unwrap();
    std::fs::write(junk.join("f0.parquet"), b"x").unwrap();
    let n = msck_repair(&store, &mut m, 100)
        .await
        .expect("junk dirs are skipped");
    assert_eq!(n, 2, "seeded dt= partitions must still be indexed");
    let specs: Vec<_> = list_partitions(&store, &m)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.spec)
        .collect();
    assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"]);
}

#[tokio::test]
async fn msck_skips_a_file_at_the_table_root() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    store.put(&m, false).await.expect("put");
    std::fs::write(dir.path().join("data/orphan.parquet"), b"x").unwrap();
    let n = msck_repair(&store, &mut m, 100)
        .await
        .expect("orphan at the table root is skipped");
    assert_eq!(n, 2);
}

#[tokio::test]
async fn analyze_without_an_index_counts_the_whole_location() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let part = data.join("dt=2026-01-01");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("a.csv"), "id\n1\n2\n").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    let stats = analyze_table(&store, &mut m).await.expect("analyze");
    assert_eq!(stats.row_count, 2);
    assert!(
        stats.entries.is_empty(),
        "no index means one total, no per-entry stats"
    );
}

#[tokio::test]
async fn add_empty_string_value_is_a_legal_string_partition() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), String::new())],
            location: None,
        },
        false,
    )
    .await
    .expect("empty string is a value, not NULL");
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=");
}

#[tokio::test]
async fn add_partition_refuses_a_forbidden_location_before_listing() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let url = datafusion::datasource::listing::ListingTableUrl::parse(format!(
        "file://{}",
        dir.path().display()
    ))
    .unwrap();
    let store = HiveManifestStore::new(
        url,
        std::sync::Arc::new(LocalFileSystem::new()),
        ManifestLimits::default(),
        TvfPolicy::default(),
        TvfCaller::trusted_local(),
    );
    let mut m = manifest(&format!("file://{}", data.display()));
    let err = add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "x".to_string())],
            location: Some("file:///etc/".into()),
        },
        false,
    )
    .await
    .expect_err("forbidden LOCATION must not be listed");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "got {err:?}"
    );
    assert!(m.sqe.partition_index.is_none());
}

#[tokio::test]
async fn msck_on_a_projected_table_is_a_noop() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "enum".into());
    m.parameters
        .insert("projection.dt.values".into(), "2026-01-01".into());
    store.put(&m, false).await.expect("put");
    let n = msck_repair(&store, &mut m, 100)
        .await
        .expect("projected MSCK is a no-op");
    assert_eq!(n, 0);
    assert!(
        m.sqe.partition_index.is_none(),
        "projection owns the set; repair must not write an index from a listing"
    );
    assert!(
        store
            .get("sales", "orders")
            .await
            .unwrap()
            .sqe
            .partition_index
            .is_none()
    );
}

#[tokio::test]
async fn msck_on_a_projected_table_leaves_a_leftover_index_untouched() {
    let dir = tempfile::tempdir().expect("tmp");
    let (store, mut m) = seeded_table(&dir).await;
    add_partition(
        &store,
        &mut m,
        &PartitionSpec {
            values: vec![("dt".to_string(), "leftover".to_string())],
            location: None,
        },
        false,
    )
    .await
    .expect("seed index");
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "enum".into());
    m.parameters
        .insert("projection.dt.values".into(), "2026-01-01".into());
    store.put(&m, false).await.expect("put");
    let n = msck_repair(&store, &mut m, 100).await.expect("no-op");
    assert_eq!(n, 0);
    let idx = m.sqe.partition_index.as_ref().expect("leftover index");
    assert_eq!(idx.entries.len(), 1);
    assert_eq!(
        idx.entries[0].values.get("dt").map(String::as_str),
        Some("leftover")
    );
}

#[tokio::test]
async fn msck_skips_an_extra_partition_key_and_an_incomplete_composite() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    // Extra key `region` on a dt-only table: skip.
    let extra = data.join("dt=2026-01-01").join("region=eu");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("f0.parquet"), b"x").unwrap();
    // Matching depth: keep.
    let ok = data.join("dt=2026-01-02");
    std::fs::create_dir_all(&ok).unwrap();
    std::fs::write(ok.join("f0.parquet"), b"x").unwrap();
    let store = store_for(dir.path());
    let mut m = manifest(&format!("file://{}", data.display()));
    store.put(&m, false).await.unwrap();
    let n = msck_repair(&store, &mut m, 100).await.expect("repair");
    assert_eq!(n, 1);
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=2026-01-02");
}

#[tokio::test]
async fn msck_skips_a_partial_composite_and_keeps_the_full_path() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let partial = data.join("dt=2026-01-01");
    std::fs::create_dir_all(&partial).unwrap();
    std::fs::write(partial.join("orphan.parquet"), b"x").unwrap();
    let full = data.join("dt=2026-01-01").join("region=eu");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("f0.parquet"), b"xx").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
         PARTITIONED BY (dt string, region string) \
         STORED AS PARQUET LOCATION 'file://{}'",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    store.put(&m, false).await.unwrap();
    let n = msck_repair(&store, &mut m, 100).await.expect("repair");
    assert_eq!(n, 1);
    let listed = list_partitions(&store, &m).await.unwrap();
    assert_eq!(listed[0].spec, "dt=2026-01-01/region=eu");
}

#[tokio::test]
async fn analyze_on_a_projected_table_counts_the_computed_set() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    let part = data.join("dt=2026-01-01");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("a.csv"), "id\n1\n2\n").unwrap();
    // A second partition on disk that projection does not include.
    let extra = data.join("dt=2026-01-02");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("a.csv"), "id\n3\n").unwrap();
    let store = store_for(dir.path());
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
        data.display()
    );
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
        .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let mut m = HiveManifest::from_spec(&spec, "sales", "jacob", "t");
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "enum".into());
    m.parameters
        .insert("projection.dt.values".into(), "2026-01-01".into());
    store.put(&m, false).await.unwrap();
    let stats = analyze_table(&store, &mut m).await.expect("analyze");
    assert_eq!(stats.row_count, 2, "only the projected partition");
    assert_eq!(stats.entries.len(), 1);
}

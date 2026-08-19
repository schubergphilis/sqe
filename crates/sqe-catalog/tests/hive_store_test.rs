#![cfg(feature = "hive-external")]

use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

fn store_for(dir: &std::path::Path, tvf: TvfPolicy) -> HiveManifestStore {
    let url = ListingTableUrl::parse(format!("file://{}", dir.display())).expect("url");
    let os: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    HiveManifestStore::new(
        url,
        os,
        ManifestLimits::default(),
        tvf,
        TvfCaller::trusted_local(),
    )
}

/// A policy that allows local paths, which is what the embedded CLI uses.
fn permissive() -> TvfPolicy {
    TvfPolicy {
        allow_local_paths: true,
        ..TvfPolicy::default()
    }
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

#[tokio::test]
async fn manifest_path_follows_the_documented_layout() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let p = s.manifest_path("sales", "orders").to_string();
    assert!(p.ends_with("sales/orders/_sqe/table.json"), "got {p}");
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("put");
    let back = s.get("sales", "orders").await.expect("get");
    assert_eq!(back.name, "orders");
}

#[tokio::test]
async fn get_on_a_missing_table_is_not_found() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let err = s.get("sales", "nope").await.expect_err("must fail");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::NotFound(_)),
        "a missing manifest is table-not-found, got {err:?}"
    );
}

#[tokio::test]
async fn a_planted_manifest_pointing_at_a_forbidden_location_is_refused_on_load() {
    let dir = tempfile::tempdir().expect("tmp");
    // Write the manifest with a permissive policy, simulating an attacker who
    // can write to the warehouse prefix directly.
    let planted = manifest("file:///var/run/secrets/kubernetes.io/serviceaccount/token");
    store_for(dir.path(), permissive())
        .put(&planted, false)
        .await
        .expect("plant");

    // Read it back with the coordinator's fail-closed policy: local paths off.
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .get("sales", "orders")
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "the location guard must run on every load, not only at create; got {err:?}"
    );
}

#[tokio::test]
async fn put_refuses_a_forbidden_location_up_front() {
    let dir = tempfile::tempdir().expect("tmp");
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .put(&manifest("file:///etc/shadow"), false)
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn if_not_exists_does_not_overwrite() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let mut m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("first put");
    m.owner = Some("someone-else".into());
    let err = s.put(&m, true).await.expect_err("must not overwrite");
    assert!(
        format!("{err}").contains("orders"),
        "message should name the table: {err}"
    );
    assert_eq!(
        s.get("sales", "orders")
            .await
            .expect("get")
            .owner
            .as_deref(),
        Some("jacob")
    );
}

#[tokio::test]
async fn list_tables_finds_manifests_by_prefix() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    for name in ["orders", "customers"] {
        let mut m = manifest(&format!("file://{}", data.display()));
        m.name = name.to_string();
        s.put(&m, false).await.expect("put");
    }
    let mut tables = s.list_tables("sales").await.expect("list");
    tables.sort();
    assert_eq!(tables, vec!["customers".to_string(), "orders".to_string()]);
    assert_eq!(
        s.list_databases().await.expect("dbs"),
        vec!["sales".to_string()]
    );
}

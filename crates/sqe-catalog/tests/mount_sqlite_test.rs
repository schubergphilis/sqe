//! Integration tests for `sqe_catalog::mount::build_catalog` with
//! `CatalogKind::Sqlite`.
//!
//! These tests construct an actual SQLite-backed Iceberg catalog over
//! a tempdir warehouse and verify the resulting handle can run
//! `list_namespaces`. The test crate must enable the `sql-sqlite`
//! feature so the embedded SQLite driver is wired through `sqlx::any`;
//! the default SQE build only enables `sql-postgres`, so this test
//! file is gated to keep the standard `cargo test -p sqe-catalog`
//! pass without requiring a Postgres stack.

#![cfg(feature = "sql-sqlite")]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::SecretStore;
use sqe_sql::{CatalogKind, OptionValue};
use tempfile::tempdir;

#[tokio::test]
async fn build_sqlite_with_filesystem_path() {
    let dir = tempdir().expect("tempdir");
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();

    let catalog = build_catalog(
        dir.path().to_str().unwrap(),
        CatalogKind::Sqlite,
        &options,
        &secrets,
    )
    .await
    .expect("build_catalog should succeed for tempdir warehouse");

    // The freshly-created catalog has no namespaces. Listing returns
    // an empty Vec, which exercises the sqlx -> SQLite path end to end.
    let namespaces = catalog
        .list_namespaces(None)
        .await
        .expect("list_namespaces against empty catalog should succeed");
    assert!(
        namespaces.is_empty(),
        "expected no namespaces in fresh catalog, got {:?}",
        namespaces
    );

    // A second build over the same path must succeed too — the SQLite
    // file is reusable and the data dir already exists.
    let _again = build_catalog(
        dir.path().to_str().unwrap(),
        CatalogKind::Sqlite,
        &options,
        &secrets,
    )
    .await
    .expect("second build_catalog over the same warehouse path");
}

#[tokio::test]
async fn build_sqlite_warehouse_option_overrides_default() {
    let dir = tempdir().expect("tempdir");
    let secrets = SecretStore::new();

    // Point the data dir at a sibling directory; the default would
    // be `<dir>/iceberg/`. We pick `<dir>/custom-data/` to prove the
    // option threaded through.
    let custom = dir.path().join("custom-data");
    std::fs::create_dir_all(&custom).unwrap();
    let warehouse_url = format!("file://{}", custom.display());

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String(warehouse_url.clone()),
    );

    let catalog = build_catalog(
        dir.path().to_str().unwrap(),
        CatalogKind::Sqlite,
        &options,
        &secrets,
    )
    .await
    .expect("build_catalog with explicit WAREHOUSE option");

    let namespaces = catalog
        .list_namespaces(None)
        .await
        .expect("list_namespaces against custom-warehouse catalog");
    assert!(namespaces.is_empty());
}

#[tokio::test]
async fn build_catalog_dispatches_sqlite() {
    // End-to-end dispatch: invoke the public entry point with
    // `CatalogKind::Sqlite` and confirm the returned handle is
    // functional. Same shape as the first test but explicitly
    // exercises the `match kind` arm.
    let dir = tempdir().expect("tempdir");
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();

    let catalog = build_catalog(
        dir.path().to_str().unwrap(),
        CatalogKind::Sqlite,
        &options,
        &secrets,
    )
    .await
    .expect("dispatch through build_catalog");

    catalog
        .list_namespaces(None)
        .await
        .expect("dispatched catalog handle is usable");
}

#[tokio::test]
async fn build_sqlite_rejects_empty_location() {
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();
    let err = build_catalog("", CatalogKind::Sqlite, &options, &secrets)
        .await
        .expect_err("empty location must error");
    assert!(
        err.contains("location must not be empty"),
        "expected empty-location error, got: {err}"
    );
}

#[tokio::test]
async fn build_sqlite_url_form_requires_warehouse() {
    let dir = tempdir().expect("tempdir");
    let secrets = SecretStore::new();
    // sqlite:// shape skips the auto-derived warehouse path. Without
    // a WAREHOUSE option, the call must fail with a clear message.
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("explicit.db").display()
    );
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();
    let err = build_catalog(&url, CatalogKind::Sqlite, &options, &secrets)
        .await
        .expect_err("URL-form sqlite without WAREHOUSE must error");
    assert!(
        err.contains("WAREHOUSE"),
        "expected error to mention WAREHOUSE, got: {err}"
    );
}

//! Proves the vendored `RewriteFilesAction::set_snapshot_properties` wiring
//! that `rewrite_data_files_once` relies on for job-identity stamping
//! (Phase 4b, Task 1): properties handed to the action land in the
//! committed snapshot's `summary()`.
//!
//! `rewrite_data_files_once` (crates/sqe-coordinator/src/maintenance.rs)
//! resolves its catalog through `SessionCatalog::for_session`, which is
//! REST-only (Polaris) -- there is no SQLite backend on that path, and the
//! public `CALL system.rewrite_data_files` surface deliberately does not
//! accept snapshot properties (the manual path always passes `None`; only
//! the future Phase 4b scheduler will pass `Some(..)`). So this test drives
//! the same `Transaction::rewrite_files().set_snapshot_properties(..)` call
//! the coordinator makes, directly, against a real SQLite-backed Iceberg
//! catalog (the harness `maintenance_log_test.rs` / `runtime_catalog_test.rs`
//! use), and checks the one fact that was not previously verified: that the
//! setter's properties actually surface on `current_snapshot().summary()`
//! after commit. The coordinator's own `Option<HashMap<..>>` threading
//! (three lines: add the param, conditionally call the setter, pass `None`
//! at both call sites) is not exercised by this test; it is covered by
//! inspection and by the existing `#[ignore]` rewrite integration tests
//! continuing to pass with the manual (`None`) path.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite`.

#![cfg(feature = "test-sqlite")]

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_core::SecretStore;
use sqe_sql::CatalogKind;
use tempfile::TempDir;

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(location, CatalogKind::Sqlite, &BTreeMap::new(), &SecretStore::new())
        .await
        .expect("sqlite catalog builds")
}

fn minimal_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build schema")
}

/// A fabricated data file. The physical parquet file is never read (the
/// commit below skips `set_check_file_existence` / delete-filter validation,
/// exactly like `rewrite_data_files_once` does for its own commit), so no
/// bytes need to exist on disk for the metadata-only commit to succeed.
fn fabricated_data_file(path: &str) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition(Struct::from_iter(std::iter::empty()))
        .partition_spec_id(0)
        .build()
        .expect("build data file")
}

/// A `RewriteFilesAction` commit with `snapshot_properties` set must stamp
/// those key/value pairs onto the resulting snapshot's `summary()`, exactly
/// as `rewrite_data_files_once` depends on for the Phase 4b scheduler's
/// job-identity attribution (`sqe.maintenance.job-id` / `.principal` /
/// `.trigger`).
#[tokio::test]
async fn rewrite_files_action_stamps_snapshot_properties() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let creation = TableCreation::builder()
        .name("rewrite_props_test".to_string())
        .schema(minimal_schema())
        .build();
    let table = catalog.create_table(&ns, creation).await.expect("create table");

    let mut props = std::collections::HashMap::new();
    props.insert("sqe.maintenance.job-id".to_string(), "job-42".to_string());
    props.insert("sqe.maintenance.principal".to_string(), "svc-compactor".to_string());
    props.insert("sqe.maintenance.trigger".to_string(), "scheduled".to_string());

    let tx = Transaction::new(&table);
    let mut action = tx
        .rewrite_files()
        .add_data_files(vec![fabricated_data_file("mem://rewrite_props_test/data/f1.parquet")])
        .delete_files(Vec::new());
    action.set_snapshot_properties(props.clone());

    let tx_applied = action.apply(tx).expect("apply rewrite_files action");
    tx_applied.commit(catalog.as_ref()).await.expect("commit rewrite_files");

    let ident = TableIdent::new(ns, "rewrite_props_test".to_string());
    let reloaded = catalog.load_table(&ident).await.expect("reload table");
    let summary = reloaded
        .metadata()
        .current_snapshot()
        .expect("committed snapshot exists")
        .summary();

    for (k, v) in &props {
        assert_eq!(
            summary.additional_properties.get(k),
            Some(v),
            "snapshot summary must carry stamped key '{k}', got {:?}",
            summary.additional_properties
        );
    }
}

/// Sanity companion: when `snapshot_properties` is never set (the manual
/// `CALL system.rewrite_data_files` path, which always passes `None` through
/// `rewrite_data_files_once`), none of the job-identity keys appear in the
/// committed snapshot's summary. Guards against the setter's defaults
/// leaking a stamp when the coordinator does not call it.
#[tokio::test]
async fn rewrite_files_action_without_snapshot_properties_leaves_summary_unstamped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let creation = TableCreation::builder()
        .name("rewrite_no_props_test".to_string())
        .schema(minimal_schema())
        .build();
    let table = catalog.create_table(&ns, creation).await.expect("create table");

    // No `set_snapshot_properties` call at all -- mirrors the coordinator's
    // `if let Some(props) = snapshot_properties { action.set_snapshot_properties(props); }`
    // being skipped when the caller passes `None`.
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_files()
        .add_data_files(vec![fabricated_data_file("mem://rewrite_no_props_test/data/f1.parquet")])
        .delete_files(Vec::new());

    let tx_applied = action.apply(tx).expect("apply rewrite_files action");
    tx_applied.commit(catalog.as_ref()).await.expect("commit rewrite_files");

    let ident = TableIdent::new(ns, "rewrite_no_props_test".to_string());
    let reloaded = catalog.load_table(&ident).await.expect("reload table");
    let summary = reloaded
        .metadata()
        .current_snapshot()
        .expect("committed snapshot exists")
        .summary();

    for key in [
        "sqe.maintenance.job-id",
        "sqe.maintenance.principal",
        "sqe.maintenance.trigger",
    ] {
        assert!(
            !summary.additional_properties.contains_key(key),
            "unstamped commit must not carry job-identity key '{key}', got {:?}",
            summary.additional_properties
        );
    }
}

//! Integration tests for `sqe_coordinator::maintenance_log::append_row`.
//!
//! Uses a real SQLite-backed Iceberg catalog over a tempdir warehouse
//! (`sqe_catalog::mount::build_catalog(..., CatalogKind::Sqlite, ...)`),
//! the same harness `runtime_catalog_test.rs` uses. That gives a genuine
//! `Arc<dyn iceberg::Catalog>` end to end (create namespace/table, append,
//! scan back) without standing up Polaris/Docker.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite`.

#![cfg(feature = "test-sqlite")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::TryStreamExt;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use sqe_coordinator::maintenance_log::{advisory_row, append_row, maintenance_log_arrow_schema};
use sqe_coordinator::table_health::TableHealth;
use sqe_core::SecretStore;
use sqe_sql::CatalogKind;
use tempfile::TempDir;

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(
        location,
        CatalogKind::Sqlite,
        &BTreeMap::new(),
        &SecretStore::new(),
    )
    .await
    .expect("sqlite catalog builds")
}

fn sample_health() -> TableHealth {
    TableHealth {
        live_data_files: 7,
        small_files: 2,
        avg_file_bytes: 1_000,
        p50_file_bytes: 900,
        delete_files: 0,
        delete_heavy_files: 0,
        eligible_groups: 1,
        est_rewrite_bytes: 2_000,
        last_compaction_snapshot_ms: None,
        maintenance_enabled: true,
    }
}

/// Create `sqe_system.maintenance_log` with the fixed schema documented in
/// `maintenance_log.rs`, via the raw `Catalog` trait (no coordinator SQL
/// path involved -- this test only exercises `append_row`).
async fn create_maintenance_log_table(catalog: &Arc<dyn Catalog>) {
    catalog
        .create_namespace(
            &NamespaceIdent::new("sqe_system".to_string()),
            HashMap::new(),
        )
        .await
        .expect("create sqe_system namespace");

    let arrow_schema = maintenance_log_arrow_schema();
    let iceberg_schema: IcebergSchema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
            .expect("arrow schema converts to iceberg schema");

    let creation = TableCreation::builder()
        .name("maintenance_log".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&NamespaceIdent::new("sqe_system".to_string()), creation)
        .await
        .expect("create maintenance_log table");
}

#[tokio::test]
async fn append_row_writes_and_is_readable_when_table_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let row = advisory_row(
        "bench.orders",
        "maintenance-svc",
        &sample_health(),
        1_700_000_000_000,
    );

    append_row(&catalog, "sqe_system.maintenance_log", &row)
        .await
        .expect("append_row succeeds when the state table exists");

    // SELECT it back: reload the table and scan its one row.
    let ident = iceberg::TableIdent::new(
        NamespaceIdent::new("sqe_system".to_string()),
        "maintenance_log".to_string(),
    );
    let table = catalog.load_table(&ident).await.expect("reload table");
    let batches: Vec<_> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to arrow")
        .try_collect()
        .await
        .expect("collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "exactly one row must have been appended");

    // Find the job_id / status / files_in columns across the collected
    // batches and assert their values round-tripped.
    use arrow_array::{Array, Int64Array, StringArray};
    let mut seen_job_id = None;
    let mut seen_status = None;
    let mut seen_files_in = None;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(col) = batch.column_by_name("job_id") {
            seen_job_id = Some(
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("job_id is Utf8")
                    .value(0)
                    .to_string(),
            );
        }
        if let Some(col) = batch.column_by_name("status") {
            seen_status = Some(
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("status is Utf8")
                    .value(0)
                    .to_string(),
            );
        }
        if let Some(col) = batch.column_by_name("files_in") {
            seen_files_in = Some(
                col.as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("files_in is Int64")
                    .value(0),
            );
        }
    }

    assert_eq!(seen_job_id, Some(row.job_id.clone()));
    assert_eq!(seen_status, Some("advisory".to_string()));
    assert_eq!(seen_files_in, Some(row.files_in));
}

#[tokio::test]
async fn append_row_table_absent_returns_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    // Namespace exists (as it typically would in a real deployment that has
    // other sqe_system objects), but the ledger table itself was never
    // created by the operator.
    catalog
        .create_namespace(
            &NamespaceIdent::new("sqe_system".to_string()),
            HashMap::new(),
        )
        .await
        .expect("create sqe_system namespace");

    let row = advisory_row(
        "bench.orders",
        "maintenance-svc",
        &sample_health(),
        1_700_000_000_000,
    );

    let result = append_row(&catalog, "sqe_system.maintenance_log", &row).await;
    assert!(
        result.is_ok(),
        "append_row must return Ok when the state table is absent, got: {result:?}"
    );
}

#[tokio::test]
async fn append_row_namespace_absent_returns_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    // Neither the namespace nor the table exist.
    let row = advisory_row(
        "bench.orders",
        "maintenance-svc",
        &sample_health(),
        1_700_000_000_000,
    );

    let result = append_row(&catalog, "sqe_system.maintenance_log", &row).await;
    assert!(
        result.is_ok(),
        "append_row must return Ok when the state table's namespace is absent, got: {result:?}"
    );
}

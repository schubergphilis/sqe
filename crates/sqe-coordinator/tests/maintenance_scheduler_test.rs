//! End-to-end test for `sqe_coordinator::maintenance_scheduler`'s advisory
//! tick (Phase 4a advisory auto-compaction, Task 5).
//!
//! Uses the same real SQLite-backed Iceberg catalog harness as
//! `maintenance_log_test.rs` (`sqe_catalog::mount::build_catalog(...,
//! CatalogKind::Sqlite, ...)`), injected into the scheduler through its
//! `catalog_factory` seam so the test never needs a live REST/Polaris
//! catalog. `MaintenancePrincipal::mint_session` still runs for real
//! against a `wiremock` OIDC token endpoint, so the full path -- mint
//! session, discover tables, filter, analyze, emit -- is exercised, not
//! just the parts downstream of a hand-built session.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite`.

#![cfg(feature = "test-sqlite")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_core::config::{
    MaintenanceCompactionConfig, MaintenanceConfig, MaintenanceMode, MaintenancePrincipalConfig,
    MaintenanceSchedulerConfig,
};
use sqe_core::{SecretStore, SecretString};
use sqe_coordinator::maintenance_principal::MaintenancePrincipal;
use sqe_coordinator::maintenance_scheduler::MaintenanceScheduler;
use sqe_metrics::audit::{AuditFormat, AuditLogger};
use sqe_metrics::MetricsRegistry;
use sqe_sql::CatalogKind;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`. Mirrors
/// `maintenance_log_test.rs::sqlite_catalog`.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(location, CatalogKind::Sqlite, &BTreeMap::new(), &SecretStore::new())
        .await
        .expect("sqlite catalog builds")
}

fn one_col_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// Create a single-column (`id BIGINT`) table, optionally opted into the
/// scheduler via `sqe.maintenance.enabled = 'true'`.
async fn create_table(
    catalog: &Arc<dyn Catalog>,
    ns: &str,
    name: &str,
    opted_in: bool,
) -> TableIdent {
    let ns_ident = NamespaceIdent::new(ns.to_string());
    if !catalog.namespace_exists(&ns_ident).await.expect("namespace_exists") {
        catalog
            .create_namespace(&ns_ident, HashMap::new())
            .await
            .expect("create namespace");
    }

    let iceberg_schema: IcebergSchema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&one_col_arrow_schema())
            .expect("arrow schema converts to iceberg schema");

    let mut properties = HashMap::new();
    if opted_in {
        properties.insert("sqe.maintenance.enabled".to_string(), "true".to_string());
    }

    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(iceberg_schema)
        .properties(properties)
        .build();

    catalog
        .create_table(&ns_ident, creation)
        .await
        .expect("create table");

    TableIdent::new(ns_ident, name.to_string())
}

/// Write `file_count` single-row data files to `ident` and commit them all
/// in one `fast_append`, so `live_data_files == file_count` and every file
/// is "small" under the default (512 MiB) target size.
async fn seed_small_files(catalog: &Arc<dyn Catalog>, ident: &TableIdent, file_count: i64) {
    let table = catalog.load_table(ident).await.expect("load table for seeding");
    let compression = sqe_coordinator::writer::parse_parquet_compression("zstd");
    let mut all_files = Vec::new();
    for i in 0..file_count {
        let batch = RecordBatch::try_new(one_col_arrow_schema(), vec![Arc::new(Int64Array::from(vec![i]))])
            .expect("build one-row batch");
        let tracker = sqe_coordinator::writer::new_upload_tracker();
        let files = sqe_coordinator::writer::write_data_files(&table, vec![batch], "seed", compression, tracker)
            .await
            .expect("write seed data file");
        all_files.extend(files);
    }

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(all_files);
    let tx = action.apply(tx).expect("apply fast_append");
    tx.commit(catalog.as_ref()).await.expect("commit seed data files");
}

/// Create `sqe_system.maintenance_log` with the fixed schema documented in
/// `maintenance_log.rs`. Mirrors `maintenance_log_test.rs`.
async fn create_maintenance_log_table(catalog: &Arc<dyn Catalog>) {
    catalog
        .create_namespace(&NamespaceIdent::new("sqe_system".to_string()), HashMap::new())
        .await
        .expect("create sqe_system namespace");

    let arrow_schema = sqe_coordinator::maintenance_log::maintenance_log_arrow_schema();
    let iceberg_schema: IcebergSchema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
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

/// Stand up a wiremock OIDC token endpoint that always returns a fixed
/// access token for the `client_credentials` grant `OidcM2mProvider` sends.
async fn mock_idp() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "fake-maintenance-catalog-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        })))
        .mount(&server)
        .await;
    server
}

fn maintenance_config(idp: &MockServer, min_input_files: usize) -> MaintenanceConfig {
    MaintenanceConfig {
        mode: MaintenanceMode::Advisory,
        principal: Some(MaintenancePrincipalConfig {
            token_endpoint: format!("{}/token", idp.uri()),
            client_id: "sqe-maintenance-test".to_string(),
            client_secret: SecretString::new("test-secret".to_string()),
            scope: None,
            user_id: "svc-sqe-maintenance".to_string(),
            roles: vec!["maintenance".to_string()],
            refresh_skew_secs: 60,
        }),
        scheduler: MaintenanceSchedulerConfig {
            // jitter_secs = 0 disables the deterministic jitter window
            // (see `table_due`), so the opted-in table is unconditionally
            // due regardless of wall-clock time when the test runs.
            jitter_secs: 0,
            state_table: "sqe_system.maintenance_log".to_string(),
            ..Default::default()
        },
        compaction: MaintenanceCompactionConfig {
            min_input_files,
            ..Default::default()
        },
        distribution: Default::default(),
    }
}

/// Look up one f64 gauge sample's value for a given `table` label on a
/// `GaugeVec` metric family, via `registry.gather()`. Does NOT call
/// `with_label_values`/`get_metric_with_label_values`, so it never
/// auto-vivifies a series that was never actually set -- an absent series
/// legitimately returns `None`.
fn gauge_sample(metrics: &MetricsRegistry, family_name: &str, table_label: &str) -> Option<f64> {
    metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == family_name)
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "table" && l.value() == table_label)
                })
                .map(|m| m.get_gauge().value())
        })
}

/// Count how many samples exist at all for a metric family (regardless of
/// label), used to assert the non-opted table never entered the pipeline
/// (i.e. the family has exactly one sample: the opted table's).
fn sample_count(metrics: &MetricsRegistry, family_name: &str) -> usize {
    metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == family_name)
        .map(|f| f.get_metric().len())
        .unwrap_or(0)
}

#[tokio::test]
async fn advisory_tick_reports_opted_table_and_mutates_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    // Two user tables: one opted in with 3 small files (>= min_input_files
    // below, so it also produces a non-zero eligible-group signal), one
    // never opted in.
    let opted_ident = create_table(&catalog, "ns", "opted", true).await;
    seed_small_files(&catalog, &opted_ident, 3).await;

    let plain_ident = create_table(&catalog, "ns", "plain", false).await;
    seed_small_files(&catalog, &plain_ident, 1).await;

    create_maintenance_log_table(&catalog).await;

    let opted_snapshot_before = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();
    let plain_snapshot_before = catalog
        .load_table(&plain_ident)
        .await
        .expect("reload plain table")
        .metadata()
        .current_snapshot_id();

    let idp = mock_idp().await;
    let cfg = maintenance_config(&idp, 2);
    let principal = Arc::new(
        MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
            .expect("build principal"),
    );
    let metrics = Arc::new(MetricsRegistry::new().expect("metrics registry builds"));

    let audit_dir = tempfile::tempdir().expect("audit tempdir");
    let audit_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(
        AuditLogger::with_config(audit_path.to_str().expect("utf8 path"), AuditFormat::Native)
            .expect("audit logger builds"),
    );

    let injected_catalog = catalog.clone();
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory =
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });

    let scheduler = MaintenanceScheduler::new(cfg, principal, metrics.clone(), Some(audit.clone()), catalog_factory);

    scheduler.advisory_tick().await.expect("advisory_tick succeeds");

    // --- Gauges: the opted table's are set to the expected values ---
    assert_eq!(
        gauge_sample(&metrics, "sqe_table_small_files", "ns.opted"),
        Some(3.0),
        "opted table should report 3 small files"
    );
    assert_eq!(
        gauge_sample(&metrics, "sqe_table_delete_files", "ns.opted"),
        Some(0.0),
        "opted table has no delete files"
    );
    assert!(
        gauge_sample(&metrics, "sqe_maintenance_est_rewrite_bytes", "ns.opted").unwrap_or(0.0) > 0.0,
        "3 files >= min_input_files(2) should form an eligible bin-pack group with nonzero bytes"
    );

    // --- The non-opted table never entered the pipeline: exactly one
    // sample per family (the opted table's), never a second one for
    // "ns.plain". This checks presence via `gather()`, never calling
    // `with_label_values` for "ns.plain" (which would auto-vivify it). ---
    assert_eq!(
        sample_count(&metrics, "sqe_table_small_files"),
        1,
        "only the opted table should have produced a sqe_table_small_files sample"
    );
    assert_eq!(
        gauge_sample(&metrics, "sqe_table_small_files", "ns.plain"),
        None,
        "the non-opted table must never have a small_files sample"
    );

    // --- No snapshot was added to either USER table: advisory mutates
    // nothing. (The maintenance_log state table below DOES get a new
    // snapshot -- that is the one allowed write.) ---
    let opted_snapshot_after = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();
    let plain_snapshot_after = catalog
        .load_table(&plain_ident)
        .await
        .expect("reload plain table")
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        opted_snapshot_before, opted_snapshot_after,
        "advisory_tick must not commit a new snapshot to the opted table"
    );
    assert_eq!(
        plain_snapshot_before, plain_snapshot_after,
        "advisory_tick must not touch the non-opted table at all"
    );

    // --- maintenance_log: exactly one advisory row, for the opted table ---
    let log_ident = TableIdent::new(
        NamespaceIdent::new("sqe_system".to_string()),
        "maintenance_log".to_string(),
    );
    let log_table = catalog.load_table(&log_ident).await.expect("reload log table");
    let batches: Vec<_> = log_table
        .scan()
        .build()
        .expect("build log scan")
        .to_arrow()
        .await
        .expect("scan log to arrow")
        .try_collect()
        .await
        .expect("collect log batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "exactly one advisory row must have been appended");

    use arrow_array::{Array, StringArray};
    let mut seen_table_name = None;
    let mut seen_status = None;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if let Some(col) = batch.column_by_name("table_name") {
            seen_table_name = Some(
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("table_name is Utf8")
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
    }
    assert_eq!(seen_table_name, Some("ns.opted".to_string()));
    assert_eq!(seen_status, Some("advisory".to_string()));

    // --- Audit: exactly one Maintenance event, for the opted table ---
    audit.flush();
    let audit_content = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let audit_lines: Vec<serde_json::Value> = audit_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid JSON audit line"))
        .collect();
    let maintenance_events: Vec<&serde_json::Value> = audit_lines
        .iter()
        .filter(|e| e["kind"].as_str() == Some("maintenance"))
        .collect();
    assert_eq!(
        maintenance_events.len(),
        1,
        "expected exactly one Maintenance audit event, got: {audit_lines:?}"
    );
    let event = maintenance_events[0];
    assert_eq!(event["actor"]["username"].as_str(), Some("svc-sqe-maintenance"));
    assert_eq!(event["resources"][0]["name"].as_str(), Some("opted"));
}

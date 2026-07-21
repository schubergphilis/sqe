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
    DistributionMode, MaintenanceCompactionConfig, MaintenanceConfig,
    MaintenanceDistributionConfig, MaintenanceMode, MaintenancePrincipalConfig,
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

/// Like [`create_table`], but always opted in AND carrying extra table
/// properties (e.g. a `sqe.maintenance.compaction.*` per-table override),
/// for the loosening-override test below.
async fn create_table_with_props(
    catalog: &Arc<dyn Catalog>,
    ns: &str,
    name: &str,
    extra_props: HashMap<String, String>,
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

    let mut properties = extra_props;
    properties.insert("sqe.maintenance.enabled".to_string(), "true".to_string());

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

/// Minimal `SqeConfig` sufficient to build a `MaintenanceHandler` in tests.
/// `MaintenanceHandler::rewrite_data_files`/`rewrite_data_files_once` only
/// read `config.catalog.parquet_compression` from this (see
/// `maintenance.rs`'s module docs on the catalog-injection refactor); every
/// other field is present only because `SqeConfig`'s TOML deserialization
/// requires the `[coordinator]`/`[auth]`/`[catalog]` sections to exist.
fn minimal_sqe_config() -> sqe_core::SqeConfig {
    toml::from_str(
        r#"
[coordinator]
[auth]
[catalog]
catalog_url = ""
"#,
    )
    .expect("minimal SqeConfig parses")
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
            // jitter_secs = 0 means zero jitter *delay* (see `table_due`);
            // it does NOT bypass the schedule check. Pair it with a
            // permissive every-minute schedule so the opted-in table is
            // due at the real wall-clock time the test happens to run,
            // regardless of what that time is.
            jitter_secs: 0,
            schedule: "* * * * *".to_string(),
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

    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));
    let scheduler = MaintenanceScheduler::new(
        cfg,
        principal,
        metrics.clone(),
        Some(audit.clone()),
        catalog_factory,
        handler,
    );

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

// ---------------------------------------------------------------------------
// Phase 4b: active-mode compaction.
// ---------------------------------------------------------------------------

/// Build an `active`-mode variant of [`maintenance_config`], for the Phase
/// 4b tests below.
fn active_maintenance_config(idp: &MockServer, min_input_files: usize) -> MaintenanceConfig {
    MaintenanceConfig {
        mode: MaintenanceMode::Active,
        ..maintenance_config(idp, min_input_files)
    }
}

/// Count the LIVE data files a fresh table scan actually plans for `ident`.
/// Distinct data file paths, not raw task count, in case a bare
/// `iceberg-rust` scan ever splits one file into multiple tasks.
async fn live_data_file_count(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> usize {
    let table = catalog.load_table(ident).await.expect("load table for file count");
    let tasks: Vec<_> = table
        .scan()
        .build()
        .expect("build scan")
        .plan_files()
        .await
        .expect("plan_files")
        .try_collect()
        .await
        .expect("collect scan tasks");
    let mut paths: std::collections::HashSet<String> = std::collections::HashSet::new();
    for t in tasks {
        paths.insert(t.data_file_path.clone());
    }
    paths.len()
}

/// Scan every live `id` value out of `ident`, sorted, for row-set
/// preservation assertions (same values survive compaction, not merely the
/// same count).
async fn scan_id_values(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<i64> {
    let table = catalog.load_table(ident).await.expect("load table for row scan");
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect batches");
    let mut values = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        for i in 0..col.len() {
            values.push(col.value(i));
        }
    }
    values.sort_unstable();
    values
}

/// One row of `sqe_system.maintenance_log`, decoded for assertions.
#[derive(Debug)]
struct LogRowView {
    table_name: String,
    status: String,
    files_in: i64,
    files_out: i64,
    bytes_in: i64,
    bytes_out: i64,
    rows_removed: i64,
    snapshot_id: Option<i64>,
}

/// Scan `sqe_system.maintenance_log` and decode every JOB row (advisory
/// report, active success/failed/skipped). Excludes lease bookkeeping rows
/// (`trigger = "lease"`, `status` `"claimed"`/`"released"` -- see
/// `crate::maintenance_lease`'s module docs): those share this table but are
/// not job rows, and every existing caller of this helper predates the
/// Phase 4d lease and asserts about job rows specifically (e.g. "exactly one
/// row for this table"), so folding lease bookkeeping into the same count
/// would be testing the wrong thing, not a stricter version of the same
/// thing.
async fn scan_log_rows(catalog: &Arc<dyn Catalog>) -> Vec<LogRowView> {
    use arrow_array::{Array, StringArray};

    let log_ident = TableIdent::new(NamespaceIdent::new("sqe_system".to_string()), "maintenance_log".to_string());
    let log_table = catalog.load_table(&log_ident).await.expect("reload log table");
    let batches: Vec<RecordBatch> = log_table
        .scan()
        .build()
        .expect("build log scan")
        .to_arrow()
        .await
        .expect("scan log to arrow")
        .try_collect()
        .await
        .expect("collect log batches");

    let mut rows = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let table_name_col = batch
            .column_by_name("table_name")
            .expect("table_name col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table_name is Utf8");
        let trigger_col = batch
            .column_by_name("trigger")
            .expect("trigger col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trigger is Utf8");
        let status_col = batch
            .column_by_name("status")
            .expect("status col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status is Utf8");
        let files_in_col = batch
            .column_by_name("files_in")
            .expect("files_in col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("files_in is Int64");
        let files_out_col = batch
            .column_by_name("files_out")
            .expect("files_out col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("files_out is Int64");
        let bytes_in_col = batch
            .column_by_name("bytes_in")
            .expect("bytes_in col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bytes_in is Int64");
        let bytes_out_col = batch
            .column_by_name("bytes_out")
            .expect("bytes_out col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bytes_out is Int64");
        let rows_removed_col = batch
            .column_by_name("rows_removed")
            .expect("rows_removed col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("rows_removed is Int64");
        let snapshot_col = batch
            .column_by_name("snapshot_id")
            .expect("snapshot_id col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is Int64");

        for i in 0..batch.num_rows() {
            if trigger_col.value(i) == "lease" {
                // Lease bookkeeping row (Phase 4d), not a job row: see this
                // function's doc comment.
                continue;
            }
            rows.push(LogRowView {
                table_name: table_name_col.value(i).to_string(),
                status: status_col.value(i).to_string(),
                files_in: files_in_col.value(i),
                files_out: files_out_col.value(i),
                bytes_in: bytes_in_col.value(i),
                bytes_out: bytes_out_col.value(i),
                rows_removed: rows_removed_col.value(i),
                snapshot_id: if snapshot_col.is_null(i) {
                    None
                } else {
                    Some(snapshot_col.value(i))
                },
            });
        }
    }
    rows
}

/// An `active`-mode tick over one opted-in table with many small files
/// actually compacts: the live file count drops, the row SET (not just
/// count) survives, a `maintenance_log` row with `status = "success"` and
/// correct `files_in`/`files_out` is written, the new snapshot's summary
/// carries all three `sqe.maintenance.*` job-identity keys, and a
/// non-opted table sitting in the same catalog is left completely alone
/// (unchanged snapshot id).
#[tokio::test]
async fn active_tick_compacts_opted_table_and_leaves_others_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    // 5 small files, min_input_files=2 below, so the group is eligible.
    let opted_ident = create_table(&catalog, "ns", "active_opted", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let rows_before = scan_id_values(&catalog, &opted_ident).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5, "seed helper must produce exactly 5 live files");

    let plain_ident = create_table(&catalog, "ns", "active_plain", false).await;
    seed_small_files(&catalog, &plain_ident, 1).await;
    let plain_snapshot_before = catalog
        .load_table(&plain_ident)
        .await
        .expect("reload plain table")
        .metadata()
        .current_snapshot_id();

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    let cfg = active_maintenance_config(&idp, 2);
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
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(
        cfg,
        principal,
        metrics.clone(),
        Some(audit.clone()),
        catalog_factory,
        handler,
    );

    scheduler.advisory_tick().await.expect("active tick succeeds");

    // --- The opted table actually compacted: fewer live files, same rows ---
    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert!(
        files_after < files_before,
        "expected live file count to drop below {files_before}, got {files_after}"
    );
    let rows_after = scan_id_values(&catalog, &opted_ident).await;
    assert_eq!(rows_after, rows_before, "row SET must be preserved by compaction");

    // --- The new snapshot's summary carries the job-identity stamp ---
    let opted_after = catalog.load_table(&opted_ident).await.expect("reload opted table");
    let snapshot = opted_after
        .metadata()
        .current_snapshot()
        .expect("opted table has a current snapshot after compaction");
    let props = &snapshot.summary().additional_properties;
    assert!(
        props.contains_key("sqe.maintenance.job-id"),
        "snapshot summary must carry sqe.maintenance.job-id, got {props:?}"
    );
    assert_eq!(
        props.get("sqe.maintenance.principal"),
        Some(&"svc-sqe-maintenance".to_string())
    );
    assert_eq!(props.get("sqe.maintenance.trigger"), Some(&"scheduled".to_string()));

    // --- The non-opted table is completely untouched ---
    let plain_snapshot_after = catalog
        .load_table(&plain_ident)
        .await
        .expect("reload plain table")
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        plain_snapshot_before, plain_snapshot_after,
        "active mode must not touch a non-opted table"
    );

    // --- maintenance_log: exactly one row, status="success", correct counts ---
    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows.iter().filter(|r| r.table_name == "ns.active_opted").collect();
    assert_eq!(opted_rows.len(), 1, "expected exactly one job row for the opted table");
    let row = opted_rows[0];
    assert_eq!(row.status, "success");
    assert_eq!(row.files_in, 5);
    assert!(
        row.files_out > 0 && row.files_out < 5,
        "expected 0 < files_out < 5, got {}",
        row.files_out
    );
    assert!(row.bytes_in > 0);
    assert!(row.bytes_out > 0);
    assert_eq!(row.rows_removed, 0, "no deletes applied in this fixture");
    assert!(row.snapshot_id.is_some());
    assert!(rows.iter().all(|r| r.table_name != "ns.active_plain"), "the non-opted table must never get a job row");

    // --- Metrics: one success sample, bytes_rewritten_total > 0 ---
    let job_success = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_job_total")
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| m.get_label().iter().any(|l| l.name() == "status" && l.value() == "success"))
                .map(|m| m.get_counter().value())
        });
    assert_eq!(job_success, Some(1.0));

    let bytes_rewritten = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_bytes_rewritten_total")
        .and_then(|f| f.get_metric().first().map(|m| m.get_counter().value()));
    assert!(
        bytes_rewritten.unwrap_or(0.0) > 0.0,
        "expected sqe_maintenance_bytes_rewritten_total > 0, got {bytes_rewritten:?}"
    );

    // --- Audit: one Maintenance event carrying the "committed" query text ---
    audit.flush();
    let audit_content = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let committed_events = audit_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|l| l.contains("\"kind\":\"maintenance\"") && l.contains("committed"))
        .count();
    assert_eq!(committed_events, 1, "expected exactly one active-mode commit audit event");
}

/// A per-table `sqe.maintenance.compaction.min-input-files` override that
/// LOOSENS the global config must win at BOTH the eligibility gate and the
/// rewrite itself. Global `min_input_files = 10` alone would skip a 5-file
/// table (`analyze_table_health` under the global config reports
/// `eligible_groups == 0`); the table's own `min-input-files => "3"`
/// override makes it eligible, and the active tick must actually compact it,
/// not silently skip it because some earlier step used the unresolved
/// global config.
#[tokio::test]
async fn active_tick_honors_per_table_override_that_global_config_would_have_skipped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let mut extra_props = HashMap::new();
    extra_props.insert(
        "sqe.maintenance.compaction.min-input-files".to_string(),
        "3".to_string(),
    );
    let opted_ident = create_table_with_props(&catalog, "ns", "override_opted", extra_props).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5);

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    // Global min_input_files = 10: without the per-table override, a 5-file
    // group never becomes eligible and this table would be skipped.
    let cfg = active_maintenance_config(&idp, 10);
    let principal = Arc::new(
        MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
            .expect("build principal"),
    );
    let metrics = Arc::new(MetricsRegistry::new().expect("metrics registry builds"));

    let injected_catalog = catalog.clone();
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory =
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(cfg, principal, metrics.clone(), None, catalog_factory, handler);

    scheduler.advisory_tick().await.expect("active tick succeeds");

    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert!(
        files_after < files_before,
        "the per-table min-input-files override must make this table eligible \
         despite the global config alone requiring 10 files; expected fewer \
         than {files_before} live files, got {files_after}"
    );

    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows.iter().filter(|r| r.table_name == "ns.override_opted").collect();
    assert_eq!(opted_rows.len(), 1);
    assert_eq!(
        opted_rows[0].status, "success",
        "expected the override to make this table compact, not skip, under a global \
         config that alone would have skipped it"
    );
}

/// An opted-in table with NO eligible compaction debt (one file, well under
/// `min_input_files`, no delete files) must be recorded as `skipped`: a
/// `maintenance_log` row with `status="skipped"`, a
/// `sqe_maintenance_job_total{status="skipped"}` sample, and (4b review, Fix
/// 1) an `AuditKind::Maintenance` audit event carrying the skip reason --
/// a skip is a real tick decision, not a silent no-op.
#[tokio::test]
async fn active_tick_skips_table_with_no_eligible_debt_and_audits_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    // 1 file, min_input_files=2 below: no group ever meets the threshold,
    // and there are no delete files, so `has_eligible_work` is false.
    let opted_ident = create_table(&catalog, "ns", "active_no_debt", true).await;
    seed_small_files(&catalog, &opted_ident, 1).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 1);
    let opted_snapshot_before = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    let cfg = active_maintenance_config(&idp, 2);
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
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(
        cfg,
        principal,
        metrics.clone(),
        Some(audit.clone()),
        catalog_factory,
        handler,
    );

    scheduler.advisory_tick().await.expect("active tick succeeds");

    // --- Never touched: same file count, same snapshot ---
    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_after, files_before, "a skipped table must not be rewritten");
    let opted_snapshot_after = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        opted_snapshot_before, opted_snapshot_after,
        "a skipped table must not get a new snapshot"
    );

    // --- maintenance_log: one row, status="skipped", reason recorded ---
    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows.iter().filter(|r| r.table_name == "ns.active_no_debt").collect();
    assert_eq!(opted_rows.len(), 1, "expected exactly one job row for the opted table");
    assert_eq!(opted_rows[0].status, "skipped");

    // --- Metrics: one skipped sample ---
    let job_skipped = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_job_total")
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| m.get_label().iter().any(|l| l.name() == "status" && l.value() == "skipped"))
                .map(|m| m.get_counter().value())
        });
    assert_eq!(job_skipped, Some(1.0));

    // --- Audit: one Maintenance event, outcome success, reason in the text ---
    audit.flush();
    let audit_content = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let skipped_events: Vec<&str> = audit_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|l| l.contains("\"kind\":\"maintenance\"") && l.contains("skipped"))
        .collect();
    assert_eq!(skipped_events.len(), 1, "expected exactly one skip audit event, got: {skipped_events:?}");
    assert!(
        skipped_events[0].contains("\"status\":\"success\""),
        "a skip is a correct no-op decision, not a failure: {}",
        skipped_events[0]
    );
    assert!(
        skipped_events[0].contains("no eligible compaction debt"),
        "skip reason must be actionable in the audit line: {}",
        skipped_events[0]
    );
}

// ---------------------------------------------------------------------------
// Phase 4c Task 5: `distribution.mode` routing.
// ---------------------------------------------------------------------------

/// An opted-in table WITH eligible compaction debt, under
/// `distribution.mode = "require"`, when the handler has no worker registry
/// attached at all (so `healthy_worker_count()` is always `0`, `< any
/// min_workers >= 1`): the active tick must skip loudly rather than fall
/// back to a coordinator-local rewrite. The table is left completely
/// untouched (same file count, same snapshot id), a `skipped`
/// `maintenance_log` row is written, the dedicated
/// `sqe_maintenance_skipped_total{reason="insufficient_workers"}` metric
/// fires, and an `AuditKind::Maintenance` skip event names the cause.
#[tokio::test]
async fn active_tick_require_mode_skips_loudly_when_no_workers_are_healthy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    // 5 small files, min_input_files=2 below: this table clears the
    // eligibility gate, so the ONLY reason it can end up skipped is the
    // distribution-mode routing this test exercises.
    let opted_ident = create_table(&catalog, "ns", "require_no_workers", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5, "seed helper must produce exactly 5 live files");
    let opted_snapshot_before = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    let mut cfg = active_maintenance_config(&idp, 2);
    cfg.distribution = MaintenanceDistributionConfig {
        mode: DistributionMode::Require,
        min_workers: 1,
        ..Default::default()
    };
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
    // Deliberately NOT `.with_worker_registry(...)`: this handler has no
    // registry at all, so `healthy_worker_count()` is `0` and `require`
    // mode with `min_workers = 1` must skip, never silently compact locally.
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(
        cfg,
        principal,
        metrics.clone(),
        Some(audit.clone()),
        catalog_factory,
        handler,
    );

    scheduler.advisory_tick().await.expect("active tick succeeds");

    // --- Never touched: same file count, same snapshot ---
    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(
        files_after, files_before,
        "distribution.mode=require below the healthy-worker floor must never fall back to a local rewrite"
    );
    let opted_snapshot_after = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        opted_snapshot_before, opted_snapshot_after,
        "a skipped table must not get a new snapshot"
    );

    // --- maintenance_log: one row, status="skipped" ---
    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> =
        rows.iter().filter(|r| r.table_name == "ns.require_no_workers").collect();
    assert_eq!(opted_rows.len(), 1, "expected exactly one job row for the opted table");
    assert_eq!(opted_rows[0].status, "skipped");

    // --- Metrics: the dedicated insufficient-workers counter fires ---
    let skipped_insufficient = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_skipped_total")
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "reason" && l.value() == "insufficient_workers")
                })
                .map(|m| m.get_counter().value())
        });
    assert_eq!(
        skipped_insufficient,
        Some(1.0),
        "expected exactly one sqe_maintenance_skipped_total{{reason=\"insufficient_workers\"}} sample"
    );

    // --- The generic job-total skip counter also fires (same shape as any other skip) ---
    let job_skipped = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_job_total")
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| m.get_label().iter().any(|l| l.name() == "status" && l.value() == "skipped"))
                .map(|m| m.get_counter().value())
        });
    assert_eq!(job_skipped, Some(1.0));

    // --- Audit: one Maintenance skip event, naming the cause ---
    audit.flush();
    let audit_content = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let skipped_events: Vec<&str> = audit_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|l| l.contains("\"kind\":\"maintenance\"") && l.contains("skipped"))
        .collect();
    assert_eq!(skipped_events.len(), 1, "expected exactly one skip audit event, got: {skipped_events:?}");
    assert!(
        skipped_events[0].contains("\"status\":\"success\""),
        "a skip is a correct no-op decision, not a failure: {}",
        skipped_events[0]
    );
    assert!(
        skipped_events[0].contains("insufficient_workers"),
        "skip reason must name the cause in the audit line: {}",
        skipped_events[0]
    );
}

/// A commit-catalog build failure (the `catalog_factory` call inside
/// `active_one_table`, AFTER eligibility passes and the session is minted)
/// must be recorded as `failed`: a `maintenance_log` row with
/// `status="failed"`, a `sqe_maintenance_job_total{status="failed"}` sample,
/// and (4b review, Fix 1) an `AuditKind::Maintenance` audit event with
/// `Outcome::Failure` carrying the error message.
///
/// `catalog_factory` is rigged to succeed exactly once (the tick-level
/// discovery call in `advisory_tick`) and fail on every later call (the
/// per-table commit-catalog build in `active_one_table`), so the table
/// reaches the real compaction attempt and fails deep in the pipeline
/// rather than being skipped or never discovered.
#[tokio::test]
async fn active_tick_records_failed_job_and_audits_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let opted_ident = create_table(&catalog, "ns", "active_fails", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5);

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    let cfg = active_maintenance_config(&idp, 2);
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
    let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory = {
        let call_count = call_count.clone();
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            let call_count = call_count.clone();
            Box::pin(async move {
                let n = call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    // Tick-level discovery call: must succeed so the table
                    // is actually found and reaches the eligibility check.
                    Ok(catalog)
                } else {
                    // Every later call (the per-table commit-catalog build):
                    // fail, forcing `active_one_table`'s `record_failed_job`
                    // path.
                    Err(sqe_core::SqeError::Catalog("injected commit-catalog failure".to_string()))
                }
            })
        })
    };
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(
        cfg,
        principal,
        metrics.clone(),
        Some(audit.clone()),
        catalog_factory,
        handler,
    );

    scheduler.advisory_tick().await.expect("active tick succeeds despite the per-table failure");

    // --- Never touched: same file count ---
    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_after, files_before, "a failed job must not have rewritten anything");

    // --- maintenance_log: one row, status="failed", error recorded ---
    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows.iter().filter(|r| r.table_name == "ns.active_fails").collect();
    assert_eq!(opted_rows.len(), 1, "expected exactly one job row for the opted table");
    assert_eq!(opted_rows[0].status, "failed");

    // --- Metrics: one failed sample ---
    let job_failed = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_job_total")
        .and_then(|f| {
            f.get_metric()
                .iter()
                .find(|m| m.get_label().iter().any(|l| l.name() == "status" && l.value() == "failed"))
                .map(|m| m.get_counter().value())
        });
    assert_eq!(job_failed, Some(1.0));

    // --- Audit: one Maintenance event, Outcome::Failure, error in the text ---
    audit.flush();
    let audit_content = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let failed_events: Vec<&str> = audit_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter(|l| l.contains("\"kind\":\"maintenance\"") && l.contains("\"status\":\"failure\""))
        .collect();
    assert_eq!(failed_events.len(), 1, "expected exactly one failure audit event, got: {failed_events:?}");
    assert!(
        failed_events[0].contains("injected commit-catalog failure"),
        "failure reason must be actionable in the audit line: {}",
        failed_events[0]
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(failed_events[0])
            .expect("valid JSON audit line")["resources"][0]["name"]
            .as_str(),
        Some("active_fails"),
        "the failure event must identify the table it failed on"
    );
}

/// The exact same many-small-files fixture as the active-mode test above,
/// but run through an `advisory`-mode tick: nothing must be mutated. This
/// is the direct A/B companion the Phase 4b brief calls for -- same tables,
/// same file counts, only `mode` differs.
#[tokio::test]
async fn advisory_tick_on_active_fixture_still_mutates_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let opted_ident = create_table(&catalog, "ns", "advisory_on_active_fixture", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    let opted_snapshot_before = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    // Advisory, not active -- everything else (min_input_files=2, so the
    // 5-file group WOULD be eligible under active mode) matches the
    // active-mode test above.
    let cfg = maintenance_config(&idp, 2);
    let principal = Arc::new(
        MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
            .expect("build principal"),
    );
    let metrics = Arc::new(MetricsRegistry::new().expect("metrics registry builds"));

    let injected_catalog = catalog.clone();
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory =
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(cfg, principal, metrics.clone(), None, catalog_factory, handler);

    scheduler.advisory_tick().await.expect("advisory tick succeeds");

    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_after, files_before, "advisory mode must not change the live file count");

    let opted_snapshot_after = catalog
        .load_table(&opted_ident)
        .await
        .expect("reload opted table")
        .metadata()
        .current_snapshot_id();
    assert_eq!(
        opted_snapshot_before, opted_snapshot_after,
        "advisory mode must not commit a new snapshot even though the table has eligible debt"
    );

    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows
        .iter()
        .filter(|r| r.table_name == "ns.advisory_on_active_fixture")
        .collect();
    assert_eq!(opted_rows.len(), 1, "expected exactly one advisory row");
    assert_eq!(opted_rows[0].status, "advisory", "advisory mode must never write a success/failed/skipped job row");

    // No active-mode job metric was ever touched.
    let job_family = metrics
        .registry
        .gather()
        .into_iter()
        .find(|f| f.name() == "sqe_maintenance_job_total");
    assert!(
        job_family.is_none() || job_family.unwrap().get_metric().is_empty(),
        "advisory mode must never increment sqe_maintenance_job_total"
    );
}

// ---------------------------------------------------------------------------
// Phase 4d Task 3: catalog HA lease wired into the active scheduler.
// ---------------------------------------------------------------------------

/// Count `sqe_system.maintenance_log` rows that are lease bookkeeping
/// (`trigger = "lease"`), regardless of table. Used to assert `lease =
/// "none"` produces zero lease traffic (Phase 4c-identical behavior), and
/// (for the contention test) that the lease rows a `catalog`-mode tick
/// writes exist at all.
async fn count_lease_rows(catalog: &Arc<dyn Catalog>) -> usize {
    use arrow_array::{Array, StringArray};

    let log_ident = TableIdent::new(NamespaceIdent::new("sqe_system".to_string()), "maintenance_log".to_string());
    let log_table = catalog.load_table(&log_ident).await.expect("reload log table");
    let batches: Vec<RecordBatch> = log_table
        .scan()
        .build()
        .expect("build log scan")
        .to_arrow()
        .await
        .expect("scan log to arrow")
        .try_collect()
        .await
        .expect("collect log batches");

    let mut count = 0;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let trigger_col = batch
            .column_by_name("trigger")
            .expect("trigger col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trigger is Utf8");
        for i in 0..batch.num_rows() {
            if trigger_col.value(i) == "lease" {
                count += 1;
            }
        }
    }
    count
}

/// Build a `MaintenanceSchedulerConfig`-carrying `active`-mode config with an
/// explicit `lease` mode, otherwise identical to [`active_maintenance_config`].
fn active_maintenance_config_with_lease(
    idp: &MockServer,
    min_input_files: usize,
    lease: sqe_core::config::LeaseMode,
    lease_ttl_secs: u64,
) -> MaintenanceConfig {
    let mut cfg = active_maintenance_config(idp, min_input_files);
    cfg.scheduler.lease = lease;
    cfg.scheduler.lease_ttl_secs = lease_ttl_secs;
    cfg.scheduler.single_scheduler_acknowledged = true;
    cfg
}

/// `lease = "none"` must be byte-identical to pre-4d (Phase 4c) behavior:
/// the table still compacts, but NOT ONE row of lease bookkeeping is ever
/// written to `sqe_system.maintenance_log` -- no `try_acquire`/`release`
/// catalog traffic at all.
#[tokio::test]
async fn active_tick_lease_none_compacts_with_zero_lease_traffic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let opted_ident = create_table(&catalog, "ns", "lease_none_opted", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5);

    create_maintenance_log_table(&catalog).await;

    let idp = mock_idp().await;
    let cfg = active_maintenance_config_with_lease(&idp, 2, sqe_core::config::LeaseMode::None, 60);
    let principal = Arc::new(
        MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
            .expect("build principal"),
    );
    let metrics = Arc::new(MetricsRegistry::new().expect("metrics registry builds"));

    let injected_catalog = catalog.clone();
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory =
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler = MaintenanceScheduler::new(cfg, principal, metrics.clone(), None, catalog_factory, handler);
    scheduler.advisory_tick().await.expect("active tick succeeds");

    let files_after = live_data_file_count(&catalog, &opted_ident).await;
    assert!(files_after < files_before, "lease=none must still compact exactly like Phase 4c");

    let rows = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> = rows.iter().filter(|r| r.table_name == "ns.lease_none_opted").collect();
    assert_eq!(opted_rows.len(), 1);
    assert_eq!(opted_rows[0].status, "success");

    assert_eq!(
        count_lease_rows(&catalog).await,
        0,
        "lease = \"none\" must never write a single lease bookkeeping row"
    );
    assert_eq!(
        metrics.maintenance_lease_skipped_total.get(),
        0,
        "lease = \"none\" must never increment the lease-skip counter either"
    );
}

/// The Step 1 TDD test: with `lease = "catalog"`, a table already claimed by
/// another coordinator (simulated here by directly holding the lease as
/// holder `"coordinator-a"`, representing a coordinator mid-compaction) is
/// NOT double-compacted by a second coordinator (`scheduler_b`, a real
/// `MaintenanceScheduler` with a DIFFERENT holder_id) ticking in the same
/// window: `scheduler_b` observes the held lease and skips, writing no job
/// row. After `coordinator-a` releases, a LATER tick from `scheduler_b`
/// acquires the lease itself and actually compacts.
#[tokio::test]
async fn active_tick_catalog_lease_prevents_concurrent_compaction_then_recovers_after_release() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;

    let opted_ident = create_table(&catalog, "ns", "lease_contended", true).await;
    seed_small_files(&catalog, &opted_ident, 5).await;
    let files_before = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(files_before, 5);

    create_maintenance_log_table(&catalog).await;

    let job_key = opted_ident.to_string();
    let state_table = "sqe_system.maintenance_log";
    let ttl_secs = 60;

    // `coordinator-a` claims the lease first -- as if it is already mid-tick
    // (or mid-compaction) for this exact table when `coordinator-b`'s
    // window starts. This is the "one window, two coordinators" scenario
    // the brief describes, using the real `maintenance_lease` primitive
    // (Task 2) directly for the "other" coordinator's side, and a real
    // `MaintenanceScheduler` for `coordinator-b`'s side.
    let now_ms = chrono::Utc::now().timestamp_millis();
    let handle_a = sqe_coordinator::maintenance_lease::try_acquire(
        &catalog,
        state_table,
        &job_key,
        "coordinator-a",
        ttl_secs,
        now_ms,
    )
    .await
    .expect("coordinator-a acquires the lease")
    .expect("no one else holds it yet, so this must be Some");

    let idp = mock_idp().await;
    let cfg = active_maintenance_config_with_lease(&idp, 2, sqe_core::config::LeaseMode::Catalog, ttl_secs);
    let principal = Arc::new(
        MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
            .expect("build principal"),
    );
    let metrics = Arc::new(MetricsRegistry::new().expect("metrics registry builds"));

    let injected_catalog = catalog.clone();
    let catalog_factory: sqe_coordinator::maintenance_scheduler::CatalogFactory =
        Arc::new(move |_session: &sqe_core::Session| {
            let catalog = injected_catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });
    let handler = Arc::new(sqe_coordinator::maintenance::MaintenanceHandler::new(minimal_sqe_config()));

    let scheduler_b = MaintenanceScheduler::new(cfg, principal, metrics.clone(), None, catalog_factory, handler)
        .with_holder_id("coordinator-b");

    // --- Window 1: coordinator-a still holds the lease; coordinator-b skips ---
    scheduler_b.advisory_tick().await.expect("scheduler_b tick 1 succeeds (a routine skip, not an error)");

    let files_after_tick1 = live_data_file_count(&catalog, &opted_ident).await;
    assert_eq!(
        files_after_tick1, files_before,
        "coordinator-b must NOT have compacted while coordinator-a holds the lease"
    );
    let rows_after_tick1 = scan_log_rows(&catalog).await;
    assert!(
        rows_after_tick1.iter().all(|r| r.table_name != "ns.lease_contended"),
        "a lease-skip must not write a maintenance_log job row: {rows_after_tick1:?}",
    );
    assert_eq!(
        metrics.maintenance_lease_skipped_total.get(),
        1,
        "coordinator-b's lease-skip must increment the dedicated counter"
    );

    // --- coordinator-a finishes and releases ---
    let release_now_ms = chrono::Utc::now().timestamp_millis();
    sqe_coordinator::maintenance_lease::release(handle_a, &catalog, state_table, release_now_ms)
        .await
        .expect("coordinator-a releases cleanly");

    // --- Window 2 (a later tick): coordinator-b now acquires and compacts ---
    scheduler_b.advisory_tick().await.expect("scheduler_b tick 2 succeeds");

    let files_after_tick2 = live_data_file_count(&catalog, &opted_ident).await;
    assert!(
        files_after_tick2 < files_before,
        "after coordinator-a released, coordinator-b must acquire the lease and actually compact"
    );
    let rows_after_tick2 = scan_log_rows(&catalog).await;
    let opted_rows: Vec<&LogRowView> =
        rows_after_tick2.iter().filter(|r| r.table_name == "ns.lease_contended").collect();
    assert_eq!(opted_rows.len(), 1, "exactly one job row: coordinator-b's successful compaction");
    assert_eq!(opted_rows[0].status, "success");

    // `crate::maintenance_lease`'s CAS design keeps exactly one LIVE lease
    // row per job_key at a time (every claim/release deletes the prior live
    // row and replaces it -- see that module's docs), so a table scan here
    // sees only the latest state, not the whole history. It is non-zero
    // (unlike the `lease = "none"` test above), confirming `catalog` mode
    // really did write lease bookkeeping.
    assert_eq!(
        count_lease_rows(&catalog).await,
        1,
        "expected exactly one LIVE lease row (coordinator-b's final release tombstone)"
    );
}

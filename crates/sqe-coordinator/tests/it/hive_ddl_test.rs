//! Server-mode Hive DDL and statement dispatch (Task 10b).
//!
//! Cycle-2 note: `MSCK REPAIR TABLE`, `SHOW PARTITIONS`, and
//! `ALTER TABLE ... ADD/DROP PARTITION` used to be listed here as
//! generic-rejection rows. Cycle 2 implements them; their dispatch tests
//! live in `hive_partition_ddl_test.rs`. `ANALYZE TABLE` is implemented
//! there too.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use datafusion::common::TableReference;
use sqe_catalog::hive::manifest::ManifestLimits;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_catalog::TableMetadataCache;
use sqe_coordinator::hive_ddl_handler::TableKind;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::config::{PolicyEngine, TvfCaller};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_metrics::audit::AuditLogger;
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{PassthroughEnforcer, PolicyStore, ResolvedPolicy, TablePrivilege};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

fn minimal_config(catalog_url: &str) -> SqeConfig {
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n"
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("minimal config parses");
    config.storage.tvf.allow_local_paths = true;
    // Lets the audit-event test's `LOCATION 's3://raw/orders/'` (unreachable,
    // by design: CREATE EXTERNAL TABLE only writes a manifest, it never
    // scans) past the object-store allowlist, the same way an operator
    // would grant a service-account role rather than an individual prefix.
    config.storage.tvf.object_store_admin_roles = vec!["service_admin".to_string()];
    config
}

/// `SESSION_CONTEXT_CACHE` (`session_context.rs`) is a process-wide cache
/// keyed on `username:token_hash:catalog:schema`. Every test in this file
/// builds its own `QueryHandler` with its own Hive root and its own
/// `MockServer`, but they all run in the SAME test binary process; a fixed
/// username/token would let two tests running in parallel collide on the
/// same cache entry and hand one test another's (possibly already-dropped)
/// `SessionContext`. A fresh UUID per session makes every test's cache key
/// unique regardless of `cargo test`'s thread count.
fn session_with_roles(roles: Vec<String>) -> Session {
    Session::new(
        format!("tester-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles,
    )
}

fn dummy_session() -> Session {
    session_with_roles(vec!["service_admin".to_string()])
}

/// A stand-in Polaris that answers `/v1/config` and `/v1/namespaces` so the
/// default Iceberg catalog `flattened_catalogs()` always carries can be
/// enumerated by `information_schema` queries without a real network
/// dependency. Mirrors `audit_e2e_test.rs`'s `make_fixture_with_url` mocks.
async fn mock_polaris() -> wiremock::MockServer {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/config"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/namespaces"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

/// The end-to-end test harness: a real `QueryHandler` with a Hive root
/// attached, a session, and an `AuditLogger` writing to a temp file so
/// `audit_events_for_test` can read back what the handler actually wrote.
struct Harness {
    handler: QueryHandler,
    session: Session,
    audit: Arc<AuditLogger>,
    log_path: PathBuf,
    _dirs: Vec<TempDir>,
    /// Kept alive for the harness's lifetime: `DESCRIBE`/`information_schema`
    /// queries enumerate every registered catalog, including the default
    /// Iceberg one this config always carries (`flattened_catalogs` never
    /// returns empty), so it needs to answer `/v1/config` and
    /// `/v1/namespaces` rather than fail on an unreachable real URL.
    _mock: wiremock::MockServer,
}

impl Harness {
    /// The single Hive/Iceberg branch point, delegated straight through to
    /// `QueryHandler::resolve_table_kind` so a test failure here can only
    /// mean the coordinator's own answer changed, not a second detector
    /// this test file grew on its own.
    fn resolve_table_kind(&self, table_ref: &TableReference) -> TableKind {
        self.handler.resolve_table_kind(table_ref)
    }

    async fn execute_for_test(&self, sql: &str) -> sqe_core::Result<Vec<RecordBatch>> {
        self.handler.execute(&self.session, sql, None).await
    }

    /// `SELECT count(*) AS n FROM ...`-shaped query, returning the scalar
    /// count rather than the batch row count (which would always be 1: one
    /// row holding the aggregate).
    async fn query_rows_for_test(&self, sql: &str) -> sqe_core::Result<i64> {
        let batches = self.execute_for_test(sql).await?;
        let batch = batches
            .first()
            .ok_or_else(|| sqe_core::SqeError::Execution("query returned no batches".into()))?;
        Ok(batch.column(0).as_primitive::<Int64Type>().value(0))
    }

    /// Every audit line written so far, flushed first, each annotated with
    /// its resources' fully-qualified names (via `Resource::fqn()`, not a
    /// re-implementation of the dot-joining) so a test can assert on the
    /// dotted `catalog.database.table` form without caring how the JSON
    /// happens to split that identity across fields.
    fn audit_events_for_test(&self) -> Vec<String> {
        self.audit.flush();
        let content = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        content
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| {
                let v: serde_json::Value =
                    serde_json::from_str(line).expect("audit line is valid JSON");
                let resources: Vec<sqe_metrics::audit::Resource> = v
                    .get("resources")
                    .cloned()
                    .and_then(|r| serde_json::from_value(r).ok())
                    .unwrap_or_default();
                let fqns: Vec<String> = resources.iter().map(|r| r.fqn()).collect();
                format!("{line} resources_fqn=[{}]", fqns.join(","))
            })
            .collect()
    }
}

/// A tempdir holding a two-partition CSV tree (three rows total), the same
/// fixture shape `embedded.rs`'s `client_with_hive_root` uses, so the same
/// `CREATE EXTERNAL TABLE ... LOCATION` DDL is valid against it.
fn write_csv_fixture(dir: &std::path::Path) {
    for (dt, rows) in [
        ("2026-01-01", "1,10.5\n2,20.0\n"),
        ("2026-01-02", "3,30.5\n"),
    ] {
        let partition = dir.join(format!("dt={dt}"));
        std::fs::create_dir_all(&partition).expect("mkdir partition");
        std::fs::write(partition.join("a.csv"), format!("id,amount\n{rows}"))
            .expect("write csv");
    }
}

fn create_orders_ddl(data_dir: &std::path::Path, catalog: &str) -> String {
    format!(
        "CREATE EXTERNAL TABLE {catalog}.sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
        data_dir.display()
    )
}

/// Build a `QueryHandler` with a Hive root named `catalog` attached (via a
/// real `ATTACH ... TYPE hive_external` statement, exercising the same
/// dispatch a client would use), audit wired to a temp file, and
/// `PassthroughEnforcer` (no policy complexity needed for dispatch/DDL
/// tests; `handler_with_root_and_tag_source` below is the one that needs a
/// real policy store).
async fn handler_with_root(catalog: &str) -> Harness {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;

    let config = minimal_config(&mock.uri());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );

    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}/wh' AS {catalog} (TYPE hive_external)",
        root_dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("attach hive root");

    Harness {
        handler,
        session,
        audit,
        log_path,
        _dirs: vec![root_dir, audit_dir],
        _mock: mock,
    }
}

/// Same as `handler_with_root`, but with a real `InMemoryPolicyStore` +
/// `PolicyPlanRewriter` wired through `policy_wiring::build_policy_enforcer`
/// (the exact production wiring in `main.rs`/`bin/sqe_server.rs`), so a
/// Hive table's row filter is genuinely computed via `CatalogRoutedTagSource`
/// rather than short-circuited by `PassthroughEnforcer`.
async fn handler_with_root_and_tag_source(catalog: &str) -> Harness {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;

    let mut config = minimal_config(&mock.uri());
    config.policy.engine = PolicyEngine::InMemory;
    let hive_catalogs = Arc::new(HiveCatalogs::new());
    let hive_manifest_cache = Arc::new(HiveMetadataCache::new());
    let (policy_enforcer, policy_store) = sqe_coordinator::policy_wiring::build_policy_enforcer(
        &config.policy,
        Some(TableMetadataCache::new(60)),
        hive_catalogs.clone(),
        hive_manifest_cache.clone(),
        None,
    )
    .expect("build_policy_enforcer");

    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        policy_enforcer,
        policy_store,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(hive_catalogs, hive_manifest_cache);

    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}/wh' AS {catalog} (TYPE hive_external)",
        root_dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("attach hive root");

    let data_dir = root_dir.path().join("data");
    write_csv_fixture(&data_dir);
    handler
        .execute(&session, &create_orders_ddl(&data_dir, catalog), None)
        .await
        .expect("create orders");

    Harness {
        handler,
        session,
        audit,
        log_path,
        _dirs: vec![root_dir, audit_dir],
        _mock: mock,
    }
}

/// Same as `handler_with_root`, but wired with a caller-supplied
/// `InMemoryPolicyStore` instead of `build_policy_enforcer`'s throwaway
/// empty one, so a test can populate it with `add_table_policy` and see
/// `HiveDdlHandler`'s `SHOW TABLES` / `SHOW CREATE TABLE` gate act on a
/// real per-table policy. `policy_enforcer` stays `PassthroughEnforcer`:
/// these tests pin `HiveDdlHandler::resolve_policy` directly, not the plan
/// rewriter's SELECT path, so a real `PolicyPlanRewriter` is not needed.
async fn handler_with_root_and_policy(catalog: &str, store: Arc<InMemoryPolicyStore>) -> Harness {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;

    let config = minimal_config(&mock.uri());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        Some(store as Arc<dyn PolicyStore>),
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );

    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}/wh' AS {catalog} (TYPE hive_external)",
        root_dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("attach hive root");

    let data_dir = root_dir.path().join("data");
    write_csv_fixture(&data_dir);
    handler
        .execute(&session, &create_orders_ddl(&data_dir, catalog), None)
        .await
        .expect("create orders");

    Harness {
        handler,
        session,
        audit,
        log_path,
        _dirs: vec![root_dir, audit_dir],
        _mock: mock,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A Hive/Glue-only deployment must not need a fake REST catalog merely to
/// build DataFusion's information schema. This is the regression the optional
/// legacy `catalog_url` is for: the query below used to probe `/v1/config` on
/// an operator-supplied dead anchor before it could enumerate `lake`.
#[test]
fn information_schema_tables_works_without_an_iceberg_anchor() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::spawn(information_schema_tables_without_anchor_inner())
                .await
                .expect("test task")
        });
}

async fn information_schema_tables_without_anchor_inner() {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let data_dir = root_dir.path().join("data");
    write_csv_fixture(&data_dir);

    let config = minimal_config("");
    assert!(config.flattened_catalogs().is_empty());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(Arc::new(HiveCatalogs::new()), Arc::new(HiveMetadataCache::new()));
    let session = dummy_session();

    handler
        .execute(
            &session,
            &format!(
                "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
                root_dir.path().display()
            ),
            None,
        )
        .await
        .expect("attach Hive root");
    handler
        .execute(&session, &create_orders_ddl(&data_dir, "lake"), None)
        .await
        .expect("create fixture table");

    let batches = handler
        .execute(
            &session,
            "SELECT table_catalog, table_schema, table_name \
             FROM information_schema.tables \
             WHERE table_catalog = 'lake' AND table_schema = 'sales' \
             AND table_name = 'orders'",
            None,
        )
        .await
        .expect("information_schema must not need an Iceberg anchor");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1);
}

/// One case per row of the spec's statement dispatch table (minus MSCK/SHOW
/// PARTITIONS, see the module doc). Every rejection must name `hive_external`
/// and, when a cycle is planned, that exact cycle -- checked as `"cycle {n}"`,
/// not a bare digit, so a `HiveError` that happens to echo user SQL
/// containing that digit cannot satisfy the assertion by accident (that SQL
/// text is never handed to `reject_unsupported`, so this also proves the
/// message came from the handler, not from an unrelated failure surfacing
/// through the same "hive_external:" prefix every `HiveError` carries).
#[tokio::test]
async fn unsupported_statements_are_rejected_by_name_with_their_cycle() {
    let h = handler_with_root("lake").await;
    let cases: &[(&str, u8, &str)] = &[
        (
            "ALTER TABLE lake.sales.orders SET TAGS (ssn = ('PII'))",
            3,
            "SET TAGS",
        ),
        // No plain-INSERT row: cycle 4 SUPPORTS it. See
        // `a_plain_insert_is_no_longer_rejected_by_the_hive_gate`.
        (
            "INSERT OVERWRITE lake.sales.orders VALUES (1, 2.0, '2026-01-01')",
            0,
            "INSERT OVERWRITE",
        ),
        // No CTAS row either: cycle 4 SUPPORTS it. A plain
        // `CREATE TABLE <hive> AS SELECT` used to reach the Iceberg write path
        // with no Hive-aware check at all; it is routed now, and the cases that
        // still fail do so for their own reasons rather than by cycle. See
        // `ctas_without_a_location_names_the_missing_clause`.
        // `CREATE EXTERNAL TABLE ... AS SELECT` still does not parse, so it
        // never reaches dispatch; task 9 pins that boundary instead.
        // Cycle 0, not 4: all three need the same primitive INSERT OVERWRITE
        // needs, and cycle 4 does not deliver them. Saying "cycle 4 adds it"
        // would be a promise the release does not keep.
        ("DELETE FROM lake.sales.orders WHERE id = 1", 0, "DELETE"),
        (
            "UPDATE lake.sales.orders SET amount = 1 WHERE id = 1",
            0,
            "UPDATE",
        ),
        ("TRUNCATE TABLE lake.sales.orders", 0, "TRUNCATE"),
        (
            "ALTER TABLE lake.sales.orders RENAME TO lake.sales.o2",
            0,
            "RENAME",
        ),
        (
            "COMMENT ON TABLE lake.sales.orders IS 'x'",
            0,
            "COMMENT ON TABLE",
        ),
    ];
    for (sql, cycle, label_fragment) in cases {
        let err = h.execute_for_test(sql).await.expect_err(sql);
        let msg = format!("{err}");
        assert!(msg.contains("hive_external"), "{sql}: should name the feature: {msg}");
        assert!(
            msg.contains(label_fragment),
            "{sql}: should name the statement ({label_fragment}): {msg}"
        );
        if *cycle > 0 {
            assert!(
                msg.contains(&format!("cycle {cycle}")),
                "{sql}: should name cycle {cycle} (not just contain its digit): {msg}"
            );
        } else {
            assert!(
                !msg.contains("cycle"),
                "{sql}: cycle 0 is the \"no planned cycle\" sentinel, the message must not \
                 claim one: {msg}"
            );
        }
    }
}

/// Every statement here must genuinely reach the Hive path, not merely
/// avoid erroring: each assertion below checks content only the Hive
/// manifest could have produced (the `orders` name, its real column names,
/// and the manifest's own `LOCATION`), so a stub that swallowed the
/// statement into an empty-but-`Ok` answer would fail this test even though
/// `unwrap()` alone would not have caught it.
#[test]
fn supported_statements_reach_the_hive_path_not_the_iceberg_path() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(async {
            tokio::spawn(async {
    let h = handler_with_root("lake").await;
    let data_dir = h._dirs[0].path().join("data");
    write_csv_fixture(&data_dir);
    h.execute_for_test(&create_orders_ddl(&data_dir, "lake"))
        .await
        .expect("create fixture table");

    let show_tables = h
        .execute_for_test("SHOW TABLES FROM lake.sales")
        .await
        .expect("SHOW TABLES should work");
    let table_names: Vec<String> = show_tables
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(table_names, vec!["orders".to_string()], "{table_names:?}");

    let describe = h
        .execute_for_test("DESCRIBE lake.sales.orders")
        .await
        .expect("DESCRIBE should work");
    let column_names: Vec<String> = describe
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    for expected in ["id", "amount", "dt"] {
        assert!(
            column_names.contains(&expected.to_string()),
            "DESCRIBE missing column {expected}: {column_names:?}"
        );
    }

    let show_create = h
        .execute_for_test("SHOW CREATE TABLE lake.sales.orders")
        .await
        .expect("SHOW CREATE TABLE should work");
    let ddl = show_create[0].column(0).as_string::<i32>().value(0);
    for needle in ["CREATE EXTERNAL TABLE", "LOCATION", "PARTITIONED BY"] {
        assert!(ddl.contains(needle), "missing {needle} in: {ddl}");
    }
            })
            .await
            .expect("join");
        });
}

#[tokio::test]
async fn an_iceberg_table_name_still_routes_to_iceberg() {
    let h = handler_with_root("lake").await;
    assert!(matches!(
        h.resolve_table_kind(&TableReference::from("iceberg.hr.employees")),
        TableKind::Iceberg
    ));
}

#[tokio::test]
async fn create_external_table_emits_an_audit_event_with_the_policy_name() {
    let h = handler_with_root("lake").await;
    h.execute_for_test(
        "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS PARQUET LOCATION \
         's3://raw/orders/'",
    )
    .await
    .expect("create");
    let events = h.audit_events_for_test();
    assert!(
        events.iter().any(|e| e.contains("lake.sales.orders")),
        "audit_resources only sees DataFusion CreateExternalTable plans, and the server path \
         produces none, so the handler must emit its own: {events:?}"
    );
}

/// The end-to-end guard for the unknown-tag-state failure mode. Without the
/// routing tag source this used to return zero rows (`lit(false)`); since
/// #840 it is refused with `AccessDenied`, which `.expect("query")` catches.
#[test]
fn a_hive_query_is_not_denied_when_a_tag_source_is_active() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(async {
            let h = handler_with_root_and_tag_source("lake").await;
            let rows = tokio::spawn(async move {
                h.query_rows_for_test("SELECT count(*) AS n FROM lake.sales.orders")
                    .await
            })
            .await
            .expect("join")
            .expect("query");
            assert_eq!(rows, 3, "a tag source must not deny an untagged Hive table");
        });
}

/// `USE lake.sales` then an unqualified name reads the Hive table. The session
/// default catalog used to accept only static-config and Polaris catalogs, so a
/// Hive root fell back to the config default and `orders` resolved against
/// Iceberg: here an error, and in the benchmark's Hive mode a same-named
/// Iceberg table read silently.
#[test]
fn an_unqualified_name_under_a_hive_default_catalog_reads_the_hive_table() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(async {
            let h = handler_with_root_and_tag_source("lake").await;
            let batches = tokio::spawn(async move {
                let mut session = h.session.clone();
                session.default_catalog = Some("lake".to_string());
                session.default_schema = Some("sales".to_string());
                h.handler
                    .execute(&session, "SELECT count(*) AS n FROM orders", None)
                    .await
            })
            .await
            .expect("join")
            .expect("unqualified name under USE lake.sales must resolve to the Hive table");
            assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 3);
        });
}

/// The `CREATE EXTERNAL TABLE` mirror of
/// `attach_hive_external_root_refused_when_hive_registries_are_not_wired`
/// (task 10a's own escalation guard, on the ATTACH path): a `QueryHandler`
/// that never called `with_hive_registries` must refuse rather than write a
/// manifest into a root the policy layer cannot see, even when the root is
/// reachable via `RuntimeCatalogRegistry` directly (bypassing the ATTACH
/// dispatch's own guard, the way a startup-config `[[hive.roots]]` path
/// would). Mutation-checked by hand: commenting out the `hive_catalogs`
/// guard in `HiveDdlHandler::create_external_table` makes this test fail
/// (the create succeeds instead of erroring).
#[tokio::test]
async fn create_external_table_refused_when_hive_registries_are_not_wired() {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let registry = RuntimeCatalogRegistry::new();
    let mut storage = sqe_core::config::StorageConfig::default();
    storage.tvf.allow_local_paths = true;
    let store = sqe_catalog::mount::build_hive_root(
        &format!("file://{}/wh", root_dir.path().display()),
        &std::collections::BTreeMap::new(),
        &storage,
        ManifestLimits::default(),
        TvfCaller::trusted_local(),
    )
    .await
    .expect("build_hive_root");
    registry
        .attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store))
        .expect("attach_hive_root");

    let mut config = minimal_config("http://localhost:59996");
    config.storage.tvf.allow_local_paths = true;
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        registry,
        SecretStore::new(),
    )
    .expect("QueryHandler::new");
    // Deliberately never calls with_hive_registries.

    let session = dummy_session();
    let err = handler
        .execute(
            &session,
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS PARQUET LOCATION \
             'file:///tmp/does-not-matter/'",
            None,
        )
        .await
        .expect_err("must refuse without the shared hive registries wired");
    let msg = err.to_string();
    assert!(
        msg.contains("hive_external") && msg.contains("with_hive_registries"),
        "error should name the wiring gap, not a generic failure: {msg}"
    );
}

// ---------------------------------------------------------------------------
// DROP TABLE against a Hive root
// ---------------------------------------------------------------------------

/// Mirrors embedded mode's `drop_table_purge_is_rejected_by_name`: PURGE
/// deletes the underlying data, which an external table's DROP must never
/// do, so it is refused by name rather than silently ignored or (worse)
/// honored.
#[tokio::test]
async fn drop_table_purge_is_rejected_by_name() {
    let h = handler_with_root("lake").await;
    let data_dir = h._dirs[0].path().join("data");
    write_csv_fixture(&data_dir);
    h.execute_for_test(&create_orders_ddl(&data_dir, "lake"))
        .await
        .expect("create fixture table");

    let err = h
        .execute_for_test("DROP TABLE lake.sales.orders PURGE")
        .await
        .expect_err("PURGE must be refused for a Hive external table");
    let msg = err.to_string();
    assert!(msg.contains("PURGE"), "should name PURGE: {msg}");
}

/// `DROP TABLE IF EXISTS` on a Hive table that was never created must be a
/// no-op, not an error -- the same contract `IF EXISTS` has on the Iceberg
/// path. This is the test that makes the `if_exists` / `HiveError::NotFound`
/// message coupling in the dispatch arm visible: change
/// `HiveError::NotFound`'s wording without updating the match in
/// `query_handler.rs`'s `Drop` arm and this fails.
#[tokio::test]
async fn drop_table_if_exists_on_a_missing_table_is_a_no_op() {
    let h = handler_with_root("lake").await;
    h.execute_for_test("DROP TABLE IF EXISTS lake.sales.orders")
        .await
        .expect("IF EXISTS must not error on a table that was never created");
}

/// `DROP TABLE` without `IF EXISTS` on a missing Hive table is a real
/// error, not swallowed -- the negative case for the same coupling.
#[tokio::test]
async fn drop_table_without_if_exists_on_a_missing_table_errors() {
    let h = handler_with_root("lake").await;
    h.execute_for_test("DROP TABLE lake.sales.orders")
        .await
        .expect_err("dropping a table that was never created must error");
}

/// The whole point of an external table: `DROP TABLE` deletes the manifest,
/// never the data. Mirrors embedded mode's
/// `drop_removes_the_manifest_and_leaves_the_data`.
#[tokio::test]
async fn drop_table_removes_the_manifest_and_leaves_the_data() {
    let h = handler_with_root("lake").await;
    let data_dir = h._dirs[0].path().join("data");
    write_csv_fixture(&data_dir);
    h.execute_for_test(&create_orders_ddl(&data_dir, "lake"))
        .await
        .expect("create fixture table");

    h.execute_for_test("DROP TABLE lake.sales.orders")
        .await
        .expect("drop");

    assert!(
        data_dir.join("dt=2026-01-01/a.csv").exists(),
        "DROP TABLE must never delete the underlying data"
    );
    // The manifest itself is gone: SHOW CREATE TABLE, which reads the
    // manifest directly, now fails rather than describing a dropped table.
    h.execute_for_test("SHOW CREATE TABLE lake.sales.orders")
        .await
        .expect_err("the manifest should be gone after DROP");
}

// No test here for "DROP TABLE invalidates the session cache", despite it
// being the one property none of the tests above pin. `SHOW CREATE TABLE`
// and the `SHOW TABLES` branch both read the manifest store directly
// (bypassing `SESSION_CONTEXT_CACHE` entirely), and `DESCRIBE`'s column
// lookup does too: `HiveSchemaProvider::table()` (`provider.rs`) always
// calls `store.get(&database, name)` fresh, never consulting the
// per-provider listing snapshots that back `table_names()`/`table_exist()`
// (`HiveListings`, review 806).
// A `DESCRIBE`-before/after-drop test was written and looked plausible, but
// a mutation check (commenting out the DROP arm's `invalidate_session_cache`
// call) proved it passed identically either way -- it was measuring
// DESCRIBE's own always-fresh manifest lookup, not the cache. Removed
// rather than left in as a test that looks like it proves something it does
// not. A test that actually pins this would need a query that resolves the
// table via `table_exist()`/`table_names()` on a REUSED provider instance
// (e.g. a plain `SELECT` needs the planner to call `table_exist` first) run
// twice inside the same 60s provider-cache window, which is a real gap this
// file leaves open.

// ---------------------------------------------------------------------------
// `TBLPROPERTIES ('sqe.secret' = ...)` validation
// ---------------------------------------------------------------------------

/// `CREATE EXTERNAL TABLE ... TBLPROPERTIES ('sqe.secret' = <name>)` must
/// fail at DDL time when `<name>` is not a known secret, rather than
/// writing a manifest that only fails later, confusingly, on first scan.
#[tokio::test]
async fn create_external_table_rejects_an_unknown_secret_reference() {
    let h = handler_with_root("lake").await;
    let err = h
        .execute_for_test(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS PARQUET LOCATION \
             's3://raw/orders/' TBLPROPERTIES ('sqe.secret' = 'does-not-exist')",
        )
        .await
        .expect_err("an unknown sqe.secret reference must fail at CREATE time");
    let msg = err.to_string();
    assert!(
        msg.contains("does-not-exist"),
        "should name the missing secret: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Policy gating on the Hive metadata-read paths (fix round: SHOW TABLES /
// SHOW CREATE TABLE against a Hive root previously read the manifest store
// with the coordinator's own credentials and returned everything, with
// nothing gating it the way `session_catalog.list_namespaces()`'s Polaris
// authorization gates the Iceberg equivalents, and the way
// `information_schema.columns`'s own `PolicyStore::resolve()` call gates
// Iceberg `SHOW CREATE TABLE`'s column list). `HiveDdlHandler::resolve_policy`
// now runs the SAME `resolve_effective_policy` the plan rewriter and `SHOW
// EFFECTIVE POLICY` already use, so there is one policy-resolution path,
// not a second one invented for Hive.
// ---------------------------------------------------------------------------

/// A table denied outright (the plan rewriter's `lit(false)` deny-all
/// sentinel) must be invisible to both `SHOW TABLES` and `SHOW CREATE
/// TABLE` against a Hive root -- omitted from the listing, and erring with
/// the SAME "table not found" wording an absent table gets, so a denial
/// cannot be told apart from a table that never existed (the project's
/// no-information-leakage convention).
///
/// Mutation-checked: with the `policy_denies_all` guard removed from both
/// `HiveDdlHandler::show_create_table` and `list_tables`, this test failed
/// -- `SHOW TABLES` still listed `orders` and `SHOW CREATE TABLE` returned
/// the full DDL -- confirming the gate, not something else, is what this
/// test pins.
#[tokio::test]
async fn a_fully_denied_hive_table_is_invisible_to_show_tables_and_show_create_table() {
    let store = Arc::new(InMemoryPolicyStore::new());
    // The harness creates the table as the session under test, and a
    // fully-denied session can no longer CREATE EXTERNAL TABLE (#717), so the
    // deny is registered after setup.
    let h = handler_with_root_and_policy("lake", store.clone()).await;
    store
        .add_table_policy(
            // Keyed like SELECT resolves a Hive root table: `{root}.{database}`
            // (#715); a policy on the bare database no longer gates metadata.
            "lake.sales",
            "orders",
            ResolvedPolicy {
                row_filters: vec![datafusion::logical_expr::lit(false)],
                ..Default::default()
            },
        )
        .await;

    let show_tables = h
        .execute_for_test("SHOW TABLES FROM lake.sales")
        .await
        .expect("SHOW TABLES itself must not error on a denied table, only omit it");
    let names: Vec<String> = show_tables
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        !names.contains(&"orders".to_string()),
        "a fully-denied table must not appear in SHOW TABLES, got {names:?}"
    );

    let err = h
        .execute_for_test("SHOW CREATE TABLE lake.sales.orders")
        .await
        .expect_err("SHOW CREATE TABLE on a fully-denied table must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("table not found"),
        "a denied table's SHOW CREATE TABLE error must read like a missing \
         table's, not confirm the table exists: {msg}"
    );
}

/// A table with a partial column restriction (not a deny-all) must still
/// appear in `SHOW TABLES` -- the restriction is column-level, not
/// table-level -- and `SHOW CREATE TABLE` must render successfully with the
/// restricted column silently dropped from the column list, mirroring how
/// `information_schema.columns` already treats a restricted Iceberg column
/// as invisible rather than erroring.
///
/// Mutation-checked: removing the `spec.columns.retain(...)` line made this
/// test fail (`amount` still appeared in the rendered DDL).
#[tokio::test]
async fn a_column_restricted_hive_table_hides_only_that_column_in_show_create_table() {
    let store = Arc::new(InMemoryPolicyStore::new());
    store
        .add_table_policy(
            "lake.sales",
            "orders",
            ResolvedPolicy {
                restricted_columns: vec!["amount".to_string()],
                ..Default::default()
            },
        )
        .await;
    let h = handler_with_root_and_policy("lake", store).await;

    let show_tables = h
        .execute_for_test("SHOW TABLES FROM lake.sales")
        .await
        .expect("SHOW TABLES should work");
    let names: Vec<String> = show_tables
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        names,
        vec!["orders".to_string()],
        "a column restriction is not a table-level denial: {names:?}"
    );

    let show_create = h
        .execute_for_test("SHOW CREATE TABLE lake.sales.orders")
        .await
        .expect("SHOW CREATE TABLE should still succeed for a partially-restricted table");
    let ddl = show_create[0].column(0).as_string::<i32>().value(0);
    assert!(
        !ddl.contains("amount"),
        "the restricted column must not appear in the rendered DDL: {ddl}"
    );
    for needle in ["id", "CREATE EXTERNAL TABLE"] {
        assert!(ddl.contains(needle), "missing {needle} in: {ddl}");
    }
}

// ---------------------------------------------------------------------------
// `resolve_table_kind_for_session` (fix round: a Bare/Partial reference
// under a session whose default catalog IS a Hive root must still be
// caught by `reject_if_hive_table`, not fall through to the Iceberg write
// path because `resolve_table_kind` alone only matches a `Full` reference).
// ---------------------------------------------------------------------------

/// `DELETE FROM sales.orders` (a two-part, catalog-less reference) under a
/// session whose default catalog is the attached Hive root must still be
/// rejected as unsupported for a Hive external table, not silently
/// resolved against whatever Iceberg catalog the write path's own fallback
/// would otherwise pick.
///
/// Mutation-checked: reverting `reject_if_hive_table` to call
/// `resolve_table_kind` directly (skipping the session-default-catalog
/// promotion) made this test fail -- the statement no longer errored with
/// `hive_external` at all, confirming it would otherwise have proceeded
/// into the Iceberg write path instead of being rejected.
#[tokio::test]
async fn an_unqualified_reference_under_a_hive_default_catalog_is_still_rejected() {
    let h = handler_with_root("lake").await;
    let mut session = h.session.clone();
    session.default_catalog = Some("lake".to_string());

    let result = h
        .handler
        .execute(&session, "DELETE FROM sales.orders", None)
        .await;
    let err = result.expect_err(
        "DELETE against a two-part reference under a Hive default catalog must be rejected, \
         not silently proceed",
    );
    let msg = err.to_string();
    // `hive_external` plus the statement name is what discriminates here: it
    // proves the reference resolved to the HIVE branch. The `cycle 4` assertion
    // this test used to carry is gone because DELETE no longer names a cycle --
    // it needs the same primitive INSERT OVERWRITE needs, so it is refused
    // outright rather than scheduled. The mutation the doc comment describes
    // (skipping the session-default-catalog promotion) still fails this test,
    // because without the promotion the statement never produces a
    // `hive_external` error at all.
    assert!(msg.contains("hive_external"), "{msg}");
    assert!(msg.contains("DELETE"), "{msg}");
}

/// The `resolve_table_kind_for_session` promotion itself, isolated from
/// statement dispatch: a `Partial` reference (`sales.orders`, no catalog)
/// resolves to `TableKind::Hive` when `default_catalog` names an attached
/// Hive root, and stays `TableKind::Iceberg` when it does not (no
/// regression on the pre-existing, unqualified-reference behaviour).
#[tokio::test]
async fn resolve_table_kind_for_session_promotes_a_partial_reference_under_a_hive_default() {
    let h = handler_with_root("lake").await;
    let partial = TableReference::from("sales.orders");

    assert!(matches!(
        h.handler
            .resolve_table_kind_for_session(Some("lake"), &partial),
        TableKind::Hive { .. }
    ));
    assert!(matches!(
        h.handler
            .resolve_table_kind_for_session(Some("some_other_iceberg_catalog"), &partial),
        TableKind::Iceberg
    ));
    assert!(matches!(
        h.handler.resolve_table_kind_for_session(None, &partial),
        TableKind::Iceberg
    ));
}

/// The other half of the review's fix: `ATTACH ... TYPE hive_external`
/// itself must refuse a name that collides with the session default
/// catalog, the same way `attach_configured_hive_roots` refuses it for a
/// `[[hive.roots]]` entry (see `sqe-coordinator/src/runtime_catalog.rs`'s
/// matching test). `minimal_config`'s legacy `[catalog]` block resolves to
/// the default catalog name `"iceberg"` (`SqeConfig::LEGACY_CATALOG_NAME`),
/// so attaching a Hive root under that exact name must be refused rather
/// than silently succeeding and leaving `resolve_policy_key`/
/// `CatalogRoutedTagSource` blind to it for any two-part reference.
///
/// Mutation-checked: removing the `stmt.name == default_catalog` check in
/// `handle_attach` made this test fail (the ATTACH succeeded).
#[tokio::test]
async fn attach_refuses_a_name_matching_the_session_default_catalog() {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;

    let config = minimal_config(&mock.uri());
    assert_eq!(
        config.resolve_default_catalog(),
        "iceberg",
        "this test's premise: the legacy [catalog] block resolves to 'iceberg'"
    );
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(Arc::new(HiveCatalogs::new()), Arc::new(HiveMetadataCache::new()));

    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}/wh' AS iceberg (TYPE hive_external)",
        root_dir.path().display()
    );
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("attaching a Hive root named like the default catalog must be refused");
    let msg = err.to_string();
    assert!(msg.contains("iceberg"), "{msg}");
    assert_eq!(
        handler.resolve_table_kind(&TableReference::full("iceberg", "sales", "orders")),
        TableKind::Iceberg,
        "the refused ATTACH must not have registered 'iceberg' as a Hive root"
    );

    drop((audit_dir, mock));
}

/// Cycle 4: a plain `INSERT` must no longer be turned away by the Hive gate.
///
/// The point is the ABSENCE of a `hive_external` rejection, not success. This
/// harness registers the manifest but not a queryable warehouse, so planning
/// fails on the table lookup -- and that failure is the evidence: it can only
/// be reached AFTER `reject_if_hive_table` would have fired. An assertion of
/// `is_ok()` would be untestable here, and an assertion of `is_err()` alone
/// would pass just as well if the statement were still rejected.
///
/// The end-to-end write is covered by `hive_write_test`, which builds a real
/// warehouse on disk and reads the rows back.
#[tokio::test]
async fn a_plain_insert_is_no_longer_rejected_by_the_hive_gate() {
    let h = handler_with_root("lake").await;
    let err = h
        .execute_for_test("INSERT INTO lake.sales.orders VALUES (1, 2.0, '2026-01-01')")
        .await
        .expect_err("this harness cannot plan the table, so the statement still errors");
    let msg = format!("{err}");
    assert!(
        !msg.contains("is not supported for Hive external tables"),
        "INSERT must reach the write path, not the rejection: {msg}"
    );
    assert!(
        msg.contains("hive insert"),
        "and it must have reached the HIVE insert path specifically, rather than \
         the Iceberg one: {msg}"
    );
}

/// The same statement with `OVERWRITE` must still be refused, and must say why.
#[tokio::test]
async fn insert_overwrite_is_refused_before_any_table_lookup() {
    let h = handler_with_root("lake").await;
    let err = h
        .execute_for_test("INSERT OVERWRITE lake.sales.orders VALUES (1, 2.0, '2026-01-01')")
        .await
        .expect_err("overwrite must be refused");
    let msg = format!("{err}");
    // Refused BEFORE the table lookup the plain insert trips over, which is how
    // this test tells "refused" apart from "failed for an unrelated reason".
    assert!(
        !msg.contains("not found"),
        "the refusal must precede planning: {msg}"
    );
    assert!(msg.contains("atomic"), "the reason must be stated: {msg}");
}

/// Cycle 4: CTAS is routed, so a CTAS that fails must fail for its OWN reason.
///
/// The rejection-by-cycle table used to carry this statement. It cannot any
/// more: the statement reaches the Hive CTAS path and stops there because no
/// `LOCATION` was given, which is a different and more useful error than "not
/// supported". Asserted on the clause name and on the reason, because
/// "requires LOCATION" without the why sends someone looking for a config knob.
#[tokio::test]
async fn ctas_without_a_location_names_the_missing_clause() {
    let h = handler_with_root("lake").await;
    let err = h
        .execute_for_test("CREATE TABLE lake.sales.o2 AS SELECT 1 AS id")
        .await
        .expect_err("no LOCATION");
    let msg = format!("{err}");
    assert!(msg.contains("hive_external"), "{msg}");
    assert!(msg.contains("LOCATION"), "{msg}");
    assert!(
        msg.contains("does not choose data paths"),
        "the message must say WHY it is required: {msg}"
    );
    // And it must NOT read as a scheduling problem. This is the assertion that
    // catches a regression to `reject_if_hive_table(.., "CTAS", 4)`.
    assert!(
        !msg.contains("cycle 4"),
        "CTAS is supported; the error must not blame a future cycle: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Hive DDL authorization: ownership, location overlap, caller identity
// ---------------------------------------------------------------------------

fn named_table_ddl(name: &str, location: &str) -> String {
    format!(
        "CREATE EXTERNAL TABLE lake.sales.{name} (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION '{location}'"
    )
}

/// A non-admin who does not own a table must not replace its manifest (a
/// plain overwrite that would drop admin-set tags) or drop it.
#[tokio::test]
async fn a_non_owner_cannot_replace_or_drop_a_hive_table() {
    let h = handler_with_root("lake").await;
    let data_dir = h._dirs[0].path().join("data");
    write_csv_fixture(&data_dir);
    h.execute_for_test(&create_orders_ddl(&data_dir, "lake"))
        .await
        .expect("admin creates orders");
    let analyst = session_with_roles(vec!["analyst".to_string()]);

    let elsewhere = format!("file://{}", h._dirs[0].path().join("elsewhere").display());
    let err = h
        .handler
        .execute(&analyst, &named_table_ddl("orders", &elsewhere), None)
        .await
        .expect_err("a non-owner must not overwrite the manifest");
    assert!(err.to_string().contains("403"), "{err}");

    let err = h
        .handler
        .execute(&analyst, "DROP TABLE lake.sales.orders", None)
        .await
        .expect_err("a non-owner must not drop the table");
    assert!(err.to_string().contains("403"), "{err}");

    // The owner (here also an admin) still can.
    h.execute_for_test("DROP TABLE lake.sales.orders")
        .await
        .expect("owner drops");
}

/// A non-admin must not define a table, or a partition, over files another
/// table already reads: the alias would read them without that table's
/// filters and masks. A disjoint location still works, and its owner may
/// drop it.
#[tokio::test]
async fn a_non_admin_cannot_alias_another_tables_location() {
    let h = handler_with_root("lake").await;
    let base = h._dirs[0].path().to_path_buf();
    let data_dir = base.join("data");
    write_csv_fixture(&data_dir);
    h.execute_for_test(&create_orders_ddl(&data_dir, "lake"))
        .await
        .expect("admin creates orders");
    let analyst = session_with_roles(vec!["analyst".to_string()]);

    for location in [
        format!("file://{}", data_dir.display()),
        format!("file://{}/", data_dir.display()),
        format!("file://{}/dt=2026-01-01", data_dir.display()),
        format!("file://{}", base.display()),
    ] {
        let err = h
            .handler
            .execute(&analyst, &named_table_ddl("orders_alias", &location), None)
            .await
            .expect_err("an overlapping location must be refused");
        assert!(err.to_string().contains("overlaps"), "{location}: {err}");
    }

    // `data2` shares a string prefix with `data` but not a path prefix.
    let own = format!("file://{}", base.join("data2").display());
    h.handler
        .execute(&analyst, &named_table_ddl("orders_mine", &own), None)
        .await
        .expect("a disjoint location is allowed");

    let err = h
        .handler
        .execute(
            &analyst,
            &format!(
                "ALTER TABLE lake.sales.orders_mine ADD PARTITION (dt='2026-01-01') \
                 LOCATION 'file://{}/dt=2026-01-01/'",
                data_dir.display()
            ),
            None,
        )
        .await
        .expect_err("a partition over another table's files must be refused");
    assert!(err.to_string().contains("overlaps"), "{err}");

    h.handler
        .execute(&analyst, "DROP TABLE lake.sales.orders_mine", None)
        .await
        .expect("the owner may drop their own table");
}

/// DDL location checks run as the session user, not as whoever attached the
/// root: an analyst outside `object_store_admin_roles` with no allowed
/// prefix cannot point a table at an object-store location, even though the
/// admin who ran the ATTACH could.
#[tokio::test]
async fn ddl_location_checks_run_as_the_session_user() {
    let h = handler_with_root("lake").await;
    let analyst = session_with_roles(vec!["analyst".to_string()]);
    let err = h
        .handler
        .execute(&analyst, &named_table_ddl("orders_raw", "s3://raw/orders/"), None)
        .await
        .expect_err("the analyst may not read s3://raw/");
    assert!(err.to_string().contains("forbidden"), "{err}");

    h.execute_for_test(&named_table_ddl("orders_raw", "s3://raw/orders/"))
        .await
        .expect("an object-store admin may");
}

// ---------------------------------------------------------------------------
// Hive DDL authorization: admin, owner, or a policy-engine grant
// ---------------------------------------------------------------------------

/// The sidecar of `lake.<db>.<table>` under a harness's `ATTACH`ed root.
fn manifest_file(h: &Harness, db: &str, table: &str) -> PathBuf {
    h._dirs[0]
        .path()
        .join("wh")
        .join(db)
        .join(table)
        .join("_sqe")
        .join("table.json")
}

/// A non-owner holding a `drop` grant from the policy engine may replace and
/// drop the table; one without it, or with only `alter`, may not.
#[tokio::test]
async fn a_policy_grant_lets_a_non_owner_replace_or_drop_a_hive_table() {
    let store = Arc::new(InMemoryPolicyStore::new());
    let h = handler_with_root_and_policy("lake", store.clone()).await;
    store
        .grant_table_privilege("lake.sales", "orders", "stewards", TablePrivilege::Drop)
        .await;
    store
        .grant_table_privilege("lake.sales", "orders", "ops", TablePrivilege::Alter)
        .await;
    let steward = session_with_roles(vec!["stewards".to_string()]);
    let ops = session_with_roles(vec!["ops".to_string()]);
    let analyst = session_with_roles(vec!["analyst".to_string()]);

    for who in [&analyst, &ops] {
        let err = h
            .handler
            .execute(who, "DROP TABLE lake.sales.orders", None)
            .await
            .expect_err("no drop grant, not the owner, not an admin");
        assert!(err.to_string().contains("403"), "{err}");
    }

    let elsewhere = format!("file://{}", h._dirs[0].path().join("elsewhere").display());
    h.handler
        .execute(&steward, &named_table_ddl("orders", &elsewhere), None)
        .await
        .expect("a drop grant may replace the definition");
    h.handler
        .execute(&steward, "DROP TABLE lake.sales.orders", None)
        .await
        .expect("a drop grant may drop the table");
}

/// A manifest with no recorded owner (an older sidecar, a Glue mirror) is
/// not admin-only: the policy-engine grant still applies to it.
#[tokio::test]
async fn an_ownerless_manifest_is_droppable_through_a_grant() {
    let store = Arc::new(InMemoryPolicyStore::new());
    let h = handler_with_root_and_policy("lake", store.clone()).await;
    let path = manifest_file(&h, "sales", "orders");
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read manifest")).expect("json");
    json.as_object_mut().expect("object").remove("owner");
    std::fs::write(&path, serde_json::to_vec(&json).unwrap()).expect("write manifest");

    let steward = session_with_roles(vec!["stewards".to_string()]);
    let err = h
        .handler
        .execute(&steward, "DROP TABLE lake.sales.orders", None)
        .await
        .expect_err("no grant yet");
    assert!(err.to_string().contains("403"), "{err}");

    store
        .grant_table_privilege("lake.sales", "orders", "stewards", TablePrivilege::Drop)
        .await;
    h.handler
        .execute(&steward, "DROP TABLE lake.sales.orders", None)
        .await
        .expect("the grant covers an ownerless manifest");
}

/// Nesting a table under one the caller owns, or holds `alter` on, is the
/// normal Hive layout; the overlap refusal only protects other people's
/// tables.
#[tokio::test]
async fn nesting_under_a_table_the_caller_may_administer_is_allowed() {
    let store = Arc::new(InMemoryPolicyStore::new());
    let h = handler_with_root_and_policy("lake", store.clone()).await;
    let base = h._dirs[0].path().join("mine");
    let analyst = session_with_roles(vec!["analyst".to_string()]);
    let other = session_with_roles(vec!["analyst".to_string()]);

    h.handler
        .execute(
            &analyst,
            &named_table_ddl("parent", &format!("file://{}", base.display())),
            None,
        )
        .await
        .expect("a disjoint location");
    let nested = format!("file://{}/dt=2026-01-01", base.display());
    h.handler
        .execute(&analyst, &named_table_ddl("child", &nested), None)
        .await
        .expect("the owner may nest a table under their own table");

    let err = h
        .handler
        .execute(&other, &named_table_ddl("alias", &nested), None)
        .await
        .expect_err("someone else may not");
    assert!(err.to_string().contains("overlaps"), "{err}");

    // `orders` (the harness's table, owned by its admin session) is open to
    // a caller with an alter grant on it.
    let data = h._dirs[0].path().join("data");
    store
        .grant_table_privilege("lake.sales", "orders", "ops", TablePrivilege::Alter)
        .await;
    let ops = session_with_roles(vec!["ops".to_string()]);
    h.handler
        .execute(
            &ops,
            &named_table_ddl(
                "orders_day",
                &format!("file://{}/dt=2026-01-01", data.display()),
            ),
            None,
        )
        .await
        .expect("an alter grant on orders covers a table nested under it");
}

/// One bad sidecar in the root must not block every non-admin's DDL.
#[tokio::test]
async fn an_unparsable_manifest_does_not_block_ddl_on_other_tables() {
    let store = Arc::new(InMemoryPolicyStore::new());
    let h = handler_with_root_and_policy("lake", store).await;
    let garbage = manifest_file(&h, "sales", "broken");
    std::fs::create_dir_all(garbage.parent().unwrap()).unwrap();
    std::fs::write(&garbage, b"{ not a manifest").unwrap();
    let bad_location = manifest_file(&h, "sales", "bad_location");
    std::fs::create_dir_all(bad_location.parent().unwrap()).unwrap();
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_file(&h, "sales", "orders")).unwrap())
            .unwrap();
    json["name"] = "bad_location".into();
    json["storage_descriptor"]["location"] = "http://[::1".into();
    std::fs::write(&bad_location, serde_json::to_vec(&json).unwrap()).unwrap();

    let analyst = session_with_roles(vec!["analyst".to_string()]);
    let own = format!("file://{}", h._dirs[0].path().join("own").display());
    h.handler
        .execute(&analyst, &named_table_ddl("mine", &own), None)
        .await
        .expect("a bad sidecar elsewhere in the root is skipped");
    h.handler
        .execute(&analyst, "DROP TABLE lake.sales.mine", None)
        .await
        .expect("and the owner can drop it again");
}

/// A `[[hive.roots]]` root is attached with the trusted startup caller, but
/// a session's SELECT scans as the session user. DDL checks locations with
/// that same caller, so whoever may SELECT a table there needs nothing more
/// for DDL than the grant: no global allowlist entry. Conversely a caller the
/// allowlist refuses is refused both ways, through the same `{user}` prefix.
#[test]
fn ddl_on_a_configured_root_needs_no_more_than_select() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(async {
            let root_dir = tempfile::tempdir().expect("tempdir");
            let mock = mock_polaris().await;
            let mut config = minimal_config(&mock.uri());
            config.storage.tvf.allowed_object_store_prefixes = vec!["s3://raw/{user}/".to_string()];
            config.hive = toml::from_str(&format!(
                "[[roots]]\nname = \"lake\"\nlocation = \"file://{}/wh\"\n",
                root_dir.path().display()
            ))
            .expect("hive config");
            let store = Arc::new(InMemoryPolicyStore::new());
            let registry = RuntimeCatalogRegistry::new();
            let hive_catalogs = Arc::new(HiveCatalogs::new());
            sqe_coordinator::runtime_catalog::attach_configured_hive_roots(
                &config.hive,
                &config.storage,
                &registry,
                &hive_catalogs,
                "iceberg",
            )
            .await
            .expect("attach [[hive.roots]]");
            let tracker = Arc::new(QueryTracker::new(&config.query_history));
            let handler = Arc::new(
                QueryHandler::new(
                    Arc::new(PassthroughEnforcer),
                    Some(store.clone() as Arc<dyn PolicyStore>),
                    config,
                    None,
                    None,
                    None,
                    None,
                    tracker,
                    None,
                    None,
                    None,
                    registry,
                    SecretStore::new(),
                )
                .expect("QueryHandler::new")
                .with_hive_registries(hive_catalogs, Arc::new(HiveMetadataCache::new())),
            );

            let admin = dummy_session();
            let data_dir = root_dir.path().join("data");
            write_csv_fixture(&data_dir);
            handler
                .execute(&admin, &create_orders_ddl(&data_dir, "lake"), None)
                .await
                .expect("create orders");
            store
                .grant_table_privilege("lake.sales", "orders", "stewards", TablePrivilege::Drop)
                .await;
            store
                .grant_table_privilege("lake.sales", "events", "stewards", TablePrivilege::Drop)
                .await;

            let steward = session_with_roles(vec!["stewards".to_string()]);
            let stranger = session_with_roles(vec!["stewards".to_string()]);
            let theirs = format!("s3://raw/{}/events/", steward.user.username);
            handler
                .execute(&admin, &named_table_ddl("events", &theirs), None)
                .await
                .expect("an object-store admin creates the s3 table");

            let (h2, s2) = (Arc::clone(&handler), steward.clone());
            tokio::spawn(async move {
                h2.execute(&s2, "SELECT count(*) AS n FROM lake.sales.orders", None)
                    .await
            })
            .await
            .expect("join")
            .expect("the steward may SELECT orders");
            for sql in [
                "SHOW PARTITIONS lake.sales.orders",
                "DROP TABLE lake.sales.orders",
            ] {
                handler
                    .execute(&steward, sql, None)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }

            let err = handler
                .execute(&stranger, "DROP TABLE lake.sales.events", None)
                .await
                .expect_err("the allowlist names the steward's prefix, not the stranger's");
            assert!(err.to_string().contains("forbidden"), "{err}");
            handler
                .execute(&steward, "DROP TABLE lake.sales.events", None)
                .await
                .expect("the `{user}` prefix and the grant are enough on an s3 location");
        });
}

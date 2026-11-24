//! Integration tests for `[query] catalog_discovery = "polaris-auto"`.
//!
//! ## Background
//!
//! `catalog_discovery = "polaris-auto"` makes the coordinator lazily register
//! an unknown 3-part catalog qualifier against Polaris using the caller's bearer
//! instead of immediately returning "unknown catalog". This is implemented in
//! `QueryHandler::preflight_resolve_catalogs` and
//! `session_context::discover_catalog_provider`.
//!
//! ## Test inventory
//!
//! ### Offline (no live stack required) -- always run
//!
//! - `config_catalog_discovery_defaults_to_static` -- verify TOML parsing.
//! - `polaris_auto_known_catalog_passes_pre_flight` -- a registered catalog is
//!   not rejected even in polaris-auto mode.
//! - `static_mode_unqualified_query_passes_pre_flight` -- 1-part name never
//!   trips the pre-flight check.
//!
//! ### Live stack -- require `#[ignore]`
//!
//! Run with:
//!   `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`
//! then:
//!   `cargo test -p sqe-coordinator --test it catalog_discovery_test -- --ignored`
//!
//! - `static_mode_rejects_undeclared_warehouse` -- static mode returns "unknown
//!   catalog" for a warehouse that exists in Polaris but isn't in `[catalogs.*]`.
//!   No probe is made.
//! - `polaris_auto_lazy_hit` -- an undeclared warehouse is discovered and its
//!   table is queryable.
//! - `polaris_auto_nonexistent_warehouse_returns_unknown_catalog` -- a warehouse
//!   that does not exist in Polaris returns "unknown catalog".
//! - `polaris_auto_in_session_reuse` -- referencing a discovered warehouse twice
//!   in one session both succeed.
//!
//! ## Two product bugs fixed in this branch (previously blocked these tests)
//!
//! 1. `REST_CATALOG_CACHE` (`sqe-catalog/src/rest_catalog.rs`) was keyed by
//!    `(catalog_url, token_fingerprint)` -- the warehouse was not in the key.
//!    A second warehouse at the same URL+token aliased to the first warehouse's
//!    cached `RestCatalog` (whose `context()` baked the first warehouse's
//!    `/v1/config`). Fix: include `warehouse` in the cache key.
//! 2. `preflight_resolve_catalogs` (`sqe-coordinator/src/query_handler.rs`)
//!    contacted Polaris (`create_session_context`) before the unknown-catalog
//!    check, breaking static-mode "no probe" behavior and the offline
//!    `multi_catalog_routing_test::unknown_catalog_qualifier_errors_clearly`.
//!    Fix: build the known set from config + attached catalogs first (no IO),
//!    only build the session ctx + probe Polaris when discovery is on AND a
//!    qualifier is still unknown.

use std::sync::Arc;

use chrono::{Duration, Utc};
use sqe_core::{Session, SqeConfig};

// ---------------------------------------------------------------------------
// Helpers shared by all tests
// ---------------------------------------------------------------------------

fn parse_config(toml: &str) -> SqeConfig {
    toml::from_str::<SqeConfig>(toml).expect("config parses")
}

fn make_handler(config: SqeConfig) -> sqe_coordinator::QueryHandler {
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    sqe_coordinator::QueryHandler::new(
        policy,
        None,
        config,
        None,
        None,
        None,
        None,
        query_tracker,
        None,
        None,
        None,
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("QueryHandler::new succeeds")
}

fn fake_session() -> Session {
    Session::new(
        "alice".to_string(),
        sqe_core::SecretString::new("tok_unused".to_string()),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    )
}

/// Base config pointing at unreachable Polaris / S3.
fn offline_config_toml(catalog_discovery: &str) -> String {
    format!(
        r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "http://127.0.0.1:9/unused"
warehouse = "test_wh"

[query]
catalog_discovery = "{catalog_discovery}"

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#
    )
}

// ---------------------------------------------------------------------------
// Offline: config parsing
// ---------------------------------------------------------------------------

/// `CatalogDiscovery` parses correctly from TOML and defaults to `Static`.
#[test]
fn config_catalog_discovery_defaults_to_static() {
    use sqe_core::config::CatalogDiscovery;

    // Default (omitted) is Static.
    let toml = r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0
[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"
[catalog]
catalog_url = "http://127.0.0.1:9/unused"
warehouse = "test_wh"
[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#;
    let cfg = parse_config(toml);
    assert_eq!(
        cfg.query.catalog_discovery,
        CatalogDiscovery::Static,
        "omitting catalog_discovery must default to Static"
    );

    let cfg_pa = parse_config(&offline_config_toml("polaris-auto"));
    assert_eq!(
        cfg_pa.query.catalog_discovery,
        CatalogDiscovery::PolarisAuto,
        "polaris-auto must deserialize to PolarisAuto"
    );

    let cfg_s = parse_config(&offline_config_toml("static"));
    assert_eq!(
        cfg_s.query.catalog_discovery,
        CatalogDiscovery::Static,
        "static must deserialize to Static"
    );
}

// ---------------------------------------------------------------------------
// Offline: pre-flight accepts known catalog regardless of mode
// ---------------------------------------------------------------------------

/// When the catalog qualifier IS registered (`iceberg` from the legacy
/// `[catalog]` block), the pre-flight must not produce "unknown catalog"
/// regardless of `catalog_discovery` mode.
#[tokio::test(flavor = "multi_thread")]
async fn polaris_auto_known_catalog_passes_pre_flight() {
    let config = parse_config(&offline_config_toml("polaris-auto"));
    let h = make_handler(config);
    let session = fake_session();

    let result = h
        .execute(&session, "SELECT * FROM iceberg.some_ns.some_tbl", None)
        .await;

    if let Err(err) = result {
        let msg = err.to_string();
        assert!(
            !msg.contains("unknown catalog"),
            "known catalog 'iceberg' must not trip the pre-flight check: {msg}"
        );
    }
}

/// Regression for the Flight `get_flight_info` path: `get_schema` plans the
/// result schema WITHOUT executing, so it must run the same
/// `preflight_resolve_catalogs` step `execute()` does. The reported bug was a
/// 3-part id into a polaris-auto workspace catalog (`ws_*`) failing "table not
/// found" during planning because `get_schema` skipped pre-flight (and so
/// skipped the lazy Polaris discovery that registers the warehouse).
///
/// We prove the wiring deterministically in STATIC mode, where pre-flight
/// rejects an unknown 3-part catalog with the bespoke "unknown catalog" error
/// using NO network IO (it returns before `create_session_context`). Before the
/// fix, `get_schema` skipped pre-flight and fell through to
/// `create_session_context`, whose error never contains "unknown catalog" -- so
/// this assertion is red without the fix and green with it. The polaris-auto
/// discovery half (the live reproduction) is covered by
/// `polaris_auto_get_schema_lazy_hit`.
#[tokio::test(flavor = "multi_thread")]
async fn get_schema_runs_pre_flight_unknown_catalog() {
    let config = parse_config(&offline_config_toml("static"));
    let h = make_handler(config);
    let session = fake_session();

    let err = h
        .get_schema(&session, "SELECT * FROM ws_undeclared.some_ns.some_tbl")
        .await
        .expect_err("unknown 3-part catalog must error in static mode");

    let msg = err.to_string();
    assert!(
        msg.contains("unknown catalog"),
        "get_schema must run pre-flight: expected the bespoke 'unknown catalog' \
         error (returned with no network IO), got: {msg}"
    );
    assert!(
        msg.contains("ws_undeclared"),
        "pre-flight error must name the unknown catalog: {msg}"
    );
}

/// Bare 1-part names never hit the pre-flight check. Mirrors
/// `unqualified_name_skips_pre_flight` in `multi_catalog_routing_test.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn static_mode_unqualified_query_passes_pre_flight() {
    let config = parse_config(&offline_config_toml("static"));
    let h = make_handler(config);
    let session = fake_session();

    let result = h.execute(&session, "SELECT * FROM foo", None).await;
    if let Err(err) = result {
        let msg = err.to_string();
        assert!(
            !msg.contains("unknown catalog"),
            "unqualified name must not trip the pre-flight check: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// Live-stack helpers
// ---------------------------------------------------------------------------

fn test_config_path() -> String {
    crate::common::test_config_path()
}

/// Build a `SqeConfig` for the live stack with `catalog_discovery = "polaris-auto"`.
/// Only `test_warehouse` is a static catalog; `discovery_test_wh` is NOT listed.
fn polaris_auto_config() -> SqeConfig {
    // Mutate after loading to avoid creating a duplicate `[query]` TOML header
    // (sqe-test.toml already has a `[query]` block).
    let mut cfg = sqe_core::SqeConfig::load(&test_config_path()).expect("load test config");
    cfg.query.catalog_discovery = sqe_core::config::CatalogDiscovery::PolarisAuto;
    cfg
}

/// Authenticate against the live Polaris as `root`.
async fn live_session() -> sqe_core::Session {
    let config = sqe_core::SqeConfig::load(&test_config_path()).expect("load test config");
    let auth = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("create authenticator");
    auth.authenticate("root", "")
        .await
        .expect("authenticate as root")
}

/// Build a `SqeConfig` that points the legacy `[catalog]` block at
/// `discovery_test_wh`. Used for seeding: `CREATE TABLE disc_ns.probe_t AS ...`
/// lands in `discovery_test_wh` because `WriteHandler::create_catalog_bridge`
/// always uses `config.catalog`.
fn seed_config() -> SqeConfig {
    let toml = r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://localhost:18181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
catalog_url = "http://localhost:18181/api/catalog"
warehouse = "discovery_test_wh"

[storage]
s3_endpoint = "http://localhost:19000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
"#;
    toml::from_str::<SqeConfig>(toml).expect("seed config parses")
}

/// Seed `disc_ns.probe_t` in `discovery_test_wh`. Idempotent.
///
/// Note: after seeding, callers must flush both SESSION_CONTEXT_CACHE and
/// REST_CATALOG_CACHE to prevent the seeding handler's cached context (which
/// has `discovery_test_wh` as its default catalog) from polluting the
/// polaris-auto handler's context.
/// Serializes the live-stack tests. They all share one `discovery_test_wh` +
/// `disc_ns.probe_t` and the process-global REST_CATALOG_CACHE / in-memory
/// Polaris, so running them concurrently races on seed/read. Each live test
/// holds this lock for its duration. (No `serial_test` dependency — a plain
/// async mutex is enough.)
static LIVE_STACK_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn seed_discovery_warehouse() {
    let config = seed_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS disc_ns.probe_t", None)
        .await;

    match handler
        .execute(
            &session,
            "CREATE TABLE disc_ns.probe_t AS SELECT 42 as id, 'discovered' as label",
            None,
        )
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => panic!("seed probe_t in discovery_test_wh: {e}"),
    }

    // Flush caches so subsequent tests build a fresh context. SESSION_CONTEXT_CACHE
    // is keyed by (username, token_hash) which is the same for all handlers in
    // the same process, so we must evict after seeding to avoid stale state.
    sqe_coordinator::session_context::invalidate_all_session_caches().await;
}

// ---------------------------------------------------------------------------
// Live-stack: static mode rejects undeclared warehouse (PASSES)
// ---------------------------------------------------------------------------

/// Static mode + live stack: `discovery_test_wh` exists in Polaris but is NOT
/// in `[catalogs.*]`. With `catalog_discovery = "static"` the warehouse must
/// NOT be discovered -- "unknown catalog" is returned immediately.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn static_mode_rejects_undeclared_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    // Mutate after loading to avoid duplicate [query] header.
    let mut config = sqe_core::SqeConfig::load(&test_config_path()).expect("load test config");
    config.query.catalog_discovery = sqe_core::config::CatalogDiscovery::Static;
    let handler = make_handler(config);
    let session = live_session().await;

    let err = handler
        .execute(
            &session,
            "SELECT id FROM discovery_test_wh.disc_ns.probe_t",
            None,
        )
        .await
        .expect_err("static mode must reject even an existing warehouse not in [catalogs.*]");

    let msg = err.to_string();
    assert!(
        msg.contains("unknown catalog"),
        "static mode must say 'unknown catalog': {msg}"
    );
    assert!(
        msg.contains("discovery_test_wh"),
        "static mode error must name the unknown catalog: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Live-stack: lazy discovery (hit / miss / reuse)
// ---------------------------------------------------------------------------

/// Lazy hit: `discovery_test_wh` is not in `[catalogs.*]`. With
/// `catalog_discovery = "polaris-auto"` it is lazily discovered at query time
/// and the SELECT returns the seeded row from the DISCOVERED warehouse (not the
/// default `test_warehouse`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_lazy_hit() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let batches = handler
        .execute(
            &session,
            "SELECT id, label FROM discovery_test_wh.disc_ns.probe_t",
            None,
        )
        .await
        .expect("polaris-auto should discover the warehouse and return rows");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "probe_t has exactly one row");

    let batch = &batches[0];
    let id_arr = crate::common::i64s(batch.column_by_name("id").expect("'id' column must exist"));
    assert_eq!(id_arr.value(0), 42);

    let label_arr = crate::common::utf8(
        batch
            .column_by_name("label")
            .expect("'label' column must exist"),
    );
    assert_eq!(label_arr.value(0), "discovered");
}

/// `SHOW CREATE TABLE cat.ns.t` as the first statement on a catalog the
/// session has not attached yet discovers it, as a SELECT does. Before, the
/// pre-flight found no catalog in the statement and reported the table
/// missing (#921).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn catalog_ops_show_create_table_on_a_fresh_context() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let handler = make_handler(polaris_auto_config());
    let session = live_session().await;
    let sql = "SHOW CREATE TABLE discovery_test_wh.disc_ns.probe_t";
    let batches = handler
        .execute(&session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let ddl = arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render")
        .to_string();
    assert!(
        ddl.contains("probe_t") && ddl.contains("label"),
        "the DDL names the table and its columns, got:\n{ddl}"
    );
}

/// A maintenance `CALL` on an unqualified `ns.t` resolves in the session
/// catalog, as a SELECT of the same name does. Before, it always used the
/// configured default warehouse, so a Trino client with `catalog=<sibling>`
/// maintained (or failed to find) a table in the wrong warehouse.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn maintenance_call_uses_the_session_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let handler = make_handler(polaris_auto_config());
    let call = "CALL system.table_health(table => 'disc_ns.probe_t')";

    let mut in_sibling = live_session().await;
    in_sibling.default_catalog = Some("discovery_test_wh".to_string());
    let batches = handler
        .execute(&in_sibling, call, None)
        .await
        .expect("table_health must find the table in the session catalog");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "table_health reports the table");

    let err = handler
        .execute(&live_session().await, call, None)
        .await
        .expect_err("without the session catalog the default warehouse has no disc_ns");
    let msg = err.to_string();
    assert!(
        msg.contains("Failed to load table") && msg.contains("probe_t"),
        "the default-warehouse call must fail to find the table, got: {msg}"
    );
}

/// `DROP TABLE` with a one- or two-part name drops from the session catalog
/// and schema, as INSERT and CTAS resolve them. Before, it went to the
/// default warehouse, where `IF EXISTS` swallowed "not found" and nothing
/// was dropped.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn drop_table_uses_the_session_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let handler = make_handler(polaris_auto_config());
    let qualified = live_session().await;
    let mut scoped = live_session().await;
    scoped.default_catalog = Some("discovery_test_wh".to_string());
    scoped.default_schema = Some("disc_ns".to_string());

    for (table, drop) in [
        ("drop_probe_one", "DROP TABLE IF EXISTS drop_probe_one"),
        (
            "drop_probe_two",
            "DROP TABLE IF EXISTS disc_ns.drop_probe_two",
        ),
    ] {
        let full = format!("discovery_test_wh.disc_ns.{table}");
        handler
            .execute(&qualified, &format!("DROP TABLE IF EXISTS {full}"), None)
            .await
            .unwrap_or_else(|e| panic!("pre-clean {full}: {e}"));
        handler
            .execute(
                &qualified,
                &format!("CREATE TABLE {full} AS SELECT 1 AS id"),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("create {full}: {e}"));

        handler
            .execute(&scoped, drop, None)
            .await
            .unwrap_or_else(|e| panic!("{drop}: {e}"));

        let err = handler
            .execute(&qualified, &format!("SELECT id FROM {full}"), None)
            .await
            .expect_err("the session-scoped DROP must have dropped the table");
        assert!(
            err.to_string().contains(table),
            "{full} must be gone after `{drop}`, got: {err}"
        );
    }
}

/// #916: ALTER TABLE, RENAME, CREATE VIEW and DROP VIEW with a one-part name
/// act on the session catalog and schema, as DROP TABLE does. Before, these
/// `catalog_ops` paths loaded the table from the default warehouse, so the
/// statement failed with "does not exist" or changed a same-named table there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn catalog_ops_ddl_uses_the_session_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let handler = make_handler(polaris_auto_config());
    let qualified = live_session().await;
    let mut scoped = live_session().await;
    scoped.default_catalog = Some("discovery_test_wh".to_string());
    scoped.default_schema = Some("disc_ns".to_string());

    let run = |session: &Session, sql: String| {
        let handler = &handler;
        let session = session.clone();
        async move {
            handler
                .execute(&session, &sql, None)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
        }
    };
    let render = |batches: &[arrow::record_batch::RecordBatch]| {
        arrow::util::pretty::pretty_format_batches(batches)
            .expect("render")
            .to_string()
    };

    let full = "discovery_test_wh.disc_ns.ops_probe";
    let renamed = "discovery_test_wh.disc_ns.ops_probe_renamed";
    for t in [full, renamed] {
        run(&qualified, format!("DROP TABLE IF EXISTS {t}")).await;
    }
    run(
        &qualified,
        "DROP VIEW IF EXISTS discovery_test_wh.disc_ns.ops_probe_v".to_string(),
    )
    .await;
    run(&qualified, format!("CREATE TABLE {full} AS SELECT 1 AS id")).await;

    // alter_table_schema
    run(
        &scoped,
        "ALTER TABLE ops_probe ADD COLUMN extra INT".to_string(),
    )
    .await;
    let described = render(&run(&qualified, format!("DESCRIBE {full}")).await);
    assert!(
        described.contains("extra"),
        "ADD COLUMN must land on {full}, got:\n{described}"
    );

    // set_table_properties
    run(
        &scoped,
        "ALTER TABLE ops_probe SET TBLPROPERTIES ('probe.key' = 'probe-916')".to_string(),
    )
    .await;
    // SHOW CREATE TABLE on the discovered catalog, on a fresh context (#921).
    sqe_coordinator::session_context::invalidate_all_session_caches().await;
    let ddl = render(&run(&qualified, format!("SHOW CREATE TABLE {full}")).await);
    assert!(
        ddl.contains("probe-916"),
        "SET TBLPROPERTIES must land on {full}, got:\n{ddl}"
    );

    // create_view / drop_view
    run(
        &scoped,
        "CREATE VIEW ops_probe_v AS SELECT id FROM ops_probe".to_string(),
    )
    .await;
    let rows = render(
        &run(
            &qualified,
            "SELECT id FROM discovery_test_wh.disc_ns.ops_probe_v".to_string(),
        )
        .await,
    );
    assert!(
        rows.contains('1'),
        "the view must exist in the session catalog, got:\n{rows}"
    );
    run(&scoped, "DROP VIEW ops_probe_v".to_string()).await;
    handler
        .execute(
            &qualified,
            "SELECT id FROM discovery_test_wh.disc_ns.ops_probe_v",
            None,
        )
        .await
        .expect_err("DROP VIEW must drop the view in the session catalog");

    // rename_table
    run(
        &scoped,
        "ALTER TABLE ops_probe RENAME TO ops_probe_renamed".to_string(),
    )
    .await;
    run(&qualified, format!("SELECT id FROM {renamed}")).await;
    run(&qualified, format!("DROP TABLE IF EXISTS {renamed}")).await;
}

/// `COMMENT ON COLUMN cat.ns.t.c` and `COMMENT ON TABLE cat.ns.t` commit to
/// the catalog the name qualifies, not the session default. Before, both went
/// to the default warehouse and failed with "does not exist" on a table a
/// SELECT of the same name reads.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn comment_on_a_discovered_catalog_table() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let handler = make_handler(polaris_auto_config());
    let session = live_session().await;
    for sql in [
        "COMMENT ON COLUMN discovery_test_wh.disc_ns.probe_t.label IS 'probe label'",
        "COMMENT ON TABLE discovery_test_wh.disc_ns.probe_t IS 'probe table'",
    ] {
        handler
            .execute(&session, sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let batches = handler
        .execute(&session, "DESCRIBE discovery_test_wh.disc_ns.probe_t", None)
        .await
        .expect("describe the commented table");
    let rendered = arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render")
        .to_string();
    assert!(
        rendered.contains("probe label"),
        "the column comment must land on the discovered table, got:\n{rendered}"
    );
}

/// #334: row-level mutations against a DISCOVERED (non-default) catalog must
/// load and commit against the discovered warehouse, not the session-default
/// one. Before the fix the UPDATE/DELETE/MERGE handlers used the passed
/// `session_catalog` (the configured default `test_warehouse`) instead of
/// resolving the target catalog from the 3-part name the way INSERT does, so
/// loading the target failed with "table does not exist" even though
/// INSERT/SELECT/CTAS resolved the same table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_mutations_on_discovered_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();

    // Seed a mutable table in `discovery_test_wh` via a handler whose default
    // catalog IS the discovery warehouse, so the rows land there.
    {
        let seed_handler = make_handler(seed_config());
        let session = live_session().await;
        let _ = seed_handler
            .execute(&session, "DROP TABLE IF EXISTS disc_ns.mut_t", None)
            .await;
        seed_handler
            .execute(
                &session,
                "CREATE TABLE disc_ns.mut_t (id BIGINT, amt BIGINT)",
                None,
            )
            .await
            .expect("create disc_ns.mut_t in discovery_test_wh");
        seed_handler
            .execute(
                &session,
                "INSERT INTO disc_ns.mut_t VALUES (1, 10), (2, 20), (3, 30)",
                None,
            )
            .await
            .expect("insert into disc_ns.mut_t");
    }
    // Flush caches so the polaris-auto handler builds a fresh context whose
    // default catalog is `test_warehouse`, not the seed handler's discovery wh.
    sqe_coordinator::session_context::invalidate_all_session_caches().await;

    // Operate through a polaris-auto handler whose DEFAULT catalog is
    // `test_warehouse`, addressing the table by its 3-part discovered name.
    let handler = make_handler(polaris_auto_config());
    let session = live_session().await;

    handler
        .execute(
            &session,
            "UPDATE discovery_test_wh.disc_ns.mut_t SET amt = amt + 1 WHERE id = 1",
            None,
        )
        .await
        .expect("UPDATE on a discovered-catalog table must succeed (#334)");

    handler
        .execute(
            &session,
            "DELETE FROM discovery_test_wh.disc_ns.mut_t WHERE id = 3",
            None,
        )
        .await
        .expect("DELETE on a discovered-catalog table must succeed (#334)");

    // Net effect: row 3 deleted (2 rows remain) AND row 1's amt bumped to 11.
    // Asserting the value (not just the row count) proves the UPDATE actually
    // committed against the discovered catalog -- a silent no-op UPDATE (no
    // error, no write) would still leave 2 rows and pass a count-only check.
    let batches = handler
        .execute(
            &session,
            "SELECT id, amt FROM discovery_test_wh.disc_ns.mut_t ORDER BY id",
            None,
        )
        .await
        .expect("read back after mutations");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "DELETE should have removed exactly one row");

    // Collect (id, amt) across all batches and check both mutations landed.
    let mut pairs: Vec<(i64, i64)> = Vec::new();
    for b in &batches {
        let id = b
            .column_by_name("id")
            .expect("'id' column")
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("'id' is Int64");
        let amt = b
            .column_by_name("amt")
            .expect("'amt' column")
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("'amt' is Int64");
        for i in 0..b.num_rows() {
            pairs.push((id.value(i), amt.value(i)));
        }
    }
    assert!(
        !pairs.iter().any(|(id, _)| *id == 3),
        "DELETE should have removed id=3: {pairs:?}"
    );
    assert_eq!(
        pairs.iter().find(|(id, _)| *id == 1).map(|(_, amt)| *amt),
        Some(11),
        "UPDATE should have committed amt=11 for id=1 (10 + 1): {pairs:?}"
    );

    let _ = handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS discovery_test_wh.disc_ns.mut_t",
            None,
        )
        .await;
}

/// Flight `get_flight_info` regression (the actual reported bug): `get_schema`
/// on a discovered warehouse must return the table's schema, not "table not
/// found". This is the schema-planning twin of `polaris_auto_lazy_hit` and the
/// exact path `get_flight_info_statement` takes before `do_get` ever runs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_get_schema_lazy_hit() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let schema = handler
        .get_schema(
            &session,
            "SELECT id, label FROM discovery_test_wh.disc_ns.probe_t",
        )
        .await
        .expect("get_schema must discover the warehouse and return the schema, not NOT_FOUND");

    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "label"],
        "get_schema must return probe_t's columns from the discovered warehouse"
    );
}

/// Miss: a warehouse name that does not exist in Polaris returns "unknown
/// catalog". The probe's `list_namespaces` hits Polaris `/v1/config` for the
/// (now warehouse-keyed) catalog, gets a 404, `discover_catalog_provider`
/// returns None, and the pre-flight check produces "unknown catalog" with no
/// Polaris HTTP details leaked to the caller.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_nonexistent_warehouse_returns_unknown_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let err = handler
        .execute(
            &session,
            "SELECT * FROM totally_nonexistent_wh_xyz.some_ns.some_tbl",
            None,
        )
        .await
        .expect_err("non-existent warehouse must error");

    let msg = err.to_string();
    assert!(
        msg.contains("unknown catalog"),
        "error must say 'unknown catalog' (Polaris probe silent failure): {msg}"
    );
    assert!(
        msg.contains("totally_nonexistent_wh_xyz"),
        "error must name the unknown catalog: {msg}"
    );
    assert!(
        !msg.contains("reqwest") && !msg.contains("connection"),
        "Polaris error details must not reach the caller: {msg}"
    );
}

/// In-session reuse: the same discovered warehouse referenced twice in one
/// session succeeds both times. The second reference reuses the provider
/// registered into the session context by the first discovery.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_in_session_reuse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let batches1 = handler
        .execute(
            &session,
            "SELECT id FROM discovery_test_wh.disc_ns.probe_t",
            None,
        )
        .await
        .expect("first query must succeed");
    let rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows1, 1, "first query: probe_t has one row");

    let batches2 = handler
        .execute(
            &session,
            "SELECT label FROM discovery_test_wh.disc_ns.probe_t",
            None,
        )
        .await
        .expect("second query in same session must also succeed");
    let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows2, 1, "second query: probe_t still has one row");

    let label_arr =
        crate::common::utf8(batches2[0].column_by_name("label").expect("'label' column"));
    assert_eq!(label_arr.value(0), "discovered");
}

/// Write into a discovered warehouse: with `catalog_discovery = "polaris-auto"`
/// and the default `[catalog]` warehouse = `test_warehouse`, an
/// `INSERT INTO discovery_test_wh.disc_ns.probe_t …` must resolve its target in
/// the DISCOVERED `discovery_test_wh`, not the default. Before the write-path
/// fix this failed because `create_catalog_bridge` always used `config.catalog`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_insert_lands_in_discovered_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await; // discovery_test_wh.disc_ns.probe_t = {(42,"discovered")}

    let config = polaris_auto_config(); // default warehouse = test_warehouse
    let handler = make_handler(config);
    let session = live_session().await;

    handler
        .execute(
            &session,
            "INSERT INTO discovery_test_wh.disc_ns.probe_t SELECT 99 as id, 'inserted' as label",
            None,
        )
        .await
        .expect("polaris-auto INSERT must resolve the target in the discovered warehouse");

    let batches = handler
        .execute(
            &session,
            "SELECT id FROM discovery_test_wh.disc_ns.probe_t",
            None,
        )
        .await
        .expect("read back from discovered warehouse");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 2,
        "seed row + inserted row both live in discovery_test_wh"
    );
}

/// CatalogOps DDL into a discovered (polaris-auto) warehouse: CREATE SCHEMA +
/// CREATE TABLE + DROP TABLE must all resolve to the DISCOVERED `discovery_test_wh`,
/// not the default. Before this fix, CatalogOps::create_catalog_bridge always used
/// the default warehouse, so `CREATE SCHEMA ws.x` landed in the default and
/// `DROP TABLE ws.x.t` reported "table does not exist".
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_ddl_resolves_discovered_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    crate::common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config(); // default warehouse = test_warehouse
    let handler = make_handler(config);
    let session = live_session().await;

    // CREATE SCHEMA into the discovered catalog (the dbt `create_schema` path).
    handler
        .execute(
            &session,
            "CREATE SCHEMA IF NOT EXISTS discovery_test_wh.ddl_ns",
            None,
        )
        .await
        .expect("CREATE SCHEMA must create the namespace in the discovered warehouse");

    // CREATE TABLE into that freshly-created namespace must succeed (the dbt seed
    // failure was "namespace does not exist" because the schema landed elsewhere).
    handler
        .execute(
            &session,
            "CREATE TABLE discovery_test_wh.ddl_ns.t AS SELECT 1 AS x",
            None,
        )
        .await
        .expect("CREATE TABLE into the discovered-warehouse namespace must succeed");

    // DROP TABLE must resolve to the same discovered catalog (CatalogOps routing).
    handler
        .execute(&session, "DROP TABLE discovery_test_wh.ddl_ns.t", None)
        .await
        .expect("DROP TABLE must resolve the target in the discovered warehouse");

    handler
        .execute(&session, "DROP SCHEMA discovery_test_wh.ddl_ns", None)
        .await
        .expect("DROP SCHEMA must resolve the target in the discovered warehouse");
}

/// Engine reads for discovered warehouses on the live stack
/// (`[query] discovery_engine_read = "template"`).
///
/// Each test makes its own Polaris principals and warehouses under the
/// `warehouse-engine` bucket, so runs never share grants or cached tokens.
/// The person is metadata-only: a read of table data can only have moved
/// with keys the engine fetched. The policy store is the in-memory one with
/// no policy, so it grants every `select`. The engine identity and its
/// audit log are process-wide, so each test also holds
/// [`crate::common::ENGINE_WIRING_LOCK`].
mod engine_read {
    use super::*;

    const POLARIS: &str = "http://localhost:18181";
    const ALLOWLIST: &str = "eng_*";
    /// Enough to list and load a table, never to read its data.
    const METADATA: &[&str] = &[
        "CATALOG_READ_PROPERTIES",
        "NAMESPACE_LIST",
        "NAMESPACE_READ_PROPERTIES",
        "TABLE_LIST",
        "TABLE_READ_PROPERTIES",
    ];

    struct Creds {
        name: String,
        id: String,
        secret: String,
    }

    /// The Polaris management API as `root`.
    struct Admin {
        http: reqwest::Client,
        token: String,
    }

    impl Admin {
        async fn new() -> Self {
            let http = reqwest::Client::new();
            let body: serde_json::Value = http
                .post(format!("{POLARIS}/api/catalog/v1/oauth/tokens"))
                .form(&[
                    ("grant_type", "client_credentials"),
                    ("client_id", "root"),
                    ("client_secret", "s3cr3t"),
                    ("scope", "PRINCIPAL_ROLE:ALL"),
                ])
                .send()
                .await
                .expect("root token request")
                .json()
                .await
                .expect("root token body");
            let token = body["access_token"]
                .as_str()
                .expect("access_token")
                .to_string();
            Self { http, token }
        }

        /// `method path body`; 2xx or 409 (already there) passes.
        async fn call(
            &self,
            method: reqwest::Method,
            path: &str,
            body: serde_json::Value,
        ) -> serde_json::Value {
            let response = self
                .http
                .request(method.clone(), format!("{POLARIS}{path}"))
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            assert!(
                status.is_success() || status == reqwest::StatusCode::CONFLICT,
                "{method} {path}: {status} {text}"
            );
            serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
        }

        /// A new principal with its own principal role.
        async fn principal(&self, name: &str) -> Creds {
            let created = self
                .call(
                    reqwest::Method::POST,
                    "/api/management/v1/principals",
                    serde_json::json!({ "principal": { "name": name } }),
                )
                .await;
            let role = format!("{name}_role");
            self.call(
                reqwest::Method::POST,
                "/api/management/v1/principal-roles",
                serde_json::json!({ "principalRole": { "name": role } }),
            )
            .await;
            self.call(
                reqwest::Method::PUT,
                &format!("/api/management/v1/principals/{name}/principal-roles"),
                serde_json::json!({ "principalRole": { "name": role } }),
            )
            .await;
            let credentials = &created["credentials"];
            Creds {
                name: name.to_string(),
                id: credentials["clientId"]
                    .as_str()
                    .expect("clientId")
                    .to_string(),
                secret: credentials["clientSecret"]
                    .as_str()
                    .expect("clientSecret")
                    .to_string(),
            }
        }

        /// A warehouse at `s3://warehouse-engine/<name>/` that `root` manages,
        /// with namespace `ns`.
        async fn warehouse(&self, name: &str) {
            let base = format!("s3://warehouse-engine/{name}/");
            self.call(
                reqwest::Method::POST,
                "/api/management/v1/catalogs",
                serde_json::json!({ "catalog": {
                    "name": name,
                    "type": "INTERNAL",
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [base],
                        "endpoint": "http://localhost:19000",
                        "endpointInternal": "http://rustfs:9000",
                        "pathStyleAccess": true
                    },
                    "properties": {
                        "default-base-location": base,
                        "polaris.config.drop-with-purge.enabled": "true"
                    }
                }}),
            )
            .await;
            self.grant(
                name,
                "service_admin",
                "catalog_admin",
                &["CATALOG_MANAGE_CONTENT"],
            )
            .await;
            self.call(
                reqwest::Method::POST,
                &format!("/api/catalog/v1/{name}/namespaces"),
                serde_json::json!({ "namespace": ["ns"] }),
            )
            .await;
        }

        /// Give `principal_role` a catalog role `catalog_role` on `warehouse`
        /// holding `privileges`.
        async fn grant(
            &self,
            warehouse: &str,
            principal_role: &str,
            catalog_role: &str,
            privileges: &[&str],
        ) {
            let roles = format!("/api/management/v1/catalogs/{warehouse}/catalog-roles");
            self.call(
                reqwest::Method::POST,
                &roles,
                serde_json::json!({ "catalogRole": { "name": catalog_role } }),
            )
            .await;
            for privilege in privileges {
                self.call(
                    reqwest::Method::PUT,
                    &format!("{roles}/{catalog_role}/grants"),
                    serde_json::json!({ "grant": { "type": "catalog", "privilege": privilege } }),
                )
                .await;
            }
            self.call(
                reqwest::Method::PUT,
                &format!(
                    "/api/management/v1/principal-roles/{principal_role}/catalog-roles/{warehouse}"
                ),
                serde_json::json!({ "catalogRole": { "name": catalog_role } }),
            )
            .await;
        }

        async fn grant_to(&self, warehouse: &str, who: &Creds, privileges: &[&str]) {
            let role = format!("{}_role", who.name);
            self.grant(warehouse, &role, &format!("{}_cr", who.name), privileges)
                .await;
        }
    }

    /// One run: a metadata-only person, an engine principal, and an audit
    /// file. Names carry a fresh suffix.
    struct Run {
        id: String,
        admin: Admin,
        person: Creds,
        engine: Creds,
        audit_dir: tempfile::TempDir,
    }

    impl Run {
        async fn new() -> Self {
            crate::common::init_tracing();
            let id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
            let admin = Admin::new().await;
            let person = admin.principal(&format!("meta_person_{id}")).await;
            let engine = admin.principal(&format!("engine_{id}")).await;
            // The person's session also builds the configured default catalog.
            admin.grant_to("test_warehouse", &person, METADATA).await;
            Self {
                id,
                admin,
                person,
                engine,
                audit_dir: tempfile::tempdir().expect("tempdir"),
            }
        }

        fn warehouse_name(&self, label: &str) -> String {
            format!("eng_{label}_{}", self.id)
        }

        /// A warehouse holding `ns.t = {(7, 'engine')}`; the person gets
        /// metadata only, the engine `engine_privileges` (none: no role).
        async fn warehouse(&self, label: &str, engine_privileges: &[&str]) -> String {
            let name = self.warehouse_name(label);
            self.admin.warehouse(&name).await;
            self.admin.grant_to(&name, &self.person, METADATA).await;
            if !engine_privileges.is_empty() {
                self.admin
                    .grant_to(&name, &self.engine, engine_privileges)
                    .await;
            }
            let mut seed = seed_config();
            seed.catalog.warehouse = name.clone();
            let root = make_handler(seed);
            root.execute(
                &live_session().await,
                "CREATE TABLE ns.t AS SELECT 7 AS id, 'engine' AS label",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("seed {name}.ns.t: {e}"));
            sqe_coordinator::session_context::invalidate_all_session_caches().await;
            name
        }

        fn config(&self, mode: sqe_core::config::DiscoveryEngineRead) -> SqeConfig {
            let mut config = polaris_auto_config();
            config.auth.client_id = self.person.id.clone();
            config.auth.client_secret = self.person.secret.clone().into();
            config.query.discovery_engine_read = mode;
            config.query.discovery_engine_warehouses = vec![ALLOWLIST.into()];
            config.catalog.require_vended_credentials = true;
            config.catalog.credentials =
                Some(sqe_core::config::CatalogAuthConfig::ClientCredentials {
                    token_endpoint: config.auth.token_endpoint.clone(),
                    client_id: self.engine.id.clone(),
                    client_secret: self.engine.secret.clone().into(),
                    client_secret_file: None,
                    scope: None,
                });
            // No static key: data moves only with vended keys.
            config.storage.s3_access_key = String::new();
            config.storage.s3_secret_key = sqe_core::SecretString::new(String::new());
            config
        }

        /// The person's session and a handler with the engine identity
        /// installed against an open in-memory policy store.
        async fn handler(
            &self,
            mode: sqe_core::config::DiscoveryEngineRead,
        ) -> (Session, sqe_coordinator::QueryHandler) {
            let config = self.config(mode);
            let session = sqe_auth::Authenticator::new(&config.auth)
                .await
                .expect("authenticator")
                .authenticate(&self.person.name, "")
                .await
                .expect("the metadata-only person authenticates");
            let store: Arc<dyn sqe_policy::PolicyStore> =
                Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
            let audit = Arc::new(
                sqe_metrics::audit::AuditLogger::new(
                    self.audit_path().to_str().expect("utf-8 path"),
                )
                .expect("audit logger opens"),
            );
            sqe_coordinator::engine_identity_wiring::install(
                Some(Arc::clone(&store)),
                config.policy.mask_precedence,
                Some(audit),
            );
            let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(
                sqe_policy::plan_rewriter::PolicyPlanRewriter::new(Arc::clone(&store))
                    .with_metadata_target(sqe_catalog::iceberg_metadata_tvf::metadata_tvf_target)
                    .with_file_table(sqe_catalog::file_tvf_prepare::is_file_table_provider),
            );
            let tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
                &config.query_history,
            ));
            let handler = sqe_coordinator::QueryHandler::new(
                policy,
                Some(store),
                config,
                None,
                None,
                None,
                None,
                tracker,
                None,
                None,
                None,
                sqe_coordinator::RuntimeCatalogRegistry::default(),
                sqe_core::SecretStore::default(),
            )
            .expect("QueryHandler::new");
            (session, handler)
        }

        fn audit_path(&self) -> std::path::PathBuf {
            self.audit_dir.path().join("audit.jsonl")
        }

        /// Audit lines that name `warehouse`.
        fn audit_about(&self, warehouse: &str) -> Vec<String> {
            std::fs::read_to_string(self.audit_path())
                .unwrap_or_default()
                .lines()
                .filter(|l| l.contains(warehouse))
                .map(str::to_string)
                .collect()
        }
    }

    impl Drop for Run {
        fn drop(&mut self) {
            sqe_coordinator::engine_identity_wiring::install(
                None,
                sqe_core::config::MaskPrecedence::default(),
                None,
            );
        }
    }

    fn engine_read_privileges() -> Vec<&'static str> {
        let mut p = METADATA.to_vec();
        p.push("TABLE_READ_DATA");
        p
    }

    fn rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    use sqe_core::config::DiscoveryEngineRead::{Off, Template};

    /// A person with metadata access only reads a discovered warehouse's
    /// data through the engine; with the opt-in off the same read fails.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
    async fn a_metadata_only_person_reads_through_the_engine() {
        let _live = LIVE_STACK_LOCK.lock().await;
        let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
        let run = Run::new().await;
        let wh = run.warehouse("read", &engine_read_privileges()).await;
        let sql = format!("SELECT id, label FROM {wh}.ns.t");

        let (session, off) = run.handler(Off).await;
        off.execute(&session, &sql, None)
            .await
            .expect_err("off: the metadata-only person cannot read the data");
        sqe_coordinator::session_context::invalidate_all_session_caches().await;

        let (session, handler) = run.handler(Template).await;
        let batches = handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("template: {sql}: {e}"));
        assert_eq!(rows(&batches), 1);
        let audit = run.audit_about(&wh);
        assert!(
            audit
                .iter()
                .any(|l| l.contains(&format!("discovered warehouse {wh}: engine read proved"))),
            "one proof event: {audit:#?}"
        );
        assert!(
            audit
                .iter()
                .any(|l| l.contains(&format!("engine key fetch: discovered warehouse {wh}"))),
            "the key fetch is marked discovered: {audit:#?}"
        );
    }

    /// A warehouse created in Polaris after the handler and session exist is
    /// read through the engine with no restart.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
    async fn a_warehouse_created_during_the_run_is_readable() {
        let _live = LIVE_STACK_LOCK.lock().await;
        let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
        let run = Run::new().await;
        let (session, handler) = run.handler(Template).await;
        let wh = run.warehouse_name("late");
        let sql = format!("SELECT id FROM {wh}.ns.t");
        handler
            .execute(&session, &sql, None)
            .await
            .expect_err("the warehouse does not exist yet");

        assert_eq!(run.warehouse("late", &engine_read_privileges()).await, wh);
        let batches = handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql} after creation: {e}"));
        assert_eq!(rows(&batches), 1);
    }

    /// The engine has no role on an allowlisted warehouse. Polaris answers
    /// any principal's `/v1/config`, so the proof passes; the grant check is
    /// Polaris's `/credentials`, which refuses the engine. The read fails
    /// closed with no rows and does not fall back to the person's keys.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
    async fn an_engine_without_a_role_fails_closed() {
        let _live = LIVE_STACK_LOCK.lock().await;
        let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
        let run = Run::new().await;
        let wh = run.warehouse("norole", &[]).await;
        let (session, handler) = run.handler(Template).await;
        let err = handler
            .execute(&session, &format!("SELECT id FROM {wh}.ns.t"), None)
            .await
            .expect_err("no rows: the engine's key fetch is refused");
        assert!(
            err.to_string().contains("403"),
            "the read fails on the refused key fetch: {err}"
        );
        let audit = run.audit_about(&wh);
        assert!(
            audit
                .iter()
                .any(|l| l.contains(&format!("discovered warehouse {wh}: engine read proved"))),
            "/v1/config proves the catalog, not a grant: {audit:#?}"
        );
        let fetch = audit
            .iter()
            .find(|l| l.contains(&format!("engine key fetch: discovered warehouse {wh}")))
            .unwrap_or_else(|| panic!("one engine key fetch event: {audit:#?}"));
        assert!(
            fetch.contains("\"status\":\"failure\"")
                && fetch.contains("ENGINE_CREDENTIALS_FAILED")
                && fetch.contains("403"),
            "the key fetch is audited as refused: {fetch}"
        );
    }

    /// Writes never borrow the engine: an engine allowed to write cannot
    /// carry a metadata-only person's INSERT, while that person's SELECT on
    /// the same warehouse reads through the engine.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
    async fn a_write_uses_the_persons_delegation() {
        let _live = LIVE_STACK_LOCK.lock().await;
        let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
        let run = Run::new().await;
        let mut engine_privileges = engine_read_privileges();
        engine_privileges.push("TABLE_WRITE_DATA");
        let wh = run.warehouse("write", &engine_privileges).await;
        let (session, handler) = run.handler(Template).await;
        let read = format!("SELECT id FROM {wh}.ns.t");
        assert_eq!(
            rows(
                &handler
                    .execute(&session, &read, None)
                    .await
                    .unwrap_or_else(|e| panic!("{read}: {e}"))
            ),
            1
        );
        handler
            .execute(
                &session,
                &format!("INSERT INTO {wh}.ns.t VALUES (8, 'person')"),
                None,
            )
            .await
            .expect_err("the INSERT runs on the person's delegation, which cannot write");
        sqe_coordinator::session_context::invalidate_all_session_caches().await;
        assert_eq!(
            rows(
                &handler
                    .execute(&session, &read, None)
                    .await
                    .unwrap_or_else(|e| panic!("{read}: {e}"))
            ),
            1,
            "nothing was written"
        );
    }

    /// A signed catalog scope (#908) that leaves the warehouse out refuses
    /// it before any proof call: the read fails and no proof event names it.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
    async fn a_catalog_scope_refuses_before_the_proof() {
        let _live = LIVE_STACK_LOCK.lock().await;
        let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
        let run = Run::new().await;
        let wh = run.warehouse("scoped", &engine_read_privileges()).await;
        let (session, handler) = run.handler(Template).await;
        let scope = sqe_core::catalog_scope::CatalogScope::new(
            session.user.username.clone(),
            "jti-engine-read",
            Utc::now() + Duration::hours(1),
            [(
                "test_warehouse".to_string(),
                sqe_core::catalog_scope::CatalogAccess::ReadOnly,
            )],
        );
        let scoped = session.with_catalog_scope(Some(Arc::new(scope)));
        handler
            .execute(&scoped, &format!("SELECT id FROM {wh}.ns.t"), None)
            .await
            .expect_err("outside the signed scope");
        let audit = run.audit_about(&wh);
        assert!(
            !audit.iter().any(|l| l.contains("engine read")),
            "no proof ran: {audit:#?}"
        );
    }
}

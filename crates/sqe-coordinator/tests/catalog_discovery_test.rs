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
//!   `cargo test -p sqe-coordinator --test catalog_discovery_test -- --ignored`
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

mod common;

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
    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );
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
        .execute(&session, "SELECT * FROM iceberg.some_ns.some_tbl")
        .await;

    if let Err(err) = result {
        let msg = err.to_string();
        assert!(
            !msg.contains("unknown catalog"),
            "known catalog 'iceberg' must not trip the pre-flight check: {msg}"
        );
    }
}

/// Bare 1-part names never hit the pre-flight check. Mirrors
/// `unqualified_name_skips_pre_flight` in `multi_catalog_routing_test.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn static_mode_unqualified_query_passes_pre_flight() {
    let config = parse_config(&offline_config_toml("static"));
    let h = make_handler(config);
    let session = fake_session();

    let result = h.execute(&session, "SELECT * FROM foo").await;
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
    common::test_config_path()
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
        .execute(&session, "DROP TABLE IF EXISTS disc_ns.probe_t")
        .await;

    match handler
        .execute(
            &session,
            "CREATE TABLE disc_ns.probe_t AS SELECT 42 as id, 'discovered' as label",
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
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn static_mode_rejects_undeclared_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();
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
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_lazy_hit() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let batches = handler
        .execute(
            &session,
            "SELECT id, label FROM discovery_test_wh.disc_ns.probe_t",
        )
        .await
        .expect("polaris-auto should discover the warehouse and return rows");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "probe_t has exactly one row");

    let batch = &batches[0];
    let id_arr = batch
        .column_by_name("id")
        .expect("'id' column must exist")
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("'id' is Int64");
    assert_eq!(id_arr.value(0), 42);

    let label_arr = batch
        .column_by_name("label")
        .expect("'label' column must exist")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("'label' is Utf8");
    assert_eq!(label_arr.value(0), "discovered");
}

/// Miss: a warehouse name that does not exist in Polaris returns "unknown
/// catalog". The probe's `list_namespaces` hits Polaris `/v1/config` for the
/// (now warehouse-keyed) catalog, gets a 404, `discover_catalog_provider`
/// returns None, and the pre-flight check produces "unknown catalog" with no
/// Polaris HTTP details leaked to the caller.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_nonexistent_warehouse_returns_unknown_catalog() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let err = handler
        .execute(
            &session,
            "SELECT * FROM totally_nonexistent_wh_xyz.some_ns.some_tbl",
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
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_in_session_reuse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config();
    let handler = make_handler(config);
    let session = live_session().await;

    let batches1 = handler
        .execute(
            &session,
            "SELECT id FROM discovery_test_wh.disc_ns.probe_t",
        )
        .await
        .expect("first query must succeed");
    let rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows1, 1, "first query: probe_t has one row");

    let batches2 = handler
        .execute(
            &session,
            "SELECT label FROM discovery_test_wh.disc_ns.probe_t",
        )
        .await
        .expect("second query in same session must also succeed");
    let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows2, 1, "second query: probe_t still has one row");

    let label_arr = batches2[0]
        .column_by_name("label")
        .expect("'label' column")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("'label' is Utf8");
    assert_eq!(label_arr.value(0), "discovered");
}

/// Write into a discovered warehouse: with `catalog_discovery = "polaris-auto"`
/// and the default `[catalog]` warehouse = `test_warehouse`, an
/// `INSERT INTO discovery_test_wh.disc_ns.probe_t …` must resolve its target in
/// the DISCOVERED `discovery_test_wh`, not the default. Before the write-path
/// fix this failed because `create_catalog_bridge` always used `config.catalog`.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_insert_lands_in_discovered_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();
    seed_discovery_warehouse().await; // discovery_test_wh.disc_ns.probe_t = {(42,"discovered")}

    let config = polaris_auto_config(); // default warehouse = test_warehouse
    let handler = make_handler(config);
    let session = live_session().await;

    handler
        .execute(
            &session,
            "INSERT INTO discovery_test_wh.disc_ns.probe_t SELECT 99 as id, 'inserted' as label",
        )
        .await
        .expect("polaris-auto INSERT must resolve the target in the discovered warehouse");

    let batches = handler
        .execute(&session, "SELECT id FROM discovery_test_wh.disc_ns.probe_t")
        .await
        .expect("read back from discovered warehouse");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "seed row + inserted row both live in discovery_test_wh");
}

/// CatalogOps DDL into a discovered (polaris-auto) warehouse: CREATE SCHEMA +
/// CREATE TABLE + DROP TABLE must all resolve to the DISCOVERED `discovery_test_wh`,
/// not the default. Before this fix, CatalogOps::create_catalog_bridge always used
/// the default warehouse, so `CREATE SCHEMA ws.x` landed in the default and
/// `DROP TABLE ws.x.t` reported "table does not exist".
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn polaris_auto_ddl_resolves_discovered_warehouse() {
    let _live = LIVE_STACK_LOCK.lock().await;
    common::init_tracing();
    seed_discovery_warehouse().await;

    let config = polaris_auto_config(); // default warehouse = test_warehouse
    let handler = make_handler(config);
    let session = live_session().await;

    // CREATE SCHEMA into the discovered catalog (the dbt `create_schema` path).
    handler
        .execute(&session, "CREATE SCHEMA IF NOT EXISTS discovery_test_wh.ddl_ns")
        .await
        .expect("CREATE SCHEMA must create the namespace in the discovered warehouse");

    // CREATE TABLE into that freshly-created namespace must succeed (the dbt seed
    // failure was "namespace does not exist" because the schema landed elsewhere).
    handler
        .execute(
            &session,
            "CREATE TABLE discovery_test_wh.ddl_ns.t AS SELECT 1 AS x",
        )
        .await
        .expect("CREATE TABLE into the discovered-warehouse namespace must succeed");

    // DROP TABLE must resolve to the same discovered catalog (CatalogOps routing).
    handler
        .execute(&session, "DROP TABLE discovery_test_wh.ddl_ns.t")
        .await
        .expect("DROP TABLE must resolve the target in the discovered warehouse");

    handler
        .execute(&session, "DROP SCHEMA discovery_test_wh.ddl_ns")
        .await
        .expect("DROP SCHEMA must resolve the target in the discovered warehouse");
}

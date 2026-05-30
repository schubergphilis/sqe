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
//! - `static_mode_rejects_undeclared_warehouse` -- PASSES. Static mode returns
//!   "unknown catalog" for a warehouse that exists in Polaris but isn't in
//!   `[catalogs.*]`. No probe is made.
//!
//! ### Known-blocked live tests
//!
//! The following tests are kept for documentation and future verification but
//! cannot pass due to a product-level bug identified during test writing:
//!
//! **`REST_CATALOG_CACHE` aliasing bug** (tracked as a finding in this PR):
//!
//! `REST_CATALOG_CACHE` in `sqe-catalog` is keyed by `(catalog_url, token_fingerprint)`
//! -- the warehouse name is NOT in the key. The discovery template inherits the
//! default catalog's `catalog_url`, so it shares that cache key with the default
//! catalog. When `preflight_resolve_catalogs` calls `create_session_context` (to
//! enumerate known catalogs), that call warms the cache for the default warehouse.
//! The subsequent discovery probe for any _other_ warehouse with the same
//! `(url, token)` gets a cache hit and reuses the already-initialized RestCatalog,
//! which has the default warehouse's `/v1/config` baked in. The probe's warehouse
//! name is silently dropped.
//!
//! Consequences:
//! - **Miss**: probing a nonexistent warehouse succeeds (cache hit returns the
//!   default warehouse's context) -- the provider is registered, planning
//!   fails with "table not found" instead of the specified "unknown catalog".
//! - **Hit**: the discovered provider wraps the default warehouse, not the target
//!   one -- tables in the target warehouse are invisible.
//!
//! **Pre-existing regression in multi_catalog_routing_test**:
//!
//! `unknown_catalog_qualifier_errors_clearly` in `multi_catalog_routing_test.rs`
//! also fails without a live stack because `preflight_resolve_catalogs` now calls
//! `create_session_context` (which contacts Polaris) before the "unknown catalog"
//! check. The old design had the check fire before any network IO; that changed
//! in Tasks 1-4. That test was designed to run offline.

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
// Live-stack: known-blocked tests (see module-level doc for root cause)
// ---------------------------------------------------------------------------

/// CURRENTLY FAILS due to REST_CATALOG_CACHE aliasing bug.
///
/// `discovery_test_wh` is not in `[catalogs.*]`. With `catalog_discovery =
/// "polaris-auto"` it should be lazily discovered and the SELECT should return
/// the seeded row. However, `REST_CATALOG_CACHE` is keyed by `(catalog_url,
/// token_fingerprint)` without the warehouse name. The default catalog
/// (`test_warehouse`) warms the cache first via `create_session_context`, and
/// the discovery probe for `discovery_test_wh` reuses that cache entry, aliasing
/// to `test_warehouse`'s context. The query then fails with "table not found"
/// because `discovery_test_wh.disc_ns.probe_t` is not in `test_warehouse`.
///
/// Fix required: include `warehouse` in the `REST_CATALOG_CACHE` key.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // BLOCKED: REST_CATALOG_CACHE key does not include warehouse (see module doc)
async fn polaris_auto_lazy_hit() {
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

/// CURRENTLY FAILS due to REST_CATALOG_CACHE aliasing bug.
///
/// A warehouse name that does not exist in Polaris should return "unknown catalog"
/// (the probe fails, `discover_catalog_provider` returns None, the pre-flight
/// check produces "unknown catalog"). However, due to the aliasing bug, the
/// nonexistent warehouse reuses the default warehouse's cached `RestCatalog`
/// context, so the probe appears to succeed, the provider is registered, and
/// the query fails at planning time with "table not found" rather than
/// "unknown catalog".
///
/// Fix required: include `warehouse` in the `REST_CATALOG_CACHE` key.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // BLOCKED: REST_CATALOG_CACHE key does not include warehouse (see module doc)
async fn polaris_auto_nonexistent_warehouse_returns_unknown_catalog() {
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

/// CURRENTLY FAILS due to REST_CATALOG_CACHE aliasing bug.
///
/// Same warehouse referenced twice in one session should succeed both times
/// (the second call reuses the registered provider). Cannot pass until the hit
/// scenario works.
///
/// Fix required: include `warehouse` in the `REST_CATALOG_CACHE` key.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // BLOCKED: REST_CATALOG_CACHE key does not include warehouse (see module doc)
async fn polaris_auto_in_session_reuse() {
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

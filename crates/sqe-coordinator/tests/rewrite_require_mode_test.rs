//! End-to-end coverage for `CALL system.rewrite_data_files(..., distributed
//! => 'require')` when the coordinator has no healthy workers.
//!
//! Phase 4c Task 5 wired `distribution.mode` routing into
//! `MaintenanceHandler::handle()`'s `RewriteDataFiles` arm: `require` below
//! the healthy-worker floor must return a loud `Err` and must NEVER fall
//! back to a coordinator-local rewrite (that silent fallback is exactly what
//! an operator who explicitly asked for `require` is trying to rule out).
//! `resolve_execution` (the pure routing function) already has unit test
//! coverage for this in `crates/sqe-coordinator/src/maintenance.rs`; this
//! test instead drives the real `CALL` dispatch path
//! (`MaintenanceHandler::handle`) parsed from actual SQL text, the gap the
//! Phase 4c compaction review flagged as untested.
//!
//! `MaintenanceHandler::handle()` builds its catalog bridge
//! (`create_catalog_bridge` -> `SessionCatalog::for_session`) BEFORE checking
//! `distribution.mode`, so this test needs a config that deserializes and
//! constructs a `SessionCatalog` without erroring, but never needs a live
//! catalog to actually answer requests: `create_catalog_bridge` discards the
//! result of its own `list_namespaces()` probe (`let _ = ...`), and the
//! distribution-mode check runs strictly before the target table is ever
//! loaded. An unreachable `catalog_url` (mirrors `write_handler.rs`'s
//! `write_test_config()` pattern) is therefore sufficient: no docker stack,
//! no `test-sqlite` feature, and no network dependency.
//!
//! Run with `cargo test -p sqe-coordinator --test rewrite_require_mode_test`.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use sqe_core::config::SqeConfig;
use sqe_core::{SecretString, Session};
use sqe_coordinator::maintenance::MaintenanceHandler;
use sqe_sql::{try_parse_call, ProcedureCall};

/// A minimal config that deserializes cleanly. `catalog_url` points at a
/// closed local port so any accidental network call fails fast instead of
/// hanging; `create_catalog_bridge` swallows that failure anyway (see the
/// module doc above), so this test never depends on the port actually being
/// closed for correctness, only for speed if the swallowed call is ever hit.
fn unreachable_catalog_config() -> SqeConfig {
    let toml_text = r#"
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
    toml::from_str::<SqeConfig>(toml_text).expect("config parses")
}

fn session_with_write_privilege() -> Session {
    // Empty roles default to allow (see `session_has_write_privilege`'s own
    // unit tests in `maintenance.rs`), so this session clears the
    // `authorize_or_deny` gate and reaches the distribution-mode check.
    Session::new(
        "alice".to_string(),
        SecretString::new("test-token".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec![],
    )
}

/// Parse a real `CALL system.rewrite_data_files(...)` statement into a
/// [`ProcedureCall`], the same parser the coordinator's SQL entry point uses.
fn parse_rewrite_call(sql: &str) -> ProcedureCall {
    let stmt = Parser::parse_sql(&GenericDialect {}, sql)
        .expect("CALL statement parses")
        .remove(0);
    try_parse_call(&stmt)
        .expect("try_parse_call succeeds")
        .expect("statement recognized as a maintenance procedure call")
}

/// `CALL system.rewrite_data_files(table => '...', distributed => 'require')`
/// with zero healthy workers registered must return an `Err` that names the
/// insufficient-worker condition, and must not silently execute a
/// coordinator-local rewrite instead.
#[tokio::test]
async fn manual_call_require_mode_errors_below_worker_floor() {
    let config = unreachable_catalog_config();
    // Default `[maintenance.distribution]` (min_workers = 2) applies; no
    // `with_worker_registry(..)` is attached, so `healthy_worker_count()`
    // always reports 0 -- the "no fleet wired up" state.
    let handler = MaintenanceHandler::new(config);
    let session = session_with_write_privilege();

    let call = parse_rewrite_call(
        "CALL system.rewrite_data_files(table => 'default.nonexistent_table', \
         distributed => 'require')",
    );

    let result = handler.handle(&session, &call).await;

    let err = result.expect_err(
        "require mode below the healthy-worker floor must error, not silently \
         fall back to a coordinator-local rewrite",
    );
    let message = err.to_string();
    assert!(
        message.contains("require") && message.contains("healthy"),
        "error message should name the insufficient-worker condition, got: {message}"
    );
}

/// Same as above but via the config-level default (`distributed` argument
/// omitted): `[maintenance.distribution] mode = "require"` must behave
/// identically to the per-call override.
#[tokio::test]
async fn config_default_require_mode_errors_below_worker_floor() {
    let toml_text = r#"
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

[maintenance.distribution]
mode = "require"
min_workers = 2
"#;
    let config: SqeConfig = toml::from_str(toml_text).expect("config parses");
    let handler = MaintenanceHandler::new(config);
    let session = session_with_write_privilege();

    let call = parse_rewrite_call(
        "CALL system.rewrite_data_files(table => 'default.nonexistent_table')",
    );

    let result = handler.handle(&session, &call).await;

    let err = result.expect_err(
        "config-level require mode below the healthy-worker floor must error too",
    );
    let message = err.to_string();
    assert!(
        message.contains("require") && message.contains("healthy"),
        "error message should name the insufficient-worker condition, got: {message}"
    );
}

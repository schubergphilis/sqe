//! Pre-flight catalog-qualifier dispatch tests for issue #1.
//!
//! Background: when a user sends a 3-part identifier like
//! `tf_main_warehouse.tf_demo_namespace.demo_t` and `tf_main_warehouse`
//! is NOT a registered catalog, DataFusion silently routes the query
//! to the session-default catalog. Users then see confusing errors
//! like "namespace does not exist" against the wrong warehouse.
//!
//! The pre-flight check in `QueryHandler::execute` walks the parsed
//! AST for any `ObjectName` with three components, extracts the
//! catalog component, and compares against the configured catalog set.
//! Unknown qualifiers fail with a clear `SqeError::Catalog` error
//! that names the unknown catalog and lists what IS configured.
//!
//! These tests do NOT touch Polaris. The pre-flight check fires
//! BEFORE `create_session_context`, so a 3-part identifier with an
//! unknown catalog never gets as far as opening a connection. The
//! "passes pre-flight" tests assert that the unknown-catalog error
//! does not appear in the returned error message; whatever later
//! failure surfaces (network, planning) is fine for these tests.
//!
//! See `crates/sqe-sql/src/catalog_qualifiers.rs` for the AST walker.

use std::sync::Arc;

use chrono::{Duration, Utc};
use sqe_core::{Session, SqeConfig};

/// A coordinator config wired to point at unreachable Polaris / S3
/// endpoints. The pre-flight unknown-qualifier check fires before
/// any network IO, so the placeholder URLs never get dialed.
fn base_config_toml() -> &'static str {
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

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#
}

fn parse_config(toml_text: &str) -> SqeConfig {
    toml::from_str::<SqeConfig>(toml_text).expect("config parses")
}

fn handler(config: SqeConfig) -> sqe_coordinator::QueryHandler {
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

/// 3-part identifier with an unknown catalog returns a clear
/// `SqeError::Catalog` error instead of silently routing to the
/// session-default catalog. The error must name the unknown
/// catalog and list the configured set so the user can fix
/// their TOML.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_catalog_qualifier_errors_clearly() {
    // Default config: only the legacy `[catalog]` block, registered
    // under `iceberg`. Plus the always-on `system` and `datafusion`.
    let config = parse_config(base_config_toml());
    let h = handler(config);
    let session = fake_session();

    let err = h
        .execute(
            &session,
            "SELECT * FROM tf_main_warehouse.tf_demo_namespace.demo_t",
            None,
        )
        .await
        .expect_err("3-part name with unknown catalog must error");

    let msg = err.to_string();
    assert!(
        msg.contains("unknown catalog"),
        "error must say 'unknown catalog': {msg}"
    );
    assert!(
        msg.contains("tf_main_warehouse"),
        "error must name the unknown catalog: {msg}"
    );
    assert!(
        msg.contains("iceberg"),
        "error must list the configured catalogs (iceberg): {msg}"
    );
    assert!(
        msg.contains("[catalogs.<name>]") || msg.contains("ATTACH"),
        "error must hint at the fix: {msg}"
    );
}

/// When the catalog qualifier IS registered, the pre-flight check
/// must not fire. Whatever happens next (network failure against
/// the placeholder Polaris URL, planning failure, etc.) is fine,
/// but the error must NOT be the "unknown catalog" one.
#[tokio::test(flavor = "multi_thread")]
async fn known_catalog_qualifier_passes_pre_flight() {
    // Register a second catalog `tf_main_warehouse` alongside the
    // legacy block. The legacy block flattens to `iceberg`; the
    // named map adds `tf_main_warehouse`.
    let toml_text = format!(
        "{}\n[catalogs.tf_main_warehouse]\ncatalog_url = \"http://127.0.0.1:9/unused\"\nwarehouse = \"tf_wh\"\n",
        base_config_toml()
    );
    let config = parse_config(&toml_text);
    let h = handler(config);
    let session = fake_session();

    let err = h
        .execute(
            &session,
            "SELECT * FROM tf_main_warehouse.tf_demo_namespace.demo_t",
            None,
        )
        .await
        .expect_err("placeholder Polaris URL must fail somewhere");

    let msg = err.to_string();
    assert!(
        !msg.contains("unknown catalog"),
        "pre-flight must accept registered catalog; got: {msg}"
    );
}

/// Bare 1-part names never hit the pre-flight check. The query
/// proceeds to session-context build, and may fail later for
/// unrelated reasons (Polaris unreachable in this test). The
/// invariant is just: no "unknown catalog" error.
#[tokio::test(flavor = "multi_thread")]
async fn unqualified_name_skips_pre_flight() {
    let config = parse_config(base_config_toml());
    let h = handler(config);
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

/// `system.runtime.queries` is a 3-part identifier whose leading
/// component is the always-on `system` catalog. The pre-flight
/// check must accept it. The query may still fail later (the
/// session catalog code that backs `system.runtime.queries` runs
/// once a session context exists), but the error must NOT be
/// the unknown-catalog one.
#[tokio::test(flavor = "multi_thread")]
async fn system_catalog_qualifier_passes() {
    let config = parse_config(base_config_toml());
    let h = handler(config);
    let session = fake_session();

    let result = h
        .execute(&session, "SELECT * FROM system.runtime.queries", None)
        .await;
    if let Err(err) = result {
        let msg = err.to_string();
        assert!(
            !msg.contains("unknown catalog"),
            "system catalog must always pass pre-flight: {msg}"
        );
    }
}

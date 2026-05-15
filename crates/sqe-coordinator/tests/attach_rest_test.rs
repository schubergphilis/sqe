//! Integration tests for SQL `ATTACH` with the `iceberg_rest` backend.
//!
//! A wiremock server stands in for Polaris.  The fixture mounts the
//! two endpoints the REST catalog client calls at `attach` time:
//!   GET /v1/config    → catalog overrides / defaults
//!   GET /v1/namespaces → empty namespace list
//!
//! Tests exercise the full coordinator path:
//!   SQL string → parse_and_classify → handle_attach → build_catalog
//!   → WritableIcebergCatalog → ctx.register_catalog
//!
//! Secret-store integration is also covered:
//!   CREATE SECRET → ATTACH (SECRET ref) → DROP SECRET (blocked while in use)
//!   → DETACH → DROP SECRET (succeeds)

use std::sync::Arc;

use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::PassthroughEnforcer;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const MINIMAL_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59998"
"#;

fn minimal_config() -> SqeConfig {
    toml::from_str(MINIMAL_TOML).expect("minimal config")
}

fn dummy_session() -> Session {
    session_with_roles(vec!["service_admin".to_string()])
}

fn session_with_roles(roles: Vec<String>) -> Session {
    Session::new(
        "tester".to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles,
    )
}

fn make_handler(secrets: SecretStore, catalogs: RuntimeCatalogRegistry) -> QueryHandler {
    let config = minimal_config();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    QueryHandler::new(
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
        catalogs,
        secrets,
    )
    .expect("QueryHandler::new")
}

/// Mount the two endpoints the iceberg-rust REST client calls at build time.
///
/// GET /v1/config    — returns empty overrides (no prefix, no token override).
/// GET /v1/namespaces — returns an empty namespace list (no auth requirement).
async fn mount_rest_fixture(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"namespaces":[]}"#),
        )
        .mount(server)
        .await;
}

/// Variant of `mount_rest_fixture` that requires `Authorization`. The
/// catalog client is expected to send the bearer (from `SECRET ...`) so
/// any unauthenticated GET fails with 401.
async fn mount_rest_fixture_requires_auth(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header_exists("Authorization"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(header_exists("Authorization"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"namespaces":[]}"#),
        )
        .mount(server)
        .await;

    Mock::given(|req: &Request| !req.headers.contains_key("authorization"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("www-authenticate", "Bearer realm=\"test\""),
        )
        .mount(server)
        .await;
}

// ---------------------------------------------------------------------------
// ATTACH iceberg_rest tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attach_rest_catalog_succeeds() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    let sql = format!(
        "ATTACH '{url}' AS remote_cat (TYPE iceberg_rest, WAREHOUSE 'test-wh')"
    );
    let result = handler.execute(&session, &sql).await;
    assert!(result.is_ok(), "ATTACH should succeed: {:?}", result.err());
    assert_eq!(result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn attach_rest_duplicate_name_errors() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    let sql = format!(
        "ATTACH '{url}' AS dup_cat (TYPE iceberg_rest, WAREHOUSE 'wh')"
    );
    handler.execute(&session, &sql).await.expect("first attach");

    let err = handler
        .execute(&session, &sql)
        .await
        .expect_err("second attach with same name should fail");

    assert!(
        err.to_string().contains("already attached"),
        "error should say 'already attached': {err}"
    );
}

#[tokio::test]
async fn attach_then_detach_then_reattach() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    let attach_sql = format!(
        "ATTACH '{url}' AS cycle_cat (TYPE iceberg_rest, WAREHOUSE 'wh')"
    );
    handler.execute(&session, &attach_sql).await.expect("first attach");
    handler.execute(&session, "DETACH cycle_cat").await.expect("detach");

    // After DETACH the name is free; a second ATTACH must succeed.
    handler.execute(&session, &attach_sql).await.expect("second attach after detach");
}

#[tokio::test]
async fn detach_unknown_catalog_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "DETACH nobody")
        .await
        .expect_err("detaching unknown catalog should fail");

    assert!(
        err.to_string().contains("not attached"),
        "error should say 'not attached': {err}"
    );
}

// ---------------------------------------------------------------------------
// CREATE SECRET + ATTACH integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attach_with_bearer_secret_ref() {
    let server = MockServer::start().await;

    // This fixture verifies the bearer is forwarded: every GET endpoint
    // is guarded by header_exists("Authorization"); requests without auth
    // get 401.
    mount_rest_fixture_requires_auth(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    handler
        .execute(&session, "CREATE SECRET rest_tok (TYPE bearer, TOKEN 'my_bearer')")
        .await
        .expect("create secret");

    let sql = format!(
        "ATTACH '{url}' AS secret_cat (TYPE iceberg_rest, WAREHOUSE 'wh', SECRET rest_tok)"
    );
    handler.execute(&session, &sql).await.expect("attach with secret ref");
}

#[tokio::test]
async fn drop_secret_blocked_while_catalog_attached() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    handler
        .execute(&session, "CREATE SECRET guard_tok (TYPE bearer, TOKEN 'tok')")
        .await
        .expect("create secret");

    handler
        .execute(
            &session,
            &format!("ATTACH '{url}' AS guarded (TYPE iceberg_rest, WAREHOUSE 'wh', SECRET guard_tok)"),
        )
        .await
        .expect("attach");

    // The secret is in use — DROP must fail.
    let err = handler
        .execute(&session, "DROP SECRET guard_tok")
        .await
        .expect_err("drop while in-use should fail");

    let msg = err.to_string();
    assert!(msg.contains("guard_tok"), "error should name the secret: {msg}");
    assert!(msg.contains("guarded"), "error should name the catalog: {msg}");
}

#[tokio::test]
async fn drop_secret_succeeds_after_detach() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let url = server.uri();

    handler
        .execute(&session, "CREATE SECRET free_tok (TYPE bearer, TOKEN 'tok')")
        .await
        .expect("create");

    handler
        .execute(
            &session,
            &format!(
                "ATTACH '{url}' AS free_cat (TYPE iceberg_rest, WAREHOUSE 'wh', SECRET free_tok)"
            ),
        )
        .await
        .expect("attach");

    handler.execute(&session, "DETACH free_cat").await.expect("detach");

    // Secret is no longer in use — DROP must now succeed.
    handler
        .execute(&session, "DROP SECRET free_tok")
        .await
        .expect("drop after detach should succeed");

    // Confirm it is gone.
    let batches = handler.execute(&session, "SHOW SECRETS").await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "secret store should be empty after drop");
}

// ---------------------------------------------------------------------------
// Admin gate regression (issue #3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attach_rejected_without_admin_role() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = session_with_roles(vec!["analyst".to_string()]);
    let url = server.uri();

    let sql = format!(
        "ATTACH '{url}' AS forbidden (TYPE iceberg_rest, WAREHOUSE 'wh')"
    );
    let err = handler
        .execute(&session, &sql)
        .await
        .expect_err("non-admin must not ATTACH");

    let msg = err.to_string();
    assert!(msg.contains("403"), "expected 403, got: {msg}");
    assert!(msg.contains("admin"), "expected admin role mention: {msg}");
}

#[tokio::test]
async fn detach_rejected_without_admin_role() {
    let server = MockServer::start().await;
    mount_rest_fixture(&server).await;

    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let admin = dummy_session();
    let url = server.uri();

    handler
        .execute(
            &admin,
            &format!("ATTACH '{url}' AS keep_it (TYPE iceberg_rest, WAREHOUSE 'wh')"),
        )
        .await
        .expect("admin attaches");

    let non_admin = session_with_roles(vec![]);
    let err = handler
        .execute(&non_admin, "DETACH keep_it")
        .await
        .expect_err("non-admin must not DETACH");

    assert!(err.to_string().contains("403"), "expected 403: {err}");
}

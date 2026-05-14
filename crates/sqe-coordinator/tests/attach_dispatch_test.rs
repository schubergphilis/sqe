//! Dispatch tests for ATTACH/DETACH/CREATE SECRET/DROP SECRET/SHOW SECRETS.
//!
//! Secret operations (CREATE/DROP/SHOW) are purely in-memory and need no
//! running catalog; a minimal SqeConfig with a fake catalog URL is enough
//! to construct a QueryHandler. ATTACH dispatch is covered separately by
//! the unit tests in `runtime_catalog_test.rs` which bypass QueryHandler.
//!
//! These tests verify that SQL reaches the right handler and that the
//! handler returns the expected result or error shape.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::PassthroughEnforcer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MINIMAL_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59999"
"#;

fn minimal_config() -> SqeConfig {
    toml::from_str(MINIMAL_TOML).expect("minimal config parses")
}

fn dummy_session() -> Session {
    session_with_roles(vec!["service_admin".to_string()])
}

fn session_with_roles(roles: Vec<String>) -> Session {
    let now = chrono::Utc::now();
    Session {
        id: "test".to_string(),
        user: sqe_core::session::SessionUser {
            username: "tester".to_string(),
            roles,
        },
        access_token: "tok".to_string(),
        refresh_token: None,
        token_expiry: now + chrono::Duration::hours(1),
        created_at: now,
        last_activity: now,
        default_catalog: None,
        default_schema: None,
        source: None,
        write_branch: None,
    }
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

// ---------------------------------------------------------------------------
// CREATE SECRET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_bearer_secret_succeeds() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let result = handler
        .execute(&session, "CREATE SECRET my_tok (TYPE bearer, TOKEN 'abc123')")
        .await;

    assert!(result.is_ok(), "unexpected error: {:?}", result);
    assert_eq!(result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn create_aws_secret_all_optional_fields() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = "CREATE SECRET aws_prod (\
        TYPE aws, \
        ACCESS_KEY_ID 'AKIA1234', \
        SECRET_ACCESS_KEY 'very_secret', \
        REGION 'eu-west-1'\
    )";
    let result = handler.execute(&session, sql).await;
    assert!(result.is_ok(), "unexpected error: {:?}", result);
}

#[tokio::test]
async fn create_basic_secret_succeeds() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = "CREATE SECRET svc_acct (TYPE basic, USERNAME 'admin', PASSWORD 'p@ss')";
    let result = handler.execute(&session, sql).await;
    assert!(result.is_ok(), "unexpected error: {:?}", result);
}

#[tokio::test]
async fn create_duplicate_secret_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    handler
        .execute(&session, "CREATE SECRET dup (TYPE bearer, TOKEN 'tok1')")
        .await
        .expect("first create");

    let err = handler
        .execute(&session, "CREATE SECRET dup (TYPE bearer, TOKEN 'tok2')")
        .await
        .expect_err("second create should fail");

    let msg = err.to_string();
    assert!(msg.contains("dup"), "error should name the secret: {msg}");
    assert!(msg.contains("already exists"), "error should say 'already exists': {msg}");
}

#[tokio::test]
async fn create_bearer_missing_token_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "CREATE SECRET bad (TYPE bearer)")
        .await
        .expect_err("missing TOKEN should fail");

    let msg = err.to_string();
    assert!(msg.contains("TOKEN"), "error should mention TOKEN: {msg}");
}

// ---------------------------------------------------------------------------
// SHOW SECRETS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_secrets_empty_returns_zero_rows() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let batches = handler.execute(&session, "SHOW SECRETS").await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn show_secrets_lists_name_and_type_not_value() {
    let secrets = SecretStore::new();
    secrets
        .create("alpha", sqe_core::Secret::Bearer { token: "supersecret".to_string() })
        .unwrap();
    secrets
        .create(
            "zeta",
            sqe_core::Secret::Aws {
                access_key: Some("AKIA123".to_string()),
                secret_key: Some("skey".to_string()),
                session_token: None,
                region: Some("us-east-1".to_string()),
                profile: None,
            },
        )
        .unwrap();

    let handler = make_handler(secrets, RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let batches = handler.execute(&session, "SHOW SECRETS").await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected 2 secrets listed");

    // Flatten into (name, type) pairs — sorted by name (store guarantees it).
    let mut pairs: Vec<(String, String)> = vec![];
    for batch in &batches {
        let names = batch.column(0).as_string::<i32>();
        let types = batch.column(1).as_string::<i32>();
        for i in 0..batch.num_rows() {
            pairs.push((names.value(i).to_string(), types.value(i).to_string()));
        }
    }
    assert_eq!(pairs[0], ("alpha".to_string(), "bearer".to_string()));
    assert_eq!(pairs[1], ("zeta".to_string(), "aws".to_string()));

    // Sanity: none of the values should appear in the output
    for batch in &batches {
        let names = batch.column(0).as_string::<i32>();
        let types = batch.column(1).as_string::<i32>();
        for i in 0..batch.num_rows() {
            assert!(!names.value(i).contains("supersecret"));
            assert!(!types.value(i).contains("AKIA123"));
        }
    }
}

// ---------------------------------------------------------------------------
// DROP SECRET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_secret_removes_from_store() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    handler
        .execute(&session, "CREATE SECRET ephemeral (TYPE bearer, TOKEN 'tok')")
        .await
        .unwrap();

    handler
        .execute(&session, "DROP SECRET ephemeral")
        .await
        .expect("drop should succeed");

    // After drop, SHOW SECRETS should be empty again.
    let batches = handler.execute(&session, "SHOW SECRETS").await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn drop_missing_secret_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "DROP SECRET nonexistent")
        .await
        .expect_err("drop of unknown secret should fail");

    let msg = err.to_string();
    assert!(msg.contains("nonexistent"), "error should name the secret: {msg}");
    assert!(msg.contains("not found"), "error should say 'not found': {msg}");
}

#[tokio::test]
async fn drop_secret_in_use_by_attached_catalog_errors() {
    let secrets = SecretStore::new();
    let catalogs = RuntimeCatalogRegistry::new();
    secrets
        .create("mytoken", sqe_core::Secret::Bearer { token: "tok".to_string() })
        .unwrap();

    // Simulate a catalog referencing the secret by building an AttachedCatalog entry
    // via SQLite — we go through the registry directly (no network call).
    use sqe_sql::{AttachStatement, CatalogKind, OptionValue};
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let mut opts = BTreeMap::new();
    opts.insert("SECRET".to_string(), OptionValue::SecretRef("mytoken".to_string()));
    let stmt = AttachStatement {
        name: "blocked".to_string(),
        location: dir.path().to_str().unwrap().to_string(),
        kind: CatalogKind::Sqlite,
        options: opts,
    };
    catalogs
        .attach(&stmt, &secrets)
        .await
        .expect("attach for in-use setup");

    let handler = make_handler(secrets, catalogs);
    let session = dummy_session();

    let err = handler
        .execute(&session, "DROP SECRET mytoken")
        .await
        .expect_err("drop of in-use secret should fail");

    let msg = err.to_string();
    assert!(msg.contains("mytoken"), "error should name the secret: {msg}");
    assert!(msg.contains("blocked"), "error should name the catalog: {msg}");
}

// ---------------------------------------------------------------------------
// Admin gate regression (issue #3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_secret_rejected_without_admin_role() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = session_with_roles(vec!["analyst".to_string()]);

    let err = handler
        .execute(&session, "CREATE SECRET denied_tok (TYPE bearer, TOKEN 'tok')")
        .await
        .expect_err("non-admin must not create secrets");

    let msg = err.to_string();
    assert!(msg.contains("403"), "expected 403, got: {msg}");
    assert!(msg.contains("admin"), "expected admin role mention: {msg}");
}

#[tokio::test]
async fn drop_secret_rejected_without_admin_role() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let admin = dummy_session();
    handler
        .execute(&admin, "CREATE SECRET to_drop (TYPE bearer, TOKEN 'tok')")
        .await
        .expect("admin creates secret");

    let non_admin = session_with_roles(vec![]);
    let err = handler
        .execute(&non_admin, "DROP SECRET to_drop")
        .await
        .expect_err("non-admin must not drop secrets");

    assert!(err.to_string().contains("403"), "expected 403: {err}");
}

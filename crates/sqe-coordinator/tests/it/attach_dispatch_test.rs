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

// ---------------------------------------------------------------------------
// CREATE SECRET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_bearer_secret_succeeds() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let result = handler
        .execute(
            &session,
            "CREATE SECRET my_tok (TYPE bearer, TOKEN 'abc123')",
            None,
        )
        .await;

    assert!(result.is_ok(), "unexpected error: {:?}", result);
    assert_eq!(
        result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(),
        0
    );
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
    let result = handler.execute(&session, sql, None).await;
    assert!(result.is_ok(), "unexpected error: {:?}", result);
}

#[tokio::test]
async fn create_basic_secret_succeeds() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = "CREATE SECRET svc_acct (TYPE basic, USERNAME 'admin', PASSWORD 'p@ss')";
    let result = handler.execute(&session, sql, None).await;
    assert!(result.is_ok(), "unexpected error: {:?}", result);
}

#[tokio::test]
async fn create_duplicate_secret_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    handler
        .execute(
            &session,
            "CREATE SECRET dup (TYPE bearer, TOKEN 'tok1')",
            None,
        )
        .await
        .expect("first create");

    let err = handler
        .execute(
            &session,
            "CREATE SECRET dup (TYPE bearer, TOKEN 'tok2')",
            None,
        )
        .await
        .expect_err("second create should fail");

    let msg = err.to_string();
    assert!(msg.contains("dup"), "error should name the secret: {msg}");
    assert!(
        msg.contains("already exists"),
        "error should say 'already exists': {msg}"
    );
}

#[tokio::test]
async fn create_bearer_missing_token_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "CREATE SECRET bad (TYPE bearer)", None)
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

    let batches = handler
        .execute(&session, "SHOW SECRETS", None)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn show_secrets_lists_name_and_type_not_value() {
    let secrets = SecretStore::new();
    secrets
        .create(
            "alpha",
            sqe_core::Secret::Bearer {
                token: "supersecret".to_string(),
            },
        )
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
    let batches = handler
        .execute(&session, "SHOW SECRETS", None)
        .await
        .unwrap();

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
        .execute(
            &session,
            "CREATE SECRET ephemeral (TYPE bearer, TOKEN 'tok')",
            None,
        )
        .await
        .unwrap();

    handler
        .execute(&session, "DROP SECRET ephemeral", None)
        .await
        .expect("drop should succeed");

    // After drop, SHOW SECRETS should be empty again.
    let batches = handler
        .execute(&session, "SHOW SECRETS", None)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn drop_missing_secret_errors() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "DROP SECRET nonexistent", None)
        .await
        .expect_err("drop of unknown secret should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent"),
        "error should name the secret: {msg}"
    );
    assert!(
        msg.contains("not found"),
        "error should say 'not found': {msg}"
    );
}

#[tokio::test]
async fn drop_secret_in_use_by_attached_catalog_errors() {
    let secrets = SecretStore::new();
    let catalogs = RuntimeCatalogRegistry::new();
    secrets
        .create(
            "mytoken",
            sqe_core::Secret::Bearer {
                token: "tok".to_string(),
            },
        )
        .unwrap();

    // Simulate a catalog referencing the secret by building an AttachedCatalog entry
    // via SQLite — we go through the registry directly (no network call).
    use sqe_sql::{AttachStatement, CatalogKind, OptionValue};
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let mut opts = BTreeMap::new();
    opts.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("mytoken".to_string()),
    );
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
        .execute(&session, "DROP SECRET mytoken", None)
        .await
        .expect_err("drop of in-use secret should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("mytoken"),
        "error should name the secret: {msg}"
    );
    assert!(
        msg.contains("blocked"),
        "error should name the catalog: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Admin gate regression (issue #3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_secret_rejected_without_admin_role() {
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = session_with_roles(vec!["analyst".to_string()]);

    let err = handler
        .execute(
            &session,
            "CREATE SECRET denied_tok (TYPE bearer, TOKEN 'tok')",
            None,
        )
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
        .execute(
            &admin,
            "CREATE SECRET to_drop (TYPE bearer, TOKEN 'tok')",
            None,
        )
        .await
        .expect("admin creates secret");

    let non_admin = session_with_roles(vec![]);
    let err = handler
        .execute(&non_admin, "DROP SECRET to_drop", None)
        .await
        .expect_err("non-admin must not drop secrets");

    assert!(err.to_string().contains("403"), "expected 403: {err}");
}

// ---------------------------------------------------------------------------
// ATTACH / DETACH ... TYPE hive_external
//
// Unlike the other `CatalogKind`s (which need `sql-sqlite`, a network REST
// endpoint, or an AWS backend and so are covered by the sqlite-gated
// `runtime_catalog_test.rs`), a Hive root is just files under a `LOCATION`:
// `build_hive_root` only needs `[storage.tvf] allow_local_paths = true` to
// accept a local temp dir, so this dispatch is cheaply testable right here.
// ---------------------------------------------------------------------------

fn make_handler_allowing_local_paths(
    secrets: SecretStore,
    catalogs: RuntimeCatalogRegistry,
) -> QueryHandler {
    let mut config = minimal_config();
    config.storage.tvf.allow_local_paths = true;
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

#[tokio::test]
async fn attach_hive_external_root_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler =
        make_handler_allowing_local_paths(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = format!(
        "ATTACH 'file://{}' AS lake (TYPE hive_external)",
        dir.path().display()
    );
    let result = handler.execute(&session, &sql, None).await;
    assert!(result.is_ok(), "unexpected error: {:?}", result);
    assert_eq!(
        result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(),
        0
    );
}

#[tokio::test]
async fn attach_hive_external_root_rejects_local_path_without_allow_local_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `make_handler`, not `make_handler_allowing_local_paths`: the default
    // config leaves `[storage.tvf] allow_local_paths` false, and `trusted`
    // (which a Hive ATTACH does not set for a session caller anyway) would
    // not bypass this gate regardless -- see `check_path`'s local-path
    // branch.
    let handler = make_handler(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = format!(
        "ATTACH 'file://{}' AS lake (TYPE hive_external)",
        dir.path().display()
    );
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("local paths disabled by default");
    let msg = err.to_string();
    assert!(
        msg.contains("local filesystem paths are disabled"),
        "{msg}"
    );
}

#[tokio::test]
async fn attach_hive_external_root_duplicate_name_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler =
        make_handler_allowing_local_paths(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}' AS lake (TYPE hive_external)",
        dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("first attach");

    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("duplicate attach must fail");
    assert!(
        err.to_string().contains("already attached"),
        "{err}"
    );
}

#[tokio::test]
async fn detach_hive_external_root_then_reattach_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler =
        make_handler_allowing_local_paths(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}' AS lake (TYPE hive_external)",
        dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("attach");

    handler
        .execute(&session, "DETACH lake", None)
        .await
        .expect("detach");

    // Re-attaching under the same name only succeeds if DETACH actually
    // forgot it -- a stronger proof than asserting DETACH returned Ok.
    handler
        .execute(&session, &sql, None)
        .await
        .expect("re-attach after detach");
}

#[tokio::test]
async fn detach_unknown_name_errors() {
    let handler =
        make_handler_allowing_local_paths(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let err = handler
        .execute(&session, "DETACH nope", None)
        .await
        .expect_err("unknown name must error");
    assert!(err.to_string().contains("not attached"), "{err}");
}

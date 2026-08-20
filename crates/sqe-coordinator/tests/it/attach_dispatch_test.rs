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

/// Wires fresh, private `hive_catalogs`/`hive_manifest_cache` instances,
/// simulating a correctly-wired production binary (`main.rs` /
/// `bin/sqe_server.rs` call `with_hive_registries` with the SAME `Arc`
/// their `PolicyPlanRewriter` was built from; a test has no rewriter to
/// share with, so a fresh instance is the closest correct analogue). Use
/// `make_handler_missing_hive_registries` instead to prove what happens
/// when a deployment forgets that call.
fn make_handler_with_hive_registries(
    secrets: SecretStore,
    catalogs: RuntimeCatalogRegistry,
    allow_local_paths: bool,
) -> QueryHandler {
    let mut config = minimal_config();
    config.storage.tvf.allow_local_paths = allow_local_paths;
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
    .with_hive_registries(
        Arc::new(sqe_policy::plan_rewriter::HiveCatalogs::new()),
        Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
    )
}

fn make_handler_allowing_local_paths(
    secrets: SecretStore,
    catalogs: RuntimeCatalogRegistry,
) -> QueryHandler {
    make_handler_with_hive_registries(secrets, catalogs, true)
}

/// Deliberately never calls `with_hive_registries` -- the deployment-forgot
/// scenario `hive_catalogs`'s own doc comment warns about: `ATTACH` looking
/// like it worked while the policy layer never learns the root exists.
fn make_handler_missing_hive_registries(
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

/// The fix for the latent escalation the team-lead's review found: a
/// `QueryHandler` built without `with_hive_registries` must REFUSE `ATTACH
/// ... TYPE hive_external` rather than silently attach into a private
/// registry the policy layer cannot see. Mutation check: reverting
/// `hive_catalogs`'s default from `None` back to `Some(Arc::new(HiveCatalogs::new()))`
/// makes this test fail because the attach now succeeds -- that failure is
/// the proof the guard is the thing closing the gap, not incidental.
#[tokio::test]
async fn attach_hive_external_root_refused_when_hive_registries_are_not_wired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handler =
        make_handler_missing_hive_registries(SecretStore::new(), RuntimeCatalogRegistry::new());
    let session = dummy_session();

    let sql = format!(
        "ATTACH 'file://{}' AS lake (TYPE hive_external)",
        dir.path().display()
    );
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("must refuse without the shared hive registries wired");
    let msg = err.to_string();
    assert!(
        msg.contains("hive_external") && msg.contains("with_hive_registries"),
        "error should name the wiring gap, not a generic failure: {msg}"
    );
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
    // Hive registries ARE wired here (unlike `make_handler`'s bare
    // default): this test is exercising `check_path`'s local-path gate
    // specifically, and must not be shadowed by the (separate)
    // hive-registries-not-wired refusal proven in
    // `attach_hive_external_root_refused_when_hive_registries_are_not_wired`.
    // The default config leaves `[storage.tvf] allow_local_paths` false,
    // and `trusted` (which a Hive ATTACH does not set for a session caller
    // anyway) would not bypass this gate regardless -- see `check_path`'s
    // local-path branch.
    let handler =
        make_handler_with_hive_registries(SecretStore::new(), RuntimeCatalogRegistry::new(), false);
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

//! Catalog-vended, table-scoped S3 credentials end to end.
//!
//! The `docker-compose.test.yml` stack vends: Polaris 1.7 keeps credential
//! subscoping on and RustFS answers its `AssumeRole`, so a `loadTable` /
//! `createTable` sent with `X-Iceberg-Access-Delegation: vended-credentials`
//! carries `s3.session-token` scoped to the table location. These tests run
//! the engine with `require_vended_credentials = true` and the `[storage]`
//! key pair blanked, so every object-store byte (CTAS, INSERT, SELECT) can
//! only have moved with vended keys: FileIO also has env / profile / IMDS
//! loading disabled in that mode.

use std::sync::Arc;

use sqe_catalog::rest_catalog::SessionCatalog;

/// The test config with vended-only FileIO and no static key pair.
fn vended_only_config() -> sqe_core::SqeConfig {
    let mut config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    config.catalog.require_vended_credentials = true;
    config.storage.s3_access_key = String::new();
    config.storage.s3_secret_key = sqe_core::SecretString::new(String::new());
    config
}

async fn vended_only_handler() -> (
    sqe_core::Session,
    sqe_coordinator::QueryHandler,
    sqe_core::SqeConfig,
) {
    crate::common::init_tracing();
    let config = vended_only_config();
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let handler = sqe_coordinator::QueryHandler::new(
        policy,
        None,
        config.clone(),
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
    .expect("Failed to create QueryHandler");
    (session, handler, config)
}

fn row_count(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// CTAS, INSERT and SELECT with no static key anywhere: every write and
/// read goes through the table-scoped keys Polaris vended. The loaded
/// table's FileIO carries a session token and an expiry and no static pair.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn vended_only_ctas_insert_select_roundtrip() {
    let (session, handler, config) = vended_only_handler().await;
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.vended_rt", None)
        .await;

    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.vended_rt AS SELECT 1 AS id, 'a' AS name",
            None,
        )
        .await
        .expect("CTAS with vended credentials");
    handler
        .execute(
            &session,
            "INSERT INTO test_ns.vended_rt VALUES (2, 'b'), (3, 'c')",
            None,
        )
        .await
        .expect("INSERT with vended credentials");
    let batches = handler
        .execute(&session, "SELECT * FROM test_ns.vended_rt ORDER BY id", None)
        .await
        .expect("SELECT with vended credentials");
    assert_eq!(row_count(&batches), 3);

    let catalog = SessionCatalog::for_session(&config, None, session.access_token().expose())
        .await
        .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs(["test_ns", "vended_rt"]).unwrap();
    let table = catalog.load_table(&ident).await.expect("load table");
    let props = table.file_io().config().props();
    assert!(
        props.get("s3.session-token").is_some_and(|t| !t.is_empty()),
        "FileIO must carry a vended session token"
    );
    assert!(
        sqe_catalog::vended_credentials::table_credential_expiry(&table).is_some(),
        "vended keys carry an expiry"
    );
    assert_ne!(
        props.get("s3.access-key-id").map(String::as_str),
        Some("s3admin"),
        "the static RustFS key must never reach FileIO"
    );

    // A fresh reload through the refresh path returns a working vended set
    // as the same user.
    let reloaded = Arc::new(catalog)
        .table_reloader(ident)
        .reload()
        .await
        .expect("reload as the same user");
    assert!(reloaded
        .file_io()
        .config()
        .props()
        .get("s3.session-token")
        .is_some_and(|t| !t.is_empty()));

    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.vended_rt", None)
        .await;
}

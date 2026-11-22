//! Engine credential identity end to end (#934).
//!
//! A catalog with `credentials` reads with keys the ENGINE fetched for the
//! table, after the policy engine said the person may `select` it. These
//! tests run the engine as the stack's `root` client (`client_credentials`)
//! with the `[storage]` key pair blanked and `require_vended_credentials`,
//! so every byte read can only have moved with vended keys, and the policy
//! store is the in-memory one so each case sets its own grants.
//!
//! One test function runs the cases in order: the policy store and audit log
//! the identity binds to are process-wide, and a second concurrent test would
//! replace them under the first.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use sqe_core::config::CatalogAuthConfig;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::ResolvedPolicy;

const TABLE: &str = "test_ns.engine_credential_t";

fn engine_config() -> sqe_core::SqeConfig {
    let mut config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    config.catalog.require_vended_credentials = true;
    config.catalog.credentials = Some(CatalogAuthConfig::ClientCredentials {
        token_endpoint: config.auth.token_endpoint.clone(),
        client_id: config.auth.client_id.clone(),
        client_secret: config.auth.client_secret.expose().to_string(),
        scope: None,
    });
    config.storage.s3_access_key = String::new();
    config.storage.s3_secret_key = sqe_core::SecretString::new(String::new());
    config
}

async fn engine_handler(
    store: Arc<InMemoryPolicyStore>,
) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    crate::common::init_tracing();
    let config = engine_config();
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let store_dyn: Arc<dyn sqe_policy::PolicyStore> = store;
    sqe_coordinator::engine_identity_wiring::install(
        Some(Arc::clone(&store_dyn)),
        config.policy.mask_precedence,
        None,
    );
    let rewriter = sqe_policy::plan_rewriter::PolicyPlanRewriter::new(Arc::clone(&store_dyn))
        .with_metadata_target(sqe_catalog::iceberg_metadata_tvf::metadata_tvf_target)
        .with_file_table(sqe_catalog::file_tvf_prepare::is_file_table_provider);
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(rewriter);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let handler = sqe_coordinator::QueryHandler::new(
        policy,
        Some(store_dyn),
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
    .expect("Failed to create QueryHandler");
    (session, handler)
}

async fn count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>()
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn engine_credential_read_gate_and_writes() {
    // Setup as the person with an ordinary (delegated) catalog.
    let (session, plain) = crate::common::setup_handler().await;
    let _ = plain
        .execute(&session, &format!("DROP TABLE IF EXISTS {TABLE}"), None)
        .await;
    plain
        .execute(
            &session,
            &format!(
                "CREATE TABLE {TABLE} AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) \
                 AS t(id, name)"
            ),
            None,
        )
        .await
        .expect("CTAS as the person");

    // 1. Select granted (no policy = open in the in-memory store): the read
    //    works with no static key anywhere, on keys the engine fetched.
    let store = Arc::new(InMemoryPolicyStore::new());
    let (session, engine) = engine_handler(Arc::clone(&store)).await;
    assert_eq!(
        count(&engine, &session, &format!("SELECT count(*) FROM {TABLE}")).await,
        3,
        "granted read through engine keys"
    );

    // 2. Writes go through the person's delegated client and keys.
    engine
        .execute(
            &session,
            &format!("INSERT INTO {TABLE} VALUES (4, 'd')"),
            None,
        )
        .await
        .expect("INSERT on an engine-keyed catalog");
    engine
        .execute(
            &session,
            &format!("UPDATE {TABLE} SET name = 'z' WHERE id = 4"),
            None,
        )
        .await
        .expect("UPDATE on an engine-keyed catalog");
    engine
        .execute(&session, &format!("DELETE FROM {TABLE} WHERE id = 1"), None)
        .await
        .expect("DELETE on an engine-keyed catalog");
    assert_eq!(
        count(&engine, &session, &format!("SELECT count(*) FROM {TABLE}")).await,
        3,
        "3 rows - 1 deleted + 1 inserted"
    );
    assert_eq!(
        count(
            &engine,
            &session,
            &format!("SELECT count(*) FROM {TABLE} WHERE name = 'z'")
        )
        .await,
        1
    );

    // 3. Select denied: no rows, no error, and no engine keys were needed.
    store
        .add_table_policy(
            "test_ns",
            "engine_credential_t",
            ResolvedPolicy {
                row_filters: vec![datafusion::logical_expr::lit(false)],
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        count(&engine, &session, &format!("SELECT count(*) FROM {TABLE}")).await,
        0,
        "a denied person reads no rows"
    );
    let fallible = format!("SELECT count(*) FROM {TABLE} WHERE 10 / (id - 1) > 0");
    assert_eq!(
        count(&engine, &session, &fallible).await,
        0,
        "a fallible predicate on a denied table neither errors nor counts"
    );

    // 4. A denied person's DELETE removes nothing.
    let _ = engine
        .execute(&session, &format!("DELETE FROM {TABLE}"), None)
        .await;
    let open = Arc::new(InMemoryPolicyStore::new());
    let (session, check) = engine_handler(open).await;
    assert_eq!(
        count(&check, &session, &format!("SELECT count(*) FROM {TABLE}")).await,
        3,
        "the denied DELETE changed nothing"
    );

    // 5. A person with metadata access only (no TABLE_READ_DATA, so no keys of
    //    their own) reads through the engine's keys. Neither they nor a
    //    read-data reader writes: every write loads the table with the
    //    person's own credentials, never the engine's read keys.
    use crate::dml_write_precheck_e2e::{
        objects_under, principal_token, principal_token_with, session_for,
    };
    let root = session.access_token().expose().to_string();
    let metadata_only = [
        "CATALOG_READ_PROPERTIES",
        "NAMESPACE_LIST",
        "NAMESPACE_READ_PROPERTIES",
        "TABLE_LIST",
        "TABLE_READ_PROPERTIES",
    ];
    let meta = session_for(
        "engine_cred_meta",
        principal_token_with(&root, "engine_cred_meta", &metadata_only).await,
    );
    let reader = session_for(
        "engine_cred_reader",
        principal_token(&root, "engine_cred_reader", &[]).await,
    );
    let select = format!("SELECT count(*) FROM {TABLE}");
    assert_eq!(
        count(&check, &meta, &select).await,
        3,
        "a metadata-only person reads with the engine's keys"
    );
    let plain_config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    let catalog =
        sqe_catalog::rest_catalog::SessionCatalog::for_session(&plain_config, None, &root)
            .await
            .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs(["test_ns", "engine_credential_t"]).unwrap();
    let location = catalog
        .load_table(&ident)
        .await
        .expect("load table")
        .metadata()
        .location()
        .to_string();
    let before = objects_under(&location).await;
    for (who, person) in [("metadata-only person", &meta), ("reader", &reader)] {
        for sql in [
            format!("INSERT INTO {TABLE} VALUES (9, 'x')"),
            format!("DELETE FROM {TABLE} WHERE id = 2"),
            format!("UPDATE {TABLE} SET name = 'y' WHERE id = 2"),
            format!(
                "MERGE INTO {TABLE} AS t USING {TABLE} AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET name = 'm'"
            ),
            format!("TRUNCATE TABLE {TABLE}"),
            format!("CREATE OR REPLACE TABLE {TABLE} AS SELECT * FROM {TABLE}"),
            format!("CALL system.rewrite_data_files(table => '{TABLE}', rewrite_all => true)"),
        ] {
            let err = check
                .execute(person, &sql, None)
                .await
                .expect_err(&format!("the {who} is refused: {sql}"));
            assert!(
                !err.to_string().contains("engine's read keys"),
                "{who}: {sql}: a write reached the engine-keyed table: {err}"
            );
        }
    }
    assert_eq!(
        objects_under(&location).await,
        before,
        "a refused write leaves no object"
    );
    assert_eq!(count(&check, &session, &select).await, 3, "nothing changed");

    let _ = plain
        .execute(&session, &format!("DROP TABLE IF EXISTS {TABLE}"), None)
        .await;
}

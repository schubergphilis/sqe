//! A caller who may read a table but not write it is refused INSERT, DELETE,
//! UPDATE, MERGE and TRUNCATE before any file is written (#880).
//!
//! With vended credentials Polaris answers a reader's delegated `loadTable`
//! with read-only keys instead of a refusal, so the statement used to plan,
//! write data files and fail on the first `PutObject`. The engine now asks
//! Polaris first with a commit that cannot apply, then writes and deletes an
//! empty object with the vended keys. This test runs the engine vended-only,
//! as `vended_credentials_e2e` does, with two fresh Polaris principals: one
//! holds `TABLE_READ_DATA` and nothing that writes (the catalog refuses),
//! one also holds `TABLE_WRITE_PROPERTIES` (the catalog lets it commit, the
//! vended keys are read-only, the storage probe refuses).

use std::sync::Arc;

use chrono::{Duration, Utc};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sqe_core::{SecretString, Session, SqeConfig, SqeErrorCode};

const POLARIS: &str = "http://localhost:18181/api";
const CATALOG: &str = "test_warehouse";
const READER: &str = "dml_precheck_reader";
const PROPS_WRITER: &str = "dml_precheck_props";
const TABLE: &str = "test_ns.dml_precheck_t";

fn vended_only_config() -> SqeConfig {
    let mut config =
        SqeConfig::load(&crate::common::test_config_path()).expect("Failed to load test config");
    config.catalog.require_vended_credentials = true;
    config.storage.s3_access_key = String::new();
    config.storage.s3_secret_key = SecretString::new(String::new());
    config
}

fn handler(config: SqeConfig) -> sqe_coordinator::QueryHandler {
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    sqe_coordinator::QueryHandler::new(
        Arc::new(sqe_policy::PassthroughEnforcer),
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
    .expect("QueryHandler::new")
}

async fn polaris_token(client_id: &str, client_secret: &str) -> String {
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{POLARIS}/catalog/v1/oauth/tokens"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ])
        .send()
        .await
        .expect("token request")
        .error_for_status()
        .expect("token granted")
        .json()
        .await
        .expect("token json");
    resp["access_token"]
        .as_str()
        .expect("access_token")
        .to_string()
}

/// A fresh principal `name` that may read every table in the catalog, plus
/// `extra` privileges; returns its token. Role creation answers 409 on a
/// rerun, which is fine: the grants are idempotent.
async fn principal_token(root: &str, name: &str, extra: &[&str]) -> String {
    let principal_role = format!("{name}_role");
    let catalog_role = format!("{name}_cr");
    let http = reqwest::Client::new();
    let mgmt = format!("{POLARIS}/management/v1");
    let _ = http
        .delete(format!("{mgmt}/principals/{name}"))
        .bearer_auth(root)
        .send()
        .await;
    let created: serde_json::Value = http
        .post(format!("{mgmt}/principals"))
        .bearer_auth(root)
        .json(&serde_json::json!({ "principal": { "name": name } }))
        .send()
        .await
        .expect("create principal")
        .error_for_status()
        .expect("principal created")
        .json()
        .await
        .expect("principal json");

    let _ = http
        .post(format!("{mgmt}/principal-roles"))
        .bearer_auth(root)
        .json(&serde_json::json!({ "principalRole": { "name": principal_role } }))
        .send()
        .await;
    let _ = http
        .post(format!("{mgmt}/catalogs/{CATALOG}/catalog-roles"))
        .bearer_auth(root)
        .json(&serde_json::json!({ "catalogRole": { "name": catalog_role } }))
        .send()
        .await;
    let read = [
        "CATALOG_READ_PROPERTIES",
        "NAMESPACE_LIST",
        "NAMESPACE_READ_PROPERTIES",
        "TABLE_LIST",
        "TABLE_READ_DATA",
    ];
    for privilege in read.iter().chain(extra) {
        http.put(format!(
            "{mgmt}/catalogs/{CATALOG}/catalog-roles/{catalog_role}/grants"
        ))
        .bearer_auth(root)
        .json(&serde_json::json!({ "grant": { "type": "catalog", "privilege": privilege } }))
        .send()
        .await
        .expect("grant request")
        .error_for_status()
        .unwrap_or_else(|e| panic!("grant {privilege}: {e}"));
    }
    http.put(format!(
        "{mgmt}/principal-roles/{principal_role}/catalog-roles/{CATALOG}"
    ))
    .bearer_auth(root)
    .json(&serde_json::json!({ "catalogRole": { "name": catalog_role } }))
    .send()
    .await
    .expect("assign catalog role")
    .error_for_status()
    .expect("catalog role assigned");
    http.put(format!("{mgmt}/principals/{name}/principal-roles"))
        .bearer_auth(root)
        .json(&serde_json::json!({ "principalRole": { "name": principal_role } }))
        .send()
        .await
        .expect("assign principal role")
        .error_for_status()
        .expect("principal role assigned");

    let creds = &created["credentials"];
    polaris_token(
        creds["clientId"].as_str().expect("clientId"),
        creds["clientSecret"].as_str().expect("clientSecret"),
    )
    .await
}

fn session_for(name: &str, token: String) -> Session {
    Session::new(
        name.to_string(),
        SecretString::new(token),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    )
}

/// Every object under the table's location, data and metadata alike.
async fn objects_under(location: &str) -> Vec<String> {
    let rest = location
        .strip_prefix("s3://")
        .unwrap_or_else(|| panic!("an s3 location: {location}"));
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint("http://localhost:19000")
        .with_region("us-east-1")
        .with_access_key_id("s3admin")
        .with_secret_access_key("s3admin")
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("S3 client");
    let mut keys: Vec<String> = store
        .list(Some(&ObjectPath::from(prefix)))
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .expect("list table objects");
    keys.sort();
    keys
}

async fn ids(handler: &sqe_coordinator::QueryHandler, session: &Session) -> Vec<String> {
    let batches = handler
        .execute(
            session,
            &format!("SELECT id FROM {TABLE} ORDER BY id"),
            None,
        )
        .await
        .expect("SELECT");
    let mut out = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            out.push(crate::common::fmt_val(b.column(0).as_ref(), r));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn a_reader_is_refused_dml_before_any_file_is_written() {
    crate::common::init_tracing();
    let config = vended_only_config();
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let root = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let handler = handler(config.clone());

    let _ = handler
        .execute(&root, &format!("DROP TABLE IF EXISTS {TABLE}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {TABLE} (id BIGINT, name STRING)"),
        format!("INSERT INTO {TABLE} VALUES (1, 'a'), (2, 'b')"),
    ] {
        handler
            .execute(&root, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let catalog = sqe_catalog::rest_catalog::SessionCatalog::for_session(
        &config,
        None,
        root.access_token().expose(),
    )
    .await
    .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs(["test_ns", "dml_precheck_t"]).unwrap();
    let location = catalog
        .load_table(&ident)
        .await
        .expect("load table")
        .metadata()
        .location()
        .to_string();

    let reader = session_for(
        READER,
        principal_token(root.access_token().expose(), READER, &[]).await,
    );
    let props_writer = session_for(
        PROPS_WRITER,
        principal_token(
            root.access_token().expose(),
            PROPS_WRITER,
            &["TABLE_WRITE_PROPERTIES"],
        )
        .await,
    );

    let before = objects_under(&location).await;
    for (who, session) in [("reader", &reader), ("properties writer", &props_writer)] {
        assert_eq!(
            ids(&handler, session).await,
            vec!["1", "2"],
            "the {who} reads"
        );
        for sql in [
            format!("INSERT INTO {TABLE} VALUES (3, 'c')"),
            format!("DELETE FROM {TABLE} WHERE id = 1"),
            format!("UPDATE {TABLE} SET name = 'z' WHERE id = 2"),
            format!(
                "MERGE INTO {TABLE} AS t USING {TABLE} AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET name = 'm'"
            ),
            format!("TRUNCATE TABLE {TABLE}"),
        ] {
            let err = handler
                .execute(session, &sql, None)
                .await
                .expect_err(&format!("the {who} is refused: {sql}"));
            assert_eq!(
                err.error_code(),
                SqeErrorCode::AccessDenied,
                "{who}: {sql}: {err}"
            );
            assert!(
                err.to_string().contains("no write access"),
                "{who}: {sql}: {err}"
            );
        }
    }
    assert_eq!(
        objects_under(&location).await,
        before,
        "a refused statement writes no object, the probe leaves none"
    );

    // The owner's DELETE goes through as before.
    handler
        .execute(&root, &format!("DELETE FROM {TABLE} WHERE id = 1"), None)
        .await
        .expect("the owner deletes");
    assert_eq!(ids(&handler, &root).await, vec!["2"]);

    let _ = handler
        .execute(&root, &format!("DROP TABLE IF EXISTS {TABLE}"), None)
        .await;
}

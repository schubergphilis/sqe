//! Writes to a named `[catalogs.<name>]` entry that reads with engine
//! credentials (#934) load their target with the person's delegated keys.
//!
//! The write path builds that catalog through
//! [`CatalogDiscovery::configured_session_catalog`]. UPDATE, DELETE and MERGE
//! load the target with a plain `load_table` and then run
//! `check_write_access`, which refuses an engine-keyed table. A catalog that
//! came back in engine mode therefore refused every such write, also for a
//! person allowed to write. The legacy `[catalog]` block never took this path.
//!
//! No stack needed: wiremock stands in for the catalog. The engine identity
//! binds to the process-wide policy store, so the test holds
//! [`crate::common::ENGINE_WIRING_LOCK`].

use std::sync::Arc;

use iceberg::{NamespaceIdent, TableIdent};
use sqe_catalog::engine_identity::is_engine_keyed;
use sqe_coordinator::session_context::PolarisCatalogDiscovery;
use sqe_coordinator::write_handler::CatalogDiscovery;
use sqe_core::config::CatalogAuthConfig;
use sqe_core::{Session, SqeConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PERSON: &str = "person-tok";
const ENGINE: &str = "engine-tok";
const LOAD_TABLE_RESPONSE: &str = include_str!(
    "../../../../vendor/iceberg-rust/crates/catalog/rest/testdata/load_table_response.json"
);

/// `[catalogs.lake]` with engine credentials for `client_id`, beside an
/// unused legacy `[catalog]` block.
fn config(server: &MockServer, client_id: &str) -> SqeConfig {
    let toml_text = format!(
        r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "http://127.0.0.1:9/unused"
warehouse = "home"

[catalogs.lake]
catalog_url = "{uri}"
warehouse = "lake_wh"
require_vended_credentials = true

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = ""
s3_secret_key = ""
s3_region = "us-east-1"
s3_path_style = true
"#,
        uri = server.uri()
    );
    let mut config = toml::from_str::<SqeConfig>(&toml_text).expect("test config parses");
    config
        .catalogs
        .get_mut("lake")
        .expect("lake configured")
        .credentials = Some(CatalogAuthConfig::ClientCredentials {
        token_endpoint: format!("{}/token", server.uri()),
        client_id: client_id.into(),
        client_secret: "s".into(),
        scope: None,
        client_secret_file: None,
    });
    config
}

fn person() -> Session {
    Session::new(
        "alice".to_string(),
        sqe_core::SecretString::new(PERSON.to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec![],
    )
}

/// The in-memory policy store with no policy grants every `select`, so a
/// read load on the engine identity would succeed with engine keys.
fn install_open_policy_store() {
    let store: Arc<dyn sqe_policy::PolicyStore> =
        Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    sqe_coordinator::engine_identity_wiring::install(
        Some(store),
        sqe_core::config::MaskPrecedence::default(),
        None,
    );
}

/// `/v1/config`, the engine token, and `sales.orders` under prefix `lake`:
/// the person's keys on `loadTable`, the engine's on `/credentials`.
async fn catalog(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({ "defaults": {}, "overrides": { "prefix": "lake" } }),
        ))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": ENGINE,
            "token_type": "bearer",
            "expires_in": 3600
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/lake/namespaces"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "namespaces": [["sales"]] })),
        )
        .mount(server)
        .await;
    let mut body: serde_json::Value = serde_json::from_str(LOAD_TABLE_RESPONSE).expect("fixture");
    body["config"] = serde_json::json!({
        "s3.access-key-id": "PERSON", "s3.secret-access-key": "person-secret"
    });
    Mock::given(method("GET"))
        .and(path("/v1/lake/namespaces/sales/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/lake/namespaces/sales/tables/orders/credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "storage-credentials": [{
                "prefix": "s3://warehouse/database",
                "config": {
                    "s3.access-key-id": "ENGINE",
                    "s3.secret-access-key": "engine-secret",
                    "s3.session-token": "engine-session"
                }
            }]
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_configured_engine_catalog_loads_write_targets_with_the_persons_keys() {
    let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
    install_open_policy_store();
    let server = MockServer::start().await;
    catalog(&server).await;
    let config = config(&server, "engine-configured-write");

    let catalog = PolarisCatalogDiscovery
        .configured_session_catalog("lake", &config, &person(), None)
        .await
        .expect("lake builds for the session");
    let orders = TableIdent::new(NamespaceIdent::new("sales".into()), "orders".into());
    let table = catalog
        .load_table(&orders)
        .await
        .unwrap_or_else(|e| panic!("load: {e}"));

    assert!(
        !is_engine_keyed(&table),
        "a write target must not carry the engine's read keys"
    );
    assert_eq!(
        table
            .file_io()
            .config()
            .props()
            .get("s3.access-key-id")
            .map(String::as_str),
        Some("PERSON")
    );
    let all = server.received_requests().await.expect("recording on");
    assert!(
        all.iter().all(|r| !r.url.path().ends_with("/credentials")),
        "no engine key fetch for a write target"
    );
}

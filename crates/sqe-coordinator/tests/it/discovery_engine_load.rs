//! Engine reads for discovered warehouses, seen from a table load
//! (`[query] discovery_engine_read`, !1948).
//!
//! The proof itself is unit-tested in `discovery_engine::tests::proof`.
//! These tests build the discovered warehouse with `discover_catalog_provider`
//! against a wiremock catalog and load `sales.orders` through it, so they see
//! what the catalog sees: the `X-Iceberg-Access-Delegation` header on the
//! person's load, and the engine's `/credentials` call.
//!
//! No stack needed: they run in a plain `cargo test --test it`, not under
//! `--ignored`. The engine identity binds to the process-wide policy store,
//! so each test holds [`crate::common::ENGINE_WIRING_LOCK`].

use std::sync::Arc;

use iceberg::{NamespaceIdent, TableIdent};
use sqe_catalog::engine_identity::is_engine_keyed;
use sqe_core::config::{CatalogAuthConfig, DiscoveryEngineRead};
use sqe_core::{Session, SqeConfig};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PERSON: &str = "person-tok";
const ENGINE: &str = "engine-tok";
const DELEGATION: &str = "x-iceberg-access-delegation";
const LOAD_TABLE_RESPONSE: &str = include_str!(
    "../../../../vendor/iceberg-rust/crates/catalog/rest/testdata/load_table_response.json"
);

/// A polaris-auto template at `server`, vended credentials required, engine
/// credentials for `client_id` with `scope = "profile"`, allowlist `ws_*`.
///
/// The engine token and the REST client are cached per process, and
/// wiremock reuses ports, so each test names its own client id and
/// warehouses: a token or client cached by an earlier test would hide calls.
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
catalog_url = "{uri}"
warehouse = "home"
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
    config.catalog.credentials = Some(CatalogAuthConfig::ClientCredentials {
        token_endpoint: format!("{}/token", server.uri()),
        client_id: client_id.into(),
        client_secret: "s".into(),
        scope: Some("profile".into()),
        client_secret_file: None,
    });
    config.query.catalog_discovery = sqe_core::config::CatalogDiscovery::PolarisAuto;
    config.query.discovery_engine_read = DiscoveryEngineRead::Template;
    config.query.discovery_engine_warehouses = vec!["ws_*".into()];
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

fn orders() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("sales".into()), "orders".into())
}

/// The engine identity needs a policy store; the in-memory one with no
/// policy grants every `select`.
fn install_open_policy_store() {
    let store: Arc<dyn sqe_policy::PolicyStore> =
        Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    sqe_coordinator::engine_identity_wiring::install(
        Some(store),
        sqe_core::config::MaskPrecedence::default(),
        None,
    );
}

fn prefix(p: &str) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(serde_json::json!({ "defaults": {}, "overrides": { "prefix": p } }))
}

/// `/v1/config?warehouse=W` answers `response` to `bearer`, `times` times.
async fn config_for(
    server: &MockServer,
    warehouse: &str,
    bearer: &str,
    response: ResponseTemplate,
    times: u64,
) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(query_param("warehouse", warehouse))
        .and(header("authorization", format!("Bearer {bearer}").as_str()))
        .respond_with(response)
        .expect(times)
        .mount(server)
        .await;
}

/// The engine token endpoint, called `times` times.
async fn engine_token(server: &MockServer, times: u64) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": ENGINE,
            "token_type": "bearer",
            "expires_in": 3600
        })))
        .expect(times)
        .mount(server)
        .await;
}

/// Namespace listing, the namespace, and `sales.orders` under prefix `p`:
/// the person's keys on `loadTable` (any headers), the engine's on
/// `/credentials`.
async fn warehouse(server: &MockServer, p: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/v1/{p}/namespaces")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "namespaces": [["sales"]] })),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/v1/{p}/namespaces/sales")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "namespace": ["sales"], "properties": {} })),
        )
        .mount(server)
        .await;
    let mut body: serde_json::Value = serde_json::from_str(LOAD_TABLE_RESPONSE).expect("fixture");
    body["config"] = serde_json::json!({
        "s3.access-key-id": "PERSON", "s3.secret-access-key": "person-secret"
    });
    Mock::given(method("GET"))
        .and(path(format!("/v1/{p}/namespaces/sales/tables/orders")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/v1/{p}/namespaces/sales/tables/orders/credentials"
        )))
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

/// Discover `w` as `session`, load `sales.orders`, return the table's access
/// key id and whether it is engine-keyed.
async fn load_orders(config: &SqeConfig, w: &str) -> (String, bool) {
    let (_, catalog) = sqe_coordinator::session_context::discover_catalog_provider(
        w,
        config,
        &person(),
        None,
        None,
    )
    .await
    .unwrap_or_else(|| panic!("{w} discovered"));
    let table = catalog
        .load_table(&orders())
        .await
        .unwrap_or_else(|e| panic!("{w} load: {e}"));
    let key = table
        .file_io()
        .config()
        .props()
        .get("s3.access-key-id")
        .cloned()
        .unwrap_or_default();
    (key, is_engine_keyed(&table))
}

async fn requests(server: &MockServer) -> Vec<wiremock::Request> {
    server.received_requests().await.expect("recording on")
}

fn under<'a>(requests: &'a [wiremock::Request], p: &str) -> Vec<&'a wiremock::Request> {
    let root = format!("/v1/{p}/");
    requests
        .iter()
        .filter(|r| r.url.path().starts_with(&root))
        .collect()
}

fn credential_fetches<'a>(requests: &[&'a wiremock::Request]) -> Vec<&'a wiremock::Request> {
    requests
        .iter()
        .filter(|r| r.url.path().ends_with("/credentials"))
        .copied()
        .collect()
}

/// Proved: no catalog request for the warehouse asks for delegated keys, the
/// table reads with keys the engine fetched with its own token, and the
/// token form carried the configured scope verbatim.
#[tokio::test]
async fn a_proved_warehouse_loads_with_engine_keys_and_no_delegation() {
    let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
    install_open_policy_store();
    let server = MockServer::start().await;
    config_for(&server, "ws_proved", PERSON, prefix("p-proved"), 2).await;
    config_for(&server, "ws_proved", ENGINE, prefix("p-proved"), 1).await;
    engine_token(&server, 1).await;
    warehouse(&server, "p-proved").await;
    let config = config(&server, "engine-proved");

    assert_eq!(
        load_orders(&config, "ws_proved").await,
        ("ENGINE".into(), true)
    );

    let all = requests(&server).await;
    let warehouse_requests = under(&all, "p-proved");
    for r in &warehouse_requests {
        assert!(
            r.headers.get(DELEGATION).is_none(),
            "{} {} carried the delegation header",
            r.method,
            r.url.path()
        );
    }
    let fetches = credential_fetches(&warehouse_requests);
    assert_eq!(fetches.len(), 1, "one engine key fetch");
    assert_eq!(
        fetches[0].headers.get("authorization").expect("bearer"),
        format!("Bearer {ENGINE}").as_str()
    );
    let token_forms: Vec<String> = all
        .iter()
        .filter(|r| r.url.path() == "/token")
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert_eq!(token_forms.len(), 1, "one engine token request");
    let form: Vec<&str> = token_forms[0].split('&').collect();
    assert!(form.contains(&"grant_type=client_credentials"), "{form:?}");
    let scopes: Vec<&str> = form
        .iter()
        .filter_map(|kv| kv.strip_prefix("scope="))
        .collect();
    assert_eq!(scopes, ["profile"], "the configured scope, verbatim");
}

/// Every refusal a load can see: the person's load asks for delegated keys
/// and the engine fetches nothing.
#[tokio::test]
async fn a_refused_warehouse_loads_with_delegation_and_no_engine_fetch() {
    let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
    install_open_policy_store();
    let server = MockServer::start().await;
    // Engine `/config` refused.
    config_for(&server, "ws_r403", PERSON, prefix("p-r403"), 2).await;
    config_for(&server, "ws_r403", ENGINE, ResponseTemplate::new(403), 1).await;
    warehouse(&server, "p-r403").await;
    // Engine `/config` names another catalog.
    config_for(&server, "ws_rprefix", PERSON, prefix("p-rprefix"), 2).await;
    config_for(&server, "ws_rprefix", ENGINE, prefix("p-x"), 1).await;
    warehouse(&server, "p-rprefix").await;
    // Outside the allowlist: no proof call, so the catalog build makes the
    // only `/config` call.
    config_for(&server, "other_r", PERSON, prefix("p-other"), 1).await;
    warehouse(&server, "p-other").await;
    engine_token(&server, 1).await;
    let config = config(&server, "engine-refused");

    for (w, p) in [
        ("ws_r403", "p-r403"),
        ("ws_rprefix", "p-rprefix"),
        ("other_r", "p-other"),
    ] {
        assert_eq!(
            load_orders(&config, w).await,
            ("PERSON".into(), false),
            "{w}"
        );
        let all = requests(&server).await;
        let warehouse_requests = under(&all, p);
        let loads: Vec<_> = warehouse_requests
            .iter()
            .filter(|r| r.url.path().ends_with("/tables/orders"))
            .collect();
        assert!(!loads.is_empty(), "{w}: the table was loaded");
        for r in loads {
            assert_eq!(
                r.headers.get(DELEGATION).map(|v| v.to_str().unwrap_or("")),
                Some("vended-credentials"),
                "{w}: the person's load asks for delegated keys"
            );
        }
        assert!(
            credential_fetches(&warehouse_requests).is_empty(),
            "{w}: no engine key fetch"
        );
    }
}

/// Two proved warehouses: one engine token serves both proofs and both key
/// fetches.
#[tokio::test]
async fn two_proved_warehouses_load_on_one_engine_token() {
    let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
    install_open_policy_store();
    let server = MockServer::start().await;
    let pairs = [("ws_two_a", "p-two-a"), ("ws_two_b", "p-two-b")];
    for (w, p) in pairs {
        config_for(&server, w, PERSON, prefix(p), 2).await;
        config_for(&server, w, ENGINE, prefix(p), 1).await;
        warehouse(&server, p).await;
    }
    engine_token(&server, 1).await;
    let config = config(&server, "engine-two");
    for (w, p) in pairs {
        assert_eq!(
            load_orders(&config, w).await,
            ("ENGINE".into(), true),
            "{w}"
        );
        let all = requests(&server).await;
        assert_eq!(credential_fetches(&under(&all, p)).len(), 1, "{w}");
    }
}

/// A session whose signed catalog scope leaves the warehouse out (#908) is
/// refused before any proof call or catalog build.
#[tokio::test]
async fn a_warehouse_outside_the_catalog_scope_makes_no_call() {
    let _wiring = crate::common::ENGINE_WIRING_LOCK.lock().await;
    install_open_policy_store();
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let config = config(&server, "engine-scoped");
    let scope = sqe_core::catalog_scope::CatalogScope::new(
        "alice",
        "jti-1",
        chrono::Utc::now() + chrono::Duration::hours(1),
        [(
            "ws_b".to_string(),
            sqe_core::catalog_scope::CatalogAccess::ReadOnly,
        )],
    );
    let scoped = person().with_catalog_scope(Some(Arc::new(scope)));
    assert!(sqe_coordinator::session_context::discover_catalog_provider(
        "ws_a", &config, &scoped, None, None
    )
    .await
    .is_none());
}

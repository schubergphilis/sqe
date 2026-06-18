//! HTTP-path tests for RangerStore against a mock Ranger Admin (wiremock).

use sqe_core::config::RangerPolicyConfig;
use sqe_core::SessionUser;
use sqe_policy::ranger_store::RangerStore;
use sqe_policy::PolicyStore;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUNDLE: &str = r#"{
  "policyVersion": 1,
  "policies": [
    {"id": 1, "policyType": 1, "isEnabled": true,
     "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}, "column": {"values": ["amount"]}},
     "dataMaskPolicyItems": [{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]},
    {"id": 2, "policyType": 2, "isEnabled": true,
     "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}},
     "rowFilterPolicyItems": [{"roles": ["analyst"], "rowFilterInfo": {"filterExpr": "region = 'EU' AND tier < 3"}}]}
  ]
}"#;

fn cfg(url: &str) -> RangerPolicyConfig {
    RangerPolicyConfig {
        url: url.to_string(),
        service_name: "hive".to_string(),
        // cache_ttl_secs = 0 keeps each test independent of moka's cache state.
        cache_ttl_secs: 0,
        ..RangerPolicyConfig::default()
    }
}

fn analyst() -> SessionUser {
    SessionUser {
        username: "alice".into(),
        roles: vec!["analyst".into()],
    }
}

#[tokio::test]
async fn resolves_mask_and_filter_over_http() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let policy = store.resolve(&analyst(), "orders", "sales").await.unwrap();

    assert!(policy.column_masks.contains_key("amount"));
    assert_eq!(policy.row_filters.len(), 1);
}

#[tokio::test]
async fn fail_closed_on_server_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let result = store.resolve(&analyst(), "orders", "sales").await;
    assert!(result.is_err(), "5xx must fail closed (Err), not allow-all");
}

#[tokio::test]
async fn fail_closed_on_garbage_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<<not json>>"))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    assert!(store.resolve(&analyst(), "orders", "sales").await.is_err());
}

#[tokio::test]
async fn non_matching_user_gets_empty_policy() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let bob = SessionUser {
        username: "bob".into(),
        roles: vec!["engineer".into()],
    };
    let policy = store.resolve(&bob, "orders", "sales").await.unwrap();
    assert!(policy.column_masks.is_empty());
    assert!(policy.row_filters.is_empty());
}

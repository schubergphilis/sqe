//! HTTP-path tests for `RangerGrantBackend` against a mock Ranger Admin
//! (wiremock). The pure mapping/parsing logic (`map_sql_to_ranger_access`,
//! `policies_to_entries`, `evaluate_access`) is unit-tested in-module; these
//! tests exercise the I/O glue (`post_grant_revoke`, `fetch_policies`) through
//! the public `GrantBackend` trait, covering the success path and the non-200
//! error path. DDL must fail loudly on a Ranger error, never silently succeed.
//!
//! Pins MED-untested-io-and-failclosed-branches (grants/ranger.rs:258-282,
//! 394-421).

use sqe_policy::grants::ranger::RangerGrantBackend;
use sqe_policy::grants::{GrantBackend, GrantFilter, GrantStatement, Grantee};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SERVICE: &str = "polaris";

fn backend(url: &str) -> RangerGrantBackend {
    RangerGrantBackend::new(
        url, SERVICE, "admin", "admin-pw", "POLARIS", 30, false,
    )
    .unwrap()
}

fn grant_stmt() -> GrantStatement {
    GrantStatement {
        privilege: "SELECT".to_string(),
        catalog: Some("wh".to_string()),
        namespace: Some("sales".to_string()),
        table: Some("orders".to_string()),
        grantee: Grantee::Role("analyst".to_string()),
    }
}

/// Success path: a 200 from the grant endpoint makes `grant()` return Ok.
#[tokio::test]
async fn grant_succeeds_on_200() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    backend
        .grant("token", &grant_stmt())
        .await
        .expect("grant against a 200 endpoint must succeed");
}

/// Error path: a non-200 from the grant endpoint makes `grant()` return Err.
/// DDL must fail loudly, not pretend the grant landed.
#[tokio::test]
async fn grant_fails_loudly_on_non_200() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let err = backend
        .grant("token", &grant_stmt())
        .await
        .expect_err("a 4xx from Ranger must surface as Err");
    assert!(
        err.to_string().contains("403") || err.to_string().to_lowercase().contains("grant"),
        "error must mention the failed grant / status, got: {err}"
    );
}

/// `fetch_policies` success path through `show_grants`: the public v2 policy API
/// returns a bare JSON array, which is parsed into GrantEntry rows and filtered.
#[tokio::test]
async fn show_grants_parses_policies_on_200() {
    let server = MockServer::start().await;
    let body = serde_json::json!([
        {
            "name": "p1",
            "resources": {
                "catalog": {"values": ["wh"]},
                "namespace": {"values": ["sales"]},
                "table": {"values": ["orders"]}
            },
            "policyItems": [
                {"users": [], "roles": ["analyst"],
                 "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
        }
    ]);
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", SERVICE))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let entries = backend
        .show_grants("token", &GrantFilter::ToGrantee(Grantee::Role("analyst".to_string())))
        .await
        .expect("show_grants against a 200 endpoint must succeed");
    assert_eq!(entries.len(), 1, "one matching grant for role analyst");
    assert_eq!(entries[0].grantee_name, "analyst");
    assert_eq!(entries[0].privilege, "table-data-read");
}

/// `fetch_policies` error path through `show_grants`: a non-200 from the policy
/// API must surface as Err, not an empty grant list (which would silently hide
/// real grants from SHOW GRANTS).
#[tokio::test]
async fn show_grants_fails_loudly_on_non_200() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let err = backend
        .show_grants("token", &GrantFilter::ToGrantee(Grantee::Role("analyst".to_string())))
        .await
        .expect_err("a 5xx from the policy API must surface as Err");
    assert!(
        err.to_string().contains("500") || err.to_string().to_lowercase().contains("fetch"),
        "error must mention the failed fetch / status, got: {err}"
    );
}

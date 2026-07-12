//! Wiremock-level integration tests for `OpaStore::resolve()`.
//!
//! Issue #5 introduced fail-closed behaviour for malformed or missing OPA
//! responses. The existing in-module tests only verified Serde defaults on
//! the `OpaResult` struct; they did not exercise the HTTP boundary. These
//! tests stand up a wiremock OPA and call `resolve()` against it, pinning
//! the contract that any degraded response yields either a deny policy
//! (FALSE filter) or a hard error. None of them is allowed to return an
//! empty/default `ResolvedPolicy`, since that would silently lift policy.
//!
//! Pairs with `tests/rewriter_integration.rs`, which covers the plan-side
//! end of the fail-closed story.

use sqe_core::SessionUser;
use sqe_policy::opa::OpaStore;
use sqe_policy::PolicyStore;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

const POLICY_PATH: &str = "sqe/policy/evaluate";

fn user() -> SessionUser {
    SessionUser {
        username: "alice".to_string(),
        roles: vec!["analyst".to_string()],
        subject: None,
        email: None,
        groups: vec![],
    }
}

async fn store_pointing_at(server: &MockServer) -> OpaStore {
    // cache_ttl_secs = 0 keeps each test independent of moka's cache state.
    OpaStore::new(&server.uri(), POLICY_PATH, 0).expect("build OPA store")
}

/// Helper: a deny policy must return a single FALSE row filter and no other
/// content. Any other shape is a fail-open regression.
fn assert_deny_policy(p: &sqe_policy::ResolvedPolicy) {
    use datafusion::logical_expr::Expr;
    use datafusion::scalar::ScalarValue;
    assert_eq!(
        p.row_filters.len(),
        1,
        "deny policy must inject exactly one FALSE row filter, got {} filters",
        p.row_filters.len()
    );
    match &p.row_filters[0] {
        Expr::Literal(ScalarValue::Boolean(Some(false)), _) => {}
        other => panic!("expected FALSE literal as the deny filter, got: {other:?}"),
    }
    assert!(
        p.column_masks.is_empty(),
        "deny policy carries no column masks"
    );
    assert!(
        p.restricted_columns.is_empty(),
        "deny policy carries no restricted columns"
    );
}

/// Explicit deny: OPA returned `allow: false`. Resolver must inject the
/// FALSE filter and not error. This is the documented happy path for an
/// "access denied" verdict.
#[tokio::test]
async fn opa_allow_false_yields_deny_policy() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": { "allow": false }
        })))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let policy = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect("explicit deny must not error");
    assert_deny_policy(&policy);
}

/// Allow with policy contents: the resolver returns a populated policy with
/// the row filter parsed into a DataFusion expression. Sanity check for the
/// happy path the deny case is contrasted against.
#[tokio::test]
async fn opa_allow_true_yields_populated_policy() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": {
                "allow": true,
                "row_filters": ["clearance >= 3"],
                "column_masks": { "ssn": "hash" },
                "restricted_columns": ["internal_notes"],
            }
        })))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let policy = store.resolve(&user(), "employees", "hr").await.unwrap();
    assert_eq!(policy.row_filters.len(), 1);
    assert_eq!(policy.column_masks.len(), 1);
    assert_eq!(
        policy.restricted_columns,
        vec!["internal_notes".to_string()]
    );
}

/// `{"result": null}` is OPA's response when the queried policy package is
/// missing (typo in policy_path, mis-deployed bundle, partial reload). The
/// resolver MUST surface an error, not silently lift policy.
#[tokio::test]
async fn opa_result_null_returns_error_not_empty_policy() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": null
        })))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("null result must be a hard error");
    let msg = err.to_string();
    assert!(
        msg.contains("policy package missing") || msg.contains("policy"),
        "error must mention the missing policy package, got: {msg}"
    );
}

/// Empty body (network blip, half-deployed proxy) deserialises to a default
/// `OpaResponse` with `result = None`. Same contract as the null-result case.
#[tokio::test]
async fn opa_empty_object_response_returns_error() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("empty response must be a hard error");
    assert!(err.to_string().contains("policy"), "got: {err}");
}

/// HTTP 500 from OPA must error. The resolver does not retry, does not
/// degrade to a default policy, and does not cache a permit.
#[tokio::test]
async fn opa_500_returns_error() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("5xx must be a hard error");
    let msg = err.to_string();
    assert!(
        msg.contains("500") || msg.to_lowercase().contains("opa"),
        "error must mention the upstream status, got: {msg}"
    );
}

/// Body that is not valid JSON must error. The resolver does not invent a
/// permissive `OpaResult`. Catches accidental "parse loosely" refactors.
#[tokio::test]
async fn opa_malformed_json_returns_error() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("not-json-at-all")
                .insert_header("content-type", "application/json"),
        )
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("malformed JSON must be a hard error");
    assert!(
        err.to_string().to_lowercase().contains("parse")
            || err.to_string().to_lowercase().contains("opa"),
        "error must mention parse failure or OPA, got: {err}"
    );
}

/// Network error: point the store at a closed port. The resolver must
/// propagate the underlying request failure, never invent a default policy.
#[tokio::test]
async fn opa_network_error_returns_error() {
    // Port 1 is not assigned and the OS refuses connections immediately.
    // No wiremock involved.
    let store = OpaStore::new("http://127.0.0.1:1", POLICY_PATH, 0).expect("build store");
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("network error must be a hard error");
    assert!(
        err.to_string().to_lowercase().contains("opa")
            || err.to_string().to_lowercase().contains("request"),
        "error must mention OPA or request failure, got: {err}"
    );
}

/// OPA returns a row filter the parser cannot understand. Fail-closed: the
/// resolver must error, NOT silently drop the filter. Otherwise an OPA
/// policy author who writes a complex filter sees it disappear, and rows
/// the policy meant to hide become visible.
#[tokio::test]
async fn opa_unparseable_row_filter_returns_error() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path(format!("/v1/data/{POLICY_PATH}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": {
                "allow": true,
                "row_filters": ["a OR b OR c"],
            }
        })))
        .mount(&server)
        .await;

    let store = store_pointing_at(&server).await;
    let err = store
        .resolve(&user(), "employees", "hr")
        .await
        .expect_err("unparseable filter must fail closed");
    assert!(
        err.to_string().contains("unsupported row filter"),
        "error must call out the unparseable filter, got: {err}"
    );
}

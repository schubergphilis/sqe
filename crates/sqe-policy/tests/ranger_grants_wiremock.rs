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
        // Ranger authorizes against this, so it is not optional. See
        // `a_grant_with_no_grantor_is_refused_before_any_request`.
        grantor: Some("carol".to_string()),
        with_grant_option: false,
        object: Default::default(),
    }
}

/// POSTs the backend actually made to the grant endpoint.
///
/// The path matters: the grant flow also GETs the policy list and may PUT a
/// provenance label, and counting every request would make "one grant" and "three
/// grants" indistinguishable.
async fn grant_posts(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            r.method == wiremock::http::Method::POST
                && r.url.path() == format!("/service/plugins/services/grant/{SERVICE}")
        })
        .count()
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

/// A grant with no grantor is refused, and refused BEFORE anything is sent.
///
/// The fallback it replaces set `grantor` to the configured Ranger admin user.
/// That is not a harmless default: Ranger authorizes against the grantor, so with
/// `grant_authority = "ranger-delegate"` standing the role gate down, a code path
/// leaving grantor unset would have performed the grant with SQE's own authority --
/// full escalation from any authenticated session. Asserting zero requests is the
/// point: refusing after the catalog level had already landed would be worse than
/// not refusing at all.
#[tokio::test]
async fn a_grant_with_no_grantor_is_refused_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let mut stmt = grant_stmt();
    stmt.grantor = None;
    let err = backend
        .grant("token", &stmt)
        .await
        .expect_err("a grant with no grantor must be refused, not performed as admin");
    assert!(
        err.to_string().contains("grantor"),
        "the error must name what is missing, got: {err}"
    );
    assert_eq!(
        grant_posts(&server).await,
        0,
        "nothing may be written when the authority the write would be checked \
         against is unknown"
    );
}

/// A traversal level the grantee already holds is not re-granted.
///
/// Not an optimization. Ranger's delegate admin does not cascade upward (verified
/// against 2.8: a grantor holding it on `cat.ns.tbl` gets 403 on `cat` and on
/// `cat.ns`), and the plan writes the catalog level FIRST, so without this every
/// delegated table grant fails on its first call -- on a write that would have
/// changed nothing. Ranger MERGES access types, so skipping a set already present
/// cannot lose anything.
#[tokio::test]
async fn an_already_held_traversal_level_is_not_re_granted() {
    let server = MockServer::start().await;
    // Both ancestors of wh.sales.orders, already carrying exactly what v4's SELECT
    // plan plans for them. `root` is the configured realm, not `*`.
    let policies = serde_json::json!([
        {
            "id": 1, "name": "catalog-level",
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]}},
            "policyItems": [{"roles": ["analyst"],
                "accesses": [{"type": "namespace-list", "isAllowed": true}]}]
        },
        {
            "id": 2, "name": "namespace-level",
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]},
                          "namespace": {"values": ["sales"]}},
            "policyItems": [{"roles": ["analyst"], "accesses": [
                {"type": "namespace-list", "isAllowed": true},
                {"type": "namespace-properties-read", "isAllowed": true}]}]
        }
    ]);
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policies))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    backend
        .grant("token", &grant_stmt())
        .await
        .expect("grant must succeed");
    assert_eq!(
        grant_posts(&server).await,
        1,
        "only the level the statement NAMES should be written; the two traversal \
         levels were already held"
    );
}

/// A DISABLED policy does not count as holding anything.
///
/// Ranger returns a console-disabled policy with `isEnabled: false` and its
/// `policyItems` intact, while enforcement ignores it. Reading those items as held
/// would skip a level the grantee does not have, and the grant would report success
/// while conferring nothing.
#[tokio::test]
async fn a_disabled_policy_does_not_count_as_already_held() {
    let server = MockServer::start().await;
    let policies = serde_json::json!([
        {
            "id": 1, "name": "catalog-level", "isEnabled": false,
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]}},
            "policyItems": [{"roles": ["analyst"],
                "accesses": [{"type": "namespace-list", "isAllowed": true}]}]
        },
        {
            "id": 2, "name": "namespace-level", "isEnabled": true,
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]},
                          "namespace": {"values": ["sales"]}},
            "policyItems": [{"roles": ["analyst"], "accesses": [
                {"type": "namespace-list", "isAllowed": true},
                {"type": "namespace-properties-read", "isAllowed": true}]}]
        }
    ]);
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policies))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    backend
        .grant("token", &grant_stmt())
        .await
        .expect("grant must succeed");
    assert_eq!(
        grant_posts(&server).await,
        2,
        "the disabled catalog policy must be re-granted; only the enabled namespace \
         level counts as held"
    );
}

/// The level the statement names is written even when it looks already-held.
///
/// The skip is deliberately ancestors-only. The named object's policy may still
/// need access types added, or `delegateAdmin` set from `WITH GRANT OPTION`, and
/// skipping it on a subset match would make `GRANT ... WITH GRANT OPTION` a silent
/// no-op for anyone who already had plain SELECT.
#[tokio::test]
async fn the_named_level_is_written_even_when_already_held() {
    let server = MockServer::start().await;
    let policies = serde_json::json!([
        {
            "id": 1, "name": "catalog-level",
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]}},
            "policyItems": [{"roles": ["analyst"],
                "accesses": [{"type": "namespace-list", "isAllowed": true}]}]
        },
        {
            "id": 2, "name": "namespace-level",
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]},
                          "namespace": {"values": ["sales"]}},
            "policyItems": [{"roles": ["analyst"], "accesses": [
                {"type": "namespace-list", "isAllowed": true},
                {"type": "namespace-properties-read", "isAllowed": true}]}]
        },
        {
            "id": 3, "name": "table-level",
            "resources": {"root": {"values": ["POLARIS"]}, "catalog": {"values": ["wh"]},
                          "namespace": {"values": ["sales"]}, "table": {"values": ["orders"]}},
            "policyItems": [{"roles": ["analyst"], "accesses": [
                {"type": "table-data-read", "isAllowed": true},
                {"type": "table-list", "isAllowed": true},
                {"type": "table-properties-read", "isAllowed": true}]}]
        }
    ]);
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policies))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let mut stmt = grant_stmt();
    stmt.with_grant_option = true;
    backend.grant("token", &stmt).await.expect("grant must succeed");
    assert_eq!(
        grant_posts(&server).await,
        1,
        "the named level must still be POSTed so delegateAdmin can be set"
    );
}

/// A 403 on a traversal level says WHICH level, and what an admin has to do.
///
/// "Ranger grant failed (HTTP 403)" on a statement that named a table sends the
/// reader looking at the table's policy, which is not where the problem is.
#[tokio::test]
async fn a_403_on_a_traversal_level_names_the_level_and_the_fix() {
    let server = MockServer::start().await;
    // No GET mock: nothing is known to be already held, so the catalog level -- the
    // first call of the plan -- is attempted and refused.
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/grant/{SERVICE}")))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"{"msgDesc":"User doesn't have necessary permission to grant access"}"#,
        ))
        .mount(&server)
        .await;

    let backend = backend(&server.uri());
    let err = backend
        .grant("token", &grant_stmt())
        .await
        .expect_err("a 403 must surface")
        .to_string();
    assert!(err.contains("catalog level"), "must name the level, got: {err}");
    assert!(
        err.contains("does not cascade"),
        "must say why holding delegate admin on the table is not enough, got: {err}"
    );
    assert!(
        err.contains("USAGE ON DATABASE"),
        "must name the statement that fixes it, got: {err}"
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

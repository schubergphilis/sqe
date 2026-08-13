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
    // RangerDelegate on purpose: these cases assert against the PLUGIN
    // grant/revoke endpoint, which is the transport only this mode uses (it is the
    // one that authorizes the `grantor` field, which is the whole point of the
    // mode). The default `admin-role` writes through the authenticated policy API
    // instead, covered by `policy_api_*` below.
    RangerGrantBackend::new(
        url,
        SERVICE,
        "admin",
        "admin-pw",
        "POLARIS",
        30,
        false,
        sqe_core::config::GrantAuthority::RangerDelegate,
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

// ── REVOKE ALL PRIVILEGES ────────────────────────────────────────────────
//
// `REVOKE ALL PRIVILEGES ON <object>` means "this grantee holds nothing here
// afterwards", which is what Unity Catalog offers and what an operator reaches
// for during an incident. It exists because closing a gate otherwise requires
// knowing the privilege implication graph: `REVOKE SELECT` leaves a grantee
// reading through a surviving INSERT, since a writer must hold the metadata
// reads that authorize a table load.

fn revoke_all_stmt() -> sqe_policy::grants::RevokeStatement {
    sqe_policy::grants::RevokeStatement {
        privilege: "ALL PRIVILEGES".to_string(),
        catalog: Some("wh".to_string()),
        namespace: Some("sales".to_string()),
        table: Some("orders".to_string()),
        grantee: Grantee::Role("analyst".to_string()),
        grantor: Some("carol".to_string()),
        object: Default::default(),
    }
}

/// A policy at the table coordinate holding two access types for `analyst` and
/// one for an unrelated role.
fn policy_with_analyst_accesses() -> serde_json::Value {
    serde_json::json!([{
        "id": 7,
        "isEnabled": true,
        "policyLabels": ["table:analyst:SELECT", "table:analyst:INSERT", "table:bob:SELECT"],
        "resources": {
            "root": {"values": ["POLARIS"]},
            "catalog": {"values": ["wh"]},
            "namespace": {"values": ["sales"]},
            "table": {"values": ["orders"]}
        },
        "policyItems": [
            {"roles": ["analyst"], "accesses": [
                {"type": "table-data-read", "isAllowed": true},
                {"type": "table-properties-read", "isAllowed": true}
            ]},
            {"roles": ["auditor"], "accesses": [
                {"type": "table-list", "isAllowed": true}
            ]}
        ]
    }])
}

async fn revoke_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            r.method == wiremock::http::Method::POST
                && r.url.path() == format!("/service/plugins/services/revoke/{SERVICE}")
        })
        .filter_map(|r| serde_json::from_slice(&r.body).ok())
        .collect()
}

/// The revoke must name every access type the grantee actually holds, read from
/// Ranger rather than planned from the privilege. A grant written before
/// provenance labels existed, or straight through the Ranger console, is caught
/// this way; planning `ALL` would miss it.
#[tokio::test]
async fn revoke_all_privileges_removes_every_access_type_the_grantee_holds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policy_with_analyst_accesses()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/revoke/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    backend(&server.uri())
        .revoke("token", &revoke_all_stmt())
        .await
        .expect("REVOKE ALL PRIVILEGES must succeed");

    let bodies = revoke_bodies(&server).await;
    assert_eq!(bodies.len(), 1, "expected exactly one revoke POST: {bodies:?}");
    let types: Vec<String> = bodies[0]["accessTypes"]
        .as_array()
        .expect("accessTypes array")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    for expected in ["table-data-read", "table-properties-read"] {
        assert!(types.contains(&expected.to_string()), "missing {expected} in {types:?}");
    }
    assert!(
        !types.contains(&"table-list".to_string()),
        "revoked another role's access type: {types:?}"
    );
}

/// `ALL PRIVILEGES` must reach the TABLE coordinate. `GRANT ALL` deliberately
/// binds no deeper than the catalog, because granting "everything" at a
/// coordinate once wrote a catalog-wide policy from a single-table grant. Revoke
/// is monotonic, so the same guard would only strand access.
#[tokio::test]
async fn revoke_all_privileges_targets_the_named_table_not_the_catalog() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policy_with_analyst_accesses()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/revoke/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    backend(&server.uri())
        .revoke("token", &revoke_all_stmt())
        .await
        .expect("must succeed");

    let bodies = revoke_bodies(&server).await;
    let resource = &bodies[0]["resource"];
    assert_eq!(resource["table"], "orders", "resource: {resource}");
    assert_eq!(resource["namespace"], "sales", "resource: {resource}");
    assert_eq!(resource["catalog"], "wh", "resource: {resource}");
}

/// Idempotent: with nothing allowed at the coordinate there is no revoke to post,
/// and the statement must still succeed rather than error. An operator running it
/// twice, or against an already-clean object, is the normal case.
#[tokio::test]
async fn revoke_all_privileges_on_a_clean_object_is_a_no_op_that_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/service/plugins/services/revoke/{SERVICE}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    backend(&server.uri())
        .revoke("token", &revoke_all_stmt())
        .await
        .expect("a no-op revoke must not error");

    assert!(
        revoke_bodies(&server).await.is_empty(),
        "nothing was held, so nothing should have been posted"
    );
}

/// A revoke is authorized against its grantor, exactly like the per-privilege
/// path. `ALL PRIVILEGES` must not become a way around that check.
#[tokio::test]
async fn revoke_all_privileges_still_requires_a_grantor() {
    let server = MockServer::start().await;
    let mut stmt = revoke_all_stmt();
    stmt.grantor = None;

    let err = backend(&server.uri())
        .revoke("token", &stmt)
        .await
        .expect_err("a revoke with no grantor must be refused");
    assert!(
        err.to_string().to_lowercase().contains("grantor"),
        "error should name the grantor: {err}"
    );
    assert!(
        server.received_requests().await.unwrap_or_default().is_empty(),
        "refusal must happen before any request"
    );
}

// ── default `admin-role` mode: the authenticated policy API ──────────────────
//
// These pin the merge semantics the plugin endpoint used to perform server-side
// and that SQE now owns. Getting the union wrong is not a crash, it is a wrong
// privilege set in production, so each case asserts on the body actually sent.

fn policy_api_backend(url: &str) -> RangerGrantBackend {
    RangerGrantBackend::new(
        url,
        SERVICE,
        "admin",
        "admin-pw",
        "POLARIS",
        30,
        false,
        sqe_core::config::GrantAuthority::AdminRole,
    )
    .unwrap()
}

/// Bodies sent to a given method+path prefix, newest last.
async fn bodies_for(server: &MockServer, m: &str, path_contains: &str) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.as_str() == m && r.url.path().contains(path_contains))
        .filter_map(|r| serde_json::from_slice(&r.body).ok())
        .collect()
}

/// No policy holds the resource yet, so the grant CREATES one. It must be a
/// policyType 0 allow policy carrying exactly this grantee and access types.
#[tokio::test]
async fn policy_api_grant_creates_a_policy_when_the_resource_has_none() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
        .mount(&server)
        .await;

    policy_api_backend(&server.uri())
        .grant("token", &grant_stmt())
        .await
        .expect("grant should create the policy");

    let posted = bodies_for(&server, "POST", "/service/public/v2/api/policy").await;
    assert!(!posted.is_empty(), "a policy should have been created");
    let deepest = posted.last().expect("a body");
    assert_eq!(deepest["policyType"], 0, "an allow policy, not deny");
    assert_eq!(deepest["service"], SERVICE);
    let items = deepest["policyItems"].as_array().expect("policyItems");
    assert_eq!(items.len(), 1, "one grantee item");
    assert_eq!(items[0]["roles"], serde_json::json!(["analyst"]));
    assert!(
        !items[0]["accesses"].as_array().expect("accesses").is_empty(),
        "the item must confer something"
    );
    // Nothing may reach the unauthenticated plugin endpoint in this mode.
    assert!(
        bodies_for(&server, "POST", "/service/plugins/services/").await.is_empty(),
        "default mode must not use the plugin grant endpoint"
    );
}

/// A policy already grants this role one access type. The grant must UNION, not
/// replace: `GRANT SELECT` then `GRANT INSERT` on one resource has to leave both,
/// which is what the plugin endpoint did.
#[tokio::test]
async fn policy_api_grant_unions_into_an_existing_item() {
    let server = MockServer::start().await;
    let existing = serde_json::json!([{
        "id": 42,
        "name": "grant-1786526606036",
        "policyType": 0,
        "resources": {
            "root":      {"values": ["POLARIS"]},
            "catalog":   {"values": ["wh"]},
            "namespace": {"values": ["sales"]},
            "table":     {"values": ["orders"]}
        },
        "policyItems": [{
            "roles": ["analyst"],
            "accesses": [{"type": "table-properties-read", "isAllowed": true}]
        }],
        "policyLabels": ["chm:ROLE:analyst:INSERT"]
    }]);
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(existing))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 42})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    policy_api_backend(&server.uri())
        .grant("token", &grant_stmt())
        .await
        .expect("grant should update the existing policy");

    let put = bodies_for(&server, "PUT", "/service/public/v2/api/policy").await;
    let table_put = put
        .iter()
        .find(|b| b["resources"]["table"]["values"] == serde_json::json!(["orders"]))
        .expect("the table-level policy should have been updated");
    let items = table_put["policyItems"].as_array().expect("policyItems");
    let analyst = items
        .iter()
        .find(|i| i["roles"] == serde_json::json!(["analyst"]))
        .expect("the analyst item survives");
    let types: Vec<&str> = analyst["accesses"]
        .as_array()
        .expect("accesses")
        .iter()
        .filter_map(|a| a["type"].as_str())
        .collect();
    assert!(
        types.contains(&"table-properties-read"),
        "the pre-existing access type must survive the union, got {types:?}"
    );
    assert!(
        types.len() > 1,
        "the granted access types must be added alongside it, got {types:?}"
    );
    // Read-modify-write must not drop what else lives on the policy: the
    // provenance labels are what a later REVOKE narrows from.
    assert_eq!(
        table_put["policyLabels"],
        serde_json::json!(["chm:ROLE:analyst:INSERT"]),
        "policyLabels must be preserved through the update"
    );
}

/// Revoking with no policy for the resource is a no-op, not an error and not a
/// stray create.
#[tokio::test]
async fn policy_api_revoke_with_no_policy_is_a_noop() {
    use sqe_policy::grants::RevokeStatement;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;

    let stmt = RevokeStatement {
        privilege: "SELECT".to_string(),
        catalog: Some("wh".to_string()),
        namespace: Some("sales".to_string()),
        table: Some("orders".to_string()),
        grantee: Grantee::Role("analyst".to_string()),
        grantor: Some("carol".to_string()),
        object: Default::default(),
    };
    policy_api_backend(&server.uri())
        .revoke("token", &stmt)
        .await
        .expect("revoking what is not granted should succeed quietly");

    assert!(
        bodies_for(&server, "POST", "/service/public/v2/api/policy").await.is_empty(),
        "a revoke must never create a policy"
    );
}

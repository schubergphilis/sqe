//! HTTP-path tests for RangerStore against a mock Ranger Admin (wiremock).

use sqe_core::config::RangerPolicyConfig;
use sqe_core::SessionUser;
use sqe_policy::ranger_store::RangerStore;
use sqe_policy::PolicyStore;
use wiremock::matchers::{method, path, query_param};
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

/// Like `cfg`, but with a real (non-zero) cache TTL so the per-user `cache` and
/// the `bundle_cache` actually retain entries within the test. Use this for the
/// cache-hit / bundle-cache tests; the default `cfg` sets ttl=0, which disables
/// caching and would re-fetch on every call (request count 2, not 1).
fn cfg_with_ttl(url: &str, ttl_secs: u64) -> RangerPolicyConfig {
    RangerPolicyConfig {
        url: url.to_string(),
        service_name: "hive".to_string(),
        cache_ttl_secs: ttl_secs,
        ..RangerPolicyConfig::default()
    }
}

fn analyst() -> SessionUser {
    SessionUser {
        username: "alice".into(),
        roles: vec!["analyst".into()],
        subject: None,
        email: None,
        groups: vec![],
    }
}

fn create_policy(sql: &str) -> sqe_sql::policy_ddl::CreatePolicyStatement {
    let parsed = sqe_sql::policy_ddl::try_parse_policy_ddl(sql)
        .unwrap()
        .unwrap();
    let sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt) = parsed else {
        panic!("expected CREATE POLICY")
    };
    stmt
}

async fn mock_service_link(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/service/name/hive"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"type": "hive", "tagService": "hive_tag"})),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn create_policy_posts_component_policy() {
    let server = MockServer::start().await;
    mock_service_link(&server).await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", "hive"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let stmt = create_policy(
        "CREATE POLICY p ON TABLE c.sales.orders COLUMN MASK MASK_NULL \
         TO ROLE analyst ON COLUMN amount",
    );
    store.create_policy(&stmt).await.unwrap();

    let request = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.method == wiremock::http::Method::POST)
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["service"], "hive");
    assert_eq!(body["resources"]["database"]["values"][0], "sales");
}

#[tokio::test]
async fn create_or_replace_policy_puts_existing_id() {
    let server = MockServer::start().await;
    mock_service_link(&server).await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", "hive"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 41, "name": "p"}])),
        )
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/service/public/v2/api/policy/41"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let stmt = create_policy(
        "CREATE OR REPLACE POLICY p ON TABLE c.sales.orders ROW FILTER \
         TO ROLE analyst USING (region = 'EU')",
    );
    store.create_policy(&stmt).await.unwrap();
}

#[tokio::test]
async fn drop_policy_deletes_matching_component_policy() {
    let server = MockServer::start().await;
    mock_service_link(&server).await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", "hive"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 42, "name": "p"}])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", "hive_tag"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/service/public/v2/api/policy/42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    store
        .drop_policy(&sqe_sql::policy_ddl::DropPolicyStatement {
            name: "p".into(),
            if_exists: false,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn resolves_mask_and_filter_over_http() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
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
        .and(path("/service/plugins/secure/policies/download/hive"))
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
        .and(path("/service/plugins/secure/policies/download/hive"))
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
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let bob = SessionUser {
        username: "bob".into(),
        roles: vec!["engineer".into()],
        subject: None,
        email: None,
        groups: vec![],
    };
    let policy = store.resolve(&bob, "orders", "sales").await.unwrap();
    assert!(policy.column_masks.is_empty());
    assert!(policy.row_filters.is_empty());
}

// ── Fail-closed / I/O glue (WA review: MED-untested-io-and-failclosed-branches) ──

/// Breaker wiring: a non-200 trips `record_failure`; once the failure threshold
/// is reached the breaker opens and the NEXT `resolve` denies (returns Err) via
/// `breaker.check()` WITHOUT issuing another HTTP request. The discriminator is
/// the request count, not the error: both calls return Err (the `?` in
/// `cached_bundle` propagates; the `lit(false)` conversion happens in the
/// rewriter, not here). With `breaker_failure_threshold = 1` the first 500 opens
/// the breaker; `breaker_recovery_secs` is kept high so `check()` does not flip
/// to half-open and admit a trial request mid-test. Errors are never cached, so
/// the second `resolve` genuinely re-enters `fetch_bundle` and is stopped at the
/// breaker before any HTTP call. Pins MED item 1.
#[tokio::test]
async fn breaker_open_denies_without_second_http_call() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let cfg = RangerPolicyConfig {
        breaker_failure_threshold: 1,
        // High recovery window: the breaker must stay OPEN for the whole test,
        // never flipping to half-open (which would admit a trial request).
        breaker_recovery_secs: 3600,
        ..cfg(&server.uri())
    };
    let store = RangerStore::from_config(&cfg).unwrap();

    // First resolve: one HTTP call returns 500 -> record_failure -> breaker opens.
    let first = store.resolve(&analyst(), "orders", "sales").await;
    assert!(first.is_err(), "first 5xx must fail closed (Err)");

    // Second resolve: breaker is open, so fetch_bundle returns Err at
    // breaker.check() before any HTTP call.
    let second = store.resolve(&analyst(), "orders", "sales").await;
    assert!(second.is_err(), "breaker-open resolve must deny (Err)");

    let count = server.received_requests().await.unwrap().len();
    assert_eq!(
        count, 1,
        "breaker-open path must NOT issue a second HTTP request (got {count} requests)"
    );
}

/// `resolve_tags` must fail closed on a bundle fetch error: a single
/// `resolve_tags` call against a 500 returns a single `lit(false)` deny row
/// filter (and no masks). This is the tag-path analogue of `resolve`'s Err
/// behaviour and is security-critical: a degraded Ranger must deny all rows on
/// a tagged table, not return them raw. The tag set MUST be non-empty, or
/// `resolve_tags` early-returns before reaching the bundle fetch. The breaker is
/// irrelevant here (a single 500 returns the deny directly from the
/// `cached_bundle` Err arm). Pins MED item 2.
#[tokio::test]
async fn resolve_tags_fails_closed_on_fetch_error() {
    use datafusion::logical_expr::{lit, Expr};
    use datafusion::scalar::ScalarValue;
    use std::collections::HashSet;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let (masks, filters, unmappable) = store.resolve_tags(&analyst(), &tags).await;

    assert!(masks.is_empty(), "fetch failure must yield no masks");
    assert!(
        unmappable.is_empty(),
        "fetch failure must yield no unmappable tags"
    );
    assert_eq!(
        filters.len(),
        1,
        "fetch failure must inject exactly one deny filter"
    );
    assert!(
        matches!(
            &filters[0],
            Expr::Literal(ScalarValue::Boolean(Some(false)), _)
        ),
        "the deny filter must be lit(false), got: {:?}",
        filters[0]
    );
    let _ = lit(false); // documents the expected value
}

/// Resolve caching: two `resolve` calls for the same (user, table) within the
/// TTL must hit the Ranger download endpoint exactly ONCE. The first call misses
/// both caches and downloads; the second is served from cache (the per-user
/// `cache` keyed by user+namespace+table+roles, backstopped by the bundle
/// cache), so no second request is issued. Requires a non-zero TTL (the shared
/// `cfg` helper sets ttl=0, which disables caching). Pins MED item 3 (the
/// two-resolves-one-download contract). NOTE: this asserts request count, which
/// the bundle cache alone could also satisfy; the per-user `cache` hit path is
/// isolated specifically by the in-module `cache_hit_counter_increments_on_ranger`
/// test (seeds the per-user cache, asserts the hit counter).
#[tokio::test]
async fn resolve_caches_per_user_within_ttl() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg_with_ttl(&server.uri(), 300)).unwrap();

    let first = store.resolve(&analyst(), "orders", "sales").await.unwrap();
    let second = store.resolve(&analyst(), "orders", "sales").await.unwrap();

    // Behaviour equivalence: both resolutions are identical.
    assert_eq!(first.column_masks.len(), second.column_masks.len());
    assert_eq!(first.row_filters.len(), second.row_filters.len());

    let count = server.received_requests().await.unwrap().len();
    assert_eq!(
        count, 1,
        "two resolves for the same user within TTL must download once (got {count})"
    );
}

/// Bundle cache: two `resolve_tags` calls within the TTL must download the
/// (user-independent) ServicePolicies bundle exactly ONCE. The bundle content is
/// irrelevant to the count, so any valid ServicePolicies JSON works; the tag set
/// MUST be non-empty so both calls reach `cached_bundle`. Pins MED item 4 and
/// HIGH-resolve-tags-no-bundle-cache (batch 1).
#[tokio::test]
async fn resolve_tags_caches_bundle_within_ttl() {
    use std::collections::HashSet;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg_with_ttl(&server.uri(), 300)).unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();

    let _ = store.resolve_tags(&analyst(), &tags).await;
    let _ = store.resolve_tags(&analyst(), &tags).await;

    let count = server.received_requests().await.unwrap().len();
    assert_eq!(
        count, 1,
        "two resolve_tags calls within TTL must download the bundle once (got {count})"
    );
}

/// `[policy.ranger] catalog_scoped` through the real store and HTTP path: two
/// catalogs hold `finance.orders`, the portal's "everyone except SG-Exempt"
/// mask names `ws_a.finance`, and it applies to `ws_a` only. With the setting
/// off, the same lookups key on `finance` and a catalog-less policy covers both.
#[tokio::test]
async fn catalog_scoped_mask_applies_to_the_named_catalog_only() {
    let bundle = serde_json::json!({
        "policyVersion": 1,
        "policies": [
            {"id": 1, "name": "ws_a-ssn", "policyType": 1, "isEnabled": true,
             "resources": {"database": {"values": ["ws_a.finance"]}, "table": {"values": ["orders"]},
                           "column": {"values": ["ssn"]}},
             "dataMaskPolicyItems": [
                {"groups": ["SG-Exempt"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}},
                {"groups": ["public"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]},
            {"id": 2, "name": "legacy", "policyType": 1, "isEnabled": true,
             "resources": {"database": {"values": ["finance"]}, "table": {"values": ["orders"]},
                           "column": {"values": ["iban"]}},
             "dataMaskPolicyItems": [{"groups": ["public"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]}
        ]
    });
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bundle))
        .mount(&server)
        .await;
    let user = |groups: &[&str]| SessionUser {
        username: "ann".into(),
        roles: vec![],
        subject: None,
        email: None,
        groups: groups.iter().map(|g| g.to_string()).collect(),
    };

    let scoped = RangerStore::from_config(&RangerPolicyConfig {
        catalog_scoped: true,
        ..cfg(&server.uri())
    })
    .unwrap();
    let a = scoped
        .resolve(&user(&["SG-DataAnalysts"]), "orders", "ws_a.finance")
        .await
        .unwrap();
    assert!(a.column_masks.contains_key("ssn"), "ws_a is masked");
    assert!(
        !a.column_masks.contains_key("iban"),
        "a catalog-less policy does not match a scoped key"
    );
    let b = scoped
        .resolve(&user(&["SG-DataAnalysts"]), "orders", "ws_b.finance")
        .await
        .unwrap();
    assert!(
        b.column_masks.is_empty(),
        "ws_b's finance.orders is not covered"
    );
    let exempt = scoped
        .resolve(&user(&["SG-Exempt"]), "orders", "ws_a.finance")
        .await
        .unwrap();
    assert!(
        exempt.column_masks.is_empty(),
        "the exemption item comes first"
    );

    let unscoped = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let flat = unscoped
        .resolve(&user(&[]), "orders", "finance")
        .await
        .unwrap();
    assert!(
        flat.column_masks.contains_key("iban"),
        "off: `finance` covers every catalog's finance.orders"
    );
    assert!(!flat.column_masks.contains_key("ssn"));
}

/// Through SQL and the rewriter: `ws_a` and `ws_b` both hold
/// `finance.orders`. With `catalog_scoped` a mask on `ws_a.finance` nulls
/// `ws_a`'s column only; with it off, a mask on `finance` (main's key) nulls
/// both catalogs, as before.
#[tokio::test]
async fn catalog_scoped_mask_through_the_rewriter_covers_one_catalog() {
    use std::sync::Arc;

    use arrow::array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use sqe_policy::plan_rewriter::PolicyPlanRewriter;
    use sqe_policy::{PolicyEnforcer, TableScope};

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("ssn", DataType::Utf8, true)]));
    for catalog in ["ws_a", "ws_b"] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["123-45-6789"]))],
        )
        .unwrap();
        let finance = Arc::new(MemorySchemaProvider::new());
        finance
            .register_table(
                "orders".into(),
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        datafusion::catalog::CatalogProvider::register_schema(cat.as_ref(), "finance", finance)
            .unwrap();
        ctx.register_catalog(catalog, cat);
    }

    let mask = |database: &str| {
        serde_json::json!({
            "policyVersion": 1,
            "policies": [
                {"id": 1, "name": "ssn", "policyType": 1, "isEnabled": true,
                 "resources": {"database": {"values": [database]}, "table": {"values": ["orders"]},
                               "column": {"values": ["ssn"]}},
                 "dataMaskPolicyItems": [
                    {"groups": ["SG-Exempt"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}},
                    {"groups": ["public"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]}
            ]
        })
    };
    let user = |groups: &[&str]| SessionUser {
        username: "ann".into(),
        roles: vec![],
        subject: None,
        email: None,
        groups: groups.iter().map(|g| g.to_string()).collect(),
    };
    /// `true` when the caller reads the raw value from `catalog`.
    async fn raw(
        ctx: &SessionContext,
        rewriter: &PolicyPlanRewriter,
        who: SessionUser,
        catalog: &str,
    ) -> bool {
        let sql = format!("SELECT ssn FROM {catalog}.finance.orders");
        let plan = ctx.sql(&sql).await.unwrap().logical_plan().clone();
        let (enforced, _) = rewriter
            .evaluate(&who, &TableScope::of(ctx), plan)
            .await
            .unwrap();
        let batches = ctx
            .execute_logical_plan(enforced)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        !batches[0].column(0).is_null(0)
    }

    for (scoped, database, masked_in) in [
        (true, "ws_a.finance", &["ws_a"][..]),
        (false, "finance", &["ws_a", "ws_b"][..]),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/service/plugins/secure/policies/download/hive"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mask(database)))
            .mount(&server)
            .await;
        let store = RangerStore::from_config(&RangerPolicyConfig {
            catalog_scoped: scoped,
            ..cfg(&server.uri())
        })
        .unwrap();
        let rewriter = PolicyPlanRewriter::new(Arc::new(store));
        for catalog in ["ws_a", "ws_b"] {
            let masked = masked_in.contains(&catalog);
            assert_eq!(
                raw(&ctx, &rewriter, user(&["SG-DataAnalysts"]), catalog).await,
                !masked,
                "catalog_scoped={scoped}, policy on `{database}`, reading {catalog}"
            );
            assert!(
                raw(&ctx, &rewriter, user(&["SG-Exempt"]), catalog).await,
                "MASK_NONE leaves the column raw for the exempt group ({catalog})"
            );
        }
    }
}

// ── GRANT ... MASKED WITH / ROWS WHERE ────────────────────────────────

/// The spec the coordinator builds for `GRANT SELECT ON <table> TO ROLE
/// <grantee> <clauses>`; the clauses go through the real parser.
fn grant_spec(table: &str, grantee: &str, clauses: &str) -> sqe_sql::grant_policy::GrantPolicySpec {
    let g = sqe_sql::grant_policy::try_parse_grant_policy(&format!(
        "GRANT SELECT ON {table} TO ROLE {grantee} {clauses}"
    ))
    .unwrap()
    .unwrap();
    sqe_sql::grant_policy::GrantPolicySpec {
        table: table.to_string(),
        principal: role(grantee),
        masks: g.masks,
        row_filter: g.row_filter,
    }
}

fn role(name: &str) -> sqe_sql::policy_ddl::PolicyPrincipal {
    sqe_sql::policy_ddl::PolicyPrincipal {
        kind: sqe_sql::policy_ddl::PolicyPrincipalKind::Role,
        name: name.into(),
    }
}

async fn mock_policy_list(server: &MockServer, policies: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/service/public/v2/api/policy"))
        .and(query_param("serviceName", "hive"))
        .respond_with(ResponseTemplate::new(200).set_body_json(policies))
        .mount(server)
        .await;
}

async fn bodies(
    server: &MockServer,
    m: wiremock::http::Method,
) -> Vec<(String, serde_json::Value)> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method == m)
        .map(|r| {
            let body = serde_json::from_slice(&r.body).unwrap_or(serde_json::Value::Null);
            (r.url.path().to_string(), body)
        })
        .collect()
}

/// With no policy on the column or table, GRANT creates one of each, and the
/// policies it writes are ones the read path resolves for the grantee.
#[tokio::test]
async fn grant_policies_create_resolvable_policies_when_absent() {
    let server = MockServer::start().await;
    mock_policy_list(&server, serde_json::json!([])).await;
    Mock::given(method("POST"))
        .and(path("/service/public/v2/api/policy"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    store
        .set_grant_policies(&grant_spec(
            "c.sales.orders",
            "analyst",
            "MASKED WITH (amount = round(amount, -2)) ROWS WHERE region = 'EU'",
        ))
        .await
        .unwrap();

    let posted: Vec<serde_json::Value> = bodies(&server, wiremock::http::Method::POST)
        .await
        .into_iter()
        .map(|(_, b)| b)
        .collect();
    let mask = posted.iter().find(|b| b["policyType"] == 1).unwrap();
    assert_eq!(mask["name"], "sqe-grant-mask-sales.orders.amount");
    assert_eq!(mask["resources"]["database"]["values"][0], "sales");
    assert_eq!(mask["resources"]["column"]["values"][0], "amount");
    assert_eq!(mask["dataMaskPolicyItems"][0]["roles"][0], "analyst");
    assert_eq!(
        mask["dataMaskPolicyItems"][0]["dataMaskInfo"]["dataMaskType"],
        "CUSTOM"
    );
    assert_eq!(
        mask["dataMaskPolicyItems"][0]["dataMaskInfo"]["valueExpr"],
        "round(amount, -2)"
    );
    let filter = posted.iter().find(|b| b["policyType"] == 2).unwrap();
    assert_eq!(filter["name"], "sqe-grant-filter-sales.orders");
    assert!(filter["resources"].get("column").is_none());
    assert_eq!(
        filter["rowFilterPolicyItems"][0]["rowFilterInfo"]["filterExpr"],
        "region = 'EU'"
    );

    // Serve what was written back as the bundle: the grantee must resolve it.
    let read = MockServer::start().await;
    let mut written = posted.clone();
    for (i, p) in written.iter_mut().enumerate() {
        p["id"] = serde_json::json!(i + 1);
    }
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"policyVersion": 1, "policies": written})),
        )
        .mount(&read)
        .await;
    let reader = RangerStore::from_config(&cfg(&read.uri())).unwrap();
    let policy = reader.resolve(&analyst(), "orders", "sales").await.unwrap();
    assert!(policy.column_masks.contains_key("amount"), "{policy:?}");
    assert_eq!(policy.row_filters.len(), 1);
    assert!(policy.restricted_columns.is_empty(), "{policy:?}");
}

/// An existing policy on the column is edited in place: the grantee is split
/// out of the item it shared, its new item goes first, and every field an
/// admin set on the policy survives. No second policy (Ranger error 3010).
#[tokio::test]
async fn grant_policies_prepend_item_in_existing_policy() {
    let server = MockServer::start().await;
    mock_policy_list(
        &server,
        serde_json::json!([{
            "id": 7, "name": "hand-written", "service": "hive", "policyType": 1,
            "description": "set by hand", "policyPriority": 0, "isEnabled": true,
            "resources": {"database": {"values": ["SALES"]}, "table": {"values": ["orders"]},
                          "column": {"values": ["amount"]}},
            "dataMaskPolicyItems": [
                {"users": [], "roles": ["analyst", "support"], "groups": [],
                 "accesses": [{"type": "select", "isAllowed": true}],
                 "dataMaskInfo": {"dataMaskType": "MASK_NULL"}},
                {"users": [], "roles": [], "groups": ["public"],
                 "accesses": [{"type": "select", "isAllowed": true}],
                 "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}
            ]
        }]),
    )
    .await;
    Mock::given(method("PUT"))
        .and(path("/service/public/v2/api/policy/7"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    store
        .set_grant_policies(&grant_spec(
            "sales.orders",
            "support",
            "MASKED WITH (amount = 0)",
        ))
        .await
        .unwrap();

    let (_, put) = bodies(&server, wiremock::http::Method::PUT).await.remove(0);
    assert_eq!(put["description"], "set by hand");
    assert_eq!(put["name"], "hand-written");
    let items = put["dataMaskPolicyItems"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["roles"], serde_json::json!(["support"]));
    assert_eq!(items[0]["dataMaskInfo"]["valueExpr"], "0");
    assert_eq!(items[1]["roles"], serde_json::json!(["analyst"]));
    assert_eq!(items[2]["groups"], serde_json::json!(["public"]));
}

/// A policy on a wildcard or a column list is a different resource
/// signature: GRANT does not edit it and writes its own policy instead.
#[tokio::test]
async fn grant_policies_leave_wildcard_policies_alone() {
    let server = MockServer::start().await;
    mock_policy_list(
        &server,
        serde_json::json!([{
            "id": 9, "name": "sqe-grant-mask-sales.orders.amount", "policyType": 1,
            "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]},
                          "column": {"values": ["*"]}},
            "dataMaskPolicyItems": []
        }]),
    )
    .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    store
        .set_grant_policies(&grant_spec(
            "sales.orders",
            "support",
            "MASKED WITH (amount = 0)",
        ))
        .await
        .unwrap();
    let (_, post) = bodies(&server, wiremock::http::Method::POST)
        .await
        .remove(0);
    // The name the wildcard policy holds is taken, so the new one is suffixed.
    assert_eq!(post["name"], "sqe-grant-mask-sales.orders.amount-2");
}

/// Scoped mode needs the catalog, as CREATE POLICY does: a policy written
/// without one would never match.
#[tokio::test]
async fn grant_policies_need_catalog_when_scoped() {
    let server = MockServer::start().await;
    let store = RangerStore::from_config(&RangerPolicyConfig {
        catalog_scoped: true,
        ..cfg(&server.uri())
    })
    .unwrap();
    let err = store
        .set_grant_policies(&grant_spec("sales.orders", "support", "ROWS WHERE a = 1"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("catalog_scoped"), "{err}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

fn drop_for(
    table: &str,
    grantee: &str,
    if_exists: bool,
) -> sqe_sql::policy_ddl::DropPrincipalPolicyStatement {
    sqe_sql::policy_ddl::DropPrincipalPolicyStatement {
        table: table.to_string(),
        principal: role(grantee),
        if_exists,
    }
}

/// `DROP POLICY ON TABLE ... FROM ROLE r` takes r out of every mask and
/// filter policy on exactly that table. An SQE-written policy left empty is
/// deleted; a hand-written one is kept, empty; other tables and access
/// policies are not touched.
#[tokio::test]
async fn drop_principal_policies_removes_items() {
    let server = MockServer::start().await;
    mock_policy_list(
        &server,
        serde_json::json!([
            {"id": 7, "name": "hand-written", "policyType": 1,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]},
                           "column": {"values": ["amount"]}},
             "dataMaskPolicyItems": [{"roles": ["support"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]},
            {"id": 8, "name": "sqe-grant-filter-sales.orders", "policyType": 2,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}},
             "rowFilterPolicyItems": [{"roles": ["support", "analyst"], "rowFilterInfo": {"filterExpr": "1 = 0"}}]},
            {"id": 9, "name": "sqe-grant-mask-sales.orders.note", "policyType": 1,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]},
                           "column": {"values": ["note"]}},
             "dataMaskPolicyItems": [{"roles": ["support"], "dataMaskInfo": {"dataMaskType": "CUSTOM", "valueExpr": "'x'"}}]},
            {"id": 10, "name": "sqe-grant-filter-sales.returns", "policyType": 2,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["returns"]}},
             "rowFilterPolicyItems": [{"roles": ["support"], "rowFilterInfo": {"filterExpr": "1 = 0"}}]},
            {"id": 11, "name": "access", "policyType": 0,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}},
             "policyItems": [{"roles": ["support"]}]}
        ]),
    )
    .await;
    for id in [7, 8] {
        Mock::given(method("PUT"))
            .and(path(format!("/service/public/v2/api/policy/{id}")))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("DELETE"))
        .and(path("/service/public/v2/api/policy/9"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let changed = store
        .drop_principal_policies(&drop_for("c.sales.orders", "support", false))
        .await
        .unwrap();
    assert_eq!(changed, 3);
    let puts: std::collections::HashMap<String, serde_json::Value> =
        bodies(&server, wiremock::http::Method::PUT)
            .await
            .into_iter()
            .collect();
    assert_eq!(
        puts["/service/public/v2/api/policy/7"]["dataMaskPolicyItems"],
        serde_json::json!([])
    );
    assert_eq!(
        puts["/service/public/v2/api/policy/8"]["rowFilterPolicyItems"][0]["roles"],
        serde_json::json!(["analyst"])
    );
    assert_eq!(
        bodies(&server, wiremock::http::Method::DELETE).await.len(),
        1
    );
}

/// Nothing to drop is an error, unless IF EXISTS, and writes nothing.
#[tokio::test]
async fn drop_principal_policies_with_nothing_to_drop() {
    let server = MockServer::start().await;
    mock_policy_list(
        &server,
        serde_json::json!([
            {"id": 8, "name": "sqe-grant-filter-sales.orders", "policyType": 2,
             "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}},
             "rowFilterPolicyItems": [{"roles": ["analyst"], "rowFilterInfo": {"filterExpr": "1 = 0"}}]}
        ]),
    )
    .await;
    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let err = store
        .drop_principal_policies(&drop_for("sales.orders", "support", false))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("no column mask or row filter"),
        "{err}"
    );
    let changed = store
        .drop_principal_policies(&drop_for("sales.orders", "support", true))
        .await
        .unwrap();
    assert_eq!(changed, 0);
    assert!(server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .all(|r| r.method == wiremock::http::Method::GET));
}

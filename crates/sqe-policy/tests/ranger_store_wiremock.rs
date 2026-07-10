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
        .and(path("/service/plugins/policies/download/hive"))
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
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri())).unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let (masks, filters, unmappable) = store.resolve_tags(&analyst(), &tags).await;

    assert!(masks.is_empty(), "fetch failure must yield no masks");
    assert!(unmappable.is_empty(), "fetch failure must yield no unmappable tags");
    assert_eq!(filters.len(), 1, "fetch failure must inject exactly one deny filter");
    assert!(
        matches!(&filters[0], Expr::Literal(ScalarValue::Boolean(Some(false)), _)),
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
        .and(path("/service/plugins/policies/download/hive"))
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
        .and(path("/service/plugins/policies/download/hive"))
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

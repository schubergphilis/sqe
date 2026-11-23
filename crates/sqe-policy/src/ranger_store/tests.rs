use super::*;
use std::sync::atomic::Ordering;

fn create(sql: &str) -> sqe_sql::policy_ddl::CreatePolicyStatement {
    let parsed = sqe_sql::policy_ddl::try_parse_policy_ddl(sql)
        .expect("valid policy SQL")
        .expect("policy statement");
    let sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt) = parsed else {
        panic!("expected create")
    };
    stmt
}

#[test]
fn sql_resource_mask_maps_to_ranger_policy_json() {
    let stmt = create(
        "CREATE POLICY p ON TABLE sales_wh.ac.orders COLUMN MASK MASK_NULL \
         TO ROLE engineer ON COLUMN amount",
    );
    let body = ranger_policy_body(&stmt, "query", "hive", false).unwrap();
    assert_eq!(body["service"], "query");
    assert_eq!(body["policyType"], 1);
    assert_eq!(body["resources"]["database"]["values"][0], "ac");
    assert_eq!(body["resources"]["table"]["values"][0], "orders");
    assert_eq!(body["resources"]["column"]["values"][0], "amount");
    assert_eq!(body["dataMaskPolicyItems"][0]["roles"][0], "engineer");
    assert_eq!(
        body["dataMaskPolicyItems"][0]["dataMaskInfo"]["dataMaskType"],
        "MASK_NULL"
    );
}

#[test]
fn sql_tag_mask_uses_component_qualified_ranger_vocabulary() {
    let stmt = create(
        "CREATE POLICY p ON TAG pii COLUMN MASK CUSTOM TO ROLE engineer \
         USING (concat('x', {col}))",
    );
    let body = ranger_policy_body(&stmt, "query_tag", "hive", false).unwrap();
    assert_eq!(body["service"], "query_tag");
    assert_eq!(body["resources"]["tag"]["values"][0], "pii");
    assert_eq!(
        body["dataMaskPolicyItems"][0]["accesses"][0]["type"],
        "hive:select"
    );
    assert_eq!(
        body["dataMaskPolicyItems"][0]["dataMaskInfo"]["dataMaskType"],
        "hive:CUSTOM"
    );
}

/// Tag-service mask types carry the owning component's prefix. Verified
/// against a live Ranger 2.8 tag servicedef, which lists ONLY prefixed
/// forms (`hive:MASK_SHOW_LAST_4`, `trino:MASK_NULL`, ...). Before this
/// normalization every tag-based mask fell through to the unsupported arm
/// and the tagged column was restricted instead of masked.
#[test]
fn mask_type_component_prefix_is_normalized() {
    assert_eq!(normalize_mask_type("MASK_SHOW_LAST_4"), "MASK_SHOW_LAST_4");
    assert_eq!(
        normalize_mask_type("hive:MASK_SHOW_LAST_4"),
        "MASK_SHOW_LAST_4"
    );
    assert_eq!(normalize_mask_type("hive:CUSTOM"), "CUSTOM");
    // Another engine's mask stays unmatched, so the caller fails closed
    // rather than applying a policy authored for Trino.
    assert_eq!(normalize_mask_type("trino:MASK_NULL"), "trino:MASK_NULL");
}

const BUNDLE: &str = r#"{
  "policyVersion": 7,
  "policies": [
    {
      "id": 1, "policyType": 1, "isEnabled": true,
      "resources": {
        "database": {"values": ["sales"]},
        "table": {"values": ["orders"]},
        "column": {"values": ["amount"]}
      },
      "dataMaskPolicyItems": [
        {"users": [], "roles": ["analyst"],
         "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}
      ]
    },
    {
      "id": 2, "policyType": 2, "isEnabled": true,
      "resources": {
        "database": {"values": ["sales"]},
        "table": {"values": ["orders"]}
      },
      "rowFilterPolicyItems": [
        {"users": [], "roles": ["analyst"],
         "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
      ]
    }
  ]
}"#;

#[test]
fn parses_bundle() {
    let sp: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    assert_eq!(sp.policy_version, Some(7));
    assert_eq!(sp.policies.len(), 2);
    assert_eq!(sp.policies[0].policy_type, 1);
    assert!(sp.policies[0].is_enabled);
    assert_eq!(
        sp.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type,
        "MASK_NULL"
    );
    assert_eq!(
        sp.policies[1].row_filter_policy_items[0]
            .row_filter_info
            .filter_expr
            .as_deref(),
        Some("region = 'EU'")
    );
}

#[test]
fn empty_bundle_is_default() {
    let sp: ServicePolicies = serde_json::from_str("{}").unwrap();
    assert!(sp.policies.is_empty());
    assert_eq!(sp.policy_version, None);
}

#[test]
fn from_config_builds_store() {
    let cfg = RangerPolicyConfig::default();
    // from_config must succeed even with an empty URL (no network call).
    let store = RangerStore::from_config(&cfg);
    assert!(store.is_ok(), "from_config failed: {:?}", store.err());
}

/// Fix 1: a fetch failure trips the breaker and the breaker-state gauge must
/// read `open` (2). With no metrics wired the OPA-style record calls are
/// no-ops; here we attach a registry and assert the gauge after a forced
/// failure. `breaker_failure_threshold = 1` so a single failed download
/// opens the breaker. The download URL is an unroutable address so the HTTP
/// send fails fast (records a failure + records the "err" metric).
#[tokio::test]
async fn breaker_open_sets_state_gauge_for_ranger() {
    let cfg = RangerPolicyConfig {
        // RFC 5737 TEST-NET-1, unroutable; with a short timeout the send fails fast.
        url: "http://192.0.2.1:6080".to_string(),
        timeout_secs: 1,
        breaker_failure_threshold: 1,
        ..RangerPolicyConfig::default()
    };
    let metrics = Arc::new(MetricsRegistry::new().unwrap());
    let store = RangerStore::from_config(&cfg)
        .unwrap()
        .with_metrics(metrics.clone());

    // First fetch fails (transport error) -> record_failure opens the
    // breaker (threshold 1) and record_metric writes the gauge.
    let err = store.fetch_bundle().await;
    assert!(err.is_err(), "fetch against unroutable URL must fail");

    let gauge = metrics
        .policy_circuit_breaker_state
        .with_label_values(&["ranger"])
        .get();
    assert_eq!(
        gauge, 2.0,
        "breaker must be open (gauge=2) after a fetch failure tripped it"
    );

    // The duration histogram must have at least one observation labelled err.
    let observed = metrics
        .policy_resolve_duration_seconds
        .with_label_values(&["ranger", "err"])
        .get_sample_count();
    assert!(
        observed >= 1,
        "fetch failure must record a resolve-duration sample"
    );
}

#[test]
fn policy_cache_distinguishes_groups_and_list_boundaries() {
    let mut alice = user("alice", &["analyst"]);
    let original = cache_key(&alice, "orders", "sales");
    alice.groups = vec!["restricted".into()];
    assert_ne!(original, cache_key(&alice, "orders", "sales"));
    alice.groups = vec!["a,b".into()];
    let combined = cache_key(&alice, "orders", "sales");
    alice.groups = vec!["a".into(), "b".into()];
    assert_ne!(combined, cache_key(&alice, "orders", "sales"));
    let ordered = cache_key(&alice, "orders", "sales");
    alice.groups.reverse();
    assert_eq!(ordered, cache_key(&alice, "orders", "sales"));
}

/// Fix 1: per-user cache hit/miss counters increment via `resolve`. We seed
/// the per-user cache directly so the hit path is taken without any network
/// call (the miss path would hit `cached_bundle`/`fetch_bundle`).
#[tokio::test]
async fn cache_hit_counter_increments_on_ranger() {
    let metrics = Arc::new(MetricsRegistry::new().unwrap());
    let store = RangerStore::from_config(&RangerPolicyConfig::default())
        .unwrap()
        .with_metrics(metrics.clone());

    let u = user("alice", &["analyst"]);
    let key = cache_key(&u, "orders", "sales");
    store
        .cache
        .insert(key, CachedPolicy(Arc::new(ResolvedPolicy::default())))
        .await;

    let _ = store.resolve(&u, "orders", "sales").await.unwrap();
    let hits = metrics
        .policy_cache_hits_total
        .with_label_values(&["ranger"])
        .get();
    assert_eq!(hits, 1, "a warm per-user cache entry must record one hit");
}

/// `cached_bundle` serves a warm bundle from the cache without re-fetching.
/// We seed the bundle cache directly (no HTTP), then assert the returned Arc
/// is pointer-identical to the seeded one -- proving the cache hit path is
/// taken and `fetch_bundle` (which would fail on the empty URL) is skipped.
#[tokio::test]
async fn cached_bundle_serves_warm_copy_without_fetch() {
    let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
    let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
    store
        .bundle_cache
        .insert(
            BUNDLE_KEY,
            CachedBundle {
                bundle: seeded.clone(),
                fetched_at: Instant::now(),
            },
        )
        .await;

    let got = store
        .cached_bundle()
        .await
        .expect("warm bundle must be served from cache");
    assert!(
        Arc::ptr_eq(&seeded, &got),
        "cached_bundle must return the cached Arc, not a re-fetched copy"
    );
}

/// A store whose bundle download is served by `server`, with a 100 s TTL.
async fn store_against(server: &wiremock::MockServer) -> Arc<RangerStore> {
    Arc::new(
        RangerStore::from_config(&RangerPolicyConfig {
            url: server.uri(),
            cache_ttl_secs: 100,
            ..RangerPolicyConfig::default()
        })
        .unwrap(),
    )
}

async fn mount_bundle(server: &wiremock::MockServer, status: u16, delay: Duration) {
    use wiremock::matchers::{method, path_regex};
    wiremock::Mock::given(method("GET"))
        .and(path_regex("/policies/download/"))
        .respond_with(
            wiremock::ResponseTemplate::new(status)
                .set_body_string(BUNDLE)
                .insert_header("content-type", "application/json")
                .set_delay(delay),
        )
        .mount(server)
        .await;
}

async fn downloads(server: &wiremock::MockServer) -> usize {
    server.received_requests().await.unwrap_or_default().len()
}

async fn concurrent_bundles(store: &Arc<RangerStore>, n: usize) -> Vec<Arc<ServicePolicies>> {
    let tasks: Vec<_> = (0..n)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.cached_bundle().await.expect("bundle") })
        })
        .collect();
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.unwrap());
    }
    out
}

/// #816: 50 queries arriving with no bundle cached download it once, not
/// 50 times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_misses_download_the_bundle_once() {
    let server = wiremock::MockServer::start().await;
    mount_bundle(&server, 200, Duration::from_millis(100)).await;
    let store = store_against(&server).await;

    concurrent_bundles(&store, 50).await;
    assert_eq!(downloads(&server).await, 1);
}

/// #816: past 80% of the TTL one reader refreshes, every other reader is
/// served the cached bundle without waiting, and the refresh resets the age.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ageing_bundle_is_refreshed_once_ahead_of_expiry() {
    let server = wiremock::MockServer::start().await;
    mount_bundle(&server, 200, Duration::from_millis(100)).await;
    let store = store_against(&server).await;
    let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
    let aged = Instant::now() - Duration::from_secs(90);
    store
        .bundle_cache
        .insert(
            BUNDLE_KEY,
            CachedBundle {
                bundle: seeded.clone(),
                fetched_at: aged,
            },
        )
        .await;

    let got = concurrent_bundles(&store, 50).await;
    assert_eq!(downloads(&server).await, 1);
    assert!(
        got.iter().filter(|b| Arc::ptr_eq(b, &seeded)).count() >= 49,
        "readers that did not refresh are served the cached bundle"
    );
    let cached = store
        .bundle_cache
        .get(BUNDLE_KEY)
        .await
        .expect("refreshed bundle");
    assert!(cached.fetched_at > aged, "the refresh restarted the TTL");

    // Fresh again: no further download.
    concurrent_bundles(&store, 10).await;
    assert_eq!(downloads(&server).await, 1);
}

/// #816: a refresh-ahead that fails keeps serving the cached bundle; it is
/// still within its TTL, so this is not a fail-open.
#[tokio::test]
async fn a_failed_refresh_ahead_serves_the_cached_bundle() {
    let server = wiremock::MockServer::start().await;
    mount_bundle(&server, 503, Duration::ZERO).await;
    let store = store_against(&server).await;
    let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
    store
        .bundle_cache
        .insert(
            BUNDLE_KEY,
            CachedBundle {
                bundle: seeded.clone(),
                fetched_at: Instant::now() - Duration::from_secs(90),
            },
        )
        .await;

    let got = store.cached_bundle().await.expect("cached bundle served");
    assert!(Arc::ptr_eq(&got, &seeded));
    assert_eq!(downloads(&server).await, 1);
}

/// The body shape `GET /service/tags/tagdefs` answers on Ranger 2.x: a bare
/// array of `RangerTagDef` (captured from the compose Ranger).
const TAGDEFS: &str = r#"[
    {"id":1,"guid":"2170fc84-f37a-4a7c-8b64-16265e27c596","isEnabled":true,"createdBy":"Admin","updatedBy":"Admin","createTime":1795207840856,"updateTime":1795207840856,"version":1,"name":"PII","source":"sqe"},
    {"id":2,"guid":"135987b3-e881-455a-b3c9-20f82ec34977","isEnabled":true,"version":1,"name":"live.pii","source":"portal","attributeDefs":[]}
]"#;

async fn mount_tagdefs(server: &wiremock::MockServer, status: u16, body: &str) {
    use wiremock::matchers::{header_exists, method, path};
    wiremock::Mock::given(method("GET"))
        .and(path("/service/tags/tagdefs"))
        .and(header_exists("authorization"))
        .respond_with(
            wiremock::ResponseTemplate::new(status)
                .set_body_string(body)
                .insert_header("content-type", "application/json")
                .set_delay(Duration::from_millis(50)),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn tag_definitions_reads_the_names_and_serves_them_from_cache() {
    let server = wiremock::MockServer::start().await;
    mount_tagdefs(&server, 200, TAGDEFS).await;
    let store = store_against(&server).await;

    let defs = store.tag_definitions(false).await.expect("tagdefs");
    let want: HashSet<String> = ["PII", "live.pii"].into_iter().map(String::from).collect();
    assert_eq!(*defs, want);
    // A cached read makes no request.
    let again = store.tag_definitions(false).await.unwrap();
    assert!(Arc::ptr_eq(&defs, &again));
    assert_eq!(downloads(&server).await, 1);
    // `fresh` re-reads once, and refills the cache.
    store.tag_definitions(true).await.unwrap();
    assert_eq!(downloads(&server).await, 2);
    store.tag_definitions(false).await.unwrap();
    assert_eq!(downloads(&server).await, 2);
    // invalidate_all (GRANT, a tag write) drops the cached list.
    store.invalidate_all();
    store.tag_definitions(false).await.unwrap();
    assert_eq!(downloads(&server).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_tagdef_misses_download_once() {
    let server = wiremock::MockServer::start().await;
    mount_tagdefs(&server, 200, TAGDEFS).await;
    let store = store_against(&server).await;
    for fresh in [false, true] {
        let before = downloads(&server).await;
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.tag_definitions(fresh).await.expect("tagdefs") })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let made = downloads(&server).await - before;
        // A miss downloads once; fresh callers that queued behind a running
        // download take it, so at most the first one and one more run.
        assert!((1..=2).contains(&made), "fresh={fresh}: {made} downloads");
    }
}

/// Fails closed, and the failure is not cached: the next call asks again.
#[tokio::test]
async fn unreadable_tag_definitions_are_an_error_and_never_cached() {
    for (status, body) in [
        (401, "{}"),
        (404, ""),
        (500, "boom"),
        (200, "{\"vXTagDefs\":[]}"),
        (200, "not json"),
    ] {
        let server = wiremock::MockServer::start().await;
        mount_tagdefs(&server, status, body).await;
        let store = store_against(&server).await;
        let err = store
            .tag_definitions(false)
            .await
            .expect_err("must fail closed");
        assert!(
            err.to_string()
                .contains("could not read the Ranger tag definitions"),
            "{status} {body:?}: {err}"
        );
        store
            .tag_definitions(false)
            .await
            .expect_err("still failing");
        assert_eq!(
            downloads(&server).await,
            2,
            "{status}: an error must not be cached"
        );
        // Tag-definition failures leave the policy breaker alone.
        for _ in 0..10 {
            let _ = store.tag_definitions(false).await;
        }
        assert!(
            store.breaker.check().is_ok(),
            "{status}: breaker must stay closed"
        );
    }
}

/// A download in flight when `invalidate_all` runs (a GRANT through SQE)
/// is returned to its caller but not cached, so the next query downloads
/// the post-GRANT bundle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_download_racing_invalidate_all_is_not_cached() {
    let server = wiremock::MockServer::start().await;
    mount_bundle(&server, 200, Duration::from_millis(300)).await;
    let store = store_against(&server).await;

    let in_flight = {
        let store = store.clone();
        tokio::spawn(async move { store.cached_bundle().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    PolicyStore::invalidate_all(store.as_ref());
    in_flight.await.unwrap().expect("bundle");

    assert!(store.bundle_cache.get(BUNDLE_KEY).await.is_none());
}

pub(super) fn user(name: &str, roles: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: roles.iter().map(|s| s.to_string()).collect(),
        subject: None,
        email: None,
        groups: vec![],
    }
}

/// Ranger wildcard and case-insensitive matching, per the `hive` service
/// definition's `matcherOptions: {wildCard: "true", ignoreCase: "true"}`.
///
/// This replaces a characterization test that pinned the OPPOSITE behaviour
/// (`ranger_glob_patterns_are_not_matched`) and said in its own comment that it
/// SHOULD fail and be rewritten when glob support landed. It has.
///
/// Both halves matter for the same reason: a policy that does not fire is
/// masking or filtering that is silently ABSENT while the Ranger console shows
/// it as configured.
#[test]
fn ranger_wildcards_and_case_folding_match_the_servicedef() {
    let r = |v: &str| RangerResource {
        values: vec![v.to_string()],
        is_excludes: false,
    };

    // Prefix, suffix and interior globs.
    assert!(
        resource_matches(&r("orders*"), "orders"),
        "* may match nothing"
    );
    assert!(resource_matches(&r("orders*"), "orders_2024"));
    assert!(!resource_matches(&r("orders*"), "sales_orders"));
    assert!(resource_matches(&r("*_pii"), "customer_pii"));
    assert!(!resource_matches(&r("*_pii"), "pii_customer"));
    assert!(resource_matches(&r("a*b*c"), "axxbyyc"));

    // `?` is exactly one character.
    assert!(resource_matches(&r("orders_20??"), "orders_2024"));
    assert!(!resource_matches(&r("orders_20??"), "orders_204"));

    // ignoreCase, both directions.
    assert!(resource_matches(&r("Orders"), "orders"));
    assert!(resource_matches(&r("orders"), "ORDERS"));
    assert!(resource_matches(&r("ORD*"), "orders_2024"));

    // Nothing else is special: a Ranger pattern is not a regex, so these are
    // literals and must NOT match arbitrary text.
    assert!(!resource_matches(&r("orders."), "ordersX"));
    assert!(!resource_matches(&r("or.*"), "orders"));

    // Bare `*` and exact match still behave.
    assert!(resource_matches(&r("*"), "anything"));
    assert!(resource_matches(&r("orders"), "orders"));
    assert!(!resource_matches(&r("orders"), "invoices"));

    // isExcludes inverts, and now inverts a GLOB too.
    let excl = RangerResource {
        values: vec!["tmp_*".to_string()],
        is_excludes: true,
    };
    assert!(
        !resource_matches(&excl, "tmp_scratch"),
        "excluded by the glob"
    );
    assert!(resource_matches(&excl, "orders"), "outside the exclusion");

    // An empty values list matches nothing.
    let empty = RangerResource {
        values: vec![],
        is_excludes: false,
    };
    assert!(!resource_matches(&empty, "orders"));
}

fn user_with_groups(name: &str, roles: &[&str], groups: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: roles.iter().map(|s| s.to_string()).collect(),
        subject: None,
        email: None,
        groups: groups.iter().map(|s| s.to_string()).collect(),
    }
}

/// A pathological pattern must not blow up: policies are operator input on a
/// per-query path, so backtracking has to stay bounded.
#[test]
fn wildcard_matching_terminates_on_a_pathological_pattern() {
    let start = std::time::Instant::now();
    assert!(!wildcard_match("*a*a*a*a*a*a*a*b", &"a".repeat(64)));
    assert!(wildcard_match(&"*".repeat(32), "anything"));
    assert!(
        start.elapsed() < std::time::Duration::from_millis(500),
        "matching took {:?}; the backtracking is not bounded",
        start.elapsed()
    );
}

#[test]
fn group_bound_items_match_the_session_groups() {
    let g = ["data-platform".to_string()];
    let empty: [String; 0] = [];

    assert!(
        item_matches(
            &empty,
            &empty,
            &g,
            &user_with_groups("bob", &[], &["data-platform"])
        ),
        "a group the session carries must match"
    );
    assert!(
        !item_matches(
            &empty,
            &empty,
            &g,
            &user_with_groups("bob", &[], &["other-group"])
        ),
        "a group the session does not carry must not match"
    );
    assert!(
        !item_matches(
            &empty,
            &empty,
            &g,
            &user_with_groups("bob", &["analyst"], &[])
        ),
        "no groups on the session cannot match a group-bound item"
    );
    // Roles and users still match on their own, and the three are OR-ed.
    assert!(item_matches(
        &["bob".to_string()],
        &empty,
        &empty,
        &user_with_groups("bob", &[], &[])
    ));
    assert!(item_matches(
        &empty,
        &["analyst".to_string()],
        &g,
        &user_with_groups("bob", &["analyst"], &[])
    ));
}

#[test]
fn flattens_iceberg_to_hive_database() {
    assert_eq!(hive_database("sales"), "sales");
    assert_eq!(hive_database("sales.eu"), "sales.eu");
}

fn db_table_policy(database: &str, table: &str) -> RangerPolicy {
    let mut p = RangerPolicy::default();
    p.resources.insert(
        "database".to_string(),
        RangerResource {
            values: vec![database.to_string()],
            is_excludes: false,
        },
    );
    p.resources.insert(
        "table".to_string(),
        RangerResource {
            values: vec![table.to_string()],
            is_excludes: false,
        },
    );
    p
}

#[test]
fn last_component_fallback_matches_and_collides_across_tenants() {
    let policy = db_table_policy("finance", "orders");
    assert!(
        policy_matches_table(&policy, "finance", "orders"),
        "exact last-component key still matches"
    );
    assert!(
        policy_matches_table(&policy, "tenant_a.finance", "orders"),
        "full namespace must still match a last-component policy"
    );
    assert!(
        policy_matches_table(&policy, "tenant_b.finance", "orders"),
        "the same last-component policy also hits the other tenant (collision)"
    );
    assert!(
        !policy_matches_table(&policy, "tenant_a.other", "orders"),
        "a different last component must not match"
    );
}

/// 902 follow-up: no fingerprint before the first bundle, then the
/// Ranger policyVersion.
#[test]
fn policy_fingerprint_follows_bundle_version() {
    let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
    assert!(store.policy_fingerprint().is_none());
    store.last_policy_version.store(7, Ordering::Relaxed);
    assert_eq!(store.policy_fingerprint().as_deref(), Some("ranger:7"));
}

#[tokio::test]
async fn new_bundle_version_drops_resolved_policy_cache() {
    let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
    let u = user("alice", &["analyst"]);
    let key = cache_key(&u, "orders", "sales");
    store
        .cache
        .insert(
            key.clone(),
            CachedPolicy(Arc::new(ResolvedPolicy::default())),
        )
        .await;
    store.last_policy_version.store(1, Ordering::Relaxed);

    store.drop_resolved_if_bundle_changed(&ServicePolicies {
        policy_version: Some(2),
        ..ServicePolicies::default()
    });
    store.cache.run_pending_tasks().await;
    assert!(
        store.cache.get(&key).await.is_none(),
        "a newer policyVersion must drop the resolved cache"
    );
}

#[tokio::test]
async fn same_bundle_version_keeps_resolved_policy_cache() {
    let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
    let u = user("alice", &["analyst"]);
    let key = cache_key(&u, "orders", "sales");
    store
        .cache
        .insert(
            key.clone(),
            CachedPolicy(Arc::new(ResolvedPolicy::default())),
        )
        .await;
    store.last_policy_version.store(7, Ordering::Relaxed);

    store.drop_resolved_if_bundle_changed(&ServicePolicies {
        policy_version: Some(7),
        ..ServicePolicies::default()
    });
    store.cache.run_pending_tasks().await;
    assert!(
        store.cache.get(&key).await.is_some(),
        "an unchanged policyVersion must keep the resolved cache"
    );
}

#[test]
fn mask_null_maps_to_nullify() {
    let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(matches!(
        policy.column_masks.get("amount"),
        Some(MaskType::Nullify)
    ));
}

#[test]
fn first_matching_mask_item_wins() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    // A second item also matching `analyst`, after the MASK_NULL one.
    let later: DataMaskPolicyItem = serde_json::from_str(
        r#"{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}}"#,
    )
    .unwrap();
    bundle.policies[0].data_mask_policy_items.push(later);
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(
        matches!(policy.column_masks.get("amount"), Some(MaskType::Nullify)),
        "the first matching item decides, a later one must not override it"
    );
}

#[test]
fn row_filter_applied_for_matching_role() {
    let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert_eq!(policy.row_filters.len(), 1);
}

#[test]
fn no_match_for_other_role() {
    let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let policy = resolve_from_bundle(
        &bundle,
        &user("bob", &["engineer"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.column_masks.is_empty());
    assert!(policy.row_filters.is_empty());
}

#[test]
fn user_match_works_too() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0].data_mask_policy_items[0].roles.clear();
    bundle.policies[0].data_mask_policy_items[0].users = vec!["alice".to_string()];
    let policy = resolve_from_bundle(&bundle, &user("alice", &[]), "orders", "sales", false);
    assert!(policy.column_masks.contains_key("amount"));
}

#[test]
fn unsupported_mask_restricts_column_failclosed() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0].data_mask_policy_items[0]
        .data_mask_info
        .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.restricted_columns.contains(&"amount".to_string()));
    assert!(!policy.column_masks.contains_key("amount"));
}

#[test]
fn custom_mask_unparseable_expr_restricts_column_failclosed() {
    // A resource CUSTOM mask whose valueExpr cannot be parsed (here a
    // qualified reference, which parse_sql_predicate rejects) must fail
    // closed: the column is restricted and carries no mask, never returned
    // raw. This pins the documented behavior of the resource CUSTOM path.
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0].data_mask_policy_items[0]
        .data_mask_info
        .data_mask_type = "CUSTOM".to_string();
    bundle.policies[0].data_mask_policy_items[0]
        .data_mask_info
        .value_expr = Some("t.department".to_string());
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(
        policy.restricted_columns.contains(&"amount".to_string()),
        "unparseable CUSTOM mask must restrict the column (fail-closed)"
    );
    assert!(
        !policy.column_masks.contains_key("amount"),
        "restricted column must not also carry a mask"
    );
}

#[test]
fn datamask_column_isexcludes_denies_failclosed() {
    // A datamask policy authored as "mask all columns EXCEPT these"
    // (column.isExcludes = true) cannot be honored on the resource path
    // (no schema for the complement). The previous code read the values as
    // an include list, leaving every intended-masked column raw. It must
    // now fail closed: deny the table.
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0]
        .resources
        .get_mut("column")
        .unwrap()
        .is_excludes = true;
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(
        policy.row_filters.contains(&lit(false)),
        "column isExcludes datamask must inject a deny (lit(false)) row filter"
    );
    assert!(
        policy.column_masks.is_empty(),
        "excludes datamask must not be treated as an include list"
    );
}

#[test]
fn mask_none_is_exemption() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0].data_mask_policy_items[0]
        .data_mask_info
        .data_mask_type = "MASK_NONE".to_string();
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(!policy.column_masks.contains_key("amount"));
    assert!(!policy.restricted_columns.contains(&"amount".to_string()));
}

#[test]
fn disabled_policy_is_skipped() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0].is_enabled = false; // the datamask policy
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.column_masks.is_empty());
}

#[test]
fn wrong_table_does_not_match() {
    let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "customers",
        "sales",
        false,
    );
    assert!(policy.column_masks.is_empty());
    assert!(policy.row_filters.is_empty());
}

#[test]
fn unparseable_row_filter_fails_closed() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[1].row_filter_policy_items[0]
        .row_filter_info
        .filter_expr = Some("this is not sql !!!".to_string());
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    // Fail-closed: a broken filter must NOT result in zero filters (which
    // would expose all rows). Expect a lit(false) deny filter instead.
    assert_eq!(policy.row_filters.len(), 1);
    // The single filter should be the literal-false deny, not a parsed predicate.
    let s = format!("{:?}", policy.row_filters[0]).to_lowercase();
    assert!(
        s.contains("false") || s.contains("boolean(false)"),
        "expected deny filter, got {s}"
    );
}

#[test]
fn masks_all_columns_in_multi_column_policy() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0]
        .resources
        .get_mut("column")
        .unwrap()
        .values = vec!["amount".to_string(), "discount".to_string()];
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.column_masks.contains_key("amount"));
    assert!(policy.column_masks.contains_key("discount"));
}

#[test]
fn wildcard_table_matches() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    bundle.policies[0]
        .resources
        .get_mut("table")
        .unwrap()
        .values = vec!["*".to_string()];
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "anything",
        "sales",
        false,
    );
    assert!(policy.column_masks.contains_key("amount"));
}

#[test]
fn excludes_inverts_match() {
    // is_excludes on table should make "orders" NOT match a values=["orders"] exclude.
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let tr = bundle.policies[0].resources.get_mut("table").unwrap();
    tr.is_excludes = true; // exclude "orders"
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.column_masks.is_empty());
}

#[test]
fn custom_mask_substitutes_column() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let mi = &mut bundle.policies[0].data_mask_policy_items[0].data_mask_info;
    mi.data_mask_type = "CUSTOM".to_string();
    mi.value_expr = Some("concat('x', {col})".to_string());
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    match policy.column_masks.get("amount") {
        Some(crate::MaskType::Custom(e)) => {
            let s = datafusion::sql::unparser::expr_to_sql(e)
                .unwrap()
                .to_string()
                .to_lowercase();
            assert!(
                s.contains("amount"),
                "custom expr must reference the real column: {s}"
            );
        }
        other => panic!("expected Custom mask, got {other:?}"),
    }
}

#[test]
fn group_only_item_does_not_match_empty_session_groups() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let item = &mut bundle.policies[0].data_mask_policy_items[0];
    item.roles.clear();
    item.users.clear();
    item.groups = vec!["analysts_group".to_string()];
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(
        policy.column_masks.is_empty(),
        "group-only item must not match a session with empty groups \
         (groups_claim unset)"
    );
}

#[test]
fn group_only_item_matches_when_session_has_groups() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let item = &mut bundle.policies[0].data_mask_policy_items[0];
    item.roles.clear();
    item.users.clear();
    item.groups = vec!["analysts_group".to_string()];
    let policy = resolve_from_bundle(
        &bundle,
        &user_with_groups("alice", &[], &["analysts_group"]),
        "orders",
        "sales",
        false,
    );
    assert!(
        policy.column_masks.contains_key("amount"),
        "group-only item must fire when the session carries that group"
    );
}

#[test]
fn count_group_only_items_is_the_bundle_warn_hook() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    assert_eq!(count_group_only_items(&bundle), 0);
    assert_eq!(note_group_only_items(&bundle, None), 0);

    bundle.policies[0].data_mask_policy_items[0].roles.clear();
    bundle.policies[0].data_mask_policy_items[0].users.clear();
    bundle.policies[0].data_mask_policy_items[0].groups = vec!["analysts_group".into()];
    assert_eq!(count_group_only_items(&bundle), 1);

    // An item that also has roles is not group-only.
    bundle.policies[1].row_filter_policy_items[0].groups = vec!["analysts_group".into()];
    assert_eq!(count_group_only_items(&bundle), 1);

    let metrics = MetricsRegistry::new().unwrap();
    assert_eq!(note_group_only_items(&bundle, Some(&metrics)), 1);
    assert_eq!(metrics.ranger_group_only_items.get(), 1);
    // A second note (another download of the same bundle) bumps again.
    assert_eq!(note_group_only_items(&bundle, Some(&metrics)), 1);
    assert_eq!(metrics.ranger_group_only_items.get(), 2);
}

// --- Hive-root access gate (policyType 0) ---

/// Access policies on a Glue root `glue`, database `bi`. Table names are
/// fixture names, not anything real.
const ACCESS_BUNDLE: &str = r#"{
  "policyVersion": 3,
  "policies": [
    {"id":1,"name":"bi-read","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]},"column":{"values":["*"]}},
     "policyItems":[{"groups":["all-employees"],"accesses":[{"type":"select","isAllowed":true}]}]},
    {"id":2,"name":"bi-secret-deny","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["secret_*"]},"column":{"values":["*"]}},
     "denyPolicyItems":[{"groups":["all-employees"],"accesses":[{"type":"select","isAllowed":true}]}],
     "denyExceptions":[{"groups":["sensitive-readers"],"accesses":[{"type":"select","isAllowed":true}]}]},
    {"id":3,"name":"bi-partial-columns","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["partial"]},"column":{"values":["amount"]}},
     "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
    {"id":4,"name":"bi-public","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["open_t"]}},
     "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
    {"id":5,"name":"bi-disabled","policyType":0,"isEnabled":false,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["hidden"]}},
     "policyItems":[{"groups":["public"],"accesses":[{"type":"all","isAllowed":true}]}]},
    {"id":6,"name":"bi-admin-except-eve","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]}},
     "policyItems":[{"roles":["admin"],"accesses":[{"type":"all","isAllowed":true}]}],
     "allowExceptions":[{"users":["eve"],"accesses":[{"type":"all","isAllowed":true}]}]},
    {"id":7,"name":"bi-mask-ssn","policyType":1,"isEnabled":true,
     "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]},"column":{"values":["ssn"]}},
     "dataMaskPolicyItems":[{"groups":["all-employees"],"dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]}
  ]
}"#;

pub(super) fn access_bundle() -> ServicePolicies {
    serde_json::from_str(ACCESS_BUNDLE).unwrap()
}

fn gated(user: &SessionUser, table: &str, namespace: &str) -> ResolvedPolicy {
    resolve_from_bundle(&access_bundle(), user, table, namespace, true)
}

#[test]
fn a_hive_table_with_no_grant_is_denied() {
    let policy = gated(
        &user_with_groups("alice", &[], &["all-employees"]),
        "orders",
        "glue.other",
    );
    assert!(
        crate::plan_rewriter::policy_denies_all(&policy),
        "no policy names glue.other"
    );
    // And a user with no groups at all gets nothing from a group-bound grant.
    let policy = gated(&user("nobody", &[]), "orders", "glue.bi");
    assert!(crate::plan_rewriter::policy_denies_all(&policy));
}

#[test]
fn a_group_grant_opens_the_table_and_masks_still_apply() {
    let policy = gated(
        &user_with_groups("alice", &[], &["all-employees"]),
        "orders",
        "glue.bi",
    );
    assert!(!crate::plan_rewriter::policy_denies_all(&policy));
    assert!(policy.row_filters.is_empty());
    assert!(
        matches!(policy.column_masks.get("ssn"), Some(MaskType::Nullify)),
        "the mask policy still lands"
    );
}

#[test]
fn a_deny_item_beats_the_allow_unless_excepted() {
    let general = user_with_groups("alice", &[], &["all-employees"]);
    assert!(crate::plan_rewriter::policy_denies_all(&gated(
        &general,
        "secret_forecast",
        "glue.bi"
    )));
    // Same person, also in the exception group: the deny is carved out and
    // policy 1 grants.
    let sensitive = user_with_groups("alice", &[], &["all-employees", "sensitive-readers"]);
    assert!(!crate::plan_rewriter::policy_denies_all(&gated(
        &sensitive,
        "secret_forecast",
        "glue.bi"
    )));
}

#[test]
fn a_column_scoped_grant_does_not_open_the_table() {
    let policy = gated(&user("carol", &[]), "partial", "glue.bi");
    assert!(
        crate::plan_rewriter::policy_denies_all(&policy),
        "select on one column is not table access"
    );
}

#[test]
fn public_grants_every_authenticated_user() {
    let policy = gated(&user("carol", &[]), "open_t", "glue.bi");
    assert!(!crate::plan_rewriter::policy_denies_all(&policy));
}

#[test]
fn a_disabled_policy_grants_nothing() {
    assert!(crate::plan_rewriter::policy_denies_all(&gated(
        &user("carol", &[]),
        "hidden",
        "glue.bi"
    )));
}

#[test]
fn allow_exceptions_carve_a_user_out_of_a_role_grant() {
    assert!(!crate::plan_rewriter::policy_denies_all(&gated(
        &user("dan", &["admin"]),
        "anything",
        "glue.bi"
    )));
    assert!(crate::plan_rewriter::policy_denies_all(&gated(
        &user("eve", &["admin"]),
        "anything",
        "glue.bi"
    )));
}

/// Column `alter` grants for the tag gate (#911). `policies` is a list of
/// `(column resource JSON or "" for none, items JSON)`; every policy sits
/// on database `lake.sales`, table `orders`.
fn column_bundle(policies: &[(&str, &str)]) -> ServicePolicies {
    let body = policies
        .iter()
        .enumerate()
        .map(|(i, (col, items))| {
            let col = if col.is_empty() { String::new() } else { format!(r#","column":{col}"#) };
            format!(
                r#"{{"id":{id},"name":"p{id}","policyType":0,"isEnabled":true,
                  "resources":{{"database":{{"values":["lake.sales"]}},"table":{{"values":["orders"]}}{col}}},
                  {items}}}"#,
                id = i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    serde_json::from_str(&format!(r#"{{"policies":[{body}]}}"#)).unwrap()
}

fn col_alter(b: &ServicePolicies, u: &SessionUser, column: &str) -> bool {
    access_grants_column_type(b, u, "lake.sales", "orders", column, "alter", true)
}

const STEWARD_ALLOW: &str =
    r#""policyItems":[{"users":["steward"],"accesses":[{"type":"alter","isAllowed":true}]}]"#;
const STEWARD_DENY: &str =
    r#""denyPolicyItems":[{"users":["steward"],"accesses":[{"type":"alter","isAllowed":true}]}]"#;

#[test]
fn column_alter_allow_covers_that_column_only() {
    let b = column_bundle(&[(r#"{"values":["ssn"]}"#, STEWARD_ALLOW)]);
    let u = user("steward", &[]);
    assert!(col_alter(&b, &u, "ssn"));
    assert!(col_alter(&b, &u, "SSN"), "case-insensitive");
    assert!(
        !col_alter(&b, &u, "email"),
        "a grant on another column is not this column"
    );
    assert!(
        !col_alter(&b, &user("other", &[]), "ssn"),
        "nobody else is granted"
    );
    // The table-level check still refuses a column-scoped grant.
    assert!(!access_grants_table_type(
        &b,
        &u,
        "lake.sales",
        "orders",
        "alter",
        true
    ));
}

#[test]
fn column_alter_star_and_wildcard_and_excludes() {
    let u = user("steward", &[]);
    let star = column_bundle(&[(r#"{"values":["*"]}"#, STEWARD_ALLOW)]);
    assert!(col_alter(&star, &u, "anything"));
    let wild = column_bundle(&[(r#"{"values":["ss*"]}"#, STEWARD_ALLOW)]);
    assert!(col_alter(&wild, &u, "ssn"));
    assert!(!col_alter(&wild, &u, "email"));
    let excl = column_bundle(&[(r#"{"values":["ssn"],"isExcludes":true}"#, STEWARD_ALLOW)]);
    assert!(!col_alter(&excl, &u, "ssn"));
    assert!(col_alter(&excl, &u, "email"));
}

#[test]
fn column_alter_deny_beats_allow_and_is_column_scoped() {
    let u = user("steward", &[]);
    let allow_all = (r#"{"values":["*"]}"#, STEWARD_ALLOW);
    let deny_ssn = column_bundle(&[allow_all, (r#"{"values":["ssn"]}"#, STEWARD_DENY)]);
    assert!(
        !col_alter(&deny_ssn, &u, "ssn"),
        "a column deny beats the allow"
    );
    assert!(
        col_alter(&deny_ssn, &u, "email"),
        "a deny on another column does not deny this one"
    );
    let deny_star = column_bundle(&[allow_all, (r#"{"values":["*"]}"#, STEWARD_DENY)]);
    assert!(!col_alter(&deny_star, &u, "ssn"));
    assert!(!col_alter(&deny_star, &u, "email"));
}

#[test]
fn column_alter_conditional_allow_is_ignored_and_no_column_resource_matches_all() {
    let u = user("steward", &[]);
    let cond = column_bundle(&[(
        r#"{"values":["ssn"]}"#,
        r#""policyItems":[{"users":["steward"],"accesses":[{"type":"alter","isAllowed":true}],
           "conditions":[{"type":"ip-range","values":["10.0.0.0/8"]}]}]"#,
    )]);
    assert!(
        !col_alter(&cond, &u, "ssn"),
        "a condition SQE cannot evaluate grants nothing"
    );
    let none = column_bundle(&[("", STEWARD_ALLOW)]);
    assert!(col_alter(&none, &u, "ssn"));
    assert!(col_alter(&none, &u, "email"));
}

#[test]
fn a_deny_on_all_denies_apply_tag() {
    let u = user("steward", &[]);
    let b = column_bundle(&[
        (
            r#"{"values":["*"]}"#,
            r#""policyItems":[{"users":["steward"],"accesses":[{"type":"apply_tag","isAllowed":true}]}]"#,
        ),
        (
            r#"{"values":["ssn"]}"#,
            r#""denyPolicyItems":[{"users":["steward"],"accesses":[{"type":"all","isAllowed":true}]}]"#,
        ),
    ]);
    let can = |c| access_grants_column_type(&b, &u, "lake.sales", "orders", c, "apply_tag", true);
    assert!(!can("ssn"), "a deny item's `all` narrows apply_tag too");
    assert!(can("email"));
    // An allow exception's `all` carves the user out as well.
    let b = column_bundle(&[(
        r#"{"values":["*"]}"#,
        r#""policyItems":[{"users":["steward"],"accesses":[{"type":"apply_tag","isAllowed":true}]}],
           "allowExceptions":[{"users":["steward"],"accesses":[{"type":"all","isAllowed":true}]}]"#,
    )]);
    assert!(!access_grants_column_type(
        &b,
        &u,
        "lake.sales",
        "orders",
        "ssn",
        "apply_tag",
        true
    ));
}

#[test]
fn remove_tag_comes_only_from_an_item_naming_it() {
    let u = user("steward", &[]);
    let can = |t: &str| {
        let item = format!(
            r#""policyItems":[{{"users":["steward"],"accesses":[{{"type":"{t}","isAllowed":true}}]}}]"#
        );
        let b = column_bundle(&[(r#"{"values":["*"]}"#, item.as_str())]);
        access_grants_column_type(&b, &u, "lake.sales", "orders", "ssn", "remove_tag", true)
    };
    assert!(can("remove_tag"));
    assert!(!can("all") && !can("apply_tag") && !can("alter"));
}

#[test]
fn without_the_gate_the_table_is_open() {
    let policy = resolve_from_bundle(
        &access_bundle(),
        &user("nobody", &[]),
        "orders",
        "glue.other",
        false,
    );
    assert!(!crate::plan_rewriter::policy_denies_all(&policy));
}

#[test]
fn the_gate_applies_to_registered_hive_roots_only() {
    let roots = Arc::new(HiveCatalogs::new());
    roots.insert("glue");
    let store = RangerStore::from_config(&RangerPolicyConfig {
        url: "http://ranger:6080".into(),
        ..RangerPolicyConfig::default()
    })
    .unwrap()
    .with_hive_catalogs(roots.clone());
    assert!(store.enforces_hive_access(), "on by default");
    assert!(
        store.requires_access_grant("glue.bi"),
        "Hive root namespace"
    );
    assert!(
        !store.requires_access_grant("sales"),
        "Iceberg namespace: the catalog decides"
    );
    assert!(!store.requires_access_grant("a.b.sales"));

    let off = RangerStore::from_config(&RangerPolicyConfig {
        url: "http://ranger:6080".into(),
        hive_access_policies: false,
        ..RangerPolicyConfig::default()
    })
    .unwrap()
    .with_hive_catalogs(roots);
    assert!(!off.requires_access_grant("glue.bi"));
}

#[test]
fn last_component_fallback_restricts_but_never_grants_access() {
    let allow_short: ServicePolicies = serde_json::from_str(
        r#"{"policies":[{"id":1,"name":"sales-allow","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["sales"]},"table":{"values":["*"]}},
         "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]}]}"#,
    )
    .unwrap();
    let carol = user("carol", &[]);
    assert!(
        crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(
            &allow_short,
            &carol,
            "t",
            "root.sales",
            true
        )),
        "an allow on `sales` must not grant `root.sales`"
    );
    assert!(!crate::plan_rewriter::policy_denies_all(
        &resolve_from_bundle(&allow_short, &carol, "t", "sales", true)
    ));

    let deny_short: ServicePolicies = serde_json::from_str(
        r#"{"policies":[
         {"id":1,"name":"root-sales-allow","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["root.sales"]},"table":{"values":["*"]}},
          "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
         {"id":2,"name":"sales-deny","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["sales"]},"table":{"values":["*"]}},
          "denyPolicyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]}]}"#,
    )
    .unwrap();
    assert!(
        crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(
            &deny_short,
            &carol,
            "t",
            "root.sales",
            true
        )),
        "a deny on `sales` still denies `root.sales`"
    );
}

#[test]
fn group_only_access_items_are_counted_but_public_is_not() {
    // Policy 1's allow and policy 2's deny bind to a real group with no
    // users or roles, as does policy 7's mask item; the public-bound
    // items (3, 4, 5), the exceptions and the role/user items (6) are not
    // at risk.
    assert_eq!(count_group_only_items(&access_bundle()), 3);
}

/// One public `select` access policy on `glue.bi.t`, with `extra` JSON
/// spliced into the policy object and `item_extra` into its allow item.
fn one_policy_bundle(extra: &str, item_extra: &str) -> ServicePolicies {
    serde_json::from_str(&format!(
        r#"{{"policies":[{{"id":42,"name":"p","policyType":0,"isEnabled":true{extra},
          "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
          "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]{item_extra}}}]}}]}}"#
    ))
    .unwrap()
}

fn denied(bundle: &ServicePolicies) -> bool {
    crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(
        bundle,
        &user("carol", &[]),
        "t",
        "glue.bi",
        true,
    ))
}

#[test]
fn an_allow_item_with_conditions_grants_nothing() {
    assert!(
        !denied(&one_policy_bundle("", "")),
        "control: the plain item grants"
    );
    let conditional = one_policy_bundle(
        "",
        r#","conditions":[{"type":"ip-range","values":["10.0.0.0/8"]}]"#,
    );
    assert!(
        denied(&conditional),
        "an unevaluated condition must not grant"
    );
    assert!(
        !denied(&one_policy_bundle("", r#","conditions":[]"#)),
        "an empty list is no condition"
    );
}

/// A deny exception widens access, so one carrying conditions SQE cannot
/// evaluate does not carve the user out of the deny.
#[test]
fn a_deny_exception_with_conditions_does_not_exempt() {
    let bundle = |exception_extra: &str| -> ServicePolicies {
        serde_json::from_str(&format!(
            r#"{{"policies":[{{"id":1,"policyType":0,"isEnabled":true,
              "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
              "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}],
              "denyPolicyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}],
              "denyExceptions":[{{"users":["carol"],"accesses":[{{"type":"select","isAllowed":true}}]{exception_extra}}}]}}]}}"#
        ))
        .unwrap()
    };
    assert!(
        !denied(&bundle("")),
        "control: the plain exception exempts carol"
    );
    assert!(
        denied(&bundle(
            r#","conditions":[{"type":"ip-range","values":["10.0.0.0/8"]}]"#
        )),
        "an unevaluated condition must not exempt from a deny"
    );
}

/// The first matching datamask item decides; when it carries conditions
/// SQE cannot evaluate, the column is restricted instead of falling
/// through to a later item (here a `MASK_NONE` that would show it raw).
#[test]
fn a_conditional_mask_item_restricts_the_column() {
    let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
    let conditional: DataMaskPolicyItem = serde_json::from_str(
        r#"{"roles": ["analyst"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
            "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}"#,
    )
    .unwrap();
    let unmask: DataMaskPolicyItem = serde_json::from_str(
        r#"{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}}"#,
    )
    .unwrap();
    bundle.policies[0].data_mask_policy_items = vec![conditional, unmask];
    let policy = resolve_from_bundle(
        &bundle,
        &user("alice", &["analyst"]),
        "orders",
        "sales",
        false,
    );
    assert!(policy.restricted_columns.contains(&"amount".to_string()));
    assert!(!policy.column_masks.contains_key("amount"));
}

#[test]
fn a_conditional_tag_mask_item_makes_the_tag_unmappable() {
    let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0].conditions =
        vec![serde_json::json!({"type": "ip-range", "values": ["10.0.0.0/8"]})];
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let (masks, _, unmappable) = resolve_tag_policies(&sp, &user("bob", &["engineer"]), &tags);
    assert!(!masks.contains_key("PII"));
    assert!(unmappable.contains("PII"));
}

#[test]
fn validity_schedules_gate_the_allow() {
    let sched = |s: &str| one_policy_bundle(&format!(r#","validitySchedules":[{s}]"#), "");
    assert!(
        denied(&sched(
            r#"{"startTime":"2000/01/01 00:00:00","endTime":"2001/01/01 00:00:00","timeZone":"UTC"}"#
        )),
        "expired"
    );
    assert!(
        denied(&sched(r#"{"startTime":"2999/01/01 00:00:00"}"#)),
        "not started"
    );
    assert!(
        !denied(&sched(
            r#"{"startTime":"2000/01/01 00:00:00","endTime":"2999/01/01 00:00:00","timeZone":"Europe/Amsterdam"}"#
        )),
        "in force"
    );
    assert!(
        denied(&sched(r#"{"endTime":"not a date"}"#)),
        "unparseable schedule fails closed"
    );
    assert!(
        denied(&sched(
            r#"{"endTime":"2999/01/01 00:00:00","timeZone":"Mars/Olympus"}"#
        )),
        "unknown zone fails closed"
    );
    assert!(
        denied(&sched(r#"{"recurrences":[{"interval":{"hours":1}}]}"#)),
        "recurrences are not modelled"
    );
    // Any one in-force schedule is enough.
    assert!(!denied(&sched(
        r#"{"endTime":"bad"},{"endTime":"2999/01/01 00:00:00"}"#
    )));
}

#[test]
fn an_expired_deny_no_longer_denies_but_an_unparseable_one_still_does() {
    let bundle = |sched: &str| -> ServicePolicies {
        serde_json::from_str(&format!(
            r#"{{"policies":[
              {{"id":1,"policyType":0,"isEnabled":true,
                "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
                "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}]}},
              {{"id":2,"policyType":0,"isEnabled":true,"validitySchedules":[{sched}],
                "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
                "denyPolicyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}]}}]}}"#
        ))
        .unwrap()
    };
    assert!(!denied(&bundle(r#"{"endTime":"2001/01/01 00:00:00"}"#)));
    assert!(denied(&bundle(r#"{"endTime":"garbage"}"#)));
    assert!(denied(&bundle(r#"{"endTime":"2999/01/01 00:00:00"}"#)));
}

#[test]
fn schedule_bounds_are_read_in_the_schedule_time_zone() {
    // 2026-06-01 10:00 UTC is 12:00 in Amsterdam (CEST, UTC+2).
    let now = Utc.with_ymd_and_hms(2026, 6, 1, 10, 0, 0).unwrap();
    let s = |start: &str, tz: &str| ValiditySchedule {
        start_time: Some(start.into()),
        time_zone: Some(tz.into()),
        ..Default::default()
    };
    assert_eq!(
        schedule_covers(&s("2026/06/01 11:00:00", "UTC"), now),
        Some(false)
    );
    assert_eq!(
        schedule_covers(&s("2026/06/01 11:00:00", "Europe/Amsterdam"), now),
        Some(true)
    );
    assert_eq!(schedule_covers(&s("2026-06-01 11:00:00", "UTC"), now), None);
}

// --- map_mask arm tests ---

#[test]
fn maps_show_last_4() {
    let info = DataMaskInfo {
        data_mask_type: "MASK_SHOW_LAST_4".into(),
        ..Default::default()
    };
    match map_mask(&info, "ssn", &SessionIdentity::default()) {
        Ok(Some(MaskType::PartialMask {
            show_last: 4,
            show_first: 0,
            ..
        })) => {}
        other => panic!("expected show-last-4 PartialMask, got {other:?}"),
    }
}

#[test]
fn maps_show_first_4() {
    let info = DataMaskInfo {
        data_mask_type: "MASK_SHOW_FIRST_4".into(),
        ..Default::default()
    };
    assert!(matches!(
        map_mask(&info, "ssn", &SessionIdentity::default()),
        Ok(Some(MaskType::PartialMask {
            show_first: 4,
            show_last: 0,
            ..
        }))
    ));
}

#[test]
fn maps_full_mask_uses_hive_default_chars() {
    let info = DataMaskInfo {
        data_mask_type: "MASK".into(),
        ..Default::default()
    };
    match map_mask(&info, "name", &SessionIdentity::default()) {
        Ok(Some(MaskType::PartialMask {
            upper: 'X',
            lower: 'x',
            digit: 'n',
            show_first: 0,
            show_last: 0,
        })) => {}
        other => panic!("got {other:?}"),
    }
}

#[test]
fn maps_date_show_year() {
    let info = DataMaskInfo {
        data_mask_type: "MASK_DATE_SHOW_YEAR".into(),
        ..Default::default()
    };
    assert!(matches!(
        map_mask(&info, "hired_at", &SessionIdentity::default()),
        Ok(Some(MaskType::DateShowYear))
    ));
}

#[test]
fn truly_unknown_mask_is_err() {
    let info = DataMaskInfo {
        data_mask_type: "MASK_FUTURE_UNSUPPORTED".into(),
        ..Default::default()
    };
    assert!(map_mask(&info, "x", &SessionIdentity::default()).is_err());
}

// --- tagPolicies tests ---

/// A ServicePolicies bundle that includes a top-level `tagPolicies` block
/// with a datamask policy for tag "PII" (role=engineer) and a row-filter
/// policy for tag "RESTRICTED" (role=analyst).
///
/// NOTE: the exact live shape of tagPolicies must be verified against a
/// real Ranger bundle that has tag-linked policies before this is used in
/// production. See the Phase 3 prerequisite task.
const TAG_BUNDLE: &str = r#"{
  "policyVersion": 1,
  "policies": [],
  "tagPolicies": {
    "serviceName": "tag",
    "policies": [
      {
        "id": 1, "policyType": 1, "isEnabled": true,
        "resources": { "tag": { "values": ["PII"] } },
        "dataMaskPolicyItems": [
          { "users": [], "roles": ["engineer"],
            "dataMaskInfo": { "dataMaskType": "MASK_SHOW_LAST_4" } }
        ]
      },
      {
        "id": 2, "policyType": 2, "isEnabled": true,
        "resources": { "tag": { "values": ["RESTRICTED"] } },
        "rowFilterPolicyItems": [
          { "users": [], "roles": ["analyst"],
            "rowFilterInfo": { "filterExpr": "region = 'EU'" } }
        ]
      }
    ]
  }
}"#;

#[test]
fn tag_mask_resolved_for_matching_role() {
    let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("bob", &["engineer"]);
    let (masks, filters, unmappable) = resolve_tag_policies(&sp, &id, &tags);
    // tag PII -> a PartialMask (MASK_SHOW_LAST_4) for engineer, wrapped in TagMaskSpec::Ready
    assert!(masks.contains_key("PII"));
    assert!(
        matches!(
            masks.get("PII"),
            Some(TagMaskSpec::Ready(crate::MaskType::PartialMask {
                show_last: 4,
                ..
            }))
        ),
        "supported mask must be wrapped in TagMaskSpec::Ready"
    );
    assert!(
        unmappable.is_empty(),
        "supported mask must not be unmappable"
    );
    let _ = filters; // not the focus of this test
}

#[test]
fn first_matching_tag_mask_item_wins() {
    let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let later: DataMaskPolicyItem = serde_json::from_str(
        r#"{"roles": ["engineer"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}"#,
    )
    .unwrap();
    sp.tag_policies.as_mut().unwrap().policies[0]
        .data_mask_policy_items
        .push(later);
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("bob", &["engineer"]);
    let (masks, _, _) = resolve_tag_policies(&sp, &id, &tags);
    assert!(
        matches!(
            masks.get("PII"),
            Some(TagMaskSpec::Ready(crate::MaskType::PartialMask {
                show_last: 4,
                ..
            }))
        ),
        "the first matching item (MASK_SHOW_LAST_4) decides, not the later MASK_NULL"
    );
}

#[test]
fn tag_mask_not_resolved_for_other_role() {
    let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("x", &["other"]);
    let (masks, _f, _u) = resolve_tag_policies(&sp, &id, &tags);
    assert!(masks.is_empty());
}

#[test]
fn tag_row_filter_resolved() {
    let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let tags: HashSet<String> = ["RESTRICTED".to_string()].into_iter().collect();
    let id = user("a", &["analyst"]);
    let (_m, filters, _u) = resolve_tag_policies(&sp, &id, &tags);
    assert_eq!(filters.len(), 1); // one (tag, Expr) row filter
}

/// Group-bound tag items must match on the session's groups: dropping
/// them would leave a group-scoped tag mask or row filter unapplied.
#[test]
fn tag_policies_match_on_groups() {
    let sp: ServicePolicies = serde_json::from_str(
        r#"{
          "policyVersion": 1, "policies": [],
          "tagPolicies": { "serviceName": "tag", "policies": [
            { "id": 1, "policyType": 1, "isEnabled": true,
              "resources": { "tag": { "values": ["PII"] } },
              "dataMaskPolicyItems": [
                { "groups": ["contractors"],
                  "dataMaskInfo": { "dataMaskType": "MASK_NULL" } } ] },
            { "id": 2, "policyType": 2, "isEnabled": true,
              "resources": { "tag": { "values": ["PII"] } },
              "rowFilterPolicyItems": [
                { "groups": ["contractors"],
                  "rowFilterInfo": { "filterExpr": "region = 'EU'" } } ] }
          ] }
        }"#,
    )
    .unwrap();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user_with_groups("carol", &[], &["contractors"]);
    let (masks, filters, _u) = resolve_tag_policies(&sp, &id, &tags);
    assert!(matches!(
        masks.get("PII"),
        Some(TagMaskSpec::Ready(crate::MaskType::Nullify))
    ));
    assert_eq!(filters.len(), 1);
}

#[test]
fn untagged_yields_nothing() {
    let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let tags: HashSet<String> = HashSet::new();
    let id = user("", &[]);
    let (m, f, u) = resolve_tag_policies(&sp, &id, &tags);
    assert!(m.is_empty() && f.is_empty() && u.is_empty());
}

/// An unsupported tag mask type must be reported as unmappable (not silently
/// skipped) so the rewriter restricts the column (fail-closed).
#[test]
fn unsupported_tag_mask_is_unmappable() {
    let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0]
        .data_mask_info
        .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("bob", &["engineer"]);
    let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
    assert!(
        !masks.contains_key("PII"),
        "unsupported mask must not produce a mask"
    );
    assert!(
        unmappable.contains("PII"),
        "unsupported mask must mark tag unmappable"
    );
}

/// A CUSTOM tag mask with a value_expr must be stored as `TagMaskSpec::Custom`
/// (not unmappable). The rewriter performs `{col}` substitution at merge time.
#[test]
fn custom_tag_mask_stored_as_custom_spec() {
    let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let mi =
        &mut sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0].data_mask_info;
    mi.data_mask_type = "CUSTOM".to_string();
    mi.value_expr = Some("concat('x', {col})".to_string());
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("bob", &["engineer"]);
    let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
    assert!(
        !unmappable.contains("PII"),
        "CUSTOM tag with value_expr must NOT be unmappable"
    );
    match masks.get("PII") {
        Some(TagMaskSpec::Custom(template)) => {
            assert_eq!(
                template, "concat('x', {col})",
                "template must be stored verbatim"
            );
        }
        other => panic!("expected TagMaskSpec::Custom for PII, got {:?}", other),
    }
}

/// A CUSTOM tag mask with no value_expr must remain unmappable (fail-closed):
/// there is nothing to substitute, so the column must be restricted.
#[test]
fn custom_tag_mask_no_value_expr_is_unmappable() {
    let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
    let mi =
        &mut sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0].data_mask_info;
    mi.data_mask_type = "CUSTOM".to_string();
    mi.value_expr = None; // no template
    let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
    let id = user("bob", &["engineer"]);
    let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
    assert!(
        !masks.contains_key("PII"),
        "CUSTOM mask with no value_expr must not produce a spec"
    );
    assert!(
        unmappable.contains("PII"),
        "CUSTOM mask with no value_expr must be unmappable (fail-closed)"
    );
}

// --- tagPolicies live-sample validation (HIGH-tagpolicies-shape-unvalidated) ---

/// Placeholder bundle for the live-capture validation. Ships as an empty
/// `tagPolicies` block (valid JSON) so this file compiles before the real
/// capture exists. During the Ranger-backend validation run, replace
/// `src/testdata/tag_bundle_live_sample.json` with a bundle captured from a
/// real Ranger (`GET /service/plugins/policies/download/<service>`) that
/// carries at least one tag-linked datamask and one tag-linked rowfilter,
/// then remove the `#[ignore]` below and adjust the constants to match the
/// capture.
const TAG_BUNDLE_LIVE_SAMPLE: &str = include_str!("../testdata/tag_bundle_live_sample.json");

/// Expected identity/tag the captured sample must resolve a non-empty result
/// for. Whoever drops in the real capture aligns these with the policies in
/// it (role bound to the datamask/rowfilter items, tag on the resources).
const LIVE_SAMPLE_ROLE: &str = "engineer";
const LIVE_SAMPLE_TAG: &str = "PII";
/// Tag carrying the row-filter policy in the capture.
const LIVE_SAMPLE_FILTER_TAG: &str = "RESTRICTED";

/// HIGH-tagpolicies-shape-unvalidated: deserialize a bundle captured from a
/// LIVE Ranger and assert `resolve_tag_policies` returns a non-empty result
/// (at least one mask OR one row filter). The whole tag-masking path is
/// currently validated only against a hand-authored `TAG_BUNDLE` fixture
/// whose shape is flagged unconfirmed (`TODO(phase3)` at the top of this
/// file). If the live `tagPolicies` JSON differs, `bundle.tag_policies`
/// deserializes to `None` and this test fails, surfacing the shape drift
/// instead of silently returning raw PII columns.
///
/// ACTIVE as of the Ranger-backend validation run: the capture in
/// `testdata/tag_bundle_live_sample.json` came from a live Ranger 2.8 via
/// `access_control_e2e::capture_live_tag_bundle` (re-capture with
/// `SQE_AC_CAPTURE=1 scripts/access-control-test.sh capture_live_tag_bundle`).
///
/// It carries BOTH tag policy types, so this asserts both branches against
/// real data: a `hive:MASK_SHOW_LAST_4` datamask on tag `PII` and a row
/// filter `region = 'EU'` on tag `RESTRICTED`, both bound to role
/// `engineer`. The row filter is only present because the e2e bootstrap
/// gives the tag servicedef a `rowFilterDef` -- Ranger propagates
/// `dataMaskDef` into the tag servicedef unconditionally but `rowFilterDef`
/// only when Ranger Admin sets
/// `ranger.servicedef.autopropagate.rowfilterdef.to.tag=true` (default
/// false).
#[test]
fn resolve_tag_policies_against_live_sample() {
    let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE_LIVE_SAMPLE)
        .expect("captured live sample must be valid ServicePolicies JSON");
    assert!(
        sp.tag_policies.is_some(),
        "live bundle must deserialize a tagPolicies block (None means the \
         shape drifted and tag masking would silently no-op)"
    );

    let tags: HashSet<String> = [
        LIVE_SAMPLE_TAG.to_string(),
        LIVE_SAMPLE_FILTER_TAG.to_string(),
    ]
    .into_iter()
    .collect();
    let id = user("live-sample-user", &[LIVE_SAMPLE_ROLE]);
    let (masks, filters, unmappable) = resolve_tag_policies(&sp, &id, &tags);
    assert!(
        masks.contains_key(LIVE_SAMPLE_TAG),
        "live capture must resolve a mask for tag {LIVE_SAMPLE_TAG}; got masks {:?}, \
         unmappable {unmappable:?} (a shape drift or an unhandled component-prefixed \
         mask type lands here)",
        masks.keys().collect::<Vec<_>>()
    );
    assert!(
        filters.iter().any(|(tag, _)| tag == LIVE_SAMPLE_FILTER_TAG),
        "live capture must resolve a row filter for tag {LIVE_SAMPLE_FILTER_TAG}; got {:?}",
        filters.iter().map(|(t, _)| t).collect::<Vec<_>>()
    );
}

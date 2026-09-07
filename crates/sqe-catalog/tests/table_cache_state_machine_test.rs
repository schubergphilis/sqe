//! The `SessionCatalog::load_table` cache state machine against a wiremock
//! Polaris (architecture review 313).
//!
//! Four `load_table` paths, each pinned by the requests the mock sees:
//!
//! * cold miss: one unconditional `GET loadTable` plus one fire-and-forget
//!   `HEAD` that attaches the ETag;
//! * fresh hit: no `GET`, one `HEAD` (authorization recheck) per lookup;
//! * stale entry, unchanged ETag: one conditional `GET` answered `304`, the
//!   cached metadata is reused and stamped fresh;
//! * stale entry, changed ETag: the conditional `GET` returns `200`, the
//!   table is reloaded through the REST client and the new ETag is stored
//!   from the revalidation response (no extra `HEAD`).
//!
//! Plus `invalidate_table_all_tokens` across two principals and the two
//! insert-after-invalidate races (cold load and 304 revalidation) with a
//! delayed mock.
//!
//! Why the clock is not `tokio::time::pause()`d: the cache's freshness clock
//! is `std::time::Instant`, which paused tokio time does not advance, and the
//! delayed mocks run inside wiremock's own I/O path. The soft TTL is shortened
//! with `TableMetadataCache::with_soft_ttl` instead and the tests sleep for
//! real, tens of milliseconds at a time.

use std::sync::Arc;
use std::time::Duration;

use iceberg::{NamespaceIdent, TableIdent};
use sqe_catalog::{rest_catalog_token_fingerprint, SessionCatalog, TableMetadataCache};
use sqe_core::config::StorageConfig;
use wiremock::matchers::{header, header_exists, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const WAREHOUSE: &str = "test-warehouse";
const TABLE_PATH: &str = "/v1/test-warehouse/namespaces/sales/tables/orders";
const TABLE_UUID: &str = "b55d9dda-6561-423a-8bfc-787980ce421f";

/// Matches a request that carries no `If-None-Match`, i.e. the REST client's
/// unconditional `loadTable`, as opposed to SQE's revalidation `GET`.
struct Unconditional;

impl wiremock::Match for Unconditional {
    fn matches(&self, request: &Request) -> bool {
        !request.headers.contains_key("if-none-match")
    }
}

fn orders() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orders".to_string(),
    )
}

/// A `loadTable` body the vendored REST client accepts. The `s3://` location
/// is never read: these tests stop at metadata.
fn load_table_body() -> serde_json::Value {
    serde_json::json!({
        "metadata-location": "s3://warehouse/sales/orders/metadata/00001.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": TABLE_UUID,
            "location": "s3://warehouse/sales/orders",
            "last-sequence-number": 0,
            "last-updated-ms": 1_646_787_054_459_i64,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": false, "type": "int"},
                    {"id": 2, "name": "data", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": []
        },
        "config": {}
    })
}

/// `/v1/config` advertising the warehouse as the path prefix, so the REST
/// client's `loadTable` and SQE's raw `HEAD` / conditional `GET` share
/// [`TABLE_PATH`].
async fn mount_config(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "overrides": {"prefix": WAREHOUSE},
            "defaults": {}
        })))
        .mount(server)
        .await;
}

/// The REST client's unconditional `GET loadTable`: `200` with `etag`.
async fn mount_load_table(server: &MockServer, etag: &str, delay: Option<Duration>) {
    let mut response = ResponseTemplate::new(200)
        .set_body_json(load_table_body())
        .insert_header("etag", etag);
    if let Some(delay) = delay {
        response = response.set_delay(delay);
    }
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .and(Unconditional)
        .respond_with(response)
        .mount(server)
        .await;
}

/// `HEAD loadTable` (authorization recheck and background ETag fetch).
async fn mount_head(server: &MockServer, etag: &str) {
    Mock::given(method("HEAD"))
        .and(path(TABLE_PATH))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", etag))
        .mount(server)
        .await;
}

async fn session(server: &MockServer, token: &str, cache: &TableMetadataCache) -> SessionCatalog {
    SessionCatalog::new(
        &server.uri(),
        WAREHOUSE,
        token,
        &StorageConfig::default(),
        Some(cache.clone()),
        None,
        None,
        false,
    )
    .await
    .expect("construct session catalog")
}

/// Whether `token`'s principal has an entry (fresh or stale) for `orders`.
/// Read through the cache's own lookup rather than `entry_count`, which moka
/// only settles after housekeeping.
async fn cached_for(cache: &TableMetadataCache, token: &str) -> bool {
    let key = format!(
        "{}|{WAREHOUSE}|sales.orders",
        rest_catalog_token_fingerprint(token)
    );
    cache.get_stale(&key).await.is_some()
}

/// Requests to [`TABLE_PATH`] partitioned as (unconditional GET, conditional
/// GET, HEAD).
async fn table_request_counts(server: &MockServer) -> (usize, usize, usize) {
    let requests = server.received_requests().await.unwrap_or_default();
    let mut counts = (0, 0, 0);
    for request in requests.iter().filter(|r| r.url.path() == TABLE_PATH) {
        match request.method.as_str() {
            "GET" if request.headers.contains_key("if-none-match") => counts.1 += 1,
            "GET" => counts.0 += 1,
            "HEAD" => counts.2 += 1,
            _ => {}
        }
    }
    counts
}

/// The cold-load ETag `HEAD` is fire-and-forget; wait for it so later
/// assertions on request counts and on the stored ETag are deterministic.
async fn wait_for_heads(server: &MockServer, at_least: usize) {
    for _ in 0..200 {
        if table_request_counts(server).await.2 >= at_least {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("background HEAD never reached the mock (wanted {at_least})");
}

/// Cold miss: exactly one unconditional `GET` and one background `HEAD`;
/// the returned table carries the mocked metadata.
#[tokio::test]
async fn cold_miss_loads_once_and_fetches_the_etag_in_the_background() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;
    let cache = TableMetadataCache::new(300);
    let catalog = session(&server, "cold-miss-token", &cache).await;

    let table = catalog.load_table(&orders()).await.expect("cold load");
    assert_eq!(table.metadata().uuid().to_string(), TABLE_UUID);
    wait_for_heads(&server, 1).await;

    assert_eq!(table_request_counts(&server).await, (1, 0, 1));
    assert!(
        cached_for(&cache, "cold-miss-token").await,
        "the cold load populated the cache"
    );
}

/// Fresh hit: no `GET` at all, one authorization `HEAD` per lookup while the
/// recheck window is zero, and none once a window is configured.
#[tokio::test]
async fn fresh_hit_pays_a_head_per_lookup_until_the_recheck_window_covers_it() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;

    // Recheck window zero: every fresh hit re-authorizes with a HEAD.
    let cache = TableMetadataCache::new(300);
    let catalog = session(&server, "fresh-hit-token", &cache).await;
    catalog.load_table(&orders()).await.expect("cold load");
    wait_for_heads(&server, 1).await;
    for _ in 0..3 {
        catalog.load_table(&orders()).await.expect("fresh hit");
    }
    assert_eq!(
        table_request_counts(&server).await,
        (1, 0, 4),
        "three fresh hits: zero GETs, one HEAD each on top of the background one"
    );

    // Recheck window open: the first fresh hit pays one HEAD and the rest
    // are served without a round trip (review 307).
    server.reset().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;
    let cache = TableMetadataCache::new(300).with_authorization_recheck(300);
    let catalog = session(&server, "fresh-hit-recheck-token", &cache).await;
    catalog.load_table(&orders()).await.expect("cold load");
    wait_for_heads(&server, 1).await;
    for _ in 0..3 {
        catalog.load_table(&orders()).await.expect("fresh hit");
    }
    assert_eq!(table_request_counts(&server).await, (1, 0, 2));
}

/// Stale entry with an ETag: one conditional `GET` carrying `If-None-Match`,
/// answered `304`; the cached table is returned and is fresh again.
#[tokio::test]
async fn stale_entry_revalidates_with_if_none_match_and_reuses_on_304() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .and(header("if-none-match", "\"v1\""))
        .respond_with(ResponseTemplate::new(304))
        .mount(&server)
        .await;
    let cache = TableMetadataCache::new(300)
        .with_soft_ttl(Duration::from_millis(40))
        .with_authorization_recheck(300);
    let catalog = session(&server, "stale-304-token", &cache).await;

    catalog.load_table(&orders()).await.expect("cold load");
    wait_for_heads(&server, 1).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let table = catalog.load_table(&orders()).await.expect("revalidated");
    assert_eq!(table.metadata().uuid().to_string(), TABLE_UUID);
    assert_eq!(
        table_request_counts(&server).await,
        (1, 1, 1),
        "the stale lookup is exactly one conditional GET"
    );

    // 304 refreshed the soft TTL: the next lookups are fresh hits. The first
    // pays the authorization HEAD (neither the cold load nor the 304 marks
    // the principal authorized); within the recheck window the second pays
    // nothing.
    catalog
        .load_table(&orders())
        .await
        .expect("fresh after 304");
    catalog
        .load_table(&orders())
        .await
        .expect("fresh after 304");
    assert_eq!(table_request_counts(&server).await, (1, 1, 2));
}

/// Stale entry whose ETag no longer matches: the conditional `GET` returns
/// `200`, the table is reloaded through the REST client, and the ETag from
/// the revalidation response is stored without a second `HEAD` (CAT-04). The
/// next expiry revalidates against the new ETag.
#[tokio::test]
async fn stale_entry_with_changed_etag_reloads_and_stores_the_new_etag() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v2\"", None).await;
    mount_head(&server, "\"v1\"").await;
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .and(header("if-none-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(load_table_body())
                .insert_header("etag", "\"v2\""),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .and(header("if-none-match", "\"v2\""))
        .respond_with(ResponseTemplate::new(304))
        .mount(&server)
        .await;
    let cache = TableMetadataCache::new(300)
        .with_soft_ttl(Duration::from_millis(40))
        .with_authorization_recheck(300);
    let catalog = session(&server, "stale-changed-token", &cache).await;

    catalog.load_table(&orders()).await.expect("cold load");
    wait_for_heads(&server, 1).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    catalog.load_table(&orders()).await.expect("full reload");
    assert_eq!(
        table_request_counts(&server).await,
        (2, 1, 1),
        "changed ETag: conditional GET, then one unconditional reload, no extra HEAD"
    );

    // The stored ETag is now v2: the next expiry asks about v2 and gets 304.
    tokio::time::sleep(Duration::from_millis(60)).await;
    catalog.load_table(&orders()).await.expect("revalidate v2");
    let requests = server.received_requests().await.unwrap_or_default();
    let conditional: Vec<&str> = requests
        .iter()
        .filter(|r| r.url.path() == TABLE_PATH && r.method.as_str() == "GET")
        .filter_map(|r| r.headers.get("if-none-match"))
        .filter_map(|v| v.to_str().ok())
        .collect();
    assert_eq!(conditional, ["\"v1\"", "\"v2\""]);
    assert_eq!(table_request_counts(&server).await, (2, 2, 1));
}

/// `invalidate_table_all_tokens` evicts the entry of every principal, so both
/// sessions reload cold; the single-key `invalidate_table` leaves the other
/// principal's entry alone.
#[tokio::test]
async fn invalidate_table_all_tokens_evicts_every_principal() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;
    let cache = TableMetadataCache::new(300).with_authorization_recheck(300);
    let alice = session(&server, "alice-token", &cache).await;
    let bob = session(&server, "bob-token", &cache).await;

    alice.load_table(&orders()).await.expect("alice cold");
    bob.load_table(&orders()).await.expect("bob cold");
    wait_for_heads(&server, 2).await;
    assert!(cached_for(&cache, "alice-token").await);
    assert!(cached_for(&cache, "bob-token").await);
    assert_eq!(table_request_counts(&server).await.0, 2);

    // Single-key: only alice reloads.
    alice.invalidate_table(&orders()).await;
    assert!(!cached_for(&cache, "alice-token").await);
    assert!(cached_for(&cache, "bob-token").await);
    alice
        .load_table(&orders())
        .await
        .expect("alice after own eviction");
    bob.load_table(&orders()).await.expect("bob still cached");
    wait_for_heads(&server, 3).await;
    assert_eq!(table_request_counts(&server).await.0, 3);

    // All tokens: both reload.
    bob.invalidate_table_all_tokens(&orders()).await;
    assert!(!cached_for(&cache, "alice-token").await);
    assert!(!cached_for(&cache, "bob-token").await);
    alice
        .load_table(&orders())
        .await
        .expect("alice after commit");
    bob.load_table(&orders()).await.expect("bob after commit");
    wait_for_heads(&server, 5).await;
    assert_eq!(table_request_counts(&server).await.0, 5);
}

/// A commit that lands while a cold load is awaiting Polaris must not be
/// overwritten by that load's (pre-commit) result: the next lookup goes back
/// to the catalog instead of hitting the cache.
#[tokio::test]
async fn cold_load_racing_an_invalidation_does_not_populate_the_cache() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", Some(Duration::from_millis(300))).await;
    mount_head(&server, "\"v1\"").await;
    let cache = TableMetadataCache::new(300).with_authorization_recheck(300);
    let catalog = Arc::new(session(&server, "race-cold-token", &cache).await);

    let loader = {
        let catalog = Arc::clone(&catalog);
        tokio::spawn(async move { catalog.load_table(&orders()).await })
    };
    // Land the commit strictly inside the delayed GET.
    tokio::time::sleep(Duration::from_millis(100)).await;
    catalog.invalidate_table_all_tokens(&orders()).await;
    loader
        .await
        .unwrap()
        .expect("the racing load still succeeds");

    assert!(
        !cached_for(&cache, "race-cold-token").await,
        "the fenced insert was refused"
    );
    catalog.load_table(&orders()).await.expect("reload");
    assert_eq!(
        table_request_counts(&server).await.0,
        2,
        "the lookup after the race is a catalog round trip, not a cache hit"
    );
}

/// The same race on the 304 path: the revalidation must not stamp an evicted
/// entry fresh. The caller still gets the (pre-commit) table it asked for,
/// but nothing is left in the cache for the next reader.
#[tokio::test]
async fn revalidation_304_racing_an_invalidation_does_not_resurrect_the_entry() {
    let server = MockServer::start().await;
    mount_config(&server).await;
    mount_load_table(&server, "\"v1\"", None).await;
    mount_head(&server, "\"v1\"").await;
    Mock::given(method("GET"))
        .and(path(TABLE_PATH))
        .and(header_exists("if-none-match"))
        .respond_with(ResponseTemplate::new(304).set_delay(Duration::from_millis(300)))
        .mount(&server)
        .await;
    let cache = TableMetadataCache::new(300)
        .with_soft_ttl(Duration::from_millis(40))
        .with_authorization_recheck(300);
    let catalog = Arc::new(session(&server, "race-304-token", &cache).await);

    catalog.load_table(&orders()).await.expect("cold load");
    wait_for_heads(&server, 1).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let revalidator = {
        let catalog = Arc::clone(&catalog);
        tokio::spawn(async move { catalog.load_table(&orders()).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    catalog.invalidate_table_all_tokens(&orders()).await;
    revalidator.await.unwrap().expect("304 path still answers");
    assert_eq!(table_request_counts(&server).await, (1, 1, 1));

    assert!(
        !cached_for(&cache, "race-304-token").await,
        "revalidate did not undo the eviction"
    );
    catalog.load_table(&orders()).await.expect("reload");
    assert_eq!(table_request_counts(&server).await.0, 2);
}

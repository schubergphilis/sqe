//! Sync-from-async catalog surfaces under concurrency on a small
//! multi-thread runtime (architecture review 313).
//!
//! `worker_threads = 2` with eight concurrent callers is where
//! `block_in_place` starvation would show: every bridged call parks a worker,
//! and with two workers a bridge that needed a third task to make progress
//! would wedge. The mock answers slowly (200 ms) so the eight calls genuinely
//! overlap, and every test is bounded by `tokio::time::timeout` so a
//! regression fails instead of hanging the suite.

use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use sqe_catalog::schema_provider::SqeSchemaProvider;
use sqe_catalog::{SessionCatalog, SqeCatalogProvider, TableMetadataCache};
use sqe_core::config::StorageConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const WAREHOUSE: &str = "test-warehouse";
const SLOW: Duration = Duration::from_millis(200);
const BOUND: Duration = Duration::from_secs(20);

async fn slow_polaris(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "overrides": {"prefix": WAREHOUSE},
            "defaults": {}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/test-warehouse/namespaces"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"namespaces": [["sales"]]}))
                .set_delay(SLOW),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/test-warehouse/namespaces/sales/tables"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "identifiers": [
                        {"namespace": ["sales"], "name": "orders"},
                        {"namespace": ["sales"], "name": "customers"}
                    ]
                }))
                .set_delay(SLOW),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/test-warehouse/namespaces/sales/views"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"identifiers": []}))
                .set_delay(SLOW),
        )
        .mount(server)
        .await;
}

async fn session(server: &MockServer, token: &str) -> Arc<SessionCatalog> {
    Arc::new(
        SessionCatalog::new(
            &server.uri(),
            WAREHOUSE,
            token,
            &StorageConfig::default(),
            Some(TableMetadataCache::new(300)),
            None,
            None,
            false,
        )
        .await
        .expect("construct session catalog"),
    )
}

/// Eight tasks each refresh the namespace listing against the slow mock and
/// then read `table_names()`; all eight finish inside the bound and every
/// one sees the full list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_concurrent_table_names_refreshes_complete_on_two_workers() {
    let server = MockServer::start().await;
    slow_polaris(&server).await;
    let catalog = session(&server, "concurrency-table-names-token").await;
    let provider = Arc::new(SqeSchemaProvider::new(
        Arc::clone(&catalog),
        "sales".to_string(),
        StorageConfig::default(),
        WAREHOUSE.to_string(),
    ));

    let started = std::time::Instant::now();
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let provider = Arc::clone(&provider);
            tokio::spawn(async move {
                provider
                    .refresh_table_names()
                    .await
                    .expect("refresh against the mock");
                provider.table_names()
            })
        })
        .collect();
    let results = tokio::time::timeout(BOUND, futures::future::join_all(tasks))
        .await
        .expect("eight concurrent table_names calls did not finish inside the bound");

    for result in results {
        let mut names = result.expect("task did not panic");
        names.sort();
        assert_eq!(names, ["customers", "orders"]);
    }
    assert!(
        started.elapsed() < BOUND,
        "elapsed {:?} exceeds the bound",
        started.elapsed()
    );
    assert!(provider.table_exist("orders"));
    assert!(!provider.table_exist("missing"));
}

/// The real sync bridge: `CatalogProvider::schema` on a name outside the
/// cached namespace set re-lists through `block_on_compat` (`block_in_place`
/// on this runtime). Eight callers on two workers must all return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_concurrent_schema_lookups_bridge_without_starving_two_workers() {
    let server = MockServer::start().await;
    slow_polaris(&server).await;
    let catalog = session(&server, "concurrency-schema-token").await;
    let provider = Arc::new(
        SqeCatalogProvider::try_new_with_options(
            catalog,
            StorageConfig::default(),
            WAREHOUSE.to_string(),
            None,
            None,
            false,
            false,
        )
        .await
        .expect("catalog provider lists namespaces"),
    );
    assert!(provider.schema("sales").is_some());

    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let provider = Arc::clone(&provider);
            tokio::spawn(async move {
                (
                    provider.schema("not_a_namespace").is_none(),
                    provider.schema("sales").is_some(),
                )
            })
        })
        .collect();
    let results = tokio::time::timeout(BOUND, futures::future::join_all(tasks))
        .await
        .expect("eight concurrent schema lookups did not finish inside the bound");

    for result in results {
        assert_eq!(result.expect("task did not panic"), (true, true));
    }
}

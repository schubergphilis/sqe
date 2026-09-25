//! Architecture review 300: every Polaris request carries a deadline.
//!
//! A catalog that accepts the connection and then goes silent must fail the
//! call within `request_timeout_secs`, surface as `CatalogUnavailable`, and
//! count as a circuit-breaker failure. Before this, the default reqwest client
//! had no timeout, so the call hung and the breaker never saw a failure.

use std::sync::Arc;
use std::time::{Duration, Instant};

use iceberg::{NamespaceIdent, TableIdent};
use sqe_catalog::circuit_breaker::CircuitBreaker;
use sqe_catalog::rest_catalog::build_catalog_http_client;
use sqe_catalog::SessionCatalog;
use sqe_core::config::StorageConfig;
use sqe_core::SqeErrorCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "http-timeout-regression-token";
const TIMEOUT_SECS: u64 = 1;

fn orders() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orders".to_string(),
    )
}

#[tokio::test]
async fn silent_catalog_head_times_out_as_unavailable_and_trips_breaker() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/v1/test-warehouse/namespaces/sales/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(TIMEOUT_SECS + 4)))
        .mount(&server)
        .await;

    // Threshold 1 so a single timeout is observable as an Open breaker.
    let breaker = Arc::new(CircuitBreaker::new(
        "http-timeout-test",
        1,
        Duration::from_secs(60),
    ));
    let client = build_catalog_http_client(TIMEOUT_SECS, TIMEOUT_SECS);
    let catalog = SessionCatalog::new(
        &server.uri(),
        "test-warehouse",
        TOKEN,
        &StorageConfig::default(),
        None,
        Some(client),
        Some(Arc::clone(&breaker)),
        false,
    )
    .await
    .expect("construct session catalog");

    let started = Instant::now();
    let err = catalog
        .authorize_table_access(&orders())
        .await
        .expect_err("a silent catalog must not authorize");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(TIMEOUT_SECS + 1),
        "call must return within timeout + 1s, took {elapsed:?}"
    );
    assert_eq!(
        err.error_code(),
        SqeErrorCode::CatalogUnavailable,
        "timeout must classify as CatalogUnavailable, got {err}"
    );
    assert_eq!(
        breaker.state_code(),
        2,
        "the breaker must record the timeout as a failure and open"
    );
}

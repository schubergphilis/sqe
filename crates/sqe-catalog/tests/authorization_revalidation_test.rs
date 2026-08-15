//! Regression for out-of-band Ranger revocation on cached SQE reads.
//!
//! Polaris authorizes HEAD and GET loadTable identically. SQE uses HEAD at
//! the metadata/result-cache seam so a grant change is observable without
//! downloading table metadata or disabling either cache.

use iceberg::{NamespaceIdent, TableIdent};
use sqe_catalog::SessionCatalog;
use sqe_core::config::StorageConfig;
use sqe_core::SqeErrorCode;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "cache-authz-regression-token";

fn load_table_path() -> &'static str {
    "/v1/test-warehouse/namespaces/sales/tables/orders"
}

async fn session(server: &MockServer) -> SessionCatalog {
    SessionCatalog::new(
        &server.uri(),
        "test-warehouse",
        TOKEN,
        &StorageConfig::default(),
        None,
        None,
        None,
    )
    .await
    .expect("construct session catalog")
}

fn orders() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orders".to_string(),
    )
}

#[tokio::test]
async fn cached_read_revalidation_observes_revoke_with_same_token() {
    let server = MockServer::start().await;
    let catalog = session(&server).await;

    Mock::given(method("HEAD"))
        .and(path(load_table_path()))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    catalog
        .authorize_table_access(&orders())
        .await
        .expect("grant should authorize cached read");

    // Same SessionCatalog and same bearer token: only the server-side policy
    // changes, matching an out-of-band Ranger REVOKE.
    server.reset().await;
    Mock::given(method("HEAD"))
        .and(path(load_table_path()))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
        .await;

    let denied = catalog
        .authorize_table_access(&orders())
        .await
        .expect_err("revoked cached read must be denied");
    assert_eq!(denied.error_code(), SqeErrorCode::AccessDenied);
}

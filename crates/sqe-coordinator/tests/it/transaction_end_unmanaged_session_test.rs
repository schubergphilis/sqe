//! COMMIT / ROLLBACK for a session the session manager does not hold.
//!
//! In hybrid mode the handler carries a session manager for Flight SQL, but a
//! Trino HTTP request builds its session per request from headers: the
//! transaction rides in `X-Trino-Transaction-Id` and the manager has never
//! seen that session. `end_transaction` published (or dropped) the branch and
//! then failed to clear a stored transaction that did not exist, so a COMMIT
//! that had succeeded came back as "session not found in manager". Ending a
//! transaction must not depend on a stored session; starting one still does.

use std::sync::Arc;

use chrono::{Duration, Utc};
use sqe_auth::{AnonymousProvider, AnonymousProviderConfig};
use sqe_coordinator::{
    query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry, SessionManager,
};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::PassthroughEnforcer;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_empty_polaris() -> MockServer {
    let server = crate::common::mock_server().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

/// A handler wired the way `sqe-server` wires it in hybrid mode: with the
/// session manager Flight SQL authenticates through.
fn hybrid_handler(catalog_url: &str) -> QueryHandler {
    let toml = format!("[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n");
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let provider = Arc::new(AnonymousProvider::new(AnonymousProviderConfig::default()));
    let manager = Arc::new(SessionManager::with_provider(provider));
    QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None, // policy_store
        config,
        None, // worker_registry
        None, // credential_tracker
        None, // metrics
        None, // audit
        tracker,
        None, // query_cache
        None, // grant_backend
        None, // lineage
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_session_manager(manager)
}

/// A per-request session, as the Trino HTTP layer builds one; never stored.
fn http_session(transaction_id: Option<String>) -> Session {
    Session::new(
        "txn_user".to_string(),
        sqe_core::SecretString::new("token".to_string()),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    )
    .with_transaction_id(transaction_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_and_rollback_succeed_for_a_session_the_manager_does_not_hold() {
    let polaris = mount_empty_polaris().await;
    let handler = hybrid_handler(&polaris.uri());
    for sql in ["COMMIT", "ROLLBACK"] {
        let session = http_session(Some(sqe_core::txn_id::new_txn_id()));
        let result = handler.execute(&session, sql, None).await;
        assert!(
            result.is_ok(),
            "{sql} on an HTTP session failed: {result:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn begin_still_needs_a_stored_session() {
    // Flight SQL BEGIN stores the transaction on the session; if the session
    // is gone, silently ignoring BEGIN would run later DML autocommit.
    let polaris = mount_empty_polaris().await;
    let handler = hybrid_handler(&polaris.uri());
    let result = handler.execute(&http_session(None), "BEGIN", None).await;
    let err = result.expect_err("BEGIN without a stored session must fail");
    assert!(
        err.to_string().contains("session not found in manager"),
        "unexpected error: {err}"
    );
}

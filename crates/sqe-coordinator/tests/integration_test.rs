//! Integration tests for SQE coordinator.
//! These tests require a running quickstart stack (Keycloak, Polaris, S3).
//! Run with: cargo test -p sqe-coordinator --test integration_test -- --ignored

use std::sync::Arc;

// Test: Authenticate against Keycloak
#[tokio::test]
#[ignore] // Requires running quickstart stack
async fn test_keycloak_authentication() {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Authentication failed");
    assert!(
        !session.access_token.is_empty(),
        "Access token should not be empty"
    );
    assert_eq!(session.user.username, "root");
}

// Test: Different users get different sessions
#[tokio::test]
#[ignore]
async fn test_different_users_get_different_sessions() {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    let session1 = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed for root");
    let session2 = authenticator
        .authenticate("testuser", "testuser123")
        .await
        .expect("Auth failed for testuser");

    assert_ne!(session1.id, session2.id);
    assert_ne!(session1.access_token, session2.access_token);
    assert_eq!(session1.user.username, "root");
    assert_eq!(session2.user.username, "testuser");
}

// Test: Token fingerprint changes with different tokens
#[tokio::test]
#[ignore]
async fn test_token_fingerprint() {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    let session1 = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed");
    let session2 = authenticator
        .authenticate("testuser", "testuser123")
        .await
        .expect("Auth failed");

    let fp1 = session1.token_fingerprint();
    let fp2 = session2.token_fingerprint();
    assert_ne!(
        fp1, fp2,
        "Different users should have different token fingerprints"
    );
    assert!(
        fp1.starts_with("root-"),
        "Fingerprint should start with username"
    );
}

// Test: Query handler executes SELECT 1
#[tokio::test]
#[ignore]
async fn test_simple_select() {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed");

    // SELECT 1 goes through the full query pipeline including catalog registration
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let handler = sqe_coordinator::QueryHandler::new(policy, config);

    let batches = handler
        .execute(&session, "SELECT 1")
        .await
        .expect("SELECT 1 should succeed");

    assert!(!batches.is_empty(), "Should return at least one batch");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "SELECT 1 should return exactly one row");
}

// Test: SQL classification works correctly
#[test]
fn test_sql_classification() {
    use sqe_sql::{parse_and_classify, StatementKind};

    assert!(matches!(
        parse_and_classify("SELECT 1"),
        Ok(StatementKind::Query(_))
    ));
    assert!(matches!(
        parse_and_classify("CREATE TABLE foo AS SELECT 1"),
        Ok(StatementKind::Ctas(_))
    ));
    assert!(matches!(
        parse_and_classify("INSERT INTO foo SELECT 1"),
        Ok(StatementKind::Insert(_))
    ));
    assert!(matches!(
        parse_and_classify("DELETE FROM foo WHERE id = 1"),
        Ok(StatementKind::Delete(_))
    ));
}

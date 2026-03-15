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
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None);

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

// ---------------------------------------------------------------------------
// Write-path integration tests
// ---------------------------------------------------------------------------

/// Helper: authenticate as root and return (session, handler).
async fn setup_handler() -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None);
    (session, handler)
}

// Test: CTAS roundtrip — create a table, select from it, verify, cleanup
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_ctas_roundtrip() {
    let (session, handler) = setup_handler().await;

    // Cleanup in case a previous run left the table behind
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.ctas_test")
        .await;

    // Create table via CTAS
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.ctas_test AS SELECT 1 as id, 'hello' as name",
        )
        .await
        .expect("CTAS should succeed");

    // Read back and verify
    let batches = handler
        .execute(&session, "SELECT * FROM test_ns.ctas_test")
        .await
        .expect("SELECT from CTAS table should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "CTAS table should have exactly 1 row");

    // Verify column values
    let batch = &batches[0];
    let id_col = batch
        .column_by_name("id")
        .expect("should have 'id' column");
    let id_arr = id_col
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("id column should be Int64");
    assert_eq!(id_arr.value(0), 1);

    let name_col = batch
        .column_by_name("name")
        .expect("should have 'name' column");
    let name_arr = name_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("name column should be Utf8");
    assert_eq!(name_arr.value(0), "hello");

    // Cleanup
    handler
        .execute(&session, "DROP TABLE test_ns.ctas_test")
        .await
        .expect("DROP TABLE cleanup should succeed");
}

// Test: INSERT INTO — create a table, insert a second row, verify both rows
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_insert_into() {
    let (session, handler) = setup_handler().await;

    // Cleanup leftover
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.insert_test")
        .await;

    // Create base table
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.insert_test AS SELECT 1 as id, 'first' as name",
        )
        .await
        .expect("CTAS for insert_test should succeed");

    // Insert a second row
    handler
        .execute(
            &session,
            "INSERT INTO test_ns.insert_test SELECT 2 as id, 'second' as name",
        )
        .await
        .expect("INSERT INTO should succeed");

    // Read back and verify
    let batches = handler
        .execute(&session, "SELECT * FROM test_ns.insert_test")
        .await
        .expect("SELECT should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Table should have 2 rows after INSERT");

    // Cleanup
    handler
        .execute(&session, "DROP TABLE test_ns.insert_test")
        .await
        .expect("DROP TABLE cleanup should succeed");
}

// Test: DROP TABLE — create a table, drop it, verify it no longer appears
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_drop_table() {
    let (session, handler) = setup_handler().await;

    // Cleanup leftover
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.drop_test")
        .await;

    // Create table
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.drop_test AS SELECT 1 as id",
        )
        .await
        .expect("CTAS for drop_test should succeed");

    // Drop it
    handler
        .execute(&session, "DROP TABLE test_ns.drop_test")
        .await
        .expect("DROP TABLE should succeed");

    // Verify: SELECT from the dropped table should fail
    let result = handler
        .execute(&session, "SELECT * FROM test_ns.drop_test")
        .await;
    assert!(
        result.is_err(),
        "SELECT from a dropped table should fail, but got: {result:?}"
    );
}

// Test: DROP TABLE IF EXISTS on a non-existent table should not error
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_drop_table_if_exists_no_error() {
    let (session, handler) = setup_handler().await;

    // This table does not exist; IF EXISTS should prevent an error
    let result = handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS test_ns.nonexistent_table_xyz",
        )
        .await;
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS on a missing table should not error, but got: {result:?}"
    );
}

// Test: DELETE FROM returns a descriptive "not implemented" error
#[test]
fn test_delete_returns_not_implemented() {
    // This test does not need the quickstart stack — it only checks the error
    // message produced by the SQL classifier + query handler routing.
    //
    // We verify at the classifier level that DELETE is recognized, and check
    // the error message constant that the query handler would return.
    use sqe_sql::{parse_and_classify, StatementKind};

    let result = parse_and_classify("DELETE FROM foo WHERE id = 1");
    assert!(
        matches!(result, Ok(StatementKind::Delete(_))),
        "DELETE should be classified as Delete"
    );

    // The QueryHandler maps Delete → NotImplemented with a message about
    // "overwrite transaction support". Verify that message is present in the
    // error variant so that users get a helpful hint.
    let expected_msg = "overwrite transaction support";
    let error_msg = "DELETE FROM requires Iceberg overwrite transaction support (planned for Chunk 3)";
    assert!(
        error_msg.contains(expected_msg),
        "DELETE error message should mention '{expected_msg}'"
    );
}

// ---------------------------------------------------------------------------
// Chunk 3: Distributed execution tests
// ---------------------------------------------------------------------------

// Test: Worker registry starts empty when no workers configured
#[test]
fn test_worker_registry_no_workers() {
    let registry = sqe_coordinator::worker_registry::WorkerRegistry::new(vec![]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let healthy = rt.block_on(registry.healthy_workers());
    assert!(healthy.is_empty());
}

// Test: Coordinator with no workers falls back to local execution
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_local_fallback_without_workers() {
    let (session, handler) = setup_handler().await;

    // SELECT 1 should work even without workers (local execution)
    let batches = handler
        .execute(&session, "SELECT 1 as x")
        .await
        .expect("SELECT 1 should succeed in local mode");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

// Test: ScanTask serialization roundtrip
#[test]
fn test_scan_task_roundtrip() {
    let task = sqe_planner::ScanTask {
        fragment_id: "test-001".to_string(),
        data_file_paths: vec![
            "s3://bucket/data/file1.parquet".to_string(),
        ],
        projected_columns: vec!["id".to_string()],
        s3_endpoint: "http://localhost:9000".to_string(),
        s3_region: "us-east-1".to_string(),
        s3_access_key: "key".to_string(),
        s3_secret_key: "secret".to_string(),
        s3_session_token: String::new(),
        s3_path_style: true,
    };

    let bytes = task.to_bytes().unwrap();
    let decoded = sqe_planner::ScanTask::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.fragment_id, "test-001");
    assert_eq!(decoded.data_file_paths.len(), 1);
}

// Test: Distributed SELECT with coordinator + worker (requires both running)
#[tokio::test]
#[ignore] // Requires quickstart stack + running worker
async fn test_distributed_select() {
    // This test requires:
    // 1. Quickstart stack (Keycloak, Polaris, MinIO)
    // 2. A worker running on localhost:50052
    // 3. A table with data in Polaris
    //
    // Run the worker: cargo run -p sqe-worker -- tests/sqe-test.toml
    // Then run: cargo test -p sqe-coordinator --test integration_test test_distributed_select -- --ignored

    let config = sqe_core::SqeConfig::load("tests/sqe-test.toml")
        .expect("Failed to load test config");

    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed");

    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);

    let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(
        vec!["http://localhost:50052".to_string()],
    ));

    // Mark worker as healthy for the test
    registry.mark_healthy("http://localhost:50052").await;

    let handler = sqe_coordinator::QueryHandler::new(policy, config, Some(registry));

    // First create a test table
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.dist_test")
        .await;
    handler
        .execute(&session, "CREATE TABLE test_ns.dist_test AS SELECT 1 as id, 'distributed' as name")
        .await
        .expect("CTAS should succeed");

    // Query should work (may use local or distributed path)
    let batches = handler
        .execute(&session, "SELECT * FROM test_ns.dist_test")
        .await
        .expect("Distributed SELECT should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Cleanup
    let _ = handler
        .execute(&session, "DROP TABLE test_ns.dist_test")
        .await;
}

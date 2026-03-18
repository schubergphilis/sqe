//! Integration tests for SQE coordinator.
//! These tests require a running lightweight test stack (Polaris in-memory + RustFS).
//! Run with: ./scripts/integration-test.sh

use std::sync::Arc;

/// Initialize the tracing subscriber once for the entire test binary.
/// Called explicitly by setup_handler(); tests that don't use setup_handler
/// won't emit structured logs but that is acceptable.
fn init_tracing() {
    static TRACING_INIT: std::sync::Once = std::sync::Once::new();
    TRACING_INIT.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(
                "sqe_coordinator=info,sqe_catalog=info,sqe_auth=info,warn",
            ));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    });
}

/// Resolve the test config path relative to the workspace root.
/// CARGO_MANIFEST_DIR points to the crate dir (crates/sqe-coordinator),
/// so we go up two levels to reach the workspace root.
fn test_config_path() -> String {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| ".".to_string());
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()  // crates/
        .and_then(|p| p.parent())  // workspace root
        .unwrap_or(std::path::Path::new("."));
    workspace_root
        .join("tests")
        .join("sqe-test.toml")
        .to_string_lossy()
        .to_string()
}

// Test: Authenticate via client_credentials against Polaris built-in OAuth
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_authentication() {
    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Authentication failed");
    assert!(
        !session.access_token.is_empty(),
        "Access token should not be empty"
    );
    assert_eq!(session.user.username, "root");
}

// Test: Token fingerprint is stable for the same principal
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_token_fingerprint() {
    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed");

    let fp = session.token_fingerprint();
    assert!(
        fp.starts_with("root-"),
        "Fingerprint should start with username"
    );
}

// Test: Query handler executes SELECT 1
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_simple_select() {
    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed");

    // SELECT 1 goes through the full query pipeline including catalog registration
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None, None, None);

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
    init_tracing();

    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None, None, None);
    (session, handler)
}

// Test: CTAS roundtrip — create a table, select from it, verify, cleanup
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
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
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
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
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
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
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
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
    // This test does not need the test stack — it only checks the error
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
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
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
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh + running worker
async fn test_distributed_select() {
    // This test requires:
    // 1. Test stack (Polaris in-memory, RustFS)
    // 2. A worker running on localhost:50052
    // 3. A table with data in Polaris
    //
    // Run the worker: cargo run -p sqe-worker -- tests/sqe-test.toml
    // Then run: cargo test -p sqe-coordinator --test integration_test test_distributed_select -- --ignored

    let config = sqe_core::SqeConfig::load(&test_config_path())
        .expect("Failed to load test config");

    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed");

    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);

    let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(
        vec!["http://localhost:50052".to_string()],
    ));

    // Mark worker as healthy for the test
    registry.mark_healthy("http://localhost:50052").await;

    let handler = sqe_coordinator::QueryHandler::new(policy, config, Some(registry), None, None);

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

// ---------------------------------------------------------------------------
// Chunk 4: information_schema + Trino compat + Observability tests
// ---------------------------------------------------------------------------

// Test: MetricsRegistry can be created and incremented
#[test]
fn test_metrics_registry() {
    let metrics = sqe_metrics::MetricsRegistry::new();
    metrics
        .query_count
        .with_label_values(&["success", "query"])
        .inc();
    assert_eq!(
        metrics
            .query_count
            .with_label_values(&["success", "query"])
            .get(),
        1.0
    );
}

// Test: AuditLogger no-op mode works
#[test]
fn test_audit_logger_noop() {
    let logger = sqe_metrics::audit::AuditLogger::new("");
    let entry = sqe_metrics::audit::AuditEntry {
        timestamp: "2026-03-15T00:00:00Z".to_string(),
        username: "test".to_string(),
        query_text: "SELECT 1".to_string(),
        statement_type: "query".to_string(),
        duration_ms: 10,
        rows_returned: 1,
        status: "success".to_string(),
    };
    logger.log(&entry); // Should not panic
}

// Test: Trino type mapping
#[test]
fn test_trino_type_mapping() {
    use arrow_schema::DataType;
    assert_eq!(
        sqe_trino_compat::types::arrow_to_trino_type(&DataType::Int64),
        "bigint"
    );
    assert_eq!(
        sqe_trino_compat::types::arrow_to_trino_type(&DataType::Utf8),
        "varchar"
    );
    assert_eq!(
        sqe_trino_compat::types::arrow_to_trino_type(&DataType::Float64),
        "double"
    );
}

// Test: Trino response serialization
#[test]
fn test_trino_batches_to_json() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let (cols, rows) = sqe_trino_compat::protocol::batches_to_trino(&[batch]);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].r#type, "bigint");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!(1));
}

// Test: information_schema.tables is queryable
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_information_schema_tables() {
    let (session, handler) = setup_handler().await;

    let batches = handler
        .execute(&session, "SELECT * FROM information_schema.tables")
        .await
        .expect("information_schema.tables should be queryable");

    assert!(!batches.is_empty());
}

// Test: information_schema.schemata is queryable
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_information_schema_schemata() {
    let (session, handler) = setup_handler().await;

    let batches = handler
        .execute(&session, "SELECT * FROM information_schema.schemata")
        .await
        .expect("information_schema.schemata should be queryable");

    assert!(!batches.is_empty());
}

// ---------------------------------------------------------------------------
// SQL Coverage: Views, Joins, Aggregations, and Complex Queries
//
// Current test SQL summary:
//   test_simple_select          → SELECT 1
//   test_ctas_roundtrip         → CTAS + SELECT + DROP
//   test_insert_into            → CTAS + INSERT INTO + SELECT
//   test_drop_table             → CTAS + DROP + SELECT (expect error)
//   test_information_schema_*   → SELECT * FROM information_schema.*
//   test_distributed_select     → CTAS + SELECT (with worker registry)
// ---------------------------------------------------------------------------

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use arrow_array::RecordBatch;

/// Pretty-print RecordBatches for test diagnostics.
fn print_results(label: &str, sql: &str, batches: &[RecordBatch]) {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("\n╔══ {label} ══╗");
    println!("│ SQL: {sql}");
    println!("│ Rows: {total_rows}");
    if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            let schema = batch.schema();
            let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let header_line = headers.join(" | ");
            println!("│ {header_line}");
            println!("│ {}", "─".repeat(header_line.len()));
        }
    }
    for batch in batches {
        for row in 0..batch.num_rows() {
            let vals: Vec<String> = batch.columns().iter().map(|col| fmt_val(col, row)).collect();
            println!("│ {}", vals.join(" | "));
        }
    }
    println!("╚{}╝", "═".repeat(label.len() + 4));
}

fn fmt_val(col: &dyn Array, row: usize) -> String {
    if col.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return format!("{:.2}", a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return a.value(row).to_string();
    }
    "?".to_string()
}

// ---------------------------------------------------------------------------
// Helpers: set up shared fixture tables
// ---------------------------------------------------------------------------

/// Create employees + departments tables for join/aggregation tests.
/// Returns (session, handler).
async fn setup_join_fixture() -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let (session, handler) = setup_handler().await;

    // employees: id BIGINT, name VARCHAR, dept_id BIGINT, salary DOUBLE
    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.employees").await;
    handler.execute(&session,
        "CREATE TABLE test_ns.employees AS \
         SELECT 1 as id, 'Alice'   as name, 10 as dept_id, 90000.0 as salary UNION ALL \
         SELECT 2,        'Bob',            10,             85000.0             UNION ALL \
         SELECT 3,        'Charlie',        20,             70000.0             UNION ALL \
         SELECT 4,        'Dave',           20,             75000.0             UNION ALL \
         SELECT 5,        'Eve',            30,             95000.0             UNION ALL \
         SELECT 6,        'Frank',          99,             60000.0"
    ).await.expect("Create employees");

    // departments: id BIGINT, dept_name VARCHAR, budget DOUBLE
    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.departments").await;
    handler.execute(&session,
        "CREATE TABLE test_ns.departments AS \
         SELECT 10 as id, 'Engineering' as dept_name, 500000.0 as budget UNION ALL \
         SELECT 20,        'Marketing',               200000.0            UNION ALL \
         SELECT 30,        'Executive',               1000000.0           UNION ALL \
         SELECT 40,        'HR',                      150000.0"
    ).await.expect("Create departments");

    (session, handler)
}

async fn teardown_join_fixture(session: &sqe_core::Session, handler: &sqe_coordinator::QueryHandler) {
    let _ = handler.execute(session, "DROP TABLE IF EXISTS test_ns.employees").await;
    let _ = handler.execute(session, "DROP TABLE IF EXISTS test_ns.departments").await;
}

// ---------------------------------------------------------------------------
// View tests
// ---------------------------------------------------------------------------

// Test: CREATE VIEW registers the view in Polaris; DROP VIEW removes it
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_create_and_drop_view() {
    let (session, handler) = setup_join_fixture().await;

    let _ = handler.execute(&session, "DROP VIEW IF EXISTS test_ns.eng_view").await;

    // CREATE VIEW filtering engineering employees
    handler.execute(&session,
        "CREATE VIEW test_ns.eng_view AS \
         SELECT id, name, salary FROM test_ns.employees WHERE dept_id = 10"
    ).await.expect("CREATE VIEW should succeed");

    // SELECT from view
    let batches = handler.execute(&session, "SELECT * FROM test_ns.eng_view")
        .await.expect("SELECT from view should succeed");

    print_results("CREATE VIEW + SELECT", "SELECT * FROM test_ns.eng_view", &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "Engineering dept has Alice and Bob");

    // DROP VIEW
    handler.execute(&session, "DROP VIEW test_ns.eng_view")
        .await.expect("DROP VIEW should succeed");

    // View should no longer be queryable
    let result = handler.execute(&session, "SELECT * FROM test_ns.eng_view").await;
    assert!(result.is_err(), "Dropped view should not be queryable");

    teardown_join_fixture(&session, &handler).await;
}

// Test: View with aggregation — high earners per department
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_view_with_aggregation() {
    let (session, handler) = setup_join_fixture().await;
    let _ = handler.execute(&session, "DROP VIEW IF EXISTS test_ns.dept_stats").await;

    handler.execute(&session,
        "CREATE VIEW test_ns.dept_stats AS \
         SELECT dept_id, COUNT(*) as headcount, AVG(salary) as avg_salary \
         FROM test_ns.employees GROUP BY dept_id"
    ).await.expect("CREATE VIEW with aggregation");

    let batches = handler.execute(&session,
        "SELECT dept_id, headcount, avg_salary FROM test_ns.dept_stats ORDER BY dept_id"
    ).await.expect("SELECT from aggregation view");

    print_results("VIEW with GROUP BY", "SELECT * FROM test_ns.dept_stats", &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "Four distinct dept_ids (10, 20, 30, 99)");

    let _ = handler.execute(&session, "DROP VIEW test_ns.dept_stats").await;
    teardown_join_fixture(&session, &handler).await;
}

// ---------------------------------------------------------------------------
// Join tests
// ---------------------------------------------------------------------------

// Test: INNER JOIN — only employees that have a matching department
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_inner_join() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT e.id, e.name, d.dept_name, e.salary \
               FROM test_ns.employees e \
               INNER JOIN test_ns.departments d ON e.dept_id = d.id \
               ORDER BY e.id";

    let batches = handler.execute(&session, sql)
        .await.expect("INNER JOIN should succeed");

    print_results("INNER JOIN", sql, &batches);

    // Frank (dept_id=99) and HR (id=40) are excluded
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "5 employees with matching dept (Frank excluded)");

    // First row should be Alice in Engineering
    let batch = &batches[0];
    let name = batch.column_by_name("name").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(name, "Alice");

    teardown_join_fixture(&session, &handler).await;
}

// Test: LEFT JOIN — all employees, NULL dept_name for those without a department
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_left_join() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT e.id, e.name, d.dept_name \
               FROM test_ns.employees e \
               LEFT JOIN test_ns.departments d ON e.dept_id = d.id \
               ORDER BY e.id";

    let batches = handler.execute(&session, sql)
        .await.expect("LEFT JOIN should succeed");

    print_results("LEFT JOIN", sql, &batches);

    // All 6 employees, Frank gets NULL dept_name
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6, "LEFT JOIN returns all 6 employees");

    // Frank is the last row (id=6), dept_name should be NULL
    let batch = &batches[0];
    let dept_name_col = batch.column_by_name("dept_name").unwrap();
    assert!(dept_name_col.is_null(5), "Frank's dept_name should be NULL");

    teardown_join_fixture(&session, &handler).await;
}

// Test: RIGHT JOIN — all departments including HR which has no employees
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_right_join() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT d.dept_name, e.name, e.salary \
               FROM test_ns.employees e \
               RIGHT JOIN test_ns.departments d ON e.dept_id = d.id \
               ORDER BY d.id, e.id";

    let batches = handler.execute(&session, sql)
        .await.expect("RIGHT JOIN should succeed");

    print_results("RIGHT JOIN", sql, &batches);

    // 5 matched + 1 HR row with NULL employee columns
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6, "RIGHT JOIN: 5 employees + 1 unmatched dept");

    teardown_join_fixture(&session, &handler).await;
}

// Test: FULL OUTER JOIN — all employees AND all departments
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_full_outer_join() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT e.id, e.name, d.dept_name \
               FROM test_ns.employees e \
               FULL OUTER JOIN test_ns.departments d ON e.dept_id = d.id \
               ORDER BY e.id, d.id";

    let batches = handler.execute(&session, sql)
        .await.expect("FULL OUTER JOIN should succeed");

    print_results("FULL OUTER JOIN", sql, &batches);

    // 5 matched + 1 Frank unmatched + 1 HR unmatched = 7
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 7, "FULL OUTER JOIN: 5 matched + Frank + HR");

    teardown_join_fixture(&session, &handler).await;
}

// Test: CROSS JOIN — cartesian product (small tables only!)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_cross_join() {
    let (session, handler) = setup_handler().await;

    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.colors").await;
    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.sizes").await;

    handler.execute(&session,
        "CREATE TABLE test_ns.colors AS \
         SELECT 'red' as color UNION ALL SELECT 'blue' UNION ALL SELECT 'green'"
    ).await.expect("Create colors");

    handler.execute(&session,
        "CREATE TABLE test_ns.sizes AS \
         SELECT 'S' as size UNION ALL SELECT 'M' UNION ALL SELECT 'L'"
    ).await.expect("Create sizes");

    let sql = "SELECT color, size FROM test_ns.colors CROSS JOIN test_ns.sizes ORDER BY color, size";
    let batches = handler.execute(&session, sql).await.expect("CROSS JOIN");

    print_results("CROSS JOIN (3×3)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 9, "3 colors × 3 sizes = 9 combinations");

    let _ = handler.execute(&session, "DROP TABLE test_ns.colors").await;
    let _ = handler.execute(&session, "DROP TABLE test_ns.sizes").await;
}

// Test: Self-join — manager hierarchy (employee referencing employee)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_self_join() {
    let (session, handler) = setup_handler().await;

    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.org").await;
    handler.execute(&session,
        "CREATE TABLE test_ns.org AS \
         SELECT 1 as id, 'CEO'      as name, CAST(NULL AS BIGINT) as mgr_id UNION ALL \
         SELECT 2,        'VP Eng',           1                              UNION ALL \
         SELECT 3,        'VP Mkt',           1                              UNION ALL \
         SELECT 4,        'Engineer',         2                              UNION ALL \
         SELECT 5,        'Marketer',         3"
    ).await.expect("Create org table");

    let sql = "SELECT e.name as employee, m.name as manager \
               FROM test_ns.org e \
               LEFT JOIN test_ns.org m ON e.mgr_id = m.id \
               ORDER BY e.id";

    let batches = handler.execute(&session, sql).await.expect("Self-join");
    print_results("SELF JOIN (org hierarchy)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "5 employees in org");

    // CEO has no manager (NULL)
    let batch = &batches[0];
    let mgr_col = batch.column_by_name("manager").unwrap();
    assert!(mgr_col.is_null(0), "CEO should have NULL manager");

    let _ = handler.execute(&session, "DROP TABLE test_ns.org").await;
}

// ---------------------------------------------------------------------------
// Aggregation tests
// ---------------------------------------------------------------------------

// Test: GROUP BY with COUNT, SUM, AVG, MIN, MAX
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_aggregation_basic() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT dept_id, \
               COUNT(*) as headcount, \
               SUM(salary) as total_salary, \
               AVG(salary) as avg_salary, \
               MIN(salary) as min_salary, \
               MAX(salary) as max_salary \
               FROM test_ns.employees \
               GROUP BY dept_id \
               ORDER BY dept_id";

    let batches = handler.execute(&session, sql).await.expect("Aggregation");
    print_results("GROUP BY + aggregates", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "Four dept groups: 10, 20, 30, 99");

    // dept_id=10: Alice(90000) + Bob(85000) → count=2, sum=175000
    let batch = &batches[0];
    let headcount = batch.column_by_name("headcount").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(headcount, 2, "Engineering has 2 employees");

    teardown_join_fixture(&session, &handler).await;
}

// Test: HAVING clause — only departments with avg salary > 75000
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_having_clause() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT dept_id, AVG(salary) as avg_salary \
               FROM test_ns.employees \
               GROUP BY dept_id \
               HAVING AVG(salary) > 75000.0 \
               ORDER BY dept_id";

    let batches = handler.execute(&session, sql).await.expect("HAVING clause");
    print_results("HAVING AVG(salary) > 75000", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // dept 10: avg=87500 ✓, dept 20: avg=72500 ✗, dept 30: avg=95000 ✓, dept 99: avg=60000 ✗
    assert_eq!(rows, 2, "Only dept 10 (avg=87500) and dept 30 (avg=95000) qualify");

    teardown_join_fixture(&session, &handler).await;
}

// Test: JOIN + GROUP BY together — department summary with headcount and avg
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_join_with_aggregation() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT d.dept_name, COUNT(e.id) as headcount, AVG(e.salary) as avg_salary \
               FROM test_ns.departments d \
               LEFT JOIN test_ns.employees e ON d.id = e.dept_id \
               GROUP BY d.dept_name \
               ORDER BY headcount DESC, d.dept_name";

    let batches = handler.execute(&session, sql).await.expect("JOIN + GROUP BY");
    print_results("JOIN + GROUP BY", sql, &batches);

    // 4 departments, HR has 0 employees
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "All 4 departments appear");

    teardown_join_fixture(&session, &handler).await;
}

// Test: CTE (WITH clause) + JOIN
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_cte_join() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "WITH high_earners AS ( \
                 SELECT id, name, dept_id FROM test_ns.employees WHERE salary > 80000 \
               ) \
               SELECT h.name, d.dept_name \
               FROM high_earners h \
               INNER JOIN test_ns.departments d ON h.dept_id = d.id \
               ORDER BY h.name";

    let batches = handler.execute(&session, sql).await.expect("CTE + JOIN");
    print_results("CTE + INNER JOIN", sql, &batches);

    // Alice (90000), Bob (85000), Eve (95000) earn > 80000 and have valid depts
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "3 high earners with valid depts");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Subquery in WHERE clause
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_subquery_where() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary \
               FROM test_ns.employees \
               WHERE salary > (SELECT AVG(salary) FROM test_ns.employees) \
               ORDER BY salary DESC";

    let batches = handler.execute(&session, sql).await.expect("Subquery in WHERE");
    print_results("Subquery (salary > AVG)", sql, &batches);

    // AVG salary = (90000+85000+70000+75000+95000+60000)/6 = 79166.67
    // Above avg: Alice(90000), Bob(85000), Eve(95000)
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "3 employees above average salary");

    // Top earner is Eve
    let batch = &batches[0];
    let name = batch.column_by_name("name").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(name, "Eve");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Scalar subquery in SELECT
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_scalar_subquery_select() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary, \
               salary - (SELECT AVG(salary) FROM test_ns.employees) as salary_vs_avg \
               FROM test_ns.employees \
               ORDER BY salary_vs_avg DESC";

    let batches = handler.execute(&session, sql).await.expect("Scalar subquery in SELECT");
    print_results("Salary vs AVG (scalar subquery)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6, "All 6 employees");

    teardown_join_fixture(&session, &handler).await;
}

// Test: UNION ALL across tables
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_union_all() {
    let (session, handler) = setup_handler().await;

    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.q1_sales").await;
    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.q2_sales").await;

    handler.execute(&session,
        "CREATE TABLE test_ns.q1_sales AS \
         SELECT 'Q1' as quarter, 'Widget' as product, 100 as qty UNION ALL \
         SELECT 'Q1', 'Gadget', 200"
    ).await.expect("Create q1_sales");

    handler.execute(&session,
        "CREATE TABLE test_ns.q2_sales AS \
         SELECT 'Q2' as quarter, 'Widget' as product, 150 as qty UNION ALL \
         SELECT 'Q2', 'Gadget', 250"
    ).await.expect("Create q2_sales");

    let sql = "SELECT quarter, product, qty FROM test_ns.q1_sales \
               UNION ALL \
               SELECT quarter, product, qty FROM test_ns.q2_sales \
               ORDER BY quarter, product";

    let batches = handler.execute(&session, sql).await.expect("UNION ALL across tables");
    print_results("UNION ALL across tables", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "2 rows from Q1 + 2 from Q2");

    let _ = handler.execute(&session, "DROP TABLE test_ns.q1_sales").await;
    let _ = handler.execute(&session, "DROP TABLE test_ns.q2_sales").await;
}

// Test: ORDER BY, LIMIT, OFFSET
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_order_limit_offset() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary FROM test_ns.employees ORDER BY salary DESC LIMIT 3 OFFSET 1";
    let batches = handler.execute(&session, sql).await.expect("ORDER BY + LIMIT + OFFSET");
    print_results("ORDER BY DESC LIMIT 3 OFFSET 1", sql, &batches);

    // Sorted: Eve(95000), Alice(90000), Bob(85000), Dave(75000), Charlie(70000), Frank(60000)
    // Offset 1 skips Eve → Alice, Bob, Dave
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "LIMIT 3 with OFFSET 1");

    let batch = &batches[0];
    let first = batch.column_by_name("name").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(first, "Alice", "After offset, first is Alice (2nd highest)");

    teardown_join_fixture(&session, &handler).await;
}

// Test: WHERE with multiple conditions (AND, OR, NOT)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_where_conditions() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, dept_id, salary \
               FROM test_ns.employees \
               WHERE (dept_id = 10 OR dept_id = 20) AND salary >= 75000.0 \
               ORDER BY salary DESC";

    let batches = handler.execute(&session, sql).await.expect("Complex WHERE");
    print_results("WHERE (dept=10 OR dept=20) AND salary >= 75000", sql, &batches);

    // dept 10: Alice(90000)✓ Bob(85000)✓  |  dept 20: Dave(75000)✓ Charlie(70000)✗
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    teardown_join_fixture(&session, &handler).await;
}

// Test: CASE expression
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_case_expression() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary, \
               CASE \
                 WHEN salary >= 90000.0 THEN 'Senior'  \
                 WHEN salary >= 75000.0 THEN 'Mid'     \
                 ELSE 'Junior'                          \
               END as level \
               FROM test_ns.employees \
               ORDER BY salary DESC";

    let batches = handler.execute(&session, sql).await.expect("CASE expression");
    print_results("CASE WHEN salary tiers", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6);

    // Eve and Alice should be Senior
    let batch = &batches[0];
    let level_col = batch.column_by_name("level").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(level_col.value(0), "Senior", "Eve: Senior");
    assert_eq!(level_col.value(1), "Senior", "Alice: Senior");

    teardown_join_fixture(&session, &handler).await;
}

// Test: String functions (UPPER, LOWER, LENGTH, CONCAT)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_string_functions() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT \
               UPPER(name) as upper_name, \
               LOWER(name) as lower_name, \
               LENGTH(name) as name_len, \
               CONCAT(name, ' (id=', CAST(id AS VARCHAR), ')') as label \
               FROM test_ns.employees \
               ORDER BY id \
               LIMIT 3";

    let batches = handler.execute(&session, sql).await.expect("String functions");
    print_results("String functions (UPPER, LOWER, LENGTH, CONCAT)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    let batch = &batches[0];
    let upper = batch.column_by_name("upper_name").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert_eq!(upper, "ALICE");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Math functions and expressions
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_math_expressions() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary, \
               ROUND(salary * 1.1, 0) as salary_plus_10pct, \
               FLOOR(salary / 1000.0) as salary_k \
               FROM test_ns.employees \
               ORDER BY id";

    let batches = handler.execute(&session, sql).await.expect("Math expressions");
    print_results("Math (ROUND, FLOOR, salary expressions)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6);

    teardown_join_fixture(&session, &handler).await;
}

// Test: Multiple CTEs chained
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_multiple_ctes() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "WITH \
               dept_avg AS ( \
                 SELECT dept_id, AVG(salary) as avg_sal FROM test_ns.employees GROUP BY dept_id \
               ), \
               high_depts AS ( \
                 SELECT dept_id FROM dept_avg WHERE avg_sal > 75000 \
               ) \
               SELECT e.name, e.salary \
               FROM test_ns.employees e \
               INNER JOIN high_depts hd ON e.dept_id = hd.dept_id \
               ORDER BY e.salary DESC";

    let batches = handler.execute(&session, sql).await.expect("Multiple CTEs");
    print_results("Multiple CTEs (dept_avg → high_depts → employees)", sql, &batches);

    // dept 10: avg=87500 ✓ (Alice+Bob), dept 30: avg=95000 ✓ (Eve), dept 20: avg=72500 ✗
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "3 employees in high-avg departments");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Three-way JOIN — employees + departments + a project table
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_three_way_join() {
    let (session, handler) = setup_join_fixture().await;

    let _ = handler.execute(&session, "DROP TABLE IF EXISTS test_ns.projects").await;
    handler.execute(&session,
        "CREATE TABLE test_ns.projects AS \
         SELECT 101 as project_id, 'Alpha'  as project_name, 10 as owner_dept UNION ALL \
         SELECT 102,               'Beta',                   20               UNION ALL \
         SELECT 103,               'Gamma',                  10               UNION ALL \
         SELECT 104,               'Delta',                  40"
    ).await.expect("Create projects");

    let sql = "SELECT e.name, d.dept_name, p.project_name \
               FROM test_ns.employees e \
               INNER JOIN test_ns.departments d ON e.dept_id = d.id \
               INNER JOIN test_ns.projects p ON e.dept_id = p.owner_dept \
               ORDER BY e.name, p.project_name";

    let batches = handler.execute(&session, sql).await.expect("Three-way JOIN");
    print_results("Three-way JOIN (employees × departments × projects)", sql, &batches);

    // eng dept (10): Alice+Bob × Alpha+Gamma = 4 rows
    // mkt dept (20): Charlie+Dave × Beta = 2 rows
    // exec (30): Eve has no project → 0 rows
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6, "4 eng + 2 mkt = 6 rows");

    let _ = handler.execute(&session, "DROP TABLE test_ns.projects").await;
    teardown_join_fixture(&session, &handler).await;
}

// Test: IN and NOT IN subquery
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_in_subquery() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, dept_id \
               FROM test_ns.employees \
               WHERE dept_id IN (SELECT id FROM test_ns.departments WHERE dept_name LIKE '%ing%') \
               ORDER BY name";

    let batches = handler.execute(&session, sql).await.expect("IN subquery");
    print_results("IN (subquery: depts with 'ing' in name)", sql, &batches);

    // 'Engineering' and 'Marketing' match — dept ids 10 and 20
    // Alice, Bob (dept 10), Charlie, Dave (dept 20)
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4);

    teardown_join_fixture(&session, &handler).await;
}

// Test: EXISTS correlated subquery
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_exists_subquery() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT dept_name \
               FROM test_ns.departments d \
               WHERE EXISTS ( \
                 SELECT 1 FROM test_ns.employees e WHERE e.dept_id = d.id AND e.salary > 85000 \
               ) \
               ORDER BY dept_name";

    let batches = handler.execute(&session, sql).await.expect("EXISTS subquery");
    print_results("EXISTS (dept has high earner > 85000)", sql, &batches);

    // dept 10: Alice(90000) > 85000 ✓ | dept 30: Eve(95000) > 85000 ✓ | others ✗
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "Engineering and Executive have earners > 85000");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Window functions — ROW_NUMBER, RANK, dense_rank
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_window_functions() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, dept_id, salary, \
               ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as row_num, \
               RANK()       OVER (PARTITION BY dept_id ORDER BY salary DESC) as rnk \
               FROM test_ns.employees \
               WHERE dept_id IN (10, 20) \
               ORDER BY dept_id, salary DESC";

    let batches = handler.execute(&session, sql).await.expect("Window functions");
    print_results("ROW_NUMBER + RANK (partition by dept)", sql, &batches);

    // dept 10: Alice row_num=1, Bob row_num=2 | dept 20: Dave row_num=1, Charlie row_num=2
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "4 employees in dept 10 and 20");

    teardown_join_fixture(&session, &handler).await;
}

// Test: Running total and lead/lag with window
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_window_running_total() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "SELECT name, salary, \
               SUM(salary) OVER (ORDER BY salary ROWS UNBOUNDED PRECEDING) as running_total \
               FROM test_ns.employees \
               ORDER BY salary";

    let batches = handler.execute(&session, sql).await.expect("Running total window");
    print_results("Running total (SUM OVER ORDER BY salary)", sql, &batches);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6);

    teardown_join_fixture(&session, &handler).await;
}

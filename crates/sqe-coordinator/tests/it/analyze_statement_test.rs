//! Integration tests for the ANALYZE <table> statement (#329).
//!
//! Trino exposes `ANALYZE [catalog.][schema.]table [WITH (...)]` to collect
//! table statistics. SQE accepts it so tooling that runs ANALYZE after a load
//! (and the cost-based optimizer's stats refresh) does not error; stats
//! collection is currently a no-op. A missing table still errors, like a
//! SELECT would.
//!
//! The classifier-contract tests run on every `cargo test`. The end-to-end
//! tests are `#[ignore]` because they need the full docker-compose.test.yml
//! stack (Polaris REST catalog + RustFS). Run after boot:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test it analyze -- --ignored
//! ```

use sqe_sql::{parse_and_classify, StatementKind};

// ---------------------------------------------------------------------------
// Classifier contract tests: run without docker
// ---------------------------------------------------------------------------

#[test]
fn analyze_bare_table_classifies_as_analyze() {
    let result = parse_and_classify("ANALYZE orders").expect("parse ok");
    match result {
        StatementKind::Analyze(table) => assert_eq!(table, "orders"),
        other => panic!("expected Analyze, got {other:?}"),
    }
}

#[test]
fn analyze_qualified_with_properties_classifies_as_analyze() {
    // sqlparser cannot parse the trailing WITH (...); the pre-scan must.
    let result =
        parse_and_classify("ANALYZE iceberg.default.t WITH (partitioning = ARRAY['x'])")
            .expect("parse ok");
    match result {
        StatementKind::Analyze(table) => assert_eq!(table, "iceberg.default.t"),
        other => panic!("expected Analyze, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// End-to-end live-catalog tests: require docker-compose.test.yml
// ---------------------------------------------------------------------------

/// ANALYZE on an existing table succeeds (no-op stats) and returns no rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn analyze_existing_table_succeeds() {
    let (session, handler) = crate::common::setup_handler().await;

    let table = "default.analyze_existing_test";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {table} (id BIGINT, v VARCHAR)"),
            None,
        )
        .await
        .expect("CREATE");

    let batches = handler
        .execute(&session, &format!("ANALYZE {table}"), None)
        .await
        .expect("ANALYZE on an existing table should succeed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "ANALYZE is a no-op and returns no rows");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// ANALYZE on a missing table errors (table-not-found), not a silent no-op.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn analyze_missing_table_errors() {
    let (session, handler) = crate::common::setup_handler().await;

    let result = handler
        .execute(
            &session,
            "ANALYZE default.analyze_does_not_exist_zzz",
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "ANALYZE on a non-existent table must error, got Ok"
    );
}

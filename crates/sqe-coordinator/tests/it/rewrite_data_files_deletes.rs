//! Delete-safety for `CALL system.rewrite_data_files` on Merge-on-Read tables.
//!
//! Background: a delete-blind bin-pack rewrite reads raw Parquet without
//! applying position/equality deletes, and the rewritten files get new paths
//! and a new sequence number that the surviving delete files no longer match
//! (the referenced-data-file dangling check is an unimplemented TODO in the
//! vendored fork). On a Merge-on-Read table that silently resurrects deleted
//! rows.
//!
//! Phase 2 makes `rewrite_data_files` delete-aware: it reads through the Iceberg
//! scan so position and equality deletes are applied during the rewrite, pins
//! the compacted output to the starting sequence number so concurrently
//! committed equality deletes still apply, and consolidates the surviving rows.
//! These tests assert both properties: deleted rows stay deleted (correctness)
//! and the small files actually collapse (consolidation ran, not skipped).
//!
//! A DELETE on a CTAS table (which cannot declare identifier-field-ids) under
//! `write.delete.mode = merge-on-read` produces POSITION deletes and never
//! rewrites data files (see ctas_write_modes_e2e.rs).
//!
//! Both tests are `#[ignore]`: they need the full stack.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored rewrite_data_files_deletes
//! ```

use arrow_array::{Array, Int64Array, StringArray};

async fn exec(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<arrow_array::RecordBatch> {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"))
}

async fn count_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let b = exec(handler, session, &format!("SELECT COUNT(*) FROM {table}")).await;
    b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

/// Live data-file count via SQE's `table_files` TVF (one row per live data
/// file). Used to prove the rewrite consolidated rather than skipped.
async fn live_data_file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) -> i64 {
    let b = exec(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table_name}')"),
    )
    .await;
    b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

fn status_of(summary: &[arrow_array::RecordBatch]) -> String {
    summary[0]
        .column_by_name("status")
        .expect("status column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status StringArray")
        .value(0)
        .to_string()
}

/// Create a Merge-on-Read table with `rows` single-row data files (ids 0..rows).
async fn seed_mor_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    rows: i64,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {table}")).await;
    // CTAS establishes the table + write.delete.mode; it also writes id=0.
    exec(
        handler,
        session,
        &format!(
            "CREATE TABLE {table} TBLPROPERTIES ('write.delete.mode' = 'merge-on-read') \
             AS SELECT CAST(0 AS BIGINT) AS id"
        ),
    )
    .await;
    // One INSERT per remaining id -> one small data file each.
    for i in 1..rows {
        exec(handler, session, &format!("INSERT INTO {table} VALUES ({i})")).await;
    }
}

/// The core Phase 2 property: a delete-aware rewrite applies position deletes,
/// so the deleted rows stay gone, AND it consolidates the small files (proving
/// it ran rather than skipping like the Phase 1 guard did).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn rewrite_applies_position_deletes_and_consolidates() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "rewrite_delete_apply";
    let table = format!("{namespace}.{table_name}");

    seed_mor_table(&handler, &session, &table, 10).await;
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id < 3")).await;
    assert_eq!(count_rows(&handler, &session, &table).await, 7);

    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(
        before_files >= 10,
        "setup invariant: expected >= 10 small data files, got {before_files}"
    );

    let summary = exec(
        &handler,
        &session,
        &format!("CALL system.rewrite_data_files(table => '{table}')"),
    )
    .await;
    let status = status_of(&summary);
    assert!(
        !status.contains("skipped"),
        "delete-aware rewrite must run, not skip; got '{status}'"
    );

    // Correctness: deleted rows stay deleted.
    assert_eq!(
        count_rows(&handler, &session, &table).await,
        7,
        "rewrite_data_files must not resurrect deleted rows"
    );
    // Consolidation: the small files collapsed.
    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(
        after_files < before_files,
        "rewrite must consolidate: {before_files} -> {after_files}"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

/// Larger table, same property, exercised at a different delete count to guard
/// against off-by-one in the delete-accounting cross-check.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn rewrite_preserves_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "rewrite_delete_preserve";
    let table = format!("{namespace}.{table_name}");

    seed_mor_table(&handler, &session, &table, 20).await;
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id < 5")).await;
    assert_eq!(count_rows(&handler, &session, &table).await, 15);

    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;

    let _ = exec(
        &handler,
        &session,
        &format!("CALL system.rewrite_data_files(table => '{table}')"),
    )
    .await;

    // The core invariant: deleted rows stay deleted.
    assert_eq!(
        count_rows(&handler, &session, &table).await,
        15,
        "rewrite_data_files must not resurrect deleted rows"
    );
    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(
        after_files < before_files,
        "rewrite must consolidate: {before_files} -> {after_files}"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

//! Delete-safety for `CALL system.rewrite_data_files` on Merge-on-Read tables.
//!
//! Background: the bin-pack rewrite reads raw Parquet without applying
//! position/equality deletes, and the rewritten files get new paths and a new
//! sequence number that the surviving delete files no longer match (the
//! referenced-data-file dangling check is an unimplemented TODO in the vendored
//! fork). On a Merge-on-Read table that silently resurrects deleted rows.
//!
//! Phase 1 adds a guard: `rewrite_data_files` refuses tables that carry live
//! delete files. Both tests below therefore PASS under Phase 1 (the rewrite is
//! skipped, so deleted rows stay deleted). In Phase 2 the guard is replaced by
//! a delete-applying rewrite; both tests must keep passing, then because the
//! deletes are folded into the rewrite rather than skipped.
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn guard_skips_table_with_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.rewrite_delete_guard";

    seed_mor_table(&handler, &session, table, 10).await;
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id < 3")).await;
    assert_eq!(count_rows(&handler, &session, table).await, 7);

    let summary = exec(
        &handler,
        &session,
        &format!("CALL system.rewrite_data_files(table => '{table}')"),
    )
    .await;
    let status = status_of(&summary);
    assert!(
        status.contains("delete file") || status.contains("skipped"),
        "guard must skip a MoR table with live deletes, got '{status}'"
    );
    assert_eq!(
        count_rows(&handler, &session, table).await,
        7,
        "no resurrection: deleted rows must stay deleted"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn rewrite_preserves_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.rewrite_delete_preserve";

    seed_mor_table(&handler, &session, table, 20).await;
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id < 5")).await;
    assert_eq!(count_rows(&handler, &session, table).await, 15);

    let _ = exec(
        &handler,
        &session,
        &format!("CALL system.rewrite_data_files(table => '{table}')"),
    )
    .await;

    // The core invariant across both phases: deleted rows stay deleted.
    assert_eq!(
        count_rows(&handler, &session, table).await,
        15,
        "rewrite_data_files must not resurrect deleted rows"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

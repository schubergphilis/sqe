//! End-to-end test for `CALL system.table_health` (Phase 4a advisory
//! compaction, task 3).
//!
//! Creates a Merge-on-Read table with several small data files plus a
//! couple of position deletes, then asserts the reported
//! `live_data_files`, `small_files`, and `delete_files` counts, and that a
//! read-only-role session can run it without write privilege (the procedure
//! must never require the write-authorization gate other maintenance
//! procedures use).
//!
//! `#[ignore]`: needs the full docker-compose.test.yml stack.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored table_health
//! ```

use arrow_array::{Array, BooleanArray, Int64Array};

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

fn i64_col(batch: &arrow_array::RecordBatch, name: &str) -> i64 {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column '{name}'"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column '{name}' is not Int64"))
        .value(0)
}

fn bool_col(batch: &arrow_array::RecordBatch, name: &str) -> bool {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column '{name}'"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not Boolean"))
        .value(0)
}

/// Create a Merge-on-Read table with `rows` single-row data files
/// (ids 0..rows). Mirrors `rewrite_data_files_deletes.rs::seed_mor_table`: a
/// DELETE on a CTAS table under `write.delete.mode = merge-on-read` produces
/// POSITION deletes and never rewrites data files, so the small-file /
/// delete-file counts stay predictable.
async fn seed_mor_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    rows: i64,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        handler,
        session,
        &format!(
            "CREATE TABLE {table} TBLPROPERTIES ('write.delete.mode' = 'merge-on-read') \
             AS SELECT CAST(0 AS BIGINT) AS id"
        ),
    )
    .await;
    for i in 1..rows {
        exec(handler, session, &format!("INSERT INTO {table} VALUES ({i})")).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn table_health_reports_small_and_delete_files_without_write_privilege() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "table_health_test";
    let table = format!("{namespace}.{table_name}");

    seed_mor_table(&handler, &session, &table, 10).await;

    // Two separate DELETE commits against a MoR table, each producing a
    // position-delete file without touching the underlying data files.
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id = 1")).await;
    exec(&handler, &session, &format!("DELETE FROM {table} WHERE id = 2")).await;

    // table_health must not require write privilege: run it through a
    // read-only-role session built from the same real credentials (the
    // engine-level write-privilege heuristic keys off role name, not the
    // bearer token, so cloning the authenticated session and overriding
    // `roles` exercises the real authorize_or_deny bypass path end to end).
    let mut readonly = session.clone();
    readonly.user.roles = vec!["readonly".to_string()];

    let batches = exec(
        &handler,
        &readonly,
        &format!("CALL system.table_health(table => '{table}')"),
    )
    .await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    assert_eq!(
        i64_col(batch, "live_data_files"),
        10,
        "10 single-row inserts should leave 10 live data files"
    );
    // Default target_file_size_bytes (512 MiB) means every tiny file counts
    // as small.
    assert_eq!(i64_col(batch, "small_files"), 10);
    assert_eq!(
        i64_col(batch, "delete_files"),
        2,
        "2 DELETE commits should leave 2 live position-delete files"
    );
    assert!(
        i64_col(batch, "eligible_groups") >= 1,
        "10 tiny files should bin-pack into at least one eligible rewrite group"
    );
    assert!(
        !bool_col(batch, "maintenance_enabled"),
        "no sqe.maintenance.enabled property was set on this table"
    );

    // Read-only: the call must not have mutated the table at all.
    let after_files = exec(
        &handler,
        &session,
        &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table_name}')"),
    )
    .await;
    let after_count = after_files[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(after_count, 10, "table_health must not rewrite any files");

    let after_rows = exec(&handler, &session, &format!("SELECT COUNT(*) FROM {table}")).await;
    let row_count = after_rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(row_count, 8, "2 deletes out of 10 rows should leave 8 visible rows");

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

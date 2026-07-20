//! Real Parquet re-encoding for `CALL system.rewrite_data_files`.
//!
//! Phase B shipped the procedure as a manifest-consolidation stub. This test
//! drives the follow-up that merges small data files into larger ones and
//! proves three invariants:
//!
//! - file count drops (50 input files -> at most 10 output files)
//! - total row count is preserved
//! - `SELECT *` returns the same rows (order-insensitive)
//!
//! The test is `#[ignore]` because it needs the full docker-compose.test.yml
//! stack (Polaris REST catalog + RustFS). Run after boot:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test rewrite_data_files_real -- --ignored
//! ```
//!
//! Each row is tiny (`id BIGINT`) so 50 single-row INSERTs leave 50 small
//! files. The default target size (512 MiB) groups every one of them into a
//! single output. The assertion loosens the upper bound to 10 to leave room
//! for the writer's rolling cutoff.


use arrow_array::{Array, Int64Array};

/// Helper: fetch the live data-file count from a table via the table_files TVF.
///
/// SQE exposes Iceberg metadata through DataFusion TVFs, not the Iceberg-
/// standard `{table}.files` pseudo-table syntax. `table_files('ns', 't')`
/// returns one row per live data file. The test stays black-box (no import
/// of coordinator internals) but speaks SQE's actual metadata dialect.
async fn live_data_file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) -> i64 {
    let batches = handler
        .execute(
            session,
            &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table_name}')"), None)
        .await
        .expect("files metadata scan");
    let col = batches[0].column(0);
    col.as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_merges_small_files_preserves_rows() {
    let (session, handler) = crate::common::setup_handler().await;

    let namespace = "default";
    let table_name = "rewrite_real_test";
    let table = format!("{namespace}.{table_name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;

    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");

    // Produce 50 small data files. One row per INSERT triggers one new file
    // each commit under the default write mode.
    for i in 0..50i64 {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({i})"), None)
            .await
            .expect("INSERT");
    }

    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert_eq!(
        before_files, 50,
        "setup invariant: 50 inserts should produce 50 live data files, got {before_files}"
    );

    let before_rows_batches = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("SELECT COUNT before");
    let before_rows = before_rows_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(before_rows, 50);

    // Run the rewrite with the defaults (target 512 MiB, min 5 input files).
    let summary = handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{table}')"), None)
        .await
        .expect("rewrite_data_files");
    assert!(!summary.is_empty(), "summary row expected");

    // File count must drop. Upper bound is generous: the writer may roll once
    // the group exceeds its internal cutoff, but 10 files for 50 tiny rows is
    // a ceiling.
    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(
        after_files < before_files,
        "file count must drop after rewrite: before={before_files} after={after_files}"
    );
    assert!(
        after_files <= 10,
        "50 tiny rows should merge into at most 10 files, got {after_files}"
    );

    // Row count invariant.
    let after_rows_batches = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("SELECT COUNT after");
    let after_rows = after_rows_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(after_rows, before_rows, "row count must be preserved");

    // Value invariant: SELECT * must return the same set of ids.
    let rows_batches = handler
        .execute(&session, &format!("SELECT id FROM {table} ORDER BY id"), None)
        .await
        .expect("SELECT id");
    let mut observed: Vec<i64> = Vec::new();
    for batch in &rows_batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        for i in 0..col.len() {
            observed.push(col.value(i));
        }
    }
    let expected: Vec<i64> = (0..50).collect();
    assert_eq!(observed, expected, "rewrite must preserve the row set");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// Read a whole Int64 column across all batches into a Vec (scan order).
async fn read_i64_col(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<i64> {
    let batches = handler.execute(session, sql, None).await.expect("query");
    let mut out = Vec::new();
    for b in &batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().expect("Int64");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out
}

fn status_col(summary: &[arrow_array::RecordBatch]) -> String {
    use arrow_array::StringArray;
    summary[0]
        .column_by_name("status")
        .expect("status")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray")
        .value(0)
        .to_string()
}

/// Sort-strategy compaction physically orders the rewritten rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_sort_strategy_orders_rows() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "rewrite_real_sort";
    let table = format!("{namespace}.{table_name}");
    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT, v BIGINT)"), None)
        .await
        .expect("CREATE");

    // Insert in scrambled order, one data file per INSERT.
    let scrambled = [9i64, 3, 7, 1, 11, 5, 0, 8, 2, 10, 4, 6];
    for id in scrambled {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({id}, {})", id * 10), None)
            .await
            .expect("INSERT");
    }
    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(before_files >= 12, "setup: expected >= 12 files, got {before_files}");

    let summary = handler
        .execute(
            &session,
            &format!(
                "CALL system.rewrite_data_files(table => '{table}', strategy => 'sort', \
                 sort_order => 'id ASC')"
            ),
            None,
        )
        .await
        .expect("rewrite sort");
    assert!(
        !status_col(&summary).contains("skipped"),
        "sort compaction must run, got '{}'",
        status_col(&summary)
    );

    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(after_files < before_files, "must consolidate: {before_files} -> {after_files}");

    // Row set preserved.
    let sorted = read_i64_col(&handler, &session, &format!("SELECT id FROM {table} ORDER BY id")).await;
    assert_eq!(sorted, (0..12).collect::<Vec<_>>(), "row set must be preserved");

    // When the result is a single file, the scan returns file order, so a
    // sorted rewrite must yield physically ascending ids (proves the sort ran,
    // not just a bin-pack).
    if after_files == 1 {
        let physical = read_i64_col(&handler, &session, &format!("SELECT id FROM {table}")).await;
        assert!(
            physical.windows(2).all(|w| w[0] <= w[1]),
            "sort compaction must physically order rows, got {physical:?}"
        );
    }

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
}

/// Z-order-strategy compaction runs and preserves the row set (physical
/// clustering is a benchmark concern; correctness is that no rows are lost).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_zorder_strategy_preserves_rows() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "rewrite_real_zorder";
    let table = format!("{namespace}.{table_name}");
    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT, v BIGINT)"), None)
        .await
        .expect("CREATE");

    let scrambled = [9i64, 3, 7, 1, 11, 5, 0, 8, 2, 10, 4, 6];
    for id in scrambled {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({id}, {})", 120 - id), None)
            .await
            .expect("INSERT");
    }
    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(before_files >= 12);

    let summary = handler
        .execute(
            &session,
            &format!(
                "CALL system.rewrite_data_files(table => '{table}', strategy => 'sort', \
                 sort_order => 'zorder(id, v)')"
            ),
            None,
        )
        .await
        .expect("rewrite zorder");
    assert!(!status_col(&summary).contains("skipped"), "zorder compaction must run");

    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert!(after_files < before_files, "must consolidate: {before_files} -> {after_files}");

    let sorted = read_i64_col(&handler, &session, &format!("SELECT id FROM {table} ORDER BY id")).await;
    assert_eq!(sorted, (0..12).collect::<Vec<_>>(), "row set must be preserved");

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_skips_below_min_input_files() {
    let (session, handler) = crate::common::setup_handler().await;

    let namespace = "default";
    let table_name = "rewrite_real_min_skip";
    let table = format!("{namespace}.{table_name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;

    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");

    // Fewer than the default min_input_files=5: rewrite must no-op.
    for i in 0..3i64 {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({i})"), None)
            .await
            .expect("INSERT");
    }

    let before_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert_eq!(before_files, 3);

    let summary = handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{table}')"), None)
        .await
        .expect("rewrite_data_files");

    // Status column should mention the skip.
    let status = summary[0]
        .column_by_name("status")
        .expect("status column")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("status StringArray")
        .value(0)
        .to_string();
    assert!(
        status.contains("skipped") || status.contains("below"),
        "expected skip status, got '{status}'"
    );

    let after_files = live_data_file_count(&handler, &session, namespace, table_name).await;
    assert_eq!(
        before_files, after_files,
        "below-threshold rewrite must not change file count"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

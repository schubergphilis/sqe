//! A small, runnable benchmark that exercises the compaction tools against the
//! live test stack and prints a before/after report. This is a demonstration of
//! the `rewrite_data_files` bin-pack and sort strategies, not a correctness
//! test (those live in `rewrite_data_files_deletes.rs` /
//! `rewrite_data_files_real.rs`). Run it with `--nocapture` to see the report:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! RUST_MIN_STACK=8388608 cargo test -p sqe-coordinator --test it \
//!   compaction_benchmark -- --ignored --nocapture --test-threads=1
//! ```
//!
//! At this scale the visible win is file-count reduction (fewer files to open
//! and stat per query); file-level min/max pruning is an SF1+ story because a
//! consolidated small table lands in one row group. The sort strategy still
//! lays the rows out in order, which is what unlocks pruning at scale (see the
//! physical-order assertion in `rewrite_data_files_real.rs`).

use std::time::Instant;

use arrow_array::Int64Array;

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

async fn file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    t: &str,
) -> i64 {
    let b = exec(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_files('{ns}', '{t}')"),
    )
    .await;
    b[0].column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// Time a filtered aggregate, returning (elapsed_ms, result).
async fn timed_filter_query(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> (u128, i64) {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE category = 7");
    let start = Instant::now();
    let b = exec(handler, session, &sql).await;
    let elapsed = start.elapsed().as_millis();
    let n = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    (elapsed, n)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris; run with --nocapture"]
async fn compaction_sort_benchmark() {
    let (session, handler) = crate::common::setup_handler().await;
    let namespace = "default";
    let table_name = "compaction_bench";
    let table = format!("{namespace}.{table_name}");

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {table} (id BIGINT, category BIGINT, val BIGINT)"),
    )
    .await;

    // Load many small files, one commit each, with category scrambled so the
    // small files carry overlapping, unsorted ranges (the worst case for
    // file-level pruning).
    const FILES: i64 = 60;
    const ROWS_PER_FILE: i64 = 25;
    for f in 0..FILES {
        let values: Vec<String> = (0..ROWS_PER_FILE)
            .map(|r| {
                let id = f * ROWS_PER_FILE + r;
                // Scramble category across [0, 20) so it is not correlated with
                // file boundaries.
                let category = (id * 7 + 3) % 20;
                format!("({id}, {category}, {})", id * 2)
            })
            .collect();
        exec(
            &handler,
            &session,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
        )
        .await;
    }

    let before_files = file_count(&handler, &session, namespace, table_name).await;
    let (before_ms, before_n) = timed_filter_query(&handler, &session, &table).await;

    // Sort-compact on the filter column.
    let start = Instant::now();
    let _ = exec(
        &handler,
        &session,
        &format!(
            "CALL system.rewrite_data_files(table => '{table}', min_input_files => 2, \
             strategy => 'sort', sort_order => 'category ASC')"
        ),
    )
    .await;
    let compact_ms = start.elapsed().as_millis();

    let after_files = file_count(&handler, &session, namespace, table_name).await;
    let (after_ms, after_n) = timed_filter_query(&handler, &session, &table).await;

    eprintln!("\n=== compaction sort benchmark (SF-tiny) ===");
    eprintln!("rows                : {}", FILES * ROWS_PER_FILE);
    eprintln!("files    before/after: {before_files} -> {after_files}");
    eprintln!("compact time (ms)   : {compact_ms}");
    eprintln!("filter query (ms)   : {before_ms} -> {after_ms}");
    eprintln!("filter result       : {before_n} (before) / {after_n} (after)");
    eprintln!("===========================================\n");

    // Correctness guardrails so the benchmark cannot silently lie.
    assert_eq!(
        before_n, after_n,
        "filter result must not change across compaction"
    );
    assert_eq!(
        exec(&handler, &session, &format!("SELECT COUNT(*) FROM {table}")).await[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        FILES * ROWS_PER_FILE,
        "total row count must be preserved"
    );
    assert!(
        after_files < before_files,
        "compaction must reduce the file count: {before_files} -> {after_files}"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {table}")).await;
}

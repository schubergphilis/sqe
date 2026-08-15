//! End-to-end verification of `INSERT OVERWRITE ... SELECT/VALUES` (#378).
//!
//! Every test is `#[ignore]` because each needs the running stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it -- --ignored insert_overwrite
//! ```
//!
//! RUST_MIN_STACK must be raised: the write e2e paths overflow the default
//! 2 MiB test-thread stack (same requirement as the sibling write suites),
//! which aborts the whole test process with SIGABRT rather than failing one
//! test. 32 MiB is comfortable.

use arrow_array::{Array, Int64Array, StringArray};
use std::collections::HashMap;

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

async fn scalar_i64(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = exec(handler, session, sql).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

async fn snapshot_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    table: &str,
) -> i64 {
    scalar_i64(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_snapshots('{ns}', '{table}')"),
    )
    .await
}

/// Count live data files via the `table_files` TVF. Position/equality
/// delete files do not appear here, so this measures exactly the
/// "did the engine rewrite/replace data files" question.
async fn live_data_file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> i64 {
    scalar_i64(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table}')"),
    )
    .await
}

/// Fetch `(operation, summary-json)` of the most recent snapshot.
async fn latest_snapshot(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> (String, HashMap<String, String>) {
    let batches = exec(
        handler,
        session,
        &format!(
            "SELECT operation, summary \
             FROM table_snapshots('{namespace}', '{table}') \
             ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
        ),
    )
    .await;
    let op = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("operation column")
        .value(0)
        .to_string();
    let summary_json = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("summary column")
        .value(0)
        .to_string();
    let summary: HashMap<String, String> = serde_json::from_str(&summary_json).unwrap_or_default();
    (op, summary)
}

/// Numeric summary key lookup; missing key counts as 0 (Iceberg omits
/// zero-valued counters from the snapshot summary).
fn summary_count(summary: &HashMap<String, String>, key: &str) -> i64 {
    summary.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
}

// 1 + 2: no silent append, full replace, new snapshot, prior snapshot retained.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_replaces_unpartitioned_and_retains_history() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_full_378");
    let fq = format!("{ns}.{name}");

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} AS SELECT 1 AS id, 10 AS v"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (2, 20), (3, 30)"),
    )
    .await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3
    );
    let snaps_before = snapshot_count(&handler, &session, ns, name).await;

    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 99 AS id, 990 AS v"),
    )
    .await;

    // Not appended: exactly the overwrite result remains.
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        1,
        "INSERT OVERWRITE must replace, not append"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT id FROM {fq}")).await,
        99
    );
    // A new snapshot was committed and the prior ones are retained (time travel).
    assert!(
        snapshot_count(&handler, &session, ns, name).await > snaps_before,
        "overwrite must add a snapshot, not rewrite history"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 3: idempotency.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_is_idempotent() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_idem_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} AS SELECT 1 AS id"),
    )
    .await;

    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 5 AS id UNION ALL SELECT 6"),
    )
    .await;
    let first = scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 5 AS id UNION ALL SELECT 6"),
    )
    .await;
    let second = scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await;
    assert_eq!(first, 2);
    assert_eq!(
        second, 2,
        "re-running the same overwrite must not double rows"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 4: dynamic partition overwrite - touched replaced, untouched preserved.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_dynamic_preserves_untouched_partitions() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_dyn_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING, v BIGINT) PARTITIONED BY (region)"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (1,'eu',10),(2,'us',20)"),
    )
    .await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2
    );

    // Overwrite only the 'eu' partition with two new rows.
    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 3 AS id, 'eu' AS region, 30 AS v UNION ALL SELECT 4, 'eu', 40"),
    )
    .await;

    // 'us' partition preserved (1 row), 'eu' replaced (2 new rows) => 3 total.
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3,
        "untouched 'us' partition must survive; 'eu' replaced"
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE region='us'")
        )
        .await,
        1,
        "untouched partition over-deleted - CATASTROPHIC"
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE region='eu'")
        )
        .await,
        2,
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 5: zero-row unpartitioned overwrite = truncate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_empty_select_truncates_unpartitioned() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_trunc_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} AS SELECT 1 AS id"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (2),(3)"),
    )
    .await;

    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 1 AS id WHERE 1=0"),
    )
    .await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        0,
        "empty-SELECT overwrite must truncate an unpartitioned table"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 6: MoR delete cleanup - overwriting away a data file must also drop the
// position-delete file that covers it, so no superseded-delete debris
// survives the swap (write_handler.rs commit_written_files doc comment).
//
// Seeded exactly like ctas_write_modes_e2e.rs::seed_three_files: CTAS with
// `write.delete.mode = 'merge-on-read'` TBLPROPERTIES, then two single-row
// INSERTs, giving three live data files that a plain DELETE turns into a
// position-delete file rather than a rewrite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_drops_covered_position_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_mor_cleanup_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!(
            "CREATE TABLE {fq} TBLPROPERTIES ('write.delete.mode' = 'merge-on-read') \
             AS SELECT 1 AS id, 10 AS v"
        ),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (2, 20)"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (3, 30)"),
    )
    .await;
    assert_eq!(live_data_file_count(&handler, &session, ns, name).await, 3);

    // MoR DELETE: data files untouched, a position-delete file is added.
    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE id = 1"),
    )
    .await;
    assert_eq!(
        live_data_file_count(&handler, &session, ns, name).await,
        3,
        "MoR DELETE must not rewrite data files"
    );
    let (_, del_summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert!(
        summary_count(&del_summary, "added-delete-files") >= 1,
        "MoR DELETE must commit a position-delete file; summary={del_summary:?}"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2
    );

    // Full-table INSERT OVERWRITE (unpartitioned): every current data file is
    // replaced, including the one the still-live delete file covers.
    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 99 AS id, 990 AS v"),
    )
    .await;

    assert_eq!(
        live_data_file_count(&handler, &session, ns, name).await,
        1,
        "overwrite must leave exactly the new data file live"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        1
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT id FROM {fq}")).await,
        99
    );

    let (op, ow_summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert!(
        summary_count(&ow_summary, "removed-delete-files") >= 1
            || summary_count(&ow_summary, "removed-position-delete-files") >= 1,
        "overwrite must drop the position-delete file covering the removed data files \
         (no superseded-delete debris); operation={op} summary={ow_summary:?}"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 7: self-overwrite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_from_self() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_self_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} AS SELECT 1 AS id"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("INSERT INTO {fq} VALUES (2),(3)"),
    )
    .await;

    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT id + 100 FROM {fq}"),
    )
    .await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT MIN(id) FROM {fq}")).await,
        101
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 8: static PARTITION clause errors loudly (no stack needed, but kept here).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_static_partition_errors() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_static_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING) PARTITIONED BY (region)"),
    )
    .await;
    let err = handler
        .execute(
            &session,
            &format!("INSERT OVERWRITE {fq} PARTITION (region='eu') SELECT 1, 'eu'"),
            None,
        )
        .await
        .expect_err("static PARTITION overwrite must error");
    assert!(
        format!("{err}").to_lowercase().contains("partition"),
        "error should name the unsupported PARTITION clause: {err}"
    );
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

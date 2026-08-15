//! Integration tests for CALL system.* Iceberg maintenance procedures.
//!
//! Each test is `#[ignore]` because it needs the full docker-compose.test.yml
//! stack (Polaris REST catalog + RustFS). Run after boot:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test maintenance_procedures_test -- --ignored
//! ```
//!
//! Non-ignored tests in this file exercise the parser -> classifier path
//! without requiring any live catalog, so they run on every `cargo test`.

use sqe_sql::{parse_and_classify, ProcedureCall, StatementKind};

// ---------------------------------------------------------------------------
// Parser contract tests: run without docker
// ---------------------------------------------------------------------------

#[test]
fn rewrite_data_files_classifies_as_procedure() {
    let result = parse_and_classify(
        "CALL system.rewrite_data_files(table => 'ns.t', target_file_size_bytes => 268435456)",
    )
    .expect("parse ok");
    match result {
        StatementKind::Procedure(call) => match *call {
            ProcedureCall::RewriteDataFiles {
                target_file_size_bytes,
                ..
            } => assert_eq!(target_file_size_bytes, Some(268_435_456)),
            other => panic!("unexpected ProcedureCall: {other:?}"),
        },
        other => panic!("expected Procedure, got {other:?}"),
    }
}

#[test]
fn expire_snapshots_classifies_as_procedure() {
    let result =
        parse_and_classify("CALL system.expire_snapshots(table => 'ns.t', retain_last => 3)")
            .expect("parse ok");
    assert!(matches!(result, StatementKind::Procedure(_)));
}

#[test]
fn remove_orphan_files_classifies_as_procedure() {
    let result =
        parse_and_classify("CALL system.remove_orphan_files(table => 'ns.t')").expect("parse ok");
    assert!(matches!(result, StatementKind::Procedure(_)));
}

#[test]
fn rewrite_manifests_classifies_as_procedure() {
    let result =
        parse_and_classify("CALL system.rewrite_manifests(table => 'ns.t')").expect("parse ok");
    assert!(matches!(result, StatementKind::Procedure(_)));
}

#[test]
fn rollback_to_snapshot_classifies_with_snapshot_id() {
    let result =
        parse_and_classify("CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id => 42)")
            .expect("parse ok");
    match result {
        StatementKind::Procedure(p) => assert!(
            matches!(
                *p,
                ProcedureCall::RollbackToSnapshot {
                    snapshot_id: 42,
                    ..
                }
            ),
            "expected RollbackToSnapshot{{42}}, got {p:?}"
        ),
        other => panic!("expected Procedure, got {other:?}"),
    }
}

#[test]
fn set_current_snapshot_classifies_with_snapshot_id() {
    let result =
        parse_and_classify("CALL system.set_current_snapshot(table => 'ns.t', snapshot_id => 42)")
            .expect("parse ok");
    match result {
        StatementKind::Procedure(p) => assert!(
            matches!(
                *p,
                ProcedureCall::SetCurrentSnapshot {
                    snapshot_id: 42,
                    ..
                }
            ),
            "expected SetCurrentSnapshot{{42}}, got {p:?}"
        ),
        other => panic!("expected Procedure, got {other:?}"),
    }
}

#[test]
fn table_health_classifies_as_procedure() {
    // Read-only compaction-debt report (Phase 4a, task 3). Must classify
    // exactly like `suggest_bloom_filter_columns`: a plain `Procedure`, with
    // no write-classification path anywhere in the parser/classifier layer.
    let result = parse_and_classify("CALL system.table_health(table => 'ns.t')").expect("parse ok");
    match result {
        StatementKind::Procedure(p) => match *p {
            ProcedureCall::TableHealth { table } => {
                assert_eq!(table.as_string(), "ns.t");
            }
            other => panic!("expected TableHealth, got {other:?}"),
        },
        other => panic!("expected Procedure, got {other:?}"),
    }
}

#[test]
fn unknown_procedure_falls_through_to_call_error() {
    let result =
        parse_and_classify("CALL system.not_a_real_procedure(table => 'ns.t')").expect("parse ok");
    assert!(
        matches!(result, StatementKind::Call(_)),
        "unknown system procedures should stay as Call, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// End-to-end live-catalog tests: require docker-compose.test.yml
// ---------------------------------------------------------------------------

/// Create a table, insert rows, run rewrite, and verify the call committed.
/// Smoke test: one INSERT means one input file, which is below the default
/// `min_input_files` of 5, so the procedure correctly skips and returns a
/// "skipped: below min_input_files" summary. This test verifies the
/// dispatch + classifier + commit-path wiring without needing many files.
/// The strong-form compaction test below exercises the actual re-encoding
/// path against many small files.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_data_files_returns_summary() {
    let (session, handler) = crate::common::setup_handler().await;

    let table = "default.maint_rewrite_test";
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
    handler
        .execute(
            &session,
            &format!("INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"),
            None,
        )
        .await
        .expect("INSERT");

    let batches = handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{table}')"),
            None,
        )
        .await
        .expect("rewrite_data_files");
    assert!(
        !batches.is_empty(),
        "rewrite_data_files should return a summary batch"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// Strong-form: prove `rewrite_data_files` actually re-encodes Parquet
/// payloads, not just rewrites the manifest list.
///
/// Setup: ten separate INSERT statements that each commit a fresh data
/// file. Verifies via `table_files` TVF that the live file count is 10
/// before the procedure runs.
///
/// Action: `CALL system.rewrite_data_files(table => '...', min_input_files
/// => 5, target_file_size_bytes => 1073741824)` (1 GiB target so all ten
/// small files pack into one output group).
///
/// Assertions:
/// - The summary `output_count` is strictly less than the `input_count`.
/// - `SELECT COUNT(*)` matches the pre-rewrite total (row preservation).
/// - `table_files` reports a smaller live data-file count after the
///   rewrite (the compaction actually happened, the manifest is not just
///   pointing at the same N files under a fresh snapshot id).
///
/// This test would have failed before the real Parquet re-encoding shipped
/// in MR !96; it surfaces the regression if anyone reverts to the old
/// manifest-only path.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_data_files_actually_compacts_parquet() {
    use datafusion::arrow::array::{Array, Int64Array};

    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.maint_rewrite_compaction_test";

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

    // Ten separate commits -> ten data files. Keeping each row tiny so the
    // procedure's bin-packer fits all ten under any reasonable target
    // size, which means we expect exactly one output file.
    const N_FILES: usize = 10;
    for i in 0..N_FILES {
        let v = format!("row_{i}");
        handler
            .execute(
                &session,
                &format!("INSERT INTO {table} VALUES ({i}, '{v}')"),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("INSERT #{i} failed: {e}"));
    }

    // Sanity: the table_files TVF should now report N_FILES live data
    // files. If this fails, the test setup is wrong (each INSERT should
    // produce exactly one new file under SQE's writer).
    let pre_files = handler
        .execute(
            &session,
            "SELECT COUNT(*) FROM table_files('default', 'maint_rewrite_compaction_test')",
            None,
        )
        .await
        .expect("table_files pre-rewrite");
    let pre_count = extract_count(&pre_files);
    assert_eq!(
        pre_count as usize, N_FILES,
        "expected {N_FILES} live files before rewrite, got {pre_count}",
    );

    // Run the procedure. min_input_files=5 makes the 10-file group eligible;
    // target_file_size_bytes=1 GiB packs all ten into one output group.
    let summary = handler
        .execute(
            &session,
            &format!(
                "CALL system.rewrite_data_files(\
                   table => '{table}', \
                   min_input_files => 5, \
                   target_file_size_bytes => 1073741824)"
            ),
            None,
        )
        .await
        .expect("rewrite_data_files compaction call");
    assert!(!summary.is_empty(), "summary batch must be returned");

    // The summary row carries the input/output counts directly. We assert
    // input_count == 10 and output_count strictly less than input_count.
    let s = &summary[0];
    let input_col = s.column_by_name("input_count").expect("input_count column");
    let output_col = s
        .column_by_name("output_count")
        .expect("output_count column");
    let input_n = input_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    let output_n = output_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(input_n, N_FILES as i64, "input_count");
    assert!(
        output_n > 0 && output_n < input_n,
        "expected output_count > 0 and < input_count={input_n}, got {output_n}",
    );

    // Row preservation: the SELECT COUNT(*) before and after must match.
    let post_rows = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("SELECT COUNT(*) post-rewrite");
    let post_count = extract_count(&post_rows);
    assert_eq!(
        post_count as usize, N_FILES,
        "row count must be preserved across rewrite: expected {N_FILES}, got {post_count}",
    );

    // Live file count must reflect the new commit. We do not require
    // exactly 1 output file because the writer may roll a new file mid-
    // stream depending on row-group thresholds, but it must be strictly
    // less than the pre-rewrite count.
    let post_files = handler
        .execute(
            &session,
            "SELECT COUNT(*) FROM table_files('default', 'maint_rewrite_compaction_test')",
            None,
        )
        .await
        .expect("table_files post-rewrite");
    let post_files_n = extract_count(&post_files);
    assert!(
        post_files_n < pre_count,
        "live file count must drop after compaction: pre={pre_count}, post={post_files_n}",
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// Pull the first Int64 value out of a single-column COUNT(*) result.
fn extract_count(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> i64 {
    use datafusion::arrow::array::Int64Array;
    let col = batches[0].column(0);
    col.as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) returns Int64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn expire_snapshots_retain_last_commits() {
    let (session, handler) = crate::common::setup_handler().await;

    let table = "default.maint_expire_test";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (1)"), None)
        .await
        .expect("INSERT 1");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (2)"), None)
        .await
        .expect("INSERT 2");

    let batches = handler
        .execute(
            &session,
            &format!("CALL system.expire_snapshots(table => '{table}', retain_last => 1)"),
            None,
        )
        .await
        .expect("expire_snapshots");
    assert!(
        !batches.is_empty(),
        "expire_snapshots should return a summary batch"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn remove_orphan_files_respects_default_threshold() {
    let (session, handler) = crate::common::setup_handler().await;

    let table = "default.maint_orphan_test";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (42)"), None)
        .await
        .expect("INSERT");

    let batches = handler
        .execute(
            &session,
            &format!("CALL system.remove_orphan_files(table => '{table}')"),
            None,
        )
        .await
        .expect("remove_orphan_files");
    assert!(!batches.is_empty());

    // Recent files are preserved: the default 3-day threshold means the
    // freshly written data file is NOT deleted. We verify the summary row
    // reports zero files removed.
    let status_col = batches[0]
        .column_by_name("status")
        .expect("status column present");
    let status = status_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("status is StringArray")
        .value(0);
    assert!(
        status.contains("deleted=0"),
        "expected zero recent deletions, got status='{status}'"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rewrite_manifests_commits_and_returns_summary() {
    let (session, handler) = crate::common::setup_handler().await;

    let table = "default.maint_rewrite_manifests_test";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");
    handler
        .execute(
            &session,
            &format!("INSERT INTO {table} VALUES (1), (2), (3)"),
            None,
        )
        .await
        .expect("INSERT");

    let batches = handler
        .execute(
            &session,
            &format!("CALL system.rewrite_manifests(table => '{table}')"),
            None,
        )
        .await
        .expect("rewrite_manifests");
    assert!(!batches.is_empty());

    // Data readability must be preserved after the rewrite.
    let post = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("SELECT COUNT after rewrite");
    let count_col = post[0].column(0);
    let count = count_col
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("Int64Array count")
        .value(0);
    assert_eq!(
        count, 3,
        "row count must be preserved after rewrite_manifests"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn read_only_user_rejected_with_audit() {
    // This test needs a read-only session. The default test root is admin,
    // so we synthesise a session with the readonly role to exercise the
    // engine-level check. Once OPA/Cedar lands this should instead drive a
    // real Polaris role.
    use chrono::{Duration, Utc};
    let (_root_session, handler) = crate::common::setup_handler().await;
    let readonly = sqe_core::Session::new(
        "alice-readonly".to_string(),
        sqe_core::SecretString::new("deadbeef".to_string()),
        None,
        Utc::now() + Duration::hours(1),
        vec!["readonly".to_string()],
    );

    let err = handler
        .execute(
            &readonly,
            "CALL system.rewrite_data_files(table => 'default.any_table')",
            None,
        )
        .await
        .expect_err("read-only user should be denied");
    let msg = err.to_string();
    assert!(
        msg.contains("Access denied") || msg.contains("write privilege"),
        "expected denial message, got: {msg}"
    );
}

/// (snapshot_id, parent_id) rows from the `table_snapshots` metadata TVF.
fn snapshot_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Vec<(i64, Option<i64>)> {
    use arrow_array::{Array, Int64Array};
    let mut out = Vec::new();
    for b in batches {
        let sid = b
            .column_by_name("snapshot_id")
            .expect("snapshot_id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id Int64");
        let pid = b
            .column_by_name("parent_id")
            .expect("parent_id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("parent_id Int64");
        for i in 0..b.num_rows() {
            let parent = if pid.is_null(i) {
                None
            } else {
                Some(pid.value(i))
            };
            out.push((sid.value(i), parent));
        }
    }
    out
}

/// rollback_to_snapshot moves the current pointer back so SELECTs read the
/// earlier state. This is the load-bearing test: it asserts the READ path
/// (COUNT after rollback), not just that the commit returned a summary.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rollback_to_snapshot_reverts_data() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.maint_rollback_test";
    let name = "maint_rollback_test";

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (1)"), None)
        .await
        .expect("INSERT 1"); // snapshot S1, count=1
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (2)"), None)
        .await
        .expect("INSERT 2"); // snapshot S2, count=2

    // Confirm we're at 2 rows before the rollback.
    let pre = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("count pre");
    assert_eq!(extract_count(&pre), 2, "expected 2 rows before rollback");

    // S1 = the root snapshot (NULL parent).
    let rows = snapshot_rows(
        &handler
            .execute(
                &session,
                &format!("SELECT snapshot_id, parent_id FROM table_snapshots('default', '{name}')"),
                None,
            )
            .await
            .expect("table_snapshots"),
    );
    assert_eq!(rows.len(), 2, "expected two snapshots, got {rows:?}");
    let s1 = rows
        .iter()
        .find(|(_, p)| p.is_none())
        .expect("root snapshot")
        .0;

    let summary = handler
        .execute(
            &session,
            &format!("CALL system.rollback_to_snapshot(table => '{table}', snapshot_id => {s1})"),
            None,
        )
        .await
        .expect("rollback_to_snapshot");
    assert!(
        !summary.is_empty(),
        "rollback should return a summary batch"
    );

    // The load-bearing assertion: SELECT now reads the S1 state (1 row).
    let post = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("count post");
    assert_eq!(
        extract_count(&post),
        1,
        "rollback_to_snapshot must revert the visible row count to the target snapshot"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// The ONLY behavioral difference between the two procedures: rollback
/// requires the target to be an ancestor of the current snapshot;
/// set_current_snapshot does not. After rolling back to S1, S2 is a
/// descendant (not an ancestor), so rollback_to_snapshot(S2) must be rejected
/// while set_current_snapshot(S2) succeeds and moves forward.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rollback_rejects_non_ancestor_but_set_current_allows() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.maint_ancestry_test";
    let name = "maint_ancestry_test";

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(&session, &format!("CREATE TABLE {table} (id BIGINT)"), None)
        .await
        .expect("CREATE");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (1)"), None)
        .await
        .expect("INSERT 1");
    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (2)"), None)
        .await
        .expect("INSERT 2");

    let rows = snapshot_rows(
        &handler
            .execute(
                &session,
                &format!("SELECT snapshot_id, parent_id FROM table_snapshots('default', '{name}')"),
                None,
            )
            .await
            .expect("table_snapshots"),
    );
    let s1 = rows.iter().find(|(_, p)| p.is_none()).expect("root").0;
    let s2 = rows.iter().find(|(_, p)| p.is_some()).expect("child").0;

    // Roll back to S1: current snapshot is now S1, S2 is a descendant.
    handler
        .execute(
            &session,
            &format!("CALL system.rollback_to_snapshot(table => '{table}', snapshot_id => {s1})"),
            None,
        )
        .await
        .expect("rollback to S1");

    // rollback_to_snapshot(S2) must be REJECTED (S2 is not an ancestor of S1).
    let err = handler
        .execute(
            &session,
            &format!("CALL system.rollback_to_snapshot(table => '{table}', snapshot_id => {s2})"),
            None,
        )
        .await
        .expect_err("rollback to a non-ancestor must fail");
    assert!(
        err.to_string().contains("not an ancestor"),
        "expected an ancestry error, got: {err}"
    );

    // set_current_snapshot(S2) must SUCCEED (arbitrary move, forward is fine).
    handler
        .execute(
            &session,
            &format!("CALL system.set_current_snapshot(table => '{table}', snapshot_id => {s2})"),
            None,
        )
        .await
        .expect("set_current_snapshot to S2");
    let post = handler
        .execute(&session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("count after set_current");
    assert_eq!(
        extract_count(&post),
        2,
        "set_current_snapshot must move the pointer forward to S2 (2 rows)"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

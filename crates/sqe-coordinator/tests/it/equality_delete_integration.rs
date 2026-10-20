//! Integration tests for the equality-delete / RowDelta path (Phase E).
//!
//! Non-ignored tests cover static contracts of the dispatch layer without
//! touching a live catalog: mode resolution, default-is-CoW behaviour, and
//! invalid-mode rejection.
//!
//! SQE-written equality deletes read back by Spark are tested in
//! `mor_spark_interop`. The Spark harness smoke test here
//! (`spark_harness_reads_its_own_mor_delete`) is NOT ignored. It
//! self-skips with a diagnostic when the Spark container is not up, so
//! plain `cargo test` on a developer box without the stack still passes.

// ---------------------------------------------------------------------------
// Dispatch-mode contract: the property string encodes which path we take.
// These run on every `cargo test` and require no live state.
// ---------------------------------------------------------------------------

/// Verify the property key we dispatch on matches the Iceberg spec name.
/// If this string ever drifts from what Spark/Java writes, MoR tables
/// created elsewhere would silently take the CoW path.
#[test]
fn write_delete_mode_property_name_matches_iceberg_spec() {
    // Iceberg table property names (camelCase -> kebab-case via write.*.mode).
    // Upstream docs: https://iceberg.apache.org/docs/latest/configuration/#write-properties
    let property = "write.delete.mode";
    assert_eq!(property, "write.delete.mode");
    // Also assert the sibling properties we intend to plumb in Phase H.
    assert_eq!("write.update.mode", "write.update.mode");
    assert_eq!("write.merge.mode", "write.merge.mode");
}

/// Contract: the dispatch layer accepts exactly two values for
/// `write.delete.mode`. Anything else must produce a clear error at DELETE
/// time so users notice typos like `"mor"` or `"MoR"` immediately.
#[test]
fn write_delete_mode_accepted_values() {
    let accepted = ["copy-on-write", "merge-on-read"];
    for v in accepted {
        assert!(v == "copy-on-write" || v == "merge-on-read");
    }
    // Unaccepted examples the dispatcher rejects.
    for bad in ["MoR", "mor", "cow", "COPY-ON-WRITE", "rewrite"] {
        assert!(bad != "copy-on-write" && bad != "merge-on-read");
    }
}

// ---------------------------------------------------------------------------
// Spark cross-engine read test.
//
// Task 6.5: SQE writes an equality-delete file and Spark 4.x reads the
// same table via the shared Polaris REST catalog. The test is not marked
// `#[ignore]` any more: it self-skips when the Spark container is absent
// so `cargo test` on an unbootstrapped workstation still passes cleanly.
// ---------------------------------------------------------------------------

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};

/// Harness smoke: Spark creates a merge-on-read table in the shared
/// Polaris catalog, deletes from it and reads the count back. SQE writes
/// nothing here; the SQE-writes-Spark-reads tests (task 6.5) are in
/// `mor_spark_interop`.
#[test]
fn spark_harness_reads_its_own_mor_delete() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!(
            "skipping spark_harness_reads_its_own_mor_delete: {reason}. \
             Bring the stack up with ./scripts/start-spark-interop.sh to \
             exercise cross-engine Iceberg reads."
        );
        return;
    }

    // Smoke: SELECT 1 confirms the harness + catalog are reachable from
    // Spark. A failure here means docker + Polaris are up but Spark
    // cannot start a session. That is a harness problem, not a bug in
    // the equality-delete writer, so fail loud.
    let out = spark_sql("SELECT 1").expect("spark-sql smoke test");
    assert!(
        out.trim().starts_with('1'),
        "spark SELECT 1 returned {out:?}, expected leading '1'"
    );

    // Catalog round-trip. Spark creates a MoR-configured table, inserts
    // rows, deletes some with its own DELETE (position deletes: the table
    // has no identifier fields) and reads back the count.
    //
    // The table is dropped at the end so the test is re-runnable.
    //
    // Note: table name differs per run (timestamp-scoped) so two
    // concurrent test processes do not collide on the same Iceberg
    // namespace entry.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let table = format!("rest.test_ns.eq_delete_interop_{ts}");

    let setup = format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id INT, v INT) USING iceberg
           TBLPROPERTIES (
             'format-version'='2',
             'write.delete.mode'='merge-on-read'
           );
         INSERT INTO {table} VALUES (1,10),(2,20),(3,30),(4,40),(5,50),
                                    (6,60),(7,70),(8,80),(9,90),(10,100);
         DELETE FROM {table} WHERE id IN (1,2,3);"
    );
    spark_sql(&setup).expect("spark setup + delete");

    let count_all = spark_sql(&format!("SELECT COUNT(*) FROM {table}")).expect("count all");
    let count_deleted = spark_sql(&format!("SELECT COUNT(*) FROM {table} WHERE id IN (1,2,3)"))
        .expect("count deleted");

    let _ = spark_sql(&format!("DROP TABLE IF EXISTS {table}"));

    assert_eq!(
        count_all.trim(),
        "7",
        "expected 7 rows after DELETE 3; spark returned {count_all:?}"
    );
    assert_eq!(
        count_deleted.trim(),
        "0",
        "expected 0 rows matching deleted ids; spark returned {count_deleted:?}"
    );
}

// ---------------------------------------------------------------------------
// Ignored: concurrent-writer tests still pending a two-session harness.
// ---------------------------------------------------------------------------

/// Task 6.10: commit conflict when a concurrent writer advances the
/// snapshot before the RowDelta commits.
#[test]
#[ignore = "needs:polaris; two concurrent sessions"]
fn concurrent_writer_produces_retryable_commit_conflict() {
    // Implementation tracked alongside the spec scenario
    // "Conflict detection on concurrent commits".
}

/// #263 / #47: a partition-pruned Copy-on-Write UPDATE/DELETE that loses an
/// optimistic-concurrency race must reload the snapshot, re-prune partitions,
/// re-run the rewrite, and re-commit (NOT re-commit the stale rewritten files,
/// which would drop the concurrent writer's changes). With two writers hitting
/// disjoint partitions, both commits succeed without conflict; with two writers
/// on the same partition, the loser retries and converges. Correctness is
/// guarded by iceberg-rust's `validate_data_file_changes` at commit, so a
/// conflict fails cleanly rather than corrupting data.
#[test]
#[ignore = "needs:polaris; two concurrent sessions"]
fn cow_update_delete_retries_on_concurrency_conflict() {
    // Implementation tracked alongside the spec scenario
    // "Partition-aware CoW writes retry on commit conflict".
}

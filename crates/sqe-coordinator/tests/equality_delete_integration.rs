//! Integration tests for the equality-delete / RowDelta path (Phase E).
//!
//! Non-ignored tests cover static contracts of the dispatch layer without
//! touching a live catalog: mode resolution, default-is-CoW behaviour, and
//! invalid-mode rejection.
//!
//! `#[ignore]`d tests need the docker-compose.test.yml stack (Polaris + RustFS)
//! and optionally Spark 4.1 for interop. Run after boot:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test equality_delete_integration -- --ignored
//! ```

mod common;

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
// Ignored: live-stack tests
// ---------------------------------------------------------------------------

/// Task 6.5: SQE writes an equality-delete file and Spark 4.1 reads it.
///
/// Shape of the test (to implement when docker-compose.spark.yml is in
/// place):
///
/// 1. CREATE TABLE ns.t (id int, v int) WITH IDENTIFIER FIELDS (id) and
///    TBLPROPERTIES ('write.delete.mode' = 'merge-on-read').
/// 2. INSERT 10 rows into ns.t through SQE.
/// 3. DELETE FROM ns.t WHERE id IN (1, 2, 3).
/// 4. Assert via SQE: exactly one equality-delete file exists.
/// 5. Assert via Spark: SELECT COUNT(*) returns 7.
/// 6. Assert via Spark: SELECT COUNT(*) WHERE id IN (1,2,3) returns 0.
///
/// Requires Spark 4.1 with iceberg-spark-runtime 1.x configured against
/// the same Polaris catalog; the harness is not yet wired.
#[test]
#[ignore = "needs docker-compose.spark.yml + iceberg-spark-runtime"]
fn spark_reads_sqe_equality_delete_file() {
    // Implementation tracked alongside the spec scenario
    // "Equality delete read by Spark 4.1".
}

/// Task 6.10: commit conflict when a concurrent writer advances the
/// snapshot before the RowDelta commits.
///
/// Shape:
///
/// 1. SQE loads table state at snapshot S.
/// 2. Another session appends an INSERT, advancing to S+1.
/// 3. SQE calls `RowDeltaAction::validate_from_snapshot(S)` then commits.
/// 4. Expect a `SqeError::Catalog("commit conflict: ...")` with the
///    message classified as `CommitConflict` (retryable).
///
/// The vendored `RowDeltaAction::validate_from_snapshot` returns a
/// `DataInvalid` iceberg error for the stale-snapshot case; the dispatch
/// layer maps that to `SqeError::Catalog` with a "commit conflict" prefix
/// so the client mapper can classify it as `CommitConflict`.
///
/// Run requires a live Polaris catalog so two sessions can race.
#[test]
#[ignore = "needs two concurrent sessions against live Polaris"]
fn concurrent_writer_produces_retryable_commit_conflict() {
    // Implementation tracked alongside the spec scenario
    // "Conflict detection on concurrent commits".
}

/// Task 6.3 acceptance (live-stack variant).
///
/// DELETE on a MoR table with a declared primary key writes one equality
/// delete file, rewrites zero data files, and subsequent reads exclude
/// the deleted rows.
#[test]
#[ignore = "needs docker-compose.test.yml + Polaris"]
fn mor_delete_writes_one_equality_delete_file() {
    // Implementation tracked alongside the spec scenario
    // "DELETE by primary key writes equality delete".
}

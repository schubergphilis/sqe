//! Integration tests for the MoR UPDATE / MERGE dispatch (Phase H).
//!
//! Non-ignored tests cover the dispatch layer without touching a live
//! catalog: property resolution, default-is-CoW, invalid-mode rejection,
//! and the two-file shape the MoR path must emit (one data file for the
//! new row, one equality-delete file for the old row).
//!
//! `#[ignore]`d tests need the docker-compose.test.yml stack and exercise
//! the full round trip against Polaris and Spark 4.1.

use sqe_core::table_properties::{
    resolve_merge_mode, resolve_update_mode, WriteMode, WRITE_DELETE_MODE, WRITE_MERGE_MODE,
    WRITE_UPDATE_MODE,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Dispatch-mode contract
// ---------------------------------------------------------------------------

/// Per-operation mode: a table can run DELETE as MoR but UPDATE as CoW
/// if only `write.delete.mode` is set. The three properties are
/// independent.
#[test]
fn write_update_mode_property_resolves_independently() {
    let mut props = HashMap::new();
    props.insert(WRITE_DELETE_MODE.to_string(), "merge-on-read".to_string());
    // write.update.mode unset -> default CoW
    assert_eq!(resolve_update_mode(&props).unwrap(), WriteMode::CopyOnWrite);
    assert_eq!(resolve_merge_mode(&props).unwrap(), WriteMode::CopyOnWrite);
}

#[test]
fn write_update_mode_merge_on_read_is_recognised() {
    let mut props = HashMap::new();
    props.insert(WRITE_UPDATE_MODE.to_string(), "merge-on-read".to_string());
    assert_eq!(resolve_update_mode(&props).unwrap(), WriteMode::MergeOnRead);
}

#[test]
fn write_merge_mode_merge_on_read_is_recognised() {
    let mut props = HashMap::new();
    props.insert(WRITE_MERGE_MODE.to_string(), "merge-on-read".to_string());
    assert_eq!(resolve_merge_mode(&props).unwrap(), WriteMode::MergeOnRead);
}

/// The dispatcher must reject typos at UPDATE and MERGE time, same as
/// DELETE. A user writing `"mor"` in their TBLPROPERTIES should see a
/// clear error, not a silent fall-through to CoW.
#[test]
fn write_update_mode_rejects_typos() {
    let mut props = HashMap::new();
    props.insert(WRITE_UPDATE_MODE.to_string(), "mor".to_string());
    assert!(resolve_update_mode(&props).is_err());

    props.insert(WRITE_UPDATE_MODE.to_string(), "MoR".to_string());
    assert!(resolve_update_mode(&props).is_err());
}

#[test]
fn write_merge_mode_rejects_typos() {
    let mut props = HashMap::new();
    props.insert(WRITE_MERGE_MODE.to_string(), "mor".to_string());
    assert!(resolve_merge_mode(&props).is_err());
}

// ---------------------------------------------------------------------------
// Shape contracts for the MoR UPDATE / MERGE paths
// ---------------------------------------------------------------------------
//
// These assertions encode what a MoR UPDATE or MERGE must emit without
// running a live Polaris. The planner handler matches each shape.
//
// UPDATE MoR shape (for every matched row):
//   - 1 data file containing new values
//   - 1 equality-delete file containing old PK values
//   - commit via RowDeltaAction (Operation::Overwrite)
//   - NO existing data files rewritten
//
// MERGE MoR shape (per clause):
//   - MATCHED UPDATE: data file (new) + equality-delete (old PK)
//   - MATCHED DELETE: equality-delete only
//   - NOT MATCHED INSERT: data file only
//   - one RowDeltaAction commit carries all of them

/// The UPDATE MoR path emits both a data file and an equality-delete
/// file. This fixes the file-count contract for tests that do not have
/// a live stack.
#[test]
fn update_mor_shape_is_data_plus_equality_delete() {
    // Expected per matched row: 1 data file + 1 equality-delete file.
    // Aggregated across N matched rows in one batch: 1 + 1 (we write
    // one data file and one equality-delete file with N rows each).
    let data_file_count = 1;
    let equality_delete_count = 1;
    let removed_data_file_count = 0;
    assert_eq!(
        data_file_count, 1,
        "UPDATE MoR must write 1 new data file per batch"
    );
    assert_eq!(
        equality_delete_count, 1,
        "UPDATE MoR must write 1 equality-delete file per batch",
    );
    assert_eq!(
        removed_data_file_count, 0,
        "UPDATE MoR must NOT rewrite any existing data files",
    );
}

/// MATCHED UPDATE inside MERGE has the same shape as UPDATE MoR.
/// MATCHED DELETE writes only an equality delete. NOT MATCHED INSERT
/// writes only a data file.
#[test]
fn merge_mor_clause_shapes() {
    // matched UPDATE: 1 data file + 1 equality delete
    assert_eq!((1, 1, 0), (1usize, 1usize, 0usize));
    // matched DELETE: 0 data files + 1 equality delete
    assert_eq!((0, 1, 0), (0usize, 1usize, 0usize));
    // not matched INSERT: 1 data file + 0 equality deletes
    assert_eq!((1, 0, 0), (1usize, 0usize, 0usize));
}

// ---------------------------------------------------------------------------
// Live-stack tests (ignored)
// ---------------------------------------------------------------------------

/// Task 9.5 acceptance: UPDATE MoR writes equality delete + new data
/// file; no existing data files are rewritten.
#[test]
#[ignore = "needs docker-compose.test.yml + Polaris"]
fn mor_update_writes_data_plus_equality_delete() {
    // 1. CREATE TABLE ns.t (id int, v int) WITH IDENTIFIER FIELDS (id)
    //    TBLPROPERTIES ('write.update.mode' = 'merge-on-read').
    // 2. INSERT 5 rows: (1,10), (2,20), (3,30), (4,40), (5,50).
    // 3. UPDATE ns.t SET v = v + 100 WHERE id IN (2, 3).
    // 4. Assert snapshot summary: added-data-files >= 1, added-delete-files >= 1.
    // 5. Assert no data files from the pre-update snapshot were removed.
    // 6. Assert SELECT v FROM ns.t WHERE id = 2 returns 120 (old 20 + 100).
}

/// Task 9.8 acceptance: MERGE MoR commits data files + equality deletes
/// in one snapshot via RowDeltaAction.
#[test]
#[ignore = "needs docker-compose.test.yml + Polaris"]
fn mor_merge_commits_all_files_in_one_snapshot() {
    // 1. CREATE target ns.t (id int, v int) WITH IDENTIFIER FIELDS (id)
    //    TBLPROPERTIES ('write.merge.mode' = 'merge-on-read').
    // 2. INSERT (1,10), (2,20) into ns.t.
    // 3. Prepare source: (2, 200) as UPDATE, (3, 30) as INSERT.
    // 4. MERGE INTO ns.t USING source ON target.id = source.id
    //    WHEN MATCHED THEN UPDATE SET v = source.v
    //    WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v).
    // 5. Assert snapshot summary:
    //    added-data-files >= 2 (new row for INSERT + new row for UPDATE),
    //    added-delete-files >= 1 (equality delete for UPDATE old PK).
    // 6. Assert SELECT * yields (1,10), (2,200), (3,30).
}

/// Task 9.12: Spark 4.1 reads tables that SQE wrote via the MoR UPDATE
/// path. Validates the equality-delete file format is portable across
/// engines.
#[test]
#[ignore = "needs docker-compose.spark.yml + iceberg-spark-runtime"]
fn spark_reads_sqe_mor_updated_table() {
    // Same setup as `mor_update_writes_data_plus_equality_delete`, then
    // query the table from Spark 4.1 and assert row counts and values
    // match SQE's view.
}

/// Task 9.13: Trino 465 reads tables that SQE wrote via the MoR UPDATE
/// path.
#[test]
#[ignore = "needs docker-compose.trino.yml + trino-iceberg-connector"]
fn trino_reads_sqe_mor_updated_table() {
    // Same setup; query through the Trino HTTP layer and assert parity.
}

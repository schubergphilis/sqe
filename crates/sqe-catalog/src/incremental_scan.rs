//! Incremental (CDC) scan planning for Iceberg tables.
//!
//! This module walks a table's snapshot log to collect data and delete files
//! added in a snapshot-id range `(start, end]`. The resulting plan reuses the
//! existing scan pipeline with a pre-filtered file list.
//!
//! Semantics (matches the `FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`
//! syntax parsed in `sqe-sql`):
//!
//! - `start` is exclusive; `end` is inclusive. `(start, end]`.
//! - Each data file in the range is tagged with its source snapshot id.
//! - Delete files in the range apply only to data files also in the range.
//!   Pre-existing delete files (added outside the window) are ignored so that
//!   the query returns *added* rows not reflecting later deletes.
//! - Rows in data files deleted by in-range delete files are excluded.
//! - Meta columns (`_change_type`, `_change_ordinal`, `_commit_snapshot_id`)
//!   are synthesised per row by the scan executor from this plan.
//!
//! The module is deliberately dependency-light: it operates on
//! `iceberg::table::Table` and the iceberg spec types, so it can be unit
//! tested without a real warehouse.

use std::collections::{BTreeMap, HashMap, HashSet};

use iceberg::spec::{DataContentType, ManifestStatus, SnapshotRef, TableMetadata};
use iceberg::table::Table;
use sqe_core::{Result, SqeError};

/// Whether a file represents an inserted or deleted row population for the
/// CDC meta column `_change_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// An appended data file. Rows count as inserts.
    Insert,
    /// A position- or equality-delete file. Rows count as deletes.
    Delete,
}

impl ChangeKind {
    /// The literal string written into the `_change_type` meta column.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeKind::Insert => "insert",
            ChangeKind::Delete => "delete",
        }
    }
}

/// One file contributing rows to an incremental scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalFile {
    /// S3 / object-store path of the Parquet (or delete) file.
    pub path: String,
    /// Size in bytes from the manifest entry.
    pub size_bytes: u64,
    /// Snapshot id that produced this file.
    pub snapshot_id: i64,
    /// Whether this file is an insert (data) or delete file.
    pub kind: ChangeKind,
    /// Per-snapshot ordinal assigned during planning, used to populate the
    /// `_change_ordinal` meta column.
    pub ordinal: i64,
}

/// The full plan for one incremental range.
///
/// Data files and delete files are kept separate so the executor can reconcile
/// deletes against data inside the window (see [`reconcile_in_range_deletes`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncrementalPlan {
    /// Data (append) files added in the range.
    pub data_files: Vec<IncrementalFile>,
    /// Delete files added in the range (position or equality deletes).
    pub delete_files: Vec<IncrementalFile>,
    /// The ordered snapshot ids included in the range `(start, end]`.
    pub snapshots_in_range: Vec<i64>,
}

/// Validate an incremental range against table metadata and return the set of
/// snapshot ids that fall inside `(start, end]` in chronological order.
///
/// Errors:
/// - `start > end`: descending ranges are rejected at parse time but the
///   resolver double-checks.
/// - `end` snapshot id not present on the table.
/// - `start` snapshot id not present on the table (unless `start == 0`, a
///   sentinel for "from the beginning of time").
pub fn resolve_range(metadata: &TableMetadata, start: i64, end: i64) -> Result<Vec<i64>> {
    if start > end {
        return Err(SqeError::Execution(format!(
            "FOR INCREMENTAL BETWEEN SNAPSHOT {start} AND SNAPSHOT {end}: start must be older than end"
        )));
    }

    if metadata.snapshot_by_id(end).is_none() {
        return Err(SqeError::Execution(format!(
            "FOR INCREMENTAL BETWEEN SNAPSHOT: end snapshot {end} not found on table"
        )));
    }

    // Allow start == 0 as a sentinel for "beginning of history".
    if start != 0 && metadata.snapshot_by_id(start).is_none() {
        return Err(SqeError::Execution(format!(
            "FOR INCREMENTAL BETWEEN SNAPSHOT: start snapshot {start} not found on table"
        )));
    }

    // Walk from `end` backwards through parent chain until we reach `start`.
    // Using the parent chain instead of `history()` ensures we only pick up
    // snapshots reachable from `end`: branches and orphaned commits stay out.
    let mut chain: Vec<i64> = Vec::new();
    let mut cursor: Option<i64> = Some(end);
    let mut seen: HashSet<i64> = HashSet::new();
    while let Some(sid) = cursor {
        if sid == start {
            break;
        }
        if !seen.insert(sid) {
            return Err(SqeError::Execution(format!(
                "FOR INCREMENTAL BETWEEN SNAPSHOT: cycle detected at snapshot {sid}"
            )));
        }
        chain.push(sid);
        let snap = match metadata.snapshot_by_id(sid) {
            Some(s) => s,
            None => {
                return Err(SqeError::Execution(format!(
                    "FOR INCREMENTAL BETWEEN SNAPSHOT: dangling parent pointer at snapshot {sid}"
                )));
            }
        };
        cursor = snap.parent_snapshot_id();
        // If we ran past the start (no more parents) and start != 0, the start
        // snapshot isn't an ancestor of end — a descending or unrelated pair.
        if cursor.is_none() && start != 0 {
            return Err(SqeError::Execution(format!(
                "FOR INCREMENTAL BETWEEN SNAPSHOT: start snapshot {start} is not an ancestor of end {end}"
            )));
        }
    }
    // Reverse so we return snapshots in chronological (oldest first) order.
    chain.reverse();
    Ok(chain)
}

/// Reconcile delete files against data files in the range.
///
/// Returns the subset of delete files whose `referenced_data_file` (position
/// deletes) or equality subject is plausibly resolved inside the window. Pure
/// equality deletes that name no referenced file are retained as-is; the
/// runtime delete-application pipeline evaluates them against matching rows.
///
/// Pre-existing delete files (added outside the window) are not part of
/// `delete_files` by construction: the planner only collects deletes from
/// snapshots in `(start, end]`.
pub fn reconcile_in_range_deletes(
    data_files: &[IncrementalFile],
    delete_files: Vec<IncrementalFile>,
    referenced_data_file: &HashMap<String, Option<String>>,
) -> Vec<IncrementalFile> {
    let in_range: HashSet<&str> = data_files.iter().map(|f| f.path.as_str()).collect();
    delete_files
        .into_iter()
        .filter(|df| {
            match referenced_data_file.get(&df.path) {
                // Position delete with explicit referenced data file: keep
                // only if the target is in range.
                Some(Some(target)) => in_range.contains(target.as_str()),
                // No referenced file (equality delete) or unknown: keep.
                _ => true,
            }
        })
        .collect()
}

/// Walk a snapshot's manifests via the table's object cache and collect
/// added data and delete files.
///
/// Returns two vectors: `(data_files, delete_files)`. Entries whose
/// `ManifestEntry.snapshot_id` does not equal `snapshot.snapshot_id()` are
/// skipped: those files were carried forward, not added in this snapshot.
pub async fn collect_added_files_for_snapshot(
    table: &Table,
    snapshot: &SnapshotRef,
) -> std::result::Result<(Vec<IncrementalFile>, Vec<IncrementalFile>), iceberg::Error> {
    let metadata_ref = table.metadata_ref();
    let cache = table.object_cache();
    let manifest_list = cache.get_manifest_list(snapshot, &metadata_ref).await?;

    let mut data_files: Vec<IncrementalFile> = Vec::new();
    let mut delete_files: Vec<IncrementalFile> = Vec::new();
    let mut data_ordinal: i64 = 0;
    let mut delete_ordinal: i64 = 0;

    for mf in manifest_list.entries() {
        // Only look at manifests added in this snapshot: if `added_snapshot_id`
        // doesn't match, the manifest is carry-over from an older append.
        if mf.added_snapshot_id != snapshot.snapshot_id() {
            continue;
        }
        let manifest = cache.get_manifest(mf).await?;
        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Added {
                continue;
            }
            // Inherit snapshot_id: if the entry's own snapshot_id is None, fall
            // back to the manifest file's added_snapshot_id.
            let entry_snapshot = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
            if entry_snapshot != snapshot.snapshot_id() {
                continue;
            }
            let data_file = entry.data_file();
            match data_file.content_type() {
                DataContentType::Data => {
                    let file = IncrementalFile {
                        path: data_file.file_path().to_string(),
                        size_bytes: data_file.file_size_in_bytes(),
                        snapshot_id: entry_snapshot,
                        kind: ChangeKind::Insert,
                        ordinal: data_ordinal,
                    };
                    data_ordinal += 1;
                    data_files.push(file);
                }
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    let file = IncrementalFile {
                        path: data_file.file_path().to_string(),
                        size_bytes: data_file.file_size_in_bytes(),
                        snapshot_id: entry_snapshot,
                        kind: ChangeKind::Delete,
                        ordinal: delete_ordinal,
                    };
                    delete_ordinal += 1;
                    delete_files.push(file);
                }
            }
        }
    }

    Ok((data_files, delete_files))
}

/// Build an [`IncrementalPlan`] covering `(start, end]` for `table`.
///
/// This is the top-level entry point used by the coordinator after parsing
/// `FOR INCREMENTAL BETWEEN SNAPSHOT start AND SNAPSHOT end`.
pub async fn plan_incremental(
    table: &Table,
    start: i64,
    end: i64,
) -> Result<IncrementalPlan> {
    let metadata = table.metadata();
    let snapshots = resolve_range(metadata, start, end)?;

    // A per-snapshot ordinal sequence is handed out across all files (inserts
    // and deletes) so `_change_ordinal` stays unique within a snapshot.
    let mut ordinals_by_snapshot: BTreeMap<i64, i64> = BTreeMap::new();

    let mut data_files: Vec<IncrementalFile> = Vec::new();
    let mut delete_files: Vec<IncrementalFile> = Vec::new();

    for sid in &snapshots {
        let snap = metadata
            .snapshot_by_id(*sid)
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "FOR INCREMENTAL BETWEEN SNAPSHOT: snapshot {sid} vanished during planning"
                ))
            })?
            .clone();

        let (mut added_data, mut added_deletes) =
            collect_added_files_for_snapshot(table, &snap)
                .await
                .map_err(|e| SqeError::Catalog(format!("Failed to read manifests for snapshot {sid}: {e}")))?;

        // Replace the per-snapshot-local ordinal with a global-per-snapshot
        // ordinal so the meta column is stable regardless of file layout.
        let start_ord = *ordinals_by_snapshot.entry(*sid).or_insert(0);
        for (i, f) in added_data.iter_mut().enumerate() {
            f.ordinal = start_ord + i as i64;
        }
        let next_ord = start_ord + added_data.len() as i64;
        for (i, f) in added_deletes.iter_mut().enumerate() {
            f.ordinal = next_ord + i as i64;
        }
        ordinals_by_snapshot.insert(*sid, next_ord + added_deletes.len() as i64);

        data_files.append(&mut added_data);
        delete_files.append(&mut added_deletes);
    }

    Ok(IncrementalPlan {
        data_files,
        delete_files,
        snapshots_in_range: snapshots,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_plan_for_test(ids: &[i64]) -> Vec<IncrementalFile> {
        ids.iter()
            .enumerate()
            .map(|(i, sid)| IncrementalFile {
                path: format!("f{i}.parquet"),
                size_bytes: 100,
                snapshot_id: *sid,
                kind: ChangeKind::Insert,
                ordinal: i as i64,
            })
            .collect()
    }

    #[test]
    fn reconcile_keeps_in_range_position_deletes() {
        let data = make_plan_for_test(&[101, 101, 102]);
        let deletes = vec![IncrementalFile {
            path: "d0.parquet".into(),
            size_bytes: 50,
            snapshot_id: 102,
            kind: ChangeKind::Delete,
            ordinal: 0,
        }];
        let mut refs = HashMap::new();
        // Delete d0 targets a data file inserted at snapshot 101 (in range).
        refs.insert("d0.parquet".to_string(), Some("f0.parquet".to_string()));
        let kept = reconcile_in_range_deletes(&data, deletes, &refs);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn reconcile_drops_deletes_targeting_out_of_range_data() {
        let data = make_plan_for_test(&[101]);
        let deletes = vec![IncrementalFile {
            path: "d_old.parquet".into(),
            size_bytes: 50,
            snapshot_id: 102,
            kind: ChangeKind::Delete,
            ordinal: 0,
        }];
        let mut refs = HashMap::new();
        // Delete targets a data file outside the range.
        refs.insert(
            "d_old.parquet".to_string(),
            Some("old_data.parquet".to_string()),
        );
        let kept = reconcile_in_range_deletes(&data, deletes, &refs);
        assert_eq!(kept.len(), 0, "delete targeting out-of-range data should drop");
    }

    #[test]
    fn reconcile_keeps_equality_deletes() {
        let data = make_plan_for_test(&[101]);
        let deletes = vec![IncrementalFile {
            path: "eq.parquet".into(),
            size_bytes: 50,
            snapshot_id: 102,
            kind: ChangeKind::Delete,
            ordinal: 0,
        }];
        // Equality delete: no referenced data file.
        let mut refs = HashMap::new();
        refs.insert("eq.parquet".to_string(), None);
        let kept = reconcile_in_range_deletes(&data, deletes, &refs);
        assert_eq!(kept.len(), 1, "equality deletes apply across the window");
    }

    #[test]
    fn change_kind_strings() {
        assert_eq!(ChangeKind::Insert.as_str(), "insert");
        assert_eq!(ChangeKind::Delete.as_str(), "delete");
    }

    // ── resolve_range unit tests ────────────────────────────────────────
    //
    // These build a minimal TableMetadata by deserialising JSON so we don't
    // pay the cost of wiring up every builder knob just to test the walker.

    fn metadata_with_snapshots(chain: &[(i64, Option<i64>)]) -> TableMetadata {
        // chain is ordered oldest first, each as (snapshot_id, parent_snapshot_id).
        let snapshots_json: Vec<String> = chain
            .iter()
            .enumerate()
            .map(|(i, (sid, parent))| {
                let parent_field = parent
                    .map(|p| format!(r#","parent-snapshot-id":{p}"#))
                    .unwrap_or_default();
                let ts = 1_600_000_000_000i64 + i as i64 * 1_000;
                format!(
                    r#"{{"snapshot-id":{sid}{parent_field},"sequence-number":{i},"timestamp-ms":{ts},"manifest-list":"ml_{sid}.avro","summary":{{"operation":"append"}},"schema-id":0}}"#
                )
            })
            .collect();
        let snapshot_log: Vec<String> = chain
            .iter()
            .enumerate()
            .map(|(i, (sid, _))| {
                let ts = 1_600_000_000_000i64 + i as i64 * 1_000;
                format!(r#"{{"snapshot-id":{sid},"timestamp-ms":{ts}}}"#)
            })
            .collect();
        let current = chain.last().map(|(sid, _)| *sid).unwrap_or(0);

        let json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh",
                "last-sequence-number": {last_seq},
                "last-updated-ms": 1600000000000,
                "last-column-id": 1,
                "schemas": [{{"schema-id":0,"type":"struct","fields":[{{"id":1,"name":"id","required":false,"type":"long"}}]}}],
                "current-schema-id": 0,
                "partition-specs": [{{"spec-id":0,"fields":[]}}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {{}},
                "current-snapshot-id": {current},
                "snapshots": [{snapshots}],
                "snapshot-log": [{snapshot_log}],
                "metadata-log": [],
                "sort-orders": [{{"order-id":0,"fields":[]}}],
                "default-sort-order-id": 0,
                "refs": {{}}
            }}"#,
            last_seq = chain.len(),
            current = current,
            snapshots = snapshots_json.join(","),
            snapshot_log = snapshot_log.join(","),
        );
        serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("failed to build test metadata: {e}\nJSON:\n{json}"))
    }

    #[test]
    fn resolve_range_walks_parent_chain() {
        let md = metadata_with_snapshots(&[
            (100, None),
            (101, Some(100)),
            (102, Some(101)),
            (103, Some(102)),
        ]);
        let snaps = resolve_range(&md, 100, 103).unwrap();
        assert_eq!(snaps, vec![101, 102, 103]);
    }

    #[test]
    fn resolve_range_excludes_start_snapshot() {
        // start is exclusive.
        let md = metadata_with_snapshots(&[
            (100, None),
            (101, Some(100)),
            (102, Some(101)),
        ]);
        let snaps = resolve_range(&md, 101, 102).unwrap();
        assert_eq!(snaps, vec![102]);
    }

    #[test]
    fn resolve_range_rejects_missing_end() {
        let md = metadata_with_snapshots(&[(100, None), (101, Some(100))]);
        let err = resolve_range(&md, 100, 99999).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("end snapshot 99999"), "msg: {msg}");
    }

    #[test]
    fn resolve_range_rejects_missing_start() {
        let md = metadata_with_snapshots(&[(100, None), (101, Some(100))]);
        let err = resolve_range(&md, 42, 101).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("start snapshot 42"), "msg: {msg}");
    }

    #[test]
    fn resolve_range_rejects_descending() {
        let md = metadata_with_snapshots(&[(100, None), (101, Some(100))]);
        let err = resolve_range(&md, 101, 100).unwrap_err();
        assert!(err.to_string().contains("start must be older than end"));
    }

    #[test]
    fn resolve_range_equal_start_end_is_empty() {
        // Empty open-closed interval (start == end).
        let md = metadata_with_snapshots(&[(100, None), (101, Some(100))]);
        let snaps = resolve_range(&md, 101, 101).unwrap();
        assert!(snaps.is_empty());
    }

    #[test]
    fn resolve_range_start_zero_walks_to_root() {
        // Sentinel "from the beginning of history".
        let md = metadata_with_snapshots(&[
            (100, None),
            (101, Some(100)),
            (102, Some(101)),
        ]);
        let snaps = resolve_range(&md, 0, 102).unwrap();
        assert_eq!(snaps, vec![100, 101, 102]);
    }
}

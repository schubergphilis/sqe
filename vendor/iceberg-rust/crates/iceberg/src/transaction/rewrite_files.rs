// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Transaction action for rewriting files.
pub struct RewriteFilesAction {
    // snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,

    // VENDOR PATCH (fix/compaction-concurrent-delete-conflict): see the
    // `validate_from_snapshot_id` doc comment on `set_validate_from_snapshot_id`
    // below and the `validate_no_new_position_deletes` free function for the
    // full rationale. Re-apply this field + its plumbing in `commit()` when
    // rebasing the vendored fork.
    validate_from_snapshot_id: Option<i64>,
}

pub struct RewriteFilesOperation;

impl RewriteFilesAction {
    pub fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: false,
            check_file_existence: false,
            validate_from_snapshot_id: None,
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        let target_size_bytes: u32 = properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    // VENDOR PATCH (fix/compaction-concurrent-delete-conflict): new-delete
    // conflict validation for concurrent compaction.
    //
    // WHY: `Transaction::commit` (transaction/mod.rs `do_commit`) reloads the
    // table on a stale base and silently RE-APPLIES this action's stale
    // `RewriteFilesAction::commit` against the freshly reloaded snapshot --
    // there is no built-in check that a *new* delete landed for one of the
    // data files being rewritten. `set_new_data_file_sequence_number` only
    // pins the sequence number of newly-added (compacted) data files, which
    // protects EQUALITY deletes (sequence-number-based matching). It does
    // NOT protect POSITION deletes, which match by file *path*: if a
    // concurrent MoR position delete lands on a data file this rewrite is
    // replacing, mid-commit-window, the rewrite commits its compacted output
    // under a NEW file path while the position delete (which still points at
    // the OLD path) becomes dangling and matches nothing. The deleted rows
    // silently resurrect.
    //
    // FIX: when the caller sets a baseline snapshot id (the snapshot the
    // rewrite was *planned* against), `commit()` scans the CURRENT/reloaded
    // snapshot's delete manifests for any live position-delete entry with a
    // data sequence number newer than the baseline whose `referenced_data_file`
    // is one of this action's `removed_data_files` (or has no
    // `referenced_data_file` at all -- see the multi-file fail-safe on
    // `validate_no_new_position_deletes` below). If found, it returns a
    // conflict `Err` instead of proceeding, so the stale compacted output is
    // never committed. The conflict message text (not `Error::retryable()`)
    // is what SQE's outer re-plan loop keys on -- see
    // `validate_no_new_position_deletes`'s doc comment for why the two
    // retry mechanisms are deliberately decoupled here. This mirrors
    // Iceberg-Java's `SnapshotProducer.validateAddedDataFiles` /
    // `RewriteFiles.validateFromSnapshot` +
    // `MergingSnapshotProducer.validateNoNewDeletesForDataFiles`.
    //
    // SCOPE NOTE: equality deletes are deliberately NOT checked here. Their
    // conflict case is already handled correctly by the seq-pin
    // (`set_new_data_file_sequence_number`): compacted output is written at
    // the *baseline* sequence number, so a newer equality delete continues to
    // apply to both the old and new files exactly as Iceberg's sequence-number
    // semantics intend. Flagging newer equality deletes as conflicts here as
    // well would be defensively "more correct-looking" but is not required
    // for correctness and risks unnecessary retries (false-positive aborts)
    // on workloads that mix compaction with concurrent equality-delete
    // writers. If upstream Iceberg ever tightens this, revisit.
    //
    // BACKWARD COMPATIBILITY: `validate_from_snapshot_id` defaults to `None`
    // (this setter is the only way to set it), so every existing caller and
    // test that never calls it gets byte-identical behavior to pre-patch.
    //
    // REBASE NOTE: this patch touches only `RewriteFilesAction` in this file
    // (struct field + constructor + this setter + the `commit()` call site
    // below) plus the free function `validate_no_new_position_deletes` at the
    // bottom of this file. No other vendored file is touched. Re-apply by
    // porting this whole comment block, the field, the setter, the `commit()`
    // guard, and the free function.
    //
    // UPDATE (multi-file position-delete fail-safe): `validate_no_new_position_deletes`
    // also treats a live, newer-than-baseline `PositionDeletes` entry whose
    // `referenced_data_file()` is `None` as an unconditional conflict. Iceberg-rust's
    // `PositionDeleteFileWriter` only stamps `referenced_data_file` when the writer saw
    // exactly one distinct data-file path over its lifetime
    // (`writer/base_writer/position_delete_file_writer.rs`); a delete spanning >= 2 data
    // files (the common case for a MoR `DELETE` whose predicate matches rows in more than
    // one file) always has `referenced_data_file() == None`. Without this fail-safe such a
    // delete would be silently skipped by the `if let Some(referenced_path) = ...` check
    // below, reopening the exact row-resurrection hole this whole patch exists to close.
    // See `validate_no_new_position_deletes`'s doc comment for the full rationale.
    pub fn set_validate_from_snapshot_id(mut self, snapshot_id: Option<i64>) -> Self {
        self.validate_from_snapshot_id = snapshot_id;
        self
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number(old_entry.file_sequence_number().unwrap())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.table.file_io(),
                    snapshot_produce.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if (entry.content_type() == DataContentType::PositionDeletes
                        || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table_metadata_ref = snapshot_produce.table.metadata();
        let file_io_ref = snapshot_produce.table.file_io();

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(file_io_ref, table_metadata_ref)
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                let survives = |entry: &ManifestEntry| {
                    entry.is_alive() && !found_deleted_files.contains(entry.data_file().file_path())
                };

                if manifest.entries().iter().any(|entry| survives(entry)) {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        manifest_file.content,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Carry survivors forward as `Existing`: `add_entry` would
                        // restamp them as `Added` under the new snapshot and drop
                        // their file sequence number.
                        if survives(entry) {
                            manifest_writer.add_existing_entry((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

#[async_trait::async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // VENDOR PATCH (fix/compaction-concurrent-delete-conflict): validate
        // against the CURRENT/reloaded `table` (this runs on every attempt,
        // including retries against a freshly reloaded base inside
        // `Transaction::do_commit`). See `set_validate_from_snapshot_id` above
        // for the full rationale.
        if let Some(baseline_snapshot_id) = self.validate_from_snapshot_id {
            let removed_data_file_paths: std::collections::HashSet<String> = self
                .removed_data_files
                .iter()
                .map(|df| df.file_path().to_string())
                .collect();
            let target_branch = self.target_branch.as_deref().unwrap_or(MAIN_BRANCH);
            validate_no_new_position_deletes(
                table,
                target_branch,
                baseline_snapshot_id,
                &removed_data_file_paths,
            )
            .await?;
        }

        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager();
        }

        if self.check_file_existence {
            snapshot_producer.validate_data_file_changes().await?;
        }

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer
                .commit(RewriteFilesOperation, process)
                .await
        } else {
            snapshot_producer
                .commit(RewriteFilesOperation, DefaultManifestProcess)
                .await
        }
    }
}

impl Default for RewriteFilesAction {
    fn default() -> Self {
        Self::new()
    }
}

// VENDOR PATCH (fix/compaction-concurrent-delete-conflict): see the doc
// comment on `RewriteFilesAction::set_validate_from_snapshot_id` for the full
// rationale. This is the actual conflict scan.
//
// Compares the CURRENT `target_branch` snapshot (as seen by the in-progress
// commit attempt) against `baseline_snapshot_id` (the snapshot the rewrite
// was planned against). If the current snapshot IS the baseline, there is
// nothing to check (fast path). Otherwise it walks the current snapshot's
// manifest list, loads every `Deletes`-content manifest, and looks for a
// live (`is_alive()`) `PositionDeletes` entry that:
//
//   1. has a data sequence number strictly greater than the baseline
//      snapshot's sequence number (i.e. it was committed *after* the plan
//      baseline, not merely carried forward from before it), AND
//   2. EITHER has a `referenced_data_file` matching one of
//      `removed_data_file_paths` (i.e. it targets one of the data files this
//      rewrite is replacing), OR has `referenced_data_file() == None`.
//
//      The `None` case is a deliberate fail-safe, not an edge case: iceberg-rust's
//      `PositionDeleteFileWriter` (`writer/base_writer/position_delete_file_writer.rs`)
//      only stamps `referenced_data_file` when the writer observed exactly one
//      distinct data-file path over its whole lifetime; a position-delete file
//      spanning two or more data files (the common shape for a MoR `DELETE` whose
//      predicate matches rows across multiple files -- see SQE's
//      `write_handler.rs` MoR delete path, which accumulates matches across ALL
//      scanned data files before writing) always has `referenced_data_file() ==
//      None`. We cannot recover which paths it actually touches without reading
//      the delete file's own `file_path` column (an explicit, costlier follow-up,
//      not done here), so we cannot prove it is disjoint from
//      `removed_data_file_paths`. Treating it as a conflict unconditionally is
//      the only fail-safe option: this converges (the next re-plan captures a
//      newer baseline, so the same delete's sequence number is no longer above
//      it, and the delete-aware read folds it into the fresh compacted output),
//      and it never resurrects rows.
//
// A hit means a concurrent MoR delete landed (or *may* have landed, in the
// `None` case) on a data file mid-compaction; this returns a conflict `Err`
// -- textually flagged as retryable so SQE's outer re-plan loop
// (`classify_commit_error` in `crates/sqe-coordinator/src/maintenance.rs`,
// which keys on the substrings "conflict"/"retry" in the message, not on
// `Error::retryable()`) picks it up immediately, but NOT marked
// `.with_retryable(true)` so the vendored `Transaction::commit` backoff loop
// (`transaction/mod.rs`, which DOES key on `Error::retryable()`) does not
// burn its retry budget re-running the identical doomed commit against an
// unchanged baseline before surfacing to that outer loop -- so the caller
// re-plans rather than committing stale compacted output over an
// undetectable (or unprovably-disjoint) dangling delete.
//
// If `baseline_snapshot_id` is no longer present in the table's snapshot
// history (e.g. concurrently expired by `expire_snapshots`), we cannot prove
// no new deletes landed, so we fail safe and also return a conflict rather
// than silently skip validation.
async fn validate_no_new_position_deletes(
    table: &Table,
    target_branch: &str,
    baseline_snapshot_id: i64,
    removed_data_file_paths: &std::collections::HashSet<String>,
) -> Result<()> {
    if removed_data_file_paths.is_empty() {
        // Nothing is being removed by this rewrite; no data file path can
        // conflict with a new delete.
        return Ok(());
    }

    let Some(current_snapshot) = table.metadata().snapshot_for_ref(target_branch) else {
        // No snapshot on the target branch yet: nothing has been committed
        // since (or ever), so there is nothing to conflict with.
        return Ok(());
    };

    if current_snapshot.snapshot_id() == baseline_snapshot_id {
        // Fast path: no snapshot has landed on this branch since the plan
        // baseline. Skip the manifest scan entirely.
        return Ok(());
    }

    let Some(baseline_snapshot) = table.metadata().snapshot_by_id(baseline_snapshot_id) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Conflict (retryable): cannot validate rewrite against baseline snapshot \
                 {baseline_snapshot_id} -- it is no longer present in the table's snapshot \
                 history (likely expired concurrently). Re-plan the rewrite against the \
                 current snapshot {} so any new deletes are applied to fresh compacted output.",
                current_snapshot.snapshot_id()
            ),
        ));
    };
    let baseline_sequence_number = baseline_snapshot.sequence_number();

    let manifest_list = current_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != DataContentType::PositionDeletes {
                continue;
            }

            let Some(seq) = entry.sequence_number() else {
                continue;
            };
            if seq <= baseline_sequence_number {
                // Carried forward from at or before the plan baseline; not a
                // new delete.
                continue;
            }

            match entry.data_file().referenced_data_file() {
                Some(referenced_path) => {
                    if removed_data_file_paths.contains(&referenced_path) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Conflict (retryable): found new position delete file '{}' \
                                 (sequence number {seq}) committed after baseline snapshot \
                                 {baseline_snapshot_id} (sequence number \
                                 {baseline_sequence_number}) that applies to data file \
                                 '{referenced_path}', which this rewrite is replacing. Re-plan \
                                 the rewrite against the current snapshot {} so the new delete \
                                 is applied to fresh compacted output instead of being silently \
                                 dropped.",
                                entry.file_path(),
                                current_snapshot.snapshot_id()
                            ),
                        ));
                    }
                }
                None => {
                    // Fail-safe: this position delete file's writer observed
                    // more than one distinct data-file path, so iceberg-rust
                    // never stamped `referenced_data_file` (see the doc
                    // comment above this function). We cannot prove it does
                    // not touch any file in `removed_data_file_paths`
                    // without reading its `file_path` column contents (a
                    // costlier follow-up, not done here), so treat it as a
                    // conflict unconditionally rather than silently skip it.
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Conflict (retryable): found new position delete file '{}' \
                             (sequence number {seq}) committed after baseline snapshot \
                             {baseline_snapshot_id} (sequence number {baseline_sequence_number}) \
                             with no `referenced_data_file` (it spans two or more data files), \
                             so it cannot be proven disjoint from the data file(s) this rewrite \
                             is replacing. Re-plan the rewrite against the current snapshot {} \
                             so the new delete is applied to fresh compacted output instead of \
                             risking a silently dropped delete.",
                            entry.file_path(),
                            current_snapshot.snapshot_id()
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::RewriteFilesOperation;
    use crate::spec::{ManifestContentType, ManifestStatus};
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_table_with_delete_manifest, position_delete_file,
    };

    /// Regression test: an rewrite that removes one delete file must not mark
    /// *unrelated* delete files as deleted.
    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = RewriteFilesOperation
            .delete_entries(&producer)
            .await
            .unwrap();
        let deleted_paths: Vec<&str> = deleted_entries
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();

        assert_eq!(
            deleted_paths,
            vec![REMOVED_DELETE_FILE],
            "only the removed delete file should be marked deleted; \
             {RETAINED_DELETE_FILE} must stay live"
        );
    }

    /// Regression test: rewriting a partially-deleted *delete* manifest must
    /// preserve its `Deletes` content type. See the `overwrite_files` twin.
    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = RewriteFilesOperation
            .existing_manifest(&mut producer)
            .await
            .unwrap();

        assert_eq!(existing.len(), 1, "the delete manifest should be rewritten");
        assert_eq!(
            existing[0].content,
            ManifestContentType::Deletes,
            "a rewritten delete manifest must stay a Deletes manifest"
        );

        let entries = existing[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = entries
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();
        assert_eq!(paths, vec![RETAINED_DELETE_FILE]);

        // The survivor is carried forward untouched, not restamped as a new
        // addition of this snapshot.
        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(
            retained.file_sequence_number(),
            Some(PARENT_SEQUENCE_NUMBER)
        );
    }
}

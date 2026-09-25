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
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, SnapshotProducer, data_file_identity};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, Operation, UNASSIGNED_SEQUENCE_NUMBER,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

const DEFAULT_MANIFEST_LOAD_CONCURRENCY: usize = 16;

/// Which snapshot [`Operation`] a file replacement records.
///
/// `rewrite_files` and `overwrite_files` differ only in this value
pub(crate) trait ReplaceFilesMode: Send + Sync + 'static {
    const OPERATION: Operation;
    const ENABLE_APPEND_RETRY_REUSE: bool;
}

/// Files were added and removed without changing table data (compaction,
/// changing file format, relocating files).
pub struct Rewrite;

/// Files were added and removed in a logical overwrite.
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
    const ENABLE_APPEND_RETRY_REUSE: bool = true;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
    const ENABLE_APPEND_RETRY_REUSE: bool = false;
}

/// A blanket `impl<M: ReplaceFilesMode> SnapshotProduceOperation for M` would
/// collide with `impl SnapshotProduceOperation for FastAppendOperation`: the
/// compiler cannot prove `FastAppendOperation` will never implement
/// `ReplaceFilesMode`. This wrapper carries the shared implementation instead.
pub(crate) struct ReplaceFilesOperation<M: ReplaceFilesMode> {
    current_manifests: Option<Vec<ManifestFile>>,
    affected_manifest_paths: Option<HashSet<String>>,
    manifest_load_concurrency: usize,
    deleted_entries: Mutex<Option<Vec<ManifestEntry>>>,
    _mode: PhantomData<M>,
}

impl<M: ReplaceFilesMode> ReplaceFilesOperation<M> {
    pub(crate) fn new() -> Self {
        Self {
            current_manifests: None,
            affected_manifest_paths: None,
            manifest_load_concurrency: DEFAULT_MANIFEST_LOAD_CONCURRENCY,
            deleted_entries: Mutex::new(None),
            _mode: PhantomData,
        }
    }

    fn with_current_manifests(
        current_manifests: Vec<ManifestFile>,
        affected_manifest_paths: Option<HashSet<String>>,
        manifest_load_concurrency: usize,
    ) -> Self {
        Self {
            current_manifests: Some(current_manifests),
            affected_manifest_paths,
            manifest_load_concurrency,
            deleted_entries: Mutex::new(None),
            _mode: PhantomData,
        }
    }

    async fn current_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        if let Some(manifests) = &self.current_manifests {
            return Ok(manifests.clone());
        }

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        Ok(snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .entries()
            .to_vec())
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        if snapshot_produce.removed_data_file_identities.is_empty()
            && snapshot_produce.removed_delete_file_identities.is_empty()
        {
            return Ok(vec![]);
        }

        if let Some(deleted_entries) = self
            .deleted_entries
            .lock()
            .expect("replace-files deleted entries poisoned")
            .as_ref()
            .cloned()
        {
            return Ok(deleted_entries);
        }

        // generate delete manifest entries from removed files
        if snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
            .is_some()
        {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let mut entry = old_entry.as_ref().clone();
                entry.status = ManifestStatus::Deleted;
                entry
            };

            let mut deleted_entries = Vec::new();

            for manifest_file in self.current_manifests(snapshot_produce).await? {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.is_alive()
                        && entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_identities
                            .contains(&data_file_identity(entry.data_file()))
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if entry.is_alive()
                        && (entry.content_type() == DataContentType::PositionDeletes
                            || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_identities
                            .contains(&data_file_identity(entry.data_file()))
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
        if snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
            .is_none()
        {
            return Ok(vec![]);
        }

        let current_manifests = self.current_manifests(snapshot_produce).await?;

        if snapshot_produce.removed_data_file_identities.is_empty()
            && snapshot_produce.removed_delete_file_identities.is_empty()
        {
            return Ok(current_manifests);
        }

        let mut existing_files = Vec::new();
        let mut deleted_entries = Vec::new();
        let file_io = snapshot_produce.table.file_io().clone();
        let mut manifests =
            futures::stream::iter(current_manifests.into_iter().filter(|manifest| {
                // Drop old deletion-only manifests; retained snapshots keep their references.
                // This commit's deletion entries are written separately.
                manifest.has_added_files() || manifest.has_existing_files()
            }))
            .map(|manifest_file| {
                let file_io = file_io.clone();
                let should_load = self
                    .affected_manifest_paths
                    .as_ref()
                    .is_none_or(|paths| paths.contains(&manifest_file.manifest_path));
                async move {
                    let manifest = if should_load {
                        Some(manifest_file.load_manifest(&file_io).await?)
                    } else {
                        None
                    };
                    Result::Ok((manifest_file, manifest))
                }
            })
            .buffered(self.manifest_load_concurrency);

        while let Some(manifest) = manifests.next().await {
            let (manifest_file, manifest) = manifest?;
            let Some(manifest) = manifest else {
                existing_files.push(manifest_file);
                continue;
            };
            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    let identity = data_file_identity(entry.data_file());
                    if entry.is_alive()
                        && (snapshot_produce
                            .removed_data_file_identities
                            .contains(&identity)
                            || snapshot_produce
                                .removed_delete_file_identities
                                .contains(&identity))
                    {
                        let mut deleted_entry = entry.as_ref().clone();
                        deleted_entry.status = ManifestStatus::Deleted;
                        deleted_entries.push(deleted_entry);
                        Some(identity)
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
                    entry.is_alive()
                        && !found_deleted_files.contains(&data_file_identity(entry.data_file()))
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
                            manifest_writer.add_existing_entry(ManifestEntry::clone(entry))?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        *self
            .deleted_entries
            .lock()
            .expect("replace-files deleted entries poisoned") = Some(deleted_entries);

        Ok(existing_files)
    }
}

#[derive(Clone)]
struct PreparedRewriteFiles {
    /// The exact parent manifests whose contents produced `output_manifests`.
    source_manifests: Vec<ManifestFile>,
    output_manifests: Vec<ManifestFile>,
    format_version: FormatVersion,
    last_sequence_number: i64,
}

struct RewriteFilesState {
    /// Identity and output artifacts shared by transaction retry attempts.
    commit_uuid: Option<Uuid>,
    proposed_snapshot_id: Option<i64>,
    manifest_counter: Arc<AtomicU64>,
    manifest_list_attempt: i64,
    prepared: Option<PreparedRewriteFiles>,
}

impl Default for RewriteFilesState {
    fn default() -> Self {
        Self {
            commit_uuid: None,
            proposed_snapshot_id: None,
            manifest_counter: Arc::new(AtomicU64::new(0)),
            manifest_list_attempt: 0,
            prepared: None,
        }
    }
}

/// Transaction action that replaces one set of files with another.
///
/// `M` is sealed to [`Rewrite`] and [`Overwrite`] via the [`RewriteFilesAction`] /
/// [`OverwriteFilesAction`] type aliases below; `ReplaceFilesMode` itself stays
/// `pub(crate)` so no other type can be substituted for `M`.
#[allow(private_bounds)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    delete_file_cleanup_min_data_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,
    manifest_load_concurrency: usize,
    // SQE PATCH (fix/compaction-concurrent-delete-conflict): plan baseline for
    // the new-position-delete conflict check; see `set_validate_from_snapshot_id`.
    validate_from_snapshot_id: Option<i64>,

    state: Mutex<RewriteFilesState>,

    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data — compaction and friends.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Rewrites files as a logical overwrite.
///
/// Manifest merging is enabled by default and can be overridden by snapshot properties.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            delete_file_cleanup_min_data_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: true,
            check_file_existence: false,
            manifest_load_concurrency: DEFAULT_MANIFEST_LOAD_CONCURRENCY,
            validate_from_snapshot_id: None,
            state: Mutex::new(RewriteFilesState::default()),
            _mode: PhantomData,
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

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable or disable filtering obsolete delete entries for this snapshot.
    ///
    /// Filtering is enabled by default so replacing a data file also drops
    /// deletion vectors that can no longer apply to any live file.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
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

    /// Set the minimum data sequence used to retire older delete files.
    ///
    /// If omitted, the minimum sequence from all data manifests is used. An
    /// override must not exceed the data sequence of any live data file to which
    /// an existing delete may apply.
    pub fn set_delete_file_cleanup_min_data_sequence_number(mut self, seq: i64) -> Self {
        self.delete_file_cleanup_min_data_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    /// SQE PATCH (fix/compaction-concurrent-delete-conflict): reject the
    /// commit when a position delete (or deletion vector) newer than
    /// `snapshot_id` targets a data file this action removes.
    ///
    /// `Transaction::commit` re-applies a stale action against a reloaded
    /// table, and nothing else notices that a concurrent merge-on-read DELETE
    /// landed on one of the files being rewritten. Position deletes match by
    /// path, so the rewritten output would silently drop them and the deleted
    /// rows would resurrect. Equality deletes are safe through
    /// `set_new_data_file_sequence_number` and are not checked. A newer
    /// position delete with no `referenced_data_file` spans several data
    /// files and cannot be proven disjoint, so it is always a conflict.
    /// Mirrors Java's `validateFromSnapshot` +
    /// `validateNoNewDeletesForDataFiles`. `None` (the default) skips it.
    pub fn set_validate_from_snapshot_id(mut self, snapshot_id: Option<i64>) -> Self {
        self.validate_from_snapshot_id = snapshot_id;
        self
    }

    /// Set the maximum number of manifest bodies loaded concurrently.
    ///
    /// This limit is currently fully honored only by [`RewriteFilesAction`].
    pub fn set_manifest_load_concurrency(mut self, limit: usize) -> Self {
        assert!(
            limit > 0,
            "manifest load concurrency must be greater than zero"
        );
        self.manifest_load_concurrency = limit;
        self
    }

    async fn current_manifests(&self, table: &Table) -> Result<Vec<ManifestFile>> {
        let target_branch = self
            .target_branch
            .as_deref()
            .unwrap_or(crate::spec::MAIN_BRANCH);
        let Some(snapshot) = table.metadata().snapshot_for_ref(target_branch) else {
            return Ok(vec![]);
        };

        Ok(table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .entries()
            .to_vec())
    }

    fn new_snapshot_producer<'a>(
        &self,
        table: &'a Table,
        commit_uuid: Uuid,
        snapshot_id: Option<i64>,
        manifest_counter: Option<Arc<AtomicU64>>,
    ) -> Result<SnapshotProducer<'a>> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(manifest_counter) = manifest_counter {
            snapshot_producer.set_manifest_counter(manifest_counter);
        }
        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }
        if let Some(seq) = self.delete_file_cleanup_min_data_sequence_number {
            snapshot_producer.set_delete_file_cleanup_min_data_sequence_number(seq);
        }
        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }
        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager(self.manifest_load_concurrency)?;
        }

        Ok(snapshot_producer)
    }

    async fn append_only_delta(
        &self,
        table: &Table,
        prepared: &PreparedRewriteFiles,
        current_manifests: &[ManifestFile],
    ) -> Result<Option<Vec<ManifestFile>>> {
        if table.metadata().format_version() != prepared.format_version {
            return Ok(None);
        }

        let current_by_path: HashMap<&str, &ManifestFile> = current_manifests
            .iter()
            .map(|manifest| (manifest.manifest_path.as_str(), manifest))
            .collect();
        if prepared.source_manifests.iter().any(|source| {
            current_by_path
                .get(source.manifest_path.as_str())
                .is_none_or(|current| *current != source)
        }) {
            return Ok(None);
        }

        let source_paths: HashSet<&str> = prepared
            .source_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();
        let delta: Vec<ManifestFile> = current_manifests
            .iter()
            .filter(|manifest| !source_paths.contains(manifest.manifest_path.as_str()))
            .cloned()
            .collect();

        // Backdated data requires full preparation. Delete manifests are checked entry by entry
        // below because newer equality and referenced position deletes can be carried forward.
        if delta.iter().any(|manifest| {
            manifest.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER
                || match manifest.content {
                    ManifestContentType::Data => {
                        manifest.min_sequence_number < prepared.last_sequence_number
                    }
                    ManifestContentType::Deletes => {
                        manifest.min_sequence_number <= prepared.last_sequence_number
                    }
                }
        }) {
            return Ok(None);
        }

        let removed_identities: HashSet<_> = self
            .removed_data_files
            .iter()
            .chain(self.removed_delete_files.iter())
            .map(data_file_identity)
            .collect();
        let removed_data_paths: HashSet<&str> = self
            .removed_data_files
            .iter()
            .map(|file| file.file_path())
            .collect();
        let added_identities: HashSet<_> = self
            .added_data_files
            .iter()
            .chain(self.added_delete_files.iter())
            .map(data_file_identity)
            .collect();

        for manifest_file in &delta {
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Added {
                    return Ok(None);
                }
                let file = entry.data_file();
                if removed_identities.contains(&data_file_identity(file))
                    || removed_data_paths.contains(file.file_path())
                    || (self.check_file_existence
                        && added_identities.contains(&data_file_identity(file)))
                {
                    return Ok(None);
                }

                if manifest_file.content == ManifestContentType::Deletes {
                    match file.content_type() {
                        DataContentType::EqualityDeletes => {
                            let Some(new_data_sequence_number) = self.new_data_file_sequence_number
                            else {
                                return Ok(None);
                            };
                            if entry.sequence_number().is_none_or(|sequence_number| {
                                sequence_number <= new_data_sequence_number
                            }) {
                                return Ok(None);
                            }
                        }
                        DataContentType::PositionDeletes => {
                            if file.referenced_data_file().is_none_or(|referenced| {
                                removed_data_paths.contains(referenced.as_str())
                            }) {
                                return Ok(None);
                            }
                        }
                        DataContentType::Data => return Ok(None),
                    }
                }
            }
        }

        Ok(Some(delta))
    }
}

#[async_trait::async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if let Some(snapshot_id) = self.snapshot_id
            && table.metadata().snapshot_by_id(snapshot_id).is_some()
        {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!("Snapshot id {snapshot_id} already exists"),
            ));
        }

        // SQE PATCH (fix/compaction-concurrent-delete-conflict): runs on every
        // attempt, including retries against a reloaded base.
        if let Some(baseline_snapshot_id) = self.validate_from_snapshot_id {
            let removed_data_file_paths: HashSet<String> = self
                .removed_data_files
                .iter()
                .map(|df| df.file_path().to_string())
                .collect();
            let target_branch = self
                .target_branch
                .as_deref()
                .unwrap_or(crate::spec::MAIN_BRANCH);
            validate_no_new_position_deletes(
                table,
                target_branch,
                baseline_snapshot_id,
                &removed_data_file_paths,
            )
            .await?;
        }

        if let Some(sequence_number) = self.new_data_file_sequence_number {
            let next_sequence_number = table.metadata().next_sequence_number();
            if sequence_number < 0 || sequence_number > next_sequence_number {
                return Err(crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "New data file sequence number {sequence_number} must be between 0 and \
                         the new snapshot sequence number {next_sequence_number}"
                    ),
                ));
            }
        }

        if let Some(sequence_number) = self.delete_file_cleanup_min_data_sequence_number {
            let last_sequence_number = table.metadata().last_sequence_number();
            if sequence_number < 0 || sequence_number > last_sequence_number {
                return Err(crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!(
                        "Delete file cleanup minimum data sequence number {sequence_number} must \
                         be between 0 and the table's last sequence number {last_sequence_number}"
                    ),
                ));
            }

            if !self.added_data_files.is_empty() {
                let added_data_sequence_number = self
                    .new_data_file_sequence_number
                    .unwrap_or(last_sequence_number);
                if sequence_number > added_data_sequence_number {
                    return Err(crate::Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!(
                            "Delete file cleanup minimum data sequence number {sequence_number} \
                             must not exceed the added data file sequence number \
                             {added_data_sequence_number}"
                        ),
                    ));
                }
            }
        }

        if !M::ENABLE_APPEND_RETRY_REUSE {
            // TODO: Propagate manifest_load_concurrency through overwrite validation and manifest
            // processing instead of using their independent defaults.
            let snapshot_producer = self.new_snapshot_producer(
                table,
                self.commit_uuid.unwrap_or_else(Uuid::now_v7),
                self.snapshot_id,
                None,
            )?;
            snapshot_producer.validate_added_files(&self.added_data_files)?;
            snapshot_producer.validate_added_files(&self.added_delete_files)?;
            if self.check_file_existence {
                snapshot_producer.validate_data_file_changes().await?;
            }
            return snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), DefaultManifestProcess)
                .await;
        }

        let (commit_uuid, proposed_snapshot_id, manifest_counter, prepared, manifest_list_attempt) = {
            let mut state = self.state.lock().expect("rewrite-files state poisoned");
            let commit_uuid = self
                .commit_uuid
                .or(state.commit_uuid)
                .unwrap_or_else(Uuid::now_v7);
            state.commit_uuid = Some(commit_uuid);
            let manifest_list_attempt = state.manifest_list_attempt;
            state.manifest_list_attempt += 1;
            (
                commit_uuid,
                self.snapshot_id.or(state.proposed_snapshot_id),
                Arc::clone(&state.manifest_counter),
                state.prepared.clone(),
                manifest_list_attempt,
            )
        };

        let mut snapshot_producer = self.new_snapshot_producer(
            table,
            commit_uuid,
            proposed_snapshot_id,
            Some(manifest_counter),
        )?;
        {
            let mut state = self.state.lock().expect("rewrite-files state poisoned");
            state.proposed_snapshot_id = Some(snapshot_producer.snapshot_id());
        }

        snapshot_producer.validate_added_files(&self.added_data_files)?;
        snapshot_producer.validate_added_files(&self.added_delete_files)?;

        let current_manifests = self.current_manifests(table).await?;
        let is_retry = prepared.is_some();

        if let Some(prepared) = prepared
            && let Some(delta) = self
                .append_only_delta(table, &prepared, &current_manifests)
                .await?
        {
            let summary = snapshot_producer
                .prepare_summary(&ReplaceFilesOperation::<M>::new())
                .map_err(|err| {
                    crate::Error::new(
                        crate::ErrorKind::Unexpected,
                        "Failed to create snapshot summary.",
                    )
                    .with_source(err)
                })?;
            let mut output_manifests = prepared.output_manifests;
            output_manifests.extend(delta);
            {
                let mut state = self.state.lock().expect("rewrite-files state poisoned");
                state.prepared = Some(PreparedRewriteFiles {
                    source_manifests: current_manifests,
                    output_manifests: output_manifests.clone(),
                    format_version: table.metadata().format_version(),
                    last_sequence_number: table.metadata().last_sequence_number(),
                });
            }
            return snapshot_producer
                .commit_prepared(output_manifests, summary, manifest_list_attempt)
                .await;
        }

        // A cache-invalidating retry must prove that every planned input still
        // exists before it can safely add the already-produced replacement.
        let affected_manifest_paths = if self.check_file_existence {
            Some(
                snapshot_producer
                    .validate_data_file_changes_with_manifests(
                        &current_manifests,
                        self.manifest_load_concurrency,
                    )
                    .await?,
            )
        } else if is_retry {
            Some(
                snapshot_producer
                    .validate_removed_data_files_with_manifests(
                        &current_manifests,
                        self.manifest_load_concurrency,
                    )
                    .await?,
            )
        } else {
            None
        };
        let operation = ReplaceFilesOperation::<M>::with_current_manifests(
            current_manifests.clone(),
            affected_manifest_paths,
            self.manifest_load_concurrency,
        );
        let summary = snapshot_producer
            .prepare_summary(&operation)
            .map_err(|err| {
                crate::Error::new(
                    crate::ErrorKind::Unexpected,
                    "Failed to create snapshot summary.",
                )
                .with_source(err)
            })?;
        let output_manifests = snapshot_producer
            .prepare_manifests(&operation, &DefaultManifestProcess)
            .await?;
        {
            let mut state = self.state.lock().expect("rewrite-files state poisoned");
            state.prepared = Some(PreparedRewriteFiles {
                source_manifests: current_manifests,
                output_manifests: output_manifests.clone(),
                format_version: table.metadata().format_version(),
                last_sequence_number: table.metadata().last_sequence_number(),
            });
        }

        snapshot_producer
            .commit_prepared(output_manifests, summary, manifest_list_attempt)
            .await
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// SQE PATCH (fix/compaction-concurrent-delete-conflict): the conflict scan
/// behind [`ReplaceFilesAction::set_validate_from_snapshot_id`].
///
/// Scans the current branch snapshot's delete manifests for a live position
/// delete (deletion vectors included) committed after the baseline that
/// targets a removed data file, or that has no `referenced_data_file`. A
/// baseline that is no longer in the snapshot history is also a conflict,
/// because nothing can then be proven. The error is `DataInvalid` with
/// "Conflict (retryable)" in its text and is deliberately NOT
/// `with_retryable(true)`: SQE's re-plan loop keys on the text, and the
/// transaction's own backoff loop must not re-run the identical doomed commit.
async fn validate_no_new_position_deletes(
    table: &Table,
    target_branch: &str,
    baseline_snapshot_id: i64,
    removed_data_file_paths: &HashSet<String>,
) -> Result<()> {
    if removed_data_file_paths.is_empty() {
        return Ok(());
    }
    let Some(current_snapshot) = table.metadata().snapshot_for_ref(target_branch) else {
        return Ok(());
    };
    if current_snapshot.snapshot_id() == baseline_snapshot_id {
        return Ok(());
    }
    let Some(baseline_snapshot) = table.metadata().snapshot_by_id(baseline_snapshot_id) else {
        return Err(crate::Error::new(
            crate::ErrorKind::DataInvalid,
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

    let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;
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
                continue;
            }
            let conflict = match entry.data_file().referenced_data_file() {
                Some(referenced_path) if removed_data_file_paths.contains(&referenced_path) => {
                    format!("that applies to data file '{referenced_path}', which this rewrite is replacing")
                }
                Some(_) => continue,
                None => "with no `referenced_data_file` (it spans two or more data files), so it \
                         cannot be proven disjoint from the data file(s) this rewrite is replacing"
                    .to_string(),
            };
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "Conflict (retryable): found new position delete file '{}' (sequence number \
                     {seq}) committed after baseline snapshot {baseline_snapshot_id} (sequence \
                     number {baseline_sequence_number}) {conflict}. Re-plan the rewrite against \
                     the current snapshot {} so the new delete is applied to fresh compacted \
                     output instead of being silently dropped.",
                    entry.file_path(),
                    current_snapshot.snapshot_id()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use uuid::Uuid;

    use super::{
        Overwrite, PreparedRewriteFiles, ReplaceFilesMode, ReplaceFilesOperation, Rewrite,
    };
    use crate::catalog::MockCatalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile, ManifestListWriter,
        ManifestStatus, ManifestWriterBuilder, Operation, Snapshot, SnapshotRef, SnapshotReference,
        SnapshotRetention, Struct, Summary, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::test_utils::make_encrypted_table;
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_minimal_table, make_v2_table_with_delete_manifest, make_v3_minimal_table,
        make_v3_minimal_table_in_catalog, position_delete_file,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

    fn retry_test_table() -> Table {
        let base = make_v2_minimal_table();
        let metadata_location =
            "memory:///test/location/metadata/1-00000000-0000-0000-0000-000000000001.metadata.json";
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some(metadata_location.to_string()))
            .set_location("memory:///test/location".to_string())
            .set_properties(HashMap::from([
                ("commit.retry.min-wait-ms".to_string(), "1".to_string()),
                ("commit.retry.max-wait-ms".to_string(), "1".to_string()),
                (
                    "commit.retry.total-timeout-ms".to_string(),
                    "1000".to_string(),
                ),
                ("commit.retry.num-retries".to_string(), "2".to_string()),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        base.with_metadata(Arc::new(metadata))
            .with_metadata_location(metadata_location.to_string())
    }

    fn retry_test_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn retry_test_equality_delete_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    fn retry_test_position_delete_file(path: &str, referenced_data_file: Option<&str>) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(referenced_data_file.map(str::to_string))
            .build()
            .unwrap()
    }

    async fn write_retry_test_manifest(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        sequence_number: i64,
        files: Vec<DataFile>,
    ) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            table.file_io().new_output(path).unwrap(),
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for file in files {
            writer
                .add_existing_file(file, snapshot_id, sequence_number, Some(sequence_number))
                .unwrap();
        }
        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = sequence_number;
        manifest
    }

    async fn write_retry_test_added_manifest(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        sequence_number: i64,
        file: DataFile,
    ) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            table.file_io().new_output(path).unwrap(),
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        writer.add_file(file, sequence_number).unwrap();
        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = sequence_number;
        manifest
    }

    async fn write_retry_test_added_delete_manifest(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        sequence_number: i64,
        file: DataFile,
    ) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            table.file_io().new_output(path).unwrap(),
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();
        writer.add_file(file, sequence_number).unwrap();
        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = sequence_number;
        manifest
    }

    async fn write_retry_test_snapshot(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        sequence_number: i64,
        parent_snapshot_id: Option<i64>,
        manifests: Vec<ManifestFile>,
        total_data_files: usize,
    ) -> Snapshot {
        let mut writer = ManifestListWriter::v2(
            table
                .file_io()
                .new_output(path)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();

        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_timestamp_ms(table.metadata().last_updated_ms() + snapshot_id)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([
                    ("total-data-files".to_string(), total_data_files.to_string()),
                    (
                        "total-records".to_string(),
                        (total_data_files * 10).to_string(),
                    ),
                    (
                        "total-files-size".to_string(),
                        (total_data_files * 100).to_string(),
                    ),
                ]),
            })
            .build()
    }

    fn retry_test_table_at_snapshot(base: &Table, snapshot: Snapshot) -> Table {
        let snapshot_id = snapshot.snapshot_id();
        let metadata_location = format!(
            "memory:///test/location/metadata/{snapshot_id}-00000000-0000-0000-0000-000000000001.metadata.json"
        );
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some(metadata_location.clone()))
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        base.clone()
            .with_metadata(Arc::new(metadata))
            .with_metadata_location(metadata_location)
    }

    fn committed_snapshot(mut commit: crate::transaction::ActionCommit) -> Snapshot {
        commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .expect("replace-files commit should add a snapshot")
    }

    async fn live_data_file_paths(table: &Table, snapshot: &Snapshot) -> Vec<String> {
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut paths = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Data)
        {
            paths.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive())
                    .map(|entry| entry.data_file().file_path().to_string()),
            );
        }
        paths.sort_unstable();
        paths
    }

    async fn delete_file_statuses(table: &Table, snapshot: &Snapshot) -> Vec<ManifestStatus> {
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut statuses = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| entry.status()),
            );
        }
        statuses
    }

    #[test]
    fn test_modes_map_to_their_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
        assert_eq!(
            ReplaceFilesOperation::<Rewrite>::new().operation(),
            Operation::Replace
        );
        assert_eq!(
            ReplaceFilesOperation::<Overwrite>::new().operation(),
            Operation::Overwrite
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_reuses_prepared_manifests_after_multiple_appends() {
        // S101 contains the files selected for compaction.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/removed.parquet");
        let retained = retry_test_data_file("test/retained.parquet");
        let replacement = retry_test_data_file("test/replacement.parquet");
        let original_manifest_path = "memory:///test/location/metadata/original.avro";
        let original_manifest =
            write_retry_test_manifest(&base, original_manifest_path, 101, 1, vec![
                removed.clone(),
                retained,
            ])
            .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-101.avro",
            101,
            1,
            None,
            vec![original_manifest.clone()],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);

        // The first action invocation prepares and caches all replacement manifests.
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .set_check_file_existence(true)
                .delete_files([removed])
                .add_data_files([replacement]),
        );
        let first_snapshot =
            committed_snapshot(Arc::clone(&action).commit(&table_v1).await.unwrap());
        let first_output_paths = {
            let state = action.state.lock().unwrap();
            state
                .prepared
                .as_ref()
                .unwrap()
                .output_manifests
                .iter()
                .map(|manifest| manifest.manifest_path.clone())
                .collect::<HashSet<_>>()
        };

        // S102 advances the table with an independent data append only.
        let appended_file = retry_test_data_file("test/appended.parquet");
        let appended_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/appended.avro",
            102,
            2,
            appended_file,
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-102.avro",
            102,
            2,
            Some(101),
            vec![original_manifest.clone(), appended_manifest.clone()],
            3,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // A reusable retry must not open any original manifest body. Removing it
        // makes an accidental full scan fail while leaving its list entry intact.
        table_v2
            .file_io()
            .delete(original_manifest_path)
            .await
            .unwrap();
        let second_snapshot =
            committed_snapshot(Arc::clone(&action).commit(&table_v2).await.unwrap());

        // The retry rebases the same proposed snapshot onto S102 with a new manifest list.
        assert_eq!(second_snapshot.snapshot_id(), first_snapshot.snapshot_id());
        assert_eq!(second_snapshot.parent_snapshot_id(), Some(102));
        assert_eq!(second_snapshot.sequence_number(), 3);
        assert_ne!(
            second_snapshot.manifest_list(),
            first_snapshot.manifest_list()
        );

        let second_manifest_list = table_v2
            .manifest_list_reader(&SnapshotRef::new(second_snapshot.clone()))
            .load()
            .await
            .unwrap();
        let second_paths = second_manifest_list
            .entries()
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<HashSet<_>>();

        // Cached rewrite output is preserved and the concurrent append is carried forward.
        assert!(first_output_paths.is_subset(&second_paths));
        assert!(second_paths.contains(&appended_manifest.manifest_path));
        assert_eq!(second_paths.len(), first_output_paths.len() + 1);

        // S103 adds another independent manifest after the first rebase.
        let second_appended_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/second-appended.avro",
            103,
            3,
            retry_test_data_file("test/second-appended.parquet"),
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v2,
            "memory:///test/location/metadata/list-103.avro",
            103,
            3,
            Some(102),
            vec![
                original_manifest,
                appended_manifest.clone(),
                second_appended_manifest.clone(),
            ],
            4,
        )
        .await;
        let table_v3 = retry_test_table_at_snapshot(&table_v2, snapshot);
        let third_snapshot =
            committed_snapshot(Arc::clone(&action).commit(&table_v3).await.unwrap());

        assert_eq!(third_snapshot.snapshot_id(), first_snapshot.snapshot_id());
        assert_eq!(third_snapshot.parent_snapshot_id(), Some(103));
        assert_eq!(third_snapshot.sequence_number(), 4);
        assert_ne!(
            third_snapshot.manifest_list(),
            second_snapshot.manifest_list()
        );

        let third_manifest_list = table_v3
            .manifest_list_reader(&SnapshotRef::new(third_snapshot.clone()))
            .load()
            .await
            .unwrap();
        let third_paths = third_manifest_list
            .entries()
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<HashSet<_>>();
        assert!(first_output_paths.is_subset(&third_paths));
        assert!(third_paths.contains(&appended_manifest.manifest_path));
        assert!(third_paths.contains(&second_appended_manifest.manifest_path));
        assert_eq!(third_paths.len(), first_output_paths.len() + 2);
        assert_eq!(
            live_data_file_paths(&table_v3, &third_snapshot).await,
            vec![
                "test/appended.parquet".to_string(),
                "test/replacement.parquet".to_string(),
                "test/retained.parquet".to_string(),
                "test/second-appended.parquet".to_string(),
            ]
        );
        assert_eq!(
            third_snapshot
                .summary()
                .additional_properties
                .get("total-data-files"),
            Some(&"4".to_string())
        );
        assert_eq!(
            third_snapshot
                .summary()
                .additional_properties
                .get("total-records"),
            Some(&"40".to_string())
        );
        assert_eq!(
            third_snapshot
                .summary()
                .additional_properties
                .get("total-files-size"),
            Some(&"400".to_string())
        );
    }

    #[tokio::test]
    async fn test_rewrite_retry_rejects_concurrently_appended_replacement() {
        let base = retry_test_table();
        let removed = retry_test_data_file("test/overlap-removed.parquet");
        let replacement = retry_test_data_file("test/overlap-replacement.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/overlap-original.avro",
            104,
            1,
            vec![removed.clone()],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-104.avro",
            104,
            1,
            None,
            vec![original_manifest.clone()],
            1,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .set_check_file_existence(true)
                .delete_files([removed])
                .add_data_files([replacement.clone()]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();

        let overlapping_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/overlap-appended.avro",
            105,
            2,
            replacement,
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-105.avro",
            105,
            2,
            Some(104),
            vec![original_manifest, overlapping_manifest],
            2,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        let err = match Arc::clone(&action).commit(&table_v2).await {
            Ok(_) => panic!("retry with an already-live replacement should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("already referenced by table"));
    }

    #[tokio::test]
    async fn test_rewrite_validation_reuses_current_manifest_list() {
        let base = retry_test_table();
        let removed = retry_test_data_file("test/validation-removed.parquet");
        let manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/validation-source.avro",
            111,
            1,
            vec![removed.clone()],
        )
        .await;
        let manifest_list_path = "memory:///test/location/metadata/validation-list.avro";
        let snapshot = write_retry_test_snapshot(
            &base,
            manifest_list_path,
            111,
            1,
            None,
            vec![manifest.clone()],
            1,
        )
        .await;
        let table = retry_test_table_at_snapshot(&base, snapshot);
        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![removed],
            vec![],
        );

        table.file_io().delete(manifest_list_path).await.unwrap();
        let affected = producer
            .validate_data_file_changes_with_manifests(&[manifest], 1)
            .await
            .unwrap();
        assert_eq!(
            affected,
            HashSet::from(["memory:///test/location/metadata/validation-source.avro".to_string()])
        );
    }

    #[tokio::test]
    async fn test_rewrite_collects_deleted_entries_while_rewriting_survivors() {
        let base = retry_test_table();
        let removed = retry_test_data_file("test/fused-removed.parquet");
        let retained = retry_test_data_file("test/fused-retained.parquet");
        let manifest_path = "memory:///test/location/metadata/fused-source.avro";
        let manifest = write_retry_test_manifest(&base, manifest_path, 112, 1, vec![
            removed.clone(),
            retained,
        ])
        .await;
        let unaffected_manifest_path =
            "memory:///test/location/metadata/fused-unaffected-source.avro";
        let unaffected_manifest =
            write_retry_test_manifest(&base, unaffected_manifest_path, 112, 1, vec![
                retry_test_data_file("test/fused-unaffected.parquet"),
            ])
            .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/fused-list.avro",
            112,
            1,
            None,
            vec![manifest.clone(), unaffected_manifest.clone()],
            3,
        )
        .await;
        let table = retry_test_table_at_snapshot(&base, snapshot);
        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![removed],
            vec![],
        );
        let operation = ReplaceFilesOperation::<Rewrite>::with_current_manifests(
            vec![manifest, unaffected_manifest.clone()],
            Some(HashSet::from([manifest_path.to_string()])),
            1,
        );

        // Unaffected descriptors must be carried forward without opening their bodies.
        table
            .file_io()
            .delete(unaffected_manifest_path)
            .await
            .unwrap();
        let existing = operation.existing_manifest(&mut producer).await.unwrap();
        assert_eq!(existing.len(), 2);
        assert_eq!(existing[1], unaffected_manifest);

        // The deleted entry must come from the survivor pass, not a second source read.
        table.file_io().delete(manifest_path).await.unwrap();
        let deleted_entries = operation.delete_entries(&producer).await.unwrap();
        assert_eq!(deleted_entries.len(), 1);
        assert_eq!(deleted_entries[0].status(), ManifestStatus::Deleted);
        assert_eq!(
            deleted_entries[0].data_file().file_path(),
            "test/fused-removed.parquet"
        );
        assert_eq!(deleted_entries[0].snapshot_id(), Some(112));
        assert_eq!(deleted_entries[0].sequence_number(), Some(1));
        assert_eq!(deleted_entries[0].file_sequence_number, Some(1));
    }

    #[tokio::test]
    async fn test_transaction_retries_rewrite_files_without_rereading_source_manifests() {
        // S301 is the table state used to prepare the first rewrite attempt.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/transaction-removed.parquet");
        let retained = retry_test_data_file("test/transaction-retained.parquet");
        let replacement = retry_test_data_file("test/transaction-replacement.parquet");
        let original_manifest_path = "memory:///test/location/metadata/transaction-original.avro";
        let original_manifest =
            write_retry_test_manifest(&base, original_manifest_path, 301, 1, vec![
                removed.clone(),
                retained,
            ])
            .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-301.avro",
            301,
            1,
            None,
            vec![original_manifest.clone()],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);

        // S302 simulates the concurrent append that wins the catalog race.
        let appended_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/transaction-appended.avro",
            302,
            2,
            retry_test_data_file("test/transaction-appended.parquet"),
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-302.avro",
            302,
            2,
            Some(301),
            vec![original_manifest, appended_manifest],
            3,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // The transaction sees S301 initially, then refreshes to S302 after the conflict.
        let load_attempt = Arc::new(AtomicU32::new(0));
        let mut catalog = MockCatalog::new();
        let load_attempt_ref = Arc::clone(&load_attempt);
        let load_v1 = table_v1.clone();
        let load_v2 = table_v2.clone();
        catalog.expect_load_table().times(2).returning_st(move |_| {
            let table = if load_attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                load_v1.clone()
            } else {
                load_v2.clone()
            };
            Box::pin(async move { Ok(table) })
        });

        // After attempt one has prepared its output, remove the source object as a
        // tripwire: retry succeeds only if it reuses the cache and reads just the append.
        let update_attempt = Arc::new(AtomicU32::new(0));
        let update_attempt_ref = Arc::clone(&update_attempt);
        let file_io = table_v2.file_io().clone();
        let committed_table = table_v2.clone();
        catalog
            .expect_update_table()
            .times(2)
            .returning_st(move |commit| {
                let attempt = update_attempt_ref.fetch_add(1, Ordering::SeqCst);
                let file_io = file_io.clone();
                let committed_table = committed_table.clone();
                Box::pin(async move {
                    if attempt == 0 {
                        file_io.delete(original_manifest_path).await.unwrap();
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "injected conflict")
                                .with_retryable(true),
                        )
                    } else {
                        commit.apply(committed_table)
                    }
                })
            });

        // Exercise the real transaction retry loop rather than replaying the action directly.
        let tx = Transaction::new(&table_v1);
        let tx = tx
            .rewrite_files()
            .set_check_file_existence(true)
            .delete_files([removed])
            .add_data_files([replacement])
            .apply(tx)
            .unwrap();
        let committed_table = tx.commit(&catalog).await.unwrap();
        let snapshot = committed_table.metadata().current_snapshot().unwrap();

        assert_eq!(snapshot.parent_snapshot_id(), Some(302));
        assert_eq!(snapshot.sequence_number(), 3);
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        assert_eq!(
            live_data_file_paths(&committed_table, snapshot).await,
            vec![
                "test/transaction-appended.parquet".to_string(),
                "test/transaction-replacement.parquet".to_string(),
                "test/transaction-retained.parquet".to_string(),
            ]
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-data-files")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-records")
                .map(String::as_str),
            Some("30")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-files-size")
                .map(String::as_str),
            Some("300")
        );
        assert_eq!(load_attempt.load(Ordering::SeqCst), 2);
        assert_eq!(update_attempt.load(Ordering::SeqCst), 2);
    }

    // Test to verify that rewrite files action reruns full preparations if the latest
    // changes to the table invalidate the cached state of the transaction.
    #[tokio::test]
    async fn test_transaction_reprepares_rewrite_files_when_conflict_replaces_source_manifest() {
        // S401 is the table state used for the first rewrite preparation.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/hard-refresh-removed.parquet");
        let retained = retry_test_data_file("test/hard-refresh-retained.parquet");
        let replacement = retry_test_data_file("test/hard-refresh-replacement.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/hard-refresh-original.avro",
            401,
            1,
            vec![removed.clone(), retained.clone()],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-401.avro",
            401,
            1,
            None,
            vec![original_manifest],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);

        // The conflicting commit rewrites the source manifest while preserving
        // the files. Its descriptor no longer matches the cached source.
        let rewritten_source = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/hard-refresh-rewritten-source.avro",
            402,
            2,
            vec![removed.clone(), retained],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-402.avro",
            402,
            2,
            Some(401),
            vec![rewritten_source.clone()],
            2,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // Keep the action handle so both attempts' prepared output can be compared.
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .set_check_file_existence(true)
                .delete_files([removed])
                .add_data_files([replacement]),
        );
        let first_output_paths = Arc::new(Mutex::new(None));

        // The retry loop loads S401 first, then the conflicting S402 state.
        let load_attempt = Arc::new(AtomicU32::new(0));
        let mut catalog = MockCatalog::new();
        let load_attempt_ref = Arc::clone(&load_attempt);
        let load_v1 = table_v1.clone();
        let load_v2 = table_v2.clone();
        catalog.expect_load_table().times(2).returning_st(move |_| {
            let table = if load_attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                load_v1.clone()
            } else {
                load_v2.clone()
            };
            Box::pin(async move { Ok(table) })
        });

        // Capture attempt one's output before injecting the catalog conflict.
        let update_attempt = Arc::new(AtomicU32::new(0));
        let update_attempt_ref = Arc::clone(&update_attempt);
        let action_ref = Arc::clone(&action);
        let first_output_paths_ref = Arc::clone(&first_output_paths);
        let committed_table = table_v2.clone();
        catalog
            .expect_update_table()
            .times(2)
            .returning_st(move |_| {
                let attempt = update_attempt_ref.fetch_add(1, Ordering::SeqCst);
                let committed_table = committed_table.clone();
                if attempt == 0 {
                    let paths = action_ref
                        .state
                        .lock()
                        .unwrap()
                        .prepared
                        .as_ref()
                        .unwrap()
                        .output_manifests
                        .iter()
                        .map(|manifest| manifest.manifest_path.clone())
                        .collect::<HashSet<_>>();
                    *first_output_paths_ref.lock().unwrap() = Some(paths);
                }
                Box::pin(async move {
                    if attempt == 0 {
                        Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            "injected invalidating conflict",
                        )
                        .with_retryable(true))
                    } else {
                        Ok(committed_table)
                    }
                })
            });

        // Run the transaction through both attempts using the same stateful action.
        let mut tx = Transaction::new(&table_v1);
        tx.actions.push(action.clone());
        tx.commit(&catalog).await.unwrap();

        let first_output_paths = first_output_paths.lock().unwrap().take().unwrap();
        let state = action.state.lock().unwrap();
        let prepared = state.prepared.as_ref().unwrap();

        // S402's source and disjoint outputs prove the retry performed a full preparation.
        assert_eq!(prepared.source_manifests, vec![rewritten_source]);
        let retry_output_paths = prepared
            .output_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<HashSet<_>>();
        assert!(first_output_paths.is_disjoint(&retry_output_paths));
        assert_eq!(load_attempt.load(Ordering::SeqCst), 2);
        assert_eq!(update_attempt.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_rewrite_files_reprepares_when_source_manifest_changes() {
        // Prepare the rewrite and retain its first set of output manifest paths.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/removed.parquet");
        let retained = retry_test_data_file("test/retained.parquet");
        let replacement = retry_test_data_file("test/replacement.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/original-fallback.avro",
            201,
            1,
            vec![removed.clone(), retained.clone()],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-201.avro",
            201,
            1,
            None,
            vec![original_manifest.clone()],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .set_check_file_existence(true)
                .delete_files([removed.clone()])
                .add_data_files([replacement]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();
        let first_output_paths = {
            let state = action.state.lock().unwrap();
            state
                .prepared
                .as_ref()
                .unwrap()
                .output_manifests
                .iter()
                .map(|manifest| manifest.manifest_path.clone())
                .collect::<HashSet<_>>()
        };

        // Replace the source manifest without changing its live files.
        let rewritten_source = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/rewritten-source.avro",
            202,
            2,
            vec![removed, retained],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-202.avro",
            202,
            2,
            Some(201),
            vec![rewritten_source.clone()],
            2,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // Replaying the same action must discard the cache and prepare from the new source.
        Arc::clone(&action).commit(&table_v2).await.unwrap();

        let state = action.state.lock().unwrap();
        let prepared = state.prepared.as_ref().unwrap();
        assert_eq!(prepared.source_manifests, vec![rewritten_source]);
        let second_output_paths = prepared
            .output_manifests
            .iter()
            .map(|manifest| manifest.manifest_path.clone())
            .collect::<HashSet<_>>();
        assert!(first_output_paths.is_disjoint(&second_output_paths));
    }

    #[tokio::test]
    async fn test_rewrite_files_reprepare_rejects_missing_input() {
        // Prime the cache without opting into first-attempt existence validation.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/missing-retry-input.parquet");
        let retained = retry_test_data_file("test/missing-retry-retained.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/missing-retry-original.avro",
            501,
            1,
            vec![removed.clone(), retained.clone()],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-501.avro",
            501,
            1,
            None,
            vec![original_manifest],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .delete_files([removed])
                .add_data_files([retry_test_data_file(
                    "test/missing-retry-replacement.parquet",
                )]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();

        // A concurrent rewrite removes the selected input before this action retries.
        let rewritten_source = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/missing-retry-new-source.avro",
            502,
            2,
            vec![retained],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-502.avro",
            502,
            2,
            Some(501),
            vec![rewritten_source],
            1,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // Cache misses always validate removals, preventing duplicate replacement data.
        let err = match Arc::clone(&action).commit(&table_v2).await {
            Ok(_) => panic!("retry with a missing input should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("not in the target branch"));
    }

    #[tokio::test]
    async fn test_rewrite_files_retry_reuses_new_equality_delete_manifest() {
        // Prepare replacement data before any concurrent row-level delete exists.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/delete-conflict-input.parquet");
        let retained = retry_test_data_file("test/delete-conflict-retained.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/delete-conflict-original.avro",
            601,
            1,
            vec![removed.clone(), retained],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-601.avro",
            601,
            1,
            None,
            vec![original_manifest.clone()],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .set_new_data_file_sequence_number(1)
                .delete_files([removed])
                .add_data_files([retry_test_data_file(
                    "test/delete-conflict-replacement.parquet",
                )]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();

        // The refreshed table includes concurrently added data and equality-delete manifests.
        let appended_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/concurrent-data.avro",
            602,
            2,
            retry_test_data_file("test/concurrent-data.parquet"),
        )
        .await;
        let delete_manifest = write_retry_test_added_delete_manifest(
            &base,
            "memory:///test/location/metadata/concurrent-delete.avro",
            602,
            2,
            retry_test_equality_delete_file("test/concurrent-equality-delete.parquet"),
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-602.avro",
            602,
            2,
            Some(601),
            vec![
                original_manifest.clone(),
                appended_manifest.clone(),
                delete_manifest.clone(),
            ],
            3,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // A newer equality delete still applies to replacement data written at sequence 1. Remove
        // the original body as a tripwire proving the retry reads only the delta manifest.
        table_v2
            .file_io()
            .delete(&original_manifest.manifest_path)
            .await
            .unwrap();
        let retry_snapshot =
            committed_snapshot(Arc::clone(&action).commit(&table_v2).await.unwrap());
        let manifest_list = table_v2
            .manifest_list_reader(&SnapshotRef::new(retry_snapshot))
            .load()
            .await
            .unwrap();
        assert!(manifest_list.entries().contains(&appended_manifest));
        assert!(manifest_list.entries().contains(&delete_manifest));
    }

    #[tokio::test]
    async fn test_append_only_delta_classifies_delete_manifests() {
        let table = retry_test_table();
        let removed = retry_test_data_file("test/delete-delta-removed.parquet");
        let prepared = PreparedRewriteFiles {
            source_manifests: vec![],
            output_manifests: vec![],
            format_version: FormatVersion::V2,
            last_sequence_number: 1,
        };
        let equality_manifest = write_retry_test_added_delete_manifest(
            &table,
            "memory:///test/location/metadata/delete-delta-equality.avro",
            621,
            2,
            retry_test_equality_delete_file("test/delete-delta-equality.parquet"),
        )
        .await;
        let referenced_position_manifest = write_retry_test_added_delete_manifest(
            &table,
            "memory:///test/location/metadata/delete-delta-referenced-position.avro",
            621,
            2,
            retry_test_position_delete_file(
                "test/delete-delta-referenced-position.parquet",
                Some("test/delete-delta-unrelated.parquet"),
            ),
        )
        .await;
        let removed_position_manifest = write_retry_test_added_delete_manifest(
            &table,
            "memory:///test/location/metadata/delete-delta-removed-position.avro",
            621,
            2,
            retry_test_position_delete_file(
                "test/delete-delta-removed-position.parquet",
                Some(removed.file_path()),
            ),
        )
        .await;
        let unreferenced_position_manifest = write_retry_test_added_delete_manifest(
            &table,
            "memory:///test/location/metadata/delete-delta-unreferenced-position.avro",
            621,
            2,
            retry_test_position_delete_file(
                "test/delete-delta-unreferenced-position.parquet",
                None,
            ),
        )
        .await;

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(1)
            .delete_files([removed.clone()]);
        assert!(
            action
                .append_only_delta(&table, &prepared, std::slice::from_ref(&equality_manifest))
                .await
                .unwrap()
                .is_some(),
            "a newer equality delete applies to replacement data at sequence 1"
        );
        assert!(
            action
                .append_only_delta(&table, &prepared, &[referenced_position_manifest])
                .await
                .unwrap()
                .is_some(),
            "a referenced position delete for an unrelated file is reusable"
        );
        assert!(
            action
                .append_only_delta(&table, &prepared, &[removed_position_manifest])
                .await
                .unwrap()
                .is_none(),
            "a position delete for a removed input requires full preparation"
        );
        assert!(
            action
                .append_only_delta(&table, &prepared, &[unreferenced_position_manifest])
                .await
                .unwrap()
                .is_none(),
            "an unreferenced position delete requires full preparation"
        );

        let action_without_sequence = Transaction::new(&table)
            .rewrite_files()
            .delete_files([removed.clone()]);
        assert!(
            action_without_sequence
                .append_only_delta(&table, &prepared, std::slice::from_ref(&equality_manifest))
                .await
                .unwrap()
                .is_none(),
            "equality-delete reuse requires an explicit replacement data sequence"
        );

        let action_at_delete_sequence = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(2)
            .delete_files([removed]);
        assert!(
            action_at_delete_sequence
                .append_only_delta(&table, &prepared, &[equality_manifest])
                .await
                .unwrap()
                .is_none(),
            "an equality delete must be strictly newer than replacement data"
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_retry_reprepares_after_removed_delete_manifest() {
        let base = retry_test_table();
        let removed = retry_test_data_file("test/removed-delete-conflict-input.parquet");
        let retained = retry_test_data_file("test/removed-delete-conflict-retained.parquet");
        let data_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/removed-delete-conflict-data.avro",
            611,
            1,
            vec![removed.clone(), retained],
        )
        .await;
        let delete_manifest = write_retry_test_added_delete_manifest(
            &base,
            "memory:///test/location/metadata/removed-delete-conflict-delete.avro",
            611,
            1,
            position_delete_file(&base, "test/removed-delete-conflict-position.parquet"),
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-611.avro",
            611,
            1,
            None,
            vec![data_manifest.clone(), delete_manifest.clone()],
            2,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .delete_files([removed])
                .add_data_files([retry_test_data_file(
                    "test/removed-delete-conflict-replacement.parquet",
                )]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();

        // An ordinary append remains safe while the exact delete descriptor is unchanged.
        let appended_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/removed-delete-conflict-appended.avro",
            612,
            2,
            retry_test_data_file("test/removed-delete-conflict-appended.parquet"),
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-612.avro",
            612,
            2,
            Some(611),
            vec![
                data_manifest.clone(),
                delete_manifest,
                appended_manifest.clone(),
            ],
            3,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);
        Arc::clone(&action).commit(&table_v2).await.unwrap();

        // Removing the descriptor can make rows visible after replacement data was produced.
        let snapshot = write_retry_test_snapshot(
            &table_v2,
            "memory:///test/location/metadata/list-613.avro",
            613,
            3,
            Some(612),
            vec![data_manifest.clone(), appended_manifest.clone()],
            3,
        )
        .await;
        let table_v3 = retry_test_table_at_snapshot(&table_v2, snapshot);
        let previous_output_paths = {
            let state = action.state.lock().unwrap();
            state
                .prepared
                .as_ref()
                .unwrap()
                .output_manifests
                .iter()
                .map(|manifest| manifest.manifest_path.clone())
                .collect::<HashSet<_>>()
        };
        Arc::clone(&action).commit(&table_v3).await.unwrap();

        let state = action.state.lock().unwrap();
        let prepared = state.prepared.as_ref().unwrap();
        assert_eq!(prepared.source_manifests, vec![
            data_manifest,
            appended_manifest
        ]);
        assert!(
            prepared
                .output_manifests
                .iter()
                .any(|manifest| !previous_output_paths.contains(&manifest.manifest_path)),
            "a removed source delete manifest should force full preparation"
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_reprepare_rejects_duplicate_removed_identity() {
        // Prime the cache with one live occurrence of the selected input file.
        let base = retry_test_table();
        let removed = retry_test_data_file("test/duplicate-retry-input.parquet");
        let original_manifest = write_retry_test_manifest(
            &base,
            "memory:///test/location/metadata/duplicate-retry-original.avro",
            701,
            1,
            vec![removed.clone()],
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &base,
            "memory:///test/location/metadata/list-701.avro",
            701,
            1,
            None,
            vec![original_manifest.clone()],
            1,
        )
        .await;
        let table_v1 = retry_test_table_at_snapshot(&base, snapshot);
        let action = Arc::new(
            Transaction::new(&table_v1)
                .rewrite_files()
                .delete_files([removed.clone()])
                .add_data_files([retry_test_data_file(
                    "test/duplicate-retry-replacement.parquet",
                )]),
        );
        Arc::clone(&action).commit(&table_v1).await.unwrap();

        // A concurrent append introduces a second live entry for the same identity.
        let duplicate_manifest = write_retry_test_added_manifest(
            &base,
            "memory:///test/location/metadata/duplicate-retry-added.avro",
            702,
            2,
            removed,
        )
        .await;
        let snapshot = write_retry_test_snapshot(
            &table_v1,
            "memory:///test/location/metadata/list-702.avro",
            702,
            2,
            Some(701),
            vec![original_manifest, duplicate_manifest],
            2,
        )
        .await;
        let table_v2 = retry_test_table_at_snapshot(&table_v1, snapshot);

        // Full preparation rejects the invalid duplicate instead of corrupting totals.
        let err = match Arc::clone(&action).commit(&table_v2).await {
            Ok(_) => panic!("retry with duplicate rewrite inputs should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
        assert!(err.message().contains("referenced multiple times"));
    }

    /// A logical overwrite may replace only part of a table. Its summary must account for the
    /// files actually removed instead of treating every overwrite as a full-table truncate.
    #[tokio::test]
    async fn test_partial_overwrite_summary_rolls_totals_forward() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let make_file = |path: String, record_count: u64, file_size_in_bytes: u64| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path)
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size_in_bytes)
                .record_count(record_count)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .build()
                .unwrap()
        };

        let parent_files = (0..5)
            .map(|index| make_file(format!("test/old-{index}.parquet"), 20, 200))
            .collect::<Vec<_>>();
        let removed_file = parent_files[0].clone();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(parent_files)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let replacement = make_file("test/replacement.parquet".to_string(), 10, 100);
        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite_files()
            .add_data_files([replacement])
            .delete_files([removed_file])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let summary = table.metadata().current_snapshot().unwrap().summary();
        assert_eq!(summary.operation, Operation::Overwrite);
        let properties = &summary.additional_properties;

        assert_eq!(
            properties.get("added-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            properties.get("deleted-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            properties.get("deleted-records").map(String::as_str),
            Some("20")
        );
        assert_eq!(
            properties.get("removed-files-size").map(String::as_str),
            Some("200")
        );
        assert_eq!(
            properties.get("total-data-files").map(String::as_str),
            Some("5")
        );
        assert_eq!(
            properties.get("total-records").map(String::as_str),
            Some("90")
        );
        assert_eq!(
            properties.get("total-files-size").map(String::as_str),
            Some("900")
        );
    }

    /// Regression test: a rewrite/overwrite that removes one delete file must not
    /// mark *unrelated* delete files as deleted.
    ///
    /// `delete_entries` once guarded the delete-file branch with
    ///   `content == PositionDeletes || content == EqualityDeletes && removed.contains(path)`
    /// and because `&&` binds tighter than `||`, every `PositionDeletes` entry in
    /// the parent snapshot matched regardless of the requested delete-file identities.
    async fn assert_only_removed_delete_files_marked<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = ReplaceFilesOperation::<M>::new()
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
    /// preserve its `Deletes` content type, and must carry survivors forward as
    /// `Existing` rather than restamping them as `Added`.
    async fn assert_delete_manifest_carried_forward_intact<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = ReplaceFilesOperation::<M>::new()
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

        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(retained.file_sequence_number, Some(PARENT_SEQUENCE_NUMBER));
    }

    #[tokio::test]
    async fn test_overwrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_replace_commit_preserves_delete_manifest_semantics() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);
        let snapshot_id = SnapshotProducer::generate_unique_snapshot_id(&table);
        let action = Transaction::new(&table)
            .overwrite_files()
            .set_snapshot_id(snapshot_id)
            .set_check_file_existence(true)
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };

        assert_eq!(snapshot.snapshot_id(), snapshot_id);
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-delete-files")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-position-deletes")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("total-files-size")
                .map(String::as_str),
            Some("100")
        );
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .all(|manifest| manifest.content == ManifestContentType::Deletes)
        );

        let mut entries = Vec::new();
        for manifest_file in manifest_list.entries() {
            entries.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.file_path().to_string(), entry.status())),
            );
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(entries, vec![
            (REMOVED_DELETE_FILE.to_string(), ManifestStatus::Deleted),
            (RETAINED_DELETE_FILE.to_string(), ManifestStatus::Existing),
        ]);
    }

    #[tokio::test]
    async fn test_replace_uses_requested_data_sequence_number() {
        let table = make_v2_table_with_delete_manifest().await;
        let added_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let added_delete_file = position_delete_file(&table, "test/new-position-delete.parquet");
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .add_data_files([added_file, added_delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let added_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| manifest.content == ManifestContentType::Data)
            .unwrap();
        let manifest = added_manifest.load_manifest(table.file_io()).await.unwrap();
        let entry = &manifest.entries()[0];

        assert_eq!(entry.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(entry.file_sequence_number, Some(snapshot.sequence_number()));

        let added_delete_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| {
                manifest.content == ManifestContentType::Deletes
                    && manifest.added_snapshot_id == snapshot.snapshot_id()
            })
            .unwrap();
        let manifest = added_delete_manifest
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(
            manifest.entries()[0].sequence_number(),
            Some(snapshot.sequence_number()),
            "the data-file sequence override must not backdate newly written delete files"
        );
    }

    #[tokio::test]
    async fn test_delete_cleanup_sequence_override() {
        let table = make_v2_table_with_delete_manifest().await;
        let parent = table.metadata().current_snapshot().unwrap();
        let later_snapshot_id = PARENT_SNAPSHOT_ID + 1;
        let later_snapshot = Snapshot::builder()
            .with_snapshot_id(later_snapshot_id)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(parent.timestamp_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER + 2)
            .with_schema_id(parent.schema_id().unwrap())
            .with_manifest_list(parent.manifest_list())
            .with_summary(parent.summary().clone())
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(later_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    later_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let added_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/compacted.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .add_data_files([added_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let delete_statuses = delete_file_statuses(&table, snapshot).await;

        assert_eq!(delete_statuses.len(), 2);
        assert!(
            delete_statuses
                .iter()
                .all(|status| *status != ManifestStatus::Deleted),
            "delete files newer than the explicitly retained data sequence must stay live"
        );

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .set_delete_file_cleanup_min_data_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .add_data_files([added_file.clone()]);
        let err = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("cleanup sequence newer than added data should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("must not exceed"));

        let action = Transaction::new(&table)
            .rewrite_files()
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .set_delete_file_cleanup_min_data_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .add_data_files([added_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let delete_statuses = delete_file_statuses(&table, snapshot).await;
        assert!(
            delete_statuses
                .iter()
                .all(|status| *status == ManifestStatus::Deleted)
        );
    }

    #[tokio::test]
    async fn test_replace_rejects_invalid_data_sequence_number() {
        let table = make_v2_minimal_table();
        let next_sequence_number = table.metadata().next_sequence_number();

        for sequence_number in [-1, next_sequence_number + 1] {
            let action = Transaction::new(&table)
                .rewrite_files()
                .set_new_data_file_sequence_number(sequence_number);
            let err = match Arc::new(action).commit(&table).await {
                Ok(_) => panic!("invalid data sequence number should fail"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains("must be between 0"));
        }
    }

    #[tokio::test]
    async fn test_replace_preserves_non_default_partition_spec_id() {
        let base = make_v2_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .set_location("memory:///test/location".to_string())
            .add_partition_spec(UnboundPartitionSpec::builder().with_spec_id(1).build())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));
        assert_eq!(table.metadata().default_partition_spec_id(), 1);

        let old_spec = table.metadata().partition_spec_by_id(0).unwrap().clone();
        let make_old_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(0)
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .build()
                .unwrap()
        };
        let removed = make_old_file("test/old-spec-removed.parquet");
        let survivor = make_old_file("test/old-spec-survivor.parquet");
        let manifest_path = "memory:///test/location/metadata/old-spec-data.avro";
        let mut manifest_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(manifest_path).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            old_spec.as_ref().clone(),
        )
        .build_v2_data();
        for file in [removed.clone(), survivor.clone()] {
            manifest_writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(file)
                        .build(),
                )
                .unwrap();
        }
        let manifest_file = manifest_writer.write_manifest_file().await.unwrap();

        let manifest_list_path = "memory:///test/location/metadata/old-spec-manifest-list.avro";
        let output = table
            .file_io()
            .new_output(manifest_list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_writer =
            ManifestListWriter::v2(output, PARENT_SNAPSHOT_ID, None, PARENT_SEQUENCE_NUMBER);
        manifest_list_writer
            .add_manifests([manifest_file].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let parent_snapshot = Snapshot::builder()
            .with_snapshot_id(PARENT_SNAPSHOT_ID)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER)
            .with_schema_id(table.metadata().current_schema_id())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-data-files".to_string(), "2".to_string()),
                    ("total-records".to_string(), "2".to_string()),
                    ("total-files-size".to_string(), "200".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(parent_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();

        assert!(
            manifest_list
                .entries()
                .iter()
                .all(|manifest| manifest.partition_spec_id == 0),
            "rewritten and deleted-entry manifests must use the source files' spec, not default spec 1"
        );
        let mut statuses = Vec::new();
        for manifest_file in manifest_list.entries() {
            statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.file_path().to_string(), entry.status())),
            );
        }
        statuses.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(statuses, vec![
            (
                "test/old-spec-removed.parquet".to_string(),
                ManifestStatus::Deleted,
            ),
            (
                "test/old-spec-survivor.parquet".to_string(),
                ManifestStatus::Existing,
            ),
        ]);
    }

    #[tokio::test]
    async fn test_replace_existence_validation_rejects_missing_file() {
        let table = make_v2_minimal_table();
        let missing_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/missing.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_check_file_existence(true)
            .delete_files([missing_file]);
        let err = match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("missing delete file should fail validation"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("branch with no snapshot"));
    }

    #[tokio::test]
    async fn test_replace_uses_target_branch_parent_and_requirement() {
        let table = make_v2_table_with_delete_manifest().await;
        let main_snapshot_id = PARENT_SNAPSHOT_ID + 1;
        let parent = table.metadata().snapshot_by_id(PARENT_SNAPSHOT_ID).unwrap();
        let main_snapshot = Snapshot::builder()
            .with_snapshot_id(main_snapshot_id)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(parent.timestamp_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .with_schema_id(parent.schema_id().unwrap())
            .with_manifest_list(parent.manifest_list())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: parent.summary().additional_properties.clone(),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v2.json".to_string()))
            .add_snapshot(main_snapshot)
            .unwrap()
            .set_ref(
                "staging",
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    main_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);
        let action = Transaction::new(&table)
            .rewrite_files()
            .set_target_branch("staging".to_string())
            .delete_files([removed]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            unreachable!()
        };

        assert_eq!(snapshot.parent_snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert!(matches!(
            &updates[1],
            TableUpdate::SetSnapshotRef { ref_name, .. } if ref_name == "staging"
        ));
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: "staging".to_string(),
            snapshot_id: Some(PARENT_SNAPSHOT_ID),
        });
    }

    #[tokio::test]
    async fn test_replace_writes_encrypted_manifest_and_manifest_list() {
        let table = make_encrypted_table().await;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/encrypted-replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::empty())
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .add_data_files([data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        assert!(snapshot.encryption_key_id().is_some());

        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let data_manifest = manifest_list
            .entries()
            .iter()
            .find(|manifest| manifest.content == ManifestContentType::Data)
            .unwrap();
        assert!(data_manifest.key_metadata.is_some());
        let manifest = data_manifest.load_manifest(table.file_io()).await.unwrap();
        let mut expected_data_file = data_file;
        expected_data_file.first_row_id = Some(0);
        assert_eq!(manifest.entries()[0].data_file(), &expected_data_file);
    }

    #[tokio::test]
    async fn test_replace_drops_only_dangling_dv_from_shared_puffin() {
        const DATA_A: &str = "test/data-a.parquet";
        const DATA_B: &str = "test/data-b.parquet";
        const PUFFIN: &str = "test/shared.puffin";
        const DATA_MANIFEST: &str = "memory:///test/location/metadata/data-manifest.avro";
        const DELETE_MANIFEST: &str = "memory:///test/location/metadata/delete-manifest.avro";
        const MANIFEST_LIST: &str = "memory:///test/location/metadata/manifest-list.avro";

        let base = make_v3_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .set_location("memory:///test/location".to_string())
            .build()
            .unwrap()
            .metadata;
        let table = base.with_metadata(Arc::new(metadata));
        let make_data_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(10)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .build()
                .unwrap()
        };
        let data_a = make_data_file(DATA_A);
        let data_b = make_data_file(DATA_B);
        let make_dv = |referenced_data_file: &str, offset: i64| {
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(PUFFIN.to_string())
                .file_format(DataFileFormat::Puffin)
                .file_size_in_bytes(256)
                .record_count(1)
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .partition(Struct::from_iter([Some(Literal::long(300))]))
                .referenced_data_file(Some(referenced_data_file.to_string()))
                .content_offset(Some(offset))
                .content_size_in_bytes(Some(64))
                .build()
                .unwrap()
        };
        let dv_a = make_dv(DATA_A, 4);
        let dv_b = make_dv(DATA_B, 68);

        let mut data_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(DATA_MANIFEST).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        for file in [data_a.clone(), data_b] {
            data_writer
                .add_existing_file(
                    file,
                    PARENT_SNAPSHOT_ID,
                    PARENT_SEQUENCE_NUMBER,
                    Some(PARENT_SEQUENCE_NUMBER),
                )
                .unwrap();
        }
        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        let mut delete_writer = ManifestWriterBuilder::new(
            table.file_io().new_output(DELETE_MANIFEST).unwrap(),
            Some(PARENT_SNAPSHOT_ID),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_deletes();
        for file in [dv_a.clone(), dv_b] {
            delete_writer
                .add_existing_file(
                    file,
                    PARENT_SNAPSHOT_ID,
                    PARENT_SEQUENCE_NUMBER,
                    Some(PARENT_SEQUENCE_NUMBER),
                )
                .unwrap();
        }
        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        let manifest_list_output = table
            .file_io()
            .new_output(MANIFEST_LIST)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut manifest_list_writer = ManifestListWriter::v3(
            manifest_list_output,
            PARENT_SNAPSHOT_ID,
            None,
            PARENT_SEQUENCE_NUMBER,
            Some(0),
        );
        manifest_list_writer
            .add_manifests([data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let parent_snapshot = Snapshot::builder()
            .with_snapshot_id(PARENT_SNAPSHOT_ID)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER)
            .with_schema_id(table.metadata().current_schema_id())
            .with_manifest_list(MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [
                    ("total-data-files".to_string(), "2".to_string()),
                    ("total-delete-files".to_string(), "2".to_string()),
                    ("total-records".to_string(), "20".to_string()),
                    ("total-files-size".to_string(), "712".to_string()),
                    ("total-position-deletes".to_string(), "2".to_string()),
                ]
                .into_iter()
                .collect(),
            })
            .with_row_range(0, 20)
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("memory:///test/location/metadata/v1.json".to_string()))
            .add_snapshot(parent_snapshot)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(
                    PARENT_SNAPSHOT_ID,
                    SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let replacement = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/replacement.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(200)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let action = Transaction::new(&table)
            .rewrite_files()
            .add_data_files([replacement])
            .delete_files([data_a]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();

        let mut surviving_data_row_id = None;
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Data)
        {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            surviving_data_row_id = manifest
                .entries()
                .iter()
                .find(|entry| entry.is_alive() && entry.data_file().file_path() == DATA_B)
                .map(|entry| entry.data_file().first_row_id)
                .or(surviving_data_row_id);
        }
        assert_eq!(surviving_data_row_id, Some(Some(10)));

        let mut dv_entries = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            dv_entries.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| {
                        (
                            entry.data_file().referenced_data_file(),
                            entry.data_file().content_offset(),
                            entry.status(),
                        )
                    }),
            );
        }
        dv_entries.sort_unstable_by_key(|entry| entry.1);

        assert_eq!(dv_entries, vec![
            (Some(DATA_A.to_string()), Some(4), ManifestStatus::Deleted),
            (Some(DATA_B.to_string()), Some(68), ManifestStatus::Existing),
        ]);

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_files([dv_a]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let snapshot = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        let manifest_list = table
            .manifest_list_reader(&SnapshotRef::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut offsets_and_statuses = Vec::new();
        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|manifest| manifest.content == ManifestContentType::Deletes)
        {
            offsets_and_statuses.extend(
                manifest_file
                    .load_manifest(table.file_io())
                    .await
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|entry| (entry.data_file().content_offset().unwrap(), entry.status())),
            );
        }
        offsets_and_statuses.sort_unstable_by_key(|entry| entry.0);

        assert_eq!(offsets_and_statuses, vec![
            (4, ManifestStatus::Deleted),
            (68, ManifestStatus::Existing),
        ]);
    }
}

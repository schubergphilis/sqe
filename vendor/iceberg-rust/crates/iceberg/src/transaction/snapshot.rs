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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, PartitionSpec, Snapshot, SnapshotReference,
    SnapshotRetention, SnapshotSummaryCollector, Struct, StructType, Summary, TableProperties,
    Transform, UNASSIGNED_SEQUENCE_NUMBER, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::{
    ActionCommit, MANIFEST_MERGE_ENABLED, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT, ManifestFilterManager, ManifestWriterContext,
};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

pub(crate) type DataFileIdentity = (String, Option<i64>, Option<i64>);

pub(crate) fn data_file_identity(file: &DataFile) -> DataFileIdentity {
    (
        file.file_path().to_string(),
        file.content_offset(),
        file.content_size_in_bytes(),
    )
}

pub(crate) fn format_data_file_identity(identity: &DataFileIdentity) -> String {
    match (identity.1, identity.2) {
        (Some(offset), Some(length)) => {
            format!("{} (offset: {offset}, length: {length})", identity.0)
        }
        _ => identity.0.clone(),
    }
}

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method is intended for future delete
///    operations to specify which manifest entries should be marked as deleted.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile>;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    pub(crate) added_data_files: Vec<DataFile>,
    pub(crate) added_delete_files: Vec<DataFile>,
    pub(crate) removed_data_file_paths: HashSet<String>,
    pub(crate) removed_data_file_identities: HashSet<DataFileIdentity>,
    pub(crate) removed_delete_file_identities: HashSet<DataFileIdentity>,
    pub(crate) removed_data_files: Vec<DataFile>,
    pub(crate) removed_delete_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It is shared with ManifestWriterContext to avoid naming conflicts.
    manifest_counter: Arc<AtomicU64>,
    new_data_file_sequence_number: Option<i64>,
    delete_file_cleanup_min_data_sequence_number: Option<i64>,
    target_branch: String,
    delete_filter_manager: Option<ManifestFilterManager>,
}

impl<'a> SnapshotProducer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        snapshot_id: Option<i64>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
        added_delete_files: Vec<DataFile>,
        removed_data_files: Vec<DataFile>,
        removed_delete_files: Vec<DataFile>,
    ) -> Self {
        let removed_data_file_paths = removed_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect();
        let removed_data_file_identities =
            removed_data_files.iter().map(data_file_identity).collect();
        let removed_delete_file_identities = removed_delete_files
            .iter()
            .map(data_file_identity)
            .collect();

        Self {
            table,
            snapshot_id: snapshot_id.unwrap_or_else(|| Self::generate_unique_snapshot_id(table)),
            commit_uuid,
            snapshot_properties,
            added_data_files,
            added_delete_files,
            removed_data_file_paths,
            removed_data_file_identities,
            removed_delete_file_identities,
            removed_data_files,
            removed_delete_files,
            manifest_counter: Arc::new(AtomicU64::new(0)),
            new_data_file_sequence_number: None,
            delete_file_cleanup_min_data_sequence_number: None,
            target_branch: MAIN_BRANCH.to_string(),
            delete_filter_manager: None,
        }
    }

    pub(crate) fn validate_added_files(&self, files: &[DataFile]) -> Result<()> {
        let is_v3 = self.table.metadata().format_version() >= FormatVersion::V3;
        for data_file in files {
            // SQE PATCH (V3 deletion vectors): "Position delete files must not
            // be added to v3 tables" (spec, Row-level Deletes). A V3 position
            // delete is a deletion vector: a Puffin blob that names its data
            // file. Refuse anything else so no writer can produce a
            // non-compliant table.
            if is_v3
                && data_file.content_type() == DataContentType::PositionDeletes
                && (data_file.file_format() != DataFileFormat::Puffin
                    || data_file.referenced_data_file().is_none())
            {
                return Err(invalid_data!(
                    "Position delete files must not be added to v3 tables; write a deletion \
                     vector instead ({})",
                    data_file.file_path()
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(invalid_data!(
                    "Data file partition spec id does not match table default partition spec id"
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
                self.table.metadata().default_partition_spec(),
            )?;
        }

        Ok(())
    }

    pub(crate) async fn validate_data_file_changes(&self) -> Result<()> {
        self.validate_data_file_changes_impl(true, None, None)
            .await
            .map(|_| ())
    }

    pub(crate) async fn validate_data_file_changes_with_manifests(
        &self,
        manifest_files: &[ManifestFile],
        concurrency_limit: usize,
    ) -> Result<HashSet<String>> {
        self.validate_data_file_changes_impl(true, Some(manifest_files), Some(concurrency_limit))
            .await
    }

    pub(crate) async fn validate_removed_data_files_with_manifests(
        &self,
        manifest_files: &[ManifestFile],
        concurrency_limit: usize,
    ) -> Result<HashSet<String>> {
        self.validate_data_file_changes_impl(false, Some(manifest_files), Some(concurrency_limit))
            .await
    }

    async fn validate_data_file_changes_impl(
        &self,
        validate_additions: bool,
        manifest_files: Option<&[ManifestFile]>,
        concurrency_limit: Option<usize>,
    ) -> Result<HashSet<String>> {
        let mut files_to_delete: HashSet<DataFileIdentity> = self
            .removed_data_files
            .iter()
            .chain(self.removed_delete_files.iter())
            .map(data_file_identity)
            .collect();
        let requested_deletes = files_to_delete.clone();
        let files_to_add: HashSet<DataFileIdentity> = if validate_additions {
            self.added_data_files
                .iter()
                .chain(self.added_delete_files.iter())
                .map(data_file_identity)
                .collect()
        } else {
            HashSet::new()
        };

        if files_to_add.is_empty() && files_to_delete.is_empty() {
            return Ok(HashSet::new());
        }

        let Some(snapshot) = self.table.metadata().snapshot_for_ref(&self.target_branch) else {
            if files_to_delete.is_empty() {
                return Ok(HashSet::new());
            }
            let mut paths = files_to_delete
                .iter()
                .map(format_data_file_identity)
                .collect::<Vec<_>>();
            paths.sort_unstable();
            return Err(invalid_data!(
                "Cannot delete files from a branch with no snapshot, files: {}",
                paths.join(", ")
            ));
        };

        let manifest_files = if let Some(manifest_files) = manifest_files {
            manifest_files.to_vec()
        } else {
            self.table
                .manifest_list_reader(snapshot)
                .load()
                .await?
                .entries()
                .to_vec()
        };
        let concurrency_limit = concurrency_limit.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
        });
        let mut manifests = futures::stream::iter(manifest_files)
            .map(|manifest_file| {
                let reader = self.table.manifest_reader();
                async move {
                    let manifest_path = manifest_file.manifest_path.clone();
                    let manifest = reader.read(&manifest_file).await?;
                    Result::Ok((manifest_path, manifest))
                }
            })
            .buffer_unordered(concurrency_limit);
        let mut duplicate_files = HashSet::new();
        let mut found_deletes = HashSet::new();
        let mut duplicate_deletes = HashSet::new();
        let mut affected_manifests = HashSet::new();

        while let Some(manifest) = manifests.next().await {
            let (manifest_path, manifest) = manifest?;
            let mut manifest_is_affected = false;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                let identity = data_file_identity(entry.data_file());
                if files_to_add.contains(&identity) {
                    duplicate_files.insert(identity.clone());
                }
                if requested_deletes.contains(&identity) && !found_deletes.insert(identity.clone())
                {
                    duplicate_deletes.insert(identity.clone());
                }
                if files_to_delete.remove(&identity) || requested_deletes.contains(&identity) {
                    manifest_is_affected = true;
                }
            }
            if manifest_is_affected {
                affected_manifests.insert(manifest_path);
            }
        }

        if !duplicate_files.is_empty() {
            let mut paths = duplicate_files
                .iter()
                .map(format_data_file_identity)
                .collect::<Vec<_>>();
            paths.sort_unstable();
            return Err(invalid_data!(
                "Cannot add files that are already referenced by table, files: {}",
                paths.join(", ")
            ));
        }

        if !duplicate_deletes.is_empty() {
            let mut paths = duplicate_deletes
                .iter()
                .map(format_data_file_identity)
                .collect::<Vec<_>>();
            paths.sort_unstable();
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot delete files referenced multiple times by the table, files: {}",
                    paths.join(", ")
                ),
            ));
        }

        if !files_to_delete.is_empty() {
            let mut paths = files_to_delete
                .iter()
                .map(format_data_file_identity)
                .collect::<Vec<_>>();
            paths.sort_unstable();
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot delete files that are not in the target branch, files: {}",
                    paths.join(", ")
                ),
            ));
        }

        Ok(affected_manifests)
    }

    pub(crate) fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    pub(crate) fn new_manifest_writer(
        &self,
        content: ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}-m{}.{}",
            self.table.metadata().metadata_location()?,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::SeqCst),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid partition spec id for new manifest writer",
                )
                .with_context("partition spec id", partition_spec_id.to_string())
            })?
            .as_ref()
            .clone();
        let schema = self.table.metadata().current_schema().clone();

        let builder = if let Some(em) = self.table.encryption_manager() {
            ManifestWriterBuilder::new_from_encrypted(
                em.encrypt(output_file),
                Some(self.snapshot_id),
                schema,
                partition_spec,
            )?
        } else {
            ManifestWriterBuilder::new(output_file, Some(self.snapshot_id), schema, partition_spec)
        };

        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
        partition_spec: &PartitionSpec,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len()
            || partition_value.fields().len() != partition_spec.fields().len()
        {
            return Err(invalid_data!(
                "Partition value is not compatible with partition type"
            ));
        }

        for (idx, (value, field)) in partition_value
            .fields()
            .iter()
            .zip(partition_type.fields())
            .enumerate()
        {
            if partition_spec.fields()[idx].transform == Transform::Void {
                if value.is_some() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Void partition field must be null",
                    ));
                }
                continue;
            }

            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(invalid_data!(
                    "Partition value is not compatible partition type"
                ));
            }
        }
        Ok(())
    }

    async fn write_added_manifest(
        &self,
        added_files: Vec<DataFile>,
        data_sequence_number: Option<i64>,
    ) -> Result<ManifestFile> {
        let Some(first_file) = added_files.first() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added files found when writing an added manifest",
            ));
        };

        let content = match first_file.content_type() {
            DataContentType::Data => ManifestContentType::Data,
            DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                ManifestContentType::Deletes
            }
        };

        if added_files.iter().any(|file| {
            matches!(file.content_type(), DataContentType::Data)
                != matches!(content, ManifestContentType::Data)
        }) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "A manifest cannot mix data files and delete files",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file)
                .sequence_number_opt(data_sequence_number);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer =
            self.new_manifest_writer(content, self.table.metadata().default_partition_spec_id())?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_delete_manifests(
        &self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        let mut groups: HashMap<(i32, ManifestContentType), Vec<ManifestEntry>> = HashMap::new();
        for entry in deleted_entries {
            let content = match entry.content_type() {
                DataContentType::Data => ManifestContentType::Data,
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    ManifestContentType::Deletes
                }
            };
            groups
                .entry((entry.data_file().partition_spec_id, content))
                .or_default()
                .push(entry);
        }

        let mut manifests = Vec::with_capacity(groups.len());
        for ((spec_id, content), entries) in groups {
            let mut writer = self.new_manifest_writer(content, spec_id)?;
            for entry in entries {
                writer.add_delete_entry(entry)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }
        Ok(manifests)
    }

    pub(crate) async fn prepare_manifests<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_file_identities.is_empty()
            && self.removed_delete_file_identities.is_empty()
            && self.snapshot_properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No file or snapshot property changes to commit",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = if let Some(mut filter) = self.delete_filter_manager.take() {
            let metadata = self.table.metadata();
            let schema_id = metadata
                .snapshot_for_ref(&self.target_branch)
                .and_then(|snapshot| snapshot.schema_id())
                .unwrap_or(metadata.current_schema_id());
            let schema = metadata.schema_by_id(schema_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid schema id for existing manifest filtering",
                )
                .with_context("schema id", schema_id.to_string())
            })?;

            let (mut data_manifests, delete_manifests): (Vec<_>, Vec<_>) = existing_manifests
                .into_iter()
                .partition(|manifest| manifest.content == ManifestContentType::Data);
            let last_sequence_number = metadata.last_sequence_number();
            let added_data_sequence_number = (!self.added_data_files.is_empty()).then_some(
                self.new_data_file_sequence_number
                    .unwrap_or(last_sequence_number),
            );
            let live_data_min_sequence_number = data_manifests
                .iter()
                .map(|manifest| manifest.min_sequence_number)
                .filter(|sequence| *sequence != UNASSIGNED_SEQUENCE_NUMBER)
                .chain(added_data_sequence_number)
                .min()
                .map(|sequence| sequence.min(last_sequence_number))
                .unwrap_or(last_sequence_number);
            // The live-data minimum reflects files that survive this commit. The optional
            // planning-derived bound is validated against added data before snapshot production,
            // so either bound can independently advance delete cleanup.
            let min_data_sequence_number = self
                .delete_file_cleanup_min_data_sequence_number
                .unwrap_or(live_data_min_sequence_number)
                .max(live_data_min_sequence_number);

            filter.drop_delete_files_older_than(min_data_sequence_number);
            filter.remove_dangling_deletes_for(&self.removed_data_file_paths);
            data_manifests.extend(
                filter
                    .filter_manifests(schema.as_ref(), delete_manifests)
                    .await?,
            );
            data_manifests.retain(|manifest| {
                manifest.has_added_files()
                    || manifest.has_existing_files()
                    || manifest.added_snapshot_id == self.snapshot_id
            });
            self.delete_filter_manager = Some(filter);
            data_manifests
        } else {
            existing_manifests
        };

        if !self.added_data_files.is_empty() {
            let added_files = std::mem::take(&mut self.added_data_files);
            let added_manifest = self
                .write_added_manifest(added_files, self.new_data_file_sequence_number)
                .await?;
            manifest_files.push(added_manifest);
        }

        if !self.added_delete_files.is_empty() {
            let added_files = std::mem::take(&mut self.added_delete_files);
            let added_manifest = self.write_added_manifest(added_files, None).await?;
            manifest_files.push(added_manifest);
        }

        let deleted_entries = snapshot_produce_operation.delete_entries(self).await?;
        manifest_files.extend(self.write_delete_manifests(deleted_entries).await?);

        if snapshot_produce_operation.operation() == Operation::Overwrite {
            manifest_files = self.merge_data_manifests(manifest_files).await?;
        }

        Ok(manifest_process.process_manifests(self, manifest_files))
    }

    async fn merge_data_manifests(
        &self,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let property = |key| {
            self.snapshot_properties
                .get(key)
                .or_else(|| self.table.metadata().properties().get(key))
        };
        if !property(MANIFEST_MERGE_ENABLED)
            .and_then(|value| value.parse().ok())
            .unwrap_or(true)
        {
            return Ok(manifests);
        }
        let target_size = property(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT as u64);
        let min_count = property(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT as usize);
        let first_manifest = manifests
            .iter()
            .find(|manifest| manifest.content == ManifestContentType::Data)
            .map(|manifest| manifest.manifest_path.clone());

        let mut groups: BTreeMap<i32, Vec<ManifestFile>> = BTreeMap::new();
        let mut result = Vec::new();
        for manifest in manifests {
            if manifest.content == ManifestContentType::Data {
                groups
                    .entry(manifest.partition_spec_id)
                    .or_default()
                    .push(manifest);
            } else {
                result.push(manifest);
            }
        }

        for (spec_id, group) in groups {
            let mut bins: Vec<(u64, Vec<ManifestFile>)> = Vec::new();
            for manifest in group {
                let size = manifest.manifest_length as u64;
                if let Some((weight, bin)) = bins
                    .iter_mut()
                    .find(|(weight, _)| weight.saturating_add(size) <= target_size)
                {
                    *weight += size;
                    bin.push(manifest);
                } else {
                    bins.push((size, vec![manifest]));
                }
            }
            for (_, bin) in bins {
                // Apply the count threshold to the first bin so larger manifests
                // do not prevent merging older groups, matching the release policy.
                if bin.len() == 1
                    || (bin.len() < min_count
                        && bin.iter().any(|manifest| {
                            Some(&manifest.manifest_path) == first_manifest.as_ref()
                        }))
                {
                    result.extend(bin);
                    continue;
                }

                let mut writer = self.new_manifest_writer(ManifestContentType::Data, spec_id)?;
                for manifest_file in bin {
                    let manifest = self.table.manifest_reader().read(&manifest_file).await?;
                    for entry in manifest.entries() {
                        let current = entry.snapshot_id() == Some(self.snapshot_id);
                        match entry.status() {
                            ManifestStatus::Added if current => {
                                writer.add_entry(entry.as_ref().clone())?
                            }
                            ManifestStatus::Deleted if current => {
                                writer.add_delete_entry(entry.as_ref().clone())?
                            }
                            ManifestStatus::Deleted => {}
                            _ => writer.add_existing_entry(entry.as_ref().clone())?,
                        }
                    }
                }
                result.push(writer.write_manifest_file().await?);
            }
        }
        Ok(result)
    }

    // Returns a `Summary` of the current snapshot
    pub(crate) fn prepare_summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        let partition_spec = |file: &DataFile| {
            table_metadata
                .partition_spec_by_id(file.partition_spec_id)
                .cloned()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "File references an unknown partition spec",
                    )
                    .with_context("partition spec id", file.partition_spec_id.to_string())
                    .with_context("file path", file.file_path())
                })
        };

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                partition_spec(data_file)?,
            );
        }

        for delete_file in &self.added_delete_files {
            summary_collector.add_file(
                delete_file,
                table_metadata.current_schema().clone(),
                partition_spec(delete_file)?,
            );
        }

        for data_file in &self.removed_data_files {
            summary_collector.remove_file(
                data_file,
                table_metadata.current_schema().clone(),
                partition_spec(data_file)?,
            );
        }

        for delete_file in &self.removed_delete_files {
            summary_collector.remove_file(
                delete_file,
                table_metadata.current_schema().clone(),
                partition_spec(delete_file)?,
            );
        }

        let previous_snapshot = table_metadata.snapshot_for_ref(&self.target_branch);

        // User-supplied snapshot properties are applied first, then the computed
        // metrics overwrite any colliding keys. This matches iceberg-java
        // (`SnapshotProducer.summary`), where computed `added-*`/`total-*` values
        // are written after user properties so a user cannot shadow them with a
        // bad (or merely wrong) value that would corrupt the snapshot summary.
        let mut additional_properties = self.snapshot_properties.clone();
        additional_properties.extend(summary_collector.build());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        // `Overwrite` is also used for partial replace-files commits. Treating it as a table
        // truncate would report every parent file as deleted and reset totals to only the files
        // added by this commit. The producer already collected the exact removed files above, so
        // all replace-files operations must roll summaries forward incrementally.
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> Result<String> {
        Ok(format!(
            "{}/snap-{}-{}-{}.{}",
            self.table.metadata().metadata_location()?,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        ))
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        // Build the summary before `prepare_manifests`, which drains the added
        // data and delete file vectors.
        let summary = self
            .prepare_summary(&snapshot_produce_operation)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.")
                    .with_source(err)
            })?;
        let manifests = self
            .prepare_manifests(&snapshot_produce_operation, &process)
            .await?;

        self.commit_prepared(manifests, summary, 0).await
    }

    /// Finalize an already-prepared set of manifests against the current table state.
    pub(crate) async fn commit_prepared(
        self,
        manifests: Vec<ManifestFile>,
        summary: Summary,
        manifest_list_attempt: i64,
    ) -> Result<ActionCommit> {
        let manifest_list_path = self.generate_manifest_list_file_path(manifest_list_attempt)?;
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let parent_snapshot_id = self
            .table
            .metadata()
            .snapshot_for_ref(&self.target_branch)
            .map(|snapshot| snapshot.snapshot_id());

        let raw_output = self
            .table
            .file_io()
            .new_output(manifest_list_path.clone())?;

        let (writer, encrypted_output) = match self.table.encryption_manager() {
            Some(em) => {
                let encrypted_output = em.encrypt(raw_output);
                (
                    encrypted_output.writer().await?,
                    Some((em.clone(), encrypted_output)),
                )
            }
            None => (raw_output.writer().await?, None),
        };

        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => {
                ManifestListWriter::v1(writer, self.snapshot_id, parent_snapshot_id)
            }
            FormatVersion::V2 => {
                ManifestListWriter::v2(writer, self.snapshot_id, parent_snapshot_id, next_seq_num)
            }
            FormatVersion::V3 => ManifestListWriter::v3(
                writer,
                self.snapshot_id,
                parent_snapshot_id,
                next_seq_num,
                Some(first_row_id),
            ),
        };

        manifest_list_writer.add_manifests(manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        let file_metadata = manifest_list_writer.close().await?;
        let encryption_key_id = match encrypted_output {
            Some((em, encrypted_output)) => Some(
                em.encrypt_manifest_list_key_metadata(
                    &encrypted_output.key_metadata_with_saved_file_metadata(&file_metadata),
                )
                .await?,
            ),
            None => None,
        };

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_encryption_key_id(encryption_key_id)
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let encryption_key_updates: Vec<TableUpdate> = self
            .table
            .encryption_manager()
            .map(|em| {
                em.with_encryption_keys(|keys| {
                    keys.values()
                        .filter(|k| self.table.metadata().encryption_key(k.key_id()).is_none())
                        .map(|k| TableUpdate::AddEncryptionKey {
                            encryption_key: k.clone(),
                        })
                        .collect()
                })
            })
            .unwrap_or_default();

        let updates = [encryption_key_updates, vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: self.target_branch.clone(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ]]
        .concat();

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: self.target_branch,
                snapshot_id: parent_snapshot_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }

    pub(crate) fn set_new_data_file_sequence_number(&mut self, sequence_number: i64) {
        self.new_data_file_sequence_number = Some(sequence_number);
    }

    pub(crate) fn set_delete_file_cleanup_min_data_sequence_number(
        &mut self,
        sequence_number: i64,
    ) {
        self.delete_file_cleanup_min_data_sequence_number = Some(sequence_number);
    }

    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub(crate) fn set_manifest_counter(&mut self, manifest_counter: Arc<AtomicU64>) {
        self.manifest_counter = manifest_counter;
    }

    pub(crate) fn set_snapshot_properties(&mut self, snapshot_properties: HashMap<String, String>) {
        self.snapshot_properties = snapshot_properties;
    }

    pub(crate) fn set_target_branch(&mut self, target_branch: String) {
        self.target_branch = target_branch;
    }

    pub(crate) fn target_branch(&self) -> &str {
        &self.target_branch
    }

    pub(crate) fn enable_delete_filter_manager(&mut self, load_concurrency: usize) -> Result<()> {
        if self.delete_filter_manager.is_some() {
            return Ok(());
        }

        let metadata = self.table.metadata();
        let mut manager = ManifestFilterManager::new(
            self.table.file_io().clone(),
            ManifestWriterContext::new(
                metadata.metadata_location()?,
                self.commit_uuid,
                self.manifest_counter.clone(),
                metadata.format_version(),
                self.snapshot_id,
                self.table.file_io().clone(),
                self.table.encryption_manager_ref(),
            ),
        );
        manager.set_load_concurrency(load_concurrency);

        for file in &self.removed_delete_files {
            manager.delete_file(file.clone())?;
        }

        self.delete_filter_manager = Some(manager);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Type, VariantType};

    #[test]
    fn test_validate_partition_value_for_legacy_void_non_primitive_source() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();
        let partition_spec: PartitionSpec = serde_json::from_value(serde_json::json!({
            "spec-id": 0,
            "fields": [
                {
                    "source-id": 2,
                    "field-id": 1000,
                    "name": "v_part",
                    "transform": "void"
                },
                {
                    "source-id": 1,
                    "field-id": 1001,
                    "name": "id_part",
                    "transform": "identity"
                }
            ]
        }))
        .unwrap();
        let partition_type = partition_spec.partition_type(&schema).unwrap();

        let ok = Struct::from_iter([None, Some(Literal::int(42))]);
        SnapshotProducer::validate_partition_value(&ok, &partition_type, &partition_spec).unwrap();

        // `void` must remain null even though its compatibility result type is `int`.
        let bad = Struct::from_iter([Some(Literal::int(1)), None]);
        SnapshotProducer::validate_partition_value(&bad, &partition_type, &partition_spec)
            .unwrap_err();
    }
}

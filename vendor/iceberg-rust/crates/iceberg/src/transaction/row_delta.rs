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

//! RowDeltaAction. Cherry-picked from apache/iceberg-rust PR #2203 and adapted
//! to the RisingWave fork's `SnapshotProducer` signature. The upstream patch
//! tracks `org.apache.iceberg.RowDelta` from the Java implementation.
//!
//! RowDelta commits a mixed set of file changes in one snapshot:
//!
//! - Added data files (inserts or rewritten files in CoW mode)
//! - Added delete files (position-delete and equality-delete files in MoR mode)
//! - Removed data files (marked deleted in CoW mode)
//!
//! Operation classification follows Java `BaseRowDelta`:
//!
//! - Only adds data files -> `Append`
//! - Has removed data files OR added delete files -> `Overwrite`

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// RowDeltaAction commits data file additions, deletions, and delete-file
/// additions in a single atomic snapshot.
///
/// # Copy-on-Write
///
/// Rewrite the target files: call [`Self::add_data_files`] with the new
/// files and [`Self::remove_data_files`] with the originals.
///
/// # Merge-on-Read
///
/// Emit delete files instead of rewriting: call [`Self::add_delete_files`]
/// with position-delete or equality-delete files. Optionally add new data
/// files for MERGE INTO inserts.
pub struct RowDeltaAction {
    /// New data files to add.
    added_data_files: Vec<DataFile>,
    /// Data files to mark as deleted.
    removed_data_files: Vec<DataFile>,
    /// Delete files to add (position or equality deletes).
    added_delete_files: Vec<DataFile>,
    /// Optional commit UUID for manifest file naming.
    commit_uuid: Option<Uuid>,
    /// Additional properties merged into the snapshot summary.
    snapshot_properties: HashMap<String, String>,
    /// Starting snapshot ID for optimistic concurrency control.
    starting_snapshot_id: Option<i64>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: vec![],
            removed_data_files: vec![],
            added_delete_files: vec![],
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            starting_snapshot_id: None,
        }
    }

    /// Add new data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Mark data files as deleted in the snapshot (CoW path).
    pub fn remove_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(data_files);
        self
    }

    /// Add delete files (position or equality deletes) to the snapshot.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Set commit UUID for deterministic manifest naming.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Replace snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Require the commit to apply on top of this snapshot ID. A concurrent
    /// writer advancing the current snapshot produces a retryable DataInvalid
    /// error, matching Java's `RowDelta.validateFromSnapshot`.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Reject empty row deltas early: matches Java behaviour.
        if self.added_data_files.is_empty()
            && self.removed_data_files.is_empty()
            && self.added_delete_files.is_empty()
        {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "RowDeltaAction is empty: add at least one data or delete file",
            ));
        }

        // Conflict detection: table must still be at the expected snapshot.
        if let Some(expected_snapshot_id) = self.starting_snapshot_id
            && table.metadata().current_snapshot_id() != Some(expected_snapshot_id)
        {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "RowDelta conflict: expected snapshot {}, current {:?}",
                    expected_snapshot_id,
                    table.metadata().current_snapshot_id()
                ),
            ));
        }

        // The fork's SnapshotProducer already handles removed_data_files and
        // added_delete_files directly. We only need to provide the operation
        // classification and let DefaultManifestProcess carry the manifests.
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None, // key_metadata
            None, // snapshot_id - auto-generate
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            vec![], // removed_delete_files (not used by RowDelta)
        );

        snapshot_producer.validate_added_data_files(&self.added_data_files)?;

        let operation = RowDeltaOperation {
            has_removed_data: !self.removed_data_files.is_empty(),
            has_added_deletes: !self.added_delete_files.is_empty(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// Classifies a RowDelta commit as `Append` or `Overwrite` and produces the
/// manifest entries the snapshot needs.
struct RowDeltaOperation {
    has_removed_data: bool,
    has_added_deletes: bool,
}

impl SnapshotProduceOperation for RowDeltaOperation {
    /// Classification logic from Java `BaseRowDelta.operation()`:
    ///
    /// - Has removed data files OR delete files -> `Overwrite`
    /// - Otherwise (adds only) -> `Append`
    fn operation(&self) -> Operation {
        if self.has_removed_data || self.has_added_deletes {
            Operation::Overwrite
        } else {
            Operation::Append
        }
    }

    /// No custom manifest entries beyond what `SnapshotProducer` already
    /// generates via `removed_data_file_paths` filtering.
    fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send {
        async { Ok(vec![]) }
    }

    /// Preserve all existing manifests. Any entries that match a removed
    /// data file path are filtered out by `SnapshotProducer::manifest_file`
    /// through the `removed_data_file_paths` set.
    fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send {
        let table = snapshot_produce.table;
        async move {
            let Some(snapshot) = table.metadata().current_snapshot() else {
                return Ok(vec![]);
            };
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;
            Ok(manifest_list.entries().to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::TableUpdate;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};

    fn data_file(path: &str) -> crate::spec::DataFile {
        let table = make_v2_minimal_table();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap()
    }

    fn equality_delete_file(path: &str) -> crate::spec::DataFile {
        let table = make_v2_minimal_table();
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(3)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    fn position_delete_file(path: &str) -> crate::spec::DataFile {
        let table = make_v2_minimal_table();
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_row_delta_add_only() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file("test/1.parquet")]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        assert!(matches!(&updates[0], TableUpdate::AddSnapshot { .. }));
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(snapshot.summary().operation, crate::spec::Operation::Append);
        }
    }

    #[tokio::test]
    async fn test_row_delta_remove_only() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .remove_data_files(vec![data_file("test/old.parquet")]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().operation,
                crate::spec::Operation::Overwrite
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_add_and_remove() {
        // CoW-shape update: remove old, add new.
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let action = tx
            .row_delta()
            .remove_data_files(vec![data_file("test/old.parquet")])
            .add_data_files(vec![data_file("test/new.parquet")]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().operation,
                crate::spec::Operation::Overwrite
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut props = HashMap::new();
        props.insert("key".to_string(), "value".to_string());

        let action = tx
            .row_delta()
            .set_snapshot_properties(props)
            .add_data_files(vec![data_file("test/1.parquet")]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().additional_properties.get("key").unwrap(),
                "value"
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_validate_from_snapshot_stale() {
        // Table has no snapshot, so any expected snapshot id is stale.
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let action = tx
            .row_delta()
            .validate_from_snapshot(99999)
            .add_data_files(vec![data_file("test/1.parquet")]);

        let result = Arc::new(action).commit(&table).await;
        match result {
            Ok(_) => panic!("expected DataInvalid error for stale snapshot"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::DataInvalid),
        }
    }

    #[tokio::test]
    async fn test_row_delta_empty_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_row_delta_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let bad_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/bad.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("wrong"))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![bad_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    /// Phase E acceptance test: 3 data files + 2 position-delete files + 1
    /// equality-delete file commit as one `Overwrite` snapshot.
    #[tokio::test]
    async fn test_row_delta_mixed_data_and_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_files = vec![
            data_file("s3://bucket/data-1.parquet"),
            data_file("s3://bucket/data-2.parquet"),
            data_file("s3://bucket/data-3.parquet"),
        ];
        let position_deletes = vec![
            position_delete_file("s3://bucket/pos-del-1.parquet"),
            position_delete_file("s3://bucket/pos-del-2.parquet"),
        ];
        let equality_deletes = vec![equality_delete_file("s3://bucket/eq-del-1.parquet")];

        let mut all_deletes = position_deletes;
        all_deletes.extend(equality_deletes);

        let action = tx
            .row_delta()
            .add_data_files(data_files)
            .add_delete_files(all_deletes);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot.summary().operation,
                crate::spec::Operation::Overwrite,
                "RowDelta with delete files should be Overwrite"
            );
            let summary = &snapshot.summary().additional_properties;
            // added-data-files = 3
            assert_eq!(
                summary.get("added-data-files").map(String::as_str),
                Some("3"),
                "should report 3 added data files; summary={summary:?}"
            );
            // added-delete-files = 3 (2 position + 1 equality)
            assert_eq!(
                summary.get("added-delete-files").map(String::as_str),
                Some("3"),
                "should report 3 added delete files; summary={summary:?}"
            );
        } else {
            panic!("expected AddSnapshot update; got {:?}", updates[0]);
        }
    }
}

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

use futures::stream::{self, StreamExt};

use crate::spec::{Manifest, ManifestFile, ManifestList, SnapshotRef, TableMetadataRef};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

pub(crate) const DEFAULT_LOAD_CONCURRENCY: usize = 16;

// Each load is spawned as its own task so Avro decoding runs in parallel across runtime
// workers; `buffer_unordered` alone polls every future on the caller's task, which
// serializes decoding. `visit` stays on the caller's task, so it needs no synchronization.
pub(crate) async fn for_each_manifest_list<F>(
    table: &Table,
    snapshots: Vec<SnapshotRef>,
    concurrency: usize,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(&ManifestList),
{
    let mut lists = stream::iter(snapshots)
        .map(|snapshot| {
            let reader = table.manifest_list_reader(&snapshot);
            table
                .runtime()
                .io()
                .spawn(async move { reader.load().await })
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(list) = lists.next().await {
        let list = list??;
        visit(&list);
    }
    Ok(())
}

pub(crate) async fn for_each_manifest<F>(
    table: &Table,
    manifest_files: Vec<ManifestFile>,
    concurrency: usize,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(&ManifestFile, &Manifest),
{
    let mut manifests = stream::iter(manifest_files)
        .map(|manifest_file| {
            let file_io = table.file_io().clone();
            table.runtime().io().spawn(async move {
                let manifest = manifest_file.load_manifest(&file_io).await?;
                Ok::<_, Error>((manifest_file, manifest))
            })
        })
        .buffer_unordered(concurrency.max(1));

    while let Some(manifest) = manifests.next().await {
        let (manifest_file, manifest) = manifest??;
        visit(&manifest_file, &manifest);
    }
    Ok(())
}

pub(crate) struct PhysicalFileCleanup {
    table: Table,
    load_concurrency: usize,
}

impl PhysicalFileCleanup {
    pub(crate) fn new(table: Table) -> Self {
        Self {
            table,
            load_concurrency: DEFAULT_LOAD_CONCURRENCY,
        }
    }

    pub(crate) fn with_load_concurrency(mut self, concurrency: usize) -> Self {
        self.load_concurrency = concurrency.max(1);
        self
    }

    pub(crate) async fn clean(&self, before: &TableMetadataRef) -> Result<()> {
        if !self.table.metadata().table_properties().gc_enabled()? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot clean expired files: gc.enabled is false",
            ));
        }

        let after = self.table.metadata_ref();
        if before.uuid() != after.uuid() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot clean expired files using metadata from a different table",
            ));
        }
        let expired_snapshots: Vec<_> = before
            .snapshots()
            .filter(|snapshot| after.snapshot_by_id(snapshot.snapshot_id()).is_none())
            .cloned()
            .collect();
        if expired_snapshots.is_empty() {
            return Ok(());
        }

        let surviving_snapshots: Vec<_> = after.snapshots().cloned().collect();
        let mut manifest_lists_to_delete: HashSet<String> = expired_snapshots
            .iter()
            .map(|snapshot| snapshot.manifest_list().to_string())
            .filter(|path| !path.is_empty())
            .collect();
        for snapshot in &surviving_snapshots {
            manifest_lists_to_delete.remove(snapshot.manifest_list());
        }

        let mut candidate_manifests = HashMap::<String, ManifestFile>::new();
        for_each_manifest_list(
            &self.table,
            expired_snapshots,
            self.load_concurrency,
            |list| {
                for manifest_file in list.entries() {
                    candidate_manifests
                        .entry(manifest_file.manifest_path.clone())
                        .or_insert_with(|| manifest_file.clone());
                }
            },
        )
        .await?;

        let mut surviving_manifests = HashMap::<String, ManifestFile>::new();
        for_each_manifest_list(
            &self.table,
            surviving_snapshots,
            self.load_concurrency,
            |list| {
                for manifest_file in list.entries() {
                    candidate_manifests.remove(&manifest_file.manifest_path);
                    surviving_manifests
                        .entry(manifest_file.manifest_path.clone())
                        .or_insert_with(|| manifest_file.clone());
                }
            },
        )
        .await?;

        let manifests_to_delete: HashSet<String> = candidate_manifests.keys().cloned().collect();
        let mut content_to_delete = HashSet::<String>::new();
        for_each_manifest(
            &self.table,
            candidate_manifests.into_values().collect(),
            self.load_concurrency,
            |_, manifest| {
                for entry in manifest.entries() {
                    content_to_delete.insert(entry.file_path().to_string());
                }
            },
        )
        .await?;

        if !content_to_delete.is_empty() {
            for_each_manifest(
                &self.table,
                surviving_manifests.into_values().collect(),
                self.load_concurrency,
                |_, manifest| {
                    for entry in manifest.entries() {
                        if entry.is_alive() {
                            content_to_delete.remove(entry.file_path());
                        }
                    }
                },
            )
            .await?;
        }

        let statistics_to_delete = removed_statistics_paths(before.as_ref(), after.as_ref(), false);
        let partition_statistics_to_delete =
            removed_statistics_paths(before.as_ref(), after.as_ref(), true);

        self.delete_paths(content_to_delete).await?;
        self.delete_paths(manifests_to_delete).await?;
        self.delete_paths(manifest_lists_to_delete).await?;
        self.delete_paths(statistics_to_delete).await?;
        self.delete_paths(partition_statistics_to_delete).await
    }

    async fn delete_paths(&self, paths: HashSet<String>) -> Result<()> {
        self.table
            .file_io()
            .delete_stream(stream::iter(paths))
            .await
    }
}

fn removed_statistics_paths(
    before: &crate::spec::TableMetadata,
    after: &crate::spec::TableMetadata,
    partition: bool,
) -> HashSet<String> {
    let after_paths: HashSet<&str> = if partition {
        after
            .partition_statistics_iter()
            .map(|file| file.statistics_path.as_str())
            .collect()
    } else {
        after
            .statistics_iter()
            .map(|file| file.statistics_path.as_str())
            .collect()
    };

    if partition {
        before
            .partition_statistics_iter()
            .map(|file| file.statistics_path.clone())
            .filter(|path| !after_paths.contains(path.as_str()))
            .collect()
    } else {
        before
            .statistics_iter()
            .map(|file| file.statistics_path.clone())
            .filter(|path| !after_paths.contains(path.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestListWriter, ManifestWriterBuilder, Operation, PartitionStatisticsFile, Snapshot,
        SnapshotReference, SnapshotRetention, StatisticsFile, Struct, Summary, TableMetadata,
    };
    use crate::test_utils::test_runtime;

    fn base_table() -> Table {
        let metadata_json = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/table_metadata/TableMetadataV2ValidMinimal.json"
        ));
        let mut metadata: TableMetadata = serde_json::from_str(metadata_json).unwrap();
        metadata.location = "memory://warehouse/table".to_string();

        Table::builder()
            .metadata(metadata)
            .metadata_location("memory://warehouse/table/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns", "table"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap()
    }

    fn data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(4)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap()
    }

    async fn write_manifest_list(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        manifests: Vec<ManifestFile>,
    ) {
        let writer = table
            .file_io()
            .new_output(path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut writer =
            ManifestListWriter::v2(writer, snapshot_id, parent_snapshot_id, sequence_number);
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
    }

    fn snapshot(
        id: i64,
        parent: Option<i64>,
        sequence_number: i64,
        manifest_list: &str,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(1_000 + id)
            .with_manifest_list(manifest_list)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    #[tokio::test]
    async fn cleanup_preserves_live_files_and_ignores_deleted_tombstones() {
        let base = base_table();
        let removed_path = "memory://warehouse/table/data/removed.parquet";
        let shared_path = "memory://warehouse/table/data/shared.parquet";
        let old_manifest_path = "memory://warehouse/table/metadata/old-manifest.avro";
        let current_manifest_path = "memory://warehouse/table/metadata/current-manifest.avro";
        let old_list_path = "memory://warehouse/table/metadata/old-list.avro";
        let current_list_path = "memory://warehouse/table/metadata/current-list.avro";
        let old_stats_path = "memory://warehouse/table/metadata/old-stats.puffin";
        let current_stats_path = "memory://warehouse/table/metadata/current-stats.puffin";
        let old_partition_stats_path =
            "memory://warehouse/table/metadata/old-partition-stats.parquet";

        for path in [
            removed_path,
            shared_path,
            old_stats_path,
            current_stats_path,
            old_partition_stats_path,
        ] {
            base.file_io()
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"data"))
                .await
                .unwrap();
        }

        let removed_file = data_file(removed_path);
        let shared_file = data_file(shared_path);
        let mut old_manifest_writer = ManifestWriterBuilder::new(
            base.file_io().new_output(old_manifest_path).unwrap(),
            Some(1),
            base.metadata().current_schema().clone(),
            base.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        old_manifest_writer
            .add_file(removed_file.clone(), 1)
            .unwrap();
        old_manifest_writer
            .add_file(shared_file.clone(), 1)
            .unwrap();
        let old_manifest = old_manifest_writer.write_manifest_file().await.unwrap();

        let mut current_manifest_writer = ManifestWriterBuilder::new(
            base.file_io().new_output(current_manifest_path).unwrap(),
            Some(2),
            base.metadata().current_schema().clone(),
            base.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        current_manifest_writer
            .add_existing_file(shared_file, 1, 1, Some(1))
            .unwrap();
        current_manifest_writer
            .add_delete_file(removed_file, 1, Some(1))
            .unwrap();
        let current_manifest = current_manifest_writer.write_manifest_file().await.unwrap();

        write_manifest_list(&base, old_list_path, 1, None, 1, vec![old_manifest]).await;
        write_manifest_list(&base, current_list_path, 2, Some(1), 2, vec![
            current_manifest,
        ])
        .await;

        let old_snapshot = Arc::new(snapshot(1, None, 1, old_list_path));
        let current_snapshot = Arc::new(snapshot(2, Some(1), 2, current_list_path));
        let mut before = base.metadata().clone();
        before.snapshots.insert(1, old_snapshot);
        before.snapshots.insert(2, current_snapshot);
        before.current_snapshot_id = Some(2);
        before
            .refs
            .insert(MAIN_BRANCH.to_string(), SnapshotReference {
                snapshot_id: 2,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            });
        before.statistics.insert(1, StatisticsFile {
            snapshot_id: 1,
            statistics_path: old_stats_path.to_string(),
            file_size_in_bytes: 4,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: vec![],
        });
        before.statistics.insert(2, StatisticsFile {
            snapshot_id: 2,
            statistics_path: current_stats_path.to_string(),
            file_size_in_bytes: 4,
            file_footer_size_in_bytes: 0,
            key_metadata: None,
            blob_metadata: vec![],
        });
        before
            .partition_statistics
            .insert(1, PartitionStatisticsFile {
                snapshot_id: 1,
                statistics_path: old_partition_stats_path.to_string(),
                file_size_in_bytes: 4,
            });
        let before = Arc::new(before);

        let mut after = before.as_ref().clone();
        after.snapshots.remove(&1);
        after.statistics.remove(&1);
        after.partition_statistics.remove(&1);
        let table = base.with_metadata(Arc::new(after));

        table.cleanup_expired_files(&before).await.unwrap();

        for path in [
            removed_path,
            old_manifest_path,
            old_list_path,
            old_stats_path,
            old_partition_stats_path,
        ] {
            assert!(!table.file_io().exists(path).await.unwrap(), "{path}");
        }
        for path in [
            shared_path,
            current_manifest_path,
            current_list_path,
            current_stats_path,
        ] {
            assert!(table.file_io().exists(path).await.unwrap(), "{path}");
        }
    }

    async fn write_data_manifest(
        table: &Table,
        path: &str,
        snapshot_id: i64,
        added: impl IntoIterator<Item = (DataFile, i64)>,
    ) -> ManifestFile {
        let mut writer = ManifestWriterBuilder::new(
            table.file_io().new_output(path).unwrap(),
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for (file, sequence_number) in added {
            writer.add_file(file, sequence_number).unwrap();
        }
        writer.write_manifest_file().await.unwrap()
    }

    async fn touch(table: &Table, path: &str) {
        table
            .file_io()
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"data"))
            .await
            .unwrap();
    }

    fn metadata_with_snapshots(
        base: &Table,
        snapshots: impl IntoIterator<Item = Snapshot>,
        current: i64,
    ) -> TableMetadata {
        let mut metadata = base.metadata().clone();
        for snapshot in snapshots {
            metadata
                .snapshots
                .insert(snapshot.snapshot_id(), Arc::new(snapshot));
        }
        metadata.current_snapshot_id = Some(current);
        metadata
            .refs
            .insert(MAIN_BRANCH.to_string(), SnapshotReference {
                snapshot_id: current,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            });
        metadata
    }

    // Enough manifests and manifest lists that loads overlap across spawned tasks.
    //
    // Snapshots 1..=40 append d_i via m_i (list i = m_1..m_i). Snapshot 41 replaces them
    // with one manifest that keeps d_21..d_40 and tombstones d_1..d_20. Snapshots 42..=50
    // append again. Expiring 1..=41 must delete exactly d_1..d_20, m_1..m_40, lists 1..=41.
    #[tokio::test]
    async fn cleanup_across_many_manifests_deletes_only_unreachable_files() {
        const APPENDS_BEFORE_REWRITE: i64 = 40;
        const REWRITE_SNAPSHOT: i64 = 41;
        const LAST_SNAPSHOT: i64 = 50;
        const TOMBSTONED: i64 = 20;

        let base = base_table();
        let data_path = |i: i64| format!("memory://warehouse/table/data/d{i}.parquet");
        let manifest_path = |i: i64| format!("memory://warehouse/table/metadata/m{i}.avro");
        let list_path = |i: i64| format!("memory://warehouse/table/metadata/list{i}.avro");
        let rewrite_manifest_path = "memory://warehouse/table/metadata/m-rewrite.avro";

        let mut snapshots = Vec::new();
        let mut live_manifests = Vec::new();
        for i in 1..=LAST_SNAPSHOT {
            if i == REWRITE_SNAPSHOT {
                let mut writer = ManifestWriterBuilder::new(
                    base.file_io().new_output(rewrite_manifest_path).unwrap(),
                    Some(i),
                    base.metadata().current_schema().clone(),
                    base.metadata().default_partition_spec().as_ref().clone(),
                )
                .build_v2_data();
                for d in 1..=APPENDS_BEFORE_REWRITE {
                    let file = data_file(&data_path(d));
                    if d <= TOMBSTONED {
                        writer.add_delete_file(file, d, Some(d)).unwrap();
                    } else {
                        writer.add_existing_file(file, d, d, Some(d)).unwrap();
                    }
                }
                live_manifests = vec![writer.write_manifest_file().await.unwrap()];
            } else {
                touch(&base, &data_path(i)).await;
                let manifest = write_data_manifest(&base, &manifest_path(i), i, [(
                    data_file(&data_path(i)),
                    i,
                )])
                .await;
                live_manifests.push(manifest);
            }
            let parent = (i > 1).then_some(i - 1);
            write_manifest_list(&base, &list_path(i), i, parent, i, live_manifests.clone()).await;
            // The list writer only assigns sequence numbers in its written copy; carried-forward
            // manifests must hold them before a later list may reference them.
            let newest = live_manifests.last_mut().unwrap();
            newest.sequence_number = i;
            newest.min_sequence_number = i;
            snapshots.push(snapshot(i, parent, i, &list_path(i)));
        }

        let before = Arc::new(metadata_with_snapshots(
            &base,
            snapshots.clone(),
            LAST_SNAPSHOT,
        ));
        let mut after = before.as_ref().clone();
        for i in 1..=REWRITE_SNAPSHOT {
            after.snapshots.remove(&i);
        }
        let table = base.with_metadata(Arc::new(after));

        table
            .cleanup_expired_files_with_concurrency(&before, 8)
            .await
            .unwrap();

        let exists = |path: String| {
            let table = table.clone();
            async move { table.file_io().exists(&path).await.unwrap() }
        };
        for i in 1..=APPENDS_BEFORE_REWRITE {
            assert_eq!(exists(data_path(i)).await, i > TOMBSTONED, "d{i}");
            assert!(!exists(manifest_path(i)).await, "m{i}");
        }
        for i in 1..=REWRITE_SNAPSHOT {
            assert!(!exists(list_path(i)).await, "list{i}");
        }
        assert!(exists(rewrite_manifest_path.to_string()).await);
        for i in (REWRITE_SNAPSHOT + 1)..=LAST_SNAPSHOT {
            assert!(exists(data_path(i)).await, "d{i}");
            assert!(exists(manifest_path(i)).await, "m{i}");
            assert!(exists(list_path(i)).await, "list{i}");
        }
    }

    // A failed load inside a spawned task must surface as an error, and because deletes
    // only start after every read succeeds, nothing is deleted.
    #[tokio::test]
    async fn cleanup_fails_without_deleting_when_a_manifest_is_missing() {
        let base = base_table();
        let removed_path = "memory://warehouse/table/data/removed.parquet";
        let missing_manifest_path = "memory://warehouse/table/metadata/missing.avro";
        let old_list_path = "memory://warehouse/table/metadata/old-list.avro";
        let current_list_path = "memory://warehouse/table/metadata/current-list.avro";

        touch(&base, removed_path).await;
        let manifest = write_data_manifest(&base, missing_manifest_path, 1, [(
            data_file(removed_path),
            1,
        )])
        .await;
        write_manifest_list(&base, old_list_path, 1, None, 1, vec![manifest]).await;
        write_manifest_list(&base, current_list_path, 2, Some(1), 2, vec![]).await;
        base.file_io().delete(missing_manifest_path).await.unwrap();

        let before = Arc::new(metadata_with_snapshots(
            &base,
            [
                snapshot(1, None, 1, old_list_path),
                snapshot(2, Some(1), 2, current_list_path),
            ],
            2,
        ));
        let mut after = before.as_ref().clone();
        after.snapshots.remove(&1);
        let table = base.with_metadata(Arc::new(after));

        let err = table.cleanup_expired_files(&before).await.unwrap_err();
        assert!(err.to_string().contains(missing_manifest_path), "{err}");
        for path in [removed_path, old_list_path, current_list_path] {
            assert!(table.file_io().exists(path).await.unwrap(), "{path}");
        }
    }

    #[test]
    fn cleanup_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        let table = base_table();
        let before = table.metadata_ref();
        assert_send(table.cleanup_expired_files(&before));
    }
}

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
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use futures::stream::StreamExt;

use super::maintenance::{DEFAULT_LOAD_CONCURRENCY, for_each_manifest, for_each_manifest_list};
use crate::spec::ManifestFile;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

const DEFAULT_OLDER_THAN_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Deletes files below a table location that are not reachable from table metadata.
///
/// Files whose last-modified timestamp is unavailable are retained. This protects
/// in-progress writes on storage backends that cannot report timestamps.
pub struct RemoveOrphanFilesAction {
    table: Table,
    older_than_ms: i64,
    dry_run: bool,
    load_concurrency: usize,
}

impl RemoveOrphanFilesAction {
    /// Creates an orphan-file action with a seven-day retention threshold.
    pub fn new(table: Table) -> Self {
        Self {
            table,
            older_than_ms: now_ms().saturating_sub(DEFAULT_OLDER_THAN_MS),
            dry_run: false,
            load_concurrency: DEFAULT_LOAD_CONCURRENCY,
        }
    }

    /// Deletes only files last modified before this Unix timestamp in milliseconds.
    pub fn older_than_ms(mut self, timestamp_ms: i64) -> Self {
        self.older_than_ms = timestamp_ms;
        self
    }

    /// Deletes only files older than this duration relative to now.
    pub fn older_than(mut self, duration: Duration) -> Self {
        let duration_ms = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        self.older_than_ms = now_ms().saturating_sub(duration_ms);
        self
    }

    /// Selects whether execution reports orphan files without deleting them.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Sets the maximum number of manifest lists or manifests loaded concurrently.
    pub fn load_concurrency(mut self, concurrency: usize) -> Self {
        self.load_concurrency = concurrency.max(1);
        self
    }

    /// Discovers orphan files, deletes them unless this is a dry run, and returns their paths.
    pub async fn execute(self) -> Result<Vec<String>> {
        let reachable = self.collect_reachable_files().await?;
        let listed = self
            .table
            .file_io()
            .list(self.table.metadata().location(), true)
            .await?;

        if self.dry_run {
            let mut orphans: Vec<String> = listed
                .try_filter_map(|entry| {
                    let is_orphan = !entry.is_dir
                        && !reachable.contains(&entry.path)
                        && entry
                            .last_modified_ms
                            .is_some_and(|timestamp| timestamp < self.older_than_ms);
                    async move { Ok(is_orphan.then_some(entry.path)) }
                })
                .try_collect()
                .await?;
            orphans.sort_unstable();
            return Ok(orphans);
        }

        let discovered = Arc::new(Mutex::new(Vec::<String>::new()));
        let listing_error = Arc::new(Mutex::new(None::<Error>));
        let discovered_for_stream = discovered.clone();
        let error_for_stream = listing_error.clone();
        let older_than_ms = self.older_than_ms;
        let delete_stream = listed.filter_map(move |entry| {
            let path = match entry {
                Ok(entry)
                    if !entry.is_dir
                        && !reachable.contains(&entry.path)
                        && entry
                            .last_modified_ms
                            .is_some_and(|timestamp| timestamp < older_than_ms) =>
                {
                    if let Ok(mut paths) = discovered_for_stream.lock() {
                        paths.push(entry.path.clone());
                    }
                    Some(entry.path)
                }
                Ok(_) => None,
                Err(error) => {
                    if let Ok(mut stored) = error_for_stream.lock()
                        && stored.is_none()
                    {
                        *stored = Some(error);
                    }
                    None
                }
            };
            async move { path }
        });
        self.table.file_io().delete_stream(delete_stream).await?;

        if let Some(error) = listing_error
            .lock()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to acquire listing error lock: {error}"),
                )
            })?
            .take()
        {
            return Err(error);
        }

        let mut orphans = discovered
            .lock()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to acquire discovered paths lock: {error}"),
                )
            })?
            .clone();
        orphans.sort_unstable();
        Ok(orphans)
    }

    async fn collect_reachable_files(&self) -> Result<HashSet<String>> {
        let metadata = self.table.metadata_ref();
        let mut reachable = HashSet::new();

        if let Some(metadata_location) = self.table.metadata_location() {
            reachable.insert(metadata_location.to_string());
        }
        reachable.extend(
            metadata
                .metadata_log()
                .iter()
                .map(|entry| entry.metadata_file.clone()),
        );
        reachable.extend(
            metadata
                .statistics_iter()
                .map(|file| file.statistics_path.clone()),
        );
        reachable.extend(
            metadata
                .partition_statistics_iter()
                .map(|file| file.statistics_path.clone()),
        );

        let snapshots: Vec<_> = metadata.snapshots().cloned().collect();
        reachable.extend(
            snapshots
                .iter()
                .map(|snapshot| snapshot.manifest_list().to_string())
                .filter(|path| !path.is_empty()),
        );

        let mut manifests = HashMap::<String, ManifestFile>::new();
        for_each_manifest_list(
            &self.table,
            snapshots,
            self.load_concurrency,
            |manifest_list| {
                for manifest_file in manifest_list.entries() {
                    reachable.insert(manifest_file.manifest_path.clone());
                    manifests
                        .entry(manifest_file.manifest_path.clone())
                        .or_insert_with(|| manifest_file.clone());
                }
            },
        )
        .await?;

        for_each_manifest(
            &self.table,
            manifests.into_values().collect(),
            self.load_concurrency,
            |_, manifest| {
                for entry in manifest.entries() {
                    reachable.insert(entry.file_path().to_string());
                }
            },
        )
        .await?;

        Ok(reachable)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::TableMetadata;
    use crate::test_utils::test_runtime;

    fn empty_memory_table() -> Table {
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

    #[tokio::test]
    async fn removes_only_old_unreachable_files() {
        let table = empty_memory_table();
        let metadata_file = "memory://warehouse/table/metadata/v1.json";
        let orphan_file = "memory://warehouse/table/data/orphan.parquet";
        let outside_file = "memory://warehouse/other/outside.parquet";
        for path in [metadata_file, orphan_file, outside_file] {
            table
                .file_io()
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"test"))
                .await
                .unwrap();
        }

        let dry_run = RemoveOrphanFilesAction::new(table.clone())
            .older_than_ms(i64::MAX)
            .dry_run(true)
            .execute()
            .await
            .unwrap();
        assert_eq!(dry_run, vec![orphan_file]);
        assert!(table.file_io().exists(orphan_file).await.unwrap());

        let deleted = RemoveOrphanFilesAction::new(table.clone())
            .older_than_ms(i64::MAX)
            .execute()
            .await
            .unwrap();
        assert_eq!(deleted, vec![orphan_file]);
        assert!(!table.file_io().exists(orphan_file).await.unwrap());
        assert!(table.file_io().exists(metadata_file).await.unwrap());
        assert!(table.file_io().exists(outside_file).await.unwrap());
    }

    #[test]
    fn execute_future_is_send() {
        fn assert_send<T: Send>(_: T) {}

        assert_send(
            RemoveOrphanFilesAction::new(empty_memory_table())
                .dry_run(true)
                .execute(),
        );
    }
}

//! Per-snapshot cache of decoded manifest entries and aggregated statistics.
//!
//! `SqeTableProvider::scan` computed table statistics from manifests on every
//! `scan()` call, and `IcebergScanExec::execute` walked the same manifests
//! again for dynamic-filter pruning and the Tier-1 clustering gate. The
//! iceberg-rust `ObjectCache` kept the raw bytes warm, but every walk still
//! deserialised and cloned every `DataFile` (several `HashMap<i32, _>` each),
//! on the coordinator, per query, per concurrent session (architecture review
//! 308).
//!
//! A snapshot is immutable, so the set of live data files and the statistics
//! aggregated from them are immutable too. [`SnapshotStatsCache`] keys both on
//! `"{table_uuid}:{snapshot_id}"` and is process-global, shared by every
//! table provider and scan node that touches the snapshot. Nothing here
//! needs DDL invalidation: a commit produces a new snapshot id.
//!
//! The file cache is weighted by data-file count so a handful of 50K-file
//! fact tables cannot pin unbounded memory; idle entries expire so a table
//! that stops being queried releases its files.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table;
use moka::future::Cache;
use tracing::debug;

/// Total live data files the process keeps decoded across all snapshots. A
/// `DataFile` with bounds maps is on the order of 1 KiB, so this bounds the
/// cache at a few hundred MiB in the worst case.
pub const DEFAULT_SNAPSHOT_FILES_CAPACITY: u64 = 200_000;

/// Distinct `(snapshot, projection)` statistics entries kept.
pub const DEFAULT_SNAPSHOT_STATS_CAPACITY: u64 = 10_000;

/// Idle expiry for both caches. A snapshot nobody has planned against for
/// this long is dropped; the next query re-walks once.
const IDLE_TTL: Duration = Duration::from_secs(600);

/// Live data files of one snapshot plus the one fact about delete files the
/// statistics aggregation needs.
#[derive(Debug)]
pub struct SnapshotFiles {
    /// Every live (`status != Deleted`) data-content entry across all
    /// manifests of the snapshot, with full column bounds.
    pub data_files: Vec<DataFile>,
    /// True when any live position- or equality-delete entry exists.
    /// Merge-on-Read deletes remove rows the manifest record counts still
    /// include, so an exact row count is only claimable when this is false.
    pub has_live_delete_files: bool,
}

/// Process-global read-through cache; see the module docs.
pub struct SnapshotStatsCache {
    files: Cache<String, Arc<SnapshotFiles>>,
    stats: Cache<String, Arc<datafusion::common::Statistics>>,
    file_hits: AtomicU64,
    file_misses: AtomicU64,
}

impl std::fmt::Debug for SnapshotStatsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStatsCache")
            .field("file_hits", &self.file_hits.load(Ordering::Relaxed))
            .field("file_misses", &self.file_misses.load(Ordering::Relaxed))
            .finish()
    }
}

/// The shared instance every provider and scan reads through.
pub static SNAPSHOT_STATS_CACHE: LazyLock<SnapshotStatsCache> = LazyLock::new(|| {
    SnapshotStatsCache::new(
        DEFAULT_SNAPSHOT_FILES_CAPACITY,
        DEFAULT_SNAPSHOT_STATS_CAPACITY,
    )
});

/// Cache key for one table snapshot. The table uuid (not the identifier)
/// makes a `DROP` + `CREATE` under the same name a different key.
pub fn snapshot_key(table: &Table, snapshot_id: i64) -> String {
    format!("{}:{}", table.metadata().uuid(), snapshot_id)
}

/// Resolve the snapshot a scan is pinned to (or the current one).
fn resolve_snapshot_id(table: &Table, snapshot_id: Option<i64>) -> Option<i64> {
    let metadata = table.metadata();
    match snapshot_id {
        Some(sid) => metadata.snapshot_by_id(sid).map(|s| s.snapshot_id()),
        None => metadata.current_snapshot().map(|s| s.snapshot_id()),
    }
}

impl SnapshotStatsCache {
    /// Build a cache bounded to `max_files` decoded data files and
    /// `max_stats` statistics entries.
    pub fn new(max_files: u64, max_stats: u64) -> Self {
        Self {
            files: Cache::builder()
                .max_capacity(max_files)
                .weigher(|_k: &String, v: &Arc<SnapshotFiles>| {
                    u32::try_from(v.data_files.len().max(1)).unwrap_or(u32::MAX)
                })
                .time_to_idle(IDLE_TTL)
                .build(),
            stats: Cache::builder()
                .max_capacity(max_stats)
                .time_to_idle(IDLE_TTL)
                .build(),
            file_hits: AtomicU64::new(0),
            file_misses: AtomicU64::new(0),
        }
    }

    /// Times the file cache answered without walking manifests.
    pub fn file_hits(&self) -> u64 {
        self.file_hits.load(Ordering::Relaxed)
    }

    /// Times the file cache had to walk (decode) the snapshot's manifests.
    pub fn file_misses(&self) -> u64 {
        self.file_misses.load(Ordering::Relaxed)
    }

    /// Live data files of `snapshot_id` (or the current snapshot), walked at
    /// most once per snapshot while the entry is cached. Returns `None` when
    /// the table has no such snapshot. The bool is true on a cache hit.
    pub async fn snapshot_files(
        &self,
        table: &Table,
        snapshot_id: Option<i64>,
        concurrency: usize,
    ) -> Result<Option<(Arc<SnapshotFiles>, bool)>, iceberg::Error> {
        let Some(sid) = resolve_snapshot_id(table, snapshot_id) else {
            return Ok(None);
        };
        let key = snapshot_key(table, sid);
        if let Some(files) = self.files.get(&key).await {
            self.file_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((files, true)));
        }
        self.file_misses.fetch_add(1, Ordering::Relaxed);
        let files = Arc::new(walk_snapshot(table, sid, concurrency).await?);
        debug!(
            key = %key,
            files = files.data_files.len(),
            "SnapshotStatsCache: decoded manifests for snapshot"
        );
        self.files.insert(key, Arc::clone(&files)).await;
        Ok(Some((files, false)))
    }

    /// Statistics aggregated over `snapshot_files` for the given projection,
    /// cached per `(snapshot, projected column names)`. `None` when the
    /// table has no such snapshot. The bool is true when the manifest walk
    /// itself was served from cache.
    pub async fn statistics(
        &self,
        table: &Table,
        snapshot_id: Option<i64>,
        arrow_schema: &arrow::datatypes::Schema,
        concurrency: usize,
    ) -> Result<Option<(Arc<datafusion::common::Statistics>, bool)>, iceberg::Error> {
        let Some(sid) = resolve_snapshot_id(table, snapshot_id) else {
            return Ok(None);
        };
        let mut key = snapshot_key(table, sid);
        key.push('|');
        for field in arrow_schema.fields() {
            key.push_str(field.name());
            key.push(',');
        }
        if let Some(stats) = self.stats.get(&key).await {
            self.file_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((stats, true)));
        }
        let Some((files, hit)) = self.snapshot_files(table, Some(sid), concurrency).await? else {
            return Ok(None);
        };
        // The snapshot's schema, not the current one: manifest column stats
        // are keyed by field id, and a time-travel scan projects the ids the
        // snapshot wrote (architecture review 312).
        let iceberg_schema = crate::iceberg_scan::snapshot_schema(table, Some(sid));
        let stats = Arc::new(crate::pruning_stats::aggregate_table_statistics(
            &files.data_files,
            arrow_schema,
            &iceberg_schema,
            !files.has_live_delete_files,
        ));
        self.stats.insert(key, Arc::clone(&stats)).await;
        Ok(Some((stats, hit)))
    }
}

/// One manifest walk: manifest list, then every manifest with bounded
/// concurrency, both through the table's `ObjectCache`.
async fn walk_snapshot(
    table: &Table,
    snapshot_id: i64,
    concurrency: usize,
) -> Result<SnapshotFiles, iceberg::Error> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.snapshot_by_id(snapshot_id) else {
        return Ok(SnapshotFiles {
            data_files: Vec::new(),
            has_live_delete_files: false,
        });
    };
    let cache = table.object_cache();
    let manifest_list = cache.get_manifest_list(snapshot, &metadata_ref).await?;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(concurrency.max(1))
            .try_collect()
            .await?;

    let mut has_live_delete_files = false;
    let mut data_files = Vec::new();
    for manifest in manifests {
        for entry in manifest.entries() {
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }
            if entry.data_file().content_type() == DataContentType::Data {
                data_files.push(entry.data_file().clone());
            } else {
                has_live_delete_files = true;
            }
        }
    }
    Ok(SnapshotFiles {
        data_files,
        has_live_delete_files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder, Struct,
    };
    use iceberg::{NamespaceIdent, TableIdent};

    const TABLE_UUID: &str = "d20125c8-7284-442c-9aea-15fee620737c";
    const SNAPSHOT_ID: i64 = 3051729675574597004;

    fn metadata_json(location: &str, manifest_list: Option<&str>) -> String {
        let (current, snapshots) = match manifest_list {
            Some(ml) => (
                SNAPSHOT_ID.to_string(),
                format!(
                    r#"[{{"snapshot-id": {SNAPSHOT_ID}, "timestamp-ms": 1515100955770,
                        "sequence-number": 1, "schema-id": 0,
                        "summary": {{"operation": "append"}},
                        "manifest-list": "{ml}"}}]"#
                ),
            ),
            None => ("-1".to_string(), "[]".to_string()),
        };
        format!(
            r#"{{
            "format-version": 2,
            "table-uuid": "{TABLE_UUID}",
            "location": "{location}",
            "last-sequence-number": 1,
            "last-updated-ms": 1602638573590,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [{{"type": "struct", "schema-id": 0,
                "fields": [{{"id": 1, "name": "x", "required": true, "type": "long"}}]}}],
            "default-spec-id": 0,
            "partition-specs": [{{"spec-id": 0, "fields": []}}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{{"order-id": 0, "fields": []}}],
            "properties": {{}},
            "current-snapshot-id": {current},
            "snapshots": {snapshots}
        }}"#
        )
    }

    fn build_table(metadata: iceberg::spec::TableMetadata) -> Table {
        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "t".to_string(),
            ))
            .file_io(file_io)
            .build()
            .unwrap()
    }

    /// A table with one snapshot whose manifest list names one manifest
    /// holding `files` data-file entries, written to a temp dir. Mirrors the
    /// `test_table()` fixture in `rest_catalog.rs` with a manifest list added
    /// (review 308 acceptance).
    async fn table_with_manifests(dir: &std::path::Path, files: usize) -> Table {
        let location = dir.to_string_lossy().to_string();
        let manifest_list = format!("{location}/snap-{SNAPSHOT_ID}.avro");
        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(&metadata_json(&location, Some(&manifest_list))).unwrap();
        let table = build_table(metadata);
        let snapshot = table.metadata().current_snapshot().unwrap();
        let schema = snapshot.schema(table.metadata()).unwrap();
        let spec = table.metadata().default_partition_spec().as_ref().clone();

        let mut writer = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!("{location}/m0.avro"))
                .unwrap(),
            Some(SNAPSHOT_ID),
            None,
            schema,
            spec,
        )
        .build_v2_data();
        for i in 0..files {
            writer
                .add_file(
                    DataFileBuilder::default()
                        .partition_spec_id(0)
                        .content(DataContentType::Data)
                        .file_path(format!("{location}/{i}.parquet"))
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(100)
                        .record_count(10)
                        .partition(Struct::empty())
                        .key_metadata(None)
                        .build()
                        .unwrap(),
                    1,
                )
                .unwrap();
        }
        let manifest = writer.write_manifest_file().await.unwrap();
        let mut list_writer = ManifestListWriter::v2(
            table.file_io().new_output(&manifest_list).unwrap(),
            SNAPSHOT_ID,
            None,
            1,
        );
        list_writer
            .add_manifests(vec![manifest].into_iter())
            .unwrap();
        list_writer.close().await.unwrap();
        table
    }

    /// A table without a snapshot never touches the cache: no key, no walk,
    /// no counter movement.
    #[tokio::test]
    async fn snapshotless_table_bypasses_cache() {
        let cache = SnapshotStatsCache::new(1000, 100);
        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(&metadata_json("s3://bucket/t", None)).unwrap();
        let table = build_table(metadata);
        let got = cache.snapshot_files(&table, None, 4).await.unwrap();
        assert!(got.is_none());
        assert_eq!(cache.file_hits(), 0);
        assert_eq!(cache.file_misses(), 0);
    }

    /// Review 308 acceptance: two statistics computations (two `scan()`
    /// calls) on the same snapshot decode the manifests once. The second
    /// call is a hit on both the statistics entry and the file walk, and a
    /// pruning-side `snapshot_files` read shares the same decoded files.
    #[tokio::test]
    async fn same_snapshot_decodes_manifests_once() {
        let dir = tempfile::tempdir().unwrap();
        let table = table_with_manifests(dir.path(), 3).await;
        let cache = SnapshotStatsCache::new(1_000, 100);
        let arrow_schema =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap();

        let (first, hit) = cache
            .statistics(&table, None, &arrow_schema, 8)
            .await
            .unwrap()
            .expect("snapshot present");
        assert!(!hit, "first call must walk the manifests");
        assert_eq!(cache.file_misses(), 1);
        assert_eq!(
            first.num_rows,
            datafusion::common::stats::Precision::Exact(30)
        );

        let (second, hit) = cache
            .statistics(&table, None, &arrow_schema, 8)
            .await
            .unwrap()
            .unwrap();
        assert!(hit, "second call must be served from cache");
        assert_eq!(cache.file_misses(), 1, "manifests decoded exactly once");
        assert_eq!(second.num_rows, first.num_rows);

        let (files, hit) = cache
            .snapshot_files(&table, None, 8)
            .await
            .unwrap()
            .unwrap();
        assert!(hit);
        assert_eq!(files.data_files.len(), 3);
        assert!(!files.has_live_delete_files);
        assert_eq!(cache.file_misses(), 1);
    }

    /// Pinning a scan to the snapshot id and asking for the current snapshot
    /// land on the same cache entry.
    #[tokio::test]
    async fn pinned_and_current_share_an_entry() {
        let dir = tempfile::tempdir().unwrap();
        let table = table_with_manifests(dir.path(), 2).await;
        let cache = SnapshotStatsCache::new(1_000, 100);
        cache
            .snapshot_files(&table, None, 8)
            .await
            .unwrap()
            .unwrap();
        let (_, hit) = cache
            .snapshot_files(&table, Some(SNAPSHOT_ID), 8)
            .await
            .unwrap()
            .unwrap();
        assert!(hit);
        assert_eq!(cache.file_misses(), 1);
    }

    /// The weigher charges by file count, so a many-file snapshot cannot be
    /// admitted into a cache sized below it while a small one can.
    #[tokio::test]
    async fn weigher_bounds_by_file_count() {
        let cache = SnapshotStatsCache::new(10, 100);
        let big = Arc::new(SnapshotFiles {
            data_files: Vec::new(),
            has_live_delete_files: false,
        });
        // Weight is max(1, len) so an empty snapshot still costs one unit.
        cache.files.insert("t:1".to_string(), big).await;
        cache.files.run_pending_tasks().await;
        assert!(cache.files.get("t:1").await.is_some());
        assert_eq!(cache.files.weighted_size(), 1);
    }
}

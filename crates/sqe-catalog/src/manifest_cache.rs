//! Global manifest file cache for Iceberg scan planning.
//!
//! Iceberg manifest files are immutable by specification: once written, their
//! content never changes. This makes them safe to cache indefinitely — there
//! is no TTL. The cache is size-bounded (weighted by entry count) and shared
//! across all sessions so that multiple concurrent queries against the same
//! table share a single cache entry.
//!
//! # What is cached
//!
//! Each manifest file (identified by its S3/object-store path) maps to a
//! `Vec<ManifestEntryData>` — a lightweight representation of the manifest
//! entries containing only the fields needed for scan planning (file path,
//! size, record count, content type, and manifest status). The full
//! `DataFile` struct is not cached because it may hold references to
//! schema-level structures that are harder to keep alive across sessions.
//!
//! # Integration point
//!
//! `IcebergScanExec::collect_data_files()` iterates over manifest entries to
//! build the list of Parquet files for a query. Without caching every
//! manifest file is fetched from S3 and parsed for every query, even when
//! the snapshot has not changed. With this cache warm queries skip the S3
//! round-trip entirely.

use std::sync::Arc;

use iceberg::spec::{DataContentType, ManifestStatus};
use moka::sync::Cache as MokaCache;

/// Lightweight, clone-friendly representation of a single manifest entry.
///
/// Only fields needed for scan planning are stored. If more fields are needed
/// (e.g. column-level statistics for min/max pruning) they should be added
/// here rather than caching the full `DataFile`, which is harder to clone.
#[derive(Clone, Debug)]
pub struct ManifestEntryData {
    /// Full S3 (or other object-store) path to the data file.
    pub file_path: String,
    /// Serialised file size in bytes; used for cost-based planning.
    pub file_size: u64,
    /// Number of records in the file.
    pub record_count: u64,
    /// Whether this is a data file, equality-delete file, or position-delete file.
    pub content_type: DataContentType,
    /// `ADDED`, `EXISTING`, or `DELETED` — deleted entries are excluded during scan.
    pub status: ManifestStatus,
}

/// Global, size-bounded LRU cache for parsed Iceberg manifest files.
///
/// Keyed by the manifest file's object-store path (e.g. `s3://bucket/…`).
/// Safe to use without TTL because Iceberg manifest files are immutable.
///
/// # Sizing
///
/// The cache is constructed with a `max_mb` budget. Each entry is
/// weighted as `entries.len() * 100` bytes (approximate; a real
/// `ManifestEntryData` is roughly 80–120 bytes depending on path length).
/// The minimum weight per entry is 256 bytes so that the cache can hold
/// at least a few entries even when the budget is very small.
///
/// # Thread safety
///
/// Uses `moka::sync::Cache`, which is `Send + Sync` and safe to share across
/// threads without external locking.
#[derive(Clone)]
pub struct ManifestCache {
    cache: MokaCache<String, Arc<Vec<ManifestEntryData>>>,
}

impl ManifestCache {
    /// Create a new manifest cache with a memory budget of `max_mb` megabytes.
    ///
    /// Pass `0` to create an effectively-disabled cache (max_capacity = 0).
    pub fn new(max_mb: u64) -> Self {
        let max_bytes = max_mb.saturating_mul(1024 * 1024);
        let cache = MokaCache::builder()
            .weigher(|_key: &String, value: &Arc<Vec<ManifestEntryData>>| {
                // Approximate weight: ~100 bytes per entry, minimum 256.
                value.len().saturating_mul(100).max(256).min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .time_to_live(std::time::Duration::from_secs(3600))
            .build();
        Self { cache }
    }

    /// Look up a manifest by its object-store path.
    ///
    /// Returns `Some(entries)` on a cache hit, `None` on a miss.
    pub fn get(&self, manifest_path: &str) -> Option<Arc<Vec<ManifestEntryData>>> {
        self.cache.get(manifest_path)
    }

    /// Insert parsed entries for a manifest path into the cache.
    pub fn insert(&self, manifest_path: String, entries: Vec<ManifestEntryData>) {
        self.cache.insert(manifest_path, Arc::new(entries));
    }

    /// Invalidate all cached entries. Call after a full catalog rebuild or
    /// when the underlying storage layout is known to have changed (rare).
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Returns the approximate number of manifest files currently cached.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns the approximate weighted size of the cache in bytes.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }
}

impl std::fmt::Debug for ManifestCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestCache")
            .field("entry_count", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(path: &str) -> ManifestEntryData {
        ManifestEntryData {
            file_path: path.to_string(),
            file_size: 1024,
            record_count: 100,
            content_type: DataContentType::Data,
            status: ManifestStatus::Added,
        }
    }

    #[test]
    fn test_miss_then_hit() {
        let cache = ManifestCache::new(64);
        assert!(cache.get("s3://bucket/manifest1.avro").is_none());

        let entries = vec![make_entry("s3://bucket/data1.parquet")];
        cache.insert("s3://bucket/manifest1.avro".to_string(), entries);

        let hit = cache.get("s3://bucket/manifest1.avro");
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().len(), 1);
    }

    #[test]
    fn test_different_manifests_are_separate() {
        let cache = ManifestCache::new(64);

        cache.insert(
            "s3://bucket/m1.avro".to_string(),
            vec![make_entry("s3://bucket/f1.parquet")],
        );
        cache.insert(
            "s3://bucket/m2.avro".to_string(),
            vec![
                make_entry("s3://bucket/f2.parquet"),
                make_entry("s3://bucket/f3.parquet"),
            ],
        );

        assert_eq!(cache.get("s3://bucket/m1.avro").unwrap().len(), 1);
        assert_eq!(cache.get("s3://bucket/m2.avro").unwrap().len(), 2);
    }

    #[test]
    fn test_invalidate_all() {
        let cache = ManifestCache::new(64);
        cache.insert(
            "s3://bucket/m.avro".to_string(),
            vec![make_entry("s3://bucket/f.parquet")],
        );
        assert!(cache.get("s3://bucket/m.avro").is_some());

        cache.invalidate_all();
        // After invalidate_all, moka clears asynchronously but entry_count
        // may lag. We verify the operation does not panic.
    }

    #[test]
    fn test_zero_budget_builds() {
        // max_mb = 0 => max_capacity = 0; should not panic.
        let cache = ManifestCache::new(0);
        cache.insert(
            "s3://bucket/m.avro".to_string(),
            vec![make_entry("s3://bucket/f.parquet")],
        );
        // With capacity 0 the entry will be immediately evicted; no panic expected.
    }
}

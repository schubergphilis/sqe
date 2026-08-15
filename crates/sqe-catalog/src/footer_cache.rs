//! LRU cache for parsed Parquet footer metadata.
//!
//! Parquet files store column statistics, row group boundaries, and schema
//! information in a footer (file metadata) section. Parsing the footer
//! requires an S3 GET of the last few kilobytes of each file. For tables
//! queried repeatedly, caching the parsed `ParquetMetaData` avoids
//! redundant S3 round-trips and deserialization.
//!
//! Uses the `moka` async cache with a size-based weigher that estimates
//! metadata memory from `num_row_groups * num_columns * 500` bytes.

use std::sync::Arc;

use moka::future::Cache;
use parquet::file::metadata::ParquetMetaData;
use prometheus::Counter;
use tracing::debug;

/// Estimate the memory weight of a cached footer entry in bytes.
///
/// Uses `num_row_groups * num_columns * 500` (minimum 1024), saturating at
/// `u32::MAX` to avoid truncation when casting to the weigher's `u32` type.
fn footer_entry_weight(num_row_groups: u64, num_columns: u64) -> u32 {
    num_row_groups
        .saturating_mul(num_columns)
        .saturating_mul(500)
        .max(1024)
        .min(u32::MAX as u64) as u32
}

/// Async LRU cache for parsed Parquet file footers, keyed by S3 URI.
#[derive(Clone)]
pub struct FooterCache {
    cache: Cache<String, Arc<ParquetMetaData>>,
    /// Cache hit counter (Prometheus).
    hits: Counter,
    /// Cache miss counter (Prometheus).
    misses: Counter,
}

impl FooterCache {
    /// Create a new footer cache with the given maximum weight in bytes.
    ///
    /// The weigher estimates each entry's memory as
    /// `num_row_groups * num_columns * 500` bytes (minimum 1024 bytes).
    pub fn new(max_size_bytes: u64, hits: Counter, misses: Counter) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_size_bytes)
            .weigher(|_key: &String, value: &Arc<ParquetMetaData>| {
                let num_row_groups = value.num_row_groups() as u64;
                let num_columns = if value.num_row_groups() > 0 {
                    value.row_group(0).num_columns() as u64
                } else {
                    // Fallback: use schema descriptor.
                    value.file_metadata().schema_descr().num_columns() as u64
                };
                footer_entry_weight(num_row_groups, num_columns)
            })
            .build();

        Self {
            cache,
            hits,
            misses,
        }
    }

    /// Get a cached footer or fetch it using the provided closure.
    ///
    /// If the footer for `path` is already cached, it is returned
    /// immediately and the hit counter is incremented. Otherwise the
    /// `fetch` closure is called, the result is cached, and the miss
    /// counter is incremented.
    pub async fn get_or_fetch<F, Fut, E>(
        &self,
        path: &str,
        fetch: F,
    ) -> Result<Arc<ParquetMetaData>, Arc<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<ParquetMetaData, E>>,
        E: Send + Sync + 'static,
    {
        // Check if already present to track hit/miss accurately.
        if let Some(cached) = self.cache.get(&path.to_string()).await {
            self.hits.inc();
            debug!(path = %path, "Footer cache hit");
            return Ok(cached);
        }

        self.misses.inc();
        debug!(path = %path, "Footer cache miss — fetching");

        let result = self
            .cache
            .try_get_with(path.to_string(), async { fetch().await.map(Arc::new) })
            .await?;

        Ok(result)
    }

    /// Returns the approximate number of entries in the cache.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns the approximate weighted size of the cache.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Invalidate all entries in the cache.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Invalidate a specific entry by path.
    pub async fn invalidate(&self, path: &str) {
        self.cache.invalidate(&path.to_string()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    /// Helper: create a minimal `ParquetMetaData` by writing a tiny Parquet
    /// file in memory and reading back its metadata. This avoids relying on
    /// internal builder APIs that vary across parquet versions.
    fn make_test_metadata(num_columns: usize) -> ParquetMetaData {
        let fields: Vec<_> = (0..num_columns)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..num_columns)
            .map(|_| Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>)
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let reader = SerializedFileReader::new(bytes::Bytes::from(buf)).unwrap();
        reader.metadata().clone()
    }

    fn test_counters() -> (Counter, Counter) {
        let hits = Counter::new("test_hits", "test").unwrap();
        let misses = Counter::new("test_misses", "test").unwrap();
        (hits, misses)
    }

    #[test]
    fn test_footer_entry_weight_does_not_overflow() {
        // Would wrap to 0 in u32: 10_000 * 100_000 * 500 = 500_000_000_000
        let weight = footer_entry_weight(10_000, 100_000);
        assert_eq!(weight, u32::MAX);
        assert_ne!(weight, 0);

        // Extreme values saturate rather than underflow.
        let weight = footer_entry_weight(u64::MAX, u64::MAX);
        assert_eq!(weight, u32::MAX);
        assert!(weight >= 1024);
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let (hits, misses) = test_counters();
        let cache = FooterCache::new(10 * 1024 * 1024, hits.clone(), misses.clone());

        let meta = make_test_metadata(5);

        // First call: miss.
        let result = cache
            .get_or_fetch("s3://bucket/file.parquet", || async {
                Ok::<_, std::io::Error>(meta.clone())
            })
            .await
            .unwrap();
        assert_eq!(result.file_metadata().schema_descr().num_columns(), 5);
        assert_eq!(misses.get(), 1.0);
        assert_eq!(hits.get(), 0.0);

        // Second call: hit.
        let result2 = cache
            .get_or_fetch::<_, _, std::io::Error>("s3://bucket/file.parquet", || async {
                panic!("should not be called on cache hit");
            })
            .await
            .unwrap();
        assert_eq!(result2.file_metadata().schema_descr().num_columns(), 5);
        assert_eq!(hits.get(), 1.0);
        assert_eq!(misses.get(), 1.0);
    }

    #[tokio::test]
    async fn test_different_keys_are_separate() {
        let (hits, misses) = test_counters();
        let cache = FooterCache::new(10 * 1024 * 1024, hits.clone(), misses.clone());

        let meta_a = make_test_metadata(3);
        let meta_b = make_test_metadata(7);

        let result_a = cache
            .get_or_fetch("s3://bucket/a.parquet", || async {
                Ok::<_, std::io::Error>(meta_a)
            })
            .await
            .unwrap();
        let result_b = cache
            .get_or_fetch("s3://bucket/b.parquet", || async {
                Ok::<_, std::io::Error>(meta_b)
            })
            .await
            .unwrap();

        assert_eq!(misses.get(), 2.0);
        // Verify the cached values are distinct.
        assert_eq!(result_a.file_metadata().schema_descr().num_columns(), 3);
        assert_eq!(result_b.file_metadata().schema_descr().num_columns(), 7);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let (hits, misses) = test_counters();
        let cache = FooterCache::new(10 * 1024 * 1024, hits.clone(), misses.clone());

        let meta = make_test_metadata(2);
        cache
            .get_or_fetch("s3://bucket/file.parquet", || async {
                Ok::<_, std::io::Error>(meta.clone())
            })
            .await
            .unwrap();

        cache.invalidate("s3://bucket/file.parquet").await;

        // After invalidation the next get_or_fetch should be a miss.
        let _result = cache
            .get_or_fetch("s3://bucket/file.parquet", || async {
                Ok::<_, std::io::Error>(meta)
            })
            .await
            .unwrap();
        assert_eq!(misses.get(), 2.0);
    }

    #[tokio::test]
    async fn test_invalidate_all() {
        let (hits, misses) = test_counters();
        let cache = FooterCache::new(10 * 1024 * 1024, hits.clone(), misses.clone());

        let meta = make_test_metadata(2);
        cache
            .get_or_fetch("s3://bucket/a.parquet", || async {
                Ok::<_, std::io::Error>(meta.clone())
            })
            .await
            .unwrap();
        cache
            .get_or_fetch("s3://bucket/b.parquet", || async {
                Ok::<_, std::io::Error>(meta.clone())
            })
            .await
            .unwrap();

        cache.invalidate_all();
    }
}

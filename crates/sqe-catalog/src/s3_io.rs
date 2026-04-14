//! S3 I/O utilities for efficient Parquet column reads.
//!
//! This module provides:
//!
//! - **Byte-range coalescing**: merge adjacent byte ranges within a configurable
//!   threshold to reduce S3 GET request count.
//! - **Parallel byte-range fetching**: fetch multiple ranges concurrently
//!   with semaphore-bounded concurrency.
//! - **File-level prefetch**: start fetching the footer of the next file while
//!   the current file is being decoded, overlapping I/O with compute.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::debug;

/// A contiguous byte range within a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRange {
    /// Byte offset from the start of the file.
    pub offset: u64,
    /// Number of bytes in this range.
    pub length: u64,
}

impl ByteRange {
    /// Create a new byte range.
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    /// The exclusive end offset of this range.
    pub fn end(&self) -> u64 {
        self.offset + self.length
    }

    /// Convert to a `std::ops::Range<u64>` for use with `object_store`.
    pub fn as_range(&self) -> Range<u64> {
        self.offset..self.end()
    }
}

/// Default coalesce threshold: 1 MB.
///
/// Byte ranges separated by a gap of at most this many bytes will be merged
/// into a single S3 GET request to reduce round-trips.
pub const DEFAULT_COALESCE_THRESHOLD: u64 = 1024 * 1024; // 1 MB

/// Merge adjacent or nearby byte ranges when the gap between them is within
/// `threshold` bytes.
///
/// The input slice is sorted by offset internally before coalescing. The
/// returned vector contains the merged ranges, which may be fewer than or
/// equal to the number of input ranges.
///
/// # Behaviour
///
/// - Empty input produces an empty output.
/// - A single range is returned as-is.
/// - Overlapping ranges are always merged (gap is effectively zero).
/// - When the gap between the end of one range and the start of the next is
///   `<= threshold`, the two are merged into one range covering both (plus the
///   gap).
pub fn coalesce_ranges(ranges: &[ByteRange], threshold: u64) -> Vec<ByteRange> {
    if ranges.is_empty() {
        return Vec::new();
    }

    // Sort by offset.
    let mut sorted: Vec<ByteRange> = ranges.to_vec();
    sorted.sort_by_key(|r| r.offset);

    let mut result: Vec<ByteRange> = Vec::with_capacity(sorted.len());
    result.push(sorted[0].clone());

    for range in &sorted[1..] {
        let last = result.last_mut().expect("result is non-empty");
        let gap = range.offset.saturating_sub(last.end());
        if gap <= threshold {
            // Extend the last range to cover this one.
            let new_end = range.end().max(last.end());
            last.length = new_end - last.offset;
        } else {
            result.push(range.clone());
        }
    }

    result
}

/// Fetch multiple byte ranges from a single S3 object concurrently.
///
/// Uses a `tokio::sync::Semaphore` to limit the number of in-flight GET
/// requests to `max_concurrent`. Each range is fetched via
/// `ObjectStore::get_range()`.
///
/// The returned `Vec<Bytes>` is in the same order as the input `ranges`.
pub async fn fetch_byte_ranges(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    ranges: &[ByteRange],
    max_concurrent: usize,
) -> object_store::Result<Vec<Bytes>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    let futures: Vec<_> = ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            let sem = Arc::clone(&semaphore);
            let store = Arc::clone(store);
            let path = path.clone();
            let r = range.as_range();
            let offset = range.offset;
            let length = range.length;
            async move {
                let _permit = sem
                    .acquire()
                    .await
                    .expect("semaphore should not be closed");
                debug!(
                    path = %path,
                    offset = offset,
                    length = length,
                    "Fetching byte range"
                );
                let data = store.get_range(&path, r).await?;
                Ok::<(usize, Bytes), object_store::Error>((idx, data))
            }
        })
        .collect();

    let results = futures::future::try_join_all(futures).await?;

    // Restore original order.
    let mut ordered = vec![Bytes::new(); ranges.len()];
    for (idx, data) in results {
        ordered[idx] = data;
    }
    Ok(ordered)
}

/// Fetch column chunks from a Parquet file, applying byte-range coalescing.
///
/// 1. Coalesces the input `ranges` using the given `coalesce_threshold`.
/// 2. Fetches the coalesced ranges concurrently (bounded by `max_concurrent`).
/// 3. Slices the fetched data back into the originally-requested ranges.
///
/// This reduces the number of S3 GET requests when column chunks are stored
/// close together in the Parquet file.
pub async fn fetch_column_chunks(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    ranges: &[ByteRange],
    coalesce_threshold: u64,
    max_concurrent: usize,
) -> object_store::Result<Vec<Bytes>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let coalesced = coalesce_ranges(ranges, coalesce_threshold);

    debug!(
        path = %path,
        original_ranges = ranges.len(),
        coalesced_ranges = coalesced.len(),
        "Coalesced byte ranges for column chunk fetch"
    );

    // Fetch the coalesced ranges.
    let fetched = fetch_byte_ranges(store, path, &coalesced, max_concurrent).await?;

    // Slice the fetched data back into the originally-requested ranges.
    let mut result = Vec::with_capacity(ranges.len());
    for range in ranges {
        // Find the coalesced range that contains this original range.
        let (ci, coalesced_range) = coalesced
            .iter()
            .enumerate()
            .find(|(_, cr)| cr.offset <= range.offset && cr.end() >= range.end())
            .ok_or_else(|| object_store::Error::Generic { store: "s3", source: Box::new(std::io::Error::other("range not covered by coalesced range")) })?;

        let local_offset = (range.offset - coalesced_range.offset) as usize;
        let local_end = local_offset + range.length as usize;
        let slice = fetched[ci].slice(local_offset..local_end);
        result.push(slice);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// File-level prefetch pipeline
// ---------------------------------------------------------------------------

/// Default number of concurrent byte-range requests per file.
pub const DEFAULT_CONCURRENT_REQUESTS_PER_FILE: usize = 4;

/// Default maximum number of files fetched concurrently.
pub const DEFAULT_MAX_CONCURRENT_FILES: usize = 8;

/// Default prefetch depth: how many files ahead to prefetch.
pub const DEFAULT_PREFETCH_DEPTH: usize = 4;

/// A handle to a prefetched footer read.
///
/// The caller spawns a background task that reads the last `n` bytes of a
/// file (the Parquet footer / metadata section). When the caller is ready
/// to decode the next file it awaits this handle to obtain the bytes.
pub struct PrefetchHandle {
    handle: JoinHandle<object_store::Result<Bytes>>,
}

impl PrefetchHandle {
    /// Await the prefetched footer bytes.
    pub async fn resolve(self) -> object_store::Result<Bytes> {
        self.handle
            .await
            .map_err(|e| object_store::Error::JoinError { source: e })?
    }
}

/// Start prefetching the footer of a file in the background.
///
/// Reads the last `footer_size` bytes from the given path. Typical Parquet
/// footers are 4-64 KB, but rich statistics or large schemas can push this
/// higher. A conservative default of 64 KB covers most cases.
///
/// Returns a [`PrefetchHandle`] that can be awaited when the caller is
/// ready to parse the footer.
pub fn prefetch_footer(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    file_size: u64,
    footer_size: u64,
) -> PrefetchHandle {
    let offset = file_size.saturating_sub(footer_size);

    let handle = tokio::spawn(async move {
        debug!(
            path = %path,
            offset = offset,
            length = file_size - offset,
            "Prefetching Parquet footer"
        );
        store.get_range(&path, offset..file_size).await
    });

    PrefetchHandle { handle }
}

/// Process multiple files with prefetch overlap.
///
/// For each file in `file_infos`, the provided `process_fn` closure is called
/// with the file path and an optional prefetched footer `Bytes`. While the
/// closure processes the current file, upcoming files' footers are prefetched
/// in the background up to `prefetch_depth` files ahead.
///
/// `file_infos` is a slice of `(path, file_size)` tuples.
/// `footer_read_size` is how many bytes to read from the end of each file
/// for the footer (default: 64 KB).
/// `prefetch_depth` controls how many files ahead to prefetch. Default: 1
/// (legacy behaviour). Set to 4-8 for high-latency S3 connections.
pub async fn process_files_with_prefetch<F, Fut, T, E>(
    store: Arc<dyn ObjectStore>,
    file_infos: &[(ObjectPath, u64)],
    footer_read_size: u64,
    process_fn: F,
) -> Result<Vec<T>, E>
where
    F: FnMut(ObjectPath, u64, Option<Bytes>) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: From<object_store::Error>,
{
    process_files_with_prefetch_depth(store, file_infos, footer_read_size, 1, process_fn).await
}

/// Like [`process_files_with_prefetch`] but with configurable prefetch depth.
///
/// `prefetch_depth` controls how many upcoming files' footers are prefetched
/// concurrently. A depth of 1 prefetches the next file only (original behavior).
/// Higher values (4-8) improve throughput on high-latency S3 connections by
/// overlapping more I/O with compute.
pub async fn process_files_with_prefetch_depth<F, Fut, T, E>(
    store: Arc<dyn ObjectStore>,
    file_infos: &[(ObjectPath, u64)],
    footer_read_size: u64,
    prefetch_depth: usize,
    mut process_fn: F,
) -> Result<Vec<T>, E>
where
    F: FnMut(ObjectPath, u64, Option<Bytes>) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: From<object_store::Error>,
{
    use std::collections::VecDeque;

    if file_infos.is_empty() {
        return Ok(Vec::new());
    }

    let depth = prefetch_depth.max(1);
    let mut results = Vec::with_capacity(file_infos.len());
    let mut prefetch_queue: VecDeque<PrefetchHandle> = VecDeque::new();

    // Seed the prefetch queue with up to `depth` upcoming files.
    for j in 1..=depth.min(file_infos.len().saturating_sub(1)) {
        let (next_path, next_size) = &file_infos[j];
        prefetch_queue.push_back(prefetch_footer(
            Arc::clone(&store),
            next_path.clone(),
            *next_size,
            footer_read_size,
        ));
    }

    for (i, (path, file_size)) in file_infos.iter().enumerate() {
        // Resolve the front of the prefetch queue (if any) for the current file.
        // The first file (i=0) was not prefetched; subsequent files have a handle.
        let footer_bytes = if i > 0 && !prefetch_queue.is_empty() {
            let handle = prefetch_queue.pop_front().unwrap();
            Some(handle.resolve().await.map_err(E::from)?)
        } else {
            None
        };

        // Enqueue the next file beyond our current prefetch window.
        let next_to_prefetch = i + 1 + prefetch_queue.len();
        if next_to_prefetch < file_infos.len() {
            let (next_path, next_size) = &file_infos[next_to_prefetch];
            prefetch_queue.push_back(prefetch_footer(
                Arc::clone(&store),
                next_path.clone(),
                *next_size,
                footer_read_size,
            ));
        }

        // Process the current file.
        let result = process_fn(path.clone(), *file_size, footer_bytes).await?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalesce_empty_input() {
        let result = coalesce_ranges(&[], 1024);
        assert!(result.is_empty());
    }

    #[test]
    fn test_coalesce_single_range() {
        let ranges = vec![ByteRange::new(100, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(100, 200));
    }

    #[test]
    fn test_coalesce_adjacent_ranges_merge() {
        // Two ranges that are exactly adjacent (gap = 0).
        let ranges = vec![ByteRange::new(0, 100), ByteRange::new(100, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(0, 300));
    }

    #[test]
    fn test_coalesce_ranges_within_threshold() {
        // Gap of 500 bytes, threshold is 1024 -- should merge.
        let ranges = vec![ByteRange::new(0, 100), ByteRange::new(600, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(0, 800));
    }

    #[test]
    fn test_coalesce_gap_exceeds_threshold() {
        // Gap of 2000 bytes, threshold is 1024 -- should NOT merge.
        let ranges = vec![ByteRange::new(0, 100), ByteRange::new(2100, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ByteRange::new(0, 100));
        assert_eq!(result[1], ByteRange::new(2100, 200));
    }

    #[test]
    fn test_coalesce_gap_exactly_at_threshold() {
        // Gap equals threshold -- should merge (gap <= threshold).
        let ranges = vec![ByteRange::new(0, 100), ByteRange::new(1124, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(0, 1324));
    }

    #[test]
    fn test_coalesce_gap_one_over_threshold() {
        // Gap is one byte over threshold -- should NOT merge.
        let ranges = vec![ByteRange::new(0, 100), ByteRange::new(1125, 200)];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_overlapping_ranges() {
        // Overlapping ranges should always merge.
        let ranges = vec![ByteRange::new(0, 150), ByteRange::new(100, 200)];
        let result = coalesce_ranges(&ranges, 0); // threshold=0, but overlapping
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(0, 300));
    }

    #[test]
    fn test_coalesce_unsorted_input() {
        // Ranges are not sorted by offset -- should still work.
        let ranges = vec![
            ByteRange::new(500, 100),
            ByteRange::new(0, 100),
            ByteRange::new(200, 100),
        ];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ByteRange::new(0, 600));
    }

    #[test]
    fn test_coalesce_multiple_groups() {
        // Three ranges: first two are close, third is far.
        let ranges = vec![
            ByteRange::new(0, 100),
            ByteRange::new(200, 100),    // gap=100, within threshold
            ByteRange::new(10_000, 100), // gap=9700, exceeds threshold
        ];
        let result = coalesce_ranges(&ranges, 1024);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ByteRange::new(0, 300));
        assert_eq!(result[1], ByteRange::new(10_000, 100));
    }

    #[test]
    fn test_coalesce_zero_threshold() {
        // With threshold=0, only adjacent/overlapping ranges merge.
        let ranges = vec![
            ByteRange::new(0, 100),
            ByteRange::new(100, 100), // adjacent, gap=0 => merge
            ByteRange::new(201, 100), // gap=1, not adjacent => separate
        ];
        let result = coalesce_ranges(&ranges, 0);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ByteRange::new(0, 200));
        assert_eq!(result[1], ByteRange::new(201, 100));
    }

    #[test]
    fn test_byte_range_end() {
        let r = ByteRange::new(100, 200);
        assert_eq!(r.end(), 300);
    }

    #[test]
    fn test_byte_range_as_range() {
        let r = ByteRange::new(100, 200);
        assert_eq!(r.as_range(), 100u64..300u64);
    }

    // ── Parallel fetch + prefetch tests ────────────────────────────

    use object_store::memory::InMemory;
    use object_store::PutPayload;

    /// Helper: create an in-memory object store with test files.
    async fn make_test_store(files: Vec<(&str, Vec<u8>)>) -> Arc<dyn ObjectStore> {
        let store = InMemory::new();
        for (path, data) in files {
            store
                .put(
                    &ObjectPath::from(path),
                    PutPayload::from(Bytes::from(data)),
                )
                .await
                .unwrap();
        }
        Arc::new(store)
    }

    #[tokio::test]
    async fn test_fetch_byte_ranges_single() {
        let data = b"Hello, World! This is test data for byte range reads.".to_vec();
        let store = make_test_store(vec![("test.parquet", data)]).await;
        let path = ObjectPath::from("test.parquet");

        let ranges = vec![ByteRange::new(0, 5)];
        let result = fetch_byte_ranges(&store, &path, &ranges, 4).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0][..], b"Hello");
    }

    #[tokio::test]
    async fn test_fetch_byte_ranges_multiple() {
        let data = b"Hello, World! This is test data for byte range reads.".to_vec();
        let store = make_test_store(vec![("test.parquet", data)]).await;
        let path = ObjectPath::from("test.parquet");

        let ranges = vec![
            ByteRange::new(0, 5),
            ByteRange::new(7, 6),
        ];
        let result = fetch_byte_ranges(&store, &path, &ranges, 4).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0][..], b"Hello");
        assert_eq!(&result[1][..], b"World!");
    }

    #[tokio::test]
    async fn test_fetch_byte_ranges_empty() {
        let store = make_test_store(vec![("test.parquet", b"data".to_vec())]).await;
        let path = ObjectPath::from("test.parquet");

        let result = fetch_byte_ranges(&store, &path, &[], 4).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_column_chunks_with_coalescing() {
        let data = vec![0xABu8; 2048];
        let store = make_test_store(vec![("test.parquet", data)]).await;
        let path = ObjectPath::from("test.parquet");

        // Two ranges with a gap of 100 bytes -- should coalesce with threshold 200.
        let ranges = vec![
            ByteRange::new(0, 100),
            ByteRange::new(200, 100),
        ];
        let result = fetch_column_chunks(&store, &path, &ranges, 200, 4)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 100);
        assert_eq!(result[1].len(), 100);
    }

    #[tokio::test]
    async fn test_prefetch_footer() {
        let data = b"file-header-data-and-then-the-FOOTER".to_vec();
        let file_size = data.len() as u64;
        let store = make_test_store(vec![("test.parquet", data)]).await;
        let path = ObjectPath::from("test.parquet");

        let handle = prefetch_footer(store, path, file_size, 6);
        let footer = handle.resolve().await.unwrap();
        assert_eq!(&footer[..], b"FOOTER");
    }

    #[tokio::test]
    async fn test_process_files_with_prefetch() {
        let file_a = vec![0u8; 1000];
        let file_b = vec![1u8; 2000];
        let file_c = vec![2u8; 500];

        let store = make_test_store(vec![
            ("a.parquet", file_a),
            ("b.parquet", file_b),
            ("c.parquet", file_c),
        ])
        .await;

        let file_infos: Vec<(ObjectPath, u64)> = vec![
            (ObjectPath::from("a.parquet"), 1000),
            (ObjectPath::from("b.parquet"), 2000),
            (ObjectPath::from("c.parquet"), 500),
        ];

        let results = process_files_with_prefetch(
            store,
            &file_infos,
            64,
            |_path, file_size, footer_bytes| async move {
                Ok::<_, object_store::Error>((file_size, footer_bytes.is_some()))
            },
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 3);
        // First file has no prefetch (None), subsequent files have prefetched footers.
        assert_eq!(results[0], (1000, false));
        assert_eq!(results[1], (2000, true));
        assert_eq!(results[2], (500, true));
    }

    #[tokio::test]
    async fn test_process_files_with_prefetch_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let results = process_files_with_prefetch(
            store,
            &[],
            64,
            |_path, _size, _footer| async { Ok::<_, object_store::Error>(()) },
        )
        .await
        .unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_process_files_with_prefetch_single_file() {
        let file_a = vec![0u8; 500];
        let store = make_test_store(vec![("a.parquet", file_a)]).await;

        let file_infos = vec![(ObjectPath::from("a.parquet"), 500u64)];

        let results = process_files_with_prefetch(
            store,
            &file_infos,
            64,
            |_path, file_size, footer_bytes| async move {
                Ok::<_, object_store::Error>((file_size, footer_bytes.is_some()))
            },
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (500, false)); // single file: no prefetch
    }
}

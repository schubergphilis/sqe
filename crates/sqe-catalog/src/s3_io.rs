//! S3 I/O utilities for efficient Parquet column reads.
//!
//! This module provides byte-range coalescing and parallel fetching to reduce
//! the number of S3 GET requests when reading Parquet column chunks. When
//! multiple column chunks are close together in the file, their byte ranges
//! are merged into a single request if the gap is within a configurable
//! threshold (default: 1 MB).

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::sync::Semaphore;
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
            .expect("every original range must be covered by a coalesced range");

        let local_offset = (range.offset - coalesced_range.offset) as usize;
        let local_end = local_offset + range.length as usize;
        let slice = fetched[ci].slice(local_offset..local_end);
        result.push(slice);
    }

    Ok(result)
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
}

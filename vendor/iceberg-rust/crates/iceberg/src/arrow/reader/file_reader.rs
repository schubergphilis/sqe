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

//! Async Parquet file reader that adapts an Iceberg `FileRead` to parquet's `AsyncFileReader`.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

use super::ParquetReadOptions;
use crate::io::{FileMetadata, FileRead};

fn validate_range_read(range: &Range<u64>, bytes: Bytes) -> parquet::errors::Result<Bytes> {
    let expected_len = range
        .end
        .checked_sub(range.start)
        .ok_or_else(|| {
            parquet::errors::ParquetError::General(format!(
                "Invalid Parquet byte range {}..{}",
                range.start, range.end
            ))
        })
        .and_then(|len| usize::try_from(len).map_err(Into::into))?;

    if bytes.len() != expected_len {
        return Err(parquet::errors::ParquetError::EOF(format!(
            "Parquet byte range {}..{} returned {} of {expected_len} bytes; \
             the file may be truncated or its recorded size may be incorrect",
            range.start,
            range.end,
            bytes.len()
        )));
    }

    Ok(bytes)
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
pub struct ArrowFileReader {
    meta: FileMetadata,
    parquet_read_options: ParquetReadOptions,
    r: Box<dyn FileRead>,
}

impl ArrowFileReader {
    /// Create a new ArrowFileReader
    pub fn new(meta: FileMetadata, r: Box<dyn FileRead>) -> Self {
        Self {
            meta,
            parquet_read_options: ParquetReadOptions::builder().build(),
            r,
        }
    }

    /// Configure all Parquet read options.
    pub(crate) fn with_parquet_read_options(mut self, options: ParquetReadOptions) -> Self {
        self.parquet_read_options = options;
        self
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let bytes = self
                .r
                .read(range.clone())
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
            validate_range_read(&range, bytes)
        })
    }

    /// Override the default `get_byte_ranges` which calls `get_bytes` sequentially.
    /// The parquet reader calls this to fetch column chunks for a row group, so
    /// without this override each column chunk is a serial round-trip to object storage.
    /// Adapted from object_store's `coalesce_ranges` in `util.rs`.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let coalesce_bytes = self.parquet_read_options.range_coalesce_bytes();
        let concurrency = self.parquet_read_options.range_fetch_concurrency().max(1);

        async move {
            // Merge nearby ranges to reduce the number of object store requests.
            let fetch_ranges = merge_ranges(&ranges, coalesce_bytes);
            let r = &self.r;

            // Fetch merged ranges concurrently.
            let fetched: Vec<Bytes> = futures::stream::iter(fetch_ranges.iter().cloned())
                .map(|range| async move {
                    let bytes = r
                        .read(range.clone())
                        .await
                        .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                    validate_range_read(&range, bytes)
                })
                .buffered(concurrency)
                .try_collect()
                .await?;

            // Slice the fetched data back into the originally requested ranges.
            ranges
                .iter()
                .map(|range| {
                    let idx = fetch_ranges
                        .partition_point(|v| v.start <= range.start)
                        .checked_sub(1)
                        .ok_or_else(|| {
                            parquet::errors::ParquetError::General(format!(
                                "No fetched Parquet range contains {}..{}",
                                range.start, range.end
                            ))
                        })?;
                    let fetch_range = &fetch_ranges[idx];
                    let fetch_bytes = &fetched[idx];
                    let start = usize::try_from(range.start - fetch_range.start)?;
                    let end = usize::try_from(range.end - fetch_range.start)?;
                    if end > fetch_bytes.len() {
                        return Err(parquet::errors::ParquetError::EOF(format!(
                            "Fetched Parquet range {}..{} does not contain requested range {}..{}",
                            fetch_range.start, fetch_range.end, range.start, range.end
                        )));
                    }
                    Ok(fetch_bytes.slice(start..end))
                })
                .collect()
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let decryption_properties = options
            .and_then(|opts| opts.file_decryption_properties())
            .cloned();

        let metadata_options = options.map(|opts| opts.metadata_options().clone());

        async move {
            let reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(self.parquet_read_options.metadata_size_hint())
                // Set the page policy first because it updates both column and offset policies.
                .with_page_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_page_index(),
                ))
                .with_column_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_column_index(),
                ))
                .with_offset_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_offset_index(),
                ))
                .with_metadata_options(metadata_options)
                .with_decryption_properties(decryption_properties);
            let size = self.meta.size;
            let meta = reader.load_and_finish(self, size).await?;

            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

/// Merge overlapping or nearby byte ranges, combining ranges with gaps <= `coalesce` bytes.
/// Adapted from object_store's `merge_ranges` in `util.rs`.
fn merge_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|r| r.start);

    let mut merged = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        merged.push(ranges[start_idx].start..range_end);
        start_idx = end_idx;
        end_idx += 1;
    }

    merged
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use parquet::arrow::async_reader::AsyncFileReader;

    use super::{ArrowFileReader, ParquetReadOptions, merge_ranges};
    use crate::io::{FileMetadata, FileRead};

    #[test]
    fn test_merge_ranges_empty() {
        assert_eq!(merge_ranges(&[], 1024), Vec::<Range<u64>>::new());
    }

    #[test]
    fn test_merge_ranges_no_coalesce() {
        // Ranges far apart should not be merged
        let ranges = vec![0..100, 1_000_000..1_000_100];
        let merged = merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..100, 1_000_000..1_000_100]);
    }

    #[test]
    fn test_merge_ranges_coalesce() {
        // Ranges within the gap threshold should be merged
        let ranges = vec![0..100, 200..300, 500..600];
        let merged = merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    #[test]
    fn test_merge_ranges_overlapping() {
        let ranges = vec![0..200, 100..300];
        let merged = merge_ranges(&ranges, 0);
        assert_eq!(merged, vec![0..300]);
    }

    #[test]
    fn test_merge_ranges_unsorted() {
        let ranges = vec![500..600, 0..100, 200..300];
        let merged = merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    /// Mock FileRead backed by a flat byte buffer.
    struct MockFileRead {
        data: bytes::Bytes,
    }

    impl MockFileRead {
        fn new(size: usize) -> Self {
            // Fill with sequential byte values so slices are verifiable.
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            Self {
                data: bytes::Bytes::from(data),
            }
        }
    }

    #[async_trait::async_trait]
    impl FileRead for MockFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<bytes::Bytes> {
            let start = usize::try_from(range.start).unwrap().min(self.data.len());
            let end = usize::try_from(range.end).unwrap().min(self.data.len());
            Ok(self.data.slice(start..end))
        }
    }

    #[tokio::test]
    async fn test_get_bytes_rejects_short_read() {
        let mut reader =
            ArrowFileReader::new(FileMetadata { size: 200 }, Box::new(MockFileRead::new(100)));

        let error = reader.get_bytes(90..110).await.unwrap_err();
        assert!(matches!(error, parquet::errors::ParquetError::EOF(_)));
        assert!(error.to_string().contains("file may be truncated"));
    }

    #[tokio::test]
    async fn test_get_byte_ranges_rejects_short_read() {
        let mut reader =
            ArrowFileReader::new(FileMetadata { size: 200 }, Box::new(MockFileRead::new(100)));

        let error = reader
            .get_byte_ranges(vec![80..90, 90..110])
            .await
            .unwrap_err();
        assert!(matches!(error, parquet::errors::ParquetError::EOF(_)));
    }

    #[tokio::test]
    async fn test_get_byte_ranges_no_coalesce() {
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
                    .with_range_coalesce_bytes(0)
                    .build(),
            );

        let result = reader
            .get_byte_ranges(vec![0..100, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_with_coalesce() {
        let mock = MockFileRead::new(1024);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(200..300);
        let expected_2 = mock.data.slice(500..600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
                    .with_range_coalesce_bytes(1024)
                    .build(),
            );

        // All ranges within coalesce threshold — should merge into one fetch.
        let result = reader
            .get_byte_ranges(vec![0..100, 200..300, 500..600])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
        assert_eq!(result[2], expected_2);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_empty() {
        let mock = MockFileRead::new(1024);
        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock));

        let result = reader.get_byte_ranges(vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_get_byte_ranges_coalesce_max() {
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
                    .with_range_coalesce_bytes(u64::MAX)
                    .build(),
            );

        // u64::MAX coalesce — all ranges merge into a single fetch.
        let result = reader
            .get_byte_ranges(vec![0..100, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_concurrency_zero() {
        // concurrency=0 is clamped to 1, so this should not hang.
        let mock = MockFileRead::new(1024);
        let expected = mock.data.slice(0..100);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
                    .with_range_fetch_concurrency(0)
                    .build(),
            );

        let result = reader
            .get_byte_ranges(vec![0..100, 200..300])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_concurrency_one() {
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(500..600);
        let expected_2 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
                    .with_range_coalesce_bytes(0)
                    .with_range_fetch_concurrency(1)
                    .build(),
            );

        // concurrency=1 with no coalescing — sequential fetches.
        let result = reader
            .get_byte_ranges(vec![0..100, 500..600, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
        assert_eq!(result[2], expected_2);
    }
}

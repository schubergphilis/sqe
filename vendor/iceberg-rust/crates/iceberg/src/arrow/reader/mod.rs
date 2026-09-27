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

//! Parquet file data reader

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::Result;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::expr::DynamicPredicate;
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::util::available_parallelism;

/// SQE PATCH (sqe#131): default split target for large file scan tasks, in
/// bytes. Whole-file Parquet tasks at least twice this size are split into
/// ranged subtasks so their row groups decode in parallel across runtime
/// worker threads.
pub const DEFAULT_TASK_SPLIT_TARGET_SIZE: u64 = 128 * 1024 * 1024;

/// SQE PATCH (sqe#367): admission control for file scan task decode work.
///
/// The reader calls [`DecodeGate::admit`] before dispatching each file scan
/// (sub)task on the parallel decode path and holds the returned guard until
/// that subtask's batches have all been forwarded. Engines implement this to
/// bound decode concurrency across the scan's partitions (each `read()` call
/// otherwise gets its own `concurrency_limit_data_files`-wide semaphore) and
/// to reserve the estimated decode memory against an operator memory pool,
/// so memory pressure surfaces as a typed scan error instead of a host OOM
/// kill.
pub trait DecodeGate: Send + Sync + std::fmt::Debug {
    /// Admit a decode whose compressed input is approximately
    /// `estimated_bytes`. Resolves once capacity is available; the returned
    /// guard releases the admission on drop. An error denies the decode and
    /// fails the scan stream.
    fn admit(&self, estimated_bytes: u64) -> BoxFuture<'_, Result<Box<dyn std::any::Any + Send>>>;

    /// SQE PATCH (sqe#scan-fetch-pipeline): admit the FETCH stage of a
    /// subtask (metadata load + row-group column-chunk fetch + row-filter
    /// evaluation), whose compressed input is approximately
    /// `estimated_bytes`. Only called on the fetch-staged path
    /// ([`ArrowReaderBuilder::with_fetch_ahead`]). Implementations bound the
    /// number of subtasks concurrently in their fetch stage and reserve the
    /// fetched bytes against a memory pool. The default admits immediately
    /// with no bound, matching pre-staging behaviour for gates that only
    /// implement `admit`.
    fn admit_fetch(
        &self,
        estimated_bytes: u64,
    ) -> BoxFuture<'_, Result<Box<dyn std::any::Any + Send>>> {
        let _ = estimated_bytes;
        Box::pin(async { Ok(Box::new(()) as Box<dyn std::any::Any + Send>) })
    }
}

/// SQE PATCH: engine-side reader controls carried from the builder into
/// the reader and the per-task pipeline. Kept in one struct so the apache
/// builder/reader shape only gains a single field.
#[derive(Clone, Debug)]
pub(crate) struct SqeReadOptions {
    /// Sampled once per file scan task and ANDed into the static predicate.
    pub(crate) dynamic_predicate: Option<Arc<dyn DynamicPredicate>>,
    /// Byte-range split target for large whole-file tasks; `None` disables.
    pub(crate) task_split_target_size: Option<u64>,
    /// SQE PATCH (sqe#367): optional decode admission gate.
    pub(crate) decode_gate: Option<Arc<dyn DecodeGate>>,
    /// SQE PATCH (sqe#369): parquet bloom-filter (SBBF) row-group probing.
    pub(crate) bloom_filter_probing_enabled: bool,
    /// SQE PATCH (sqe#369): literal cap for a bloom-probed conjunct.
    pub(crate) bloom_probe_max_values: usize,
    /// SQE PATCH (sqe#scan-fetch-pipeline): fetch/decode pipelining depth;
    /// `0` disables staging.
    pub(crate) fetch_ahead: usize,
}

impl Default for SqeReadOptions {
    fn default() -> Self {
        Self {
            dynamic_predicate: None,
            task_split_target_size: Some(DEFAULT_TASK_SPLIT_TARGET_SIZE),
            decode_gate: None,
            bloom_filter_probing_enabled: true,
            bloom_probe_max_values:
                crate::expr::sbbf_row_group_evaluator::DEFAULT_BLOOM_PROBE_MAX_VALUES,
            fetch_ahead: 0,
        }
    }
}

/// Default gap between byte ranges below which they are coalesced into a
/// single request. Matches object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
const DEFAULT_RANGE_COALESCE_BYTES: u64 = 1024 * 1024;

/// Default maximum number of coalesced byte ranges fetched concurrently.
/// Matches object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
const DEFAULT_RANGE_FETCH_CONCURRENCY: usize = 10;

/// Default number of bytes to prefetch when parsing Parquet footer metadata.
/// Matches DataFusion's default `ParquetOptions::metadata_size_hint`.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;

mod file_reader;
mod options;
mod pipeline;
mod positional_deletes;
mod predicate_visitor;
mod projection;
mod row_filter;
mod row_lineage;
pub use file_reader::ArrowFileReader;
pub(crate) use options::ParquetReadOptions;
use predicate_visitor::{CollectFieldIdVisitor, PredicateConverter};
use projection::{
    add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema,
    find_leaf_by_field_id,
};

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    concurrency_limit_data_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    bloom_filter_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    runtime: Runtime,
    sqe: SqeReadOptions,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, runtime: Runtime) -> Self {
        let num_cpus = available_parallelism().get();

        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            concurrency_limit_data_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
            bloom_filter_enabled: false,
            parquet_read_options: ParquetReadOptions::builder().build(),
            runtime,
            sqe: SqeReadOptions::default(),
        }
    }

    /// SQE PATCH (sqe#scan-fetch-pipeline): enable fetch/decode pipelining
    /// with up to `fetch_ahead` subtasks per stream in their fetch stage.
    /// `0` (default) keeps the single-stage behaviour, where one admission
    /// gates a subtask's whole fetch + decode + emit lifetime. Staged, a
    /// subtask holds only a fetch admission ([`DecodeGate::admit_fetch`])
    /// through metadata load, column-chunk fetch and row-filter evaluation,
    /// and swaps it for a decode admission ([`DecodeGate::admit`]) before
    /// draining decoded batches. Needs a decode gate; ignored without one.
    pub fn with_fetch_ahead(mut self, fetch_ahead: usize) -> Self {
        self.sqe.fetch_ahead = fetch_ahead;
        self
    }

    /// SQE PATCH (sqe#369): enable or disable parquet bloom-filter (SBBF)
    /// row-group probing for positive membership conjuncts (`IN` sets /
    /// equality literals, including sealed hash-join runtime filters).
    /// Requires row group filtering and blooms present in the file.
    pub fn with_bloom_filter_probing_enabled(mut self, enabled: bool) -> Self {
        self.sqe.bloom_filter_probing_enabled = enabled;
        self
    }

    /// SQE PATCH (sqe#369): cap on the number of literals a membership
    /// conjunct may carry and still be bloom-probed.
    pub fn with_bloom_probe_max_values(mut self, max_values: usize) -> Self {
        self.sqe.bloom_probe_max_values = max_values;
        self
    }

    /// SQE PATCH (sqe#367): attach a [`DecodeGate`] consulted before each
    /// file scan (sub)task decode. See the trait docs.
    pub fn with_decode_gate(mut self, gate: Arc<dyn DecodeGate>) -> Self {
        self.sqe.decode_gate = Some(gate);
        self
    }

    /// SQE PATCH (sqe#131): sets the byte-range split target for large
    /// Parquet file scan tasks, or disables splitting with `None`. Whole-file
    /// tasks at least twice this size are split into ranged subtasks (row
    /// groups assigned by midpoint) so a handful of large files decode on
    /// many cores. Has no effect when the data-file concurrency limit is 1
    /// and no decode gate is set.
    pub fn with_task_split_target_size(mut self, val: Option<u64>) -> Self {
        self.sqe.task_split_target_size = val;
        self
    }

    /// SQE PATCH (dynamic predicate, apache/iceberg-rust#2376): attach a
    /// [`DynamicPredicate`] that the reader samples once per file scan task
    /// and ANDs into the static predicate. See
    /// [`crate::scan::TableScanBuilder::with_dynamic_predicate`].
    pub fn with_dynamic_predicate(mut self, dynamic_predicate: Arc<dyn DynamicPredicate>) -> Self {
        self.sqe.dynamic_predicate = Some(dynamic_predicate);
        self
    }

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.concurrency_limit_data_files = val;
        self
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Determines whether to enable row group filtering.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Determines whether to enable bloom filter-based row group filtering.
    ///
    /// When enabled, if a read is performed with an equality or IN predicate,
    /// the bloom filter for relevant columns in each row group is read and
    /// checked. Row groups where the bloom filter proves the value is absent
    /// are skipped entirely.
    ///
    /// Defaults to disabled. Each bloom filter is a separate read, and they are
    /// issued serially — one round trip per relevant column per row group, before
    /// any data is read. TODO(#3191)
    pub fn with_bloom_filter_enabled(mut self, bloom_filter_enabled: bool) -> Self {
        self.bloom_filter_enabled = bloom_filter_enabled;
        self
    }

    /// Provide a hint as to the number of bytes to prefetch for parsing the Parquet metadata
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.parquet_read_options.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Sets the gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's OBJECT_STORE_COALESCE_DEFAULT.
    pub fn with_range_coalesce_bytes(mut self, range_coalesce_bytes: u64) -> Self {
        self.parquet_read_options.range_coalesce_bytes = range_coalesce_bytes;
        self
    }

    /// Sets the maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's OBJECT_STORE_COALESCE_PARALLEL.
    pub fn with_range_fetch_concurrency(mut self, range_fetch_concurrency: usize) -> Self {
        self.parquet_read_options.range_fetch_concurrency = range_fetch_concurrency;
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                self.concurrency_limit_data_files,
                self.runtime.clone(),
            ),
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            bloom_filter_enabled: self.bloom_filter_enabled,
            parquet_read_options: self.parquet_read_options,
            runtime: self.runtime,
            sqe: self.sqe,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,

    /// the maximum number of data files that can be fetched at the same time
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    bloom_filter_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    /// SQE PATCH (sqe#131): spawns the per-subtask decode workers.
    runtime: Runtime,
    sqe: SqeReadOptions,
}

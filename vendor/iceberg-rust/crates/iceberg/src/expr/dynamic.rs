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

//! Dynamic predicates that may evolve during scan execution.
//!
//! This is the iceberg-rust-side counterpart to Trino's `DynamicFilter`
//! and Spark's `SupportsRuntimeV2Filtering`. It lets a caller (typically
//! a query engine integration such as iceberg-datafusion) hand the scan a
//! filter whose contents can change between calls — for example, a hash
//! join build-side bloom that is empty at planning time and fills in
//! only after the build side has finished.
//!
//! The reader samples the dynamic predicate **once per file scan task**
//! (right before the Parquet file for that task is opened) and ANDs the
//! result into whatever static predicate is already on the task. The
//! combined predicate then participates in all three of the reader's
//! existing pruning paths: row-group min/max skipping, page-index row
//! selection, and the post-decode `RowFilter`. No reader-side changes
//! are needed beyond the intersection.
//!
//! See the upstream tracking issue
//! <https://github.com/apache/iceberg-rust/issues/2376> for context.

use std::fmt::Debug;

use super::Predicate;

/// A factory for predicates whose contents may change between calls.
///
/// `current()` is consulted once per [`crate::scan::FileScanTask`].
/// Returning `None` means "no useful filter is available yet" and the
/// task's static predicate alone is used. Returning `Some(predicate)`
/// asks the reader to AND that predicate into the task's static one
/// before evaluating row-group statistics, page indexes, or the row
/// filter.
///
/// Implementations must be `Send + Sync` because the reader processes
/// tasks concurrently.
pub trait DynamicPredicate: Send + Sync + Debug {
    /// Snapshot the current state of this dynamic predicate. Called
    /// once per file scan task. The returned value need not be stable
    /// across calls; later calls may return a more selective predicate
    /// once the underlying source (e.g. a hash join build) has more
    /// information.
    fn current(&self) -> Option<Predicate>;
}

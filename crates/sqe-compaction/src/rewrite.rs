//! Delete-aware bin-pack / sort rewrite primitives shared by
//! `sqe-coordinator`'s `CALL system.rewrite_data_files` procedure and, in a
//! later Phase 4c task, the worker-side `compact_file_group` distributed
//! action.
//!
//! Moved verbatim from `sqe-coordinator/src/maintenance.rs` (Phase 4c Task
//! 1). What stayed behind in `maintenance.rs`: `rewrite_data_files` /
//! `rewrite_data_files_once` (the retry loop + the commit via
//! `RewriteFilesAction`, which needs a `Session`/catalog bridge), the
//! `collect_live_data_files` catalog scan (still catalog-shaped enough to
//! stay put), and `parse_sort_spec` (parses the `CALL` procedure's string
//! arguments into a [`SortSpec`], not a compaction primitive itself). Those
//! callers still reach every symbol below via `crate::maintenance::`
//! re-exports.
//!
//! `is_live_delete_entry` / `collect_live_delete_files` were later
//! deduplicated into this module (they were byte-identical copies in
//! `sqe-coordinator::maintenance` and `sqe-worker::compaction`, both walking
//! an `IcebergTable`'s manifest list with no catalog dependency): both crates
//! already depend on `sqe-compaction`, so this is a pure move, not a new
//! dependency edge.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{DataContentType, DataFile, ManifestStatus};
use iceberg::table::Table as IcebergTable;
use sqe_core::SqeError;

use crate::writer::{write_data_files_streaming, FanoutLimits, UploadedPaths};

/// A snapshot-pinned, delete-aware read plan for a compaction pass.
///
/// `scan` is the configured `TableScan`; `tasks_by_path` maps each live data
/// file's path to the `FileScanTask`s that cover it, with their applicable
/// position and equality delete files attached. Reading a data file's tasks
/// through `scan.read_tasks_to_arrow_with_metrics` applies those deletes, so the
/// compacted output never carries logically-deleted rows.
///
/// Modeled on `WriteHandler::plan_delete_aware_read`; kept as a free function
/// here so the maintenance path does not depend on the write handler's session
/// context.
pub struct DeleteAwareReadPlan {
    scan: iceberg::scan::TableScan,
    pub tasks_by_path: HashMap<String, Vec<iceberg::scan::FileScanTask>>,
}

/// Build the delete-aware read plan for the current snapshot. Plan once, right
/// after loading the table, so the task set matches the snapshot whose files the
/// rewrite deletes.
pub async fn plan_delete_aware_read(table: &IcebergTable) -> sqe_core::Result<DeleteAwareReadPlan> {
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| SqeError::Execution(format!("Failed to build compaction read scan: {e}")))?;
    let tasks: Vec<iceberg::scan::FileScanTask> = scan
        .plan_files()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to plan compaction read: {e}")))?
        .try_collect()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to plan compaction read: {e}")))?;
    let mut tasks_by_path: HashMap<String, Vec<iceberg::scan::FileScanTask>> = HashMap::new();
    for task in tasks {
        tasks_by_path
            .entry(task.data_file_path.clone())
            .or_default()
            .push(task);
    }
    Ok(DeleteAwareReadPlan {
        scan,
        tasks_by_path,
    })
}

/// True when a manifest entry is a live delete file (position or equality).
///
/// A live entry is one whose status is not `Deleted`; a delete file is any
/// entry whose content type is not `Data`. The delete-aware rewrite uses this
/// to collect the delete files it must apply during the read
/// (`collect_live_delete_files`) and account for in the row cross-check.
pub fn is_live_delete_entry(entry: &iceberg::spec::ManifestEntry) -> bool {
    entry.status() != ManifestStatus::Deleted && entry.content_type() != DataContentType::Data
}

/// Collect the live delete files (position + equality) of the current snapshot.
/// Mirrors `collect_live_data_files` but keeps delete-content entries instead of
/// data entries. The delete-aware rewrite needs the delete `DataFile`s
/// themselves (not just a count) to compute the post-delete row cross-check and
/// to identify fully-covered position deletes for removal.
///
/// Shared by `sqe-coordinator`'s `CALL system.rewrite_data_files` and
/// `sqe-worker`'s `compact_file_group` action: both walk an `IcebergTable`'s
/// manifest list directly (no catalog call), so this is catalog-independent
/// and safe to depend on from a worker.
pub async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(vec![]);
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

    Ok(manifests
        .into_iter()
        .flat_map(|m| {
            m.entries()
                .iter()
                .filter(|e| is_live_delete_entry(e))
                .map(|e| e.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Rows expected after applying deletes to `group`, or `None` when it cannot be
/// computed exactly.
///
/// Only position-delete files whose `referenced_data_file` points at a file in
/// the group are attributable, so their `record_count` subtracts cleanly.
/// Equality deletes are value-based (they can match rows across many files at
/// any lower sequence number) and position deletes without a
/// `referenced_data_file` cannot be attributed to this group, so either makes
/// the exact count unknowable. Callers fall back to the looser
/// "cannot manufacture rows" bound in that case. Delete files are deduped by
/// path so a delete referenced from multiple manifest entries counts once.
pub fn expected_rows_after_deletes(group: &[DataFile], live_deletes: &[DataFile]) -> Option<u64> {
    let group_paths: HashSet<&str> = group.iter().map(|f| f.file_path()).collect();
    let base: u64 = group.iter().map(|f| f.record_count()).sum();
    let mut deleted: u64 = 0;
    let mut seen: HashSet<&str> = HashSet::new();
    for d in live_deletes {
        if !seen.insert(d.file_path()) {
            continue;
        }
        match d.content_type() {
            DataContentType::PositionDeletes => match d.referenced_data_file() {
                Some(ref_path) if group_paths.contains(ref_path.as_str()) => {
                    deleted += d.record_count();
                }
                // References a data file outside this group: not our concern.
                Some(_) => {}
                // Unattributable position delete: exact count unknowable.
                None => return None,
            },
            // Value-based deletes: exact count unknowable.
            DataContentType::EqualityDeletes => return None,
            DataContentType::Data => {}
        }
    }
    Some(base.saturating_sub(deleted))
}

/// Position delete files that are fully covered by the rewritten data set and so
/// can be dropped in the same commit: their `referenced_data_file` points at a
/// data file we are removing, so after the rewrite they reference a path that no
/// longer exists.
///
/// Only position deletes with a `referenced_data_file` are returned. Equality
/// deletes are value-based and may still match data files we are not touching
/// (or, via the sequence-number pin, higher-sequence data), so they are left in
/// place and aged out by `expire_snapshots`/`drop_delete_files_older_than`.
/// Position deletes without a `referenced_data_file` cannot be attributed
/// cheaply and are likewise left (harmless: they reference removed paths and
/// match nothing). Deduped by path.
pub fn covered_position_deletes(
    removed_data_paths: &HashSet<String>,
    live_deletes: &[DataFile],
) -> Vec<DataFile> {
    let mut seen: HashSet<&str> = HashSet::new();
    live_deletes
        .iter()
        .filter(|d| seen.insert(d.file_path()))
        .filter(|d| d.content_type() == DataContentType::PositionDeletes)
        .filter(|d| {
            d.referenced_data_file()
                .is_some_and(|p| removed_data_paths.contains(&p))
        })
        .cloned()
        .collect()
}

/// Stable grouping key for a data file's partition. Files that share a key
/// belong to the same partition of the same partition spec and can be safely
/// compacted together; files with different keys must never share an output
/// file. `Struct` is not `Hash`, so we key on its `Debug` form, which is
/// deterministic and sufficient as an in-memory grouping key (never persisted).
fn partition_key(f: &DataFile) -> String {
    format!("{}:{:?}", f.partition_spec_id(), f.partition())
}

/// Greedy bin-pack a list of data files into groups whose total size stays
/// under `target_bytes`. Files already at or above target are dropped: there
/// is no benefit to rewriting a file that is already large.
///
/// The algorithm sorts files descending by size so larger small-files anchor
/// each group and the remaining capacity is filled with the smallest files.
/// Simple, deterministic, and good enough for the maintenance use case.
pub fn pack_file_groups(
    files: &[DataFile],
    target_bytes: u64,
    force_include: &HashSet<String>,
) -> Vec<Vec<DataFile>> {
    // Filter files that are already at or above target: no point re-emitting,
    // unless a file is force-included (delete-heavy) and must be rewritten to
    // apply its accumulated deletes. A force-included file at/above target ends
    // up anchoring its own group (nothing else fits), so it is rewritten alone.
    let mut small: Vec<DataFile> = files
        .iter()
        .filter(|f| {
            f.file_size_in_bytes() < target_bytes || force_include.contains(f.file_path())
        })
        .cloned()
        .collect();
    // Descending by size.
    small.sort_by_key(|b| std::cmp::Reverse(b.file_size_in_bytes()));

    let mut groups: Vec<Vec<DataFile>> = Vec::new();
    for f in small {
        let size = f.file_size_in_bytes();
        // Try to fit into an existing group.
        let mut placed = false;
        for g in groups.iter_mut() {
            let current: u64 = g.iter().map(|x| x.file_size_in_bytes()).sum();
            if current + size <= target_bytes {
                g.push(f.clone());
                placed = true;
                break;
            }
        }
        if !placed {
            groups.push(vec![f]);
        }
    }
    groups
}

/// Bin-pack files without ever mixing partitions. Groups by `partition_key`
/// first, then applies the greedy `pack_file_groups` within each partition.
/// Every returned group contains files from exactly one partition.
///
/// Global bin-packing is not a correctness bug in SQE (the writer re-splits
/// rows per partition on write), but a cross-partition group fans back out to
/// roughly one output file per partition, paying full read+write I/O for near
/// zero consolidation. Grouping per partition is what makes compaction actually
/// reduce file counts on partitioned tables.
pub fn pack_file_groups_partition_aware(
    files: &[DataFile],
    target_bytes: u64,
    force_include: &HashSet<String>,
) -> Vec<Vec<DataFile>> {
    use std::collections::BTreeMap;
    let mut by_partition: BTreeMap<String, Vec<DataFile>> = BTreeMap::new();
    for f in files {
        by_partition
            .entry(partition_key(f))
            .or_default()
            .push(f.clone());
    }
    let mut out: Vec<Vec<DataFile>> = Vec::new();
    for (_key, part_files) in by_partition {
        out.extend(pack_file_groups(&part_files, target_bytes, force_include));
    }
    out
}

/// Data file paths carrying at least `threshold` distinct delete files, keyed
/// by a delete-aware read plan's per-file task list (`DeleteAwareReadPlan::
/// tasks_by_path`). The scan planner attaches every applicable delete file
/// (position and equality) to a data file's `FileScanTask`s, so distinct
/// `deletes[].data_file_path` across all of a file's tasks is the count of
/// delete files that must be read to serve that data file. Deduped per data
/// file so a file split into multiple tasks counts each delete once.
///
/// Takes the task map directly (rather than the `DeleteAwareReadPlan`
/// wrapper, which also carries a live `TableScan` that only a real table can
/// produce) so this stays pure and unit-testable with synthetic
/// `FileScanTask`s; `table_health::analyze_table_health` reuses it the same
/// way.
pub fn delete_heavy_files(
    tasks_by_path: &HashMap<String, Vec<iceberg::scan::FileScanTask>>,
    threshold: usize,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for (path, tasks) in tasks_by_path {
        let distinct: HashSet<&str> = tasks
            .iter()
            .flat_map(|t| t.deletes.iter().map(|d| d.data_file_path.as_str()))
            .collect();
        if distinct.len() >= threshold {
            out.insert(path.clone());
        }
    }
    out
}

/// Group every file of a partition into a single group, one group per
/// partition, ignoring file size. Used by the sort/z-order strategy: the whole
/// partition must be sorted as one stream so the rolling writer can cut it into
/// files with disjoint key ranges. Unlike `pack_file_groups`, this keeps files
/// already at or above the target size, because a large unsorted file still has
/// to be re-laid-out to participate in the sorted layout.
pub fn group_files_by_partition(files: &[DataFile]) -> Vec<Vec<DataFile>> {
    use std::collections::BTreeMap;
    let mut by_partition: BTreeMap<String, Vec<DataFile>> = BTreeMap::new();
    for f in files {
        by_partition
            .entry(partition_key(f))
            .or_default()
            .push(f.clone());
    }
    by_partition.into_values().collect()
}

/// How the rows in each rewritten group should be ordered before they are
/// written back. `Columns` is a plain lexicographic sort; `ZOrder` clusters on
/// a space-filling (Morton) curve for multi-dimensional locality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortSpec {
    /// (column, ascending) pairs, applied in order.
    Columns(Vec<(String, bool)>),
    /// Z-order clustering across these columns.
    ZOrder(Vec<String>),
}

impl SortSpec {
    pub fn columns(&self) -> Vec<&str> {
        match self {
            SortSpec::Columns(c) => c.iter().map(|(n, _)| n.as_str()).collect(),
            SortSpec::ZOrder(c) => c.iter().map(|n| n.as_str()).collect(),
        }
    }
}

/// Everything the sort-compaction path needs beyond the bin-pack path: the
/// shared spillable runtime and the resolved sort specification.
pub struct SortCtx {
    pub runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    pub spec: SortSpec,
}

/// A DataFusion `PartitionStream` that hands out a pre-built record-batch stream
/// exactly once. Lets us feed the delete-applying compaction read into a
/// `SortExec` (via a `StreamingTable`) so the sort spills to disk instead of
/// buffering the whole group in memory.
struct OneShotStream {
    schema: arrow_schema::SchemaRef,
    inner: std::sync::Mutex<Option<datafusion::execution::SendableRecordBatchStream>>,
}

impl std::fmt::Debug for OneShotStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneShotStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl datafusion::physical_plan::streaming::PartitionStream for OneShotStream {
    fn schema(&self) -> &arrow_schema::SchemaRef {
        &self.schema
    }
    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::execution::SendableRecordBatchStream {
        match self.inner.lock().expect("OneShotStream poisoned").take() {
            Some(s) => s,
            None => Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::once(async {
                    Err(datafusion::error::DataFusionError::Execution(
                        "compaction sort source polled more than once".into(),
                    ))
                }),
            )),
        }
    }
}

/// Sort a group's (delete-applied) record-batch stream on the shared spillable
/// runtime, returning the sorted stream. Builds a single-partition
/// `StreamingTable` over `input`, applies the sort (or z-order projection),
/// and returns `df.execute_stream()`. DataFusion inserts a spillable `SortExec`
/// because the session runs on the coordinator's FairSpillPool + DiskManager.
pub async fn sort_group_stream(
    ctx: &SortCtx,
    input: datafusion::execution::SendableRecordBatchStream,
    schema: arrow_schema::SchemaRef,
) -> sqe_core::Result<datafusion::execution::SendableRecordBatchStream> {
    use datafusion::prelude::{col, SessionConfig, SessionContext};

    let session = SessionContext::new_with_config_rt(SessionConfig::new(), ctx.runtime.clone());
    session.register_udf(crate::zorder::zorder_udf());

    let provider = datafusion::catalog::streaming::StreamingTable::try_new(
        schema.clone(),
        vec![Arc::new(OneShotStream {
            schema: schema.clone(),
            inner: std::sync::Mutex::new(Some(input)),
        })],
    )
    .map_err(|e| SqeError::Execution(format!("compaction sort: build source failed: {e}")))?;

    const SRC: &str = "__sqe_compact_src";
    session
        .register_table(SRC, Arc::new(provider))
        .map_err(|e| SqeError::Execution(format!("compaction sort: register source failed: {e}")))?;
    let df = session
        .table(SRC)
        .await
        .map_err(|e| SqeError::Execution(format!("compaction sort: read source failed: {e}")))?;

    let sorted = match &ctx.spec {
        SortSpec::Columns(cols) => {
            let exprs = cols
                .iter()
                .map(|(name, asc)| col(name).sort(*asc, !*asc))
                .collect::<Vec<_>>();
            df.sort(exprs)
                .map_err(|e| SqeError::Execution(format!("compaction sort failed: {e}")))?
        }
        SortSpec::ZOrder(cols) => {
            // Project a Morton z-value, sort on it, then drop it so the written
            // schema matches the table. Iceberg's SortOrder cannot express
            // z-order, so no sort-order metadata is stamped (matches Spark).
            let zargs = cols.iter().map(|c| col(c.as_str())).collect::<Vec<_>>();
            let zexpr = crate::zorder::zorder_udf().call(zargs).alias("__sqe_zvalue");
            let passthrough = schema
                .fields()
                .iter()
                .map(|f| col(f.name()))
                .collect::<Vec<_>>();
            let mut projected = passthrough.clone();
            projected.push(zexpr);
            df.select(projected)
                .and_then(|d| d.sort(vec![col("__sqe_zvalue").sort(true, false)]))
                .and_then(|d| d.select(passthrough))
                .map_err(|e| SqeError::Execution(format!("compaction z-order sort failed: {e}")))?
        }
    };

    sorted
        .execute_stream()
        .await
        .map_err(|e| SqeError::Execution(format!("compaction sort execution failed: {e}")))
}

/// Rewrite one file group, applying its position and equality deletes, and emit
/// the surviving rows as a fresh set of data files. Streams the delete-aware
/// scan straight into the streaming writer so the group is never fully buffered
/// in coordinator memory. Returns `(new_files, old_files, rows_written)`.
///
/// The read routes through the shared `DeleteAwareReadPlan`: every task for the
/// group's data files (with its delete files attached) is fed to
/// `read_tasks_to_arrow_with_metrics`, which applies the deletes during decode.
/// Before returning, the exact post-delete row count is cross-checked against
/// `expected_rows_after_deletes`; when that is knowable (position deletes with a
/// `referenced_data_file`) any mismatch aborts before commit, and when it is not
/// (equality deletes) the caller enforces the looser "cannot manufacture rows"
/// bound.
///
/// `progress`, when `Some`, is forwarded straight through to
/// `write_data_files_streaming` so the caller can observe incremental
/// rows-written during the rewrite (used by `sqe-worker`'s distributed
/// `compact_file_group` action to emit heartbeat frames). The coordinator's
/// own local `CALL system.rewrite_data_files` path passes `None` and is
/// unaffected.
#[allow(clippy::too_many_arguments)]
pub async fn rewrite_group(
    table: &IcebergTable,
    plan: &DeleteAwareReadPlan,
    live_deletes: &[DataFile],
    arrow_schema: &arrow_schema::SchemaRef,
    sort_ctx: Option<&SortCtx>,
    group: Vec<DataFile>,
    compression: parquet::basic::Compression,
    tracker: UploadedPaths,
    target_bytes: u64,
    progress: Option<crate::progress::ProgressReporter>,
) -> sqe_core::Result<(Vec<DataFile>, Vec<DataFile>, u64)> {
    use futures::StreamExt;

    // Gather every scan task for the group's data files. Each task carries the
    // delete files that apply to it; missing a file from the plan means we would
    // read it without its deletes and resurrect deleted rows, so fail loud.
    let mut tasks: Vec<iceberg::scan::FileScanTask> = Vec::new();
    for df in &group {
        match plan.tasks_by_path.get(df.file_path()) {
            Some(t) => tasks.extend(t.iter().cloned()),
            None => {
                return Err(SqeError::Execution(format!(
                    "delete-aware compaction: data file '{}' is missing from the scan plan; \
                     refusing to read it without its delete files (data-file path mismatch \
                     between the manifest and the scan planner)",
                    df.file_path()
                )));
            }
        }
    }

    if tasks.is_empty() {
        // Empty group: caller treats this as a no-op (nothing added/removed).
        return Ok((vec![], vec![], 0));
    }

    // Feed the group's tasks through the scan's delete-applying reader, then
    // adapt the Iceberg record-batch stream into a DataFusion stream for the
    // streaming writer. The declared schema is the table's current schema in
    // Arrow form; the writer re-stamps field IDs by position per batch.
    let task_stream: iceberg::scan::FileScanTaskStream =
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));
    let scan_result = plan
        .scan
        .read_tasks_to_arrow_with_metrics(task_stream)
        .map_err(|e| {
            SqeError::Execution(format!("delete-aware compaction read failed: {e}"))
        })?;
    let df_stream = scan_result.stream().map(|item| {
        item.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    });
    let sendable: datafusion::execution::SendableRecordBatchStream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            arrow_schema.clone(),
            df_stream,
        ),
    );

    // Sort strategy: route the delete-applied stream through a spillable
    // DataFusion sort (or z-order clustering) before writing. Bin-pack writes
    // the stream directly.
    let writer_input = match sort_ctx {
        Some(ctx) => sort_group_stream(ctx, sendable, arrow_schema.clone()).await?,
        None => sendable,
    };

    let (new_files, rows_written) = write_data_files_streaming(
        table,
        writer_input,
        "rewrite",
        compression,
        tracker,
        FanoutLimits::unbounded(),
        // Roll output at the requested target so a globally-sorted partition
        // stream is cut into multiple files with disjoint key ranges (the
        // property that makes sort compaction prunable at scale). Bin-pack
        // groups are already packed under target, so this is a no-op for them.
        Some(target_bytes),
        progress,
    )
    .await?;
    let rows_written = rows_written as u64;

    // Per-group delete-accounting cross-check. This is the guard that makes the
    // relaxed (added <= removed) invariant trustworthy: it catches a scan that
    // silently fails to apply deletes (rows too high) or a writer that drops
    // surviving rows (rows too low).
    match expected_rows_after_deletes(&group, live_deletes) {
        Some(expected) if rows_written != expected => {
            return Err(SqeError::Execution(format!(
                "compaction delete-accounting mismatch: wrote {rows_written} rows, expected \
                 {expected} after applying position deletes to a group of {} files; aborting \
                 before commit",
                group.len()
            )));
        }
        _ => {
            // Equality / unattributable deletes: exact count unknowable. Enforce
            // the looser bound that a rewrite can never manufacture rows.
            let base: u64 = group.iter().map(|f| f.record_count()).sum();
            if rows_written > base {
                return Err(SqeError::Execution(format!(
                    "compaction wrote {rows_written} rows from {base} input rows; deletes \
                     cannot increase the row count; aborting before commit"
                )));
            }
        }
    }

    if new_files.is_empty() {
        // Whole group deleted away: nothing to add, but we still remove the old
        // files. Return them so the caller drops them from the table.
        return Ok((vec![], group, rows_written));
    }

    Ok((new_files, group, rows_written))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Empty force-include set: the default for bin-pack tests that don't
    /// exercise delete_file_threshold.
    fn no_force() -> HashSet<String> {
        HashSet::new()
    }

    fn data_file_of_size(path: &str, size: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    /// Like `data_file_of_size` but lets the test vary the partition value and
    /// partition spec id, so partition-aware grouping can be exercised.
    fn data_file_part(path: &str, size: u64, spec_id: i32, part: i64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(part))]))
            .partition_spec_id(spec_id)
            .build()
            .expect("build data file")
    }

    #[test]
    fn partition_key_distinguishes_partitions_and_specs() {
        let a = data_file_part("a", 10, 0, 1);
        let b = data_file_part("b", 10, 0, 2);
        let c = data_file_part("c", 10, 1, 1);
        assert_eq!(partition_key(&a), partition_key(&a));
        assert_ne!(partition_key(&a), partition_key(&b), "different partition value");
        assert_ne!(partition_key(&a), partition_key(&c), "different spec id");
    }

    #[test]
    fn partition_aware_never_mixes_partitions() {
        let files = vec![
            data_file_part("p1-a", 10, 0, 1),
            data_file_part("p1-b", 10, 0, 1),
            data_file_part("p2-a", 10, 0, 2),
            data_file_part("p2-b", 10, 0, 2),
        ];
        let groups = pack_file_groups_partition_aware(&files, 1024, &no_force());
        for g in &groups {
            let keys: HashSet<String> = g.iter().map(partition_key).collect();
            assert_eq!(keys.len(), 1, "each group must be single-partition, got {keys:?}");
        }
        let total: usize = groups.iter().map(|g| g.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn group_files_by_partition_keeps_whole_partition_including_large_files() {
        // Sort strategy grouping: one group per partition, containing every
        // file regardless of size. A file at/above target (which bin-pack would
        // drop) must still be included so it can be re-laid-out into the sorted
        // layout.
        let target = 1024u64;
        let files = vec![
            data_file_part("p1-small", 10, 0, 1),
            data_file_part("p1-huge", target * 4, 0, 1), // bin-pack would drop this
            data_file_part("p2-a", 10, 0, 2),
            data_file_part("p2-b", 10, 0, 2),
        ];
        let groups = group_files_by_partition(&files);
        // Exactly one group per partition.
        assert_eq!(groups.len(), 2, "one group per partition, got {}", groups.len());
        // Every group is single-partition and no file was dropped.
        for g in &groups {
            let keys: HashSet<String> = g.iter().map(partition_key).collect();
            assert_eq!(keys.len(), 1, "each group must be single-partition, got {keys:?}");
        }
        let total: usize = groups.iter().map(|g| g.len()).sum();
        assert_eq!(total, 4, "no file may be dropped, including the large one");
        // The large file must be present (bin-pack would have excluded it).
        let has_huge = groups
            .iter()
            .flatten()
            .any(|f| f.file_path() == "p1-huge");
        assert!(has_huge, "sort grouping must keep files at/above target");
    }

    // ---------------------------------------------------------------------
    // Delete-accounting cross-check (expected_rows_after_deletes). Pure over
    // DataFile metadata, so no live catalog needed.
    // ---------------------------------------------------------------------

    fn data_file_rows(path: &str, rows: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Literal, Struct};
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    fn pos_delete_file(path: &str, rows: u64, referenced: &str) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .referenced_data_file(Some(referenced.to_string()))
            .build()
            .expect("build position delete file")
    }

    fn eq_delete_file(path: &str, rows: u64) -> DataFile {
        use iceberg::spec::{DataFileBuilder, DataFileFormat, Struct};
        DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(rows * 8)
            .record_count(rows)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .equality_ids(Some(vec![1]))
            .build()
            .expect("build equality delete file")
    }

    #[test]
    fn expected_rows_subtracts_referenced_position_deletes() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        assert_eq!(expected_rows_after_deletes(&[d], &[pd]), Some(90));
    }

    #[test]
    fn expected_rows_ignores_position_delete_outside_group() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd2.parquet", 5, "s3://b/other.parquet");
        assert_eq!(expected_rows_after_deletes(&[d], &[pd]), Some(100));
    }

    #[test]
    fn expected_rows_ambiguous_on_equality_delete() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let ed = eq_delete_file("s3://b/ed1.parquet", 5);
        assert_eq!(expected_rows_after_deletes(&[d], &[ed]), None);
    }

    #[test]
    fn expected_rows_dedupes_delete_by_path() {
        let d = data_file_rows("s3://b/d1.parquet", 100);
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        // Same delete file listed twice must only subtract once.
        assert_eq!(expected_rows_after_deletes(&[d], &[pd.clone(), pd]), Some(90));
    }

    #[test]
    fn covered_position_deletes_selects_only_referenced() {
        let mut removed = HashSet::new();
        removed.insert("s3://b/d1.parquet".to_string());
        let pd_in = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        let pd_out = pos_delete_file("s3://b/pd2.parquet", 5, "s3://b/d2.parquet");
        let ed = eq_delete_file("s3://b/ed1.parquet", 3);
        let got = covered_position_deletes(&removed, &[pd_in, pd_out, ed]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].file_path(), "s3://b/pd1.parquet");
    }

    #[test]
    fn covered_position_deletes_empty_when_none_referenced() {
        let removed = HashSet::new();
        let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
        assert!(covered_position_deletes(&removed, &[pd]).is_empty());
    }

    #[test]
    fn partition_aware_matches_global_when_single_partition() {
        let files = vec![
            data_file_part("a", 300, 0, 0),
            data_file_part("b", 300, 0, 0),
            data_file_part("c", 300, 0, 0),
        ];
        let pa = pack_file_groups_partition_aware(&files, 1024, &no_force());
        let global = pack_file_groups(&files, 1024, &no_force());
        let pa_sizes: usize = pa.iter().map(|g| g.len()).sum();
        let gl_sizes: usize = global.iter().map(|g| g.len()).sum();
        assert_eq!(pa_sizes, gl_sizes);
    }

    #[test]
    fn pack_empty_input_returns_empty() {
        let out = pack_file_groups(&[], 1024, &no_force());
        assert!(out.is_empty());
    }

    #[test]
    fn pack_files_at_or_above_target_are_skipped() {
        let target = 1024;
        let files = vec![
            data_file_of_size("a", target),     // equal to target
            data_file_of_size("b", target + 1), // above target
        ];
        let out = pack_file_groups(&files, target, &no_force());
        assert!(
            out.is_empty(),
            "files at or above target must not be packed"
        );
    }

    #[test]
    fn pack_force_include_keeps_file_at_or_above_target() {
        // A delete-heavy file at/above target is normally dropped, but
        // force_include keeps it so its deletes can be applied. It anchors its
        // own group (nothing else fits above target).
        let target = 1024;
        let big = data_file_of_size("delete-heavy-big", target * 2);
        let small = data_file_of_size("small", 100);
        let files = vec![big, small];
        let mut force = HashSet::new();
        force.insert("delete-heavy-big".to_string());

        let out = pack_file_groups(&files, target, &force);
        let all: Vec<&str> = out.iter().flatten().map(|f| f.file_path()).collect();
        assert!(
            all.contains(&"delete-heavy-big"),
            "force-included file at/above target must be kept, got {all:?}"
        );
        // Without force_include the same big file is dropped.
        let out_plain = pack_file_groups(&files, target, &no_force());
        let plain: Vec<&str> = out_plain.iter().flatten().map(|f| f.file_path()).collect();
        assert!(
            !plain.contains(&"delete-heavy-big"),
            "without force_include the large file must be dropped, got {plain:?}"
        );
    }

    #[test]
    fn pack_small_files_group_under_target() {
        let target = 1000;
        let files: Vec<_> = (0..10)
            .map(|i| data_file_of_size(&format!("f{i}"), 100))
            .collect();
        let out = pack_file_groups(&files, target, &no_force());
        // 10 * 100 == 1000 == target; exactly one group at the boundary.
        assert_eq!(out.len(), 1, "expected one packed group, got {}", out.len());
        assert_eq!(out[0].len(), 10);
        let sum: u64 = out[0].iter().map(|f| f.file_size_in_bytes()).sum();
        assert_eq!(sum, 1000);
    }

    #[test]
    fn pack_respects_target_boundary() {
        let target = 1000;
        let files: Vec<_> = (0..11)
            .map(|i| data_file_of_size(&format!("f{i}"), 100))
            .collect();
        let out = pack_file_groups(&files, target, &no_force());
        // Greedy descending-first packing: first 10 fill the first group
        // (sum=1000, fits because current+size<=target). The 11th starts a
        // fresh group.
        assert_eq!(out.len(), 2);
        let total_packed: usize = out.iter().map(|g| g.len()).sum();
        assert_eq!(total_packed, 11);
    }

    #[test]
    fn pack_mixed_sizes_sorted_descending() {
        let target = 1000;
        let files = vec![
            data_file_of_size("small", 50),
            data_file_of_size("big", 800),
            data_file_of_size("medium", 300),
        ];
        let out = pack_file_groups(&files, target, &no_force());
        // Descending order: 800 first in group. Then 300: 800+300>1000, new
        // group. Then 50: 800+50<=1000, placed in first.
        assert_eq!(out.len(), 2);
        // Group 0: 800 + 50 = 850
        // Group 1: 300
        let sizes: Vec<u64> = out
            .iter()
            .map(|g| g.iter().map(|f| f.file_size_in_bytes()).sum())
            .collect();
        assert!(sizes.contains(&850) && sizes.contains(&300));
    }
}

# Scheduling Evolution — From File-Count to Cost-Aware Distribution

**Date:** 2026-04-01
**Status:** Draft
**Scope:** Evolve SQE's fragment scheduler from naive file-count distribution to cost-aware, locality-aware, adaptive scheduling across three phases.

## Motivation

SQE's current scheduler (`WeightedScheduler`) assigns Iceberg scan fragments to workers based on **file count** — each file costs 1 unit. This has three problems:

1. **Wrong cost metric**: A 10MB file and a 10GB file both cost 1. The scheduler can't balance work by actual data volume.
2. **Distribute everything**: Even a query touching one tiny file gets distributed to a worker, paying serialization + network + coordination overhead that exceeds the scan time.
3. **No file grouping**: 100 small files become 100 separate tasks when they should be grouped into a few appropriately-sized chunks.

These are well-understood problems in distributed query scheduling. Trino, Spark, Impala, and CockroachDB all solve them differently, but the core principles are shared.

## Research Foundation

This design draws on:

| Paper/System | Key Insight Applied |
|---|---|
| **Presto: SQL on Everything** (ICDE 2019) | Lazy split streaming, phased vs all-at-once stage scheduling |
| **Spark AQE** (Databricks 2020) | Runtime re-optimization, partition coalescing, skew detection |
| **Impala** (CIDR 2015) | Short-query optimization — skip distribution for small queries |
| **Morsel-Driven Parallelism** (Leis et al., SIGMOD 2014) | Don't bake parallelism into the plan; adjust at runtime |
| **Sparrow** (Ousterhout et al., SOSP 2013) | Power-of-two-choices for decentralized scheduling |
| **Iceberg spec** | Manifest metadata provides file sizes, row counts, partition ranges |

## Current State

### `ScanTask` (`crates/sqe-planner/src/scan_task.rs`)
```rust
pub struct ScanTask {
    pub fragment_id: String,
    pub data_file_paths: Vec<String>,   // just paths, no file sizes
    pub projected_columns: Vec<String>,
    // S3 credentials...
}
```

### `estimate_cost()` (`crates/sqe-coordinator/src/scheduler.rs`)
```rust
fn estimate_cost(task: &ScanTask) -> u64 {
    task.data_file_paths.len().max(1) as u64   // file count, not size
}
```

### `split_files()` (`crates/sqe-planner/src/splitter.rs`)
```rust
// Round-robin: file[i] goes to worker[i % num_workers]
groups[i % num_workers].push(file);
```

### Distribution decision (`crates/sqe-coordinator/src/query_handler.rs`)
```rust
// Distributes if: workers available AND scan has files AND file_count >= worker_count
```

---

## Phase A: Cost-Aware Scheduling (Quick Wins)

**Goal:** Use actual file sizes for cost estimation, skip distribution for small queries, group small files.

### A.1: File-Size Cost Estimation

**Problem:** `estimate_cost()` counts files. A task with 2 files of 5GB each is "cheaper" than a task with 10 files of 1MB each.

**Solution:** Add `file_sizes_bytes: Vec<u64>` to `ScanTask`. Populate from Iceberg manifest metadata (which already contains `file_size_in_bytes` per data file). Change cost estimation:

```rust
pub struct ScanTask {
    pub fragment_id: String,
    pub data_file_paths: Vec<String>,
    pub file_sizes_bytes: Vec<u64>,       // NEW: bytes per file
    pub projected_columns: Vec<String>,
    // ...
}

fn estimate_cost(task: &ScanTask) -> u64 {
    let total_bytes: u64 = task.file_sizes_bytes.iter().sum();
    // Cost in megabytes, minimum 1
    (total_bytes / (1024 * 1024)).max(1)
}
```

**Where file sizes come from:** When `IcebergScanExec::data_file_paths()` is called in `distributed_scan.rs`, it already iterates `FileScanTask` objects. Each `FileScanTask` has `data_file().file_size_in_bytes()` from the Iceberg manifest. Extract both path and size:

```rust
// In distributed_scan.rs, where data_file_paths are collected:
let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
let file_info: Vec<(String, u64)> = tasks.iter()
    .map(|t| (t.data_file_path().to_string(), t.data_file().file_size_in_bytes()))
    .collect();
```

**Files to change:**
- `crates/sqe-planner/src/scan_task.rs` — add `file_sizes_bytes` field
- `crates/sqe-coordinator/src/scheduler.rs` — change `estimate_cost()`
- `crates/sqe-coordinator/src/distributed_scan.rs` — populate file sizes from manifest
- `crates/sqe-catalog/src/iceberg_scan.rs` — expose file sizes from `data_file_paths()`

### A.2: Coordinator-Only Threshold

**Problem:** A query scanning one 5MB Parquet file gets serialized to JSON, sent to a worker via gRPC, executed there, and streamed back. The overhead exceeds the scan time.

**Solution:** Add a `distribution_threshold_bytes` config. Queries with total scan size below this threshold execute locally on the coordinator.

```toml
[query]
# Minimum total scan size to distribute. Below this, execute on coordinator.
# Default: 128MB. Set to 0 to always distribute.
distribution_threshold_bytes = "128MB"
```

**Decision point:** In `try_distribute()` in `query_handler.rs`, after collecting file sizes but before scheduling:

```rust
let total_scan_bytes: u64 = file_sizes.iter().sum();
let threshold = parse_memory_limit(&self.config.query.distribution_threshold_bytes)
    .unwrap_or(128 * 1024 * 1024);

if threshold > 0 && total_scan_bytes < threshold as u64 {
    debug!(
        total_scan_bytes,
        threshold,
        "Scan below distribution threshold — executing locally"
    );
    return plan;  // skip distribution, execute on coordinator
}
```

This mirrors Impala's `EXEC_SINGLE_NODE_ROWS_THRESHOLD` but uses bytes instead of rows (more predictable for Parquet).

**Files to change:**
- `crates/sqe-core/src/config.rs` — add `distribution_threshold_bytes` to `QueryConfig`
- `crates/sqe-coordinator/src/query_handler.rs` — add threshold check in `try_distribute()`

### A.3: File Bin-Packing

**Problem:** `split_files()` uses round-robin. 100 small files across 3 workers = 33/33/34 tasks. But if each file is 1MB, that's 33 tasks of 1MB each — extreme overhead.

**Solution:** Replace round-robin with bin-packing that targets a configurable task size.

```toml
[query]
# Target size per scan task for distributed execution.
# Small files are grouped together until this threshold.
# Default: 256MB.
target_task_size_bytes = "256MB"
```

Algorithm:
1. Sort files by size descending (largest first).
2. For each file, add it to the smallest current bin.
3. If the smallest bin exceeds `target_task_size`, start a new bin.
4. Limit total bins to `num_workers * 2` (avoid too many small tasks).

```rust
pub fn bin_pack_files(
    files: Vec<(String, u64)>,  // (path, size_bytes)
    target_size: u64,
    max_bins: usize,
) -> Vec<Vec<(String, u64)>> {
    if files.is_empty() { return vec![]; }

    let mut bins: Vec<(u64, Vec<(String, u64)>)> = vec![];  // (total_size, files)

    // Sort largest first
    let mut sorted = files;
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    for (path, size) in sorted {
        // Find the smallest bin that won't exceed target_size, or smallest overall
        let best = bins.iter_mut()
            .enumerate()
            .filter(|(_, (total, _))| *total + size <= target_size)
            .min_by_key(|(_, (total, _))| *total);

        if let Some((_, (total, files))) = best {
            *total += size;
            files.push((path, size));
        } else if bins.len() < max_bins {
            bins.push((size, vec![(path, size)]));
        } else {
            // All bins full — add to smallest
            let min = bins.iter_mut().min_by_key(|(total, _)| *total).unwrap();
            min.0 += size;
            min.1.push((path, size));
        }
    }

    bins.into_iter().map(|(_, files)| files).collect()
}
```

**Files to change:**
- `crates/sqe-planner/src/splitter.rs` — add `bin_pack_files()`, keep `split_files()` for backward compat
- `crates/sqe-core/src/config.rs` — add `target_task_size_bytes`
- `crates/sqe-coordinator/src/distributed_scan.rs` — use bin-packing instead of round-robin

---

## Phase B: Adaptive Scheduling (Medium-Term)

### B.1: Stage-Aware Execution

**Current limitation:** SQE only distributes leaf scans. All post-scan work (filters, aggregations, joins) runs on the coordinator. For a `SELECT COUNT(*) FROM big_table`, the coordinator receives millions of rows just to count them.

**Solution:** Push partial aggregation to workers. The worker applies the filter + partial aggregate, returns only the partial result. The coordinator performs final aggregation.

This requires:
1. Identifying which plan operators can be pushed to workers (filters, projections, partial aggregations).
2. Serializing DataFusion physical plan fragments (not just scan tasks) to workers.
3. Workers executing the full fragment, not just the scan.

**Complexity:** High. Requires serializing `ExecutionPlan` nodes to protobuf (Ballista's `PhysicalPlanNode`). Defer to Phase B.

### B.2: Partition Coalescing

After scan tasks are assigned, if any worker has many tiny tasks, merge them. Similar to Spark AQE's `coalescePartitions`.

**Trigger:** If a worker's assigned tasks total less than `target_task_size / 4`, merge them into a single task.

### B.3: Worker-Side Caching

Workers that repeatedly scan the same Parquet files (e.g., dashboard queries) should cache file footer metadata. Use consistent hashing to route scans for the same file to the same worker:

```rust
fn preferred_worker(file_path: &str, workers: &[WorkerInfo]) -> &WorkerInfo {
    let hash = seahash::hash(file_path.as_bytes());
    let idx = hash as usize % workers.len();
    &workers[idx]
}
```

The scheduler uses this as a soft preference (not mandatory) — if the preferred worker is overloaded, fall back to least-loaded.

### B.4: Straggler Detection

Log a warning when a fragment takes > 3x the median completion time:

```rust
// After all fragments complete:
let durations: Vec<u64> = fragments.iter().map(|f| f.elapsed_ms).collect();
let median = durations[durations.len() / 2];
for f in &fragments {
    if f.elapsed_ms > median * 3 {
        warn!(
            fragment_id = %f.id,
            elapsed_ms = f.elapsed_ms,
            median_ms = median,
            worker = %f.worker_url,
            "Straggler detected"
        );
    }
}
```

---

## Phase C: Advanced Scheduling (Longer-Term)

### C.1: Adaptive Re-Optimization (Spark AQE Style)

After shuffle stages materialize, collect runtime statistics and re-optimize:
- Coalesce small post-shuffle partitions
- Convert sort-merge join to broadcast join when one side is small
- Split skewed partitions

### C.2: Push-Down of Operators to Workers

Send full plan fragments (not just scans) to workers. Requires DataFusion physical plan serialization.

### C.3: Multi-Tenant Resource Groups

Trino-style resource groups with weighted fair scheduling:
```toml
[[resource_groups]]
name = "interactive"
max_running = 20
max_queued = 50
scheduling_weight = 10

[[resource_groups]]
name = "batch"
max_running = 5
max_queued = 100
scheduling_weight = 1
```

### C.4: Speculative Execution

Re-launch stragglers on another worker after detecting them (Phase B.4 prerequisite).

---

## What NOT to Build

| Feature | Why Not |
|---|---|
| ML-based cost models | Overkill — Iceberg manifest metadata + heuristics is sufficient |
| Decentralized scheduling (Sparrow) | Only needed at millions of tasks/second |
| Work-stealing | For intra-node threads (morsel-driven), not inter-node tasks |
| Geo-distributed scheduling | Not relevant until SQE spans multiple regions |

---

## Anti-Patterns Addressed

| Anti-Pattern | How We Fix It | Phase |
|---|---|---|
| Count-based cost | File-size cost from Iceberg manifests | A.1 |
| Distribute everything | Coordinator-only threshold (128MB default) | A.2 |
| No bin-packing | First-fit-decreasing bin-packing to target size | A.3 |
| Fixed parallelism | Bin count adapts to data size, not worker count | A.3 |
| Shuffle to coordinator | Push partial aggregation to workers | B.1 |
| Ignore stragglers | Log warning at 3x median, future speculative exec | B.4 / C.4 |
| Static plan | Runtime re-optimization after shuffle | C.1 |

---

## Config Summary

```toml
[query]
# Existing
timeout_secs = 300
max_result_rows = 1000000
max_concurrent_queries = 100
max_query_memory = "256MB"

# Phase A — new
distribution_threshold_bytes = "128MB"   # below this, execute on coordinator
target_task_size_bytes = "256MB"         # bin-pack small files to this size
```

---

## File Plan (Phase A)

| File | Action |
|---|---|
| `crates/sqe-planner/src/scan_task.rs` | Add `file_sizes_bytes: Vec<u64>` field |
| `crates/sqe-planner/src/splitter.rs` | Add `bin_pack_files()` function |
| `crates/sqe-coordinator/src/scheduler.rs` | Update `estimate_cost()` to use bytes |
| `crates/sqe-coordinator/src/distributed_scan.rs` | Extract file sizes from manifest, use bin-packing |
| `crates/sqe-catalog/src/iceberg_scan.rs` | Expose `data_file_info()` returning (path, size) pairs |
| `crates/sqe-core/src/config.rs` | Add `distribution_threshold_bytes`, `target_task_size_bytes` |
| `crates/sqe-coordinator/src/query_handler.rs` | Add threshold check in `try_distribute()` |

---

## Success Criteria

### Phase A
- Queries touching < 128MB total scan size execute 2-5x faster (no distribution overhead)
- Tables with many small files (100+ files < 10MB each) produce 3-5 tasks instead of 100
- Load balancing improves: workers get equal bytes, not equal file count
- No regression on existing integration tests

### Phase B
- `SELECT COUNT(*)` on a 1GB table returns in < 1s (vs. current: coordinator-bound)
- Straggler detection logs appear in production traces
- Cache-friendly routing shows measurable reduction in S3 GetObject calls

### Phase C
- Multi-tenant workloads achieve fair sharing within 10% of ideal
- Skewed joins don't cause OOM (partition splitting handles it)

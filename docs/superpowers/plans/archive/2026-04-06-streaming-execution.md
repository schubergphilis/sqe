# Streaming Execution Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform SQE into a streaming execution engine capable of processing 1TB+ datasets regardless of available RAM. Phase A: spill-to-disk + late materialization (safe). Phase B: distributed computation via DoExchange shuffle (fast).

**Architecture:** Phase A adds memory management and I/O optimizations within the existing coordinator-centric model. Phase B moves computation to workers via Arrow Flight DoExchange, making the coordinator a thin planner.

**Tech Stack:** Rust, DataFusion 52, iceberg-rust 0.9 (RisingWave fork), arrow-rs, arrow-flight 57, moka, tokio

**Spec:** `docs/superpowers/specs/2026-04-06-streaming-execution-design.md`

**Dependency constraint:** Stay on DataFusion 52 / arrow-rs 57 / iceberg-rust RisingWave fork (`1978911ec4`). DF 53 upgrade deferred until fork rebases — see spec for details.

---

## Prerequisites (before Phase A)

### Prereq 1: Finish Pluggable Auth (3 remaining tasks)

The pluggable auth plan (`docs/superpowers/plans/2026-03-30-device-auth-and-trino-sso.md`) has 3 open tasks:

- [ ] **13.6** Modify `submit_query` → 401 with `WWW-Authenticate: Bearer x_redirect_server, x_token_server`
- [ ] **14.2** Add `[auth.external]` section to `sqe.toml.example`
- [ ] **14.3** Construct services in coordinator startup from config

These clear the "← NEXT" marker in nextsteps.md and complete Step 4.

### Prereq 2: Verify DF 52 Capabilities

Confirm all Phase A features work on current deps before starting implementation.

- [ ] **Step 1: Verify FairSpillPool API**

Confirm `FairSpillPool::new()`, `MemoryPool::reserved()`, and `MemoryPool::memory_limit()` are available in DF 52. Write a minimal test:

```rust
let pool = Arc::new(FairSpillPool::new(1024 * 1024 * 1024)); // 1GB
assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1_073_741_824)));
```

- [ ] **Step 2: Verify SortMergeJoinExec availability**

Confirm `SortMergeJoinExec` compiles and can be instantiated in DF 52. It's marked experimental but functional:

```rust
use datafusion::physical_plan::joins::SortMergeJoinExec;
// Verify it exists and can be used in a plan
```

- [ ] **Step 3: Test pushdown_filters with Iceberg scan**

Enable `pushdown_filters=true` and `reorder_filters=true` on a SessionContext, run a selective query against an Iceberg table, and verify:
- Correct results
- Reduced execution time vs. without pushdown
- No panics or regressions

```rust
let config = SessionConfig::new()
    .set_bool("datafusion.execution.parquet.pushdown_filters", true)
    .set_bool("datafusion.execution.parquet.reorder_filters", true);
```

If pushdown_filters works through iceberg-datafusion, Tasks 4-6 (late materialization) reduce to enabling config + adding metrics rather than building custom RowFilter plumbing.

- [ ] **Step 4: Verify iceberg-rust manifest stats access**

Confirm `DataFile` exposes `lower_bounds()`, `upper_bounds()`, `null_value_counts()` in the RisingWave fork at `1978911ec4`.

- [ ] **Step 5: Verify iceberg-rust sort order access**

Confirm `table.metadata().current_sort_order()` returns sort order fields.

- [ ] **Step 6: Commit verification results**

Document which features work as-is and which need custom implementation. Update Tasks 4-6 scope based on pushdown_filters findings.

```bash
git commit -m "chore: verify DF 52 capabilities for streaming execution phase

FairSpillPool, SortMergeJoinExec, pushdown_filters, iceberg manifest
stats, and sort order all verified on current dependency set."
```

### Prereq 3: Monitor RisingWave Fork for DF 53 Rebase

Not a blocking task — just a watch item. Check periodically:
```bash
git ls-remote https://github.com/risingwavelabs/iceberg-rust.git | grep dev_rebase
```
When a new rev appears that includes DF 53 (arrow 58, sqlparser 0.61), plan the upgrade as a dedicated task before Phase B.

---

## Parallel Workstream Map

```
Prerequisites (sequential, ~1 day)
  Prereq 1: Finish pluggable auth         (3 tasks)
  Prereq 2: Verify DF 52 capabilities     (6 steps)

Phase A — Safe Execution (5 parallel streams)
  Stream 1: Coordinator spill-to-disk     (Tasks 1-3)   ← Agent 1
  Stream 2: Late materialization           (Tasks 4-6)   ← Agent 2
  Stream 3: Iceberg scan planning          (Tasks 7-10)  ← Agent 3
  Stream 4: S3 I/O pipeline               (Tasks 11-13) ← Agent 4
  Stream 5: SortMergeJoin fallback         (Tasks 14-15) ← Agent 5

Phase B — Fast Execution (5 parallel streams, starts after Phase A)
  Stream 6: DoExchange shuffle infra       (Tasks 16-19) ← Agent 1
  Stream 7: Distributed sort + aggregation (Tasks 20-23) ← Agent 2 (needs Stream 6)
  Stream 8: Distributed joins              (Tasks 24-27) ← Agent 3 (needs Stream 6)
  Stream 9: Multi-endpoint Flight SQL      (Task 28)     ← Agent 4
  Stream 10: Observability                 (Task 29)     ← Agent 5

Deferred: DF 53 upgrade (when RisingWave fork rebases)
```

---

## File Map

| File | Action | Stream | Responsibility |
|---|---|---|---|
| `crates/sqe-core/src/config.rs` | Modify | 1 | Add coordinator memory/spill config, S3 I/O config |
| `sqe.toml.example` | Modify | 1 | Add new config sections |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | 1 | Wire memory pool on SessionContext |
| `crates/sqe-coordinator/src/memory.rs` | Create | 1 | Memory watermark monitor |
| `crates/sqe-planner/src/join_strategy.rs` | Create | 5 | SortMergeJoin fallback optimizer rule |
| `crates/sqe-catalog/src/iceberg_scan.rs` | Modify | 2, 3 | Late materialization, file pruning, sort detection |
| `crates/sqe-catalog/src/late_materialize.rs` | Create | 2 | RowFilter builder from predicates |
| `crates/sqe-catalog/src/footer_cache.rs` | Create | 4 | LRU Parquet footer cache |
| `crates/sqe-catalog/src/s3_io.rs` | Create | 4 | Byte-range coalescing, prefetch |
| `crates/sqe-worker/src/flight_service.rs` | Modify | 6 | Add DoExchange handler |
| `crates/sqe-worker/src/shuffle.rs` | Create | 6 | Hash/range partitioner, shuffle writer/reader |
| `crates/sqe-planner/src/distributed_sort.rs` | Create | 7 | Range-partition sort planning |
| `crates/sqe-planner/src/distributed_join.rs` | Create | 8 | Broadcast/shuffle join planning |
| `crates/sqe-planner/src/predicate_transfer.rs` | Create | 8 | Join predicate pushdown |
| `crates/sqe-coordinator/src/flight_sql.rs` | Modify | 9 | Multi-endpoint FlightInfo |
| `crates/sqe-metrics/src/lib.rs` | Modify | 10 | Spill + shuffle metrics |

---

# Phase A: Safe Execution

## Stream 1: Coordinator Spill-to-Disk

### Task 1: Add coordinator memory and spill config

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Modify: `sqe.toml.example`

- [ ] **Step 1: Add memory/spill fields to CoordinatorConfig**

Add after the existing `worker_secret` field in `CoordinatorConfig`:

```rust
/// Memory limit for the coordinator's DataFusion runtime.
/// Accepts human-readable sizes: "8GB", "80%", "4096MB".
/// Default: "8GB". Applies to all query operator memory (sorts, joins, aggregates).
#[serde(default = "default_coordinator_memory")]
pub memory_limit: String,
/// Enable spill-to-disk when memory limit is reached. Default: true.
#[serde(default = "default_true")]
pub spill_to_disk: bool,
/// Directory for spill files. Must be on fast local storage (SSD recommended).
/// Default: "/tmp/sqe-coordinator-spill".
#[serde(default = "default_coordinator_spill_dir")]
pub spill_dir: String,
/// Compression for spill files. "none", "lz4" (default), or "zstd".
#[serde(default = "default_spill_compression")]
pub spill_compression: String,
```

Add the default functions:
```rust
fn default_coordinator_memory() -> String { "8GB".to_string() }
fn default_coordinator_spill_dir() -> String { "/tmp/sqe-coordinator-spill".to_string() }
fn default_spill_compression() -> String { "lz4".to_string() }
```

- [ ] **Step 2: Update sqe.toml.example**

Add under `[coordinator]`:
```toml
# Memory limit for coordinator query processing (sorts, joins, aggregates).
# Supports: B, KB, MB, GB, TB, or percentage of physical RAM (e.g. "80%").
memory_limit = "8GB"
# Spill to disk when memory limit is reached. Recommended: true for production.
spill_to_disk = true
# Directory for spill files. Use fast local SSD. Will be created if it doesn't exist.
spill_dir = "/tmp/sqe-coordinator-spill"
# Compression for spill files: "none", "lz4" (fast, recommended), "zstd" (smaller)
spill_compression = "lz4"
```

- [ ] **Step 3: Verify config parsing**

Run: `cargo test --all 2>&1 | grep -E "test result"`

Ensure existing config tests still pass and the new defaults don't break deserialization of existing `sqe.toml` files (all new fields have defaults).

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-core/src/config.rs sqe.toml.example
git commit -m "feat: add coordinator memory_limit and spill_to_disk config

Add memory_limit, spill_to_disk, spill_dir, and spill_compression to
[coordinator] config section. Defaults: 8GB limit, spill enabled,
LZ4 compression. Mirrors existing [worker] memory config pattern."
```

---

### Task 2: Wire FairSpillPool on coordinator SessionContext

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Extract coordinator runtime builder**

Create a helper function `build_coordinator_runtime()` in `query_handler.rs` (or a new `crates/sqe-coordinator/src/runtime.rs` module) that mirrors `sqe-worker/src/runtime.rs::build_session_context()`:

```rust
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

/// Build a DataFusion RuntimeEnv for the coordinator with memory limits
/// and spill-to-disk support.
fn build_coordinator_runtime(config: &CoordinatorConfig) -> anyhow::Result<Arc<RuntimeEnv>> {
    let memory_bytes = sqe_core::parse_memory_limit(&config.memory_limit)
        .map_err(|e| anyhow::anyhow!("Invalid coordinator memory_limit '{}': {e}", config.memory_limit))?;

    info!(
        memory_limit = %config.memory_limit,
        memory_bytes = memory_bytes,
        spill_to_disk = config.spill_to_disk,
        spill_dir = %config.spill_dir,
        spill_compression = %config.spill_compression,
        "Configuring coordinator DataFusion runtime"
    );

    let memory_pool = Arc::new(FairSpillPool::new(memory_bytes));
    let mut builder = RuntimeEnvBuilder::new().with_memory_pool(memory_pool);

    if config.spill_to_disk {
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        let disk_builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    Ok(Arc::new(builder.build()?))
}
```

- [ ] **Step 2: Wire into create_session_context()**

Find the `create_session_context()` method in `QueryHandler`. Currently it creates a `SessionContext::new()` or similar without memory limits. Change it to use the runtime from Step 1.

The runtime should be built once (in `QueryHandler::new()`) and reused across all queries, so the memory pool is shared and enforced globally.

Add a `runtime: Arc<RuntimeEnv>` field to `QueryHandler`, initialized in `new()`:

```rust
let runtime = build_coordinator_runtime(&config.coordinator)
    .expect("Failed to build coordinator runtime");
```

In `create_session_context()`:
```rust
let session_config = SessionConfig::new();
let ctx = SessionContext::new_with_config_rt(session_config, Arc::clone(&self.runtime));
```

- [ ] **Step 3: Ensure spill directory exists**

At coordinator startup (before building runtime), create the spill directory if it doesn't exist:

```rust
if config.coordinator.spill_to_disk {
    std::fs::create_dir_all(&config.coordinator.spill_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create spill directory '{}': {e}", config.coordinator.spill_dir))?;
}
```

- [ ] **Step 4: Add unit tests**

Test that:
1. Coordinator runtime respects memory_limit
2. Spill directory is created
3. Disabled spill prevents disk manager
4. Invalid memory_limit errors gracefully

- [ ] **Step 5: Build + test**

Run: `cargo build --all && cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: wire FairSpillPool and spill-to-disk on coordinator runtime

Coordinator DataFusion SessionContext now uses FairSpillPool with
configurable memory_limit and spill_dir. Sorts, hash aggregates, and
other spillable operators will spill to disk instead of OOM.
Runtime is built once and shared across all queries."
```

---

### Task 3: Memory watermarks and admission control

**Files:**
- Create: `crates/sqe-coordinator/src/memory.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Create memory monitor**

Create `crates/sqe-coordinator/src/memory.rs`:

```rust
use std::sync::Arc;
use datafusion::execution::memory_pool::MemoryPool;
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    Green,  // < 70%
    Yellow, // 70-85%
    Orange, // 85-95%
    Red,    // > 95%
}

impl MemoryPressure {
    pub fn from_usage(used: usize, limit: usize) -> Self {
        if limit == 0 { return Self::Green; }
        let pct = (used as f64 / limit as f64) * 100.0;
        match pct as u64 {
            0..=69 => Self::Green,
            70..=84 => Self::Yellow,
            85..=94 => Self::Orange,
            _ => Self::Red,
        }
    }

    pub fn admits_new_query(&self) -> bool {
        !matches!(self, Self::Red)
    }
}

/// Check current memory pressure level from the DataFusion MemoryPool.
pub fn check_pressure(pool: &Arc<dyn MemoryPool>) -> MemoryPressure {
    // MemoryPool exposes used() and limit() via the MemoryLimit trait
    // Implementation depends on DataFusion version — check FairSpillPool API
    // Fallback: always Green if pool doesn't expose usage
    // FairSpillPool doesn't expose used() directly — read from metrics
    // DataFusion tracks memory via MemoryPool::reserved(). Check API:
    // pool.reserved() returns current bytes in use.
    let used = pool.reserved();
    let limit = match pool.memory_limit() {
        datafusion::execution::memory_pool::MemoryLimit::Finite(n) => n,
        _ => return MemoryPressure::Green,
    };
    MemoryPressure::from_usage(used, limit)
}
```

- [ ] **Step 2: Wire admission control into execute()**

In `QueryHandler::execute()`, before acquiring the semaphore, check memory pressure:

```rust
let pressure = memory::check_pressure(&self.memory_pool);
if !pressure.admits_new_query() {
    return Err(SqeError::Execution(
        "Server under memory pressure. Please retry later.".to_string(),
    ));
}
```

- [ ] **Step 3: Expose pressure as Prometheus gauge**

Add `sqe_memory_pressure` gauge (0=green, 1=yellow, 2=orange, 3=red) and `sqe_memory_used_bytes` / `sqe_memory_limit_bytes` gauges to `sqe-metrics`.

- [ ] **Step 4: Tests + commit**

Run: `cargo build --all && cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

```bash
git commit -m "feat: add coordinator memory watermarks and admission control

Monitor coordinator memory pool usage. Reject new queries at >95%
utilization (Red). Expose sqe_memory_pressure and sqe_memory_used_bytes
Prometheus gauges."
```

---

## Stream 2: Late Materialization

> **Prereq 2 finding (2026-04-06):** `pushdown_filters=true` does NOT work for Iceberg scans. SQE's custom `IcebergScanExec` calls `scan.to_arrow()` directly, bypassing DataFusion's `ParquetExec` (which is where `pushdown_filters` takes effect). The worker's `executor.rs` uses `ParquetRecordBatchStreamBuilder` but doesn't set `RowFilter`. The full custom RowFilter implementation below is required.

### Task 4: Classify predicate vs projection columns

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`
- Create: `crates/sqe-catalog/src/late_materialize.rs`

- [ ] **Step 1: Create column classifier**

Create `crates/sqe-catalog/src/late_materialize.rs`:

Given a DataFusion `PhysicalExpr` (the filter predicate) and the full projection column list, classify each column as:
- **Predicate column**: referenced in the filter expression
- **Projection-only column**: in the SELECT but not in the filter

```rust
use std::collections::HashSet;
use datafusion::physical_expr::PhysicalExpr;
use arrow_schema::SchemaRef;

pub struct ColumnClassification {
    /// Columns needed for predicate evaluation (Phase 1)
    pub predicate_columns: Vec<String>,
    /// Columns needed only for output (Phase 2, read only for surviving rows)
    pub projection_only_columns: Vec<String>,
}

/// Walk the PhysicalExpr tree and collect all column references.
pub fn classify_columns(
    predicate: &dyn PhysicalExpr,
    projection: &[String],
    schema: &SchemaRef,
) -> ColumnClassification {
    let predicate_cols: HashSet<String> = collect_column_refs(predicate);
    let projection_only: Vec<String> = projection.iter()
        .filter(|col| !predicate_cols.contains(*col))
        .cloned()
        .collect();
    ColumnClassification {
        predicate_columns: predicate_cols.into_iter().collect(),
        projection_only_columns: projection_only,
    }
}
```

- [ ] **Step 2: Unit tests for column classification**

Test cases:
- `WHERE a > 10` with projection `[a, b, c]` → predicate: `[a]`, projection-only: `[b, c]`
- `WHERE a > 10 AND b = 'x'` with projection `[a, b, c]` → predicate: `[a, b]`, projection-only: `[c]`
- `WHERE a > 10` with projection `[a]` → predicate: `[a]`, projection-only: `[]` (no late mat benefit)
- No predicate → all columns are projection-only (no Phase 1)

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: add column classifier for late materialization

Classify projected columns as predicate or projection-only based on
WHERE clause references. Foundation for two-phase Parquet scan."
```

---

### Task 5: Implement RowFilter-based two-phase scan

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`
- Modify: `crates/sqe-catalog/src/late_materialize.rs`

- [ ] **Step 1: Build RowFilter from predicate expressions**

In `late_materialize.rs`, add a function that converts a DataFusion `PhysicalExpr` predicate into an arrow-rs `RowFilter`:

```rust
use parquet::arrow::arrow_reader::{RowFilter, ArrowPredicate, ArrowPredicateFn};

pub fn build_row_filter(
    predicate: Arc<dyn PhysicalExpr>,
    predicate_schema: SchemaRef, // schema containing only predicate columns
) -> RowFilter {
    let arrow_predicate = ArrowPredicateFn::new(
        predicate_schema.into(),
        move |batch: RecordBatch| {
            // Evaluate the predicate on the batch of predicate columns
            let result = predicate.evaluate(&batch)?;
            Ok(result.into_array(batch.num_rows())?)
        },
    );
    RowFilter::new(vec![Box::new(arrow_predicate)])
}
```

- [ ] **Step 2: Wire RowFilter into ParquetRecordBatchReaderBuilder**

In `IcebergScanExec::execute()` (or wherever the Parquet reader is constructed), when late materialization is beneficial (predicate columns < total columns):

```rust
let builder = ParquetRecordBatchReaderBuilder::try_new(file_reader)?
    .with_projection(projection_mask)
    .with_row_filter(row_filter)  // NEW: enables two-phase read
    .with_batch_size(batch_size);
```

The `RowFilter` causes arrow-rs to:
1. Read only predicate columns first
2. Evaluate the filter
3. Read remaining columns only for passing rows

- [ ] **Step 3: Handle edge cases**

- No predicate → skip late materialization, read all columns in one pass
- All projected columns are predicate columns → no benefit, skip
- Predicate on partition column → already pruned at manifest level, don't re-evaluate
- Complex predicates (OR, BETWEEN, LIKE) → still works, RowFilter evaluates any ArrowPredicate

- [ ] **Step 4: Add metrics**

Track in query-level metrics:
- `bytes_read_predicate_only`: bytes fetched for Phase 1
- `bytes_read_projection`: bytes fetched for Phase 2
- `late_materialization_selectivity`: rows surviving filter / total rows

- [ ] **Step 5: Integration test**

Create a test with a wide table (20+ columns), selective predicate (5% selectivity), verify:
- Correct results (same as without late mat)
- Reduced bytes read (from S3 or local file metrics)

- [ ] **Step 6: Commit**

```bash
git commit -m "feat: implement late materialization via arrow-rs RowFilter

Two-phase Parquet scan: read predicate columns first, evaluate filter,
then read projection columns only for surviving rows. Reduces S3 reads
by 10-50x for selective queries on wide tables."
```

---

### Task 6: CachedArrayReader for shared columns

**Files:**
- Modify: `crates/sqe-catalog/src/late_materialize.rs`

- [ ] **Step 1: Handle predicate columns that are also in the projection**

When a column appears in both the predicate and the projection (e.g., `SELECT user_id, name FROM t WHERE user_id = 42`), the `RowFilter` already reads `user_id` in Phase 1. Phase 2 should reuse the already-decoded `user_id` array rather than re-reading it from Parquet.

Check if arrow-rs `RowFilter` handles this automatically (it should — the `ArrowPredicate` trait has a `projection()` method that tells the reader which columns the predicate needs, and the reader caches them). If it does, document it and add a test confirming no double-read. If not, implement caching manually.

- [ ] **Step 2: Test + commit**

```bash
git commit -m "feat: verify CachedArrayReader avoids double-read for shared columns

Confirm that arrow-rs RowFilter reuses predicate column arrays for
projection when the same column appears in both WHERE and SELECT."
```

---

## Stream 3: Iceberg Scan Planning

### Task 7: File-level min/max pruning

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Read per-file statistics from manifest entries**

When listing data files from the Iceberg manifest, each `ManifestEntry` contains:
- `lower_bounds`: per-column minimum values
- `upper_bounds`: per-column maximum values
- `null_value_counts`: per-column null counts

Extract these during scan planning (they're already fetched — just not used for pruning).

- [ ] **Step 2: Evaluate query predicates against file statistics**

For each data file, check if the predicate can be satisfied:
- `col > 100` → skip file if `upper_bound(col) <= 100`
- `col = 'foo'` → skip file if `lower_bound(col) > 'foo'` or `upper_bound(col) < 'foo'`
- `col IN (1, 2, 3)` → skip file if `upper_bound(col) < 1` or `lower_bound(col) > 3`
- `col IS NULL` → skip file if `null_count(col) = 0`
- `col IS NOT NULL` → skip file if `null_count(col) = total_rows`

Use DataFusion's `PruningPredicate` which already implements this logic against `PruningStatistics`. Implement `PruningStatistics` for Iceberg manifest entry statistics.

- [ ] **Step 3: Add metrics**

Track `files_pruned_minmax` in query metrics.

- [ ] **Step 4: Test with TPC-H**

Run TPC-H SF1 queries and verify file pruning occurs for queries with range predicates (Q1, Q3, Q6 all have date-range predicates).

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add file-level min/max pruning from Iceberg manifest statistics

Evaluate query predicates against per-file min/max bounds from manifest
entries. Skip files that cannot satisfy the predicate. Uses DataFusion
PruningPredicate infrastructure."
```

---

### Task 8: Sort-order detection and streaming merge

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Read sort order from Iceberg table metadata**

iceberg-rust's `Table` exposes `sort_order()` which returns the table's sort order. Read this during scan planning.

```rust
let sort_order = table.metadata().current_sort_order();
```

- [ ] **Step 2: Compare table sort order against query ORDER BY**

Implement `check_sort_compatibility()`:

```rust
enum SortStrategy {
    NoSort,           // No ORDER BY in query
    StreamingMerge,   // Table sort fully satisfies query ORDER BY
    PartialMerge { prefix_len: usize }, // Partial match
    TopK { limit: usize },             // ORDER BY + small LIMIT
    FullSort,         // No match, full sort required
}
```

- [ ] **Step 3: Implement streaming merge when sort-compatible**

When `SortStrategy::StreamingMerge`:
- Order FileTask assignments by file `lower_bound` (from manifests)
- DataFusion's `SortPreservingMergeExec` can merge pre-sorted streams
- Tell DataFusion that each scan partition is pre-sorted via `EquivalenceProperties` on the `IcebergScanExec` output

This eliminates sorting entirely for queries whose ORDER BY matches the table's sort order.

- [ ] **Step 4: Test**

Create an Iceberg table sorted by `id`, insert pre-sorted data, verify that `SELECT * FROM t ORDER BY id` produces a streaming merge plan (no `SortExec` in `EXPLAIN`).

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: detect Iceberg sort order and use streaming merge

Read table sort order from Iceberg metadata. When query ORDER BY matches,
emit pre-sorted EquivalenceProperties so DataFusion uses
SortPreservingMergeExec instead of full sort. Zero spill for pre-sorted
ORDER BY queries."
```

---

### Task 9: TopK pushdown for ORDER BY ... LIMIT N

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Detect ORDER BY ... LIMIT pattern**

DataFusion already has TopK optimization in `SortExec` when `fetch` is set. Verify it's enabled on the coordinator. If not, ensure `SortExec::new().with_fetch(Some(limit))` is used when the logical plan contains `Sort { fetch: Some(n) }`.

- [ ] **Step 2: Verify TopK works with distributed scan**

When `DistributedScanExec` is used, each worker should return at most `limit` rows (if possible), and the coordinator's `SortExec` with `fetch` does the final merge-TopK.

Currently workers don't know about LIMIT — they return all rows from their files. For Phase A, this is acceptable (workers scan fully, coordinator does TopK). Phase B will push LIMIT to workers.

- [ ] **Step 3: Test + commit**

Verify `EXPLAIN SELECT * FROM t ORDER BY id LIMIT 10` shows `SortExec: fetch=10` (TopK mode).

```bash
git commit -m "feat: verify TopK pushdown for ORDER BY ... LIMIT N

Confirm DataFusion's SortExec uses heap-based TopK when LIMIT is set.
O(N) memory regardless of table size."
```

---

### Task 10: Bloom filter and page-level pruning

**Files:**
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Enable page-level pruning via PageIndex**

When building the `ParquetRecordBatchReaderBuilder`, enable page index filtering:

```rust
let builder = ParquetRecordBatchReaderBuilder::try_new(file_reader)?
    .with_page_index(true)  // Enable page-level min/max pruning
    .with_row_filter(row_filter)
    .with_projection(projection_mask);
```

This tells arrow-rs to read the Parquet PageIndex (column index + offset index) and skip pages whose min/max don't satisfy the predicate. Only works for Parquet files written with PageIndex (Parquet v2+, which Iceberg writes by default).

- [ ] **Step 2: Enable bloom filter pruning**

When the predicate contains equality checks (`col = value`), check if the Parquet file has a bloom filter for that column:

```rust
let builder = builder.with_bloom_filter(true);  // if available in arrow-rs API
```

Note: Check arrow-rs API for bloom filter support on `ParquetRecordBatchReaderBuilder`. If not directly available, read bloom filter metadata manually and build a `RowSelection` that skips non-matching row groups.

- [ ] **Step 3: Add metrics**

Track `files_pruned_bloom` and `pages_pruned_index` in query metrics.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: enable page-level and bloom filter pruning in Parquet reader

Enable PageIndex for page-level min/max pruning and bloom filter checks
for equality predicates. Reduces bytes read from S3 within already-selected files."
```

---

## Stream 4: S3 I/O Pipeline

### Task 11: Byte-range coalescing

**Files:**
- Create: `crates/sqe-catalog/src/s3_io.rs`

- [ ] **Step 1: Implement range coalescing logic**

When late materialization selects specific column chunks, the byte ranges may be close together. Coalescing merges ranges that are within `coalesce_threshold` bytes of each other into a single S3 GET request:

```rust
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

/// Merge adjacent byte ranges if the gap between them is <= threshold.
pub fn coalesce_ranges(ranges: &mut Vec<ByteRange>, threshold: u64) -> Vec<ByteRange> {
    ranges.sort_by_key(|r| r.offset);
    let mut result = Vec::new();
    for range in ranges {
        if let Some(last) = result.last_mut() {
            let gap = range.offset.saturating_sub(last.offset + last.length);
            if gap <= threshold {
                // Extend the last range to cover this one
                let end = range.offset + range.length;
                last.length = end - last.offset;
                continue;
            }
        }
        result.push(range.clone());
    }
    result
}
```

- [ ] **Step 2: Add config**

Add `coalesce_threshold` to a new `[s3]` or `[storage]` config section (default: "1MB").

- [ ] **Step 3: Unit tests + commit**

```bash
git commit -m "feat: add S3 byte-range coalescing for Parquet column reads

Merge adjacent byte ranges within configurable threshold to reduce S3
request count. Default threshold: 1MB."
```

---

### Task 12: Parquet footer cache

**Files:**
- Create: `crates/sqe-catalog/src/footer_cache.rs`
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Create LRU footer cache**

Use `moka` (already a dependency) for an async TTL cache:

```rust
use moka::future::Cache;
use parquet::file::metadata::ParquetMetaData;

pub struct FooterCache {
    cache: Cache<String, Arc<ParquetMetaData>>, // key = S3 URI
}

impl FooterCache {
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(max_size_bytes)
                .weigher(|_key: &String, value: &Arc<ParquetMetaData>| {
                    // Estimate metadata size
                    // Each column chunk metadata is ~200-500 bytes
                    // Rough: 1KB per column per row group
                    // Estimate: ~500 bytes per column per row group in metadata
                    // A 50-column table with 100 row groups ≈ 2.5MB of metadata
                    let num_row_groups = value.num_row_groups() as u32;
                    let num_columns = value.file_metadata().schema().get_fields().len() as u32;
                    (num_row_groups * num_columns * 500).max(1024)
                })
                .build(),
        }
    }

    pub async fn get_or_fetch<F, Fut>(&self, path: &str, fetch: F) -> Result<Arc<ParquetMetaData>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<ParquetMetaData>>,
    {
        self.cache.try_get_with(path.to_string(), async {
            fetch().await.map(Arc::new)
        }).await
    }
}
```

- [ ] **Step 2: Wire into IcebergScanExec**

Pass a shared `Arc<FooterCache>` to `IcebergScanExec`. Before fetching footer from S3, check cache.

- [ ] **Step 3: Add config and metrics**

Config: `footer_cache_size = "256MB"` in `[storage]` section.
Metrics: `sqe_footer_cache_hits`, `sqe_footer_cache_misses`, `sqe_footer_cache_size_bytes`.

- [ ] **Step 4: Test + commit**

```bash
git commit -m "feat: add LRU Parquet footer cache

Cache parsed Parquet metadata (footers) across queries using moka.
Eliminates repeated S3 reads for footers of frequently queried tables.
Default: 256MB cache."
```

---

### Task 13: Parallel byte-range reads and prefetch

**Files:**
- Modify: `crates/sqe-catalog/src/s3_io.rs`
- Modify: `crates/sqe-catalog/src/iceberg_scan.rs`

- [ ] **Step 1: Implement parallel column chunk fetching**

Within a single file, fetch multiple column chunks concurrently using `tokio::join!` or `FuturesUnordered`:

```rust
pub async fn fetch_column_chunks(
    store: &dyn ObjectStore,
    path: &Path,
    ranges: &[ByteRange],
    max_concurrent: usize,
) -> Result<Vec<Bytes>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let futures: Vec<_> = ranges.iter().map(|range| {
        let sem = Arc::clone(&semaphore);
        async move {
            let _permit = sem.acquire().await?;
            store.get_range(path, range.offset..range.offset + range.length).await
        }
    }).collect();
    futures::future::try_join_all(futures).await
}
```

- [ ] **Step 2: Implement prefetch overlap**

Start fetching the footer of the next file while the current file is being decoded:

```rust
// Pseudocode for prefetch pipeline
let mut prefetch: Option<JoinHandle<Footer>> = None;
for file in files {
    let footer = if let Some(prev) = prefetch.take() {
        prev.await?
    } else {
        fetch_footer(file).await?
    };
    // Start prefetching next file's footer
    if let Some(next) = files.peek() {
        prefetch = Some(tokio::spawn(fetch_footer(next.clone())));
    }
    // Process current file using cached footer
    process_file(file, footer).await?;
}
```

- [ ] **Step 3: Add config**

```toml
[storage]
concurrent_requests_per_file = 4
max_concurrent_files = 8
prefetch_buffer = "32MB"
```

- [ ] **Step 4: Benchmark + commit**

Run TPC-H SF10 and measure S3 request count and wall-clock time before/after.

```bash
git commit -m "feat: parallel S3 byte-range reads and file prefetch

Fetch multiple column chunks concurrently within each file (default: 4).
Prefetch next file's footer during current file decode. Configurable
via [storage] section."
```

---

## Stream 5: SortMergeJoin Fallback

### Task 14: Implement JoinStrategyRule

**Files:**
- Create: `crates/sqe-planner/src/join_strategy.rs`

- [ ] **Step 1: Create PhysicalOptimizerRule**

```rust
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};

pub struct JoinStrategyRule {
    /// Maximum build-side size (bytes) for hash join.
    /// Above this, rewrite to SortMergeJoin.
    hash_join_threshold: usize,
}

impl PhysicalOptimizerRule for JoinStrategyRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            if let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() {
                let build_side_size = estimate_build_side_size(hash_join);
                if build_side_size > self.hash_join_threshold {
                    // Rewrite to SortMergeJoin
                    let smj = convert_to_sort_merge_join(hash_join)?;
                    return Ok(Transformed::yes(smj));
                }
            }
            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str { "JoinStrategyRule" }
    fn schema_check(&self) -> bool { true }
}
```

- [ ] **Step 2: Estimate build-side size from Iceberg statistics**

Use the input plan's statistics (DataFusion `Statistics` trait) or fall back to scanning the Iceberg manifest total file size for the build-side table.

- [ ] **Step 3: Convert HashJoinExec → SortMergeJoinExec**

Preserve: join type, join condition, build/probe sides.
Add: `SortExec` on both sides if not already sorted on join keys.

- [ ] **Step 4: Register rule on coordinator SessionContext**

In `build_coordinator_runtime()` or `create_session_context()`:

```rust
let config = SessionConfig::new()
    .with_physical_optimizer_rule(Arc::new(JoinStrategyRule {
        hash_join_threshold: parse_memory_limit(&config.hash_join_memory_threshold)?,
    }));
```

- [ ] **Step 5: Test + commit**

Test: join two tables where build side exceeds threshold → EXPLAIN shows SortMergeJoinExec, not HashJoinExec.

```bash
git commit -m "feat: add JoinStrategyRule to fall back to SortMergeJoin for large joins

When hash join build side exceeds configurable threshold (default: 25%
of coordinator memory_limit), rewrite to SortMergeJoin which spills
gracefully via external sort. Prevents OOM on large joins."
```

---

### Task 15: Add hash_join_memory_threshold config

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Modify: `sqe.toml.example`

- [ ] **Step 1: Add config field**

Add to `QueryConfig`:
```rust
/// Maximum estimated build-side size for hash join before falling back to
/// SortMergeJoin. Default: "2GB". Set to "0" to always use hash join.
#[serde(default = "default_hash_join_threshold")]
pub hash_join_memory_threshold: String,
```

Default: `"2GB"`.

- [ ] **Step 2: Update sqe.toml.example + commit**

```bash
git commit -m "feat: add hash_join_memory_threshold config

Configure when hash join falls back to sort-merge join. Default: 2GB.
Above this, JoinStrategyRule rewrites to SortMergeJoin."
```

---

# Phase B: Fast Execution

## Stream 6: DoExchange Shuffle Infrastructure

### Task 16: Flight DoExchange service on workers

**Files:**
- Modify: `crates/sqe-worker/src/flight_service.rs`
- Create: `crates/sqe-worker/src/shuffle.rs`

- [ ] **Step 1: Implement DoExchange handler on worker Flight service**

Add `do_exchange()` to the worker's `FlightService` implementation:

```rust
async fn do_exchange(
    &self,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<FlightDataStream>, Status> {
    // 1. Read descriptor from first message — contains partition assignment
    // 2. Receive incoming RecordBatch stream (from another executor)
    // 3. Buffer or process the batches
    // 4. Return result stream
}
```

The descriptor encodes the exchange type:
```rust
#[derive(Serialize, Deserialize)]
enum ExchangeDescriptor {
    /// Receive hash-partitioned data for a join/aggregate
    HashPartition { query_id: String, stage_id: String, partition_id: u32 },
    /// Receive range-partitioned data for a distributed sort
    RangePartition { query_id: String, stage_id: String, range_bounds: Vec<ScalarValue> },
}
```

- [ ] **Step 2: Implement partition buffer**

Incoming batches need to be buffered on the receiving executor until the consuming operator reads them. Use a bounded `tokio::sync::mpsc` channel per partition:

```rust
pub struct ShuffleReceiver {
    /// Per-partition receive channels
    channels: HashMap<u32, mpsc::Receiver<RecordBatch>>,
}
```

Memory accounting: register the buffer with the executor's `MemoryPool`. If buffer exceeds reservation, spill oldest batches to disk (Arrow IPC).

- [ ] **Step 3: Test DoExchange round-trip**

Test: executor A sends 10 RecordBatches to executor B via DoExchange, B receives all 10 in order with correct data.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: implement Flight DoExchange handler on workers

Workers can now receive Arrow RecordBatch streams from other workers
via Flight DoExchange. Foundation for shuffle-based distributed sort,
join, and aggregation."
```

---

### Task 17: Hash partitioner

**Files:**
- Modify: `crates/sqe-worker/src/shuffle.rs`

- [ ] **Step 1: Implement hash partitioner**

Given a RecordBatch and partition key columns, compute the target partition for each row:

```rust
pub struct HashPartitioner {
    key_columns: Vec<String>,
    num_partitions: usize,
}

impl HashPartitioner {
    /// Partition a RecordBatch by hashing the key columns.
    /// Returns a Vec of (partition_id, RecordBatch) pairs.
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<(u32, RecordBatch)>> {
        // 1. Extract key column arrays
        // 2. Compute hash per row (use DataFusion's create_hashes)
        // 3. hash % num_partitions → partition assignment
        // 4. Split batch by partition (use take() with partition indices)
    }
}
```

Use DataFusion's `datafusion::common::hash_utils::create_hashes()` for consistent hashing.

- [ ] **Step 2: Implement range partitioner**

Given sort-key boundaries (from sampling), assign each row to a range:

```rust
pub struct RangePartitioner {
    boundaries: Vec<ScalarValue>, // P-1 boundaries for P partitions
    key_column: String,
}

impl RangePartitioner {
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<(u32, RecordBatch)>> {
        // Binary search each row's key value against boundaries
    }
}
```

- [ ] **Step 3: Unit tests + commit**

```bash
git commit -m "feat: add hash and range partitioners for shuffle

Hash partitioner for distributed joins/aggregations.
Range partitioner for distributed sorts."
```

---

### Task 18: ShuffleWriterExec and ShuffleReaderExec

**Files:**
- Create: `crates/sqe-planner/src/shuffle_exec.rs`

- [ ] **Step 1: Implement ShuffleWriterExec**

DataFusion `ExecutionPlan` that partitions its input and sends batches to remote executors:

```rust
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioner: Arc<dyn Partitioner>,
    target_endpoints: Vec<String>, // Flight endpoints of target executors
    properties: PlanProperties,
}

impl ExecutionPlan for ShuffleWriterExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // 1. Execute input plan
        // 2. For each batch: partition → send to target executor via Flight DoExchange
        // 3. Return empty stream (data was sent, not returned locally)
    }
}
```

- [ ] **Step 2: Implement ShuffleReaderExec**

DataFusion `ExecutionPlan` that reads batches from the DoExchange buffer:

```rust
pub struct ShuffleReaderExec {
    schema: SchemaRef,
    shuffle_receiver: Arc<ShuffleReceiver>,
    partition_id: u32,
    properties: PlanProperties,
}

impl ExecutionPlan for ShuffleReaderExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // Read from the shuffle receiver channel for this partition
    }
}
```

- [ ] **Step 3: Test full shuffle pipeline**

Integration test: 2 executors, hash-partition a table on `id`, verify each executor receives only rows where `hash(id) % 2` matches their partition.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: add ShuffleWriterExec and ShuffleReaderExec plan nodes

DataFusion ExecutionPlan nodes for distributed shuffle. ShuffleWriter
partitions and sends via Flight DoExchange. ShuffleReader receives from
the shuffle buffer. Foundation for distributed sort/join/aggregate."
```

---

### Task 19: Stage decomposition in coordinator

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Create: `crates/sqe-planner/src/stage_planner.rs`

- [ ] **Step 1: Implement stage decomposition**

A distributed query is broken into stages separated by shuffle boundaries:

```rust
pub struct QueryStage {
    pub stage_id: String,
    pub plan_fragment: Arc<dyn ExecutionPlan>, // plan to execute on each executor
    pub input_stages: Vec<String>,             // stages that must complete first
    pub shuffle_type: Option<ShuffleType>,     // Hash or Range partition for output
    pub assigned_executors: Vec<String>,        // which executors run this stage
}

pub enum ShuffleType {
    Hash { key_columns: Vec<String>, num_partitions: usize },
    Range { key_column: String, boundaries: Vec<ScalarValue> },
    Broadcast,
}
```

Walk the physical plan tree. At each shuffle boundary (join, sort, aggregate that requires redistribution), split into a new stage.

- [ ] **Step 2: Plan execution order**

Topological sort of stages by dependency. Execute stages in waves:
- Wave 1: Leaf stages (scans)
- Wave 2: Stages that consume Wave 1 output (joins, aggregates)
- Wave 3: Final result stage

- [ ] **Step 3: Wire into try_distribute()**

Currently `try_distribute()` only handles single-scan distribution. Extend it to support multi-stage plans when shuffle infrastructure is available.

- [ ] **Step 4: Test + commit**

```bash
git commit -m "feat: add stage decomposition for multi-stage distributed queries

Break distributed queries into stages at shuffle boundaries. Each stage
runs on assigned executors. Stages execute in topological order (scan →
shuffle → join/agg → result)."
```

---

## Stream 7: Distributed Sort + Aggregation

### Task 20: Range boundary sampling

**Files:**
- Create: `crates/sqe-planner/src/distributed_sort.rs`

- [ ] **Step 1: Implement manifest-based boundary estimation**

Read per-file min/max for the sort column from Iceberg manifests. Use these to compute approximate quantile boundaries:

```rust
pub async fn compute_range_boundaries(
    file_stats: &[(String, ScalarValue, ScalarValue)], // (path, min, max)
    num_partitions: usize,
) -> Result<Vec<ScalarValue>> {
    // 1. Collect all min/max values
    // 2. Sort them
    // 3. Pick P-1 evenly spaced values as boundaries
    // 4. This gives approximate quantiles without reading any data
}
```

- [ ] **Step 2: Implement sample-based refinement**

When file-level statistics are too coarse (e.g., only 3 files but need 8 partitions), request executors to sample:

```rust
pub async fn sample_from_executors(
    executors: &[String],
    table: &str,
    sort_column: &str,
    sample_size: usize, // per executor
) -> Result<Vec<ScalarValue>> {
    // Send reservoir sampling request to each executor
    // Collect samples, compute global quantiles
}
```

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add range boundary sampling for distributed sort

Compute sort partition boundaries from Iceberg manifest statistics.
Fall back to executor sampling when file-level stats are insufficient."
```

---

### Task 21: DistributedSortExec plan node

**Files:**
- Modify: `crates/sqe-planner/src/distributed_sort.rs`

- [ ] **Step 1: Implement DistributedSortExec**

Replaces `SortExec` in the physical plan when a distributed sort is beneficial:

```rust
pub struct DistributedSortExec {
    /// The scan + filter plan to execute on each executor
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    sort_exprs: Vec<PhysicalSortExpr>,
    /// Range boundaries for partitioning
    boundaries: Vec<ScalarValue>,
    /// Target executors
    executors: Vec<String>,
    /// Optional LIMIT
    fetch: Option<usize>,
    properties: PlanProperties,
}
```

The plan expands to:
1. Each executor runs `input` (scan + filter)
2. Each executor range-partitions output on sort key using `boundaries`
3. Each executor sends partitioned data to owning executor via DoExchange
4. Each executor locally sorts its range partition
5. Result is globally sorted (ranges are disjoint)

- [ ] **Step 2: Wire into physical optimizer**

Add a rule that replaces `SortExec` with `DistributedSortExec` when:
- Distributed mode is available
- Input data size exceeds threshold
- Enough executors are healthy

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add DistributedSortExec for range-partition sort

Distributed sort: sample boundaries, range-partition via DoExchange,
local sort per executor. Ranges are disjoint so concatenation = global
sorted output. Coordinator memory: ~0."
```

---

### Task 22: Two-phase distributed aggregation

**Files:**
- Create: `crates/sqe-planner/src/distributed_aggregate.rs`

- [ ] **Step 1: Implement partial aggregation push-down**

For GROUP BY queries, split aggregation into two phases:

Phase 1 (on each executor):
```sql
-- Partial: SUM → partial_sum, COUNT → partial_count
SELECT key, SUM(val) as partial_sum, COUNT(*) as partial_count
FROM scan_partition GROUP BY key
```

Phase 2 (on coordinator or single executor):
```sql
-- Final: merge partials
SELECT key, SUM(partial_sum), SUM(partial_count)
FROM phase1_results GROUP BY key
```

- [ ] **Step 2: Handle high-cardinality GROUP BY**

When the number of distinct groups is high (estimated from Iceberg column stats `distinct_count`), use hash-partition shuffle instead of collecting all partials on coordinator:
- Hash-partition on GROUP BY keys via DoExchange
- Each executor does full aggregation on its hash partition
- No coordinator bottleneck

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add two-phase distributed aggregation

Partial aggregation on executors, final merge on coordinator or via
hash-partition shuffle for high-cardinality groups."
```

---

### Task 23: Predicate ordering optimization

**Files:**
- Modify: `crates/sqe-catalog/src/late_materialize.rs`

- [ ] **Step 1: Order predicates by evaluation cost**

When multiple predicates exist, evaluate cheapest/most selective first:
1. Predicates on partition columns (free — manifest level)
2. Predicates with bloom filter support
3. Predicates on sort-order columns (best zone map pruning)
4. Remaining predicates by estimated selectivity (from column stats)

This maximizes the filtering effect of early predicate columns in late materialization.

- [ ] **Step 2: Test + commit**

```bash
git commit -m "feat: order predicate evaluation by selectivity for late materialization

Most selective/cheapest predicates evaluated first to maximize early row
elimination and minimize S3 reads."
```

---

## Stream 8: Distributed Joins

### Task 24: Broadcast join

**Files:**
- Create: `crates/sqe-planner/src/distributed_join.rs`

- [ ] **Step 1: Implement broadcast join planning**

When one join side is small (< `broadcast_threshold`, default 64MB estimated from Iceberg manifest file sizes):

```rust
pub struct BroadcastJoinPlan {
    /// Small side: collected on coordinator, broadcast to all executors
    small_side: Arc<dyn ExecutionPlan>,
    /// Large side: scanned on executors, joined locally with broadcast data
    large_side: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    join_condition: JoinFilter,
}
```

Execution:
1. Coordinator collects small side into memory
2. Broadcasts as Flight `DoPut` to all executors
3. Executors probe large side against broadcast hash table during scan
4. No shuffle of large side — just scan + probe

- [ ] **Step 2: Add broadcast_threshold config**

Add to `QueryConfig`:
```rust
pub broadcast_threshold: String, // default: "64MB"
```

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add broadcast join for small/large table joins

When build side < broadcast_threshold (default 64MB), broadcast to all
executors. Large side scanned locally with join probe. No shuffle of
large side."
```

---

### Task 25: Shuffle hash join

**Files:**
- Modify: `crates/sqe-planner/src/distributed_join.rs`

- [ ] **Step 1: Implement shuffle hash join planning**

When both sides are large:
1. Hash-partition both sides on join keys via DoExchange
2. Each executor builds hash table for its partition of the build side
3. Each executor probes its partition of the probe side

```rust
pub struct ShuffleHashJoinPlan {
    build_side: Arc<dyn ExecutionPlan>,
    probe_side: Arc<dyn ExecutionPlan>,
    join_keys: Vec<(Column, Column)>,
    join_type: JoinType,
    num_partitions: usize, // = number of executors
}
```

Memory per executor: `build_side_total / num_executors` (or SortMergeJoin fallback if that exceeds threshold).

- [ ] **Step 2: Test + commit**

```bash
git commit -m "feat: add shuffle hash join for large/large table joins

Both sides hash-partitioned on join keys via DoExchange. Each executor
builds/probes its partition. Memory per executor: O(build_side / N)."
```

---

### Task 26: Sort-merge join for pre-sorted tables

**Files:**
- Modify: `crates/sqe-planner/src/distributed_join.rs`

- [ ] **Step 1: Detect when both sides are sorted on join key**

If both Iceberg tables have sort orders matching the join keys, use streaming merge join:
- No hash table (O(batch_size) memory)
- No shuffle needed
- Just merge the two sorted streams

Wire this as a plan-time optimization: if `check_sort_compatibility()` returns `StreamingMerge` for both sides on the join key columns, use `SortMergeJoinExec` directly.

- [ ] **Step 2: Test + commit**

```bash
git commit -m "feat: detect pre-sorted tables for streaming merge join

When both join sides are sorted on join keys (from Iceberg sort order),
use SortMergeJoinExec directly. O(batch_size) memory, zero shuffle."
```

---

### Task 27: Predicate transfer

**Files:**
- Create: `crates/sqe-planner/src/predicate_transfer.rs`

- [ ] **Step 1: Implement join predicate pushdown**

After scanning the build side, extract the set of distinct join-key values and push as a predicate to the probe side:

```rust
pub struct PredicateTransfer {
    /// Distinct join-key values extracted from the build side
    key_values: HashSet<ScalarValue>,
    /// Column name on the probe side
    probe_column: String,
}

impl PredicateTransfer {
    /// Convert to an IN-list predicate for the probe side
    pub fn to_predicate(&self) -> Expr {
        col(&self.probe_column).in_list(
            self.key_values.iter().cloned().map(lit).collect(),
            false,
        )
    }
}
```

This predicate enables:
- File-level pruning on the probe side (using min/max from manifests)
- Bloom filter pruning (if available)
- Typically skips 90%+ of probe-side files for selective joins

- [ ] **Step 2: Size limit**

Only apply predicate transfer when the distinct key set is small enough (< 10,000 values). Large sets would bloat the IN-list predicate and slow evaluation.

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add predicate transfer for join optimization

After scanning build side, push distinct join-key values as IN-list
predicate to probe side's Iceberg scan. Enables file-level and bloom
filter pruning on probe side."
```

---

## Stream 9: Multi-Endpoint Flight SQL

### Task 28: Return multiple FlightEndpoints for distributed results

**Files:**
- Modify: `crates/sqe-coordinator/src/flight_sql.rs`

- [ ] **Step 1: Multi-endpoint GetFlightInfo**

When query results are partitioned across executors (distributed sort, distributed aggregate), return one `FlightEndpoint` per executor:

```rust
fn build_flight_info_distributed(
    &self,
    schema: &SchemaRef,
    executor_endpoints: &[(String, Ticket)], // (executor_url, ticket)
) -> FlightInfo {
    let endpoints: Vec<FlightEndpoint> = executor_endpoints.iter().map(|(url, ticket)| {
        FlightEndpoint::new()
            .with_ticket(ticket.clone())
            .with_location(url)
    }).collect();

    FlightInfo::new()
        .with_schema(schema)
        .with_endpoint(endpoints)
}
```

- [ ] **Step 2: Backward compatibility**

For non-distributed queries (single-node mode, small queries), continue returning a single endpoint pointing at the coordinator. Existing clients work unchanged.

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: return multiple FlightEndpoints for distributed query results

GetFlightInfo returns one FlightEndpoint per executor when results are
distributed (sort, aggregate). Clients can open parallel DoGet streams
for maximum throughput. Backward compatible: single endpoint for
non-distributed queries."
```

---

## Stream 10: Observability

### Task 29: Spill and shuffle metrics

**Files:**
- Modify: `crates/sqe-metrics/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Add new Prometheus metrics**

```rust
// Spill metrics
sqe_sort_spill_count_total          // Counter: number of sort spill events
sqe_sort_spill_bytes_total          // Counter: bytes spilled for sorts
sqe_join_spill_count_total          // Counter: join spill events (SortMergeJoin fallback)
sqe_join_spill_bytes_total          // Counter: bytes spilled for joins

// Shuffle metrics (Phase B)
sqe_shuffle_bytes_sent_total        // Counter: bytes sent via DoExchange
sqe_shuffle_bytes_received_total    // Counter: bytes received via DoExchange
sqe_shuffle_partitions_total        // Counter: shuffle partitions created

// Late materialization metrics
sqe_late_mat_bytes_predicate_total  // Counter: bytes read for predicate evaluation
sqe_late_mat_bytes_projection_total // Counter: bytes read for projection
sqe_late_mat_selectivity            // Histogram: rows surviving / total rows

// Memory metrics
sqe_coordinator_memory_used_bytes   // Gauge: current memory pool usage
sqe_coordinator_memory_limit_bytes  // Gauge: configured memory limit
sqe_memory_pressure                 // Gauge: 0=green, 1=yellow, 2=orange, 3=red

// Pruning metrics
sqe_files_pruned_minmax_total       // Counter: files skipped by min/max pruning
sqe_files_pruned_bloom_total        // Counter: files skipped by bloom filter
sqe_pages_pruned_index_total        // Counter: pages skipped by page index

// Latency
sqe_time_to_first_row_seconds       // Histogram: time from query submit to first result row
```

- [ ] **Step 2: Wire metrics into operators**

Read spill stats from DataFusion's `ExecutionPlan::metrics()` after query completion. Aggregate and publish to Prometheus.

- [ ] **Step 3: Test + commit**

```bash
git commit -m "feat: add spill, shuffle, and late materialization Prometheus metrics

Track sort/join spill events, shuffle bytes, late materialization
effectiveness, memory pressure, pruning stats, and time-to-first-row."
```

---

## Final: Integration Verification

### Task 30: End-to-end validation

- [ ] **Step 1: Phase A validation**

Run TPC-H SF100 on coordinator with `memory_limit = "8GB"`:
- All 22 queries complete without OOM
- Spill metrics show spill events for large sorts/joins
- Late materialization reduces bytes read (compare with/without via metrics)

- [ ] **Step 2: Phase B validation** (after all shuffle infrastructure)

Run TPC-H SF100 on 4-executor cluster with `memory_limit = "4GB"` each:
- All queries complete
- Shuffle metrics show inter-executor data movement
- Distributed sort uses range-partition (verify via EXPLAIN)
- Time-to-first-row < 1s for LIMIT queries

- [ ] **Step 3: Update README.md roadmap and nextsteps.md**

Mark streaming execution items as complete. Update status line.

- [ ] **Step 4: Commit**

```bash
git commit -m "docs: update roadmap for streaming execution milestone

Phase A: coordinator spill, late materialization, scan planning.
Phase B: DoExchange shuffle, distributed sort/join/aggregate."
```

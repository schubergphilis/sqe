# Adaptive Sort Stripping Under Memory Pressure

**Date:** 2026-04-07
**Status:** Draft
**Author:** Jacob Verhoeks

## Problem

SQE's memory behavior under pressure is counterintuitive: an 8GB coordinator spills *more* than a 512MB one because it can start queries that 512MB rejects outright. The primary memory consumer in these cases is `SortExec` — ORDER BY on non-partition columns allocates sort buffers that, under pressure, spill to disk (slow) or trigger OOM (fatal).

The insight: **sorting by non-partition columns is a convenience, not a structural requirement.** When memory is scarce, returning unsorted data is better than spilling, timing out, or crashing.

## Solution

A three-mode `sort_mode` configuration that controls whether `SortExec` nodes are preserved or stripped from the physical plan, based on whether sort keys match Iceberg partition columns and (optionally) current memory pressure.

## Research Foundations

This design draws from established query processing research:

- **Adaptive Query Processing** (Deshpande et al., FTDB 2007) — Runtime plan adaptation based on observed conditions. Our adaptive mode applies this principle: the plan changes based on memory state at optimization time.
- **Eddies and SteMs** (Avnur & Hellerstein, SIGMOD 2000) — Pioneered per-tuple routing decisions based on runtime conditions. Our approach is coarser-grained (per-query) but follows the same principle of runtime-aware plan selection.
- **Memory-Adaptive External Sorting** (Graefe, VLDB 2006) — Describes replacement-selection sort that adapts partition count to available memory. We go further: when memory is insufficient, skip the sort entirely rather than degrading into multi-pass external sort.
- **Progressive Optimization** (Markl et al., VLDB 2004) — Re-optimizes query plans mid-execution when cardinality estimates prove wrong. Our adaptive mode is a simpler variant: check memory once at plan time and adjust.
- **How Good Are Query Optimizers, Really?** (Leis et al., VLDB 2015) — Demonstrates that cardinality misestimation causes catastrophic memory usage in sorts and joins. By making sorts conditional on available memory, we decouple correctness from estimation accuracy.
- **Morsel-Driven Parallelism** (Leis et al., SIGMOD 2014, Umbra/DuckDB) — Work-stealing execution with per-morsel memory accounting. Informs our approach of checking global memory state before committing to expensive operators.

The key principle: **graceful degradation over hard failure.** Return results in partition order rather than spilling to disk or killing the query.

## Configuration

New field in `QueryConfig`:

```rust
/// Controls when ORDER BY clauses are preserved vs stripped to save memory.
/// - "strict":         Always sort. Spill to disk if needed. (backwards-compatible)
/// - "partition_only": Only sort when keys match Iceberg partition columns.
/// - "adaptive":       Sort when memory is Green; strip non-partition sorts under pressure.
/// Default: "adaptive"
#[serde(default = "default_sort_mode")]
pub sort_mode: String,
```

```toml
[query]
sort_mode = "adaptive"   # default
```

## AdaptiveSortRule — Physical Optimizer Rule

**Location:** `crates/sqe-planner/src/adaptive_sort.rs`

Runs **before** `DistributedSortRule` in the physical optimizer chain (no point distributing a sort we're about to strip).

### Algorithm

```
for each SortExec node in the physical plan:
    if sort has LIMIT (fetch != None):
        KEEP — TopK is cheap, don't strip
    
    partition_cols = walk input subtree → find IcebergScanExec → extract partition column names
    sort_cols = extract column names from SortExec.expr()
    
    is_partition_sort = all sort_cols are in partition_cols
    
    match (sort_mode, pressure, is_partition_sort):
        (strict, _, _)                    → KEEP
        (partition_only, _, true)          → KEEP
        (partition_only, _, false)         → STRIP
        (adaptive, Green, _)              → KEEP
        (adaptive, Yellow|Orange|Red, true)  → KEEP
        (adaptive, Yellow|Orange|Red, false) → STRIP
    
    STRIP means: replace SortExec with its input child
```

### Partition Key Extraction

Add to `IcebergScanExec`:

```rust
/// Returns the names of identity-transform partition columns.
/// Bucket/truncate/date transforms are excluded (not sortable as raw columns).
pub fn partition_column_names(&self) -> Vec<String> {
    let spec = self.table.metadata().default_partition_spec();
    let schema = self.table.metadata().current_schema();
    spec.fields()
        .iter()
        .filter(|f| f.transform() == &Transform::Identity)
        .filter_map(|f| schema.field_by_id(f.source_id()).map(|sf| sf.name.clone()))
        .collect()
}
```

### Memory Pressure Access

The rule receives a reference to the coordinator's `RuntimeEnv` → `MemoryPool`, and calls `memory::check_pressure()`. This is already available at plan optimization time since the shared runtime is passed to `create_session_context`.

## Error Messages — Clear and Actionable

When a sort is stripped, the user must understand *what happened* and *what to do*. This is not a silent optimization — the user's result set is different from what they asked for.

### In-band Warning (Flight SQL info header)

Attached to the query result as a gRPC trailing metadata header:

```
x-sqe-warning: ORDER BY [column_name, ...] was removed due to memory pressure 
  (sort_mode=adaptive, pressure=yellow). Results are returned in partition order. 
  To force sorting, set sort_mode=strict or remove ORDER BY if ordering is not required.
```

### Rejection Message (Red pressure, strict mode)

When memory is Red and sort can't be stripped (strict mode), the current error is vague:

```
Server under memory pressure (>95% utilized). Please retry later.
```

Replace with an actionable message:

```
Query rejected: server memory is >95% utilized. Your query includes ORDER BY 
[col1, col2] which requires sort buffers. Options:
  1. Remove ORDER BY from your query (data returns in partition order)
  2. Add LIMIT to reduce sort memory (e.g., ORDER BY col1 LIMIT 1000)
  3. Retry later when memory pressure decreases
  4. Ask your administrator to set sort_mode=adaptive for automatic sort management
```

### Log Entry (WARN)

```
WARN sort_stripped{query_id=abc123 user=jacob}: 
  Stripped ORDER BY [col1, col2] — non-partition columns, pressure=yellow, sort_mode=adaptive. 
  Partition columns: [year, month]. Consider removing ORDER BY or adding LIMIT.
```

### Metric

```rust
pub sorts_stripped_total: IntCounterVec,  // labels: ["mode", "reason"]
// reason: "partition_only" | "memory_pressure"
```

## What This Covers and What It Does NOT

### Covered

| Scenario | Behavior |
|---|---|
| `SELECT * FROM t ORDER BY name` (name not partition key) | Stripped under pressure or partition_only mode |
| `SELECT * FROM t ORDER BY year, month` (partition keys) | Always kept |
| `SELECT * FROM t ORDER BY name LIMIT 100` | Always kept (TopK is cheap) |
| `SELECT * FROM t` (no ORDER BY) | Unaffected |
| Data already sorted by Iceberg sort order | DataFusion elides sort via EquivalenceProperties — unaffected |

### NOT Covered (Future Work)

- **Join memory** — Hash joins on large tables still need `FairSpillPool` + `SortMergeJoin` fallback. Sort stripping doesn't help here.
- **Aggregation memory** — `GROUP BY` on high-cardinality columns still builds hash tables. Future: adaptive aggregation spill.
- **Runtime re-optimization** — This checks pressure once at plan time. A query planned at Green could execute at Red. Full progressive optimization (Markl 2004) would re-plan mid-execution.
- **Distributed sort interaction** — When sort is kept, `DistributedSortRule` still fires. When stripped, the distributed sort is also eliminated (no sort = nothing to distribute).

## Integration Points

### Plan Optimization Chain (sqe-planner)

```
PhysicalPlan
  → AdaptiveSortRule        ← NEW: strip non-essential sorts
  → DistributedSortRule     ← existing: distribute remaining sorts
  → StageDecomposition      ← existing: split into stages
```

### Session Context (sqe-coordinator)

Pass `sort_mode` config and `RuntimeEnv` reference to the planner's optimizer chain.

### Config (sqe-core)

Add `sort_mode` to `QueryConfig` with default `"adaptive"`.

## Testing

1. **Unit: AdaptiveSortRule** — Construct physical plans with `SortExec` over mock `IcebergScanExec`, verify strip/keep decisions for all 6 cells in the decision matrix.
2. **Unit: partition_column_names()** — Test with identity, bucket, truncate transforms; verify only identity columns returned.
3. **Unit: error messages** — Verify warning header content and rejection message formatting.
4. **Integration: benchmark comparison** — Run TPC-H SF1 under 512MB with `strict` vs `adaptive`; measure queries completed and spill events.

## Success Criteria

- `adaptive` mode reduces spill events by >50% on the 8GB TPC-H benchmark
- 512MB coordinator completes >80% of TPC-H queries with `adaptive` (vs current ~60% with `strict`)
- Zero correctness regressions: queries with LIMIT preserve ordering; partition-key sorts always preserved
- Clear, actionable error messages that tell users exactly what to change

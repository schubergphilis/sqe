# Streaming Execution Engine — Design Spec

## Summary

Transform SQE from a coordinator-centric execution model (where all post-scan computation happens on the coordinator) into a streaming execution engine that can process arbitrarily large datasets (1TB+) regardless of available RAM. Two phases: Phase A makes the system safe (nothing OOMs), Phase B makes it fast (computation moves to workers).

**Origin:** Architecture review identified that the coordinator holds all intermediate state in memory — sorts, joins, aggregations. A 1TB ORDER BY on a 16GB coordinator will OOM. Workers already have spill-to-disk via `FairSpillPool`, but the coordinator does not.

## Prerequisites

1. **Finish pluggable auth** — 3 remaining tasks (13.6 Trino 401 header, 14.2 config example, 14.3 coordinator wiring) must be completed to clear the "← NEXT" marker in nextsteps.md.
2. **Stay on DataFusion 52 / arrow-rs 57** — the RisingWave iceberg-rust fork (`rev 1978911ec4`) is pinned to DF 52.1 / arrow 57.1. Upgrading to DF 53 / arrow 58 is blocked until the fork rebases (the fork provides `rewrite_files()` which upstream lacks). All Phase A features are available on DF 52.
3. **Pluggable catalogs** (0/83 tasks) and **semantic AI layer** (0/50 tasks) are independent future steps — not blocked by and not blocking this work.

### Dependency Constraints

| Dependency | Current | Target | Constraint |
|---|---|---|---|
| DataFusion | 52 | **52** (stay) | RisingWave fork pinned to DF 52.1. Upgrade to DF 53 deferred until fork rebases. |
| arrow-rs / parquet | 57 | **57** (stay) | Locked to DF 52 arrow version |
| iceberg-rust | RisingWave fork `1978911ec4` | Same | Required for `rewrite_files()`. Upstream lacks this API (EPIC #624 closed). |
| sqlparser | 0.53 | **0.53** (stay) | DF 53 requires 0.61 — defer migration until DF upgrade |

### DF 53 Upgrade Path (deferred)

Monitor the RisingWave fork's `dev_rebase_main_20260303` branch. When a DF53-compatible rev appears, upgrade SQE in one coordinated step: new fork rev + DF 53 + arrow 58 + sqlparser 0.53→0.61 + `sqe-sql` AST migration. This upgrade becomes a prerequisite for Phase B (SpillManager unification, stable SortMergeJoin, arrow 58 decode speedups benefit shuffle-heavy workloads).

### Upstream Capabilities Available on DF 52

| Feature | Status on DF 52 | Notes |
|---|---|---|
| `FairSpillPool` | Available | Recommended pool, exposes `reserved()` and `memory_limit()` |
| Sort spill (external merge) | Available | Working, some edge cases under extreme memory pressure (DF#16132) |
| Hash aggregate spill | Available | Working since ~DF 32 |
| Hash join spill | NOT available | Proposal only (DF#17267). SortMergeJoin fallback required. |
| `SortMergeJoinExec` | Available (experimental flag) | Functional. Becomes stable in DF 53. Buffered-side spill works. |
| `pushdown_filters` (late materialization) | Available (opt-in) | Set `pushdown_filters=true`. Known regressions on some patterns (DF#20324) — test before enabling. |
| PageIndex pruning | Available (arrow-rs 57) | Parquet reader supports page-level min/max |
| Bloom filter read | Available (arrow-rs 57) | `parquet::bloom_filter` module. Manual integration needed. |
| iceberg-rust manifest stats | Available (v0.9) | `DataFile` exposes `lower_bounds`, `upper_bounds`, `null_value_counts` |
| iceberg-rust sort order | Available (v0.9) | `table.metadata().current_sort_order()` |
| iceberg-datafusion predicate pushdown | Available (v0.9) | Partition pruning + some file-level pushdown |
| Ballista shuffle primitives | Available (v52) | `ShuffleWriterExec` / `ShuffleReaderExec` — basic but usable as starting point |

## Motivation

- **1TB queries on small servers**: Users need to query datasets far exceeding available RAM
- **Three deployment profiles**: Single-node (16GB), small coordinator + many workers, medium coordinator + few workers — all must work
- **Streaming results**: Clients should receive rows before the full query completes
- **Graceful degradation**: Seamless transition from in-memory to external processing under memory pressure

## Design Principles

1. **Never buffer what you can stream** — pipeline data through operators via Arrow batches
2. **Never read what you can prune** — exploit Iceberg metadata at every level (partition → file min/max → page index → bloom filter)
3. **Never materialize what you can skip** — predicate columns first, projection columns only for surviving rows
4. **Graceful degradation** — seamlessly transition from in-memory to spill-to-disk under pressure
5. **Zero-copy where possible** — Arrow IPC between executors, Arrow Flight SQL to clients

## Key References

| # | Paper | Venue | Relevance |
|---|---|---|---|
| R1 | Graefe, "Volcano — An Extensible and Parallel Query Evaluation System" | IEEE TKDE 1994 | Exchange operator for distributed parallelism |
| R2 | Shapiro, "A Study of the Hybrid Hash Join Algorithm" | VLDB 1986 | Partition-based spilling for joins |
| R3 | Sethi et al., "Presto: SQL on Everything" | ICDE 2019 | Coordinator/worker architecture SQE mirrors |
| R4 | Leis et al., "Morsel-Driven Parallelism" | SIGMOD 2014 | NUMA-aware parallel execution |
| R5 | Pedreira et al., "Velox: Meta's Unified Execution Engine" | VLDB 2022 | Cooperative memory arbitration |
| R6 | Raasveldt & Mühleisen, "DuckDB: An Embeddable Analytical Database" | SIGMOD 2019 | Single-node out-of-core execution |
| R7 | Pang et al., "Memory-Adaptive External Sorting" | VLDB 1993 | Dynamic sort splitting for multi-tenant memory |
| R8 | Abadi et al., "Materialization Strategies in a Column-Oriented DBMS" | ICDE 2007 | Late materialization tradeoffs |
| R9 | Yang et al., "Accelerate Distributed Joins with Predicate Transfer" | SIGMOD 2025 | Pre-filtering multi-join queries |

---

## Phase A: Safe Execution (Spill-to-Disk + Late Materialization)

### Goal

Make SQE safe for all deployment profiles. No query OOMs regardless of data size.

### A1. Coordinator Spill-to-Disk

**Problem:** The coordinator's DataFusion `SessionContext` in `query_handler.rs:512` is created without a memory pool or spill directory. All operators (sort, hash join, hash aggregate) allocate unbounded memory.

**Solution:** Apply the same `FairSpillPool` + `DiskManager` pattern that `sqe-worker/src/runtime.rs` already uses. Add config fields to `CoordinatorConfig`:

```rust
// In sqe-core/src/config.rs — add to CoordinatorConfig
pub memory_limit: String,      // default: "80%" or "8GB"
pub spill_to_disk: bool,       // default: true
pub spill_dir: String,         // default: "/tmp/sqe-coordinator-spill"
pub spill_compression: String, // default: "lz4" — "none" | "lz4" | "zstd"
```

The `create_session_context()` method in `query_handler.rs` must build a `RuntimeEnv` with:
- `FairSpillPool` configured to `memory_limit`
- `DiskManager` pointing to `spill_dir`
- Spill compression via DataFusion's `SessionConfig::with_sort_spill_reservation_bytes()`

**Memory watermarks** (logged, not enforced in Phase A):
| Level | Threshold | Action |
|---|---|---|
| Green | < 70% | Normal operation |
| Yellow | 70-85% | Log warning, pause prefetch |
| Orange | 85-95% | Trigger voluntary spill on largest consumer |
| Red | > 95% | Reject new query admissions |

Query admission control: when memory pool usage exceeds 95%, `execute()` returns `RESOURCE_EXHAUSTED` instead of accepting the query. Uses the existing `query_semaphore` pattern.

### A2. SortMergeJoin Fallback

**Problem:** DataFusion's hash join spill is still a proposal (upstream issue #17267). A hash join on two large tables will OOM.

**Solution:** Add a `join_strategy` config option and plan-time check:
- If estimated build-side size (from Iceberg manifest file sizes) exceeds `hash_join_memory_threshold` (default: 25% of `memory_limit`), rewrite `HashJoinExec` → `SortMergeJoinExec` in the physical plan
- SortMergeJoin spills gracefully via DataFusion's existing external sort
- Implemented as a `PhysicalOptimizerRule` registered on the coordinator's `SessionContext`

```rust
// New file: crates/sqe-planner/src/join_strategy.rs
pub struct JoinStrategyRule {
    hash_join_threshold: usize, // bytes
}

impl PhysicalOptimizerRule for JoinStrategyRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        // Walk plan tree, find HashJoinExec nodes
        // If build-side estimated size > threshold, replace with SortMergeJoinExec
        // Preserve join type (inner/left/right/full/semi/anti)
    }
}
```

### A3. Late Materialization

> **Note:** DataFusion's `pushdown_filters=true` + `reorder_filters=true` (available on DF 52) may already provide late materialization through the Parquet reader's RowFilter API. If iceberg-datafusion passes predicates through to the Parquet reader correctly, this section reduces to enabling config flags + testing for regressions (DF#20324). Verify in Prereq 2 before building custom plumbing.

**Problem:** Current Iceberg scan reads all projected columns in one pass. For a query like `SELECT * FROM events WHERE user_id = 42`, all 50 columns are decoded even though only `user_id` is needed for filtering. On a 1TB table, this means reading ~1TB from S3 when only ~20GB (the predicate column) needs to be read for filtering.

**Solution:** Implement two-phase scan using arrow-rs `RowFilter` API:

```
Phase 1: Fetch + decode predicate columns only → evaluate → RowSelection bitmask
Phase 2: Fetch + decode projection columns only for surviving rows (using RowSelection)
```

This requires modifying `IcebergScanExec` (in `sqe-catalog`) to:
1. Classify columns as predicate vs. projection-only
2. Build a `RowFilter` from the WHERE clause predicates
3. Pass the `RowFilter` to the `ParquetRecordBatchReaderBuilder`
4. Use `CachedArrayReader` to avoid re-reading predicate columns that are also in the projection

**Memory impact:** For a 1TB table with 5% selectivity and 50 columns:
- Before: read ~1TB, decode 1TB, filter to 50GB
- After: read ~20GB (predicate col), filter, read ~1GB (projection cols for survivors)
- **50x reduction in S3 reads, 20x reduction in peak memory**

### A4. Iceberg Scan Planning Enhancements

**Problem:** Current scan planning does partition pruning but skips file-level and page-level pruning.

**Solution:** Enhance the Iceberg scan planning pipeline:

1. **File-level min/max pruning**: Manifest entries contain per-file min/max for each column. Filter files whose ranges don't intersect with query predicates. Already available in iceberg-rust manifest metadata.

2. **Sort-order detection**: Read `sort-order` from Iceberg table metadata, compare against `ORDER BY`:
   - Table sort = query ORDER BY → streaming k-way merge (zero spill)
   - Table sort is prefix → merge on prefix, local sort within groups
   - ORDER BY ... LIMIT N (small N) → TopK pushdown (heap of size N per worker)
   - No match → external sort with spill

3. **Page-level pruning**: When Parquet `PageIndex` is available, use page-level min/max to skip pages within row groups. This narrows the byte ranges fetched from S3.

4. **Bloom filter pruning**: When bloom filters are configured on the table, check point-lookup predicates (e.g., `user_id = 42`) against bloom filters before fetching data.

### A5. S3 I/O Pipeline

**Problem:** Current scan issues sequential S3 GETs per file. No prefetch, no byte-range coalescing, no footer caching.

**Solution:**

| Optimization | Description | Impact |
|---|---|---|
| **Parallel byte-range GETs** | Fetch multiple column chunks concurrently per file | 3-5x throughput per file |
| **Byte-range coalescing** | Merge adjacent column chunk ranges within configurable gap threshold (default 1MB) | Fewer S3 requests |
| **Parquet footer cache** | LRU cache for parsed Parquet footers across queries (default 256MB) | Eliminates repeated footer reads for frequently queried tables |
| **Prefetch overlap** | Footer of file[N+1] fetched during decode of file[N] | Hides S3 latency |
| **Connection pooling** | Configurable HTTP/2 connection pool to S3 endpoint (default 64 connections) | Sustained throughput |

Config section:
```toml
[executor.s3]
io_threads = 16
concurrent_requests_per_file = 4
max_concurrent_files = 8
coalesce_threshold = "1MB"
connection_pool_size = 64
prefetch_buffer = "32MB"
footer_cache_size = "256MB"
```

---

## Phase B: Fast Execution (Distributed Computation)

### Goal

Move computation to workers. Coordinator becomes a thin planner/orchestrator. Enables linear scaling for sorts, joins, and aggregations.

### B1. Flight DoExchange Shuffle Infrastructure

**Problem:** Workers currently only scan. All post-scan work (sort, join, aggregate) runs on the coordinator. For a 1TB ORDER BY, all 1TB flows through the coordinator.

**Solution:** Implement Arrow Flight `DoExchange` on executors for bidirectional streaming. Two partitioning modes:

1. **Hash partitioning**: For joins and GROUP BY — rows routed to executor `hash(key) % num_executors`
2. **Range partitioning**: For ORDER BY — rows routed to executor owning that sort-key range

New `ExecutionPlan` nodes:
```rust
// Sends batches to remote executors based on partition function
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioner: Partitioner, // Hash or Range
    target_endpoints: Vec<FlightEndpoint>,
}

// Receives batches from remote executors
pub struct ShuffleReaderExec {
    schema: SchemaRef,
    source_endpoints: Vec<FlightEndpoint>,
}
```

Transport: Arrow IPC with LZ4 compression over Flight `DoExchange`. Natural backpressure via gRPC streaming — if a receiver is slow (spilling), the sender slows its scan rate.

### B2. Distributed Range-Partition Sort

**Problem:** Global sort of 1TB requires either 1TB spill on coordinator, or distributing the sort.

**Solution:** Four-phase distributed sort (ref: R1, R7):

```
Phase 1 — Sample (coordinator, zero data I/O)
  Read per-file min/max from Iceberg manifests for sort columns
  Compute approximate quantile boundaries
  If insufficient: request ~1000 rows per executor via reservoir sampling
  Determine P-1 range boundaries for P executors

Phase 2 — Scan + Range-Partition Shuffle
  Each executor scans its files, applies predicates (late materialization)
  Routes surviving batches to the executor owning that sort range
  Transport: Arrow Flight DoExchange

Phase 3 — Local Sort
  Each executor sorts its range partition (with spill if needed)
  Ranges are disjoint → concatenation = globally sorted

Phase 4 — Streaming Result Delivery
  Executors stream sorted ranges in order
  Coordinator returns multiple FlightEndpoint objects to client
  Client opens parallel DoGet streams
```

**1TB sort on 8 executors with 8GB RAM each:**
- Each executor sorts ~125GB with ~8GB RAM + spill
- Spill per executor: ~125GB
- Coordinator memory: ~0 (just orchestrates)
- Total cluster disk: ~1TB (spread across 8 executors)

### B3. Distributed Joins

**Broadcast join** (build side < `broadcast_threshold`, default 64MB):
- Small side scanned, collected, broadcast to all executors via Flight
- Large side applies join probe during scan — no shuffle of large side

**Shuffle hash join** (both sides large):
- Both sides hash-partitioned on join keys via DoExchange
- Each executor builds hash table for its partition, probes against incoming
- Memory per executor: O(build_side / num_executors)

**Sort-merge join** (one or both sides sorted on join key):
- If Iceberg sort order matches join key → streaming merge join
- Memory: O(batch_size) — no hash table, no spill

**Predicate transfer** (ref: R9):
- After scanning build side, extract distinct join-key values
- Push as IN-list predicate to probe side's Iceberg scan
- Enables file-level + bloom filter pruning on probe side
- Can skip 90%+ of probe-side files for selective joins

### B4. Multi-Endpoint Flight SQL

**Problem:** Currently all results flow through the coordinator endpoint. For distributed sort/aggregation, results are already on executors.

**Solution:** When query results are partitioned across executors:
- `GetFlightInfo` returns multiple `FlightEndpoint` objects (one per executor)
- Client opens parallel `DoGet` streams directly to executors
- Coordinator never touches result data
- For non-distributed queries, single endpoint (backward compatible)

### B5. Distributed Aggregation

**Two-phase aggregation:**
1. **Partial aggregation on workers**: Each executor computes partial aggregates (SUM → partial_sum + count, AVG → sum + count, etc.) during scan
2. **Final aggregation on coordinator** (or single executor): Merge partial aggregates

For high-cardinality GROUP BY:
- Hash-partition on GROUP BY keys via DoExchange
- Each executor aggregates its partition
- No single-node bottleneck

### B6. Observability Extensions

Per-operator spill metrics:
- `sqe_sort_spill_count` / `sqe_sort_spill_bytes`
- `sqe_join_spill_count` / `sqe_join_spill_bytes`
- `sqe_shuffle_bytes_sent` / `sqe_shuffle_bytes_received`
- `time_to_first_row_ms` — latency from query submit to first result row
- `peak_memory_bytes` per query
- `bytes_read_predicate_only` vs `bytes_read_projection` (late materialization effectiveness)

---

## What's Not In Scope

- Coordinator HA (architecture review W1) — separate effort
- Policy enforcement (architecture review C1) — blocked on Polaris OPA SPI
- Coordinator↔worker mTLS (architecture review W5) — separate security effort
- Deletion vector support (Iceberg v3) — future
- Adaptive memory rebalancing (Velox-style arbitration) — future, start with `FairSpillPool`
- DataFusion correlated subquery limitation — orthogonal, tracked separately

---

## Success Criteria

**Phase A:**
- `ORDER BY` on 1TB dataset completes (with spill) on a 16GB single-node deployment
- Late materialization reduces S3 bytes read by >10x for selective queries
- Parquet footer cache eliminates repeated footer reads in benchmark suite
- No OOM for any TPC-H SF100 query on coordinator with 8GB memory limit

**Phase B:**
- 1TB ORDER BY across 8 executors completes with <125GB spill per executor
- Distributed hash join of two 500GB tables completes without OOM
- Time-to-first-row for LIMIT queries is <1s regardless of table size
- Linear throughput scaling: 2x executors → ~1.8x throughput for scan-heavy queries

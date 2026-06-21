# Streaming Execution

SQE's streaming execution engine enables 1TB-scale queries on memory-constrained servers. The implementation is split into two phases: Phase A (safe) handles single-node memory management and scan optimization, while Phase B (fast) distributes computation across workers via Arrow Flight DoExchange.

## The Problem

A coordinator-centric query engine hits a hard wall: every intermediate result flows through one process. An `ORDER BY` on 1TB of data requires 1TB of memory (or spill space) on the coordinator, regardless of how many workers scanned the data. A four-way hash join between large tables can exhaust coordinator memory long before the result set is assembled.

The fundamental tension is between sovereignty (run on your own hardware, which may be small) and scale (query datasets that don't fit in memory). SQE solves this in two stages: first, make the coordinator survive large queries through spill-to-disk and scan optimization (Phase A); then, push computation to workers so the coordinator handles only final aggregation (Phase B).

## Phase A: Safe (Single-Node)

Phase A ensures that a single coordinator with limited memory (e.g., 512MB) can execute large analytical queries without OOM kills.

### Coordinator Spill-to-Disk

DataFusion's `FairSpillPool` divides available memory across all active operators. When an operator (sort, hash aggregate, hash join) exceeds its share, it spills intermediate results to disk as sorted runs.

Key components:

- **FairSpillPool** -- configured via `memory_limit` in `sqe.toml`. Divides memory equally among registered `MemoryConsumer` instances. Triggers spill when any consumer exceeds its fair share.
- **Watermark system** -- four levels (green/yellow/orange/red) based on pool utilization percentage. Green (<60%) allows normal execution. Yellow (60-75%) triggers advisory warnings. Orange (75-90%) forces spillable operators to spill. Red (>90%) activates admission control, queueing new queries until memory drops below the orange threshold.
- **Admission control** -- when the pool is in the red zone, new queries wait in a bounded queue rather than competing for memory. This prevents cascade failures where N concurrent queries each grab 1/N of memory and all spill simultaneously.
- **External merge sort** -- when a `SortExec` spills, it writes sorted runs to `spill_dir` (default: `/tmp/sqe-spill`). On final output, a k-way merge reads all runs simultaneously, producing a globally sorted stream with constant memory overhead.

Configuration in `sqe.toml`:

```toml
[coordinator]
memory_limit = "512MB"
spill_dir = "/tmp/sqe-spill"
spill_compression = "zstd"  # lz4, zstd, or none
```

### Late Materialization

Standard Parquet scans read all projected columns from every row group. Late materialization splits this into two phases:

1. **Predicate phase** -- read only the columns referenced in `WHERE` clauses. Apply filters. Produce a set of surviving row indices.
2. **Projection phase** -- for surviving rows only, read the remaining projected columns.

This is implemented as a two-phase `RowFilter` scan in the Iceberg scan planning layer. For queries with selective predicates (e.g., `WHERE status = 'CLOSED'` on a table where 5% of rows match), late materialization reduces I/O by up to 95% on the non-predicate columns.

The optimization is transparent to the rest of the plan -- the `TableScan` still produces the same Arrow schema. The difference is entirely in how many bytes are read from Parquet.

### Iceberg Scan Planning

Three optimizations happen before any Parquet data is read:

- **File-level min/max pruning** -- Iceberg manifest files contain per-column min/max statistics for each data file. SQE reads these statistics and skips files where the predicate cannot match. For example, `WHERE order_date > '2025-01-01'` skips any file whose `order_date` max is before 2025.
- **Sort-order detection** -- Iceberg metadata records the sort order of each data file. When a query includes `ORDER BY` on the sort column, SQE can skip the sort operator entirely and produce output directly from the pre-sorted scan. When multiple sorted files need merging, a merge-sort is cheaper than a full re-sort.
- **PageIndex pruning** -- for Parquet files with page-level statistics (column index), SQE prunes individual pages within a row group, further reducing I/O for selective predicates.
- **TopK optimization** -- `ORDER BY ... LIMIT N` queries use a heap-based TopK operator that maintains only N rows in memory, avoiding a full sort and spill.

### S3 I/O Pipeline

Reading Parquet files from S3 involves many small HTTP GET requests (one per column chunk per row group). SQE optimizes this with:

- **Request coalescing** -- adjacent byte ranges within `coalesce_threshold` (default: 1MB) are merged into a single GET request. This reduces the number of HTTP round-trips, which dominate latency on high-latency S3 endpoints.
- **Footer cache** -- Parquet file footers (schema, row group metadata, column chunk offsets) are cached in a `footer_cache_size`-bounded LRU cache. Repeated queries against the same table skip the footer read entirely.
- **Prefetch** -- while the executor processes the current row group, the next row group's column chunks are fetched in the background, hiding S3 latency behind compute.

Configuration:

```toml
[storage]
coalesce_threshold = "1MB"
footer_cache_size = 256  # number of footers
```

### SortMergeJoin Fallback

DataFusion's default join strategy is hash join, which builds a hash table from the build side in memory. For large joins, this hash table can exceed the memory limit. DataFusion does not yet support hash join spill-to-disk upstream.

SQE registers a `SortMergeJoin` fallback: when the estimated build-side size exceeds `hash_join_memory_threshold`, the optimizer rewrites the join as a sort-merge join. Both sides are sorted (spilling to disk if needed via the external merge sort) and then merged with constant memory. This is slower than an in-memory hash join but avoids OOM on large joins.

```toml
[optimizer]
hash_join_memory_threshold = "256MB"
```

## Phase B: Fast (Distributed)

Phase B pushes computation past the scan boundary. Instead of workers sending raw Arrow batches to the coordinator for all processing, workers perform filters, partial aggregations, partial sorts, and join probes locally.

### DoExchange Shuffle

Arrow Flight's `DoExchange` RPC enables bidirectional streaming between workers. SQE uses this to implement a hash-partitioned shuffle:

1. The coordinator decomposes the physical plan into stages separated by shuffle boundaries (e.g., a hash join requires both sides to be hash-partitioned on the join key).
2. Each stage runs on a set of workers. When a stage completes, its output is hash-partitioned by the shuffle key and streamed to the next stage's workers via `DoExchange`.
3. The partitioning function uses the same `hash(key) % num_partitions` scheme as DataFusion's `RepartitionExec`, ensuring compatibility with the existing hash join and hash aggregate operators.

### Distributed Sort (Range-Partition)

A distributed `ORDER BY` proceeds in three steps:

1. **Sample** -- each worker samples its local partition and sends the sample to the coordinator.
2. **Range boundaries** -- the coordinator computes quantile boundaries from the samples, producing N-1 split points for N workers.
3. **Range-partition and merge** -- each worker range-partitions its data and sends each range to the designated worker. Each receiving worker sorts its range locally (spilling if needed). The coordinator merges the sorted ranges via a k-way merge.

This distributes both the memory cost and the CPU cost of sorting. A 1TB `ORDER BY` with 8 workers requires roughly 125GB of spill per worker instead of 1TB on the coordinator.

### Two-Phase Aggregation

Aggregation queries (`GROUP BY`) use a two-phase approach:

1. **Partial aggregation** -- each worker computes partial aggregates on its local data. For `SUM(amount) GROUP BY region`, each worker produces a partial sum per region from its partition.
2. **Final aggregation** -- partial results are shuffled by the grouping key to a set of finalizer workers (or the coordinator for small result sets). Each finalizer merges the partial aggregates into the final result.

This solves the q18 problem: TPC-H query 18 has a high-cardinality `GROUP BY` that produces millions of groups. On a single coordinator with 512MB, the `GroupedHashAggregate` exceeds memory. With two-phase aggregation, each worker handles a fraction of the groups, and memory pressure is distributed.

### Distributed Joins

SQE supports four join strategies in distributed mode:

- **Broadcast join** -- when one side of the join is small (below `broadcast_threshold`, default 10MB), it is broadcast to all workers. Each worker probes its local partition of the large side against the broadcast table. No shuffle required.
- **Shuffle hash join** -- both sides are hash-partitioned on the join key and shuffled to matching workers. Each worker performs a local hash join on its partition.
- **Pre-sorted merge join** -- when both sides are already sorted on the join key (detected via Iceberg sort-order metadata), workers perform a merge join without re-sorting. This avoids the sort cost entirely.
- **Predicate transfer** -- before executing a join, the build side's distinct join keys are collected and pushed as an `IN`-list filter to the probe side's scan. This skips probe-side files that contain no matching keys, reducing I/O by 90%+ for selective joins. Based on the predicate transfer technique from Yang et al. (SIGMOD 2025).

```toml
[optimizer]
broadcast_threshold = "10MB"
```

### Multi-Endpoint Flight SQL

In Phase A, all results flow through the coordinator's single Flight SQL endpoint. Phase B adds multi-endpoint support: `get_flight_info` can return multiple `FlightEndpoint` entries, each pointing to a different worker. The client fetches results directly from workers, bypassing the coordinator for data transfer.

This eliminates the coordinator NIC bottleneck for large result sets. A query returning 4GB across 4 workers streams 1GB directly from each worker to the client, achieving 4x the effective bandwidth.

### Stage Decomposition

The coordinator decomposes the physical plan into stages:

1. **Scan stage** -- workers read Parquet files, apply predicates and projections.
2. **Shuffle stage** -- workers hash-partition or range-partition output for the next stage.
3. **Join/Aggregate stage** -- workers perform local joins or aggregations on shuffled data.
4. **Final stage** -- coordinator (or a designated worker) performs final aggregation, sort, or limit.

Each stage boundary is a shuffle point. The coordinator tracks stage completion and triggers the next stage when all workers in the current stage have finished.

## Memory Model

SQE uses a four-level watermark system to manage memory pressure:

| Level | Pool Utilization | Behavior |
|-------|-----------------|----------|
| Green | < 60% | Normal execution, no restrictions |
| Yellow | 60-75% | Advisory: log warnings, increment metrics |
| Orange | 75-90% | Spillable operators forced to spill |
| Red | > 90% | Admission control: new queries queued |

The `FairSpillPool` divides the total `memory_limit` equally among all registered `MemoryConsumer` instances. When a consumer's `try_grow` call would push the pool past the orange threshold, the pool asks other spillable consumers to spill first. If spilling frees enough memory, the allocation succeeds. If not, the allocation fails with `ResourceExhausted`.

Per-operator behavior:

- **SortExec** -- spills sorted runs to disk, later merged via k-way merge.
- **HashAggregateExec** -- spills partition groups to disk (when supported by DataFusion).
- **HashJoinExec** -- not spillable upstream; SQE rewrites to SortMergeJoin when estimated size exceeds threshold.
- **SortMergeJoinExec** -- both sides sort-and-spill independently, then merge with constant memory.

## Configuration Reference

| Field | Section | Default | Description |
|-------|---------|---------|-------------|
| `memory_limit` | `[coordinator]` / `[worker]` | `8GB` | Maximum memory for the DataFusion runtime |
| `spill_dir` | `[coordinator]` / `[worker]` | `/tmp/sqe-spill` | Directory for spill files |
| `spill_compression` | `[coordinator]` / `[worker]` | `zstd` | Compression for spill files (lz4, zstd, none) |
| `hash_join_memory_threshold` | `[optimizer]` | `256MB` | Build-side size above which hash join is rewritten to sort-merge join |
| `broadcast_threshold` | `[optimizer]` | `10MB` | Join side size below which broadcast join is used |
| `coalesce_threshold` | `[storage]` | `1MB` | Maximum gap between byte ranges to coalesce into one S3 GET |
| `footer_cache_size` | `[storage]` | `256` | Number of Parquet footers to cache |

## Benchmark Results

TPC-H at scale factor 1 (approximately 1GB of data) on a coordinator with 512MB memory and spill-to-disk enabled:

- **21 of 22 queries pass.** All queries produce correct results within the memory budget.
- **1 failure: q18.** TPC-H query 18 uses a high-cardinality `GROUP BY` with `HAVING` that produces millions of intermediate groups. DataFusion's `GroupedHashAggregate` does not yet support spill-to-disk for hash aggregation, so the operator exceeds the 512MB limit. This is a known upstream limitation. With Phase B's two-phase aggregation (distributing the groups across workers), q18 passes.

These results demonstrate that SQE can run analytical workloads on hardware that would be considered undersized for traditional query engines. The combination of spill-to-disk, late materialization, and scan planning keeps memory usage bounded regardless of data size.

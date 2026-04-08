## Why

SQE's coordinator-centric execution model works for scan-heavy queries where intermediate results fit in memory. It fails when:

- **Large sorts**: `ORDER BY` on 1TB requires 1TB in coordinator memory (or spill). With 8 workers, each sorts 125GB locally.
- **High-cardinality aggregations**: `GROUP BY` with millions of groups exceeds `GroupedHashAggregate` memory. Two-phase aggregation distributes groups across workers.
- **Large joins**: Hash join build side exceeds memory. Sort-merge join with spill is the safe fallback; distributed shuffle hash join distributes the work.
- **Result set bottleneck**: All results flow through the coordinator's single NIC. Multi-endpoint Flight SQL lets clients fetch directly from workers.

The fix is a two-phase streaming execution engine: Phase A makes the single-node path survive (spill, late materialization, scan planning, S3 I/O), and Phase B makes the distributed path fast (DoExchange shuffle, distributed sort/join/aggregate).

## What Changes

### Phase A (Safe)
- Coordinator spill-to-disk via FairSpillPool with watermark-based admission control
- Late materialization (two-phase RowFilter scan: predicate columns first, projection for survivors)
- Iceberg scan planning: file-level min/max pruning, sort-order detection, PageIndex, TopK optimization
- S3 I/O pipeline: request coalescing, footer cache, prefetch
- SortMergeJoin fallback when hash join build side exceeds memory threshold

### Phase B (Fast)
- DoExchange shuffle between workers (hash-partition and range-partition)
- Distributed range-partition sort
- Two-phase aggregation (partial on workers, final merge on coordinator/designated workers)
- Distributed joins: broadcast, shuffle hash, pre-sorted merge, predicate transfer
- Multi-endpoint Flight SQL (clients fetch directly from workers)
- Stage decomposition (plan split at shuffle boundaries)

### Trino Compatibility
- date_format, date_parse, now() function implementations
- json_object() function
- Transaction stubs (BEGIN/COMMIT/ROLLBACK return success for compatibility)

## Capabilities

### New Capabilities
- `streaming-spill`: coordinator and worker spill-to-disk with watermark-based memory management
- `late-materialization`: two-phase scan reads predicate columns first
- `scan-planning`: file pruning, sort-order detection, PageIndex
- `s3-io-pipeline`: coalescing, footer cache, prefetch
- `sort-merge-join`: fallback for large joins
- `do-exchange-shuffle`: worker-to-worker data exchange
- `distributed-sort`: range-partition sort across workers
- `distributed-aggregation`: two-phase partial/final aggregation
- `distributed-joins`: broadcast, shuffle hash, pre-sorted merge, predicate transfer
- `multi-endpoint-flight`: clients fetch results directly from workers
- `trino-functions`: date_format, date_parse, now, json_object

## Impact

- `sqe-coordinator`: FairSpillPool configuration, watermark system, admission control, stage decomposition
- `sqe-worker`: DoExchange handler, local sort/aggregate/join execution, multi-endpoint Flight SQL
- `sqe-planner`: late materialization rewrite, scan planning optimizations, stage decomposer, join strategy selection
- `sqe-catalog`: file-level statistics extraction, sort-order metadata
- `sqe-trino-compat`: new function implementations, transaction stubs
- `sqe-metrics`: spill, shuffle, late-mat, pruning, time-to-first-row metrics
- Config: new fields in `[coordinator]`, `[worker]`, `[optimizer]`, `[storage]` sections

## Benchmark Results

TPC-H SF1 on 512MB coordinator with spill: 21/22 pass. q18 fails on single-node (GroupedHashAggregate does not spill); passes with Phase B two-phase aggregation.

## Rollback

Phase A features are backward-compatible defaults (spill enabled, late materialization transparent). Phase B features activate only when workers are present and DoExchange is configured. Removing worker configuration reverts to Phase A single-node behavior.

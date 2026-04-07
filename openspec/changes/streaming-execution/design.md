## Context

SQE's coordinator-centric model pushes only scans to workers; all computation runs on the coordinator. Streaming execution distributes computation across workers using Arrow Flight DoExchange, with memory safety provided by FairSpillPool and watermark-based admission control.

## Goals / Non-Goals

**Goals:**
- Single-node survival: 1TB queries on 512MB coordinator via spill-to-disk
- Distributed computation: sort, aggregate, join pushed to workers
- Memory safety: watermark system prevents OOM kills
- Scan optimization: late materialization, file pruning, S3 I/O pipeline
- Trino compatibility: date functions, transaction stubs

**Non-Goals:**
- Hash join spill (blocked on DataFusion upstream)
- Adaptive query execution (AQE) / runtime re-optimization
- GPU acceleration
- Custom shuffle service (reuse Arrow Flight DoExchange)

## Architecture

### Phase A: Single-Node Memory Safety

```
Query → Plan → [Late Materialization Rewrite] → [Scan Planning] → Execute
                                                      │
                                            FairSpillPool (watermarks)
                                                      │
                                              ┌───────┴───────┐
                                              │ Green: normal  │
                                              │ Yellow: warn   │
                                              │ Orange: spill  │
                                              │ Red: admit ctl │
                                              └────────────────┘
```

### Phase B: Distributed Computation

```
Coordinator: Plan → Stage Decomposition → [Stage 1: Scan] → [Shuffle] → [Stage 2: Join/Agg] → [Final]
                                               │                              │
                                          Workers (scan)              Workers (compute)
                                               │                              │
                                          DoExchange ──────────────── DoExchange
```

## Key Components

### FairSpillPool + Watermarks
- `memory_limit` configures total pool size
- Four watermark levels: green (<60%), yellow (60-75%), orange (75-90%), red (>90%)
- Orange forces spillable operators to spill
- Red activates admission control (query queue)

### Late Materialization
- Scan planner splits columns into predicate set and projection set
- Phase 1: read predicate columns, apply RowFilter
- Phase 2: read projection columns for surviving rows only
- Transparent to operators above the scan

### Scan Planning
- File-level min/max pruning from Iceberg manifest statistics
- Sort-order detection from Iceberg metadata (skip re-sort for pre-sorted data)
- PageIndex pruning for Parquet files with column index
- TopK optimization for ORDER BY ... LIMIT N

### S3 I/O Pipeline
- Request coalescing: merge adjacent byte ranges within coalesce_threshold
- Footer cache: LRU cache for Parquet footers (schema, row group metadata)
- Prefetch: background fetch of next row group while processing current

### SortMergeJoin Fallback
- Optimizer rule: rewrite hash join to sort-merge join when estimated build-side > hash_join_memory_threshold
- Both sides sorted via external merge sort (spill-safe)
- Merge with constant memory

### DoExchange Shuffle
- Hash-partition: hash(key) % num_partitions for join and aggregate redistribution
- Range-partition: quantile-based split points for distributed sort
- Bidirectional streaming via Arrow Flight DoExchange RPC

### Distributed Sort
- Sample → range boundaries → range-partition → local sort → k-way merge on coordinator

### Two-Phase Aggregation
- Partial: each worker computes local aggregates
- Shuffle: hash-partition by grouping key
- Final: designated workers merge partial aggregates

### Distributed Joins
- Broadcast: small side replicated to all workers (< broadcast_threshold)
- Shuffle hash: both sides hash-partitioned on join key
- Pre-sorted merge: both sides sorted on join key (from Iceberg metadata)
- Predicate transfer: build-side distinct keys pushed as IN-list to probe-side scan

### Multi-Endpoint Flight SQL
- get_flight_info returns multiple FlightEndpoint entries
- Each endpoint points to a worker holding a partition of the result
- Client fetches directly from workers, bypassing coordinator NIC

### Stage Decomposition
- Physical plan split at shuffle boundaries
- Each stage assigned to a set of workers
- Coordinator orchestrates stage transitions

## Configuration

```toml
[coordinator]
memory_limit = "512MB"
spill_dir = "/tmp/sqe-spill"
spill_compression = "zstd"

[worker]
memory_limit = "8GB"
spill_dir = "/tmp/sqe-spill"
spill_compression = "zstd"

[optimizer]
hash_join_memory_threshold = "256MB"
broadcast_threshold = "10MB"

[storage]
coalesce_threshold = "1MB"
footer_cache_size = 256
```

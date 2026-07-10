# Sizing and capacity

This page is about how to reason about provisioning, not a table of numbers to copy. The right memory and worker counts depend on your data, your query shapes, and your concurrency. The principles below tell you what to watch and which knobs move it. Measure against your own workload before committing to fixed limits.

## What drives memory

A query's memory cost is dominated by the operators that hold state, not by the bytes scanned. Scan is streaming. The pressure comes from:

- **Join build sides.** A hash join builds a table from one side in memory. A large build side is the most common way to run out of memory. SQE rewrites a hash join to a sort-merge join when the estimated build side exceeds `hash_join_memory_threshold`, trading speed for survival.
- **Sorts.** `ORDER BY` over a large input needs the input in memory or spilled. SQE's external merge sort spills sorted runs to disk and merges them with constant overhead.
- **Aggregations.** A high-cardinality `GROUP BY` holds one entry per group. This is the documented hard edge: hash-aggregate spill is limited by upstream DataFusion, so a single node can OOM on a query like TPC-H q18 that produces millions of groups. Distribution is the fix.
- **Result buffering.** Large result sets held before streaming to the client.
- **Write buffers.** Copy-on-Write MERGE and per-file rewrites for UPDATE, DELETE, and Merge-on-Read buffer rows before committing. These register against the pool, so an oversized write now fails with `ResourceExhausted` instead of OOM-killing the coordinator. See [Write Path, Memory Safety](../features/write-path.md#memory-safety).

Scan volume drives I/O and parallelism, not steady-state memory. The scan optimizations (file and page pruning, late materialization, the S3 I/O pipeline) reduce bytes read; they do not change the fact that the join, sort, and aggregate operators are where memory goes. See [Streaming Execution](../architecture/streaming-execution.md) for the full operator-by-operator behaviour.

## Coordinator vs worker

In single-node mode the coordinator does everything: scan, join, sort, aggregate, final assembly. Its memory must cover the heaviest single query you run.

In distributed mode the split changes the shape of the pressure. Workers do the scans, partial aggregations, partial sorts, and join probes. The coordinator handles final aggregation, the final sort or limit, and result assembly. A distributed `ORDER BY` on a large dataset spreads the spill across workers instead of landing all of it on the coordinator. Two-phase aggregation spreads a high-cardinality `GROUP BY` across workers so no single process holds every group.

Provision the coordinator for planning, scheduling, and final-stage work; provision workers for the bulk of the scan and the join and sort state. The documented defaults illustrate the asymmetry: the streaming-execution reference uses an `8GB` runtime `memory_limit` default for both roles, while the Helm chart ships a smaller coordinator limit (`2Gi`) and a larger worker limit (`8Gi`) with workers disabled by default. Both are documented starting points, not targets. Pick yours by measuring.

## The single-node cutoff

Single-node mode is the default and the recommendation for development and datasets under roughly 100GB. Beyond that, enable workers. The cutoff is a guideline: spill-to-disk lets a memory-constrained coordinator survive queries far larger than its memory, so the real ceiling depends on query shape and spill budget. A scan-and-filter over a large table behaves very differently from a high-cardinality aggregation over the same data. See [System Overview, Single-node vs distributed](../architecture/overview.md#single-node-vs-distributed).

## How adding workers helps, and the caveat

Workers are stateless and scale horizontally. Adding workers buys you parallel scan I/O, distributed join and sort memory, and the two-phase aggregation that turns single-node OOM cases into passing queries. A worker loss costs the fragments it was running, not the cluster. Scale by raising the worker replica count.

The caveat is the coordinator. It is a single replica and a single point of failure. Adding workers does not add coordinator redundancy. The coordinator still plans, schedules, and assembles the final stage, and a coordinator restart drops in-flight queries and invalidates sessions. Size and protect the coordinator with that in mind. See [The coordinator is a single point of failure](kubernetes.md#the-coordinator-is-a-single-point-of-failure) and [Limitations](../reference/limitations.md#availability).

## The knobs

The provisioning levers, all in [Configuration](configuration.md) and detailed in [Streaming Execution, Configuration Reference](../architecture/streaming-execution.md#configuration-reference):

| Knob | Section | What it controls |
|---|---|---|
| `memory_limit` | `[coordinator]` / `[worker]` | Total DataFusion runtime memory for the role. The primary lever. |
| `spill_to_disk` | `[worker]` | Allow large sorts and joins to spill rather than OOM. |
| `spill_dir` | `[coordinator]` / `[worker]` | Where spill files land. Size and speed of this disk matter under heavy spill. |
| `spill_compression` | `[coordinator]` / `[worker]` | `zstd`, `lz4`, or `none`. Trades CPU for spill I/O. |
| `hash_join_memory_threshold` | `[optimizer]` | Build-side size above which a hash join becomes a sort-merge join. |
| `broadcast_threshold` | `[optimizer]` | Join side size below which a broadcast join is used (no shuffle). |
| `max_query_memory` | `[query]` | Per-query memory cap, independent of the runtime pool. |
| `max_concurrent_queries` | `[query]` | Concurrency limit. More concurrency means each query gets a smaller slice of the pool. |
| `distribution_threshold` | `[query]` | Minimum scan size before a query is distributed to workers. |

## How to provision in practice

1. Start from a documented default (`8GB` runtime, or the Helm `2Gi` coordinator / `8Gi` worker) and the single-node mode if your data is under roughly 100GB.
2. Run your real queries. Watch for spill activity and `ResourceExhausted` errors. The memory watermark metrics (green / yellow / orange / red) show when the pool is under pressure.
3. If a single node spills heavily or OOMs on aggregation, enable workers and distribute rather than chasing a bigger single box.
4. Raise `memory_limit` and tune the optimizer thresholds against the operators that actually dominate your workload, not against scan volume.
5. Keep the coordinator a single replica, protect it, and size it for planning and final-stage work plus your concurrency.

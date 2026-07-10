## Context

`IcebergScanExec` runs serially because `target_partitions` defaults to 1 (`crates/sqe-catalog/src/iceberg_scan.rs:75`) and the table provider refuses to auto-wire it (`crates/sqe-catalog/src/table_provider.rs:229-246`). The refusal is documented and correct as written:

> Setting it to `state.config_options().execution.target_partitions` causes `IcebergScanExec` to advertise `Partitioning::UnknownPartitioning(N)`, which is the worst possible signal for DataFusion's EnforceDistribution rule -- it cannot promote the downstream HashJoin to `Partitioned` mode ... so the planner falls back to `CollectLeft` and inserts `CoalescePartitionsExec` immediately above the scan to gather the N streams back into 1. ... tpcds q72 SF1 regressed 5-6x (~17s -> ~100s) until the wiring was removed; see issue #131.

So the lesson is not "do not parallelize". It is "do not announce parallelism the optimizer cannot use". `UnknownPartitioning(N)` tells `EnforceDistribution` that the N streams have no useful structure, so to satisfy the join's distribution requirement it gathers them (`CoalescePartitionsExec`) and rebuilds, paying the parallel I/O cost and then throwing the parallelism away.

The recommendation: emit partitioning the optimizer can consume, and place repartitions explicitly. The `with_target_partitions` setter is already retained for exactly this follow-up (`crates/sqe-catalog/src/table_provider.rs:242-245`).

## Goals / Non-Goals

**Goals:**
- Parallelize single-node scans across cores without triggering a redundant gather + rebuild.
- Speed up scan-bound queries on multi-core coordinators.
- Never regress q72 (or any join-heavy query) past the gate threshold.

**Non-Goals:**
- The distributed path. It splits files into `ScanTask`s itself and does not read `target_partitions`.
- A general DataFusion exchange rewrite. The change targets the scan-to-consumer boundary only.
- Auto-tuning the partition count. N comes from `target_partitions` / core count; adaptive sizing is a follow-up.

## Architecture

### Why UnknownPartitioning regressed q72

```
   target_partitions = N, scan advertises UnknownPartitioning(N)

        HashJoinExec  (needs HashPartitioning on join key)
              │
        EnforceDistribution sees UnknownPartitioning(N): unusable
              │  -> falls back to CollectLeft
              v
        CoalescePartitionsExec   (gather N streams back to 1)
              │
        IcebergScanExec(N)       (parallel I/O ... then serialized)

   result: parallel read, immediate coalesce, single-threaded hash build,
   batches fragmented into tiny round-robin chunks. q72: ~17s -> ~100s.
```

### The fix: announce usable partitioning

The partitioning a scan should advertise depends on what consumes it:

| Consumer above the scan | Partitioning to emit | Effect on EnforceDistribution |
|---|---|---|
| Hash join (scan is a join input) | `HashPartitioning(join_key, N)` | Join promotes to `Partitioned`; no `CollectLeft`, no coalesce |
| Hash aggregate (`GROUP BY`) | `HashPartitioning(group_key, N)` | Final aggregate stays partitioned; no gather before partial |
| Filter / projection / pipeline-only | `RoundRobinBatch(N)` | No distribution requirement; no exchange inserted |
| Order by / global sort | single partition or range, per the sort | Avoids a coalesce the sort would force anyway |

The partition assignment of files to streams must agree with the announced partitioning. For `HashPartitioning(key, N)` the scan cannot simply round-robin files; the data within a file is not hash-clustered on the key. Two correct options:

1. **Scan round-robin + explicit `RepartitionExec(Hash, N)`.** The scan emits `RoundRobinBatch(N)` for parallel I/O, then a deliberately placed `RepartitionExec(Hash(key), N)` produces the hash distribution the join needs. The repartition is the one exchange we pay for on purpose, instead of the optimizer's `CoalescePartitionsExec` + rebuild. This is the safe default.
2. **Doris-style local shuffle.** A lightweight in-process exchange that hash-routes batches between scan threads and join-build threads without a full `RepartitionExec`. Lower overhead, more code. Deferred behind the same flag as a phase-2 optimization once option 1 proves the gate.

For pipeline-only consumers (filter, projection, no join/aggregate), `RoundRobinBatch(N)` straight off the scan needs no exchange at all and is the cheapest win.

### Placement is explicit, not inferred

The planner pass walks the physical plan, finds each `IcebergScanExec`, inspects its parent operator, and:
- If the parent is a hash join / hash aggregate: set scan partitioning to `RoundRobinBatch(N)` and insert `RepartitionExec(Hash(key), N)` between scan and parent (option 1), so `EnforceDistribution` sees its requirement already satisfied and inserts nothing.
- If the parent has no distribution requirement: set scan partitioning to `RoundRobinBatch(N)`; no insertion.
- Otherwise (sort, single-row, etc.): leave `target_partitions = 1`.

Doing the placement ourselves is the whole point. `EnforceDistribution` inserts wasteful exchanges only when the partitioning it sees does not match the requirement. If we hand it a plan that already satisfies the requirement, it leaves it alone.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Approach | Emit usable partitioning + explicit `RepartitionExec` | The regression was wrong-signal, not parallelism; give the optimizer something it can use |
| Hash-fed scans | RoundRobin scan + explicit `RepartitionExec(Hash)` | Correct hash distribution; one intentional exchange instead of the optimizer's gather + rebuild |
| Pipeline-fed scans | `RoundRobinBatch(N)` off the scan, no exchange | Cheapest parallelism; nothing to gather |
| Local shuffle | Deferred behind the flag | Lower overhead but more code; prove the gate with `RepartitionExec` first |
| Default | `execution.parallel_scan = false` | Identical to today until q72 gate is green |
| Distributed path | Untouched | It splits files itself; does not read `target_partitions` |

## Risks

| Risk | Mitigation |
|---|---|
| q72-style regression returns | Hard benchmark gate against `compare-tpcds-sf1-2026-05-28T14:19:18.json` (q72 = 756ms); flag stays off until <= 1.1x; assert no `CoalescePartitionsExec` above the scan in the q72 plan |
| Explicit `RepartitionExec` adds overhead on small scans | Only parallelize above a file-count / byte threshold (reuse the distribution thresholds); below it, keep `target_partitions = 1` |
| Wrong key chosen for hash repartition | Derive the key from the join / aggregate node directly; fall back to round-robin (pipeline case) when no key is recoverable |
| Plan-shape assumptions break on complex trees | Pass is conservative: any unrecognized parent leaves the scan serial; correctness never depends on parallelizing |
| Interaction with sort-order trust / runtime filters | Keep existing scan options; the partitioning pass runs after, and disables itself when a single-partition ordering is required |

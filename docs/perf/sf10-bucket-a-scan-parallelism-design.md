# SF10 Bucket A: scan parallelism design

Bucket A (single-partition fact-table scan / decode) is 87 percent of the SF10
gap against Trino: 114s of the 131s SQE loses across the loser set, spread over
27 queries, with TPC-DS q72 alone worth 37s. See `sf10-loser-aggregation.md` for
the per-query evidence. This doc proposes the fix, grounded in DataFusion's own
scan, Trino, and the morsel-driven parallelism literature.

## Root cause (verified, not assumed)

SQE's `IcebergScanExec` advertises `Partitioning::UnknownPartitioning(1)`
(`DEFAULT_TARGET_PARTITIONS = 1`). One output partition means:

1. iceberg-rust's `to_arrow()` decodes up to `num_cpus` data files concurrently
   (its `concurrency_limit_data_files` default), but every decoded batch funnels
   into **one** merged stream.
2. That one stream is polled by one thread. The Tier-2 dynamic-filter wrapper
   (per-batch predicate eval) runs on that same thread, so for a selective scan
   like q50 the engine decodes ~28M store_returns rows and filters them down to
   34k **single-threaded** (its `IcebergScanExec` shows 8.5s of elapsed_compute to
   emit 34k rows).
3. Downstream parallelism only begins after a `RepartitionExec(RoundRobinBatch(8))`
   pulls from that single stream and redistributes. The round-robin can only pull
   as fast as one consumer.

Trino runs the same scan as a split-level SOURCE pipeline: many splits decode in
parallel across `task.concurrency` driver threads, each driver running its own
filter, and the rows fan out before any single thread is the bottleneck.

### Why the obvious knobs do not fix it

- **Raising `small_file_threshold_mb` (route files to the `buffer_unordered`
  direct path): measured, no effect.** q50, q72, and SSB q2.2 were unchanged at
  16MB. SF10 fact files are either larger than the threshold (SSB lineorder is ~4
  files of ~150MB) or the direct path is not the lever. The cap is the single
  output partition, not which reader path runs.
- **Decode concurrency is already `num_cpus`.** iceberg-rust decodes 8 files at
  once; #131 further splits a single >32MB file into row-group subtasks. Decode is
  not the serial part. The merge to one stream is.

### Why the previous attempt regressed (the q72 trap)

`IcebergScanExec` has a `with_target_partitions(n)` setter that statically
advertises `UnknownPartitioning(n)`. Wiring it to `execution.target_partitions`
regressed TPC-DS q72 from ~17s to ~100s (issue #131, `table_provider.rs` comment).
Mechanism: `UnknownPartitioning(N)` is set **before** join-mode selection.
DataFusion's planner then cannot promote the downstream hash join to `Partitioned`
mode (which needs `HashPartitioning`), so for a build side under the 64MB
`hash_join_single_partition_threshold` it picks `CollectLeft`, and
`EnforceDistribution` inserts a `CoalescePartitionsExec` to gather the N scan
streams back into 1 for the single-node build. Net: parallel I/O, then immediate
serialization, then a single-threaded hash build fragmented into tiny batches.

**The real gap: `IcebergScanExec` does not implement `ExecutionPlan::repartitioned()`.**
It uses the trait default, which returns `None`. So the optimizer can never split
the scan on its own terms; the only lever was the all-or-nothing static setter
that fights the planner.

## Prior art

**DataFusion's own `DataSourceExec` / `FileScanConfig` is the template.** It also
advertises `UnknownPartitioning(N)` (N = number of file groups), yet it feeds
Partitioned hash joins without the regression. The difference is the
`repartitioned()` hook: `EnforceDistribution` calls `ExecutionPlan::repartitioned()`
on scan nodes when `datafusion.optimizer.repartition_file_scans` is set, and it
does so **contextually, after the join mode is chosen**. A scan feeding a probe
side gets split into N partitions; a scan feeding a `CollectLeft` build is left at
1 partition (no coalesce). The optimizer pulls parallelism where it helps instead
of having it forced from below. SQE reimplemented the executor but skipped this
hook, which is the whole bug.

**Trino** schedules `max-split-size`-bounded splits (~64MB) across `task.concurrency`
driver threads per node. Parallelism = min(splits, drivers). The unit of
parallel work is the split, not the file or the partition.

**Morsel-driven parallelism (Leis et al., SIGMOD 2014).** Break scan work into
small fragments (morsels), dispatch them to a work-stealing thread pool, vary the
degree of parallelism elastically at runtime. Reports ~30x speedup on 32 cores for
TPC-H and SSB. The principle SQE wants: the scan emits independent units of work
that the runtime spreads across cores, rather than one stream a single thread
drains.

## Solution

Make `IcebergScanExec` a first-class multi-partition source the DataFusion way,
then let the optimizer drive it.

### Phase 1: implement `repartitioned()` (most of the win, low planner risk)

1. **Implement `ExecutionPlan::repartitioned(target_partitions, config)`** on
   `IcebergScanExec` to return a clone advertising `UnknownPartitioning(target_partitions)`
   whose `execute(partition)` reads partition `i`'s slice of the work list. The
   slicing already exists (`with_target_partitions` + the `execute(partition)`
   round-robin over planned files); `repartitioned()` just exposes it to the
   optimizer instead of forcing it from the provider.
2. **Enable `datafusion.optimizer.repartition_file_scans = true`** in the session
   config (`session_context.rs`), alongside the existing pushdown flags.
3. **Remove the static `target_partitions` default of 1 from the provider path.**
   Keep `DEFAULT_TARGET_PARTITIONS = 1` as the un-repartitioned base; the optimizer
   raises it via `repartitioned()` only where beneficial.
4. **Split by size / row-group, not by file count.** A round-robin over the file
   list makes one 150MB SSB lineorder file the straggler partition. Assign work in
   `task_split_target_size`-bounded morsels (the #131 byte-range subtasks already
   exist) so a 150MB file contributes ~5 morsels spread across partitions, and a
   table of many ~10MB files balances by bytes. This is the morsel principle and
   matches Trino's split sizing.

Why this is low-risk: `repartitioned()` is only invoked by `EnforceDistribution`
where the plan benefits, after join-mode selection. A `CollectLeft` build is left
single-partition, so the q72 coalesce regression cannot recur by construction. The
Tier-2 dynamic-filter wrapper and its per-scan snapshot cache already run inside
`execute(partition)`, so each partition filters independently and in parallel.

Expected to recover the probe-side-scan majority of Bucket A: SSB (all 10 losers),
q50, q09, q08, q10, q12, q17, q24, q64 (where the fact scan is the probe side).

### Phase 2: Partitioned join for large builds (the q72 class)

q72 and a few TPC-DS losers join two large fact-derived inputs. Even with parallel
scans, if the planner picks `CollectLeft` (build under 64MB) the build serializes.
The lever is the partition-mode decision: ensure large-build joins choose
`Partitioned` mode so `EnforceDistribution`'s `add_hash_on_top` hash-repartitions
both sides across cores. Options: tune `hash_join_single_partition_threshold` with
real statistics, or rely on SQE's own distributed shuffle-hash join
(`ShuffleHashJoinPlan`) which already partitions by key. This overlaps with
distributed execution and is a separate, smaller change after Phase 1.

## Validation

EXPLAIN-driven, on the clean rig (data already loaded):

1. After Phase 1, `EXPLAIN ANALYZE` a probe-side loser (q50) and confirm the scan
   shows N partitions and no `CoalescePartitionsExec` above it.
2. `EXPLAIN ANALYZE` q72 and confirm Phase 1 alone did **not** reintroduce the
   CollectLeft+coalesce (it should not, by construction); confirm Phase 2 makes its
   big join `Partitioned`.
3. Re-run the loser-set compare and re-aggregate seconds lost. Target: collapse the
   114s Bucket A toward parity, leaving only genuinely distribution-bound joins.

## Risk and scope

- The dynamic-filter snapshot cache (!371) is per-`execute(partition)`, so it is
  already partition-safe.
- Sort-order equivalence: a multi-partition scan must not claim a global sort it no
  longer provides. `with_trust_sort_order` rebuilds `EquivalenceProperties`; verify
  N-partition scans drop any cross-partition ordering guarantee so the optimizer
  inserts `SortPreservingMerge` only when truly needed.
- This is a design. Phase 1 is one focused change (`repartitioned()` +
  config flag + morsel-balanced slicing) and is independently shippable and
  measurable before Phase 2.

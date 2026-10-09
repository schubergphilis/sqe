# Adaptive join fallback: replan as sort-merge when a hash build does not fit (#784)

Status: design, no code yet. Tracking issue #784 (P2, under the #814 ordering).

## Problem

`JoinStrategyRule` rewrites a hash join to a sort-merge join only when the
build-side size is `Precision::Exact` and above 2 GB. Iceberg statistics are
usually `Inexact` or `Absent`, so large uncertain builds stay `HashJoinExec`.

The 2026-10-05 SF10 measurements (on #784) showed that an inexact estimate cannot
drive this decision:

- The estimate counts column bytes, and a hash table costs 2-4x that. q09 orders
  was estimated at 180 MB and built at 546 MB.
- Filters the planner cannot see shrink builds by orders of magnitude. q72
  catalog_sales was estimated at 404 MB and built at 2.6 MB because of runtime
  filters. The q18 HAVING semi-join input was estimated at 288 MB and built at
  2.1 MB.

At SF100 against a 2 GB threshold, an estimate-driven rule would move q72 to
sort-merge (real build about 26 MB) and keep q09 hashed (real build about
5.5 GB). Both choices are wrong.

The build side of DataFusion 55.1's `HashJoinExec` cannot spill.
`collect_left_input` calls `reservation.try_grow` for each batch and returns
`ResourcesExhausted`. A build that does not fit the per-query cap
(`query.max_query_memory`) therefore ends the query with a clean resource
error. It is never an OOM, and the server keeps serving, but the query does not
complete. q09 at a 1 GB cap is the reference case.

The decision has to be made at run time, on what the build actually costs.
Ballista 55 does this: it picks broadcast or partitioned after the build side
is materialized, using AQE with a 128 MB threshold.

## Facts this design rests on (DataFusion 55.1, verified 2026-10-09)

| Fact | Where |
|---|---|
| With `optimizer.prefer_hash_join = false`, `target_partitions > 1` and `repartition_joins = true`, an equi-join is planned as `SortMergeJoinExec`. | `datafusion-55.1.0/src/physical_planner.rs` ~1702 |
| Null-aware anti joins (`NOT IN` over a nullable subquery) always stay `HashJoinExec` in `CollectLeft` mode. `SortMergeJoinExec` does not implement null-aware semantics. | same, ~1705 |
| Joins with no equi-keys stay nested-loop / cross / piecewise-merge. They are not affected. | same, ~1573-1700 |
| `SortMergeJoinExec` accepts every join type, including `LeftMark` / `RightMark`. Decorrelated `EXISTS` / `IN` subqueries can therefore switch too. | `joins/sort_merge_join/exec.rs` 217-240 |
| `SortMergeJoinExec` spills its buffered side through a `SpillManager`. | `sort_merge_join/materializing_stream.rs`, `bitwise_stream.rs` |
| `ExternalSorter` spills (`with_can_spill(true)`). Its merge reservation `ExternalSorterMerge[..]` does not, so the merge needs room that is not reserved for spillable operators. | `sorts/sort.rs` 286-292 |
| SQE already keeps that room: `coordinator.unspillable_headroom_percent` (default 20) holds back a share of each per-query cap from spillable consumers (#788). | `sqe-core/src/config/mod.rs`, `memory_headroom.rs` |

The last two rows together mean a sort-merge plan has a bounded floor: sorts
spill, the SMJ buffered side spills, and the merge draws on the headroom. The
floor is not zero. It is measured below rather than assumed.

## Options

### A. Coordinator retry: replan the whole query with `prefer_hash_join = false` (recommended)

When the execution stream fails with `ResourcesExhausted` before it has
produced its first output batch, the coordinator drops the failed plan, builds
a new physical plan from the same policy-enforced `LogicalPlan` with
`prefer_hash_join = false`, and executes that instead.

- **Where the decision sits:** in the coordinator, around the execution
  stream, in both `execute_query` and `execute_stream_inner`.
- **Replaying consumed build input:** nothing is replayed. The retry rescans.
  Any work done before the failure is wasted. Because the trigger fires only
  before the first output batch, the client has seen nothing, so the
  replacement is invisible apart from latency.
- **Dynamic and runtime filters:** they are rebuilt by the new plan. The
  physical optimizer runs again on the new plan (`apply_post_planning_rules`,
  `try_distribute`), so every filter, pushdown and distribution decision
  matches the plan that actually runs. Nothing has to be carried across from
  the failed plan.
- **Partitioning:** DataFusion's own planner picks the join, and only then do
  `EnforceDistribution` and `EnforceSorting` run. They insert the hash
  repartition on the join keys and the per-partition sorts. This avoids the
  defect class behind the !724 regression. There, `JoinStrategyRule` swapped
  in a sort-merge join after `EnforceDistribution`, no exchange was inserted,
  and a `SortExec` read partition 0 only or forced a single-partition
  coalesce. Acceptance criterion 2 ("no unnecessary single-partition
  coalesce") is satisfied by construction and checked by a test.

Cost: the work done before the failure is lost. For q09 at SF10 that is
seconds. At SF100+ it can be most of the hash attempt's runtime, which is
bounded by how long it takes to fill the per-query cap.

### B. Exec wrapper around `HashJoinExec`

A custom `AdaptiveJoinExec` buffers (and spills) the build batches it
consumes. When a reservation fails or the measured size passes a threshold,
it switches to a partitioned sort-merge join over the spilled build plus the
rest of the build stream and the probe stream.

- It must replay the consumed build from its own spill files.
- It must repartition both inputs itself, inside one operator, because
  `EnforceDistribution` has already run. That is exactly the !724 failure
  mode, re-implemented by hand.
- Dynamic filters pushed from this join into the probe scan have already been
  published from a partial build. The wrapper has to keep them conservative
  (no pruning until the build completes) or retract them. DataFusion has no
  retraction API.
- Only the failing join switches. Other joins in the plan keep their hash
  builds and their broadcasts. This is the advantage over A.

B is the right shape for a mature AQE. It is several times the code of A and
carries the !724 risk. It becomes worth building once measurements under A
show the wasted work dominating: a retry that typically costs more than, say,
30% extra wall time on the queries that fall back.

### C. Measured-size threshold (Ballista-style AQE)

This is not a separate mechanism. It is B with a size trigger instead of a
failure trigger. It is deferred with B.

### Recommendation

A, as phase 1. B is deferred until A's wasted-work metric justifies it.

## Phase 1 specification (option A)

### Trigger

Retry exactly once when all of the following hold:

1. The statement kind is `Query`. CTAS, `INSERT ... SELECT`, `MERGE` and the
   other write paths are out of scope: a failed attempt may have written data
   files, and retrying them needs orphan cleanup. That is deferred.
2. The error is a `DataFusionError::ResourcesExhausted` (after unwrapping
   `Context` / `Shared`, as `query_memory::execution_error` does), raised by
   the execution stream.
3. No output batch has been produced yet, either to the client or to the
   buffered result.
4. The physical plan contains at least one equi-join `HashJoinExec` that the
   replan would change, meaning one that is not null-aware.
5. `coordinator.spill_to_disk` is true. Without spill, a sort-merge plan
   cannot get under the cap any better than the hash plan.
6. The new setting `query.join_fallback` is on (default on; off restores
   today's behaviour).

The trigger does **not** inspect the memory consumer named in the error. At a
cap, whichever consumer asks last is refused. At 1 GB, q09 was refused at
scan-decode admission while hash builds held about 85% of the pool. Matching
on `HashJoinInput[..]` would miss the very case this design targets.

Result-retention refusals (`BufferedResultBudget::retain`,
`max_result_rows`) never trigger a retry. Condition 3 already excludes them:
they fire only after batches exist. A result that does not fit does not fit
under any join strategy.

### Mechanism

1. Keep the policy-enforced `LogicalPlan`: the output of `evaluate`, before
   `create_physical_plan`. Policy, time travel and incremental rewrites are
   not run again.
2. On trigger, drop the failed stream and plan first, so that every
   reservation is released. Then build a fresh per-query pool with
   `with_query_limit`.
3. Derive the session state with `prefer_hash_join = false`
   (`SessionStateBuilder::new_from_existing(..)` with the changed config). The
   query's catalog, UDFs and policy context are unchanged.
4. Run `create_physical_plan` and then `finish_physical_plan` again.
5. Execute it. A second `ResourcesExhausted` is returned to the client as
   today, with a message that names both attempts ("hash plan exceeded
   query.max_query_memory; sort-merge fallback also exceeded it: ..."), so
   the user knows that raising the cap is the remaining lever.

Streaming path: `execute_stream_inner` returns the stream before execution
starts. A small adapter stream holds the replan closure. If the inner stream
yields `Err(ResourcesExhausted)` before any `Ok(batch)`, the adapter runs the
replan and continues from the new stream. Errors after the first batch pass
through unchanged. Admission permits and the per-user reservation stay with
the adapter, as they do with the current wrapper.

Distributed queries: workers execute only scans, and every join runs on the
coordinator. The replan re-runs `try_distribute`, so the worker scan tasks
are rebuilt and dispatched again. Worker-side state from the failed attempt
is cancelled in the same way as a client cancel.

### What the fallback plan gives up

The fallback is meant to be complete, not fast:

- Small dimension joins that were `CollectLeft` broadcasts become partitioned
  sort-merge joins too. The setting is plan-wide.
- Hash-join dynamic filters and bloom-probe pruning (#369) do not exist for
  sort-merge joins, so probe-side scans read more.
- Every equi-join input is sorted.

A per-join fallback that keeps the small broadcasts would need either B or a
physical rewrite of individual joins, which brings back the !724 risk. It is
deferred.

### Observability

- One query id across both attempts.
- Audit: the query's audit record gains `join_fallback: true` and the first
  attempt's error text.
- Metric: `sqe_join_fallback_total{outcome="completed"|"failed"}` and a
  histogram of time wasted on the failed attempt (the input for the B
  decision).
- `EXPLAIN ANALYZE` reports the plan that actually ran, plus a line saying
  that the hash plan was abandoned.
- A `warn!` log line with the query id, the cap, and the reserved bytes at
  failure.

## Tests and acceptance

Mapped to the #784 acceptance criteria:

1. **Exact / Inexact / Absent under a constrained cap.**
   - Unit: an in-memory `MemTable` join with stats forced to each precision,
     a per-query cap below the build size, and spill enabled. Assert the query
     completes, the result equals the uncapped run, and the fallback counter
     moved.
   - Assert that with `spill_to_disk = false` or `query.join_fallback = false`
     the query returns the original clean resource error.
   - Assert that a null-aware `NOT IN` plan does not retry, because nothing
     would change.
2. **No unnecessary single-partition coalesce.** Plan-shape test on the
   fallback plan for a TPC-H q09-shaped join. Every `SortMergeJoinExec` input
   is hash-partitioned on its join keys, with no `CoalescePartitionsExec` or
   `SortPreservingMergeExec` between the join and its partitioned input.
3. **SF1 regressions, then SF10/SF30 scaling with memory and spill metrics.**
   - SF1 all suites at the default cap: the fallback must never fire, and
     timings are unchanged (same compare recipe as 2026-09-28).
   - SF10 q09 at 1 GB, using the 10-05 recipe
     (`RUST_LOG=sqe_planner::join_strategy=debug`). Hypothesis, not yet a
     fact: q09 completes under sort-merge at 1 GB. Record wall time against
     16 GB, spill bytes, and wasted-attempt time.
   - Lower the cap until the fallback also fails, to find the sort-merge
     floor for q09. Repeat at SF30.
   - Integration: one `make test-integration` case that runs a two-table join
     under a small `query.max_query_memory` and asserts completion.

## Rollout

- Behind `query.join_fallback` (default on). The trigger only fires on
  queries that fail today, so a query that succeeds today runs the same plan.
- Document the setting and the "complete, not fast" behaviour in the book's
  memory-management page.

## Related

- `2026-06-21-memory-safety-oom-prevention-design.md` lists hash-join build
  spill as existing DataFusion support (sections 2 and 4). That is not true in
  DataFusion 55.1. This design is the substitute until upstream gains a
  spilling hash join.
- #788 (headroom budget): the fallback relies on the unspillable headroom for
  the sort merge.
- #369 (bloom probe): its pruning is lost on the fallback plan.
- #416: SF100 qualification umbrella.

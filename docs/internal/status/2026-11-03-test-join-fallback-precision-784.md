**STATUS 2026-11-03: the join fallback is tested for Exact, Inexact and Absent build estimates and for a join chain (`test/join-fallback-precision-784`, #784).**

Covers #784 criteria (a) and (b) on the runtime fallback path. Test-only:
no runtime code changed.

- `the_fallback_covers_every_build_estimate`: three queries whose first-attempt
  hash join has an Exact (plain `MemTable`), Inexact (behind `WHERE id >= 0`)
  and Absent (`StreamingTable` without statistics) build-side byte estimate.
  The test asserts that precision with the same `StatisticsContext` call
  `JoinStrategyRule` makes. Each one overflows the quota, completes on the
  sort-merge retry with the closed-form answer and releases the pool to 0.
- `a_join_chain_falls_back_without_a_single_partition_input`: a q09-shaped
  two-join chain retries as two `SortMergeJoinExec`, with no `HashJoinExec`
  left.
- `assert_joins_stay_partitioned`: every sort-merge join input reaches a
  `RepartitionExec` with `Hash(_, n > 1)` before any `CoalescePartitionsExec`
  or `SortPreservingMergeExec`. A throwaway probe confirmed the assertion
  fails on the plan the static `JoinStrategyRule` produces (`SortExec` over
  `CoalescePartitionsExec`). The probe was then deleted.

Gate: `cargo test -p sqe-coordinator --lib -- join_fallback` -> 13 passed.
Clippy (`-p sqe-coordinator --all-targets -D warnings`) is clean, and the
rustfmt ratchet added no hunks.

Left open on #784:

- (c) SF10/SF30 scaling runs with the fallback taken.
- **Finding, not changed here:** the static `JoinStrategyRule` (registered
  unconditionally in `post_planning_rules`) still funnels both inputs to one
  partition through `ensure_sorted` -> `CoalescePartitionsExec`. On Iceberg it
  is dormant, because it fires only on an Exact estimate over 2 GB and Iceberg
  scans report Inexact or Absent. Gating it on `query.join_fallback` alone
  would be wrong: `JoinFallback::new` returns `None` without spill files, so
  with `spill_to_disk = false` that would remove the only guard. The fix the
  issue names is partition-preserving planning in `ensure_sorted`: hash
  repartition plus a per-partition sort instead of the coalesce, which flips
  `ensure_sorted_coalesces_multi_partition_input`. That is a `sqe-planner`
  change for its own branch.

**NEXT:** `fix/tpcbb-item-domain-785` (align the tpcbb `num_items` with
`tpcds::item_rows`), then the embedded join-fallback move (#786), the #787
SF10 compare, and the engine-bench 150M rerun and MR.

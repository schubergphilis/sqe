# 404: Three hand-rolled copies of the post-planning physical rule pipeline, run outside the optimizer with default config

- Scope: sqe-coordinator, read side (query_handler.rs execute_query / open_stream, explain.rs)
- Severity: medium
- Category: architecture
- Effort: M (days)
- Status: open
- Issue: #584

## Problem

After `create_physical_plan()` the coordinator applies its own physical rules by hand, in
three places that have already drifted apart:

| Rule | `execute_query` (`query_handler.rs:3512-3585`) | `open_stream` (`query_handler.rs:3139-3268`) | `ExplainHandler::apply_plan_shape_rules` (`explain.rs:48-92`) |
|---|---|---|---|
| StarSchemaReorderRule | yes (`:3512`) | yes (`:3143`) | yes |
| DimBuildSwapRule | yes (`:3532`) | yes (`:3163`) | yes |
| ParallelProbeScanRule | **no** | yes (`:3185`) | yes |
| ParallelScanRule | **no** | yes (`:3211`) | yes |
| JoinStrategyRule | yes (`:3548`) | yes (`:3230`) | **no** |
| adaptive_sort | yes (`:3566`) | yes (`:3246`) | **no** |
| SortMemoryRule | yes (`:3579`) | yes (`:3259`) | **no** |
| try_distribute | yes (`:3589`) | yes (`:3272`) | **no** |
| strip_self_join_dynamic_filters | yes (`:3630`) | yes (`:3279`) | **no** |

Every rule is invoked with `optimize(plan, &ConfigOptions::new())`
(`query_handler.rs:3149, 3234, 3264, 3519, 3554, 3583`, `explain.rs:58`) except the two that
were noticed (`dim_build_swap`, `parallel_*` use `ctx.state().config_options()`), so
session-level settings (`target_partitions`, `batch_size`, `SET` statements) are invisible to
StarSchemaReorder, JoinStrategy and SortMemory.

Every `Err` arm falls back to the previous plan with a `debug!`
(`query_handler.rs:3154-3159, 3168-3172, 3237-3241`, and the same in `execute_query`), which is
the pattern the memory note "JoinStrategyRule SMJ regression" identifies as having hidden a
five-query regression: the regression was in the `Ok` path, but the `Err` arm meant nobody
noticed rule failures either.

Because the rules run after DataFusion's optimizer, `DimBuildSwapRule` has to undo and redo
optimizer work itself: strip pushed-down dynamic filters, rebuild every `HashJoinExec`, insert
`CoalescePartitionsExec`, then re-run `FilterPushdown::new_post_optimization()`
(`crates/sqe-coordinator/src/dim_build_swap.rs:33-45`). `strip_self_join_dynamic_filters`
runs after `try_distribute` has already serialized dynamic filters into the ScanTasks
(`query_handler.rs:3589` then `:3630`), so the worker predicate and the coordinator join can
disagree about which dynamic filters exist.

## Why it matters

- The batch path (`execute`) is what MERGE sources, the Trino compat layer, `INSERT ... SELECT`
  and non-streaming Flight calls run through. It gets neither parallel-scan rule, so the
  same SELECT is single-partition on one entry point and N-partition on another. Perf results
  measured over Flight (`execute_stream`) do not describe what `INSERT ... SELECT` or dbt's
  Trino path runs.
- `EXPLAIN` shows a plan that skips JoinStrategy, SortMemory and distribution, so the tool
  meant to explain regressions cannot show the plan that regressed.
- Each new rule costs three edits and a fourth (EXPLAIN ANALYZE) to keep in sync. The
  divergence above is the evidence that this does not happen.

## Proposed change

1. Implement the coordinator rules as `PhysicalOptimizerRule`s registered on the
   `SessionState` in `session_context.rs` (`SessionStateBuilder::with_physical_optimizer_rule`
   appended after DataFusion's defaults, in the documented order). `try_distribute` becomes a
   rule that takes the `WorkerRegistry` and config by `Arc` (it is already
   async-free apart from `data_file_info()`; pre-fetch that in `IcebergScanExec` or run the
   rule in an async wrapper once). Then `ctx.sql().create_physical_plan()` yields the executed
   plan and `EXPLAIN` is correct for free.
2. Until then, extract one `fn apply_post_planning_rules(&self, ctx, plan) -> Result<plan>` in
   `query_handler.rs`, call it from all three sites, pass `ctx.state().config_options()`, and
   turn rule failure into `Err` (or a `warn!` + metric `plan_rule_failures{rule}`), not `debug!`.
3. Order `strip_self_join_dynamic_filters` before `try_distribute`.

## Acceptance criteria

- One function (or the SessionState rule list) is the only place that names the rule sequence;
  `grep -n 'StarSchemaReorderRule::new' crates/sqe-coordinator/src` returns one hit.
- `EXPLAIN SELECT ...` on the two-worker rig prints `DistributedScanExec`.
- A test plans the same SQL via `execute` and `execute_stream` and asserts identical
  `displayable(plan).indent(true)` output.
- Rule failure increments a metric and is visible at `warn!`.

## Related

- 2026-07-25 review P0-6 (plan-shape cliffs) and P1-1 (single-output-partition scans) still
  hold; this is the structural reason fixes to them land unevenly.
- Memory notes "JoinStrategyRule SMJ regression", "Plan-node width substitution",
  "Parallel-scan findings 2026-07-08" (ParallelScanRule never fired due to ordering).

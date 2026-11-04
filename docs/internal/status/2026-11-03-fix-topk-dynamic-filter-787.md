**STATUS 2026-11-03: TopK and rebuilt-join dynamic filters reach the Iceberg scan again (`fix/topk-dynamic-filter-787`, part of #787).**

Root cause: `ParallelScanRule` / `ParallelProbeScanRule` re-run
`EnsureRequirements`, which rebuilds the TopK `SortExec` (`with_fetch` makes a
new `DynamicFilterPhysicalExpr`). The scan kept the old filter. Nothing updated
it (generation 1 forever), so it never pruned, and the stream-open wait spent
its full 100 ms per partition on it. `StarSchemaReorderRule` (on by default)
and `StripSelfJoinDynamicFiltersRule` orphan join filters the same way, since
`HashJoinExec::try_new` drops the filter.

Fix (`crates/sqe-catalog/src/dynamic_filter_wiring.rs`):

- `settle_dynamic_filters` drops every scan filter whose `expression_id` no
  plan node produces (`dynamic_expressions_produced`). If it dropped one, it
  re-runs `FilterPushdown::new_post_optimization()`, so the rebuilt sort or
  join pushes a live filter, then settles once more.
- Filters from single-child producers (TopK, aggregate) are marked
  progressive on `IcebergScanExec`. The scan does not wait for them. The
  direct-read path hands the live filter to the Parquet row filter (re-read
  per batch). The Tier-2 wrapper bypasses its sealed-snapshot cache for them.
- `handle_child_pushdown_result` dedups by `expression_id`, because the re-run
  re-pushes filters the scan already holds.
- Wired as `SettleDynamicFiltersRule` in the coordinator post-planning
  pipeline (after `SortMemoryRule`, before the self-join strip, which now
  settles its own output) and after `ParallelScanRule` in embedded mode.

Leak check: TopK and aggregate filters now reach a scan below a row filter.
Every scan-side evaluation treats an error as "keep the rows", so a failing
bound cannot reveal hidden rows. Documented on `plan_has_fallible_predicate`
in `sqe-policy/src/leakproof.rs`.

Gate: `cargo test -p sqe-catalog --lib` (758 passed, 4 new in
`dynamic_filter_wiring::tests`, 2 in `physical_to_predicate::tests`); `RUST_MIN_STACK=33554432 cargo test -p
sqe-coordinator --lib -- post_planning dim_build_swap parallel` (15 passed);
clippy `-D warnings` on sqe-catalog, sqe-coordinator, sqe-cli and sqe-policy:
clean.

Second cause, in the predicate conversion
(`crates/sqe-catalog/src/physical_to_predicate.rs`): Tier-2 scans turn the
dynamic filters into an Iceberg predicate per task for row-group pruning. The
default DESC (NULLS FIRST) TopK bound is `x IS NULL OR x > t`. `IsNullExpr`
did not convert, so the whole OR was dropped and nothing was pruned. It now
converts `IS [NOT] NULL` (null counts are exact).

NaN hazard, found while measuring: Parquet min/max statistics exclude NaN
(checked: a row group with NaN and 5.0 reports max 5.0). The vendored
`row_group_metrics_evaluator::visit_inequality` returns CANT_MATCH when
max <= datum. DataFusion orders +NaN above every number, so `x > t` pruned on
a float key could drop the NaN rows that belong in the result. Float range
comparisons (`<`, `<=`, `>`, `>=` against a `Float32` / `Float64` literal)
therefore never convert. That also closes the same hole for float join
bounds, which existed before this branch. Equality still converts.

Live, 150M-row `bench_150m.sales` on Polaris (engine-bench), release build,
`SELECT order_id, <key> FROM sales ORDER BY <key> DESC|ASC LIMIT 10`:

| | before | after |
|---|---|---|
| scan `filter_wait_time` (embedded) | 837-851 ms | 8 ns |
| scan rows out to the TopK (`price`) | 150.0 M | 90.1 K |
| `price` (DOUBLE): rows decoded | 150.0 M | 150.0 M |
| `price` wall, embedded / server | 2.6-2.7 s / 2.6 s | 2.6-3.0 s / 2.6-2.8 s |
| `customer_id` (BIGINT) DESC: rows decoded / embedded wall | 150.0 M / 3.67-3.85 s | 59-60 M / 2.69-2.76 s |
| `customer_id` (BIGINT) ASC: rows decoded / embedded wall | 150.0 M / 3.75-3.89 s | 60-62 M / 2.34-2.74 s |

The `price` "before" is the release build from earlier the same day, without
this change. The `customer_id` "before" is the same branch binary with
`enable_topk_dynamic_filter_pushdown = false`. Wall times include CLI start,
ATTACH and server start (3 runs each). A float sort key gets the removed wait
and a smaller TopK input, but no decode savings, because its bound no longer
reaches row-group pruning. An earlier build of this branch showed `price ASC`
at 1.2 s on the server; that came from the unsound float pruning and is gone.

Correctness: ten TopK shapes (`price` ASC, DESC, `NULLS FIRST`, with `WHERE`,
with `OFFSET`, `SELECT *`; `customer_id` ASC, DESC, `NULLS LAST`, with
`WHERE`) return identical rows with
`enable_topk_dynamic_filter_pushdown` / `enable_dynamic_filter_pushdown` on and
off. A local NaN probe (20 000 doubles plus a NaN row) returns the NaN row
first for `ORDER BY x DESC`, with pushdown on and off. Unit tests in
`physical_to_predicate::tests` pin the IS NULL conversion and the float
exclusion.

Behaviour change: joins reordered by `StarSchemaReorderRule` get dynamic
filters again (the re-push). SSB / TPC-H impact is not measured yet.

Open:

- `DistributedScanExec` (worker scans) is not settled; a distributed TopK
  still holds the orphan.
- Float sort keys: pruning them soundly needs NaN-aware statistics (the
  Iceberg `nan_value_counts` manifest field) or a NaN guard in the vendored
  evaluator.
- File and row-group pruning at stream open do not use progressive filters.
  Pruning row groups per file against the bound at file open would also skip
  I/O, not only decode.
- The rest of #787: unsorted vs clustered layouts and delayed payload
  decode for q23.

**NEXT:** SSB/TPC-H SF10 compare for the star-schema re-push, then settle `DistributedScanExec`.

# SSB SF1 trace investigation

> Why SSB at scale factor 1 sits at 0.50-0.85x vs Trino while every other
> suite ships in the 1-7x range. Captured 2026-04-30 against
> `tpch-sf1-flight-2026-04-30T16:25:01.json` baselines using the new
> phase-level rows in `EXPLAIN ANALYZE` (MR !121).

## Setup

The `EXPLAIN ANALYZE` output now prefixes per-operator metrics with five
phase rows:

```
 step  operation                                              elapsed_ms
 -5    [phase] parse + logical plan                                  X
 -4    [phase] policy evaluate                                       X
 -3    [phase] physical plan                                         X
 -2    [phase] execute (per-op detail below)                         X
 -1    [phase] framework overhead (parse + plan + policy + result)   X
```

Combined with `BENCH_DEBUG=1` printing the result rows from
`sqe-bench` (MR !122), we can see exactly where each query spends its
time.

## What the trace shows

Five SSB queries patched to `EXPLAIN ANALYZE`, run through the live
bench harness:

| Query | parse+plan | policy | physical | execute | framework | result rows |
|-------|-----------:|-------:|---------:|--------:|----------:|------------:|
| q1.1 (cold) | 20.2 | 0.02 | 3.1 | 585.7 | 23.3 | 1 |
| q2.2 | 0.40 | 0.001 | 1.8 | 470.8 | 2.2 | 0 |
| q3.2 | 0.42 | 0.001 | 1.9 | 432.0 | 2.4 | 600 |
| q3.3 | 0.46 | 0.001 | 2.0 | 417.5 | 2.4 | 0 |
| q4.1 | 0.52 | 0.001 | 2.1 | 651.0 | 2.7 | 35 |

Two findings up front:

1. q1.1 (the first query) shows a real DataFusion warmup: ~20 ms parse
   + ~23 ms framework overhead. From q2.2 onward the parse+plan drops to
   < 0.5 ms and framework overhead to ~2 ms. The warmup amortizes to
   ~3 ms / query across the suite.
2. The execute phase is 90+% of every query's wall time. The framework
   overhead a plan cache could shave is ~0.5 ms / query.

So **the SSB SF1 floor is not framework cost.** Plan-cache and parse
optimizations save < 1 ms / query.

## Per-operator breakdown

Per-query, focusing on the `lineorder` scan (the fact table):

| Query | lineorder rows scanned | scan elapsed_compute | first join output | join elapsed_compute |
|-------|-----------------------:|--------------------:|------------------:|--------------------:|
| q1.1 | **786,156** (date-pruned) | 189.8 | 112,292 | 2.9 |
| q2.2 | 6,000,000 (full) | 127.9 | 6,000,000 | 41.9 |
| q3.2 | 6,000,000 (full) | 135.6 | 5,145,010 | 38.0 |
| q3.3 | 6,000,000 (full, **but result is 0**) | 118.1 | 5,145,010 | 41.9 |
| q4.1 | 6,000,000 (full) | 166.9 | 6,000,000 | 46.9 |

q1.1 is fastest because its `WHERE lo_orderdate BETWEEN 19940101 AND
19940131` is a literal range filter on lineorder. SQE's static
predicate pushdown reduces the scan from 6M to 786K rows. Every other
query scans all 6M lineorder rows even when the result is zero (q2.2,
q3.3, q3.4 all return 0 rows after dim filters that match nothing).

The expectation was that runtime filter pushdown (Path B-2,
[runtime-filter-pushdown.md](./runtime-filter-pushdown.md)) would prune
lineorder via dim build-side filtering. The trace shows it does not.

## Why Path B-2 did not engage

Added temporary `eprintln!` traces to `convert_physical` and
`convert_in_list` in
`vendor/iceberg-rust/crates/integrations/datafusion/src/physical_plan/physical_to_predicate.rs`
and ran the full SSB suite. **Zero invocations.** The dynamic
predicate code path was never reached on any SSB SF1 query. The
ground truth: DataFusion was not pushing runtime filters down to
SQE's `IcebergScanExec` at all. Why is covered in the
[root cause](#root-cause-missing-gather_filters_for_pushdown-override)
subsection below.

### Debugging Path B-2 going forward

SQE's `IcebergScanExec` (the production scan node, in
`crates/sqe-catalog/src/iceberg_scan.rs`) emits a `tracing::debug!`
line in `handle_child_pushdown_result` so future investigators can
see whether DataFusion is offering filters to the scan and how many
of them are dynamic:

```bash
RUST_LOG="info,sqe_catalog=debug" \
  BENCH_SCALE=1 ./scripts/benchmark-test.sh ssb
```

The relevant log fields:

```
target=sqe_catalog::iceberg_scan
  IcebergScanExec::handle_child_pushdown_result
    table=...  parent_filter_count=N  dynamic_filter_count=N
```

The vendored `IcebergTableScan` in `iceberg-rust` emits equivalent
logs under `target=iceberg_datafusion::physical_plan::scan`, but SQE
queries hit the SQE node, not the vendored one. Use the
`iceberg_datafusion=debug` filter only when investigating direct uses
of the vendored crate.

If `parent_filter_count = 0` on every call, DataFusion never offered
a runtime filter to this scan: an intermediate node in the plan
blocked pushdown, or the cost-model rule decided this join was not
worth a dynamic filter, or, as it turned out for SSB SF1, the scan
itself failed to declare itself a filter-absorbing leaf via
`gather_filters_for_pushdown`.

If `parent_filter_count > 0` but `dynamic_filter_count = 0`, the
parent forwarded only static filters (already handled at plan time)
and there is no runtime filter to honor.

If `dynamic_filter_count > 0` but the bench shows no scan reduction,
the runtime filter is reaching the scan but pruning is not happening.
Two reasons that might be: the dynamic filter is still at its
`lit(true)` placeholder when the scan executes (normal for the first
batches), or the iceberg row-group/file pruning is not honoring it.
Check `physical_to_predicate.rs::convert_physical` and the
`file_entries.len() > 1` gate in
`iceberg_scan.rs::execute_partition_inner` next.

### Root cause: missing `gather_filters_for_pushdown` override

The hypotheses above were all wrong. The dynamic filter never reached
the scan because SQE's `IcebergScanExec` (in
`crates/sqe-catalog/src/iceberg_scan.rs`) was missing the
`gather_filters_for_pushdown` override.

The default `ExecutionPlan::gather_filters_for_pushdown` returns
`FilterDescription::all_unsupported(...)`. That tells DataFusion's
filter-pushdown rule "this node does not support any of these
filters." The optimizer then abandons the dynamic filter, and
`handle_child_pushdown_result` is never called. Path B-2 silently
no-ops.

Adding the override (returning `FilterDescription::new()`, the
leaf-scan convention used by the vendored `IcebergTableScan` in
iceberg-rust) tells DataFusion the scan absorbs filters. With debug
logging on, lineorder now shows:

```
target=sqe_catalog::iceberg_scan
  IcebergScanExec::handle_child_pushdown_result
    table=lineorder  parent_filter_count=1  dynamic_filter_count=1
```

The dynamic filter from the dim build side now reaches the scan.

The vendored `IcebergTableScan` already had the correct override;
SQE's reimplementation simply did not. None of the three earlier
hypotheses (intermediate `RepartitionExec`, missing
`DynamicFilterPhysicalExpr`, cost-model rejection) was right.

### Why SSB SF1 still does not see a wall-clock improvement

The fix engages Path B-2 correctly but does not move the SSB SF1
floor. Two reasons:

1. SSB SF1 lineorder fits in one Parquet file. SQE's file-level
   pruning is gated by `file_entries.len() > 1` in
   `iceberg_scan.rs`: it only attempts to skip files when there is
   more than one to choose between. Single-file tables fall through
   to a full scan.

2. SQE's `IcebergScanExec` does no row-group level pruning with the
   dynamic filter. Even if the file-level gate were lifted, it would
   either keep or skip the entire 6 M-row file. To get sub-file
   pruning, the runtime filter would need to be evaluated against
   per-row-group min/max from the Parquet footer.

Expected to pay off at SF10+ where lineorder spans multiple files
and a selective dim build side can skip whole files. Track row-group
level dynamic-filter pruning as a follow-up.

## What the empty-IN-list fix changes

While investigating, found a real but minor correctness issue in
`convert_in_list`: when the IN-list is empty (which would happen with a
zero-row build side that did get to push a runtime filter), the
converter returned `None` instead of `Predicate::AlwaysFalse`. That
means even if Path B-2 fired with an empty build, the lineorder scan
would not be pruned. The fix emits `AlwaysFalse` for empty IN-lists so
iceberg's metrics evaluator can prune every data file.

This fix is correct in principle but **does not help SSB SF1**, because
Path B-2 does not fire at all. Kept as a small correctness improvement
for any future case where DataFusion does propagate an empty IN-list
down to a leaf scan.

## Where the SSB SF1 floor actually lives

`elapsed_compute` (per-operator CPU time) sums to 30-50% less than the
execute-phase wall clock. The gap is asynchronous I/O wait: S3 GETs
for Parquet data files. Per query this is roughly 100-200 ms.

For 0-row queries (q2.2, q3.3, q3.4), the wasted work is:

- 6 M lineorder rows scanned and decoded
- Joined against a 0-row dim, produces 0 rows
- Aggregation runs over 0 rows (~0 ms)

If DataFusion short-circuited the entire join subtree to `EmptyExec`
the moment it knew one build side was empty, the lineorder scan could
be skipped. DataFusion does this at the join level (the `HashJoinExec`
itself returns immediately when build is empty), but the lineorder
probe-side scan has already been started by then. The cost is paid
before the join discovers it has no work.

## Candidate optimizations, ranked by trace evidence

1. **Plan-time cardinality estimation for dim filters that resolve to
   constant predicates.** Pre-evaluate the dim filter at plan time when
   the filter is a constant `IN`-list or equality on a column with
   tracked min/max. If 0 files survive metrics-based pruning, replace
   the join subtree with `EmptyRelation`.
   Helps q3.3, q3.4, q2.2 specifically. Estimated savings: ~120 ms x 3
   queries = ~360 ms across SSB SF1.

2. **Better join reordering for star-schema selectivity.** q3.3 chose
   `lineorder × supplier` first (build = 2192 rows, output = 5.1 M)
   instead of `lineorder × customer` first (build = 0 rows, output =
   0). SQE has `star_schema_reorder` enabled by default; investigate
   why it picked the wrong order on this query shape.
   Helps any star-schema query with one highly-selective dim.
   Estimated savings: ~160 ms / query when the optimizer flips the order
   correctly.

3. **Row-group level dynamic-filter pruning in `IcebergScanExec`.**
   Path B-2 now engages (the `gather_filters_for_pushdown` fix lands
   the runtime filter on the scan) but SF1 still scans 6 M rows
   because lineorder is one file and SQE only prunes at file level.
   Wire the resolved dynamic filter through `PruningPredicate` over
   per-row-group min/max from the Parquet footer, the way DataFusion
   does for static filters in `ParquetExec`. Lifts the
   `file_entries.len() > 1` gate as well so single-file tables can
   still benefit. Estimated savings: matches the SF10+ Path B-2
   numbers when the dim build is selective.

4. **Investigate Path B-2 at SF10.** Path B-2 is now wired up but the
   SSB SF1 cardinality is too small to demonstrate it. Re-run SSB at
   SF10 to confirm the fix actually reduces lineorder scans on
   queries with selective dim builds.

5. **Plan cache.** Production hygiene; adds ~0.5 ms / query benefit on
   repeated SQL. Not the SF1 fix.

The trace work itself (MR !121, !122) is the foundation: every future
SSB optimization can be measured against the phase rows.

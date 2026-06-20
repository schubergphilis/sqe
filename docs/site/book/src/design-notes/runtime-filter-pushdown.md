# Runtime filter pushdown into the Iceberg scan

> Engineering log for the work that closes most of the SF10 TPC-H gap to
> Trino on lineitem-heavy join queries (q06, q14, q15, q16, q17, q20).
> Pairs with upstream issue
> [apache/iceberg-rust#2376](https://github.com/apache/iceberg-rust/issues/2376).

## Problem

DataFusion 53 has a runtime / dynamic filter pushdown path: a `HashJoinExec`
build side emits a `DynamicFilterPhysicalExpr` (initially `lit(true)`,
sealed once the build completes), and the framework walks the plan to
push that filter into the probe-side scan. The probe scan then uses the
filter to skip Parquet row groups, consult bloom filters, and avoid
reading data that can't possibly match.

Until the work documented here, **none of that reached our Iceberg
scan**. The vendored `IcebergTableScan` left
`gather_filters_for_pushdown` and `handle_child_pushdown_result` at the
`ExecutionPlan` defaults, which reject every parent filter. So the
runtime filter ended up sitting above the scan as a `FilterExec` and
only ran AFTER the data had been decoded. No Parquet-level pruning at
all.

The visible symptom: at TPC-H SF10, lineitem-heavy joins ran 3-5x
slower than Trino on the same data:

```
q06  SQE 9546  vs Trino 1217   0.13x
q08  SQE 9262  vs Trino 1070   0.12x
q14  SQE 8139  vs Trino 1664   0.20x
q15  SQE 10715 vs Trino 2546   0.24x
```

## What we shipped

Three fix branches, in order:

| branch | commit | what it does |
|---|---|---|
| `fix/bench-tpch-decimal-types` | merged | Bench generator was emitting `Float64` for TPC-H money/quantity columns even though the schema said `DECIMAL(15, 2)`. Float64 SUM is non-associative, which broke q15's `total_revenue = MAX(total_revenue)` equality compare (the row-count flipped between runs). Fixing the generator made q15 deterministic and incidentally simplified DataFusion's type coercion. |
| `fix/bench-tpcc-decimal-types` | merged | Same shape on the TPC-C generator (8 columns, varying precisions). |
| `feat/iceberg-scan-runtime-filter` | open MR | The actual runtime filter pushdown. Two layered commits described below. |

### Path B: post-batch runtime filtering (commit `dd300b3`)

`vendor/iceberg-rust/crates/integrations/datafusion/src/physical_plan/scan.rs`

- `IcebergTableScan` gained a `runtime_filters: Vec<Arc<dyn PhysicalExpr>>` field.
- `gather_filters_for_pushdown` returns an empty `FilterDescription` (leaf with no children).
- `handle_child_pushdown_result` clones self with the parent filters appended and reports `PushedDown::Yes` per filter, so the framework drops the wrapping `FilterExec`.
- `execute()` wraps the iceberg-rust output stream with a per-batch `PhysicalExpr::evaluate` + `arrow::compute::filter_record_batch`. The `DynamicFilterPhysicalExpr` starts as `lit(true)` (no-op while the build side is loading) and becomes selective once the build accumulator is sealed, so the filter kicks in mid-stream without restarting the scan.

Bench delta vs the post-DECIMAL baseline:

| | SF1 | SF10 |
|---|---:|---:|
| total | -21.3% (18,384 -> 14,464 ms) | -9.4% (163,858 -> 148,511 ms) |
| matched | 22/22 | 22/22 |

Big per-query wins at SF1: q20 -45%, q07 -36%, q15 -35%, q06 -30%, q14 -28%.

### Path B-2: per-task scan-time pruning (commit `c564a89`)

Path B filters AFTER the row group is decoded. To reach Parquet
row-group skipping, the dynamic predicate has to participate in the
reader's existing static-predicate pruning paths. We extended
iceberg-rust with a Trino-style "sample once per file scan task" hook.

`vendor/iceberg-rust/crates/iceberg/src/expr/dynamic.rs` (new):

```rust
pub trait DynamicPredicate: Send + Sync + Debug {
    fn current(&self) -> Option<Predicate>;
}
```

`vendor/iceberg-rust/crates/iceberg/src/scan/mod.rs`:
`TableScanBuilder::with_dynamic_predicate(...)` plumbs the trait
through `TableScan` to `ArrowReaderBuilder`.

`vendor/iceberg-rust/crates/iceberg/src/arrow/reader.rs`: at the start
of `process_file_scan_task` we sample `dp.current()`, bind the result
to the task schema, and 3-way AND with the static predicate and the
equality-delete predicate. The combined predicate flows into the
reader's existing row-group min/max, page-index, and `RowFilter` paths
unchanged.

`vendor/iceberg-rust/crates/integrations/datafusion/src/physical_plan/physical_to_predicate.rs`
(new): minimal physical -> iceberg-`Predicate` converter for the
expression shapes `HashJoinExec`'s
`enable_dynamic_filter_pushdown` produces:

| input | output |
|---|---|
| `DynamicFilterPhysicalExpr` (wrapper) | unwrap to `current()`, recurse |
| `Literal(Boolean(true))` | `None` (build hasn't run yet) |
| `BinaryExpr(col cmp lit)` for Eq/NotEq/Lt/LtEq/Gt/GtEq | `Predicate::Binary` |
| `BinaryExpr(And)` | best-effort AND of two sides |
| `BinaryExpr(Or)` | both sides must translate (else `None`) |
| `InListExpr(col, [literals])` | `Predicate::Set` |
| anything else | `None` (per-batch evaluator from Path B handles it) |

`scan.rs::RuntimeFiltersDynamicPredicate` wraps the existing
`runtime_filters` Vec and exposes them via the trait. `execute()`
constructs the bridge and hands it to the iceberg-rust scan_builder.

Bench delta vs Path B alone:

| | SF1 | SF10 |
|---|---:|---:|
| total | +3.7% (~noise) | **-3.3% (148,511 -> 143,590 ms)** |
| vs 4/21 baseline (SF10) | n/a | **-12.4%** |

SF10 per-query wins from scan-time pruning (Path B + B-2 vs 4/21
baseline):

```
q06   9546 -> 4660  -51%
q07  14290 -> 9799  -31%
q14   8139 -> 5458  -33%   (canonical target query)
q15  10715 -> 8997  -16%
q16   1084 ->  563  -48%
q17   6196 -> 5168  -17%
q20   6118 -> 5173  -15%
```

q18, which had regressed under an earlier bloom-filter experiment, is
back to baseline (17,640 -> 17,392 ms, -1.4%). That regression was a
bloom-only artifact; Path B-2 doesn't touch it.

## What we tried and reverted

After Path B-2, five SF10 queries regressed vs Path B alone (q02 +22%,
q04 +11%, q08 +18%, q09 +11%, q11 +16%). All five share a shape:
**multi-join chains above one big scan**, so every HashJoinExec emits a
runtime filter, and the leaf scan absorbs all of them. Per-task
sampling cost compounds.

We tried five follow-up fixes across four fresh attempts. All five
made things worse and got reverted. Each failed for a different
reason; cataloguing them here so the next person doesn't repeat the
same mistakes.

The fourth failure (cap=200 below) is the most informative. It
revealed that **SF10 has a ~5-7% run-to-run noise floor**, which is
larger than every effect size we were trying to optimize. Without
multi-run statistics or a more sensitive measurement, we cannot
distinguish "the fix helped" from "this run got lucky."

### Failed attempt 1: IN-list size cap

The fix: in `convert_in_list`, return `None` when the IN-list has more
than 4096 values. q04 at SF10's runtime filter is `l_orderkey IN (~580K
orderkeys)`, which is expensive to construct as
`Predicate::Set` and to bind per task.

Why it backfired: queries with **multiple** runtime filters keep paying
construction cost on the smaller filters while losing the row-group
pruning the big filter was actually delivering. Worst-of-both-worlds.
q04 went +24% rather than recovering.

### Failed attempt 2: Arc::ptr_eq cache

The fix: cache the converted `Predicate` keyed on
`Arc::as_ptr` of the inner expression returned by
`DynamicFilterPhysicalExpr::current()`, so we skip retranslation when
the filter hasn't changed between tasks. Backed by a `std::sync::Mutex`.

Why it backfired:
`DynamicFilterPhysicalExpr::current()` calls `remap_children`, which
in the join-pushdown path returns a freshly-built `Arc<dyn PhysicalExpr>`
each call (column indices get remapped into the probe schema). So
`Arc::ptr_eq` never matched, the cache never hit, and the only
observable effect was Mutex contention across the scan's 11 concurrent
FileScanTask processors.

Net SF10 result with both fixes: **150,693 ms**. *slower* than
Path B (148,511 ms) and Path B-2 (143,590 ms). q14 went +35%, q16
+91%, q01 +24%.

### Failed attempt 3: OnceLock first-success cache

The fix: replace the `Mutex<Vec<...>>` cache with a `std::sync::OnceLock<Predicate>`
that fills on the first call returning a non-`None` predicate and is
read lock-free thereafter. The intent: avoid both the Mutex contention
from attempt 2 and the conversion cost on every per-task call. The
trade-off was acknowledged up front: lock in the FIRST snapshot we
observe.

Why it backfired (a new failure mode, distinct from attempts 1-2):
multi-filter scans (q04, q06, q08, q09) have several DynamicFilterPhysicalExpr
runtime filters that seal at *different times* as the upstream hash
joins finish their builds. The first task that observes ANY filter
sealed converts to a Predicate that AND-combines whatever sealed so
far. The OnceLock then locks that **partial** predicate in for the
rest of the scan, so later tasks miss the additional pruning that
would have come from filters sealing later. q04 got +34%, q06 +74%,
q16 +42% relative to Path B-2 alone.

Net SF10 result: **150,811 ms** (worse than Path B-2's 143,590 ms by
~5%, and even slightly worse than Path B alone's 148,511 ms).

The takeaway for any future cache-based attempt: caching only works
if all dynamic filters in the bundle have sealed by the time you
populate the cache. A single `OnceLock` keyed on "first non-None" is
therefore unsafe for multi-filter scans. A correct cache needs either
(a) per-filter `OnceLock` slots indexed by stable filter identity, or
(b) a sentinel that says "all filters in this bundle are sealed" before
the cache fills. Both depend on iceberg-rust / DataFusion exposing a
"is this filter sealed?" predicate, which is the upstream API ask in
issue 2376.

### Failed attempt 4: Trino-aligned IN-list cap at 200

The fix: cap `convert_in_list` at 200 values, matching iceberg-rust's
own `IN_PREDICATE_LIMIT` constant (defined identically in
`row_group_metrics_evaluator.rs`, `manifest_evaluator.rs`, and
`inclusive_metrics_evaluator.rs`). Above 200 values the iceberg-rust
evaluator unconditionally returns `ROW_GROUP_MIGHT_MATCH`, so any
larger `Predicate::Set` we hand it has the conversion + binding cost
amortized across zero pruning benefit. The intent: align with
upstream's threshold to never do work the reader will discard.

This was a different failure mode from attempt 1 (which used 4096):
attempt 1 sat in the dead zone (200-4096 values: full converter cost,
zero pruning), whereas attempt 4 was placed exactly at the upstream
boundary so we should never waste work.

Why it backfired (the smoking-gun moment): SF10 result was
**151,635 ms** versus Path B-2's 143,590 ms, a 5.6% regression. But
look at what regressed:

| q | join? | Path B-2 | cap=200 | delta |
|---|---|---:|---:|---:|
| q06 | none | 4,660 | 6,612 | +42% |
| q03 | none | 8,153 | 10,514 | +29% |
| q20 | yes | 5,173 | 6,579 | +27% |
| q05 | yes | 6,700 | 8,101 | +21% |

q06 has zero joins, so no runtime filters reach its scan, so the
cap setting is **literally a no-op for that query**. It still moved
+42%. The variance band of SF10 itself is wider than the effect
we're trying to measure.

Cross-checking SF10 totals across functionally-equivalent runs:

| date  | code state                | total |
|-------|---------------------------|------:|
| 4/27  | Path B alone              | 148,511 |
| 4/27  | Path B-2 (final)          | 143,590 |
| 4/27  | Path B-2 + OnceLock       | 150,811 |
| 4/28  | Path B-2 + cap=200        | 151,635 |

The spread is ±5-7% with no single run-to-run difference attributable
to a code change. SF10's noise sources include cold object-cache state
on every regenerate, Trino's per-run JIT warm-up, OS page-cache
variance across the 60M-row lineitem, and Polaris's age-since-restart
affecting catalog response latency.

The takeaway: at SF10, a single bench run can't reliably measure
effects below 7%. The Path B-2 baseline already has 22/22 match and
−12.4% vs the 4/21 baseline; that's signal we know is real because it
sits well above the noise band. Anything inside ±5% needs either a
multi-run confidence interval (5+ runs, 2 hours), a focused single-
query EXPLAIN ANALYZE, or a smaller deterministic benchmark.

### Failed attempt 5: bound-predicate cache (post-microbench)

The fix: add a `current_bound(schema, case_sensitive) -> Option<BoundPredicate>`
method to the `DynamicPredicate` trait, plumb the reader through it,
and override in `RuntimeFiltersDynamicPredicate` with a per-filter
sealed-state cache backed by a `Mutex<Option<CachedBound>>`. The
microbenchmark (commit `5eeb00c`) showed `Predicate::bind` is the
dominant per-task cost (~61 ms at N=580K, 3x the conversion cost),
so caching the *bound* result rather than the unbound one targets the
right layer.

The design supposedly avoided the partial-seal trap from attempt 3 by
treating each filter independently: walk every filter, contribute the
ones whose `DynamicFilterPhysicalExpr` inner is no longer `lit(true)`,
mark `fully_sealed` only when every filter passed the check this round,
and short-circuit subsequent calls to a lock-free clone of the cached
combined `BoundPredicate`.

Why it backfired (despite being microbench-correct): SF10 result was
**162,589 ms vs Path B-2's 143,590 ms (+13.2%)**. q11 -28% and q12 -15%
showed the cache works when filters seal early, but q05 +73%, q20 +40%,
q01/q03 +33%, q22 +30% all regressed because:

1. **Multi-join queries seal filters at staggered times.** Until the
   last filter seals, every task hits the slow path: walk every filter,
   convert each sealed one, AND them, bind once. That work is the same
   as Path B-2 plus a Mutex round-trip.

2. **The Mutex contends across the scan's concurrent tasks.** Every
   slow-path call grabs the Mutex twice (read for fast path, write
   to update). With ~12 concurrent FileScanTask processors per scan,
   the lock serializes them. This is the same failure mode as
   attempt 2; the lock-free fast path only kicks in after `fully_sealed`,
   which can be late in queries with deep join chains.

3. **The microbench measured single-threaded steady-state cost; the
   real workload pays Mutex acquisition cost on every call.** The
   bench predicted ~80 ms saved per task; the lock contention burnt
   most of that and added some.

This was the most disappointing failure because the microbench data
strongly suggested it should work. The lesson: targeting the right
layer (bind) is necessary but not sufficient. Without an upstream
signal that lets us know when filters are sealed (so the cache can
populate exactly once at the right moment), every Mutex-based design
hits this contention floor.

### Failed attempt 6: SQE-side unconditional `with_dynamic_predicate` (2026-05-03)

The fix: after MR !124 (`6a03124`) brought Path B-2 to SQE's own
`IcebergScanExec` via the `gather_filters_for_pushdown` override,
the natural next step was to call `with_dynamic_predicate` on the
iceberg-rust scan builder from SQE's streaming path too. The wiring
mirrors what the vendored `IcebergTableScan` already does: build a
`RuntimeFiltersDynamicPredicate` from the resolved
`pushed_down_filters` and pass it via `sb.with_dynamic_predicate(dp)`
right next to the existing `sb.with_filter(static_pred)` call. The
runtime filter then participates in iceberg-rust's row-group min/max,
page-index, and post-decode RowFilter passes alongside the static
predicate.

Why it backfired (recovering known ground): SSB SF1 has single-file
`lineorder` with high-cardinality unsorted join keys (`lo_custkey`,
`lo_suppkey`). Row-group min/max evaluator runs across the file's
row groups but every row group spans the full key range, so no row
group is excluded. Pure conversion plus bind cost is paid 26 times
across the suite (once per `FileScanTask` reaching lineorder), with
zero pruning benefit.

Bisect on a single day, same docker stack:

| SSB SF1 configuration | SQE total | Wiring fires |
|---|---:|---:|
| Bisect (no wiring at all) | 6,647 ms | 0x |
| Unconditional wiring (run 1) | 9,454 ms | 26x |
| Unconditional wiring (run 2) | 9,086 ms | 26x |

Net SF1 regression: **~+37%**. The same failure mode as attempts 1-5:
the per-task pruning surface charges a fixed cost per filter and only
recovers it when the filter shape matches the data layout. SSB join-key
filters at SF1 satisfy neither condition.

Trace evidence the wiring fired correctly (so this is genuinely the
data-shape problem, not a bug):

```
target=sqe_catalog::iceberg_scan
  IcebergScanExec: wired runtime filters into iceberg-rust DynamicPredicate
    table=ssb_sf1.lineorder  count=1  files=1
```

### Failed attempt 7: SQE-side gated `with_dynamic_predicate` (2026-05-03)

The fix: gate attempt 6 on `file_entries.len() > 1`, mirroring the
existing file-level pruning gate. SF1 single-file lineorder skips
the wiring (post-decode filter still applies). SF10+ multi-file
lineorder activates it.

```rust
if !pushed_down_filters.is_empty() && file_entries.len() > 1 {
    let dp = RuntimeFiltersDynamicPredicate::new(pushed_down_filters.clone());
    sb = sb.with_dynamic_predicate(dp);
}
```

Why it backfired (the gate is too coarse): SF1 is preserved cleanly
(7,412 ms gated, 6,647 ms bisect, gate fires 0x). But SF10 reproduces
the per-class TPC-H Path B-2 pattern from attempts 1-5:

| SSB SF10 q | Apr 21 baseline | Today (gated) | Delta | Class |
|------:|------:|------:|------:|---|
| q1.1 | 5,179 |  3,656 | **-29%** | 1 dim, sorted-column probe (`lo_orderdate`) |
| q1.2 | 4,359 |  3,819 | -12%   | same shape |
| q1.3 | 3,884 |  2,740 | **-29%** | same shape |
| q3.4 | 4,935 |  4,318 | -13%   | 0-row dim, `AlwaysFalse` short-circuit |
| q3.1 | 6,121 |  8,972 | **+47%** | 3 dim, high-card key probe |
| q3.2 | 5,080 |  7,071 | +39%   | same |
| q3.3 | 4,253 |  8,269 | **+94%** | same, worst case |
| q4.2 | 7,634 | 13,556 | **+78%** | 4 dim, high-card key probe |
| q4.3 | 7,389 | 10,755 | +46%   | same |
| **TOT** | **76,059** | **89,855** | **+18.1%** | net regression |

The gate catches "is there file-level pruning potential", but does
not differentiate by **filter shape** (whether iceberg-rust's
row-group evaluator can use the predicate at all) or **column
layout** (whether differentiated min/max stats exist on the probe
column). The gate fires on q3.x and q4.x where neither factor is
favourable, paying the per-task cost without earning pruning.

The pattern is identical to TPC-H attempts 1-5: queries with one
dominant filter on a clustered column win, multi-join queries with
high-cardinality probe columns lose. SSB SF10 confirms what the
TPC-H work already established. The per-task surface is structurally
mixed without an additional gating mechanism (filter shape + column
layout).

### Resolution

All seven attempts reverted; SQE's `IcebergScanExec` stays at the
post-decode-filter-only path on main. The vendored `IcebergTableScan`
keeps Path B-2 (working as designed for the TPC-H mix). Postmortem
comment posted to
[apache/iceberg-rust#2376](https://github.com/apache/iceberg-rust/issues/2376#issuecomment-4330042368)
with API recommendations for upstream:

1. Expose a cheap monotonic version (`generation`) on
   `DynamicFilterPhysicalExpr` so consumers can cache without relying
   on `Arc::ptr_eq`, OR
2. Have the `DynamicPredicate` trait return an opaque `(Predicate,
   version)` so the reader can manage its own cache key.

Either is small and forward-compatible. Both unblock the multi-filter
staggered-seal class.

## Bench timeline at a glance

Chronological inflection points of the runtime-filter work, grouped
by what changed in the codebase. Each row is a single bench run; the
totals are exact (not averaged) so the noise floor (±5-7% at SF10,
±5-10% at SF1) is visible inline.

### TPC-H SF10

| Date | Code state | SQE total | Match | Notes |
|------|------------|----------:|------:|-------|
| 4/20 | Pre-runtime-filter, broken q15 | 125,600 ms | 9/22 | quoted DECIMAL bug active |
| 4/21 | Pre-DECIMAL Float64 baseline | 163,858 ms | 6/22 | q15 broken, q07/q14 etc fail |
| 4/27 | Path B (post-batch only) | 148,511 ms | **22/22** | -9.4% on TOTAL, fixes correctness |
| 4/27 | Path B + B-2 (per-task DP) | **143,590 ms** | 17/22 | additional -3.3%; current main |
| 4/27 | + IN-list cap=4096 | reverted | - | q04 +24%, others mixed |
| 4/27 | + Arc::ptr_eq cache | 150,693 ms | - | Mutex contention, q14 +35%, q16 +91% |
| 4/27 | + OnceLock first-success | 150,811 ms | - | partial-seal trap, q06 +74%, q16 +42% |
| 4/28 | + cap=200 (Trino-aligned) | 151,635 ms | - | noise floor visible: q06 (no joins) +42% |
| 4/29 | + bound-predicate cache | 162,589 ms | - | +13.2%, Mutex contention |
| 4/30 | Path B + B-2 (cleaned) | matches 143,590 | - | all attempts reverted |

### SSB SF1

| Date | Code state | SQE total | Notes |
|------|------------|----------:|-------|
| 4/14 | pre-perf baseline | 7,554 ms | early benchmark stack |
| 4/15 | + small-file fast path | 6,208 ms | first wins |
| 4/16 | + dynamic filter pushdown | 6,191 ms | DF 53 dynamic filter wired |
| 4/20 | + parallel small-file fast path | 6,552 ms | within noise |
| 4/30 | + Phase O+ catalog dispatch | 6,908 ms | within noise |
| 5/1  | post MR !124 (Path B-2 wiring SSB) | 8,410 ms | SSB-only run, cold Trino |
| 5/3  | bisect (no wiring) | **6,647 ms** | today's stable baseline |
| 5/3  | unconditional wiring (run 1) | 9,454 ms | **+37% regression** |
| 5/3  | unconditional wiring (run 2) | 9,086 ms | **+34% regression** |
| 5/3  | gated wiring (`file_count > 1`) | 7,412 ms | within noise (gate fires 0x at SF1) |

### SSB SF10

| Date | Code state | SQE total | Notes |
|------|------------|----------:|-------|
| 4/20 | pre-runtime-filter | 92,300 ms | first SF10 SSB run |
| 4/21 | clean baseline | 76,059 ms | reference baseline for today |
| 5/3  | gated wiring (today) | 89,855 ms | +18% net, mixed per-query |

### Per-query effect at TPC-H SF10 (cumulative pre-B vs B+B-2)

The work that landed on main. Negative percentages = faster.

| q | Pre-B | B+B-2 | Delta | Class |
|---|------:|------:|------:|---|
| q01 |  8,404 |  7,180 | -15% | full lineitem scan, group-by |
| q02 |  1,339 |  1,587 | **+19%** | 5-way join, NOT LIKE on a column the converter can't translate |
| q03 |  9,369 |  8,153 | -13% | mktsegment + 3-way join |
| q04 |  5,370 |  4,711 | -12% | orderdate range + EXISTS subquery |
| q05 |  7,930 |  6,700 | -16% | regional 6-way join, low-card region keys |
| **q06** |  9,546 |  4,660 | **-51%** | 1 table, sorted-column range filter (canonical win) |
| **q07** | 14,290 |  9,436 | **-34%** | 2 selective hash joins, lineitem clustered |
| q08 |  9,262 |  8,045 | -13% | regional brand share |
| q09 |  7,058 |  6,982 |  -1% | flat |
| q10 |  9,419 | 10,203 |  +8% | LEFT JOIN noise |
| **q11** |    756 |  1,114 | **+47%** | tiny query, all shuffled keys, overhead dominant |
| q12 |  8,815 |  7,767 | -12% | shipmode filter |
| q13 |  3,606 |  3,185 | -12% | left outer join NOT LIKE |
| **q14** |  8,139 |  5,458 | **-33%** | 1 join, 1 month range filter (canonical win) |
| q15 | 10,715 |  8,997 | -16% | CTE plus revenue threshold |
| q16 |    578 |    563 |  -3% | tiny, flat |
| q17 |  5,409 |  5,168 |  -4% | flat |
| q18 | 17,640 | 17,646 |  +0% | dominated by full scan |
| q19 |  8,332 |  9,428 | **+13%** | OR-of-AND on multiple columns (translation failure) |
| **q20** |  6,391 |  5,173 | **-19%** | semi-join chain, selective key filters |
| q21 | 10,673 | 10,684 |  +0% | flat |
| q22 |    817 |    750 |  -8% | small |
| **TOT** | **163,858** | **143,590** | **-12.4%** | |

Five clean wins in the win signature class (q06, q07, q14, q15, q20).
Three regressions in the loss signature class (q02, q11, q19). Eleven
queries in the noise band. The +12.4% total is real because the wins
are large enough (q06 -51%, q07 -34%, q14 -33%) to dominate the noise
and the small regressions.

### Per-query effect at SSB SF10 (Apr 21 baseline vs today gated wiring)

The data behind the +18.1% total today. Same shape pattern as TPC-H.

| q | Apr 21 | Today | Delta | Class |
|---|------:|------:|------:|---|
| **q1.1** |  5,179 |  3,656 | **-29%** | 1 dim, sorted column (mirrors q06/q14) |
| q1.2 |  4,359 |  3,819 | -12% | same |
| **q1.3** |  3,884 |  2,740 | **-29%** | same |
| q2.1 |  5,374 |  6,175 | +15% | 3 dim, varied selectivity |
| q2.2 |  4,932 |  5,490 | +11% | same |
| q2.3 |  5,928 |  5,975 |  +1% | same |
| **q3.1** |  6,121 |  8,972 | **+47%** | 3 dim, high-card key probe (mirrors q11) |
| q3.2 |  5,080 |  7,071 | +39% | same |
| **q3.3** |  4,253 |  8,269 | **+94%** | same, worst regression |
| q3.4 |  4,935 |  4,318 | -13% | 0-row dim, `AlwaysFalse` |
| q4.1 | 10,991 |  9,059 | -18% | 4 dim, date-range dominant |
| **q4.2** |  7,634 | 13,556 | **+78%** | 4 dim, high-card key probe |
| q4.3 |  7,389 | 10,755 | +46% | same |
| **TOT** | **76,059** | **89,855** | **+18%** | |

The cross-suite mapping is one-for-one. SSB q1.x ≈ TPC-H q06/q14
(sorted column wins). SSB q3.x ≈ TPC-H q11 (high-card probe loses).
SSB q3.4 ≈ TPC-H q15's `AlwaysFalse`-ish path (selective short-circuit
wins). 4-dim queries (q4.x) regress more than 3-dim because each
extra filter compounds the overhead.

## Cross-suite reading: what the SSB and TPC-H data add up to

The per-query tables above (TPC-H SF10 cumulative effect, SSB SF10
gated wiring) line up class-for-class:

| Class | TPC-H SF10 | SSB SF10 |
|-------|-----------:|---------:|
| 1 dim, sorted-column probe (clean win) | q06 -51%, q07 -34%, q14 -33% | q1.1 -29%, q1.3 -29% |
| Selective short-circuit / `AlwaysFalse` | q15 -16% | q3.4 -13% |
| Multi-dim, high-cardinality key probe (clean loss) | q11 +47%, q19 +13%, q02 +19% | q3.1 +47%, q3.3 +94%, q4.2 +78% |
| Tiny query, overhead-dominated | q11 (756 ms baseline) | q3.x at SF1 (sub-second) |

The mixed bag is **not noise** and not an implementation bug. It is
structural and reproduces across two benchmarks. The SF10 SSB run
explains why the `with_dynamic_predicate` call is intentionally
absent from SQE's `IcebergScanExec` on main even though the
machinery exists in the vendored crate: the author of `c564a89`
already knew the regression class would dominate on the SSB mix.
SQE's scan path stays at post-batch filter-only because that
matches the realistic SQL workloads we run.

## Why mixed results: a per-query walkthrough

The cross-suite data shows clearly that the wiring helps some queries
and hurts others. The why is not "noise" or "implementation bugs": it
is a small set of compounding mechanical reasons. Each is illustrated
with a query that exhibits it.

### 1. IN_PREDICATE_LIMIT = 200 in iceberg-rust's evaluators

The row-group + manifest + inclusive metric evaluators all bail to
`MIGHT_MATCH` when an IN-list has more than 200 values. The
`Predicate::Set` we built is **fully evaluated** before that bail-out:
field reference resolution, type binding, conversion cost. All of
that is paid; none of it produces pruning.

**SSB q3.3 hits this hard.** The customer build emits ~8K custkey
values for `c_city IN ('UNITED KI1','UNITED KI5')` (8K customers
from 30K total whose city matches). 8K is 40x the cap. Conversion +
bind run, evaluator returns MIGHT_MATCH, no row group skipped. With
3 dim joins all paying this cost per `FileScanTask`, the overhead
compounds linearly.

```sql
-- q3.3
FROM lineorder, dim_date, customer, supplier
WHERE lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND lo_orderdate = d_datekey
  AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')      -- ~8K custkeys
  AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')      -- ~5 suppkeys
  AND d_year BETWEEN 1992 AND 1997                          -- 2191 datekeys
```

q3.4 has the **identical join structure** but `d_yearmonth = 'Dec1997'`
narrows the date filter to ~30 days. The narrower date dimension
fits under the 200 cap AND happens to align with lineorder's natural
orderdate clustering, so it wins (-13%). Same query, different filter
selectivity, opposite outcome.

### 2. Probe-side data layout (clustered vs shuffled)

`lineorder` is naturally written ordered by `lo_orderdate`. Files
that arrive into the table ship in chronological order and Iceberg
preserves that. Row groups within a file therefore have
**differentiated** min/max on `lo_orderdate`: a 1993 row group has
`min=19930101, max=19931231`, a 1994 row group has 1994 bounds, etc.

Date-range filters are **bounds** (BETWEEN, >, <), not IN-lists.
Bounds against differentiated min/max prune cleanly: a 1993 filter
against a 1995 row group is provably outside the range, drop the
row group.

`lo_custkey`, `lo_suppkey`, `lo_partkey` are **shuffled** across
files because customer/supplier/part are dim tables joined into
fact rows. Every row group has min ≈ 1 and max ≈ N for these. The
evaluator runs but cannot find a bound that excludes the row group.
We pay the cost; nothing prunes.

**SSB q1.1 wins (-29%) on this**:

```sql
-- q1.1 (1 dim join, date filter only)
FROM lineorder, dim_date
WHERE lo_orderdate = d_datekey
  AND d_year = 1993
  ...
```

The `dim_date` build emits 365 datekeys for d_year=1993. DataFusion
recognises this as a contiguous range and emits the dynamic filter
as bounds (`l_orderdate BETWEEN 19930101 AND 19931231`) rather than
an IN-list. Bounds against a sorted-column row group min/max =
clean prune. Most non-1993 row groups skip entirely.

**SSB q3.1 loses (+47%)** with the same orderdate column AND a
6-year date range filter (so date pruning is weaker), PLUS two
shuffled-key joins:

```sql
-- q3.1 (3 dim joins, two on shuffled keys)
WHERE lo_custkey = c_custkey  -- shuffled
  AND lo_suppkey = s_suppkey  -- shuffled
  AND lo_orderdate = d_datekey  -- sorted, but date range is 6 years
  ...
```

Net effect: weaker date pruning + zero key-column pruning + cost on
3 filters per task = -47% slower.

### 3. Filter shape: bounds vs IN-list

DataFusion's hash join chooses between two dynamic-filter shapes
based on build size:

- **Small build**: emit `InListExpr(probe_col, [build_keys...])`.
  Translates to `Predicate::Set` for iceberg-rust. Subject to the
  200-element cap.
- **Build is a contiguous numeric range**: emit
  `BinaryExpr(probe_col >= min AND probe_col <= max)`. Translates to
  `Predicate::Binary` bounds. Always evaluated; no cap.

Bounds work against any column with differentiated row-group min/max
(sorted or partitioned). IN-list shapes only prune when the probe
column has clusters of contiguous values.

**TPC-H q11 illustrates the cost side without the benefit side**:

```sql
-- q11 (2 hash joins, ALL on shuffled keys)
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
HAVING SUM(...) > (subquery scans the same tables again)
```

`s_nationkey` build for n_name='GERMANY' = 1 nationkey, well under
the cap. Bind succeeds. But `partsupp` is the probe and is unsorted
by suppkey across its files. Min/max evaluator runs, can't prune.
Total query is small (756 ms baseline); per-task overhead becomes a
visible fraction. Net +47% regression.

### 4. Multi-filter staggered sealing

`HashJoinExec` build sides complete at different wall-clock times.
The earliest dim that finishes building seals its dynamic filter
first; later dims seal later. Tasks that start scanning while only
some filters are sealed see a partial predicate.

Concretely: when the per-task `dp.current()` is called early in the
scan, it returns `Some(predicate)` only for the dims whose builds
have completed. Tasks that call later get a more selective predicate.
Iceberg-rust does not re-call `dp.current()` for a task once the file
is open, so early-task tasks miss the late-sealing filters entirely.

**SSB q4.2 (+78%) is the canonical example**:

```sql
-- q4.2 (4 dim joins, all sealing at different times)
FROM lineorder, dim_date, customer, supplier, part
WHERE lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND lo_partkey = p_partkey
  AND lo_orderdate = d_datekey
  AND c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  ...
```

Four dims build in parallel. supplier (smallest) seals first, then
maybe date, then customer, then part. Lineorder scan tasks ramp up
as soon as the first build is ready. The probe side does most of
its decoding while only 1-2 of 4 filters have sealed. By the time
all 4 are sealed, the scan is mostly done. The runtime filter never
gets to apply with full selectivity at the scan layer.

This is the same failure mode that killed the OnceLock cache (failed
attempt 3).

### 5. OR-branch translation failures

iceberg-rust's predicate AST supports `Or`, but
`convert_physical_filters_to_predicate` requires **both** sides of an
OR to translate. If one side has a shape it doesn't understand
(e.g., a complex BinaryExpr nested inside, or a non-supported
literal type), the whole OR returns `None`.

**TPC-H q19 is the classic OR-of-AND query**:

```sql
WHERE
  (p_partkey = l_partkey AND p_brand = 'Brand#12'
   AND p_container IN ('SM CASE','SM BOX',...) AND ...)
  OR (p_partkey = l_partkey AND p_brand = 'Brand#23'
      AND p_container IN ('MED BAG','MED BOX',...) AND ...)
  OR (p_partkey = l_partkey AND p_brand = 'Brand#34'
      AND p_container IN ('LG CASE','LG BOX',...) AND ...)
```

DataFusion may emit a single dynamic filter combining all three
branches, or one per branch with union semantics. Either way, the
converter has to walk every leaf and translate. Any leaf failing to
translate kills the whole branch. q19 +13% regressed because we
walked the tree, failed somewhere, and returned None. We paid the
walk cost for nothing.

### 6. Small-query overhead amortization

Per-task `DynamicPredicate` sampling has a fixed cost: walk the
runtime filters, downcast each, build the iceberg `Predicate`, bind
to the file schema. The microbench (commit `5eeb00c`) measured this
at ~80 ms per task at N=580K IN-list size; less at smaller sizes
but never zero.

For tiny queries (q11 at 756 ms baseline, q22 at 817 ms, q3.1 at
~6s) the per-task overhead is a visible fraction of total wall-clock.
For large queries (q07 at 14 s, q15 at 10 s) the overhead amortizes
across more useful work.

This is why noise is so visible at SF1 (most queries < 1 s) and
muted at SF10 (most queries 5-15 s). It is also why the failed
attempt 4 (cap=200) measured +42% on q06 even though q06 has zero
joins: the noise band is wider than the effect on small queries.

### 7. The signature of a clean win

A query wins from `with_dynamic_predicate` when **all** of these are
true:

1. The probe-side column is naturally clustered or sorted.
2. The build side is small enough (<200 values) to land as a real
   IN-list, OR the dim filter is contiguous (date range, region IN)
   so DataFusion emits bounds.
3. The total query has > ~5 s of decode work for the per-task
   overhead to amortize against.
4. There is one dominant filter, or one filter that seals first
   and is selective on its own.

TPC-H q06, q14, q07, q20 all match this signature. SSB q1.1, q1.2,
q1.3 do too. q3.4 wins via the AlwaysFalse short-circuit (special
case of #2 with build size = 0).

### 8. The signature of a clean loss

A query loses when:

1. Multiple dim joins with shuffled probe-side columns
   (custkey, suppkey, partkey).
2. At least one dim build is large (> 200 values) so its
   `Predicate::Set` is wasted conversion.
3. Total query is small (< 5 s) so per-task overhead dominates.

TPC-H q11, q19, q02 match this. SSB q3.1, q3.2, q3.3 do too. The
worst offenders are the 4-dim queries (q4.2, q4.3) which compound
all three.

## Core issue and smart solutions

Reduced to one line: **SQE pays per-task eval cost on every filter,
regardless of whether that filter can actually prune row groups for
the data shape it targets.**

Everything else falls out of that. The per-task overhead is fixed.
The pruning benefit varies by query shape. So the cost-benefit goes
positive or negative based on factors the wiring decision never
considers.

The 5 failed attempts tried to reduce per-task cost via caching
(Arc::ptr_eq, OnceLock, bound-cache). All hit Mutex contention or
partial-seal traps. The cache attempts were solving the wrong
problem. Caching reduces the per-task cost when the cost is paid.
The real fix is **don't pay the cost when the filter can't help**.

Five candidate paths, ranked by depth of fix:

### A. Read Parquet bloom filters in iceberg-rust's row-group evaluator

Deepest fix. Today the evaluator reads only min/max statistics. Bloom
filters can prune **high-cardinality unsorted columns** (custkey,
suppkey, partkey) where min/max can't.

Concretely: when the evaluator sees `Predicate::Set(col, values)` and
min/max returns MIGHT_MATCH, fall through to the column's Parquet
bloom filter chunk. Hash each value, check membership. If no value
hits the bloom, the row group provably has no matching rows. Skip.

We **already write** Parquet bloom filters on join-key columns when
`write.parquet.bloom-filter-columns` is set (matrix-f, commits
`eb95e72`, `9172dc3`). The data is on disk. The reader does not
consult it.

This is what Trino does internally and why Trino wins on q3.x and
q4.x where we lose. It composes with everything else and addresses
the root cause for the entire regression class.

Cost: vendored iceberg-rust patch. Bounded scope. Plumbing into the
existing `row_group_metrics_evaluator` path.

### B. Predicate-shape-aware wiring (practical, shippable next)

Translate the runtime filter once at scan-builder time, inspect the
resulting `Predicate`, then wire to `with_dynamic_predicate` only
when the shape can prune:

```rust
let dp = RuntimeFiltersDynamicPredicate::new(filters);
match dp.current() {
    Some(Predicate::AlwaysFalse) => sb = sb.with_dynamic_predicate(dp), // free win
    Some(Predicate::Binary(_))   => sb = sb.with_dynamic_predicate(dp), // bounds prune sorted cols
    Some(Predicate::Set(_, vals)) if vals.len() <= 200 => {
        sb = sb.with_dynamic_predicate(dp);                              // under iceberg-rust IN_PREDICATE_LIMIT
    }
    _ => { /* skip wiring; post-decode filter still applies */ }
}
```

This avoids the IN_PREDICATE_LIMIT trap (cause #1), the OR-translation
failure trap (cause #5), and never pays the eval cost when the
predicate can't translate to a prunable shape.

Open issue: at scan-builder time the build side may not have sealed
yet. `current()` returns `None` or `AlwaysTrue` and we skip wiring.
Later when the build seals, the predicate is prunable but the wiring
decision is already made.

Mitigation: keep the SQE post-decode filter as the safety net. Net
effect: B is **strictly additive on top of Path B**, no SF1
regression, real wins on the queries where the predicate translates
to a pruneable shape.

Bounded scope, ~50 lines in `iceberg_scan.rs`.

### C. Column-layout-aware wiring (catches the data shape)

At scan startup, inspect manifest data: for each column referenced
by a runtime-filter target, compute the variance of per-file
min/max ranges:

- High variance (different files have different ranges) means the
  column is clustered. Wire predicates targeting it.
- Low variance (every file has roughly the full range) means the
  column is shuffled. Skip wiring.

Catches `lo_orderdate is sorted` vs `lo_custkey is shuffled`
structurally, regardless of filter shape. Combine with B: wire only
when both the predicate shape is prunable AND the probe column has
differentiated stats.

Cost: one-time per scan. Manifest data already loaded. Cheap.

### D. Don't double-evaluate

When wiring `with_dynamic_predicate`, iceberg-rust's RowFilter
applies the predicate post-decode. SQE's per-batch loop also applies
it post-decode. The same predicate runs twice on the same surviving
rows.

Track which predicates were handed to iceberg-rust. Skip those in
the SQE post-decode loop. Apply only the predicates that didn't
translate.

Independent of A/B/C, removes a small redundant cost. Worth a few
percent on queries where multiple filters translate.

### E. Plan-time cardinality estimation (orthogonal)

Pre-evaluate constant dim filters at plan time using tracked Iceberg
metadata. If the result is provably empty (no manifest entries match
the filter), replace the join subtree with `EmptyRelation`. Captures
q3.4 / q2.2 / q2.3 SSB and any TPC-DS empty-result query.

Doesn't touch the per-task surface. Pure plan-time logic. Bounded
scope.

### Recommended order: B + D + E first, then A, then C

B first because it is the smallest change with the cleanest signal:
ship predicate-shape gating, watch the bench. Should reclaim the
q3.4 / q1.1 wins without the q3.3 / q4.2 regressions. If B alone
nets positive on SSB SF10, the per-task surface is done for this
round.

D as a follow-up cleanup. Doing the same predicate twice is wasted
work once we know about it.

E in parallel. Solves the 0-row dim case fundamentally. Composes
additively with B (B catches prunable shapes that aren't 0-row, E
catches 0-row before the scan even starts).

A as a focused follow-up MR after B + D + E land. Deepest fix but
touches the upstream evaluator. Better measured against a clean
baseline.

C as a refinement of B once A and E are in place. C only helps in
cases B doesn't already catch.

The thing all 5 failed attempts missed: the cost reduction was
always inside the per-task path. The smart move is to **not enter
the per-task path at all when the predicate can't be prunable**.
B + C do that. A makes more predicates prunable.

## How to reproduce

```bash
# SF1 - quick smoke (~5 min)
BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpch

# SF10 - real-world signal (~25-40 min)
BENCH_SCALE=10 ./scripts/benchmark-test.sh --compare-trino tpch
```

Result JSONs land in `benchmarks/results/`. Compare against historical
baselines:

```bash
# All TPC-H SF10 results, oldest first
ls -tr benchmarks/results/compare-tpch-sf10-*.json
```

The relevant baselines for this work:

| date / file | SQE total | label |
|---|---:|---|
| `compare-tpch-sf10-2026-04-21T11:32:08.json` | 163,858ms | Pre-DECIMAL Float64 baseline |
| `compare-tpch-sf10-2026-04-27T18:38:53.json` | 148,511ms | Path B (post-batch only) |
| `compare-tpch-sf10-2026-04-27T19:19:28.json` | 143,590ms | Path B + B-2 (current head) |

## Open follow-ups

| item | size | priority |
|---|---|---|
| Multi-join queries (q08, q09, q04) still regress slightly vs Path B | medium. needs upstream `generation` API or equivalent cache key | medium |
| Decimal Datums not yet handled in the physical converter | small. extend `scalar_to_datum` once `iceberg::spec::Datum::decimal` accepts a raw i128 + (precision, scale) | low for TPC-H, blocks decimal-keyed hash joins |
| q15 CTE re-scan independent of Path B-2 | requires DataFusion CTE materialization (DF 53 has none); SQE-level rewrite alternative is fragile | low |
| Trino bench reliability at SF10 | container OOM / timeout on q18+ in some runs; not a SQE bug | low |

## Negative result: bloom-on-write does not compose with Path B-2

A side branch (`feat/bench-bloom-on-join-keys`, commit `f022619`)
explored writing Parquet bloom filters on TPC-H/SSB join-key columns
at data-generation time. The hypothesis was that blooms would prune
row groups when the runtime filter could not, and the two would
compose multiplicatively.

The actual numbers, measured before and after Path B-2 landed:

- **SF1**: bloom-on-write was already a regression on its own (+24%
  slower) because at SF1 the per-row-group bloom evaluation overhead
  exceeded the prune benefit. DataFusion only consults blooms for
  *literal* equality predicates, not for the build side of a hash
  join, and TPC-H has no literal predicates on join keys.
- **SF10 with Path B alone** (pre-B-2): bloom-on-write recovered the
  SF1 cost and produced -7.5% wins on q06 / q07 / q14 because larger
  row groups tipped the cost-benefit toward bloom pruning.
- **SF10 with Path B-2**: bloom-on-write *regressed* by +25.9s when
  layered on top. Path B-2's runtime filter already prunes the row
  groups the bloom would address; the bloom adds eval overhead with
  no incremental benefit.

The takeaway is that bloom-on-write and runtime filter pushdown
target the same row-group pruning surface for join keys. Path B-2's
runtime filter is more selective: it carries actual min/max bounds or
in-list literals from the build side, and arrives at the reader
through the same `DynamicPredicate` machinery the static predicate
uses. Adding a parallel bloom probe burns CPU on row groups Path B-2
has already pruned.

The branch is deliberately unmerged. The matrix `bloom-filters:v2/v3`
cells are still `full` because the per-table bloom write path is
correct end-to-end (verified by the
`writer_props_emit_bloom_filter_in_parquet_footer` test in
`crates/sqe-catalog/src/parquet_writer_config.rs`): users who ask for
blooms via `write.parquet.bloom-filter-columns` get them. The
negative result here is specifically about *forcing* blooms on join
keys at bench-data-generation time, which is a benchmark-stack
choice rather than a property of the engine's bloom support.

When blooms still help (and the per-table path covers):

- Literal predicates on bloomed columns at scan time
  (`WHERE bloomed_col = 5`)
- Point-lookup workloads with skewed value distributions where
  column min/max stats provide a wide range
- IN-list filters with a small constant set on a bloomed column

When blooms do not help (the bench-bloom-on-write path):

- Hash join build-side filtering on join keys (Path B-2 covers it)
- Range scans on dense integer columns (min/max stats already do
  this for free)
- Anything where the runtime filter or static predicate has already
  pruned the row group

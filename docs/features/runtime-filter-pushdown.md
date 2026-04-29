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

### Resolution

All five attempts reverted; branch is back at `5eeb00c` (clean
Path B-2 plus the criterion microbench).
Postmortem comment posted to
[apache/iceberg-rust#2376](https://github.com/apache/iceberg-rust/issues/2376#issuecomment-4330042368)
with API recommendations for upstream:

1. Expose a cheap monotonic version (`generation`) on
   `DynamicFilterPhysicalExpr` so consumers can cache without relying
   on `Arc::ptr_eq`, OR
2. Have the `DynamicPredicate` trait return an opaque `(Predicate,
   version)` so the reader can manage its own cache key.

Either is small and forward-compatible.

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

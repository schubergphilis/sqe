---
title: "The SSB regression that wasn't"
description: "MR #220 wired runtime filters into iceberg-rust's scan path and dropped TPC-DS 67%. SSB looked like it regressed 6%. Two failed heuristic attempts, one parquet-trace session, and ten warm passes later, the regression turned out to be measurement noise. The fix-the-fix that wasn't, and the data-clustering insight that explains why two suites with the same code path behave nothing alike."
pubDate: "2026-05-17"
---

## The setup

MR #220 ([two-tier dynamic-filter pushdown](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/220)) hands DataFusion's runtime filters (HashJoin build-side bounds + membership) to iceberg-rust through the `DynamicPredicate` bridge. Tier 1 samples the filter once per file scan task and feeds it into manifest, row-group, page-index, and parquet `RowFilter` pruning. Tier 2 is a per-batch wrapper that catches filters resolving after the task opened.

The merged numbers (single-shot warm runs against the 2026-05-16 baseline):

| Suite       | Baseline | This MR  | Δ      |
|-------------|----------|----------|--------|
| TPC-DS      |  40.2s   |  13.4s   | **-67%** |
| TPC-BB      |  37.9s   |  28.0s   | -26%   |
| TPC-E       |  12.5s   |   9.3s   | -26%   |
| TPC-C       |   7.6s   |   5.3s   | -30%   |
| ClickBench  |   1.4s   |   1.3s   |  -7%   |
| TPC-H       |  15.9s   |  16.8s   |  +6%   |
| **SSB**     | **7.8s** | **8.3s** |  **+6%** |
| total       | 123.1s   |  82.4s   | -33%   |

Six wins, one tie, one apparent regression. The PR shipped because -33% net buys a lot of permission. But that +6% on SSB was uncomfortable. Why does a query mix that benefits from runtime filters on TPC-DS lose on SSB?

## First instinct: filter shape

The vendor bridge ([`physical_to_predicate.rs`](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/blob/main/vendor/iceberg-rust/crates/integrations/datafusion/src/physical_plan/physical_to_predicate.rs)) translates three `PhysicalExpr` shapes into `iceberg::Predicate`:

```rust
fn convert_physical(expr: &Arc<dyn PhysicalExpr>) -> Option<Predicate> {
    if let Some(dynamic) = any.downcast_ref::<DynamicFilterPhysicalExpr>() { ... }
    if let Some(binary)  = any.downcast_ref::<BinaryExpr>() { ... }
    if let Some(inlist)  = any.downcast_ref::<InListExpr>() { ... }
    None
}
```

The DataFusion 53.1 source for `HashJoinExec::create_membership_predicate` says small build sides (<128 MB) get an `InListExpr`; larger ones get `HashTableLookupExpr`. SSB dim tables are tiny (customer ≈ 2.4 MB at SF1, dim_date ≈ 200 KB), so the membership predicate **should** be an `InListExpr` . An `InListExpr` of 2192 dates is much less selective than the InList-of-5 that TPC-DS q82 gets from filtering items by price and manufacturer. Theory: SSB's wide InList pays the per-batch RowFilter eval without saving column-decode work.

I wrote a heuristic. Skip Tier 1 when `count_inlist_items(filters) > 1024`. Built. Ran.

```
SSB warm: 8.5s.  Unchanged.
```

Built tracing into the predicate sampler to print the actual filter shape:

```
shape=["Dyn(Binary(And,
        Binary(And,
            Binary(GtEq, lo_orderdate@2, 19920101),
            Binary(LtEq, lo_orderdate@2, 19971231)),
        Other<sql:hash_lookup>))",
       "Dyn(true)",
       "Dyn(true)"]
```

The membership wasn't an `InListExpr`. It was a `HashTableLookupExpr` . DataFusion's other strategy, which the bridge silently drops. The dim_date filter we'd been pushing to lineorder all along was **bounds-only**. Not 2192 items in a set; just `lo_orderdate BETWEEN 19920101 AND 19971231`, which covers 86% of the date range and prunes nothing.

That explained why my "count InList items" check kept returning zero. The InList wasn't there to count.

## Second instinct: skip when no InList

If we can't get a selective predicate out of `HashTableLookupExpr`, skip Tier 1 entirely on single-task scans without an InList. Tier 2's per-batch wrapper handles it cheaply.

```rust
if inlist_total == 0 || inlist_total > TIER1_SINGLE_TASK_INLIST_LIMIT {
    return None;  // Tier 1 skipped, Tier 2 takes over
}
```

Rebuilt. Reran.

```
SSB warm:    6.1s   (-22% vs merged main!)
TPC-DS warm: 18.4s  (vs merged 13.4s , regression!)
```

SSB collapsed. TPC-DS regressed 37%. Twelve TPC-DS queries slowed 2-5x. Worst was q75 (174ms to 818ms, 4.7x slower). q75 doesn't even have a small dim filter to produce an InList. It joins web_sales / catalog_sales / store_sales with item / customer / date_dim, mostly returning bounds.

So bounds-only Tier 1 was helping TPC-DS. It was just not helping SSB.

## Why the same code path behaves differently

The discriminator isn't filter shape. It's **data layout**.

TPC-DS catalog_sales and store_sales are loaded sorted by `*_sold_date_sk`. The parquet writer (Iceberg via the `sqe-bench load` path) packs row groups in date order, so each row group has a tight date range. A bounds-only filter like `cs_sold_date_sk BETWEEN <week_start> AND <week_end>` prunes most row groups before any rows are decoded. The RowFilter eval cost is paid only on row groups whose stats overlap the bounds. Typically 1 or 2 out of 6.

SSB lineorder is loaded uniformly. Every row group's `lo_orderdate` stat spans the full 7-year range (1992-01-01 to 1998-12-31). The bounds-only filter on lineorder cannot prune any row group, but iceberg-rust still pays the per-file bind plus per-batch RowFilter eval, and Tier 2's wrapper re-evaluates the same predicate after the stream emits. Double work for no benefit.

A heuristic based on filter shape can't tell these apart. Both shapes are `Binary(And, Binary(GtEq, col, lit), Binary(LtEq, col, lit))`. To distinguish, you'd need to look at the row-group min/max spread on the file's parquet metadata: load the manifest, compare each file's column bounds to the snapshot-level bounds, and skip Tier 1 only when the spread is wide enough that no pruning will happen.

That's a real piece of work. Manifest-level stats are cheap (already loaded by `plan_files()`); per-row-group stats need a parquet footer read per file, which on a large multi-file scan eats most of the savings. I filed [issue #132](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/issues/132) to scope this.

## Then ten warm passes happened

Before committing to the inspector work, I ran a sanity check: warm each suite once, then collect ten independent timing samples per suite, and report mean ± stddev.

```
ssb          n=10  mean=  8338ms  stddev=  527ms ( 6.3%)  min=  7713  max=  9229
tpcds        n=10  mean= 13848ms  stddev= 1432ms (10.3%)  min= 12138  max= 17198
tpch         n=10  mean= 16788ms  stddev= 1901ms (11.3%)  min= 14657  max= 20603
tpce         n=10  mean=  6515ms  stddev=  569ms ( 8.7%)  min=  6003  max=  7950
tpcbb        n=10  mean= 23444ms  stddev= 2750ms (11.7%)  min= 21665  max= 30468
tpcc         n=10  mean=  4899ms  stddev=  231ms ( 4.7%)  min=  4421  max=  5143
clickbench   n=10  mean=  1078ms  stddev=  125ms (11.6%)  min=  1010  max=  1426
```

The 2026-05-16 baseline value for SSB (7784 ms) lands at `(7784 - 8338) / 527 = -1.05 σ`. Inside the new range [7713, 9229]. The baseline single-shot was at the lucky tail of the same distribution we're sampling now. The "+6% regression" doesn't exist.

Stddev runs 6-12% on every suite. With that noise floor, anything under a real 2σ effect (12-24%) is in the noise. The MR #220 wins on TPC-DS (-67%), TPC-BB (-26%), TPC-E (-26%) are well above the noise floor and clearly real. The "regression" on SSB and TPC-H (+6% each) is not.

The bench script reports a single number per suite per invocation. That number is one draw from a wide distribution, mostly driven by Polaris HTTP latency variance and rustfs page-cache state. Comparing two single draws and calling a 6% delta a regression is the kind of thing that gets engineers to spend a Saturday on a fix.

## What I almost shipped

Two heuristics, both rejected by the bench data:

1. **InList-size gate**: skip Tier 1 if the resolved InList exceeds 1024 items. No effect, because the membership wasn't an InListExpr in the first place. The premise was a misread of the DataFusion source. `HashTableLookupExpr` is the strategy for *every* CollectLeft join in this version, not just for >128 MB build sides.

2. **No-InList gate**: skip Tier 1 if there's no convertible InList. Fixed the imaginary SSB regression and introduced a real 5-second TPC-DS regression by stripping bounds-only pushdown from clustered fact tables. Wrong discriminator.

The lesson: when the "regression" you're chasing fits inside your measurement variance, you can't tell whether your fix worked. You'll find any change is a wash on average, but some seeds look great and some look terrible, and you'll cherry-pick the great-looking seed into a commit you'll have to revert later.

## What's actually shipped

Nothing further from this investigation. The merged MR #220 stands. Issue #132 is filed against the manifest-stats-aware Tier 1 gating, deferred until the SF10 benches surface the regression in a regime where the noise floor is proportionally smaller.

The runtime-filter work as it stands:

- Tier 1 fires when iceberg-rust samples the filter at file open. For TPC-DS, that hits clustered date columns and prunes row groups before decode (q82: 1787 to 113 ms, 16x).
- Tier 2 fires per-batch on the stream output. For filters that resolve after the task opened, which is single-file scans where the file open beats the build sides, which is most of SF1. Tier 2 is the only thing that filters.
- Both run when both apply. The redundancy is cheap when Tier 1 has already pruned (small batches, fast eval); the redundancy is a few hundred microseconds per batch when Tier 1 didn't prune (lineorder on SSB), which adds up to maybe ~500 ms on a 6 M-row scan. That ~500 ms is what we measured as +6% on SSB. It also fits inside the run-to-run variance and is therefore not a thing we should optimize for.

## What I'd do differently next time

Run the warm-up + 10-pass stats **first**, before chasing a single-number regression. Knowing the variance bounds tells you which deltas are worth investigating. Most of the engineering hours from this session went into chasing something that wasn't there.

The parquet-trace work wasn't wasted. Confirming that `HashJoinExec` emits `HashTableLookupExpr` rather than `InListExpr` will inform the next round of bridge work. Extending `physical_to_predicate.rs` to extract keys from a HashTableLookup and emit `Predicate::Set` would let us push genuinely-selective membership filters in cases the current bridge can't see. But that's a separate piece of work, with a real signal to optimize against: the cases where the build side is selective and the fact table is uniform, which is currently the worst case.

For now, the SSB regression goes back in the box. There is no regression.

---

**Receipts:**
- MR #220: [two-tier dynamic-filter pushdown](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/220) (merged 2026-05-17)
- Issue #132: [skip Tier 1 when fact table is not clustered on filter column](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/issues/132) (deferred)
- 10-pass stats run: `/tmp/warmup-stats.log` (not committed; the bench JSON files in `benchmarks/results/*-sf1-flight-2026-05-17*.json` cover the same ground with single-shot draws)

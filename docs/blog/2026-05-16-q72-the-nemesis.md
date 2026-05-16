---
title: "q72, our nemesis, and the Int32 that hid for a month"
description: "TPC-DS q72 sat at 10 seconds while every other query ran in under 1.4. Five days of investigation chased scan parallelism, range-based NDV, iceberg-rust upgrades, and the RisingWave fork. None of those were the bug. The bug was a silently-skipped Err arm in our dynamic-filter evaluator that swallowed every Int32 vs Int64 type clash. Fixing it: 15.5s to 0.77s. q72 now beats Trino."
pubDate: "2026-05-16"
author: "Jacob Verhoeks"
tags:
  - "performance"
  - "datafusion"
  - "iceberg"
  - "debugging"
---



*May 16, 2026*

Every benchmark suite has one query that breaks the chart. For SQE on TPC-DS SF1 it was q72. Ten point seven seconds while every other query in the 99-query sweep ran in under 1.4. The next slowest was q09 at 1.16. q72 alone burned a quarter of the total runtime.

It sat there for a month.

This post is about why it took so long to find, what we chased that turned out to be wrong, and what the actual bug was. Spoiler: an Int32 column, an Int64 literal, and an `Err` arm that quietly said `continue`.

## What q72 looks like

```sql
SELECT i_item_desc, w_warehouse_name, d1.d_week_seq,
       SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
       SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory          ON cs_item_sk        = inv_item_sk
JOIN warehouse          ON w_warehouse_sk     = inv_warehouse_sk
JOIN item               ON i_item_sk          = cs_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
JOIN date_dim d1        ON cs_sold_date_sk    = d1.d_date_sk
JOIN date_dim d2        ON inv_date_sk        = d2.d_date_sk
JOIN date_dim d3        ON cs_ship_date_sk    = d3.d_date_sk
LEFT OUTER JOIN promotion ON cs_promo_sk      = p_promo_sk
WHERE d1.d_week_seq       = d2.d_week_seq
  AND i_current_price     BETWEEN 1.00 AND 1.00 + 1.00
  AND hd_buy_potential    = '501-1000'
  AND d1.d_year           = 1999
  AND cd_marital_status   = 'D'
  AND d3.d_date            > d1.d_date + INTERVAL '5' DAY
  AND inv_quantity_on_hand < cs_quantity
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq
LIMIT 100;
```

Nine joins. Three of them on `date_dim`. Two cross-table inequality predicates. The selective filters (`i_current_price BETWEEN 1 AND 2`, `d_year = 1999`, `cd_marital_status = 'D'`) live on the dim tables, not on the facts. Catalog_sales has 1.44M rows at SF1. Inventory has 11.7M. The join key `item_sk` ranges over 18,000 distinct values that appear repeatedly on both sides, so a naive `inv x cs` join blows up into a 47M-row intermediate before any dim selectivity can apply.

That intermediate was where every CPU second went.

## The four wrong answers

### Wrong answer 1: scan parallelism

The first hypothesis was that our `IcebergScanExec` was running single-partition and the join was therefore single-threaded. That had been the symptom of an earlier regression (issue #131), which we fixed by removing automatic `target_partitions` wiring.

EXPLAIN ANALYZE killed that hypothesis cleanly. The bottom `inv x cs` join was already running `mode=Partitioned` with `RepartitionExec(Hash[item_sk], 11)` on both sides. DataFusion's `EnforceDistribution` rule had inserted the local shuffle. The join ran across 11 partitions, used 83 CPU-seconds of work, took roughly 7.6 seconds of wall time. Parallelism was fine. The join was just doing too much.

### Wrong answer 2: range-based distinct_count

DataFusion's docs say swapping a join's build and probe sides can be a thousand-to-one runtime difference, and the swap is driven by cardinality estimates derived from `distinct_count` column statistics. Our `compute_table_statistics` returned `distinct_count: Absent` for every column. The fix looked obvious.

We added an estimator: `min(num_rows, max - min + 1)` for integer types, `Absent` for strings and floats. The reasoning was that `max - min + 1` is a true upper bound on distinct integer values, and the row count is another. Take the smaller.

q72 went from 10.7s to 14.6s. The CBO took the new estimates, decided the bottom join's output was small enough that the partitioning overhead was not worth it, and dropped `mode=Partitioned` for `mode=CollectLeft`. We lost the implicit local shuffle and built one giant 11.7M-row hash table on inventory.

The estimate was technically correct as an upper bound. The CBO used it as a point estimate. Reverted on the spot.

The takeaway: for fact tables, range-based NDV lies to the optimizer because the keys are dense (every value in `[1, 18000]` appears, just many times each). The CBO needed a true sketch or actual sampling. Iceberg manifests carry min/max but no NDV. Implementing real NDV would be days of work. We did not have days.

### Wrong answer 3: upgrade iceberg-rust to 0.9.1

The 0.9.0 release notes called out a fast-path in `ArrowReader::read` for `concurrency=1` (apache/iceberg-rust PR #2020) plus LIMIT, Binary, and LIKE predicate pushdown in `iceberg-datafusion`. Q72's fallback scan path goes through `scan.to_arrow()` which routes through `ArrowReader`. The fast-path sounded promising.

Two surprises killed this option. First, our `vendor/iceberg-rust/crates/iceberg/src/arrow/reader.rs` already had a `get_byte_ranges` implementation that parallelizes across multiple ranges up to a cap of 16, plus the `Vec<Arc<FileScanTask>>` memory optimization. Whoever vendored at commit `17fbaa6` ("upgrade to DataFusion 53 / Arrow 58 / Parquet 58") didn't take pure apache 0.8.0. They took a derivative that already carried both of those patches. PR #2020's `concurrency=1` fast-path was strictly less than what we already had.

Second, `iceberg-datafusion`'s LIMIT and pushdown improvements don't help SQE because SQE bypasses `iceberg-datafusion::IcebergTableProvider` entirely. We have our own `SqeTableProvider` and `IcebergScanExec` so we can keep per-user vended S3 credentials. The 0.9.x story for `iceberg-datafusion` is for people who use it directly. We don't.

A full vendor swap to 0.9.1 would mean re-porting 18 SBP patches (REST auth hardening, SigV4 for S3 Tables, runtime filter pushdown via DF 53's `filter_pushdown` API, dynamic-predicate sampling on the reader, plus cherry-picks from open upstream PRs we depend on). Days of work. For roughly no q72 gain.

### Wrong answer 4: the RisingWave fork

RisingWave maintains their own iceberg-rust fork at `risingwavelabs/iceberg-rust`, branch `dev_rebase_main_20260303`. Their HEAD commit was from yesterday. 72 patches ahead of apache main. Some of those patches are genuine perf wins, including their famous `aa84cd06 feat: parallel get byte ranges for parquet (#119)` and `73e30a18 feat: optimize plan files memory consumption (#64)`.

Both of those are already in our vendor. So is most of the rest of their q72-shaped patch set.

What RW has and we don't: accurate `ObjectCache` heap-size estimation (PR #153), parallel manifest loading inside `RewriteManifests` (PR #144), a copy-avoidance in `DeletionVectorWriter::write` (PR #150), a handful of transaction summary fixes. None of those move query latency. They're cache-quality, write-path, and correctness work.

That ended the iceberg-rust angle entirely. The fast path was already in. The remaining gaps were not perf-shaped.

## What the actual bug was

By this point we had EXPLAIN ANALYZE'd q72 with all five dynamic filters pushed into the `catalog_sales` scan. The plan said `pushed_down_filters=5`. The scan's output row count said `1,441,548`. Every row. Zero pruning.

We had read the relevant code path (`crates/sqe-catalog/src/iceberg_scan.rs`, the post-batch dynamic-filter loop around line 1080) and the comments looked right. Per-batch re-snapshot of `dynamic.current()`. Boolean evaluation. `arrow::compute::filter_record_batch`. Comments mentioned a `coerce_batch_types` step to widen `Int32` columns to `Int64` "because Iceberg often stores as Int32, but DataFusion promotes to Int64 in expressions."

We added diagnostic `warn!` logs in every branch of the filter loop, rebuilt, ran q72 once, and went to look at the warnings. Eighteen thousand log lines.

The dynamic filters had real values:

```
inv_warehouse_sk@2 >= 1 AND inv_warehouse_sk@2 <= 5 AND inv_warehouse_sk@2 IN (SET) ([1, 2, 3, 4, 5])
cs_item_sk@4 >= 1 AND cs_item_sk@4 <= 17999 AND hash_lookup
cs_bill_cdemo_sk@2 >= 6 AND cs_bill_cdemo_sk@2 <= 1920791 AND hash_lookup
cs_bill_hdemo_sk@3 >= 5 AND cs_bill_hdemo_sk@3 <= 7179 AND hash_lookup
cs_sold_date_sk@0 >= 367 AND cs_sold_date_sk@0 <= 732 AND hash_lookup
```

The `IN (SET) ([1, 2, 3, 4, 5])` on warehouse is selective. So is `cs_sold_date_sk` in `[367, 732]` (calendar days in 1999). Every filter looked like it should prune meaningfully.

Zero `filter applied` log lines. Eighteen thousand `ev returned Err, skipping` lines. The error:

```
Arrow error: Invalid argument error: Invalid comparison operation: Int64 >= Int32
```

We had widened the column to Int64 in `coerce_batch_types`. But the literal `1` in `>= 1` was Int32, because the dynamic filter's runtime min/max came directly from the build side's column, and the build side's column was a vended Iceberg `Int32`. After our widening: Int64 column on the left of `>=`, Int32 literal on the right. Arrow refused to compare them.

The Err arm of the eval match was a `continue`. Silent. Just kept walking the filter list. Every dynamic filter failed identically. Every batch. For a month.

The static-filter path worked the whole time because DataFusion's planner inserts CAST nodes during logical-to-physical translation, so `cs_sold_date_sk = 12345` becomes `CAST(cs_sold_date_sk AS Int64) = 12345` before it ever reaches us. Static filters had the CAST. Dynamic filters did not.

## The fix

```rust
let coerced = if is_dynamic {
    result.clone()
} else {
    PhysicalExprPredicate::coerce_batch_types(&expr, &result)
        .unwrap_or_else(|_| result.clone())
};
```

If the filter is a `DynamicFilterPhysicalExpr`, skip the widening. The runtime literals already match the column's native Iceberg type. Done.

Five lines.

## Numbers

| | Before | After |
|---|---:|---:|
| q72 SF1 | 10.7s | **0.68s** (15.6x) |
| q72 vs Trino | 10x slower | **1.4x faster** |
| TPC-DS SF1 total (99 queries) | 41.8s | **31.4s** |

The full sweep dropped 10 seconds. q72 fell from #1 slowest to #10, sitting behind q13 and q82 instead of dominating. Nothing else regressed: the other top-10 queries are within run-to-run noise of their previous timings.

## What made it hard to find

The bug had three properties that conspired against us.

First, the failure was silent. `Err(_) => continue` does not log, does not surface in metrics, does not show up in EXPLAIN ANALYZE. The plan node says `pushed_down_filters=5`. The scan emits 1.44M rows. There is no signal that those filters did nothing. We had to add the warn logs to see it.

Second, the type asymmetry was unintuitive. The comment on `coerce_batch_types` says "Iceberg often stores as Int32, but DataFusion promotes to Int64 in expressions." That's true for static filters. The author had not anticipated that DynamicFilterPhysicalExpr would carry Int32 literals from the build side directly, with no Cast wrapping. The fix is one branch on `is_dynamic`. The miss is one assumption that held for the cases we tested.

Third, every plausible-sounding hypothesis was load-bearing for a real performance lever in some other context. Scan parallelism IS the right hypothesis for some queries. NDV IS the right input for the CBO. Upstream upgrades DO unlock real wins. The patches we considered worked for the world we thought we were in. The world had a smaller, weirder bug in it.

## Lessons we took out of this

**Diagnostic logs over hypotheses.** We spent four days on three plausible theories before adding 30 lines of `warn!`. The logs settled it in one rebuild. If a perf problem is sticky and the public signals don't explain it, instrument first.

**`continue` and `unwrap_or` in hot paths deserve a log.** A silent fallback in a tight loop will hide work that isn't happening. Every `Err(_) => continue` arm should at least `tracing::trace!` what it skipped. The trace level costs nothing when disabled.

**Don't chase upstream when the symptom is local.** Two days went into iceberg-rust 0.9.1 and the RisingWave fork. The bug was in 30 lines of our own code, in a file we had touched a dozen times. The upstream investigation was useful (we now know exactly what 0.9.x adds, why our vendor isn't pure 0.8.0, and where RW diverges), but it was not the fix.

**Dynamic types from runtime are not the same as static literals from the planner.** DataFusion's logical-to-physical translation rewrites static predicates with explicit CASTs. Runtime-injected predicates skip that pass. Anywhere we accept user-pluggable physical expressions, we should assume the literals carry the build side's exact type, not the optimizer's preferred wide type.

## What's next

SF1 is settled. The dynamic-filter fix opens the door to SF10 testing, which is where the lineitem-heavy SSB queries and the bigger TPC-DS facts will exercise the runtime filter at meaningful row counts. We expect q72 to scale linearly with SF; the 0.68s at SF1 should land around 6-7s at SF10, well inside the 60-second timeout. The earlier 10.7s baseline at SF1 would have meant 100+ seconds at SF10.

There is still a real q82 / q37 cluster sitting 3-5x slower than Trino. Those are different shape: small-build, large-probe, where the build is *already* selective from a dim filter and the dynamic filter is meaningful. With Int32 literals now applying correctly, we expect those to move too.

We have no idea what we'll find next. That's the point.

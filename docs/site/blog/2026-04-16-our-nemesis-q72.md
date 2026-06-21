---
title: "Our Nemesis: TPC-DS Query 72 and the Limits of a Custom SQL Engine"
description: "One query. Ten tables. Twelve times slower than Trino. Everything we tried, what worked, what didn't, and where the ceiling is."
pubDate: "2026-04-16"
author: "Jacob Verhoeks"
tags: ["performance", "tpc-ds", "datafusion", "trino", "query-optimization"]
---

There is one query in TPC-DS that refuses to be fast.

SQE beats Trino on 55 of 99 TPC-DS queries at scale factor 1. The overall average is 1.6x in SQE's favour. On some queries the gap is dramatic: q01 runs 24x faster, q06 runs 6x faster, q64 runs 4x faster. The engine works. The caching works. The optimizer works.

Then there is q72. SQE: 16.8 seconds. Trino: 1.4 seconds. Twelve times slower. It accounts for 35% of SQE's total TPC-DS time. Without q72, SQE would win the entire suite.

This is the story of everything we tried.

## What q72 does

Q72 joins ten tables. Nine inner joins plus one left outer join. The core pattern is a star schema with a twist: two aliases of the same dimension table cross-referenced through a shared column.

```sql
FROM catalog_sales              -- 1.44M rows (fact)
JOIN inventory ON cs_item_sk = inv_item_sk  -- 11.7M rows (fact)
JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk
LEFT OUTER JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE d1.d_year = 1999
  AND hd_buy_potential = '501-1000'
  AND cd_marital_status = 'D'
  AND i_current_price BETWEEN 1.00 AND 2.00
  AND d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date + INTERVAL '5' DAY
```

After filtering, the dimension tables are tiny. date_dim d1 has 365 rows (d_year = 1999). household_demographics has 1,400 rows. item has 180 rows. warehouse has 5 rows. The optimal strategy is obvious: join the tiny tables first, use their keys to prune the fact tables, join the facts last.

Trino does exactly this. Its cost-based optimizer enumerates join orderings, picks the cheapest, broadcasts the small dimension tables, and uses dynamic filtering to push the dimension keys into the fact table scans. Inventory goes from 11.7 million rows to a few thousand before the expensive join.

DataFusion does not do this.

## Attempt 1: Table statistics for JoinSelection

DataFusion's `JoinSelection` optimizer swaps the build and probe sides of hash joins based on row counts. It puts the smaller table on the build side. But it does not reorder the join chain. It processes joins in the order they appear in the SQL text.

We added `partition_statistics()` to `IcebergScanExec`, returning row counts and byte sizes from the Iceberg snapshot summary. JoinSelection started making better build/probe decisions. TPC-DS improved from 0.8x to 1.2x overall.

q72 did not move. The join order remained catalog_sales x inventory first.

## Attempt 2: Star-schema join reorder rule

We built a custom `PhysicalOptimizerRule` that detects chains of `HashJoinExec` (inner joins only), sorts inputs by row count ascending, and rebuilds the chain with the smallest tables first. The rule has safeguards: only inner equi-joins, only when statistics are available, only when the fact/dimension ratio exceeds 10x, configurable via `[query] star_schema_reorder = true`.

TPC-DS improved further. q01 went from 4.3x to 24.2x. q06 went from 0.3x to 5.9x. The overall average rose to 1.6x.

q72 did not move. The rule saw the chain but the top-level join is a LEFT OUTER JOIN (promotion), and the initial implementation aborted on non-inner joins.

## Attempt 3: Handle LEFT JOIN boundaries

We modified the rule to treat LEFT OUTER joins as opaque boundaries instead of aborting. The INNER chain below the LEFT JOIN gets reordered. `transform_down` recurses into the LEFT JOIN's children, finds the INNER chain, and reorders it.

q72 still did not move. The rule fired on the INNER chain, but the flattened chain did not include enough inputs to trigger reordering (the complex join graph with three date_dim aliases and cross-references fragments the chain).

## Attempt 4: Broadcast join threshold

This was the breakthrough for everything except q72.

DataFusion's default `hash_join_single_partition_threshold` is 1 MB. Below this threshold, the build side is collected entirely in memory (broadcast mode). Above it, both sides are partitioned. For q72's dimension tables (date_dim at 5 MB, customer_demographics at 80 MB), the default was too low.

We raised the threshold to 64 MB. This matches Trino and Spark's broadcast strategies. The effect was immediate: q39 dropped from 1.9s to 0.9s. q46 flipped from 0.4x to 3.2x. q47 from 0.6x to 3.2x. SQE started winning queries it had been losing.

q72 barely changed. The problem is not the dimension tables. The problem is the catalog_sales x inventory join: 1.44 million rows times 11.7 million rows. Neither side is small enough to broadcast.

## Attempt 5: Dynamic filter type coercion

DataFusion 53's dynamic filters push build-side min/max values into probe-side scans at runtime. Hash join builds a hash table from date_dim (365 rows), extracts the min/max of d_date_sk, and pushes it to the catalog_sales scan. The scan skips Parquet row groups where cs_sold_date_sk falls outside the range.

This required fixing a type mismatch issue. Iceberg stores some columns as Int32. DataFusion promotes to Int64 in expressions. The dynamic filter compared Utf8 with Int32 and failed. We added type coercion in `PhysicalExprPredicate` (widen Int32 to Int64, Float32 to Float64 before evaluation). Filters that still fail after coercion return all-true gracefully.

This improved many queries but not q72. The inventory join's equi condition (`cs_item_sk = inv_item_sk`) operates on item keys with high cardinality. The min/max range covers almost all values. Dynamic filtering does not help when the key range is wide.

## Attempt 6: Enable DataFusion's dynamic filter pushdown config

We enabled `datafusion.optimizer.enable_dynamic_filter_pushdown = true` explicitly. This tells DataFusion's optimizer to insert dynamic filter nodes between hash join build and probe sides.

No measurable change on q72. The config was already effectively enabled through our manual dynamic filter wiring.

## Attempt 7: Column statistics from manifests (May 2026)

The April work gave DataFusion table-level row counts and byte sizes. Per-column statistics stayed `Absent`. Without min/max bounds and null counts, the optimizer could not estimate the selectivity of `i_current_price BETWEEN 1.00 AND 2.00`, could not predict the row count after filtering `date_dim` by `d_year = 1999`, and could not pick a sensible build/probe direction beyond the simplest two-table case.

We added `aggregate_column_statistics` to `pruning_stats.rs`. For each Arrow field, sum `null_value_counts` across data files, take the min of `lower_bounds`, take the max of `upper_bounds`. The result is one `ColumnStatistics` entry per projected column, in order. The async aggregation runs once at `TableProvider::scan` time and is cached on `IcebergScanExec`. `partition_statistics` stays synchronous as DataFusion requires.

The effect on TPC-DS SF1 was bigger than expected:

```
q72   24817ms -> 18547ms (-25.3%)
q39    1394 ->   977 (-29.9%)
q28     518 ->   317 (-38.8%)
q44    1573 ->  1300 (-17.4%)
q37    1431 ->  1248 (-12.8%)
q82    1648 ->  1410 (-14.4%)
q61     548 ->   503  (-8.2%)

TPC-DS SF1 total: 75205ms -> 59179ms (-21%); 93/99 still match.
catalog_sales now receives 5 dynamic filters at scan vs 1 before.
```

q72 finally moved. The deepest hash join still picks `inventory` as build side (DataFusion's reorder rule does not enumerate full join orders), but the upper-tree restructured: dimension filters apply earlier, and the wider dynamic-filter chain prunes `catalog_sales` row groups before they reach the join. q72 went from 24.8s back to ~16s, close to its April baseline. Still 13x slower than Trino, but no longer the runaway it had become.

## The q73 footnote

Right after column-stats landed, q73 looked 84% slower (458ms -> 842ms). The next run came back at 258ms. Same code, same query, different cache state, different plan. With more selectivity bounds available the optimizer has more degrees of freedom, and q73's `WHERE` clause is OR-heavy:

```sql
WHERE (date_dim.d_dom BETWEEN 1 AND 3 OR date_dim.d_dom BETWEEN 25 AND 28)
  AND (household_demographics.hd_buy_potential = '1001-5000'
       OR household_demographics.hd_buy_potential = '5001-10000')
```

OR predicates sit in a region where small changes in estimated row counts flip the join order, because each branch's selectivity is hard to estimate from min/max alone. With histograms or NDV we would estimate `d_dom IN (1,2,3,25,26,27,28)` at 7/31 = 22% selectivity. With only min/max we see `d_dom BETWEEN 1 AND 28` and assume nearly the full range.

We logged it, kept the change, and noted the lesson: better statistics make most plans better and a few plans more variance-prone. The fix when it bites is column histograms or NDV. Both are still upstream gaps in DataFusion (DF#3843 covers the broader CBO work).

## Why Trino wins

Per DataFusion issue #17494, the canonical upstream analysis of this exact problem:

1. **Full CBO with join enumeration.** Trino evaluates all possible join orders using table statistics including column-level NDV (number of distinct values). DataFusion only swaps build/probe sides. It does not reorder the join chain.

2. **Broadcast joins for all dimension tables.** Trino broadcasts date_dim, warehouse, item, household_demographics, customer_demographics. After broadcasting, dynamic filters prune the fact table scans before any data is read. Spark's contributor analysis showed that disabling broadcast made Spark as slow as DataFusion.

3. **Cache-efficient hash join.** DataFusion's `HashJoinExec` uses linked-list chain traversal for hash collisions. Profiling shows most time is spent in the `chain_traverse` loop. Trino's hash join is more cache-friendly. An upstream proposal for radix hash joins (issue #18939) addresses this.

4. **Pipeline parallelism.** Trino starts the probe side before the build side finishes. DataFusion processes joins sequentially. A recent PR (#19761) adds probe-side buffering but it is disabled by default because it conflicts with dynamic filters.

## The ceiling

q72 is a known hard query. Even Trino has issues with it under data skew (Trino issue #18539). The combination of a 10-table join, cross-referenced dimension aliases, a non-equi condition (`inv_quantity_on_hand < cs_quantity`), and the 11.7M row inventory table makes this a worst case for any optimizer that does not do full cost-based join enumeration with NDV.

DataFusion does not have full CBO. It is on the roadmap (issue #3843). Until it lands, q72 will remain SQE's nemesis.

## What we shipped anyway

The May SF1 numbers, after column statistics landed:

| Suite | SQE | Trino | Avg per-query speedup |
|---|---|---|---|
| TPC-H | 19.3s | 26.6s | 2.3x |
| TPC-DS | 57.1s | 39.7s | 1.4x |
| TPC-C | 0.45s | 3.4s | 9.6x |
| TPC-E | 10.4s | 138.8s | 7.8x |
| TPC-BB | 36.9s | 323.6s | 5.5x |
| ClickBench | 1.7s | 6.3s | 4.6x |
| SSB | 7.6s | 8.3s | 1.1x |

SQE wins six of seven suites at SF1. q72 alone accounts for 28% of TPC-DS total time. Without it, SQE wins TPC-DS comfortably.

The optimizations that got us here:
- DataFusion 53 upgrade (40x faster planning, hash join dynamic filters)
- 5-layer metadata caching (warm queries under 1ms overhead)
- Table-level statistics from Iceberg snapshot summary
- Column-level statistics aggregated from manifest entries
- Star-schema join reorder rule (dimension tables first)
- Broadcast threshold 64 MB (matches Trino and Spark)
- Dynamic filter type coercion (Int32 to Int64 widening)
- Dynamic filter execution in both scan paths (direct-read + fallback)
- Manifest-level file pruning with dynamic filter bounds

q72 is one query out of 99. It takes 16 seconds. It will get faster when DataFusion ships radix hash joins, full CBO with NDV, or probe-side buffering. Until then, we document it, track the upstream issues, and ship the engine that wins the other 98.

Sometimes the honest answer is: this is as fast as it gets today. And today is fast enough.

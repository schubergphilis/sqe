---
title: "The benchmark that lied, the oracle that didn't, and the day Trino was wrong"
description: "Our SF0.1 compare run looked great: zero mismatches across seven suites. Then we asked DuckDB to check the data and found that 16 'passing' TPC-DS queries had never selected a single row, TPC-C had zero warehouses, and the one real disagreement between SQE and Trino was Trino's fault. Plus: the dynamic filter that shipped 6 million rows because nobody carried it across a node swap."
pubDate: "2026-06-12"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "testing"
  - "duckdb"
  - "performance"
  - "datafusion"
---

*June 12, 2026*

This morning's compare run looked perfect. Seven benchmark suites, SQE and Trino side by side on the same Iceberg tables, every query diffed row by row. Zero mismatches. TPC-H matched 22 of 22 and ran 6.8x faster. The kind of report you screenshot.

It was hiding twelve broken queries and a benchmark suite with zero warehouses.

This post is about how we found that, what an independent oracle buys you, and the one query where the differential test fired and the engine that was wrong turned out to be Trino.

## Agreement on nothing

A differential benchmark harness runs the same query against two engines and diffs the rows. If they agree, the query passes. The blind spot is small and brutal: if the generated data contains no rows the query can select, both engines return empty, empty equals empty, and the harness prints Match.

We had patched this hole once already. Since last week the harness reports zero-rows-on-both as `Vacuous` instead of `Match`. That made the blind spot visible. At SF0.1, TPC-DS reported 29 vacuous queries out of 99.

Twenty-nine empty results at a small scale factor is easy to wave away. Small data, selective predicates, of course some queries come up empty. That explanation is plausible, comfortable, and untestable from inside the harness. Both engines read the same tables. If the data is wrong, they agree on the same wrong answer.

You need a referee that does not share the data path.

## DuckDB as the oracle

DuckDB ships the official TPC-DS data generator as an extension. `CALL dsdgen(sf=0.1)` produces spec-conformant data, the same data every published TPC-DS result is built on. That gives us an engine-free experiment: run all 99 queries inside DuckDB twice, once against official data and once against ours. No SQE in the loop. No Trino. Just two datasets and one engine.

A query that returns rows on official data and zero rows on ours is not a scale artifact. It is a generator bug.

Sixteen of our 29 vacuous queries failed that test.

The root causes were vocabulary. Our generator produced `ca_county` values from a random name generator; the qualification queries probe real county names like `Williamson County` and `Franklin Parish`. Zero overlap, so q10 and q31 could never return a row. Our items had 8 colors; dsdgen has 92, and q56 wants `slate`, `blanched`, `burnished`. Our item classes were `Class1` through `Class5`; the real ones are `personal`, `portable`, `romance`, `dvd/vcr players`, 99 of them, mapped per category. Our household demographics capped dependent counts at 5 where the spec crosses to 9, so every query windowing on `hd_dep_count` matched nothing.

The deepest one was q63. It filters on brand AND category AND class together. We fixed the categories. We fixed the classes. We gave items the official 76 brand base names. Still zero rows. The dsdgen output had one more secret: the brand name is a deterministic function of the category and class. Every `Electronics`/`portable` item is some `scholaramalgamalg #N`. Draw brands independently and the three-way conjunction in q63 is unsatisfiable. The data has structure the queries depend on, and the structure goes deeper than any column profile shows.

## Zero warehouses

TPC-C was worse. The compare reported 2 of 8 queries matched and 6 vacuous. The tables had rows. The joins returned nothing.

One line:

```rust
let num_warehouses = scale as i32;
```

At scale 0.1 that truncates to zero. The warehouse table itself was generated with `max(1)`, so it had one row with `w_id = 1`. But every foreign key in every other table went through `.min(num_warehouses)`, which pinned `c_w_id`, `d_w_id`, and `s_w_id` to a warehouse id of 0. A warehouse that does not exist. Every join against the warehouse table, in every query, returned the empty set. Both engines agreed, of course.

The fix is `.max(1)`. The lesson is not about integer truncation. It is that a differential test cannot see data bugs, ever, by construction. Both engines read the same files. You get a second opinion on the engine and no opinion at all on the data.

## The day Trino was wrong

Across all seven suites and both scale factors, exactly one query produced different rows on the two engines: TPC-DS q75. SQE said 57 rows. Trino said 55.

Our track record says assume SQE is the broken one. q75 computes a year-over-year sales ratio and keeps rows where the ratio is below 0.9:

```sql
CAST(curr_yr.sales_cnt AS DECIMAL(17,2))
  / CAST(prev_yr.sales_cnt AS DECIMAL(17,2)) < 0.9
```

The two extra rows had true ratios of 0.8983 and 0.8984. Trino computes `DECIMAL(17,2) / DECIMAL(17,2)` at scale 2. Both ratios round up to 0.90, the comparison fails, the rows vanish. DataFusion keeps a higher-scale quotient and keeps the rows.

DuckDB, on the same parquet files, returns 57 rows. SQE's 57 rows, value for value.

Decimal division scale is implementation-defined in SQL, so this is not a Trino defect you can file. But it is the difference between "our engine disagrees with Trino" and "our engine agrees with the reference implementation and Trino rounds away two rows." Without the third engine we would have spent the afternoon hunting a bug in our own decimal kernel that does not exist.

## The filter that never arrived

The same day's profiling work found the performance version of the same disease: a component that silently did nothing while every report said fine.

We test distributed execution on a deliberately hostile rig. One worker, distribution thresholds at zero, so every fact-table scan is forced over Arrow Flight even when a single file could run locally. Gates that decide whether code runs decide whether it gets tested.

Under that rig, SSB was losing to Trino on every query, uniformly, by about 1.5 seconds. The new per-query profiles showed why in one line: the lineorder scan shipped 6 million rows and 115 megabytes from the worker to the coordinator. Per query. The dimension joins above it would discard 96% of those rows on arrival.

DataFusion has runtime filters for exactly this. The hash join builds its small side, computes the key bounds, and pushes a dynamic filter down into the probe-side scan. Our local Iceberg scan honors them. The distributed scan did not, twice over. It never implemented the pushdown hooks, so the optimizer dropped the filters. And the plan rewrite that swaps the local scan for the distributed one runs after the optimizer, so even the filters already deposited on the local scan were discarded with the node.

The fix has three parts. The distributed scan now accepts dynamic filters. The swap carries them across. And at dispatch time, after a bounded 100ms wait for the join build sides, the filter snapshot is converted back to a logical expression and ANDed into the predicate that already rides the scan ticket. The worker applies it through the same two-phase parquet read it uses for static predicates. No wire change. A worker that ignores the predicate ships extra rows and the coordinator's join still produces correct results.

SSB q3.3's lineorder scan went from 6 million rows to 449.

TPC-DS SF1 under the forced-distribution rig went from 4.4x slower than Trino to 1.7x faster, on data that now actually exercises the queries.

One honest asterisk: SSB itself still trails Trino under the rig. DataFusion's filter snapshot is `lo_partkey >= 8 AND lo_partkey <= 79984 AND hash_lookup`. The range bounds convert to a predicate and push. The `hash_lookup` term is the actual selectivity, an opaque membership probe against the build-side hash table, and there is no logical expression for it. Shipping the build-side key set to workers as a bloom filter is the next piece of work, and it was on the roadmap before today. Now it has a number attached.

## What this cost and what it bought

The vocabulary fixes, the cross-product demographics, the weekly inventory snapshots, the brand correlation: a day of work, most of it spent in DuckDB diffing distributions rather than writing Rust. The validator went from 17 failures to 5 at SF0.1. The five survivors each represent 4 or fewer rows on official data and need correlation machinery we have not built: customers who buy in the same store across years, returns that trigger catalog repurchases. They are documented, not hidden.

The compare suite now means what it says. When TPC-DS reports 91 matched, 91 queries produced identical rows on two engines, on data the spec's own generator agrees is shaped right. When it reports 8 vacuous, we can name the correlation each one is waiting for.

Three rules fell out of this:

Agreement is not validation. Two engines reading the same broken data agree perfectly. Count your empty results and treat them as debt.

The oracle must not share the data path. DuckDB's dsdgen is the reference implementation, it runs in-process, and a validation pass costs minutes. There is no excuse for a homegrown generator without one.

When the engines disagree, get a third opinion before you debug. The bug you are about to hunt in your own code might be the other engine rounding at scale 2.

---
title: "The filter that rebuilt itself 14,600 times"
description: "A two-table TPC-H join that Trino ran in 2.2s took us 161s at SF10. We blamed partition layout, then single-node joins, then a subquery pattern. All three were wrong. A CPU profile and two timers found the truth: a runtime filter we pushed to the probe scan was getting re-snapshotted once per batch, and each snapshot rebuilt a 300,000-node expression tree. The fix snapshots it once. q12 went 161s to 2.7s, q17 176s to 7.1s, q10 from a 300s failure to 3.3s, with the result rows unchanged and no knob touched."
pubDate: "2026-06-15"
author: "Jacob Verhoeks"
tags:
  - "performance"
  - "datafusion"
  - "iceberg"
  - "benchmarks"
---

*June 15, 2026*

TPC-H query 12 is a two-table join. Orders against lineitem, a date range, a group-by on two columns. Trino runs it at SF10 in 2.2 seconds. We ran it in 161.

That kind of gap is not a slow join. A join that loses by 30 percent is a slow join. A join that loses by 70x is a different category of wrong, and the shape of the number tells you so before you read a single plan. Slow scans degrade gradually. Blow-ups like this come from a plan doing work that scales with something it should never have touched.

We got the diagnosis wrong three times before we got it right. The story is worth telling because the wrong answers were all reasonable, and the right answer was hiding behind a timer we had not thought to add.

## Three wrong answers

The first answer was partition layout. We had just loaded the TPC-H fact tables partitioned by month, and q12 filters on `l_receiptdate` while the partition key is `l_shipdate`. No pruning. The scan opens all 84 partition files. That is real, and it is a genuine cost, so it was easy to write down and move on.

The second answer was join distribution. At SF10 Trino hash-partitions its joins across workers. Our run was a single-node coordinator. Single node builds every join in one place, so of course it loses on the big ones. Also true, also tempting, also not the cause.

The third answer was the subquery family. We had spent the prior week on q95, a self-join that materialized because of a runtime-filter interaction. q17 and q20 have correlated subqueries. q12 sits next to them in the same suite. Pattern-matching said: same family, same fix.

We wrote all three into a document as the root cause. Then we ran the profiler, and the document fell apart.

## One scan, 161 seconds

`EXPLAIN ANALYZE` does not care about your theories. Here is q12, per operator:

```
IcebergScanExec (orders)    output_rows=2,571,931   elapsed=0.6 ms
FilterExec                  output_rows=308,523     elapsed=51 ms
IcebergScanExec             output_rows=305,427     elapsed=161,824 ms
HashJoinExec                output_rows=308,523     elapsed=28 ms
```

The join is 28 milliseconds. The filter is 51. One scan is 161 seconds and emits 305,000 rows. The entire query is one scan, and that scan is roughly a hundred times too slow for the work it reports doing.

So we isolated it. A bare count over lineitem with q12's predicates: 0.5 seconds. A full count over all 60 million rows: 15 seconds. The scan, on its own, is fast. The 161 seconds only appears when the scan runs inside the join.

The difference is one thing. Inside the join, that scan carries a runtime filter. The hash join builds a filter from its build side and pushes it down to the probe-side scan, so the scan can skip rows the join would reject anyway. It is an optimization. It is supposed to make the scan faster.

It was making it 300 times slower.

## What the runtime filter actually is

We assumed the cost was evaluating the filter. A big IN-list of join keys, checked against every probe row. The obvious suspect, and the one we had tuned before: raise or lower the distinct-value threshold and the IN-list turns into a hash-set probe or a linear scan. We toggled it. The query stayed slow. So the IN-list evaluation was not it.

We added two timers to the scan's per-batch filter step. One around the call that fetches the current filter snapshot. One around the call that evaluates it. Then we let q12 run and printed the running totals.

```
batches=2000   current()=20,611 ms   evaluate()=199 ms
batches=4000   current()=41,156 ms   evaluate()=399 ms
```

Evaluating the filter cost 0.4 seconds across 4,000 batches. Fetching it cost 41 seconds. The work was never in using the filter. It was in asking for it.

Here is why. On a partitioned hash join, the build-side filter is not a flat IN-list. It is a `CASE` over the row's hash partition: when the row hashes to partition 0, check partition 0's keys, when it hashes to partition 1, check partition 1's keys, and so on for all eleven partitions. Each branch carries that partition's build keys as a literal set. For q12 at SF10 that is eleven branches of roughly 28,000 keys each, about 300,000 literal nodes in one expression tree.

DataFusion's dynamic filter rebuilds that whole tree every time you ask for its current value. The call walks the tree to remap its children, which for a tree this size costs about ten milliseconds. We were calling it once per batch. A 15-million-row scan is around 14,600 batches. Ten milliseconds times 14,600 is 146 seconds spent rebuilding a filter we then used in under a millisecond.

The filter never changed across those 14,600 calls. The build side seals once, when it finishes loading, and never moves again. We were reconstructing the same 300,000-node tree from scratch for every 8,000 rows of probe input.

## Snapshot once

The fix is to stop asking. We snapshot the filter the first time it is sealed, cache that snapshot, and reuse it for the rest of the scan. While the build side is still loading, the snapshot is a tiny `true` placeholder, cheap to re-sample, so the not-yet-sealed case keeps polling. Once a real filter arrives, we take it once and never call back.

The safety argument is the part that makes this clean. The runtime filter is an optimization, not a correctness boundary. The hash join re-checks every probe row against its build side regardless of what the scan filter let through. A cached snapshot that is slightly stale can only pass a few extra rows into the join, which the join then drops. It can never remove a row that should have matched. So caching is free. There is no case where reusing the snapshot changes an answer.

We confirmed that. q12's output is byte-identical before and after: 154,379 rows on MAIL, 154,144 on SHIP.

## What it bought

Same rig, same default threshold, nothing tuned:

| Query | Before | After |
|---|---:|---:|
| TPC-H q12 | 161 s | 2.7 s |
| TPC-H q17 | 176 s | 7.1 s |
| TPC-H q10 | 300 s (failed) | 3.3 s |
| SSB q4.1 | 11.6 s | 6.8 s |
| SSB q3.1 | 5.6 s | 3.1 s |

The three TPC-H queries we had filed under three different wrong causes were one bug. They all run partitioned joins at SF10, they all push a partition-keyed `CASE` filter to a probe scan, and they all paid the per-batch rebuild.

SSB improved too, and that is the interesting part. SSB never exploded, so it never drew attention, but its star joins also push runtime filters to a fact scan, and they were paying the same tax in miniature. The fix took q4.1 from 11.6 to 6.8 seconds without our asking it to.

That last result retired a tradeoff we thought we had. The distinct-value threshold that controls IN-list pushdown was set high to help SSB and we believed lowering it would help these TPC-H cases at SSB's expense. There was never a tradeoff. Both sides wanted the same fix, and it had nothing to do with the threshold. We left the threshold where it was.

## What we took from it

The bug was an optimization that did not pay its own cost. The runtime filter exists to make the probe scan cheaper. On these queries it made the scan the entire runtime, and it did so through the bookkeeping around the filter, not the filter itself. We were measuring the wrong thing because we were measuring the part that was supposed to be expensive.

Two timers found it in one run. We should have added them before we wrote a word of root cause. The document with three wrong answers was written from plan shapes and intuition. The plan shapes were real and the intuition was experienced, and they still pointed at the wrong operator, because none of them measured the cost that mattered. The profiler that splits a number into its parts beats the reading that guesses which part is large.

There is an upstream thread here too. We have an open ask on iceberg-rust for a sealed-snapshot helper on the dynamic-filter API, a way to learn that a filter has stopped changing without reconstructing it to find out. Our cache is the downstream version of that. When the API lands, the cache gets simpler. Until then, the rule is the one the timers taught us: if a thing does not change, do not rebuild it 14,600 times to confirm.

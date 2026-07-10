---
title: "The fix that fixed nothing: SSB, dynamic filters, and a 4x bug"
description: "SSB is the one benchmark suite SQE loses to Trino, 2.5x at scale factor 10, while it wins everything else. The obvious cause was a single-threaded fact-table scan, so we built a planner rule to parallelize it. A correctness smoke caught the rule returning 240 million rows from a 60-million-row table, a latent 4x duplication bug we then fixed. Then the parallel scan turned out to change nothing: SSB did not move. The real cause is a dynamic filter that is a huge win on clustered fact tables and pure overhead on SSB's uniformly-distributed one. A slow number is a hypothesis, and the fix you are sure of can be perf-neutral."
pubDate: "2026-06-25"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "performance"
  - "datafusion"
  - "testing"
---

*June 25, 2026*

SSB is the suite we lose. At scale factor 10, SQE runs the Star Schema Benchmark in 34 seconds against Trino's 13. That is 0.39x, a 2.5x loss, and it is the only number in any suite where Trino is ahead. We win the rest, and not by a little: TPC-E by 13x, TPC-BB by 3x, ClickBench by 2x, TPC-H and TPC-DS comfortably. One suite refuses to come along.

The story we believed was clean. SSB is a star schema. A large `lineorder` fact table joins a handful of small dimensions. The dimensions broadcast into hash tables, and `lineorder` is the probe side. We had already established that the fact scan runs as a single output partition. One stream feeding one probe. Parallelize that scan across cores and SSB should catch up.

So we built it. A physical optimizer rule that finds the probe-side scan of a broadcast join and fans it out to N partitions, leaving the build side alone so it cannot trigger an older regression. Plan-shape tests first. Then, before trusting a single timing, a correctness smoke.

The smoke saved us.

With the rule on, `SELECT count(*) FROM lineorder` returned 240 million rows. The table has 60 million. Every aggregate in every SSB query came back exactly four times too large.

The bug was old and quiet. The scan's read path for large files asked the catalog for the whole table and ignored the per-partition slice it had just computed. At one output partition this is correct by accident, because one partition's slice is the whole table anyway. At many partitions, each partition that held a file went and read all of them. Four files, four partitions, every row emitted four times. The fix is small: plan the files once, then hand each partition only the files assigned to it. Results went back to exact, down to floating-point reassociation in the parallel sums.

Then we re-ran for speed, and the rule did nothing.

SSB with the parallel scan: 34 seconds. SSB without it: 32. Inside the noise. The hypothesis we had built a feature around was wrong. The decode was already parallel inside a single partition, split by row group across cores. Adding output partitions added no work to the cores that were already busy.

This is where the profile earns its keep. For a 6-second SSB query, the operators account for about 300 milliseconds. The hash join, the aggregation, the filter, all of it. The fetch from object storage is 50 milliseconds. The other 95 percent of the wall clock is the parquet decode and a per-batch runtime filter over 60 million rows. None of that is charged to an operator, which is exactly why parallelizing operators changed nothing.

ClickBench is decode-heavy too, and we win it by 2x. So it is not the decoder. It is something SSB does that ClickBench does not, and the difference is the dynamic filter.

SSB filters its dimensions on keys that are spread uniformly through the fact table. We push those keys into the scan as a runtime filter, the way every modern engine does. The filter prunes nothing, because no row group can be skipped when every row group spans the whole range of values. We still pay to bind it per file and evaluate it per batch, and a second pass re-evaluates the same predicate after the rows are decoded. On SSB the runtime filter is cost with no benefit. We do too much.

The same machinery is a large win where the fact table is clustered. TPC-DS sorts its facts by date. There the per-file min and max are tight, the filter skips most of a file before a byte is decoded, and one query drops from 1.8 seconds to 113 milliseconds. The feature pays for itself everywhere the data is sorted and taxes us where it is not.

We have a switch for exactly this: skip the runtime filter when the planned files are uniform on the filter columns. Turning it on bought 11 percent. Real, and not the 2.5x. The rest is raw decode and filter throughput, the kind of distance you close with vectorized execution, not a configuration flag.

Here is the honest ledger. The parallel-scan rule is correct, regresses nothing, and ships off by default, because it does not help the suite it was built for and we will not pretend otherwise. The 4x duplication it uncovered is fixed, and that bug was the most valuable thing the work produced. It sat latent behind a scan that was always pinned to one partition. It would have surfaced as silently wrong sums the day anyone turned parallel scan on. SSB is still the one suite we lose, but now for a reason we can name and point at, not a story we liked.

A slow number is a hypothesis until you reproduce it. A fix you are certain of can change nothing. And the smoke test you are tempted to skip is the one that catches the four-times-too-many rows.

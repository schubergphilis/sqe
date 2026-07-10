---
title: "One file, one thread, and the 910ms that explained the SSB gap"
description: "On SSB at SF1 Trino ran scan-heavy queries about 2x faster than SQE, even though we pruned just as well. A new line in our query profile found it: a 151MB lineorder file decoded on a single thread, 94% of a 969ms query spent waiting on one scan. The obvious fix (more partitions) was a trap that regressed q72 from 17s to 100s once before. The safe fix parallelizes decode inside the scan without changing the plan the optimizer sees."
pubDate: "2026-06-13"
author: "Jacob Verhoeks"
tags:
  - "performance"
  - "datafusion"
  - "iceberg"
  - "benchmarks"
---

*June 13, 2026*

On the Star Schema Benchmark, Trino was roughly twice as fast as SQE. SF1 and SF10, the same story. The scan-heavy queries, the ones that pull most of the fact table before the joins thin it out, lost by about 2x.

We already pruned well. Same files matched, same rows decoded after the predicate. The gap was not in what we read. It was in how fast we read it.

One line in a query profile told us where the time went. The fix that line suggested was the wrong one, and we knew it was wrong because we had shipped it once before and watched it regress our hardest query from 17 seconds to 100.

## The line that found it

Last week we added scan-visibility counters to the per-query profile: `files_matched`, `bytes_planned`, `bytes_scanned`, `rows_decoded`, `fetch_time`. The point was to make the scan stop being a black box. You can argue about join order all day; you cannot argue with how many bytes left the disk.

SSB q4.1 at SF1, the profile for the node directly above the lineorder scan:

```
RepartitionExec(RoundRobinBatch(11))  fetch_time=910ms
  files_matched=1  bytes_scanned=151MB  rows_decoded=<post-filter count>
```

Total query time was 969ms. The scan alone was 910 of it. Ninety-four percent of the query was one thread waiting on one file.

`files_matched=1` is the whole story. The lineorder fact table at SF1 is a single 151MB Parquet file, six row groups. `rows_decoded` already equalled the count after the date and dimension predicates, so pruning had done its job. We were not decoding rows we would throw away. We were decoding the rows we needed, all of them, on one core, while seven other cores sat idle.

Trino splits that one file into many ~64MB pieces and hands them to eight workers. We handed the file to one thread and waited.

## The obvious fix is a trap

DataFusion has a knob for this. Set `target_partitions` above 1 and the scan is supposed to fan out. We tried that exact thing months ago. It is worth walking through why it fails, because the failure is not obvious and the recovery cost us a day.

When the scan advertises more than one output partition, it does not advertise a useful one. It reports `UnknownPartitioning(N)`: N streams, no promise about which keys land where. DataFusion's `EnforceDistribution` rule reads that and draws a conclusion. It cannot run the hash join in `Partitioned` mode, because `Partitioned` needs `HashPartitioning` on the join key and `UnknownPartitioning` is not that. So it falls back to `CollectLeft`.

`CollectLeft` wants a single probe-side stream. The optimizer gets one by inserting a `CoalescePartitionsExec` directly above the scan. The shape that produces is almost a parody of parallelism: fan the file out to N threads, decode in parallel, then immediately funnel all N streams back into one, then build the hash probe single-threaded on the merged stream. Parallel I/O, instant re-serialization, fragmented single-threaded build. You pay the coordination cost and keep the single-threaded bottleneck.

That is not theory. The earlier attempt did exactly this, and it regressed TPC-DS q72 at SF1 from around 17 seconds to about 100. q72 is the query whose bottleneck is the join, not the scan. Hand it parallel partitions it cannot use and the `CoalescePartitionsExec` tax lands on the one query that can least afford it. We reverted it. (q72 has since had its own separate fix and now runs under a second; the 17-second figure is the pre-fix baseline of that era. The point stands: more partitions made the wrong query slower.)

The lesson from the revert is narrow and precise. Do not change the partitioning the optimizer sees. The optimizer made a correct decision given the partitioning we advertised. Change the advertisement and you change every downstream choice, including the ones that were already right.

## Parallelize under the covers

The scan needs to stay at one output partition. The optimizer should see exactly the plan it saw before: one stream, `CollectLeft` where it wants `CollectLeft`, no coalesce node injected. The parallelism has to happen below the line the optimizer can see, inside the single partition.

Our Iceberg reader already knew how to do this. It can take one `FileScanTask` and split it into byte-range subtasks, decode them on separate spawned tasks, and merge the results into the one output stream. Row groups are assigned to subtasks by midpoint, so each row group is read by exactly one subtask. No double reads, no torn row groups. The single output partition stays single. The work inside it goes wide.

The machinery was there. It was never triggered.

The target split size defaulted to 128MB, and the reader only splits a file when it exceeds twice the target. A 151MB file sits under 256MB, so it stayed whole, decoded on one thread, every time. There was no knob to lower the target either, so even knowing the behaviour you could not change it.

We threaded a configurable split target through the scan and set it to 32MB. The 151MB file now splits into roughly five subtasks, one per row group give or take, decoded across the spawned tasks. The plan the optimizer sees does not change at all. Same single output partition, same `CollectLeft`, no `CoalescePartitionsExec`. The q72 regression cannot recur, because the thing that caused it does not exist in this fix.

## Tuning is the whole game

32MB was not the first number. The temptation is to split small and go wide, so the first try was 16MB.

16MB over-splits. A 151MB file becomes ten or eleven subtasks where it has six row groups, so subtasks share row groups and each one pays a Parquet footer read and a task spawn. On a query whose bottleneck is the scan, you eat that cost and still come out ahead. On q72, whose bottleneck is the join, the scan was never the problem, so the extra footer reads and spawns are pure overhead. 16MB cost q72 about 17 percent: 797ms to 935ms.

32MB recovered q72 to 768ms, below its own baseline, while keeping the SSB win. The rule that fell out: aim for roughly one row group per subtask. Split finer than the row-group boundary and you re-read footers and spawn tasks for work that cannot be divided any finer anyway. The row group is the unit. Respect it.

q72 is the canary here for the same reason it was the casualty of the wrong fix. It is the query most sensitive to anything that touches the scan without helping it. If a scan change leaves q72 flat or better, the change did not leak cost into the join-bound case. 768ms against a 797ms baseline is the signal we wanted.

## Results

SF1, against baseline:

| Suite    | Before     | After      | Result                          |
|----------|------------|------------|---------------------------------|
| SSB      | 0.64x      | 1.45x      | 9.2s to 5.5s, now beats Trino   |
| TPC-DS   | -          | -14% total | q72 flat (797 to 768ms)         |
| TPC-H    | -          | -29% total |                                 |

SSB went from 0.64x of Trino to 1.45x. We were losing by a third; we now win by nearly half. In wall-clock that is 9.2 seconds to 5.5, a 1.7x speedup on the suite, and it crosses the line from slower than Trino to faster. TPC-DS total dropped 14 percent with q72 held flat. TPC-H total dropped 29 percent.

SF10 is where I owe you the honest asterisk.

| Suite    | SF10 result                                          |
|----------|------------------------------------------------------|
| TPC-H    | -22% total; q10 (had been timing out at 300s) finishes |
| SSB      | +15% (65.6s to 56.0s); still trails Trino ~2x        |

TPC-H at SF10 dropped 22 percent total, and q10, which had been hitting the 300-second timeout and failing outright, now completes. That is the kind of win that turns a red cell green. SSB improved 15 percent, 65.6 seconds to 56.0, and still trails Trino by about 2x.

The reason SSB stays behind at SF10 is the same mechanism that explains the SF1 win. At SF1 the fact table is one file, so going from one thread to five is a 5x change in scan parallelism. At SF10 the fact table is already four files, so the scan already had four-way parallelism before we touched anything. Splitting each file finer adds less, and the bottleneck shifts from decode-per-thread toward I/O wait.

The profile says it plainly. With the fix on, the SF10 lineorder scan still owns 97 to 99 percent of every SSB query, but the scan's `elapsed_compute` is around 20ms while `fetch_time` is several seconds. The decode is not the cost; the wait for bytes is. Effective throughput sits at roughly 175 to 218 MB/s and does not move with more decode threads. That number is the tell: our benchmark harness runs the coordinator as a host process reading object storage over a localhost port-forward, and the port-forward caps there. Trino reads inside the container network and is not throttled the same way. So part of the SF10 gap is the harness, not the engine, and the honest SF10 engine number needs the in-network rig where both sides read on equal footing. More decode threads cannot buy back a capped pipe. The fix is a clear win at SF1 and a partial win at SF10. It is not a cure.

## What this taught us about the gap

When two engines pick the same plan and prune the same rows, the gap between them is mechanical, not algorithmic. There is no clever rewrite to find. Both engines decided to scan, filter, and join in the same order, and they read the same bytes after pruning. The only thing left that can differ is how the work is divided across cores, and that lives below the query plan, in the reader.

The profile told us in one line. Before `fetch_time` was on the scan node, the SSB gap was a mystery you could attribute to a dozen things. After, it was 910 of 969 milliseconds on one node, and the diagnosis took the length of one report.

The obvious knob was a trap, and a regression canary caught it. More partitions is the answer the documentation hands you, and it is wrong here, because it changes the plan the optimizer sees and the optimizer was already right. The safe fix went the other way: leave the plan alone, parallelize the decode underneath it, and let q72 confirm that nothing leaked into the join-bound case. The split target is now a knob, set to one row group per subtask, and the engine reads one file on as many threads as the file has row groups.

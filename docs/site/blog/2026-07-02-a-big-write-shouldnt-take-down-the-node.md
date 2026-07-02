---
title: "A big write shouldn't take down the node"
description: "Reads on SQE spill to disk when they run out of memory. Writes did not. A large CTAS, a wide MERGE, or an oversized client upload could balloon a coordinator buffer past its memory limit and get the process OOM-killed, which takes every other query on that node with it. We gave the write path the same memory discipline the read path already had: pool-track the buffers that must exist so an oversized write fails as one typed error, and stream the ones that never needed to buffer at all."
pubDate: "2026-07-02"
author: "Jacob Verhoeks"
tags:
  - "write-path"
  - "iceberg"
  - "reliability"
  - "datafusion"
---

*July 2, 2026*

A query that runs out of memory should fail. It should not take its neighbors with it.

That distinction is the whole story here. On a shared coordinator, an out-of-memory kill is not a query failure. It is a process failure. The kernel picks the coordinator, sends a SIGKILL, and every query running on that node dies at once: the innocent ones alongside the one that asked for too much. The blast radius is the box, not the offender.

Our read path already knew this. A join with a build side too big to fit becomes a sort-merge join. A sort that overflows spills sorted runs to disk. A high-cardinality aggregate is the documented hard edge, and the fix there is to distribute it. Reads degrade. They get slower, they touch the disk, but they stay inside their memory budget and they fail as one query when they cannot.

The write path did none of that. Writes buffered, and the buffers were invisible.

## Where the writes were hiding memory

Some of the write path was already clean. CTAS and INSERT write Parquet in a streaming loop: read a batch, encode a row group, drop it, repeat. Peak memory is one batch plus one row group, flat no matter how many rows the SELECT produces.

The problem was everywhere else. Three buffers, each unbounded, each invisible to the memory pool that governs the rest of the engine.

The first was client ingest. A Flight `DoPut` upload was collected whole into a `Vec<RecordBatch>` on the coordinator before a single byte hit S3. A large client load balloons that vector with nothing watching it.

The second was the Copy-on-Write MERGE target. A CoW MERGE reads the target table into memory, runs the merge SELECT against it, and rewrites the affected files. Reading the target meant materializing the whole target into a scratch table first. On a big table that is the single largest buffer in the engine, and it existed before the merge did any work.

The third was file decode. UPDATE, DELETE, and Merge-on-Read all read affected data files one at a time and rewrite them. Reading one meant decompressing the entire file into Arrow in one shot. A wide Parquet file decompresses to several gigabytes, resident all at once, per file, and the pool never saw it.

None of these showed up on the memory watermark. None of them competed for the query budget. They just grew until the cgroup limit, and then the kernel made the decision for us.

## Layer A: make the buffers count

The first fix does not remove the buffers. Some of them have to exist. It makes them visible.

We wrapped the buffers that must materialize in a `TrackedBatchBuffer`. It reserves against the same DataFusion memory pool that joins and sorts already draw from, before it appends each batch. When a push would exceed the pool, it does not append. It returns a typed `ResourceExhausted` error and releases what it held.

The MERGE target read now fails part-way through its file loop if the target is larger than the pool, as one query, with a reason. The CoW and MoR file decode reserves both the compressed bytes and the decoded batches while they are live, so the peak during decompression is charged to the query that caused it. The reservations release when the read returns.

The behavior change is small to describe and large in practice. A write that used to OOM-kill the coordinator now fails itself and leaves the node running. Same failure, different blast radius. The offending query dies. Everything else on the coordinator keeps going.

## Layer B: stream what never needed to buffer

Tracking a buffer is the right answer only when the buffer is unavoidable. Two of the three were not.

Client ingest never needed the whole upload in memory. It feeds the decoded batch stream straight into the same streaming Parquet sink CTAS and INSERT use. Resident memory drops to one batch plus one row group, and there is nothing left to track because there is nothing left to buffer.

The MERGE output was the same shape. A CoW MERGE used to collect the entire merged full-outer-join result before writing it, and the merged output is at least as large as the target table. It now executes the join as a stream and feeds it to the streaming sink. The join and the CASE logic live inside DataFusion operators, which are pool-tracked and spillable, instead of an invisible vector on the side.

Streaming is strictly better than tracking when you can do it. A tracked buffer fails gracefully. A streamed one never fills up.

## The fanout problem

Partitioned writes have their own way to run out of memory, and it is not one big buffer. It is many small ones.

Writing partitioned data means one open Parquet writer per partition, each holding a row group in flight. A partition key with a few values is fine. A high-cardinality key opens thousands of writers at once, and the sum of their buffers is the blow-up.

The bounded fanout writer caps how many writers stay open. When a batch arrives for a new partition and the cap is full, the least-recently-written writer closes and flushes first, then the new one opens. Memory stays bounded by the cap instead of the cardinality of the key.

The cost is honest. Closing and reopening a writer for a partition you see again produces more, smaller files. That is small-file debt, and it is exactly what `CALL system.rewrite_data_files` exists to repair. You trade a memory ceiling for a compaction pass you were probably going to run anyway.

## What is on by default, and what is not

The tracking is on. `write_buffer_tracking` defaults true, so every deployment gets the Layer A safety net without asking. It also has an off switch, documented as a diagnostic rather than a tuning knob: if an accounting false positive ever denies a write that would have fit, you can disable the reservations without touching the streaming paths.

The rest is opt-in, and deliberately so. Streaming the MERGE target from the pinned files instead of buffering it, and the bounded fanout writer, are both off until we validate them against a live Polaris and S3 stack. We wrote the validation as a turnkey runbook: MERGE parity against the buffered path, the fanout cutover round-tripped through `rewrite_data_files`, tiny-pool forcing to prove the typed error fires, and a check that the auto-derived caps land where we expect.

```toml
[query]
write_buffer_tracking = true    # on by default: big writes fail typed, not OOM
merge_target_streaming = false  # opt-in: stream the CoW MERGE target
fanout_max_open_writers = 0     # 0 = auto (pool-derived, 8..64); opt-in bounded fanout
fanout_buffer_budget = "0"      # 0 = auto; byte budget for buffered fanout memory
```

Shipping the safety net on and the unproven paths off is the split we trust. The thing that changes the failure mode for everyone runs by default. The things that still need a live catalog to earn our confidence wait behind a flag until they have it.

## What we took from it

Writes deserve the same memory discipline as reads, and for a long time ours did not have it. The read path had years of attention on the operators that hold state. The write path streamed the easy case and buffered the rest, and the buffers were invisible because nobody had made them count.

The real lesson is about blast radius, not memory. A single-tenant engine can afford to OOM. The user who ran the query is the only one who loses. A shared engine cannot, because the query that asks for too much is never the only one running. Turning a process kill into a query error is not a performance fix and it does not make any single write faster. It changes who pays when a write is too big, from everyone on the node to the one who wrote it.

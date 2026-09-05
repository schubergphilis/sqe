# 504: Merge-on-Read DML paths accumulate their whole output in untracked Vecs

- Scope: sqe-coordinator write path (write_handler MoR DELETE/UPDATE/MERGE)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

The write-path memory-safety work (TrackedBatchBuffer, `cow-keep-buffer`,
`merge-target-buffer`) covers the copy-on-write paths and the MERGE target
read. The merge-on-read paths still buffer everything they will write in plain
`Vec`s that the memory pool never sees, then hand them to the buffered
`write_data_files_with_metrics`.

- Position-delete DELETE, `crates/sqe-coordinator/src/write_handler.rs:2424`
  and `:2452`: `Vec<(String, i64)>` with one heap `String` (the full S3 path)
  per deleted row. A delete of 100M rows holds roughly 100 x 10^6 x (~120 B)
  of path strings before `write_position_delete_files` sorts them (`writer.rs:368-372`,
  another full copy into `PositionDeleteInput`).
- Equality DELETE, `write_handler.rs:2607` and `:2637`: `key_batches` collects
  full-width matched rows for every file.
- MoR UPDATE, `write_handler.rs:3159-3160`, `:3200`, `:3222`: `new_row_batches`
  and `key_batches`.
- MoR MERGE, `write_handler.rs:4017-4018` and pushes at `:4064, 4076, 4091,
  4134, 4159, 4185, 4194`: each per-clause subquery is `df.collect()`ed
  (`run_merge_subquery`, `:3678-3691`) and appended.

Contrast the CoW paths that gate accumulation through
`TrackedBatchBuffer::gated(&pool, "cow-keep-buffer", ...)`
(`write_handler.rs:2238-2242`, `:2888-2892`) and the CoW MERGE that streams its
output (`write_data_files_streaming_with_metrics`, `:3625-3634`).

## Why it matters

`write.delete.mode = merge-on-read` is the mode recommended for large tables
with small deletes, so these are the paths that run against the biggest
tables. A wide WHERE or a large MERGE source turns the coordinator's resident
set into "size of matched rows" with no `ResourceExhausted` and no spill:
the process is OOM-killed, taking every concurrent session down. The
`memory_limit` the operator configured is not consulted. This is the same
failure class `docs/internal/specs/2026-07-02-write-path-memory-safety-design.md`
set out to remove.

## Proposed change

1. Position deletes: switch the accumulator to `Vec<(Arc<str>, i64)>` with one
   `Arc<str>` per file (paths are already sorted per file, so flush per file
   into the position-delete writer instead of collecting globally; the spec
   only requires ordering within a delete file). Track the Vec's bytes with a
   `WriteReservation`.
2. Equality DELETE / MoR UPDATE / MoR MERGE: replace `Vec<RecordBatch>` with
   `TrackedBatchBuffer::gated(...)` for both the new-row and key batches, and
   route the new-row side through `write_data_files_streaming_with_metrics`
   by wrapping the per-clause DataFrames in a stream (`execute_stream` instead
   of `collect`, chained with `futures::stream::select_all`).
3. Project key batches to identifier columns before buffering (the equality
   writer projects anyway) so the key side holds only PK bytes.

## Acceptance criteria

- A test with a 256 MiB `FairSpillPool` and a MoR DELETE matching more rows
  than the pool allows fails with `ResourceExhausted` naming the consumer,
  not with an abort.
- `grep -n 'Vec<RecordBatch> = Vec::new()' crates/sqe-coordinator/src/write_handler.rs`
  returns no hits inside `handle_delete_equality`, `handle_update_equality`,
  `handle_merge_equality`.
- Position-delete path holds at most one path allocation per file.

## Related

- `docs/internal/specs/2026-07-02-write-path-memory-safety-design.md`.
- Memory note "Write-path memory safety: MERGE done, ingest/fanout next" (the
  MoR side was not on that list).
- 508 (buffered writer duplication).

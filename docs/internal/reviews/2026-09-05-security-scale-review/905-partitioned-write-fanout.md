# 905: Partitioned streaming writes are unbounded by default and bypass shared sink accounting

- Area: Memory safety and large-data execution
- Priority: P2
- Order within area: 2
- Scope: security and large-data execution
- Severity: high
- Category: performance
- Effort: M
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`WriteHandler::fanout_limits` returns unbounded when both settings are zero (`crates/sqe-coordinator/src/write_handler.rs:804`, `:825`); these are the defaults (`crates/sqe-core/src/config.rs:470`). The streaming sink takes `TaskWriter` with fanout enabled (`crates/sqe-compaction/src/writer.rs:420`). The vendored fanout writer retains a `HashMap<Struct, B::R>` until close (`vendor/iceberg-rust/crates/iceberg/src/writer/partitioning/fanout_writer.rs:51`, `:75`, `:120`).

Even the bounded branch constructs `BoundedFanoutWriter` with `reservation: None` (`crates/sqe-compaction/src/writer.rs:388`). Its byte budget is an input-byte estimate and can overflow when the current partition is the only open writer (`:705`); it is not a strict process-memory cap.

## Why it matters

Streaming the input does not make sink memory constant. Unsorted inserts/CTAS touching many partitions retain many Parquet writers and their dictionaries/row-group buffers. With P active partitions and average buffered state B, the sink component grows approximately P × B, outside shared pool accounting. Concurrent bounded writes also multiply independent per-write budgets.

For illustration only, 10,000 writers averaging 1 MiB of buffered state consume about 9.8 GiB before scan/join memory. This is arithmetic, not a measured benchmark.

## Proposed change

Enable bounded fanout by default, wire reservations to the shared pool, and handle denied reservations explicitly. Account for writer state and oversize batches; flush/roll or reject before uncontrolled growth. Validate the resulting small-file tradeoff and cleanup on failure.

## Validation and acceptance criteria

Run unsorted partitioned INSERT/CTAS at 1k/10k/100k partition cardinalities, with concurrent writers and a fixed node budget. Measure RSS, pool reservations, throughput and file count; require bounded growth, exact committed rows and cleanup on rejection. No scale benchmark was run during this review.

## Related

504 discusses MoR result vectors and mentions fanout as future work. This finding establishes the separate live streaming INSERT/CTAS default and the missing shared reservation in the bounded branch.

## Resolution (2026-09-05)

Bounded fanout is now the default: `WriteHandler::fanout_limits` (`crates/sqe-coordinator/src/write_handler.rs`) auto-derives both caps from the coordinator pool when the knobs are 0, and `write_buffer_tracking = false` is the only route back to the unbounded `TaskWriter`. `FanoutLimits` carries the session memory pool; the bounded branch of `write_data_files_streaming` registers a `fanout-buffer` `WriteReservation` on it, and `BoundedFanoutWriter::write` reserves the post-write estimate before writing: a denial cuts over least-recently-written writers first and fails with a typed `ResourceExhausted` error once only the current partition remains, instead of the previous best-effort mirror. CTAS, INSERT and MERGE pass the session pool; Flight DoPut ingest has no session context and passes `None` (caps still apply). Tests: `bounded_fanout_pool_denial_cuts_over_then_fails_typed`, `fanout_limits_default_to_bounded_with_pool`. Compaction rewrites keep `FanoutLimits::unbounded()`. The 1k/10k/100k cardinality benchmark was not run.

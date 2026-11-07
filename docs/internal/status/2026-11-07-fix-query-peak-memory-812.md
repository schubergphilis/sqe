# 2026-11-07 fix/query-peak-memory-812

Stacked on !1686 (feat/audit-streaming-stats-812); the MR shows its two
commits until that one merges. Part of #812: the history needs a peak that
means something.

## What changed

- `query_memory.rs`: `QueryMemoryPool` keeps a `QueryPeak`, a high
  watermark of the query's own charged bytes, raised after every `grow` and
  every admitted `try_grow`. `query_peak(ctx)` reads it from a query
  context (`None` without a quota, `query.max_query_memory = 0`). A pool
  built from a context that already has one (`join_fallback::retry_context`)
  shares its counter, so the peak covers the retry.
- `query_handler.rs` (buffered) and `streaming.rs` (via a new
  `StreamFinalizer::query_peak`, returned by `open_stream`) record that peak
  as `peak_memory_bytes` on the tracker and the audit record. Before, both
  recorded `used_bytes` of the coordinator-wide pool at query end: the sum
  of every concurrent query, and nothing of a query whose memory was
  already freed.
- Docs: `operations/audit-logging.md` defines the field.

## Tests

- `query_peak_is_this_querys_high_watermark_across_retries`: another
  query's 600 bytes do not count, a shrink keeps the peak, a smaller retry
  keeps it, a larger retry raises the same counter.
- `peak_memory_is_the_querys_own_on_both_paths`: a buffered and a streaming
  `ORDER BY` through a real `QueryHandler` while 32 MiB is held on the
  coordinator pool; both records are above 0 and below 32 MiB. The old
  code fails it (the pool total includes the 32 MiB).
- `streaming_audit_event_carries_stats_and_timing_on_every_outcome` now
  asserts `stats.peak_memory_bytes == 300` (5000 held by another consumer)
  on success, error and cancel.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --lib`: 802
  passed, 1 ignored. `--test it audit`: 28 passed, 1 ignored. clippy
  `-p sqe-coordinator --all-targets -D warnings`: clean.
- No `make test-integration`: memory accounting only, no catalog or
  storage path changed.

## Left open

- Untracked allocations (outside DataFusion reservations) are not in the
  peak, as they are not in the quota.
- #812 (b) history read path, (c) web UI.

**NEXT:** #812 (b) `system.runtime.query_history` over the audit JSONL, or
#870 truncated string bounds, or #848 (`to_json` reads an `arrow.json`
marker).

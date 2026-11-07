# 2026-11-07 feat/audit-streaming-stats-812

Part of #812 (durable query history), step 1 of the plan: the audit log gets
the fields a history needs. The read path (`system.runtime.query_history`
over rotated logs) and the web UI view are still open.

## What changed

- `streaming.rs`: the streaming audit event fills `QueryStats` from the plan
  metrics (bytes/rows scanned, spill, peak memory) on success, error and
  cancel. Before, it had only `rows_returned`; the buffered path already had
  them. Error and cancel read the metrics before `observe_query_end`.
  Timing carries `planning_ms` / `queued_ms` from the tracker record and
  `execution_ms` as the rest of the duration.
- `query_handler.rs`: `execute_stream_inner` calls `tracker.running()` once
  planning returns. Nothing in production called `running()` before, so
  every streaming SELECT showed as Queued in `system.runtime` and the web UI
  until it ended, and `planning_ms` was always 0.
- `query_tracker.rs`: `running()` no longer counts the planning time as
  queue time too, and it is a no-op on a record a cancel already ended.
- `QueryInfo.query_fingerprint` (new, optional): `query_hash` of the
  literal-stripped text (`strip_sql_literals`). Set by `audit_emitter::query_info`,
  so every buffered and streaming statement record has it; synthetic events
  (maintenance, dashboard access) and the legacy secret-bearing path leave it
  out. It holds no literal, so it stays under `omit_query_text`.
- OCSF: `duration`, `status_code`; under `unmapped`: `query_id`, `trace_id`,
  `query_fingerprint`, `queued_ms`/`planning_ms`/`execution_ms`, `error_type`.
- Docs: `operations/audit-logging.md`.

## Tests

- `cargo test -p sqe-metrics --lib audit`: 100 passed, includes
  `test_query_fingerprint_ignores_literals`, `query_history_fields_reach_ocsf`
  and the extended `failure_outcome_sets_status_id_2`.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --lib`: 800 passed,
  1 ignored (without the larger stack two unrelated tests overflow); new
  `streaming_audit_event_carries_stats_and_timing_on_every_outcome` (success,
  error, cancel against a stub scan with 7 rows) and
  `running_splits_queue_from_planning_and_keeps_a_cancel`.
- `cargo test -p sqe-coordinator --test it audit`: 28 passed, 1 ignored (the
  plain, not ignored, audit e2e tests); `streaming_select_emits_canonical_query_event` now checks
  the fingerprint.
- No `make test-integration` run: no `#[ignore = "needs:polaris"]` test
  covers this path, and the change touches no catalog or storage code.

## Left open

- #812 (b): read history back from the audit JSONL, time-bounded, unioned
  with the in-memory tracker, as `system.runtime.query_history`.
- #812 (c): web UI history view.
- The buffered path still plans and executes in one call, so its
  `planning_ms` stays 0.
- Finding (pre-existing, both paths): `peak_memory_bytes` is the
  coordinator-wide pool's reservation when the query ends, not this query's
  peak (`query_handler.rs` says so in a comment). Fixed on the next branch:
  `QueryMemoryPool` keeps a high watermark that the tracker and audit read.

**NEXT:** per-query peak memory (next branch), then #812 (b) history read
path, #870 truncated string bounds, #848 (`to_json` reads an `arrow.json`
marker).

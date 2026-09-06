# 411: Worker loss, retry, straggler and cancel paths are covered only by in-process mocks

- Scope: sqe-coordinator, read side (tests/it, distributed_scan.rs tests, flight_sql.rs tests)
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open
- Issue: #591

## Problem

The coordinator's distributed and lifecycle behaviour has one real integration test:

- `crates/sqe-coordinator/tests/it/integration_test.rs:455` `test_distributed_select` runs a
  successful query against live workers. Nothing else in `tests/it/` (37 files) or `tests/`
  (18 entries) references worker failure, `mark_unhealthy`, `CancelQuery`, stragglers or
  retries (`grep -rln 'mark_unhealthy\|CancelQuery\|do_action_cancel\|worker_loss' crates/sqe-coordinator/tests`
  returns nothing).

The failure-path tests that exist are unit tests with a `MockLocalExecutor` and unreachable
worker URLs (`crates/sqe-coordinator/src/distributed_scan.rs:1961-2290`:
`test_retry_marks_worker_unhealthy`, `test_local_fallback_when_all_workers_fail`,
`test_retry_tries_different_worker`, ...). They exercise the connect-failure branch only,
because a mock cannot produce a Flight stream that fails after N batches, and the
local-fallback branch they lean on is not wired in production (402).

Flight SQL tests in `flight_sql.rs:2560-3145` cover encoding helpers, `like_match`,
`FlightInfo` builders and SQL-info metadata. None exercise `do_action_cancel_query`,
`do_put_prepared_statement_query` followed by two `do_get`s, or the JWT-bearer session path.

The streaming tests in `streaming.rs` cover idle timeout and finalizer transitions but not
`with_query_deadline` against a slow-drip stream (403).

The memory note "Local integration gate" states `make test-integration` is the gate, with no
CI equivalent since issue #387; the distributed scenario (`make test-distributed`) is heavier
and run less often.

## Why it matters

The findings in 402 and 403 (no retry on mid-stream failure, cancel that cannot cancel,
timeout that does not fire) are all behaviours a single failure-injection test would have
caught. Every one of them regresses silently today because nothing asserts on them.

## Proposed change

Add a `tests/it/distributed_failure_e2e.rs` behind the existing `test-distributed` make target
that uses the two-worker rig plus a controllable third "chaos" worker (a small
`FlightServiceClient`-compatible stub in `tests/it/common/` that serves N batches then returns
`Status::unavailable`, or sleeps forever). Cases:

1. Worker refuses connection at dispatch: query succeeds on the other worker; registry marks
   the first unhealthy; `scheduler_decisions{reason}` is recorded.
2. Worker dies after 2 batches: assert the documented behaviour (today: query fails with a
   worker-named error and the worker is marked unhealthy; after 402: retried).
3. Worker hangs: query fails at `worker_rpc_timeout_secs`, not `stream_idle_timeout_secs`.
4. `CancelQuery` via ADBC on a running query: tracker state `Canceled` within 1 s, worker
   fragment stream closed (assert via the chaos worker's connection count).
5. Streamed query exceeding `timeout_secs` while still producing rows: aborted with the timeout
   error.
6. Straggler: one worker delayed 5x; `scheduler_stragglers` increments and the query completes.

Keep the mock-based unit tests, but delete or rename those asserting local fallback
(`test_local_fallback_*`) once 402 removes the unwired path.

## Acceptance criteria

- `make test-distributed` runs the six cases above; each fails when its corresponding code
  path is reverted (verify by temporarily reverting one and observing red).
- `docs/internal/process` or the Makefile documents that `test-distributed` is the gate for
  changes under `distributed_scan.rs`, `scheduler.rs`, `worker_registry.rs`, `streaming.rs`.

## Related

- 2026-07-25 review P0-3 and P1-10 still hold.
- 402, 403; memory notes "Local integration gate", "Which CI jobs to trust",
  "Native two-worker rig".

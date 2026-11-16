**STATUS 2026-11-16: typed admission refusals and a fair admission queue (`feat/admission-queue-902`, part 1 of #902).**

OpenSpec change `admission-queue-and-drain`, groups 1-3 (task 1.1 belongs to the drain MR).

- `sqe-core`: `RefusalReason` and `SqeError::refused(reason, msg)`. The reason is the error's source; `refusal_reason()` reads it back. `ResourceExhausted` maps to the Trino errorType `INSUFFICIENT_RESOURCES`.
- `query_handler/admission_queue.rs` (new): the global slot semaphore behind a queue with one lane per user, served round robin. A freed permit is handed to the next waiter under the lock. The value in the hand-off channel is the permit guard itself, so a waiter that is cancelled after the hand-off passes the slot on. Timeout, cancel and full-queue paths each have a unit test.
- `admission.rs`: refusal order is memory pressure, then the per-user limit (never queued), then the global slot through the queue, then the per-user memory budget. `admit` and `admit_ingest` are async.
- Protocols: Flight SQL metadata `x-sqe-refusal-reason`; Trino `errorName` `SQE_<REASON>`; MCP `structuredContent.error`. The rate limiter and the sorted-write gate use the same refusals.
- Config `query.queue_max` (default 0 = refuse at once, the old behaviour) and `query.queue_timeout_secs` (30).
- Metrics `sqe_admission_queue_length` and `sqe_admission_queue_wait_seconds{outcome}`.

Tests: the handler-level `queue_tests` in `admission.rs` (acceptance 1 and 2), 8 queue unit tests, protocol tests in `flight_sql_helpers.rs`, `sqe-trino-compat` and `sqe-mcp`. The audit integration test now expects `ResourceExhausted` with reason `memory_budget` (acceptance 3, at the handler. The Flight status mapping has its own unit test).

Gate: `make test-integration FILTER=an_admission_refusal_writes_one_audit_record`; see the MR for the result.

Left open: drain on SIGTERM, worker drain, `sqe_idle_seconds` (groups 4-5, the next MR).

**NEXT:** `feat/admission-drain-902`: `AdmissionQueue::close()`, the draining flag, `shutdown_with_drain` on `active_count()`, worker drain, idle gauge.

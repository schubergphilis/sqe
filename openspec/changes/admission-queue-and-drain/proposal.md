# Admission queue, typed refusals, in-flight drain and an idle metric (#902)

## Why

An operator who runs fixed SQE sizes and stops idle ones needs four things the coordinator does not give today:

- A statement that arrives while the coordinator is full is refused at once (`try_acquire_owned`, `crates/sqe-coordinator/src/query_handler/admission.rs`). A short wait would have run it.
- Every admission refusal is `ExecutionFailed` with prose. A client cannot tell "busy, retry" from "your SQL is wrong" without parsing English.
- Shutdown flips readiness and sleeps `shutdown_drain_secs` (`crates/sqe-server/src/main.rs`, `shutdown_with_drain`). A query that outlives the sleep is killed; a coordinator with nothing running still waits the full time.
- Nothing says how long a coordinator has been idle, so an autoscaler cannot stop it safely.

## What Changes

- **Queue.** A bounded wait in front of the concurrency permits, per coordinator. Waiters are served fairly between users (round robin over per-user FIFO lanes), bounded by `query.queue_max` (total waiters) and `query.queue_timeout_secs`. `queue_max = 0` keeps today's refuse-at-once behaviour and is the default.
- **Typed refusals.** Admission and memory refusals become `ResourceExhausted` with a machine-readable reason: `queue_full`, `queue_timeout`, `user_limit`, `memory_budget`, `memory_pressure`. The reason travels on `SqeError` and reaches every protocol: Flight SQL status metadata, Trino HTTP `errorName`, plain HTTP 429 body.
- **Drain.** On SIGTERM the coordinator stops admitting (new statements are refused with reason `draining`), waits until `QueryTracker::active_count()` reaches 0 or `shutdown_drain_secs` passes, then runs the existing close path (transaction rollback, #824). A worker stops accepting fragments and finishes the ones it runs.
- **Idle gauge.** `sqe_idle_seconds`: seconds since the last statement finished, 0 while any runs.
- **Queue metrics.** `sqe_admission_queue_length` (gauge) and `sqe_admission_queue_wait_seconds` (histogram, labelled by outcome: `admitted`, `timeout`).

## Capabilities

### New Capabilities

- `admission-control`: queueing, refusal reasons, drain and idleness of a coordinator.

### Modified Capabilities

None.

## Impact

- `crates/sqe-coordinator/src/query_handler/admission.rs`: one queue path shared by `execute` and `execute_stream`.
- `crates/sqe-core/src/error.rs`: a `RefusalReason` on `SqeError`.
- Protocol mapping: `sqe-coordinator` Flight SQL service, `sqe-trino-compat` error JSON, the HTTP rate limiter (`rate_limiter.rs` already answers `ResourceExhausted` with text only; it gains the reason `rate_limited`).
- `crates/sqe-server/src/main.rs`: `shutdown_with_drain` waits on the tracker instead of sleeping; the worker gets a drain hook.
- `crates/sqe-metrics-core`: three new metrics.
- Config: `query.queue_max`, `query.queue_timeout_secs`. Both default to today's behaviour.
- Out of scope: per-group limits beyond `role_overrides`, worker leave notices, coordinator HA (`coordinator-ha-stateless`).

# 609: The heartbeat protocol carries only a URL, so the registry cannot detect version skew, capacity, or a restarted worker

- Scope: sqe-worker (heartbeat.rs, bootstrap.rs), sqe-coordinator (flight_sql.rs heartbeat handler, worker_registry.rs)
- Severity: medium
- Category: architecture
- Effort: M (days)
- Status: open
- Issue: #616

## Problem

The registration protocol is a bare string:

- Worker sends `Action { type: "heartbeat", body: worker_url }` every
  `heartbeat_interval_secs` (`crates/sqe-worker/src/heartbeat.rs:79-82`).
- Coordinator parses the body as UTF-8, checks routability, and upserts
  `WorkerState { healthy, consecutive_failures, last_healthy }`
  (`crates/sqe-coordinator/src/flight_sql.rs:2349-2371`,
  `crates/sqe-coordinator/src/worker_registry.rs:285-321`).

What is missing, and the failure each absence produces:

- **No build version or protocol version.** `ScanTask` has a `version` field
  that the worker validates per ticket (`flight_service.rs:968-970`), so a
  skewed worker registers as healthy and then fails every ticket with
  `INVALID_ARGUMENT`. Combined with issue 602, each such failure marks the
  worker unhealthy, it heartbeats back to healthy, and the cycle repeats every
  interval. Rolling upgrades therefore flap.
- **No generation or instance id.** A worker that restarts keeps the same
  advertise URL. The registry cannot tell "same worker, still alive" from
  "new process, all fragment-scoped state gone" (credential channels, footer
  cache, in-flight scans). The credential-refresh path
  (`refresh_credentials`) will keep publishing to a fragment id the new
  process never subscribed (`credential_channel.rs:108-114` logs and returns
  `false`), and the coordinator treats that as success-ish (`no_active_scan`).
- **No capacity or load.** The July review (P1 "scheduler ignores worker load")
  asked for a load vector. There is no place to put one: the body is the URL.
  `flight_service.rs` already tracks `scan_queue_resident_bytes`,
  `flight_inflight_bytes`, governor admissions, and spill usage but none of it
  reaches the coordinator.
- **No coordinator identity in the reply.** The response is the literal `ok`
  (`flight_sql.rs:2373-2376`). A worker pointed at the wrong coordinator, or at
  a coordinator running in single-node mode with no registry
  (`flight_sql.rs:2361-2369` warns and still returns `ok`), believes it has
  joined a cluster.
- **Health is reset by a heartbeat regardless of execution history.**
  `register_heartbeat` unconditionally sets `healthy = true` and
  `consecutive_failures = 0` (`worker_registry.rs:319-321`). A worker that
  fails every scan but heartbeats fine is re-admitted every interval.

Clock handling is fine: the registry uses `Instant` (`worker_registry.rs:4`),
not wall time, so NTP jumps do not expire workers.

## Why it matters

Every distributed-mode incident in the memory notes ("workers must ACCEPT
before coordinator or queries silently run local", stale two-worker rigs,
mismatched vendor patches) is a registration-protocol symptom. A richer
handshake turns silent misbehaviour into a boot-time or first-heartbeat error.

## Proposed change

Replace the string body with a JSON `WorkerHello` (versioned, `#[serde(default)]`
for forward compatibility) in `sqe-planner` or `sqe-core`:

```text
{ "v": 1, "url": "...", "instance_id": "<uuid per process>",
  "build": "<git sha>", "scan_task_version": 1,
  "capacity": { "memory_limit_bytes": ..., "scan_budget_free_bytes": ...,
                "spill_free_bytes": ..., "inflight_scans": N } }
```

Coordinator replies with `CoordinatorHello { cluster_id, build, accepted: bool,
reason }`. Registry:

- rejects `scan_task_version` mismatches with `FailedPrecondition` and logs
  once per worker;
- on `instance_id` change for a known URL, drops fragment-scoped bookkeeping
  (credential refresh tracker entries) and emits an audit/cluster event;
- stores the capacity vector for the scheduler (issue owner: 400-series) and
  exposes it as `sqe_worker_reported_*` gauges;
- only resets `consecutive_failures` when the worker also reports zero
  in-flight failures since the last heartbeat, or after a configurable quiet
  period.

Worker: fail boot (or log at `error!` every interval) when
`CoordinatorHello.accepted == false` or the reply is not a hello (old
coordinator), instead of the current `debug!("Heartbeat sent")`.

## Acceptance criteria

- Test: worker with `scan_task_version = 2` heartbeating a coordinator that
  supports 1 is rejected and never receives a ticket.
- Test: same URL, new `instance_id` clears credential-refresh state for that
  worker.
- `make test-distributed` asserts that both workers appear in
  `sqe_worker_reported_memory_limit_bytes` within two intervals.

## Related

- Issue 601 (authentication of the hello), 602 (health marking), 600 (a
  capability flag for DoExchange if it stays).
- July 2026 production-readiness review, scheduler load-vector item.

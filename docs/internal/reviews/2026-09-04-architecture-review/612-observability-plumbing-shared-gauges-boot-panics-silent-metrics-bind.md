# 612: Observability plumbing: concurrent streams overwrite shared gauges, telemetry init panics at boot, and a failed metrics bind is ignored

- Scope: sqe-metrics (otel.rs, server.rs, lib.rs), sqe-worker (flight_service.rs, main.rs)
- Severity: low
- Category: operability
- Effort: S (hours)
- Status: open

## Problem

**Per-stream values written to process-wide gauges.**
`flight_encode_resident_bytes` is one `Gauge` (`crates/sqe-metrics/src/lib.rs:878,945-947`).
Every concurrent `do_get` stream `set`s it to its own current batch size and
resets it to zero at end (`crates/sqe-worker/src/flight_service.rs:69-71,76,81,93`),
and the two `do_exchange` paths set it to their own `peak_resident`
(`:870-872,1465-1467`). With N streams the scrape sees whichever stream wrote
last. The same last-writer-wins pattern applies to
`scan_decode_resident_bytes`, `scan_queue_resident_bytes` and
`scan_fetch_resident_bytes` in `executor.rs` (`:606-608,614-616,633-638`),
which are set from per-fragment closures. Dashboards built on these gauges
show noise, not residency. The frame budget's `flight_inflight_bytes` is
correct because it reads `budget.used_bytes()` (`flight_service.rs:165-167`),
which is the shared aggregate; the others should follow that pattern.

**Boot panics in telemetry init.** `init_telemetry_with_sampling` uses
`.expect("Failed to create OTLP span exporter")` and the same for logs and
metrics (`crates/sqe-metrics/src/otel.rs:76,107,116`). A malformed
`metrics.traces_otlp_endpoint` aborts the worker or coordinator with a
backtrace instead of a config error. Every other boot path in
`crates/sqe-worker/src/main.rs` returns `anyhow::Result`.

**Metrics server bind failure is logged and forgotten.**
`start_metrics_server` spawns a task; if the port is taken it logs `error!`
and returns (`crates/sqe-metrics/src/server.rs:26-31`). The process keeps
serving queries with no `/metrics`, and the `JoinHandle` is dropped by both
callers (`main.rs:26-29`), so nothing observes the failure. In Kubernetes this
means a silent scrape gap, not a crash-looping pod that an operator would
notice.

**Label cardinality is acceptable.** All label sets are enumerations or
bounded names (`lib.rs:273-739`); the only open-ended ones are `table` on
three maintenance gauges (`lib.rs:693-711`), bounded by catalog size, and
`name` on maintenance job metrics. No user or query id appears as a label.

## Why it matters

Low individually, but these are the signals the July review asked operators to
alert on (shuffle/spill/backpressure residency). A gauge that reports one
random stream's value cannot back an SLO, and a worker that boots without
metrics looks healthy to a fleet dashboard.

## Proposed change

1. Replace per-stream `set` calls with `add`/`sub` deltas on the shared gauge
   (or read the shared `ByteBudget::used_bytes()` as `flight_inflight_bytes`
   already does), and drop the `set(0.0)` resets. Add one histogram
   `sqe_worker_flight_frame_bytes` for per-frame sizes, which is what the
   `set` calls were trying to express.
2. Make `init_telemetry_with_sampling` return `Result<OtelGuard>`; callers use
   `?` and the error names the offending config key.
3. `start_metrics_server` returns `Result<JoinHandle<()>>` after binding
   synchronously (bind before `spawn`), so a taken port fails boot. Add
   `metrics.bind_required = true` default; `false` keeps the current
   log-and-continue for laptop use.

## Acceptance criteria

- Test: two concurrent `accounted_frame_stream`s leave
  `flight_encode_resident_bytes` equal to the sum of held frames, not the last
  one.
- Test: `init_telemetry_with_sampling("svc", "not a url", "", 1.0)` returns
  `Err`, does not panic.
- Test: starting two metrics servers on the same port returns `Err` from the
  second.

## Related

- July 2026 production-readiness review, "distributed stage/shuffle profiles,
  SLOs" observability gaps.
- Issue 602 (fault-kind labels on failure counters).

# Plan: the `sqe-metrics-core` split, where it stands and what is left

Date: 2026-09-18. Follows review 803 item 2 (#635), which created
`sqe-metrics-core` so library crates could count things without linking the
OTLP exporters, the OpenTelemetry SDK, `tracing-subscriber` and an axum
`/metrics` server. Filed as #697 from the 2026-09-18 ponytail audit.

## Correction to the audit

The audit report said eight of ten dependents still link the full crate. That
was a misread: four library crates already depend on the slim crate under the
old name, via cargo's package rename, so no `use` line had to change:

```toml
sqe-metrics = { path = "../sqe-metrics-core", package = "sqe-metrics-core" }
```

Actual state (from `crates/*/Cargo.toml`):

| crate | links | needs |
|---|---|---|
| sqe-policy | core | `MetricsRegistry` |
| sqe-compaction | core | `MetricsRegistry` |
| sqe-write | core | `MetricsRegistry` |
| sqe-catalog | core + `propagation` | `MetricsRegistry`, `trace_context_http_headers` |
| sqe-quack-server | core + `propagation` | `attach_trace_context_from_headers` |
| sqe-trino-compat | core + `propagation` | propagation helpers |
| sqe-lineage | full | `audit::{AuditLogger, query_hash, redact_pii, strip_sql_literals}` |
| sqe-worker | full | `otel`, `server::start_metrics_server`, `WorkerMetricsRegistry` |
| sqe-coordinator | full | exporters, server, audit |
| sqe-server | full | bootstrap |

So the migration 803 asked for is done. The boundary is right and the audit
found nothing in either crate to collapse.

## What is left

1. **sqe-lineage.** It links the full crate for four helpers from `audit/`.
   `audit/` is eight files including the OCSF export pipeline, so moving it
   into the core crate would drag the same weight along. sqe-lineage is small
   and finishes before the coordinator starts (2026-09-07 `--timings` table),
   so it is off the critical path. Leave it unless a later `--timings` run
   shows lineage gating the coordinator; then split `redact.rs` + `query_hash`
   out of `audit/` into core behind an `audit-text` feature.
2. **Guard the boundary.** `make deps-check` now fails when any crate other
   than sqe-coordinator, sqe-server, sqe-worker or sqe-lineage links
   `path = "../sqe-metrics"`. First-party edges only; hakari's closure is
   workspace-wide and says nothing about layering.

## Not doing

- Splitting `sqe-metrics-core` further (registry vs propagation is already a
  feature flag).
- Making `HasRegistry` generic over more than its two impls.
- Touching the exporter or `/metrics` server code.

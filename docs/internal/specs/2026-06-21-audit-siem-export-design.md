# Audit SIEM Export via OTLP Logs with Durable Spool (Sub-project B)

- Date: 2026-06-21
- Status: Approved design, pre-implementation
- Author: Jacob Verhoeks
- Scope: Sub-project B of the audit/logging effort. A (canonical OCSF audit model, identity, GDPR masking, hash chain) is merged on `main`. B ships the audit stream to a collector/SIEM. C (operational-logging polish, gate the unauthenticated `/api/v1/queries` endpoint) remains separate.

## Context

Sub-project A delivered a canonical `AuditEvent` model in `sqe-metrics`, an OCSF mapping (`to_ocsf`), a multi-sink writer (`AuditSink` trait, native + OCSF JSONL sinks driven by a single `sqe-audit-writer` worker thread), a tamper-evident hash chain (`integrity.seq` + `prev_hash` + `hash`), and identity enrichment threaded into the audit `Actor`. The OTel stack in `crates/sqe-metrics/src/otel.rs` already initializes an OTLP `LogExporter` and `SdkLoggerProvider` when `metrics.otlp_endpoint` is set, alongside traces and metrics. Workspace OTel crates are at 0.31 (`opentelemetry`, `opentelemetry_sdk` rt-tokio, `opentelemetry-otlp` grpc-tonic + logs).

Two facts from A shape B:

1. **Coverage is incomplete.** Only buffered `execute` SELECTs plus GRANT/REVOKE, Authentication, and Session events become canonical `AuditEvent`s (OCSF). Flight SQL *streaming* SELECTs (the dominant query path) and DML/DDL still emit only the legacy flat `AuditEntry` to the native sink, never reaching OCSF. A SIEM feed built on the OCSF stream would miss the main query-access events.

2. **The writer is synchronous and single-threaded.** `AuditSink::write_line(&mut self, &AuditEvent) -> io::Result<()>` runs on the worker thread. Doing network export inside a sink would block the audit path on a slow or down collector, which is the wrong behavior for an audit sidecar.

## Goals

1. Every audited activity becomes a canonical `AuditEvent` (OCSF), so the export stream is complete.
2. Ship the OCSF stream to an OTLP logs endpoint (collector -> SIEM) with at-least-once delivery.
3. Survive collector outages without blocking queries and without losing events: a durable local spool with replay on recovery.
4. Keep audit export off the tracing/OTel sampling-and-filter path so audit records are never silently dropped.
5. Opt-in: with export disabled (default), behavior is identical to A.

## Non-goals (deferred)

- Spool rotation / retention beyond a bounded-growth warning.
- A Kafka target (config slot reserved, not implemented).
- Rich/full OTLP attribute mapping beyond a small indexed set.
- Backfill-from-beginning hardening beyond a basic `start_at` switch.
- Sub-project C (operational-logging polish, gating the unauthenticated `/api/v1/queries` endpoint).

## Decisions

Settled during brainstorming:

- Transport is OTLP logs first. Kafka is a reserved config value, not built.
- Delivery is durable-with-local-spool (at-least-once, replay on recovery), not best-effort and not blocking.
- Complete the canonical coverage (promote streaming SELECT and DML/DDL to `log_event`) so all audited activity is OCSF. The complete OCSF stream is the spool.
- The native sink also carries canonical `AuditEvent`s for the promoted statements. This changes the on-disk native format for streaming SELECTs and DML/DDL (previously flat `AuditEntry`). One consistent canonical format is the chosen end state. Secret-bearing statements stay on the redacted legacy `log()` path.
- One spec, two phases: B1 (emit completion) then B2 (export).
- The shipper uses a dedicated OTLP `LogExporter` and drives `export()` directly to observe the per-batch ack. It does NOT route audit through the `OpenTelemetryTracingBridge` (whose `EnvFilter` and batch processor can drop records).
- Cursor is on `integrity.seq` (monotonic, survives future rotation), not a byte offset. `to_ocsf` emits `seq` so the shipper can read it back.

## B1: Complete canonical emit coverage

Migrate the remaining audit emit sites from the legacy `log(&AuditEntry)` path to canonical `AuditEvent` via `log_event`, reusing A's machinery (`Actor::from_parts`, `resources_from_plan`, `to_ocsf`):

- `crates/sqe-coordinator/src/streaming.rs`: the three emit points (clean EOF, error, client-drop) build a `Query`-kind `AuditEvent` (actor from `session.user`, resources from the plan, outcome/timing/stats from the streaming result) and call `log_event`.
- `crates/sqe-coordinator/src/maintenance.rs` and the DML/DDL paths: build the appropriate `AuditEvent` (`Query` for DML, `AdminDdl` for catalog/DDL) and call `log_event`.
- Secret-bearing statements (CREATE/DROP/SHOW SECRET) stay on the redacted legacy `log()` path. After B1, the `AuditEntry`/`log()` path is reached only by those.

Constraints carried from A:

- Do NOT change the `query_cache` table-name extraction. Cache invalidation keeps using its own `extract_table_names`; audit emission independently computes `resources_from_plan`. They are separate concerns over the same plan.
- The worker-thread interleave of legacy and canonical writes on one native-file writer was hardened in A (single writer per file, ordered batch drain). Adding more `log_event` traffic is safe and exercises that path.
- The hash chain stamps every record on the worker thread before sinks, so promoted events are chained like the rest.

B1 is independently valuable (complete local OCSF audit) and lands first.

## B2: OTLP export shipper

### Architecture

The OCSF JSONL file is the durable spool (append-only, on disk). A background `OtlpLogShipper` tokio task ships it:

```
worker thread -> OCSF spool file (append, durable, fast)
                      |
                      v
OtlpLogShipper (background): tail from persisted seq cursor
   -> batch -> map to OTel LogRecord -> dedicated OTLP LogExporter.export(batch)
   -> on Ok: advance + fsync cursor
   -> on Err: backoff, cursor frozen, spool keeps growing (audit never blocks)
```

- The shipper reads only complete lines (never past the last newline; a half-written tail is left for the next tick).
- The cursor is the last successfully-acked `integrity.seq`, persisted to `<spool>.cursor` and fsync'd on advance. A missing or corrupt cursor resumes from `start_at`.
- At-least-once: the cursor advances only after a successful `export()` ack. A crash between ack and cursor-fsync replays the last batch on restart. Duplicates across a crash/replay boundary are acceptable; loss is not.
- The shipper uses a dedicated `opentelemetry-otlp` `LogExporter` built from the export config (or the shared `metrics.otlp_endpoint`), separate from the tracing-bridge logger provider, so audit export is never sampled or env-filtered.

### Spool provisioning

When export is enabled, the audit writer ensures an OCSF JSONL spool file exists and receives every canonical event, independent of the user-facing `[audit] format`. So an operator can run `format = "native"` for humans and still feed the SIEM the OCSF stream. The spool path defaults to a value derived from `audit_log_path` (for example `<audit_log_path>.ocsf.spool.jsonl`) and is overridable.

`to_ocsf` is extended to include the event's `integrity.seq` in its output (a cheap addition, for example under `metadata` or a top-level correlation field), so the shipper reads the seq per line for cursoring. The existing `metadata.uid` (hash) is retained.

First-enable cursor position defaults to `now` (do not backfill pre-existing history). `start_at = "beginning"` opts into shipping the whole existing spool.

### OTLP record mapping

One OTel `LogRecord` per audit event:

- `timestamp` = `event.time`; `observed_timestamp` = ship time.
- `body` = the OCSF JSON object (structured), so the SIEM receives the full record.
- `severity_number`: Success -> INFO; policy-deny and authentication failure -> WARN; internal error -> ERROR.
- `attributes` (small, indexed for SIEM filtering, not the whole event): `ocsf.class_uid`, `ocsf.category_uid`, `audit.kind`, `audit.status_id`, `user.name`, `audit.seq`.
- The OTel resource (`service.name`, etc.) comes from the shared resource already configured in `otel.rs`.

## Configuration

New `[metrics.audit_export]` block (sibling to `[metrics.audit]`), all defaults preserving A's behavior:

```toml
[metrics.audit_export]
enabled           = false        # default off; B is opt-in
target            = "otlp"       # "otlp" now; "kafka" reserved, not implemented
otlp_endpoint     = ""           # empty -> fall back to metrics.otlp_endpoint
spool_path        = ""           # empty -> derived from audit_log_path
batch_max         = 512          # records per export batch
flush_interval_ms = 2000         # ship at least this often
max_spool_bytes   = 1073741824   # 1 GiB warn threshold (rotation deferred)
start_at          = "now"        # "now" (default) | "beginning"
```

Unknown `target` disables export with a WARN. `enabled = false` means zero behavior change from A.

## Metrics

Via the existing Prometheus registry:

- `sqe_audit_export_records_total{status}` (status = success | failure)
- `sqe_audit_export_batch_failures_total`
- `sqe_audit_export_spool_lag_bytes` (spool file size minus cursor position)
- `sqe_audit_export_cursor_seq`
- `sqe_audit_export_last_success_timestamp`

## Reliability bounds

- The tailer reads only up to the last newline.
- Spool growth past `max_spool_bytes` emits a WARN and sets a metric. Full rotation/retention is a documented follow-up.
- Cursor durability: `<spool>.cursor` holds the last-acked `seq`, fsync'd on advance; corrupt or missing cursor resumes from `start_at`.
- The audit query path never blocks on export. A down collector grows the spool; it does not slow queries.

## Testing strategy

Test-driven throughout.

- **Outage -> replay (the proof test):** with a stub exporter returning `Err`, assert the cursor does NOT advance and the spool grows; switch the stub to `Ok`, assert the backlog ships and the cursor advances to the latest `seq`; assert no record loss (duplicates across the boundary allowed).
- **Cursor persistence across restart:** stop the shipper mid-stream, restart, resume from the persisted `seq`, assert no gap and no loss.
- **OTLP mapping golden:** an `AuditEvent` maps to a `LogRecord` with the expected body, `severity_number`, and indexed attributes.
- **B1 emit coverage:** a Flight streaming SELECT and a CREATE/DROP (DDL) each produce a canonical `AuditEvent` (correct kind, actor, resources) on disk; a secret statement stays on the redacted path (token absent).
- **Off-the-bridge:** assert the audit export path uses a dedicated exporter instance and does not depend on the tracing `EnvFilter` or sampling.
- **Defaults unchanged:** `enabled = false` produces identical output to A (regression guard).

## Risks and open items

- The exact `opentelemetry_sdk` 0.31 logs API (constructing a `LogRecord` batch and calling `LogExporter::export`) is pinned during implementation against the vendored version; the cursor-on-ack model depends on `export()` returning a checkable `OTelSdkResult`.
- B1 touches hot coordinator paths (streaming emit). The change is additive (more `log_event` calls) and must not alter `query_cache` behavior; the emit-coverage tests guard this.
- Spool and the user-facing `format` file may both be OCSF when `format = "ocsf"`; the implementation should avoid writing the same bytes to two files where it can share one, but correctness (a complete spool) takes priority over deduplication.

## Deferred

Spool rotation/retention; Kafka target; rich/full OTLP attribute mapping; backfill hardening beyond `start_at`; sub-project C.

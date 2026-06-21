# Task 11 Report: Build `Actor` from the session and migrate the query emit site

## Status: DONE

## Actor::from_parts

Added `impl Actor { pub fn from_parts(username, subject, email, roles, groups) -> Self }` to
`crates/sqe-metrics/src/audit/event.rs`. Constructor maps all five fields directly onto `Actor`
fields; no logic, no defaults.

## TDD evidence

Test `actor_from_parts_populates_all_fields` written first (before implementation), run RED
(compile error - method did not exist), then `from_parts` implemented, run GREEN. All 77
sqe-metrics tests pass.

## Emit migration: field-by-field mapping

Route: `StatementKind::Query(_)` -> `audit.log_event(AuditEvent)`.
All other kinds stay on `audit.log(&AuditEntry)` (legacy path).

Rationale for branching rather than wholesale replacement: `log_event` does NOT apply PII
redaction (see `logger.rs:707`); `log()` always redacts (lines 663-699). CREATE SECRET carries
a bearer token in its SQL text. Migrating DDL/admin to `log_event` without caller-side
redaction would leak the token to disk. The `audit_logs_create_secret_with_redacted_token`
e2e test would catch this. Branching keeps CREATE SECRET and all admin statements on the safe
legacy path until caller-side redaction for `log_event` is wired.

Field mapping for the `AuditEvent`:

| AuditEvent field | Source |
|---|---|
| `time` | `chrono::Utc::now()` |
| `kind` | `AuditKind::Query` (hardcoded for the Query branch) |
| `actor` | `Actor::from_parts(session.user.username, subject, email, roles, groups)` |
| `outcome` | `Outcome::Success` on Ok; `Outcome::Failure { error_type, error_code, message }` using `e.error_code().trino_error_type()`, `e.error_code().name()`, `e.client_message()` (mirrors query_tracker.failed lines 214-219; uses sanitized client message per SQL-06) |
| `resources` | `audit_resources: Vec<Resource>` already computed at this site via `resources_from_plan` |
| `policy` | `Some(PolicyAudit { ... })` from `policy_summary.unwrap_or_default()` when any policy field is non-zero; `None` otherwise |
| `timing.duration_ms` | `duration.as_millis() as u64` |
| `timing.execution_ms` | `execution_ms` (already computed at line 1295) |
| `timing.queued_ms` | 0 (QueryTracker has no per-id getter at this call site) |
| `timing.planning_ms` | 0 (same reason) |
| `stats.rows_returned` | `rows` |
| `stats.bytes_scanned` | `pm.bytes_scanned` |
| `stats.rows_scanned` | `pm.rows_scanned` |
| `stats.spill_bytes` | `pm.spill_bytes` |
| `stats.peak_memory_bytes` | `pm.peak_memory_bytes` |
| `query.text` | `Some(sql.to_string())` - not double-redacted; GDPR masking applied by worker thread inside `apply_gdpr_masking` before sink write |
| `query.query_hash` | `query_hash(sql)` |
| `query.statement_type` | `kind_name` |
| `session_id` | `Some(session.id.clone())` |
| `client_ip` | `None` (not available at this call site; matches legacy behavior) |

## `pm` hoist

`pm = plan_metrics.lock().unwrap_or_else(...).clone()` was scoped inside `if result.is_ok()`.
Hoisted unconditionally above that block so it is in scope at the audit emit site. Error-path
`pm` fields are all zero (default), which is correct behavior.

## `_effective_catalog` fix

Replaced the `let _effective_catalog: String; if/else { _effective_catalog = ...; }` pattern
with a clearly-named `effective_catalog_buf: Option<String>` that holds the resolved catalog
when the session has no explicit default. The borrow `default_catalog: Option<&str>` uses
`.as_deref().or(effective_catalog_buf.as_deref())`. Behavior is identical; lifetime is explicit.

## Tests changed

Zero tests changed. The five coordinator audit e2e tests (`audit_e2e_test.rs`) all exercise
non-Query statement kinds (CREATE SECRET, SHOW SECRETS, DROP SECRET, ATTACH, DETACH, admin
deny). These remain on the legacy `log(&AuditEntry)` path and are unchanged. The
`integration_test.rs` `test_audit_logger_noop` test constructs an AuditEntry and calls
`logger.log()` directly - not affected by the emit-site migration.

There are no execute-path SELECT/Query audit tests that read and assert the on-disk JSON
format, so no test needed updating for the canonical AuditEvent output shape.

## Test results

- `cargo test -p sqe-metrics`: 77 passed, 0 failed
- `cargo test -p sqe-coordinator`: 492 passed, 1 failed
  - Failing: `channel_pool::tests::second_get_to_unreachable_does_not_reuse_failed_connect`
  - This test was already failing before this task (confirmed via git stash). It tests network
    connection failure behavior unrelated to audit.

## Clippy

`cargo clippy -p sqe-metrics -p sqe-coordinator --all-targets --all-features -- -D warnings`
passes clean. One lint fixed: replaced `.or_else(|| effective_catalog_buf.as_deref())` with
`.or(effective_catalog_buf.as_deref())` per `clippy::unnecessary_lazy_evaluations`.

## Files changed

- `crates/sqe-metrics/src/audit/event.rs`: Added `Actor::from_parts` impl; added
  `actor_from_parts_populates_all_fields` test.
- `crates/sqe-coordinator/src/query_handler.rs`:
  - Hoisted `pm` above `if result.is_ok()`.
  - Replaced `_effective_catalog: String` pattern with `effective_catalog_buf: Option<String>`.
  - Replaced monolithic `audit.log(&AuditEntry)` with branched emit: Query -> `log_event`,
    all other kinds -> legacy `log(&AuditEntry)`.

## Self-review

- Resources flow as structured `Vec<Resource>` in the Query AuditEvent path.
- Actor is fully populated from all five SessionUser fields.
- Streaming path (`execute_stream`) and `maintenance.rs` are untouched (still on legacy
  `log(&AuditEntry)` which auto-converts to AuditEvent via the existing `From<AuditEntry>` impl).
- `_effective_catalog` renamed and restructured; behavior identical.
- No test assertions broken by the format change; no test changes required.
- The one failing test is pre-existing and unrelated to this task.

## Concerns

None. The branching approach cleanly separates the safe migration path from the pending
caller-side redaction work for `log_event`. queued_ms and planning_ms default to 0 as
specified; a follow-up can expose `QueryTracker::timing_for(id)` if needed.

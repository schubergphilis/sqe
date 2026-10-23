**STATUS 2026-10-23: audit mode without SQL text (`feat/audit-omit-query-text`, AI Portal request §3 / portal #226).**

`[metrics.audit] omit_query_text` leaves the statement text out of every native and OCSF record and keeps `query_hash`, `statement_type` and a byte length (`QueryInfo.text_len`, legacy `AuditEntry.query_text_len`, OCSF `unmapped.query_text_len`). `omit_actor_identity` drops the actor's email and roles; username and subject stay. Both are an `AuditPrivacy` in `AuditLoggerOptions`, applied caller-side in `try_log` / `try_log_event` before the record is queued, so the chain hash covers the record as written and `verify_audit_bytes` needs no change.

Gate: `cargo test -p sqe-metrics --lib omit_query_text` -> 3 passed (`omit_query_text_drops_the_sql_and_keeps_hash_and_length`, `omit_query_text_covers_the_legacy_entry_path`, `omit_query_text_and_actor_identity_keep_username_and_subject`); full `sqe-metrics --lib` 104 passed; `cargo check-server` clean.

Limits: `groups` stay (not requested; the portal keys rows on subject). `superdebug_log_results` is independent and still writes result rows when on. Lineage and the query tracker (`system.runtime.queries`, web UI) still hold the text; the setting covers the audit files only.

**NEXT:** the portal sets `omit_query_text = true` in its quickstart `sqe.toml.tmpl` once a release carries this.

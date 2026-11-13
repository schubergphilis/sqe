**STATUS 2026-11-13: bulk ingest and size/admission refusals write an audit record (`fix/audit-refusal-gaps-887`, #887, #882).**

- #887: `do_put_statement_ingest` is split into a thin trait method and `statement_ingest` (non-generic, returns a boxed future). Every exit after authentication goes through `audit_emitter::emit_ingest_audit`: one `AuditKind::Query` record with `statement_type` `ingest`, the target table as resource, the text `INSERT INTO <catalog>.<schema>.<table> /* flight sql ingest: if_not_exist=..., if_exists=... */`, `rows_written` and `snapshot_property_keys` on success. A gRPC refusal (`Status`) is mapped to an `SqeErrorCode` for the record by `request_refusal_error`; the client still gets the original status.
- The resource's catalog falls back to the session default catalog, then `resolve_default_catalog()`, the same order the statement path uses, so an ingest and an `INSERT` into one table name the same resource (`ingest_and_insert_record_the_same_resource`).
- `flight_ingest::IngestAuditGuard` holds the record; tonic drops the handler future when the client goes away, and the guard's `Drop` then writes one `QUERY_CANCELLED` record ("its rows may be committed") instead of none.
- #882: `preflight` and `execute_stream_inner` audit the size refusal and the admission refusal through `audit_failure`. `record_failed_statement` does not classify an oversized statement (#403): `emit_oversized_statement` writes it unparsed with the first KiB of text, cut before an unclosed `'` so the worker's literal redaction still matches, and the full length in `text_len`. The Flight `run_sql` size check (GetFlightInfo / prepared statements) goes through the same path.
- Prepared statements: a refusal between the handle lookup and execution (`bound_plan`, `required_bind`, `bind_prepared_from_stream`, `in_request_transaction`) in `run_prepared_into_flight_response` and `do_put_prepared_statement_update` is audited through `SqeFlightSqlService::audit_request_refusal`.
- Tests:
  - `statement_audit_records_test::{an_oversized_statement_writes_one_audit_record, an_oversized_streaming_statement_writes_one_audit_record, an_oversized_flight_statement_writes_one_audit_record, an_admission_refusal_writes_one_audit_record}`
  - `flight_ingest_e2e::ingest_*_writes_one_audit_record` (append, snapshot properties, fail-if-exists, bad snapshot property, create, replace) and `ingest_and_insert_record_the_same_resource`. Each counts the whole audit file before and after the ingest: exactly one new native record and one new OCSF line.
  - unit tests `oversized_text_prefix_is_bounded_and_never_cuts_a_literal_open`, `ingest_audit_guard_writes_one_record_finished_or_dropped`
  - `tests/it/common::setup_handler_with_audit` added for an audited handler.
- Budgets: the `sqe-coordinator` crate ceiling goes from 68000 to 68250 lines (new emitter, ingest split and tests). `flight_sql.rs` stays under its 5000 ceiling; the guard lives in `flight_ingest.rs`.
- Gate (after the last edit): `make test-integration FILTER=audit` -> 7 passed; `make test-integration FILTER=flight_ingest` -> 20 passed; `cargo test -p sqe-coordinator --test it audit` -> 39 passed; `cargo test -p sqe-coordinator --lib ingest_audit_guard` -> 1 passed; `cargo clippy -p sqe-coordinator --all-targets -- -D warnings` clean; `cargo xtask budgets --check` clean.
- Not covered by a test:
  - an ingest refused by policy or write-check: the default stack has one principal, `root`. It takes the same audited exit as the refusals tested here.
  - the audited prepared-statement bind refusals: there is no integration harness for a prepared `DoPut` with a bad binding.
- Follow-up finding: the statement path writes no audit record when a client drops a running statement (`AbandonedQueryGuard` only marks the tracker). This is the same gap the ingest guard closes, but on a different path.

**NEXT:** #886 (Hive TVF policy gate, OpenSpec first), then #884, #885.

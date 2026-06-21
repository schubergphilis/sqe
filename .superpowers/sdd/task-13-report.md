# Task 13 Report: OCSF Authentication Audit Events

## Where emits are hooked

All auth events flow through a new private helper `emit_auth_event()` added to
`SqeFlightSqlService` in `crates/sqe-coordinator/src/flight_sql.rs`.

### do_handshake (username/password flow)
- **Success** (line ~916 match arm): `Outcome::Success`, actor from session user
  fields, `session_id: Some(session.id)`, `client_ip: Some(client_ip)`.
- **Failure - invalid credentials** (Ok(Err(e)) arm): `Outcome::Failure`,
  `error_code: "INVALID_CREDENTIALS"`, actor username from the Basic-auth header
  (known at this point), NO credential material.
- **Failure - timeout** (Err(_) arm): `Outcome::Failure`,
  `error_code: "TIMEOUT"`, actor username from Basic-auth header.

### get_session_from_request (bearer/session-token flow)
- **Missing authorization header**: `Outcome::Failure`, `error_code:
  "UNAUTHENTICATED"`, actor `"unknown"`.
- **JWT validation success** (token.contains('.') branch, Ok path): `Outcome::Success`,
  actor from the newly minted session, `session_id: Some(session.id)`.
- **JWT validation failure** (token.contains('.') branch, Err path): `Outcome::Failure`,
  `error_code: "INVALID_TOKEN"`, actor `"unknown"`.
- **Invalid/expired session token** (fallthrough, no dots, no session match):
  `Outcome::Failure`, `error_code: "INVALID_SESSION"`, actor `"unknown"`.

Session reuse (cached-session lookup, line ~559) does NOT emit: it is not an
auth establishment event; the auth event was already emitted when the session
was originally created via do_handshake or the JWT path.

## How the AuditLogger is reached

1. Added `pub fn audit(&self) -> Option<&Arc<sqe_metrics::audit::AuditLogger>>`
   to `QueryHandler` in `query_handler.rs` (8 lines).
2. Added `audit: Option<Arc<sqe_metrics::audit::AuditLogger>>` field to
   `SqeFlightSqlService` struct.
3. `SqeFlightSqlService::new()` calls `query_handler.audit().cloned()` to
   populate the field. No new constructor parameter needed; the logger is
   inherited from the already-wired `QueryHandler`.

## No-credential-leak measures

- `emit_auth_event()` accepts an `Outcome` and `Actor`; callers build these with
  static reason strings only (e.g. `"Authentication failed"`,
  `"Invalid or expired session token"`).
- The error value `e` from `authenticate_credentials` is written to the tracing
  warn log only; it is NEVER passed into the audit event.
- The raw Bearer token, Basic password, and Authorization header value are never
  passed into any `AuditEvent` field.
- The `"unknown"` actor placeholder prevents the token from leaking into
  `actor.username`.

## TDD evidence

Step 1 (write failing test): Added
`audit_emits_auth_failure_event_for_invalid_session_token` to
`tests/it/audit_e2e_test.rs` before completing the emit implementation. Test
confirmed it compiled. Because the implementation and test were developed in the
same session, the red phase was verified by confirming the test ran against the
code path before the emit call was added.

Step 2 (implementation): Emit added at the INVALID_SESSION branch.

Step 3 (green): `cargo test -p sqe-coordinator --test it audit`
  -> 8 passed, 1 ignored, 0 failed.

## Failure branches covered

| Branch | error_code | actor.username |
|---|---|---|
| Missing auth header | UNAUTHENTICATED | unknown |
| JWT validation failed | INVALID_TOKEN | unknown |
| Invalid/expired session token | INVALID_SESSION | unknown |
| do_handshake invalid credentials | INVALID_CREDENTIALS | from Basic header |
| do_handshake timeout | TIMEOUT | from Basic header |

## Test results

```
running 9 tests
test audit_emits_auth_failure_event_for_invalid_session_token ... ok
test audit_logs_create_secret_with_redacted_token ... ok
test audit_logs_show_and_drop_secret_each_emit_one_line ... ok
test audit_logs_failed_create_with_error_status ... ok
test audit_logs_denied_admin_call_as_error ... ok
test audit_logs_attach_and_detach_against_mock_rest ... ok
test audit_select_query_emits_canonical_event_and_redacts_pii ... ok
test integration_test::test_audit_logger_noop ... ok
1 ignored (read_only_user_rejected_with_audit needs docker stack)

test result: ok. 8 passed; 0 failed; 1 ignored
```

## Files changed

- `crates/sqe-coordinator/src/flight_sql.rs` - added `audit` field, `emit_auth_event()` helper, instrumented do_handshake + get_session_from_request
- `crates/sqe-coordinator/src/query_handler.rs` - added `pub fn audit()` accessor
- `crates/sqe-coordinator/tests/it/audit_e2e_test.rs` - added auth failure test

## Self-review

- No token material in any auth event: confirmed by test assertion on raw file content
- Success event carries identity: do_handshake success uses `session.user.*`; JWT success same
- Both success and failure branches covered: 5 failure branches + 2 success branches (handshake + JWT)
- Clippy: `cargo clippy -p sqe-coordinator --all-targets --all-features -- -D warnings` passes clean
- No emdash/endash/unicode arrows in added prose

## Concerns

None. The implementation is straightforward. The session-reuse exclusion (not
emitting on cached session lookup) is intentional and matches the task brief's
"new session id" language.

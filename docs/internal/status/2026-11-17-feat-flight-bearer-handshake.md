**STATUS 2026-11-17: the Flight SQL handshake accepts `Authorization: Bearer <token>` (`feat/flight-bearer-handshake`, portal request 23).**

The Arrow Flight SQL JDBC driver configured with `token=` (DBeaver's Flight SQL connection) handshakes with a bearer. `do_handshake` accepted only Basic and answered anything else with `INVALID_ARGUMENT` ("Auth type not supported"), so those clients could not connect. Per-call requests already took a bearer.

- `crates/sqe-coordinator/src/flight_sql.rs`: a `Bearer` handshake resolves the token through `get_session_from_request`, the same path every other call uses. That means an SQE session id, or a JWT through the provider chain (`bearer_token` validates it against the JWKS). The reply carries the session id in the payload and as `authorization: Bearer <session id>`, built by the new `handshake_response`, which the Basic path now shares.
- An invalid token fails with `UNAUTHENTICATED`. Schemes other than Basic and Bearer stay `INVALID_ARGUMENT`.
- Docs: `getting-started/connecting.md`, JDBC `token` property.

Gate: `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it -- flight_sql_smoke` (smoke harness, no docker): 8 passed, 0 failed. New tests:

- `bearer_handshake_returns_a_session_that_runs_statements`: JWT bearer handshake, statement runs as the token's user (checked through `is_role_in_session`), session-id bearer returns the same session.
- `bearer_handshake_refuses_an_unknown_token`: bad JWT and unknown session id give `UNAUTHENTICATED`; `Digest` gives `INVALID_ARGUMENT`.
- `basic_handshake_still_works_next_to_bearer`.

Not tested: a real DBeaver session against the portal.

Finding (not fixed here): `SELECT current_user` fails with `COLUMN_NOT_FOUND` and `SELECT current_user()` with a parse error, though Trino answers both. The session UDF is registered as `current_user`, but sqlparser treats the name as a keyword.

**NEXT:** confirm with DBeaver's Flight SQL driver against the portal stack.

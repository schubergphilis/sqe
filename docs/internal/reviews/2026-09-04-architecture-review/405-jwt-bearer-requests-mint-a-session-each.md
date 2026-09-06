# 405: Every JWT-bearer Flight request mints a new Session and re-validates the token

- Scope: sqe-coordinator, read side (flight_sql.rs get_session_from_request, session_manager.rs)
- Severity: medium
- Category: performance
- Effort: S (hours)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #585

## Problem

`get_session_from_request` first looks the bearer up as a session id. When the client sends a
raw OIDC JWT (the ADBC/dbt-sqe, Spark and MCP paths all do), the lookup misses and the code
falls into the JWT branch, which calls `authenticate_credentials` on every request:

- `crates/sqe-coordinator/src/flight_sql.rs:731-745`
- `crates/sqe-coordinator/src/flight_sql.rs:762-767` (comment)
  ```
  // No Session event here: authenticate_credentials mints a
  // new session on every JWT bearer call (no dedup by token),
  ```

`authenticate_credentials` runs the provider chain (JWKS signature check, claim mapping),
then inserts a fresh `Session` keyed by a new random id into `sessions` and `last_activity`
(`crates/sqe-coordinator/src/session_manager.rs:203-217`). Nothing ever looks that session up
again, because the next request carries the JWT, not the id. The entries live until
`sweep_expired_sessions` (idle timeout) or until `evict_to_make_room` hits `max_sessions`, at
which point every new request pays a full scan of the map to find the LRU entry
(`session_manager.rs:150-196`, `self.sessions.iter()...min_by_key`).

Each request also emits an auth-success audit event (`flight_sql.rs:749-754`), and, because
`create_session_context` keys by username + token fingerprint
(`crates/sqe-coordinator/src/session_context.rs:209-216`), the DataFusion `SessionContext` is
shared correctly. So the waste is confined to `SessionManager`, the auth chain, and the
audit log, but it scales with request count, not client count.

## Why it matters

- A dbt run issues thousands of GetFlightInfo/DoGet/metadata calls per model. Each one
  creates a session, an audit event and a JWKS validation. With the default `max_sessions`
  cap reached, every request additionally runs an O(sessions) LRU scan under `DashMap`
  iteration.
- Audit volume for "authentication" becomes per-request noise, burying the real
  session-establishment events the OCSF 3003 mapping is meant to capture.
- The session table stops meaning "connected clients" for the dashboard and for
  `active_sessions` metrics.

## Proposed change

Dedupe by token fingerprint. In `SessionManager` add
`by_token: DashMap<TokenFingerprint, String /* session id */>` populated in
`authenticate_credentials` for bearer-token identities. In `get_session_from_request`,
compute `rest_catalog_token_fingerprint(token)` (already exists in sqe-catalog) and return the
existing session when present and unexpired; only fall through to `authenticate_credentials`
on miss. Emit the auth-success audit event only on the miss path. Remove the fingerprint entry
in `remove_session` and the sweeper.

## Acceptance criteria

- Unit test: 100 `get_session_from_request` calls with the same JWT produce one session and
  one auth-success audit event.
- Unit test: a JWT whose `exp` has passed produces a new session and evicts the old mapping.
- `tests/it/flight_sql_smoke.rs` with bearer auth asserts `active_sessions == 1` after a batch
  of queries.

## Related

- 2026-07-25 review P1-6 (auth failure invalidates the whole REST catalog cache) is the
  neighbouring identity-keying gap; the same fingerprint should key both.
- 403 (ticket carries query id) touches the same handlers.

## Resolution (2026-09-05)

`SessionManager::session_for_bearer` (`crates/sqe-coordinator/src/session_manager.rs`) indexes sessions by the SHA-256 fingerprint of the bearer token: a hit returns the existing session through `get_session` (so expiry eviction still applies) and a miss authenticates once and records the mapping. Fingerprints are dropped on `remove_session`, the idle/absolute sweeper, the COORD-04 LRU eviction and the expiry path. `get_session_from_request` in `flight_sql.rs` uses it and emits the auth-success audit event only when a session was created. Tests: `same_bearer_reuses_one_session_and_authenticates_once` (100 requests, one session, one provider call) and `expired_bearer_session_is_reauthenticated_not_reused`.

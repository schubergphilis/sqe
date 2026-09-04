# 209: Pre-auth rate limiting is decided per entry point and the Trino bearer path has none

- Scope: sqe-coordinator rate_limiter + mcp_server, sqe-trino-compat server, sqe-quack-server app
- Severity: medium
- Category: architecture
- Effort: S (hours)
- Status: open

## Problem

`AuthRateLimiter` is a coordinator type, and each protocol adapter chooses
whether and how to call it:

- Trino Basic auth: checked (`crates/sqe-trino-compat/src/server.rs:1090-1103`).
- Trino Bearer auth: not checked. The bearer branch at `server.rs:1082-1089`
  goes straight to `authenticate_bearer`.
- MCP: checked, but with a constant username so only the per-IP bucket has
  meaning:

```rust
// crates/sqe-coordinator/src/mcp_server.rs:18-22
impl sqe_mcp::AuthRateLimiter for AuthRateLimiter {
    fn allow(&self, peer_ip: &str) -> bool { self.check(peer_ip, "mcp-bearer").is_ok() }
}
```

- Quack: checked per IP via its own `check_auth_rate`
  (`crates/sqe-quack-server/src/app.rs:391-400`).
- Dashboard bearer guard (`web_auth.rs:207`): not checked.

The limiter is also disabled by default (`rate_limiter.rs:31-37`); only
`production_mode` forces it on (`config.rs:5171-5176`).

## Why it matters

Bearer validation is not free: an unknown `kid` triggers a JWKS refetch
(throttled to one per 30 s, `bearer_token.rs:138`), `token_exchange` and
`client_credentials_passthrough` call the IdP per attempt, and
`aws_iam` calls STS. An attacker with the Trino HTTP port open can drive that
cost with garbage bearers indefinitely, while the same attempt over Basic
auth is capped. Every new adapter (Quack was the most recent, MCP before it)
re-decides this; there is no single place where "a credential is about to be
validated" is guaranteed to pass the limiter.

## Proposed change

1. Move the gate into the auth layer: add `RateLimitedAuthProvider` in
   `sqe-auth` (or `sqe-coordinator`) that wraps `Arc<dyn AuthProvider>` and
   takes a `peer_ip` from a request-scoped extension, calling
   `AuthRateLimiter::check` before delegating. Key on
   `(peer_ip, username-or-token-fingerprint)` so bearers get the same
   per-tuple budget as passwords.
2. Wire it once where the chain is built (`main.rs`, `bin/sqe_server.rs`) so
   Flight, Trino, MCP, Quack and the dashboard all inherit it; delete the
   per-adapter calls.
3. Keep the adapter-level checks only where the protocol needs a specific
   status code (Trino 429 with `Retry-After`), mapping from the
   `SqeError::Auth("rate-limited")` the wrapper returns.

## Acceptance criteria

- Test in `sqe-trino-compat`: with a limiter configured at 1/min, the second
  Bearer request from the same IP returns 429.
- Test in `sqe-coordinator`: the wrapper denies before the inner provider is
  called (inner provider is a counting stub).
- `grep -rn "auth_rate_limiter" crates/sqe-trino-compat crates/sqe-mcp crates/sqe-quack-server`
  shows only status-code mapping, no `check` calls.

## Related

- 204 (same "every entry point re-implements it" shape).
- docs/internal/reviews/2026-07-25-production-readiness-review.md P1 "rate
  limiting defaults to disabled" is still true outside production_mode.

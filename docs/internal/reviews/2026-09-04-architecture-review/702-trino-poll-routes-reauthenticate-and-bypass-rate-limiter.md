# 702: Trino poll and cancel routes re-run the OIDC password grant on every request and bypass the pre-auth rate limiter

- Scope: sqe-trino-compat (server.rs), sqe-coordinator (Trino adapters in bin/sqe_server.rs)
- Severity: high
- Category: security
- Effort: S (hours)
- Status: open
- Issue: #622

## Problem

The pre-auth rate limiter (issue #57) is consulted only on `POST /v1/statement`, `crates/sqe-trino-compat/src/server.rs:1091-1104`. The three other authenticated routes authenticate first and check nothing:

- `get_results`, `server.rs:1454-1466`
- `get_queued_results`, `server.rs:1567-1579`
- `cancel_query`, `server.rs:1705-1717`

Each does:

```rust
} else if let Some((user, pass)) = extract_basic_auth(&headers) {
    match state.authenticator.authenticate(&user, &pass).await {
        Ok(s) => s,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    }
```

before looking up the query id. In production the adapter routes Basic auth through the provider chain with no caching, `crates/sqe-coordinator/src/bin/sqe_server.rs:662-690`, and the password provider performs a `grant_type=password` token request per call, `crates/sqe-auth/src/oidc_password.rs:60-70`. There is no session token or cookie tying a poll to the submitting request; the session is rebuilt from credentials each time.

## Why it matters

- **Credential-guessing oracle without throttling.** `GET /v1/statement/any/0` with Basic credentials returns 401 or 404 depending only on whether the password was right, and the rate limiter never runs. The POST route's limiter is defeated by using any GET.
- **IdP amplification.** A Trino client polls roughly once per second per running query (`DEFAULT_MAX_WAIT` is 1 s, `server.rs:52`). Ten dbt threads over Basic auth generate ten password grants per second against Keycloak for the whole run. Keycloak brute-force detection can lock the account of a legitimate user who is merely running a long query.
- **Latency.** Every page fetch pays a full OIDC round trip before the first byte.

Bearer-token callers are less affected (JWT validation is local after JWKS fetch), but the ownership check on poll is by username string (`server.rs:1481`, `:1596`), so any valid credential for the same username from any provider in the chain can read another session's results.

## Proposed change

1. Bind polls to the submitting session, not to credentials. On submit, store `session.id` (or a random `poll_token`) in `QueryHandle` and return it in the `nextUri` path or as `X-Trino-Set-Session`-style header the client replays. On poll, validate `(query_id, poll_token)` first and skip re-authentication entirely when it matches. Fall back to re-auth only for the Trino CLI's cross-node redirect case.
2. Until (1) lands: call `auth_rate_limiter.check` on every route before `authenticate`, and cache successful Basic-auth sessions in the Trino server for the lifetime of the query (`moka` keyed on `sha256(user:pass)` with TTL equal to the token's `expires_in`).
3. Compare ownership by `session.user.id`/subject plus provider, not the display username.

## Acceptance criteria

- Unit test: a `GET /v1/statement/{id}/{token}` with wrong Basic credentials, repeated past the limiter's budget, returns 429 with `Retry-After`.
- Unit test with a counting `TrinoAuthenticator` mock: submit plus N polls of one query invoke `authenticate` exactly once.
- `make test-integration FILTER=trino` run with Keycloak brute-force detection enabled does not lock the test user.

## Related

- 200-range findings on `sqe-auth` session caching.
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` issue #57 (auth rate limiting) was fixed for POST only.

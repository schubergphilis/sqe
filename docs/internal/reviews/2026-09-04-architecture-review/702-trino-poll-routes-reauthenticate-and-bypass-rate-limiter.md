# 702: Trino poll and cancel routes re-run the OIDC password grant on every request and bypass the pre-auth rate limiter

- Scope: sqe-trino-compat (server.rs), sqe-coordinator (Trino adapters in bin/sqe_server.rs)
- Severity: high
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
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

## Resolution (2026-09-05)

All three parts of the proposed change landed in
`crates/sqe-trino-compat/src/server.rs`. Nothing outside that crate changed.

### 1. Polls are bound to the submitting session, not to credentials

`QueryOwner { username, id, poll_token }` replaces the bare
`owner_username: String` on both `QueryHandle` and `PaginatedResult`. At submit
time `QueryOwner::new` mints one 32-byte random token (base64url, 43 chars, the
same construction as the OAuth2 PKCE verifier) and the same value goes onto the
in-flight handle, onto the finished result, and into every `nextUri` the query
emits.

**Wire format.** The token travels as a `{slug}` path segment, matching real
Trino's `/v1/statement/executing/{queryId}/{slug}/{token}`:

- `GET /v1/statement/{id}/{slug}/{token}` (results paging)
- `GET /v1/statement/queued/{id}/{slug}/{token}` (status poll)
- `DELETE` is accepted on both of the above, plus the legacy
  `DELETE /v1/statement/{id}` with no slug.

The two-segment `GET` forms are gone. Every client in the tree follows `nextUri`
verbatim (`sqe-cli/src/http.rs`, `sqe-bench/src/client/trino.rs`, the parity
scripts, the Trino JDBC driver and CLI), so nothing constructs these URLs by
hand and the change is invisible on the wire. A client mid-poll across a
coordinator restart gets a 404, as it already did — the query registry is
in-process.

`get_results`, `get_queued_results` and `cancel_query` now validate
`(query_id, poll_token)` **first**, with a non-short-circuiting comparison, and
skip authentication entirely on a match. Only a missing or wrong token falls
back to credentials.

### 2. The pre-auth rate limiter runs on every route

`authenticate_request` is the single auth ladder all four routes share. It
consults `TrinoAuthRateLimiter::check` immediately before anything that would
reach a provider — including the Bearer branch on `POST /v1/statement`, which
previously went straight to `authenticate_bearer`. Bearers are keyed on
`bearer:<truncated sha256>` so an attacker-supplied token never becomes a map
key or a log field. Denial is `429` with `Retry-After: 1` on every route, the
shape the POST route already used.

The bucket is the peer host, via a rewritten `strip_port` that no longer
truncates a bare IPv6 address from `x-forwarded-for` at its last colon (which
would have folded a whole `/64` into one bucket).

### 3. Successful Basic-auth sessions are cached

A `moka` cache (`build_auth_session_cache`, 4096 entries, 300 s TTL) holds
sessions the provider chain already validated, keyed on
`sha256(len(user) ‖ user ":" pass)`. The password is never stored. Every read
re-checks the session's own `token_expiry()`, so the TTL is a ceiling and never
extends a dead token. A cache hit does not spend pre-auth budget: the credential
was already validated, and charging it would make a legitimate 1/s poll exhaust
a 10/min budget in ten seconds.

The length prefix in the key removes a real (if contrived) confusion: a plain
`user ":" pass` digest makes user `a:b` / password `c` collide with user `a` /
password `b:c`, which would have shared one cached session between two
principals.

### 4. Ownership compares the subject, not the display name

`QueryOwner::is` compares `stable_principal_id` — the IdP `sub` when the
provider forwarded one, else the username — so a second provider in the chain
that mints the same display name cannot read another session's results.

### Tests

Ten new unit tests in `crates/sqe-trino-compat/src/server.rs`:

- `poll_with_wrong_credentials_is_rate_limited` — the acceptance criterion:
  wrong Basic credentials on a `GET` poll, repeated past a 2-attempt budget,
  return `429` with `Retry-After`, and the limiter is keyed on
  `(host, username)`.
- `submit_plus_polls_authenticates_exactly_once` — the acceptance criterion:
  a counting `TrinoAuthenticator` sees exactly one `authenticate` across a
  submit plus five polls that all carry credentials, and the limiter is
  consulted exactly once (the POST).
- `poll_with_valid_poll_token_needs_no_credentials` — a poll with a valid token
  and no `Authorization` header at all succeeds against an authenticator that
  rejects everything.
- `wrong_poll_token_without_credentials_does_not_leak_existence` — a wrong
  token with no credentials returns a `401` byte-identical to the one for a
  query id that does not exist.
- `bearer_post_past_limiter_budget_returns_429` — covers finding 209's Trino
  half; also asserts the bucket is a digest, not the raw bearer.
- `poll_tokens_are_long_and_unique`, `constant_time_eq_matches_ordinary_equality`,
  `strip_port_keeps_the_host`, `ownership_compares_subject_not_display_name`,
  `basic_session_key_hides_the_password`.

`cargo test -p sqe-trino-compat`: 163 passed, 0 failed.
`cargo clippy -p sqe-trino-compat --all-targets`: clean, no new warnings.

The `make test-integration FILTER=trino` criterion (Keycloak brute-force
detection enabled, test user not locked) has **not** been run and is still open.

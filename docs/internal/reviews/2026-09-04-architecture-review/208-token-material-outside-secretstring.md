# 208: Access and refresh tokens live in plain `String` in the legacy auth path

- Scope: sqe-auth (token_cache, authenticator, oidc_password, oauth), sqe-coordinator session_manager
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #561

## Problem

`FlightCredentials`, `Identity` and `Session` wrap secrets in `SecretString`
(issue #100), but the legacy `Authenticator` path that still backs
`client_credentials` and the no-providers fallback does not:

```rust
// crates/sqe-auth/src/token_cache.rs:5-9
#[derive(Debug, Clone)]
pub struct CachedToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
```

```rust
// crates/sqe-auth/src/authenticator.rs:29-32
struct ServiceToken { access_token: String, expiry: DateTime<Utc> }
```

```rust
// crates/sqe-auth/src/oidc_password.rs:13   pub access_token: String,
// crates/sqe-auth/src/oauth.rs:12           pub access_token: String,
```

The coordinator reads the plain value back out:

```rust
// crates/sqe-coordinator/src/session_manager.rs:280-284
if cached.access_token != session.access_token().expose() {
    ... SecretString::new(cached.access_token)
```

`CachedToken` derives `Debug`, so any `debug!(?cached)` or a panic message
that formats it prints the bearer. `TokenCache::expiring_sessions` and the
refresh loop clone these structs freely, so token bytes are copied around and
never zeroized on drop.

## Why it matters

The `SecretString` discipline exists so that log lines, panics and
`tracing` fields cannot leak bearers. Having two token representations means
the guarantee holds on the new provider chain and not on the legacy one, and
a future `debug!(?cached)` compiles cleanly. `client_credentials` is the mode
that carries a shared service token for every user (SEC-03), which is the
highest-value single credential in the process.

## Proposed change

1. Change `CachedToken` and `ServiceToken` fields to `SecretString`; drop the
   `Debug` derive or implement it with the redacting formatter
   `FlightCredentials` uses (`provider.rs:32-40`).
2. Change `TokenResponse.access_token` / `refresh_token` in `oauth.rs` and
   `oidc_password.rs` to `SecretString` with a serde `deserialize_with`.
3. Update `session_manager.rs:280-284` to compare via `expose()` on both sides
   and move the `SecretString` without re-wrapping.
4. Add a clippy-style guard test: a unit test that formats `CachedToken` with
   `{:?}` and asserts the token bytes are absent (mirrors
   `identity_debug_does_not_leak_token`).

## Acceptance criteria

- `grep -rn "access_token: String" crates/sqe-auth/src` returns nothing
  outside `#[cfg(test)]`.
- New redaction test passes; `cargo test -p sqe-auth -p sqe-coordinator --lib`
  passes.

## Related

- docs/internal/reviews/2026-06-26-codebase-analysis.md item 7 (plaintext
  session file) is resolved by the snapshot omitting tokens; this is the
  in-memory remainder.

## Resolution (2026-09-05)

The legacy path now carries the same `SecretString` discipline as the provider
chain. `CachedToken` (`crates/sqe-auth/src/token_cache.rs`) holds
`SecretString` / `Option<SecretString>` and has a hand-written redacting `Debug`
mirroring the `FlightCredentials` formatter in `provider.rs`; `ServiceToken`
(`crates/sqe-auth/src/authenticator.rs`) — the shared `client_credentials`
service token, SEC-03's single highest-value credential — is a `SecretString`
too. `TokenResponse.access_token` in `crates/sqe-auth/src/oauth.rs` and
`TokenResponse.access_token` / `.refresh_token` in
`crates/sqe-auth/src/oidc_password.rs` are `SecretString`: no `deserialize_with`
helper was needed because `sqe_core::SecretString` already implements
`Deserialize` from a JSON string, so the bearer never exists as a plain `String`
and is zeroized on drop. Call sites updated inside sqe-auth
(`authenticator.rs` session/cache construction and the background refresh loop,
`per_catalog.rs` which exposes at its `String`-returning boundary) and in
`crates/sqe-coordinator/src/session_manager.rs::get_session`, which now compares
via `expose()` on both sides and moves the `SecretString` into
`Credentials::new` instead of re-wrapping a plain `String`. New guard test
`token_cache::tests::debug_does_not_leak_token_material` formats a `CachedToken`
with `{:?}` and asserts the access/refresh bytes and the `eyJ` JWT prefix are
absent and the `<set>` sentinel is present (mirrors
`identity_debug_does_not_leak_token`). `cargo test -p sqe-auth` is green (282
passed, 0 failed) and `cargo check -p sqe-coordinator` builds.

Scope note: the acceptance grep is not fully clean yet. The newer provider-chain
response/cache types still hold raw `String` bearers under `#[derive(Debug)]` —
`oidc_provider.rs:66`, `oidc_client_credentials.rs:80`, `oidc_m2m.rs:28,84`,
`auth_code.rs:51`, `device_code.rs:69`, `token_exchange.rs:91`,
`pending_auth.rs:10`, `per_catalog.rs:52`. This change covered the legacy path
named in the problem statement; those eight are the same class of exposure and
should be swept in a follow-up.

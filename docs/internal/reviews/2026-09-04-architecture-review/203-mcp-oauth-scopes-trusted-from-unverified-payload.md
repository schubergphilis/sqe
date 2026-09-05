# 203: MCP OAuth mode reads scopes from the raw JWT payload and only refuses the anonymous provider

- Scope: sqe-mcp, sqe-core config validation, sqe-auth bearer_passthrough
- Severity: high
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Issue: #556

## Problem

`authenticate_oauth` hands the bearer to the auth chain, then decodes the
payload itself, without verification, to read `scope`/`scp`:

```rust
// crates/sqe-mcp/src/lib.rs:381-395
let scopes = token_scopes(token).ok_or_else(|| { ... })?;
if !scopes.contains(&state.config.read_scope) { return Err(...) }
```

```rust
// crates/sqe-mcp/src/lib.rs:426-443
fn token_scopes(token: &str) -> Option<BTreeSet<String>> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
```

That is sound only if the chain verified the signature. Config validation
acknowledges this and refuses one provider that does not:

```rust
// crates/sqe-core/src/config.rs:4776-4787
if self.auth.providers.iter()
    .any(|provider| matches!(provider, AuthProviderConfig::Anonymous { .. }))
{ errors.push("mcp.auth.mode = \"oauth\" cannot be combined with the anonymous auth provider ...") }
```

But `BearerPassthroughProvider` accepts any non-empty bearer without
validation (`crates/sqe-auth/src/bearer_passthrough.rs:62-83`) and is not in
that match. Neither are chains whose only bearer-capable provider falls
through to `client_credentials_passthrough` with `fallthrough_on_reject`.

There is also no per-provider signal in `Identity` saying "this token's
signature was verified", so the MCP layer cannot check at runtime; it can only
trust config.

## Why it matters

With `[[auth.providers]] type = "bearer_passthrough"` (a documented mode for
proxy-terminated deployments) plus MCP OAuth mode, an unsigned JWT such as
`header.{"scope":"sqe:mcp:read sqe:mcp:write","sub":"x"}.sig` authenticates,
passes the read gate, and with `allow_writes` and a passthrough role list that
includes a writer role, passes `authorize_write` too. The MCP surface is meant
to be reached by third-party agents; this is exactly where a forged token
shows up. Outside `production_mode`, nothing blocks this combination.

## Proposed change

1. Extend the `config.rs:4776` check to reject `BearerPassthrough` as well,
   with the same message, and cover `fallthrough_on_reject = true` on any
   provider preceding a passthrough one.
2. Add `Identity::token_verified: bool` (set `true` by `BearerTokenProvider`,
   `OidcPasswordProvider`, `TokenExchangeProvider`, `OidcClientCredentials`;
   `false` by passthrough, anonymous, api key, mTLS). In
   `authenticate_oauth`, refuse with `invalid_token` when it is `false`
   instead of reading scopes. This makes the guarantee a runtime property
   rather than a config lint.
3. Prefer scopes from the verified claims the provider already parsed
   (`BearerTokenProvider` has `token_data.claims`) over re-decoding the payload
   in `sqe-mcp`; expose them on `Identity` as `scopes: Option<BTreeSet<String>>`.

## Acceptance criteria

- Unit test in `crates/sqe-mcp/src/lib.rs`: an auth provider stub returning
  `token_verified: false` gets `401 invalid_token` in OAuth mode even when the
  payload carries the read scope.
- Config test in `sqe-core`: `mcp.auth.mode = "oauth"` with a
  `bearer_passthrough` provider fails `SqeConfig::validate`.
- Existing MCP tests (`request_bearer_is_forwarded_to_the_sqe_session`) still
  pass with `MockAuth` updated to set `token_verified: true`.

## Related

- 206 (identity-to-session duplication is where `token_verified` must be
  carried through).
- memory note "MCP server + auth modes" (!902).

## Resolution (2026-09-05)

`AuthProvider::authenticate_oauth` (`crates/sqe-auth/src/provider.rs`) defaults to `NotMyCredentials`; only `BearerTokenProvider` opts in, and it additionally refuses OAuth when no audience is configured under `allow_unbounded_audience`. `AuthChain::authenticate_oauth` never falls back to generic `authenticate`, and the MCP OAuth handler (`crates/sqe-mcp/src/lib.rs`) calls `authenticate_oauth`, so passthrough, anonymous and service-credential providers can no longer establish OAuth scopes from an unverified payload. Test `oauth_never_falls_back_to_generic_authentication`.

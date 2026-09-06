# 207: BearerTokenProvider leaves issuer, nbf and key-retirement to defaults

- Scope: sqe-auth (bearer_token)
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: open
- Issue: #560

## Problem

Audience is required by default (good, `bearer_token.rs:144-162`). Three
neighbouring checks are not:

1. Issuer is optional and silently skipped:

```rust
// crates/sqe-auth/src/bearer_token.rs:387-393
match &self.config.issuer {
    Some(iss) if !iss.is_empty() => { validation.set_issuer(&[iss]); }
    _ => {}
}
```

2. `nbf` is never validated. `Validation::new(Algorithm::RS256)` in
   jsonwebtoken 9.3.1 sets `validate_nbf: false` and `leeway: 60`
   (`~/.cargo/registry/src/.../jsonwebtoken-9.3.1/src/validation.rs:120-124`).
   The provider does not override either, and the 60 s leeway is not visible
   in config.

3. On refetch, keys that disappeared from the JWKS are kept until the 15
   minute TTL:

```rust
// crates/sqe-auth/src/bearer_token.rs:352-366
// Merge fresh keys over the existing cache. New keys win; keys that
// disappeared from the JWKS are retained until the TTL expires
```

4. Only `kty == "RSA"` keys are loaded (`bearer_token.rs:287-290`); an IdP
   publishing EC keys yields "Key ID not found in JWKS after refresh" with no
   startup diagnostic.

## Why it matters

- A multi-tenant IdP (Entra, Okta org with several authorization servers)
  shares one JWKS URL across issuers. Audience binding alone does not stop a
  token minted by a sibling issuer for the same audience string.
- An IdP that revokes a signing key on compromise expects relying parties to
  stop trusting it on the next JWKS fetch; SQE keeps honouring it for up to
  15 minutes. The comment justifies this for rotation, but rotation and
  revocation look identical to the client and the safe default is the
  revocation one.
- `nbf` off means pre-dated tokens are accepted; low impact but free to fix.

## Proposed change

1. Require `issuer` by default, mirroring the audience guard: empty issuer
   fails construction unless `allow_unbounded_issuer = true`. Populate it
   automatically from OIDC discovery when `jwks_url` came from a discovery
   document (`oidc_discovery.rs` already fetches `issuer`).
2. Set `validation.validate_nbf = true` and expose `leeway_secs` in
   `BearerTokenProviderConfig` (default 60).
3. In `refetch_jwks`, replace the merge with a replace-on-success, and keep a
   short grace window only for keys seen within the last N minutes and only
   when the fresh JWKS still contains at least one key. Document the
   trade-off in the field comment.
4. Load `kty == "EC"` keys via `DecodingKey::from_ec_components` and pin the
   algorithm per key from `alg` (`ES256`/`RS256`) instead of a single global
   `RS256`; warn at startup when a JWKS contains no loadable key.

## Acceptance criteria

- Unit tests in `bearer_token.rs`: missing issuer config fails `new` unless
  opted out; wrong `iss` claim rejected; `nbf` in the future rejected; a key
  removed from the mock JWKS is rejected after a refetch; an ES256 token
  validates against an EC JWKS.
- `cargo test -p sqe-auth` passes.

## Related

- docs/internal/reviews/2026-06-26-codebase-analysis.md item 8 (audience
  default) is fixed; this is the sibling set.

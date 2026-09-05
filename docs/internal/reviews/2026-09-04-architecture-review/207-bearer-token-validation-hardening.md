# 207: BearerTokenProvider leaves issuer, nbf and key-retirement to defaults

- Scope: sqe-auth (bearer_token)
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
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

## Resolution (2026-09-05)

All four items landed in `crates/sqe-auth/src/bearer_token.rs`. `issuer` is now
required at construction and mirrors the audience guard: an empty/missing
`issuer` fails `BearerTokenProvider::new` unless the new
`allow_unbounded_issuer` opt-out is set (plumbed through
`AuthProviderConfig::BearerToken` in `crates/sqe-core/src/config.rs` as a
`#[serde(default)]` bool and through `crates/sqe-auth/src/factory.rs`, so every
existing config that already sets `issuer` keeps working unchanged). There is no
discovery hook to populate it from — the provider takes `jwks_url` verbatim from
config and never fetches `.well-known/openid-configuration` — so the field docs
now state that the config loader/operator must set `issuer` to the discovery
document's `issuer`. Validation additionally marks `iss` a required claim when an
issuer is configured, because `jsonwebtoken` only compares the issuer when the
claim is present and would otherwise wave through a token that simply omits it.
`validate_nbf` is on and the clock-skew allowance is a config field
(`leeway_secs`, default 60 = `DEFAULT_LEEWAY_SECS`). `refetch_jwks` replaces the
cache on success instead of merging: a key missing from the fresh JWKS survives
only if it was seen inside `KEY_RETIREMENT_GRACE` (5 min, injectable for tests)
and only when the fresh JWKS still has at least one key — a successful fetch
yielding zero loadable keys empties the cache and logs a warning rather than
extending a retired key's life; the rotation-vs-revocation trade-off is written
up on the constant. JWKS parsing now loads `kty == "EC"` keys via
`DecodingKey::from_ec_components`, pins the algorithm per key from the JWK `alg`
(falling back to `kty`/`crv`, rejecting family mismatches such as an RSA entry
advertising `ES256`) instead of a single global RS256, and warns loudly when a
JWKS has no loadable key. Covered by new unit tests in the same file:
`new_rejects_missing_issuer_by_default`, `new_rejects_empty_string_issuer`,
`new_accepts_missing_issuer_when_explicit_opt_in`,
`new_accepts_non_empty_issuer_without_opt_in`, `issuer_match_succeeds`,
`missing_issuer_claim_rejected_when_issuer_configured`,
`nbf_in_the_future_returns_auth_failed`, `nbf_in_the_past_is_accepted`,
`es256_token_validates_against_ec_jwks` (static P-256 keypair, no new
dev-dependency), `rs256_token_rejected_against_ec_key_entry`,
`key_algorithm_pins_alg_per_key`,
`key_removed_from_jwks_is_rejected_after_refetch` (grace window zero),
`key_removed_from_jwks_is_honoured_inside_grace_window`, and
`empty_fresh_jwks_clears_the_cache`. `cargo test -p sqe-auth` is green (282
passed, 0 failed).

Follow-up worth its own finding: a *missing* `aud` claim still passes a
configured audience for the same `jsonwebtoken` reason `iss` did — only `iss`
was added to the required-claims set here, because forcing `aud` could break
deployments whose IdP omits it. `leeway_secs` is also not yet exposed in
`AuthProviderConfig::BearerToken`; it defaults to 60 from the factory.

# 212: production_mode accepts `policy.engine = passthrough` and no grant backend

- Scope: sqe-core config validation, sqe-coordinator policy_wiring
- Severity: low
- Category: security
- Effort: S (hours)
- Status: open

## Problem

The default policy engine enforces nothing:

```rust
// crates/sqe-core/src/config.rs:3225-3230
pub enum PolicyEngine {
    /// No policy enforcement: every plan passes through unmodified.
    /// Default for the open-source distribution.
    #[default]
    Passthrough,
```

`validate_production` (`config.rs:5154-5290`) checks anonymous providers,
rate limiting, transport, shared identities, worker secrets, SQL size,
vended credentials, mask keys and tag projection. It never checks that
`policy.engine != Passthrough` or that `access_control.backend != None`.
`build_policy_enforcer` returns `PassthroughEnforcer` silently for the
default (`policy_wiring.rs:47-49`, `:87`), with no startup log line at
`warn` level equivalent to the ones emitted for anonymous auth
(`factory.rs:257`) or bearer passthrough.

## Why it matters

A production deployment that forgets `[policy]` gets Polaris table-level
authorization only and no row filters, masks or restrictions, with no
signal in the logs and no refusal at boot. Every other dev-only default in
this codebase (anonymous, passthrough bearer, unkeyed hash, unprojected tags)
is either refused under `production_mode` or behind an `allow_*` flag that
shows up in a config diff. This one is not, so the hardening story is
inconsistent with itself.

## Proposed change

1. In `validate_production`, refuse `policy.engine = passthrough` unless
   `security.allow_no_policy_engine = true`, with a message naming the two
   real engines. Same for `access_control.backend = none` with
   `security.allow_no_grant_backend = true`.
2. In `build_policy_enforcer`, log at `warn` when Passthrough is selected:
   "policy enforcement DISABLED: every user sees every row and column the
   catalog allows".
3. Document both flags next to `allow_unkeyed_hash_masks` in
   `docs/site/book/src/operations/production-mode.md` (or wherever the
   production checklist lives).

## Acceptance criteria

- Config unit test: `production_mode = true` with default `[policy]` fails
  `validate`; with `allow_no_policy_engine = true` it passes.
- Helm `values-production.yaml` (if present) sets `policy.engine`.

## Related

- docs/internal/reviews/2026-07-25-production-readiness-review.md, "Ship an
  opinionated values-production.yaml" recommendation.

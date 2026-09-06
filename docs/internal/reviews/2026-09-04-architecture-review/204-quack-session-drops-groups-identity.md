# 204: Identity-to-Session conversion is copied five times and the Quack copy drops groups

- Scope: sqe-coordinator (session_manager, auth_session, maintenance_principal), sqe-mcp, sqe-quack-server
- Severity: high
- Category: security
- Effort: S (hours)
- Status: open
- Issue: #557

## Problem

The same conversion exists in five places:

| Site | Carries subject/email/groups |
|---|---|
| `crates/sqe-coordinator/src/session_manager.rs:249-268` (`identity_to_session`) | yes, via `with_identity` |
| `crates/sqe-coordinator/src/auth_session.rs:16-31` (`identity_to_session`, Trino) | yes |
| `crates/sqe-mcp/src/lib.rs:393-407` (`authenticate_oauth`) | yes |
| `crates/sqe-coordinator/src/maintenance_principal.rs:84-108` | yes |
| `crates/sqe-quack-server/src/session.rs:29-40` (`identity_to_core_session`) | **no** |

```rust
// crates/sqe-quack-server/src/session.rs:29-40
pub fn identity_to_core_session(identity: &Identity) -> CoreSession {
    let token_expiry = identity.expires_at.unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1));
    CoreSession::new(identity.user_id.clone(), identity.catalog_token.clone().unwrap_or_default(),
        identity.refresh_token.clone(), token_expiry, identity.roles.clone())
}
```

`grep -rn with_identity crates/sqe-quack-server/src/` returns nothing. The
doc comment says it is a "mirror" of the coordinator's function, which it no
longer is. Each copy also re-implements the one-hour fallback expiry and the
`unwrap_or_default()` empty-token fallback.

## Why it matters

Ranger policy items match on `users || roles || groups`
(`crates/sqe-policy/src/ranger_store.rs:492-495`). A group-only row-filter or
mask item never matches a session with empty groups, and "no item matched"
means no filter is injected. So a user who connects through Quack gets
UNFILTERED rows and RAW columns from a policy that filters them correctly
over Flight SQL, Trino or MCP. The same user, same token, different wire
protocol, different data. Audit events from Quack also lack subject and email.

Structurally, every new entry point (Quack was the latest) copies this
function and can quietly diverge; there is no compiler or test pressure toward
the canonical mapping.

## Proposed change

1. Add `impl From<Identity> for Session` (or `Session::from_identity(identity,
   fallback_token: Option<&str>)`) in `sqe-core` or `sqe-auth`, containing the
   expiry fallback, the token fallback and `with_identity`. `sqe-auth` already
   depends on `sqe-core::Session`, so this needs no new edge.
2. Replace all five sites with the shared function. `maintenance_principal`
   keeps its `with_maintenance_authority(true)` and id override as a wrapper.
3. Delete `crates/sqe-quack-server/src/session.rs::identity_to_core_session`.

## Acceptance criteria

- Unit test on the shared function asserting `subject`, `email`, `groups`,
  `roles`, `expires_at` and `catalog_token` all survive.
- Quack integration test: a group-only InMemory row filter applies to a query
  submitted over the Quack protocol.
- `grep -rn "Duration::hours(1)" crates/` shows one production occurrence.

## Related

- 203 (the `token_verified` flag proposed there rides on this function).
- docs/internal/reviews/2026-07-25-production-readiness-review.md P1
  "Groups" section describes the group-matching contract this breaks.

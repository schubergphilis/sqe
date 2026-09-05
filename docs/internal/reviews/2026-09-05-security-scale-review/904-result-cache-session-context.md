# 904: Result-cache keys omit default catalog/schema and stable principal identity

- Area: Result-cache authorization and correctness
- Priority: P1
- Order within area: 2
- Scope: security and large-data execution
- Severity: high
- Category: security
- Effort: M
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`ResultCache::cache_key` accepts only `user: &str` and `sql: &str` (`crates/sqe-coordinator/src/query_cache.rs:101`). Its callers supply `session.user.username` (`crates/sqe-coordinator/src/query_handler.rs:1087`, `:2274`). Neither default catalog/schema nor subject is included, although sessions carry those fields (`crates/sqe-core/src/session.rs:50`, `:83`). Cache lookup happens before resolving the current query plan.

## Why it matters

Two sessions for the same username, with different default schemas/catalogs, running `SELECT * FROM orders` can receive the first session's result. The authorization helper checks the cached table names, not whether the current SQL resolves to those tables. A user authorized for both tables therefore reliably passes the check while receiving data from the wrong namespace.

Cross-principal disclosure is additionally possible in deployments where distinct authenticated subjects share a username; that depends on identity-provider configuration and is not assumed universally.

## Proposed change

Include stable authenticated principal/provider identity and all result-affecting session state, especially default catalog/schema, in cache identity. Resolve dependencies before accepting a hit, or conservatively partition the cache per session. Fix alongside 902 without treating username as a security boundary.

## Validation and acceptance criteria

Create two schemas containing same-named tables with different rows. Run identical unqualified SQL from two same-user sessions with different defaults and verify distinct results. Test a same-session default-schema change. Where supported, test separate subjects with the same username. This is a static call-path finding, not a live reproduction.

## Related

Separate from 903: preserving SQL bytes does not add missing session context. Separate from 405: session allocation efficiency does not fix result isolation.

## Resolution (2026-09-05)

Cache identity is now `ResultCache::session_scope(session)`: a digest over session id, username, subject, roles, groups, default catalog, default schema, write branch and the current access token, so results never cross sessions, namespaces or credential refreshes; only the digest is used as a key. Both the lookup and the store call sites in `query_handler.rs` use it. Test `cache_scope_tracks_session_and_namespace` covers default schema, default catalog and session id changes. Same-username distinct-subject sharing is covered by the subject and session id components.

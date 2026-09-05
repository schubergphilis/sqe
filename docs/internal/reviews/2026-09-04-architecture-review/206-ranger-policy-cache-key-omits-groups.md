# 206: Ranger resolved-policy cache key includes roles but not groups

- Scope: sqe-policy (ranger_store)
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)

## Problem

The per-user resolved policy cache is keyed by username, namespace, table and
sorted roles:

```rust
// crates/sqe-policy/src/ranger_store.rs:897-906
fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
    let mut roles = user.roles.clone();
    roles.sort();
    format!("{}:{}:{}:{}", user.username, namespace, table, roles.join(","))
}
```

Policy matching uses groups as a third OR term:

```rust
// crates/sqe-policy/src/ranger_store.rs:492-495
let matched = users.iter().any(|u| u == &user.username)
    || roles.iter().any(|r| user.roles.contains(r))
    || groups.iter().any(|g| user.groups.contains(g));
```

So two sessions for the same username with different `groups` (a re-login
after a directory change, or the same user arriving via two providers, one
with `groups_claim` set and one without, see 204) share one cached
`ResolvedPolicy` for `cache_ttl_secs`.

`resolve_tags` (`ranger_store.rs:1251-1296`) has no per-user cache at all and
rebuilds `SessionIdentity` with `database: None, schema: None`, so the two
resolution paths already differ in what identity they see.

## Why it matters

Group membership is the normal enterprise Ranger authoring model (the
2026-07-25 review's P1 on groups). A user removed from `finance` keeps the
group-bound filter exemption until TTL; a user added to it does not gain the
mask until TTL. Worse, the FIRST session to populate the key decides for
everyone with that username and role set: a Quack session with empty groups
(204) can poison the entry for a Flight session that carried groups, so the
group-bound filter never applies to either for the TTL window. Roles were
added to the key precisely to avoid this class of bug; groups were missed.

## Proposed change

1. Include sorted `user.groups` in `cache_key`, and hash the composite (the
   key is unbounded in length today; a SHA-256 of
   `username|namespace|table|roles|groups` keeps moka key sizes fixed).
2. Add the same key discipline to `resolve_tags` results, or document why the
   bundle-level cache makes a per-user tag cache unnecessary.
3. Emit `sqe_policy_cache_key_cardinality` or log at debug when the number of
   distinct keys per username exceeds a threshold, so operators can see role
   and group churn.

## Acceptance criteria

- Unit test: two `SessionUser`s identical except for `groups` produce
  different cache keys; a group-only row filter applies to the second user
  right after the first was cached.
- Existing `ranger_store_wiremock.rs` tests pass.

## Related

- 204 (empty-groups sessions from Quack).
- docs/internal/reviews/2026-07-25-production-readiness-review.md, "P1: group
  support"; the runtime gap there is now partly closed by `item_matches`, this
  is the caching remainder.

## Resolution (2026-09-05)

`ranger_store::cache_key` now includes sorted, deduplicated groups alongside roles and uses a Debug tuple encoding so list boundaries cannot be forged with commas. Test `policy_cache_distinguishes_groups_and_list_boundaries`.

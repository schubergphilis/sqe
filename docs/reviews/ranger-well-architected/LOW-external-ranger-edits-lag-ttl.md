# External Ranger Admin edits not honored until cache TTL elapses

- **ID:** external-ranger-edits-lag-ttl
- **Pillar:** Reliability
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs:151-154,548-562,165; contrast :576-621

## Problem
`resolve()` caches `ResolvedPolicy` for `cache_ttl_secs` with no external-invalidation hook. Masks and row filters edited directly in Ranger Admin, which is the normal authoring path, are not honored until the TTL elapses. The result is a bounded over-permissive window. This is asymmetric with `resolve_tags`, which re-fetches every call.

## Proposed fix
Lower the default `cache_ttl_secs` for the resource path, or add `lastKnownVersion` / HTTP-304 polling (see `TODO(phase2)` at :165).

## Acceptance criteria
Documented. Optionally a shorter default or a version poll.

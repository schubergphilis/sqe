# resolve_tags re-downloads the Ranger bundle per query per tagged table

- **ID:** resolve-tags-no-bundle-cache
- **Pillar:** Performance
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs:576-621,166-196,554-561; crates/sqe-policy/src/plan_rewriter.rs:164

## Problem
`resolve_tags` calls `fetch_bundle` with NO cache lookup. `resolve()` caches only the `ResolvedPolicy`, not the bundle. Per query, per tagged table (serial in the rewriter loop), a full Ranger bundle HTTP download plus full JSON parse happens. A cold tagged table fetches the same bundle twice (once in `resolve`, once in `resolve_tags`). An N-tagged-table join becomes N downloads.

The pattern also defeats the "parse only at cache-miss frequency" assumption for the tag path.

## Proposed fix
Cache the bundle itself (it is user-independent) under a constant key with the existing TTL. Both `resolve` and `resolve_tags` read from it.

## Acceptance criteria
Stub Ranger with a download counter. A 3-tagged-table join issues 1 download cold and 0 warm, instead of 3 to 6.

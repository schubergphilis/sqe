# Tag masks/filters silently skipped on cache miss or when cache disabled

- **ID:** tag-mask-cache-miss-failopen
- **Pillar:** Reliability + Security
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-coordinator/src/tag_source_impl.rs:54,89-99; crates/sqe-policy/src/plan_rewriter.rs:156-190; crates/sqe-catalog/src/rest_catalog.rs:158-167

## Problem
`CacheTagSource::column_tags` returns an empty map on a cache miss, or when the metadata cache is disabled (`ttl_secs=0` -> `max_capacity(0)`). The rewriter only calls `resolve_tags` when `col_tags` is non-empty, so tag masks AND tag row-filters are silently skipped on a miss. The `resolve_tags` own `lit(false)` deny never fires.

The common query path pre-warms the cache, but `ttl_secs=0` globally and silently disables all tag-based governance. The "fail-safe: no tags" doc comment is inverted for a security control: an unknown state is treated as "no governance" instead of "deny."

## Proposed fix
Distinguish "no tags" from "tags unknown (miss or disabled)". On unknown, fail closed (deny) or load metadata synchronously. Change `TagSource::column_tags` to return an `Option` or enum. Reject metadata cache `ttl_secs=0` when `policy.engine != passthrough`.

## Acceptance criteria
With `ttl_secs=0` and a tag mask policy, the tagged column is masked or denied, not raw. A cache miss for a known-tagged table denies.

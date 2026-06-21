# Token-fingerprinted cache leaves stale tags for other users after a tag write

- **ID:** stale-tags-cross-token
- **Pillar:** Reliability
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-coordinator/src/catalog_ops.rs:754,933; crates/sqe-catalog/src/rest_catalog.rs:279-297,1002-1009,1250-1254; crates/sqe-coordinator/src/tag_source_impl.rs:24-33,89

## Problem
Metadata cache keys are token-fingerprinted (`{token}|{ns}.{table}`). `set_column_tags` and `set_table_properties` commit, then call `invalidate_table`, which evicts only the WRITER's own token key. But `properties_for`, the tag read path, scans ALL cached entries and returns the FIRST matching `|{ns}.{table}` in arbitrary order.

After a tag write, other users (and even the writer via first-match) keep reading the old tag map until TTL. The window is over-permissive: a removed mask exposes raw data, and an added tag is not applied. `invalidate_policy_cache` only flushes the Ranger bundle, not the column-to-tags map.

## Proposed fix
Invalidate by suffix across all tokens, or have `properties_for` return the freshest entry (track `validated_at`). Add `invalidate_table_all_tokens(ns, table)`.

## Acceptance criteria
After user A runs `SET TAGS`, user B's next query reflects the new tags immediately.

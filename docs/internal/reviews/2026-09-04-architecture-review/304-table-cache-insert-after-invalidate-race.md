# 304: Table metadata cache has no version check, so an invalidated entry can be re-inserted stale

- Scope: sqe-catalog (rest_catalog.rs TableMetadataCache, SessionCatalog::load_table)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

`TableMetadataCache` is a plain key/value moka cache with unconditional `insert`. Three code paths perform get-or-fetch followed by insert with no fencing against an invalidation that happens in between:

- `crates/sqe-catalog/src/rest_catalog.rs:1543-1564`: `load_table` awaits the REST `GET` (tens to hundreds of ms), then `insert_with_etag(cache_key, table, ..)`. If a writer commits and calls `invalidate_table_all_tokens` (`rest_catalog.rs:2270`) between the reader's GET returning snapshot S1 and its insert, the cache ends up holding S1 marked fresh (`validated_at: Instant::now()`) for a full soft TTL, and the writer's invalidation is lost.
- `rest_catalog.rs:326-331`: `revalidate` does `get` then `insert` of the same entry with a new `validated_at`. An invalidation between the two awaits is undone.
- `rest_catalog.rs:360-365` and `1577-1584`: the fire-and-forget ETag `HEAD` task calls `update_etag`, again `get` then `insert`, from a spawned task that may complete after an invalidation.

The windows are small for `revalidate` and `update_etag` but the `load_table` window is a full network round trip, and the coordinator runs many concurrent sessions against the same tables (dbt threads).

## Why it matters

Read-your-writes across sessions is what the invalidation exists for (`rest_catalog.rs:2267-2269`, "a different user cannot rebuild a SessionContext from stale metadata after INSERT"). Under concurrency it is probabilistic: a reader that happened to be planning while the writer committed pins the old snapshot for everyone who shares its token for `catalog.table_cache_ttl_secs`. The symptom is a query that "does not see the rows dbt just wrote" and cannot be reproduced serially. Combined with 303 the same race can pin stale tags.

## Proposed change

1. Add a per-key generation counter to `TableMetadataCache` (a `DashMap<String, u64>` bumped by every `invalidate*`). `load_table` snapshots the generation before the fetch and inserts only if unchanged (`insert_if_generation(key, gen, entry)`).
2. Make `revalidate` and `update_etag` compare-and-swap on the entry's `validated_at`/`etag` via the same generation, or drop `update_etag`'s background task entirely and read the ETag from the vendored client (the fork is ours; `load_table` can return response headers).
3. Alternatively key entries by `(table, metadata_location)` and let invalidation bump the expected location; a stale insert then lands under a key nobody reads.

## Acceptance criteria

- Unit test with a mock catalog that blocks inside `load_table`: start a load, invalidate while blocked, release; `get_fresh` returns `None` afterwards.
- Unit test: `update_etag` after `invalidate` does not resurrect the entry.
- Loom or a tokio `pause()`-driven test for `revalidate` vs `invalidate` ordering.

## Related

- 302, 303. Memory note "Writes need session-cache invalidation".

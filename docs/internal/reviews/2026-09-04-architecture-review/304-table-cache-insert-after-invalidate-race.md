# 304: Table metadata cache has no version check, so an invalidated entry can be re-inserted stale

- Scope: sqe-catalog (rest_catalog.rs TableMetadataCache, SessionCatalog::load_table)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #570

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

## Resolution (2026-09-05)

`TableMetadataCache` in `crates/sqe-catalog/src/rest_catalog.rs` gained a
generation fence. `CacheGeneration` is a snapshot of three counters, because
three eviction scopes exist and the race the finding describes happens when the
reader has NO entry in the cache for the writer's eviction to hit:

- `global_generation` (`AtomicU64`), bumped by `invalidate_all`;
- `table_generations` (`DashMap<String, u64>`) keyed by `{ns}.{table}`, bumped
  by `invalidate_table_all_tokens`;
- `key_generations` (`DashMap<String, u64>`) keyed by the full cache key, bumped
  by the single-key `invalidate`.

A per-key counter alone would not have closed the reported window: a cold
`load_table` has no entry, so an all-token eviction bumps nothing the reader is
watching. The per-table counter is what the reader snapshots.

`SessionCatalog::load_table` takes `generation(&cache_key, suffix)` before any
fetch and every insert on that path is fenced with it:

- the cold-load insert and the post-revalidation insert use
  `insert_if_generation`, which returns `false` and stores nothing when a commit
  landed while the `GET` was in flight;
- the 304 path calls `revalidate(key, &generation)` with the snapshot taken
  before the conditional `GET`;
- the fire-and-forget ETag `HEAD` task carries the same snapshot into
  `update_etag(key, &generation, etag)`, and is not spawned at all when the
  fenced insert was refused.

The fence is lock-free rather than a compare-and-swap, which moka does not
offer. Every `invalidate*` bumps its generation BEFORE it evicts, and every
fenced write checks the generation before AND after its own insert, rolling its
entry back on a mismatch. For any interleaving either the eviction runs after
the insert and removes it, or the bump is visible to the post-check and the
writer removes what it just wrote. Holding a lock across the insert instead
would serialise every session on one table.

Generation stamps come from one monotonic counter and are never reused, so an
absent map entry cannot compare equal to a generation an in-flight reader holds.
The maps are capped at `GENERATION_MAP_MAX_KEYS` (10 000) and cleared past it,
with a `prune_floor` raised first so an absent key reads as newer than anything
snapshotted before the prune. Pruning can therefore refuse an insert but never
admit a stale one; there is no ABA window.

The finding's option 3 (key entries by `(table, metadata_location)`) was not
taken: it changes the cache key shape that `properties_for` and the
cross-token eviction scan both parse.

Finding 303's work is intact: the `properties_for` freshness filter and the
`insert_aged` test hook are untouched, and
`table_metadata_cache_properties_ignore_stale_entries` still passes (its
`revalidate` call now passes a generation snapshot).

### Tests

Unit tests in `rest_catalog.rs`, module `invalidation_tests`:

- `load_blocked_across_an_invalidation_does_not_repopulate_the_cache` is the
  acceptance case. A `StubCatalog` blocks inside `load_table` (via the new
  `SessionCatalog::for_test_with_backend` hook), the test invalidates while it
  is blocked, releases it, and asserts `get_fresh` and `get_stale` both return
  `None` afterwards. Deterministic: no sleeps, no timing assumptions.
- `load_blocked_without_an_invalidation_populates_the_cache` is its positive
  control, so the assertion above cannot pass against a load that never caches.
- `update_etag_after_invalidate_does_not_resurrect_the_entry`.
- `revalidate_after_invalidate_does_not_undo_it` (deterministic ordering) and
  `revalidate_racing_invalidate_never_leaves_an_entry` (the same pair spawned
  concurrently with a varying number of yields, sampling interleavings a
  fixed-order test cannot reach).
- `fenced_insert_on_a_cold_key_is_refused_after_a_commit`,
  `fenced_insert_is_refused_after_a_single_key_invalidate`,
  `fenced_insert_is_refused_after_invalidate_all`, and
  `fenced_insert_succeeds_when_nothing_was_invalidated` cover the three scopes
  plus the ordinary path.
- `unfenced_insert_after_an_invalidation_resurrects_the_entry` is the control
  that pins the pre-fix behaviour of the primitive `load_table` used to call.

`cargo test -p sqe-catalog`: 570 lib tests plus every integration binary pass.
`cargo clippy -p sqe-catalog -p sqe-coordinator --all-targets`: no warnings in
the changed files.

Not done: no loom model. The concurrency argument rests on the bump-before-evict
ordering and the check-insert-check protocol, exercised by the deterministic
tests above plus the interleaving sampler, not on an exhaustive schedule search.
The docker-stack integration scenario (concurrent readers against a committing
writer) is also not covered.

# 303: Column-tag masking reads table properties from any cached entry regardless of freshness

- Scope: sqe-catalog (rest_catalog.rs TableMetadataCache::properties_for), sqe-coordinator tag_source_impl.rs
- Severity: high
- Category: security
- Effort: S (hours)
- Status: fixed (branch `fix/authz-policy-enforcement-p1`, 2026-09-05)
- Issue: #569

## Problem

The policy layer's `TagSource` resolves `sqe.column-tags` for a table from the shared metadata cache:

- `crates/sqe-coordinator/src/tag_source_impl.rs:89`: `self.cache.properties_for(catalog, &ns_display, table)`.
- `crates/sqe-catalog/src/rest_catalog.rs:443-483`: `properties_for` iterates `self.inner.iter()` and returns the properties of the first entry whose key ends in `|{warehouse}|{ns}.{table}`. It never checks `CachedTableEntry::validated_at` against `soft_ttl`; `get_fresh` (`rest_catalog.rs:309-316`) does, but `properties_for` bypasses it.
- `rest_catalog.rs:277-283`: the hard TTL of the underlying moka cache is 3600s, deliberately long so ETag revalidation has a stale copy to work from.

So the tag map that decides column masking can come from another principal's entry that is up to one hour old, even when the querying principal's own entry was refreshed seconds ago by `load_table`. The soft TTL, the ETag revalidation and the `HEAD` authorization check all protect the *data* path; none of them protect the *policy* path that reads the same cache.

SQE's own tag writes are covered because `catalog_ops.rs:1023-1040` evicts all tokens after `SET PROPERTIES`. Tag changes committed by any other engine (Spark `ALTER TABLE ... SET TBLPROPERTIES`, Polaris API, a Ranger-to-properties sync) are not: the entry sits until hard TTL, an SQE write, or eviction by capacity.

Iteration order of `moka::future::Cache::iter()` is not defined, so which principal's copy wins is nondeterministic. Two coordinators can answer differently for the same query.

## Why it matters

A tag added externally to newly classify a column as PII is not enforced by SQE for up to an hour on a warm coordinator. A tag removed externally keeps masking for up to an hour (annoying but safe). The first direction is a masking bypass window whose length depends on cache occupancy, not on any configured TTL, and the metric "catalog.table_cache_ttl_secs" gives operators a false sense of the bound. The memory note on "Spark/Ranger AC parity" records that `sqe.column-tags` fail OPEN in Spark; this is the SQE-side counterpart.

## Proposed change

1. In `properties_for`, only consider entries with `validated_at.elapsed() < soft_ttl`; return `None` (fail closed, which `tag_source_impl.rs:91-101` already treats as UNKNOWN) when none is fresh. Prefer the caller's own token entry when `token_fingerprint` is passed.
2. Better: store table properties once per `(warehouse, ns, table)` in a separate `PropertiesCache` populated on every successful `load_table` and ETag `304`, with the same soft TTL, so the policy path has its own freshness rule and does not depend on which user happened to load the table.
3. Wire the existing event-driven invalidation (catalog refresh POST, memory note "Catalog cache refresh") to evict this properties entry as well.

## Acceptance criteria

- Unit test: insert an entry with `validated_at` older than soft TTL under token A and a fresh one under token B with different `sqe.column-tags`; `properties_for` returns B's tags (or `None` if only A exists).
- Integration test (`make test-access-control`): change `sqe.column-tags` via a direct Polaris `POST .../tables/{t}` (not via SQE), then query as a warm session; masking follows the new tags within `catalog.table_cache_ttl_secs`.

## Related

- 302 (invalidation scope), 304 (stale entry resurrection).
- Memory notes: "Tag storage decision", "RENAME/DROP carry their tags", "Spark/Ranger AC parity".

## Resolution (2026-09-05)

`properties_for` in `crates/sqe-catalog/src/rest_catalog.rs` only serves table properties from a cache entry whose age is inside the metadata cache TTL and skips entries invalidated for another token, so column-tag masking never reads a stale property set left by a different session. Test `table_metadata_cache_properties_ignore_stale_entries` seeds an aged entry via the `insert_aged` test hook and asserts it is not returned.

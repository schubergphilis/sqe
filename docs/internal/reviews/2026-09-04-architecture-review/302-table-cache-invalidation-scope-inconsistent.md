# 302: Table metadata cache invalidation is per-token for schema changes but per-table for data changes

- Scope: sqe-catalog (rest_catalog.rs), sqe-coordinator catalog_ops.rs
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: open

## Problem

`TableMetadataCache` keys entries by `{token_fingerprint}|{warehouse}|{ns}.{table}` (`crates/sqe-catalog/src/rest_catalog.rs:801-811`). Two eviction primitives exist: `invalidate_table` evicts the caller's own key (`rest_catalog.rs:1748-1752`), and `invalidate_table_all_tokens` evicts every principal's copy (`rest_catalog.rs:1757-1761`). The catalog crate applies them inconsistently:

| Path | Eviction | Evidence |
|---|---|---|
| `SessionCatalogBridge::update_table` (INSERT/DELETE/MERGE commits) | all tokens | `rest_catalog.rs:2264-2272` |
| `SessionCatalog::commit_schema_update` (ADD/DROP/RENAME COLUMN, SET PROPERTIES) | own token only | `rest_catalog.rs:2145-2146` |
| `SessionCatalogBridge::create_table` | own token only | `rest_catalog.rs:2219-2227` |
| `SessionCatalogBridge::drop_table` | own token only | `rest_catalog.rs:2238-2246` |
| `SessionCatalogBridge::rename_table` | none | `rest_catalog.rs:2252-2254` |

The coordinator compensates in one place only: after `SET PROPERTIES` it calls `invalidate_table_all_tokens` (`crates/sqe-coordinator/src/catalog_ops.rs:1023-1040`, comment "Evict every token's entry so the updated properties are visible to all users"). The schema-evolution commit at `catalog_ops.rs:806-808` does not, and neither does `rename_table` at `catalog_ops.rs:383-386` (it invalidates session contexts, `query_handler.rs:1425`, which does not touch this cache).

The fresh-hit path returns the cached `Table` after a `HEAD` authorization check only (`rest_catalog.rs:1432-1445`); a HEAD says nothing about schema version. So after user A runs `ALTER TABLE t ADD COLUMN c`, user B's next query planning against `t` gets B's own cached entry with the old schema for up to the soft TTL, and `SELECT c` fails with "column not found" while A sees it.

## Why it matters

Schema changes are the case where staleness is most visible: dbt runs as one principal and BI users as another, so the "column does not exist" report arrives from a different person than the one who ran the migration, minutes after it succeeded. DROP then CREATE with a different schema by A leaves B holding a `Table` whose `FileIO` credentials and metadata location point at the dropped table's files. The comment on `update_table` already states the rule ("A table commit is visible to every principal"); the other four paths simply do not follow it.

## Proposed change

1. In `rest_catalog.rs`, make every mutation on `SessionCatalogBridge` and `SessionCatalog` (`create_table`, `drop_table`, `rename_table` for both src and dest, `commit_schema_update`, `register_table`) call `invalidate_table_all_tokens`. Keep `invalidate_table` for the authorization-revoke case at `rest_catalog.rs:1663-1665` where the per-principal scope is the point.
2. Delete the compensating call in `catalog_ops.rs:1027-1040` once the crate does it, so there is one place that owns the rule.
3. Add a doc comment on `TableMetadataCache` stating the invariant: any commit evicts all tokens; only authorization failures evict one.

## Acceptance criteria

- Unit test in `rest_catalog.rs` with two token fingerprints cached for the same table: after `commit_schema_update` (mocked via wiremock), both entries are gone. Same for `create_table`, `drop_table`, `rename_table` through `SessionCatalogBridge`.
- Integration test in `make test-integration`: session A `ALTER TABLE ADD COLUMN`, session B (different token) `SELECT new_col` succeeds immediately with `catalog.table_cache_ttl_secs = 300`.

## Related

- Memory note "Writes need session-cache invalidation" covers the coordinator-side session context; this issue is the crate-side metadata cache.
- 303 (tag reads across tokens make the same inconsistency a security problem), 304 (insert-after-invalidate race).

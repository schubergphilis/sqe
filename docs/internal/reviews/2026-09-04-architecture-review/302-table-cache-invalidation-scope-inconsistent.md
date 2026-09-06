# 302: Table metadata cache invalidation is per-token for schema changes but per-table for data changes

- Scope: sqe-catalog (rest_catalog.rs), sqe-coordinator catalog_ops.rs
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #568

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

## Resolution (2026-09-05)

Every commit path in `crates/sqe-catalog/src/rest_catalog.rs` now evicts all
tokens, so the rule lives in the crate that owns the cache:

| Path | Before | After |
|---|---|---|
| `SessionCatalogBridge::create_table` | own key | `invalidate_table_all_tokens` |
| `SessionCatalogBridge::drop_table` | own key | `invalidate_table_all_tokens` |
| `SessionCatalogBridge::rename_table` | none | `invalidate_table_all_tokens` for source AND destination |
| `SessionCatalogBridge::register_table` | none | `invalidate_table_all_tokens` |
| `SessionCatalog::commit_schema_update` | own key | `invalidate_table_all_tokens` |
| `SessionCatalogBridge::update_table` | all tokens | unchanged |

`rename_table` and `register_table` also gained the eviction they never had.
Both evict on failure as well as success: a partially applied rename is exactly
when a surviving entry describes a table that is not there.

`TableMetadataCache::invalidate` (single key) is now documented as reserved for
`SessionCatalog::authorize_table_access`, the authorization-revoke path where
dropping one principal's vended credentials is the point. The type-level doc
comment on `TableMetadataCache` states the invariant: any commit evicts all
tokens; only authorization failures evict one.

The compensating call at the `ALTER TABLE SET TBLPROPERTIES` site in
`crates/sqe-coordinator/src/catalog_ops.rs` is deleted; the comment there now
points at the crate-side rule. The other `invalidate_table_all_tokens` calls in
`catalog_ops.rs` (tag authoring, tag rollback, schema-evolution tag carry) were
left in place: they are redundant now but they are inside `_guard`-held
read-modify-write sequences whose ordering is load-bearing, and rewriting them
was out of scope for this change.

`table_metadata_cache_suffix` replaces two hand-rolled `format!`s so the
eviction scope and the fenced-insert scope (finding 304) cannot drift apart.

### Tests

Unit tests in `rest_catalog.rs`, module `invalidation_tests`. Each seeds the
table under the committer's fingerprint AND a second principal's, asserts both
are present, then asserts both are gone:

- `bridge_create_table_evicts_every_token`
- `bridge_drop_table_evicts_every_token`
- `bridge_rename_table_evicts_every_token_for_source_and_destination`
- `bridge_register_table_evicts_every_token`
- `bridge_update_table_evicts_every_token`
- `commit_schema_update_evicts_every_token` (wiremock POST 200)
- `single_key_invalidate_leaves_other_principals_cached` is the control: it
  pins the pre-fix behaviour, so the six tests above cannot pass vacuously
  against a cache that stores nothing.

The DDL verbs are driven through a `StubCatalog` backend behind the new
`SessionCatalog::for_test_with_backend` test hook rather than a mock REST server
per verb; `commit_schema_update` goes through wiremock because it talks HTTP
directly.

`cargo test -p sqe-catalog`: 570 lib tests plus every integration binary pass.
`cargo clippy -p sqe-catalog -p sqe-coordinator --all-targets`: no warnings in
the changed files.

The integration-test acceptance item (session A `ALTER TABLE ADD COLUMN`,
session B reads the new column immediately with a 300 s TTL) is NOT done: it
needs the docker Polaris + RustFS stack. The unit tests cover the eviction
scope; the end-to-end freshness claim is unverified.

# Materialized views with full refresh (#905)

## Why

Materialized views migrated from other engines should land as real MVs that any Iceberg engine can read. Today `CREATE MATERIALIZED VIEW` is refused (`crates/sqe-coordinator/src/query_handler/ddl.rs:102-106`), `DROP MATERIALIZED VIEW IF EXISTS` is a no-op, and `system.metadata.materialized_views` is always empty.

## What Changes

- New statements: `CREATE [OR REPLACE] MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, `DROP MATERIALIZED VIEW [IF EXISTS]`, `SHOW CREATE MATERIALIZED VIEW`.
- Storage is one plain Iceberg table. The definition, the definer and each source's `(table uuid, snapshot id)` are stored in table properties and in the refresh snapshot summary, using the field names proposed in apache/iceberg#11041. Other engines read the MV as a table.
- A refresh is one atomic replace commit through the schema-changing CREATE OR REPLACE path (#892). A failed refresh leaves the previous contents.
- `system.metadata.materialized_views` lists MVs with, per source, the snapshot used and the current one, and a `freshness` of `FRESH` or `STALE`.
- An MV over a source that has a mask or row filter in force for the creating or refreshing user is refused and names the source.
- One OpenLineage event per refresh. dbt-sqe gets a `materialized_view` materialization.
- **Refresh identity (decision for the owner):** SQE has no service account, so a refresh runs as the caller, with the caller's token. Only the definer or a catalog admin may refresh. See design D3.

## Capabilities

### New Capabilities

- `materialized-views`: MV DDL, storage format, refresh and freshness reporting.

### Modified Capabilities

None.

## Impact

- `sqe-sql`: pre-parse `REFRESH MATERIALIZED VIEW` and `SHOW CREATE MATERIALIZED VIEW`.
- `sqe-coordinator`: `classify_materialized_view` (`query_handler.rs:5938`) and new handlers; the DROP no-op goes.
- `sqe-write`: refresh via the one-commit replace; snapshot summary fields.
- `sqe-catalog`: `build_materialized_views_table` (`system_metadata.rs:327`) fills rows; its doc and test change.
- `sqe-policy`: a source-policy probe (mask or row filter in force for the user).
- `adapters/dbt-sqe`: `materialized_view` materialization; `list_relations_without_caching` starts returning MVs.
- Out of scope: scheduling, query rewrite to use an MV, incremental refresh.

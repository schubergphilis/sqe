# 500: CREATE OR REPLACE TABLE AS SELECT is drop-then-create and resolves the drop in the default catalog

- Scope: sqe-coordinator write path (query_handler CTAS arm, write_handler CTAS)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

`CREATE OR REPLACE TABLE ... AS SELECT` on an Iceberg table is implemented as an
unconditional `drop_table` followed by an ordinary CTAS. The two steps are not
one transaction and the drop does not honour the target's catalog qualifier.

`crates/sqe-coordinator/src/query_handler.rs:1676-1680`:

```rust
// Handle CREATE OR REPLACE TABLE AS SELECT:
// drop the existing table first, then create a new one.
if ct.or_replace {
    self.drop_table_if_exists(session, &ct.name).await?;
}
```

`crates/sqe-coordinator/src/query_handler.rs:6780-6810` builds the catalog for
that drop with `SessionCatalog::for_session(&self.config, ...)`, i.e. always the
configured default warehouse, and resolves the name through
`catalog_ops::resolve_table_ident`, which calls `parse_table_ref`
(`crates/sqe-coordinator/src/catalog_ops.rs:1884-1910`) whose 3-part arm reads
"ignore catalog prefix". The CTAS that follows resolves the target catalog
correctly via `resolve_target_catalog(catalog_qualifier(table_name), session)`
(`crates/sqe-coordinator/src/write_handler.rs:1278`), so the drop and the
create can address two different catalogs.

`WriteHandler::handle_ctas_streaming` receives `ct.or_replace` and discards it
(`write_handler.rs:1225`, bound as `_or_replace`).

The Hive path already refuses the same statement for exactly this class of
reason (`crates/sqe-coordinator/src/hive_write.rs:240-253`); the Iceberg path
ships it.

## Why it matters

Three concrete data-loss shapes today:

- `CREATE OR REPLACE TABLE t AS SELECT ... FROM t` (self-referencing full
  refresh, a common dbt pattern) drops `t` before the SELECT is planned. The
  SELECT then fails with "table not found" and the original data is gone from
  the catalog.
- Any failure in the SELECT or the write after the drop (policy denial, S3
  error, four lost commit races) leaves the user with no table. The CTAS
  rollback at `write_handler.rs:1404-1410` only undoes the new table.
- `CREATE OR REPLACE TABLE ws_b.ns.t AS SELECT ...` drops `ns.t` in the
  default warehouse, not in `ws_b`, then creates `ws_b.ns.t`. A same-named
  table in the default catalog is silently destroyed; if none exists the CTAS
  fails with "already exists" in `ws_b`.

Iceberg also loses the table's history: a replace via drop gives the new table
a fresh UUID and no prior snapshots, so `rollback_to_snapshot` and time travel
across the replace are impossible.

## Proposed change

Implement replace as an Iceberg transaction on the existing table, not a drop:

1. In `handle_ctas_streaming`, when `or_replace` is set and the table exists,
   write the new data files first (existing streaming path), then commit one
   `rewrite_files()` that deletes every live data file plus covered position
   deletes (reuse `extend_with_superseded_deletes`) and adds the new files,
   combined with a schema update when the SELECT schema differs. If the vendor
   fork exposes a replace-table transaction (`Catalog::update_table` with
   `TableUpdate::AddSchema` + `SetCurrentSchema` is enough), use that; the
   commit goes through `commit_with_retry`.
2. When the table does not exist, fall through to the current create path.
3. Delete `drop_table_if_exists` from `query_handler.rs`, or at minimum route it
   through `resolve_target_catalog` + `create_catalog_bridge` so it cannot hit
   the wrong catalog.
4. Reject `or_replace` combined with a SELECT that reads the target only if
   step 1 cannot be made snapshot-safe; with step 1 the read completes before
   the swap so self-reference is fine.

## Acceptance criteria

- Integration test: `CREATE OR REPLACE TABLE t AS SELECT * FROM t WHERE ...`
  succeeds and the table history contains the pre-replace snapshot.
- Integration test: a failing SELECT inside `CREATE OR REPLACE` leaves the
  original table and its data intact.
- Integration test with two catalogs: `CREATE OR REPLACE TABLE cat_b.ns.t`
  never touches `cat_a.ns.t`.
- `grep -n drop_table_if_exists crates/sqe-coordinator/src` returns nothing, or
  the function takes the resolved catalog as an argument.

## Related

- 508 (dead buffered CTAS twin also ignores `or_replace`).
- 513 (no in-process write tests; the three scenarios above need the sqlite rig).
- `docs/internal/plans/2026-07-13-sqe-write-crate-extraction.md` moves this
  code; land the fix before or with the move.

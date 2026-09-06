# 500: CREATE OR REPLACE TABLE AS SELECT is drop-then-create and resolves the drop in the default catalog

- Scope: sqe-coordinator write path (query_handler CTAS arm, write_handler CTAS)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #592

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

## Resolution (2026-09-05)

`CREATE OR REPLACE TABLE ... AS SELECT` is no longer a drop followed by a
create, and the drop that resolved against the default warehouse is gone with
it. The whole statement is handled inside `WriteHandler::handle_ctas_streaming`
against the catalog the target name resolves to.

**What changed**

- `crates/sqe-coordinator/src/query_handler.rs`
  - The `if ct.or_replace { self.drop_table_if_exists(...) }` call in the CTAS
    arm is deleted, and so is `drop_table_if_exists` itself.
    `grep -n drop_table_if_exists crates/sqe-coordinator/src` returns nothing.
- `crates/sqe-coordinator/src/write_handler.rs`
  - `handle_ctas_streaming` binds `or_replace` (it was `_or_replace`) and, when
    it is set, asks the *resolved* `SessionCatalog` whether the target exists.
    The SELECT is planned and policy-enforced first, as before, but nothing is
    dropped before it runs.
  - **In-place swap** (`ctas_replace_in_place`) when the SELECT already fits the
    live table: the new data files are written into the live table, then ONE
    `rewrite_files` commit removes every live data file plus the position
    deletes those files supersede (`extend_with_superseded_deletes`) and adds
    the new ones. It goes through `commit_rewrite_with_retry` (506) with
    `op = "ctas-replace"`. The table keeps its UUID, its snapshot history (so
    time travel and `rollback_to_snapshot` cross the replace), its properties
    and its grants. `CREATE OR REPLACE TABLE t AS SELECT ... FROM t` is safe:
    the stream is fully consumed against the pre-swap snapshot before the
    commit. A failing SELECT, a failing write or four lost races leave the
    original data in place and the `WriteCleanupGuard` deletes the parquet
    files the attempt wrote.
  - **Staged swap** otherwise: `ctas_replace_is_in_place` decides. The
    streaming writer stamps every batch onto the live table's schema and
    partitions with the live default spec, so a different column set, order,
    type or nullability — or a `PARTITIONED BY` naming different source columns
    or transforms — cannot be written into the live table at all. Those
    statements create `<name>__sqe_replace_<uuid>` in the same namespace, write
    and commit into it, then `drop_table` the target and `rename_table` the
    staged table into place. Both swap calls are metadata-only. The existing
    table still survives a failing SELECT or write; only the swap itself is two
    catalog calls, and a failure between them names the staged table in the
    error with the `ALTER TABLE ... RENAME TO` that finishes it by hand.
  - When the table does not exist, `or_replace` falls through to the ordinary
    create path (the create now uses `create_ident`, which is the target name
    unless a staged swap is in flight).
- Everything above runs against `resolve_target_catalog(...)`'s catalog, so
  `CREATE OR REPLACE TABLE ws_b.ns.t` can no longer touch `ns.t` in the default
  warehouse.

**Known limitation, deliberate.** The in-place path does not re-apply the
statement's `WITH` / `TBLPROPERTIES` clause: an in-place replace keeps the live
table's properties (`ALTER TABLE ... SET TBLPROPERTIES` changes them). The
staged path creates a fresh table, so it applies them and loses the history —
the trade is documented in the code and logged at `warn` when it fires.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler merge_sql
hive_write`, 132 passed / 0 failed)

- `write_handler::tests::ctas_replace_is_in_place_for_a_matching_schema`
- `write_handler::tests::ctas_replace_is_not_in_place_when_the_schema_differs`
  — added column, widened type, renamed column, changed nullability.
- `write_handler::tests::ctas_replace_in_place_requires_matching_partitioning`
  — `PARTITIONED BY (region)` on a table whose default spec is `region` swaps
  in place; `PARTITIONED BY (country)` does not.

**Not covered by unit tests.** The three integration scenarios in the
acceptance criteria (`CREATE OR REPLACE TABLE t AS SELECT * FROM t WHERE ...`
keeping the pre-replace snapshot in its history, a failing SELECT leaving the
original data intact, and a two-catalog run where `cat_b.ns.t` never touches
`cat_a.ns.t`) need a live Polaris + object store, i.e. the docker rig
(`make test-integration`). The unit tests cover the predicate that routes
between the two paths, which is where the correctness decision is made.

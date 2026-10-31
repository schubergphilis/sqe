**STATUS 2026-10-31: MERGE `UPDATE SET *` / `INSERT *` (`fix/merge-update-set-star`).**

Reported from the playground4 Databricks migration: `MERGE ... UPDATE SET *`
does not parse. sqlparser 0.62 accepts neither `UPDATE SET *` nor `INSERT *`.

- `sqe-sql/src/merge_star.rs`: a parse-gated front-door fallback turns
  `THEN UPDATE SET *` into `UPDATE SET __sqe_merge_star__ = NULL` and
  `THEN INSERT *` into `INSERT (__sqe_merge_star__) VALUES (NULL)`.
- `sqe-write/src/merge_sql.rs` `classify_merge_clauses`: a clause made of the
  marker alone expands against the loaded target schema. `UPDATE SET *`
  becomes `"col" = <source alias>."col"` per user column (V3 row-lineage
  columns excluded); `INSERT *` becomes `INSERT ROW`. Both require every
  target column in the source. `MergeOp::Update` now holds a
  `Cow<[Assignment]>`; both CoW and MoR handlers read it unchanged.

Verified: sqe-sql unit tests (4), sqe-write merge_sql (31, incl. an executed
star merge), live IT `merge_rows_affected_e2e::merge_update_set_star_and_insert_star`
(CoW + MoR, reordered source with an extra column, missing-column refusal),
and all 12 `make test-integration FILTER=merge` tests.

Found on the way: `SELECT <expr> FROM t ORDER BY <column not projected>`
returns unordered rows when the scan has more than one partition (MoR
table): `restore_single_partition_root` sees no ordering at the projection
root and adds `CoalescePartitionsExec` over a per-partition `SortExec`.
Not fixed here; it gets its own branch (`fix/order-by-dropped-sort-key`).
The star IT orders by the projected value to stay clear of it.

**NEXT:** fix the dropped sort key; then the handover asks still open: `USE` does not hold across statements on
the portal statement API; about 30 statements a minute.

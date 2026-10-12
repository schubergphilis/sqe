**STATUS 2026-10-12: column `DEFAULT` is written on INSERT and MERGE INSERT (`fix/column-default-828`, #828, openspec iceberg-matrix-parity 5.7/5.10).**

`CREATE TABLE ... DEFAULT` and `ALTER TABLE ADD COLUMN ... DEFAULT` stored `write_default` on the schema, but no write path read it, so an INSERT that omitted the column silently wrote NULL. Now `omitted_column_sql` renders an omitted column's `write_default` as `arrow_cast('<iceberg json text>', '<arrow type>')`. Three places use it:
- `reorder_insert_select`, for INSERT with a column list.
- The copy-on-write MERGE.
- The equality-delete MERGE NOT MATCHED INSERT, through `MergeNames::column_defaults`.

An explicit NULL stays NULL. Binary, fixed and UUID defaults cannot round-trip through a text cast, so they are refused rather than written as NULL.

Tests:
- Unit: rendering per type (int, long, double, bool, string with a quote, date, decimal).
- SQLite rig: INSERT and MERGE INSERT that omit columns.
- Polaris: `v3_e2e.rs::create_table_default_applies_on_insert_and_merge`, plus the post-ALTER insert in `alter_add_column_with_default_on_v3_table`. The V3 suite passes 17/17.

The read side is still open. Verified live: a row written before `ALTER ADD COLUMN ... DEFAULT` reads NULL instead of the `initial_default`. The vendored iceberg-rust scan and reader do handle it, so SQE's own scan path must be NULL-filling missing fields. The matrix entry `sqe:column-default-values:v3` is `partial` until that is fixed.

**NEXT:** #830 (`initial_default` on read for files that predate the column); then dbt-sqe per-connection transactions (openspec `single-table-transactions` 5.4), #816 and #803.

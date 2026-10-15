**STATUS 2026-10-15: SQL `DROP TABLE ... PURGE` is refused instead of silently keeping the data (`fix/sql-drop-purge-841`, #841).**

`catalog_ops::drop_table` matched `Statement::Drop` and ignored its `purge` flag, so `DROP TABLE t PURGE` removed `t` from the catalog and left every data file behind, while the DDL reference said `PURGE` deletes data files immediately. A user asking for deletion got an orphaned dataset and a success.

The Iceberg path now refuses `purge: true` with `SqeErrorCode::NotSupported` before touching the catalog. The message says nothing was dropped and points to `CALL system.drop_table(table => ..., purge => true)`. Hive already refused `PURGE` in `hive_ddl_handler.rs`. The DDL reference row is corrected.

Refuse-with-pointer rather than routing through the procedure: the procedure's purge is implemented in !1435, which is still open, and on main it returns "not yet implemented". Building on it would have made this a stacked MR.

Verified: `make test-integration FILTER=test_drop_table` 3/3, including the new `test_drop_table_purge_is_refused_and_keeps_the_table` (both `DROP TABLE ... PURGE` and `DROP TABLE IF EXISTS ... PURGE` are refused, the table still returns its row, and a plain `DROP` then succeeds).

**NEXT:** once !1435 merges, route SQL `PURGE` through the same purge path as `system.drop_table`, including its shared-location refusal, and drop the refusal.

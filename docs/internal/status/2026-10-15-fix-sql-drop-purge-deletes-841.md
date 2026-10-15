**STATUS 2026-10-15: SQL `DROP TABLE ... PURGE` deletes the table's files through `system.drop_table` (`fix/sql-drop-purge-deletes-841`, #841).**

!1437 made `DROP TABLE ... PURGE` refuse rather than silently keep the data, because the procedure's purge was still in an open MR. Both are merged now (!1435, !1437), so the SQL statement runs the purge itself.

`QueryHandler`'s `Drop` arm, after the Hive branch, resolves the name with the new `CatalogOps::purge_target` (the same resolution as the plain drop; `IF EXISTS` on a missing table returns `None`, a missing namespace still errors) and calls `MaintenanceHandler::handle` with `ProcedureCall::DropTable { purge: true }`. Going through `handle`, not the inner `drop_table`, keeps the admin gate: a catalog DROP grant alone would otherwise delete files the CALL would refuse to delete. The gate's denial is reworded for a SQL caller (`sql_purge_denial`), with the code kept as `AccessDenied`. The shared-location refusal and the purge audit event come with it. The `PURGE` arm in `catalog_ops.drop_table` is now an invariant error. A multi-level namespace is refused (`TableRef` holds one level).

The changelog fragment from !1437 is replaced, not added to: no release has shipped it.

Verified: `make test-integration FILTER=drop_table` 7/7, including `sql_drop_table_purge_deletes_the_table_prefix` (prefix empty on RustFS via an independent client, `IF EXISTS` on the dropped table is a no-op, a missing table without `IF EXISTS` fails) and `sql_drop_table_purge_needs_an_admin_role` (no roles: `AccessDenied`, the table and its data file remain).

Open decision: whether `PURGE` should need an admin role or only the catalog's drop privilege. Admin-only matches the CALL and is the fail-closed choice.

**NEXT:** #844 (incremental scans read column tags from the scan provider).

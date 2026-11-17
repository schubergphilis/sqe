**STATUS 2026-11-17: `system.jdbc` serves every table the Trino JDBC driver reads, and lists views (`fix/system-jdbc-session-catalog`).**

Reported symptom: DBeaver over the Trino driver shows an empty table list, while queries, masks and row filters work.

What the driver reads: `TrinoDatabaseMetaData` (Trino master) queries 13 `system.jdbc` tables. SQE served 5 (`types`, `catalogs`, `schemas`, `tables`, `columns`). `getTableTypes()` reads `system.jdbc.table_types` and failed. `getProcedures()`, `getUDTs()`, `getSuperTypes()`, `getSuperTables()`, `getAttributes()` and `getPseudoColumns()` send `... LIMIT 0` against tables that did not exist, so they failed as well.

Changes in `crates/sqe-catalog/src/system_jdbc.rs`:

- `table_types` returns `TABLE` and `VIEW`.
- `procedures`, `procedure_columns`, `udts`, `super_types`, `super_tables`, `attributes` and `pseudo_columns` exist with the driver's columns and no rows.
- `tables` lists views as `VIEW` on a REST catalog. A failed view listing drops the views, never the tables (the same rule `SHOW VIEWS` follows).
- Namespaces are looked up by their parts, not by the dotted name as a single level. Before, a nested namespace `a.b` was looked up as one level named `a.b`. Listing is still top-level only.

Tests:

- Unit: `every_table_the_trino_driver_reads_exists`.
- Integration: `trino_metadata_test::trino_jdbc_driver_metadata_lists_tables_and_views`. It creates a table and a view, asserting that both creations succeed, then runs the driver's own statements. It asserts the `table_types` rows, the tables listing (`t` TABLE, `v` VIEW), every shape-only query with `LIMIT 0`, the inline `getPrimaryKeys` query, and the `getColumns` order.

Real driver: a throwaway probe (not committed) ran `trino-jdbc` 480, the jar DBeaver ships, over the Trino HTTP endpoint behind a TLS front. It called `DatabaseMetaData`.

- On main: `getTableTypes()`, `getProcedures()`, `getProcedureColumns()`, `getUDTs()`, `getSuperTypes()`, `getSuperTables()`, `getAttributes()` and `getPseudoColumns()` fail with `table 'system.jdbc.<name>' not found`, and `getTables()` returns the table without the view.
- On this branch: every call answers, and `getTables()` returns the table as `TABLE` and the view as `VIEW`. The `LIMIT 0` and `WHERE false` statements return their full column lists.

One more finding from the probe, left as it is: with the connection catalog set to the config warehouse name (`test_warehouse`) under static discovery, `getTables(<that catalog>, ...)` returns no rows, because the rows carry the config key (`iceberg`). DBeaver browses by the names `getCatalogs()` returns, so its tree is unaffected. Aliasing the warehouse name onto the key is out: policies are keyed by the catalog name as typed.

Gate: `make test-integration FILTER=trino_metadata`: 4 passed, 0 failed. `cargo test -p sqe-catalog --lib system_jdbc`: 26 passed. Clippy on `sqe-catalog` is clean.

Budget: the `sqe-catalog` line ceiling goes from 64000 to 64100. Main had about 50 lines of headroom; the change adds about 100. !1824 edits the same line (64000 -> 64250, about 206 lines added), so whichever merges second rebases and sets the ceiling to about 64300.

Not reproduced: a fully empty list. On the integration stack and the Ranger stack, the 5 tables SQE already served return rows for a user with access. Probes covered the admin, the analyst and the no-role persona, under static and `polaris-auto` discovery, with and without a session catalog.

Two patterns give an empty or near-empty tree:

1. The failing `getTableTypes()` call, fixed here. Without table types, DBeaver may not ask for the tables at all.
2. A connection with no catalog set, under a principal whose catalogs are not in the config. `system.jdbc.catalogs` lists the configured catalogs plus the session catalog only, so the user's catalog is absent. The docs now say to set the catalog on the connection.

Left open:

- Views have no rows in `system.jdbc.columns`. This needs the view schema as the caller sees it, policies applied, so it is a planning-side change.
- Nested namespaces are not walked: `list_namespaces` is top-level only here and in `list_visible_namespace_names`. This is an engine-wide gap.
- Under static discovery, `SHOW CATALOGS` lists the session catalog even when it does not resolve, while `system.jdbc.catalogs` does not list it.

**NEXT:** confirm with the reporter's DBeaver build (driver version, connection URL, whether a catalog is set) that the tree fills on this branch; then view columns in `system.jdbc.columns`.

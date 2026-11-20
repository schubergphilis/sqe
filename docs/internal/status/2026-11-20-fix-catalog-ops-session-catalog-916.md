**STATUS 2026-11-20: session catalog for the remaining `catalog_ops` statements (`fix/catalog-ops-session-catalog-916`, #916).**

- `sqe-write/src/catalog_ops.rs`: `rename_table` and every `session_catalog_for` caller (CREATE/DROP VIEW, ALTER TABLE schema, nested column drop and type change, SET TBLPROPERTIES, ref DDL, partition evolution, identifier fields) resolve the catalog with `resolve_target_catalog`. An unqualified name now uses the `USE` catalog, as DROP TABLE has since #915.
- RENAME still refuses a destination that names a different catalog than the resolved source catalog.
- `set_column_tags` / `load_column_tags` already resolved the session catalog; that part of #916 was stale.
- New IT `catalog_ops_ddl_uses_the_session_catalog` (catalog_discovery_test.rs): ADD COLUMN, SET TBLPROPERTIES, CREATE/DROP VIEW and RENAME on a scoped session, each checked through the qualified name.

Gates: `cargo check-server`, clippy `-p sqe-write -p sqe-coordinator --tests -D warnings`, `cargo test -p sqe-write --lib catalog_ops` (20 passed). Live IT `catalog_ops_ddl_uses_the_session_catalog` + `drop_table_uses_the_session_catalog`: 2 passed.

Found on the way: `SHOW CREATE TABLE` on a discovered catalog's three-part name returns table-not-found (it reads columns through the session context, which holds only static catalogs). The IT reads the property back through a handler whose default catalog is the discovered one.

**NEXT:** fix `SHOW CREATE TABLE` for discovered catalogs (separate issue).

**STATUS 2026-11-13: `DROP SCHEMA ... CASCADE` on an embedded Iceberg catalog (`fix/embedded-drop-schema-cascade`, follow-up to #895).**

Before this change, `WritableIcebergCatalog::drop_namespace` refused CASCADE with "not implemented for embedded iceberg catalogs". The coordinator has supported CASCADE since #895 (!1783).

Changed:

- `crates/sqe-catalog/src/writable_iceberg_catalog.rs`: `drop_namespace_contents` follows the coordinator's cascade:
  - it lists tables, views (only when the catalog was attached with views) and child schemas before it drops anything;
  - it refuses a schema with child schemas untouched;
  - it drops views, then tables;
  - it never purges;
  - it skips objects that are already gone;
  - a failure part-way gets the same "Dropped: ... Still there: ..." message.

  The existence check (`IF EXISTS`) now runs before the cascade.
- `crates/sqe-cli/src/embedded.rs`: new test `drop_schema_cascade_drops_the_tables` on a SQLite-backed Iceberg catalog. It checks that:
  - without CASCADE, a non-empty schema is refused and its table still reads;
  - CASCADE drops both tables and the schema;
  - `IF EXISTS ... CASCADE` on the now-missing schema is a no-op;
  - the name can be created again.

Gate:

- `cargo test -p sqe-cli --bins`: 117 passed, including the new test.
- `cargo clippy -p sqe-catalog -p sqe-cli --all-targets -- -D warnings`: clean.

Not covered by a test: the view branch and the part-way failure message on an embedded catalog. A SQLite catalog has no views and no per-object privileges. The coordinator's #895 integration tests cover the same logic.

**NEXT:** the #892 OpenSpec change (atomic schema-changing CREATE OR REPLACE).

**STATUS 2026-11-13: `DROP SCHEMA ... CASCADE` drops the schema's views and tables (`fix/drop-schema-cascade-895`, #895).**

`CatalogOps::drop_schema` never read `cascade`, so a schema holding tables failed with the catalog's 409 "Namespace is not empty". Found while proving the dbt-sqe fixes of #891.

Changed:

- `crates/sqe-write/src/catalog_ops.rs`:
  - `drop_schema` drops every name in order. Before, `DROP SCHEMA a, b` dropped only `a`.
  - `drop_schema_contents` (CASCADE):
    - lists tables, views (REST backends only) and child schemas before it drops anything, and refuses a schema with child schemas untouched;
    - drops views first, then tables, through the caller's `SessionCatalog`, so the catalog checks the drop privilege per object, as for `DROP TABLE` / `DROP VIEW`;
    - never purges: data files stay, as with a plain `DROP TABLE`;
    - skips an object that is already gone;
    - is not atomic: a failure part-way names what was dropped and what is still there.
  - Without CASCADE, a "not empty" error says so and points at CASCADE.
  - `IF EXISTS ... CASCADE` on a missing schema is a no-op.
- `crates/sqe-coordinator/src/query_handler.rs`: after a DROP SCHEMA, success or failure (a failed cascade may have dropped part of the schema):
  - the query result cache is cleared;
  - so are results read through views;
  - all session caches are invalidated, as before.

Gate: `make test-integration FILTER=drop_schema_cascade_e2e`, 6 passed:

- `cascade_drops_the_schemas_views_and_tables`
- `without_cascade_a_non_empty_schema_is_a_clear_error_and_nothing_is_dropped`
- `cascade_refuses_a_schema_with_child_schemas_before_dropping_anything`
- `a_cascade_refused_part_way_names_what_was_dropped_and_what_is_left`: a fresh Polaris principal that may drop views but not tables
- `cascade_on_a_missing_schema_honours_if_exists`
- `drop_schema_with_two_names_drops_both`

The MCP change tool's catalog gate (`authorize_write_target`) resolves a schema target to the same catalog as the tables in it, so CASCADE does not widen what it allows.

Not in this MR:

- **Lineage.** A statement carries one lineage hint, so the objects a cascade drops get no per-table DROP event. Multi-name `DROP TABLE a, b` has the same limit: only `a` gets one.
- **Embedded CLI.** `WritableIcebergCatalog::drop_namespace` (`sqe-catalog`) still refuses CASCADE. That is the next branch of this batch.

**NEXT:** CASCADE in the embedded CLI, then #892 (atomic schema-changing CREATE OR REPLACE) as an OpenSpec change.

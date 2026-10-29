**STATUS 2026-10-28: `CatalogOps` moved to `sqe-write` (`refactor/catalog-ops-to-sqe-write`, Databricks UC interop gap 3b, part 2 groundwork).**

Embedded `ALTER TABLE` needs the coordinator's `CatalogOps` (ADD/DROP/RENAME/
ALTER COLUMN, SET TBLPROPERTIES, RENAME TO, partition evolution), and
`sqe-cli` does not depend on `sqe-coordinator`. `catalog_ops.rs` and
`tag_source_impl.rs` moved to `sqe-write` with `git mv`, the same split as
`write_handler` (2026-09-07):

- The coordinator re-exports both under their old paths
  (`sqe_coordinator::catalog_ops`, `sqe_coordinator::tag_source_impl`).
- `session_catalog_for` takes the Polaris discovery through
  `CatalogOps::with_catalog_discovery(Arc<dyn CatalogDiscovery>)`, the hook
  `WriteHandler` already has; the coordinator passes
  `PolarisCatalogDiscovery`.
- `nested_namespace` is a private copy; `is_namespace_not_found`,
  `PROP_KEY`, `scan_column_tags` and `parse_column_tags` became `pub` for the
  coordinator callers.

No behaviour change. Found for the follow-up: `commit_schema_update` (every
ALTER except RENAME TO commits through it) is REST-only, so the `--warehouse`
SQLite catalog needs a non-REST branch through `Catalog::update_table` and a
public `TableCommit` constructor in the vendored iceberg-rust.

**NEXT:** `feat/embedded-alter-table`: non-REST `commit_schema_update`, embedded
routing for AlterSchema / AlterTableProps / Rename / PartitionEvolution, the
front-door fallback for `ADD COLUMNS (...)`, `UNSET TBLPROPERTIES` and
`ADD PARTITION FIELD`.

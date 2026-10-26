# fix/embedded-ctas-show-tables

**Date:** 2026-10-26

## What changed

Found while testing `sqe-cli --embedded` against Databricks Unity Catalog
(playground3) and a local `--warehouse`.

- **CTAS into an Iceberg catalog** (`embedded_handle_iceberg_ctas`).
  - DataFusion's CTAS registers a filled `MemTable`. The vendored
    `IcebergSchemaProvider::register_table` refuses any table with rows.
  - Embedded now runs `CREATE TABLE <t> AS SELECT * FROM (<q>) LIMIT 0`
    (an empty table passes), then `INSERT INTO <t> <q>`.
  - A failing INSERT drops the empty table. These are two commits, not one.
  - `IF NOT EXISTS` skips an existing table. Without it, an existing table
    is an error.
  - `OR REPLACE` is refused with a clear error.
  - Only targets in `iceberg_catalogs` go this way. A one- or two-part name
    resolves in the default catalog (`default`, a memory catalog).
- **`SHOW TABLES {IN|FROM} [cat.]schema [LIKE|ILIKE '...']`**
  (`embedded_handle_show_tables_in`).
  - DataFusion answers only the unscoped form.
  - The scoped form is now answered from `information_schema.tables`, with
    the same four columns.
  - An unknown catalog is an error.
- **Refactor:** the DataFusion tail of `execute` became `run_datafusion`, so
  both handlers reuse the hf/TVF rewrites and the planning.

## Gate

- `cargo test -p sqe-cli`: 103 + 10 passed. New tests:
  - `ctas_into_an_iceberg_catalog_writes_the_rows`
  - `show_tables_in_a_schema`
- `cargo clippy -p sqe-cli --all-targets -- -D warnings`: clean.
- No integration test: embedded mode has no Polaris dependency.

## Still open (embedded, from the same probe)

- **Statements that fail:**
  - `FOR VERSION AS OF` / `FOR SYSTEM_TIME`;
  - `t$snapshots`;
  - UPDATE / DELETE / MERGE;
  - ALTER TABLE;
  - CREATE BRANCH / TAG;
  - CALL;
  - CREATE TABLE with PARTITIONED BY / TBLPROPERTIES (embedded skips
    `pre_parse_pipeline`);
  - CREATE VIEW in an Iceberg catalog.
- **Why:** embedded does not run the coordinator's QueryHandler/WriteHandler.
  A parity issue is to be drafted.
- **Docs:** `docs/site/book/src/features/embedded.md` claims several of the
  above work. To be corrected in a docs MR.

**NEXT:** docs MR correcting embedded.md; draft the embedded parity issue;
then `feat/oauth2-secret` (`CREATE SECRET (TYPE oauth2, ...)` for
self-refreshing REST catalog tokens).

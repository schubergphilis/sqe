## Why

A CTAS on an Iceberg REST catalog registers the table first (`createTable`), then checks that the caller may write, streams the data files and commits the first snapshot. When any step after the registration fails, SQE rolls back as the caller: it deletes the table's S3 prefix, then drops the catalog entry without purge.

The rollback needs the caller to hold drop on the new table. A caller who may create and write tables but not drop them is common: the shared grant profile's CREATE and MODIFY confer no drop. For that caller, a refused or failed CTAS leaves a registered table with no snapshot and no data. Since the change that names it, the statement's error says so, but the entry stays until someone with drop removes it. Workspace cleanup in the platform has to cope with these entries.

The Iceberg REST spec has a way to create a table that registers nothing until its first commit: `createTable` with `stage-create = true`, then a commit that carries `assert-create`. A failed CTAS built on it leaves no catalog entry to roll back.

## What changes

- The streaming CTAS into an Iceberg REST catalog uses a staged create. `createTable` is sent with `stage-create = true`; it returns the table metadata (and, when requested, write credentials) without registering the table.
- SQE writes the data files to the staged location, then registers the table and its first snapshot in one commit with the `assert-create` requirement.
- A failure before that commit leaves no catalog entry. The cleanup is the S3 prefix delete only, best-effort as today.
- The drop-based rollback stays for catalogs that do not support staged create, and for any path that still registers first.
- The SQL surface, the result, the lineage event and the audit event of a successful CTAS do not change.

## Out of scope

- Plain `CREATE TABLE`, which registers by design.
- `CREATE OR REPLACE TABLE ... AS SELECT` on an existing table, which writes into a live table (its own rewrite path).
- Granting drop to creators. That is a grant-profile question for the owner, recorded separately.
- How the platform cleans up entries that already exist.

## Impact

- `vendor/iceberg-rust` REST catalog: today `create_table` always sends `stage_create: Some(false)` and has no staged path. A staged create and a create-commit need to exist there. Upstream first: check apache/iceberg-rust for staged-create or create-transaction support before patching the vendor tree.
- `sqe-catalog`: a staged create on `SessionCatalog`, with the same credential vending and access delegation as `create_table`.
- `sqe-write`: the streaming CTAS path in `write_handler.rs`, and its rollback (`write_handler/ctas_rollback.rs`).
- Tests: wiremock for the request shapes; the Polaris test stack for the commit's authorization.

## Context

`do_put_statement_ingest` builds the qualified name, resolves the transaction, admits the request and calls `WriteHandler::handle_ingest_streaming`, which loads the table, checks write access (#880), stamps each batch onto the table schema and fast-appends the files. The proto's `table_definition_options` is never read. ADBC always sends explicit values: `append` = (missing: FAIL, exists: APPEND), `create` = (CREATE, FAIL), `create_append` = (CREATE, APPEND), `replace` = (CREATE, REPLACE).

## Goals / Non-Goals

**Goals:** every ADBC ingest mode does what it says; requests without options behave exactly as today; a refused ingest writes no file.

**Non-Goals:** temporary tables (`temporary = true` stays unsupported); partitioned or property-carrying creates (ingest creates an unpartitioned table with default properties); changing the table's schema on replace.

## Decisions

### Mode mapping lives in the coordinator
`sqe-write` has no `arrow-flight` dependency and should not get one. The coordinator maps the proto to a plain `IngestMode { on_missing: Fail | Create, on_existing: Append | Fail | Replace }` defined in `sqe-write`. Absent options and `UNSPECIFIED` map to `(Fail, Append)`.

### Stream schema for create
The Flight stream announces its schema in the first message. When `on_missing` is `Create`, the coordinator polls the decoded `FlightRecordBatchStream` until its schema is known (first batch, or end of stream), keeps any first batch, and hands the handler the schema plus a stream that yields that batch first. A stream that ends without a schema is refused (`InvalidArguments`). For other modes the stream is not peeked, as today.

The handler converts the schema the way CTAS does: `timestamps_for_format_version`, a variant column upgrades to V3, `arrow_schema_to_iceberg`. Column names are taken as sent, as CTAS takes its output names. Rejected alternative: generating `CREATE TABLE` SQL from the Arrow types; it round-trips types through text and breaks on nested types.

### Create goes through the catalog with the caller's credentials
Creation uses the session catalog (`ensure_namespace`, then `create_table` with `create_table_properties`), the same calls SQL `CREATE TABLE` makes, so the catalog enforces the caller's create privilege exactly as it does for SQL. After creation the normal path runs: load, `check_write_access`, write.

A concurrent create that wins the race surfaces as "already exists" from the catalog; the handler then treats the table as existing and applies `on_existing` (so `create` refuses, `create_append` appends).

### Fail is checked before the stream is consumed
`on_existing = Fail` on an existing table returns `DuplicateTable` before any file is written. In create modes the coordinator has already read the first batch to learn the schema; nothing past it is read.

### Replace is an overwrite commit
`on_existing = Replace` runs `refuse_replace_under_policy` (as INSERT OVERWRITE does), writes the stream onto the table's current schema, and commits with `commit_written_files(.., overwrite = true)`: one snapshot that removes the old files and adds the new ones. A stream whose batches do not fit the schema fails per batch in `apply_stamped_schema`, and nothing is committed. Rejected alternative: drop and recreate, which loses history and is not atomic.

### Transactions
Matches SQL DDL (#824): inside a client-held Flight transaction the create runs autocommit and the rows go to the transaction's branch, so a rollback leaves the new table empty. Inside a `BEGIN` transaction a create is refused with `AutocommitWriteConflict` before anything happens. Append and replace run in the transaction like INSERT and INSERT OVERWRITE. The handler calls `admit_txn_write` before writing, like every other write path.

### Caches
Create and replace go through `after_commit`, and the coordinator keeps invalidating the result cache for the target, as it does today.

## Risks / Trade-offs

- [Peeking holds one batch] -> only in create modes, one decoded batch, within the ingest's memory admission.
- [Names taken verbatim] -> a client sending `ID` creates a column that SQL must quote; same as CTAS today, documented in the changelog.
- [Rollback after create leaves an empty table] -> same as SQL `CREATE TABLE` in a client transaction; stated in the docs.

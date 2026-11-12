**STATUS 2026-11-11: Flight SQL bulk ingest honours `table_definition_options` (`fix/ingest-table-definition-options`, #881, openspec `ingest-table-definition-options`).**

`do_put_statement_ingest` now maps the request's `TableDefinitionOptions` to an
`IngestMode` (`sqe-write`, so no `arrow-flight` dependency there) and
`handle_ingest_streaming` acts on it. The code moved to
`crates/sqe-write/src/write_handler/ingest.rs` (a child module of
`write_handler`, which was near its size budget); the Flight mapping and the
stream-schema peek are in `crates/sqe-coordinator/src/flight_ingest.rs`.

- `append` = (FAIL, APPEND), `create` = (CREATE, FAIL), `create_append` =
  (CREATE, APPEND), `replace` = (CREATE, REPLACE). Absent or `UNSPECIFIED`
  keeps the old behaviour and costs no extra catalog lookup.
- Missing table + create: the table is made from the stream's Arrow schema
  with the CTAS schema helpers, autocommit, with the caller's credentials. A
  BEGIN transaction refuses (`AutocommitWriteConflict`), a `WRITE_BRANCH`
  session refuses (the create would land on main). In a client-held Flight
  transaction the create is autocommit and the rows go to the transaction.
  A lost create race re-applies `on_existing` to the table that now exists.
- Existing table + fail: `DuplicateTable` (gRPC `AlreadyExists`) before the
  stream is consumed past its first batch and before any file is written.
- Existing table + replace: `refuse_replace_under_policy` as INSERT OVERWRITE,
  then one `commit_written_files(.., overwrite = true)` snapshot. A mismatched
  stream fails in the writer, before the commit, so the table is unchanged.
- A create from a stream with no schema is `InvalidArgument` and makes no
  table. The Flight encoder sends no schema for a stream with no batches
  unless the client sets one, so a driver that wants an empty-table create
  must announce its schema first (ADBC drivers do).
- `admit_txn_write` runs for every ingest, as for other writes.

Tests: unit tests for the plan and its refusal order (`sqe-write`), for every
ADBC mode mapping and the schema peek (`sqe-coordinator`); e2e in
`flight_ingest_e2e.rs`: the two `bug:#881` tests are back in the gate, plus
create_append twice, replace (exact rows, previous snapshot kept), replace with
a mismatched stream, empty-stream create, create without a schema, and create
then rollback in a Flight transaction.

**NEXT:** add the Ranger-stack test for an ingest by a read-only principal
(the default stack has one principal, `root`).

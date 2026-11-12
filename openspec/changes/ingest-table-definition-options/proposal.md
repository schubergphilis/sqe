## Why

Flight SQL bulk ingest (`DoPut` + `CommandStatementIngest`, what ADBC `adbc_ingest` sends) ignores `table_definition_options` (#881). Every request behaves as "append to an existing table":

- `mode="create"`, the pandas and polars default, fails on a missing table with `TABLE_NOT_FOUND`.
- `mode="create"` on an existing table, and any request with `TableExistsOption::Fail`, appends and reports success.
- `mode="replace"` appends instead of replacing.

The two silent cases are correctness bugs: the client asked for a refusal or a replacement and got rows appended with a success count. `tests/it/flight_ingest_e2e.rs` (!1724) reproduces both.

The same read found that the ingest path never calls `admit_txn_write`, which every other write path calls before it writes inside a transaction. An ingest into a second table of a client transaction is not refused on a catalog without atomic multi-table commit.

## What Changes

- `do_put_statement_ingest` reads `table_definition_options` into an `IngestMode` (what to do when the table is missing, what to do when it exists) and passes it to `WriteHandler::handle_ingest_streaming`.
- A request without options, or with `UNSPECIFIED` values, keeps today's behaviour: missing table fails, existing table appends.
- Missing table + `Create`: the table is created from the uploaded stream's Arrow schema, through the same schema conversion CTAS uses, with the caller's own catalog credentials.
- Existing table + `Fail`: refused with `DUPLICATE_TABLE` (gRPC `AlreadyExists`) before any file is written.
- Existing table + `Replace`: the rows replace the table's contents in one overwrite commit, with the same policy refusal INSERT OVERWRITE has. The table keeps its schema and history.
- The ingest path calls `admit_txn_write` like the other writes.

## Capabilities

### New Capabilities
- `flight-ingest`: what a Flight SQL `CommandStatementIngest` does for each combination of table state and `TableDefinitionOptions`.

### Modified Capabilities
None.

## Impact

- `crates/sqe-coordinator/src/flight_sql.rs`: map the options, read the stream schema when a create may be needed.
- `crates/sqe-write/src/write_handler.rs`: `handle_ingest_streaming` takes the mode and an optional stream schema; create and replace branches reuse the CTAS schema helpers, `commit_written_files` and `refuse_replace_under_policy`.
- Clients: ADBC `create`, `create_append` and `replace` start working; `append` and requests without options are unchanged.
- Rebases over !1722, which moved the `DoPut` ingest call into the boxed `ingest_flight_stream`.

**STATUS 2026-11-11: Flight SQL bulk ingest has end-to-end coverage (`test/flight-ingest-e2e`).**

New `crates/sqe-coordinator/tests/it/flight_ingest_e2e.rs` starts the production
`SqeFlightSqlService` on an OS-assigned port with the real `Authenticator` and
one shared `SessionManager`, then drives `DoPut` + `CommandStatementIngest`
with `FlightSqlServiceClient::execute_ingest`. Seven tests, one per behaviour:
append returns the count and the exact rows; two appends keep both batches;
a four-batch stream arrives complete (11 rows); an ingest in a client
transaction is invisible to a second session until commit; a rolled-back
ingest leaves nothing; create-if-not-exists on a missing table; fail-if-exists
on an existing table. Run with `make test-integration FILTER=flight_ingest_e2e`.

Result: 5 pass, 2 fail, and the two failures are server gaps, not test bugs.
`do_put_statement_ingest` never reads `table_definition_options`, and
`handle_ingest_streaming` starts with `load_table`:

- `TableNotExistOption::Create` on a missing table answers `NOT_FOUND`
  (`TABLE_NOT_FOUND`) instead of creating the table, so ADBC
  `mode="create"` / `"create_append"` cannot work.
- `TableExistsOption::Fail` on an existing table appends and returns `Ok(1)`;
  `Replace` is ignored the same way (not tested yet).

Filed as #881. The two tests carry `bug:#881` (see `xtask/src/test_groups.rs`)
and come back into the gate when the fix lands.

Not covered: ingest refused for a principal without write privilege. The
Polaris + RustFS stack has one principal (`root`); a second one needs the
Ranger stack (`make test-access-control`), which is a different suite.

**NEXT:** fix #881 on `fix/ingest-table-definition-options` (create from the
stream schema, fail-if-exists, atomic replace) and drop the `bug:#881`
markers; then add the Ranger-stack test for a read-only principal.

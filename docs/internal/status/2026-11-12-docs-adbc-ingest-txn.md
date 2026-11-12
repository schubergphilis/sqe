# docs/adbc-ingest-txn (2026-11-12)

A client probe with ADBC 1.13.0 showed `cursor.adbc_ingest` on a connection
without autocommit committing to main at once, while README.md,
`sql-reference/dml.md` and `getting-started/connecting.md` said ingest stays on
the transaction branch until `commit()`.

Cause, read from arrow-adbc `go/adbc/driver/flightsql/flightsql_bulk_ingest.go`
(release 22 and main): `buildExecuteIngestOpts` copies table, mode, catalog,
schema and temporary into `ExecuteIngestOpts` and never sets `TransactionId`,
and arrow-go's `Txn` has no `ExecuteIngest`. The server side
(`do_put_statement_ingest`) joins whatever id the command carries, so the gap
is in the client driver.

Docs and the code comment at the ingest handler now state that. No behaviour
change: refusing an id-less ingest would break `adbc_ingest` on every default
DB-API connection.

**NEXT:** file an arrow-adbc issue (and a PR setting `TransactionId` from the
connection's open transaction in `executeIngest`) once the owner agrees, then
add a Python ADBC e2e test that pins ingest-in-transaction when a fixed driver
ships.

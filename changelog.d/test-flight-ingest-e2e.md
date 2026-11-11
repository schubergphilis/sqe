### Added

- End-to-end tests for Flight SQL bulk ingest (`DoPut` with `CommandStatementIngest`)

    `tests/it/flight_ingest_e2e.rs` drives the ingest path (the one ADBC `adbc_ingest`
    uses) through a real gRPC socket against the Polaris + RustFS stack: append into an
    existing table with exact row contents, two appends, a four-batch stream, and an
    ingest inside `BeginTransaction` that stays invisible to another session until
    `EndTransaction(COMMIT)` and disappears on `ROLLBACK`. Two further tests pin what the
    protocol asks for and the server does not do yet: `TableNotExistOption::Create` on a
    missing table and `TableExistsOption::Fail` on an existing one. Both fail today
    because `do_put_statement_ingest` ignores `table_definition_options` (#881) and
    are ignored as `bug:#881` until the fix lands. Test-only change, no server code
    touched.

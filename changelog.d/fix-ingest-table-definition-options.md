### Fixed

- Flight SQL bulk ingest honours `table_definition_options` (#881)

    ADBC `adbc_ingest` modes now work: `create` and `create_append` create a
    missing table from the stream's Arrow schema, `create` refuses an existing
    table with `ALREADY_EXISTS` before any file is written, and `replace` swaps
    the rows in one overwrite snapshot that keeps the table's schema and
    history. Before, the options were ignored: a missing table answered
    `TABLE_NOT_FOUND` and every ingest into an existing table appended. Absent
    or `UNSPECIFIED` options behave as before (fail if missing, append if
    present). A create refuses inside a SQL `BEGIN` transaction and on a
    session with `WRITE_BRANCH`; inside a client-held Flight transaction the
    table is created at once and the rows follow the transaction. A replace
    is refused under row-level policy, as `INSERT OVERWRITE` is. A create
    needs the stream to announce its schema; one that does not fails with
    `INVALID_ARGUMENT`.

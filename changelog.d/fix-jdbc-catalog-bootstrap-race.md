### Fixed

- Concurrent sessions no longer fail on a fresh JDBC (Postgres) catalog (#872)

    Each session builds its own JDBC catalog, and each build runs
    `CREATE TABLE IF NOT EXISTS` for the catalog tables. Postgres can fail
    one of two such statements racing for the same table, so the first
    concurrent queries against an empty database could error. A failed
    create is now ignored when the table exists afterwards.

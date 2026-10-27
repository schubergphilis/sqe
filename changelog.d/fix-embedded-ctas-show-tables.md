### Fixed

- Embedded mode: `CREATE TABLE ... AS` into an Iceberg catalog, and `SHOW TABLES IN`

    In `sqe-cli --embedded`, CTAS into a `--warehouse` or attached Iceberg
    catalog failed with "register_table does not support tables with
    data". The table is now created empty from the query's schema and then
    filled by an `INSERT`. A failing `INSERT` drops the empty table again.
    `IF NOT EXISTS` skips an existing table. `CREATE OR REPLACE TABLE ... AS`
    is refused with a clear error. `SHOW TABLES {IN|FROM} [catalog.]schema
    [LIKE '...']` failed with "SHOW TABLES IN not supported" and now lists
    the tables of that schema.

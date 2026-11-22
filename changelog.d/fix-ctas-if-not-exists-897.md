### Fixed

- `CREATE TABLE IF NOT EXISTS ... AS SELECT` on an existing Iceberg table is a no-op (#897)

    It used to fail with "table already exists", so dbt-trino's `on_table_exists = 'skip'` failed on every run after the first. The existing table is now left as it is and the statement reports 0 rows without running the query, as in Trino. Stock dbt-trino 1.12 was run three times against the Trino endpoint with `on_table_exists` = `rename`, `drop`, `replace` and `skip`, and with `views_enabled = false` incrementals (merge and append); every table held the expected rows and no temporary or backup table was left.

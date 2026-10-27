### Fixed

- `information_schema.columns`, `DESCRIBE`, `SHOW COLUMNS` and `SHOW CREATE TABLE` work on Databricks Unity Catalog

    Unity Catalog lists its Delta tables in the Iceberg REST namespace and
    answers 400 ("not an Iceberg compatible table") to loading them.
    `information_schema.columns` loads every listed table and stopped at the
    first error, so one Delta table failed every column read of the catalog.
    A 400 on loading a listed name now resolves to no table, as an absent
    name does; 401, 403 and 5xx still fail. `DESCRIBE`, `SHOW COLUMNS` and
    `SHOW CREATE TABLE` read the columns of the named table alone instead
    of the whole `information_schema`, so another table that does not load
    no longer fails them, and a bare name resolves as a `SELECT` resolves
    it: in the session's default catalog and schema.

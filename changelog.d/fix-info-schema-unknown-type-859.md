### Fixed

- `information_schema.columns` reports an Iceberg `unknown` column as `unknown` (#859)

    Over Flight SQL and the embedded CLI the `data_type` column showed
    Arrow's `Null`, while `DESCRIBE`, `SHOW COLUMNS` and the Trino endpoint
    already said `unknown`. A filter such as `WHERE data_type = 'unknown'`
    matches too. Other types keep their names.

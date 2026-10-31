### Added

- `MERGE ... UPDATE SET *` and `INSERT *`

    The Spark and Databricks shorthand now parses and runs in both write
    modes: `WHEN MATCHED THEN UPDATE SET *` sets every target column from
    the source column of the same name, and `WHEN NOT MATCHED THEN INSERT *`
    inserts every target column the same way. Column order may differ and
    extra source columns are ignored; a source missing a target column fails
    the statement, as in Spark. `UPDATE SET *` is refused in a
    `WHEN NOT MATCHED BY SOURCE` clause.

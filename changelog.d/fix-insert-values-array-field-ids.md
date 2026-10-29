### Fixed

- Embedded `INSERT ... VALUES` into an `ARRAY` column, and filters on `ARRAY` columns

    On the `--warehouse` catalog and on attached Iceberg catalogs,
    `INSERT ... VALUES (1, ARRAY['a'])` failed with "column types must match
    schema types": DataFusion typed the rows against the table's list type,
    which carries the element's Iceberg field id. Embedded `INSERT` and the
    insert step of CTAS now run on the coordinator's insert handler, which
    plans the source as a plain query. A query with a predicate on a list or
    map column, such as `WHERE tags IS NOT NULL`, failed with "Accessor for
    Field tags not found"; the Iceberg scan now skips a predicate it cannot
    bind and DataFusion applies it above the scan.

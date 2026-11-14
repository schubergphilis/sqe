### Changed

- A schema-changing `CREATE OR REPLACE TABLE ... AS SELECT` is one commit on the existing table (#892)

    A replace whose SELECT changes the columns, the partitioning or the
    format version used to build the new table under a staging name, drop
    the old one and rename the staged one into place. Readers could see no
    table in between, and the table lost its UUID, snapshot history, branches,
    tags and grants. The replace now commits the new schema, partition spec
    and data in one catalog update on the live table, as an Iceberg replace
    transaction does. Columns keep their field id when their name and type
    (or a type promotion) are kept; a column stays NOT NULL only when it keeps
    the id of a NOT NULL column. Time travel and rollback cross the replace.
    A replace that lowers the format version, or changes the shape while
    `WRITE_BRANCH` is set or inside a transaction, is refused before any data
    is written. A concurrent schema change fails the replace with a message
    naming it; a concurrent append is retried.

### Fixed

- A commit that removes files written under an older partition spec whose source column was dropped no longer fails with "value is not compatible with type" (#892)

    iceberg-rust bound an old spec to the current schema, where an identity
    field on a dropped column types as `unknown`. The old spec now binds to
    the newest schema that still holds its source columns.

- `information_schema` and `system` catalog queries are no longer served from the result cache

    A write evicts the cached results of the table it wrote, but a result
    read from `information_schema.columns` is indexed under `columns`, so a
    query repeated after a CTAS, CREATE OR REPLACE or DROP returned the old
    column or table list until the cache entry expired. Results that read
    catalog metadata are now never cached.

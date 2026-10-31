### Fixed

- `sqe-cli --embedded --catalog-backend glue|s3tables` no longer writes to the shared catalog

    These catalogs were documented as read-only, but the datafusion-iceberg
    provider they were mounted through is writable: `CREATE TABLE` and
    `CREATE VIEW` created a table in Glue or S3 Tables, `DROP TABLE` dropped
    the real table, and `INSERT` committed to it. The catalog, its schemas and
    its tables are now wrapped so every write (`CREATE` / `DROP` of a table,
    view or schema, `INSERT`, `DELETE`, `UPDATE`, `TRUNCATE`) fails with
    "catalog `glue` is attached read-only; use the server for writes".
    `SELECT`, `DESCRIBE` and `SHOW TABLES` are unchanged.

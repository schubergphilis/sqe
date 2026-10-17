### Added

- `VARIANT` columns (Iceberg V3) (#849)

    `CREATE TABLE t (id BIGINT, v VARIANT)` creates a format-version 3
    table with an Iceberg variant column, as do CTAS from a variant column
    and `CREATE TABLE ... LIKE`. A string written to the column is parsed
    as JSON (`'{"a": 1}'` is an object, `'"text"'` a JSON string); invalid
    JSON fails the write rather than storing NULL. Values read back as JSON
    text: the Trino protocol types the column `json`, Flight SQL returns a
    string, and `DESCRIBE` / `information_schema` report `variant`.
    `ALTER TABLE ... ADD COLUMN v VARIANT` is refused on a V2 table.
    Existing JSON-as-string columns are not converted; Iceberg does not
    allow evolving `string` to `variant`, so copy them with
    `INSERT INTO <variant table> SELECT ...`.

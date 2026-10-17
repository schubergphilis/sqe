## Why

Iceberg format v3 adds a `variant` type for semi-structured data, and the
engines around SQE have converged on it: Spark 4.0 with Iceberg 1.10, Trino
481+, and Snowflake all store JSON-shaped data as variant on a v3 table. On
v1 / v2 tables there is no such type, and every engine (Trino included)
stores JSON as a plain string.

Before this change SQE mapped `JSON` to a string column on every table, and
had no `VARIANT` type at all. A table SQE created could not hold the column
type that Spark 4 and recent Trino write, and a v3 table holding a variant
column could not be created or extended from SQE (#849).

## What Changes

- **`VARIANT` type.** `CREATE TABLE` and `ALTER TABLE ADD COLUMN` accept
  `VARIANT`, also nested in `STRUCT<...>` / `ROW(...)`, mapped to the
  Iceberg v3 `variant` type. A table with one is created as format v3; adding
  one to a v1 / v2 table is refused. Writes parse text as JSON (invalid JSON
  fails the statement). Reads render each value as JSON text: Trino type
  `json`, Flight `Utf8`, `DESCRIBE` / `information_schema` `variant`,
  `SHOW CREATE TABLE` `VARIANT`. (MR !1459.)
- **`JSON` follows the table version.** On a v3 table `JSON` is stored as a
  variant, at any depth; on a v1 / v2 table it stays a string. A `CREATE
  TABLE` is v3 when it has a v3-only column (variant, nanosecond timestamp,
  DEFAULT), when it asks for v3 (`WITH ('format-version' = '3')`, Trino's
  `format_version = 3`, or `TBLPROPERTIES`), or when the configured
  `default_table_format_version` is 3. `ALTER TABLE ADD COLUMN ... JSON`
  adds a variant to a v3 table.
- **JSON functions read a variant.** `json_extract`, `json_extract_scalar`,
  the `json_get_*` family and every other `json*` scalar function take a
  variant argument as its JSON text, so a query reads the same against a v2
  string column and a v3 variant column.
- **Contradictions are errors.** Asking for `format-version` 1 or 2 on a
  table that has a v3-only column, or for a version other than 1, 2 or 3, is
  refused instead of creating a table whose metadata disagrees with its
  properties.

Non-goals:

- **No reader-aware version choice.** `default_table_format_version` now
  defaults to 3. A v3 table is unreadable by Trino < 481, Spark < 4.0 and
  Iceberg < 1.10; deployments sharing tables with those readers set the
  default to 2 or create tables `WITH ('format-version' = '2')`. Existing
  tables keep their version.
- **No in-place conversion.** Iceberg forbids evolving `string` to
  `variant`; an existing JSON-as-text column is converted by
  `INSERT INTO <variant table> SELECT ...`.
- **No variant-native functions yet.** `variant_get`, `parse_json`,
  `typeof` on variant, and reading shredded variants are a follow-up.

## Success Criteria

- A `JSON` column of a table created with `format-version` 3 is an Iceberg
  `variant`, round-trips JSON values, and `json_extract` reads it.
- A `JSON` column of a v2 table is a string and accepts any text, as before.
- The integration suite (`make test-integration`) passes, including
  `variant_e2e`.

## Rollback strategy

Revert the MR. Tables already created as v3 with variant columns stay
readable by v3-capable engines; SQE reads them only while the `VARIANT`
support of !1459 is in place.

### Fixed

- `CREATE TABLE ... WITH (format_version = 3)` and CTAS create the table at that version; `ALTER TABLE ... SET PROPERTIES format_version = 3` upgrades it (#897)

    The Trino `format_version` option was stored as a table property, and
    the table was created at the server default (`catalog.default_table_format_version`).
    So dbt-trino marts with `+properties: {format_version: "3"}` came out V2 on a
    V2-default server. CREATE TABLE and CTAS now take the version from
    `format_version` (a number or a string), `'format-version'` or
    `TBLPROPERTIES ('format-version' = ...)`. A version below what a column
    needs (variant, geometry, nanosecond timestamps, DEFAULT) is an error.
    `ALTER TABLE ... SET TBLPROPERTIES` / `SET PROPERTIES` with a higher version
    upgrades the table in the same commit and refuses a lower one.

- dbt-trino's `create table t comment '...' WITH (...) as (...)` parses (#897)

    sqlparser keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one options
    slot, so any model with `persist_docs: {relation: true}` and `properties`
    failed with "Expected: end of statement, found: WITH". The comment now joins
    the options as the `comment` property. A plain `CREATE TABLE t COMMENT '...'`
    used to drop its comment; it is now stored as the `comment` property too, where
    `COMMENT ON TABLE` writes it. `WITH (...) TBLPROPERTIES (...)` parsed, but
    sqlparser kept only the second list, so the `WITH` items were dropped without
    an error. They now join the `TBLPROPERTIES` list.

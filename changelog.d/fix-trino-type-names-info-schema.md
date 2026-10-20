### Fixed

- Type names agree across information_schema, typeof and SHOW CREATE TABLE (#848, #867)

    `information_schema.columns.data_type` now parses the Arrow type and names it with the same mapper as the Trino wire protocol, so nested and parameterized types read `timestamp(6)`, `array(bigint)`, `row(x bigint,y varchar)` and `map(varchar,bigint)` instead of a partial string translation. `typeof` names a variant `variant` at any depth (`array(variant)`, was `array(json)`), as DESCRIBE does. SHOW CREATE TABLE prints Trino types that CREATE TABLE accepts (`integer`, `timestamp(9)` for a nanosecond column, lowercase `variant`) instead of Arrow names, and fails with "table not found" for a missing table instead of printing an empty statement. An exponent literal such as `1e0` is now `double`, as in Trino, rather than a decimal.

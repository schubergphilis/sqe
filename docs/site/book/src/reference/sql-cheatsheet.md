# SQL at a glance

A scannable answer to "is X supported?". Every row points at the detailed [SQL Reference](../sql-reference/index.md) page that carries the dialect-comparison columns and the source line. This page is the index, not the authority. When a row and a reference page disagree, the reference page wins.

## Statements

### DDL

| Statement | Supported | Page |
|---|---|---|
| `CREATE SCHEMA` / `DROP SCHEMA` / `ALTER SCHEMA RENAME` | yes | [DDL](../sql-reference/ddl.md) |
| `CREATE TABLE (cols)` with V3 column defaults | yes | [DDL](../sql-reference/ddl.md) |
| `CREATE TABLE ... PARTITIONED BY (transform(col))` | yes (`bucket`, `truncate`, `year`, `month`, `day`, `hour`, identity) | [DDL](../sql-reference/ddl.md) |
| `CREATE TABLE AS SELECT` / `CREATE OR REPLACE TABLE AS SELECT` | yes | [DDL](../sql-reference/ddl.md) |
| `CREATE TABLE LIKE` | yes (schema only) | [DDL](../sql-reference/ddl.md) |
| `ALTER TABLE ADD/DROP/RENAME COLUMN`, nullability, type promotion | yes | [DDL](../sql-reference/ddl.md) |
| `ALTER TABLE RENAME TO`, `SET TBLPROPERTIES`, `COMMENT ON` | yes | [DDL](../sql-reference/ddl.md) |
| Partition evolution (`ADD/DROP/REPLACE PARTITION FIELD`) | yes | [DDL](../sql-reference/ddl.md) |
| Branches and tags (`CREATE/DROP BRANCH`, `CREATE/DROP TAG`) | yes | [DDL](../sql-reference/ddl.md) |
| `CREATE [OR REPLACE] VIEW` | yes | [DDL](../sql-reference/ddl.md) |

### DML

| Statement | Supported | Page |
|---|---|---|
| `SELECT` with `WHERE` / `GROUP BY` / `HAVING` / `ORDER BY` / `LIMIT` | yes | [DML](../sql-reference/dml.md) |
| `WITH` and `WITH RECURSIVE` CTEs | yes | [DML](../sql-reference/dml.md) |
| `SELECT * EXCLUDE` / `SELECT * REPLACE` | yes | [DML](../sql-reference/dml.md) |
| Joins (`INNER`/`LEFT`/`RIGHT`/`FULL`, `SEMI`/`ANTI`, `USING`, `LATERAL`) | yes | [DML](../sql-reference/dml.md) |
| `TABLESAMPLE BERNOULLI` | yes | [DML](../sql-reference/dml.md) |
| Time travel (`FOR VERSION AS OF`, `FOR SYSTEM_TIME AS OF`, `FOR INCREMENTAL BETWEEN`) | yes | [DML](../sql-reference/dml.md) |
| `INSERT INTO ... VALUES` / `INSERT INTO ... SELECT` / `INSERT OVERWRITE` | yes | [DML](../sql-reference/dml.md) |
| `UPDATE ... SET ... WHERE` (CoW or MoR) | yes | [DML](../sql-reference/dml.md) |
| `DELETE FROM ... WHERE`, `TRUNCATE TABLE` (CoW or MoR) | yes | [DML](../sql-reference/dml.md) |
| `MERGE INTO ... WHEN MATCHED / NOT MATCHED` | yes | [DML](../sql-reference/dml.md) |
| `COPY (...) TO 'path' (FORMAT ...)` | yes | [DML](../sql-reference/dml.md) |

Copy-on-Write is the default for UPDATE / DELETE / MERGE. Set `write.delete.mode = 'merge-on-read'` (and the `update` / `merge` siblings) per table to switch.

### CALL procedures

| Procedure | Supported | Page |
|---|---|---|
| `system.rewrite_data_files` | yes | [CALL procedures](../sql-reference/procedures.md) |
| `system.expire_snapshots` | yes | [CALL procedures](../sql-reference/procedures.md) |
| `system.remove_orphan_files` | yes | [CALL procedures](../sql-reference/procedures.md) |
| `system.rewrite_manifests` | yes | [CALL procedures](../sql-reference/procedures.md) |
| `system.suggest_bloom_filter_columns` | yes (SQE-specific) | [CALL procedures](../sql-reference/procedures.md) |
| `rewrite_position_deletes`, `cherrypick_snapshot`, `expire_snapshots_by_id` | not exposed | [CALL procedures](../sql-reference/procedures.md#what-is-not-exposed) |

### SHOW and EXPLAIN

| Statement | Supported | Page |
|---|---|---|
| `SHOW CATALOGS` / `SCHEMAS` / `TABLES` / `VIEWS` / `COLUMNS` | yes | [SHOW and EXPLAIN](../sql-reference/show-explain.md) |
| `SHOW CREATE TABLE`, `SHOW STATS`, `DESCRIBE` | yes | [SHOW and EXPLAIN](../sql-reference/show-explain.md) |
| `EXPLAIN`, `EXPLAIN ANALYZE`, `EXPLAIN FULL` | yes (`FULL` is SQE-specific) | [SHOW and EXPLAIN](../sql-reference/show-explain.md) |
| `information_schema.tables` / `columns` / `schemata` / `views` | yes | [SHOW and EXPLAIN](../sql-reference/show-explain.md) |

### GRANT, REVOKE, and policy

The security SQL surface parses today. The active enforcer is `passthrough` by default, so the masks and filters below are a documented surface rather than a live control out of the box. See [Limitations](limitations.md#fine-grained-policy-enforcement-is-off-by-default).

| Statement | Supported | Page |
|---|---|---|
| `GRANT` / `REVOKE` (SQL standard) | yes (parsed) | [GRANT and REVOKE](../sql-reference/grant-revoke.md) |
| `GRANT ... MASKED WITH` (column masks) | parsed; enforcement off by default | [GRANT and REVOKE](../sql-reference/grant-revoke.md) |
| `GRANT ... ROWS WHERE` (row filters) | parsed; enforcement off by default | [GRANT and REVOKE](../sql-reference/grant-revoke.md) |
| `SHOW GRANTS`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS` | yes (SQE-specific) | [GRANT and REVOKE](../sql-reference/grant-revoke.md) |
| `WITH GRANT OPTION`, column-level INSERT grants, aggregate masks | not supported | [GRANT and REVOKE](../sql-reference/grant-revoke.md#known-gaps) |

## Functions

Function names are case-insensitive. SQE registers DataFusion built-ins plus a Trino-compatibility layer (Trino-named aliases for things DataFusion calls differently). Each page lists the Trino, Snowflake, Spark SQL, and DuckDB equivalents per function.

| Family | Notable supported | Page |
|---|---|---|
| Conditional / null | `if`, `iff`, `case`, `coalesce`, `nullif`, `greatest`, `least`, `nvl`, `nvl2`, `typeof`, `try` | [Conditional](../sql-reference/conditional.md) |
| String | `concat`, `substring`, `trim`, `lower`, `upper`, regex, `split`, `format`, normalisation | [String](../sql-reference/string.md) |
| Math | trig, rounding, logs, exponents, `sign`, modular, base conversion | [Math](../sql-reference/math.md) |
| Date / time | construction, extraction, formatting, parsing, arithmetic, time zones; Trino `year()` / `month()` / `day_of_week()` | [Date and time](../sql-reference/datetime.md) |
| Array / map / struct | 40+ nested functions plus `map_agg`, `histogram` | [Array, map, struct](../sql-reference/array-map.md) |
| JSON | two surfaces: Trino-named (`json_extract`, `json_parse`) and the `json_get_*` family | [JSON](../sql-reference/json.md) |
| Encoding / hashing / URL | base64, hex, `md5`, `sha224..512`, `url_extract_*`, `url_encode`, `url_decode` | [Encoding, URL](../sql-reference/encoding-url.md) |
| Aggregate | `count`, `sum`, `avg`, statistical, regression, `array_agg`, `string_agg` / `listagg`, `histogram`, `map_agg`, approximation | [Aggregate](../sql-reference/aggregate.md) |
| Window | `row_number`, `rank`, `lag`, `lead`, `first_value`, frames (`ROWS`/`RANGE`/`GROUPS BETWEEN`) | [Window](../sql-reference/window.md) |

### Table-valued functions

| Function | Purpose | Page |
|---|---|---|
| `read_parquet`, `read_csv`, `read_json`, `read_delta` | Read external files (local, S3, HTTPS, `hf://`) | [Table-valued functions](../sql-reference/table-functions.md) |
| `SELECT * FROM 'file.ext'` | Quoted-string auto-detect by extension | [Table-valued functions](../sql-reference/table-functions.md) |
| `table_snapshots`, `table_history`, `table_files`, `table_manifests`, `table_partitions`, `table_refs` | Iceberg metadata | [Table-valued functions](../sql-reference/table-functions.md) |
| `generate_series`, `range`, `unnest` | Generators | [Table-valued functions](../sql-reference/table-functions.md) |

## Intentionally not in SQE

These are absent on purpose. The reasoning lives on the [SQL Reference overview](../sql-reference/index.md#what-is-intentionally-not-in-sqe).

| Construct | Why it is out |
|---|---|
| `PIVOT`, `UNPIVOT`, `QUALIFY`, `ASOF JOIN`, FROM-first syntax | DataFusion's parser does not accept them. Tracked upstream. |
| Lambda expressions, list comprehensions | No AST node for closures in DataFusion. |
| Oracle / Snowflake `DECODE` | Name collides with DataFusion's `decode(input, encoding)`. Use `CASE WHEN`. |
| `IIF` (T-SQL) | Covered by `if` and `iff`, both registered. |
| `postgres_table_scanner`, `mysql_table_scanner`, `sqlite_scanner` | Out of scope. SQE is Iceberg-first. |
| `spatial`, `vss`, `fts`, `excel` | Niche. Use a tool built for the job. |

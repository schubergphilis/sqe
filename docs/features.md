# SQL Feature Comparison: SQE vs Trino vs Spark SQL

SQE is built on **Apache DataFusion 51** which provides the SQL execution engine. All standard SQL features come from DataFusion; SQE adds catalog integration (Polaris/Iceberg), auth (Keycloak), and DDL routing.

## Quick Summary

| Category | SQE (DataFusion 51) | Trino | Spark SQL |
|----------|-------------------|-------|-----------|
| Window functions | Full | Full | Full |
| Aggregate functions | Full | Full | Full |
| Joins | Full (7 types) | Full | Full |
| Subqueries | Full | Full | Full |
| CTEs | WITH + recursive | WITH + recursive | WITH + recursive |
| Set operations | Full | Full | Full |
| JSON | Partial | Full | Full |
| Array/Map types | Partial | Full | Full |
| MERGE INTO | Planned | Full | Full |
| DELETE | Planned | Full (w/ connectors) | Full |
| PIVOT/UNPIVOT | No | No | PIVOT only |
| QUALIFY | No | No | No |
| Lambda expressions | No | Yes | Yes |
| GROUPING SETS | Yes | Yes | Yes |
| Iceberg time travel | No | Yes | Yes |
| Federated queries | No | Yes (connectors) | Yes (connectors) |
| UDFs | Rust API only | Java/Python | Java/Scala/Python |

---

## Window Functions

**Yes — LEAD, LAG, PARTITION BY, ORDER BY, and frame specs are all supported.**

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `ROW_NUMBER()` | Yes | Yes | Yes |
| `RANK()` | Yes | Yes | Yes |
| `DENSE_RANK()` | Yes | Yes | Yes |
| `NTILE(n)` | Yes | Yes | Yes |
| `LEAD(col, offset, default)` | Yes | Yes | Yes |
| `LAG(col, offset, default)` | Yes | Yes | Yes |
| `FIRST_VALUE(col)` | Yes | Yes | Yes |
| `LAST_VALUE(col)` | Yes | Yes | Yes |
| `NTH_VALUE(col, n)` | Yes | Yes | Yes |
| `CUME_DIST()` | Yes | Yes | Yes |
| `PERCENT_RANK()` | Yes | Yes | Yes |
| `PARTITION BY` | Yes | Yes | Yes |
| `ORDER BY` in window | Yes | Yes | Yes |
| `ROWS BETWEEN ... AND ...` | Yes | Yes | Yes |
| `RANGE BETWEEN ... AND ...` | Yes | Yes | Yes |
| `GROUPS BETWEEN` | Yes | No | No |

**Example — the query you asked about works identically in SQE:**

```sql
SELECT
  customer_id,
  order_date,
  amount,
  LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
  LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
  SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;
```

---

## Aggregate Functions

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | Yes | Yes | Yes |
| `COUNT(DISTINCT col)` | Yes | Yes | Yes |
| `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP` | Yes | Yes | Yes |
| `VARIANCE`, `VAR_POP`, `VAR_SAMP` | Yes | Yes | Yes |
| `COVAR_POP`, `COVAR_SAMP` | Yes | Yes | Yes |
| `CORR` | Yes | Yes | Yes |
| `APPROX_DISTINCT` | Yes | Yes | Yes (approx_count_distinct) |
| `APPROX_PERCENTILE_CONT` | Yes | Yes (approx_percentile) | Yes (percentile_approx) |
| `APPROX_MEDIAN` | Yes | No | No |
| `MEDIAN` | Yes | No | No |
| `BOOL_AND`, `BOOL_OR` | Yes | Yes | Yes (every/some) |
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | Yes | Yes | Yes |
| `ARRAY_AGG` | Yes | Yes | Yes (collect_list) |
| `STRING_AGG` / `LISTAGG` | Yes | Yes (listagg) | No |
| `GROUPING SETS` | Yes | Yes | Yes |
| `CUBE` | Yes | Yes | Yes |
| `ROLLUP` | Yes | Yes | Yes |
| `FILTER` clause | Yes | Yes | Yes |
| `GROUPING()` function | Yes | Yes | Yes |

---

## String Functions

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `CONCAT`, `\|\|` | Yes | Yes | Yes |
| `SUBSTRING` | Yes | Yes | Yes |
| `TRIM`, `LTRIM`, `RTRIM`, `BTRIM` | Yes | Yes | Yes |
| `UPPER`, `LOWER` | Yes | Yes | Yes |
| `LENGTH`, `CHAR_LENGTH` | Yes | Yes | Yes |
| `REPLACE` | Yes | Yes | Yes |
| `REGEXP_REPLACE` | Yes | Yes | Yes |
| `REGEXP_MATCH` | Yes | Yes (regexp_extract) | Yes (regexp_extract) |
| `SPLIT_PART` | Yes | Yes | No (split + index) |
| `STARTS_WITH`, `ENDS_WITH` | Yes | Yes (starts_with) | Yes (startswith) |
| `LPAD`, `RPAD` | Yes | Yes | Yes |
| `REVERSE` | Yes | Yes | Yes |
| `REPEAT` | Yes | Yes | Yes |
| `TRANSLATE` | Yes | Yes | Yes |
| `INITCAP` | Yes | Yes | Yes |
| `LEFT`, `RIGHT` | Yes | Yes | Yes |
| `POSITION`, `STRPOS` | Yes | Yes | Yes (locate) |
| `CHR`, `ASCII` | Yes | Yes | Yes |
| `OVERLAY` | Yes | Yes | Yes |
| `ENCODE`, `DECODE` | Yes | Yes | Yes |
| `MD5`, `SHA256`, `SHA512` | Yes | Yes | Yes |
| `TO_HEX` | Yes | Yes | Yes |
| `UUID` | Yes | Yes | Yes |

---

## Math Functions

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `ABS`, `SIGN` | Yes | Yes | Yes |
| `CEIL`, `FLOOR` | Yes | Yes | Yes |
| `ROUND`, `TRUNC` | Yes | Yes | Yes |
| `POWER`, `SQRT`, `CBRT` | Yes | Yes | Yes |
| `LOG`, `LOG2`, `LOG10`, `LN` | Yes | Yes | Yes |
| `EXP` | Yes | Yes | Yes |
| `MOD`, `%` | Yes | Yes | Yes |
| `PI`, `RANDOM` | Yes | Yes | Yes |
| `GCD`, `LCM` | Yes | Yes | No |
| `FACTORIAL` | Yes | No | Yes |
| `SIN`, `COS`, `TAN` | Yes | Yes | Yes |
| `ASIN`, `ACOS`, `ATAN`, `ATAN2` | Yes | Yes | Yes |
| `SINH`, `COSH`, `TANH` | Yes | Yes | No |
| `DEGREES`, `RADIANS` | Yes | Yes | Yes |
| `NANVL` | Yes | No | Yes |
| `ISNAN` | Yes | Yes (is_nan) | Yes |
| `ISZERO` | Yes | No | No |

---

## Date/Time Functions

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `NOW()`, `CURRENT_TIMESTAMP` | Yes | Yes | Yes |
| `CURRENT_DATE`, `CURRENT_TIME` | Yes | Yes | Yes |
| `DATE_TRUNC` | Yes | Yes | Yes |
| `DATE_PART` / `EXTRACT` | Yes | Yes | Yes |
| `DATE_BIN` | Yes | No | No |
| `TO_TIMESTAMP` | Yes | No (from_iso8601_timestamp) | Yes |
| `TO_DATE` | Yes | No | Yes |
| `TO_CHAR` | Yes | Yes (format_datetime) | Yes (date_format) |
| `INTERVAL` arithmetic | Yes | Yes | Yes |
| `MAKE_DATE` | Yes | No | Yes |
| `MAKE_TIMESTAMP` | Yes | No | Yes (make_timestamp) |
| `DATE_ADD`, `DATE_SUB` | No (use +/- INTERVAL) | Yes | Yes |
| `DATEDIFF` | No (use subtraction) | Yes (date_diff) | Yes |
| Timezone conversion | Yes (AT TIME ZONE) | Yes | Yes |
| `EPOCH` | Yes (extract epoch) | Yes | Yes |

---

## Type System & Casting

| Feature | SQE | Trino | Spark SQL |
|---------|-----|-------|-----------|
| `CAST(x AS type)` | Yes | Yes | Yes |
| `TRY_CAST(x AS type)` | Yes | Yes | No (try_cast UDF) |
| `::type` shorthand | Yes | Yes | No |
| `BOOLEAN` | Yes | Yes | Yes |
| `TINYINT`/`SMALLINT`/`INT`/`BIGINT` | Yes | Yes | Yes |
| `FLOAT`/`DOUBLE`/`REAL` | Yes | Yes | Yes |
| `DECIMAL(p,s)` | Yes | Yes | Yes |
| `VARCHAR`/`TEXT` | Yes | Yes | Yes |
| `DATE`/`TIMESTAMP`/`TIME` | Yes | Yes | Yes |
| `TIMESTAMP WITH TIME ZONE` | Yes | Yes | Yes |
| `BINARY`/`VARBINARY` | Yes | Yes | Yes |
| `INTERVAL` | Yes | Yes | Yes |
| `ARRAY` | Yes | Yes | Yes |
| `MAP` | Partial | Yes | Yes |
| `STRUCT`/`ROW` | Yes | Yes | Yes |
| `JSON` type | No (use VARCHAR) | Yes | No (use STRING) |
| `UUID` type | No | Yes | No |

---

## Joins

| Join Type | SQE | Trino | Spark SQL |
|-----------|-----|-------|-----------|
| `INNER JOIN` | Yes | Yes | Yes |
| `LEFT OUTER JOIN` | Yes | Yes | Yes |
| `RIGHT OUTER JOIN` | Yes | Yes | Yes |
| `FULL OUTER JOIN` | Yes | Yes | Yes |
| `CROSS JOIN` | Yes | Yes | Yes |
| `LEFT SEMI JOIN` | Yes | Yes | Yes |
| `LEFT ANTI JOIN` | Yes | Yes | Yes |
| `NATURAL JOIN` | Yes | Yes | Yes |
| `LATERAL JOIN` | Yes | Yes | Yes |
| `USING` clause | Yes | Yes | Yes |
| Non-equi joins | Yes | Yes | Yes |
| `ASOF JOIN` | No | No | No |

---

## Subqueries

| Feature | SQE | Trino | Spark SQL |
|---------|-----|-------|-----------|
| Scalar subquery | Yes | Yes | Yes |
| `IN (subquery)` | Yes | Yes | Yes |
| `EXISTS (subquery)` | Yes | Yes | Yes |
| `NOT EXISTS` | Yes | Yes | Yes |
| Correlated subqueries | Yes | Yes | Yes |
| Subquery in FROM | Yes | Yes | Yes |
| Subquery in SELECT | Yes | Yes | Yes |

---

## Common Table Expressions (CTEs)

| Feature | SQE | Trino | Spark SQL |
|---------|-----|-------|-----------|
| `WITH ... AS` | Yes | Yes | Yes |
| Multiple CTEs | Yes | Yes | Yes |
| Recursive CTEs | Yes | Yes | No |
| CTE in INSERT | Yes | Yes | Yes |
| CTE in CREATE TABLE AS | Yes | Yes | Yes |

---

## Set Operations

| Operation | SQE | Trino | Spark SQL |
|-----------|-----|-------|-----------|
| `UNION` | Yes | Yes | Yes |
| `UNION ALL` | Yes | Yes | Yes |
| `INTERSECT` | Yes | Yes | Yes |
| `INTERSECT ALL` | Yes | Yes | Yes |
| `EXCEPT` | Yes | Yes | Yes |
| `EXCEPT ALL` | Yes | Yes | Yes |

---

## Conditional Expressions

| Expression | SQE | Trino | Spark SQL |
|------------|-----|-------|-----------|
| `CASE WHEN ... THEN ... END` | Yes | Yes | Yes |
| `COALESCE(a, b, ...)` | Yes | Yes | Yes |
| `NULLIF(a, b)` | Yes | Yes | Yes |
| `GREATEST(a, b, ...)` | Yes | Yes | Yes |
| `LEAST(a, b, ...)` | Yes | Yes | Yes |
| `IF(cond, then, else)` | No (use CASE) | Yes | Yes |
| `IIF` | No | No | No |
| `DECODE` | No | No | Yes |
| `NVL` / `IFNULL` | No (use COALESCE) | No | Yes |

---

## Table & Generator Functions

| Function | SQE | Trino | Spark SQL |
|----------|-----|-------|-----------|
| `UNNEST(array)` | Yes | Yes | Yes (explode) |
| `generate_series(start, stop)` | Yes | Yes (sequence) | Yes (sequence) |
| `VALUES` clause | Yes | Yes | Yes |
| Table functions in FROM | Yes | Yes | Yes |

---

## DDL & DML (via SQE + Iceberg)

| Statement | SQE | Trino + Iceberg | Spark + Iceberg |
|-----------|-----|-----------------|-----------------|
| `SELECT` | Yes | Yes | Yes |
| `CREATE TABLE AS SELECT` | Yes | Yes | Yes |
| `INSERT INTO ... SELECT` | Yes | Yes | Yes |
| `CREATE VIEW` | Yes (via Polaris REST) | Yes | Yes |
| `DROP VIEW` | Yes (via Polaris REST) | Yes | Yes |
| `DROP TABLE` | Yes | Yes | Yes |
| `ALTER TABLE RENAME` | Yes | Yes | Yes |
| `MERGE INTO` | Planned | Yes | Yes |
| `DELETE FROM` | Planned | Yes | Yes |
| `UPDATE` | Planned | Yes | Yes |
| `ALTER TABLE ADD COLUMN` | Planned (via Polaris) | Yes | Yes |
| `ALTER TABLE DROP COLUMN` | Planned (via Polaris) | Yes | Yes |
| `CREATE SCHEMA` | No | Yes | Yes |
| `DROP SCHEMA` | No | Yes | Yes |
| `TRUNCATE TABLE` | No | Yes | Yes |

---

## Iceberg-Specific Features

| Feature | SQE | Trino + Iceberg | Spark + Iceberg |
|---------|-----|-----------------|-----------------|
| Partition pruning | Basic (via DataFusion) | Full | Full |
| Time travel (`AS OF`) | No | Yes | Yes |
| Snapshot queries | No | Yes | Yes |
| Schema evolution | Via Polaris REST | Yes | Yes |
| Hidden partitioning | Via Iceberg metadata | Yes | Yes |
| Merge-on-Read deletes | Planned | Yes | Yes |
| Copy-on-Write deletes | No | Yes | Yes |
| Compaction | No | Yes (OPTIMIZE) | Yes (rewrite_data_files) |
| Manifest caching | Planned | Yes | Yes |
| Row-level security | Planned (OPA/Cedar) | No (needs Ranger) | No (needs Ranger) |
| Column masking | Planned (OPA/Cedar) | No | No |

---

## Metadata Queries

| Query | SQE | Trino | Spark SQL |
|-------|-----|-------|-----------|
| `SHOW CATALOGS` | Yes | Yes | Yes |
| `SHOW SCHEMAS` | Yes | Yes | Yes |
| `SHOW TABLES` | Yes | Yes | Yes |
| `SHOW COLUMNS` | Via information_schema | Yes | Yes |
| `SHOW CREATE TABLE` | No | Yes | Yes |
| `DESCRIBE table` | Via information_schema | Yes | Yes |
| `information_schema.tables` | Yes | Yes | Yes |
| `information_schema.columns` | Yes | Yes | Yes |
| `information_schema.schemata` | Yes | Yes | Yes |
| `EXPLAIN` | Yes | Yes | Yes |
| `EXPLAIN ANALYZE` | Via DataFusion | Yes | No |

---

## Protocol & Connectivity

| Feature | SQE | Trino | Spark SQL |
|---------|-----|-------|-----------|
| Arrow Flight SQL (gRPC) | Yes (primary) | No | No |
| Trino HTTP protocol | Yes (compat layer) | Yes (native) | No |
| JDBC | Via Flight SQL JDBC | Yes (native) | Yes (Thrift) |
| ODBC | Via Flight SQL ODBC | Yes | Yes |
| Python (ADBC) | Yes | Yes (trino-python) | Yes (PySpark) |
| dbt | Planned (dbt-sqe) | Yes (dbt-trino) | Yes (dbt-spark) |

---

## Key Advantages of SQE over Trino

1. **Arrow-native** — no serialization overhead between engine and clients; Flight SQL transfers columnar Arrow batches directly
2. **Rust performance** — no JVM GC pauses, lower memory footprint, faster startup
3. **Fine-grained security** — row-level filters and column masks via OPA/Cedar policy engine (planned), enforced at the query plan level before optimization
4. **Bearer token passthrough** — every query runs as the authenticated user against Polaris; no service account with god-mode access
5. **Iceberg-native** — built specifically for Iceberg via iceberg-rust; no connector abstraction layer

## Key Limitations vs Trino

1. **No federated queries** — SQE reads only from Iceberg/Polaris (Trino has 50+ connectors)
2. **No UDFs in SQL** — custom functions require Rust; no CREATE FUNCTION support
3. **No time travel** — snapshot/AS OF queries not yet implemented
4. **Single-node only** — distributed execution is structurally in place but not wired
5. **No MERGE/DELETE/UPDATE** — write path is append-only (CTAS + INSERT); in-place mutations planned

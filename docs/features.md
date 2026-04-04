# SQL Feature Comparison: SQE vs Trino vs Spark SQL

SQE is built on **Apache DataFusion 51** which provides the SQL execution engine. All standard SQL features come from DataFusion; SQE adds catalog integration (Polaris/Iceberg), auth (Keycloak), and DDL routing.

## Quick Summary

| Category | SQE (DataFusion 51) | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| Window functions | ✅ Full | ✅ Full | ✅ Full |
| Aggregate functions | ✅ Full | ✅ Full | ✅ Full |
| Joins | ✅ Full (7 types) | ✅ Full | ✅ Full |
| Subqueries | ✅ Full | ✅ Full | ✅ Full |
| CTEs | ✅ WITH + recursive | ✅ WITH + recursive | ✅ WITH + recursive |
| Set operations | ✅ Full | ✅ Full | ✅ Full |
| JSON | ⚠️ Partial | ✅ Full | ✅ Full |
| Array/Map types | ⚠️ Partial | ✅ Full | ✅ Full |
| MERGE INTO | ✅ CoW | ✅ | ✅ |
| DELETE | ✅ CoW | ✅ | ✅ |
| UPDATE | ✅ CoW | ✅ | ✅ |
| PIVOT/UNPIVOT | ❌ | ❌ | ⚠️ PIVOT only |
| QUALIFY | ❌ | ❌ | ❌ |
| Lambda expressions | ❌ | ✅ | ✅ |
| GROUPING SETS | ✅ | ✅ | ✅ |
| Iceberg time travel | ❌ | ✅ | ✅ |
| Federated queries | ❌ | ✅ (connectors) | ✅ (connectors) |
| UDFs | ⚠️ Rust API only | ✅ Java/Python | ✅ Java/Scala/Python |

---

## Window Functions

**✅ LEAD, LAG, PARTITION BY, ORDER BY, and frame specs are all supported.**

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `ROW_NUMBER()` | ✅ | ✅ | ✅ |
| `RANK()` | ✅ | ✅ | ✅ |
| `DENSE_RANK()` | ✅ | ✅ | ✅ |
| `NTILE(n)` | ✅ | ✅ | ✅ |
| `LEAD(col, offset, default)` | ✅ | ✅ | ✅ |
| `LAG(col, offset, default)` | ✅ | ✅ | ✅ |
| `FIRST_VALUE(col)` | ✅ | ✅ | ✅ |
| `LAST_VALUE(col)` | ✅ | ✅ | ✅ |
| `NTH_VALUE(col, n)` | ✅ | ✅ | ✅ |
| `CUME_DIST()` | ✅ | ✅ | ✅ |
| `PERCENT_RANK()` | ✅ | ✅ | ✅ |
| `PARTITION BY` | ✅ | ✅ | ✅ |
| `ORDER BY` in window | ✅ | ✅ | ✅ |
| `ROWS BETWEEN ... AND ...` | ✅ | ✅ | ✅ |
| `RANGE BETWEEN ... AND ...` | ✅ | ✅ | ✅ |
| `GROUPS BETWEEN` | ✅ | ❌ | ❌ |

**Example — works identically in SQE:**

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
|----------|:---:|:---:|:---:|
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | ✅ | ✅ | ✅ |
| `COUNT(DISTINCT col)` | ✅ | ✅ | ✅ |
| `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP` | ✅ | ✅ | ✅ |
| `VARIANCE`, `VAR_POP`, `VAR_SAMP` | ✅ | ✅ | ✅ |
| `COVAR_POP`, `COVAR_SAMP` | ✅ | ✅ | ✅ |
| `CORR` | ✅ | ✅ | ✅ |
| `APPROX_DISTINCT` | ✅ | ✅ | ✅ |
| `APPROX_PERCENTILE_CONT` | ✅ | ✅ | ✅ |
| `APPROX_MEDIAN` | ✅ | ❌ | ❌ |
| `MEDIAN` | ✅ | ❌ | ❌ |
| `BOOL_AND`, `BOOL_OR` | ✅ | ✅ | ✅ |
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | ✅ | ✅ | ✅ |
| `ARRAY_AGG` | ✅ | ✅ | ✅ |
| `STRING_AGG` / `LISTAGG` | ✅ | ✅ | ❌ |
| `GROUPING SETS` | ✅ | ✅ | ✅ |
| `CUBE` | ✅ | ✅ | ✅ |
| `ROLLUP` | ✅ | ✅ | ✅ |
| `FILTER` clause | ✅ | ✅ | ✅ |
| `GROUPING()` function | ✅ | ✅ | ✅ |
| `REGR_SLOPE`, `REGR_INTERCEPT`, etc. | ✅ | ✅ | ❌ |

---

## String Functions

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `CONCAT`, `\|\|` | ✅ | ✅ | ✅ |
| `SUBSTRING` | ✅ | ✅ | ✅ |
| `TRIM`, `LTRIM`, `RTRIM`, `BTRIM` | ✅ | ✅ | ✅ |
| `UPPER`, `LOWER` | ✅ | ✅ | ✅ |
| `LENGTH`, `CHAR_LENGTH` | ✅ | ✅ | ✅ |
| `REPLACE` | ✅ | ✅ | ✅ |
| `REGEXP_REPLACE` | ✅ | ✅ | ✅ |
| `REGEXP_MATCH` | ✅ | ✅ | ✅ |
| `REGEXP_LIKE` | ✅ | ✅ | ❌ |
| `REGEXP_COUNT` | ✅ | ❌ | ❌ |
| `SPLIT_PART` | ✅ | ✅ | ❌ |
| `STARTS_WITH`, `ENDS_WITH` | ✅ | ✅ | ✅ |
| `LPAD`, `RPAD` | ✅ | ✅ | ✅ |
| `REVERSE` | ✅ | ✅ | ✅ |
| `REPEAT` | ✅ | ✅ | ✅ |
| `TRANSLATE` | ✅ | ✅ | ✅ |
| `INITCAP` | ✅ | ✅ | ✅ |
| `LEFT`, `RIGHT` | ✅ | ✅ | ✅ |
| `POSITION`, `STRPOS` | ✅ | ✅ | ✅ |
| `CHR`, `ASCII` | ✅ | ✅ | ✅ |
| `OVERLAY` | ✅ | ✅ | ✅ |
| `ENCODE`, `DECODE` | ✅ | ✅ | ✅ |
| `MD5`, `SHA256`, `SHA512` | ✅ | ✅ | ✅ |
| `TO_HEX` | ✅ | ✅ | ✅ |
| `UUID` | ✅ | ✅ | ✅ |
| `LEVENSHTEIN` | ✅ | ✅ | ✅ |
| `CONTAINS` | ✅ | ❌ | ✅ |

---

## Math Functions

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `ABS`, `SIGN` | ✅ | ✅ | ✅ |
| `CEIL`, `FLOOR` | ✅ | ✅ | ✅ |
| `ROUND`, `TRUNC` | ✅ | ✅ | ✅ |
| `POWER`, `SQRT`, `CBRT` | ✅ | ✅ | ✅ |
| `LOG`, `LOG2`, `LOG10`, `LN` | ✅ | ✅ | ✅ |
| `EXP` | ✅ | ✅ | ✅ |
| `MOD`, `%` | ✅ | ✅ | ✅ |
| `PI`, `RANDOM` | ✅ | ✅ | ✅ |
| `GCD`, `LCM` | ✅ | ✅ | ❌ |
| `FACTORIAL` | ✅ | ❌ | ✅ |
| `SIN`, `COS`, `TAN` | ✅ | ✅ | ✅ |
| `ASIN`, `ACOS`, `ATAN`, `ATAN2` | ✅ | ✅ | ✅ |
| `SINH`, `COSH`, `TANH` | ✅ | ✅ | ❌ |
| `DEGREES`, `RADIANS` | ✅ | ✅ | ✅ |
| `NANVL` | ✅ | ❌ | ✅ |
| `ISNAN` | ✅ | ✅ | ✅ |
| `ISZERO` | ✅ | ❌ | ❌ |

---

## Date/Time Functions

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `NOW()`, `CURRENT_TIMESTAMP` | ✅ | ✅ | ✅ |
| `CURRENT_DATE`, `CURRENT_TIME` | ✅ | ✅ | ✅ |
| `DATE_TRUNC` | ✅ | ✅ | ✅ |
| `DATE_PART` / `EXTRACT` | ✅ | ✅ | ✅ |
| `DATE_BIN` | ✅ | ❌ | ❌ |
| `TO_TIMESTAMP` | ✅ | ❌ | ✅ |
| `TO_DATE` | ✅ | ❌ | ✅ |
| `TO_CHAR` | ✅ | ✅ | ✅ |
| `INTERVAL` arithmetic | ✅ | ✅ | ✅ |
| `MAKE_DATE` | ✅ | ❌ | ✅ |
| `MAKE_TIMESTAMP` | ✅ | ❌ | ✅ |
| `FROM_UNIXTIME` | ✅ | ✅ | ✅ |
| `DATE_ADD`, `DATE_SUB` | ❌ | ✅ | ✅ |
| `DATEDIFF` | ❌ | ✅ | ✅ |
| Timezone (`AT TIME ZONE`) | ✅ | ✅ | ✅ |
| `EPOCH` | ✅ | ✅ | ✅ |

---

## Type System & Casting

| Feature | SQE | Trino | Spark SQL |
|---------|:---:|:---:|:---:|
| `CAST(x AS type)` | ✅ | ✅ | ✅ |
| `TRY_CAST(x AS type)` | ✅ | ✅ | ❌ |
| `::type` shorthand | ✅ | ✅ | ❌ |
| `BOOLEAN` | ✅ | ✅ | ✅ |
| `TINYINT`/`SMALLINT`/`INT`/`BIGINT` | ✅ | ✅ | ✅ |
| `FLOAT`/`DOUBLE`/`REAL` | ✅ | ✅ | ✅ |
| `DECIMAL(p,s)` | ✅ | ✅ | ✅ |
| `VARCHAR`/`TEXT` | ✅ | ✅ | ✅ |
| `DATE`/`TIMESTAMP`/`TIME` | ✅ | ✅ | ✅ |
| `TIMESTAMP WITH TIME ZONE` | ✅ | ✅ | ✅ |
| `BINARY`/`VARBINARY` | ✅ | ✅ | ✅ |
| `INTERVAL` | ✅ | ✅ | ✅ |
| `ARRAY` | ✅ | ✅ | ✅ |
| `MAP` | ⚠️ Partial | ✅ | ✅ |
| `STRUCT`/`ROW` | ✅ | ✅ | ✅ |
| `JSON` type | ❌ | ✅ | ❌ |
| `UUID` type | ❌ | ✅ | ❌ |

---

## Joins

| Join Type | SQE | Trino | Spark SQL |
|-----------|:---:|:---:|:---:|
| `INNER JOIN` | ✅ | ✅ | ✅ |
| `LEFT OUTER JOIN` | ✅ | ✅ | ✅ |
| `RIGHT OUTER JOIN` | ✅ | ✅ | ✅ |
| `FULL OUTER JOIN` | ✅ | ✅ | ✅ |
| `CROSS JOIN` | ✅ | ✅ | ✅ |
| `LEFT SEMI JOIN` | ✅ | ✅ | ✅ |
| `LEFT ANTI JOIN` | ✅ | ✅ | ✅ |
| `NATURAL JOIN` | ✅ | ✅ | ✅ |
| `LATERAL JOIN` | ✅ | ✅ | ✅ |
| `USING` clause | ✅ | ✅ | ✅ |
| Non-equi joins | ✅ | ✅ | ✅ |
| `ASOF JOIN` | ❌ | ❌ | ❌ |

---

## Subqueries

| Feature | SQE | Trino | Spark SQL |
|---------|:---:|:---:|:---:|
| Scalar subquery | ✅ | ✅ | ✅ |
| `IN (subquery)` | ✅ | ✅ | ✅ |
| `EXISTS (subquery)` | ✅ | ✅ | ✅ |
| `NOT EXISTS` | ✅ | ✅ | ✅ |
| Correlated subqueries | ✅ | ✅ | ✅ |
| Subquery in FROM | ✅ | ✅ | ✅ |
| Subquery in SELECT | ✅ | ✅ | ✅ |

---

## Common Table Expressions (CTEs)

| Feature | SQE | Trino | Spark SQL |
|---------|:---:|:---:|:---:|
| `WITH ... AS` | ✅ | ✅ | ✅ |
| Multiple CTEs | ✅ | ✅ | ✅ |
| Recursive CTEs | ✅ | ✅ | ❌ |
| CTE in INSERT | ✅ | ✅ | ✅ |
| CTE in CREATE TABLE AS | ✅ | ✅ | ✅ |

---

## Set Operations

| Operation | SQE | Trino | Spark SQL |
|-----------|:---:|:---:|:---:|
| `UNION` | ✅ | ✅ | ✅ |
| `UNION ALL` | ✅ | ✅ | ✅ |
| `INTERSECT` | ✅ | ✅ | ✅ |
| `INTERSECT ALL` | ✅ | ✅ | ✅ |
| `EXCEPT` | ✅ | ✅ | ✅ |
| `EXCEPT ALL` | ✅ | ✅ | ✅ |

---

## Conditional Expressions

| Expression | SQE | Trino | Spark SQL |
|------------|:---:|:---:|:---:|
| `CASE WHEN ... THEN ... END` | ✅ | ✅ | ✅ |
| `COALESCE(a, b, ...)` | ✅ | ✅ | ✅ |
| `NULLIF(a, b)` | ✅ | ✅ | ✅ |
| `GREATEST(a, b, ...)` | ✅ | ✅ | ✅ |
| `LEAST(a, b, ...)` | ✅ | ✅ | ✅ |
| `NVL` / `NVL2` | ✅ | ❌ | ✅ |
| `IF(cond, then, else)` | ❌ | ✅ | ✅ |
| `IIF` | ❌ | ❌ | ❌ |
| `DECODE` | ❌ | ❌ | ✅ |

---

## Array & Map Functions

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `ARRAY[1, 2, 3]` / `MAKE_ARRAY` | ✅ | ✅ | ✅ |
| `ARRAY_AGG` | ✅ | ✅ | ✅ |
| `ARRAY_APPEND` / `ARRAY_PREPEND` | ✅ | ✅ | ❌ |
| `ARRAY_CONCAT` | ✅ | ✅ | ✅ |
| `ARRAY_LENGTH` / `CARDINALITY` | ✅ | ✅ | ✅ |
| `ARRAY_CONTAINS` / `ARRAY_HAS` | ✅ | ✅ | ✅ |
| `ARRAY_POSITION` | ✅ | ✅ | ❌ |
| `ARRAY_REMOVE` | ✅ | ✅ | ✅ |
| `ARRAY_SORT` | ✅ | ✅ | ✅ |
| `ARRAY_DISTINCT` | ✅ | ✅ | ✅ |
| `ARRAY_INTERSECT` | ✅ | ✅ | ✅ |
| `ARRAY_UNION` | ✅ | ✅ | ✅ |
| `ARRAY_EXCEPT` | ✅ | ✅ | ✅ |
| `ARRAY_MIN` / `ARRAY_MAX` | ✅ | ✅ | ❌ |
| `FLATTEN` | ✅ | ✅ | ✅ |
| `UNNEST` / `EXPLODE` | ✅ | ✅ | ✅ |
| `MAP(keys, values)` | ✅ | ✅ | ✅ |
| `MAP_KEYS` / `MAP_VALUES` | ✅ | ✅ | ✅ |
| `MAP_EXTRACT` | ✅ | ✅ | ✅ |
| Lambda (`x -> x + 1`) | ❌ | ✅ | ✅ |

---

## Table & Generator Functions

| Function | SQE | Trino | Spark SQL |
|----------|:---:|:---:|:---:|
| `UNNEST(array)` | ✅ | ✅ | ✅ |
| `generate_series(start, stop)` | ✅ | ✅ | ✅ |
| `VALUES` clause | ✅ | ✅ | ✅ |
| Table functions in FROM | ✅ | ✅ | ✅ |

---

## DDL & DML (via SQE + Iceberg)

| Statement | SQE | Trino + Iceberg | Spark + Iceberg |
|-----------|:---:|:---:|:---:|
| `SELECT` | ✅ | ✅ | ✅ |
| `CREATE TABLE AS SELECT` | ✅ | ✅ | ✅ |
| `CREATE OR REPLACE TABLE AS SELECT` | ✅ | ✅ | ✅ |
| `INSERT INTO ... SELECT` | ✅ | ✅ | ✅ |
| `CREATE VIEW` | ✅ | ✅ | ✅ |
| `DROP VIEW` | ✅ | ✅ | ✅ |
| `DROP TABLE` | ✅ | ✅ | ✅ |
| `ALTER TABLE RENAME` | ✅ | ✅ | ✅ |
| `MERGE INTO` | ✅ (CoW) | ✅ | ✅ |
| `DELETE FROM` | ✅ (CoW) | ✅ | ✅ |
| `UPDATE` | ✅ (CoW) | ✅ | ✅ |
| `ALTER TABLE ADD COLUMN` | 🔜 | ✅ | ✅ |
| `ALTER TABLE DROP COLUMN` | 🔜 | ✅ | ✅ |
| `CREATE SCHEMA` | ✅ | ✅ | ✅ |
| `DROP SCHEMA` | ✅ | ✅ | ✅ |
| `TRUNCATE TABLE` | ❌ | ✅ | ✅ |

---

## Iceberg-Specific Features

| Feature | SQE | Trino + Iceberg | Spark + Iceberg |
|---------|:---:|:---:|:---:|
| Partition pruning | ⚠️ Basic | ✅ Full | ✅ Full |
| Time travel (`AS OF`) | ❌ | ✅ | ✅ |
| Snapshot queries | ❌ | ✅ | ✅ |
| Schema evolution | ✅ via Polaris | ✅ | ✅ |
| Hidden partitioning | ✅ via metadata | ✅ | ✅ |
| Merge-on-Read deletes | 🔜 | ✅ | ✅ |
| Copy-on-Write deletes | ✅ | ✅ | ✅ |
| Compaction | ❌ | ✅ OPTIMIZE | ✅ rewrite_data_files |
| Manifest caching | 🔜 | ✅ | ✅ |
| Row-level security | 🔜 OPA/Cedar | ❌ needs Ranger | ❌ needs Ranger |
| Column masking | 🔜 OPA/Cedar | ❌ | ❌ |

---

## Metadata Queries

| Query | SQE | Trino | Spark SQL |
|-------|:---:|:---:|:---:|
| `SHOW CATALOGS` | ✅ | ✅ | ✅ |
| `SHOW SCHEMAS` | ✅ | ✅ | ✅ |
| `SHOW TABLES` | ✅ | ✅ | ✅ |
| `SHOW COLUMNS` | ⚠️ via info_schema | ✅ | ✅ |
| `SHOW CREATE TABLE` | ❌ | ✅ | ✅ |
| `DESCRIBE table` | ⚠️ via info_schema | ✅ | ✅ |
| `information_schema.tables` | ✅ | ✅ | ✅ |
| `information_schema.columns` | ✅ | ✅ | ✅ |
| `information_schema.schemata` | ✅ | ✅ | ✅ |
| `EXPLAIN` | ✅ | ✅ | ✅ |
| `EXPLAIN ANALYZE` | ✅ | ✅ | ❌ |

---

## Protocol & Connectivity

| Feature | SQE | Trino | Spark SQL |
|---------|:---:|:---:|:---:|
| Arrow Flight SQL (gRPC) | ✅ primary | ❌ | ❌ |
| Trino HTTP protocol | ✅ compat | ✅ native | ❌ |
| JDBC | ✅ via Flight SQL | ✅ native | ✅ Thrift |
| ODBC | ✅ via Flight SQL | ✅ | ✅ |
| Python (ADBC) | ✅ | ✅ trino-python | ✅ PySpark |
| dbt | 🔜 dbt-sqe | ✅ dbt-trino | ✅ dbt-spark |

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully supported |
| ⚠️ | Partially supported / workaround available |
| 🔜 | Planned / in roadmap |
| ❌ | Not supported |

---

## Key Advantages of SQE over Trino

1. **Arrow-native** — no serialization overhead; Flight SQL transfers columnar Arrow batches directly
2. **Rust performance** — no JVM GC pauses, lower memory footprint, faster startup
3. **Fine-grained security** — row-level filters and column masks via OPA/Cedar policy engine (planned), enforced at the query plan level before optimization
4. **Bearer token passthrough** — every query runs as the authenticated user against Polaris; no service account with god-mode access
5. **Iceberg-native** — built specifically for Iceberg via iceberg-rust; no connector abstraction layer

## Key Limitations vs Trino

1. **No federated queries** — SQE reads only from Iceberg/Polaris (Trino has 50+ connectors)
2. **No UDFs in SQL** — custom functions require Rust; no CREATE FUNCTION support
3. **No time travel** — snapshot/AS OF queries not yet implemented
4. **Single-node only** — distributed execution is structurally in place but not wired
5. **No Merge-on-Read** — row-level mutations use Copy-on-Write (full file rewrite); MoR with position deletes planned for write-heavy workloads

# SQL Support

SQE inherits DataFusion's broad SQL support and adds Iceberg-specific operations.

## Query Language

### SELECT & Expressions

```sql
-- Full ANSI SQL
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id
HAVING SUM(amount) > 1000
ORDER BY total DESC
LIMIT 10;

-- CTEs
WITH monthly AS (
    SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS total
    FROM orders GROUP BY 1
)
SELECT month, total, LAG(total) OVER (ORDER BY month) AS prev_month
FROM monthly;

-- Subqueries, EXISTS, IN
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders WHERE amount > 500);
```

### Window Functions

```sql
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg,
    salary - LAG(salary) OVER (ORDER BY hire_date) AS salary_diff
FROM employees;
```

Supported: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `CUME_DIST`, `PERCENT_RANK`, with `PARTITION BY`, `ORDER BY`, and frame clauses (`ROWS BETWEEN`, `RANGE BETWEEN`).

### Joins

```sql
-- All join types
SELECT * FROM a INNER JOIN b ON a.id = b.id;
SELECT * FROM a LEFT JOIN b ON a.id = b.id;
SELECT * FROM a RIGHT JOIN b ON a.id = b.id;
SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id;
SELECT * FROM a CROSS JOIN b;

-- Anti and semi joins (via EXISTS/NOT EXISTS)
SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id);
```

### Set Operations

```sql
SELECT id FROM a UNION ALL SELECT id FROM b;
SELECT id FROM a INTERSECT SELECT id FROM b;
SELECT id FROM a EXCEPT SELECT id FROM b;
```

## DDL

```sql
-- Schemas
CREATE SCHEMA analytics;
DROP SCHEMA staging;

-- Tables (via CTAS)
CREATE TABLE analytics.summary AS
SELECT region, SUM(revenue) AS total FROM sales GROUP BY region;

-- CREATE OR REPLACE
CREATE OR REPLACE TABLE analytics.summary AS
SELECT region, SUM(revenue) AS total FROM sales GROUP BY region;

-- Views
CREATE VIEW active_customers AS
SELECT * FROM customers WHERE status = 'active';

DROP VIEW active_customers;

-- Drop
DROP TABLE analytics.summary;
DROP TABLE IF EXISTS analytics.summary;
```

## DML

```sql
-- Insert from query
INSERT INTO target_table
SELECT * FROM source_table WHERE condition;

-- CTAS
CREATE TABLE new_table AS SELECT * FROM existing_table;

-- DELETE (Copy-on-Write by default)
DELETE FROM orders WHERE status = 'cancelled';
DELETE FROM orders WHERE customer_id IN (SELECT id FROM blacklist);

-- DELETE (Merge-on-Read; opt in via table property)
ALTER TABLE orders SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');
DELETE FROM orders WHERE status = 'cancelled';  -- writes a position delete file

-- UPDATE (Copy-on-Write)
UPDATE orders SET status = 'shipped' WHERE tracking_id IS NOT NULL;
UPDATE orders SET amount = CASE WHEN amount > 1000 THEN amount * 0.9 ELSE amount END;

-- MERGE INTO (Copy-on-Write)
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET value = source.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value);
```

All row-level write operations (DELETE, UPDATE, MERGE INTO) default to Copy-on-Write via the RisingWave iceberg-rust fork's `rewrite_files()` transaction API. Affected data files are read, filtered/transformed, and rewritten as new files in a single atomic commit.

DELETE also supports Merge-on-Read when `write.delete.mode = 'merge-on-read'` is set on the table. SQE writes a position-delete file (or an equality-delete file when the table declares an identifier-field-id) and commits via `FastAppendAction` / `RowDeltaAction`. MoR avoids rewriting whole data files for small deletes against large tables.

## Data Types

SQE accepts the standard ANSI SQL type set plus a few Iceberg-specific extensions:

```sql
CREATE TABLE events (
    id              BIGINT,
    payload         JSON,                       -- Aliases to Utf8 underneath
    occurred_at     TIMESTAMP(6),
    occurred_time   TIME(6),                    -- Time-of-day, microseconds
    occurred_at_tz  TIMESTAMP(6) WITH TIME ZONE,
    occurred_ns     TIMESTAMP_NS,               -- V3-only: nanosecond precision
    region_id       INTEGER,
    amount          DECIMAL(18, 2)
);
```

- `JSON` columns store as `Utf8`. `CAST(json_col AS BIGINT|VARCHAR|DOUBLE)` rides DataFusion's built-in coercion. JSON-shaped extraction works through `json_extract`, `json_extract_scalar`, `json_array_length`, `json_parse`, `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`.
- `TIME` / `TIME(p)` maps to Arrow `Time64(Microsecond)` since Iceberg's `time` primitive is microsecond-only across V2 and V3. Precisions 0..=6 collapse to microsecond. `TIME(p > 6)` rejects with a clear NotImplemented; use `TIMESTAMP(9)` for sub-microsecond resolution.
- `TIME WITH TIME ZONE` rejects at CREATE TABLE: Arrow has no equivalent. Use `TIMESTAMP WITH TIME ZONE` instead.
- `TIMESTAMP_NS` (and `TIMESTAMP_NS WITH TIME ZONE`) is a V3-only nanosecond timestamp. SQE auto-upgrades the table to format-version 3 when one of these types appears in a CREATE.
- `localtime()` returns Time64. `EXTRACT(HOUR|MINUTE|SECOND FROM time_col)` works through the Trino-aliased `hour()` / `minute()` / `second()` UDFs. `year()` / `month()` / `day()` on a TIME column raise a clear plan error per Trino spec.

## Metadata Queries

```sql
SHOW CATALOGS;
SHOW SCHEMAS;
SHOW TABLES;
SHOW TABLES IN schema_name;

-- information_schema
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.schemata;
SELECT * FROM information_schema.columns WHERE table_name = 'orders';

-- Query plan (logical + physical)
EXPLAIN SELECT * FROM orders WHERE amount > 100;

-- With actual execution metrics
EXPLAIN ANALYZE SELECT * FROM orders WHERE amount > 100;

-- With Iceberg file/row estimates (no execution)
EXPLAIN FULL SELECT * FROM orders WHERE amount > 100;
```

## Feature Comparison

| Category | SQE | Trino | Spark SQL |
|---|---|---|---|
| Window functions | Full | Full | Full |
| CTEs | Full | Full | Full |
| Joins (all types) | Full | Full | Full |
| Set operations | Full | Full | Full |
| CTAS | Yes | Yes | Yes |
| INSERT INTO SELECT | Yes | Yes | Yes |
| MERGE INTO | Yes (CoW) | Yes | Yes |
| DELETE FROM | Yes (CoW) | Yes | Yes |
| UPDATE | Yes (CoW) | Yes | Yes |
| Views | Yes | Yes | Yes |
| Arrow-native wire format | Yes | No (JSON) | No (Thrift) |
| Row-level security | [Yes (plan-rewritten, pluggable, off by default)](../sql-reference/grant-revoke.md) | Plugin | Ranger |
| Bearer token passthrough | Yes | No | No |

# SQL Support

SQE inherits DataFusion's comprehensive SQL support and adds Iceberg-specific operations.

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
```

> **Coming soon:** `MERGE INTO`, `DELETE FROM`, `UPDATE` — blocked on iceberg-rust OverwriteAction support.

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
| MERGE INTO | Planned | Yes | Yes |
| DELETE FROM | Planned | Yes | Yes |
| Views | Yes | Yes | Yes |
| Arrow-native wire format | Yes | No (JSON) | No (Thrift) |
| Row-level security | Planned | Plugin | Ranger |
| Bearer token passthrough | Yes | No | No |

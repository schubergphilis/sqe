# DML

Data Manipulation Language: reads, writes, updates, deletes, merges. SQE adds Iceberg time-travel clauses on `SELECT`, an `Iceberg`-aware `MERGE INTO`, and a `SET WRITE_BRANCH` shortcut for routing writes to a named branch.

Source: `crates/sqe-sql/src/time_travel.rs`, `crates/sqe-coordinator/src/{query_handler, write_handler}.rs`.

## SELECT

| Form | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `SELECT cols FROM t [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT N]` | `datafusion-builtin` | Standard SQL. | yes | yes | yes | yes |
| `WITH cte AS (...) SELECT ...` | `datafusion-builtin` | CTE. Multiple CTEs allowed. | yes | yes | yes | yes |
| `WITH RECURSIVE cte AS (...) SELECT ...` | `datafusion-builtin` | Recursive CTE. | yes | yes | no | yes |
| `SELECT DISTINCT cols FROM t` | `datafusion-builtin` | Distinct rows. | yes | yes | yes | yes |
| `SELECT * EXCLUDE (col, ...) FROM t` | `datafusion-builtin` | Exclude columns from `*`. | yes | - | - | yes |
| `SELECT * REPLACE (expr AS col) FROM t` | `datafusion-builtin` | Substitute one or more columns. | yes | - | - | yes |
| `SELECT cols FROM t1 [INNER\|LEFT\|RIGHT\|FULL] JOIN t2 ON ...` | `datafusion-builtin` | All join types including SEMI / ANTI. | yes | yes | yes | yes |
| `SELECT cols FROM t1 USING (col1, col2)` | `datafusion-builtin` | Equality on shared column names. | yes | yes | yes | yes |
| `SELECT cols FROM t1, LATERAL (SELECT ... FROM t2 WHERE ...)` | `datafusion-builtin` | Correlated subquery in FROM. | yes | yes | partial | yes |
| `SELECT cols FROM t TABLESAMPLE BERNOULLI (5)` | `datafusion-builtin` | Random sampling. | yes | yes | yes | yes |

### Time travel (Iceberg-specific)

| Form | Origin | Notes |
|---|---|---|
| `SELECT ... FROM t FOR VERSION AS OF snapshot_id` | `sqe-sql/time_travel.rs` | Read a specific snapshot (snapshot id, branch name, or tag name). |
| `SELECT ... FROM t FOR SYSTEM_TIME AS OF timestamp` | `datafusion-builtin` (sqlparser native) | Read snapshot active at the given timestamp. |
| `SELECT ... FROM t FOR INCREMENTAL BETWEEN SNAPSHOT s1 AND SNAPSHOT s2` | `sqe-sql/time_travel.rs` | SQE-specific. Returns rows added between two snapshots; useful for CDC-style processing. |

```sql
-- By snapshot id
SELECT * FROM events FOR VERSION AS OF 8472810294831234567;

-- By branch
SELECT * FROM events FOR VERSION AS OF 'dev_2026_05';

-- By tag
SELECT * FROM events FOR VERSION AS OF 'release_2026_q2';

-- By timestamp
SELECT * FROM events FOR SYSTEM_TIME AS OF TIMESTAMP '2026-04-01 00:00:00';

-- Incremental between two snapshots
SELECT * FROM events
FOR INCREMENTAL BETWEEN SNAPSHOT 1234 AND SNAPSHOT 5678;
```

## INSERT

| Form | Origin | Notes |
|---|---|---|
| `INSERT INTO t (cols) VALUES (...), (...)` | `sqlparser-rs` + `sqe-coordinator` | Multi-row literal insert. |
| `INSERT INTO t SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Insert from query. |
| `INSERT INTO t (col1, col2) SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Subset of columns; others get DEFAULT or NULL. |
| `INSERT OVERWRITE t SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Replace partition or table data. Targets the partitions implied by the SELECT. |

```sql
INSERT INTO events VALUES
    (1, 'click', TIMESTAMP '2026-05-08 09:00:00'),
    (2, 'view',  TIMESTAMP '2026-05-08 09:01:00');

INSERT INTO events (id, event_type, occurred_at)
SELECT id, kind, ts FROM staging.raw_events;
```

## UPDATE

| Form | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `UPDATE t SET col = expr [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | CoW or MoR by table property. | yes | yes | yes | yes |
| `UPDATE t SET col1 = e1, col2 = e2 [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | Multi-column set. | yes | yes | yes | yes |
| `UPDATE t SET col = expr FROM other o WHERE t.k = o.k` | `sqlparser-rs` + `sqe-coordinator` | Update from another table. | partial | yes | yes | yes |

```sql
UPDATE orders
SET status = 'shipped', shipped_at = now()
WHERE tracking_id IS NOT NULL;
```

## DELETE

| Form | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `DELETE FROM t [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | CoW (default) or MoR (`write.delete.mode = 'merge-on-read'`). | yes | yes | yes | yes |
| `DELETE FROM t USING other o WHERE t.k = o.k` | `sqlparser-rs` + `sqe-coordinator` | Delete by join. | partial | yes | yes | yes |
| `TRUNCATE TABLE t` | `sqe-sql/classifier.rs` | Rewrites to `DELETE FROM t`. Same MoR / CoW behaviour. | yes | yes | yes | yes |

```sql
DELETE FROM events WHERE event_type = 'spam';
TRUNCATE TABLE staging.tmp;
```

## MERGE

| Form | Origin | Notes |
|---|---|---|
| `MERGE INTO t USING s ON cond WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...)` | `sqlparser-rs` + `sqe-coordinator` | CoW or MoR. Multiple `WHEN MATCHED` branches with extra predicates allowed. |
| `MERGE INTO t USING s ON cond WHEN MATCHED THEN DELETE` | `sqlparser-rs` + `sqe-coordinator` | Delete matched rows. |
| `MERGE INTO t USING s ON cond WHEN MATCHED AND pred THEN UPDATE SET ... WHEN MATCHED THEN DELETE` | `sqlparser-rs` + `sqe-coordinator` | Conditional MATCHED branches. |

```sql
MERGE INTO orders t
USING staging.order_updates s
ON t.id = s.id
WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
WHEN MATCHED THEN UPDATE SET status = s.status, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, status, created_at) VALUES (s.id, s.status, s.created_at);
```

## Copy-on-Write vs Merge-on-Read

Three table properties control write semantics:

| Property | Default | Effect |
|---|---|---|
| `write.delete.mode` | `'copy-on-write'` | Switch to `'merge-on-read'` to write position / equality delete files instead of rewriting whole data files. |
| `write.update.mode` | `'copy-on-write'` | Same options. MoR UPDATE writes both deletes and inserts. |
| `write.merge.mode` | `'copy-on-write'` | Same options. |

Set per-table via `ALTER TABLE`:

```sql
ALTER TABLE orders SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);
```

When to choose:

- **CoW**: small tables, infrequent writes, predictable read latency. Default.
- **MoR**: large tables, frequent small deletes, willing to trade read amplification for write speed. Compact periodically with `system.rewrite_data_files`.

## COPY TO

| Form | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `COPY (SELECT ...) TO 'path' (FORMAT csv\|json\|parquet)` | `datafusion-builtin` | Write query result to a file. | - | yes | - | yes |
| `COPY t TO 'path' (FORMAT parquet)` | `datafusion-builtin` | Write whole table. | - | yes | - | yes |
| `COPY (SELECT ...) TO 'path' (FORMAT parquet, PARTITION_BY 'col1, col2')` | `datafusion-builtin` | Hive-style partitioned output. | - | yes | - | yes |

```sql
COPY (SELECT * FROM events WHERE occurred_at >= DATE '2026-05-01')
    TO '/tmp/may_events.parquet'
    (FORMAT parquet);

COPY events TO 's3://export/events.csv'
    (FORMAT csv, COMPRESSION gzip, HEADER true);
```

## Branch routing (SQE-specific)

Iceberg branches let you isolate writes from the production snapshot. SQE exposes branch routing as a session variable.

| Statement | Notes |
|---|---|
| `SET WRITE_BRANCH = 'name'` | Subsequent INSERT / UPDATE / DELETE / MERGE writes go to the named branch. |
| `SET WRITE_BRANCH = DEFAULT` | Reset to the default (main) branch. |
| `SET WRITE_BRANCH = NULL` | Same as `DEFAULT`. |

```sql
ALTER TABLE events CREATE BRANCH staging;
SET WRITE_BRANCH = 'staging';

INSERT INTO events SELECT * FROM new_data;
-- ^ writes to the 'staging' branch only

SELECT count(*) FROM events FOR VERSION AS OF 'staging';
-- ^ reads from staging branch

SET WRITE_BRANCH = DEFAULT;
```

## Session control

| Statement | Origin | Notes |
|---|---|---|
| `USE catalog.schema` | `sqe-sql/classifier.rs` | Switch active catalog and schema. Subsequent unqualified table references use this scope. |
| `USE schema` | `sqe-sql/classifier.rs` | Switch schema only. |
| `SET <variable> = <value>` | `sqlparser-rs` (DataFusion) | DataFusion session config. See `EXPLAIN ANALYZE` documentation for valid keys. |
| `BEGIN` / `COMMIT` / `ROLLBACK` | `sqe-sql/classifier.rs` | No-op stubs for JDBC compatibility. SQE does not implement multi-statement transactions; each commit is single-statement. |

## Comparison summary

| Operation | SQE | Trino + Iceberg | Spark + Iceberg | DuckDB |
|---|---|---|---|---|
| `SELECT` time travel | yes (`FOR VERSION AS OF`, branch / tag / id) | yes | yes | partial |
| Incremental SELECT | `FOR INCREMENTAL BETWEEN` (SQE-specific) | partial via Iceberg incremental APIs | yes | - |
| `INSERT INTO` | yes | yes | yes | yes |
| `UPDATE` (CoW + MoR) | yes | yes | yes | yes |
| `DELETE` (CoW + MoR) | yes | yes | yes | yes |
| `MERGE INTO` (CoW + MoR) | yes | yes | yes | - |
| `TRUNCATE TABLE` | yes (rewrites to DELETE) | yes | yes | yes |
| `COPY TO` | yes | - | - | yes |
| Branch-routed writes | `SET WRITE_BRANCH` (SQE) | partial | yes | - |
| Multi-statement transactions | no (no-op stubs) | no | no | yes |

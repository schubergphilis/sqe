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
| `INSERT INTO t (cols) VALUES (...), (...)` | `sqlparser-rs` + `sqe-coordinator` | Multi-row literal insert. Returns `rows_affected`. |
| `INSERT INTO t SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Insert from query. Returns `rows_affected`. |
| `INSERT INTO t (col1, col2) SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Subset of columns; others get DEFAULT or NULL. Returns `rows_affected`. Fails at planning if column count mismatches target. |
| `INSERT OVERWRITE t SELECT ... FROM s` | `sqlparser-rs` + `sqe-coordinator` | Replace partition or table data. Targets the partitions implied by the SELECT. Returns `rows_affected`. Retries on conflict. |

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
| `UPDATE t SET col = expr [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | CoW or MoR by table property. Returns `rows_affected`. Retries on conflict. | yes | yes | yes | yes |
| `UPDATE t SET col1 = e1, col2 = e2 [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | Multi-column set. Returns `rows_affected`. Retries on conflict. | yes | yes | yes | yes |
| `UPDATE t SET col = expr FROM other o WHERE t.k = o.k` | `sqlparser-rs` + `sqe-coordinator` | Update from another table. Returns `rows_affected`. Retries on conflict. | partial | yes | yes | yes |

```sql
UPDATE orders
SET status = 'shipped', shipped_at = now()
WHERE tracking_id IS NOT NULL;
```

## DELETE

| Form | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `DELETE FROM t [WHERE pred]` | `sqlparser-rs` + `sqe-coordinator` | CoW (default) or MoR (`write.delete.mode = 'merge-on-read'`). Returns `rows_affected`. Retries on conflict. | yes | yes | yes | yes |
| `DELETE FROM t USING other o WHERE t.k = o.k` | `sqlparser-rs` + `sqe-coordinator` | Delete by join. Returns `rows_affected`. Retries on conflict. | partial | yes | yes | yes |
| `TRUNCATE TABLE t` | `sqe-sql/classifier.rs` | Rewrites to `DELETE FROM t`. Returns the deleted row count. MoR/CoW behaviour per property. | yes | yes | yes | yes |
| `DELETE ... WHERE ...` (MoR, repeated) | `sqe-coordinator` write_handler | Identical statement run twice on MoR: first returns the deleted count, second returns 0 (rows already hidden). | - | - | - | - |

```sql
DELETE FROM events WHERE event_type = 'spam';
TRUNCATE TABLE staging.tmp;
```

## MERGE

| Form | Origin | Notes |
|---|---|---|
| `MERGE INTO t USING s ON cond WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...)` | `sqlparser-rs` + `sqe-coordinator` | CoW or MoR. Multiple `WHEN MATCHED` branches with extra predicates allowed. Returns `rows_affected`. Retries on conflict. |
| `MERGE INTO t USING s ON cond WHEN MATCHED THEN DELETE` | `sqlparser-rs` + `sqe-coordinator` | Delete matched rows. Returns `rows_affected`. Retries on conflict. |
| `MERGE INTO t USING s ON cond WHEN MATCHED AND pred THEN UPDATE SET ... WHEN MATCHED THEN DELETE` | `sqlparser-rs` + `sqe-coordinator` | Conditional MATCHED branches. Returns `rows_affected`. Retries on conflict. |
| `MERGE INTO t USING (empty source) ...` | `sqe-coordinator` | If `WHEN NOT MATCHED BY SOURCE` clauses exist, they apply (source is empty, so all target rows match that condition). Returns claimed-row count (number of target rows affected by BY SOURCE clauses). | - |

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

## DML result semantics

All DML statements (INSERT, UPDATE, DELETE, MERGE, TRUNCATE) return one row containing a single column `rows_affected`, the count of rows the statement inserted, updated, or deleted. Conflicts (concurrent writes on the same table) retry automatically for all DML statement types, not just INSERT/OVERWRITE.

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
| `SET WRITE_BRANCH = 'name'` | Subsequent INSERT / UPDATE / DELETE / MERGE / TRUNCATE writes, and Flight SQL bulk ingest, go to the named branch. |
| `SET WRITE_BRANCH = DEFAULT` | Reset to the default (main) branch. |
| `SET WRITE_BRANCH = NULL` | Same as `DEFAULT`. |

While a branch is set:

- The branch must already exist on the table being written (`ALTER TABLE ... CREATE BRANCH`). A write to a table without it fails instead of creating it. A table with no snapshot cannot hold a branch, so write its first rows on main.
- UPDATE, DELETE and MERGE read the table at the branch, so they change the branch's rows, not main's. A source table that holds a branch of the same name is also read at it, as Spark's `spark.wap.branch` does. Plain SELECT statements still read main; use `FOR VERSION AS OF 'name'` to read the branch.
- `CREATE TABLE ... AS SELECT` is refused, because a new table has no branch. Writes inside an explicit transaction are refused too, because a transaction publishes to main. `RESET WRITE_BRANCH` first.

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
| `USE catalog.schema` | `sqe-sql/classifier.rs` | Switch active catalog and schema. Subsequent unqualified table references use this scope. Over the Trino protocol the scope travels back as `X-Trino-Set-Catalog` / `X-Trino-Set-Schema` and the client echoes it; over Flight SQL it is stored on the session behind the bearer token, so every later request on that token, on any connection, resolves in the new scope. `current_schema()` reports it. |
| `USE schema` | `sqe-sql/classifier.rs` | Switch schema only; the catalog stays as it was. |
| `SET <variable> = <value>` | `sqlparser-rs` (DataFusion) | DataFusion session config. See `EXPLAIN ANALYZE` documentation for valid keys. |
| `BEGIN` / `START TRANSACTION` / `COMMIT` / `ROLLBACK` / `SET TRANSACTION` | `sqe-sql/classifier.rs` | Explicit transactions. See [Transactions](#transactions). |

## Transactions

Inside `START TRANSACTION` (or `BEGIN`), `INSERT`, `UPDATE`, `DELETE`, `MERGE` and `TRUNCATE` write to a private branch, `sqe-txn-<id>`, of each table they touch. Later statements of the transaction read those writes. Nobody else sees them. `COMMIT` publishes every branch to `main` in one catalog commit. If another writer moved a table first, `COMMIT` fails with `TRANSACTION_CONFLICT`, nothing is published, and the client retries the whole transaction. `ROLLBACK` drops the branches.

A transaction may write several tables in one catalog when the catalog commits them atomically: a REST catalog that serves `/v1/transactions/commit` (Polaris) or the embedded SQLite catalog. DDL, CTAS, writes to a second catalog and Hive writes stay refused with `AUTOCOMMIT_WRITE_CONFLICT`. A nested `BEGIN`, `SNAPSHOT` isolation and savepoints are refused. A transaction idle for 30 minutes is rolled back, and so is one open for 24 hours, however busy.

The transaction lives in the memory of the coordinator that started it. Over Trino the client carries its id in `X-Trino-Transaction-Id`, and the id names the coordinator process that minted it. A statement or `COMMIT` that reaches another coordinator, or the same one after a restart, fails with `TRANSACTION_CONFLICT` ("not found on this coordinator"). `ROLLBACK` of such an id succeeds, so the client can start over. Route every request of a transaction to one coordinator; SQE supports one coordinator replica today (see [Kubernetes](../deployment/kubernetes.md)).

Over Flight SQL a transaction starts one of two ways. The first is the `BeginTransaction` and `EndTransaction` RPCs, which ADBC uses. A DB-API connection opened without `autocommit=True` is in a transaction from its first statement. The client holds the id and sends it with every statement, prepared statement and bulk ingest, so the transaction spans connections on the same token. Uncommitted writes are lost when the client closes without `commit()`; the branches are rolled back once the transaction has sat idle for 30 minutes. A connection that runs one statement and closes needs `autocommit=True`.

Inside an RPC transaction, DDL and CTAS run in autocommit and the transaction stays open. The DML waits for `EndTransaction`. Dropping or replacing a table the transaction wrote makes its `COMMIT` fail. A prepared statement belongs to the transaction it was created in. A statement that sends another transaction id while the session holds a SQL `BEGIN` fails with `FAILED_PRECONDITION`. Isolation is `READ COMMITTED`.

A SQL `BEGIN` over Flight SQL instead stores the id on the session behind the bearer token, like `USE`. It spans every connection on that token. When the session ends (idle timeout, token expiry, a restart, or the session cap), the transaction is rolled back. The client is not told. Its next request authenticates a fresh session with no transaction, so later writes commit one by one and `COMMIT` is a no-op. Keep SQL transactions over Flight SQL shorter than the session idle timeout and the token lifetime.

A graceful shutdown rolls back every open transaction. A crash leaves the branches behind, holding no visible data. A branch whose last write is more than 25 hours old (the 24-hour cap plus an hour for clock skew) cannot belong to an open transaction. `CALL system.expire_snapshots` drops such branches before it expires snapshots, and the maintenance scheduler in `active` mode drops them on the tables it maintains. `remove_orphan_files` then deletes the files they staged. `ALTER TABLE ... DROP BRANCH` removes a branch by hand, including one that never got past its first write. Dropping the branch of a transaction that is still open loses no data: the transaction fails with `TRANSACTION_CONFLICT`.

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
| Multi-statement transactions | yes (DML; one catalog) | no | no | yes |

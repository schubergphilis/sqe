# SHOW and EXPLAIN

Metadata queries (catalog / schema / table listing) and plan inspection (`EXPLAIN`, `EXPLAIN ANALYZE`, `EXPLAIN FULL`). Most are routed through the coordinator; `EXPLAIN FULL` is SQE-specific.

Source: `crates/sqe-sql/src/classifier.rs` (statement routing), `crates/sqe-coordinator/src/query_handler.rs` (handlers).

## SHOW statements

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `SHOW CATALOGS` | `sqe-sql/classifier.rs:154` | Lists every catalog the session can see. Honours auth: catalogs the user has no `SELECT` on are filtered. | yes | - | yes | yes |
| `SHOW SCHEMAS [IN cat]` | `sqlparser-rs` + `sqe-coordinator` | List namespaces. Filters by catalog if `IN` supplied. | yes | yes | yes | yes |
| `SHOW TABLES [IN cat.ns]` | `sqlparser-rs` + `sqe-coordinator` | List tables. | yes | yes | yes | yes |
| `SHOW VIEWS [IN cat.ns]` | `sqlparser-rs` + `sqe-coordinator` | List views. | yes | yes | yes | partial |
| `SHOW COLUMNS FROM cat.ns.t` | `sqe-coordinator/query_handler.rs:1858` | Trino syntax. Rewrites to `information_schema.columns` query. | yes | yes | yes | yes |
| `SHOW CREATE TABLE cat.ns.t` | `sqe-sql/classifier.rs` | Reconstruct the CREATE statement from current metadata. | yes | yes | yes | yes |
| `SHOW STATS FOR cat.ns.t` | `sqe-sql/classifier.rs:166` | Per-column NDV, null fraction, min, max. From Iceberg manifest stats. | yes | - | partial | yes |
| `DESCRIBE cat.ns.t` | `datafusion-builtin` | Three-column projection: `column_name`, `data_type`, `is_nullable`. | yes | yes | yes | yes |
| `SHOW GRANTS ON ...` | `sqe-sql/classifier.rs:186` | See [GRANT and REVOKE](./grant-revoke.md). | partial | yes | partial | - |
| `SHOW EFFECTIVE GRANTS FOR USER "x"` | `sqe-sql/classifier.rs:174` | SQE-specific. See [GRANT and REVOKE](./grant-revoke.md). | - | - | - | - |

```text
sqe> SHOW CATALOGS;
+---------------+
| catalog_name  |
+---------------+
| default       |
| analytics     |
| iceberg_main  |
+---------------+

sqe> SHOW TABLES IN analytics;
+--------------+--------------+--------------+
| table_catalog | table_schema | table_name  |
+--------------+--------------+--------------+
| analytics    | public       | events       |
| analytics    | public       | users        |
| analytics    | staging      | tmp_dedup    |
+--------------+--------------+--------------+
```

## DESCRIBE vs SHOW COLUMNS

Both work, slightly different shapes:

```sql
DESCRIBE analytics.events;
-- column_name | data_type | is_nullable

SHOW COLUMNS FROM analytics.events;
-- column_name | data_type | is_nullable | extra
```

`DESCRIBE` is DataFusion-native (3 columns). `SHOW COLUMNS` is Trino syntax, rewritten by SQE to query `information_schema.columns` directly so external dbt models that expect 4 columns work unmodified.

## SHOW STATS

Per-column statistics from manifest aggregates. Unlike `DESCRIBE`, this returns one row per column with summary numbers:

```text
sqe> SHOW STATS FOR analytics.events;
+--------------+--------------+--------------+----------------+--------+--------+
| column_name  | data_size    | distinct     | null_fraction  | min    | max    |
+--------------+--------------+--------------+----------------+--------+--------+
| id           | 96000000     | 12000000     | 0.0            | 1      | 12000000 |
| user_id      | 96000000     | 8473210      | 0.0            | 1      | 9999    |
| amount       | 144000000    | 9921458      | 0.001          | -50.00 | 12500.00 |
| occurred_at  | 96000000     | 11973247     | 0.0            | 2024-..| 2026-...|
+--------------+--------------+--------------+----------------+--------+--------+
```

`distinct` and bounds are upper bounds from manifest stats, not exact. For exact counts use `count(distinct col)` or `.summarize`. The output drives planner cost estimates.

## EXPLAIN

| Statement | Origin | Notes |
|---|---|---|
| `EXPLAIN SELECT ...` | `datafusion-builtin` | Logical and physical plans, no execution. |
| `EXPLAIN ANALYZE SELECT ...` | `datafusion-builtin` | Run the query; show physical plan with per-operator metrics. |
| `EXPLAIN FULL SELECT ...` | `sqe-sql/classifier.rs:159` | SQE-specific. Logical plan + physical plan + Iceberg scan plan (manifest counts, file counts, partition pruning, residual filter), no execution. |

`EXPLAIN` is the cheapest:

```sql
EXPLAIN SELECT user_id, count(*) FROM events
WHERE occurred_at >= DATE '2026-05-01' GROUP BY user_id;
```

```text
+---------------+--------------------------------------------------------------+
| plan_type     | plan                                                         |
+---------------+--------------------------------------------------------------+
| logical_plan  | Projection: user_id, count(*)                                |
|               |   Aggregate: groupBy=[user_id], aggr=[count(*)]              |
|               |     Filter: occurred_at >= Date32("2026-05-01")              |
|               |       TableScan: events                                      |
| physical_plan | ProjectionExec ...                                           |
|               |   AggregateExec ...                                          |
|               |     CoalesceBatchesExec ...                                  |
|               |       FilterExec ...                                         |
|               |         IcebergScanExec(events): files=12, bytes=180MB       |
+---------------+--------------------------------------------------------------+
```

`EXPLAIN ANALYZE` runs the query and overlays per-operator counters:

```text
| physical_plan | ProjectionExec, metrics=[output_rows=4823, elapsed=12ms]
|               |   AggregateExec, metrics=[output_rows=4823, elapsed=42ms]
|               |     IcebergScanExec, metrics=[files=12, files_pruned=0, bytes=180MB, elapsed=89ms]
```

`EXPLAIN FULL` shows the iceberg planning detail without executing:

```text
| iceberg_plan  | files_total=120, files_after_partition_prune=12,             |
|               | files_after_min_max_prune=12, residual_filter=true            |
|               | bytes_planned=180MB, partition_columns=[day(occurred_at)]     |
```

## Comparison

| Statement | SQE | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|
| `EXPLAIN` | yes | yes | yes (`EXPLAIN`) | yes | yes |
| `EXPLAIN ANALYZE` | yes | yes (`EXPLAIN ANALYZE`) | partial (query profile) | yes | yes |
| `EXPLAIN FULL` (planning detail w/o exec) | yes (SQE-specific) | partial | - | partial | - |
| `SHOW STATS` | yes | yes | partial (`information_schema`) | partial | yes |

## Information schema (DataFusion-native)

Always available; standard SQL surface.

| Table | Notes |
|---|---|
| `information_schema.schemata` | Schemas in every catalog. |
| `information_schema.tables` | Tables in every catalog. |
| `information_schema.columns` | Per-column metadata. |
| `information_schema.views` | Views. |
| `information_schema.df_settings` | DataFusion session config. |

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'analytics' AND table_type = 'BASE TABLE';
```

The `dotcommands` `.tables`, `.schema`, `.catalogs` are convenience wrappers around these.

## Iceberg metadata

For Iceberg-specific metadata (snapshots, manifests, files, partitions, refs, history), see [Table-valued functions](./table-functions.md). Both SQE TVF syntax and Trino `t$snapshots` syntax are accepted.

# DDL

Data Definition Language: schemas, tables, views, columns, partitions, branches, tags. Most statements parse via `sqlparser-rs`; SQE adds branch / tag / partition-evolution syntax that `sqlparser-rs` does not natively understand.

Source: `crates/sqe-sql/src/classifier.rs`, `crates/sqe-sql/src/ddl.rs`, `crates/sqe-sql/src/partition.rs`, `crates/sqe-sql/src/partition_evolution.rs`. Coordinator handlers in `crates/sqe-coordinator/src/catalog_ops.rs`.

## Schema

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `CREATE SCHEMA [IF NOT EXISTS] cat.ns` | `sqlparser-rs` + `sqe-coordinator` | Creates a namespace in the catalog. | yes | yes | yes | yes |
| `CREATE SCHEMA [IF NOT EXISTS] cat.ns LOCATION 's3://...'` | `sqlparser-rs` + `sqe-coordinator` | Override default location. Only on catalogs that accept location at namespace level (Polaris, S3 Tables). | yes | yes | yes | - |
| `DROP SCHEMA [IF EXISTS] cat.ns [CASCADE\|RESTRICT]` | `sqlparser-rs` + `sqe-coordinator` | `CASCADE` drops contained tables. | yes | yes | yes | yes |
| `ALTER SCHEMA cat.ns RENAME TO new_name` | `sqlparser-rs` + `sqe-coordinator` | Catalog must support namespace rename. | partial | yes | yes | yes |

```sql
CREATE SCHEMA IF NOT EXISTS analytics.staging;
CREATE SCHEMA marketing LOCATION 's3://my-warehouse/marketing/';
DROP SCHEMA staging CASCADE;
```

## Table creation

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `CREATE TABLE t (col TYPE [DEFAULT expr], ...)` | `sqlparser-rs` + `sqe-sql` + `sqe-coordinator` | Iceberg V3 column defaults supported. | yes | yes | yes | yes |
| `CREATE TABLE t (...) PARTITIONED BY (transform(col), ...)` | `sqe-sql/partition.rs` | Partition transforms: `bucket(N, col)`, `truncate(N, col)`, `year(col)`, `month(col)`, `day(col)`, `hour(col)`, identity (just `col`). | partial | - | yes | - |
| `CREATE TABLE t AS SELECT ...` (CTAS) | `sqlparser-rs` + `sqe-coordinator` | Inferred schema; partitioning via `WITH (partitioning = ARRAY['day(ts)'])`. | yes | yes | yes | yes |
| `CREATE OR REPLACE TABLE t AS SELECT ...` | `sqlparser-rs` + `sqe-coordinator` | Atomic replace. New snapshot replaces the table; old data files retained until `expire_snapshots`. | yes | yes | partial | yes |
| `CREATE TABLE [IF NOT EXISTS] t LIKE other_table` | `sqlparser-rs` + `sqe-coordinator` | Copy schema only, no data. | yes | yes | yes | yes |

```sql
CREATE TABLE analytics.events (
    id          BIGINT,
    user_id     BIGINT,
    event_type  VARCHAR,
    occurred_at TIMESTAMP(6),
    payload     JSON,
    region      VARCHAR DEFAULT 'unknown'
)
PARTITIONED BY (day(occurred_at), bucket(16, user_id));

CREATE TABLE analytics.daily_events AS
SELECT day(occurred_at) AS d, count(*) AS n
FROM analytics.events GROUP BY 1;
```

## Schema evolution

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `ALTER TABLE t ADD COLUMN c TYPE [DEFAULT expr]` | `sqlparser-rs` + `sqe-coordinator` | New column. Existing rows get the default (V3) or NULL (V2). | yes | yes | yes | yes |
| `ALTER TABLE t DROP COLUMN [IF EXISTS] c` | `sqlparser-rs` + `sqe-coordinator` | Logical drop. Field id retained in old data files. | yes | yes | yes | yes |
| `ALTER TABLE t RENAME COLUMN old TO new` | `sqlparser-rs` + `sqe-coordinator` | Iceberg field id stays the same; only the name changes. | yes | yes | yes | yes |
| `ALTER TABLE t ALTER COLUMN c SET NOT NULL` | `sqlparser-rs` + `sqe-coordinator` | Tighten nullability. Fails if existing rows have NULL. | yes | yes | yes | yes |
| `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` | `sqlparser-rs` + `sqe-coordinator` | Loosen nullability. | yes | yes | yes | yes |
| `ALTER TABLE t ALTER COLUMN c SET DEFAULT expr` | `sqlparser-rs` + `sqe-coordinator` | Iceberg V3 column default. | partial | yes | yes | yes |
| `ALTER TABLE t ALTER COLUMN c TYPE new_type` | `sqlparser-rs` + `sqe-coordinator` | Type promotion only (e.g. INT -> BIGINT). Lossy changes rejected. | partial | partial | partial | partial |
| `ALTER TABLE t RENAME TO new_t` | `sqlparser-rs` + `sqe-coordinator` | Catalog rename. Different catalog support varies. | yes | yes | yes | yes |
| `ALTER TABLE t SET TBLPROPERTIES (...)` | `sqlparser-rs` + `sqe-coordinator` | Set Iceberg properties (e.g. `write.delete.mode`). | yes | yes | yes | - |
| `COMMENT ON TABLE t IS 'description'` | `sqlparser-rs` + `sqe-coordinator` | Stored in Iceberg properties. | yes | yes | yes | yes |
| `COMMENT ON COLUMN t.c IS 'description'` | `sqlparser-rs` + `sqe-coordinator` | Stored on the column metadata. | yes | yes | yes | yes |

```sql
ALTER TABLE analytics.events ADD COLUMN device VARCHAR DEFAULT 'unknown';
ALTER TABLE analytics.events DROP COLUMN IF EXISTS deprecated_field;
ALTER TABLE analytics.events RENAME COLUMN payload TO body;
ALTER TABLE analytics.events ALTER COLUMN region SET NOT NULL;
ALTER TABLE analytics.events SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.bloom-filter-columns' = 'user_id,event_id'
);
```

## Partition evolution (SQE / Iceberg-specific)

Iceberg lets you change partition spec without rewriting data. SQE parses these in `crates/sqe-sql/src/partition_evolution.rs` because `sqlparser-rs` only knows Hive-style `PARTITION (col = val)`.

| Statement | Notes | Trino | Snowflake | Spark SQL |
|---|---|---|---|---|
| `ALTER TABLE t ADD PARTITION FIELD transform(col)` | Add a new partition field. Existing data stays in the old spec. | partial | - | yes |
| `ALTER TABLE t ADD PARTITION FIELD transform(col) AS alias` | Same with explicit name for the partition column. | - | - | yes |
| `ALTER TABLE t DROP PARTITION FIELD transform(col)` | Remove a partition field from the current spec. | partial | - | yes |
| `ALTER TABLE t REPLACE PARTITION FIELD old_transform(col) WITH new_transform(col)` | Replace one transform with another. | - | - | yes |

```sql
-- Originally partitioned by day(ts); switch to hour() for finer granularity.
ALTER TABLE events REPLACE PARTITION FIELD day(occurred_at) WITH hour(occurred_at);

-- Add a bucketing field on top of existing daily partitions.
ALTER TABLE events ADD PARTITION FIELD bucket(64, user_id);
```

## Branches and tags (SQE / Iceberg-specific)

Iceberg branches are named pointers to a snapshot, like git branches. Tags are immutable named pointers. SQE parses these in `crates/sqe-sql/src/ddl.rs`.

| Statement | Notes |
|---|---|
| `ALTER TABLE t CREATE BRANCH name` | New branch from current snapshot. |
| `ALTER TABLE t CREATE BRANCH name AS OF VERSION snapshot_id` | New branch from a specific snapshot. |
| `ALTER TABLE t CREATE BRANCH name WITH RETENTION (max_ref_age_ms = N)` | Auto-expire branch after N ms of inactivity. |
| `ALTER TABLE t CREATE [OR REPLACE] TAG name` | New tag pointing at current snapshot. `OR REPLACE` is allowed because tags are not strictly immutable in iceberg-rust. |
| `ALTER TABLE t CREATE TAG name AS OF VERSION snapshot_id` | Tag a specific snapshot. |
| `ALTER TABLE t DROP BRANCH [IF EXISTS] name` | Remove a branch. |
| `ALTER TABLE t DROP TAG [IF EXISTS] name` | Remove a tag. |

```sql
-- Branch a snapshot for development work
ALTER TABLE analytics.events CREATE BRANCH dev_2026_05;

-- Pin a known-good snapshot as a tag
ALTER TABLE analytics.events CREATE TAG release_2026_q2 AS OF VERSION 8472810294831234567;

-- Query the branch
SELECT * FROM analytics.events FOR VERSION AS OF 'dev_2026_05';
```

## Views

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `CREATE [OR REPLACE] VIEW v AS SELECT ...` | `sqlparser-rs` + `sqe-coordinator` | Standard SQL view. Iceberg views format-version 1. | yes | yes | yes | yes |
| `CREATE [OR REPLACE] VIEW v (col1, col2) AS SELECT ...` | `sqlparser-rs` + `sqe-coordinator` | Explicit column list. | yes | yes | yes | yes |
| `DROP VIEW [IF EXISTS] v` | `sqlparser-rs` + `sqe-coordinator` | Remove a view. | yes | yes | yes | yes |

```sql
CREATE OR REPLACE VIEW analytics.recent_events AS
SELECT * FROM analytics.events
WHERE occurred_at >= now() - INTERVAL '7' DAY;

DROP VIEW IF EXISTS analytics.recent_events;
```

## Drop

| Statement | Origin | Notes |
|---|---|---|
| `DROP TABLE [IF EXISTS] t [PURGE]` | `sqlparser-rs` + `sqe-coordinator` | `PURGE` deletes data files immediately; default keeps the metadata so `system.remove_orphan_files` can clean later. |
| `DROP VIEW [IF EXISTS] v` | `sqlparser-rs` + `sqe-coordinator` | Standard. |
| `DROP SCHEMA [IF EXISTS] s [CASCADE\|RESTRICT]` | `sqlparser-rs` + `sqe-coordinator` | `CASCADE` drops contained tables. |

## Iceberg V3 type system

These types only exist in format-version 3. Adding one to a CREATE TABLE auto-bumps the table to V3.

| Type | Notes |
|---|---|
| `TIMESTAMP_NS`, `TIMESTAMP_NS WITH TIME ZONE` | Nanosecond precision timestamps. Arrow `Timestamp(Nanosecond, ...)`. |
| `GEOMETRY`, `GEOGRAPHY` | Stub types in V3; SQE accepts them in CREATE but does not yet provide spatial functions. |
| Default values via `DEFAULT expr` | Existing rows in older snapshots inherit the default at read time. |

## What CREATE / ALTER does NOT cover

- **`CREATE INDEX`**. Iceberg has no equivalent. Bloom filter columns and partition fields cover the same ground; configure via `SET TBLPROPERTIES` and `ADD PARTITION FIELD`.
- **`CREATE FUNCTION` / `CREATE PROCEDURE`**. UDFs are Rust-side. SQL-defined functions and procedures are not supported.
- **`CREATE SEQUENCE`**. no auto-increment / sequence support today. Use `row_number()` over a deterministic ordering for synthetic keys.
- **`CREATE TYPE`**. no user-defined types. Use `STRUCT<...>` or `MAP<...>`.

These are tracked but not on the immediate roadmap.

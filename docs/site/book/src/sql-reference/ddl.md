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
| `CREATE OR REPLACE TABLE t AS SELECT ...` | `sqlparser-rs` + `sqe-coordinator` | Atomic replace. In-place snapshot replace when schema and partitioning match (table keeps UUID, grants, history; properties not re-applied). Staged table + swap otherwise. Self-referencing replace safe (reads old snapshot before swap). | yes | yes | partial | yes |
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

A column `DEFAULT` takes a literal and makes the table format-version 3. An `INSERT` with a column list and a `MERGE ... WHEN NOT MATCHED THEN INSERT` write the default into every column they omit; an explicit `NULL` is written as NULL. Defaults on `BINARY`, `FIXED` and `UUID` columns are refused on INSERT; name the column and give its value.

## Schema evolution

| Statement | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `ALTER TABLE t ADD COLUMN c TYPE [DEFAULT expr]` | `sqlparser-rs` + `sqe-coordinator` | New column. Rows written before the ALTER read the default, and a later INSERT or MERGE INSERT that omits the column writes it (V3). | yes | yes | yes | yes |
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
| `CREATE VIEW v COMMENT '<text>' AS ...` | `sqe-sql` `view_compat` + `sqe-coordinator` | Trino's comment spelling, without `=`. Stored as the view's `comment` property. | yes | no | no | no |
| `CREATE VIEW v SECURITY {DEFINER \| INVOKER} AS ...` | `sqe-sql` `view_compat` + `sqe-coordinator` | Accepted and recorded. See below: `DEFINER` is not enforced. | yes | no | no | no |

```sql
CREATE OR REPLACE VIEW analytics.recent_events AS
SELECT * FROM analytics.events
WHERE occurred_at >= now() - INTERVAL '7' DAY;

DROP VIEW IF EXISTS analytics.recent_events;
```

### `SECURITY DEFINER` is recorded, not enforced

Trino's `SECURITY DEFINER` runs a view with its creator's privileges, so a reader
needs access to the view but not to its base tables. `SECURITY INVOKER` checks the
querying user against the base tables instead.

**SQE always behaves as `INVOKER`**, and that is architectural rather than
unfinished. Every query runs as the authenticated user via bearer-token
passthrough, with no service account anywhere in the engine, so there is no
credential to run a view as its definer with. Honouring `DEFINER` would mean
storing one.

The clause is accepted because rejecting it blocks every dbt model that uses the
default `view` materialization, and because the direction of the difference is
safe: `INVOKER` is stricter than `DEFINER` asks for, so a reader who was meant to
be shielded from base-table grants is denied rather than allowed. It fails closed.

What SQE does with it:

| | |
|---|---|
| Recorded as | view properties `sqe.view-security` and, for `definer`, `sqe.view-definer` |
| Enforcement | `INVOKER` semantics: the base table is authorized as the querying user |
| On `DEFINER` | one warning at creation, naming the reader-denial consequence |

The properties live in the Iceberg view metadata, so the intent travels with the
object and a later implementation, or another engine, can honour it.

## Drop

| Statement | Origin | Notes |
|---|---|---|
| `DROP TABLE [IF EXISTS] t [PURGE]` | `sqlparser-rs` + `sqe-coordinator` | Removes the table from the catalog and keeps its data files. `PURGE` also deletes everything under the table's location once the catalog drop succeeds; it runs `CALL system.drop_table(table => 't', purge => true)`, so it needs an admin role (`auth.admin_roles`) and is refused, with nothing dropped, when another table shares the location. Hive tables refuse `PURGE`. |
| `DROP VIEW [IF EXISTS] v` | `sqlparser-rs` + `sqe-coordinator` | Standard. |
| `DROP SCHEMA [IF EXISTS] s [CASCADE\|RESTRICT]` | `sqlparser-rs` + `sqe-coordinator` | `CASCADE` drops contained tables. |

## Iceberg V3 type system

These types only exist in format-version 3, the default for new tables. Adding one to a CREATE TABLE makes the table V3 even when the configured default is 2.

| Type | Notes |
|---|---|
| `TIMESTAMP_NS`, `TIMESTAMP_NS WITH TIME ZONE` | Nanosecond precision timestamps. Arrow `Timestamp(Nanosecond, ...)`. |
| `VARIANT` | Semi-structured value (object, array, string, number, boolean, null) in the Iceberg variant binary encoding. See [VARIANT](#variant) below. |
| `GEOMETRY`, `GEOGRAPHY` | Stub types in V3; SQE accepts them in CREATE but does not yet provide spatial functions. |
| Default values via `DEFAULT expr` | Existing rows in older snapshots inherit the default at read time. |

### VARIANT

```sql
CREATE TABLE events (id BIGINT, payload VARIANT);
INSERT INTO events VALUES (1, '{"user": "a", "tags": [1, 2]}'), (2, '"plain text"'), (3, NULL);
```

- **Writes parse text as JSON.** A string written to a `VARIANT` column is parsed as a JSON document: `'{"a": 1}'` stores an object and `'42'` stores a number. To store a JSON string, quote it: `'"text"'`. Text that is not valid JSON fails the statement; it is never stored as NULL. Other column types (numbers, booleans, structs, lists) are stored as the variant of their value.
- **Reads return JSON text.** Clients have no variant type. The Trino protocol reports the column as `json` and sends each value as JSON text; Flight SQL / ADBC / JDBC return it as a UTF-8 string. `DESCRIBE` and `information_schema.columns` report `variant`, and `SHOW CREATE TABLE` prints `VARIANT`.
- **The table becomes format-version 3.** `CREATE TABLE` and CTAS with a variant column create a V3 table. `ALTER TABLE ... ADD COLUMN c VARIANT` is refused on a V2 table. V3 tables can be read by Spark 4.0 with Iceberg 1.10 or later, and by Trino 481 or later; older readers cannot open them.
- **`JSON` is a variant on a V3 table.** A `JSON` column, also inside a `STRUCT` or `ROW`, is stored as `VARIANT` when the table is format-version 3 and as a string otherwise. New tables are V3 by default (`catalog.default_table_format_version = 3`), so a plain `CREATE TABLE t (doc JSON)` stores a variant. A table is V2 when it asks for it with `WITH ('format-version' = '2')`, the Trino form `WITH (format_version = 2)` or `TBLPROPERTIES ('format-version' = '2')`, or when the configured default is 2; its `JSON` columns are strings, which readers without V3 support (Trino < 481, Spark < 4.0, Iceberg < 1.10) can open. `ALTER TABLE ... ADD COLUMN c JSON` adds a variant to a V3 table and a string to a V2 one. Asking for format-version 1 or 2 on a table with a V3-only column is an error.
- **JSON functions read a variant.** `json_extract`, `json_extract_scalar`, the `json_get_*` functions and the other `json*` functions take a variant as its JSON text, so the same query works on a V2 `JSON` string and a V3 variant.
- **Existing JSON columns are not converted.** Iceberg does not allow evolving a `string` column to `variant`, so a column that holds JSON text stays a string. To convert one, copy it into a table declared with a `VARIANT` column, where the write parses each value: `CREATE TABLE t2 (id BIGINT, doc VARIANT); INSERT INTO t2 SELECT id, doc FROM t1;`.
- **Not yet:** variant path functions (`variant_get`), reading shredded variant files written by other engines, and nested variants (inside a `ROW` or `ARRAY`) on Flight SQL, which arrive as the raw storage struct.

## What CREATE / ALTER does NOT cover

- **`CREATE INDEX`**. Iceberg has no equivalent. Bloom filter columns and partition fields cover the same ground; configure via `SET TBLPROPERTIES` and `ADD PARTITION FIELD`.
- **`CREATE FUNCTION` / `CREATE PROCEDURE`**. UDFs are Rust-side. SQL-defined functions and procedures are not supported.
- **`CREATE SEQUENCE`**. no auto-increment / sequence support today. Use `row_number()` over a deterministic ordering for synthetic keys.
- **`CREATE TYPE`**. no user-defined types. Use `STRUCT<...>` or `MAP<...>`.

These are tracked but not on the immediate roadmap.

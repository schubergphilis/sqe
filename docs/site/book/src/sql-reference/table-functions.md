# Table-valued functions

A TVF returns a table you can use in a `FROM` clause. SQE ships two families:

1. **File format readers**: `read_parquet`, `read_csv`, `read_json`, `read_delta`. Implemented in `crates/sqe-catalog/`.
2. **Iceberg metadata readers**: `table_snapshots`, `table_history`, `table_files`, `table_partitions`, `table_manifests`, `table_refs`. Implemented in `crates/sqe-catalog/src/iceberg_metadata_tvf.rs`.

DataFusion contributes the generators (`generate_series`, `unnest`) and the URL-table auto-detect path (`SELECT * FROM 'file.parquet'`).

## File format TVFs

Detailed per-function docs: [File-format TVFs](../features/file-format-tvfs.md), [read_parquet TVF](../features/read-parquet.md). Quick reference here.

| TVF | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `read_parquet(path, ...)` | `sqe-catalog` | Parquet on local FS / S3 / HTTPS / `hf://`. Inline auth args. `read_parquet.rs` | Hive table only | `infer_schema`+stage | `parquet` source | `read_parquet` |
| `read_csv(path, ...)` | `sqe-catalog` | DuckDB-style aliases (`sep`, `delim`, `header`, `nullstr`, `compress`). Smart defaults from extension. `read_csv.rs` | - | `infer_schema`+stage | `csv` source | `read_csv` |
| `read_json(path, ...)` | `sqe-catalog` | NDJSON (one document per line). `read_json.rs` | - | - | `json` source | `read_json` |
| `read_delta(path, ...)` | `sqe-catalog` | Read-only Delta Lake reader. Time travel via `version => N` or `timestamp => 'RFC3339'`. `read_delta.rs` | via connector | - | native | via extension |
| `SELECT * FROM 'file.ext'` | `datafusion-builtin` | Quoted-string auto-detect. Dispatches by extension to one of the readers above. Requires `enable_url_table()`. | - | - | - | yes |

### Common arguments

All four file readers accept the same path scheme set: local, S3, HTTPS, `hf://`. Arguments are positional path + named keyword arguments:

```sql
SELECT * FROM read_parquet(
    's3://bucket/key.parquet',
    access_key => 'AKIA...',
    secret_key => '...',
    endpoint => 'http://localhost:9000',
    region => 'us-east-1'
);
```

The full keyword list per reader lives in [File-format TVFs](../features/file-format-tvfs.md). The same shape works for `read_csv`, `read_json`, `read_delta`.

### Path schemes

| Scheme | Auth | Example |
|---|---|---|
| Local | filesystem perms | `/data/sales.parquet` |
| `s3://` | inline args, `[storage]` block, or AWS provider chain (V10) | `s3://bucket/key.parquet` |
| `https://` | session bearer for HF, otherwise public | `https://example.com/data.csv` |
| `hf://datasets/<org>/<name>/...` | `HF_TOKEN` env var, optional `?revision=` | `hf://datasets/squad/plain_text/train.parquet` |
| `hf://...@v1.0/...` | revision inline (V12.1) | `hf://datasets/foo/bar@v1.0/train.parquet` |
| `hf://...@~parquet/...` | auto-generated parquet view (V12.1) | `hf://datasets/foo/bar@~parquet/default/train/0.parquet` |

## Iceberg metadata TVFs

Six TVFs that expose Iceberg internal state without leaving SQL. Useful for observability, audit, planning.

| TVF | Origin | Returns | Trino | Snowflake | Spark SQL |
|---|---|---|---|---|---|
| `table_snapshots(ns, table)` | `sqe-catalog` | One row per snapshot, in Trino's `$snapshots` column shape. Columns: `committed_at`, `snapshot_id`, `parent_id`, `operation`, `manifest_list`, `summary`. `iceberg_metadata_tvf.rs:93` | `t$snapshots` | - | `t.snapshots` |
| `table_history(ns, table)` | `sqe-catalog` | Linear snapshot history. Columns: `made_current_at`, `snapshot_id`, `parent_id`, `is_current_ancestor`. `iceberg_metadata_tvf.rs:356` | `t$history` | - | `t.history` |
| `table_files(ns, table)` | `sqe-catalog` | One row per data file in the current snapshot. Columns: `content`, `file_path`, `partition`, `record_count`, `file_size_in_bytes`. `iceberg_metadata_tvf.rs:469` | `t$files` | - | `t.files` |
| `table_manifests(ns, table)` | `sqe-catalog` | One row per manifest in the current snapshot. `iceberg_metadata_tvf.rs:217` | `t$manifests` | - | `t.manifests` |
| `table_partitions(ns, table)` | `sqe-catalog` | Per-partition aggregate. `iceberg_metadata_tvf.rs:622` | `t$partitions` | - | `t.partitions` |
| `table_refs(ns, table)` | `sqe-catalog` | One row per branch / tag. Columns: `name`, `type`, `snapshot_id`, `max_ref_age_ms`. `iceberg_metadata_tvf.rs:768` | `t$refs` | - | `t.refs` |

Trino's `table$snapshots` syntax is also accepted; `crates/sqe-coordinator/src/query_handler.rs` rewrites it to the TVF call.

```sql
-- DuckDB / Trino-style $-syntax
SELECT * FROM analytics."events$snapshots";

-- Equivalent SQE TVF call
SELECT * FROM table_snapshots('analytics', 'events');
```

### Examples

What's the current snapshot's row count?

```sql
SELECT SUM(record_count) AS rows
FROM table_files('analytics', 'events');
```

When did each branch fork?

```sql
SELECT name, type, snapshot_id
FROM table_refs('analytics', 'events')
WHERE type = 'branch';
```

How big are recent snapshots in megabytes?

```sql
SELECT
    snapshot_id,
    summary['added-files-size'] AS added_bytes,
    summary['total-files-size'] AS total_bytes
FROM table_snapshots('analytics', 'events')
ORDER BY committed_at DESC
LIMIT 10;
```

The `summary` column is a `MAP<VARCHAR, VARCHAR>`; cast values numerically when needed.

## Generators (DataFusion built-ins)

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `generate_series(start, stop)` | `datafusion-builtin` | Integer sequence, inclusive both ends. | `sequence` | `sequence` | `sequence` | `generate_series` |
| `generate_series(start, stop, step)` | `datafusion-builtin` | With step. Negative step counts down. | `sequence` | - | - | `generate_series` |
| `range(start, stop)` / `range(start, stop, step)` | `datafusion-builtin` | Half-open: includes `start`, excludes `stop`. | - | - | - | `range` |
| `unnest(array)` | `datafusion-builtin` | Lateral expansion: one input row -> N output rows. | `unnest` | `flatten` | `explode` | `unnest` |

Examples:

```sql
SELECT * FROM generate_series(1, 5);
-- 1, 2, 3, 4, 5

SELECT day FROM generate_series(DATE '2026-05-01', DATE '2026-05-07') AS t(day);
-- 7 dates, May 1 through May 7

SELECT id, value FROM orders, UNNEST(items) AS t(value);
-- Lateral unnest: one row per (order, item)
```

## Quoted-string auto-detect

`SELECT * FROM '<path>'` works as a shortcut when the path's extension is recognised:

| Extension | Dispatches to |
|---|---|
| `.parquet` | `read_parquet` |
| `.csv`, `.tsv`, `.psv`, `.ssv` (with optional `.gz` / `.bz2` / `.xz` / `.zst`) | `read_csv` |
| `.json`, `.jsonl`, `.ndjson` (with optional codec suffix) | `read_json` |
| `.avro` | DataFusion's avro reader |

The mechanism is DataFusion's `enable_url_table()` SessionConfig, called at `crates/sqe-cli/src/embedded.rs:158`. Auto-detect works in cluster mode too.

```sql
-- All three are equivalent (assuming the file is a CSV)
SELECT * FROM read_csv('/data/sales.csv');
SELECT * FROM '/data/sales.csv';
SELECT * FROM 'hf://datasets/squad/plain_text/train.csv';
```

## When to register vs query directly

A `read_*` TVF call reads on every query. Two cases where registering as a table is better:

1. **Repeated queries**: register once via `CREATE TABLE foo AS SELECT * FROM read_parquet(...)` so subsequent queries skip the URL fetch.
2. **You need writes**: TVFs are read-only. Writes need a catalog-registered Iceberg table.

For ad-hoc analytics on a one-shot file, the TVF is faster: no schema decision, no commit, no metadata.

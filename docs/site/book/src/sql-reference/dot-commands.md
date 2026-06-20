# Dot-commands

Embedded-CLI shortcuts. Lines beginning with `.` bypass the SQL parser and run client-side; everything else is SQL. The convention matches `sqlite3` and the DuckDB shell.

Dot-commands work in the embedded CLI (`sqe-cli --embedded`) only. Cluster mode uses the same shortcuts indirectly through `dbt-sqe` or a Flight SQL client; the dot-commands themselves are a REPL feature.

Source: `crates/sqe-cli/src/dotcommands.rs`.

## Reference

| Command | Aliases | Argument | Origin | Action |
|---|---|---|---|---|
| `.help` | `.h`, `.?` | - | `sqe-cli` | Print the dot-command list. |
| `.exit` | `.quit`, `.q` | - | `sqe-cli` | Leave the REPL. End-of-input does the same. |
| `.tables [schema]` | - | optional schema name | `sqe-cli` | Query `information_schema.tables`. Filter by schema if given. |
| `.schema <table>` | `.describe`, `.d` | required table name | `sqe-cli` | Query `information_schema.columns`. Accepts 1-, 2-, or 3-part names. |
| `.summarize <table>` | `.summary` | required table name | `sqe-cli` | Per-column `count`, `distinct`, `null_count`, `min`, `max` via UNION ALL. SQE's V9 answer to DuckDB's `SUMMARIZE`. |
| `.catalogs` | `.databases` | - | `sqe-cli` | Query `information_schema.schemata`. |
| `.read <path>` | - | required file path | `sqe-cli` | Execute a SQL script file. Errors abort. |
| `.timer on\|off` | - | required `on` or `off` | `sqe-cli` | Toggle per-query elapsed-time output below each result. |
| `.format [table\|csv\|tsv\|json]` | - | optional format | `sqe-cli` | Show the current format with no argument; set with one. |

## Comparison to other shells

| Command | SQE | sqlite3 | DuckDB CLI | psql | Trino CLI |
|---|---|---|---|---|---|
| help | `.help` | `.help` | `.help` | `\?` | `help` |
| exit | `.exit`, `.quit` | `.exit`, `.quit` | `.exit` | `\q` | `quit` |
| list tables | `.tables` | `.tables` | `.tables` | `\dt` | `SHOW TABLES` |
| describe table | `.schema t` | `.schema t` | `.schema t` / `DESCRIBE t` | `\d t` | `DESCRIBE t` |
| summary stats | `.summarize t` | - | `SUMMARIZE t` | - | - |
| toggle timing | `.timer on` | `.timer on` | `.timer on` | `\timing` | - |
| run script | `.read f.sql` | `.read f.sql` | `.read f.sql` | `\i f.sql` | `--file=f.sql` |

## Examples

### Inspect a table you just created

```text
sqe> CREATE TABLE orders AS SELECT * FROM read_parquet('s3://bucket/orders.parquet');
sqe> .schema orders
+-------------+-----------------+-------------+
| column_name | data_type       | is_nullable |
+-------------+-----------------+-------------+
| id          | BigInt          | NO          |
| customer_id | BigInt          | YES         |
| amount      | Decimal(18, 2)  | YES         |
| created_at  | Timestamp(Microsecond, None) | YES |
+-------------+-----------------+-------------+
```

`.schema` accepts qualified names: `.schema iceberg.staging.orders` works the same way against a 3-part name.

### Summarize before deciding

```text
sqe> .summarize orders
+-------------+-------+----------+------------+--------+----------+
| column      | count | distinct | null_count | min    | max      |
+-------------+-------+----------+------------+--------+----------+
| id          | 12000 | 12000    | 0          | 1      | 12000    |
| customer_id | 12000 | 8473     | 0          | 1      | 9999     |
| amount      | 12000 | 9921     | 12         | -50.00 | 12500.00 |
| created_at  | 12000 | 11973    | 0          | 2024-... | 2026-...|
+-------------+-------+----------+------------+--------+----------+
```

A `count == distinct` column is a candidate primary key. A high `null_count` rules out a NOT NULL constraint. The min / max range hints at distribution skew.

### Time queries while iterating

```text
sqe> .timer on
sqe> SELECT count(*) FROM read_parquet('hf://datasets/squad/plain_text/train.parquet');
+----------+
| count(*) |
+----------+
| 87599    |
+----------+
1 row in set (1.412s)
```

### Pipe results to a file via `.format` + shell redirect

```bash
$ echo ".format csv
SELECT id, name FROM users" | sqe-cli --embedded --warehouse /data/wh > users.csv
```

## What dot-commands do not do

- **They do not run on the cluster.** Use a Flight SQL client (`pyarrow`, `dbt-sqe`) or call `information_schema` directly in SQL.
- **They do not support tab completion or up-arrow recall** of dot-command syntax. Tab completion exists for SQL keywords and table names but not for `.foo` arguments.
- **They are not pluggable.** Adding a new dot-command means a code change in `crates/sqe-cli/src/dotcommands.rs`.

## Adding new dot-commands

The pattern is small enough to read in one sitting. Each new command needs:

1. A new `DotCommand` enum variant in `dotcommands.rs`.
2. A match arm in `parse_dot_command()`.
3. A line added to `help_text()`.
4. Optional: a query builder helper if the command translates to SQL.
5. A handler in the REPL loop (`crates/sqe-cli/src/repl.rs`).

Two existing examples cover the spectrum: `.tables` (one-shot SQL builder), `.summarize` (multi-step: read schema, then build aggregate UNION ALL).

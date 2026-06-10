# Embedded: persistent local catalog (SQLite)

## Goal

`sqe-cli --embedded --warehouse <dir>` runs the engine in-process and attaches a
SQLite-backed Iceberg catalog at `<dir>`. Unlike the `embedded-files` quickstart
(which uses `--memory` and keeps nothing), `CREATE TABLE` and its data persist on
disk between CLI invocations: the catalog is a `sqe.db` SQLite file and the table
data lives next to it as Iceberg metadata and Parquet files.

This is the single-binary, local-first way to keep Iceberg tables on a laptop.
No server, no Polaris, no catalog service — just a directory on disk.

## Components

| Component | Role |
|---|---|
| `sqe-cli` | Embedded engine binary (in-process; no separate server) |
| `./warehouse/` | SQLite catalog (`sqe.db`) + Iceberg metadata/data on local disk |
| `queries-init.sql` | Multi-statement script: create schema, create table, insert rows |
| Docker (optional) | Container wrapper when no local `sqe-cli` build is available |

## Configuration

### CLI

```bash
# Process 1: create schema + table + insert (writes to the SQLite catalog)
docker run --rm --entrypoint sqe-cli \
  -v "$PWD/warehouse":/data/wh \
  -v "$PWD/queries-init.sql":/init.sql:ro \
  sqe-quickstart:latest \
  --embedded --warehouse /data/wh --file /init.sql --stop-on-error

# Process 2: a separate invocation reopens the same warehouse and reads
docker run --rm --entrypoint sqe-cli \
  -v "$PWD/warehouse":/data/wh \
  sqe-quickstart:latest \
  --embedded --warehouse /data/wh \
  -e "SELECT kind, COUNT(*) AS n, ROUND(SUM(amount),2) AS total
      FROM iceberg.demo.events GROUP BY kind ORDER BY total DESC"
```

`--warehouse <dir>` names the catalog `iceberg`, so tables are referenced as
`iceberg.<namespace>.<table>`. Use `--file script.sql` for multi-statement
scripts; `sqe-cli` accepts a single `-e` per invocation. `./run.sh --clean`
resets the warehouse directory.

## The test

`run.sh` runs two separate `sqe-cli` processes against the same `./warehouse`
directory: the first process executes `queries-init.sql` (create schema, create
table, insert 4 rows); the second opens the same warehouse in a fresh invocation
and reads the rows back with a GROUP BY query. The on-disk presence of `sqe.db`
and the `iceberg/` data directory is verified after the read. Last validated
2026-06-06.

## Output

```
## Process 1 -- create schema + table + insert (writes to the SQLite catalog)
$ sqe-cli --embedded --warehouse ./warehouse --file queries-init.sql
sqe-cli 0.31.4 embedded engine (1GB memory pool, warehouse: /data/wh)
(0 rows)
(0 rows)
+-------+
| count |
+-------+
| 4     |
+-------+
(1 rows)

## Process 2 -- a *separate* invocation reopens the same warehouse and reads
$ sqe-cli --embedded --warehouse ./warehouse -e "SELECT ... FROM iceberg.demo.events"
sqe-cli 0.31.4 embedded engine (1GB memory pool, warehouse: /data/wh)
(2 rows)
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+

## On disk: the SQLite catalog (sqe.db) + Iceberg metadata/data
iceberg
sqe.db
```

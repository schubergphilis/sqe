# Embedded: query local and remote files

## Goal

SQE's engine runs in-process. `sqe-cli --embedded --memory` starts DataFusion,
the Iceberg writers, and the file-reader table-valued functions in a single
binary: no coordinator, no workers, no network listeners, no catalog.

`read_csv`, `read_json`, and `read_parquet` read files directly, whether they
live on local disk or behind an HTTPS URL. Nothing persists between runs — the
`--memory` flag makes the session ephemeral. This is the fastest way to query
data with SQE SQL: no stack to bring up, no catalog to configure.

## Components

| Component | Role |
|---|---|
| `sqe-cli` | Embedded engine binary (in-process; no separate server) |
| `data/` | Three sample files (CSV, JSON, Parquet) — five rows each |
| Docker (optional) | Container wrapper when no local `sqe-cli` build is available |

## Configuration

### CLI

```bash
# Wrapper: run the embedded CLI in the SQE image with ./data mounted read-only
sqe() { docker run --rm --entrypoint sqe-cli -v "$PWD/data":/data:ro \
          sqe-quickstart:latest --embedded --memory "$@"; }

# Local CSV — aggregate by kind
sqe -e "SELECT kind, COUNT(*) AS n, ROUND(SUM(amount),2) AS total
        FROM read_csv('/data/events.csv') GROUP BY kind ORDER BY total DESC"

# Local JSON — count + sum
sqe -e "SELECT COUNT(*) AS rows, ROUND(SUM(amount),2) AS total
        FROM read_json('/data/events.json')"

# Local Parquet — sum by kind
sqe -e "SELECT kind, ROUND(SUM(amount),2) AS total
        FROM read_parquet('/data/events.parquet') GROUP BY kind ORDER BY kind"

# Join two files of different formats in one query
sqe -e "SELECT c.id, c.kind FROM read_csv('/data/events.csv') c
        JOIN read_parquet('/data/events.parquet') p ON c.id = p.id
        WHERE c.amount > 10 ORDER BY c.id"

# Remote file over HTTPS
sqe -e "SELECT COUNT(*) AS rows FROM read_parquet('https://example.com/data.parquet')"
```

`--memory` runs with no persistent catalog (nothing survives the process).
`--embedded` without `--memory` attaches a SQLite-backed Iceberg catalog at
`~/.sqe/warehouse` instead, so `CREATE TABLE` persists. See
`embedded-sqlite-catalog` for that mode.

## The test

`run.sh` exercises `read_csv`, `read_json`, and `read_parquet` on the local
sample files in `data/`, runs a cross-format JOIN between CSV and Parquet, and
queries a remote Parquet file over HTTPS. All output is captured to `OUTPUT.md`.
The local Parquet path mirrors the `test_read_parquet_local_file` integration
test. Last validated 2026-06-06.

## Output

```
## Local CSV (read_csv)
sqe-cli 0.31.4 embedded engine (1GB memory pool, ephemeral)
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
| refund   | 1 | -5.0  |
+----------+---+-------+
(3 rows)

## Join two files of different formats in one query
sqe-cli 0.31.4 embedded engine (1GB memory pool, ephemeral)
(2 rows)
+----+----------+
| id | kind     |
+----+----------+
| 2  | purchase |
| 4  | purchase |
+----+----------+

## Remote file over HTTPS (read_parquet on a URL)
sqe-cli 0.31.4 embedded engine (1GB memory pool, ephemeral)
+------+
| rows |
+------+
| 1000 |
+------+
(1 rows)
```

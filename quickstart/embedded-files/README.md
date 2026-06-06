---
slug: embedded-files
title: "Embedded: query local and remote files"
description: "Run SQE's engine in-process with sqe-cli --embedded and query CSV, JSON, and Parquet files directly with read_csv / read_json / read_parquet. No server, no catalog, local files or HTTPS URLs."
---

# Embedded: query local and remote files

SQE's engine runs in-process. `sqe-cli --embedded` starts DataFusion, the
Iceberg writers, and the file-reader TVFs in a single binary: no coordinator, no
workers, no network listeners, no catalog. The `read_csv`, `read_json`, and
`read_parquet` table-valued functions read files directly, whether they live on
local disk or behind an HTTPS URL.

This is the fastest way to poke at data with SQL. There is no stack to bring up,
so this quickstart has no `docker-compose.yml`: it just runs the CLI.

## What you get

Three sample files in [`data/`](./data/) (the same five rows in three formats)
and a `run.sh` that queries them, plus a public HTTPS Parquet file to show
remote reads.

## Prerequisites

- Docker, and the SQE image (`sqe-quickstart:latest`). Build it once from any of
  the server quickstarts: `(cd ../polaris-keycloak-client-id && docker compose build sqe)`.
- Or, if you have the CLI built locally (`cargo install --path crates/sqe-cli`),
  drop the `docker run` wrapper and use `sqe-cli` directly.

## Run it

```bash
cd quickstart/embedded-files
./run.sh
```

`run.sh` queries the local files and a remote one, capturing the result to
[`OUTPUT.md`](./OUTPUT.md). Nothing persists: the `--memory` engine is ephemeral.

## The commands

The wrapper runs the embedded CLI in the SQE image with `./data` mounted:

```bash
sqe() { docker run --rm --entrypoint sqe-cli -v "$PWD/data":/data:ro \
          sqe-quickstart:latest --embedded --memory "$@"; }

# Local CSV
sqe -e "SELECT kind, COUNT(*) n, ROUND(SUM(amount),2) total
        FROM read_csv('/data/events.csv') GROUP BY kind ORDER BY total DESC"

# Local Parquet
sqe -e "SELECT * FROM read_parquet('/data/events.parquet') WHERE amount > 10"

# Join two formats in one query
sqe -e "SELECT c.id, c.kind FROM read_csv('/data/events.csv') c
        JOIN read_parquet('/data/events.parquet') p ON c.id = p.id"

# Remote file over HTTPS
sqe -e "SELECT COUNT(*) FROM read_parquet('https://example.com/data.parquet')"
```

`--memory` runs with no persistent catalog (nothing survives the process).
`--embedded` alone attaches a SQLite-backed Iceberg catalog at `~/.sqe/warehouse`
instead, so `CREATE TABLE` persists. See the `embedded-sqlite-catalog` quickstart
for that.

## How it works

| Flag / function | What it does |
|---|---|
| `--embedded` | Run the engine in-process; no coordinator or workers. |
| `--memory` | No catalog; session-only. Pure file querying. |
| `read_csv('path')` | Read a CSV file (local path or `http(s)://` / `s3://` URL). |
| `read_json('path')` | Read newline-delimited JSON. |
| `read_parquet('path')` | Read Parquet, including remote URLs (streamed via range requests). |

The same object-store layer backs all three, so a local path, an HTTPS URL, and
an `s3://` URI are interchangeable as long as the engine can reach them.

## Output

Captured from a clean run (`./run.sh`), committed in [`OUTPUT.md`](./OUTPUT.md):

```
# local CSV aggregate
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+

# join CSV + Parquet, amount > 10
+----+----------+
| id | kind     |
+----+----------+
| 2  | purchase |
| 4  | purchase |
+----+----------+

# remote HTTPS Parquet
+------+
| rows |
+------+
| 1000 |
+------+
```

## How it is tested

`run.sh` exercises `read_csv`, `read_json`, `read_parquet` on local files, a
cross-format join, and a remote HTTPS read, and captures the output. The local
Parquet path mirrors the `test_read_parquet_local_file` integration test. Last
validated 2026-06-06.

## Gotchas

- **Remote reads need outbound network** from wherever the engine runs (inside
  the container here). HTTPS and `s3://` URLs both work; `s3://` needs the AWS
  credential chain or `[storage]` config.
- **`read_json` expects newline-delimited JSON** (one object per line), not a
  top-level JSON array.
- **`--memory` keeps nothing.** To persist `CREATE TABLE` results locally, use
  `--embedded` without `--memory` (SQLite catalog) or `--warehouse <path>`.

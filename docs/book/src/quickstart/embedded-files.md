---
slug: embedded-files
title: "Embedded: query local and remote files"
description: "Run SQE's engine in-process with sqe-cli --embedded and query CSV, JSON, and Parquet files directly with read_csv / read_json / read_parquet. No server, no catalog, local files or HTTPS URLs."
---

# Embedded: query local and remote files

SQE's engine runs in-process. `sqe-cli --embedded --memory` starts the query
engine and the file-reader table-valued functions in a single binary — no
coordinator, no workers, no network listeners, no catalog. The `read_csv`,
`read_json`, and `read_parquet` TVFs read files directly, whether they live on
local disk or behind an HTTPS URL.

This is the fastest way to explore data with SQL. There is no Docker stack to
bring up; this quickstart just runs the CLI.

## How it works

- `--embedded` runs the engine in-process. `--memory` disables the persistent
  catalog — the session is ephemeral and nothing is written to disk.
- Sample data files (CSV, JSON, Parquet) in the `data/` directory are mounted
  into the container at runtime.
- `read_csv`, `read_json`, and `read_parquet` accept a local path, an `https://`
  URL, or an `s3://` URI — the same object-store layer backs all three.
- `run.sh` exercises local CSV aggregation, a cross-format JOIN (CSV + Parquet),
  and a remote HTTPS Parquet read, and captures the output.

## What it demonstrates

- Querying CSV, JSON, and Parquet files with SQL in a single binary, no server.
- Cross-format joins: `read_csv(...)` and `read_parquet(...)` in one query.
- Remote file reads over HTTPS: `read_parquet('https://...')` streams the file
  via range requests.
- The `--memory` flag: no state survives the process (for persistent local
  tables, see the [embedded-sqlite-catalog quickstart](./embedded-sqlite-catalog.md)).

**Status:** validated (2026-06-06).

## Run it

Full sample data, queries, and captured output are in the repo:

**→ [quickstart/embedded-files/](https://github.com/schubergphilis/sqe/tree/main/quickstart/embedded-files/)**

```bash
cd quickstart/embedded-files
cp .env.example .env
./run.sh
```

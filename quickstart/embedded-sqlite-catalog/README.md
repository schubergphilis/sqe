---
slug: embedded-sqlite-catalog
title: "Embedded: persistent local catalog (SQLite)"
description: "Run SQE's engine in-process with a persistent, SQLite-backed Iceberg catalog on local disk. CREATE TABLE survives across CLI invocations, no server or external catalog service."
---

# Embedded: persistent local catalog (SQLite)

`sqe-cli --embedded --warehouse <dir>` runs the engine in-process and attaches a
**SQLite-backed Iceberg catalog** at `<dir>`. Unlike the
[`embedded-files`](../embedded-files/) quickstart (which uses `--memory` and keeps
nothing), here `CREATE TABLE` and the data persist on disk: the catalog is a
`sqe.db` SQLite file and the table data lives next to it as Iceberg metadata +
Parquet. No server, no Polaris, no catalog service.

This is the single-binary, local-first way to keep Iceberg tables on a laptop.

## What you get

A `queries-init.sql` (create + insert) and a `run.sh` that runs **two separate
`sqe-cli` processes** against the same `./warehouse`: the first writes, the
second (a fresh invocation) reads it back, proving on-disk persistence.

## Prerequisites

- Docker + the SQE image (`sqe-quickstart:latest`); build once from any server
  quickstart, or `cargo install --path crates/sqe-cli` for a host binary.

## Run it

```bash
cd quickstart/embedded-sqlite-catalog
./run.sh            # process 1 writes, process 2 (separate) reads
./run.sh --clean    # reset ./warehouse first
```

By hand:

```bash
# process 1: create + write
docker run --rm --entrypoint sqe-cli -v "$PWD/warehouse":/data/wh \
  -v "$PWD/queries-init.sql":/init.sql:ro sqe-quickstart:latest \
  --embedded --warehouse /data/wh --file /init.sql

# process 2: a separate invocation reopens the same warehouse
docker run --rm --entrypoint sqe-cli -v "$PWD/warehouse":/data/wh \
  sqe-quickstart:latest --embedded --warehouse /data/wh \
  -e "SELECT * FROM iceberg.demo.events"
```

`--warehouse <dir>` names the catalog `iceberg`, so tables are
`iceberg.<namespace>.<table>`. `sqe-cli` takes one `-e`; use `--file` for a
multi-statement script.

## How it works

| Flag | What it does |
|---|---|
| `--embedded` | Run the engine in-process; no coordinator/workers/listeners. |
| `--warehouse <dir>` | Persistent SQLite Iceberg catalog named `iceberg` at `<dir>` (`sqe.db` + Iceberg data). Survives the process. |
| `--memory` | (the other mode) no catalog, session-only -- see `embedded-files`. |
| `--catalog name=PATH` | Attach several named persistent catalogs at once -- see `attach-catalogs`. |

## Output

Captured from a clean run (`./run.sh --clean`), in [`OUTPUT.md`](./OUTPUT.md):

```
# process 1: INSERT 4 rows  ->  count 4
# process 2 (separate invocation): SELECT reads them back
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
# on disk:  iceberg/   sqe.db
```

## How it is tested

`run.sh` writes in one process and reads in a second, asserting the rows survive,
and lists the on-disk `sqe.db` + `iceberg/`. Validated 2026-06-06.

## Gotchas

- **`--memory` vs `--warehouse`**: `--memory` keeps nothing; `--warehouse`
  persists. They are mutually exclusive (also with `--catalog`).
- **Multi-statement**: `sqe-cli` accepts a single `-e`; use `--file script.sql`
  for several statements.
- **The catalog is local**: `./warehouse/sqe.db` is the whole catalog. Copy or
  delete the directory to move/reset it. `./run.sh --clean` resets it.

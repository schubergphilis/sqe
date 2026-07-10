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
./run.sh --clean    # reset ./warehouse first, then run
./run.sh --check    # run, then assert the persisted read returns the rows
```

There is no long-running stack here, so there is no `--down`; `--clean` deletes
`./warehouse` to start from an empty catalog.

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

Embedded mode runs the whole engine inside the `sqe-cli` process: DataFusion,
the Iceberg writers, and a catalog, with no coordinator, workers, or network
listeners. `--warehouse <dir>` attaches a catalog whose metadata is a `sqe.db`
SQLite file at `<dir>` and whose data files (Iceberg metadata + Parquet) live
next to it under `<dir>/iceberg`.

The persistence proof is the two-process flow: process 1 creates the table and
inserts the rows, then exits. Process 2 is a separate `sqe-cli` invocation that
opens the same warehouse and reads the rows back. The data survives because it
is on disk in the SQLite catalog plus the Iceberg files, not in process memory.

## Configuration explained

This scenario has no config file. The configuration is the `sqe-cli` flags plus
the SQL in `queries-init.sql`.

| Flag | What it does |
|---|---|
| `--embedded` | Run the engine in-process; no coordinator, workers, or listeners. |
| `--warehouse <dir>` | Attach a persistent SQLite Iceberg catalog named `iceberg` at `<dir>` (`sqe.db` + Iceberg data under `iceberg/`). It survives the process. |
| `--file PATH` | Run a multi-statement SQL script (the seed step uses this). |
| `-e "SQL"` | Run a single statement and print the result (the read step uses this). |
| `--stop-on-error` | Abort on the first failing statement instead of continuing. |

`--warehouse` names the catalog `iceberg`, so tables resolve as
`iceberg.<namespace>.<table>`. It is mutually exclusive with `--memory` (no
catalog, session-only, see `embedded-files`) and `--catalog NAME=PATH` (attach
several named catalogs at once, see `attach-catalogs`).

`queries-init.sql` creates `iceberg.demo.events` and inserts four rows. The read
step in process 2 aggregates them by `kind`.

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

`./run.sh --check` opens the persisted warehouse in a fresh process (the same
read process 2 runs) and asserts the invariants in `run.sh`:

- the persisted read returns rows (not empty, not `0 rows`),
- it shows the purchase total `55.25` (the rows process 1 wrote survived the
  process exit),
- the read contains no `error`.

That is the whole point of the scenario: a second, separate invocation sees what
the first wrote. The demo run also lists the on-disk `sqe.db` + `iceberg/` to
show where the catalog lives. Validated 2026-06-06.

## Gotchas

- **`--memory` vs `--warehouse`**: `--memory` keeps nothing; `--warehouse`
  persists. They are mutually exclusive (also with `--catalog`).
- **Multi-statement**: `sqe-cli` accepts a single `-e`; use `--file script.sql`
  for several statements.
- **The catalog is local**: `./warehouse/sqe.db` is the whole catalog. Copy or
  delete the directory to move/reset it. `./run.sh --clean` resets it.

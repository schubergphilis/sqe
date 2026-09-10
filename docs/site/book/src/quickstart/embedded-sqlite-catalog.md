---
slug: embedded-sqlite-catalog
title: "Embedded: persistent local catalog (SQLite)"
description: "Run SQE's engine in-process with a persistent, SQLite-backed Iceberg catalog on local disk. CREATE TABLE survives across CLI invocations, no server or external catalog service."
---

# Embedded: persistent local catalog (SQLite)

`sqe-cli --embedded --warehouse <dir>` runs the engine in-process and attaches a
**SQLite-backed Iceberg catalog** rooted at `<dir>`. Unlike the
[embedded-files quickstart](./embedded-files.md) (which uses `--memory` and keeps
nothing), here `CREATE TABLE` and its data persist on disk: the catalog is a
SQLite file and the table data lives next to it as Iceberg metadata and Parquet.
No server, no Polaris, no catalog service.

## How it works

- `--embedded --warehouse <dir>` names the catalog `iceberg` and stores both the
  SQLite catalog metadata (`sqe.db`) and the Iceberg data files under `<dir>`.
- `run.sh` runs **two separate `sqe-cli` processes** against the same warehouse
  directory: the first writes (create + insert), the second (a fresh invocation)
  reads it back. This proves that the data survives across process restarts.
- The `./warehouse` directory persists on the host; `./run.sh --clean` resets it.

## Which SQLite engine

Two engines can serve `sqe.db`, selected at build time:

| Build | Engine | Notes |
|---|---|---|
| `cargo build -p sqe-cli` (any feature set, including `--no-default-features`) | [Lab271/sqlite-rs](https://github.com/Lab271/sqlite-rs), a pure-Rust SQLite, vendored under `vendor/sqlite-rs` | Always compiled in. No `libsqlite3` in the binary. |
| `cargo build -p sqe-cli --features sqlite-c` | `sqlx` + the bundled C `libsqlite3` | The engine releases before 2026-09 shipped; kept as an opt-in fallback. |

Both engines read and write the same file. A warehouse created by one opens
under the other, in either direction: the pure-Rust engine maintains the
`sqlite_autoindex_*` a stock-created file carries, and a file it creates
itself gets named `UNIQUE` indexes that stock `sqlite3` verifies clean
(`PRAGMA integrity_check` = `ok`). One `Connection` is kept per catalog file
per process; there is no pool.

## What it demonstrates

- Data written in one process is readable by a separate, subsequent process:
  on-disk persistence via SQLite + Iceberg.
- The full local Iceberg lifecycle: `CREATE SCHEMA`, then `CREATE TABLE`, then `INSERT`
  in process 1; `SELECT` in process 2.
- The difference between `--memory` (session-only) and `--warehouse` (persistent).

**Status:** validated (2026-06-06).

## Run it

Full queries and captured output are in the repo:

**See: [quickstart/embedded-sqlite-catalog/](https://github.com/schubergphilis/sqe/tree/main/quickstart/embedded-sqlite-catalog/)**

```bash
cd quickstart/embedded-sqlite-catalog
cp .env.example .env
./run.sh
```

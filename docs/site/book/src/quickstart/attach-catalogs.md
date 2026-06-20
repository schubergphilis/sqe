---
slug: attach-catalogs
title: "Embedded: attach multiple catalogs"
description: "Attach several persistent Iceberg catalogs in one embedded SQE session with --catalog NAME=PATH and query (JOIN) across them. No server."
---

# Embedded: attach multiple catalogs

`sqe-cli --embedded --catalog NAME=PATH` (repeatable) mounts several persistent,
SQLite-backed Iceberg catalogs in one in-process session. Each catalog shows up
under its name in 3-part SQL identifiers (`name.namespace.table`), and a single
query can JOIN across them. No server, no catalog service.

Useful for local analysis that spans more than one warehouse — for example, a
`sales` catalog and a `ref` (reference-data) catalog, joined in one query.

## How it works

- Two seed scripts populate two independent catalogs under separate directories
  (`./catalogs/sales` and `./catalogs/ref`), each with its own SQLite metadata
  and Iceberg data.
- A single `sqe-cli` session attaches both with two `--catalog` flags and runs a
  cross-catalog JOIN.
- `--catalog NAME=PATH` is repeatable and mutually exclusive with `--memory` and
  `--warehouse`.
- `./run.sh --clean` resets both catalog directories.

## What it demonstrates

- Attaching two independent persistent catalogs in one embedded session.
- A cross-catalog JOIN resolving tables from each catalog by name.
- The `--catalog NAME=PATH` flag as an alternative to `--warehouse` when more
  than one named catalog is needed.

**Status:** validated (2026-06-06).

## Run it

Full seed scripts and captured output are in the repo:

**→ [quickstart/attach-catalogs/](https://github.com/schubergphilis/sqe/tree/main/quickstart/attach-catalogs/)**

```bash
cd quickstart/attach-catalogs
cp .env.example .env
./run.sh
```

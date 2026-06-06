---
slug: attach-catalogs
title: "Embedded: attach multiple catalogs"
description: "Attach several persistent Iceberg catalogs in one embedded SQE session with --catalog NAME=PATH and query (JOIN) across them. No server."
---

# Embedded: attach multiple catalogs

`sqe-cli --embedded --catalog NAME=PATH` (repeatable) mounts several persistent,
SQLite-backed Iceberg catalogs in one in-process session. Each catalog shows up
under its name in 3-part SQL identifiers, and a single query can JOIN across
them. No server, no catalog service.

Useful for local analysis that spans more than one warehouse: a `sales` warehouse
and a `ref` (reference-data) warehouse, joined in one query.

## What you get

Two seed scripts (`seed-sales.sql`, `seed-ref.sql`) that populate separate
catalogs, and a `run.sh` that seeds both then attaches both for a cross-catalog
JOIN.

## Prerequisites

- Docker + the SQE image (`sqe-quickstart:latest`), or a host `sqe-cli`.

## Run it

```bash
cd quickstart/attach-catalogs
./run.sh            # seed sales + ref, then attach both and JOIN
./run.sh --clean    # reset ./catalogs first
```

By hand:

```bash
sqe-cli --embedded \
  --catalog sales=./catalogs/sales \
  --catalog ref=./catalogs/ref \
  -e "SELECT r.name, SUM(o.amount) FROM sales.public.orders o
      JOIN ref.public.regions r ON o.region_id = r.region_id GROUP BY r.name"
```

`--catalog NAME=PATH` is repeatable and mutually exclusive with `--memory` /
`--warehouse`. Each catalog gets its own SQLite metadata + data root and resolves
as `NAME.namespace.table`.

## Output

Captured from a clean run (`./run.sh --clean`), in [`OUTPUT.md`](./OUTPUT.md) --
a JOIN of `sales.public.orders` against `ref.public.regions`:

```
+------+---+-------+
| name | n | total |
+------+---+-------+
| EU   | 2 | 49.25 |
| US   | 1 | 13.5  |
+------+---+-------+
```

## How it is tested

`run.sh` seeds two independent catalogs, attaches both, and runs a cross-catalog
JOIN, asserting it resolves tables from each. Validated 2026-06-06.

## Gotchas

- `--catalog` is mutually exclusive with `--memory` and `--warehouse`.
- Each catalog persists under its own path (`./catalogs/<name>`); `--clean` resets.
- Multi-statement seeding uses `--file` (one `-e` per invocation).

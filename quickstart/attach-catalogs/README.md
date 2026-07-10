---
slug: attach-catalogs
title: "Embedded: attach multiple catalogs"
description: "Attach several persistent Iceberg catalogs in one embedded SQE session with --catalog NAME=PATH and query (JOIN) across them. No server, no catalog service, no config file."
---

# Embedded: attach multiple catalogs

`sqe-cli --embedded --catalog NAME=PATH` (repeatable) mounts several persistent,
SQLite-backed Iceberg catalogs in one in-process session. Each catalog shows up
under its name in 3-part SQL identifiers (`name.namespace.table`), and a single
query can JOIN across them. No server, no catalog service, no config file.

This is the path for local analysis that spans more than one warehouse: a
`sales` warehouse and a `ref` (reference-data) warehouse, joined in one query.
Each warehouse stays on disk between runs, so you can rebuild one without
touching the other.

## What you get

There is no Docker stack and no `sqe.toml` here. The scenario is a single
binary driven by command-line flags:

| Asset | Role |
|---|---|
| `sqe-cli --embedded` | The engine running in-process; no separate server. |
| `./catalogs/sales/` | SQLite-backed Iceberg catalog for the `sales` warehouse (created on first run). |
| `./catalogs/ref/` | SQLite-backed Iceberg catalog for the `ref` warehouse. |
| `seed-sales.sql` | Creates and populates `sales.public.orders` (three rows). |
| `seed-ref.sql` | Creates and populates `ref.public.regions` (two rows). |
| Docker | Wrapper that runs the published `sqe-quickstart:latest` image when there is no host `sqe-cli`. |

## Prerequisites

- Docker, with the SQE image (`sqe-quickstart:latest`). Build it once with
  `(cd ../polaris-keycloak-client-id && docker compose build sqe)`, or point
  `SQE_IMAGE` at an image you already have.
- A host `sqe-cli` works too if you prefer to skip Docker; the flags are the
  same.

## Run it

```bash
cd quickstart/attach-catalogs
./run.sh             # seed sales + ref, then attach both and JOIN
./run.sh --clean     # reset ./catalogs first, then run
./run.sh --check     # run, then assert the cross-catalog JOIN returns rows
```

`run.sh` seeds the two catalogs in two separate `sqe-cli` invocations, then
opens both in one session for the cross-catalog JOIN, and writes the result to
[`OUTPUT.md`](./OUTPUT.md). There is no long-running stack, so there is no
`--down`; `--clean` deletes `./catalogs` to start from an empty warehouse.

By hand:

```bash
sqe-cli --embedded \
  --catalog sales=./catalogs/sales \
  --catalog ref=./catalogs/ref \
  -e "SELECT r.name, SUM(o.amount) FROM sales.public.orders o
      JOIN ref.public.regions r ON o.region_id = r.region_id GROUP BY r.name"
```

## How it works

Embedded mode runs the whole engine inside the `sqe-cli` process. Each
`--catalog NAME=PATH` registers an Iceberg catalog whose metadata lives in a
SQLite file under `PATH` and whose data files live alongside it. The catalogs
are independent: seeding `sales` does not touch `ref`, and they can be created
in separate invocations.

When two catalogs are attached at once, SQE resolves a 3-part identifier
(`sales.public.orders`, `ref.public.regions`) to the matching catalog, plans the
JOIN across both, and executes it in-process. The data path is local: SQLite
metadata plus on-disk Iceberg data, no network, no token.

## Configuration explained

This scenario has no config file. The configuration is the set of `sqe-cli`
flags, and the SQL in the two seed scripts.

### CLI flags

- `--embedded` selects in-process mode: the engine runs inside `sqe-cli`, no
  coordinator or worker. This is mutually exclusive with connecting to a server
  (`--port`).
- `--catalog NAME=PATH` registers a persistent SQLite-backed Iceberg catalog at
  `PATH`, exposed in SQL as `NAME`. The flag is repeatable, and that is the
  point of this quickstart: pass it twice to attach two warehouses at once. It
  is mutually exclusive with `--memory` (ephemeral in-RAM catalog) and
  `--warehouse` (a single default catalog).
- `--file PATH` runs a multi-statement SQL script. The seed step uses this
  because `-e` takes a single statement.
- `--stop-on-error` aborts the script on the first failure instead of
  continuing, so a broken seed fails loudly.
- `-e "SQL"` runs one statement and prints the result. The JOIN step uses this.

### Seed scripts

`seed-sales.sql` creates `sales.public.orders` (id, region_id, amount) and
inserts three rows. `seed-ref.sql` creates `ref.public.regions` (region_id,
name) and inserts the `EU` (10) and `US` (20) rows. The JOIN keys orders to
regions on `region_id`, which is why the two warehouses can be queried as one.

### Docker wrapping

`run.sh` runs the image with `--entrypoint sqe-cli` and bind-mounts each
catalog directory and seed file into the container (`-v
"$PWD/catalogs/sales":/d/sales`). The catalog paths inside the container
(`/d/sales`, `/d/ref`) are what the `--catalog` flags point at; the host
directories under `./catalogs` are what persists. Set `SQE_IMAGE` to use a
different image.

## Output

Captured from a clean run (`./run.sh --clean`), committed in
[`OUTPUT.md`](./OUTPUT.md). The cross-catalog JOIN of `sales.public.orders`
against `ref.public.regions`:

```
+------+---+-------+
| name | n | total |
+------+---+-------+
| EU   | 2 | 49.25 |
| US   | 1 | 13.5  |
+------+---+-------+
```

EU is two orders (42.00 + 7.25 = 49.25) and US is one (13.50), which only
resolves if both catalogs are attached and the JOIN crosses them.

## How it is tested

`./run.sh --check` re-runs the cross-catalog JOIN over both warehouses and
asserts the invariants in `run.sh`:

- the JOIN returns rows (not empty, not `0 rows`),
- the result contains the `EU` region,
- the EU total is `49.25` (the sum that only appears when `sales.orders` is
  joined to `ref.regions`),
- the output contains no `error`.

Those four assertions together prove both catalogs resolved and the JOIN
crossed them. Validated 2026-06-06.

## Gotchas

- `--catalog` is mutually exclusive with `--memory` and `--warehouse`. Pick one
  catalog model per session.
- Each catalog persists under its own path (`./catalogs/<name>`). `--clean`
  deletes `./catalogs`; without it, a second run reuses the existing warehouses.
- Multi-statement seeding needs `--file`; `-e` runs a single statement only.
- The image must exist locally. If `sqe-quickstart:latest` is missing, build it
  (see Prerequisites) or set `SQE_IMAGE`.

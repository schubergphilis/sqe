# Embedded: attach multiple catalogs

## Goal

`sqe-cli --embedded --catalog NAME=PATH` (the flag is repeatable) mounts several
persistent, SQLite-backed Iceberg catalogs in one in-process session. Each catalog
appears under its own name in 3-part SQL identifiers (`name.namespace.table`), and
a single query can JOIN across them.

This is useful for local analysis that spans more than one warehouse — for example,
a `sales` catalog and a `ref` (reference-data) catalog joined in one query — with
no server, no catalog service, and no configuration file required.

## Components

| Component | Role |
|---|---|
| `sqe-cli` | Embedded engine binary (in-process; no separate server) |
| `./catalogs/sales/` | SQLite-backed Iceberg catalog for the `sales` warehouse |
| `./catalogs/ref/` | SQLite-backed Iceberg catalog for the `ref` (reference-data) warehouse |
| `seed-sales.sql` | Creates and populates `sales.public.orders` |
| `seed-ref.sql` | Creates and populates `ref.public.regions` |
| Docker (optional) | Container wrapper when no local `sqe-cli` build is available |

## Configuration

### CLI

```bash
# Step 1: seed the sales catalog
docker run --rm --entrypoint sqe-cli \
  -v "$PWD/catalogs/sales":/d/sales \
  -v "$PWD/seed-sales.sql":/s.sql:ro \
  sqe-quickstart:latest \
  --embedded --catalog sales=/d/sales --file /s.sql --stop-on-error

# Step 2: seed the ref catalog
docker run --rm --entrypoint sqe-cli \
  -v "$PWD/catalogs/ref":/d/ref \
  -v "$PWD/seed-ref.sql":/r.sql:ro \
  sqe-quickstart:latest \
  --embedded --catalog ref=/d/ref --file /r.sql --stop-on-error

# Step 3: attach BOTH catalogs and JOIN across them in one query
docker run --rm --entrypoint sqe-cli \
  -v "$PWD/catalogs/sales":/d/sales \
  -v "$PWD/catalogs/ref":/d/ref \
  sqe-quickstart:latest \
  --embedded --catalog sales=/d/sales --catalog ref=/d/ref \
  -e "SELECT r.name, COUNT(*) AS n, ROUND(SUM(o.amount),2) AS total
      FROM sales.public.orders o
      JOIN ref.public.regions r ON o.region_id = r.region_id
      GROUP BY r.name ORDER BY total DESC"
```

`--catalog NAME=PATH` is mutually exclusive with `--memory` and `--warehouse`.
Each catalog persists under its own path. `./run.sh --clean` resets the
`./catalogs` directory.

## The test

`run.sh` seeds two independent catalogs (`sales` and `ref`) in separate
`sqe-cli` invocations, each using `--file` for the multi-statement seed script.
It then opens both catalogs in a single session and runs a cross-catalog JOIN
(`sales.public.orders` against `ref.public.regions`), asserting that tables
from each catalog resolve correctly. Output is captured to `OUTPUT.md`. Last
validated 2026-06-06.

## Output

```
## Seed the `sales` and `ref` catalogs (separate warehouses)
$ sqe-cli --embedded --catalog sales=./catalogs/sales --file seed-sales.sql
sqe-cli 0.31.4 embedded engine (1GB memory pool, warehouse: /d/sales)
(0 rows)
(0 rows)
(1 rows)
+-------+
| count |
+-------+
| 3     |
+-------+
$ sqe-cli --embedded --catalog ref=./catalogs/ref --file seed-ref.sql
sqe-cli 0.31.4 embedded engine (1GB memory pool, warehouse: /d/ref)
(0 rows)
(0 rows)
+-------+
| count |
+-------+
| 2     |
+-------+
(1 rows)

## Attach BOTH catalogs and JOIN across them in one query
$ sqe-cli --embedded --catalog sales=... --catalog ref=... -e "... JOIN ..."
sqe-cli 0.31.4 embedded engine (1GB memory pool, catalogs: sales=/d/sales, ref=/d/ref)
+------+---+-------+
| name | n | total |
+------+---+-------+
| EU   | 2 | 49.25 |
| US   | 1 | 13.5  |
+------+---+-------+
(2 rows)
```

---
slug: hive-external-s3
title: "Hive External Tables on S3-Compatible Storage"
description: "Query CSV, line-delimited JSON, and partitioned Parquet on any S3-compatible store with Athena CREATE EXTERNAL TABLE syntax. No Hive Metastore, no Polaris, no Nessie: the table definition is a JSON manifest next to the data, so a fresh process re-attaches and reads the same tables."
---

# Hive External Tables on S3-Compatible Storage

Point SQE at a bucket, declare a table with Athena's own `CREATE EXTERNAL TABLE` syntax, and query it. There is no metastore in this picture. The definition lands in a small JSON manifest inside the warehouse root you attached, so the engine holds no state: stop it, start a new one, attach the same root, and the tables are there.

This quickstart generates its own data, in four formats, laid out the way Hive and Athena expect, and proves the round trip with a second process that shares nothing with the first.

## What you get

An S3-compatible object store (RustFS in a container), about 51MB of seeded random event data by default (`--size-mb 64`, roughly 4 million rows), and four external tables over it:

| Table | Format | Partitions | Declared as |
|---|---|---|---|
| `lake.demo.events_parquet` | Parquet | `dt`, `region` | `ParquetHiveSerDe`, `STORED AS PARQUET` |
| `lake.demo.events_csv` | CSV with a header | `dt` | `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','` |
| `lake.demo.events_json` | line-delimited JSON | `dt` | `org.openx.data.jsonserde.JsonSerDe` |
| `lake.demo.events_ndjson` | line-delimited JSON | none | `STORED AS JSONFILE` |

Pick this scenario when your data is already in a bucket in an open file format and you want SQL over it without converting it to Iceberg first, without running a Hive Metastore, and without registering anything in a catalog service. Raw-zone exploration, a Glue-shaped table you want to read from your own infrastructure, a lift of Athena DDL onto a store you control.

Nothing here is RustFS-specific. MinIO, Ceph RGW, Garage, or a real AWS bucket work the same way, which is the sovereignty point: SQE speaks S3 and needs nothing else.

## Prerequisites

- Docker (for the object store, and for the aws CLI if you do not have one)
- Python 3.9+ (the data generator; `run.sh` installs pyarrow into a local `./.venv` when needed)
- A built `sqe-cli`: `cargo build --release -p sqe-cli` from the repo root

The `aws` CLI is optional. Without it, `run.sh` uses the `amazon/aws-cli` container on the compose network instead.

## Run it

```bash
./run.sh                    # full run, captures OUTPUT.md
./run.sh --check            # full run, then assert the invariants
./run.sh --server           # the same SQL through a real sqe-server
./run.sh --server --check   # ...with the same 21 assertions
./run.sh --size-mb 1024     # more data (5120 is the generator's ceiling)
./run.sh --regen            # regenerate and re-upload even if data is present
./run.sh --down             # stop the store, keep the bucket
./run.sh --clean            # stop, delete the bucket volume, ./data and ./.venv
```

`--server` needs `sqe-server` built too: `cargo build --release -p sqe-coordinator --bin sqe-server`.

A first run from nothing takes well under a minute at the default size. Re-runs skip generation and upload when the data already matches the requested size, so iterating on the SQL is fast.

## How it works

```
gen_data.py ──> ./data ──> s3://lake/data/<table>/<partitions>/   the data
                                                                 
sqe-cli --hive-root lake=s3://lake/wh/                            the root
   CREATE EXTERNAL TABLE ──> s3://lake/wh/demo/<table>/_sqe/table.json
```

Four things are worth following.

**The manifest is the metastore.** `CREATE EXTERNAL TABLE` writes one JSON object to `<root>/<database>/<table>/_sqe/table.json` and nothing else. It is shaped like a Glue `TableInput` (a `storage_descriptor` with `columns`, `serde_info`, `location`; `partition_keys` beside it; SQE's own settings in an `sqe` block), so a future Glue-backed catalog is a field mapping rather than a rewrite. `OUTPUT.md` contains a real one.

**Data and manifests live apart.** The root is `s3://lake/wh/`; the data is under `s3://lake/data/`. A cycle-1 scan lists a table's `LOCATION` without filtering by file extension, so a `LOCATION` that contained the `_sqe` directory would hand `table.json` to the CSV reader. Same reason each table gets its own prefix: two formats under one prefix means one of them gets parsed by the wrong reader.

**Partition columns come from the path.** `dt=2026-01-02/region=eu/` is where `dt` and `region` come from; they are declared in `PARTITIONED BY`, never in the column list, and they land last in the schema. Pinning the leading partition columns with `=` is what narrows the listing; a range or an `IN` list lists the whole table and filters in memory.

**Every `sqe-cli` call in `run.sh` is its own process.** The engine runs with `--memory`, so it persists nothing locally. The queries after the "process 2" heading are a cold start that attaches the root and nothing else: no DDL, no schema, no paths. Whatever they return came out of the bucket.

## The same SQL, two deployment shapes

`--server` changes exactly one thing: how a statement reaches the engine. It starts a real `sqe-server`, which attaches the warehouse root from `[[hive.roots]]` in `sqe.toml` at startup, and `sqe-cli` connects over Flight SQL. The DDL file, the query file, and all 21 assertions are the same ones the embedded path runs, and the row counts come out identical, because both are reading the same manifests out of the same bucket.

The re-attach proof works differently in the two modes, and it has to. Embedded gets its process boundary for free: every `sqe-cli --embedded` call is a new engine. A server is long-lived, so the script kills the coordinator and boots a new one, which re-reads `[[hive.roots]]` and every manifest from scratch. That is a stronger claim than the embedded one, and it is the reason `OUTPUT-server.md` has a "restarting the coordinator" step where `OUTPUT.md` does not.

`sqe.toml` deliberately points `[catalog]` at a port nobody is listening on. A Hive external table has no catalog service behind it, so a coordinator serving only Hive roots should not need one, and this proves it: the server boots, attaches the root, and answers every Hive query. Two statements do fail as a result, which is the interesting part. See the mode-asymmetry gotcha below.

## Configuration explained

### `run.sh` flags to the engine

```bash
sqe-cli --embedded --memory \
  --s3-endpoint http://localhost:19100 \
  --hive-root lake=s3://lake/wh/
```

- `--embedded` runs the engine in-process. No coordinator, no auth, no Polaris.
- `--memory` skips the local Iceberg catalog entirely. Nothing about these tables is local, so there is nothing to persist.
- `--s3-endpoint` is what makes a non-AWS store reachable. Without it the engine talks to real AWS and, with no static key, falls through to the instance-metadata service and times out. An endpoint override also switches to path-style addressing (`host:port/bucket/key`), because `bucket.host:port` does not resolve for a self-hosted store; `--s3-virtual-hosted` turns that back off for a store with wildcard DNS.
- `--hive-root NAME=URL` attaches the warehouse root. Repeatable. Attaching does not list or read anything; it is a path check against the storage allowlist, not a network call.

Credentials resolve per setting, in order: the flag (`--s3-access-key`, `--s3-secret-key`, `--s3-region`), then `SQE_STORAGE__S3_*` (the same names the coordinator's config honors), then the standard `AWS_*` variables. `run.sh` puts the key in the environment rather than on the command line, where `ps` and shell history would both keep a copy.

### `create-tables.sql`

The DDL, with a comment per clause explaining why it is there. Two clauses trip up copied Athena DDL:

- `STORED AS CSV` and `STORED AS JSON` do not parse. The format keywords are `TEXTFILE`, `SEQUENCEFILE`, `ORC`, `PARQUET`, `AVRO`, `RCFILE`, `JSONFILE`. For comma-separated data the working form is `ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE`.
- Bare `TEXTFILE` means Hive's control-A delimiter (`\001`), not comma. Declare the delimiter or get a one-column table, exactly as in Athena.

### `sqe.toml` (server mode only)

Annotated per section. The parts that matter for Hive:

- `[[hive.roots]]` with `name` and `location` is the config equivalent of `ATTACH ... TYPE hive_external`. The startup attach is a path check against the allowlist, not a network call, so a wrong bucket fails at the first query rather than at boot.
- `[storage.tvf] allowed_object_store_prefixes` is **not optional in server mode.** Server mode fails closed on object-store paths: with no allowlist, every `s3://` read is denied, including the manifest load behind a Hive table. Embedded mode skips this gate because the local user already owns the process, its config and its credentials; a coordinator serving remote identities does not get that assumption.
- `[[auth.providers]] type = "anonymous"` accepts every connection as one identity. DEV ONLY, and the server logs a security error on startup saying so. It is what lets this quickstart run with no IdP.
- Anything `.env` can change is passed to the server as an `SQE_*` env override rather than templated into the file, so the TOML stays readable as documentation. That is the same mechanism a container deployment uses.

### `gen_data.py`

Seeded random event data: `event_id`, `user_id`, `kind`, `amount`. The same `--size-mb` always produces the same bytes, which is why the `--check` row counts are exact rather than approximate. `--size-mb` splits 70% Parquet, 20% CSV, 5% and 5% JSON, and is approximate to within about 10%: the bytes-per-row constants are measured per format, and Snappy Parquet compresses these columns much harder than JSON does. The ceiling is 5120 (5GB), because the generator writes locally before uploading.

### `docker-compose.yml`

`rustfs` is the store, on host port 19100 (offset from the integration stack's 19000, so both can run at once). `awscli` is a `profiles: ["tools"]` service, so `docker compose up` never starts it; `run.sh` invokes it only on a machine with no host `aws`. It reaches the store as `http://rustfs:9000` while the engine reaches the same bucket on `localhost` from the host.

### `.env.example`

Port, credentials, region, and default size. Copy to `.env` to override. The bucket name is deliberately not configurable: `create-tables.sql` names `s3://lake/...` literally, and templating the SQL would make the file unreadable as documentation.

## Output

`OUTPUT.md` is a real captured run at the default `--size-mb 64` (51MB on disk, 3.96M rows across the four tables): 3,355,440 Parquet, 516,220 CSV, 46,600 JSON, 46,603 ndjson. It contains the object listing, the DDL, every query result, a real manifest, and the process-2 re-attach. The manifest's `owner` field is whoever ran the capture, taken from the OS user, so it will read differently on your machine; nothing else in the file varies between runs at the same size.

The 1024MB size has also been run green end to end: 63.4M rows across the four tables, 73 seconds from generation to the last assertion, on a laptop.

## How it is tested

`./run.sh --check` asserts 21 invariants. The ones that carry weight:

- **Row counts match the generator**, per table, exactly. The generator writes what it produced into `data/generated.json`, so the engine is checked against an independent record rather than against itself.
- **Process 2 agrees with process 1** on the row count, and `SHOW CREATE TABLE` in the new process renders back the location, both partition keys, and the SerDe. A weaker check ("a query returned rows") would pass even if the table had been silently recreated.
- **The CSV header is skipped, not read as data.** `WHERE kind = 'kind'` must return zero.
- **Partition filters actually filter.** Pinning `dt` and `region` returns the pinned partition and excludes both other days and the other region.
- **The manifest is Glue-shaped**: `table_type: EXTERNAL_TABLE`, a `storage_descriptor`, `partition_keys`.
- **`DROP TABLE` deletes the manifest and leaves the data.** Both halves are asserted; the table is recreated afterwards so a `--check` run leaves the same state a plain run does.

Before any of that, the run verifies that every object's listed size matches the local file it was uploaded from. That check exists because of the RustFS metadata-cache bug in the gotchas below: without it, a re-run at a different size produces either a parse error or a confidently wrong row count.

`S3_CLIENT=container ./run.sh --check` runs the same suite through the containerized aws CLI, which is the path a machine without a host `aws` takes. Both paths, and the 32MB and 1024MB sizes, have been run green.

## Gotchas

**One prefix per table, always.** A cycle-1 scan does not filter by file extension, so anything under a table's `LOCATION` is treated as that table's data. Mixing formats under one prefix, or pointing `LOCATION` at a directory that also holds manifests, gives parse errors or wrong answers rather than a clear complaint.

**RustFS caches object metadata and does not invalidate it on overwrite.** Measured on `rustfs/rustfs:latest` (2026-08-20): after replacing an object with a different-sized body, `LIST` and `HEAD` keep reporting the previous size and mtime while `GET` serves the new bytes. Deleting the object first does not help. Deleting and recreating the bucket does not help. Only restarting the store clears it. Any reader plans byte ranges from the listed size, so a stale size ends the last range mid-line: you get `Csv error: incorrect number of fields`, or, worse, the row count of the data that used to be there. `run.sh` restarts the store after every upload for exactly this reason and then verifies each object's listed size against the local file it came from. Worth knowing beyond this quickstart: if you overwrite data files in place under a RustFS-backed table, queries can return stale results until the store restarts. Plain `aws s3 ls` reports the same stale size, so this is the store, not the engine.

**JSON means line-delimited JSON.** One object per line, in both the `.json` and `.ndjson` tables here. A `[{...},{...}]` array document is not readable, in SQE or in Athena. The file extension is irrelevant; the framing is what matters.

**Listing a root's tables takes a different statement in each mode, and neither works in the other.** Measured both ways:

| | `SHOW TABLES FROM lake.demo` | `information_schema.tables` |
|---|---|---|
| embedded | not parsed at all (a pre-existing embedded gap, against any catalog kind) | works |
| server | works, and is policy-gated | fails here |

The server-mode failure is worth understanding, because it is not Hive-specific. `information_schema` enumerates **every** registered catalog before any `WHERE` clause is applied, so an unreachable Iceberg catalog fails the whole query even when you asked only about a Hive root. This `sqe.toml` points at a dead Iceberg endpoint on purpose, so that failure shows up in `OUTPUT-server.md` for both `information_schema.tables` and `DESCRIBE`. On a coordinator whose Polaris is reachable, both work; on one whose Polaris is *down*, expect the same failure this capture shows. `run.sh` picks the right statement per mode; `queries.sql` keeps the embedded-canonical form so the file stays copy-pasteable for the common case.

**A binary older than the feature boots fine and serves nothing.** `[[hive.roots]]` is unknown config to a pre-Hive `sqe-server`, so it starts perfectly, attaches no root, and every query fails with "unknown catalog". `run.sh` prefers `target/release` over `target/dev-release`, so a stale release build silently wins over a fresh dev one. Both binary paths and their build dates are printed at startup, and the server-mode path hard-fails with that diagnosis if no root attaches. This cost two debugging detours while building the quickstart; `SQE_CLI` and `SQE_SERVER` override the search.

**Reads use the engine's credentials, not the caller's.** An Iceberg read gets per-user credentials because Polaris vends them per table; nothing plays that role for a raw `s3://` prefix. In embedded mode this distinction is moot (one local user, who owns the config). On a shared coordinator it matters: bound exposure with `[storage.tvf] allowed_object_store_prefixes` and grants, and keep Hive roots out of buckets that should not be uniformly readable by the engine's identity.

**The compressed-text single-thread rule.** A compressed CSV or JSON file is not splittable, so one file is one thread no matter how many cores you have. The generator writes one file per partition for that reason. Parquet splits by row group and gets real footer-statistics pruning.

**Hive scans run on the coordinator in server mode.** Not on workers, this cycle. The embedded engine here is single-process anyway, so the quickstart is unaffected, but do not size a distributed deployment expecting worker parallelism for Hive tables yet.

**A root name may not collide with a catalog name.** `--hive-root iceberg=...` against an attached `iceberg` catalog is refused, because a three-part reference would be ambiguous and policy resolution keys off the catalog component.

## Where to go next

`docs/site/book/src/reference/hive-external-tables.md` is the reference: the full SerDe allow-list, the semantics decisions, the deviations from Athena, and what each later cycle adds (partition DDL and a persisted partition index, tag-based masks and row filters, writes, worker execution).

---
title: "Snowflake and SQE read each other's tables"
description: "A live run against Snowflake's Horizon catalog: each engine writes Iceberg tables the other reads, both run DML on the other's tables, and 33 reads are compared value by value. All 33 match in every SQE mode. Getting there took seven fixes, among them a distributed read that put one column's values under another column's name."
pubDate: "2026-11-05"
author: "Jacob Verhoeks"
tags:
  - "iceberg"
  - "snowflake"
  - "horizon"
  - "interoperability"
  - "deletion-vectors"
---

Last week Databricks and SQE read each other's tables through Unity Catalog. This week it is Snowflake's turn. Snowflake serves its Iceberg tables through Horizon, an Iceberg REST endpoint at `/polaris/api/catalog`. SQE talks to it with the same REST client it uses for Polaris and Unity. The Iceberg matrix rated `snowflake-horizon-catalog` partial on both format versions, because nobody had run it against a real account. Both cells are full now, and SQE is at 158/162.

The method is the same as before. Each engine writes, the other reads, both run DML on the other's tables, and every read runs on both engines with the rows compared value by value. A read that returns wrong values passes every smoke test there is, so "no error" is not the gate.

## No service account

A machine client has no browser to log in with, so the user has a key pair. The private key signs a JWT, and Horizon's token endpoint trades that JWT for an access token scoped to one Snowflake role. SQE passes the token through to Horizon on every call. Horizon decides what the role may see and vends short-lived storage credentials for those tables. SQE holds no key of its own.

```toml
[[auth.providers]]
type = "bearer_passthrough"
user = "<label for the audit log>"

[catalogs.sf]
catalog_url = "https://<orgname>-<accountname>.snowflakecomputing.com/polaris/api/catalog"
warehouse = "<Snowflake database>"
access_delegation = "vended-credentials"
```

The Databricks setup validated the user's token against the workspace JWKS. Horizon's access tokens are opaque and Horizon publishes no key to check them against, so SQE forwards the token and Horizon is the one that authenticates it. That makes `bearer_passthrough` a test-rig provider. Horizon also vends storage credentials only when asked, which is what `access_delegation` does.

## Reading first

The first pass only read. The coordinator loaded and planned every table Snowflake had made: V2 and V3, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, nested objects, arrays and maps, partitions. The embedded CLI did not get that far, and three fixes came out of it.

- `ATTACH` had no way to ask for vended credentials. It now takes `ACCESS_DELEGATION 'vended-credentials'`.
- `ATTACH` loads every table in a schema at once. Horizon answered the burst with 429 Too Many Requests, and the throttled tables vanished without a word. The REST client now waits and retries a 429, and the loader asks for 16 tables at a time.
- Snowflake writes no offset index for `VARIANT` and nested columns. SQE required a page index on every file, so any read of such a table failed with "missing offset index". The page index is optional now. Without it, SQE reads the file without page pruning.

Snowflake also pins an Iceberg `GEOMETRY` column to SRID 4326. SQE reads it as `geometry(4326)`, with the right WKT.

## The run

The harness is in `scripts/interop/snowflake-horizon/`, a Python driver and five SQL files.

1. Snowflake creates its tables: the scalar types including `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ` and nanosecond timestamps, `BINARY` and `TIME`, objects, arrays and maps, identity and month partitions, a table with schema changes, and V3 tables with `VARIANT`, `GEOMETRY` and `GEOGRAPHY`. Then it runs DELETE and UPDATE on them.
2. SQE creates its own set in the same schema with CREATE, CTAS, INSERT, UPDATE, DELETE, MERGE and ALTER.
3. Snowflake runs DELETE, UPDATE and INSERT on SQE's tables.
4. SQE runs DELETE, UPDATE and MERGE on Snowflake's tables.
5. Thirty-three reads run on both engines, and the rows are compared value by value.
6. SQE's metadata commands run against the result.

Phase 5 runs once per SQE mode: the embedded CLI, a single coordinator, and a coordinator with one worker. In the last mode every scan goes to the worker.

## Writes

SQE's writes failed before the first row landed, three times over.

The first failure was ours. A write to a named catalog, one declared as `[catalogs.sf]`, went to the warehouse of the default catalog. Reads had always resolved the name. Writes had not.

The second was Horizon's. SQE names a location when it creates a table, and Horizon refuses a `CREATE TABLE` that does, with HTTP 400. Snowflake places its tables itself. SQE now resends the create without a location, and the table lands where Horizon puts it.

The third took longer. Horizon refuses a commit that comes within about half a second of the previous one on the same table. It answers in one of two ways: a 409 "concurrently modified" that names no failed requirement, or a 500 "Indeterminate result during conflict resolution check". In both cases nothing was applied. A CTAS commits twice in quick succession, once for the schema and once for the data, so it hit this every time.

A 409 is the easy case: the commit did not happen, so wait and send it again. SQE now resends a schema commit unchanged and backs a data commit off at least 500 ms. The 500 is harder. "Indeterminate" means Horizon does not say whether the commit landed. Retrying blind can commit the same files twice. Giving up can leave a committed table whose files the cleanup then deletes. SQE does neither. It waits a second, reloads the table and compares the metadata location. If the table has not moved, the commit did not land, and SQE retries. If the table moved, SQE returns the error and keeps the written files, because they may now belong to a snapshot.

## The wrong column

With writes working, embedded and the single coordinator matched every read. The coordinator with a worker did not. One table came back with `region` holding `1`, which was the value of `id`.

SQE had written that table, partitioned by region. Snowflake had then run DELETE and UPDATE on the `eu` rows. Snowflake's DML on Iceberg is copy-on-write: it rewrites the affected data files. The rewritten files are Snowflake's files, and Snowflake writes parquet columns in an order of its own. It also adds two row-lineage columns, `METADATA$RL_ROW_ID` and `METADATA$RL_LAST_UPDATED_SEQUENCE_NUMBER`, that are not in the table schema. The table now held files from two writers with two column orders.

The coordinator reads every file by Iceberg field id and never noticed. A worker conforms each batch to the planned schema with a projection fill that the coordinator sends in the scan ticket. The coordinator sent that fill only when the table's schema had evolved. This table had not evolved, so the worker got no fill, labelled each batch with the column order of the first file it opened, and bound the other files by position.

The fix is to always send the fill when the scan projects columns. The worker then matches every file by field id, whatever its column order. The cost is compatibility: the fill needs scan ticket V4, so a worker older than that refuses projected scans. Upgrade the workers with the coordinator.

The bug was older than Snowflake. An earlier fix had moved the coordinator's small-file path to field ids and left the worker path for later. It took a second engine rewriting part of a table to make a file order SQE had never written.

## Deletion vectors

Snowflake's DELETE, UPDATE and MERGE are copy-on-write on V2 and on V3. The run checked every snapshot, and none of them had a delete file. So the run cannot show SQE reading a deletion vector that Snowflake wrote, because Snowflake does not write one.

The other direction works. One of SQE's tables is merge-on-read, so SQE's DELETE on it writes a deletion vector. Snowflake reads that table and drops the deleted rows, and the compare checks it in every SQE mode. In a separate probe, Snowflake ran its own DELETE on the same table, and SQE read the result back correctly.

## 33, 33, 33

After those fixes, embedded matched 33 of 33, the single coordinator 33 of 33, and the coordinator with a worker 33 of 33. The worker scanned all 33 reads. We ran the Databricks harness again on the same build, to check that the fill and the commit changes broke nothing there. It matched 27 of 27 in all three modes, including Databricks' own deletion vectors read on the worker.

## What else the run found

None of these change a value.

- Snowflake upper-cases unquoted names, and SQE folds them to lower case. A Snowflake table is `sf."SALES"."ORDERS"` from SQE, and an SQE table is `"orders"` from Snowflake.
- `ST_AREA` on an Iceberg `GEOMETRY` comes back negative in Snowflake, whichever engine wrote the table: -12 for the rectangle whose literal gives 12. The harness compares the absolute value.
- An external volume encrypted with a customer KMS key does not work. The credentials Horizon vends cannot decrypt, so even loading a table fails with 403. Snowflake-managed storage works.
- `$files` lists data files only. Delete files and deletion vectors show in the `$snapshots` summary instead.

## Why run it by hand

The run needs a Snowflake account, a user with a key pair, and a role that may create Iceberg tables in one scratch database. None of that belongs in CI. The harness reads the account from environment variables, writes its output with the token, account and storage names redacted, and exits non-zero on any mismatch. Point it at your own account and run it before you trust either engine with the other's tables.

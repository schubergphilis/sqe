---
title: "Snowflake and SQE read each other's tables"
description: "A live run against Snowflake's Horizon catalog: each engine writes Iceberg tables the other reads, both run DML on the other's tables, and 40 reads are compared value by value. All 40 match in every SQE mode. Getting there took eight fixes, among them a distributed read that put one column's values under another column's name and a parquet decoder that could not apply Snowflake's deletion vectors."
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

1. Snowflake creates its tables: the scalar types including `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ` and nanosecond timestamps, `BINARY` and `TIME`, objects, arrays and maps, identity and month partitions, a table with schema changes, and V3 tables with `VARIANT`, `GEOMETRY` and `GEOGRAPHY`, and two merge-on-read tables of a million rows each. Then it runs DELETE and UPDATE on them.
2. SQE creates its own set in the same schema with CREATE, CTAS, INSERT, UPDATE, DELETE, MERGE and ALTER.
3. Snowflake runs DELETE, UPDATE and INSERT on SQE's tables.
4. SQE runs DELETE, UPDATE and MERGE on Snowflake's tables.
5. Forty reads run on both engines, and the rows are compared value by value.
6. SQE's metadata commands run against the result.

Phase 5 runs once per SQE mode: the embedded CLI, a single coordinator, and a coordinator with one worker. In the last mode every scan goes to the worker.

## What the 40 reads cover

Each read is one line in `compare.tsv`: a name, the SQE query, and the Snowflake query when the two differ. Twenty-seven read tables Snowflake wrote, thirteen read tables SQE wrote.

| Area | Snowflake wrote | SQE wrote | What a match proves |
|---|---|---|---|
| Scalar types | 4 | 1 | `TIMESTAMP_LTZ` and `TIMESTAMP_NTZ` filters, `DECIMAL`, `BINARY`, `TIME`, aggregates |
| Nanosecond timestamps | 1 | | all nine digits survive, compared as epoch nanos |
| Objects, arrays, maps | 1 | | nested fields by path, Snowflake's 0-based arrays against SQE's 1-based |
| Partitions | 2 | 2 | partitioned tables, grouped and filtered |
| V2 and V3 after DML | 5 | 3 | rows after DELETE, UPDATE and MERGE rewrote files written by the other engine |
| Merge-on-read deletes | 7 | 2 | Snowflake's position deletes and deletion vectors on a million rows, SQE's deletion vector |
| `VARIANT` | 1 | 1 | the same path into the same document on both sides |
| `GEOMETRY`, `GEOGRAPHY` | 3 | 2 | WKT, SRID and area |
| Schema evolution | 3 | 1 | added, renamed, widened and dropped columns, old files read by field id |
| CTAS | | 1 | a table SQE created from a query |

A match means the same rows with the same values, not the same row count. Timestamps compare as epoch micros, so a time zone slip of one hour fails the read. Snowflake widened a decimal and inserted 4000000000.00 after it; SQE widened an `INT` and inserted five billion. Both values have to survive the other engine's reader. The million-row tables compare a checksum over every id, a range across the deleted ones, and point lookups at the deleted ids and their neighbours. A reader that drops one row too many, or keeps one deleted row, fails all three.

## Writes

SQE's writes failed before the first row landed, three times over.

The first failure was ours. A write to a named catalog, one declared as `[catalogs.sf]`, went to the warehouse of the default catalog. Reads had always resolved the name. Writes had not.

The second was Horizon's. SQE names a location when it creates a table, and Horizon refuses a `CREATE TABLE` that does, with HTTP 400. Snowflake places its tables itself. SQE now resends the create without a location, and the table lands where Horizon puts it.

The third took longer. Horizon refuses a commit that comes within about half a second of the previous one on the same table. It answers in one of two ways: a 409 "concurrently modified" that names no failed requirement, or a 500 "Indeterminate result during conflict resolution check". In both cases nothing was applied. A CTAS commits twice in quick succession, once for the schema and once for the data, so it hit this every time.

A 409 is the easy case: the commit did not happen, so wait and send it again. SQE now resends a schema commit unchanged and backs a data commit off at least 500 ms. The 500 is harder. "Indeterminate" means Horizon does not say whether the commit landed. Retrying blind can commit the same files twice. Giving up can leave a committed table whose files the cleanup then deletes. SQE does neither. It waits a second, reloads the table and compares the metadata location. If the table has not moved, the commit did not land, and SQE retries. If the table moved, SQE returns the error and keeps the written files, because they may now belong to a snapshot.

## The wrong column

With writes working, embedded and the single coordinator matched every read. The coordinator with a worker did not. One table came back with `region` holding `1`, which was the value of `id`.

SQE had written that table, partitioned by region. Snowflake had then run DELETE and UPDATE on the `eu` rows. On a table that small, Snowflake's DML is copy-on-write: it rewrites the affected data files. The rewritten files are Snowflake's files, and Snowflake writes parquet columns in an order of its own. It also adds two row-lineage columns, `METADATA$RL_ROW_ID` and `METADATA$RL_LAST_UPDATED_SEQUENCE_NUMBER`, that are not in the table schema. The table now held files from two writers with two column orders.

The coordinator reads every file by Iceberg field id and never noticed. A worker conforms each batch to the planned schema with a projection fill that the coordinator sends in the scan ticket. The coordinator sent that fill only when the table's schema had evolved. This table had not evolved, so the worker got no fill, labelled each batch with the column order of the first file it opened, and bound the other files by position.

The fix is to send the fill on every scan that reads columns, `SELECT *` included. Only `COUNT(*)` goes without, because it reads no column. The worker then matches every file by field id, whatever its column order. A worker too old to apply the fill never sees one: the coordinator checks the scan ticket version when a worker registers and refuses an older one, so a mixed cluster fails at startup and not in a query.

The bug was older than Snowflake. An earlier fix had moved the coordinator's small-file path to field ids and left the worker path for later. It took a second engine rewriting part of a table to make a file order SQE had never written.

## Deletion vectors

The first run found no delete file in any Snowflake snapshot. Snowflake had run DELETE and UPDATE on V2 and V3 tables, and every snapshot was a rewrite. We wrote it down as "Snowflake's DML is copy-on-write" and nearly published that.

It was wrong. The tables were a hundred rows each, and on a table that size Snowflake rewrites the file whatever the setting. With the table property `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'enabled'` and a million rows, the same DELETE writes a position delete file on V2 and a deletion vector on V3. The snapshot summaries say so: `added-position-delete-files` on one, `added-dvs` on the other. A later UPDATE adds a position delete on V2 and replaces the vector on V3.

SQE could not read either table. Every read that crossed a deleted row failed with "cannot skip miniblock of size 512".

The error came from below SQE. DataFusion reads parquet through the `parquet` crate from Apache Arrow's Rust implementation, and so does SQE. A deletion vector is a bitmap of row positions. SQE turns it into a row selection, and the parquet reader decodes the rows it keeps and skips the rest. Skipping is the fast path: the reader steps over values without materialising them.

Snowflake writes integer columns as `DELTA_BINARY_PACKED`. That encoding packs values in blocks, and each block in miniblocks that share one bit width. The parquet spec lets a writer pick the miniblock size. Parquet-java uses 32 values, and the Rust reader's skip path was tuned for that: it decoded into a small buffer sized for 32 or 64 values, and refused anything else. The source said so in a comment: "no current writers use anything else". Snowflake uses 512.

The small tables never showed it. A rewritten file has no deleted rows, so the reader never skips. Only merge-on-read creates a row selection with holes, and only a million rows made Snowflake write merge-on-read.

The bug was not ours, and it was already fixed. Databricks Photon writes 256-value miniblocks, and the same rejection was reported for those ([apache/arrow-rs#11018](https://github.com/apache/arrow-rs/issues/11018)). The fix ([#11021](https://github.com/apache/arrow-rs/pull/11021), backported in [#11754](https://github.com/apache/arrow-rs/pull/11754)) keeps the small stack buffer and skips a wide miniblock in 64-value chunks. It shipped in parquet 59.3.2. SQE was on 59.3.1. It is on 59.3.2 now, with a test that hand-encodes a page of 512-value miniblocks and skips inside one and across two, so a downgrade fails the tests.

Two commercial engines picked the same corner of the spec, and the open-source reader caught up within one patch release. That is how a shared format is supposed to work. Nobody had to wait for Snowflake or ask Databricks to change a writer.

The harness keeps both tables. Seven reads check them: an aggregate over all million rows, a range across the deleted ids, point lookups at the deleted multiples of 100000 and their neighbours, and a `SELECT *` on V3. All seven match in every mode.

The other direction works too. One of SQE's tables is merge-on-read, so SQE's DELETE on it writes a deletion vector. Snowflake reads that table and drops the deleted rows, and the compare checks it in every SQE mode. In a separate probe, Snowflake ran its own DELETE on the same table, and SQE read the result back correctly.

## 40, 40, 40

After those fixes, embedded matched 40 of 40, the single coordinator 40 of 40, and the coordinator with a worker 40 of 40. The worker scanned all 40 reads. We ran the Databricks harness again on the same build, to check that the fill, the commit changes and the parquet upgrade broke nothing there. It matched 27 of 27 in all three modes, including Databricks' own deletion vectors read on the worker.

## Names

Both engines match names exactly, but they fold unquoted names in opposite directions. Snowflake upper-cases them and SQE lower-cases them, as PostgreSQL and Trino do. So every Snowflake table needed quotes in SQE: `sf."SALES"."ORDERS"`, `"ID"`. The harness quotes every name, and nobody would write queries that way.

A catalog now sets `identifier_case = "upper"`, or `IDENTIFIER_CASE 'upper'` on `ATTACH`. SQE then folds unquoted names for that catalog the Snowflake way, so `SELECT id FROM sf.sales.orders` finds `SALES.ORDERS` and its `ID` column. `USE sf.sales` folds too. Columns fold only when every table in the statement is in an upper-case catalog. A join with a lower-case catalog folds the table names and leaves the columns to be quoted.

The fold runs before policy rewriting, so a folded query is governed exactly like the same query quoted by hand. A mask on `ssn` still covers `SSN`. A row filter is not folded, so it quotes its Snowflake column: `"REGION" = 'EU'`. An unquoted `region` there names a column that does not exist, and the query fails. It never returns the rows the filter was meant to hide.

The fold copies Snowflake's cost along with its rule. A table SQE writes with lower-case names into that catalog needs quotes in SQE, just as it does in Snowflake.

## What else the run found

None of these change a value.

- `ST_AREA` on an Iceberg `GEOMETRY` comes back negative in Snowflake, whichever engine wrote the table: -12 for the rectangle whose literal gives 12. The harness compares the absolute value.
- An external volume encrypted with a customer KMS key does not work. The credentials Horizon vends cannot decrypt, so even loading a table fails with 403. Snowflake-managed storage works.
- `$files` listed data files only, so Snowflake's delete files showed only in the `$snapshots` summary. Spark's `files` and Trino's `$files` list every live file with a `content` column, and SQE's does now too: 0 for data, 1 for position deletes and deletion vectors, 2 for equality deletes.

## Why run it by hand

The run needs a Snowflake account, a user with a key pair, and a role that may create Iceberg tables in one scratch database. None of that belongs in CI. The harness reads the account from environment variables, writes its output with the token, account and storage names redacted, and exits non-zero on any mismatch. Point it at your own account and run it before you trust either engine with the other's tables.

## One copy, three engines

The run ended with 120 compared reads against Snowflake and 81 against Databricks on one build, every one of them a value match. Two commercial data platforms and an open-source engine now write the same tables, change each other's rows, and agree on every value. What they share is the Iceberg spec, a REST catalog protocol and the parquet format. When those three are enough, your data is not in Snowflake or in Databricks or in SQE. It sits in your bucket, and each engine is a reader you can swap.

We are proud of that result. Most of the fixes it took were small, and every one of them makes SQE a better Iceberg citizen, not a better Snowflake client. The writes cope with a catalog that refuses quick commits. The worker reads files by field id whatever their column order. The parquet reader skips any miniblock width the spec allows. Each of those helps the next engine we point SQE at.

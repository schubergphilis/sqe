---
title: "Databricks and SQE read each other's tables"
description: "A live run against Databricks-hosted Unity Catalog: each engine writes Iceberg tables the other reads, both run DELETE, UPDATE and MERGE on the other's tables, and 25 reads are compared value by value. All 25 match in every SQE mode. The distributed run found a real bug first: workers ignored delete files, so rows Databricks deleted with a deletion vector came back."
pubDate: "2026-10-27"
author: "Jacob Verhoeks"
tags:
  - "iceberg"
  - "databricks"
  - "unity-catalog"
  - "interoperability"
  - "deletion-vectors"
---

Two engines that both speak Iceberg do not automatically agree on what a table contains. They agree on the spec. They disagree on timestamps without a zone, on what a deletion vector covers, on how a renamed column maps to a file written before the rename. The only way to know is to let each one write and the other one read, and then compare the rows.

This week we did that against Databricks. Unity Catalog on a Databricks workspace serves its managed Iceberg tables over the Iceberg REST protocol. SQE talks to it with the same REST client it uses for Polaris. Until now we had only a read-only smoke against Unity Catalog OSS, and the Iceberg matrix rated `unity-catalog` partial on both format versions. Both cells are full now, and SQE is at 156/162.

## No service account

The user logs in to Databricks. SQE validates that user's token against the workspace JWKS and passes the same token to Unity Catalog. Unity decides what the user may see and vends short-lived storage credentials for those tables. SQE never holds a key of its own.

```toml
[[auth.providers]]
type = "bearer_token"
jwks_url = "https://<region>.cloud.databricks.com/oidc/jwks.json"
issuer = "https://<workspace host>/oidc"
audience = "<workspace id>"
user_claim = "sub"

[catalogs.dbx]
catalog_url = "https://<workspace host>/api/2.1/unity-catalog/iceberg-rest"
warehouse = "<Unity catalog name>"
```

The token lives about an hour. For a session that runs longer, an `ATTACH` with an oauth2 secret refreshes a service principal's token on a 401. That trades the user's grants for the principal's, so it is a choice, not a default.

## The run

The harness is in `scripts/interop/databricks-uc/`, a Python driver and five SQL files. It runs in six phases.

1. Databricks creates tables through its SQL warehouse: every scalar type including `TIMESTAMP_NTZ` and `BINARY`, structs, arrays and maps, a table with liquid clustering, a table with four schema changes, a V3 table with deletion vectors, a `VARIANT` and a `GEOMETRY(4326)`.
2. SQE creates its own set in the same Unity schema: CREATE, CTAS, INSERT, UPDATE, DELETE, MERGE, ADD COLUMN, a partitioned table, V2 and V3 tables, a variant and a geometry.
3. Databricks runs DELETE, UPDATE and INSERT on SQE's tables.
4. SQE runs DELETE, UPDATE, MERGE and INSERT on Databricks' tables.
5. Twenty-five reads run on both engines, and the rows are compared value by value.
6. SQE's metadata commands run against the result: `$snapshots`, `$files`, `DESCRIBE`, `SHOW CREATE TABLE`, `information_schema`.

"No error" is not the gate. A read that returns the wrong rows passes every smoke test there is. Timestamps compare as epoch microseconds, because the Databricks statement API prints milliseconds and a text compare would hide a lost digit. Decimals compare as numbers. Geometry compares as WKT with the whitespace removed.

Phase 5 runs three times, once per SQE mode: the embedded CLI with `ATTACH`, a single coordinator, and a coordinator with one worker. For the last one the distribution thresholds are zero, so every scan goes to the worker.

## 25, 25, 23

Embedded matched 25 of 25. The single coordinator matched 25 of 25. The coordinator with a worker matched 23.

The two misses were reads of one table. SQE had written a small partitioned V3 table. Databricks had then updated one row in it, and on a V3 table Databricks writes that update as a deletion vector plus a new row. Read through the worker, both rows came back: the new one and the deleted one.

The cause was older than the run. A worker gets a scan ticket from the coordinator with a list of data files, and it reads them with a plain Parquet reader. The ticket has no field for delete files. It never had. So a worker applied no position delete, no deletion vector and no equality delete, on any table, ever. The single coordinator was correct because it reads through iceberg-rust, which applies them.

Two things had hidden it. The distribution thresholds keep small tables on the coordinator, and most merge-on-read tables in our tests are small. And the other deletion-vector tables in this run matched because SQE's own copy-on-write DML had already rewritten their files, which dropped the vectors. The bug needed a table that a second engine had deleted from, read through a worker, with nothing in between.

## The fix

One check in the distribution decision. A snapshot whose manifest list has delete manifests stays on the coordinator. An error reading the manifest list counts as "has deletes", so a failure fails safe.

The regression test writes a V2 table with position deletes and a V3 table with deletion vectors through an in-process worker. Before the delete, a control read asserts that the scan does reach the worker, so the test cannot pass by never distributing. After the delete, the deleted rows must stay gone. With the check disabled, the test fails the same way the run did: four rows instead of two.

After the fix, the distributed run matched 25 of 25.

The cost is real. A merge-on-read table with live deletes now scans on one node until it is compacted. The proper fix carries the delete files in the ticket and applies them on the worker. That is the next change.

## What else the run found

None of these change a value, and all of them are on the list.

- The embedded CLI only reads an attached catalog yet. It refuses UPDATE, DELETE, MERGE and ALTER, and its CREATE TABLE takes plain columns only. Writes go through the coordinator. (An earlier version of this post said DML worked embedded. It does not: the run only ever wrote through the coordinator.)
- `DESCRIBE` shows a Databricks `GEOMETRY` column as `varbinary`. The values still read as geometry, and `ST_AsText` and `ST_Area` match Databricks.
- `SHOW CREATE TABLE` prints Unity's internal table properties, including the storage paths, and no clustering clause.
- SQE has no `ST_SRID`, so the run cannot compare the SRID column for column.
- A Delta table in the same schema is refused with "not an Iceberg table". SQE reads Iceberg only, and now it says so instead of "not found".

Databricks had its own surprises. Liquid clustering on a managed Iceberg table refuses to start unless deletion vectors and row tracking are switched off for that table. Its `element_at` raises an error on an index past the end of an array, where SQE returns NULL, so the harness uses `try_element_at`.

## Why run it by hand

The run needs a workspace, a SQL warehouse and a user who may create tables in a scratch schema. None of that belongs in CI. The harness takes the workspace from environment variables, writes its output to a scratch directory with the token redacted, and exits non-zero on any mismatch. Point it at your own workspace and run it before you trust either engine with the other's tables.

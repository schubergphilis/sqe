---
slug: glue-lake-formation
title: "AWS Glue + Lake Formation"
description: "Run SQE against a Lake-Formation-governed AWS Glue database. A CDK stack creates the database (so LF governs it); the run shows SQE denied until an explicit LF grant, then succeeding. Table/database-level LF permissions, not column/row masking."
---

# AWS Glue + Lake Formation

Point SQE at an AWS Glue database that **Lake Formation governs**. Unlike the
[`aws-glue` quickstart](./aws-glue.md), which lets SQE create the database
(making the caller its owner to sidestep LF), here the database is created by
CloudFormation. In a Lake-Formation-enabled account that means it is governed
with no grants, so SQE is **denied** until you grant it LF permissions
explicitly. The run shows the denial, the grant, and the same statement
succeeding.

## How it works

- A **TypeScript CDK stack** creates both an S3 warehouse bucket and an
  LF-governed Glue database. Because CloudFormation creates the database, Lake
  Formation governs it with no permissions granted by default.
- **Phase A**: `run.sh` starts SQE and runs `queries.sql`. `CREATE TABLE` fails
  with an LF `AccessDeniedException` — the principal has no LF permission on the
  database.
- **Grant**: `run.sh` calls `aws lakeformation grant-permissions` to give the
  principal `CREATE_TABLE`, `ALTER`, `DROP`, and `DESCRIBE` on the database.
- **Phase B**: `run.sh` restarts SQE (to flush any cached state) and runs the
  same queries. `CREATE TABLE` → `INSERT` → `SELECT` all succeed.
- CDK destroy removes the database, bucket, and LF grant. Nothing is left behind.

Note the boundary: this quickstart demonstrates **table- and database-level** LF
permission gating. SQE reads Iceberg data files from S3 directly with the
caller's IAM credentials; it does not call Lake Formation's filtered
credential-vending for column masking or row filtering. SQE's own column/row
masking is a separate policy engine (OPA/Cedar plan rewriting).

## What it demonstrates

- LF enforcement in action: `CREATE TABLE` denied before the grant, succeeding
  after.
- The deny → grant → succeed arc captured in a single `run.sh` run.
- Full create/write/read round-trip in Phase B: `CREATE TABLE` → `INSERT` →
  `SELECT … GROUP BY`.
- Clean teardown: no stack, database, bucket, or LF grants left in the account.

**Status:** validated (2026-06-07).

## Run it

Full config, CDK stack, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/glue-lake-formation/](https://github.com/schubergphilis/sqe/tree/main/quickstart/glue-lake-formation/)**

```bash
cd quickstart/glue-lake-formation
cp .env.example .env
./run.sh
```

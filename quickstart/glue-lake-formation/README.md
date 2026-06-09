---
slug: glue-lake-formation
title: "AWS Glue + Lake Formation"
description: "Run SQE against a Lake-Formation-governed AWS Glue database. A CDK stack creates the database (so LF governs it); the run shows SQE denied until an explicit LF grant, then succeeding. Table/database-level LF permissions, not column/row masking."
---

# AWS Glue + Lake Formation

Point SQE at an AWS Glue database that **Lake Formation governs**. Unlike the
[`aws-glue`](../aws-glue/) quickstart, which lets SQE create the database (making
the caller its owner to side-step LF), here the database is created by
CloudFormation. In a Lake-Formation-enabled account that means it is governed
with no grants, so SQE is **denied** until you grant it LF permissions
explicitly. The run shows the denial, the grant, and the same statement
succeeding.

## What this shows, and what it does not

Be precise about the boundary, because it is easy to overclaim.

- **What LF governs here:** the Glue **catalog** operations SQE calls
  (`CreateTable`, `GetTable`, ...). Those go through the Glue API, which Lake
  Formation gates. Grant the principal `CREATE_TABLE` on the database and the
  call succeeds; without it, `AccessDeniedException`.
- **What it does not do:** SQE does **not** enforce LF column-masking or
  row-filtering. SQE reads Iceberg data files straight from S3 with the caller's
  IAM credentials; it never calls LF's filtered credential-vending
  (`GetUnfilteredTableMetadata`). So "fine-grained" here means **table /
  database-level permission gating**, not cell-level filtering.
- **SQE's own fine-grained path** (column masks, row filters) is the policy
  engine: OPA/Cedar plan rewriting, applied before execution. That is a separate
  mechanism, independent of the catalog. See the security-policy docs.

## What you get

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 warehouse bucket **and** an LF-governed Glue database `sqe_lf_quickstart`. `cdk destroy` removes both. |
| `docker-compose.yml` | Just the SQE coordinator (glue backend), AWS creds via env. |
| `sqe.toml` | Annotated config; `run.sh` fills in the bucket + region. |

## Prerequisites

- Docker, Node + `npx`, and AWS CLI v2.
- AWS credentials whose principal is a **Lake Formation data-lake admin** (so it
  can grant), with Glue + S3 permissions. Set `AWS_PROFILE` / `AWS_REGION`.
- The account must be **cdk-bootstrapped** once (`cdk bootstrap`).
- An SQE image with the Glue write fix (in `main`):
  `docker build -t sqe-quickstart:latest .` from the repo root.

## Run it

```bash
cd quickstart/glue-lake-formation
cp .env.example .env          # set AWS_PROFILE / AWS_REGION
./run.sh
```

`run.sh` does the whole loop: `cdk deploy` (bucket + LF-governed database) ->
start SQE -> **Phase A** run [`queries.sql`](./queries.sql) and capture the LF
denial -> `grant-permissions` -> restart SQE -> **Phase B** run the same
statements and capture the success -> write [`OUTPUT.md`](./OUTPUT.md) -> drop
the table -> `cdk destroy`. Use `./run.sh --keep` to leave it up, `./run.sh
--destroy` to tear down.

## The grant

```bash
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=<your-principal-arn> \
  --resource '{"Database":{"Name":"sqe_lf_quickstart"}}' \
  --permissions CREATE_TABLE ALTER DROP DESCRIBE
```

Granting at the **database** level lets the principal create tables; as the
creator it owns the resulting table, so `INSERT` and `SELECT` follow. The S3
bucket is not LF-registered, so data writes use ordinary IAM access control.

## Output

Captured live against AWS Glue + Lake Formation (`./run.sh`), in
[`OUTPUT.md`](./OUTPUT.md). Phase A denies:

```
AccessDeniedException: Insufficient Lake Formation permission(s):
Required Create Table on sqe_lf_quickstart
```

Phase B, after the grant, succeeds:

```
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
```

## How it is tested

`run.sh` runs the full denial -> grant -> success arc against a real
LF-governed Glue database, then tears the resources down. Validated live
2026-06-07 (account `123456789012`, eu-example-1): Phase A returned the LF
`AccessDeniedException`; after `grant-permissions` Phase B did CREATE TABLE ->
INSERT -> SELECT cleanly; teardown left no stack, database, bucket, or LF grant
(verified with `describe-stacks` / `get-database` / `s3 ls` / `list-permissions`).

## Gotchas

- **Caller must be an LF admin.** `grant-permissions` requires it. Check with
  `aws lakeformation get-data-lake-settings`.
- **IAMAllowedPrincipals default.** If the account still grants
  `IAMAllowedPrincipals` by default, LF is not enforcing and Phase A will not
  deny. This quickstart assumes LF enforcement is on
  (`CreateDatabaseDefaultPermissions` empty).
- **Restart between phases.** `run.sh` restarts SQE after the grant so no stale
  catalog state masks the change.
- **Table must be dropped before destroy.** The database is CloudFormation-owned
  but the table is not; `run.sh` deletes the table so `cdk destroy` can remove
  the database.

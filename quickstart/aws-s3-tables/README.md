---
slug: aws-s3-tables
title: "AWS S3 Tables (managed Iceberg)"
description: "Run SQE against AWS S3 Tables, AWS's managed Iceberg service (metadata + storage in one). A CDK stack bootstraps a throwaway table bucket and tears it down; SQE creates the namespace and does a full create/write/read round-trip over the AWS SDK."
---

# AWS S3 Tables (managed Iceberg)

Point SQE at **AWS S3 Tables**, AWS's managed Iceberg product. Unlike Glue
(metadata only), S3 Tables bundles the catalog *and* the storage into one
service: you create a *table bucket*, and namespaces + tables live inside it.
SQE talks to it over the AWS SDK with your IAM credentials.

A small CDK stack bootstraps the throwaway table bucket and tears it down, so the
quickstart leaves nothing behind.

## What you get

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 Tables table bucket (`cdk deploy`) and removes it (`cdk destroy`). |
| `docker-compose.yml` | Just the SQE coordinator (s3tables backend), AWS creds via env. |
| `sqe.toml` | Annotated config; `run.sh` fills in the table-bucket ARN + region. |

## Prerequisites

- Docker, Node + `npx`, AWS CLI v2.
- AWS credentials (`AWS_PROFILE` or `AWS_*` env) with S3 Tables permissions, in a
  region where S3 Tables is available.
- A **cdk-bootstrapped** account.
- The SQE image must include the Glue/non-REST write fix
  ([!286](https://github.com/schubergphilis/sqe),
  in `main`) -- S3 Tables is a non-REST catalog, so `CREATE TABLE` hit the same
  reserved-`format-version` bug before that fix. Build: `docker build -t sqe-quickstart:latest .`.

## Run it

```bash
cd quickstart/aws-s3-tables
cp .env.example .env          # set AWS_PROFILE / AWS_REGION
./run.sh
```

`run.sh`: `cdk deploy` (table bucket) -> start SQE -> run [`queries.sql`](./queries.sql)
(create namespace + table, insert, aggregate) -> capture [`OUTPUT.md`](./OUTPUT.md)
-> delete the table + namespace -> `cdk destroy`. `--keep` leaves it up; `--destroy`
tears down.

## Configuration

```toml
[catalog.backend]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:<region>:<account>:bucket/<name>"  # run.sh fills this in
```

The s3tables backend registers under the SQL catalog name `iceberg`, so tables
are `iceberg.<namespace>.<table>`. SQE creates the namespace itself
(`CREATE SCHEMA`), which makes the caller its owner -- the same Lake-Formation-safe
pattern as the [aws-glue](../aws-glue/) quickstart. Auth is the `anonymous` dev
provider (S3 Tables authenticates via AWS IAM).

## Output

Captured live against AWS S3 Tables (`./run.sh`), in [`OUTPUT.md`](./OUTPUT.md):

```
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
```

## How it is tested

`run.sh` runs the full create/write/read round-trip against a real S3 Tables
table bucket and asserts the queries succeed, then deletes the table + namespace
and destroys the bucket. Validated live 2026-06-06 (account `ACCOUNT_ID`,
eu-example-1): CREATE SCHEMA -> CREATE TABLE -> INSERT -> SELECT, then a clean
teardown (no leftover stack, bucket, namespace, or table).

## Gotchas

- **S3 Tables will not delete a non-empty table bucket**, so `run.sh` deletes the
  SQE-created table + namespace before `cdk destroy` (the teardown does this
  idempotently).
- **Table-bucket name** is account+region-scoped (not globally unique like S3);
  `sqe-s3tables-quickstart` is fine unless you already have one by that name.
- **Stale image** -> the write fix (!286) must be in your image; rebuild from `main`.
- **Region availability** -> S3 Tables is not in every region; pick a supported one.

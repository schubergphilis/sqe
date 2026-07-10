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
./run.sh                      # deploy -> queries -> capture -> tear down
./run.sh --keep               # same, but leave SQE up and the bucket deployed
./run.sh --destroy            # just tear down (empty + delete the table bucket)
```

`run.sh`: `cdk deploy` (table bucket) -> start SQE -> run [`queries.sql`](./queries.sql)
(create namespace + table, insert, aggregate) -> capture [`OUTPUT.md`](./OUTPUT.md)
-> delete the table + namespace -> `cdk destroy`. There is no `--check` mode (the
run asserts the queries succeed, then destroys the live AWS resources). `--keep`
leaves it up; `--destroy` runs the teardown on its own.

## How it works

S3 Tables is AWS's managed Iceberg product, and it bundles the catalog and the
storage into one service. You create a *table bucket* and namespaces + tables
live inside it; there is no separate Glue database or S3 warehouse bucket to
wire up. SQE talks to it over the AWS SDK with the IAM credentials passed into
the container env (resolved from your `AWS_PROFILE` by `run.sh`).

S3 Tables is a non-REST catalog backend, the same code path as Glue. SQE creates
the namespace itself via `CREATE SCHEMA`, which makes the calling principal its
owner, then creates the table, inserts, and reads back, all through the S3
Tables API.

## Configuration explained

### `sqe.toml` (the engine, templated)

`run.sh` copies `sqe.toml` to `sqe.toml.local`, substitutes
`__TABLE_BUCKET_ARN__` and `__REGION__` from the CDK outputs, and compose mounts
the result. The catalog block:

```toml
[catalog.backend]
type = "s3tables"
table_bucket_arn = "__TABLE_BUCKET_ARN__"   # arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME
```

- `type = "s3tables"` selects the managed S3 Tables backend. It registers under
  the legacy SQL catalog name `iceberg`, so tables are
  `iceberg.<namespace>.<table>`.
- `table_bucket_arn` is the only catalog coordinate it needs, because S3 Tables
  is metadata + storage in one. The CDK stack creates the bucket and `run.sh`
  fills the ARN in.
- `[catalog] polaris_url = "https://placeholder.invalid"` is the legacy
  single-catalog block the deserialiser still requires; the s3tables backend
  ignores it.
- `[storage] s3_region = "__REGION__"` is all the storage config needed: S3
  Tables manages its own storage, so there is no endpoint or bucket to set.
- Auth is the `anonymous` dev provider; S3 Tables authenticates via AWS IAM.

### `.env.example`

`AWS_PROFILE` / `AWS_REGION` (whose credentials and which region), the offset
host ports, and `SQE_IMAGE` (must include the non-REST write fix that S3 Tables
needs).

### `docker-compose.yml`

Runs only the SQE coordinator, passes the resolved AWS credentials into the
container env, mounts `sqe.toml.local` + `queries.sql`, and exposes the offset
Flight SQL / Trino HTTP ports.

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

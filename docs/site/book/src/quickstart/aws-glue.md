---
slug: aws-glue
title: "AWS Glue Data Catalog"
description: "Run SQE against the AWS Glue Data Catalog with S3 as storage. A CDK stack bootstraps a throwaway S3 warehouse bucket and tears it down; SQE creates the database and does a full create/write/read round-trip over the AWS SDK."
---

# AWS Glue Data Catalog

Point SQE at the **AWS Glue Data Catalog**. Glue is the catalog (table metadata)
and S3 is the storage; SQE talks to both over the AWS SDK using your IAM
credentials. No Polaris, no Keycloak, no local object store.

A small CDK stack bootstraps a throwaway S3 warehouse bucket and tears it down
at the end, so the quickstart leaves nothing behind in your account.

## How it works

- A **TypeScript CDK stack** creates an S3 bucket to use as the Iceberg
  warehouse. The bucket is removed on CDK destroy (including any Iceberg data
  inside it).
- **SQE** uses the `glue` catalog backend, configured with your AWS region and
  the S3 warehouse path. Both are injected at runtime by `run.sh`.
- AWS IAM credentials authenticate all Glue catalog and S3 storage operations.
- SQE creates the Glue database with `CREATE SCHEMA`, making the calling
  principal its owner. This is deliberate: in a Lake Formation-enabled account, a
  database created out-of-band is LF-governed with no grants, which blocks
  `CREATE TABLE`. By creating the database itself, SQE avoids that. See the
  [glue-lake-formation quickstart](./glue-lake-formation.md) for the governed
  variant.
- `run.sh` runs the full loop: CDK deploy, then start SQE, then run queries, then capture
  output, then drop the Glue database, then CDK destroy.

## What it demonstrates

- SQE connecting to AWS Glue as a non-REST Iceberg catalog with S3 storage.
- Full create/write/read round-trip: `CREATE SCHEMA`, then `CREATE TABLE`, then
  `INSERT`, then `SELECT … GROUP BY`, all against live Glue + S3.
- Clean teardown: the S3 bucket (and all Iceberg data) and the Glue database are
  removed; no resources left in the account.

**Status:** validated (2026-06-06).

## Run it

Full config, CDK stack, `docker compose`, queries, and captured output are in the repo:

**See: [quickstart/aws-glue/](https://github.com/schubergphilis/sqe/tree/main/quickstart/aws-glue/)**

```bash
cd quickstart/aws-glue
cp .env.example .env
./run.sh
```

---
slug: aws-s3-tables
title: "AWS S3 Tables (managed Iceberg)"
description: "Run SQE against AWS S3 Tables, AWS's managed Iceberg service (metadata + storage in one). A CDK stack bootstraps a throwaway table bucket and tears it down; SQE creates the namespace and does a full create/write/read round-trip over the AWS SDK."
---

# AWS S3 Tables (managed Iceberg)

Point SQE at **AWS S3 Tables**, AWS's managed Iceberg product. Unlike Glue
(metadata only), S3 Tables bundles the catalog and the storage into one service:
you create a table bucket, and namespaces and tables live inside it. SQE talks
to it over the AWS SDK with your IAM credentials.

A small CDK stack bootstraps the throwaway table bucket and tears it down at the
end, so the quickstart leaves nothing behind in your account.

## How it works

- A **TypeScript CDK stack** creates an S3 Tables table bucket in your AWS
  account on deploy and removes it on destroy.
- **SQE** uses the `s3tables` catalog backend, configured with the table bucket
  ARN. The bucket ARN and your AWS region are injected at runtime by `run.sh`.
- AWS IAM credentials (via `AWS_PROFILE` or environment variables) authenticate
  all catalog and storage operations — no separate identity provider.
- SQE creates the namespace itself with `CREATE SCHEMA`, which makes the calling
  principal its owner and avoids Lake Formation permission conflicts.
- `run.sh` runs the full loop: CDK deploy → start SQE → run queries → capture
  output → delete table and namespace → CDK destroy.

## What it demonstrates

- SQE connecting to AWS S3 Tables as a managed, non-REST Iceberg catalog.
- Full create/write/read round-trip: `CREATE SCHEMA` → `CREATE TABLE` →
  `INSERT` → `SELECT … GROUP BY`, all against live S3 Tables.
- Clean teardown: the table bucket, namespace, and table are all removed; no
  resources left in the account.

**Status:** validated (2026-06-06).

## Run it

Full config, CDK stack, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/aws-s3-tables/](https://github.com/schubergphilis/sqe/tree/main/quickstart/aws-s3-tables/)**

```bash
cd quickstart/aws-s3-tables
cp .env.example .env
./run.sh
```

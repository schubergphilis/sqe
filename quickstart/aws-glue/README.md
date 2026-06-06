---
slug: aws-glue
title: "AWS Glue Data Catalog"
description: "Run SQE against the AWS Glue Data Catalog with S3 as storage. A CDK stack bootstraps a throwaway S3 warehouse bucket and tears it down; SQE creates the database and does a full create/write/read round-trip over the AWS SDK."
---

# AWS Glue Data Catalog

Point SQE at the **AWS Glue Data Catalog**. Glue is the catalog (table metadata)
and S3 is the storage; SQE talks to both over the AWS SDK using your IAM
credentials. No Polaris, no Keycloak, no RustFS.

A small CDK stack bootstraps the throwaway test resource (an S3 warehouse
bucket) and tears it back down, so the quickstart leaves nothing behind.

## What you get

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 warehouse bucket (`cdk deploy`) and removes it (`cdk destroy`). |
| `docker-compose.yml` | Just the SQE coordinator (glue backend), AWS creds via env. |
| `sqe.toml` | Annotated config; `run.sh` fills in the bucket + region. |

## Prerequisites

- Docker, Node + `npx`, and AWS CLI v2.
- AWS credentials for the target account (`AWS_PROFILE`, or `AWS_*` env vars)
  with Glue + S3 permissions.
- The account must be **cdk-bootstrapped** once (`cdk bootstrap`); most are.
- The SQE image must include the Glue write fix
  ([MR !286](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/286),
  now in `main`). Build it: `docker build -t sqe-quickstart:latest .` from the repo root.

## Run it

```bash
cd quickstart/aws-glue
cp .env.example .env          # set AWS_PROFILE / AWS_REGION
./run.sh
```

`run.sh` does the whole loop: `cdk deploy` (S3 bucket) -> start SQE -> run
[`queries.sql`](./queries.sql) (create DB + table, insert, aggregate) -> capture
[`OUTPUT.md`](./OUTPUT.md) -> stop SQE -> drop the Glue database -> `cdk destroy`.
Use `./run.sh --keep` to leave it running, `./run.sh --destroy` to tear down.

## Configuration

```toml
[catalog.backend]
type = "glue"
region = "<your-region>"
warehouse = "s3://<bucket>/"   # the CDK-created bucket; run.sh fills this in
```

The glue backend registers under the SQL catalog name `iceberg`, so tables are
`iceberg.<glue_database>.<table>`. Auth is the `anonymous` dev provider (Glue
authenticates via AWS IAM, not a user token); for real multi-user auth put SQE
behind Keycloak while the catalog still uses IAM.

## Why SQE creates the database (Lake Formation)

The CDK stack creates **only the S3 bucket**, not the Glue database. SQE creates
the database itself via `CREATE SCHEMA`. That is deliberate: in a
**Lake-Formation-enabled** account, a database created out-of-band (by
CloudFormation) is LF-governed with no grants, so even a Lake Formation admin is
denied `Create Table` on it:

```
AccessDeniedException: Insufficient Lake Formation permission(s):
Required Create Table on sqe_glue_quickstart
```

A database that **SQE creates** makes the calling principal its owner, which
carries the create/alter permissions. Regular S3 is not an LF-registered data
location, so table data writes use ordinary IAM access control. Net: this works
the same with or without Lake Formation. The dedicated `glue-lake-formation`
quickstart demonstrates explicit fine-grained LF grants.

## Output

Captured live against AWS Glue (`./run.sh`), in [`OUTPUT.md`](./OUTPUT.md):

```
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
```

## How it is tested

`run.sh` runs the full create/write/read round-trip against a real Glue catalog
+ S3 bucket and asserts the queries succeed, then tears the resources down.
Validated live 2026-06-06 (account `ACCOUNT_ID`, eu-central-1): CREATE SCHEMA ->
CREATE TABLE -> INSERT -> SELECT, then a clean `cdk destroy` (no leftover stack,
bucket, or database).

## Gotchas

- **CREATE TABLE denied by Lake Formation** -> let SQE create the database (this
  quickstart does); do not pre-create it in CDK/Glue.
- **Stale image** -> the Glue write fix must be in your image; rebuild from `main`.
- **Bucket not empty on destroy** -> the CDK bucket has `autoDeleteObjects`, so
  `cdk destroy` empties it; the Iceberg metadata/data go with it.
- **CDK not bootstrapped** -> run `cdk bootstrap` once for the account/region.

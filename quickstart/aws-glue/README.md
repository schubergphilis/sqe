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
  ([an earlier change](https://github.com/schubergphilis/sqe),
  now in `main`). Build it: `docker build -t sqe-quickstart:latest .` from the repo root.

## Run it

```bash
cd quickstart/aws-glue
cp .env.example .env          # set AWS_PROFILE / AWS_REGION
./run.sh                      # deploy -> queries -> capture -> tear down
./run.sh --keep               # same, but leave SQE up and the CDK stack deployed
./run.sh --destroy            # just tear down (compose down + drop DB + cdk destroy)
```

`run.sh` does the whole loop: `cdk deploy` (S3 bucket) -> start SQE -> run
[`queries.sql`](./queries.sql) (create DB + table, insert, aggregate) -> capture
[`OUTPUT.md`](./OUTPUT.md) -> stop SQE -> drop the Glue database -> `cdk destroy`.
This scenario has no `--check` mode (the run itself asserts the queries succeed
and then destroys the live AWS resources). `--keep` leaves it running so you can
connect from a client; `--destroy` runs the teardown on its own.

## How it works

SQE talks to two AWS services over the AWS SDK using your IAM credentials: Glue
for the catalog (table metadata) and S3 for the storage (the Iceberg data
files). There is no Polaris, Keycloak, or RustFS. The credentials flow from your
`AWS_PROFILE` into the container env as `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, and SQE's SDK clients pick them up
from there.

The catalog operations (create database, create table, get table) go through the
Glue API. The data writes go straight to the S3 bucket. SQE does the
`CREATE SCHEMA` itself rather than letting CDK create the database, which matters
under Lake Formation (see below).

## Configuration explained

### `sqe.toml` (the engine, templated)

`run.sh` copies `sqe.toml` to `sqe.toml.local`, substitutes `__WAREHOUSE__` and
`__REGION__` from the CDK stack outputs, and `docker-compose.yml` mounts the
filled-in file. The catalog block is the part that matters:

```toml
[catalog.backend]
type = "glue"
region = "__REGION__"           # run.sh fills in your region
warehouse = "__WAREHOUSE__"     # s3://<bucket>/ created by the CDK stack
```

- `type = "glue"` selects the Glue catalog backend (not REST). It registers
  under the legacy SQL catalog name `iceberg`, so tables are
  `iceberg.<glue_database>.<table>`.
- `warehouse` is the S3 location Glue writes new table data under, set to the
  CDK-created bucket.
- `[catalog] polaris_url = "https://placeholder.invalid"` is a legacy
  single-catalog block the config deserialiser still requires; the glue backend
  ignores it.
- `[storage] s3_path_style = false` uses virtual-host S3 addressing (real AWS
  S3, no endpoint override), unlike the RustFS quickstarts which set path-style.
- Auth is the `anonymous` dev provider: Glue authenticates via AWS IAM, not a
  user token, so there is no IdP. For real multi-user auth put SQE behind
  Keycloak while the catalog still uses IAM.

AWS credentials are deliberately **not** in `sqe.toml`. They come from the SDK
provider chain via the container env.

### `.env.example`

Sets `AWS_PROFILE` and `AWS_REGION` (which profile and region `run.sh` resolves
credentials from), the offset host ports (`SQE_FLIGHT_PORT`, `SQE_TRINO_PORT`),
and `SQE_IMAGE` (build from source on first `up`, or point at an existing image
that includes the Glue write fix).

### `docker-compose.yml`

Runs only the SQE coordinator. It passes the resolved AWS credentials into the
container env, mounts `sqe.toml.local` and `queries.sql`, and exposes Flight SQL
and the Trino HTTP port on the offset host ports. The `AWS_ACCESS_KEY_ID:?...`
syntax makes compose fail loudly if `run.sh` did not export the credentials.

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
the same with or without Lake Formation. The dedicated
[`glue-lake-formation`](../glue-lake-formation/) quickstart keeps the database
LF-governed and grants the principal explicit LF permissions instead. That is
table/database-level permission gating: SQE does not enforce LF column-masking
or row-filtering (it reads S3 directly with the caller's credentials). SQE's own
column/row masking is the OPA/Cedar policy engine, not Lake Formation.

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
Validated live 2026-06-06 (account `ACCOUNT_ID`, eu-example-1): CREATE SCHEMA ->
CREATE TABLE -> INSERT -> SELECT, then a clean `cdk destroy` (no leftover stack,
bucket, or database).

## Gotchas

- **CREATE TABLE denied by Lake Formation** -> let SQE create the database (this
  quickstart does); do not pre-create it in CDK/Glue.
- **Stale image** -> the Glue write fix must be in your image; rebuild from `main`.
- **Bucket not empty on destroy** -> the CDK bucket has `autoDeleteObjects`, so
  `cdk destroy` empties it; the Iceberg metadata/data go with it.
- **CDK not bootstrapped** -> run `cdk bootstrap` once for the account/region.

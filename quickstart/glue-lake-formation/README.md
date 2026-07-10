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
./run.sh                      # deploy -> denied -> grant -> succeeds -> tear down
./run.sh --keep               # same, but leave SQE up and the stack deployed
./run.sh --destroy            # just tear down (compose down + drop table + cdk destroy)
```

`run.sh` does the whole loop: `cdk deploy` (bucket + LF-governed database) ->
start SQE -> **Phase A** run [`queries.sql`](./queries.sql) and capture the LF
denial -> `grant-permissions` -> restart SQE -> **Phase B** run the same
statements and capture the success -> write [`OUTPUT.md`](./OUTPUT.md) -> drop
the table -> `cdk destroy`. There is no `--check` mode (the two-phase run is the
assertion, and it destroys the live AWS resources). `--keep` leaves it up;
`--destroy` runs the teardown on its own.

## How it works

SQE talks to Glue (catalog) and S3 (storage) over the AWS SDK with your IAM
credentials, exactly like the [`aws-glue`](../aws-glue/) quickstart. The
difference is who creates the database. Here the CDK stack creates it, so in a
Lake-Formation-enabled account it is governed with no grants. Glue catalog calls
(`CreateTable`, `GetTable`) go through the Glue API, which Lake Formation gates,
so SQE is denied until the principal holds an LF grant.

`run.sh` proves the arc in two phases against the same database. Phase A runs the
queries with no grant and captures the `AccessDeniedException`. The script then
grants the principal `CREATE_TABLE`/`ALTER`/`DROP`/`DESCRIBE` at the database
level, restarts SQE to clear stale catalog state, and Phase B runs the identical
statements, which now succeed. Data writes still go straight to S3 under ordinary
IAM, because the bucket is not LF-registered.

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

## Configuration explained

### `sqe.toml` (the engine, templated)

The config is identical to the `aws-glue` quickstart: a glue catalog backend
over AWS IAM. The difference is operational, not in the file. `run.sh` copies
`sqe.toml` to `sqe.toml.local`, fills in `__WAREHOUSE__` / `__REGION__` from the
CDK outputs, and compose mounts it.

```toml
[catalog.backend]
type = "glue"
region = "__REGION__"
warehouse = "__WAREHOUSE__"     # s3://<bucket>/ created by the CDK stack
```

- `type = "glue"` registers the catalog under the SQL name `iceberg`, so tables
  are `iceberg.sqe_lf_quickstart.<table>`.
- `[catalog] polaris_url = "https://placeholder.invalid"` is the legacy block the
  deserialiser requires; the glue backend ignores it.
- `[storage] s3_path_style = false` uses virtual-host S3 addressing against real
  AWS S3.
- Auth is the `anonymous` dev provider; the Glue API authenticates the SDK calls
  via AWS IAM, and Lake Formation gates them on top of that.

### `.env.example`

`AWS_PROFILE` must be a principal that is a **Lake Formation data-lake admin**
(otherwise `grant-permissions` in `run.sh` fails). Also sets `AWS_REGION`, the
offset host ports, and `SQE_IMAGE`.

### `docker-compose.yml`

Runs only the SQE coordinator with the resolved AWS credentials in its env. The
two-phase flow uses `docker compose up -d --force-recreate sqe` between phases so
the second run starts from clean catalog state and sees the new LF grant.

### CDK (`cdk/`)

Unlike the `aws-glue` quickstart, the CDK stack here creates the Glue database
`sqe_lf_quickstart` itself (not just the bucket). That is the whole point: a
CloudFormation-created database is LF-governed with no grants, which is what
makes Phase A deny.

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

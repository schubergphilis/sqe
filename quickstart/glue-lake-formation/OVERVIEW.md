# AWS Glue + Lake Formation

## Goal

Point SQE at an AWS Glue database that Lake Formation governs. Unlike the `aws-glue` quickstart (which lets SQE create the database, making the caller its owner to side-step LF), here the database is created by CloudFormation. In a Lake-Formation-enabled account that means it is governed with no grants, so SQE is denied until the principal is granted LF permissions explicitly.

The run demonstrates the full arc: denial, the grant, and the same statements succeeding. Be precise about the boundary: LF governs the Glue catalog operations SQE calls (`CreateTable`, `GetTable`). SQE reads Iceberg data files straight from S3 with the caller's IAM credentials and does not enforce LF column-masking or row-filtering. Fine-grained access here means table/database-level permission gating, not cell-level filtering. SQE's own column/row masking is the OPA/Cedar policy engine, independent of the catalog.

## Components

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 warehouse bucket **and** an LF-governed Glue database `sqe_lf_quickstart` (`cdk deploy`). `cdk destroy` removes both. |
| `docker-compose.yml` | Runs just the SQE coordinator with the glue backend; AWS credentials passed via env. |
| `sqe.toml` | Annotated config template; `run.sh` fills in the bucket URI and region. |

## Configuration

### Backend (sqe.toml)

```toml
[catalog.backend]
type = "glue"
region = "__REGION__"
warehouse = "__WAREHOUSE__"   # s3://<bucket>/ from CDK outputs; run.sh fills this in

[storage]
s3_region = "__REGION__"
s3_path_style = false

[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]
```

Config is identical to `aws-glue`; the difference is operational. The database is created by CloudFormation (not SQE), so Lake Formation governs it with no grants until one is added explicitly.

### SQL (queries.sql)

```sql
-- No CREATE SCHEMA: the database already exists (CloudFormation made it).
-- Phase A: denied by LF. Phase B (after the grant): succeeds.
CREATE TABLE iceberg.sqe_lf_quickstart.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.sqe_lf_quickstart.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.sqe_lf_quickstart.events
GROUP BY kind
ORDER BY total DESC;
```

## The test

`run.sh` runs the full denial → grant → success arc against a real LF-governed Glue database. It: deploys the CDK stack (S3 bucket + LF-governed Glue database) → starts SQE → **Phase A**: executes `queries.sql` and captures the LF denial → issues `aws lakeformation grant-permissions` (`CREATE_TABLE ALTER DROP DESCRIBE` on the database) → restarts SQE to clear any stale catalog state → **Phase B**: re-runs the same statements and captures success → drops the SQE-created table (so CDK can delete the database) → `cdk destroy` (also revokes the LF grant).

The caller must be a Lake Formation data-lake admin. The account must have LF enforcement on (`CreateDatabaseDefaultPermissions` empty); if `IAMAllowedPrincipals` is still the default, Phase A will not produce a denial.

Validated live 2026-06-07 (account `123456789012`, eu-example-1): Phase A returned the LF `AccessDeniedException`; Phase B did CREATE TABLE → INSERT → SELECT cleanly; teardown left no stack, database, bucket, or LF grant.

## Output

```
## Phase A -- before the LF grant: Lake Formation denies CREATE TABLE

AccessDeniedException: Insufficient Lake Formation permission(s):
Required Create Table on sqe_lf_quickstart

## Phase B -- after the LF grant: the same statements succeed

sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
(0 rows)
(0 rows)
(2 rows)
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
```

# AWS Glue Data Catalog

## Goal

Point SQE at the AWS Glue Data Catalog with S3 as storage. Glue is the catalog (table metadata) and S3 is the storage; SQE talks to both over the AWS SDK using your IAM credentials. No Polaris, no Keycloak, no RustFS.

A small CDK stack bootstraps the throwaway S3 warehouse bucket and tears it back down after the run, so the quickstart leaves nothing behind.

## Components

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 warehouse bucket (`cdk deploy`) and removes it (`cdk destroy`). |
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

The glue backend registers under the SQL catalog name `iceberg`, so tables are `iceberg.<glue_database>.<table>`. Auth is the `anonymous` dev provider; Glue authenticates via AWS IAM. For real multi-user auth, put SQE behind Keycloak while the catalog still uses IAM.

### SQL (queries.sql)

```sql
-- SQE creates the Glue database (makes the caller its owner — Lake Formation safe)
CREATE SCHEMA IF NOT EXISTS iceberg.sqe_glue_quickstart;

DROP TABLE IF EXISTS iceberg.sqe_glue_quickstart.events;
CREATE TABLE iceberg.sqe_glue_quickstart.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.sqe_glue_quickstart.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.sqe_glue_quickstart.events
GROUP BY kind
ORDER BY total DESC;
```

## The test

`run.sh` runs the full create/write/read round-trip against a real Glue catalog and S3 bucket. It: deploys the CDK stack (S3 bucket only) → generates `sqe.toml.local` from the stack outputs → starts SQE → executes `queries.sql` (CREATE SCHEMA → CREATE TABLE → INSERT → SELECT) and captures output to `OUTPUT.md` → stops SQE → drops the Glue database → `cdk destroy`.

SQE creates the Glue database via `CREATE SCHEMA` rather than CDK. This is deliberate: in a Lake-Formation-enabled account, a database created out-of-band is LF-governed with no grants, which would deny `CreateTable`. A database SQE creates makes the calling principal its owner, granting the required permissions. This pattern works with or without Lake Formation. The `glue-lake-formation` quickstart explores the governed path instead.

Validated live 2026-06-06 (account `123456789012`, eu-example-1): full round-trip succeeded, teardown left no leftover stack, bucket, or database.

## Output

```
sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
(0 rows)
(0 rows)
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

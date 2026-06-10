# AWS S3 Tables (managed Iceberg)

## Goal

Point SQE at AWS S3 Tables, AWS's managed Iceberg product. Unlike Glue (metadata only), S3 Tables bundles the catalog _and_ the storage into one service: you create a _table bucket_, and namespaces plus tables live inside it. SQE talks to it over the AWS SDK with your IAM credentials.

A small CDK stack bootstraps the throwaway table bucket and tears it down after the run, so the quickstart leaves nothing behind.

## Components

| Piece | Role |
|---|---|
| `cdk/` (TypeScript) | Creates an S3 Tables table bucket (`cdk deploy`) and removes it (`cdk destroy`). |
| `docker-compose.yml` | Runs just the SQE coordinator with the s3tables backend; AWS credentials passed via env. |
| `sqe.toml` | Annotated config template; `run.sh` fills in the table-bucket ARN and region. |

## Configuration

### Backend (sqe.toml)

```toml
[catalog.backend]
type = "s3tables"
table_bucket_arn = "__TABLE_BUCKET_ARN__"   # run.sh fills this in from CDK outputs

[storage]
s3_region = "__REGION__"
s3_path_style = false

[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]
```

The s3tables backend registers under the SQL catalog name `iceberg`, so tables are `iceberg.<namespace>.<table>`. Auth is the `anonymous` dev provider; S3 Tables authenticates via AWS IAM.

### SQL (queries.sql)

```sql
-- Create the namespace (SQE -> S3 Tables CreateNamespace)
CREATE SCHEMA IF NOT EXISTS iceberg.demo;

DROP TABLE IF EXISTS iceberg.demo.events;
CREATE TABLE iceberg.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO iceberg.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM iceberg.demo.events
GROUP BY kind
ORDER BY total DESC;
```

## The test

`run.sh` runs the full create/write/read round-trip against a real S3 Tables table bucket. It: deploys the CDK stack (table bucket) → generates `sqe.toml.local` from the stack outputs → starts SQE → executes `queries.sql` (CREATE SCHEMA → CREATE TABLE → INSERT → SELECT) and captures output to `OUTPUT.md` → then tears down: deletes the SQE-created table and namespace (S3 Tables won't delete a non-empty bucket), then `cdk destroy`.

Validated live 2026-06-06 (account `123456789012`, eu-example-1): full round-trip succeeded, teardown left no leftover stack, bucket, namespace, or table.

## Output

```
sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
(0 rows)
(0 rows)
(0 rows)
(0 rows)
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+
(2 rows)
```

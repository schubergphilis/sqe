# Embedded mode

SQE can run the full query engine in-process, with no server and no network.
`sqe-cli` in embedded mode starts DataFusion, the Iceberg reader, and the same
SQL planner locally, inside the CLI process. This is the fastest path for
querying a warehouse from a laptop, a CI job, or a script.

Four warehouse modes:

- **In-memory** (`--memory`): a transient DataFusion catalog. Nothing is
  persisted. Good for ad-hoc SQL and testing SQL functions.
- **Filesystem warehouse** (`--warehouse PATH`): an Iceberg warehouse on local
  disk or object storage with **no catalog service**. SQE walks the path for
  `metadata.json` files and treats the prefix as the catalog. The "Iceberg
  without a catalog" case.
- **Persistent SQLite catalog** (`--catalog-backend sqlite`): a durable
  single-node catalog backed by a local SQLite file. Survives restarts.
- **Cloud catalogs embedded**: Glue and S3 Tables can be attached directly,
  with no coordinator, using the standard AWS credential chain.

See the quickstarts:

- [Embedded: query local and remote files](../quickstart/embedded-files.md)
- [Embedded: persistent local catalog (SQLite)](../quickstart/embedded-sqlite-catalog.md)
- [Embedded: attach multiple catalogs](../quickstart/attach-catalogs.md)

---

## In-memory

```bash
sqe-cli --embedded --memory -e "SELECT 1 AS one"
```

```text
+-----+
| one |
+-----+
| 1   |
+-----+
```

## Filesystem warehouse (no catalog service)

Point at a directory; SQE reads the Iceberg metadata directly. No Polaris, no
Glue, no metastore.

```bash
sqe-cli --embedded --warehouse /data/warehouse \
    -e "SELECT COUNT(*) FROM sales.orders"
```

This is the catalog-free Hadoop mode. Writes need atomic rename, which object
stores do not all provide, so this mode is read-oriented; for writes use a real
catalog. The same backend powers the `[catalog.backend] type = "hadoop"` server
config.

## Cloud catalogs embedded

The embedded engine can attach a Glue or S3 Tables catalog directly, with no
coordinator. Pass `--catalog-backend` plus the cloud warehouse; credentials come
from the standard AWS provider chain (`AWS_PROFILE`, instance profile, SSO).
These catalogs attach read-only (query, not write); use the server for writes.
Requires the `aws` cargo feature, which is off by default to keep the AWS SDK
out of standard builds: `cargo install --path crates/sqe-cli --features aws`.

```bash
# AWS Glue Data Catalog (warehouse is an s3:// prefix)
AWS_PROFILE=analytics sqe-cli --embedded \
    --catalog-backend glue \
    --catalog-warehouse s3://my-bucket/warehouse --region eu-central-1 \
    -e "SELECT * FROM glue.analytics.events LIMIT 10"

# AWS S3 Tables (warehouse is the table-bucket ARN)
AWS_PROFILE=analytics sqe-cli --embedded \
    --catalog-backend s3tables \
    --catalog-warehouse arn:aws:s3tables:eu-central-1:ACCOUNT:bucket/NAME \
    --region eu-central-1 \
    -e "SHOW SCHEMAS"
```

The catalog mounts under the backend name by default (`glue.` / `s3tables.`);
override with `--catalog-name`.

## Writing data

The embedded engine ships the same Iceberg write path as the cluster: DDL,
CTAS, `INSERT`, `UPDATE`, `DELETE`, `MERGE INTO`, all against the local
SQLite-backed catalog. A laptop session can build real Iceberg tables, not
just read them.

```sql
CREATE SCHEMA iceberg.sales;

CREATE TABLE iceberg.sales.orders (
    id     BIGINT,
    region VARCHAR,
    ts     TIMESTAMP,
    total  DECIMAL(18,2)
);

-- Land external data straight into Iceberg. CTAS streams, so a file
-- larger than memory still loads without OOM.
CREATE TABLE iceberg.sales.orders_2026 AS
SELECT id, region, ts, total
FROM read_parquet('s3://bucket/2026/*.parquet')
WHERE total > 0;

INSERT INTO iceberg.sales.orders VALUES (1, 'eu', NOW(), 99.95);

UPDATE iceberg.sales.orders SET total = total * 1.21 WHERE region = 'eu';

DELETE FROM iceberg.sales.orders WHERE id < 100;

MERGE INTO iceberg.sales.orders t
USING iceberg.sales.orders_2026 s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET total = s.total
WHEN NOT MATCHED THEN INSERT (id, region, ts, total)
                    VALUES (s.id, s.region, s.ts, s.total);
```

Default DML mode is Copy-on-Write. Opt a table into Merge-on-Read with the
standard Iceberg properties:

```sql
ALTER TABLE iceberg.sales.orders
SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');
```

Branching, tagging, and time travel work from the embedded prompt too;
see [DML](../sql-reference/dml.md) for `SET WRITE_BRANCH` and
`FOR VERSION AS OF`, and [DDL](../sql-reference/ddl.md) for
`CREATE BRANCH` / `CREATE TAG`. Exporting results to plain files goes
through `COPY ... TO`; see [Using the CLI](../getting-started/cli.md).

Tables in the warehouse are valid Iceberg. Point a cluster deployment (or
any other Iceberg reader) at the same path later and they come along
unchanged.

## Cookbook

Common embedded patterns in one place. Details on each TVF live in
[read_parquet](./read-parquet.md) and
[the file-format TVFs](./file-format-tvfs.md); path and credential forms in
[Storage backends](../getting-started/storage-backends.md).

Inspect an unknown file:

```sql
SELECT * FROM '/tmp/unknown.parquet' LIMIT 10;
```

Pull a public dataset into a local Iceberg table:

```sql
CREATE TABLE iceberg.demo.titanic AS
SELECT * FROM read_csv(
    'https://raw.githubusercontent.com/datasets/titanic/main/data/titanic.csv'
);
```

Materialize a HuggingFace dataset locally:

```sql
CREATE TABLE iceberg.demo.squad AS
SELECT * FROM read_parquet(
    'hf://datasets/squad/plain_text/train-00000-of-00001.parquet'
);
```

Join an Iceberg table against a raw file without loading it first:

```sql
SELECT i.region, sum(d.amount)
FROM iceberg.sales.orders i
JOIN read_parquet('/exports/transactions.parquet') d ON i.id = d.order_id
GROUP BY i.region;
```

Read a semicolon-separated European CSV:

```sql
SELECT * FROM read_csv('data/financial.ssv', sep => ';');
```

One-shot query from a shell script, machine-readable output:

```bash
sqe-cli --embedded \
    -e "SELECT count(*) FROM read_parquet('s3://bucket/sales/*.parquet')" \
    --format csv
```

Run a SQL script, abort on the first error:

```bash
sqe-cli --embedded --file daily-report.sql --stop-on-error
```

## Differences from cluster mode

The embedded prompt speaks the same SQL surface as the coordinator's Flight
SQL endpoint: same parser, same planner, same Trino-compat function set,
same Iceberg V2/V3 readers and writers. What changes is the deployment
shape, and a few things fall away with it:

- **No auth.** No OIDC, no bearer tokens, no per-user identity, no policy
  enforcement. The process runs as the Unix user.
- **Local catalogs plus read-only Glue / S3 Tables.** Shared REST catalogs
  (Polaris, Nessie, Unity) and writes to cloud catalogs need the cluster
  path.
- **Single-node execution.** No worker fan-out, no distributed shuffle.
  The query memory pool is capped by `--memory-limit` (default 1GB).
- **Single writer.** The SQLite catalog is a single-process catalog. Two
  embedded sessions writing the same warehouse at once will conflict.
- **No observability endpoints.** Prometheus metrics, OpenTelemetry, and
  the audit log live in the server.

## How it is tested

- `crates/sqe-cli/tests/cli_smoke.rs`: binary-level flag parsing, exit codes,
  mutually-exclusive flag rejection, and the `--embedded --memory` happy path.
- The catalog spec parser (`NAME=PATH`) is validated for empty names, missing
  separators, and dotted names.

## Notes

- `--memory` and `--warehouse` are mutually exclusive.
- Local-path TVFs (`read_csv` and friends) work in embedded mode; the embedded
  engine enables `allow_local_paths` so a laptop user can read local files.
- Embedded mode authenticates the OS user against the configured catalog's
  credential source, not OIDC; there is no server to pass tokens through.

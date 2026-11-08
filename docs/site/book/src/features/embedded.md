# Embedded mode

SQE can run the query engine in-process, with no server and no network.
`sqe-cli --embedded` starts DataFusion, the Iceberg reader and writer, and the
SQE function set inside the CLI process. It is the fastest path for querying a
warehouse from a laptop, a CI job, or a script.

Embedded mode is not the coordinator in a smaller box. It plans SQL with
DataFusion directly, plus a set of SQE statements (ATTACH, secrets, schema
DDL, CTAS, `SHOW TABLES IN`). `CREATE TABLE`, `ALTER TABLE`, `UPDATE`,
`DELETE` and `MERGE` on an Iceberg catalog run on the cluster's own handlers.
Time travel reads Iceberg tables pinned to a snapshot, branch or tag.
`CREATE VIEW` stores Iceberg views in an attached REST catalog.
Table maintenance is not part of it yet. The
[support table](#what-embedded-mode-supports) below lists what runs.

Catalog choices:

- **In-memory** (`--memory`): a transient DataFusion catalog. Nothing is
  persisted. Good for ad-hoc SQL and testing SQL functions.
- **Local warehouse** (`--warehouse PATH`, default `~/.sqe/warehouse/`): a
  SQLite-backed Iceberg catalog named `iceberg`, with metadata in
  `PATH/sqe.db` and data under `PATH/iceberg/`. Survives restarts.
- **Several local catalogs** (`--catalog NAME=PATH`, repeatable): one SQLite
  catalog per name, joinable in one query.
- **Glue and S3 Tables** (`--catalog-backend glue|s3tables`): attached
  read-only, with the standard AWS credential chain.
- **Any other catalog at the prompt**: `ATTACH ... (TYPE iceberg_rest)` mounts
  a REST catalog such as Polaris, Nessie or Unity Catalog. Its data files
  are read from the store the CLI is configured for, unless the ATTACH names
  its own `S3_*` options. See
  [Using the CLI](../getting-started/cli.md#catalog-backends).

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

## Local warehouse

```bash
sqe-cli --embedded --warehouse /data/warehouse \
    -e "SELECT COUNT(*) FROM iceberg.sales.orders"
```

The catalog is a single-process SQLite file. Two embedded sessions writing the
same warehouse at once will conflict.

## Cloud catalogs embedded

The embedded engine can attach a Glue or S3 Tables catalog directly, with no
coordinator. Pass `--catalog-backend` plus the cloud warehouse. Credentials come
from the standard AWS provider chain (`AWS_PROFILE`, instance profile, SSO).
These catalogs attach read-only. Requires the `aws` cargo feature, which is off
by default to keep the AWS SDK out of standard builds:
`cargo install --path crates/sqe-cli --features aws`.

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

Schemas, tables, CTAS and `INSERT` write real Iceberg tables into the local
warehouse or an attached catalog:

```sql
CREATE SCHEMA iceberg.sales;

CREATE TABLE iceberg.sales.orders (
    id     BIGINT,
    region VARCHAR,
    ts     TIMESTAMP,
    total  DECIMAL(18,2)
);

INSERT INTO iceberg.sales.orders VALUES (1, 'eu', NOW(), 99.95);

-- Land external data straight into Iceberg.
CREATE TABLE iceberg.sales.orders_2026 AS
SELECT id, region, ts, total
FROM read_parquet('s3://bucket/2026/*.parquet')
WHERE total > 0;
```

`INSERT` into an Iceberg catalog runs on the cluster's insert handler: the
source is planned as a plain query and written the same way a coordinator
writes it, so `VALUES` rows with `ARRAY` columns need no casts.

CTAS into an Iceberg catalog runs as two commits. The first creates the empty
table from the query's schema, the second inserts the rows. When the insert
fails, the empty table is dropped again. `CREATE TABLE IF NOT EXISTS ... AS`
skips an existing table. `CREATE OR REPLACE TABLE ... AS` is refused: drop the
table first.

`UPDATE`, `DELETE` and `MERGE INTO` run on the cluster's own write handlers.
A table's `write.delete.mode` picks copy-on-write or merge-on-read, the same as
on a coordinator. Embedded mode has no access policy, so nothing filters or
masks the rows these statements read.

`CREATE TABLE` with a column list runs on the cluster's DDL handler too.
`PARTITIONED BY` transforms (`day(ts)`, `bucket(16, id)`), `TBLPROPERTIES` or
`WITH (...)` options and nested `STRUCT`, `ARRAY` and `MAP` columns all reach
the table metadata. New tables are format version 3 unless an option picks
`'format-version' = '2'`. A CTAS with `PARTITIONED BY` or options is refused,
because the CTAS path would drop them: create the table first, then
`INSERT INTO ... SELECT`. `CREATE TABLE ... LIKE` runs on a coordinator only.

`ALTER TABLE` runs on the cluster's DDL handler as well. Columns can be added,
dropped, renamed and retyped, nested `STRUCT` fields included. `SET
TBLPROPERTIES`, `ADD` / `DROP` / `REPLACE PARTITION FIELD`, `SET IDENTIFIER
FIELDS`, `CREATE BRANCH` / `CREATE TAG` and `RENAME TO` each land as one
Iceberg commit. A rename stays inside its catalog. `SET TAGS` and masking
policies need a cluster, because embedded mode has no policy engine.

`CREATE [OR REPLACE] VIEW` and `DROP VIEW [IF EXISTS]` store an Iceberg view
in a catalog attached with `TYPE iceberg_rest`, through the same code as a
coordinator. The view records the body's SQL, its column types and the
catalog's warehouse, and `OR REPLACE` lands as one commit that keeps the
view's history. `SELECT` and `DESCRIBE` read through the view. Qualify the
tables in the body with the attach name (`pol.sales.orders`), because the
session default catalog is the in-memory one. The stored body names the
warehouse instead of the attach name (`pol.sales.orders` is stored as
`<warehouse>.sales.orders`), so a coordinator or another attach that knows
the catalog under a different name still resolves it. `SHOW CREATE VIEW`
renders the body with the attach name again. `SHOW TABLES` and
`information_schema.tables` list the views with type `VIEW`. The list is
read when the catalog is attached and after each `CREATE VIEW` or `DROP
VIEW` in the session; a view another client creates later reads fine but
appears in `SHOW TABLES` only after the next attach. A view whose body
reaches itself, directly or through other views, fails to read with
`references itself through a -> b -> a` instead of recursing, and so does a
chain more than 32 views deep. Views work with a bearer `TOKEN`, a bearer
`SECRET` or an oauth2 `SECRET` on the attach; with oauth2 the view calls
use the token the catalog exchanged and renew it after a 401. The SQLite warehouse
stores no views: `CREATE VIEW` there fails, and an unqualified `CREATE VIEW
v` is still a session view.

Tables in the warehouse are valid Iceberg. Point a cluster deployment (or any
other Iceberg reader) at the same catalog later and they come along unchanged.
Exporting results to plain files goes through `COPY ... TO`; see
[Using the CLI](../getting-started/cli.md).

## What embedded mode supports

| Statement | Embedded | Cluster |
|---|---|---|
| `SELECT`, joins, aggregates, file TVFs | yes | yes |
| `CREATE SCHEMA` / `DROP SCHEMA` | yes | yes |
| `CREATE TABLE` with a column list | yes | yes |
| `CREATE TABLE` with `PARTITIONED BY` or `TBLPROPERTIES` | yes | yes |
| `CREATE TABLE ... AS` (CTAS) | yes, two commits | yes |
| `INSERT INTO` | yes | yes |
| `UPDATE`, `DELETE`, `MERGE INTO` | yes | yes |
| `ALTER TABLE` (columns, properties, partitions, rename) | yes | yes |
| `ALTER TABLE ... SET TAGS`, masking policies | no | yes |
| `DROP TABLE` | yes | yes |
| `CREATE VIEW` / `DROP VIEW` in an Iceberg catalog | REST catalogs | yes |
| `SHOW TABLES [IN schema]`, `DESCRIBE` | yes | yes |
| `SHOW CREATE TABLE` | yes | yes |
| `SHOW CREATE VIEW` on an Iceberg view | REST catalogs | yes |
| `FOR VERSION AS OF` / `FOR SYSTEM_TIME AS OF` | Iceberg tables | yes |
| Metadata tables (`orders$snapshots`) | yes | yes |
| `CREATE BRANCH` / `CREATE TAG` | yes | yes |
| `SET WRITE_BRANCH` / `RESET WRITE_BRANCH` | Iceberg tables | yes |
| `CALL system.*` procedures | no | yes |
| `ATTACH` / `DETACH`, `CREATE SECRET` | yes | yes |

`SET WRITE_BRANCH` holds for the CLI process, as it does for a cluster
session: INSERT, UPDATE, DELETE and MERGE on an Iceberg table read and commit
at the branch, and CTAS is refused while it is set. The rules are those of
[branch routing](../sql-reference/dml.md#branch-routing-sqe-specific). A
write to a table outside an Iceberg catalog ignores it.

Metadata tables (`$snapshots`, `$history`, `$manifests`, `$files`,
`$partitions`, `$refs`) read the catalog named in front of the table:
`SELECT * FROM iceberg.sales."orders$snapshots"`. Without a catalog the
table resolves only when exactly one Iceberg catalog is mounted.

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
CREATE SCHEMA iceberg.demo;

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

The embedded prompt shares the coordinator's SQL front door and function set:
the same Trino-compat functions, the same file TVFs, the same Iceberg readers.
Reads go to DataFusion's planner. `CREATE TABLE`, `ALTER TABLE`, `INSERT`,
CTAS, `UPDATE`, `DELETE` and `MERGE` on an Iceberg catalog go to the
coordinator's write handlers. The statements marked "no" above are cluster-only.
Beyond that:

- **No auth.** No OIDC, no per-user identity, no policy enforcement. The
  process runs as the Unix user. An attached REST catalog uses the secret you
  give it, and a bearer token in a secret is not refreshed.
- **Single-node execution.** No workers. The query memory pool is capped by
  `--memory-limit` (default 1GB) and shared like the coordinator's default
  pool: first come, first served, with the top 20% kept for consumers that
  cannot spill, such as the scan's decodes.
- **Single writer.** The SQLite catalog is a single-process catalog.
- **No observability endpoints.** Prometheus metrics, OpenTelemetry, and
  the audit log live in the server.

## How it is tested

- `crates/sqe-cli/tests/cli_smoke.rs`: binary-level flag parsing, exit codes,
  mutually-exclusive flag rejection, and the `--embedded --memory` happy path.
- Unit tests in `crates/sqe-cli/src/embedded.rs` cover the persistent
  warehouse across a restart, ATTACH, schema DDL, CTAS into an Iceberg catalog
  and `SHOW TABLES IN`.
- The catalog spec parser (`NAME=PATH`) is validated for empty names, missing
  separators, and dotted names.

## Notes

- `--memory`, `--warehouse` and `--catalog` are mutually exclusive.
- Local-path TVFs (`read_csv` and friends) work in embedded mode; the embedded
  engine enables `allow_local_paths` so a laptop user can read local files.

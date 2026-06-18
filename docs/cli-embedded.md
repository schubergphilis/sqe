# SQE Embedded CLI Reference

The `sqe-cli` binary in single-process mode runs the engine in-process. No coordinator, no workers, no network listeners. The same DataFusion tuning, the same Iceberg writers, and the same `read_*()` TVFs the cluster mode ships with, all in one binary.

This document is the reference for everything you can do from the prompt: launch modes, dot commands, file readers, storage backends (S3, S3 Tables, R2, MinIO, ADLS, GCS), catalog backends, write paths.

For cluster-mode flags and Polaris setup, see [`deployment.md`](./deployment.md).

## Install and launch

```bash
cargo install --path crates/sqe-cli
# or build from source
cargo build --release -p sqe-cli && cp target/release/sqe-cli ~/.local/bin/
```

The binary name is `sqe-cli`. Many users add a shell alias so embedded mode is the short form:

```bash
# zsh / bash
alias sqe='sqe-cli --embedded'

# now: `sqe`, `sqe -e "..."`, `sqe --warehouse /path`
```

Examples below show the long form (`sqe-cli --embedded ...`). With the alias they become `sqe ...`.

Default launch attaches a persistent SQLite-backed Iceberg catalog at `~/.sqe/warehouse/`:

```bash
sqe-cli --embedded                               # ~/.sqe/warehouse named `iceberg`
sqe-cli --embedded --warehouse ./local           # ./local named `iceberg`
sqe-cli --embedded --catalog prod=/data/prod \
    --catalog stage=/data/stage                  # multi-catalog mount
sqe-cli --embedded --memory                      # session-only, no disk persistence
```

Mutually exclusive: `--memory` / `--warehouse` / `--catalog`.

## CLI flags

| Flag | Default | What it does |
|---|---|---|
| `--embedded` | off | run in-process (no remote coordinator). With this off, `sqe-cli` connects to a remote coordinator on `--host`/`--port`. |
| `--memory-limit` | `1GB` | per-process query memory pool. Floored at 64 MB. |
| `--warehouse PATH` | `~/.sqe/warehouse/` | shorthand for `--catalog iceberg=PATH` |
| `--catalog NAME=PATH` | (repeatable) | attach a named persistent Iceberg catalog |
| `--memory` | off | skip persistent catalogs entirely |
| `-e, --execute SQL` | off | run one query and exit |
| `--file PATH` | off | run a SQL script file then drop to REPL |
| `--stop-on-error` | off | with `--file`, abort on first error |
| `-f, --format FMT` | `table` | output format: `table`, `csv`, `tsv`, `json` |
| `--tls`, `--insecure` | off | for cluster mode only; ignored when embedded |

The cluster-mode flags (`-H, --host`, `-p, --port`, `--protocol`, `-u, --user`, `--token`) are also accepted but ignored in embedded mode.

## REPL dot commands

Lines starting with `.` are client-side commands. They map to either built-in REPL actions or standard SQL the engine already knows how to run.

| Command | What it does |
|---|---|
| `.help` | print this list |
| `.exit`, `.quit` | leave the REPL |
| `.tables [schema]` | list tables (optionally filtered to one schema) |
| `.schema TABLE`, `.describe TABLE` | describe a table's columns |
| `.summarize TABLE` | per-column count, distinct, null, min, max |
| `.catalogs`, `.databases` | list catalogs visible to the session |
| `.read PATH` | execute a SQL script file (same as `--file`) |
| `.timer on\|off` | toggle per-query elapsed-time output |
| `.format FMT` | show or set the output format |

The dot-command surface matches the muscle memory every Postgres / SQLite / DuckDB user already has. Backslash forms (`\format`, `\q`) work too for backward compatibility.

## Reading data

Four TVFs and one auto-detect path. All four TVFs share path resolution: local filesystem, S3 (and S3-compatible), HTTPS, HuggingFace `hf://`.

### `read_parquet`, `read_csv`, `read_json`, `read_delta`

```sql
-- Local
SELECT count(*) FROM read_parquet('/data/orders.parquet');
SELECT * FROM read_csv('/data/sales.tsv.gz') LIMIT 5;
SELECT * FROM read_json('/var/log/events.jsonl');
SELECT * FROM read_delta('/data/delta/transactions');

-- S3
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');

-- HTTPS (V10)
SELECT count(*) FROM read_csv(
    'https://raw.githubusercontent.com/datasets/airport-codes/main/data/airport-codes.csv'
);

-- HuggingFace (V10) basic form
SELECT * FROM read_parquet(
    'hf://datasets/squad/plain_text/train-00000-of-00001.parquet'
);

-- HuggingFace with revision (V12.1)
SELECT * FROM read_parquet('hf://datasets/foo/bar@v1.0/data.parquet');

-- HuggingFace auto-generated parquet view (V12.1)
SELECT * FROM read_parquet(
    'hf://datasets/foo/bar@~parquet/default/train/0.parquet'
);
```

### Quoted-string auto-detect

`SELECT * FROM '<path>'` dispatches to the right TVF based on extension. Same path forms above work without naming the TVF:

```sql
SELECT * FROM '/data/orders.parquet';
SELECT * FROM 's3://bucket/data.csv';
SELECT * FROM 'hf://datasets/foo/bar/data.csv';
```

Format dispatch by extension: `.parquet`, `.csv` / `.tsv` / `.psv` / `.ssv`, `.json` / `.jsonl` / `.ndjson`, `.avro`. Compressed extensions (`.csv.gz`, `.tsv.zst`, `.json.bz2`) are recognised.

### `read_csv` named arguments

```sql
SELECT * FROM read_csv(
    '<path>',
    [delimiter | delim | sep => '<byte>',]
    [has_header | header => 'true|false',]
    [quote => '<byte>',]
    [escape => '<byte>',]
    [comment => '<byte>',]
    [null_regex | nullstr => '<regex>',]
    [compression | compress => 'auto|none|gzip|bz2|xz|zstd',]
    [file_extension => '<.ext>']
);
```

Smart defaults:
- Delimiter from extension: `.csv` is `,`, `.tsv` is tab, `.psv` is `|`, `.ssv` is `;`
- Compression from extension: `.gz`, `.bz2`, `.xz`, `.zst`
- Compression suffix is stripped before delimiter detection so `data.tsv.gz` still picks tab

### `read_delta` named arguments

```sql
SELECT * FROM read_delta(
    '<path>',
    [access_key | secret_key | endpoint | region,]
    [version => '<u64>',]
    [timestamp => '<RFC3339>']
);
```

Time travel via `version` (snapshot id) or `timestamp` (RFC3339); mutually exclusive. Read-only.

## Storage backends

### Local filesystem

Just pass an absolute or relative path. No setup.

```sql
SELECT * FROM '/data/orders.parquet';
SELECT * FROM './report.csv';
```

### AWS S3 (and S3-compatible)

Defaults come from the engine's `[storage]` block (or process env via the AWS SDK chain). Inline credentials override per-query:

```sql
SELECT * FROM read_parquet(
    's3://bucket/path/*.parquet',
    access_key => 'AKIA...',
    secret_key => '...',
    endpoint   => 'https://s3.eu-west-1.amazonaws.com',
    region     => 'eu-west-1'
);
```

If `access_key` / `secret_key` are omitted the AWS SDK chain is used: env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), `~/.aws/credentials`, IMDS on EC2, IRSA on EKS.

### Cloudflare R2

R2 is S3-compatible. Set the endpoint to your R2 account URL:

```sql
SELECT * FROM read_parquet(
    's3://my-r2-bucket/data.parquet',
    access_key => '<R2_ACCESS_KEY_ID>',
    secret_key => '<R2_SECRET_ACCESS_KEY>',
    endpoint   => 'https://<account-id>.r2.cloudflarestorage.com',
    region     => 'auto'
);
```

R2 ignores the region and accepts `auto` as a placeholder.

### MinIO, Ceph RGW, SeaweedFS, Garage, rustfs

All S3-compatible. Same TVF args as AWS S3, with the endpoint pointing at your server:

```sql
SELECT * FROM read_parquet(
    's3://bucket/data.parquet',
    access_key => 'minio-access-key',
    secret_key => 'minio-secret-key',
    endpoint   => 'http://localhost:9000',
    region     => 'us-east-1'
);
```

Set `s3_allow_http = true` in the engine's `[storage]` block (or pass `endpoint` with `http://`) to allow plain HTTP for local development.

### HTTPS (V10 lazy fetch)

Any HTTPS URL works without registration. The first request to a new `scheme://host` builds an `HttpStore` on the fly and caches it in the session's object-store registry:

```sql
SELECT * FROM 'https://example.com/data.parquet';
SELECT * FROM read_csv('https://raw.githubusercontent.com/.../titanic.csv');
```

### HuggingFace `hf://`

The `hf://` resolver translates HuggingFace dataset / model / spaces URLs to their HTTPS form on the Hub:

```sql
-- Implicit revision = main
SELECT * FROM read_parquet(
    'hf://datasets/squad/plain_text/train-00000-of-00001.parquet'
);

-- Inline revision (DuckDB-style, V12.1)
SELECT * FROM read_csv('hf://datasets/foo/bar@v1.0/data.csv');

-- Auto-generated parquet view (V12.1)
SELECT * FROM read_parquet(
    'hf://datasets/foo/bar@~parquet/default/train/0.parquet'
);

-- Equivalent ?revision query parameter
SELECT * FROM read_csv(
    'hf://datasets/foo/bar/data.csv?revision=v1.0'
);
```

Public datasets work anonymously. Private datasets read `HF_TOKEN` from the environment if set.

Globs (`**/*.parquet`) on hf:// URLs are tracked for V12.2; today the path must point to a specific file.

### Azure ADLS Gen2 / Blob

Three URL forms accepted: `abfss://<container>@<account>.dfs.core.windows.net/<path>` (Hadoop-style), `abfs://...` (plaintext), or the shorthand `azure://<container>/<path>` / `az://<container>/<path>` with the account from config.

```sql
-- Shared key
SELECT * FROM read_parquet(
    'abfss://my-container@myaccount.dfs.core.windows.net/data.parquet',
    azure_access_key => '<storage-account-key>'
);

-- SAS token
SELECT * FROM read_csv(
    'abfss://logs@myaccount.dfs.core.windows.net/events.csv',
    azure_sas_token => 'sv=2024-08-04&ss=b&...'
);

-- Azurite emulator (local dev)
SELECT * FROM read_parquet('azure://devstoreaccount1/test/data.parquet');
```

Permanent config:

```toml
[storage]
azure_account      = "myaccount"
azure_access_key   = "<storage-account-key>"
# OR:
azure_sas_token    = "sv=2024-08-04&..."
azure_use_emulator = false
```

### Google Cloud Storage (GCS)

`gs://` and `gcs://` URLs both work. Auth is a service-account JSON file path, an inline JSON key, or Application Default Credentials.

```sql
-- Service-account JSON file
SELECT * FROM read_parquet(
    'gs://my-bucket/data.parquet',
    gcs_service_account_path => '/var/secrets/gcs-key.json'
);

-- Inline JSON key
SELECT * FROM read_csv(
    'gs://my-bucket/data.csv',
    gcs_service_account_key => '{"type":"service_account",...}'
);

-- ADC (gcloud config / GCE metadata / GKE Workload Identity)
SELECT * FROM read_parquet('gs://my-bucket/data.parquet');
```

Permanent config:

```toml
[storage]
gcs_service_account_path = "/var/secrets/gcs-key.json"
# OR inline:
gcs_service_account_key  = "{\"type\":\"service_account\",...}"
```

## Catalogs

Embedded mode attaches one or more named Iceberg catalogs. Each catalog is a SQLite file (catalog metadata) plus a data root (where Parquet lands). They live side-by-side at the path you choose.

### Default `iceberg` catalog

```bash
sqe-cli --embedded                                   # ~/.sqe/warehouse/sqe.db + .../iceberg/
sqe-cli --embedded --warehouse /data/local           # /data/local/sqe.db + /data/local/iceberg/
```

```sql
sqe> CREATE SCHEMA iceberg.staging;
sqe> CREATE TABLE iceberg.staging.orders (id BIGINT, total DECIMAL(18,2));
sqe> INSERT INTO iceberg.staging.orders VALUES (1, 99.95);
sqe> .quit
$ sqe-cli --embedded                                 # restart, table survives
sqe> SELECT * FROM iceberg.staging.orders;
```

### Multiple catalogs

```bash
sqe-cli --embedded --catalog prod=/data/prod \
    --catalog stage=/data/stage
```

Cross-catalog joins work in 3-part SQL names:

```sql
SELECT s.id, p.region
FROM stage.sales.orders s
JOIN prod.sales.orders p ON s.id = p.id;
```

Each catalog is independent: separate SQLite file, separate Iceberg metadata scope, separate data root. Duplicate `--catalog NAME` rejects at startup.

### AWS S3 Tables

S3 Tables is AWS's managed Iceberg backend. It speaks an Iceberg REST profile under the `s3tables://` ARN form. SQE talks to it via the `iceberg-catalog-s3tables` workspace crate.

In cluster mode, the config block is:

```toml
[catalog]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
```

In embedded mode, S3 Tables registration through the `--catalog` flag is on the V12.x roadmap. Today, the cluster path is the supported way to talk to S3 Tables. The TVF path is a workaround for read-only access:

```sql
-- Read S3 Tables data files directly via S3 (read-only, no DML)
SELECT * FROM read_parquet(
    's3://<table-bucket>/<table-id>/data/*.parquet',
    access_key => '<key>',
    secret_key => '<secret>',
    region     => 'us-east-1'
);
```

For full S3 Tables read + write from embedded mode, track [`docs/catalogs.md`](./catalogs.md) for the embedded-S3-Tables follow-up.

### Runtime catalog mounts via `ATTACH`

Embedded mode supports the same SQL `ATTACH` / `DETACH` and `CREATE` / `DROP` / `SHOW SECRETS` primitives as the cluster server. Use them to mount any of the six supported backends from the REPL without editing TOML or restarting:

```sql
sqe> CREATE SECRET prod (TYPE bearer, TOKEN 'eyJ...');
sqe> ATTACH 'http://catalog.example.com/api/catalog' AS prod_cat
       (TYPE iceberg_rest, WAREHOUSE 'analytics', SECRET prod);
sqe> SELECT * FROM prod_cat.sales.orders LIMIT 5;
sqe> DETACH prod_cat;
```

Full reference: [`docs/book/src/operations/catalogs.md`](book/src/operations/catalogs.md). The supported `TYPE` values are `iceberg_rest`, `glue`, `s3tables`, `hms`, `jdbc`, `sqlite`, and `hadoop`.

ATTACH is process-local. The registry and the secret store are wiped on CLI exit; persistent catalogs still go through `--catalog NAME=PATH` or the default `~/.sqe/warehouse/`.

### HMS, AWS Glue, JDBC, Nessie

Both cluster mode and embedded mode now support all six backends. Cluster mode reads them from `[catalogs.*]` TOML; embedded mode supports both startup flags (SQLite via `--catalog`) and runtime SQL `ATTACH` for any backend.

The matrix:

| Backend | Cluster mode (TOML) | Embedded mode (`--catalog`) | Embedded mode (`ATTACH`) |
|---|---|---|---|
| Iceberg REST (Polaris, Nessie, Unity) | yes | no | yes |
| AWS Glue | yes | no | yes |
| AWS S3 Tables | yes | no | yes |
| Hive Metastore | yes | no | yes |
| JDBC (Postgres / MySQL / SQLite) | yes | SQLite only | yes |
| Hadoop (storage-only) | yes | yes (file:// path scan) | yes |

## Writing data

The same DDL and DML surface that cluster mode ships with works against the embedded SQLite catalog.

### CREATE TABLE / CTAS

```sql
sqe> CREATE TABLE iceberg.sales.orders (
       id     BIGINT,
       region STRING DEFAULT 'unknown',
       ts     TIMESTAMP_NS(9),
       total  DECIMAL(18,2)
     );

sqe> CREATE TABLE iceberg.staging.orders_2026 AS
     SELECT id, region, total
     FROM read_parquet('s3://bucket/2026/*.parquet')
     WHERE total > 0;
```

V11 Delta Lake reads can land into Iceberg via CTAS:

```sql
sqe> CREATE TABLE iceberg.warehouse.legacy_sales AS
     SELECT * FROM read_delta('/legacy/delta/sales');
```

### INSERT / UPDATE / DELETE / MERGE

```sql
sqe> INSERT INTO iceberg.sales.orders VALUES (1, 'eu', NOW(), 99.95);

sqe> UPDATE iceberg.sales.orders
     SET total = total * 1.21
     WHERE region = 'eu';

sqe> DELETE FROM iceberg.sales.orders WHERE id < 100;

sqe> MERGE INTO iceberg.sales.orders t
     USING new_orders s ON t.id = s.id
     WHEN MATCHED THEN UPDATE SET total = s.total
     WHEN NOT MATCHED THEN INSERT (id, region, total)
                         VALUES (s.id, s.region, s.total);
```

Default DML mode is Copy-on-Write. Per the Iceberg spec, set `write.delete.mode`, `write.update.mode`, or `write.merge.mode` on the table to `merge-on-read` to opt into the equality-delete writer:

```sql
sqe> ALTER TABLE iceberg.sales.orders
     SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');
```

### COPY TO

DataFusion-native `COPY` writes one file per query.

```sql
sqe> COPY (SELECT * FROM iceberg.sales.orders WHERE region = 'eu')
     TO '/tmp/eu-orders.parquet';

sqe> COPY (SELECT region, sum(total) AS revenue
           FROM iceberg.sales.orders
           GROUP BY region)
     TO '/tmp/revenue-by-region.csv'
     (FORMAT csv);
```

### Branches and tags

Phase C of the matrix-parity work shipped Iceberg ref DDL:

```sql
sqe> ALTER TABLE iceberg.sales.orders CREATE BRANCH feature_x;

sqe> SET WRITE_BRANCH = 'feature_x';
sqe> INSERT INTO iceberg.sales.orders VALUES (...);

sqe> SET WRITE_BRANCH = 'main';   -- or just unset
sqe> SELECT * FROM iceberg.sales.orders FOR VERSION AS OF 'feature_x';

sqe> ALTER TABLE iceberg.sales.orders CREATE TAG release_v1;
```

## Cookbook

Common patterns gathered in one place.

### Quick file inspection

```sql
sqe> SELECT * FROM '/tmp/unknown.parquet' LIMIT 10;
sqe> .schema unknown.parquet     -- works after enable_url_table promoted it
sqe> .summarize unknown.parquet
```

### Public dataset prototype to local table

```sql
sqe> CREATE TABLE iceberg.demo.titanic AS
     SELECT * FROM read_csv(
         'https://raw.githubusercontent.com/datasets/titanic/main/data/titanic.csv'
     );
```

### HuggingFace parquet view to local Iceberg

```sql
sqe> CREATE TABLE iceberg.demo.squad AS
     SELECT * FROM read_parquet(
         'hf://datasets/squad/plain_text@~parquet/default/train/0.parquet'
     );
```

### Cross-format join

```sql
SELECT i.region, sum(d.amount)
FROM iceberg.sales.orders i
JOIN read_delta('/legacy-delta/transactions') d
    ON i.id = d.order_id
GROUP BY i.region;
```

### CSV ingest with European decimal separator

```sql
SELECT * FROM read_csv(
    'data/financial.ssv',
    sep => ';'
);
```

### Compressed CSV with explicit codec

```sql
SELECT count(*) FROM read_csv(
    's3://logs/audit-2026-05.csv.zst',
    compress => 'auto'
);
```

### One-shot SQL via shell

```bash
sqe-cli --embedded -e "SELECT count(*) FROM read_parquet('s3://bucket/sales/*.parquet')" \
    --format csv
```

### Run a script file

```bash
echo "
.timer on
CREATE SCHEMA IF NOT EXISTS iceberg.report;
CREATE TABLE iceberg.report.daily AS
  SELECT date_trunc('day', ts) AS day, sum(total) AS revenue
  FROM iceberg.sales.orders
  GROUP BY 1;
" > daily.sql

sqe-cli --embedded --file daily.sql --stop-on-error
```

## Differences from cluster mode

The embedded prompt is the same SQL surface as the coordinator's Flight SQL endpoint. A few specifics differ:

- **No auth.** No OIDC, no bearer tokens, no per-user identity. The CLI runs as the Unix user.
- **No remote catalogs.** Only the SQLite-backed catalogs at `--warehouse` / `--catalog`. The cluster path is required for HMS, Glue, S3 Tables, Polaris, Nessie, Unity Catalog REST.
- **Single-node execution.** No coordinator-worker shuffle, no spill across machines. Memory pool capped by `--memory-limit`.
- **No metrics endpoint.** Prometheus, OpenTelemetry, audit log all live in the cluster path.

Everything else is identical: DataFusion 54, Trino-compat function set, JSON helpers, Iceberg V2 / V3 read + write, time travel, branching, partition evolution, schema evolution, equality deletes, position deletes, MoR, CoW.

## Implementation references

- `crates/sqe-cli/src/main.rs`: CLI flag parsing
- `crates/sqe-cli/src/embedded.rs`: `build_embedded_context`, `EmbeddedClient`, warehouse attachment
- `crates/sqe-cli/src/dotcommands.rs`: dot-command parser
- `crates/sqe-catalog/src/read_parquet.rs`, `read_csv.rs`, `read_json.rs`, `read_delta.rs`: TVFs
- `crates/sqe-catalog/src/file_tvf_common.rs`: shared path / S3 / HTTPS / hf:// resolver
- `crates/sqe-catalog/src/lazy_object_store.rs`: V10 lazy HTTPS object-store registry
- `crates/sqe-catalog/src/hf_tree_cache.rs`: V12.2 prerequisite (HF tree-API cache)
- `docs/catalogs.md`: catalog backend configuration in cluster mode
- `docs/duckdb-comparision.md`: V8-V12 audit and remaining gaps

# Catalog backends

SQE talks to Iceberg tables through one of six catalog backends. The
choice is per-deployment in `sqe.toml`. Default release builds ship
every backend; slim builds drop the unused ones to save binary size.

The five non-Hadoop backends share one dispatch path through the
upstream `iceberg-catalog-loader` crate. Hadoop is the lone outlier
because it is filesystem-only: no metadata service to talk to,
just a warehouse path to walk.

## Quick reference

| Backend  | `type` value | Required keys | Optional keys | Cargo feature | Vendored crate |
|----------|--------------|---------------|---------------|---------------|----------------|
| REST     | `rest` (default) | `catalog_url`, `warehouse` (on `[catalog]`) | bearer / OAuth headers via runtime auth | `rest` (always) | `iceberg-catalog-rest` |
| HMS      | `hms` | `uri`, `warehouse` | | `hms` | `iceberg-catalog-hms` |
| Glue     | `glue` | `region`, `warehouse` | `endpoint` | `glue` | `iceberg-catalog-glue` |
| S3 Tables | `s3tables` | `table_bucket_arn` | `endpoint_url` | `s3tables` | `iceberg-catalog-s3tables` |
| JDBC     | `jdbc` | `url`, `warehouse` | | `sql-postgres` | `iceberg-catalog-sql` |
| Hadoop   | `hadoop` | `warehouse` | | `hadoop` | (SQE-native) |

All six are smoke-tested in CI. Two of them, Glue and S3 Tables,
are verified live against production AWS deployments (account
`123456789012`, eu-central-1 and eu-west-1).

## Multiple catalogs in one coordinator

SQE supports attaching several named catalogs (potentially of different backend types) to one coordinator. Each one becomes a top-level SQL identifier and cross-catalog joins work without any session-state setup.

```toml
# Legacy single-catalog block kept as a placeholder for backwards
# compatibility. When `[catalogs.*]` is populated the legacy block
# is dropped unless `query.default_catalog` names it explicitly.
[catalog]
catalog_url = ""

[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "production"

[catalogs.polaris.backend]
type = "rest"

[catalogs.nessie]
catalog_url = "http://nessie:19120/iceberg"
warehouse = "lake"

[catalogs.nessie.backend]
type = "rest"

[catalogs.aws_glue]
catalog_url = ""
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://my-bucket/wh"

[catalogs.aws_s3tables]
catalog_url = ""
[catalogs.aws_s3tables.backend]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"

[catalogs.legacy_hms]
catalog_url = ""
[catalogs.legacy_hms.backend]
type = "hms"
uri = "metastore.example.com:9083"
warehouse = "s3a://my-bucket/wh"

[query]
# Optional. Picks the catalog DataFusion uses for unqualified
# names. Defaults to the first entry from `[catalogs.*]` sorted
# alphabetically (so `aws_glue` would win the example above).
default_catalog = "polaris"
```

3-part SQL identifiers route to the right catalog:

```sql
SELECT *
FROM polaris.sales.orders p
LEFT JOIN nessie.archive.orders n ON p.id = n.id
WHERE n.id IS NULL;

SELECT count(*) FROM aws_glue.iceberg_demo_analytics.iceberg_user_events;

SELECT * FROM aws_s3tables.testnamespace.daily_sales LIMIT 10;
```

Each catalog uses its own backend dispatch path (REST + bearer token, native AWS SDK, Thrift, etc.). The user's bearer token from the session auth applies to all REST catalogs registered. Per-catalog credential scoping is a future change; today storage credentials are coordinator-wide.

The legacy single-catalog form (the `[catalog]` block alone, no `[catalogs.*]`) keeps working unchanged. Existing deployments need no migration.

### Per-catalog auth and storage

Each catalog can override the global session bearer token and the global S3 credentials via optional `[catalogs.<name>.auth]` and `[catalogs.<name>.storage]` blocks. Federation across organisations becomes a config change rather than a separate deployment.

```toml
# Default Polaris uses the user's session token (V6 behaviour).
[catalogs.polaris]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalogs.polaris.backend]
type = "rest"

# A partner Polaris uses its own OAuth client and S3 bucket.
[catalogs.partner]
catalog_url = "https://partner.com/iceberg"
warehouse = "shared"
[catalogs.partner.backend]
type = "rest"
[catalogs.partner.auth]
type = "client_credentials"
token_endpoint = "https://partner.com/oauth/tokens"
client_id = "sqe-partner"
client_secret = "..."   # use env override SQE__catalogs__partner__auth__client_secret
[catalogs.partner.storage]
s3_endpoint = "https://partner-s3.example.com"
s3_region = "us-east-1"
s3_access_key = "..."
s3_secret_key = "..."

# A public Nessie endpoint we read anonymously.
[catalogs.public_archive]
catalog_url = "https://nessie.public.example.com/iceberg"
warehouse = "public"
[catalogs.public_archive.backend]
type = "rest"
[catalogs.public_archive.auth]
type = "anonymous"

# AWS Glue lets the AWS SDK provider chain handle auth.
[catalogs.aws_glue]
[catalogs.aws_glue.backend]
type = "glue"
region = "eu-central-1"
warehouse = "s3://wh/"
[catalogs.aws_glue.auth]
type = "aws"
```

#### `[catalogs.<name>.auth].type` values

| `type` | What it does | When to use |
|---|---|---|
| `session_bearer` (default) | Pass the user's session bearer token through unchanged | One OIDC provider fronts every Iceberg REST endpoint (the common case) |
| `client_credentials` | Cluster-level OAuth2 `client_credentials` grant against the catalog's own token endpoint | Federation with a partner Iceberg REST that has its own OAuth |
| `anonymous` | No `Authorization` header | Public read-only Nessie or Polaris |
| `static` | Pre-issued bearer token | Internal gateway with a fixed key, integration tests |
| `aws` | AWS SDK provider chain | Glue / S3 Tables native backends, AWS REST endpoints with SigV4 |

#### `[catalogs.<name>.storage]` overrides

The block accepts the same keys as the top-level `[storage]` block: `s3_endpoint`, `s3_region`, `s3_access_key`, `s3_secret_key`, `s3_path_style`, `s3_allow_http`. Iceberg credential vending from REST catalogs still wins per-table over both this and the global block, so you only need to fill out per-catalog storage when the catalog does not vend credentials (Hadoop, raw Iceberg over Ceph, etc.) or when the underlying buckets live behind different S3 endpoints.

Today storage overrides apply at scan / write time. The `client_credentials` token is fetched once at session-build time and reused for the session lifetime; refresh-on-expiry is a future change.

## REST: Polaris, Nessie, Unity OSS, AWS Glue REST, AWS S3 Tables REST

The default. Most production deployments speak Iceberg REST.

```toml
[catalog]
catalog_url = "https://polaris.example.com:18181/api/catalog"
warehouse   = "production_warehouse"

[catalog.backend]
type = "rest"   # default; this block can be omitted entirely
```

Local Polaris stack from the repo:

```bash
docker compose -f docker-compose.test.yml up -d
# Polaris listens on http://localhost:18181
```

AWS REST endpoints (Glue REST, S3 Tables REST) work transparently:
when the server's `/v1/config` response advertises
`rest.sigv4-enabled=true`, SQE engages SigV4 automatically. AWS
credentials come from the standard SDK chain (env vars, profiles,
IMDS).

| Service | REST endpoint | Auth |
|---------|---------------|------|
| Apache Polaris | `https://polaris/api/catalog` | OIDC bearer |
| Project Nessie 0.107+ | `https://nessie/api/v1/iceberg` | bearer / anonymous |
| Unity Catalog OSS | `https://unity/api/2.1/unity-catalog/iceberg` | bearer (Databricks) / anonymous (OSS) |
| AWS Glue Iceberg REST | `https://glue.<region>.amazonaws.com/iceberg` | AWS SigV4 (auto-detected) |
| AWS S3 Tables REST | `https://s3tables.<region>.amazonaws.com/iceberg/v1` | AWS SigV4 (auto-detected) |

REST is the most-tested path. Every benchmark suite (TPC-H, SSB,
TPC-DS, TPC-C, TPC-E, TPC-BB, ClickBench) runs against the local
Polaris stack on every release build.

## HMS: Hive Metastore over Thrift

For deployments still on Hive Metastore.

```toml
[catalog.backend]
type      = "hms"
uri       = "metastore.example.com:9083"     # Thrift host:port
warehouse = "s3a://my-bucket/warehouse"
```

Pulls in `volo-thrift` and `pilota` (~10-15 MB).

Authentication via Kerberos / Knox is not supported directly.
Deployments that need it should sit behind a sidecar that handles
the SASL handshake and exposes a plain Thrift port. SQE expects
the metastore to speak unauthenticated Thrift on its data plane.

The HMS path is verified by the integration test in
`crates/sqe-catalog/tests/backends_integration.rs` and runs
against a docker-compose overlay during CI.

## Glue: AWS Glue Data Catalog

```toml
[catalog.backend]
type      = "glue"
region    = "eu-central-1"
warehouse = "s3://my-bucket/warehouse"
# endpoint = "http://localhost:4566"   # optional, e.g. LocalStack
```

Run with the right AWS credentials:

```bash
AWS_PROFILE=my-profile ./target/release/sqe-coordinator ~/sqe-config.toml
```

The AWS SDK reads `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_REGION`,
or IMDS in that order. The `region` field in the config sets the
Glue API region; `warehouse` is the S3 path Glue uses for new
tables.

Pulls in `aws-sdk-glue` + `aws-config` (~50-80 MB).

**Live verification (2026-05-05)** against AWS Glue in eu-central-1
(account `123456789012`, database `iceberg_demo_analytics`):

```
sqe> SHOW SCHEMAS;
+------------------------+
| schema_name            |
+------------------------+
| admin_consumer         |
| admin_producer         |
| default                |
| iceberg-demo_catalog   |
| iceberg_demo_analytics |
| saleslhdev_pub_db      |
| saleslhdev_sub_db      |
+------------------------+
(7 rows)

sqe> SELECT region, event_type, COUNT(*) AS n
   . FROM iceberg_demo_analytics.iceberg_user_events
   . GROUP BY region, event_type ORDER BY n DESC LIMIT 5;
+------------+------------+-------+
| region     | event_type | n     |
+------------+------------+-------+
| ap-south   | login      | 50524 |
| eu-central | login      | 50424 |
| eu-central | click      | 50391 |
| eu-central | view       | 50251 |
| us-west    | click      | 50155 |
+------------+------------+-------+
```

Aggregations, filter pushdown, and ORDER BY all work correctly
across ~1.5M rows.

## S3 Tables: AWS managed Iceberg

AWS's first-class managed Iceberg service. Different from Glue
(which is metadata-only): S3 Tables manages metadata **and**
storage in one product. Tables live in a "table bucket" addressed
by ARN.

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
# endpoint_url   = "http://localhost:4566"   # optional, custom endpoint
```

Same AWS credential story as Glue. The bucket ARN format is
`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`.

Pulls in `aws-sdk-s3tables`. Shares the AWS SDK runtime that
`glue` already pulls, so the incremental binary cost on top of an
AWS-enabled build is small (~5 MB).

**Live verification (2026-05-05)** against
`arn:aws:s3tables:eu-west-1:123456789012:bucket/testtablebucket`:

```
sqe> SHOW SCHEMAS;
+---------------+
| schema_name   |
+---------------+
| testnamespace |
+---------------+

sqe> SELECT product_category, COUNT(*) AS sales_count, SUM(sales_amount) AS total_sales
   . FROM testnamespace.daily_sales
   . GROUP BY product_category ORDER BY total_sales DESC;
+------------------+-------------+-------------+
| product_category | sales_count | total_sales |
+------------------+-------------+-------------+
| Laptop           | 4           | 4500.0      |
| Monitor          | 3           | 925.0       |
| Keyboard         | 1           | 60.0        |
| Mouse            | 1           | 25.0        |
+------------------+-------------+-------------+
```

Two backends in one repo, both writing to AWS through SQE's
identical scan + aggregation path. The only thing that differs
is which `CatalogBuilder` the loader hands back from
`load("glue")` vs `load("s3tables")`.

## JDBC: Postgres / MySQL / SQLite

Iceberg's JDBC catalog stores table metadata in a relational
database. Useful when you want a single SQL endpoint without
running a metadata service.

```toml
[catalog.backend]
type      = "jdbc"
url       = "postgresql://user:pass@host:5432/iceberg"
warehouse = "s3://my-bucket/warehouse"
```

The URL prefix selects the driver:

| Prefix | Driver | Notes |
|--------|--------|-------|
| `sqlite:path/to/file.db` | SQLite | Local file, no separate server |
| `postgresql://...` or `postgres://...` | PostgreSQL | Production-grade, recommended |
| `mysql://...` | MySQL | Tested on MySQL 8.0+ |

The catalog tables follow the Iceberg JDBC catalog schema
(`iceberg_tables`, `iceberg_namespace_properties`). SQE creates
them on first connect.

Pulls in `sqlx` + the requested DB driver (~5-10 MB for Postgres).

The Postgres path is verified by an integration test against a
docker-compose Postgres in `crates/sqe-catalog/tests/backends_integration.rs`.

## Hadoop: filesystem-only catalog

No metadata service. SQE walks `warehouse` for `metadata.json`
files and treats the prefix as the catalog. Useful for read-only
access to a warehouse another engine wrote, or for one-off
investigations on a S3 / GCS / Azure prefix without standing up
Polaris.

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

This is SQE's only native catalog backend. The other five all
delegate to the upstream `iceberg-rust` builder via the
`iceberg-catalog-loader` crate. Hadoop has no upstream loader
counterpart because it is not really a catalog. There is no
metadata service to talk to. Implementation lives in
`crates/sqe-catalog/src/backends/hadoop.rs`.

Read-only. No commit path. Use a real catalog if you need INSERT,
UPDATE, DELETE, or MERGE.

## How the loader works

Every non-REST backend's dispatch goes through one function call:

```rust
let catalog = iceberg_catalog_loader::load(catalog_type)?
    .load(name.to_string(), props)
    .await?;
```

`catalog_type` is the lowercase string (`"glue"`, `"s3tables"`,
etc). `props` is a `HashMap<String, String>` of the upstream
`*_CATALOG_PROP_*` keys. The loader's registry is feature-gated
so a slim build only links the backends the SQE binary actually
uses.

The patch sits in
`vendor/iceberg-rust/crates/catalog/loader/src/lib.rs`, documented
inline at the touch site and in the vendor README under "SQE-only
patches." It is forward-compatible with upstream: every existing
caller of the loader sees all backends present by default; nobody
loses anything.

## Slim builds

Default release builds include every backend. Operators who want
a smaller image can opt out:

```bash
# REST only: no AWS SDK, no Thrift, no sqlx
cargo build --release --no-default-features --features rest -p sqe-coordinator

# REST + AWS managed Iceberg
cargo build --release --no-default-features --features rest,glue,s3tables -p sqe-coordinator

# REST + Hive
cargo build --release --no-default-features --features rest,hms -p sqe-coordinator
```

Approximate cost on top of a `rest`-only build:

| Feature | Adds | Why |
|---------|-----:|-----|
| `hadoop` | ~0 | Reuses existing `object_store` |
| `sql-postgres` | 5-10 MB | sqlx + Postgres driver |
| `hms` | 10-15 MB | volo-thrift + pilota |
| `glue` | 50-80 MB | full AWS SDK |
| `s3tables` | ~5 MB on top of `glue` | shares AWS SDK runtime |

Default release binary lands around 180-200 MB on Linux x86_64.

## Verifying the connection

Once the coordinator is up, run these in order. Each one
exercises a deeper layer and tells you exactly where things break
if they do.

```bash
# 1. Auth + Flight handshake
SQE_PASSWORD=s3cr3t sqe-cli --port 60051 --user root -e "SELECT 1"

# 2. Catalog reachable, namespaces visible
SQE_PASSWORD=s3cr3t sqe-cli --port 60051 --user root -e "SHOW SCHEMAS"

# 3. Pick a namespace, list its tables
SQE_PASSWORD=s3cr3t sqe-cli --port 60051 --user root -e "SHOW TABLES IN <namespace>"

# 4. Read a row
SQE_PASSWORD=s3cr3t sqe-cli --port 60051 --user root \
    -e "SELECT * FROM <namespace>.<table> LIMIT 1"
```

If step 4 works, every other Iceberg query path works too: filter
pushdown, GROUP BY, JOIN, time-travel, write back.

## Troubleshooting

**`Invalid or expired bearer token`** when the CLI passes
`--token`: the bearer was minted by something SQE's auth chain
does not recognize. Use `--user` + `SQE_PASSWORD` instead and let
SQE mint its own token via the auth endpoint configured in
`[auth]`.

**`Catalog '<X>' build failed`** with no further detail: check
the coordinator log. Common causes:

- AWS credentials not on the chain (no `AWS_PROFILE`, no env
  vars, not running on EC2 / EKS).
- HMS Thrift port not reachable.
- JDBC `url` typo (the prefix selects the driver).
- S3 Tables ARN region mismatch (the ARN's region must match
  whatever the AWS SDK resolves; set `AWS_REGION` to be safe).

**`No such table`** but the table exists in the catalog: namespace
case sensitivity. Iceberg namespaces are usually lowercase; some
HMS deployments treat them as case-insensitive.

**Slow first query** every time the coordinator restarts: cold
manifest cache. Subsequent queries hit `ObjectCache` and run
faster. Expected.

## Where to go from here

- [`Quickstart`](./quickstart.md): top-level walkthrough that
  covers SQE end-to-end including auth and CLI connection
- [`Iceberg Integration`](../features/iceberg.md): REST surface,
  credential vending, read / write path, V3 features
- [`Configuration Reference`](../deployment/configuration.md):
  every TOML key and `SQE_*` env var
- [`Architecture: Coordinator`](../architecture/coordinator.md):
  how the catalog plugs into session management

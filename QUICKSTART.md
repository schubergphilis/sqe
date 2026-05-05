# SQE Quickstart

Five minutes from a fresh checkout to your first query. SQE talks to
six catalog backends and connects to anything that exposes Iceberg
tables. This guide walks through each backend with a concrete
config, the right credentials, and a `SELECT` you can copy.

## Prerequisites

- Rust 1.92 (any stable 1.92.x)
- Docker (only if you use the bundled local stack)
- An Iceberg-compatible catalog you can reach (or use the local
  Polaris stack from the repo's `docker-compose.test.yml`)

```bash
git clone https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine.git
cd sqlengine
cargo build --release -p sqe-coordinator -p sqe-cli
```

The default release build ships every backend. If you want a slim
binary (no AWS SDK, no Thrift, no sqlx), pick a feature subset:

```bash
# REST only - smallest binary
cargo build --release --no-default-features --features rest \
    -p sqe-coordinator

# REST + AWS managed Iceberg
cargo build --release --no-default-features --features rest,glue,s3tables \
    -p sqe-coordinator

# REST + Hive
cargo build --release --no-default-features --features rest,hms \
    -p sqe-coordinator
```

Approximate cost on top of a `rest`-only binary:
`hadoop` ~0, `sql-postgres` 5-10 MB, `hms` 10-15 MB, `glue` 50-80 MB,
`s3tables` ~5 MB on top of `glue`.

## The shape of an SQE config

Every backend uses the same TOML structure. The only thing that
changes is the `[catalog.backend]` block. A minimum config looks
like this:

```toml
[coordinator]
flight_sql_port = 60051
trino_http_port = 18080

[auth]
# Where SQE goes to validate bearer tokens. Leave pointing at
# Polaris (or whatever issues your tokens) even if your data
# lives elsewhere.
token_endpoint = "http://localhost:18181/api/catalog/v1/oauth/tokens"
client_id      = "root"
client_secret  = "s3cr3t"

[catalog]
polaris_url = "https://placeholder.invalid"   # required by schema, not used by non-REST backends
warehouse   = "default-warehouse"

[catalog.backend]
type = "rest"   # or "hms" / "glue" / "s3tables" / "jdbc" / "hadoop"
# backend-specific keys go here

[storage]
s3_region     = "us-east-1"
s3_path_style = false
```

Save as `~/sqe-config.toml`. Start the coordinator:

```bash
./target/release/sqe-coordinator ~/sqe-config.toml
```

In another shell, connect with the CLI:

```bash
SQE_PASSWORD=s3cr3t \
    ./target/release/sqe-cli --port 60051 --user root \
    -e "SHOW SCHEMAS"
```

The username + password get exchanged for a bearer token at the
auth endpoint above. Subsequent queries reuse the session.

## REST: Polaris, Nessie, Unity OSS

The default. Most production deployments speak Iceberg REST.

```toml
[catalog]
polaris_url = "https://polaris.example.com:18181/api/catalog"
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

## Hive Metastore (HMS)

For deployments still on Hive Metastore over Thrift.

```toml
[catalog.backend]
type      = "hms"
uri       = "metastore.example.com:9083"     # Thrift host:port
warehouse = "s3a://my-bucket/warehouse"
```

Authentication via Kerberos or Knox is not supported directly.
Deployments that need it should sit behind a sidecar that handles
the SASL handshake and exposes a plain Thrift port.

## AWS Glue Data Catalog

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
Glue API region; `warehouse` is the S3 path Glue uses for new tables.

Verified live against a production AWS Glue catalog
(`iceberg_demo_analytics`) in eu-central-1: SHOW SCHEMAS returns 7
databases, SHOW TABLES enumerates Iceberg tables, SELECT + WHERE +
GROUP BY all work over ~1.5M rows.

## AWS S3 Tables (managed Iceberg)

AWS's managed Iceberg service. Different from Glue (which is
metadata only) - S3 Tables is metadata + storage in one product.

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
# endpoint_url   = "http://localhost:4566"   # optional, custom endpoint
```

Same AWS credential story as Glue:

```bash
AWS_PROFILE=my-profile AWS_REGION=eu-west-1 \
    ./target/release/sqe-coordinator ~/sqe-config.toml
```

The bucket ARN format is
`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`. AWS handles
namespace and table storage automatically.

Verified live against S3 Tables in eu-west-1
(`testtablebucket / testnamespace / daily_sales`): all standard
Iceberg operations work, including aggregations and filter
pushdown.

## JDBC catalog (Postgres / MySQL / SQLite)

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

| Prefix | Driver |
|--------|--------|
| `sqlite:path/to/file.db` | SQLite (local file) |
| `postgresql://...` or `postgres://...` | PostgreSQL |
| `mysql://...` | MySQL |

The catalog tables follow the standard Iceberg JDBC schema
(`iceberg_tables`, `iceberg_namespace_properties`). SQE creates them
on first connect.

## Hadoop (filesystem-only)

No metadata service. SQE walks a warehouse path and treats every
`metadata.json` it finds as a table. Useful for read-only access
to a warehouse that another engine wrote, or for one-off
investigations on a S3 / GCS / Azure prefix.

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

This is SQE's only native catalog backend. Every other backend
delegates to the upstream `iceberg-rust` builder via the
`iceberg-catalog-loader` crate.

## Verifying the connection

Once the coordinator is up, run these in order. Each one exercises
a deeper layer and tells you exactly where things break if they do.

```bash
# 1. Auth + Flight handshake
SQE_PASSWORD=s3cr3t ./target/release/sqe-cli --port 60051 --user root \
    -e "SELECT 1"

# 2. Catalog reachable, namespaces visible
SQE_PASSWORD=s3cr3t ./target/release/sqe-cli --port 60051 --user root \
    -e "SHOW SCHEMAS"

# 3. Pick a namespace, list its tables
SQE_PASSWORD=s3cr3t ./target/release/sqe-cli --port 60051 --user root \
    -e "SHOW TABLES IN <namespace>"

# 4. Read a row
SQE_PASSWORD=s3cr3t ./target/release/sqe-cli --port 60051 --user root \
    -e "SELECT * FROM <namespace>.<table> LIMIT 1"
```

If step 4 works, every other Iceberg query path works too: filter
pushdown, GROUP BY, JOIN, time-travel, the works.

## Connecting BI tools

SQE speaks Arrow Flight SQL, so any Flight SQL client works:

- **DBeaver**: install the Apache Arrow Flight SQL driver, point
  at `grpc://your-host:60051`, log in with the same username +
  password.
- **dbt**: use the `dbt-sqe` adapter
  (https://github.com/schubergphilis/dbt-sqe) which talks ADBC
  Flight SQL.
- **Python**: `pyarrow.flight.FlightClient`,
  or `adbc_driver_flightsql` for SQLAlchemy / pandas integration.
- **Trino-compat HTTP**: SQE also exposes a Trino HTTP endpoint on
  `trino_http_port` (default 18080). Useful for clients that don't
  speak Flight SQL but do speak Trino REST.

## Troubleshooting

**`Invalid or expired bearer token`** when the CLI passes
`--token`: the bearer was minted by something SQE's auth chain
doesn't recognize. Use `--user` + `SQE_PASSWORD` instead and let
SQE mint its own token.

**`Catalog 'X' build failed`** with no further detail: check the
coordinator log. Common causes:
- AWS credentials not on the chain (no `AWS_PROFILE`, no env vars,
  not running on EC2/EKS).
- HMS Thrift port not reachable.
- JDBC `url` typo (the prefix selects the driver).
- S3 Tables ARN region mismatch (the ARN's region must match
  whatever the AWS SDK resolves; set `AWS_REGION` to be safe).

**`No such table`** but the table exists in the catalog: namespace
case sensitivity. Iceberg namespaces are usually lowercase; some
HMS deployments treat them as case-insensitive.

**Slow first query** every time the coordinator restarts: cold
manifest cache. Subsequent queries hit `ObjectCache` and run
faster. This is expected.

## Where to go from here

- **`docs/catalogs.md`**: per-backend reference (full TOML schema,
  cargo features, AWS bucket ARN format).
- **`docs/datafusion-architecture.md`**: how SQE composes
  DataFusion, iceberg-rust, and Arrow Flight.
- **`docs/features/runtime-filter-pushdown.md`**: engineering log
  of the runtime filter work, including the eight failed attempts
  and what the bench data showed.
- **`docs/dbt-sqe.md`**: dbt adapter reference, including the
  Trino-compat function shims.
- **`vendor/iceberg-rust/README.md`**: vendored crates, SQE-only
  patches, alignment plan with upstream.
- **`docs/ebook/`**: the long-form story of how SQE got built. Read
  chapter 06 ("The Catalog Is the API") and chapter 06b ("Speaking
  to Many Catalogs") for the catalog story end-to-end.

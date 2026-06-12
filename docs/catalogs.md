# Catalog backends

SQE talks to Iceberg tables through one of six catalog backends. The
choice is per-deployment in `sqe.toml`. Default release builds ship
every backend; slim builds drop the unused ones to save binary size
(see "Slim builds" below).

Backend dispatch (for non-REST backends) goes through the vendored
`iceberg-catalog-loader` crate, which forwards a uniform
`(catalog_type, name, props)` shape to the upstream `CatalogBuilder`
implementation. The catalog loader replaces SQE's earlier per-backend
wrapper modules; the only SQE-native backend left is Hadoop, which
has no upstream loader equivalent.

## Quick reference

| Backend  | `type` value | Required keys | Optional keys | Cargo feature | Vendored crate |
|----------|--------------|---------------|---------------|---------------|----------------|
| REST     | `rest` (default) | `catalog_url`, `warehouse` (on `[catalog]`) | bearer-token / OAuth headers via runtime auth | `rest` (always) | `iceberg-catalog-rest` |
| HMS      | `hms` | `uri`, `warehouse` | | `hms` | `iceberg-catalog-hms` |
| Glue     | `glue` | `region`, `warehouse` | `endpoint` | `glue` | `iceberg-catalog-glue` |
| S3 Tables | `s3tables` | `table_bucket_arn` | `endpoint_url` | `s3tables` | `iceberg-catalog-s3tables` |
| JDBC     | `jdbc` | `url`, `warehouse` | | `sql-postgres` | `iceberg-catalog-sql` |
| Hadoop   | `hadoop` | `warehouse` | | `hadoop` | (SQE-native, `crates/sqe-catalog/src/backends/hadoop.rs`) |

## REST (Polaris, Nessie, Unity OSS, Glue REST, S3 Tables REST)

```toml
[catalog]
catalog_url = "https://polaris.example.com:18181/api/catalog"
warehouse   = "test_warehouse"

# `backend` defaults to "rest"; the block can be omitted.
[catalog.backend]
type = "rest"
```

REST is the default. AWS endpoints engage SigV4 automatically when
the server's `/v1/config` response advertises
`rest.sigv4-enabled=true`.

### Namespace visibility filtering

On the REST backend, SQE hides the names of namespaces the caller holds
no grants in from every metadata listing: `SHOW SCHEMAS`,
`information_schema.schemata`, and Flight SQL `GetDbSchemas`. When the
session's catalog provider is built, each namespace returned by
`listNamespaces` is probed once with the caller's bearer token
(`GetNamespace`, which Polaris authorizes as `LOAD_NAMESPACE_METADATA`
per caller). A 403 drops the name. The probes run 8 at a time, once per
session, never per query.

The filter fails open: a probe that times out or errors for any reason
other than 403 keeps the name listed. Namespace contents are protected
by the per-operation checks regardless of what the list shows, so a
catalog hiccup degrades to today's behavior instead of blanking the
user's schema tree. `information_schema` itself is always listed and
never probed.

```toml
[catalog]
# default true; set false to restore unfiltered listings
namespace_visibility_filter = false
```

Single-identity backends (Glue, HMS, JDBC, Hadoop) skip the filter
entirely: they authenticate as the coordinator's service identity, so
there is no per-caller answer to give. See the shared-identity warning
those backends log at startup.

## Hive Metastore

```toml
[catalog.backend]
type      = "hms"
uri       = "metastore.example.com:9083"   # Thrift host:port
warehouse = "s3a://bucket/warehouse"        # default warehouse path
```

Pulls in `volo-thrift` and `pilota`. Authentication via Kerberos /
Knox is not yet supported; deployments that need it should sit
behind a sidecar that handles the SASL handshake and exposes a
plain Thrift port.

## AWS Glue

```toml
[catalog.backend]
type      = "glue"
region    = "us-east-1"
warehouse = "s3://my-bucket/warehouse"
# endpoint = "http://localhost:4566"  # optional, e.g. LocalStack
```

Pulls in `aws-sdk-glue` + `aws-config`. Authentication uses the
standard AWS SDK credential chain: env vars
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`),
profiles (`~/.aws/credentials`), IMDS (when running on EC2 / EKS),
or assume-role flows configured via `~/.aws/config`.

## AWS S3 Tables (managed Iceberg)

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
# endpoint_url   = "http://localhost:4566"  # optional, custom endpoint
```

The bucket ARN format is
`arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`. AWS S3 Tables
namespaces map to S3 Tables namespaces; tables map to S3 Tables
tables; storage is automatically managed by AWS.

Pulls in `aws-sdk-s3tables`. Shares the AWS SDK runtime that `glue`
already pulls, so the incremental binary cost on top of an
AWS-enabled build is small. Standalone `--features rest,s3tables`
builds also work and are a good fit for AWS-first deployments that
do not need Glue compatibility.

## JDBC (Postgres, MySQL, SQLite)

```toml
[catalog.backend]
type      = "jdbc"
url       = "postgresql://user:pass@host:5432/iceberg"
warehouse = "s3://my-bucket/warehouse"
```

The `url` prefix selects the driver:

- `sqlite:path/to/file.db` for local files
- `postgresql://...` or `postgres://...` for Postgres
- `mysql://...` for MySQL

The catalog tables follow the Iceberg JDBC catalog schema
(`iceberg_tables`, `iceberg_namespace_properties`).

## Hadoop (filesystem-only)

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

No metadata service. SQE walks `warehouse` for `metadata.json` files
and treats the prefix as the catalog. Useful for read-only access
to a warehouse written by another engine without standing up a
metastore. Implemented in `crates/sqe-catalog/src/backends/hadoop.rs`.

## Slim builds

Default release builds include every backend. Operators who want a
smaller image (e.g. Kubernetes deployments behind Polaris where AWS
SDK weight is wasted) can opt out:

```bash
# REST only - no AWS SDK, no Thrift, no sqlx
cargo build --release --no-default-features --features rest

# REST + AWS managed Iceberg
cargo build --release --no-default-features --features rest,glue,s3tables

# REST + Hive
cargo build --release --no-default-features --features rest,hms
```

Approximate binary cost on top of a `rest`-only build:

- `hadoop`: ~0 (uses existing `object_store`)
- `sql-postgres`: 5-10 MB (sqlx + Postgres driver)
- `hms`: 10-15 MB (volo-thrift)
- `glue`: 50-80 MB (full AWS SDK)
- `s3tables`: ~5 MB on top of `glue` (shares AWS SDK runtime)

Default release binary lands around 180-200 MB.

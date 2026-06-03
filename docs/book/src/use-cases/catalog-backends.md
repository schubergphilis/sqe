# Catalog backends

SQE talks to one catalog at a time, selected by a single `[catalog.backend]`
block. The same binary reaches Apache Polaris, AWS Glue, AWS S3 Tables, Unity
Catalog OSS, Hive Metastore, Project Nessie, and a bare filesystem warehouse.
This page is the validation view: for each backend, how to verify it works and
what came back. For the full per-backend TOML, credential setup, and
troubleshooting, see [Catalog backends](../getting-started/catalogs.md).

Catalog weight is opt-in through cargo features. A default REST-only build
pulls no AWS SDK, no Thrift, no sqlx. Add `glue`, `s3tables`, `hms`, or `sql`
as needed; `rest` and `hadoop` are always available.

## Polaris (and any Iceberg REST catalog)

The default. Verified throughout the test suite and every use-case page that
uses `docker-compose.test.yml`.

```toml
[catalog]
polaris_url = "http://localhost:18181/api/catalog"
warehouse   = "test_warehouse"
```

## AWS Glue

Native AWS SDK against the regional Glue Data Catalog. Credentials come from
the standard provider chain (`AWS_PROFILE`, instance profile, SSO).

```toml
[catalog.backend]
type      = "glue"
region    = "eu-central-1"
warehouse = "s3://my-bucket/warehouse"
```

Verify (live AWS):

```bash
set -a; source .env; set +a   # AWS_PROFILE, AWS_REGION, SQE_TEST_GLUE_WAREHOUSE
cargo test -p sqe-catalog --features glue --test backends_integration -- --ignored glue::
```

Run live this round against `eu-central-1` (account `311141556126`):

```text
test glue::live_glue_namespace_round_trip ... ok
test result: ok. 1 passed; 0 failed (with s3tables: 2 passed in 8.38s)
```

## AWS S3 Tables

Managed Iceberg, reached through the federated Glue Iceberg REST endpoint with
AWS SigV4 on every request (the vendored `iceberg-catalog-rest` enables this
with the `aws-sigv4` feature).

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-central-1:ACCOUNT:bucket/NAME"
```

Verify (live AWS):

```bash
set -a; source .env; set +a   # + SQE_TEST_S3TABLES_WAREHOUSE
cargo test -p sqe-catalog --features s3tables --test backends_integration -- --ignored s3_tables::
```

Run live this round through the federated Glue Iceberg REST endpoint:

```text
S3 Tables round-trip ok: namespace=table_demo_analytics tables=["table_demo_analytics.table_user_events"]
test s3_tables::list_namespaces_via_glue_rest ... ok
```

## Unity Catalog OSS

Unity OSS exposes an Iceberg REST adapter at
`/api/2.1/unity-catalog/iceberg/`. The OSS image is read-only on create/drop,
so the verification is a read smoke against the seeded table.

```bash
docker compose -f docker-compose.unity.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --test backends_integration -- --ignored unity_catalog::
```

The Unity test is in the suite (a read smoke against the seeded
`unity.default.marksheet_uniform` table). A live re-run during this round was
blocked by local Docker capacity (the test stack plus several catalog stacks
saturated the daemon); run the command above against a healthy daemon to
reproduce.

## Hive Metastore

Thrift metastore. Requires the `hms` feature.

```bash
docker compose -f docker-compose.hms.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --features hms --test backends_integration -- --ignored hms::
```

Covered by the suite (`hms::live_hms_namespace_round_trip`, a create / list /
drop round-trip against the Thrift metastore). Run the command above to
reproduce against a running HMS.

## Project Nessie

Git-like Iceberg REST catalog.

```bash
docker compose -f docker-compose.nessie.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --test backends_integration -- --ignored nessie::
```

Covered by the suite (`nessie::nessie_namespace_round_trip`). Run the command
above against a running Nessie to reproduce.

## Hadoop (filesystem, no catalog service)

No metadata service. SQE walks the warehouse prefix for `metadata.json`. See
[Embedded and single-node CLI](./embedded.md) for the catalog-free flow.

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

## How it is tested

- `crates/sqe-catalog/tests/backends_integration.rs`: live round-trips per
  backend (create / list / drop namespace, or read smoke), gated on
  `#[ignore]` plus the `.env` warehouse variables.
- `crates/sqe-catalog/tests/mount_*_test.rs`: mount-time validation per
  backend (rejects bad secrets, requires a warehouse, and so on).

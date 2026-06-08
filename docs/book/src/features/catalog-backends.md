# Supported catalog backends

SQE supports multiple Iceberg catalog backends and wire-protocol client
adapters. The same binary works with every option below; choose by setting the
`[catalog.backend]` block in your TOML. Catalog weight is opt-in through cargo
features: a default REST-only build pulls no AWS SDK, no Thrift, no sqlx. Add
`glue`, `s3tables`, `hms`, or `sql` as needed; `rest` and `hadoop` are always
available.

For per-backend TOML configuration, credential setup, and troubleshooting, see
[Catalog backends](../getting-started/catalogs.md).

---

## Polaris (and any Iceberg REST catalog)

Apache Polaris exposes the Iceberg REST catalog specification. SQE uses this as
its default backend. Any Iceberg REST-compatible service (Polaris, Lakeformation
REST, custom) works with the same config block.

```toml
[catalog]
polaris_url = "http://localhost:18181/api/catalog"
warehouse   = "test_warehouse"
```

See the quickstart: [Polaris + Keycloak (client credentials)](../quickstart/polaris-keycloak-client-id.md).

---

## AWS Glue Data Catalog

Native AWS SDK integration against the regional Glue Data Catalog. Credentials
come from the standard provider chain (`AWS_PROFILE`, instance profile, SSO).

```toml
[catalog.backend]
type      = "glue"
region    = "eu-central-1"
warehouse = "s3://my-bucket/warehouse"
```

See the quickstart: [AWS Glue Data Catalog](../quickstart/aws-glue.md).

---

## AWS S3 Tables

Managed Iceberg via the federated Glue Iceberg REST endpoint with AWS SigV4
authentication on every request. Backed by the vendored `iceberg-catalog-rest`
crate with the `aws-sigv4` feature.

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:eu-central-1:ACCOUNT:bucket/NAME"
```

See the quickstart: [AWS S3 Tables (managed Iceberg)](../quickstart/aws-s3-tables.md).

---

## Unity Catalog OSS

Unity Catalog OSS exposes an Iceberg REST adapter at
`/api/2.1/unity-catalog/iceberg/`. The OSS image is read-only on create/drop;
use for query workloads.

```bash
docker compose -f docker-compose.unity.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --test backends_integration -- --ignored unity_catalog::
```

See the quickstart: [Unity Catalog OSS (Iceberg REST, read-only)](../quickstart/unity-oss.md).

---

## Hive Metastore

Thrift metastore protocol. Requires the `hms` cargo feature.

```bash
docker compose -f docker-compose.hms.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --features hms --test backends_integration -- --ignored hms::
```

Covered by the suite (`hms::live_hms_namespace_round_trip`, a create / list /
drop round-trip against the Thrift metastore).

---

## Project Nessie

Git-like Iceberg REST catalog with branch/tag semantics.

```bash
docker compose -f docker-compose.nessie.yml up -d
set -a; source .env; set +a
cargo test -p sqe-catalog --test backends_integration -- --ignored nessie::
```

See the quickstart: [Project Nessie (Iceberg REST catalog)](../quickstart/nessie.md).

---

## Hadoop (filesystem warehouse, no catalog service)

No metadata service. SQE walks the warehouse prefix for `metadata.json` files.
See [Embedded mode](./embedded.md) for the catalog-free embedded flow.

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

---

## Quack (DuckDB wire protocol)

Quack is **not an Iceberg catalog**; it is a client/server wire protocol that
lets DuckDB clients (and other Quack-compatible tools) issue SQL to SQE and
receive Arrow-serialised results. It sits alongside the Iceberg catalog
backends, not in competition with them.

See [Quack](../quickstart/quack.md).

---

## How catalog backends are tested

- `crates/sqe-catalog/tests/backends_integration.rs`: live round-trips per
  backend (create / list / drop namespace, or read smoke), gated on
  `#[ignore]` plus the `.env` warehouse variables.
- `crates/sqe-catalog/tests/mount_*_test.rs`: mount-time validation per
  backend (rejects bad secrets, requires a warehouse, and so on).

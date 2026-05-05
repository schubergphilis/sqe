# iceberg-rust (SQE vendored fork)

This is a vendored copy of the [RisingWave Labs iceberg-rust fork](https://github.com/risingwavelabs/iceberg-rust),
branch `dev_rebase_main_20260303` at commit `645f02a4b533`, with DataFusion upgraded to 53.0 / Arrow 58 / Parquet 58.

## Vendored crates

| Crate | Purpose | Used by |
|-------|---------|---------|
| `iceberg` | Core Iceberg types, expressions, scan API, transactions | always |
| `iceberg-catalog-rest` | Iceberg REST protocol client (Polaris, Nessie, Unity, Glue REST, S3 Tables REST) | always |
| `iceberg-catalog-glue` | AWS Glue Data Catalog over the AWS SDK | sqe-catalog `glue` feature |
| `iceberg-catalog-hms` | Hive Metastore over Thrift | sqe-catalog `hms` feature |
| `iceberg-catalog-s3tables` | AWS S3 Tables (managed Iceberg) over the AWS SDK | sqe-catalog `s3tables` feature |
| `iceberg-catalog-sql` | JDBC catalog (Postgres / MySQL / SQLite) via sqlx | sqe-catalog `sql` feature |
| `iceberg-catalog-loader` | Dispatches catalog construction by string type name with a uniform `(name, props)` shape; used by SQE in `crates/sqe-catalog/src/rest_catalog.rs::for_session_other_backend` | always |
| `iceberg-datafusion` | DataFusion `TableProvider`, `IcebergTableScan`, runtime filter bridge, predicate converter | always |
| `iceberg_test_utils` | Shared test helpers | dev-only |

The loader crate is patched (vs upstream) to make its backend deps
optional, gated on cargo features, so a slim build does not
transitively pull every backend's AWS SDK / Thrift / sqlx weight.
The trait `BoxedCatalogBuilder` is also patched to require
`Send + Sync` so the returned box can cross await points in async
contexts.

## Why a fork?

Apache upstream iceberg-rust (v0.9.0) lacks:
- `RewriteFilesAction` / `OverwriteFilesAction` (Copy-on-Write DELETE/UPDATE)
- `PositionDeleteFileWriter` (Merge-on-Read position deletes)
- `DeletionVectorWriter` (Iceberg V3)

The RisingWave fork provides all of these. SQE applied the DF 53 migration
on top (same changes as upstream PR #2206).

## Upstream tracking

- RisingWave fork: `dev_rebase_main_20260303` @ `645f02a4b533`
- Apache upstream: tracking PRs #2185 (OverwriteAction) and #2203 (RowDeltaAction)
- When upstream merges these, SQE will migrate to official apache/iceberg-rust

## SQE-only patches in this vendor copy

Five patch families ride on top of the upstream snapshot. Each is
documented inline at the touch site so a future rebase can re-apply
them quickly.

1. **`iceberg::expr::dynamic` (DynamicPredicate API)**: runtime
   filter pushdown into IcebergTableScan. Files: `crates/iceberg/src/expr/dynamic.rs`,
   `crates/iceberg/src/scan/mod.rs`, `crates/iceberg/src/arrow/reader.rs`.
   Filed upstream as apache/iceberg-rust#2376; not yet landed.
2. **`iceberg-catalog-rest::sigv4`**: AWS SigV4 signer gated behind
   the `aws-sigv4` cargo feature. Files: `crates/catalog/rest/src/sigv4.rs`,
   `crates/catalog/rest/src/client.rs`, `crates/catalog/rest/src/lib.rs`.
   Used for AWS S3 Tables and Glue REST federation. Not filed upstream yet.
3. **`CatalogBuilder::with_storage_factory`**: trait default in
   `iceberg::catalog`, added so the upstream HMS / Glue / SQL catalog
   crates compile against the fork's trait unmodified.
4. **`FileIOBuilder` scheme-string shims**: in the vendored apache
   v0.9.0 catalog crates (`hms`, `glue`, `sql`) so they speak the
   fork's FileIO API.
5. **`iceberg-catalog-loader` feature gates + `Send + Sync`**: added
   to the loader so SQE's slim builds work and so the boxed builder
   can cross await points. Files:
   `crates/catalog/loader/Cargo.toml`,
   `crates/catalog/loader/src/lib.rs`.

## Alignment opportunity (deferred)

Risingwavelabs's main branch landed its own DataFusion 53 + Arrow 58
rebase on 2026-04-15 (commit `fb290e4c9`, PR #148). SQE's downstream
DF 53 patches now overlap with upstream main; we are no longer the
only fork carrying that work.

Aligning the vendor pin with risingwavelabs main would let us drop
the DF 53 patch family. The remaining SQE-only patches above would
need to ride on top of the new base.

Costs of doing the alignment now: roughly a day to redo the rebase
and re-apply the five patch families. Benefit: smaller patch surface
vs upstream, easier next vendor refresh.

The natural moment to align is when one of these happens:

- We upstream the SigV4 signer (item 2 above) into either
  risingwavelabs or apache/iceberg-rust. That removes one patch
  family from the rebase.
- apache/iceberg-rust#2376 lands (item 1 above). That removes
  another.
- We need a feature from risingwavelabs main that we don't
  currently have. Then we get the rebase as a side effect.
- Next major version bump (DataFusion 54 / Arrow 59) ships and we
  rebase anyway.

Until one of those, the vendor stays pinned to `645f02a4b533` with
SQE's DF 53 patches.

## Catalog config: URL and bucket conventions

User-facing config in `sqe.toml`. Each `[catalog]` block selects
exactly one backend; the keys per backend mirror the upstream
`*_CATALOG_PROP_*` constants and what the upstream builders expect.

### REST (Polaris / Nessie / Unity OSS / Glue REST / S3 Tables REST)

```toml
[catalog]
polaris_url = "https://polaris.example.com:18181/api/catalog"
warehouse   = "test_warehouse"
# `backend` defaults to "rest" so it can be omitted.
[catalog.backend]
type = "rest"
```

REST is the default. AWS endpoints engage SigV4 automatically when
the server's `/v1/config` response advertises
`rest.sigv4-enabled=true` (see SQE-only patch family 2 above).

### Hive Metastore

```toml
[catalog.backend]
type      = "hms"
uri       = "metastore.example.com:9083"   # Thrift host:port
warehouse = "s3a://bucket/warehouse"        # default warehouse path
```

Requires the `hms` cargo feature on `sqe-catalog` (default-on).
Pulls in `volo-thrift` and `pilota`.

### AWS Glue

```toml
[catalog.backend]
type      = "glue"
region    = "us-east-1"
warehouse = "s3://my-bucket/warehouse"
# endpoint = "http://localhost:4566"        # optional, e.g. LocalStack
```

Requires the `glue` cargo feature (default-on). Pulls in
`aws-sdk-glue` + `aws-config`. Authentication uses the standard AWS
SDK chain (env vars, profiles, IMDS).

### AWS S3 Tables (managed Iceberg)

```toml
[catalog.backend]
type             = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
# endpoint_url   = "http://localhost:4566"  # optional, custom endpoint
```

Requires the `s3tables` cargo feature (default-on). Pulls in
`aws-sdk-s3tables` (shares the AWS SDK runtime already pulled by
`glue`, so the incremental binary cost is small). Authentication
uses the standard AWS SDK chain.

The bucket ARN format is `arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`.
S3 Tables namespaces map to S3 Tables namespaces; tables map to
S3 Tables tables; storage is automatically managed by AWS.

### JDBC (Postgres / MySQL / SQLite)

```toml
[catalog.backend]
type      = "jdbc"
url       = "postgresql://user:pass@host:5432/iceberg"
warehouse = "s3://my-bucket/warehouse"
```

Requires the `sql-postgres` cargo feature (default-on). The url
prefix selects the driver: `sqlite:` for local files, `postgresql:`
for Postgres, `mysql:` for MySQL.

### Hadoop (filesystem-only, SQE-native)

```toml
[catalog.backend]
type      = "hadoop"
warehouse = "s3://my-bucket/warehouse"
```

No metadata service. SQE walks `warehouse` for `metadata.json` files
and treats the prefix as the catalog. Implemented in
`crates/sqe-catalog/src/backends/hadoop.rs`. Requires the `hadoop`
cargo feature (default-on, no extra dependency cost).

## Slim builds

A REST-only build (no AWS SDK, no Thrift, no sqlx, no S3 Tables)
ships in roughly 80 MB compressed:

```bash
cargo build --release --no-default-features --features rest
```

Add features as needed: `--features rest,glue,s3tables` for
"REST plus AWS." `--features rest,hms` for "REST plus Hive."
The default ships every backend (see
`crates/sqe-catalog/Cargo.toml`).

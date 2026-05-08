# Storage backends

SQE separates two concerns:

- **Catalog backend**: where the *table metadata* lives. Polaris, Nessie, AWS Glue, S3 Tables, Hive Metastore, JDBC, Hadoop. See [Catalog backends](./catalogs.md).
- **Storage backend**: where the *data files* live. AWS S3, GCS, ADLS Gen2, R2, MinIO, Ceph, local filesystem. This page.

Both are independent. A table whose metadata is in Polaris can have data files in any storage backend the engine knows how to talk to. The catalog hands SQE a `s3://...` (or `gs://...`, `abfss://...`) URL when loading a table; SQE picks the right object-store driver from the URL scheme.

Implementation lives in `crates/sqe-catalog/src/file_tvf_common.rs` and `crates/sqe-catalog/src/lazy_object_store.rs`.

## Compatibility matrix

| Backend | URL scheme | Default build | TVF reads | Catalog reads | Writes | Notes |
|---|---|---|---|---|---|---|
| Local filesystem | `/path` or `./path` | yes | yes | yes | yes | No setup required. |
| AWS S3 | `s3://bucket/key` | yes | yes | yes | yes | Provider chain (env / `~/.aws` / IMDS / IRSA) when no inline creds. |
| AWS S3 (SSE / KMS) | `s3://bucket/key` | yes | yes | yes | yes | Server-side encryption is transparent. |
| Cloudflare R2 | `s3://bucket/key` (S3-compatible endpoint) | yes | yes | yes | yes | Set `endpoint = https://<account>.r2.cloudflarestorage.com`, `region = auto`. |
| MinIO | `s3://bucket/key` | yes | yes | yes | yes | Allow plain HTTP via `s3_allow_http = true`. |
| Ceph RGW | `s3://bucket/key` | yes | yes | yes | yes | Same as MinIO. |
| SeaweedFS | `s3://bucket/key` | yes | yes | yes | yes | Same as MinIO. |
| Garage | `s3://bucket/key` | yes | yes | yes | yes | Same as MinIO. |
| rustfs | `s3://bucket/key` | yes | yes | yes | yes | Same as MinIO. |
| HTTPS | `https://host/path` | yes | yes | partial | no | Lazy `HttpStore` per host (V10). Read-only. |
| HuggingFace | `hf://datasets/...` | yes | yes | no | no | V10 + V12.1. Auto-resolves to HTTPS. Read-only. |
| Azure ADLS Gen2 | `abfss://container@account/path` | **opt-in** | when wired | when wired | when wired | Cargo feature flip; see below. |
| Google Cloud Storage | `gs://bucket/path` | **opt-in** | when wired | when wired | when wired | Cargo feature flip; see below. |

"Default build" means the cargo workspace ships the driver linked. For ADLS / GCS, you need a one-line feature flip plus an init helper, then rebuild.

## Local filesystem

No configuration. Pass an absolute or relative path:

```sql
SELECT * FROM '/data/orders.parquet';
SELECT * FROM read_csv('./report.csv');
SELECT * FROM read_delta('/var/lake/orders');
```

Catalog backends that store data on local disk (`hadoop` type, or any REST catalog with `file://` warehouse paths) work the same way.

## AWS S3

The default storage backend. Two ways to provide credentials:

### 1. AWS provider chain (recommended for production)

When inline `access_key` / `secret_key` are absent, SQE delegates to the AWS SDK provider chain:

1. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`).
2. `~/.aws/credentials` profile (`AWS_PROFILE` selects which).
3. EC2 IMDS instance role.
4. EKS IRSA service-account role.

```toml
# config.toml
[storage]
type   = "s3"
region = "eu-west-1"
# No access_key / secret_key here -> provider chain.
```

```sql
SELECT * FROM read_parquet('s3://bucket/key.parquet');
```

### 2. Inline credentials (per-query override)

```sql
SELECT * FROM read_parquet(
    's3://bucket/path/*.parquet',
    access_key => 'AKIA...',
    secret_key => '...',
    endpoint   => 'https://s3.eu-west-1.amazonaws.com',
    region     => 'eu-west-1'
);
```

Inline values win over `[storage]` defaults for that query only.

## Cloudflare R2

R2 speaks the S3 protocol. Two pieces are non-default:

- **Endpoint** points at your R2 account URL.
- **Region** is the literal string `auto` (R2 ignores region but rejects the empty string).

```sql
SELECT * FROM read_parquet(
    's3://my-r2-bucket/data.parquet',
    access_key => '<R2_ACCESS_KEY_ID>',
    secret_key => '<R2_SECRET_ACCESS_KEY>',
    endpoint   => 'https://<account-id>.r2.cloudflarestorage.com',
    region     => 'auto'
);
```

For permanent setup put the endpoint and creds in `[storage]`:

```toml
[storage]
type            = "s3"
endpoint        = "https://<account-id>.r2.cloudflarestorage.com"
region          = "auto"
s3_access_key   = "<R2_ACCESS_KEY_ID>"
s3_secret_key   = "<R2_SECRET_ACCESS_KEY>"
```

## MinIO, Ceph RGW, SeaweedFS, Garage, rustfs

All are S3-compatible. Two extra knobs versus AWS S3:

- **Endpoint**: your server URL. Typical: `http://minio:9000` (Docker), `https://s3.internal:9000` (TLS).
- **`s3_allow_http`**: set to `true` if the endpoint is plain HTTP. SQE refuses HTTP by default to prevent accidental cleartext credentials.

```sql
SELECT * FROM read_parquet(
    's3://bucket/data.parquet',
    access_key => 'minio-access-key',
    secret_key => 'minio-secret-key',
    endpoint   => 'http://localhost:9000',
    region     => 'us-east-1'
);
```

```toml
[storage]
type            = "s3"
endpoint        = "http://localhost:9000"
region          = "us-east-1"
s3_access_key   = "minio-access-key"
s3_secret_key   = "minio-secret-key"
s3_allow_http   = true
```

## HTTPS

Any `https://` URL works without registration. The first request to a new `scheme://host` builds an `HttpStore` on the fly and caches it in the session's object-store registry. Subsequent reads from the same host reuse the cached store.

```sql
SELECT * FROM 'https://example.com/data.parquet';
SELECT * FROM read_csv('https://raw.githubusercontent.com/.../titanic.csv');
SELECT * FROM read_json('https://api.example.com/events.ndjson');
```

Configurable via the `[storage.http]` block (custom headers, bearer auth):

```toml
[storage.http]
default_headers = { Authorization = "Bearer ${API_TOKEN}" }
```

HTTPS reads are stateless: each request fetches the byte range needed for the current Parquet operation. Range request support is required (every modern object store has it).

## HuggingFace `hf://`

The `hf://` resolver translates HuggingFace dataset, model, and spaces URLs into HTTPS calls against the Hub. Public datasets work anonymously; private datasets read `HF_TOKEN` from the environment.

```sql
-- Default revision (main)
SELECT * FROM read_parquet(
    'hf://datasets/squad/plain_text/train-00000-of-00001.parquet'
);

-- Pinned revision (DuckDB-style, V12.1)
SELECT * FROM read_csv('hf://datasets/foo/bar@v1.0/data.csv');

-- Auto-generated Parquet view (V12.1)
SELECT * FROM read_parquet(
    'hf://datasets/foo/bar@~parquet/default/train/0.parquet'
);

-- Equivalent ?revision query parameter
SELECT * FROM read_csv('hf://datasets/foo/bar/data.csv?revision=v1.0');
```

Glob expansion (`**/*.parquet`) on `hf://` is tracked for V12.2; today the path must point to a specific file.

See [File-format TVFs](../features/file-format-tvfs.md) for the full path-form table.

## Azure ADLS Gen2

**Status: not linked in the default build.** Adding it is a one-line `Cargo.toml` change plus a small registration helper. The underlying object_store crate already supports Azure; we leave it off the default to keep the binary slim.

### Wire it up

In the workspace `Cargo.toml`, add `azure` to the `object_store` features list:

```toml
[workspace.dependencies]
object_store = { version = "0.13", features = ["aws", "http", "azure"] }
```

Then add a `register_azure_store_if_needed` helper alongside the existing S3 / HTTP helpers in `crates/sqe-catalog/src/file_tvf_common.rs`. The pattern is the same as `register_s3_store_if_needed`: read credentials from inline args or `[storage.azure]`, build an `AzureBuilder`, register in the session's object-store map.

PR welcome. The work is small and the test pattern is already in place for S3 / HTTP / HF.

### URL form when wired

```sql
SELECT * FROM read_parquet(
    'abfss://container@account.dfs.core.windows.net/path/data.parquet',
    azure_access_key => '<key>'
);
```

Or with shared key in `[storage.azure]`:

```toml
[storage.azure]
account_name = "myaccount"
access_key   = "<storage-account-key>"
```

OAuth2 / managed-identity auth is on the same wire-up PR.

## Google Cloud Storage

**Status: not linked in the default build.** Same shape as Azure: a Cargo feature flip plus a registration helper.

### Wire it up

```toml
[workspace.dependencies]
object_store = { version = "0.13", features = ["aws", "http", "gcp"] }
```

Plus a `register_gcs_store_if_needed` helper. Service-account JSON credentials read from `GOOGLE_APPLICATION_CREDENTIALS` work via the underlying `gcp` driver.

### URL form when wired

```sql
SELECT * FROM read_parquet(
    'gs://bucket/path/data.parquet'
);
```

```toml
[storage.gcs]
service_account_path = "/var/secrets/gcs-key.json"
```

ADC (Application Default Credentials) chain works the same way as AWS provider chain: env var, gcloud config, GCE metadata server, Workload Identity on GKE.

## Per-query vs configured

Inline TVF arguments override `[storage]` defaults for one query. This matters when:

- Querying a dataset in a different region than your default storage.
- Running ad-hoc reads against a customer's bucket without changing engine config.
- Passing through end-user credentials in a multi-tenant deployment (see [Authentication Flow](../architecture/auth-flow.md) for the policy-controlled variant).

The Iceberg catalog backend has its own credential flow (catalog credential vending; see [Iceberg Integration](../features/iceberg.md)). Storage credentials there come from the catalog's STS exchange, not `[storage]`.

## Why two layers

Catalogs name tables; storage holds bytes. Iceberg already separates these in the spec (every table's `location` field is independent of the catalog hosting it). SQE keeps the same separation. The pay-off:

- The same dataset can be served by multiple catalogs simultaneously (Polaris in dev, Glue REST in prod, with both pointed at the same S3 prefix).
- A catalog migration (Polaris -> Nessie) does not move any data files.
- Storage feature flags (R2 vs AWS) do not touch metadata.

## Implementation references

- `crates/sqe-catalog/src/file_tvf_common.rs`: shared inline-arg parsing for `read_parquet` / `read_csv` / `read_json` / `read_delta`. Decides which object-store driver to use based on URL scheme.
- `crates/sqe-catalog/src/lazy_object_store.rs`: V10's lazy HTTPS object-store registry. Same shape as the future Azure / GCS lazy registries.
- `crates/sqe-catalog/src/s3_store.rs`: AWS S3 driver registration with provider-chain fallback.
- `crates/sqe-catalog/src/iceberg_storage.rs`: catalog-backed storage credential resolution (vended creds vs static `[storage]`).
- The DuckDB-comparison audit row for backends lives in [`docs/duckdb-comparision.md`](../../../duckdb-comparision.md).

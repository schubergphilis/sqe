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
| Azure ADLS Gen2 | `abfss://container@account.dfs.core.windows.net/path` | yes | yes | yes | yes | Shared key, SAS, and Azurite emulator supported. |
| Azure (shorthand) | `azure://container/path`, `az://container/path` | yes | yes | yes | yes | Account name from `[storage.azure]` or `azure_account => '...'`. |
| Google Cloud Storage | `gs://bucket/path`, `gcs://bucket/path` | yes | yes | yes | yes | Service-account JSON path or inline; ADC fallback when neither set. |

All backends ship in the default `cargo build`. The `object_store` workspace dependency is built with `aws`, `http`, `azure`, and `gcp` features; no opt-in feature flip needed.

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

## Azure ADLS Gen2 / Blob

Three URL shapes are accepted:

| URL form | When to use |
|---|---|
| `abfss://<container>@<account>.dfs.core.windows.net/<path>` | Hadoop-style; account encoded in URL. Most portable across tools. |
| `abfs://...` | Same shape, plaintext variant. Avoid in production. |
| `azure://<container>/<path>`, `az://<container>/<path>` | Shorthand. Account comes from `[storage.azure]` or the `azure_account` inline arg. |

Three auth methods:

```sql
-- 1. Shared key (storage account key)
SELECT * FROM read_parquet(
    'abfss://my-container@myaccount.dfs.core.windows.net/path/data.parquet',
    azure_access_key => '<storage-account-key>'
);

-- 2. SAS token (sub-account scope)
SELECT * FROM read_csv(
    'abfss://logs@myaccount.dfs.core.windows.net/2026-05-08/events.csv',
    azure_sas_token => 'sv=2024-08-04&ss=b&srt=sco&sp=r...'
);

-- 3. Azurite emulator (local development)
SELECT * FROM read_parquet('azure://devstoreaccount1/test/data.parquet');
```

For permanent setup put credentials in `[storage.azure]`:

```toml
[storage]
azure_account     = "myaccount"
azure_access_key  = "<storage-account-key>"
# OR:
azure_sas_token   = "sv=2024-08-04&..."
# Local development against Azurite:
azure_use_emulator = true
```

OAuth2 / managed-identity auth is not yet wired through the inline args; service-account flows go through the AWS-style env-var fallback that `object_store::azure::MicrosoftAzureBuilder` provides.

## Google Cloud Storage

```sql
-- 1. Service-account JSON file
SELECT * FROM read_parquet(
    'gs://my-bucket/path/data.parquet',
    gcs_service_account_path => '/var/secrets/gcs-key.json'
);

-- 2. Inline service-account JSON
SELECT * FROM read_csv(
    'gs://my-bucket/data.csv',
    gcs_service_account_key => '{"type":"service_account",...}'
);

-- 3. Application Default Credentials (gcloud config / GCE metadata / GKE Workload Identity)
SELECT * FROM read_parquet('gs://my-bucket/data.parquet');
```

For permanent setup:

```toml
[storage]
gcs_service_account_path = "/var/secrets/gcs-key.json"
# OR inline:
gcs_service_account_key  = "{\"type\":\"service_account\",...}"
```

When neither is set the underlying GCS driver falls back to ADC: `GOOGLE_APPLICATION_CREDENTIALS` env var, `gcloud config`, GCE metadata server, GKE Workload Identity. No SQE config needed for the workload-identity path.

The `gcs://` scheme is also accepted as a synonym for `gs://`.

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

- `crates/sqe-catalog/src/file_tvf_common.rs`: shared inline-arg parsing for `read_parquet` / `read_csv` / `read_json` / `read_delta`. URL-scheme dispatch via `is_s3_path`, `is_http_path`, `is_hf_path`, `is_azure_path`, `is_gcs_path`.
  - `register_s3_store_if_needed`, `register_azure_store_if_needed`, `register_gcs_store_if_needed`, `register_http_store_if_needed`: per-backend `SessionContext` registration.
  - `extract_bucket`, `extract_azure_container_account`, `extract_gcs_bucket`: URL parsers.
- `crates/sqe-catalog/src/lazy_object_store.rs`: V10's lazy HTTPS object-store registry.
- `crates/sqe-catalog/src/iceberg_storage.rs`: catalog-backed storage credential resolution (vended creds vs static `[storage]`).
- Workspace `Cargo.toml` line 37: `object_store = { ..., features = ["aws", "http", "azure", "gcp"] }`.
- The DuckDB-comparison audit row for backends lives at [getsqe.com/compare/duckdb](https://getsqe.com/compare/duckdb).

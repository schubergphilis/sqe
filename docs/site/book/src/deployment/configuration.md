# Configuration

SQE is configured via a **TOML file** with **environment variable overrides**. Environment variables take precedence over the config file.

## Config File

Default path: `sqe.toml` in the current directory. Override with:

```bash
sqe-server --config /etc/sqe/sqe.toml
# or
SQE_CONFIG=/etc/sqe/sqe.toml sqe-server
```

## Full Reference

```toml
[coordinator]
flight_sql_port = 50051         # Flight SQL gRPC port
trino_http_port = 8080          # Trino-compat HTTP port (0 to disable)
mode = "hybrid"                 # "coordinator", "worker", "hybrid", "local", "distributed"
worker_urls = []                # Worker Flight URLs for distributed mode
worker_secret = ""              # Shared secret for worker heartbeat auth (empty disables the check)
debug = false                   # When true, error messages include internal details (dev only)
flight_compression = "lz4"      # IPC compression for client DoGet responses
shuffle_compression = "zstd"    # IPC compression for internal DoExchange shuffle

[coordinator.tls]
cert_file = ""                  # PEM certificate — TLS enabled when both cert + key are set
key_file = ""                   # PEM private key
ca_file = ""                    # Optional PEM CA for mTLS client certificate verification

[worker]
coordinator_url = "http://coordinator:50051"
flight_port = 50052             # Worker Flight port
advertise_url = ""              # URL the coordinator uses to reach this worker.
                                # Empty -> auto-derived (POD_IP, else HOSTNAME if
                                # an IP, else first non-loopback interface). Never
                                # advertise 0.0.0.0; the coordinator rejects it.
heartbeat_interval_secs = 5     # Health check interval
memory_limit = "8GB"            # Worker memory limit (supports B/KB/MB/GB/TB)
spill_to_disk = true            # Allow spilling large sorts/joins to disk
spill_dir = "/tmp/sqe-spill"    # Temp directory for spilling

[auth]
keycloak_url = ""               # Keycloak base URL (OIDC password grant mode)
realm = ""                      # Keycloak realm name
token_endpoint = ""             # Generic OAuth2 token endpoint (client_credentials mode)
client_id = "sqe-client"        # OIDC client ID (required)
client_secret = ""              # Set via SQE_AUTH__CLIENT_SECRET env var
token_refresh_buffer_secs = 60  # Refresh tokens this many seconds before expiry
ssl_verification = true         # Set false for dev (self-signed certs)

[catalog]
catalog_url = "http://polaris:8181/api/catalog"   # REST catalog endpoint
warehouse = "iceberg"           # warehouse identifier the catalog expects
metadata_cache_ttl_secs = 30    # Table metadata cache TTL
default_table_format_version = 2 # Iceberg table format version (2 or 3)
trust_sort_order = false        # Trust Iceberg sort order for all columns, not just partition keys
small_file_threshold_mb = 3     # Max file size for the direct-read fast path (0 to disable)
parquet_compression = "zstd"    # Write-path Parquet codec: zstd, lz4, snappy, none

# `catalog_url` accepts any Iceberg REST endpoint. SQE has been
# verified live against Apache Polaris, Project Nessie 0.107+,
# Unity Catalog OSS, AWS Glue Iceberg REST, and AWS S3 Tables REST.
# For AWS REST endpoints the vendored REST client signs requests
# with SigV4 when the server advertises `rest.sigv4-enabled=true`
# in its /v1/config defaults.

# When `[catalog.backend]` is omitted, SQE defaults to `type = "rest"`
# and uses `catalog_url` + `warehouse` above. To target a non-REST
# catalog (HMS, AWS Glue native, AWS S3 Tables native, JDBC, Hadoop),
# set the backend block explicitly. See `docs/book/src/getting-started/
# catalogs.md` for the full per-backend reference.

# [catalog.backend]
# type = "hms"
# uri  = "metastore.example.com:9083"
# warehouse = "s3a://my-bucket/warehouse"

# [catalog.backend]
# type   = "glue"
# region = "eu-central-1"
# warehouse = "s3://my-bucket/warehouse"
# # endpoint = "http://localhost:4566"   # optional, e.g. LocalStack

# [catalog.backend]
# type             = "s3tables"
# table_bucket_arn = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-bucket"
# # endpoint_url   = "http://localhost:4566"

# [catalog.backend]
# type      = "jdbc"
# url       = "postgresql://user:pass@host:5432/iceberg"
# warehouse = "s3://my-bucket/warehouse"

# [catalog.backend]
# type      = "hadoop"
# warehouse = "s3://my-bucket/warehouse"

# Non-REST backends dispatch through the upstream
# `iceberg-catalog-loader` crate. End-to-end SQL through HMS, Glue,
# S3 Tables, and JDBC works on main today. Hadoop has its own
# dispatch in `crates/sqe-catalog/src/backends/hadoop.rs`.

[storage]
s3_endpoint = "http://s3:9000"
s3_region = "us-east-1"
s3_access_key = ""              # Set via SQE_STORAGE__S3_ACCESS_KEY
s3_secret_key = ""              # Set via SQE_STORAGE__S3_SECRET_KEY
s3_path_style = true            # true for MinIO/Ceph, false for AWS S3
s3_allow_http = false           # Allow plaintext HTTP for S3 (dev/test only)
concurrent_requests_per_file = 4 # Max concurrent byte-range requests per file
max_concurrent_files = 8        # Max files fetched concurrently
prefetch_buffer = "32MB"        # Prefetch buffer for overlapping footer reads
# coalesce_threshold and footer_cache_size are documented in
# architecture/streaming-execution.md alongside the S3 I/O pipeline.

# Access control and policy are two independent axes. See
# [GRANT and REVOKE](../sql-reference/grant-revoke.md) for the full model.

[access_control]
# Where GRANT/REVOKE are stored and resolved.
backend = "none"                # none (default) | polaris | ranger | chameleon

[policy]
# Fine-grained enforcement engine (row filters + column masks).
# Wired: passthrough (default), in-memory, ranger.
# opa and cedar are defined but not yet wired; selecting them errors at startup.
engine = "passthrough"

[session]
idle_timeout_secs = 900         # 15 min — sessions idle longer are expired
absolute_timeout_secs = 28800   # 8 hours — hard session lifetime cap
persistence = "memory"          # "memory" (default) or "file"
persistence_path = "/tmp/sqe-sessions.json"  # Path for file-based persistence
snapshot_interval_secs = 60     # How often file persistence snapshots sessions to disk

[query]
timeout_secs = 300              # 5 min — max execution time per query
max_result_rows = 1000000       # Max rows per query (0 = unlimited)
max_concurrent_queries = 100    # Concurrency limit (0 = unlimited)
max_query_memory = "256MB"      # Per-query memory limit
slow_query_threshold_secs = 30  # WARN-log threshold for slow queries
distribution_threshold = "128MB" # Min scan size to distribute to workers
distribution_file_threshold = 4 # Min file count to distribute
target_task_size = "256MB"      # Target scan task size for bin-packing
sort_mode = "adaptive"          # "adaptive", "partition_only", or "strict"

[query.role_overrides]          # Per-role timeout overrides (seconds)
# admin = 3600                  # Admins get 1 hour
# analyst = 600                 # Analysts get 10 minutes

[query_cache]
enabled = false                 # Enable query result caching
max_memory_mb = 128             # Total cache memory budget
max_entry_mb = 5                # Max size per cached result
ttl_secs = 300                  # Cache entry TTL

[query_history]
max_entries = 10000             # Max queries retained in history
ttl_secs = 1800                 # History entry TTL (30 min)

[rate_limit]
enabled = false                 # Enable per-user and global rate limiting
per_user_queries_per_minute = 60
global_queries_per_minute = 1000

[metrics]
prometheus_port = 9090          # Prometheus /metrics endpoint
otlp_endpoint = ""              # OTLP gRPC endpoint (empty = disabled)
audit_log_path = ""             # Audit JSONL file (empty = disabled)
```

## Environment Variable Overrides

Every config field can be overridden via environment variable. Convention: `SQE_<SECTION>__<FIELD>` (double underscore separating section from field).

| Env Var | Config Field | Type |
|---|---|---|
| **Coordinator** | | |
| `SQE_COORDINATOR__FLIGHT_SQL_PORT` | `coordinator.flight_sql_port` | u16 |
| `SQE_COORDINATOR__TRINO_HTTP_PORT` | `coordinator.trino_http_port` | u16 |
| `SQE_COORDINATOR__MODE` | `coordinator.mode` | string |
| `SQE_COORDINATOR__DEBUG` | `coordinator.debug` | bool |
| **TLS** | | |
| `SQE_TLS__CERT_FILE` | `coordinator.tls.cert_file` | string |
| `SQE_TLS__KEY_FILE` | `coordinator.tls.key_file` | string |
| `SQE_TLS__CA_FILE` | `coordinator.tls.ca_file` | string |
| **Worker** | | |
| `SQE_WORKER__COORDINATOR_URL` | `worker.coordinator_url` | string |
| `SQE_WORKER__FLIGHT_PORT` | `worker.flight_port` | u16 |
| `SQE_WORKER__ADVERTISE_URL` | `worker.advertise_url` | string |
| `SQE_WORKER__HEARTBEAT_INTERVAL_SECS` | `worker.heartbeat_interval_secs` | u64 |
| `SQE_WORKER__MEMORY_LIMIT` | `worker.memory_limit` | string |
| `SQE_WORKER__SPILL_TO_DISK` | `worker.spill_to_disk` | bool |
| `SQE_WORKER__SPILL_DIR` | `worker.spill_dir` | string |
| **Auth** | | |
| `SQE_AUTH__KEYCLOAK_URL` | `auth.keycloak_url` | string |
| `SQE_AUTH__REALM` | `auth.realm` | string |
| `SQE_AUTH__TOKEN_ENDPOINT` | `auth.token_endpoint` | string |
| `SQE_AUTH__CLIENT_ID` | `auth.client_id` | string |
| `SQE_AUTH__CLIENT_SECRET` | `auth.client_secret` | string |
| `SQE_AUTH__TOKEN_REFRESH_BUFFER_SECS` | `auth.token_refresh_buffer_secs` | u64 |
| `SQE_AUTH__SSL_VERIFICATION` | `auth.ssl_verification` | bool |
| **Catalog** | | |
| `SQE_CATALOG__CATALOG_URL` | `catalog.catalog_url` | string |
| `SQE_CATALOG__POLARIS_URL` | `catalog.catalog_url` (legacy alias) | string |
| `SQE_CATALOG__WAREHOUSE` | `catalog.warehouse` | string |
| `SQE_CATALOG__METADATA_CACHE_TTL_SECS` | `catalog.metadata_cache_ttl_secs` | u64 |
| `SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION` | `catalog.default_table_format_version` | u8 |
| **Storage** | | |
| `SQE_STORAGE__S3_ENDPOINT` | `storage.s3_endpoint` | string |
| `SQE_STORAGE__S3_REGION` | `storage.s3_region` | string |
| `SQE_STORAGE__S3_ACCESS_KEY` | `storage.s3_access_key` | string |
| `SQE_STORAGE__S3_SECRET_KEY` | `storage.s3_secret_key` | string |
| `SQE_STORAGE__S3_PATH_STYLE` | `storage.s3_path_style` | bool |
| **Policy** | | |
| `SQE_POLICY__ENGINE` | `policy.engine` | string |
| **Session** | | |
| `SQE_SESSION__IDLE_TIMEOUT_SECS` | `session.idle_timeout_secs` | u64 |
| `SQE_SESSION__ABSOLUTE_TIMEOUT_SECS` | `session.absolute_timeout_secs` | u64 |
| **Query** | | |
| `SQE_QUERY__TIMEOUT_SECS` | `query.timeout_secs` | u64 |
| **Rate Limit** | | |
| `SQE_RATE_LIMIT__ENABLED` | `rate_limit.enabled` | bool |
| `SQE_RATE_LIMIT__PER_USER_QUERIES_PER_MINUTE` | `rate_limit.per_user_queries_per_minute` | u32 |
| `SQE_RATE_LIMIT__GLOBAL_QUERIES_PER_MINUTE` | `rate_limit.global_queries_per_minute` | u32 |
| **Metrics** | | |
| `SQE_METRICS__PROMETHEUS_PORT` | `metrics.prometheus_port` | u16 |
| `SQE_METRICS__OTLP_ENDPOINT` | `metrics.otlp_endpoint` | string |
| `SQE_METRICS__AUDIT_LOG_PATH` | `metrics.audit_log_path` | string |

Boolean values accept: `true`/`false`, `1`/`0`, `yes`/`no`.

## TLS

SQE supports optional TLS encryption for the Flight SQL gRPC listener.

**Server-side TLS:** Set `cert_file` and `key_file` to enable. When both are set, the server listens on TLS; when omitted, plaintext.

**mTLS (mutual TLS):** Set `ca_file` to a PEM CA bundle. Clients must present a certificate signed by this CA.

```toml
[coordinator.tls]
cert_file = "/etc/sqe/server.crt"
key_file  = "/etc/sqe/server.key"
ca_file   = "/etc/sqe/ca.crt"    # Optional: enables mTLS
```

Validation rules:
- If either `cert_file` or `key_file` is set, both must be set
- All referenced files must exist when TLS is enabled
- `ca_file` is optional -- when set, it must also exist

## Authentication Modes

SQE supports two OAuth2 flows, selected by which config fields are populated:

### OIDC Password Grant (Keycloak)

For environments with Keycloak (or any OIDC provider supporting ROPC). The coordinator exchanges the user's username/password for tokens:

```toml
[auth]
keycloak_url = "https://keycloak.example.com"
realm = "iceberg"
client_id = "sqe-client"
```

### OAuth2 Client Credentials

For service-to-service auth or providers without ROPC support. The coordinator obtains tokens using a client ID and secret. Set `token_endpoint` directly:

```toml
[auth]
token_endpoint = "http://polaris:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"
```

At least one of `keycloak_url` or `token_endpoint` must be configured. If both are set, `keycloak_url` takes priority (OIDC mode).

### Provider chain

The two flows above are the single-provider shorthand. For anything beyond one OIDC provider, configure a chain of `[[auth.providers]]` entries. SQE tries each in order and the first that authenticates a request wins. The chain takes precedence over the legacy `[auth]` fields when it is non-empty, and the legacy fields stay backward-compatible for existing single-provider configs.

Each entry requires a `type`:

| Type | Required fields | Description |
|------|-----------------|-------------|
| `oidc_password` | `token_url`, `client_id` | OIDC Resource Owner Password Credentials |
| `client_credentials` | `token_endpoint`, `client_id`, `client_secret` | OAuth2 client credentials |
| `oidc_m2m` | `token_endpoint`, `client_id`, `client_secret` | OIDC machine-to-machine client-credentials (Unity Catalog and generic IdPs) |
| `bearer_token` | `jwks_url` | Pre-obtained JWT validated via JWKS |
| `token_exchange` | `token_url`, `client_id` | RFC 8693 token exchange |
| `aws_iam` | none | AWS IAM via STS `GetCallerIdentity` |
| `api_key` | `keys_file` | API key from a TOML keys file |
| `mtls` | none | Client certificate authentication |
| `anonymous` | none | Fixed identity for dev / test |

A common production chain accepts both interactive logins (password grant) and pre-minted JWTs from programmatic clients:

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "your-client-secret"   # via SQE_AUTH__CLIENT_SECRET
roles_claim = "realm_access.roles"

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/certs"
issuer = "https://keycloak.example.com/realms/iceberg"
```

Auth0 and Okta use the same two-provider shape, differing only in `token_url`, `jwks_url`, `issuer`, and the `roles_claim` path (Auth0 uses a namespaced claim, Okta uses `groups`).

AWS IAM maps caller ARNs to SQE roles:

```toml
[[auth.providers]]
type = "aws_iam"
region = "eu-west-1"
validate_with_sts = true

[auth.role_mappings]
"arn:aws:iam::123456789012:role/DataAnalyst" = ["analyst", "reader"]
"arn:aws:iam::123456789012:role/DataEngineer" = ["admin"]
```

API keys read from a separate TOML file, each key carrying a user and roles:

```toml
[[auth.providers]]
type = "api_key"
keys_file = "/etc/sqe/api-keys.toml"
key_prefix = "sqe_"
```

```toml
# api-keys.toml
[[keys]]
key = "sqe_abc123def456"
user = "service-account-etl"
roles = ["writer"]
```

Unity Catalog REST accepts OAuth2 client-credentials (machine-to-machine) in addition to personal access tokens. The `oidc_m2m` provider caches the access token and refreshes it shortly before expiry, so catalog requests never see a stale token:

```toml
[catalog]
catalog_url = "https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog"
warehouse = "main"

[[auth.providers]]
type = "oidc_m2m"
token_endpoint = "https://<workspace>.cloud.databricks.com/oidc/v1/token"
client_id = "<service-principal-application-id>"
client_secret = "<service-principal-secret>"
scope = "all-apis"
```

The `anonymous` provider pins a fixed identity for dev and test. SQE logs a startup warning whenever it is configured.

```toml
[[auth.providers]]
type = "anonymous"
user = "dev-user"
roles = ["admin"]
```

For CLI logins without a username and password, configure the interactive device-code flow under `[auth.external]`:

```toml
[auth.external]
issuer = "https://keycloak.example.com/realms/iceberg"
client_id = "sqe-cli"
scopes = ["openid", "profile"]

[auth.external.device]
client_id = "sqe-cli-device"
scopes = ["openid", "profile"]
```

## Validation

SQE validates config at startup and fails fast on errors:

- `auth.client_id` must not be empty
- `catalog.catalog_url` must not be empty
- At least one of `auth.keycloak_url` or `auth.token_endpoint` must be set
- `coordinator.flight_sql_port` must differ from `coordinator.trino_http_port`
- `coordinator.flight_sql_port` must differ from `metrics.prometheus_port`
- TLS: if either cert or key is set, both must be set; referenced files must exist

## Priority Order

```
CLI flags (--mode, --config) > Environment variables > Config file > Defaults
```

## Sensitive Values

Never put secrets in the TOML config file. Use environment variables or Kubernetes Secrets:

```bash
# Environment
export SQE_AUTH__CLIENT_SECRET="my-secret"
export SQE_STORAGE__S3_ACCESS_KEY="minioadmin"
export SQE_STORAGE__S3_SECRET_KEY="minioadmin"

# Kubernetes Secret (via Helm)
helm install sqe deploy/helm/sqe/ \
  --set secrets.SQE_AUTH__CLIENT_SECRET=xxx \
  --set secrets.SQE_STORAGE__S3_SECRET_KEY=xxx
```

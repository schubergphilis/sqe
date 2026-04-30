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
debug = false                   # When true, error messages include internal details (dev only)

[coordinator.tls]
cert_file = ""                  # PEM certificate — TLS enabled when both cert + key are set
key_file = ""                   # PEM private key
ca_file = ""                    # Optional PEM CA for mTLS client certificate verification

[worker]
coordinator_url = "http://coordinator:50051"
flight_port = 50052             # Worker Flight port
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
polaris_url = "http://polaris:8181/api/catalog"   # REST catalog endpoint
warehouse = "iceberg"           # warehouse identifier the catalog expects
metadata_cache_ttl_secs = 30    # Table metadata cache TTL
default_table_format_version = 2 # Iceberg table format version (2 or 3)
# `polaris_url` accepts any Iceberg REST endpoint. SQE has been
# verified live against Apache Polaris, Project Nessie 0.107+,
# Unity Catalog OSS, AWS Glue Iceberg REST, and AWS S3 Tables (via
# the federated Glue endpoint). For AWS endpoints the vendored
# REST client signs requests with SigV4 when the server advertises
# `rest.sigv4-enabled=true` in its /v1/config defaults.
#
# Hive Metastore, native AWS Glue (SDK path), and JDBC catalogs
# have working backend libraries in `crates/sqe-catalog/src/backends/`
# (live integration tests under
# `crates/sqe-catalog/tests/backends_integration.rs`) but the engine
# session manager still routes SQL through the REST path. End-to-end
# SQL dispatch through HMS/Glue/JDBC is tracked as a follow-up.

[storage]
s3_endpoint = "http://s3:9000"
s3_region = "us-east-1"
s3_access_key = ""              # Set via SQE_STORAGE__S3_ACCESS_KEY
s3_secret_key = ""              # Set via SQE_STORAGE__S3_SECRET_KEY
s3_path_style = true            # true for MinIO/Ceph, false for AWS S3

[policy]
engine = "passthrough"          # "passthrough" (only option currently; "opa", "cedar" planned)

[session]
idle_timeout_secs = 900         # 15 min — sessions idle longer are expired
absolute_timeout_secs = 28800   # 8 hours — hard session lifetime cap

[query]
timeout_secs = 300              # 5 min — max execution time per query

[query.role_overrides]          # Per-role timeout overrides (seconds)
# admin = 3600                  # Admins get 1 hour
# analyst = 600                 # Analysts get 10 minutes

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
| `SQE_CATALOG__POLARIS_URL` | `catalog.polaris_url` | string |
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

## Validation

SQE validates config at startup and fails fast on errors:

- `auth.client_id` must not be empty
- `catalog.polaris_url` must not be empty
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

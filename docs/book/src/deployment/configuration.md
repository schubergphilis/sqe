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

[worker]
coordinator_url = "http://coordinator:50051"
flight_port = 50052             # Worker Flight port
heartbeat_interval_secs = 5     # Health check interval
memory_limit = "8GB"            # Worker memory limit
spill_dir = "/tmp/sqe-spill"    # Temp directory for spilling

[auth]
keycloak_url = "https://keycloak.example.com"
realm = "iceberg"
client_id = "sqe-client"
client_secret = ""              # Set via SQE_AUTH__CLIENT_SECRET env var
token_refresh_buffer_secs = 60  # Refresh tokens this many seconds before expiry
ssl_verification = true         # Set false for dev (self-signed certs)

[catalog]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "iceberg"
metadata_cache_ttl_secs = 30    # Table metadata cache TTL

[storage]
s3_endpoint = "http://s3:9000"
s3_region = "us-east-1"
s3_access_key = ""              # Set via SQE_STORAGE__S3_ACCESS_KEY
s3_secret_key = ""              # Set via SQE_STORAGE__S3_SECRET_KEY
s3_path_style = true            # true for MinIO, false for AWS S3

[policy]
engine = "passthrough"          # "passthrough", "opa", "cedar" (Phase 5)

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
| **Worker** | | |
| `SQE_WORKER__COORDINATOR_URL` | `worker.coordinator_url` | string |
| `SQE_WORKER__FLIGHT_PORT` | `worker.flight_port` | u16 |
| `SQE_WORKER__HEARTBEAT_INTERVAL_SECS` | `worker.heartbeat_interval_secs` | u64 |
| `SQE_WORKER__MEMORY_LIMIT` | `worker.memory_limit` | string |
| `SQE_WORKER__SPILL_DIR` | `worker.spill_dir` | string |
| **Auth** | | |
| `SQE_AUTH__KEYCLOAK_URL` | `auth.keycloak_url` | string |
| `SQE_AUTH__REALM` | `auth.realm` | string |
| `SQE_AUTH__CLIENT_ID` | `auth.client_id` | string |
| `SQE_AUTH__CLIENT_SECRET` | `auth.client_secret` | string |
| `SQE_AUTH__TOKEN_REFRESH_BUFFER_SECS` | `auth.token_refresh_buffer_secs` | u64 |
| `SQE_AUTH__SSL_VERIFICATION` | `auth.ssl_verification` | bool |
| **Catalog** | | |
| `SQE_CATALOG__POLARIS_URL` | `catalog.polaris_url` | string |
| `SQE_CATALOG__WAREHOUSE` | `catalog.warehouse` | string |
| `SQE_CATALOG__METADATA_CACHE_TTL_SECS` | `catalog.metadata_cache_ttl_secs` | u64 |
| **Storage** | | |
| `SQE_STORAGE__S3_ENDPOINT` | `storage.s3_endpoint` | string |
| `SQE_STORAGE__S3_REGION` | `storage.s3_region` | string |
| `SQE_STORAGE__S3_ACCESS_KEY` | `storage.s3_access_key` | string |
| `SQE_STORAGE__S3_SECRET_KEY` | `storage.s3_secret_key` | string |
| `SQE_STORAGE__S3_PATH_STYLE` | `storage.s3_path_style` | bool |
| **Policy** | | |
| `SQE_POLICY__ENGINE` | `policy.engine` | string |
| **Metrics** | | |
| `SQE_METRICS__PROMETHEUS_PORT` | `metrics.prometheus_port` | u16 |
| `SQE_METRICS__OTLP_ENDPOINT` | `metrics.otlp_endpoint` | string |
| `SQE_METRICS__AUDIT_LOG_PATH` | `metrics.audit_log_path` | string |

Boolean values accept: `true`/`false`, `1`/`0`, `yes`/`no`.

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

# SQE Deployment Guide

This guide covers how to deploy the Sovereign Query Engine in development, test, and production environments.

## 1. Quick Start with Docker Compose

The fastest way to run SQE is the test stack: two containers (Apache Polaris + RustFS for S3-compatible storage) plus SQE running natively or in Docker.

### Prerequisites

- Docker and Docker Compose v2
- Rust toolchain (if running SQE natively)

### Start the infrastructure

```bash
# Start Polaris (catalog) + RustFS (S3-compatible storage)
docker compose -f docker-compose.test.yml up -d

# Bootstrap: create the test warehouse and S3 bucket
./scripts/bootstrap-test.sh
```

Ports (offset by +10000 from production defaults):

| Service | Port | Purpose |
|---------|------|---------|
| Polaris | 18181 | Iceberg REST Catalog API |
| RustFS  | 19000 | S3-compatible storage |

### Run SQE natively

```bash
SQE_CONFIG=tests/sqe-test.toml cargo run --release --bin sqe-server
```

SQE is now available at:
- Flight SQL: `localhost:60051`
- Trino HTTP: `localhost:18080`
- Prometheus metrics: `localhost:19090`

### Run SQE in Docker (distributed mode)

```bash
# Build and start coordinator + 2 workers alongside Polaris + RustFS
docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d

# Bootstrap the catalog
./scripts/bootstrap-test.sh
```

This starts:

| Service | Port | Purpose |
|---------|------|---------|
| Coordinator | 60051 | Flight SQL |
| Coordinator | 28080 | Trino HTTP |
| Coordinator | 29090 | Prometheus metrics |
| Worker 1 | 60061 | Flight (internal) |
| Worker 2 | 60062 | Flight (internal) |

### Connect with the CLI

```bash
cargo run --bin sqe-cli -- \
  --host localhost --port 60051 \
  --username root --password "" \
  --protocol flight
```

### Run a smoke test

```bash
# Load TPC-H SF0.01 and run all 22 queries
./scripts/benchmark-test.sh tpch
```

---

## 2. Configuration Reference

SQE uses a single TOML file specified via `SQE_CONFIG` environment variable or `--config` flag. All settings have defaults; only `catalog.catalog_url` (legacy alias `polaris_url`) and an auth source are strictly required.

Every TOML key can be overridden via environment variable using the pattern `SQE_<SECTION>__<KEY>` (double underscore). For example: `SQE_AUTH__CLIENT_ID=my-client`.

### `[coordinator]`

| Key | Default | Description |
|-----|---------|-------------|
| `flight_sql_port` | `50051` | gRPC port for Arrow Flight SQL |
| `trino_http_port` | `8080` | HTTP port for Trino wire protocol. Set to `0` to disable |
| `mode` | `"hybrid"` | Execution mode: `"hybrid"` (single-node + distributed fallback) |
| `worker_urls` | `[]` | List of worker Flight URLs for distributed mode |
| `worker_secret` | `""` | Shared secret for worker heartbeat auth. Empty disables the check |
| `debug` | `false` | Include full error chain in client responses (dev only) |
| `memory_limit` | `"8GB"` | DataFusion runtime memory limit |
| `spill_to_disk` | `true` | Enable spill-to-disk when memory limit is reached |
| `spill_dir` | `"/tmp/sqe-coordinator-spill"` | Directory for spill files (SSD recommended) |
| `spill_compression` | `"lz4"` | Compression for spill files: `"none"`, `"lz4"`, `"zstd"` |
| `flight_compression` | `"lz4"` | IPC compression for client DoGet responses |
| `shuffle_compression` | `"zstd"` | IPC compression for internal DoExchange shuffle |

### `[coordinator.tls]`

| Key | Default | Description |
|-----|---------|-------------|
| `cert_file` | `""` | Path to PEM server certificate. Both `cert_file` and `key_file` must be set to enable TLS |
| `key_file` | `""` | Path to PEM private key |
| `ca_file` | `""` | Path to PEM CA certificate for client verification (mTLS) |

### `[worker]`

| Key | Default | Description |
|-----|---------|-------------|
| `coordinator_url` | `""` | Coordinator Flight URL for heartbeat registration |
| `flight_port` | `50052` | Worker Flight server port |
| `advertise_url` | `""` | URL the coordinator uses to reach this worker. Empty -> auto-derived (`POD_IP` env, else `HOSTNAME` if an IP literal, else first non-loopback interface). Never advertise `0.0.0.0`: the coordinator rejects it |
| `heartbeat_interval_secs` | `5` | Heartbeat interval in seconds |
| `memory_limit` | `"8GB"` | Per-worker memory limit |
| `spill_to_disk` | `true` | Enable spill-to-disk |
| `spill_dir` | `"/tmp/sqe-spill"` | Spill directory |
| `scan_timeout_secs` | `600` | Maximum duration per scan task (10 min). `0` to disable |

### `[auth]`

Legacy fields (backward-compatible with single-provider configs):

| Key | Default | Description |
|-----|---------|-------------|
| `keycloak_url` | `""` | Keycloak base URL (deprecated -- use `providers`) |
| `realm` | `""` | Keycloak realm (deprecated -- use `providers`) |
| `client_id` | `""` | OAuth2 client_id |
| `client_secret` | `""` | OAuth2 client_secret |
| `token_endpoint` | `""` | Generic OAuth2 token endpoint (for client_credentials) |
| `token_refresh_buffer_secs` | `60` | Refresh token N seconds before expiry |
| `tls_skip_verify` | `false` | Skip TLS certificate verification for auth endpoints |

Pluggable providers (takes precedence over legacy fields when non-empty):

| Key | Default | Description |
|-----|---------|-------------|
| `providers` | `[]` | Array of `[[auth.providers]]` entries |
| `role_mappings` | `{}` | Group/ARN to roles mapping |
| `external` | none | Interactive OIDC config (device code, auth code + PKCE) |

### `[[auth.providers]]`

Each entry requires a `type` field. Supported types:

| Type | Required Fields | Description |
|------|-----------------|-------------|
| `oidc_password` | `token_url`, `client_id` | OIDC Resource Owner Password Credentials |
| `client_credentials` | `token_endpoint`, `client_id`, `client_secret` | OAuth2 client credentials |
| `bearer_token` | `jwks_url` | Pre-obtained JWT validated via JWKS |
| `token_exchange` | `token_url`, `client_id` | RFC 8693 token exchange |
| `aws_iam` | (none required) | AWS IAM via STS GetCallerIdentity |
| `api_key` | `keys_file` | API key from TOML keys file |
| `mtls` | (none required) | Client certificate authentication |
| `anonymous` | (none required) | Fixed identity for dev/test |

### `[catalog]`

| Key | Default | Description |
|-----|---------|-------------|
| `catalog_url` (alias `polaris_url`) | (required) | Iceberg REST catalog endpoint URL |
| `warehouse` | `""` | Default warehouse name |
| `metadata_cache_ttl_secs` | `30` | Table metadata cache TTL |
| `default_table_format_version` | `2` | Iceberg table format version for new tables |
| `trust_sort_order` | `false` | Trust Iceberg sort order for all columns (not just partition keys) |
| `small_file_threshold_mb` | `3` | Max file size for direct-read fast path. `0` to disable |
| `parquet_compression` | `"zstd"` | Write-path Parquet compression: `"zstd"`, `"lz4"`, `"snappy"`, `"none"` |

### `[storage]`

| Key | Default | Description |
|-----|---------|-------------|
| `s3_endpoint` | `""` | S3 endpoint URL |
| `s3_region` | `""` | AWS region |
| `s3_access_key` | `""` | S3 access key (use env override for secrets) |
| `s3_secret_key` | `""` | S3 secret key (use env override for secrets) |
| `s3_path_style` | `false` | Enable path-style addressing (required for MinIO, RustFS) |
| `s3_allow_http` | `false` | Allow plaintext HTTP for S3 (dev/test only) |
| `coalesce_threshold` | `"1MB"` | Byte-range coalescing for S3 GETs |
| `footer_cache_size` | `"256MB"` | Parquet footer cache size |
| `concurrent_requests_per_file` | `4` | Max concurrent byte-range requests per file |
| `max_concurrent_files` | `8` | Max files fetched concurrently |
| `prefetch_buffer` | `"32MB"` | Prefetch buffer for overlapping footer reads |

### `[query]`

| Key | Default | Description |
|-----|---------|-------------|
| `timeout_secs` | `300` | Max query execution time (5 min) |
| `role_overrides` | `{}` | Per-role timeout overrides (highest wins) |
| `max_result_rows` | `1000000` | Max rows per query. `0` for unlimited |
| `max_concurrent_queries` | `100` | Concurrency limit. `0` for unlimited |
| `slow_query_threshold_secs` | `30` | WARN log threshold for slow queries |
| `max_query_memory` | `"256MB"` | Per-query memory limit |
| `distribution_threshold` | `"128MB"` | Min scan size to distribute to workers |
| `distribution_file_threshold` | `4` | Min file count to distribute |
| `target_task_size` | `"256MB"` | Target scan task size for bin-packing |
| `sort_mode` | `"adaptive"` | Sort behavior: `"adaptive"`, `"partition_only"`, `"strict"` |

### `[query_cache]`

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Enable query result caching |
| `max_memory_mb` | `128` | Total cache memory budget |
| `max_entry_mb` | `5` | Max size per cached result |
| `ttl_secs` | `300` | Cache entry TTL |

### `[query_history]`

| Key | Default | Description |
|-----|---------|-------------|
| `max_entries` | `10000` | Max queries retained in history |
| `ttl_secs` | `1800` | History entry TTL (30 min) |

### `[policy]`

| Key | Default | Description |
|-----|---------|-------------|
| `engine` | `"passthrough"` | Policy engine: `"passthrough"`, `"opa"`, `"cedar"` |

### `[metrics]`

| Key | Default | Description |
|-----|---------|-------------|
| `prometheus_port` | `9090` | Prometheus scrape endpoint port |
| `otlp_endpoint` | `""` | OpenTelemetry OTLP/gRPC endpoint. Empty disables OTel |
| `audit_log_path` | `""` | Path for JSON audit log file. Empty disables audit log |
| `trace_sample_rate` | `0.01` | OTel trace sampling rate (0.0-1.0). `1.0` traces everything |

### `[rate_limit]`

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Enable rate limiting |
| `per_user_queries_per_minute` | `60` | Per-user query rate limit |
| `global_queries_per_minute` | `1000` | Global query rate limit |

### `[session]`

| Key | Default | Description |
|-----|---------|-------------|
| `idle_timeout_secs` | `900` | Idle session timeout (15 min) |
| `absolute_timeout_secs` | `28800` | Absolute session timeout (8 hours) |
| `persistence` | `"memory"` | Session persistence: `"memory"`, `"file"` |
| `persistence_path` | `"/tmp/sqe-sessions.json"` | Path for file-based persistence |
| `snapshot_interval_secs` | `60` | How often to snapshot sessions to disk |

### Complete example

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
memory_limit = "16GB"
spill_to_disk = true
spill_dir = "/data/sqe-spill"
worker_urls = ["http://worker-1:50052", "http://worker-2:50052"]
worker_secret = "change-me-in-production"

[coordinator.tls]
cert_file = "/etc/sqe/tls/server.crt"
key_file = "/etc/sqe/tls/server.key"

[auth]
providers = []
tls_skip_verify = false

[[auth.providers]]
type = "oidc_password"
token_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "your-client-secret"

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/certs"

[catalog]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "production"
metadata_cache_ttl_secs = 30

[storage]
s3_endpoint = "https://s3.eu-west-1.amazonaws.com"
s3_region = "eu-west-1"

[query]
timeout_secs = 600
max_concurrent_queries = 200
sort_mode = "adaptive"

[metrics]
prometheus_port = 9090
otlp_endpoint = "http://otel-collector:4317"
audit_log_path = "/var/log/sqe/audit.json"
trace_sample_rate = 0.01

[rate_limit]
enabled = true
per_user_queries_per_minute = 120
global_queries_per_minute = 2000

[session]
idle_timeout_secs = 900
absolute_timeout_secs = 28800
```

---

## 3. Kubernetes Deployment

SQE ships with a Helm chart in `deploy/helm/sqe/`.

### Install with Helm

```bash
# Single-node mode (coordinator only)
helm install sqe deploy/helm/sqe/ \
  --set config.auth.keycloak_url=https://keycloak.example.com \
  --set config.auth.realm=iceberg \
  --set config.auth.client_id=sqe-client \
  --set config.catalog.catalog_url=http://polaris:8181/api/catalog \
  --set config.catalog.warehouse=production

# Distributed mode (coordinator + 3 workers)
# workerSecret is REQUIRED in distributed mode. Without it the coordinator and
# workers fail startup validation and crashloop (ISSUE-218). Prefer an existing
# Secret in production; the inline value below is for dev/test.
helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set worker.replicas=3 \
  --set workerSecret.value=change-me-shared-secret \
  --set config.catalog.catalog_url=http://polaris:8181/api/catalog
```

### Worker secret (distributed mode)

In distributed mode the coordinator and every worker share a secret that
authenticates worker registration and credential push. The engine refuses to
start when a `coordinator_url` / `worker_urls` is set with an empty
`worker_secret`. The chart enforces this: it renders `coordinator_url` only when
`worker.enabled=true`, so a single-node `helm install` needs no secret, while a
distributed install needs one or the pods crashloop.

Provide the secret one of two ways. Both pods mount the same ConfigMap, whose
rendered config carries both `worker_urls` and `coordinator_url`, so the engine
validates both guards on both pods. The chart injects the value under both
`SQE_COORDINATOR__WORKER_SECRET` and `SQE_WORKER__WORKER_SECRET` on the
coordinator and the workers, all from one Secret key, so every guard is
satisfied and the values match.

```bash
# Preferred: reference a Secret you manage, naming the key that holds the value.
kubectl create secret generic sqe-worker-secret \
  --from-literal=SQE_WORKER_SECRET="$(openssl rand -hex 32)"

helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set workerSecret.existingSecret=sqe-worker-secret \
  --set workerSecret.key=SQE_WORKER_SECRET

# Dev/test: inline value. The chart creates the Secret for you.
helm install sqe deploy/helm/sqe/ \
  --set worker.enabled=true \
  --set workerSecret.value=dev-only-shared-secret
```

### Helm values overview

```yaml
image:
  repository: sqe
  tag: latest
  pullPolicy: IfNotPresent

# Coordinator pod
coordinator:
  replicas: 1
  resources:
    requests:
      memory: "512Mi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "2"

# Worker pods (disabled by default -- single-node mode)
worker:
  enabled: false
  replicas: 2
  resources:
    requests:
      memory: "1Gi"
      cpu: "1"
    limits:
      memory: "8Gi"
      cpu: "4"

# Service exposure
service:
  type: ClusterIP
  flightSqlPort: 50051
  trinoHttpPort: 8080
  metricsPort: 9090

# SQE configuration (rendered to sqe.toml ConfigMap)
config:
  coordinator:
    flight_sql_port: 50051
    trino_http_port: 8080
    mode: "hybrid"
  worker:
    heartbeat_interval_secs: 5
    memory_limit: "8GB"
    spill_dir: "/tmp/sqe-spill"
  auth:
    keycloak_url: "https://keycloak.example.com"
    realm: "iceberg"
    client_id: "sqe-client"
    ssl_verification: true
  catalog:
    catalog_url: "http://polaris:8181/api/catalog"
    warehouse: "iceberg"
    metadata_cache_ttl_secs: 30
  storage:
    s3_endpoint: "http://s3:9000"
    s3_region: "us-east-1"
    s3_path_style: true
  policy:
    engine: "passthrough"
  metrics:
    prometheus_port: 9090
    otlp_endpoint: ""
    audit_log_path: ""

# Prometheus Operator ServiceMonitor
serviceMonitor:
  enabled: false
  interval: 30s
  labels: {}

# Secrets -- reference an existing K8s Secret or provide inline
existingSecret: ""
secrets: {}
  # SQE_AUTH__CLIENT_SECRET: "my-secret"
  # SQE_STORAGE__S3_ACCESS_KEY: "accesskey"
  # SQE_STORAGE__S3_SECRET_KEY: "secretkey"
```

### Managing secrets

Sensitive values (client secrets, S3 credentials) should not be in `values.yaml`. Use either:

**Option A: Reference an existing Kubernetes Secret**

```bash
# Create the secret
kubectl create secret generic sqe-secrets \
  --from-literal=SQE_AUTH__CLIENT_SECRET=my-oidc-secret \
  --from-literal=SQE_STORAGE__S3_ACCESS_KEY=AKIAEXAMPLE \
  --from-literal=SQE_STORAGE__S3_SECRET_KEY=wJalrXUtnFEMI/K7EXAMPLE

# Reference it in Helm
helm install sqe deploy/helm/sqe/ \
  --set existingSecret=sqe-secrets
```

**Option B: Inline secrets (a Kubernetes Secret is auto-created)**

```yaml
secrets:
  SQE_AUTH__CLIENT_SECRET: "my-oidc-secret"
  SQE_STORAGE__S3_ACCESS_KEY: "AKIAEXAMPLE"
  SQE_STORAGE__S3_SECRET_KEY: "wJalrXUtnFEMI/K7EXAMPLE"
```

### Pod security

The Helm chart enforces security best practices by default:

- `runAsNonRoot: true` (UID 1000)
- `readOnlyRootFilesystem: true`
- `allowPrivilegeEscalation: false`
- All Linux capabilities dropped

### Health probes

Both coordinator and worker pods expose:

- **Liveness**: `GET /healthz` on port 9091
- **Readiness**: `GET /readyz` on port 9091

### Disruption budgets and spreading

The chart ships PodDisruptionBudgets and default pod anti-affinity so a node
drain or rolling upgrade cannot take the cluster down at once (ISSUE-249).

- **Coordinator PDB**: `minAvailable: 1`. The coordinator is single-replica
  today, so this blocks an unforced eviction of the only coordinator. A node
  drain that targets it will not proceed until you act (cordon and delete the
  pod, or scale the deployment to 0 first). The budget protects a SPOF; it does
  not provide HA. See the SPOF note below.
- **Worker PDB**: `maxUnavailable: 1`. A drain rolls through one node at a time
  while the rest keep serving fragments. Rendered only when `worker.enabled`.
- **Anti-affinity**: when you leave a component's `affinity` empty, the chart
  applies a preferred `podAntiAffinity` by hostname so replicas spread across
  nodes. Workers always get it; the coordinator gets it only at `replicas > 1`.
  We chose preferred `podAntiAffinity` over `topologySpreadConstraints` because
  it slots into the existing per-component `affinity` passthrough: set
  `affinity` to override the default entirely, or set `defaultAntiAffinity:
  false` to render none.

Toggle the budgets with `podDisruptionBudget.enabled` (default `true`) and tune
`podDisruptionBudget.coordinator.minAvailable` /
`podDisruptionBudget.worker.maxUnavailable`.

### Coordinator is a single point of failure (ISSUE-221)

The coordinator runs as a single replica. Session state, the worker registry,
and in-flight query state are process-local. There is no shared store.

A coordinator restart drops every in-flight query and invalidates client
sessions: connected clients must re-authenticate and re-run. A node drain that
moves the coordinator pod is a brief outage, not a transparent failover.

Running more than one coordinator replica is **not yet safe**. Two replicas do
not share sessions or the registry, so clients would land on a coordinator that
has never seen their session. Keep `coordinator.replicas: 1`. The coordinator
PDB and the default chart values reflect this single-replica reality on purpose;
they do not imply HA. Full coordinator HA (shared session and registry state) is
a separate design tracked in ISSUE-221.

For the on-call response to a coordinator restart, OOM, or worker flap, see the
[Operational Runbook](runbook.md).

---

## 4. TLS Configuration

### Flight SQL (gRPC) TLS

Add a `[coordinator.tls]` section to your config:

```toml
[coordinator.tls]
cert_file = "/etc/sqe/tls/server.crt"
key_file = "/etc/sqe/tls/server.key"
```

Or via environment variables:

```bash
SQE_TLS__CERT_FILE=/etc/sqe/tls/server.crt
SQE_TLS__KEY_FILE=/etc/sqe/tls/server.key
```

SQE logs a warning at startup when TLS is disabled:

```
WARNING: TLS is DISABLED -- Flight SQL and worker connections are unencrypted.
Set [coordinator.tls] cert_file and key_file for production.
```

### Mutual TLS (mTLS)

To require client certificates, add the CA file:

```toml
[coordinator.tls]
cert_file = "/etc/sqe/tls/server.crt"
key_file = "/etc/sqe/tls/server.key"
ca_file = "/etc/sqe/tls/ca.crt"
```

When `ca_file` is set, SQE requires clients to present a valid certificate signed by that CA.

### TLS for auth endpoints

By default, SQE verifies TLS certificates when communicating with OIDC providers. To skip verification (dev/test only):

```toml
[auth]
tls_skip_verify = true
```

SQE logs a warning when TLS verification is disabled:

```
WARNING: TLS certificate verification is DISABLED for auth endpoints --
vulnerable to MITM. Set auth.tls_skip_verify = false for production.
```

### TLS with Kubernetes

Mount TLS certificates via a Kubernetes Secret:

```yaml
# In values.yaml
extraEnv:
  - name: SQE_TLS__CERT_FILE
    value: "/etc/sqe/tls/tls.crt"
  - name: SQE_TLS__KEY_FILE
    value: "/etc/sqe/tls/tls.key"

# Add a volume mount for the TLS secret in your deployment overlay
```

Or use a cert-manager Certificate resource and reference the generated Secret.

---

## 5. Authentication Setup

SQE supports a pluggable chain of authentication providers. The first provider that successfully authenticates a request wins. Configure one or more providers in `[[auth.providers]]`.

### Keycloak

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "your-keycloak-client-secret"
roles_claim = "realm_access.roles"

# Also accept pre-obtained JWTs (for programmatic clients)
[[auth.providers]]
type = "bearer_token"
jwks_url = "https://keycloak.example.com/realms/iceberg/protocol/openid-connect/certs"
issuer = "https://keycloak.example.com/realms/iceberg"
```

### Auth0

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "https://your-tenant.auth0.com/oauth/token"
client_id = "your-auth0-client-id"
client_secret = "your-auth0-client-secret"
roles_claim = "https://your-app.example.com/roles"

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://your-tenant.auth0.com/.well-known/jwks.json"
issuer = "https://your-tenant.auth0.com/"
audience = "https://sqe.example.com"
```

### Okta

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "https://your-org.okta.com/oauth2/default/v1/token"
client_id = "your-okta-client-id"
client_secret = "your-okta-client-secret"
roles_claim = "groups"

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://your-org.okta.com/oauth2/default/v1/keys"
issuer = "https://your-org.okta.com/oauth2/default"
```

### AWS IAM

```toml
[[auth.providers]]
type = "aws_iam"
region = "eu-west-1"
validate_with_sts = true

# Map IAM roles to SQE roles
[auth.role_mappings]
"arn:aws:iam::123456789012:role/DataAnalyst" = ["analyst", "reader"]
"arn:aws:iam::123456789012:role/DataEngineer" = ["admin"]
```

### API key

```toml
[[auth.providers]]
type = "api_key"
keys_file = "/etc/sqe/api-keys.toml"
key_prefix = "sqe_"
```

API keys file format (`api-keys.toml`):

```toml
[[keys]]
key = "sqe_abc123def456"
user = "service-account-etl"
roles = ["writer"]

[[keys]]
key = "sqe_xyz789ghi012"
user = "service-account-dashboard"
roles = ["reader"]
```

### Anonymous (development only)

```toml
[[auth.providers]]
type = "anonymous"
user = "dev-user"
roles = ["admin"]
```

SQE logs a startup warning when an anonymous provider is configured.

### Device code flow (interactive)

For CLI-based authentication without username/password:

```toml
[auth.external]
issuer = "https://keycloak.example.com/realms/iceberg"
client_id = "sqe-cli"
scopes = ["openid", "profile"]

[auth.external.device]
client_id = "sqe-cli-device"
scopes = ["openid", "profile"]
```

---

## 5b. Catalog Backends

SQE ships with the Iceberg REST catalog as the default backend. Phase A of the
iceberg-matrix-parity change adds feature-gated backends for Glue, HMS, JDBC
(SQLite/PostgreSQL), and a Hadoop storage-only catalog. Pick the backends you
need at build time via Cargo features:

```bash
# Default build: REST only (Polaris, Nessie, Snowflake Horizon).
cargo build --release

# Full build: every backend available at runtime.
cargo build --release --features sql,hadoop,hms,glue
```

Each feature maps to a `catalog.type` value in `sqe.toml`.

### REST: Polaris, Snowflake Horizon, Nessie

The REST backend speaks the Iceberg REST Catalog protocol. It works against
Apache Polaris (the default target), Snowflake Horizon (Polaris-based), and
Project Nessie via Nessie's REST adapter. Bearer tokens pass through from the
authenticated user session.

```toml
# Polaris (the built-in reference target).
[catalog]
type = "rest"
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "polaris"

# Nessie via the REST adapter on the same host.
[catalog]
type = "rest"
catalog_url = "http://nessie:19120/iceberg"
warehouse = "main"
```

Nessie exposes branches as reference names. SQE's REST client sends the bearer
token verbatim; Nessie validates it against its configured OIDC provider.
Read-only operations (SELECT, SHOW TABLES, DESCRIBE) work out of the box.
Write operations depend on Nessie's Iceberg REST parity for CTAS and INSERT,
which tracks apache/polaris-rest behaviour.

### JDBC: SQLite (local dev) and PostgreSQL

The JDBC backend stores metadata in a relational database. SQLite is handy for
local development and tests; PostgreSQL is the production target.

```toml
# SQLite file (created on first use).
[catalog]
type = "jdbc"
url = "sqlite:///tmp/sqe-catalog.db"
catalog_name = "sqe"

# PostgreSQL.
[catalog]
type = "jdbc"
url = "postgresql://catalog.example.com/iceberg"
catalog_name = "sqe"
```

The SQLite path is fully implemented today. PostgreSQL support arrives when
SQE adopts the upstream `iceberg-catalog-sql` crate from apache/iceberg-rust.

### Hadoop: storage-only warehouse

Hadoop catalogs have no catalog service. SQE lists tables by walking the
warehouse path and reading the highest `metadata/v<N>.metadata.json` per
directory. S3-compatible stores and local filesystems both work; object-store
credentials come from the `[storage]` section.

```toml
[catalog]
type = "hadoop"
warehouse = "s3://lake/warehouse"
```

Write operations through the Hadoop layout are race-prone on object stores
without atomic rename. Production workloads should use REST, HMS, or JDBC.

### HMS (Hive Metastore)

The HMS backend speaks Thrift to a running metastore on port 9083.

```toml
[catalog]
type = "hms"
uri = "thrift://hms.example.com:9083"
warehouse = "s3://lake/warehouse"
```

HMS enforces table-level locking on write. SQE acquires the lock before the
commit RPC and releases it after success or failure. The current build carries
a marker implementation; the real Thrift wiring adopts
`iceberg-catalog-hms` from apache/iceberg-rust in a follow-up change.

### Glue (AWS)

```toml
[catalog]
type = "glue"
region = "eu-west-1"
warehouse = "s3://lake/warehouse"
```

The AWS SDK picks credentials from the standard chain (env vars, profile,
instance role). The current build carries a marker implementation; real
wiring via `iceberg-catalog-glue` is tracked in the matrix-parity change.

### Unity Catalog (OIDC machine-to-machine)

Unity Catalog REST supports OAuth2 client-credentials (M2M) in addition to
personal access tokens. SQE has an `OidcM2mProvider` in the auth chain.

```toml
[catalog]
type = "rest"
catalog_url = "https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog"
warehouse = "main"

[[auth.providers]]
type = "oidc_m2m"
token_endpoint = "https://<workspace>.cloud.databricks.com/oidc/v1/token"
client_id = "<sp_application_id>"
client_secret = "<sp_secret>"
scope = "all-apis"
```

The provider caches the access token and refreshes it 60 seconds before it
would expire, so catalog requests never see a stale token.

---

## 6. Monitoring

### Prometheus metrics

SQE exposes a Prometheus metrics endpoint on the configured port (default `:9090`).

```bash
curl http://localhost:9090/metrics
```

Key metric families:

| Metric | Type | Description |
|--------|------|-------------|
| `sqe_queries_total` | Counter | Total queries executed (by status) |
| `sqe_query_duration_seconds` | Histogram | Query execution time |
| `sqe_active_queries` | Gauge | Currently executing queries |
| `sqe_spill_bytes_total` | Counter | Bytes spilled to disk |
| `sqe_shuffle_bytes_total` | Counter | Bytes shuffled between workers |
| `sqe_s3_requests_total` | Counter | S3 requests (by operation) |
| `sqe_cache_hits_total` | Counter | Cache hits (by layer) |
| `sqe_cache_misses_total` | Counter | Cache misses (by layer) |
| `sqe_time_to_first_row_seconds` | Histogram | Time from query start to first result row |

### Observability stack (Docker Compose)

SQE ships with a lightweight observability overlay using VictoriaMetrics (Prometheus-compatible, ~30 MB RAM) and Grafana:

```bash
# Start with the test stack
docker compose -f docker-compose.test.yml -f docker-compose.observability.yml up -d

# Open Grafana
open http://localhost:13000    # admin / admin
```

This auto-scrapes metrics from:
- Single-node coordinator: `localhost:19090`
- Distributed coordinator: `localhost:29090`
- Workers: `localhost:29091-29094`

### Prometheus Operator (Kubernetes)

Enable the ServiceMonitor in Helm values:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  labels:
    release: prometheus    # Match your Prometheus Operator selector
```

This creates a `ServiceMonitor` CRD that Prometheus Operator auto-discovers.

### OpenTelemetry

SQE emits traces, metrics, and logs via OTLP/gRPC when an endpoint is configured:

```toml
[metrics]
otlp_endpoint = "http://otel-collector:4317"
trace_sample_rate = 0.01    # 1% sampling (adjust for production load)
```

SQE uses the following OTel libraries:
- `opentelemetry-otlp` with gRPC transport (tonic)
- `tracing-opentelemetry` for span bridging
- `opentelemetry-appender-tracing` for log bridging

Traces propagate across coordinator-to-worker boundaries.

**Example: Jaeger backend**

```bash
# Start Jaeger all-in-one
docker run -d --name jaeger \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# Configure SQE
export SQE_METRICS__OTLP_ENDPOINT=http://localhost:4317

# View traces
open http://localhost:16686
```

### Grafana dashboards

The repository includes a pre-built Grafana dashboard at `deploy/observability/sqe-benchmark-dashboard.json`. When using the observability Docker Compose overlay, this dashboard is auto-provisioned.

For manual import: copy the JSON file into your Grafana instance and configure a Prometheus/VictoriaMetrics data source.

### Audit logging

Enable structured JSON audit logging for compliance:

```toml
[metrics]
audit_log_path = "/var/log/sqe/audit.json"
```

Each query produces a JSON log entry with: timestamp, user, query text (truncated), duration, row count, and status.

---

## Docker Images

SQE builds a single multi-binary image containing three binaries:

| Binary | Purpose |
|--------|---------|
| `sqe-server` | Coordinator (default entrypoint). Also runs embedded workers in hybrid mode |
| `sqe-worker` | Standalone worker for distributed mode |
| `sqe-cli` | Interactive SQL client |

```bash
# Build the image
docker build -t sqe:latest .

# Run as coordinator
docker run -p 50051:50051 -v ./config.toml:/etc/sqe/sqe.toml sqe:latest --config /etc/sqe/sqe.toml

# Run as worker
docker run -p 50052:50052 -v ./worker.toml:/etc/sqe/sqe.toml --entrypoint sqe-worker sqe:latest /etc/sqe/sqe.toml
```

The image is based on `debian:bookworm-slim` (~80 MB), runs as non-root (UID 1000), and exposes ports 50051 (Flight SQL), 50052 (worker Flight), 8080 (Trino HTTP), 9090 (Prometheus), and 9091 (health).

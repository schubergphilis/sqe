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
session_context_cache_ttl_secs = 60  # Per-user SessionContext cache TTL. Also the
                                # passive backstop for catalog-set discovery: a
                                # catalog created/rebound out-of-band appears within
                                # this window. POST /api/v1/catalogs/refresh (health
                                # port, admin) is the instant path. Lower = fresher
                                # catalogs, more session rebuilds under concurrency.

[coordinator.tls]
cert_file = ""                  # PEM certificate (TLS enabled when both cert + key are set)
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
idle_timeout_secs = 900         # 15 min, sessions idle longer are expired
absolute_timeout_secs = 28800   # 8 hours, hard session lifetime cap
persistence = "memory"          # "memory" (default) or "file"
persistence_path = "/tmp/sqe-sessions.json"  # Path for file-based persistence
snapshot_interval_secs = 60     # How often file persistence snapshots sessions to disk
# Optional CREATE SECRET snapshot (plaintext JSON, mode 0600). Empty = memory
# only. ATTACH mounts stay process-local even when this is set.
# secrets_path = "/var/lib/sqe/secrets.json"

[query]
timeout_secs = 300              # 5 min, max execution time per query
max_result_rows = 1000000       # Max rows per query (0 = unlimited)
max_concurrent_queries = 100    # Concurrency limit (0 = unlimited)
max_query_memory = "256MB"      # Per-query memory limit
slow_query_threshold_secs = 30  # WARN-log threshold for slow queries
distribution_threshold = "128MB" # Min scan size to distribute to workers
distribution_file_threshold = 4 # Min file count to distribute
target_task_size = "256MB"      # Target scan task size for bin-packing
sort_mode = "adaptive"          # "adaptive", "partition_only", or "strict"

# Write-path memory safety (see Features -> Write Path)
write_buffer_tracking = true    # Pool-track write buffers; big writes fail with ResourceExhausted, not OOM
fanout_max_open_writers = 0     # Cap on open per-partition writers (0 = auto: pool-derived, 8..64); opt-in bounded fanout
fanout_buffer_budget = "0"      # Byte budget for buffered fanout memory, "512MB" style (0 = auto); opt-in bounded fanout
merge_target_streaming = false  # Stream CoW MERGE target from files instead of buffering (opt-in, needs write_buffer_tracking)

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
traces_otlp_endpoint = ""       # trace-only OTLP gRPC endpoint
trace_sample_rate = 0.01         # 0.0 to 1.0
audit_log_path = ""             # Audit JSONL file (empty = disabled)

# Advisory / active auto-compaction (Phase 4a+). Off by default: no
# maintenance principal is constructed and no scheduler task runs
# unless mode is set. See "Maintenance (auto-compaction)" below.
[maintenance]
mode = "off"                    # "off" (default) | "advisory" | "active"

# Required only when mode != "off" (validation rejects mode without this block).
# [maintenance.principal]
# token_endpoint = "https://idp.example.com/realms/sqe/protocol/openid-connect/token"
# client_id = "sqe-maintenance"
# client_secret = ""            # TOML-only in Phase 4a; no env var override yet
# scope = "PRINCIPAL_ROLE:sqe_maintenance"
# user_id = "svc-sqe-maintenance"   # audit display identity
# roles = ["maintenance"]
# refresh_skew_secs = 60

[maintenance.scheduler]
enabled = false                 # in-process tick loop off by default; drive via
                                 # an external Kubernetes CronJob instead, or flip
                                 # this on for an in-coordinator loop
tick_secs = 60                  # how often the loop wakes up when enabled
schedule = "0 2 * * *"          # global default cron; per-table property overrides
jitter_secs = 900               # per-table jitter so a fleet doesn't all fire at once
max_concurrent_jobs = 1
lease = "catalog"               # "none" | "catalog" | "kubernetes"
lease_ttl_secs = 300
state_table = "sqe_system.maintenance_log"  # operator-created; see note below
single_scheduler_acknowledged = false       # required true for enabled=true + lease="none"

[maintenance.compaction]
target_file_size_bytes = 536870912   # 512 MiB
min_input_files = 5
delete_file_threshold = 2
strategy = "binpack"             # "binpack" | "sort" | "zorder"

[maintenance.distribution]
mode = "auto"                    # "auto" (default) | "local" | "require"
min_workers = 2
max_inflight_groups_per_worker = 1
group_attempts = 2
group_timeout_secs = 3600
group_heartbeat_timeout_secs = 120
partial_progress = false
partial_progress_batch = 10
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
| `SQE_METRICS__TRACES_OTLP_ENDPOINT` | `metrics.traces_otlp_endpoint` | string |
| `SQE_METRICS__TRACE_SAMPLE_RATE` | `metrics.trace_sample_rate` | f64 |
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

## Maintenance (auto-compaction)

`[maintenance]` configures SQE's background compaction subsystem: a
non-human service principal, an in-coordinator scheduler loop, and the
sizing knobs `CALL system.rewrite_data_files` and `CALL
system.table_health` both use. Phase 4a shipped the advisory arm: it
reports compaction debt but never mutates a table. Phase 4b adds the
active arm: with `mode = "active"`, the scheduler commits real
`rewrite_data_files` rewrites against opted-in, due tables on a cron
schedule, through the same code path `CALL system.rewrite_data_files`
uses interactively.

Advisory is the recommended first step for any new deployment. Run it
long enough to see real compaction debt and validate the schedule and
per-table knobs before opting a table into active mode. Active mode
mutates data files and commits new snapshots; treat the switch from
advisory to active per table as a deliberate, reviewed change, not a
default.

### The mode ladder

`maintenance.mode` gates the whole subsystem and only moves up a ladder an
operator chooses explicitly:

- `"off"` (default). No maintenance principal is constructed, no scheduler
  task is spawned, `[maintenance.principal]` is not required.
- `"advisory"`. The scheduler loop (if `scheduler.enabled = true`) discovers
  opted-in tables and publishes health/metrics per table, the same report
  `CALL system.table_health` returns. Nothing is rewritten.
- `"active"`. The scheduler commits real `rewrite_data_files` rewrites, on
  the configured cron schedule, against tables that are both due and
  opted in via the per-table `sqe.maintenance.enabled` property. A due,
  opted-in table with no eligible compaction debt is skipped (a `skipped`
  `maintenance_log` row, not a rewrite); see "The three gates" and
  "`sqe_system.maintenance_log`" below.

Any mode beyond `"off"` requires a `[maintenance.principal]` block.
Validation rejects the config otherwise, so a typo'd `mode` value cannot
silently run with no credentials.

`mode = "off"` is total absence, not a runtime no-op: coordinator bootstrap
constructs neither the maintenance principal nor the scheduler task when
`mode` is `"off"`, so there is nothing in the process that could reach a
table, not merely a loop that declines to run. `CALL system.table_health`
is unaffected by `mode`: it is a plain read-only procedure available to any
session with `SELECT` on the table, regardless of the maintenance
subsystem's state.

### The maintenance principal

`[maintenance.principal]` is a dedicated OAuth2 client-credentials (M2M)
identity used solely by the maintenance scheduler. It is never added to
the interactive auth chain (`[[auth.providers]]`), so the query path
cannot authenticate as this principal even by accident. Fields:
`token_endpoint`, `client_id`, `client_secret`, `scope`, `user_id` (the
audit display identity for events this principal emits), `roles`, and
`refresh_skew_secs` (pre-emptive token refresh before expiry, default 60).

`client_secret` is a `SecretString`: it never round-trips through a
config-dump path and zeroizes on drop, same treatment as `auth.client_secret`.
Unlike `auth.client_secret`, there is no `SQE_MAINTENANCE__...` environment
variable override wired up in Phase 4a, so keep it out of a checked-in TOML
file and mount it in via a file-based secret instead.

Startup validation warns (not an error) if `maintenance.principal.client_id`
matches a configured auth-provider `client_id`: sharing an identity between
the interactive and maintenance paths makes audit trails ambiguous about
which one acted.

### The scheduler loop

`[maintenance.scheduler].enabled` defaults to `false`. A `false` value
suits external-trigger deployments: leave the in-process loop off and drive
timing from a Kubernetes `CronJob` (`concurrencyPolicy: Forbid`) that issues
the maintenance `CALL` on its own schedule. Set `enabled = true` to run the
tick loop inside the coordinator instead; it wakes every `tick_secs`
seconds and evaluates `schedule` for every opted-in table.

`schedule` is a standard 5-field cron expression (minute hour day-of-month
month day-of-week), e.g. the default `"0 2 * * *"` for daily at 02:00.
SQE evaluates it in UTC, never the host's local timezone. A per-table
`sqe.maintenance.compaction.schedule` property overrides the global
`schedule` for that table; see "Per-table overrides" below.

`jitter_secs` adds a deterministic, per-table delay on top of each cron
fire time, so a fleet of tables sharing one schedule does not all fire in
the same tick and thunder against Polaris/S3 simultaneously. The delay is
a hash of the table's identifier modulo `jitter_secs`, so it is stable
across ticks and restarts for a given table. `jitter_secs = 0` removes
that stagger delay only: the effective fire instant then equals the raw
cron fire time exactly, but the cron schedule is still parsed and
enforced as normal. A table is never treated as always-due just because
its jitter is zero.

### The lease ladder

`lease` is the double-fire guard for multi-coordinator deployments: which
backend (if any) the scheduler uses to keep two coordinators from both
compacting the same table in the same window. Three settings:

- `"none"`. No lease. Fine for a single-coordinator deployment, and only
  for one: validation rejects `scheduler.enabled = true` with
  `lease = "none"` unless `single_scheduler_acknowledged = true` is also
  set, an explicit operator opt-in rather than a silent default. Running
  the in-process scheduler unleased against more than one coordinator can
  make them both dispatch a rewrite for the same table at once; see
  "Multi-coordinator HA" below for why that wastes work rather than
  corrupting data.
- `"catalog"` (default). Before dispatching the one expensive step of a
  tick, the rewrite itself, the scheduler claims a lease row in
  `state_table` (`sqe_system.maintenance_log`). A coordinator that finds
  the lease already held by another holder skips its tick for that table.
  `lease_ttl_secs` (default 300) bounds how long a crashed holder's claim
  stays valid: past the TTL with no renewal, the next coordinator to check
  steals the expired lease instead of waiting forever. Claiming and
  releasing the lease each commit a row to `state_table`, so catalog mode
  costs a couple of extra state-table commits per due table per tick
  beyond `lease = "none"`. A single-coordinator deployment that does not
  need the guard can set `lease = "none"` (with
  `single_scheduler_acknowledged = true`) to skip that overhead.
- `"kubernetes"`. Not implemented in this release. Validation rejects it
  outright when `scheduler.enabled = true`, naming `"catalog"` (works
  today for multi-coordinator HA) or `"none"` (single-coordinator only)
  as the settings that actually start. Reserved for a future Kubernetes
  `Lease`-object backend.

`tick_secs` and `lease_ttl_secs` must both be greater than zero; either at
zero fails validation.

### Multi-coordinator HA

Two supported ways to run the maintenance subsystem safely across more
than one coordinator:

- Set `scheduler.enabled = true` with `lease = "catalog"` (the default)
  on every coordinator. Each one ticks independently against the same
  cron schedule and the same tables; the catalog lease arbitrates so only
  one of them actually dispatches a rewrite for a given table in a given
  window, and the others skip that tick for that table.
- Leave `scheduler.enabled = false` on every coordinator and drive timing
  externally instead: a Kubernetes `CronJob` with
  `concurrencyPolicy: Forbid` that issues the maintenance `CALL` (e.g.
  `CALL system.rewrite_data_files(...)`) on its own schedule.
  `concurrencyPolicy: Forbid` guarantees the CronJob itself never overlaps
  its own runs, so this shape is HA-safe with no internal lease at all:
  there is only ever one caller in flight.

Neither shape depends on the lease for correctness. If a lease operation
fails, or two coordinators somehow compact the same table concurrently
anyway, Iceberg's optimistic-concurrency commit still guarantees exactly
one of them wins; the other re-plans against the winner's new snapshot and
finds a no-op. The lease only avoids paying for the loser's redundant scan
and rewrite. See
[Distributed compaction](../design-notes/distributed-compaction.md#multi-coordinator-ha-the-lease-is-an-efficiency-layer-not-a-correctness-mechanism)
for the full argument.

### Per-table overrides

A table owner can override the global schedule and every
`[maintenance.compaction]` sizing knob for one table, without touching the
coordinator's config file, via `ALTER TABLE ... SET TBLPROPERTIES`:

| Table property | Overrides |
|---|---|
| `sqe.maintenance.compaction.schedule` | `maintenance.scheduler.schedule` |
| `sqe.maintenance.compaction.target-file-size-bytes` | `maintenance.compaction.target_file_size_bytes` |
| `sqe.maintenance.compaction.min-input-files` | `maintenance.compaction.min_input_files` |
| `sqe.maintenance.compaction.delete-file-threshold` | `maintenance.compaction.delete_file_threshold` |
| `sqe.maintenance.compaction.strategy` | `maintenance.compaction.strategy` |

An absent or blank property falls back to the global config value. A
numeric override that fails to parse also falls back to the global
value; the scheduler logs a warning naming the property and the rejected
value rather than failing the whole tick over one bad property on one
table. In active mode the resolved, per-table value is what both gates
eligibility and drives the rewrite: a table that loosens an override
(for example a lower `min-input-files`) is evaluated against its own
knob, not the global default it opted out of.

### The three gates

Autonomous mutation requires all three to line up. The advisory scheduler
loop already respects the first two when deciding which tables to
discover and report on; active mode additionally needs the third to hold
before it will commit anything:

1. Global `maintenance.mode` is `"advisory"` or `"active"` (never `"off"`).
2. The table owner has set the per-table property `sqe.maintenance.enabled
   = true` via `ALTER TABLE`. A table without this property is never
   selected, no matter what `mode` is set to.
3. The maintenance principal holds a least-privilege Polaris grant on the
   opted-in namespace: `TABLE_READ_DATA` for advisory mode, plus
   `TABLE_WRITE_DATA` for active mode, no `CREATE`/`DROP`/admin either way.
   Polaris enforces this server-side as defense-in-depth on top of SQE's
   own gates. In active mode, a table with the property but no write grant
   never silently skips: the rewrite attempt fails, and SQE records a
   `failed` `sqe_system.maintenance_log` row plus a
   `sqe_maintenance_job_total{status="failed"}` metric sample for it.

### `sqe_system.maintenance_log`

`maintenance.scheduler.state_table` (default `sqe_system.maintenance_log`)
holds job history, last-run state, and the catalog lease rows. SQE treats
this table as operator-created: nothing in SQE creates it, and the
scheduler degrades to warn-and-skip rather than failing hard when the table
is absent. Create it once with a schema matching `(job_id, table, trigger,
principal, started_at, finished_at, status, files_in, files_out, bytes_in,
bytes_out, rows_removed, snapshot_id, error)` before turning on
`scheduler.enabled`.

`status` is `"advisory"` for every table an advisory-mode tick analyzes.
In active mode a due, opted-in table produces exactly one terminal row per
tick: `"success"` for a committed rewrite, `"skipped"` when the table had
no eligible compaction debt (or the underlying rewrite itself chose to
skip), or `"failed"` for any error along the way (session mint, token
refresh, catalog build, or the rewrite commit itself). One table's
failure never aborts the tick or blocks any other opted-in table from
being considered.

An audit event (`AuditKind::Maintenance`) accompanies every advisory-mode
analysis and every active-mode `"success"` commit. `"skipped"` and
`"failed"` rows are not paired with an audit event; the `maintenance_log`
row plus the `sqe_maintenance_job_total{status=...}` metric sample are
the record of those outcomes.

### Snapshot stamping

Every active-mode commit stamps three Iceberg snapshot-summary
properties: `sqe.maintenance.job-id` (ties the snapshot back to its
`maintenance_log` row), `sqe.maintenance.principal` (the maintenance
service identity that committed it), and `sqe.maintenance.trigger`
(currently always `"scheduled"`). A compaction snapshot is therefore
attributable in the table's own history, independent of the state table:
inspect the snapshot summary directly (Iceberg snapshot metadata, or a
future `DESCRIBE HISTORY`-style surface) rather than `CALL
system.table_health`. `table_health`'s `last_compaction_snapshot_ms`
column is reserved for this but is not yet wired to read it; it always
returns `NULL` in this release.

### Distribution: `mode` picks coordinator-local vs the worker fleet

`[maintenance.distribution]` is active: `mode` decides whether an
active-mode rewrite runs on the coordinator alone or fans its file groups
out to the worker fleet. See [Distributed
compaction](../design-notes/distributed-compaction.md) for the full data
flow (planning, dispatch, worker rewrite, coordinator commit).

- `"auto"` (default). Coordinator-local when the healthy worker count is
  below `min_workers`, fans out to the fleet once it reaches
  `min_workers`.
- `"local"`. Always coordinator-local, even with a fleet present.
- `"require"`. Always fans out; never runs coordinator-local. Below
  `min_workers` it does not fall back to `"local"`, and the two call
  paths react differently, on purpose:
  - A scheduled active-mode tick SKIPS the job loudly: a `skipped`
    `sqe_system.maintenance_log` row, the existing `AuditKind::Maintenance`
    skip event, and a dedicated
    `sqe_maintenance_skipped_total{reason="insufficient_workers"}` metric
    sample an operator can alert on independently of the generic
    `sqe_maintenance_job_total{status="skipped"}` counter (which also
    fires for "no eligible debt").
  - A manual `CALL system.rewrite_data_files(..., distributed =>
    'require')` ERRORS instead: an interactive caller who explicitly asked
    to require the fleet gets a loud failure, never a silent
    coordinator-local rewrite.

`CALL system.rewrite_data_files` also accepts a per-call `distributed =>
'auto'|'local'|'require'` argument that overrides the configured `mode`
for that one call; omit it to use `[maintenance.distribution] mode`. See
[CALL procedures](../sql-reference/procedures.md).

Other knobs, all under `[maintenance.distribution]`:

- `min_workers` (default `2`). The healthy-worker floor `"auto"` and
  `"require"` compare against.
- `max_inflight_groups_per_worker` (default `1`). Hard per-worker cap on
  concurrently dispatched groups. A worker at the cap is never chosen for
  a new group; a group that cannot fit anywhere is deferred and retried
  briefly, never force-assigned past the cap.
- `group_attempts` (default `2`). Retries for one failed group, each on a
  worker other than the one that just failed it. A group that exhausts
  every currently-healthy worker, or every attempt, fails the whole
  job -- a distributed rewrite either commits everything or nothing;
  dispatch never continues once one group has permanently failed.
- `group_timeout_secs` (default `3600`). The real end-to-end bound on one
  group dispatch attempt: from the coordinator's `do_action` call to that
  group's terminal `Done` frame. A worker computes the entire rewrite
  (read, delete-apply, re-encode) before it emits any frame at all, so
  nothing about a hung or slow worker is visible until either this fires
  or the worker finally responds. Size it for the slowest group you
  expect to dispatch.
- `group_heartbeat_timeout_secs` (default `120`). Bounds the wait between
  frames once a worker has started responding (its first `Progress`
  heartbeat). Workers now emit a `Progress` frame every few record batches
  while the rewrite is still running, an internal fixed cadence, not a
  config knob, so a fresh frame arrives well inside this window as long as
  the worker keeps making forward progress. That makes this field a real
  mid-compute liveness bound: a worker that stalls partway through, a
  wedged read or a hung write, stops producing frames and is caught here
  instead of only at the coarser `group_timeout_secs`. A stalled worker's
  group is retried on a different healthy worker, up to `group_attempts`.
- `partial_progress` (default `false`). Opt-in: commit successful groups
  in batches of `partial_progress_batch` instead of collecting every group
  and committing one all-or-nothing `RewriteFilesAction`. Off, the job
  behaves exactly as before: one atomic commit for the whole job, any
  failure commits nothing. On, a terminal failure after one or more
  batches have already committed keeps those batches (they are never
  rolled back) and records `status = "partial"` in
  `sqe_system.maintenance_log` instead of failing the whole job. Trades a
  larger commit-conflict surface, N commits instead of one, each
  independently racing concurrent writers, for incremental durability on
  very large tables, where losing an entire multi-hour job to one late
  group failure is expensive. See [Distributed
  compaction](../design-notes/distributed-compaction.md) for the full
  per-batch commit sequence and retry layering.
- `partial_progress_batch` (default `10`). Number of eligible groups
  committed per `RewriteFilesAction` when `partial_progress` is `true`.
  Ignored when `partial_progress` is `false`. Must be at least 1 when
  `partial_progress` is `true`; validation rejects `0`.

**Accepted trade-off: orphans on a commit-conflict retry.** A concurrent
writer that commits between the coordinator's read and its
`RewriteFilesAction` commit produces a retryable conflict. On retry the
coordinator re-plans and re-dispatches the whole job from scratch rather
than patch the stale attempt, the same correctness rule the local path
already follows. Whatever the superseded attempt's workers already wrote
to S3 is never referenced by any commit and becomes an orphan, left for
`CALL system.remove_orphan_files`'s normal age-thresholded sweep to
reclaim. The trade-off is deliberate: correctness comes from never
committing a stale plan, not from cleaning up every write the moment its
result turns out to be unneeded.

The whole-job re-plan above applies to the first commit of a job, with
`partial_progress` on or off. With `partial_progress` on, a conflict on a
later batch is instead retried in place against the same worker-produced
files, no re-plan and no orphaned output, up to the same retry budget; see
[Distributed compaction](../design-notes/distributed-compaction.md) for
the retry-layering detail.

### Safety notes

- **Advisory first.** Run advisory mode against a table long enough to see
  its real compaction debt and validate the schedule and per-table knobs
  before opting that table into active mode.
- **Opt-in is per table, twice over.** A table is only ever touched by
  active mode when its owner has both set `sqe.maintenance.enabled = true`
  and granted the maintenance principal `TABLE_WRITE_DATA` in Polaris.
  Neither alone is sufficient.
- **Least-privilege grant.** Give the maintenance principal only
  `TABLE_READ_DATA` / `TABLE_WRITE_DATA` on the opted-in namespace, never
  `CREATE`/`DROP`/admin.
- **Compactions are reversible within the retention window.** A
  compaction commit is an ordinary Iceberg snapshot; the data files and
  manifests it superseded remain in place until `expire_snapshots` removes
  them. Reading a compacted table's history back to a prior snapshot and
  running `CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id
  => <id>)` undoes the compaction as long as that prior snapshot has not
  aged out.
- **`distribution.mode` decides the footprint.** `"local"`, and `"auto"`
  below `min_workers`, run coordinator-local: size `target_file_size_bytes`
  and `max_concurrent_jobs` for that single-process footprint. `"auto"`
  at or above `min_workers`, and `"require"`, fan groups out to the
  worker fleet instead; see [Distributed
  compaction](../design-notes/distributed-compaction.md).
- **Commit authority never leaves the coordinator.** In distributed mode
  workers read and write S3 directly but never obtain a catalog token and
  never commit; the coordinator validates every worker's output and
  commits one atomic `RewriteFilesAction`, exactly like the local path.

## Validation

SQE validates config at startup and fails fast on errors:

- `auth.client_id` must not be empty
- `catalog.catalog_url` must not be empty
- At least one of `auth.keycloak_url` or `auth.token_endpoint` must be set
- `coordinator.flight_sql_port` must differ from `coordinator.trino_http_port`
- `coordinator.flight_sql_port` must differ from `metrics.prometheus_port`
- TLS: if either cert or key is set, both must be set; referenced files must exist
- `maintenance.mode` other than `"off"` requires a `[maintenance.principal]` block
- `maintenance.scheduler.tick_secs` and `maintenance.scheduler.lease_ttl_secs` must both be greater than zero
- `maintenance.scheduler.enabled = true` with `lease = "none"` requires `single_scheduler_acknowledged = true`
- `maintenance.scheduler.enabled = true` with `lease = "kubernetes"` is rejected outright (not implemented; use `"catalog"` or `"none"`)
- `maintenance.distribution.partial_progress = true` requires `maintenance.distribution.partial_progress_batch` to be at least 1

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

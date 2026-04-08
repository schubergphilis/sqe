## Context

SQE is open-sourced under Apache 2.0. Two categories of changes:
1. **Vendor-neutral naming** — strip Keycloak/MinIO specifics from code, config, and docs
2. **Production security controls** — rate limiting, TLS, timeouts, audit, health, error sanitisation

## Goals / Non-Goals

**Goals:**
- Remove all `keycloak` identifiers from public API surface (config keys, metric names, log lines, docs)
- Remove MinIO references; replace with generic S3-compatible storage language
- Add security controls expected of a production SQL engine exposed to the network
- Zero breaking behaviour change for existing OIDC + S3 users (config migration with deprecation warning)

**Non-Goals:**
- New auth provider types (covered by pluggable-auth change)
- New catalog backends (covered by pluggable-catalogs change)
- Kubernetes Helm charts or production deployment guides

## Architecture

### Rename Map

| Old identifier | New identifier | Location |
|---|---|---|
| `sqe-auth/src/keycloak.rs` | `sqe-auth/src/oidc_password.rs` | crate |
| `[keycloak]` config section | `[auth.oidc]` | sqe.toml |
| `keycloak.token_url` | `auth.oidc.token_url` | config key |
| `keycloak.client_id` | `auth.oidc.client_id` | config key |
| `keycloak.client_secret` | `auth.oidc.client_secret` | config key |
| `keycloak.realm` | removed (absorbed into token_url) | config key |
| `sqe_auth_keycloak_*` metrics | `sqe_auth_oidc_*` | Prometheus |
| MinIO in docker-compose | removed | dev infra |
| MinIO in docs | "S3-compatible storage" | docs |

Old keys accepted for one release with a `WARN` log line: `config key 'keycloak.*' is deprecated, use 'auth.oidc.*'`.

### Security Controls Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Flight SQL Listener (TLS)                     │
├─────────────────────────────────────────────────────────────────┤
│  RateLimiter (token bucket per user + global)                   │
├─────────────────────────────────────────────────────────────────┤
│  Auth Handshake → SessionManager                                │
├─────────────────────────────────────────────────────────────────┤
│  Query Execution                                                │
│    QueryTimeout (tokio::time::timeout wrapping execution)       │
│    QueryCancellation (CancellationToken propagated to workers)  │
├─────────────────────────────────────────────────────────────────┤
│  SessionLifecycle (idle + absolute timeout sweeper)             │
├─────────────────────────────────────────────────────────────────┤
│  AuditLog (structured JSON, per query)                          │
└─────────────────────────────────────────────────────────────────┘

Admin HTTP (separate port, no auth):
  GET /healthz/live   → 200 always (process alive)
  GET /healthz/ready  → 200 when catalog reachable, 503 otherwise
```

### Rate Limiting

Token bucket per user (moka cache, evicts after idle). Global bucket as second gate.

```toml
[rate_limit]
enabled = true
per_user_queries_per_minute = 60
global_queries_per_minute = 1000
```

Implementation: `governor` crate (rate-limiting on top of `moka`). On limit exceeded: Flight error `RESOURCE_EXHAUSTED`, no session drop.

### TLS

```toml
[server]
tls_cert = "/etc/sqe/server.crt"
tls_key  = "/etc/sqe/server.key"
# dev mode only:
allow_plaintext = false
```

TLS is applied at the `tonic` transport layer. mTLS (client cert validation) is configured separately in the pluggable-auth change but the cert loading infrastructure lives here.

### Query Timeout

```toml
[query]
timeout_secs = 300        # default 5 minutes
# per-role overrides (optional):
[query.role_overrides]
admin = 3600
```

Implemented as `tokio::time::timeout` wrapping the execution future. On timeout:
- Cancel token fired → propagated to workers via Arrow Flight cancel
- Client receives Flight error `DEADLINE_EXCEEDED`
- Audit log records `outcome = "timeout"`

### Session Lifecycle

```toml
[session]
idle_timeout_secs    = 1800   # 30 minutes
absolute_timeout_secs = 28800 # 8 hours
```

Background sweeper runs every 60s alongside the token refresh task. Expired sessions are cleaned; active Flight streams receive a `UNAUTHENTICATED` error.

### Query Cancellation

Arrow Flight has a native cancel mechanism. Coordinator listens for `DoExchange` cancel signals and fires a `CancellationToken` that is:
1. Passed into the DataFusion execution plan context
2. Propagated to workers in the plan fragment metadata

DataFusion's `RecordBatchStream` polls the token and stops returning batches on cancel.

### Audit Log

One JSON line per query, written to stdout (12-factor) or a file.

```json
{
  "ts": "2026-03-19T10:00:00.123Z",
  "event": "query",
  "user": "alice",
  "session_id": "abc123",
  "query_hash": "sha256:deadbeef",
  "tables": ["catalog.db.orders"],
  "rows_returned": 42000,
  "duration_ms": 812,
  "outcome": "success",
  "client_ip": "10.0.0.5"
}
```

`query_hash` is SHA-256 of the normalised SQL text — the actual SQL is not logged by default (privacy). A `log_query_text = true` config flag enables full SQL logging.

### Error Sanitisation

In production mode (`server.debug = false`, the default), errors returned to clients contain:
- A short human message: `"query execution failed"`
- A request ID for correlation with server logs
- No catalog URLs, no stack traces, no internal type names

Internal errors are logged at `ERROR` level with full detail on the coordinator. The mapping uses a typed error enum (`SqeError`) with a `client_message()` method separate from `Display`.

### Health Endpoints

Separate `axum` HTTP server on `admin_port` (default 9090):

```
GET /healthz/live   → 200 OK (process started)
GET /healthz/ready  → 200 OK | 503 (catalog ping succeeds)
GET /metrics        → Prometheus text format (existing)
```

### Startup Validation

`sqe-core` config parsing runs a `validate()` pass after deserialization:
- Required fields: `server.bind`, `auth.oidc.token_url`, `auth.oidc.client_id`
- TLS: if `allow_plaintext = false`, cert+key must be readable at startup
- Port conflicts: `bind` port ≠ `admin_port`
- Unknown keys: `WARN` log (not error — forwards compatibility)

Fail-fast: coordinator exits with a clear error message and non-zero exit code if validation fails.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| SQL not logged by default | opt-in flag | Privacy for OSS deployments; PII in queries |
| Rate limit: token bucket | `governor` crate | Simple, well-tested, async-native |
| Admin port separate | yes (9090) | K8s readiness probes must not go through auth |
| Config deprecation window | 1 release | Breaking but safe; old key logs a warning |
| MinIO removal | hard remove | BSL licence makes it unsuitable for Apache 2.0 project |

## Risks

| Risk | Mitigation |
|---|---|
| Config key rename breaks existing deployments | Deprecation shim + clear changelog entry |
| TLS cert management complexity | Default ships with self-signed cert generation in dev mode |
| Audit log volume at high QPS | Log at INFO level, disable with `audit_log.enabled = false` |

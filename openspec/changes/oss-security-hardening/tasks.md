## 1. Rename Keycloak → OAuth2/OIDC

- [x] 1.1 Rename `sqe-auth/src/keycloak.rs` → `sqe-auth/src/oidc_password.rs`; update all internal references
- [x] 1.2 Rename config section: add `[auth.oidc]` parser; keep `[keycloak]` as deprecated alias with `WARN` log
- [x] 1.3 Rename Prometheus metrics `sqe_auth_keycloak_*` → `sqe_auth_oidc_*`
- [x] 1.4 Update all doc comments, log messages, and error strings containing "Keycloak"
- [x] 1.5 Update CLAUDE.md, README, and arch docs to use "OIDC provider" language
- [x] 1.6 Unit test: deprecated `[keycloak]` config loads with warning

## 2. Remove MinIO References

- [x] 2.1 Remove MinIO service from `docker-compose.yml` (quickstart); add comment pointing to S3-compatible alternatives
- [x] 2.2 Remove MinIO-specific examples from `sqe.toml.example`; replace with generic S3 config with endpoint override example
- [x] 2.3 Update all docs referencing MinIO to use "S3-compatible storage (e.g. Ceph, Garage, Cloudflare R2)"
- [x] 2.4 Update integration tests to use a different S3-compatible backend (Localstack or Ceph container)

## 3. Startup Config Validation

- [x] 3.1 Add `Config::validate()` method in `sqe-core`; checks required fields and port conflicts
- [x] 3.2 Coordinator calls `config.validate()` at startup; exits with clear message on failure
- [x] 3.3 TLS validation: if `allow_plaintext = false`, verify cert+key files are readable at startup
- [x] 3.4 Unit tests: missing required field, port conflict, missing cert file all produce clear errors

## 4. TLS — SKIPPED (deferred to pluggable-auth change)

- [ ] 4.1 ~~Add `[server.tls]` config~~ — deferred
- [ ] 4.2 ~~Configure `tonic` server transport with TLS~~ — deferred
- [ ] 4.3 ~~Dev mode plaintext~~ — deferred
- [ ] 4.4 ~~Integration test~~ — deferred

## 5. Rate Limiting

- [x] 5.1 Add `[rate_limit]` config: `enabled`, `per_user_queries_per_minute`, `global_queries_per_minute`
- [x] 5.2 Integrate `governor` crate; implement per-user token bucket (moka cache keyed by user_id)
- [x] 5.3 Add global bucket as second gate
- [x] 5.4 On limit exceeded: return Flight `RESOURCE_EXHAUSTED` error; do not drop session
- [x] 5.5 Unit tests: rate limit fires after configured threshold; passes below threshold

## 6. Query Timeouts

- [x] 6.1 Add `[query] timeout_secs` config (default 300); `[query.role_overrides]` map
- [x] 6.2 Wrap coordinator execution future with `tokio::time::timeout`
- [x] 6.3 On timeout: return error to client
- [x] 6.4 Unit test: query exceeding timeout returns error; query within limit completes

## 7. Session Lifecycle

- [x] 7.1 Add `[session] idle_timeout_secs` and `absolute_timeout_secs` config
- [x] 7.2 Track `last_activity_at` on session; update on every query
- [x] 7.3 Extend background sweeper (token refresh task) to also expire idle/absolute sessions
- [x] 7.4 On session expiry mid-stream: return `UNAUTHENTICATED` error
- [x] 7.5 Unit test: idle session expires; active session survives idle timeout

## 8. Query Cancellation

- [x] 8.1 Create `CancellationToken` per query (tokio-util); store in active query registry
- [x] 8.2 Handle Arrow Flight client cancel signal: fire token; clean up registry entry
- [x] 8.3 Pass token into DataFusion `TaskContext`; propagate to workers via Flight metadata
- [x] 8.4 Integration test: cancel in-flight query; verify workers stop and resources freed

## 9. Audit Log

- [x] 9.1 Define `AuditEvent` struct: ts, event, user, session_id, query_hash, tables, rows, duration_ms, outcome, client_ip
- [x] 9.2 Compute `query_hash` as SHA-256 of normalised SQL (whitespace-collapsed, uppercase keywords)
- [x] 9.3 Add `[audit_log] enabled`, `log_query_text` config
- [x] 9.4 Emit one `AuditEvent` per query completion (success, error, timeout, cancel) as structured JSON via `tracing`
- [x] 9.5 Unit test: audit event emitted for successful query; `query_text` absent unless flag set

## 10. Error Sanitisation

- [x] 10.1 Add `client_message()` method to `SqeError` returning short, safe strings
- [x] 10.2 In production mode (`server.debug = false`): return `client_message()` + request_id to client
- [x] 10.3 In debug mode: return full error chain (dev only)
- [x] 10.4 Log full error at `ERROR` level on coordinator regardless of mode
- [x] 10.5 Unit test: production mode hides internal details; debug mode exposes them

## 11. Health Endpoints — ALREADY DONE (core engine)

- [x] 11.1 Start separate `axum` HTTP server on `admin_port` (default 9090) alongside Flight SQL
- [x] 11.2 `GET /healthz/live` → 200 always once process started
- [x] 11.3 `GET /healthz/ready` → ping catalog; 200 if reachable, 503 if not
- [x] 11.4 `GET /metrics` → existing Prometheus endpoint (move here from main port if currently on main)
- [x] 11.5 Integration test: liveness returns 200 immediately; readiness returns 503 before catalog up, 200 after

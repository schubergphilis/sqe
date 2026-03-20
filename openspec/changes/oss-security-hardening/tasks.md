## 1. Rename Keycloak → OAuth2/OIDC

- [ ] 1.1 Rename `sqe-auth/src/keycloak.rs` → `sqe-auth/src/oidc_password.rs`; update all internal references
- [ ] 1.2 Rename config section: add `[auth.oidc]` parser; keep `[keycloak]` as deprecated alias with `WARN` log
- [ ] 1.3 Rename Prometheus metrics `sqe_auth_keycloak_*` → `sqe_auth_oidc_*`
- [ ] 1.4 Update all doc comments, log messages, and error strings containing "Keycloak"
- [ ] 1.5 Update CLAUDE.md, README, and arch docs to use "OIDC provider" language
- [ ] 1.6 Unit test: deprecated `[keycloak]` config loads with warning

## 2. Remove MinIO References

- [ ] 2.1 Remove MinIO service from `docker-compose.yml` (quickstart); add comment pointing to S3-compatible alternatives
- [ ] 2.2 Remove MinIO-specific examples from `sqe.toml.example`; replace with generic S3 config with endpoint override example
- [ ] 2.3 Update all docs referencing MinIO to use "S3-compatible storage (e.g. Ceph, Garage, Cloudflare R2)"
- [ ] 2.4 Update integration tests to use a different S3-compatible backend (Localstack or Ceph container)

## 3. Startup Config Validation

- [ ] 3.1 Add `Config::validate()` method in `sqe-core`; checks required fields and port conflicts
- [ ] 3.2 Coordinator calls `config.validate()` at startup; exits with clear message on failure
- [ ] 3.3 TLS validation: if `allow_plaintext = false`, verify cert+key files are readable at startup
- [ ] 3.4 Unit tests: missing required field, port conflict, missing cert file all produce clear errors

## 4. TLS

- [ ] 4.1 Add `[server.tls]` config: `cert_file`, `key_file`, `allow_plaintext` (default false)
- [ ] 4.2 Configure `tonic` server transport with TLS from loaded cert/key
- [ ] 4.3 Dev mode: if no cert configured and `allow_plaintext = true`, start without TLS (warning logged)
- [ ] 4.4 Integration test: TLS-enabled coordinator accepts Flight SQL client with matching cert

## 5. Rate Limiting

- [ ] 5.1 Add `[rate_limit]` config: `enabled`, `per_user_queries_per_minute`, `global_queries_per_minute`
- [ ] 5.2 Integrate `governor` crate; implement per-user token bucket (moka cache keyed by user_id)
- [ ] 5.3 Add global bucket as second gate
- [ ] 5.4 On limit exceeded: return Flight `RESOURCE_EXHAUSTED` error; do not drop session
- [ ] 5.5 Unit tests: rate limit fires after configured threshold; passes below threshold

## 6. Query Timeouts

- [ ] 6.1 Add `[query] timeout_secs` config (default 300); `[query.role_overrides]` map
- [ ] 6.2 Wrap coordinator execution future with `tokio::time::timeout`
- [ ] 6.3 On timeout: fire cancellation token; return Flight `DEADLINE_EXCEEDED` to client
- [ ] 6.4 Unit test: query exceeding timeout returns error; query within limit completes

## 7. Session Lifecycle

- [ ] 7.1 Add `[session] idle_timeout_secs` and `absolute_timeout_secs` config
- [ ] 7.2 Track `last_activity_at` on session; update on every query
- [ ] 7.3 Extend background sweeper (token refresh task) to also expire idle/absolute sessions
- [ ] 7.4 On session expiry mid-stream: return `UNAUTHENTICATED` error
- [ ] 7.5 Unit test: idle session expires; active session survives idle timeout

## 8. Query Cancellation

- [ ] 8.1 Create `CancellationToken` per query (tokio-util); store in active query registry
- [ ] 8.2 Handle Arrow Flight client cancel signal: fire token; clean up registry entry
- [ ] 8.3 Pass token into DataFusion `TaskContext`; propagate to workers via Flight metadata
- [ ] 8.4 Integration test: cancel in-flight query; verify workers stop and resources freed

## 9. Audit Log

- [ ] 9.1 Define `AuditEvent` struct: ts, event, user, session_id, query_hash, tables, rows, duration_ms, outcome, client_ip
- [ ] 9.2 Compute `query_hash` as SHA-256 of normalised SQL (whitespace-collapsed, uppercase keywords)
- [ ] 9.3 Add `[audit_log] enabled`, `log_query_text` config
- [ ] 9.4 Emit one `AuditEvent` per query completion (success, error, timeout, cancel) as structured JSON via `tracing`
- [ ] 9.5 Unit test: audit event emitted for successful query; `query_text` absent unless flag set

## 10. Error Sanitisation

- [ ] 10.1 Add `client_message()` method to `SqeError` returning short, safe strings
- [ ] 10.2 In production mode (`server.debug = false`): return `client_message()` + request_id to client
- [ ] 10.3 In debug mode: return full error chain (dev only)
- [ ] 10.4 Log full error at `ERROR` level on coordinator regardless of mode
- [ ] 10.5 Unit test: production mode hides internal details; debug mode exposes them

## 11. Health Endpoints

- [ ] 11.1 Start separate `axum` HTTP server on `admin_port` (default 9090) alongside Flight SQL
- [ ] 11.2 `GET /healthz/live` → 200 always once process started
- [ ] 11.3 `GET /healthz/ready` → ping catalog; 200 if reachable, 503 if not
- [ ] 11.4 `GET /metrics` → existing Prometheus endpoint (move here from main port if currently on main)
- [ ] 11.5 Integration test: liveness returns 200 immediately; readiness returns 503 before catalog up, 200 after

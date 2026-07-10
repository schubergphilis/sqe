# SQE Security & Functional Audit

**Date:** 2026-04-08
**Scope:** Full codebase security review + functional verification
**Version:** v0.15.0 (pre-release, 44 MRs, 493+ commits)
**Branch:** feat/oss-release-and-audit

---

## Security Audit

### Auth Passthrough

**Status:** ✅ CONFIRMED

- No bearer tokens, access tokens, or refresh tokens are logged in production code paths
- All logging uses session IDs, usernames, or client IPs — never raw token values
- Session cleanup is well-implemented: `remove_session()` for explicit disconnect, `sweep_expired_sessions()` for idle + absolute timeout, and proactive eviction on session access
- Comprehensive unit tests cover sweep empty, idle, absolute, active, mixed, and concurrent cases

**⚠️ Issue: Session file persistence writes raw tokens**

When file persistence is enabled (`persistence = "file"` in config), `snapshot_to_file()` serializes `access_token` in plaintext JSON to `persistence_path` (default: `/tmp/sqe-sessions.json`). File persistence is opt-in (not enabled by default), but when enabled, tokens are stored unencrypted with no documentation about required file permissions.

**Recommendation:** Add a startup warning when file persistence is enabled. Document required file permissions (600). Consider encrypting tokens at rest or storing only session IDs.

**Note:** Token fingerprint pattern (last 8 chars for debug correlation) is not yet implemented. This is a nice-to-have for operational debugging, not a security vulnerability.

### Error Sanitization

**Status:** ✅ CONFIRMED

- `SqeError::client_message()` classifies errors as user vs. system
- System errors (Internal, Config, CatalogError, StorageError) return generic messages:
  - "Internal error", "Catalog operation failed", "Storage operation failed"
- User errors (syntax, not-found, auth, not-supported) show cleaned detail with DataFusion prefix noise stripped
- `to_client_error(debug)` provides debug mode toggle for dev environments
- 24 unit tests cover all error variants and classification
- All Flight SQL protocol handlers use `sqe_error_to_status()` which calls `client_message()` — zero bypasses
- Internal details logged server-side via `internal_detail = %e` (correct design — forensics in server logs, sanitized message to client)

### Token Validation

**Status:** ✅ CONFIRMED

- JWT expiry (`exp` claim) enforced by default via `jsonwebtoken` crate's `Validation::new()`
- Expired tokens rejected with `AuthError::AuthFailed("JWT has expired")`
- Explicit test (`expired_jwt_returns_auth_failed`) verifies expiry rejection
- JWKS fetched from configured endpoint, cached for subsequent validations

**⚠️ Issue: Audience validation silently disabled**

When the `audience` config field is absent or empty, `validation.validate_aud = false` is set (`bearer_token.rs:254`). This means deployments without an explicit audience claim in config accept tokens from any audience — including tokens issued for different services. The `sqe.toml.example` does not set an `audience` field, so audience validation is disabled by default in the reference config.

**Recommendation:** Log a warning at startup when audience validation is disabled. Add `audience` to the example config (commented out) with documentation explaining the risk.

### TLS Enforcement

**Status:** ✅ CONFIRMED

- `[coordinator.tls]` config section with `cert_file`, `key_file`, optional `ca_file` (mTLS)
- Dedicated `tls.rs` module in sqe-coordinator implements `build_server_tls_config()`
- Both `main.rs` and `sqe_server.rs` apply TLS to Flight SQL gRPC listener
- Startup WARNING emitted when TLS is not configured: "TLS is DISABLED — Flight SQL and worker connections are unencrypted"
- TLS is opt-in (correct for local development) but warns loudly when disabled

### Config Secrets

**Status:** ⚠️ ISSUES FOUND

1. **S3 credentials use MinIO defaults:** `sqe.toml.example:101-102` contains `s3_access_key = "s3admin"` and `s3_secret_key = "s3admin"`. While clearly development-only, these real-looking values commonly get copied verbatim into production.

2. **`ssl_verification = false` active in example:** `sqe.toml.example:37` has SSL verification disabled for OIDC/token exchange calls. Intended for local dev with self-signed certs, but the value is active (not commented out) and will be copied by operators.

**Recommendation:** Replace S3 credentials with `"<your-s3-access-key>"` placeholders. Comment out `ssl_verification = false` or add a prominent warning.

### Query Cancellation

**Status:** ✅ CONFIRMED

- `CancellationToken` from `tokio-util` used correctly
- `QueryTracker` holds `DashMap<Uuid, CancellationToken>` — `cancel()` fires token and records cancellation
- `do_action_cancel_query` handler in Flight SQL wires client cancel requests through to tracker
- Unit tests confirm `cancel_fires_token_and_records` and `cancel_unknown_returns_false`

---

## Dependency Security

| Crate | Advisory | Severity | Action |
|---|---|---|---|
| `rsa` v0.9.10 | RUSTSEC-2023-0071 (Marvin Attack) | Medium | **Removed entirely.** Replaced with static test keypair in sqe-auth. |
| `paste` v1.0.15 | RUSTSEC-2024-0436 (unmaintained) | Informational | **Documented.** Transitive from arrow-flight, datafusion, parquet, tonic. No vulnerability. Feature-complete crate. Ignored in `deny.toml`. Resolves when upstream migrates. |

---

## Functional Audit

| Check | Status | Details |
|---|---|---|
| `cargo clippy --all-targets --all-features -- -D warnings` | ✅ PASS | Zero warnings (1 unused import fixed during audit) |
| `cargo test --all` | ✅ PASS | **1,218 tests passed**, 0 failed, 66 ignored |
| `cargo audit` | ✅ PASS | No active advisories (rsa removed) |
| `cargo deny check advisories` | ✅ PASS | paste ignored per deny.toml policy |
| Docker build | ⏭️ DEFERRED | Requires Docker daemon |
| Integration tests | ⏭️ DEFERRED | Requires Polaris + S3 quickstart stack |

### Test Distribution

| Crate | Tests |
|---|---|
| sqe-coordinator | 274 |
| sqe-auth | 201 |
| sqe-sql | 175 |
| sqe-policy | 139 |
| sqe-planner | 113 |
| sqe-core | 73 |
| sqe-catalog | 59 |
| sqe-metrics | 51 |
| sqe-worker | 76 |
| sqe-trino-compat | 41 |
| sqe-bench | 8 |
| sqe-cli | 8 |

---

## Issues Summary

| # | Area | Severity | Description | Status |
|---|---|---|---|---|
| 1 | Auth | Medium | Session file persistence writes raw tokens to disk | ✅ Fixed — startup WARNING emitted when file persistence enabled, advises chmod 600 |
| 2 | Token | Medium | JWT audience validation silently disabled when unconfigured | ✅ Fixed — WARNING logged at `BearerTokenProvider::new()` when no audience configured; `audience` field added to sqe.toml.example (commented) |
| 3 | Config | Low | S3 credentials use MinIO defaults in example | ✅ Fixed — replaced with `<your-s3-access-key>` / `<your-s3-secret-key>` placeholders |
| 4 | Config | Low | `ssl_verification = false` active in example | ✅ Fixed — commented out with warning. Code default is `true` (via `default_true`); startup WARNING already emitted when disabled |
| 5 | Auth | Info | Token fingerprint pattern not implemented | ✅ Already implemented — `Session::token_fingerprint()` (hash-based) used in `SessionCatalog` debug logging |

**No critical security vulnerabilities found.** All 5 issues resolved.

---

## Deferred Items

| Item | Tracking |
|---|---|
| Integration test full run | Requires Polaris + S3 stack. Run via `scripts/integration-test.sh`. |
| Docker build verification | Requires Docker daemon. |
| Rate limiting load test | Rate limiting implemented (governor crate). Full load test deferred. |
| EXPLAIN FULL metrics accuracy | Requires live query execution. Deferred to integration phase. |
| Iceberg partition pruning edge cases | Functional, but deeply nested partitions not exhaustively tested. |

---

*This audit covers the codebase at the v0.15.0 milestone (44 merged MRs, 493+ commits). Next audit recommended after Spec C (Pluggable Catalogs) completion.*

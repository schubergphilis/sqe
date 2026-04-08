# OSS Security Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename all Keycloak/MinIO vendor-specific identifiers to generic OAuth2/OIDC/S3-compatible language, remove MinIO from dev infrastructure, and add production security controls (rate limiting, TLS, timeouts, audit log, health endpoints, error sanitisation).

**Architecture:** Security controls layer into the coordinator as thin middleware between the Flight listener and execution engine. No changes to query path semantics — only lifecycle, limits, and observability. Config rename uses a one-version deprecation shim so existing deployments are not broken on upgrade.

**Tech Stack:** Rust, tonic (TLS), governor (rate limiting), tokio-util (CancellationToken), sha2 (query hash), axum (health HTTP), notify (file watch), tracing (audit JSON)

**Spec:** `openspec/changes/oss-security-hardening/`

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `crates/sqe-auth/src/oidc_password.rs` | rename from `keycloak.rs` | generalised OIDC ROPC provider |
| `crates/sqe-core/src/config.rs` | modify | add deprecation shim for `[keycloak]`, add new config sections |
| `crates/sqe-coordinator/src/rate_limit.rs` | create | token bucket rate limiter |
| `crates/sqe-coordinator/src/session.rs` | modify | add idle + absolute timeout tracking |
| `crates/sqe-coordinator/src/timeout.rs` | create | query timeout wrapper |
| `crates/sqe-coordinator/src/cancel.rs` | create | cancellation token registry |
| `crates/sqe-coordinator/src/audit.rs` | create | audit event struct + emitter |
| `crates/sqe-coordinator/src/health.rs` | create | axum health HTTP server |
| `crates/sqe-coordinator/src/error.rs` | modify | add `client_message()`, request ID |
| `docker-compose.yml` | modify | remove MinIO service |
| `sqe.toml.example` | modify | update config examples |
| `docs/book/src/architecture/auth-flow.md` | modify | remove Keycloak-specific language |
| `docs/book/src/deployment/configuration.md` | modify | document new config sections |

---

### Task 1: Rename keycloak → oidc in code

**Files:**
- Rename: `crates/sqe-auth/src/keycloak.rs` → `crates/sqe-auth/src/oidc_password.rs`
- Modify: `crates/sqe-auth/src/lib.rs`
- Modify: `crates/sqe-auth/src/oidc_password.rs` (all internal identifiers)
- Modify: `crates/sqe-coordinator/src/session.rs` (import paths)

- [ ] **Step 1: Write failing test for renamed module**
```rust
// crates/sqe-auth/tests/rename_test.rs
use sqe_auth::OidcPasswordClient;  // should not compile yet if name is wrong

#[test]
fn oidc_client_is_accessible() {
    let _ = std::any::type_name::<sqe_auth::OidcPasswordClient>();
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sqe-auth rename_test 2>&1 | head -20`
Expected: compile error — `OidcPasswordClient` not found

- [ ] **Step 3: Rename the file and update identifiers**

```bash
mv crates/sqe-auth/src/keycloak.rs crates/sqe-auth/src/oidc_password.rs
```

In `oidc_password.rs`: rename `KeycloakClient` → `OidcPasswordClient`, `KeycloakConfig` → `OidcPasswordConfig`, `KeycloakError` → `OidcAuthError`. Update `lib.rs` to `pub mod oidc_password; pub use oidc_password::OidcPasswordClient;`

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-auth 2>&1`
Expected: all tests pass

- [ ] **Step 5: Search for remaining keycloak references**

Run: `grep -ri "keycloak" crates/ --include="*.rs" | grep -v "deprecated\|warn\|test"`
Expected: zero results (only deprecation shim and tests should remain)

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-auth/
git commit -m "refactor(auth): rename keycloak module to oidc_password"
```

---

### Task 2: Config deprecation shim

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Test: `crates/sqe-core/tests/config_rename_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-core/tests/config_rename_test.rs
use sqe_core::config::Config;

#[test]
fn deprecated_keycloak_section_loads_with_warning() {
    let toml = r#"
[keycloak]
token_url = "http://example.com/token"
client_id = "sqe"
"#;
    let (config, warnings) = Config::from_str_with_warnings(toml).unwrap();
    assert_eq!(config.auth.oidc.token_url, "http://example.com/token");
    assert!(warnings.iter().any(|w| w.contains("deprecated")));
}

#[test]
fn new_auth_oidc_section_loads_without_warning() {
    let toml = r#"
[auth.oidc]
token_url = "http://example.com/token"
client_id = "sqe"
"#;
    let (config, warnings) = Config::from_str_with_warnings(toml).unwrap();
    assert!(warnings.is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-core config_rename 2>&1`
Expected: compile error — `from_str_with_warnings` not found

- [ ] **Step 3: Implement shim**

In `config.rs`: add `KeycloakCompatConfig` with serde `#[serde(rename = "keycloak")]`; in `from_str_with_warnings`, try both sections; populate `auth.oidc` from whichever is present; return deprecation warning if `[keycloak]` was used.

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-core config_rename 2>&1`
Expected: both tests pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-core/src/config.rs crates/sqe-core/tests/
git commit -m "feat(config): deprecate [keycloak] config section, add [auth.oidc] alias"
```

---

### Task 3: Remove MinIO, update dev infra

**Files:**
- Modify: `docker-compose.yml`
- Modify: `sqe.toml.example`
- Modify: `docs/book/src/deployment/configuration.md`

- [ ] **Step 1: Remove MinIO from docker-compose**

In `docker-compose.yml`: delete the `minio` service block and any `depends_on: minio` entries. Add a comment:
```yaml
# Object storage: use any S3-compatible backend (Ceph, Garage, Cloudflare R2, SeaweedFS)
# or AWS S3. For local dev, configure storage pointing to your preferred emulator.
```

- [ ] **Step 2: Update sqe.toml.example**

Replace MinIO-specific example:
```toml
# S3-compatible storage (AWS S3, Ceph, Cloudflare R2, Garage, SeaweedFS)
[storage]
type   = "s3"
region = "us-east-1"
# Uncomment for S3-compatible endpoints (Ceph, R2, etc.):
# endpoint   = "https://s3.example.com"
# path_style = true

[storage.credentials]
type             = "static"
access_key_id    = "REPLACE_ME"
secret_access_key = "REPLACE_ME"
```

- [ ] **Step 3: Update docs**

In `configuration.md`: replace all "MinIO" occurrences with "S3-compatible storage".

- [ ] **Step 4: Commit**
```bash
git add docker-compose.yml sqe.toml.example docs/
git commit -m "chore: remove MinIO (BSL licence); update to generic S3-compatible storage"
```

---

### Task 4: Startup config validation

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Create: `crates/sqe-core/src/config_validate.rs`
- Test: `crates/sqe-core/tests/config_validate_test.rs`

- [ ] **Step 1: Write failing tests**
```rust
// crates/sqe-core/tests/config_validate_test.rs
use sqe_core::config::Config;

#[test]
fn missing_token_url_fails_validation() {
    let toml = r#"[auth.oidc]
client_id = "sqe""#;
    let config = Config::from_str(toml).unwrap();
    let err = config.validate().unwrap_err();
    assert!(err.to_string().contains("auth.oidc.token_url"));
}

#[test]
fn valid_config_passes_validation() {
    let toml = r#"
[auth.oidc]
token_url = "http://idp/token"
client_id = "sqe"
[server]
bind = "0.0.0.0:50051"
allow_plaintext = true
"#;
    let config = Config::from_str(toml).unwrap();
    assert!(config.validate().is_ok());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-core config_validate 2>&1`
Expected: `validate` method not found

- [ ] **Step 3: Implement validate()**

```rust
impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.auth.oidc.token_url.is_empty() {
            return Err(ConfigError::missing("auth.oidc.token_url"));
        }
        if self.auth.oidc.client_id.is_empty() {
            return Err(ConfigError::missing("auth.oidc.client_id"));
        }
        if let Some(tls) = &self.server.tls {
            if !self.server.allow_plaintext {
                std::fs::metadata(&tls.cert_file)
                    .map_err(|_| ConfigError::file_not_found("server.tls.cert_file", &tls.cert_file))?;
                std::fs::metadata(&tls.key_file)
                    .map_err(|_| ConfigError::file_not_found("server.tls.key_file", &tls.key_file))?;
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Wire into coordinator main**

In `sqe-coordinator/src/main.rs`: call `config.validate().unwrap_or_else(|e| { eprintln!("Configuration error: {e}"); std::process::exit(1); });`

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-core config_validate 2>&1`
Expected: all pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-core/ crates/sqe-coordinator/src/main.rs
git commit -m "feat(config): add startup validation with clear error messages"
```

---

### Task 5: Rate limiting

**Files:**
- Create: `crates/sqe-coordinator/src/rate_limit.rs`
- Modify: `crates/sqe-coordinator/Cargo.toml` (add `governor`)
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Test: `crates/sqe-coordinator/tests/rate_limit_test.rs`

- [ ] **Step 1: Add governor dependency**
```toml
governor = { version = "0.6", features = ["std", "dashmap"] }
```

- [ ] **Step 2: Write failing test**
```rust
// crates/sqe-coordinator/tests/rate_limit_test.rs
use sqe_coordinator::rate_limit::RateLimiter;
use sqe_core::config::RateLimitConfig;

#[tokio::test]
async fn per_user_limit_fires_at_threshold() {
    let cfg = RateLimitConfig { enabled: true, per_user_queries_per_minute: 3, global_queries_per_minute: 1000 };
    let limiter = RateLimiter::new(cfg);
    assert!(limiter.check_user("alice").is_ok());
    assert!(limiter.check_user("alice").is_ok());
    assert!(limiter.check_user("alice").is_ok());
    assert!(limiter.check_user("alice").is_err()); // 4th exceeds limit
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p sqe-coordinator rate_limit 2>&1`
Expected: `rate_limit` module not found

- [ ] **Step 4: Implement RateLimiter**

```rust
// crates/sqe-coordinator/src/rate_limit.rs
use governor::{Quota, RateLimiter as GovernorRL, state::keyed::DashMapStateStore};
use std::{num::NonZeroU32, sync::Arc};

pub struct RateLimiter {
    per_user: Arc<GovernorRL<String, DashMapStateStore<String>, ...>>,
    global: Arc<GovernorRL<...>>,
    enabled: bool,
}

impl RateLimiter {
    pub fn new(cfg: RateLimitConfig) -> Self { ... }
    pub fn check_user(&self, user_id: &str) -> Result<(), RateLimitError> { ... }
}
```

- [ ] **Step 5: Run test**

Run: `cargo test -p sqe-coordinator rate_limit 2>&1`
Expected: passes

- [ ] **Step 6: Wire into Flight request handler**

In coordinator's Flight `do_get` handler: call `rate_limiter.check_user(&session.user_id)?` before executing query.

- [ ] **Step 7: Commit**
```bash
git add crates/sqe-coordinator/
git commit -m "feat(coordinator): add per-user and global query rate limiting"
```

---

### Task 6: Query timeout + cancellation

**Files:**
- Create: `crates/sqe-coordinator/src/timeout.rs`
- Create: `crates/sqe-coordinator/src/cancel.rs`
- Modify: `crates/sqe-coordinator/src/executor.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-coordinator/tests/timeout_test.rs
use sqe_coordinator::timeout::with_query_timeout;
use std::time::Duration;

#[tokio::test]
async fn timeout_fires_after_deadline() {
    let result = with_query_timeout(
        Duration::from_millis(50),
        async { tokio::time::sleep(Duration::from_secs(10)).await; Ok::<_, ()>(42) }
    ).await;
    assert!(result.is_err()); // timed out
}

#[tokio::test]
async fn fast_query_completes_before_timeout() {
    let result = with_query_timeout(
        Duration::from_secs(5),
        async { Ok::<_, ()>(42) }
    ).await;
    assert_eq!(result.unwrap(), 42);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-coordinator timeout 2>&1`
Expected: module not found

- [ ] **Step 3: Implement timeout wrapper**
```rust
// crates/sqe-coordinator/src/timeout.rs
use tokio::time::{timeout, Duration};
use crate::error::SqeError;

pub async fn with_query_timeout<F, T>(duration: Duration, fut: F) -> Result<T, SqeError>
where F: std::future::Future<Output = Result<T, SqeError>> {
    timeout(duration, fut).await
        .map_err(|_| SqeError::QueryTimeout)?
}
```

- [ ] **Step 4: Implement CancellationToken registry**
```rust
// crates/sqe-coordinator/src/cancel.rs
use tokio_util::sync::CancellationToken;
use dashmap::DashMap;

pub struct CancelRegistry {
    tokens: DashMap<Uuid, CancellationToken>,
}

impl CancelRegistry {
    pub fn register(&self, query_id: Uuid) -> CancellationToken { ... }
    pub fn cancel(&self, query_id: Uuid) { ... }
    pub fn remove(&self, query_id: Uuid) { ... }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-coordinator timeout cancel 2>&1`
Expected: all pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-coordinator/src/timeout.rs crates/sqe-coordinator/src/cancel.rs
git commit -m "feat(coordinator): add query timeout and cancellation token registry"
```

---

### Task 7: Audit log

**Files:**
- Create: `crates/sqe-coordinator/src/audit.rs`
- Modify: `crates/sqe-coordinator/src/executor.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-coordinator/tests/audit_test.rs
use sqe_coordinator::audit::{AuditEvent, AuditLogger};

#[test]
fn audit_event_serialises_to_json() {
    let event = AuditEvent::new_success("alice", "sess1", "SELECT 1", &["catalog.db.t"], 100, 50);
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"user\":\"alice\""));
    assert!(json.contains("\"outcome\":\"success\""));
    assert!(!json.contains("SELECT 1")); // query text not included by default
    assert!(json.contains("query_hash"));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-coordinator audit 2>&1`
Expected: module not found

- [ ] **Step 3: Implement AuditEvent + logger**
```rust
// crates/sqe-coordinator/src/audit.rs
use sha2::{Sha256, Digest};
use serde::Serialize;
use chrono::Utc;

#[derive(Serialize)]
pub struct AuditEvent {
    pub ts: String,
    pub event: &'static str,
    pub user: String,
    pub session_id: String,
    pub query_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    pub tables: Vec<String>,
    pub rows_returned: u64,
    pub duration_ms: u64,
    pub outcome: String,
}

impl AuditEvent {
    pub fn new_success(user: &str, session_id: &str, sql: &str, tables: &[&str], rows: u64, duration_ms: u64) -> Self {
        let hash = format!("sha256:{}", hex::encode(Sha256::digest(sql.to_uppercase().split_whitespace().collect::<Vec<_>>().join(" ").as_bytes())));
        Self { ts: Utc::now().to_rfc3339(), event: "query", user: user.into(), session_id: session_id.into(), query_hash: hash, query_text: None, tables: tables.iter().map(|s| s.to_string()).collect(), rows_returned: rows, duration_ms, outcome: "success".into() }
    }
}

pub struct AuditLogger { pub log_query_text: bool }
impl AuditLogger {
    pub fn emit(&self, mut event: AuditEvent, sql: &str) {
        if self.log_query_text { event.query_text = Some(sql.to_string()); }
        tracing::info!(target: "sqe_audit", "{}", serde_json::to_string(&event).unwrap());
    }
}
```

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-coordinator audit 2>&1`
Expected: passes

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-coordinator/src/audit.rs
git commit -m "feat(coordinator): add structured JSON audit log per query"
```

---

### Task 8: Health endpoints

**Files:**
- Create: `crates/sqe-coordinator/src/health.rs`
- Modify: `crates/sqe-coordinator/src/main.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-coordinator/tests/health_test.rs
use axum_test::TestServer;
use sqe_coordinator::health::health_router;

#[tokio::test]
async fn liveness_returns_200() {
    let app = health_router(None);
    let server = TestServer::new(app).unwrap();
    let resp = server.get("/healthz/live").await;
    resp.assert_status_ok();
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-coordinator health 2>&1`
Expected: module not found

- [ ] **Step 3: Implement health router**
```rust
// crates/sqe-coordinator/src/health.rs
use axum::{Router, routing::get, response::IntoResponse, http::StatusCode};

pub fn health_router(catalog_ping: Option<impl Fn() -> bool + Clone + Send + 'static>) -> Router {
    Router::new()
        .route("/healthz/live", get(|| async { StatusCode::OK }))
        .route("/healthz/ready", get(move || {
            let ping = catalog_ping.clone();
            async move {
                if ping.map(|f| f()).unwrap_or(true) { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE }
            }
        }))
}
```

- [ ] **Step 4: Start admin server in main**

Spawn `health_router` on `admin_port` (default 9090) as a separate tokio task alongside the Flight server.

- [ ] **Step 5: Run test**

Run: `cargo test -p sqe-coordinator health 2>&1`
Expected: passes

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-coordinator/src/health.rs crates/sqe-coordinator/src/main.rs
git commit -m "feat(coordinator): add health endpoints on admin port"
```

---

### Task 9: Error sanitisation

**Files:**
- Modify: `crates/sqe-coordinator/src/error.rs`

- [ ] **Step 1: Write failing test**
```rust
#[test]
fn production_error_hides_internals() {
    let err = SqeError::CatalogConnection { url: "http://internal-url".into(), detail: "connection refused".into() };
    let client_msg = err.client_message();
    assert!(!client_msg.contains("internal-url"));
    assert!(!client_msg.contains("connection refused"));
    assert!(client_msg.contains("query execution failed"));
}
```

- [ ] **Step 2: Add `client_message()` to SqeError**
```rust
impl SqeError {
    pub fn client_message(&self) -> String {
        match self {
            SqeError::QueryTimeout => "query timed out".into(),
            SqeError::RateLimited => "rate limit exceeded".into(),
            SqeError::Unauthenticated => "authentication required".into(),
            _ => "query execution failed".into(),
        }
    }
}
```

- [ ] **Step 3: Use `client_message()` in Flight response handlers**

In `do_get` and `do_put` handlers: map `SqeError` to Flight status using `client_message()`, not `Display`.

- [ ] **Step 4: Run test + full suite**

Run: `cargo test 2>&1`
Expected: all pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-coordinator/src/error.rs
git commit -m "feat(coordinator): sanitise error messages to clients in production mode"
```

---

### Task 10: Update docs

**Files:**
- Modify: `docs/book/src/architecture/auth-flow.md`
- Modify: `docs/book/src/deployment/configuration.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update auth-flow.md** — replace all "Keycloak" with "OIDC provider (e.g. Keycloak, Okta, Entra ID, Zitadel)"
- [ ] **Step 2: Update configuration.md** — document all new config sections with examples
- [ ] **Step 3: Update CLAUDE.md** — replace MinIO references, update auth naming
- [ ] **Step 4: Commit**
```bash
git add docs/ CLAUDE.md
git commit -m "docs: update auth and storage references for OSS release"
```

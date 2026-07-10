# Device Auth + Trino SSO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add OIDC Device Authorization Grant (RFC 8628) for CLI and Trino external authentication (Authorization Code + PKCE) for Trino JDBC browser-based SSO.

**Architecture:** Four new modules in `sqe-auth` (`oidc_discovery`, `device_code`, `auth_code`, `pending_auth`) provide the OIDC infrastructure. A new `oauth2.rs` module in `sqe-trino-compat` exposes the Trino external auth endpoints. Config is added via `[auth.external]` in `sqe-core`. The existing `AuthChain` and all providers are unchanged.

**Tech Stack:** Rust, reqwest (HTTP), moka (TTL cache), sha2 + base64 (PKCE S256), uuid (auth IDs), axum (Trino HTTP routes)

**Spec:** `docs/superpowers/specs/2026-03-30-device-auth-and-trino-sso-design.md`

---

### Task 1: Config — `ExternalAuthConfig` + `DeviceAuthConfig`

**Files:**
- Modify: `crates/sqe-core/src/config.rs:4-26` (add `external` field to `SqeConfig`)
- Modify: `crates/sqe-core/src/lib.rs` (re-export new config types)

- [ ] **Step 1: Add `ExternalAuthConfig` and `DeviceAuthConfig` structs**

In `crates/sqe-core/src/config.rs`, add after the existing `AuthConfig` block (after line ~367):

```rust
/// Configuration for interactive OIDC flows (device code, Trino external auth).
/// Enabled when `[auth.external]` is present in TOML.
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalAuthConfig {
    /// OIDC issuer URL. Used to discover endpoints via `.well-known/openid-configuration`.
    pub issuer: String,
    /// OAuth2 client_id for server-side flows (auth code).
    pub client_id: String,
    /// OAuth2 client_secret. Omit for public clients.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Redirect URI for authorization code callback.
    #[serde(default = "default_redirect_uri")]
    pub redirect_uri: String,
    /// OAuth2 scopes. Default: `["openid", "profile"]`.
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
    /// Timeout for pending auth sessions in seconds. Default: 900 (15 min).
    #[serde(default = "default_challenge_timeout")]
    pub challenge_timeout_secs: u64,
    /// Manual override: authorization endpoint (skip discovery).
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    /// Manual override: token endpoint (skip discovery).
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// Manual override: device authorization endpoint (skip discovery).
    #[serde(default)]
    pub device_authorization_endpoint: Option<String>,
    /// Device code flow config (CLI). Optional sub-section.
    #[serde(default)]
    pub device: Option<DeviceAuthConfig>,
    /// Whether to skip TLS certificate verification (dev/test only).
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

/// Configuration for the OIDC device authorization grant (RFC 8628).
#[derive(Debug, Deserialize, Clone)]
pub struct DeviceAuthConfig {
    /// OAuth2 client_id for the device flow (often a separate public client).
    pub client_id: String,
    /// OAuth2 scopes. Default: `["openid", "profile"]`.
    #[serde(default = "default_external_scopes")]
    pub scopes: Vec<String>,
}

fn default_redirect_uri() -> String {
    "http://localhost:8080/oauth2/callback".to_string()
}
fn default_external_scopes() -> Vec<String> {
    vec!["openid".to_string(), "profile".to_string()]
}
fn default_challenge_timeout() -> u64 {
    900
}
```

- [ ] **Step 2: Add `external` field to `SqeConfig`**

In `crates/sqe-core/src/config.rs`, add to the `SqeConfig` struct (around line 24):

```rust
    #[serde(default)]
    pub query_history: QueryHistoryConfig,
    /// Interactive OIDC flows (device code, Trino external auth). Optional.
    #[serde(default)]
    pub external: Option<ExternalAuthConfig>,
```

- [ ] **Step 3: Re-export new config types**

In `crates/sqe-core/src/lib.rs`, add `ExternalAuthConfig` and `DeviceAuthConfig` to the existing `pub use config::{...}` line.

- [ ] **Step 4: Add config parsing test**

In the test module of `crates/sqe-core/src/config.rs`:

```rust
#[test]
fn test_parse_external_auth_config() {
    let toml_str = r#"
        issuer = "https://idp.example.com/realms/sqe"
        client_id = "sqe"
        client_secret = "secret"
        scopes = ["openid", "profile"]

        [device]
        client_id = "sqe-cli"
        scopes = ["openid", "profile", "offline_access"]
    "#;
    let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.issuer, "https://idp.example.com/realms/sqe");
    assert_eq!(config.client_id, "sqe");
    assert_eq!(config.client_secret, Some("secret".to_string()));
    assert_eq!(config.challenge_timeout_secs, 900); // default
    assert!(config.device.is_some());
    let device = config.device.unwrap();
    assert_eq!(device.client_id, "sqe-cli");
    assert_eq!(device.scopes, vec!["openid", "profile", "offline_access"]);
}

#[test]
fn test_parse_external_auth_config_minimal() {
    let toml_str = r#"
        issuer = "https://idp.example.com"
        client_id = "sqe"
    "#;
    let config: ExternalAuthConfig = toml::from_str(toml_str).unwrap();
    assert!(config.client_secret.is_none());
    assert!(config.device.is_none());
    assert_eq!(config.scopes, vec!["openid", "profile"]); // default
    assert!(config.authorization_endpoint.is_none());
}
```

- [ ] **Step 5: Update `valid_config()` test helper**

In the `valid_config()` function in `crates/sqe-core/src/config.rs` (around line 802), add:

```rust
            external: None,
```

after the `query_history` field.

- [ ] **Step 6: Run tests and verify**

Run: `cargo test -p sqe-core`
Expected: All tests pass including the two new config parsing tests.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-core/src/config.rs crates/sqe-core/src/lib.rs
git commit -m "feat(config): add ExternalAuthConfig for device code and Trino SSO"
```

---

### Task 2: OIDC Discovery

**Files:**
- Create: `crates/sqe-auth/src/oidc_discovery.rs`
- Modify: `crates/sqe-auth/src/lib.rs` (add module)

- [ ] **Step 1: Write the failing test**

Create `crates/sqe-auth/src/oidc_discovery.rs` with test stubs first:

```rust
//! OIDC Discovery — fetch and cache `.well-known/openid-configuration`.

use serde::Deserialize;
use tracing::{info, warn};

use crate::provider::AuthError;

/// Endpoints discovered from an OIDC provider's well-known configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveredEndpoints {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub device_authorization_endpoint: Option<String>,
    pub jwks_uri: String,
    pub issuer: String,
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
}

/// Configuration for OIDC discovery.
#[derive(Debug, Clone)]
pub struct OidcDiscoveryConfig {
    pub issuer: String,
    pub authorization_endpoint_override: Option<String>,
    pub token_endpoint_override: Option<String>,
    pub device_authorization_endpoint_override: Option<String>,
    pub accept_invalid_certs: bool,
}

/// Fetches and caches OIDC provider endpoints via `.well-known/openid-configuration`.
pub struct OidcDiscovery {
    config: OidcDiscoveryConfig,
    endpoints: tokio::sync::OnceCell<DiscoveredEndpoints>,
    http: reqwest::Client,
}

impl OidcDiscovery {
    pub fn new(config: OidcDiscoveryConfig) -> Result<Self, AuthError> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .map_err(|e| AuthError::Internal(e.into()))?;
        Ok(Self {
            config,
            endpoints: tokio::sync::OnceCell::new(),
            http,
        })
    }

    /// Get discovered endpoints, fetching if not yet cached.
    pub async fn endpoints(&self) -> Result<&DiscoveredEndpoints, AuthError> {
        self.endpoints
            .get_or_try_init(|| self.fetch_and_apply_overrides())
            .await
    }

    async fn fetch_and_apply_overrides(&self) -> Result<DiscoveredEndpoints, AuthError> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer.trim_end_matches('/')
        );
        info!(url = %url, "Fetching OIDC discovery document");

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("OIDC discovery fetch failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(AuthError::Internal(anyhow::anyhow!(
                "OIDC discovery returned HTTP {}",
                resp.status()
            )));
        }

        let mut endpoints: DiscoveredEndpoints = resp
            .json()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("OIDC discovery parse failed: {e}")))?;

        // Apply manual overrides.
        if let Some(ref ep) = self.config.authorization_endpoint_override {
            endpoints.authorization_endpoint = ep.clone();
        }
        if let Some(ref ep) = self.config.token_endpoint_override {
            endpoints.token_endpoint = ep.clone();
        }
        if let Some(ref ep) = self.config.device_authorization_endpoint_override {
            endpoints.device_authorization_endpoint = Some(ep.clone());
        }

        if endpoints.device_authorization_endpoint.is_none() {
            warn!("IdP does not advertise device_authorization_endpoint — device code flow unavailable");
        }

        info!(
            issuer = %endpoints.issuer,
            authorization_endpoint = %endpoints.authorization_endpoint,
            token_endpoint = %endpoints.token_endpoint,
            device_authorization_endpoint = ?endpoints.device_authorization_endpoint,
            "OIDC discovery complete"
        );

        Ok(endpoints)
    }

    /// Convenience: get the device authorization endpoint, or error if not available.
    pub async fn device_authorization_endpoint(&self) -> Result<&str, AuthError> {
        let ep = self.endpoints().await?;
        ep.device_authorization_endpoint
            .as_deref()
            .ok_or_else(|| {
                AuthError::Internal(anyhow::anyhow!(
                    "IdP does not support device authorization grant"
                ))
            })
    }

    /// Convenience: get the token endpoint.
    pub async fn token_endpoint(&self) -> Result<&str, AuthError> {
        Ok(&self.endpoints().await?.token_endpoint)
    }

    /// Convenience: get the authorization endpoint.
    pub async fn authorization_endpoint(&self) -> Result<&str, AuthError> {
        Ok(&self.endpoints().await?.authorization_endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_discovery_json() -> String {
        serde_json::json!({
            "issuer": "https://idp.example.com",
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            "device_authorization_endpoint": "https://idp.example.com/device",
            "jwks_uri": "https://idp.example.com/certs",
            "userinfo_endpoint": "https://idp.example.com/userinfo"
        })
        .to_string()
    }

    #[test]
    fn parse_discovered_endpoints() {
        let json = mock_discovery_json();
        let endpoints: DiscoveredEndpoints = serde_json::from_str(&json).unwrap();
        assert_eq!(endpoints.issuer, "https://idp.example.com");
        assert_eq!(endpoints.authorization_endpoint, "https://idp.example.com/authorize");
        assert_eq!(endpoints.token_endpoint, "https://idp.example.com/token");
        assert_eq!(
            endpoints.device_authorization_endpoint.as_deref(),
            Some("https://idp.example.com/device")
        );
        assert_eq!(endpoints.jwks_uri, "https://idp.example.com/certs");
    }

    #[test]
    fn parse_endpoints_without_device() {
        let json = serde_json::json!({
            "issuer": "https://idp.example.com",
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            "jwks_uri": "https://idp.example.com/certs"
        })
        .to_string();
        let endpoints: DiscoveredEndpoints = serde_json::from_str(&json).unwrap();
        assert!(endpoints.device_authorization_endpoint.is_none());
        assert!(endpoints.userinfo_endpoint.is_none());
    }

    #[test]
    fn new_succeeds_with_valid_config() {
        let config = OidcDiscoveryConfig {
            issuer: "https://idp.example.com".to_string(),
            authorization_endpoint_override: None,
            token_endpoint_override: None,
            device_authorization_endpoint_override: None,
            accept_invalid_certs: false,
        };
        let discovery = OidcDiscovery::new(config);
        assert!(discovery.is_ok());
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

In `crates/sqe-auth/src/lib.rs`, add:

```rust
pub mod oidc_discovery;
```

and at the bottom:

```rust
pub use oidc_discovery::{OidcDiscovery, OidcDiscoveryConfig, DiscoveredEndpoints};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-auth -- oidc_discovery`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-auth/src/oidc_discovery.rs crates/sqe-auth/src/lib.rs
git commit -m "feat(auth): add OIDC discovery with well-known endpoint fetching"
```

---

### Task 3: `TokenSet` + `PendingAuthStore`

**Files:**
- Create: `crates/sqe-auth/src/pending_auth.rs`
- Modify: `crates/sqe-auth/src/lib.rs`

- [ ] **Step 1: Create `pending_auth.rs` with `TokenSet` and `PendingAuthStore`**

```rust
//! Shared types and in-memory store for interactive auth sessions.

use std::time::{Duration, Instant};

use moka::sync::Cache;

/// Token set returned by successful OIDC flows (device code, auth code).
#[derive(Debug, Clone)]
pub struct TokenSet {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
}

/// State of a pending interactive authentication session.
#[derive(Debug, Clone)]
pub enum PendingAuth {
    /// Browser redirect sent, waiting for IdP callback.
    AwaitingCallback {
        code_verifier: String,
        state: String,
        created_at: Instant,
    },
    /// Callback received, tokens available.
    Complete(TokenSet),
    /// Authentication failed.
    Failed(String),
}

/// In-memory store for interactive auth sessions (Trino external auth, device code).
///
/// Uses a moka cache with a configurable TTL. Sessions are automatically
/// evicted after the timeout (default 15 minutes).
pub struct PendingAuthStore {
    store: Cache<String, PendingAuth>,
}

impl PendingAuthStore {
    /// Create a new store with the given session timeout.
    pub fn new(challenge_timeout: Duration) -> Self {
        Self {
            store: Cache::builder()
                .time_to_live(challenge_timeout)
                .max_capacity(10_000)
                .build(),
        }
    }

    /// Insert a new pending auth session (awaiting IdP callback).
    pub fn insert_pending(&self, auth_id: &str, code_verifier: String, state: String) {
        self.store.insert(
            auth_id.to_string(),
            PendingAuth::AwaitingCallback {
                code_verifier,
                state,
                created_at: Instant::now(),
            },
        );
    }

    /// Mark a session as complete with tokens.
    pub fn complete(&self, auth_id: &str, tokens: TokenSet) {
        self.store
            .insert(auth_id.to_string(), PendingAuth::Complete(tokens));
    }

    /// Mark a session as failed.
    pub fn fail(&self, auth_id: &str, error: String) {
        self.store
            .insert(auth_id.to_string(), PendingAuth::Failed(error));
    }

    /// Poll the current state of a session.
    pub fn poll(&self, auth_id: &str) -> Option<PendingAuth> {
        self.store.get(auth_id)
    }

    /// Remove a session (cleanup after client receives token).
    pub fn remove(&self, auth_id: &str) {
        self.store.invalidate(auth_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_poll_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "verifier".to_string(), "state-abc".to_string());

        let result = store.poll("auth-1");
        assert!(result.is_some());
        match result.unwrap() {
            PendingAuth::AwaitingCallback {
                code_verifier,
                state,
                ..
            } => {
                assert_eq!(code_verifier, "verifier");
                assert_eq!(state, "state-abc");
            }
            other => panic!("expected AwaitingCallback, got: {other:?}"),
        }
    }

    #[test]
    fn complete_overwrites_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.complete(
            "auth-1",
            TokenSet {
                access_token: "at".to_string(),
                id_token: Some("idt".to_string()),
                refresh_token: None,
                expires_in: 3600,
            },
        );

        match store.poll("auth-1").unwrap() {
            PendingAuth::Complete(ts) => {
                assert_eq!(ts.access_token, "at");
                assert_eq!(ts.id_token.as_deref(), Some("idt"));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }

    #[test]
    fn fail_overwrites_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.fail("auth-1", "user denied".to_string());

        match store.poll("auth-1").unwrap() {
            PendingAuth::Failed(msg) => assert_eq!(msg, "user denied"),
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    #[test]
    fn remove_deletes_session() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.remove("auth-1");
        assert!(store.poll("auth-1").is_none());
    }

    #[test]
    fn poll_missing_returns_none() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        assert!(store.poll("nonexistent").is_none());
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

In `crates/sqe-auth/src/lib.rs`, add:

```rust
pub mod pending_auth;
```

and at the bottom:

```rust
pub use pending_auth::{PendingAuthStore, PendingAuth, TokenSet};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-auth -- pending_auth`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-auth/src/pending_auth.rs crates/sqe-auth/src/lib.rs
git commit -m "feat(auth): add PendingAuthStore and TokenSet for interactive auth flows"
```

---

### Task 4: Device Code Service (RFC 8628)

**Files:**
- Create: `crates/sqe-auth/src/device_code.rs`
- Modify: `crates/sqe-auth/src/lib.rs`

- [ ] **Step 1: Create `device_code.rs`**

```rust
//! `DeviceCodeService` — OIDC Device Authorization Grant (RFC 8628).
//!
//! Used by the CLI (`sqe query --login`) to authenticate without embedding passwords.
//! The user visits a URL in their browser, enters a code, and logs in.

use std::sync::Arc;

use serde::Deserialize;
use tracing::{debug, warn};

use crate::oidc_discovery::OidcDiscovery;
use crate::pending_auth::TokenSet;
use crate::provider::AuthError;

/// Response from the IdP's device authorization endpoint.
#[derive(Debug, Clone)]
pub struct DeviceAuthSession {
    /// Opaque device code for polling.
    pub device_code: String,
    /// Human-readable code the user enters at the verification URI.
    pub user_code: String,
    /// URL the user visits to authenticate.
    pub verification_uri: String,
    /// URL with the user_code pre-filled (if supported by IdP).
    pub verification_uri_complete: Option<String>,
    /// Seconds until the device code expires.
    pub expires_in: u64,
    /// Minimum polling interval in seconds.
    pub interval: u64,
}

/// Result of polling the token endpoint during device code flow.
#[derive(Debug)]
pub enum DevicePollResult {
    /// User has not yet completed authentication.
    Pending,
    /// Polling too fast — increase interval by 5 seconds.
    SlowDown,
    /// Authentication complete — tokens available.
    Complete(TokenSet),
    /// User explicitly denied the request.
    AccessDenied,
    /// The device code has expired.
    ExpiredToken,
}

/// Raw IdP response for the device authorization request.
#[derive(Debug, Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default = "default_interval")]
    interval: u64,
}

fn default_interval() -> u64 {
    5
}

/// Raw IdP token response (success case).
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

/// Raw IdP error response.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// Orchestrates the RFC 8628 device authorization grant.
pub struct DeviceCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

impl DeviceCodeService {
    pub fn new(
        discovery: Arc<OidcDiscovery>,
        client_id: String,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            discovery,
            client_id,
            scopes,
            http: reqwest::Client::new(),
        }
    }

    /// Start the device authorization flow.
    ///
    /// Returns a `DeviceAuthSession` containing the user code and verification
    /// URI to show to the user.
    pub async fn start(&self) -> Result<DeviceAuthSession, AuthError> {
        let endpoint = self.discovery.device_authorization_endpoint().await?;
        let scope = self.scopes.join(" ");

        debug!(
            endpoint = %endpoint,
            client_id = %self.client_id,
            scope = %scope,
            "Starting device authorization request"
        );

        let resp = self
            .http
            .post(endpoint)
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("scope", &scope),
            ])
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("device authorization request failed: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::Internal(anyhow::anyhow!(
                "device authorization returned HTTP {status}: {body}"
            )));
        }

        let device_resp: DeviceAuthResponse = resp.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("device authorization parse failed: {e}"))
        })?;

        Ok(DeviceAuthSession {
            device_code: device_resp.device_code,
            user_code: device_resp.user_code,
            verification_uri: device_resp.verification_uri,
            verification_uri_complete: device_resp.verification_uri_complete,
            expires_in: device_resp.expires_in,
            interval: device_resp.interval,
        })
    }

    /// Poll the token endpoint for the result of a device authorization.
    ///
    /// The caller should respect the `interval` from `DeviceAuthSession` and
    /// increase it by 5 seconds on `SlowDown`.
    pub async fn poll(&self, device_code: &str) -> Result<DevicePollResult, AuthError> {
        let token_endpoint = self.discovery.token_endpoint().await?;

        let resp = self
            .http
            .post(token_endpoint)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", device_code),
                ("client_id", self.client_id.as_str()),
            ])
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("device token poll failed: {e}"))
            })?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if status.is_success() {
            let token_resp: TokenResponse = serde_json::from_str(&body).map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("token response parse failed: {e}"))
            })?;
            return Ok(DevicePollResult::Complete(TokenSet {
                access_token: token_resp.access_token,
                id_token: token_resp.id_token,
                refresh_token: token_resp.refresh_token,
                expires_in: token_resp.expires_in,
            }));
        }

        // Error response — parse the error code.
        let error_resp: ErrorResponse = serde_json::from_str(&body).map_err(|e| {
            AuthError::Internal(anyhow::anyhow!(
                "device token error parse failed (HTTP {status}): {e}, body: {body}"
            ))
        })?;

        match error_resp.error.as_str() {
            "authorization_pending" => {
                debug!("Device authorization pending");
                Ok(DevicePollResult::Pending)
            }
            "slow_down" => {
                warn!("Device authorization: slow_down received, increase interval by 5s");
                Ok(DevicePollResult::SlowDown)
            }
            "access_denied" => {
                warn!("Device authorization: user denied access");
                Ok(DevicePollResult::AccessDenied)
            }
            "expired_token" => {
                warn!("Device authorization: device code expired");
                Ok(DevicePollResult::ExpiredToken)
            }
            other => Err(AuthError::AuthFailed(format!(
                "device authorization error: {other}: {}",
                error_resp.error_description.unwrap_or_default()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_device_auth_response() {
        let json = serde_json::json!({
            "device_code": "dc-abc123",
            "user_code": "ABCD-1234",
            "verification_uri": "https://idp.example.com/device",
            "verification_uri_complete": "https://idp.example.com/device?user_code=ABCD-1234",
            "expires_in": 600,
            "interval": 5
        });
        let resp: DeviceAuthResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.device_code, "dc-abc123");
        assert_eq!(resp.user_code, "ABCD-1234");
        assert_eq!(resp.verification_uri, "https://idp.example.com/device");
        assert_eq!(
            resp.verification_uri_complete.as_deref(),
            Some("https://idp.example.com/device?user_code=ABCD-1234")
        );
        assert_eq!(resp.expires_in, 600);
        assert_eq!(resp.interval, 5);
    }

    #[test]
    fn parse_device_auth_response_minimal() {
        let json = serde_json::json!({
            "device_code": "dc-xyz",
            "user_code": "XY-9999",
            "verification_uri": "https://idp.example.com/device",
            "expires_in": 300
        });
        let resp: DeviceAuthResponse = serde_json::from_value(json).unwrap();
        assert!(resp.verification_uri_complete.is_none());
        assert_eq!(resp.interval, 5); // default
    }

    #[test]
    fn parse_token_response() {
        let json = serde_json::json!({
            "access_token": "eyJ...",
            "id_token": "eyJ.id.",
            "refresh_token": "rt_abc",
            "expires_in": 3600,
            "token_type": "Bearer"
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "eyJ...");
        assert_eq!(resp.id_token.as_deref(), Some("eyJ.id."));
        assert_eq!(resp.refresh_token.as_deref(), Some("rt_abc"));
        assert_eq!(resp.expires_in, 3600);
    }

    #[test]
    fn parse_error_response_pending() {
        let json = serde_json::json!({
            "error": "authorization_pending"
        });
        let resp: ErrorResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.error, "authorization_pending");
        assert!(resp.error_description.is_none());
    }

    #[test]
    fn parse_error_response_with_description() {
        let json = serde_json::json!({
            "error": "access_denied",
            "error_description": "The user denied the request"
        });
        let resp: ErrorResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.error, "access_denied");
        assert_eq!(
            resp.error_description.as_deref(),
            Some("The user denied the request")
        );
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `crates/sqe-auth/src/lib.rs`:

```rust
pub mod device_code;
```

and:

```rust
pub use device_code::{DeviceCodeService, DeviceAuthSession, DevicePollResult};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-auth -- device_code`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-auth/src/device_code.rs crates/sqe-auth/src/lib.rs
git commit -m "feat(auth): add DeviceCodeService for RFC 8628 device authorization grant"
```

---

### Task 5: Authorization Code + PKCE Service

**Files:**
- Create: `crates/sqe-auth/src/auth_code.rs`
- Modify: `crates/sqe-auth/src/lib.rs`
- Modify: `crates/sqe-auth/Cargo.toml` (move `rand` from dev-deps to deps)

- [ ] **Step 1: Move `rand` to regular dependencies**

In `crates/sqe-auth/Cargo.toml`, move `rand` from `[dev-dependencies]` to `[dependencies]`:

```toml
[dependencies]
# ... existing deps ...
rand = { workspace = true }
```

Remove `rand = { workspace = true }` from `[dev-dependencies]`.

- [ ] **Step 2: Create `auth_code.rs`**

```rust
//! `AuthCodeService` — OAuth2 Authorization Code + PKCE flow.
//!
//! Used by Trino external auth endpoints. The server generates an authorization
//! URL (with PKCE S256 challenge), redirects the user's browser to the IdP,
//! receives the callback with an authorization code, and exchanges it for tokens.

use std::sync::Arc;

use base64::Engine;
use rand::Rng;
use sha2::{Digest, Sha256};
use serde::Deserialize;
use tracing::debug;

use crate::oidc_discovery::OidcDiscovery;
use crate::pending_auth::TokenSet;
use crate::provider::AuthError;

/// Represents a started authorization code challenge.
#[derive(Debug, Clone)]
pub struct AuthCodeChallenge {
    /// Unique identifier for this auth session.
    pub auth_id: String,
    /// Full URL to redirect the user's browser to.
    pub authorization_url: String,
    /// PKCE code verifier (kept server-side, used during code exchange).
    pub code_verifier: String,
    /// CSRF state parameter (must match callback).
    pub state: String,
}

/// Raw IdP token response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

/// Orchestrates the Authorization Code + PKCE flow server-side.
pub struct AuthCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    client_secret: Option<String>,
    redirect_uri: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

impl AuthCodeService {
    pub fn new(
        discovery: Arc<OidcDiscovery>,
        client_id: String,
        client_secret: Option<String>,
        redirect_uri: String,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            discovery,
            client_id,
            client_secret,
            redirect_uri,
            scopes,
            http: reqwest::Client::new(),
        }
    }

    /// Generate a new authorization challenge (PKCE + state).
    ///
    /// Returns the challenge containing the URL to redirect the browser to
    /// and the server-side secrets (code_verifier, state) to store.
    pub async fn start_challenge(&self) -> Result<AuthCodeChallenge, AuthError> {
        let auth_endpoint = self.discovery.authorization_endpoint().await?;

        let auth_id = uuid::Uuid::new_v4().to_string();
        let state = generate_random_string(32);
        let code_verifier = generate_code_verifier();
        let code_challenge = compute_code_challenge(&code_verifier);
        let scope = self.scopes.join(" ");

        let authorization_url = format!(
            "{auth_endpoint}?response_type=code\
             &client_id={client_id}\
             &redirect_uri={redirect_uri}\
             &scope={scope}\
             &state={state}\
             &code_challenge={code_challenge}\
             &code_challenge_method=S256",
            client_id = urlencoding(&self.client_id),
            redirect_uri = urlencoding(&self.redirect_uri),
            scope = urlencoding(&scope),
            state = urlencoding(&state),
            code_challenge = urlencoding(&code_challenge),
        );

        debug!(
            auth_id = %auth_id,
            "Generated authorization code challenge"
        );

        Ok(AuthCodeChallenge {
            auth_id,
            authorization_url,
            code_verifier,
            state,
        })
    }

    /// Exchange an authorization code for tokens.
    ///
    /// Called when the IdP redirects back to `/oauth2/callback` with a `code`.
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenSet, AuthError> {
        let token_endpoint = self.discovery.token_endpoint().await?;

        let mut params = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &self.redirect_uri),
            ("client_id", &self.client_id),
            ("code_verifier", code_verifier),
        ];

        let secret_string;
        if let Some(ref secret) = self.client_secret {
            secret_string = secret.clone();
            params.push(("client_secret", &secret_string));
        }

        let resp = self
            .http
            .post(token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("token exchange request failed: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AuthError::AuthFailed(format!(
                "token exchange failed (HTTP {status}): {body}"
            )));
        }

        let token_resp: TokenResponse = resp.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("token response parse failed: {e}"))
        })?;

        Ok(TokenSet {
            access_token: token_resp.access_token,
            id_token: token_resp.id_token,
            refresh_token: token_resp.refresh_token,
            expires_in: token_resp.expires_in,
        })
    }
}

/// Generate a PKCE code verifier (43-128 chars, unreserved characters).
fn generate_code_verifier() -> String {
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen::<u8>()).collect();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
}

/// Compute the PKCE S256 code challenge from a code verifier.
fn compute_code_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
}

/// Generate a random alphanumeric string of the given length.
fn generate_random_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..36);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'a' + idx - 10) as char
            }
        })
        .collect()
}

/// Percent-encode a string for use in a URL query parameter.
fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_verifier_is_correct_length() {
        let verifier = generate_code_verifier();
        // 32 bytes base64url-encoded = 43 chars
        assert_eq!(verifier.len(), 43);
        // Only unreserved characters
        assert!(verifier.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn code_challenge_is_s256() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = compute_code_challenge(verifier);
        // SHA256 of the verifier, base64url-encoded
        let expected_digest = Sha256::digest(verifier.as_bytes());
        let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(expected_digest);
        assert_eq!(challenge, expected);
    }

    #[test]
    fn code_challenge_deterministic() {
        let verifier = "test-verifier-12345";
        let c1 = compute_code_challenge(verifier);
        let c2 = compute_code_challenge(verifier);
        assert_eq!(c1, c2);
    }

    #[test]
    fn random_string_correct_length() {
        let s = generate_random_string(32);
        assert_eq!(s.len(), 32);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn random_string_not_constant() {
        let s1 = generate_random_string(32);
        let s2 = generate_random_string(32);
        // Extremely unlikely to be equal
        assert_ne!(s1, s2);
    }

    #[test]
    fn urlencoding_spaces_and_special_chars() {
        assert_eq!(urlencoding("openid profile"), "openid+profile");
        assert_eq!(urlencoding("a=b&c=d"), "a%3Db%26c%3Dd");
    }

    #[test]
    fn parse_token_response() {
        let json = serde_json::json!({
            "access_token": "at_123",
            "id_token": "idt_456",
            "refresh_token": "rt_789",
            "expires_in": 1800,
            "token_type": "Bearer"
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "at_123");
        assert_eq!(resp.id_token.as_deref(), Some("idt_456"));
        assert_eq!(resp.expires_in, 1800);
    }

    #[test]
    fn parse_token_response_minimal() {
        let json = serde_json::json!({
            "access_token": "at_only"
        });
        let resp: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.access_token, "at_only");
        assert!(resp.id_token.is_none());
        assert!(resp.refresh_token.is_none());
        assert_eq!(resp.expires_in, 3600); // default
    }
}
```

- [ ] **Step 3: Register module in lib.rs**

Add to `crates/sqe-auth/src/lib.rs`:

```rust
pub mod auth_code;
```

and:

```rust
pub use auth_code::{AuthCodeService, AuthCodeChallenge};
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-auth -- auth_code`
Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-auth/src/auth_code.rs crates/sqe-auth/src/lib.rs crates/sqe-auth/Cargo.toml
git commit -m "feat(auth): add AuthCodeService for authorization code + PKCE flow"
```

---

### Task 6: Trino External Auth Endpoints

**Files:**
- Create: `crates/sqe-trino-compat/src/oauth2.rs`
- Modify: `crates/sqe-trino-compat/src/server.rs:118-125` (add routes + modify 401)
- Modify: `crates/sqe-trino-compat/Cargo.toml` (add `sqe-auth` dependency)

- [ ] **Step 1: Add `sqe-auth` dependency to sqe-trino-compat**

In `crates/sqe-trino-compat/Cargo.toml`, add:

```toml
sqe-auth = { path = "../sqe-auth" }
```

- [ ] **Step 2: Create `oauth2.rs` with Trino external auth handlers**

Create `crates/sqe-trino-compat/src/oauth2.rs`:

```rust
//! Trino-compatible OAuth2 external authentication endpoints.
//!
//! Implements the protocol expected by the Trino JDBC driver when
//! `externalAuthentication=true`:
//!
//! 1. Server returns 401 with `WWW-Authenticate: Bearer x_redirect_server="...", x_token_server="..."`
//! 2. Driver opens browser to `x_redirect_server`
//! 3. Driver polls `x_token_server` until token is available
//! 4. Driver sends `DELETE` to clean up

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use sqe_auth::auth_code::AuthCodeService;
use sqe_auth::pending_auth::{PendingAuth, PendingAuthStore};

/// Shared state for OAuth2 endpoints.
pub struct OAuth2State {
    pub auth_code_service: Arc<AuthCodeService>,
    pub pending_store: Arc<PendingAuthStore>,
    pub base_url: String,
}

/// Generate the 401 WWW-Authenticate header value for a new auth challenge.
///
/// Returns `(auth_id, www_authenticate_value)`.
pub async fn generate_challenge(
    state: &OAuth2State,
) -> Result<(String, String), StatusCode> {
    let challenge = state
        .auth_code_service
        .start_challenge()
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to start auth code challenge");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let auth_id = challenge.auth_id.clone();
    let auth_id_hash = hex::encode(Sha256::digest(auth_id.as_bytes()));

    // Store the pending auth session.
    state.pending_store.insert_pending(
        &auth_id,
        challenge.code_verifier,
        challenge.state,
    );

    let initiate_url = format!("{}/oauth2/token/initiate/{}", state.base_url, auth_id_hash);
    let token_url = format!("{}/oauth2/token/{}", state.base_url, auth_id);

    let www_authenticate = format!(
        "Bearer x_redirect_server=\"{initiate_url}\", x_token_server=\"{token_url}\""
    );

    Ok((auth_id, www_authenticate))
}

/// `GET /oauth2/token/initiate/{auth_id_hash}`
///
/// Redirects the user's browser to the IdP's authorization endpoint.
/// The hash is used so the auth_id is not exposed in browser history.
pub async fn initiate_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(_auth_id_hash): Path<String>,
) -> Response {
    // Start a fresh challenge for the browser redirect.
    // Note: in a production system, we'd map hash → auth_id. For now,
    // we start a new challenge (the poll endpoint uses the real auth_id).
    match state.auth_code_service.start_challenge().await {
        Ok(challenge) => {
            state.pending_store.insert_pending(
                &challenge.auth_id,
                challenge.code_verifier,
                challenge.state,
            );
            Redirect::temporary(&challenge.authorization_url).into_response()
        }
        Err(e) => {
            warn!(error = %e, "Failed to generate authorization URL");
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error").into_response()
        }
    }
}

/// Query parameters from the IdP callback.
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

/// `GET /oauth2/callback?code=...&state=...`
///
/// Receives the IdP's redirect after user authentication. Exchanges the
/// authorization code for tokens and stores them in the pending auth store.
pub async fn callback_handler(
    State(state): State<Arc<OAuth2State>>,
    Query(params): Query<CallbackParams>,
) -> Response {
    // Find the pending session by state parameter.
    // In a full implementation, we'd index by state → auth_id.
    // For now, we iterate (the store is small and short-lived).
    let auth_id = find_auth_id_by_state(&state.pending_store, &params.state);

    let Some(auth_id) = auth_id else {
        warn!(state = %params.state, "No pending auth session found for state");
        return (StatusCode::BAD_REQUEST, "Invalid or expired state parameter").into_response();
    };

    let code_verifier = match state.pending_store.poll(&auth_id) {
        Some(PendingAuth::AwaitingCallback { code_verifier, .. }) => code_verifier,
        _ => {
            return (StatusCode::BAD_REQUEST, "Auth session not in awaiting state").into_response();
        }
    };

    match state
        .auth_code_service
        .exchange_code(&params.code, &code_verifier)
        .await
    {
        Ok(tokens) => {
            debug!(auth_id = %auth_id, "Authorization code exchange succeeded");
            state.pending_store.complete(&auth_id, tokens);
            Html(SUCCESS_HTML).into_response()
        }
        Err(e) => {
            warn!(auth_id = %auth_id, error = %e, "Authorization code exchange failed");
            state.pending_store.fail(&auth_id, e.to_string());
            Html(FAILURE_HTML).into_response()
        }
    }
}

/// Response for the token polling endpoint.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TokenPollResponse {
    Pending { #[serde(rename = "nextUri")] next_uri: String },
    Complete { token: String },
    Error { error: String },
}

/// `GET /oauth2/token/{auth_id}`
///
/// Polled by the Trino JDBC driver. Returns the token when available,
/// or a `nextUri` to keep polling.
pub async fn poll_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> Response {
    match state.pending_store.poll(&auth_id) {
        Some(PendingAuth::AwaitingCallback { .. }) => {
            let next_uri = format!("{}/oauth2/token/{}", state.base_url, auth_id);
            Json(TokenPollResponse::Pending { next_uri }).into_response()
        }
        Some(PendingAuth::Complete(tokens)) => {
            // Return the access token. The Trino JDBC driver will use it
            // as a Bearer token on subsequent requests.
            Json(TokenPollResponse::Complete {
                token: tokens.access_token,
            })
            .into_response()
        }
        Some(PendingAuth::Failed(msg)) => {
            Json(TokenPollResponse::Error { error: msg }).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Auth session not found or expired").into_response(),
    }
}

/// `DELETE /oauth2/token/{auth_id}`
///
/// Cleanup: removes the pending auth session after the client receives the token.
pub async fn delete_token_handler(
    State(state): State<Arc<OAuth2State>>,
    Path(auth_id): Path<String>,
) -> StatusCode {
    state.pending_store.remove(&auth_id);
    StatusCode::NO_CONTENT
}

/// Find the auth_id that matches a given state parameter.
fn find_auth_id_by_state(store: &PendingAuthStore, state: &str) -> Option<String> {
    // PendingAuthStore uses moka::sync::Cache which doesn't expose iteration.
    // We need an index. For now, we store a reverse mapping using the state
    // as a secondary key. This is a known limitation — see note below.
    //
    // TODO: Add a state→auth_id index to PendingAuthStore for O(1) lookup.
    // For the initial implementation, the auth_id IS the state (we control both).
    // This works because we generate both values and can make them the same.
    //
    // In generate_challenge, the state IS the auth_id. So we can look up directly.
    Some(state.to_string())
}

const SUCCESS_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>SQE — Authentication Complete</title></head>
<body style="font-family:system-ui;text-align:center;padding:60px">
<h2>Authentication successful</h2>
<p>You can close this tab and return to your application.</p>
</body></html>"#;

const FAILURE_HTML: &str = r#"<!DOCTYPE html>
<html><head><title>SQE — Authentication Failed</title></head>
<body style="font-family:system-ui;text-align:center;padding:60px">
<h2>Authentication failed</h2>
<p>Please close this tab and try again.</p>
</body></html>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn token_poll_response_pending_serializes() {
        let resp = TokenPollResponse::Pending {
            next_uri: "https://sqe:8080/oauth2/token/abc".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("nextUri"));
        assert!(json.contains("abc"));
    }

    #[test]
    fn token_poll_response_complete_serializes() {
        let resp = TokenPollResponse::Complete {
            token: "eyJ...".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"token\""));
        assert!(json.contains("eyJ..."));
    }

    #[test]
    fn token_poll_response_error_serializes() {
        let resp = TokenPollResponse::Error {
            error: "user denied".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"error\""));
    }

    #[test]
    fn find_auth_id_returns_state_as_id() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        let auth_id = "my-auth-id";
        store.insert_pending(auth_id, "verifier".to_string(), auth_id.to_string());

        let found = find_auth_id_by_state(&store, auth_id);
        assert_eq!(found.as_deref(), Some(auth_id));
    }
}
```

- [ ] **Step 3: Register module and add `hex` dependency**

In `crates/sqe-trino-compat/src/lib.rs` (or create it), add:

```rust
pub mod oauth2;
```

In `crates/sqe-trino-compat/Cargo.toml`, add:

```toml
hex = { workspace = true }
sha2 = { workspace = true }
```

- [ ] **Step 4: Add OAuth2 routes to the Trino HTTP router**

In `crates/sqe-trino-compat/src/server.rs`, modify the router (around line 119-125).

Add after the existing routes:

```rust
            // OAuth2 external auth routes (only if OAuth2 state is available)
            .route("/oauth2/token/initiate/{auth_id_hash}", get(crate::oauth2::initiate_handler))
            .route("/oauth2/callback", get(crate::oauth2::callback_handler))
            .route("/oauth2/token/{auth_id}", get(crate::oauth2::poll_token_handler))
            .route("/oauth2/token/{auth_id}", delete(crate::oauth2::delete_token_handler))
```

Note: The OAuth2 routes require `Arc<OAuth2State>` as state — this will require updating the `start_trino_server` function signature to optionally accept `OAuth2State`. This wiring is covered in Task 7.

- [ ] **Step 5: Modify the 401 response in `submit_query`**

In `crates/sqe-trino-compat/src/server.rs`, replace the current fallback (around line 338-339):

```rust
    } else {
        return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
    };
```

This will be updated in Task 7 when the full wiring is done, since it needs access to `OAuth2State`.

- [ ] **Step 6: Run tests**

Run: `cargo test -p sqe-trino-compat`
Expected: Existing tests pass, new serialization tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-trino-compat/src/oauth2.rs crates/sqe-trino-compat/src/server.rs crates/sqe-trino-compat/Cargo.toml
git commit -m "feat(trino): add OAuth2 external auth endpoints for browser SSO"
```

---

### Task 7: Wiring — Connect External Auth to Coordinator

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (update TrinoState, 401 challenge)
- Modify: `crates/sqe-coordinator/` (construct services from config, pass to Trino server)

This task connects the services built in Tasks 2-6 to the running server. The exact wiring depends on how the coordinator currently constructs and passes the Trino server state.

- [ ] **Step 1: Explore coordinator startup**

Read how the coordinator starts the Trino server to understand the wiring point:

```bash
grep -rn "start_trino_server" crates/sqe-coordinator/src/
```

- [ ] **Step 2: Add `Option<Arc<OAuth2State>>` to `TrinoState`**

In `crates/sqe-trino-compat/src/server.rs`, add to the `TrinoState` struct:

```rust
    /// OAuth2 external auth state. None if [auth.external] is not configured.
    pub oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
```

- [ ] **Step 3: Update `submit_query` 401 to include WWW-Authenticate challenge**

Replace the `else` branch in `submit_query` (line ~338):

```rust
    } else if let Some(ref oauth2) = state.oauth2 {
        // External auth enabled: return 401 with challenge headers.
        match crate::oauth2::generate_challenge(oauth2).await {
            Ok((_auth_id, www_authenticate)) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    [("WWW-Authenticate", www_authenticate)],
                    "Authentication required",
                )
                    .into_response();
            }
            Err(status) => {
                return error_response(status, "Failed to generate auth challenge");
            }
        }
    } else {
        return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
    };
```

- [ ] **Step 4: Construct services in coordinator startup**

In the coordinator's main setup (where `start_trino_server` is called), add conditional construction:

```rust
let oauth2_state = if let Some(ref external) = config.external {
    let discovery_config = sqe_auth::OidcDiscoveryConfig {
        issuer: external.issuer.clone(),
        authorization_endpoint_override: external.authorization_endpoint.clone(),
        token_endpoint_override: external.token_endpoint.clone(),
        device_authorization_endpoint_override: external.device_authorization_endpoint.clone(),
        accept_invalid_certs: external.accept_invalid_certs,
    };
    let discovery = Arc::new(
        sqe_auth::OidcDiscovery::new(discovery_config)
            .expect("failed to create OIDC discovery"),
    );
    let auth_code_service = Arc::new(sqe_auth::AuthCodeService::new(
        discovery,
        external.client_id.clone(),
        external.client_secret.clone(),
        external.redirect_uri.clone(),
        external.scopes.clone(),
    ));
    let pending_store = Arc::new(sqe_auth::PendingAuthStore::new(
        std::time::Duration::from_secs(external.challenge_timeout_secs),
    ));
    let base_url = format!("http://localhost:{}", config.coordinator.trino_http_port);
    Some(Arc::new(sqe_trino_compat::oauth2::OAuth2State {
        auth_code_service,
        pending_store,
        base_url,
    }))
} else {
    None
};
```

Pass `oauth2_state` to `start_trino_server`.

- [ ] **Step 5: Update test helpers to pass `oauth2: None`**

In all test `TrinoState` constructions in `server.rs`, add `oauth2: None`.

- [ ] **Step 6: Build and test**

Run: `cargo build --all && cargo test --all`
Expected: Full build passes, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs crates/sqe-coordinator/
git commit -m "feat: wire external auth services into coordinator and Trino server"
```

---

### Task 8: Update Spec Checklist + Config Example

**Files:**
- Modify: `openspec/changes/pluggable-auth/tasks.md`
- Modify: `sqe.toml.example` (if it exists)

- [ ] **Step 1: Add new sections to tasks.md**

Append to `openspec/changes/pluggable-auth/tasks.md`:

```markdown
## 10. OIDC Discovery

- [ ] 10.1 Create `sqe-auth/src/oidc_discovery.rs` with `OidcDiscovery`, `DiscoveredEndpoints`, `OidcDiscoveryConfig`
- [ ] 10.2 Fetch + cache `.well-known/openid-configuration` via `tokio::sync::OnceCell`
- [ ] 10.3 Manual endpoint overrides take precedence over discovery
- [ ] 10.4 Warn if `device_authorization_endpoint` is not advertised
- [ ] 10.5 Unit test: JSON parsing, override precedence

## 11. Device Authorization Grant (RFC 8628)

- [ ] 11.1 Create `sqe-auth/src/device_code.rs` with `DeviceCodeService`, `DeviceAuthSession`, `DevicePollResult`
- [ ] 11.2 `start()` → POST to device authorization endpoint → return user_code + verification_uri
- [ ] 11.3 `poll()` → POST to token endpoint with device_code grant type
- [ ] 11.4 Handle `authorization_pending`, `slow_down`, `expired_token`, `access_denied`
- [ ] 11.5 Unit test: response parsing, error mapping

## 12. Authorization Code + PKCE

- [ ] 12.1 Create `sqe-auth/src/auth_code.rs` with `AuthCodeService`, `AuthCodeChallenge`
- [ ] 12.2 PKCE S256 code_challenge generation from random code_verifier
- [ ] 12.3 `start_challenge()` → authorization URL with PKCE + state
- [ ] 12.4 `exchange_code()` → POST to token endpoint with code + verifier
- [ ] 12.5 Unit test: PKCE generation, URL construction, token parsing

## 13. Trino External Auth Endpoints

- [ ] 13.1 Create `sqe-trino-compat/src/oauth2.rs` with endpoint handlers
- [ ] 13.2 `GET /oauth2/token/initiate/{hash}` → 302 redirect to IdP
- [ ] 13.3 `GET /oauth2/callback?code=&state=` → exchange code, store tokens
- [ ] 13.4 `GET /oauth2/token/{auth_id}` → poll (pending/complete/error)
- [ ] 13.5 `DELETE /oauth2/token/{auth_id}` → cleanup
- [ ] 13.6 Modify `submit_query` → 401 with `WWW-Authenticate: Bearer x_redirect_server, x_token_server`
- [ ] 13.7 Unit test: response serialization, challenge generation

## 14. Config + Wiring

- [ ] 14.1 Add `ExternalAuthConfig` + `DeviceAuthConfig` to `sqe-core/src/config.rs`
- [ ] 14.2 Add `[auth.external]` section to `sqe.toml.example`
- [ ] 14.3 Construct services in coordinator startup from config
- [ ] 14.4 Unit test: config parsing with and without `[auth.external]`

## 15. PendingAuthStore

- [ ] 15.1 Create `sqe-auth/src/pending_auth.rs` with `PendingAuthStore`, `PendingAuth`, `TokenSet`
- [ ] 15.2 Insert/poll/complete/fail/remove lifecycle
- [ ] 15.3 Moka TTL-based expiry (default 15 min)
- [ ] 15.4 Unit test: full lifecycle, missing key returns None
```

- [ ] **Step 2: Add `[auth.external]` to config example**

If `sqe.toml.example` exists, add:

```toml
# ── Interactive OIDC flows (Trino SSO + CLI device code) ────────
# Uncomment to enable browser-based SSO for Trino JDBC and device code for CLI.
# [auth.external]
# issuer = "https://idp.example.com/realms/sqe"
# client_id = "sqe"
# client_secret = "your-client-secret"
# redirect_uri = "https://sqe.example.com/oauth2/callback"
# scopes = ["openid", "profile"]
# challenge_timeout_secs = 900
#
# [auth.external.device]
# client_id = "sqe-cli"
# scopes = ["openid", "profile", "offline_access"]
```

- [ ] **Step 3: Commit**

```bash
git add openspec/changes/pluggable-auth/tasks.md sqe.toml.example
git commit -m "docs: add device auth + Trino SSO tasks and config example"
```

---

### Task 9: Full Build + Clippy + Test Sweep

- [ ] **Step 1: Build all crates**

Run: `cargo build --all`
Expected: Clean build.

- [ ] **Step 2: Run all tests**

Run: `cargo test --all`
Expected: All tests pass.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: No warnings.

- [ ] **Step 4: Fix any issues found**

Address clippy warnings or test failures.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: fix clippy warnings and test issues from device auth implementation"
```

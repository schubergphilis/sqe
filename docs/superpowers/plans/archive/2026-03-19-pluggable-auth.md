# Pluggable Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardwired Keycloak ROPC auth with a pluggable `AuthProvider` trait and five implementations: OIDC password, bearer token, API key (group-based, external config file), anonymous, and mTLS.

**Architecture:** `AuthChain` (Vec of providers, first-match) wraps all providers. `SessionManager` depends only on `Arc<dyn AuthProvider>`. Each provider handles its credential type and returns an `Identity`. The OIDC password provider (renamed from Keycloak) is the default; all others are opt-in via `[[auth.providers]]` config.

**Tech Stack:** Rust, jsonwebtoken (JWT validation), reqwest (JWKS fetch), moka (JWKS cache), subtle (constant-time key compare), notify (file watch), async-trait

**Spec:** `openspec/changes/pluggable-auth/`

**Prerequisite:** `oss-security-hardening` (Keycloak rename) should be applied first, or applied in the same branch.

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `crates/sqe-auth/src/provider.rs` | create | `AuthProvider` trait, `AuthError`, `Identity`, `FlightCredentials` |
| `crates/sqe-auth/src/chain.rs` | create | `AuthChain` first-match logic |
| `crates/sqe-auth/src/oidc_password.rs` | modify | implement `AuthProvider` trait (was standalone) |
| `crates/sqe-auth/src/bearer_token.rs` | create | JWT validation + JWKS cache |
| `crates/sqe-auth/src/api_key.rs` | create | opaque key lookup + hot reload |
| `crates/sqe-auth/src/anonymous.rs` | create | fixed identity, no validation |
| `crates/sqe-auth/src/mtls.rs` | create | TLS cert CN extraction |
| `crates/sqe-core/src/config.rs` | modify | `AuthProviderConfig` enum, `[[auth.providers]]` array |
| `crates/sqe-coordinator/src/session.rs` | modify | accept `Arc<dyn AuthProvider>` |

---

### Task 1: AuthProvider trait + AuthChain

**Files:**
- Create: `crates/sqe-auth/src/provider.rs`
- Create: `crates/sqe-auth/src/chain.rs`
- Test: `crates/sqe-auth/tests/chain_test.rs`

- [ ] **Step 1: Write failing tests**
```rust
// crates/sqe-auth/tests/chain_test.rs
use sqe_auth::{AuthChain, AuthProvider, AuthError, Identity, FlightCredentials};
use async_trait::async_trait;

struct AlwaysDeclines;
#[async_trait] impl AuthProvider for AlwaysDeclines {
    async fn authenticate(&self, _: &FlightCredentials) -> Result<Identity, AuthError> {
        Err(AuthError::NotMyCredentials)
    }
}

struct AlwaysAccepts { user: &'static str }
#[async_trait] impl AuthProvider for AlwaysAccepts {
    async fn authenticate(&self, _: &FlightCredentials) -> Result<Identity, AuthError> {
        Ok(Identity::new(self.user, vec![]))
    }
}

#[tokio::test]
async fn chain_skips_declining_provider() {
    let chain = AuthChain::new(vec![
        Box::new(AlwaysDeclines),
        Box::new(AlwaysAccepts { user: "bob" }),
    ]);
    let creds = FlightCredentials::basic("bob", "pass");
    let identity = chain.authenticate(&creds).await.unwrap();
    assert_eq!(identity.user_id, "bob");
}

#[tokio::test]
async fn chain_fails_when_all_decline() {
    let chain = AuthChain::new(vec![Box::new(AlwaysDeclines)]);
    let result = chain.authenticate(&FlightCredentials::basic("x", "y")).await;
    assert!(matches!(result, Err(AuthError::AuthFailed(_))));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-auth chain_test 2>&1`
Expected: compile error

- [ ] **Step 3: Define trait and chain**
```rust
// crates/sqe-auth/src/provider.rs
use async_trait::async_trait;

pub struct FlightCredentials {
    pub username: Option<String>,
    pub password: Option<String>,  // may be a JWT
    pub bearer_header: Option<String>,
    pub peer_cert_cn: Option<String>,
}

pub struct Identity {
    pub user_id: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub catalog_token: Option<String>,
}

pub enum AuthError {
    NotMyCredentials,
    AuthFailed(String),
    Internal(anyhow::Error),
}

#[async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError>;
    async fn refresh_catalog_token(&self, identity: &Identity) -> Result<Option<String>, AuthError> {
        Ok(None)
    }
}
```

- [ ] **Step 4: Implement AuthChain**
```rust
// crates/sqe-auth/src/chain.rs
pub struct AuthChain { providers: Vec<Box<dyn AuthProvider>> }
impl AuthChain {
    pub fn new(providers: Vec<Box<dyn AuthProvider>>) -> Self { Self { providers } }
}
#[async_trait]
impl AuthProvider for AuthChain {
    async fn authenticate(&self, creds: &FlightCredentials) -> Result<Identity, AuthError> {
        for provider in &self.providers {
            match provider.authenticate(creds).await {
                Ok(identity) => return Ok(identity),
                Err(AuthError::NotMyCredentials) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AuthError::AuthFailed("no provider accepted these credentials".into()))
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-auth chain_test 2>&1`
Expected: all pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-auth/src/provider.rs crates/sqe-auth/src/chain.rs crates/sqe-auth/tests/
git commit -m "feat(auth): add AuthProvider trait and AuthChain"
```

---

### Task 2: OidcPasswordProvider implements AuthProvider

**Files:**
- Modify: `crates/sqe-auth/src/oidc_password.rs`
- Test: `crates/sqe-auth/tests/oidc_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-auth/tests/oidc_test.rs
use sqe_auth::{OidcPasswordProvider, AuthProvider, FlightCredentials};

#[tokio::test]
async fn wrong_password_returns_auth_failed() {
    // Use a mock HTTP server (wiremock)
    let mock = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(401).set_body_json(serde_json::json!({"error":"invalid_grant"})))
        .mount(&mock).await;
    let provider = OidcPasswordProvider::new(format!("{}/token", mock.uri()), "sqe".into(), None, "realm_access.roles".into());
    let creds = FlightCredentials::basic("alice", "wrongpass");
    let result = provider.authenticate(&creds).await;
    assert!(matches!(result, Err(sqe_auth::AuthError::AuthFailed(_))));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-auth oidc_test 2>&1`
Expected: compile/test failure

- [ ] **Step 3: Implement AuthProvider for OidcPasswordProvider**

Wrap existing ROPC logic in `authenticate()`: if credentials have `username` + `password`, perform ROPC; otherwise return `NotMyCredentials`. Move refresh logic into `refresh_catalog_token()`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-auth 2>&1`
Expected: all pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-auth/src/oidc_password.rs
git commit -m "feat(auth): OidcPasswordProvider implements AuthProvider trait"
```

---

### Task 3: BearerTokenProvider

**Files:**
- Create: `crates/sqe-auth/src/bearer_token.rs`
- Test: `crates/sqe-auth/tests/bearer_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-auth/tests/bearer_test.rs
// (Use jsonwebtoken to create test tokens signed with a known key)
#[tokio::test]
async fn valid_jwt_as_password_is_accepted() { ... }

#[tokio::test]
async fn expired_jwt_is_rejected() { ... }

#[tokio::test]
async fn non_jwt_password_returns_not_my_credentials() { ... }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-auth bearer_test 2>&1`
Expected: module not found

- [ ] **Step 3: Implement BearerTokenProvider**

Key logic:
- Detect JWT: `password.starts_with("eyJ")` or `bearer_header.is_some()`
- JWKS cache: `moka::future::Cache<String, JwkSet>` keyed by JWKS URL, 15-min TTL
- Validate: `jsonwebtoken::decode()` with `jwks.find(kid)` → `DecodingKey`
- On `kid` not found: invalidate cache entry, refetch once, retry
- Extract `user_claim` and `roles_claim` from token claims
- Return `NotMyCredentials` if not a JWT-shaped credential

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-auth bearer_test 2>&1`
Expected: all pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-auth/src/bearer_token.rs
git commit -m "feat(auth): add BearerTokenProvider with JWKS validation and cache"
```

---

### Task 4: ApiKeyProvider

**Files:**
- Create: `crates/sqe-auth/src/api_key.rs`
- Test: `crates/sqe-auth/tests/api_key_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[tokio::test]
async fn correct_api_key_returns_identity_with_groups() {
    let keys_toml = r#"
[[keys]]
key = "sqe_k_testkey123"
groups = ["data-engineering"]
"#;
    let provider = ApiKeyProvider::from_str(keys_toml, role_mappings).unwrap();
    let creds = FlightCredentials::basic("", "sqe_k_testkey123");
    let identity = provider.authenticate(&creds).await.unwrap();
    assert!(identity.roles.contains(&"writer".to_string())); // from data-engineering mapping
}

#[tokio::test]
async fn wrong_api_key_is_rejected() {
    // same provider, wrong key → AuthFailed
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-auth api_key_test 2>&1`
Expected: module not found

- [ ] **Step 3: Implement ApiKeyProvider**

Key logic:
- Load keys from TOML file at startup; watch file with `notify` crate for changes
- On credential: check if password looks like API key (configurable prefix, or just try all keys)
- Constant-time compare using `subtle::ConstantTimeEq`
- Map groups → roles via injected `HashMap<String, Vec<String>>`
- Hot-reload: spawn tokio task watching file; on change, atomically swap the loaded key set (`Arc<RwLock<Vec<ApiKey>>>`)

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-auth api_key_test 2>&1`
Expected: all pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-auth/src/api_key.rs
git commit -m "feat(auth): add ApiKeyProvider with group-based roles and hot reload"
```

---

### Task 5: AnonymousProvider + MtlsProvider

**Files:**
- Create: `crates/sqe-auth/src/anonymous.rs`
- Create: `crates/sqe-auth/src/mtls.rs`

- [ ] **Step 1: Write tests**
```rust
#[tokio::test]
async fn anonymous_accepts_any_credentials() {
    let p = AnonymousProvider::new("anon", vec!["public"]);
    let identity = p.authenticate(&FlightCredentials::basic("", "")).await.unwrap();
    assert_eq!(identity.user_id, "anon");
}

#[tokio::test]
async fn mtls_extracts_cn_from_cert() {
    let creds = FlightCredentials { peer_cert_cn: Some("alice".into()), ..Default::default() };
    let p = MtlsProvider::new(None);
    let identity = p.authenticate(&creds).await.unwrap();
    assert_eq!(identity.user_id, "alice");
}

#[tokio::test]
async fn mtls_returns_not_my_credentials_without_cert() {
    let creds = FlightCredentials::basic("alice", "pass");
    let p = MtlsProvider::new(None);
    assert!(matches!(p.authenticate(&creds).await, Err(AuthError::NotMyCredentials)));
}
```

- [ ] **Step 2: Implement both providers** (straightforward)

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-auth 2>&1`
Expected: all pass

- [ ] **Step 4: Commit**
```bash
git add crates/sqe-auth/src/anonymous.rs crates/sqe-auth/src/mtls.rs
git commit -m "feat(auth): add AnonymousProvider and MtlsProvider"
```

---

### Task 6: Config + wiring into SessionManager

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Create: `crates/sqe-auth/src/factory.rs`
- Modify: `crates/sqe-coordinator/src/session.rs`
- Test: `crates/sqe-core/tests/auth_config_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[test]
fn default_config_builds_oidc_chain() {
    let toml = r#"[auth.oidc]
token_url = "http://idp/token"
client_id = "sqe""#;
    let config = Config::from_str(toml).unwrap();
    assert_eq!(config.auth.providers.len(), 0); // no explicit providers = default OIDC
    let chain = build_auth_chain(&config.auth).unwrap();
    assert_eq!(chain.provider_count(), 1);
}

#[test]
fn explicit_multi_provider_config_builds_chain_in_order() {
    let toml = r#"
[[auth.providers]]
type = "bearer_token"
jwks_url = "http://idp/.well-known/jwks.json"

[[auth.providers]]
type = "oidc_password"
token_url = "http://idp/token"
client_id = "sqe"
"#;
    let config = Config::from_str(toml).unwrap();
    let chain = build_auth_chain(&config.auth).unwrap();
    assert_eq!(chain.provider_count(), 2);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-core auth_config 2>&1`
Expected: failure

- [ ] **Step 3: Implement AuthProviderConfig enum + factory**
```rust
// sqe-core/src/config.rs — new section
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthProviderConfig {
    OidcPassword { token_url: String, client_id: String, client_secret: Option<String>, roles_claim: Option<String> },
    BearerToken { jwks_url: String, audience: Option<String>, user_claim: Option<String>, roles_claim: Option<String> },
    ApiKey { keys_file: String },
    Anonymous { user: String, groups: Vec<String> },
    Mtls { groups_from: Option<String> },
}
```

- [ ] **Step 4: Update SessionManager**

Change `SessionManager::new(keycloak_client)` → `SessionManager::new(Arc<dyn AuthProvider>)`. Remove all direct Keycloak references from coordinator.

- [ ] **Step 5: Run all tests**

Run: `cargo test 2>&1`
Expected: all pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-core/ crates/sqe-auth/src/factory.rs crates/sqe-coordinator/src/session.rs
git commit -m "feat(auth): wire AuthProvider trait into SessionManager; add config-driven factory"
```

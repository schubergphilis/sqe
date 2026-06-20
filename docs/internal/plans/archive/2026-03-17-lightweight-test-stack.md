# Lightweight Test Stack Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 5+ container quickstart stack with a 2-container test stack (Polaris in-memory + RustFS) and add client_credentials auth support so integration tests can run without Keycloak.

**Architecture:** Add `token_endpoint` field to `AuthConfig`. If set (and `keycloak_url` is empty), the authenticator uses OAuth2 client_credentials grant against that endpoint instead of Keycloak ROPC. Docker compose starts Polaris (in-memory) + RustFS. Bootstrap script creates bucket, warehouse, and namespace.

**Tech Stack:** Rust, reqwest, Docker Compose, Polaris REST API, RustFS (S3-compatible)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/sqe-core/src/config.rs` | Modify | Add `token_endpoint` to `AuthConfig`, make `keycloak_url`/`realm` optional |
| `crates/sqe-auth/src/oauth.rs` | Create | Generic OAuth2 client_credentials token fetcher |
| `crates/sqe-auth/src/authenticator.rs` | Modify | Branch on config: Keycloak ROPC vs client_credentials |
| `crates/sqe-auth/src/lib.rs` | Modify | Export new `oauth` module |
| `docker-compose.test.yml` | Create | Polaris in-memory + RustFS, 2 containers |
| `scripts/bootstrap-test.sh` | Create | Create S3 bucket, Polaris warehouse, namespace |
| `tests/sqe-test.toml` | Modify | Point at lightweight stack with client_credentials |
| `crates/sqe-coordinator/tests/integration_test.rs` | Modify | Update auth tests for client_credentials mode |

---

## Chunk 1: Auth config + client_credentials grant

### Task 1: Make AuthConfig support both modes

**Files:**
- Modify: `crates/sqe-core/src/config.rs`

- [ ] **Step 1: Update AuthConfig to support optional keycloak_url and new token_endpoint**

```rust
#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    /// Keycloak URL — if set, uses ROPC grant (production mode)
    #[serde(default)]
    pub keycloak_url: String,
    #[serde(default)]
    pub realm: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Direct OAuth2 token endpoint — if set (and keycloak_url empty), uses client_credentials grant
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
}
```

Add env override in `apply_env_overrides`:
```rust
env_override_str("SQE_AUTH__TOKEN_ENDPOINT", &mut self.auth.token_endpoint);
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `cargo test -p sqe-core`
Expected: PASS (AuthConfig is backwards-compatible since `token_endpoint` defaults to empty)

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat: add token_endpoint to AuthConfig for client_credentials support"
```

### Task 2: Implement OAuth2 client_credentials token client

**Files:**
- Create: `crates/sqe-auth/src/oauth.rs`
- Modify: `crates/sqe-auth/src/lib.rs`

- [ ] **Step 1: Create oauth.rs with client_credentials grant**

```rust
// crates/sqe-auth/src/oauth.rs
use serde::Deserialize;
use tracing::debug;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    #[serde(default)]
    pub refresh_token: Option<String>,
}

/// Generic OAuth2 client that supports client_credentials grant.
/// Works with any OAuth2 token endpoint (Polaris, Keycloak, etc.).
pub struct OAuthClient {
    client: reqwest::Client,
    token_endpoint: String,
    client_id: String,
    client_secret: String,
}

impl OAuthClient {
    pub fn new(
        token_endpoint: &str,
        client_id: &str,
        client_secret: &str,
        accept_invalid_certs: bool,
    ) -> sqe_core::Result<Self> {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| sqe_core::SqeError::Auth(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            client,
            token_endpoint: token_endpoint.to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
    }

    /// Exchange client_id + client_secret for an access token.
    pub async fn get_token(&self) -> sqe_core::Result<TokenResponse> {
        debug!("Requesting token via client_credentials grant");

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ];

        let response = self
            .client
            .post(&self.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            return Err(sqe_core::SqeError::Auth(format!(
                "Token endpoint returned {status}: {body}"
            )));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Failed to parse token response: {e}")))
    }
}
```

- [ ] **Step 2: Export oauth module from lib.rs**

Add to `crates/sqe-auth/src/lib.rs`:
```rust
pub mod keycloak;
pub mod oauth;
pub mod token_cache;
pub mod authenticator;

pub use authenticator::Authenticator;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p sqe-auth`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-auth/src/oauth.rs crates/sqe-auth/src/lib.rs
git commit -m "feat: add OAuth2 client_credentials token client"
```

### Task 3: Update Authenticator to support both modes

**Files:**
- Modify: `crates/sqe-auth/src/authenticator.rs`

- [ ] **Step 1: Refactor Authenticator to branch on config**

The authenticator should use `OAuthClient` when `token_endpoint` is set, or `KeycloakClient` when `keycloak_url` is set. The `authenticate` method in client_credentials mode ignores username/password and returns a session from the service token.

```rust
use crate::keycloak::KeycloakClient;
use crate::oauth::OAuthClient;
use crate::token_cache::{CachedToken, TokenCache};

enum AuthBackend {
    Keycloak(KeycloakClient),
    ClientCredentials(OAuthClient),
}

pub struct Authenticator {
    backend: AuthBackend,
    cache: TokenCache,
    refresh_buffer_secs: u64,
}

impl Authenticator {
    pub async fn new(config: &AuthConfig) -> sqe_core::Result<Self> {
        let backend = if !config.token_endpoint.is_empty() {
            // Client credentials mode (Polaris, generic OAuth2)
            info!("Auth mode: client_credentials ({})", config.token_endpoint);
            AuthBackend::ClientCredentials(OAuthClient::new(
                &config.token_endpoint,
                &config.client_id,
                &config.client_secret,
                !config.ssl_verification,
            )?)
        } else {
            // Keycloak ROPC mode (production)
            info!("Auth mode: keycloak ROPC ({})", config.keycloak_url);
            AuthBackend::Keycloak(KeycloakClient::new(config)?)
        };

        Ok(Self {
            backend,
            cache: TokenCache::new(),
            refresh_buffer_secs: config.token_refresh_buffer_secs,
        })
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<Session> {
        let (access_token, refresh_token, expires_in, roles) = match &self.backend {
            AuthBackend::Keycloak(kc) => {
                let resp = kc.exchange_credentials(username, password).await?;
                let roles = kc.extract_roles(&resp.access_token);
                (resp.access_token, resp.refresh_token, resp.expires_in, roles)
            }
            AuthBackend::ClientCredentials(oauth) => {
                let resp = oauth.get_token().await?;
                // client_credentials tokens don't have user-specific roles
                (resp.access_token, resp.refresh_token, resp.expires_in, vec![])
            }
        };

        let token_expiry = Utc::now() + Duration::seconds(expires_in as i64);

        let session = Session::new(
            username.to_string(),
            access_token.clone(),
            refresh_token.clone(),
            token_expiry,
            roles,
        );

        self.cache.insert(
            &session.id,
            CachedToken {
                access_token,
                refresh_token,
                expiry: token_expiry,
            },
        );

        debug!(session_id = session.id, username = username, "Session created");
        Ok(session)
    }

    // get_cached_token — unchanged
    // refresh_session — only works in Keycloak mode, no-op or error in client_credentials
    // start_refresh_task — only runs in Keycloak mode
}
```

For `refresh_session` and `start_refresh_task`, check the backend:
- Keycloak: works as before
- ClientCredentials: `refresh_session` re-fetches via `oauth.get_token()`, `start_refresh_task` does the same

- [ ] **Step 2: Verify compilation and existing tests**

Run: `cargo test`
Expected: All 127+ tests PASS

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-auth/src/authenticator.rs
git commit -m "feat: authenticator supports keycloak ROPC and client_credentials modes"
```

---

## Chunk 2: Docker Compose + Bootstrap Script

### Task 4: Create docker-compose.test.yml

**Files:**
- Create: `docker-compose.test.yml`

- [ ] **Step 1: Create the 2-container compose file**

```yaml
# docker-compose.test.yml — Lightweight test stack
# Usage: docker compose -f docker-compose.test.yml up -d
# Then: ./scripts/bootstrap-test.sh

services:
  polaris:
    image: apache/polaris:1.3.0-incubating
    environment:
      POLARIS_PERSISTENCE_TYPE: in-memory
      POLARIS_BOOTSTRAP_CREDENTIALS: "iceberg,root,s3cr3t"
      POLARIS_PRODUCTION_READINESS_CHECKS_ENABLED: "false"
      QUARKUS_HTTP_PORT: 8181
      QUARKUS_LOG_LEVEL: WARN
    ports:
      - "8181:8181"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8181/q/health"]
      interval: 5s
      timeout: 3s
      retries: 15
      start_period: 10s

  rustfs:
    image: rustfs/rustfs:latest
    command: server /data
    environment:
      RUSTFS_ROOT_USER: s3admin
      RUSTFS_ROOT_PASSWORD: s3admin
    ports:
      - "9000:9000"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 5
```

- [ ] **Step 2: Verify containers start**

Run: `docker compose -f docker-compose.test.yml up -d`
Run: `docker compose -f docker-compose.test.yml ps`
Expected: Both services healthy within ~15s

- [ ] **Step 3: Commit**

```bash
git add docker-compose.test.yml
git commit -m "feat: add lightweight test stack docker-compose (Polaris in-memory + RustFS)"
```

### Task 5: Create bootstrap script

**Files:**
- Create: `scripts/bootstrap-test.sh`

- [ ] **Step 1: Write the bootstrap script**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Bootstrap lightweight test stack: create S3 bucket, Polaris warehouse, namespace.
# Idempotent — safe to re-run.

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
S3_URL="${S3_URL:-http://localhost:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
WAREHOUSE="${WAREHOUSE:-test_warehouse}"
NAMESPACE="${NAMESPACE:-default}"

echo "=== SQE Test Stack Bootstrap ==="
echo "Polaris:  $POLARIS_URL"
echo "S3:       $S3_URL"
echo "Warehouse: $WAREHOUSE"
echo ""

# ── Wait for services ──────────────────────────────────────────
echo -n "Waiting for Polaris..."
for i in $(seq 1 30); do
    if curl -sf "$POLARIS_URL/q/health" > /dev/null 2>&1; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 1
done

echo -n "Waiting for RustFS..."
for i in $(seq 1 15); do
    if curl -sf "$S3_URL/minio/health/live" > /dev/null 2>&1; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 1
done

# ── 1. Create S3 bucket ───────────────────────────────────────
echo -n "Creating S3 bucket 'warehouse'... "
# Use AWS CLI-style PUT with basic auth
DATE=$(date -u +"%a, %d %b %Y %H:%M:%S GMT")
curl -sf -X PUT "http://${S3_ACCESS_KEY}:${S3_SECRET_KEY}@${S3_URL#http://}/warehouse" \
    > /dev/null 2>&1 || true
echo "done"

# ── 2. Get Polaris OAuth2 token ────────────────────────────────
echo -n "Getting Polaris token... "
TOKEN=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null \
    || curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
        | jq -r '.access_token')
echo "done"

# ── 3. Create warehouse catalog ────────────────────────────────
echo -n "Creating warehouse catalog '$WAREHOUSE'... "
HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
        \"catalog\": {
            \"name\": \"$WAREHOUSE\",
            \"type\": \"INTERNAL\",
            \"storageConfigInfo\": {
                \"storageType\": \"S3\",
                \"allowedLocations\": [\"s3://warehouse/\"],
                \"properties\": {
                    \"s3.endpoint\": \"http://rustfs:9000\",
                    \"s3.path-style-access\": \"true\",
                    \"s3.access-key-id\": \"$S3_ACCESS_KEY\",
                    \"s3.secret-access-key\": \"$S3_SECRET_KEY\",
                    \"region\": \"us-east-1\"
                }
            },
            \"properties\": {
                \"default-base-location\": \"s3://warehouse/\"
            }
        }
    }" 2>/dev/null || echo "409")

if [ "$HTTP_CODE" = "409" ]; then
    echo "already exists"
else
    echo "done"
fi

# ── 4. Grant catalog access to root principal ──────────────────
echo -n "Granting catalog admin to root... "
# Get the root principal ID
PRINCIPAL=$(curl -sf "$POLARIS_URL/api/management/v1/principals" \
    -H "Authorization: Bearer $TOKEN" \
    | python3 -c "import sys,json; ps=json.load(sys.stdin)['principals']; print(next(p['name'] for p in ps if p['name']=='root'))" 2>/dev/null || echo "root")

# Create catalog admin role if needed
curl -sf -X POST "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' > /dev/null 2>&1 || true

# Grant catalog_admin all privileges
curl -sf -X PUT "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' > /dev/null 2>&1 || true

# Assign catalog role to root's principal role
curl -sf -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/$WAREHOUSE" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' > /dev/null 2>&1 || true
echo "done"

# ── 5. Create default namespace ────────────────────────────────
echo -n "Creating namespace '$NAMESPACE'... "
HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/catalog/v1/$WAREHOUSE/namespaces" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\": [\"$NAMESPACE\"]}" 2>/dev/null || echo "409")

if [ "$HTTP_CODE" = "409" ]; then
    echo "already exists"
else
    echo "done"
fi

echo ""
echo "=== Bootstrap complete ==="
echo "SQE can connect with:"
echo "  token_endpoint = \"$POLARIS_URL/api/catalog/v1/oauth/tokens\""
echo "  polaris_url    = \"$POLARIS_URL/api/catalog\""
echo "  warehouse      = \"$WAREHOUSE\""
```

- [ ] **Step 2: Make executable**

Run: `chmod +x scripts/bootstrap-test.sh`

- [ ] **Step 3: Commit**

```bash
git add scripts/bootstrap-test.sh
git commit -m "feat: add bootstrap script for lightweight test stack"
```

---

## Chunk 3: Test config + integration test updates

### Task 6: Update test config

**Files:**
- Modify: `tests/sqe-test.toml`

- [ ] **Step 1: Update sqe-test.toml for lightweight stack**

```toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080

[auth]
token_endpoint = "http://localhost:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
polaris_url = "http://localhost:8181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://localhost:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
```

- [ ] **Step 2: Commit**

```bash
git add tests/sqe-test.toml
git commit -m "feat: update test config for lightweight stack (client_credentials + RustFS)"
```

### Task 7: Update integration tests

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Update auth test to work with client_credentials mode**

The `test_keycloak_authentication` test should be renamed and work with client_credentials:

```rust
#[tokio::test]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn test_authentication() {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");

    // In client_credentials mode, username/password are ignored — token comes from client_id/secret
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Authentication failed");
    assert!(!session.access_token.is_empty(), "Access token should not be empty");
}
```

Remove the `test_different_users_get_different_sessions` test (not applicable in client_credentials mode — single principal).

Update test comments from "Requires running quickstart stack" to:
```rust
// Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
```

- [ ] **Step 2: Verify all tests compile**

Run: `cargo test -p sqe-coordinator --no-run`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/tests/integration_test.rs
git commit -m "feat: update integration tests for lightweight stack"
```

### Task 8: Verify end-to-end

- [ ] **Step 1: Start the stack**

```bash
docker compose -f docker-compose.test.yml up -d
```

- [ ] **Step 2: Run bootstrap**

```bash
./scripts/bootstrap-test.sh
```

Expected: All 5 steps succeed, "Bootstrap complete" message.

- [ ] **Step 3: Run integration tests**

```bash
cargo test -p sqe-coordinator --test integration_test -- --ignored test_authentication
```

Expected: PASS

- [ ] **Step 4: Tear down**

```bash
docker compose -f docker-compose.test.yml down -v
```

- [ ] **Step 5: Commit any fixes and push**

```bash
git push
```

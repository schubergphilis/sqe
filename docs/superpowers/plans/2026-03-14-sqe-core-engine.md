# SQE Core Engine Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a distributed SQL query engine that authenticates users via Keycloak and passes their tokens through to Polaris (Iceberg REST catalog) and S3, replacing the Trino DCAF fork.

**Architecture:** Custom coordinator/worker over Arrow Flight. Coordinator handles SQL parsing, planning, auth, and scheduling. Workers execute read-only plan fragments with user credentials. All write operations execute on the coordinator. DataFusion for query execution, iceberg-rust for Iceberg table access.

**Tech Stack:** Rust, DataFusion, iceberg-rust, Arrow Flight SQL, sqlparser-rs, axum, tokio, reqwest, moka, serde

**Spec:** `docs/superpowers/specs/2026-03-14-sqe-core-engine-design.md`
**OpenSpec Tasks:** `openspec/changes/sqe-core-engine/tasks.md`

---

## Chunk 1: Workspace + Core + Auth + Catalog (First query end-to-end)

This chunk gets to the first milestone: `sqe-coordinator` binary that accepts a Flight SQL connection with username/password, authenticates via Keycloak, and runs a SELECT query against an Iceberg table in Polaris using the user's bearer token.

### File Structure (Chunk 1)

```
Cargo.toml                          # workspace root
sqe.toml.example                    # example config
crates/
  sqe-core/
    Cargo.toml
    src/lib.rs                      # re-exports
    src/config.rs                   # sqe.toml parsing via serde + toml
    src/error.rs                    # SqeError enum, Result alias
    src/session.rs                  # Session, SessionUser structs
  sqe-auth/
    Cargo.toml
    src/lib.rs                      # re-exports
    src/keycloak.rs                 # Keycloak OIDC client (ROPC token exchange, refresh)
    src/token_cache.rs              # DashMap-based token cache with expiry
    src/authenticator.rs            # authenticate(user, pass) -> Session
  sqe-catalog/
    Cargo.toml
    src/lib.rs                      # re-exports
    src/rest_catalog.rs             # Per-session iceberg-rust REST catalog wrapper
    src/catalog_provider.rs         # DataFusion CatalogProvider impl
    src/schema_provider.rs          # DataFusion SchemaProvider impl
    src/table_provider.rs           # DataFusion TableProvider wrapper around iceberg-rust
    src/credential_vending.rs       # Extract + cache vended S3 creds from Polaris
  sqe-sql/
    Cargo.toml
    src/lib.rs                      # re-exports
    src/classifier.rs               # Statement classification enum + classify()
  sqe-policy/
    Cargo.toml
    src/lib.rs                      # PolicyEnforcer trait + PassthroughEnforcer
  sqe-coordinator/
    Cargo.toml
    src/main.rs                     # Binary entry point: load config, start servers
    src/flight_sql.rs               # Arrow Flight SQL server implementation
    src/query_handler.rs            # Parse → plan → optimize → execute pipeline
    src/session_manager.rs          # Create/track/expire sessions
```

---

### Task 1: Workspace and sqe-core

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/sqe-core/Cargo.toml`
- Create: `crates/sqe-core/src/lib.rs`
- Create: `crates/sqe-core/src/config.rs`
- Create: `crates/sqe-core/src/error.rs`
- Create: `crates/sqe-core/src/session.rs`
- Create: `sqe.toml.example`

- [ ] **Step 1: Create workspace Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "crates/sqe-core",
    "crates/sqe-auth",
    "crates/sqe-catalog",
    "crates/sqe-sql",
    "crates/sqe-policy",
    "crates/sqe-planner",
    "crates/sqe-coordinator",
    "crates/sqe-worker",
    "crates/sqe-trino-compat",
    "crates/sqe-metrics",
]

[workspace.dependencies]
# Query engine
datafusion = "49"
datafusion-common = "49"
datafusion-expr = "49"
datafusion-sql = "49"
datafusion-proto = "49"

# Arrow
arrow = { version = "55", features = ["prettyprint"] }
arrow-flight = { version = "55", features = ["flight-sql-experimental"] }
arrow-schema = "55"
arrow-array = "55"

# Iceberg
iceberg = "0.4"
iceberg-catalog-rest = "0.4"
iceberg-datafusion = "0.4"

# SQL parser
sqlparser = "0.53"

# Async
tokio = { version = "1", features = ["full"] }

# HTTP
axum = "0.8"
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }

# gRPC (Arrow Flight)
tonic = "0.12"
prost = "0.13"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"

# Concurrency / Caching
dashmap = "6"
moka = { version = "0.12", features = ["future"] }

# Auth
jsonwebtoken = "9"

# Observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
prometheus = "0.13"

# Error handling
thiserror = "2"
anyhow = "1"

# Misc
async-trait = "0.1"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
url = "2"
bytes = "1"
futures = "0.3"
```

- [ ] **Step 2: Create sqe-core crate scaffold**

Create `crates/sqe-core/Cargo.toml`:
```toml
[package]
name = "sqe-core"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { workspace = true }
toml = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
uuid = { workspace = true }
url = { workspace = true }
tracing = { workspace = true }
```

- [ ] **Step 3: Implement error types**

Create `crates/sqe-core/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqeError {
    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Query execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SqeError>;
```

Add `anyhow` to sqe-core Cargo.toml dependencies.

- [ ] **Step 4: Implement config parsing**

Create `crates/sqe-core/src/config.rs` — parse `sqe.toml` into typed structs:
```rust
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    pub catalog: CatalogConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default = "default_flight_port")]
    pub flight_sql_port: u16,
    #[serde(default = "default_trino_port")]
    pub trino_http_port: u16,
    #[serde(default = "default_mode")]
    pub mode: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub keycloak_url: String,
    pub realm: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    pub polaris_url: String,
    #[serde(default)]
    pub warehouse: String,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_region: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
    #[serde(default)]
    pub s3_path_style: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PolicyConfig {
    #[serde(default = "default_passthrough")]
    pub engine: String,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self { engine: "passthrough".to_string() }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    #[serde(default)]
    pub otlp_endpoint: String,
    #[serde(default)]
    pub audit_log_path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
            otlp_endpoint: String::new(),
            audit_log_path: String::new(),
        }
    }
}

fn default_flight_port() -> u16 { 50051 }
fn default_trino_port() -> u16 { 8080 }
fn default_mode() -> String { "hybrid".to_string() }
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
fn default_refresh_buffer() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_cache_ttl() -> u64 { 30 }
fn default_passthrough() -> String { "passthrough".to_string() }
fn default_prometheus_port() -> u16 { 9090 }

impl SqeConfig {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to read {path}: {e}")))?;
        toml::from_str(&content)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to parse config: {e}")))
    }
}
```

- [ ] **Step 5: Implement Session types**

Create `crates/sqe-core/src/session.rs`:
```rust
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub user: SessionUser,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_expiry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SessionUser {
    pub username: String,
    pub roles: Vec<String>,
}

impl Session {
    pub fn new(username: String, access_token: String, refresh_token: Option<String>, token_expiry: DateTime<Utc>, roles: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            user: SessionUser { username, roles },
            access_token,
            refresh_token,
            token_expiry,
            created_at: Utc::now(),
        }
    }

    pub fn token_fingerprint(&self) -> String {
        let token = &self.access_token;
        let tail = &token[token.len().saturating_sub(8)..];
        format!("{}-{}", self.user.username, tail)
    }

    pub fn is_token_expiring(&self, buffer_secs: u64) -> bool {
        let buffer = chrono::Duration::seconds(buffer_secs as i64);
        Utc::now() + buffer >= self.token_expiry
    }
}
```

- [ ] **Step 6: Create lib.rs and sqe.toml.example**

Create `crates/sqe-core/src/lib.rs`:
```rust
pub mod config;
pub mod error;
pub mod session;

pub use config::SqeConfig;
pub use error::{Result, SqeError};
pub use session::{Session, SessionUser};
```

Create `sqe.toml.example` with the default config from the design spec.

- [ ] **Step 7: Verify workspace compiles**

Run: `cargo check -p sqe-core`
Expected: compiles with no errors

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/sqe-core/ sqe.toml.example
git commit -m "feat: initialize workspace and sqe-core crate with config, error, session types"
```

---

### Task 2: sqe-auth — Keycloak OIDC Client

**Files:**
- Create: `crates/sqe-auth/Cargo.toml`
- Create: `crates/sqe-auth/src/lib.rs`
- Create: `crates/sqe-auth/src/keycloak.rs`
- Create: `crates/sqe-auth/src/token_cache.rs`
- Create: `crates/sqe-auth/src/authenticator.rs`

- [ ] **Step 1: Create sqe-auth crate scaffold**

`crates/sqe-auth/Cargo.toml`:
```toml
[package]
name = "sqe-auth"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
reqwest = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
dashmap = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
jsonwebtoken = { workspace = true }
base64 = "0.22"
```

- [ ] **Step 2: Implement Keycloak client — token exchange**

Create `crates/sqe-auth/src/keycloak.rs`:
- Struct `KeycloakClient` with `reqwest::Client`, `token_url: String`, `client_id`, `client_secret`
- `async fn exchange_credentials(&self, username: &str, password: &str) -> Result<TokenResponse>` — POST to Keycloak token endpoint with `grant_type=password`
- `async fn refresh_token(&self, refresh_token: &str) -> Result<TokenResponse>` — POST with `grant_type=refresh_token`
- `TokenResponse` struct: `access_token`, `refresh_token`, `expires_in`, `token_type`
- Extract roles from JWT claims (decode without verification — Keycloak already validated)

- [ ] **Step 3: Implement token cache**

Create `crates/sqe-auth/src/token_cache.rs`:
- `TokenCache` wrapping `DashMap<String, CachedToken>` keyed by session_id
- `CachedToken`: `access_token`, `refresh_token`, `expiry: DateTime<Utc>`
- `get()`, `insert()`, `remove()`, `is_expiring(session_id, buffer_secs) -> bool`

- [ ] **Step 4: Implement authenticator**

Create `crates/sqe-auth/src/authenticator.rs`:
- `Authenticator` struct holding `KeycloakClient` + `TokenCache`
- `async fn authenticate(&self, username: &str, password: &str) -> Result<Session>` — exchange creds, cache token, return Session
- `async fn refresh_session(&self, session: &mut Session) -> Result<()>` — refresh token, update session + cache
- `fn start_refresh_task(self: Arc<Self>, buffer_secs: u64)` — spawns tokio task that periodically checks for expiring tokens

- [ ] **Step 5: Create lib.rs**

```rust
pub mod keycloak;
pub mod token_cache;
pub mod authenticator;

pub use authenticator::Authenticator;
```

- [ ] **Step 6: Verify compiles**

Run: `cargo check -p sqe-auth`

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-auth/
git commit -m "feat: sqe-auth crate with Keycloak OIDC client, token cache, and authenticator"
```

---

### Task 3: sqe-catalog — Iceberg REST Catalog with Bearer Token

**Files:**
- Create: `crates/sqe-catalog/Cargo.toml`
- Create: `crates/sqe-catalog/src/lib.rs`
- Create: `crates/sqe-catalog/src/rest_catalog.rs`
- Create: `crates/sqe-catalog/src/catalog_provider.rs`
- Create: `crates/sqe-catalog/src/schema_provider.rs`
- Create: `crates/sqe-catalog/src/table_provider.rs`
- Create: `crates/sqe-catalog/src/credential_vending.rs`

- [ ] **Step 1: Create sqe-catalog crate scaffold**

```toml
[package]
name = "sqe-catalog"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
datafusion = { workspace = true }
arrow = { workspace = true }
arrow-schema = { workspace = true }
iceberg = { workspace = true }
iceberg-catalog-rest = { workspace = true }
iceberg-datafusion = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
reqwest = { workspace = true }
moka = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }
anyhow = { workspace = true }
url = { workspace = true }
dashmap = { workspace = true }
```

- [ ] **Step 2: Implement REST catalog wrapper with bearer token**

Create `crates/sqe-catalog/src/rest_catalog.rs`:
- `SessionCatalog` struct: wraps `iceberg_catalog_rest::RestCatalog` configured with user's bearer token
- Constructor takes `polaris_url`, `warehouse`, `bearer_token`, and optional S3 config
- Configure iceberg-rust REST catalog with OAuth2 token from the session
- Token fingerprint in the catalog identifier for cache invalidation

Key: iceberg-rust's `RestCatalog` supports `RestCatalogConfig` with `token` field for OAuth2 bearer. Set this to the session's access_token.

- [ ] **Step 3: Implement credential vending extraction**

Create `crates/sqe-catalog/src/credential_vending.rs`:
- After `load_table()`, extract vended S3 credentials from table config/properties
- `VendedCredentials` struct: `access_key`, `secret_key`, `session_token`, `expiry`
- Cache per `(session_id, table_name)` using moka with TTL from credential expiry
- Fallback to static S3 config when Polaris doesn't vend

- [ ] **Step 4: Implement DataFusion CatalogProvider**

Create `crates/sqe-catalog/src/catalog_provider.rs`:
- `SqeCatalogProvider` implements `datafusion::catalog::CatalogProvider`
- `schema_names()` → call Polaris `listNamespaces`
- `schema(&self, name)` → return `SqeSchemaProvider` for that namespace

- [ ] **Step 5: Implement DataFusion SchemaProvider**

Create `crates/sqe-catalog/src/schema_provider.rs`:
- `SqeSchemaProvider` implements `datafusion::catalog::SchemaProvider`
- `table_names()` → call Polaris `listTables` for the namespace
- `table(&self, name)` → load table via iceberg-rust, wrap in `SqeTableProvider`

- [ ] **Step 6: Implement DataFusion TableProvider wrapper**

Create `crates/sqe-catalog/src/table_provider.rs`:
- `SqeTableProvider` wraps `iceberg_datafusion::IcebergTableProvider`
- Delegates `schema()`, `scan()`, `supports_filters_pushdown()` to inner provider
- Ensures S3 credentials (vended or static) are configured for data access

- [ ] **Step 7: Create lib.rs**

```rust
pub mod rest_catalog;
pub mod catalog_provider;
pub mod schema_provider;
pub mod table_provider;
pub mod credential_vending;

pub use catalog_provider::SqeCatalogProvider;
pub use rest_catalog::SessionCatalog;
```

- [ ] **Step 8: Verify compiles**

Run: `cargo check -p sqe-catalog`

Note: This is where iceberg-rust API compatibility will be validated. If iceberg-rust's API doesn't match expected patterns, adapt. Check `iceberg-catalog-rest` docs for RestCatalogConfig and token injection.

- [ ] **Step 9: Commit**

```bash
git add crates/sqe-catalog/
git commit -m "feat: sqe-catalog with per-session Iceberg REST catalog, S3 credential vending, DataFusion providers"
```

---

### Task 4: sqe-sql — Statement Classification

**Files:**
- Create: `crates/sqe-sql/Cargo.toml`
- Create: `crates/sqe-sql/src/lib.rs`
- Create: `crates/sqe-sql/src/classifier.rs`

- [ ] **Step 1: Create sqe-sql crate**

```toml
[package]
name = "sqe-sql"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
sqlparser = { workspace = true }
tracing = { workspace = true }
```

- [ ] **Step 2: Implement statement classifier**

Create `crates/sqe-sql/src/classifier.rs`:
```rust
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub enum StatementKind {
    Query(Box<Statement>),
    Ctas(Box<Statement>),
    Insert(Box<Statement>),
    Merge(Box<Statement>),
    Delete(Box<Statement>),
    Drop(Box<Statement>),
    Rename(Box<Statement>),
    CreateView(Box<Statement>),
    DropView(Box<Statement>),
    ShowCatalogs,
    ShowSchemas(String),
    ShowTables(String),
    Policy(Box<Statement>),
    Utility(Box<Statement>),
}

pub fn parse_and_classify(sql: &str) -> sqe_core::Result<StatementKind> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| sqe_core::SqeError::Execution(format!("Parse error: {e}")))?;

    let stmt = statements.into_iter().next()
        .ok_or_else(|| sqe_core::SqeError::Execution("Empty SQL".to_string()))?;

    classify(stmt)
}

fn classify(stmt: Statement) -> sqe_core::Result<StatementKind> {
    match &stmt {
        Statement::Query(_) => Ok(StatementKind::Query(Box::new(stmt))),
        Statement::CreateTable(ct) if ct.query.is_some() => Ok(StatementKind::Ctas(Box::new(stmt))),
        Statement::Insert(_) => Ok(StatementKind::Insert(Box::new(stmt))),
        Statement::Merge { .. } => Ok(StatementKind::Merge(Box::new(stmt))),
        Statement::Delete(_) => Ok(StatementKind::Delete(Box::new(stmt))),
        Statement::Drop { .. } => Ok(StatementKind::Drop(Box::new(stmt))),
        Statement::AlterTable { .. } => Ok(StatementKind::Rename(Box::new(stmt))),
        Statement::CreateView { .. } => Ok(StatementKind::CreateView(Box::new(stmt))),
        Statement::Grant { .. } | Statement::Revoke { .. } => Ok(StatementKind::Policy(Box::new(stmt))),
        Statement::SetVariable { .. } | Statement::ExplainTable { .. } | Statement::Explain { .. } => {
            Ok(StatementKind::Utility(Box::new(stmt)))
        }
        _ => Err(sqe_core::SqeError::NotImplemented(format!("Unsupported statement: {stmt}"))),
    }
}
```

Note: The exact sqlparser-rs AST variants may differ by version. Adapt match arms to actual API.

- [ ] **Step 3: Write unit tests for classifier**

Test that SELECT → Query, CREATE TABLE AS → Ctas, INSERT → Insert, etc.

- [ ] **Step 4: Verify compiles and tests pass**

Run: `cargo test -p sqe-sql`

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-sql/
git commit -m "feat: sqe-sql with SQL statement parsing and classification"
```

---

### Task 5: sqe-policy — PassthroughEnforcer Stub

**Files:**
- Create: `crates/sqe-policy/Cargo.toml`
- Create: `crates/sqe-policy/src/lib.rs`

- [ ] **Step 1: Create sqe-policy crate**

```toml
[package]
name = "sqe-policy"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
datafusion = { workspace = true }
async-trait = { workspace = true }
```

- [ ] **Step 2: Implement PolicyEnforcer trait and PassthroughEnforcer**

```rust
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use sqe_core::SessionUser;

#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn evaluate(&self, user: &SessionUser, plan: LogicalPlan) -> sqe_core::Result<LogicalPlan>;
}

pub struct PassthroughEnforcer;

#[async_trait]
impl PolicyEnforcer for PassthroughEnforcer {
    async fn evaluate(&self, _user: &SessionUser, plan: LogicalPlan) -> sqe_core::Result<LogicalPlan> {
        Ok(plan)
    }
}
```

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p sqe-policy`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-policy/
git commit -m "feat: sqe-policy with PolicyEnforcer trait and PassthroughEnforcer stub"
```

---

### Task 6: sqe-coordinator — Flight SQL Server + Query Pipeline

**Files:**
- Create: `crates/sqe-coordinator/Cargo.toml`
- Create: `crates/sqe-coordinator/src/main.rs`
- Create: `crates/sqe-coordinator/src/flight_sql.rs`
- Create: `crates/sqe-coordinator/src/query_handler.rs`
- Create: `crates/sqe-coordinator/src/session_manager.rs`

- [ ] **Step 1: Create sqe-coordinator crate**

```toml
[package]
name = "sqe-coordinator"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "sqe-coordinator"
path = "src/main.rs"

[dependencies]
sqe-core = { path = "../sqe-core" }
sqe-auth = { path = "../sqe-auth" }
sqe-catalog = { path = "../sqe-catalog" }
sqe-sql = { path = "../sqe-sql" }
sqe-policy = { path = "../sqe-policy" }
datafusion = { workspace = true }
arrow = { workspace = true }
arrow-flight = { workspace = true }
arrow-schema = { workspace = true }
arrow-array = { workspace = true }
tonic = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
dashmap = { workspace = true }
async-trait = { workspace = true }
futures = { workspace = true }
bytes = { workspace = true }
uuid = { workspace = true }
prost = { workspace = true }
anyhow = { workspace = true }
```

- [ ] **Step 2: Implement session manager**

Create `crates/sqe-coordinator/src/session_manager.rs`:
- `SessionManager` holds `Arc<Authenticator>` + `DashMap<String, Arc<Session>>`
- `async fn authenticate(&self, username: &str, password: &str) -> Result<Arc<Session>>`
- `fn get_session(&self, token: &str) -> Option<Arc<Session>>`
- `fn remove_session(&self, id: &str)`

- [ ] **Step 3: Implement query handler**

Create `crates/sqe-coordinator/src/query_handler.rs`:
- `QueryHandler` holds `Arc<dyn PolicyEnforcer>`, creates per-session DataFusion `SessionContext` with `SqeCatalogProvider`
- `async fn execute(&self, session: &Session, sql: &str) -> Result<SendableRecordBatchStream>`
- Pipeline: `parse_and_classify(sql)` → for Query: create LogicalPlan via DataFusion SQL planner → `policy_enforcer.evaluate()` → DataFusion optimizer → execute → stream
- For policy/utility: return appropriate response or error

- [ ] **Step 4: Implement Flight SQL server**

Create `crates/sqe-coordinator/src/flight_sql.rs`:
- Implement `arrow_flight::flight_service_server::FlightService` for `SqeFlightSqlService`
- `do_handshake` → extract username/password from Basic auth → `session_manager.authenticate()` → return session token
- `do_get_flight_info` → parse SQL from ticket → get schema
- `do_get` → execute query via `query_handler.execute()` → stream RecordBatches as FlightData
- `get_catalogs`, `get_schemas`, `get_tables` → query Polaris metadata

Key reference: `arrow-flight` crate's `FlightSqlService` trait (if using the `flight-sql-experimental` feature).

- [ ] **Step 5: Implement main.rs**

```rust
use sqe_core::SqeConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sqe=info".parse()?))
        .json()
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;

    tracing::info!("Starting SQE coordinator on Flight SQL port {}", config.coordinator.flight_sql_port);

    // Initialize auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);

    // Initialize session manager
    let session_manager = Arc::new(SessionManager::new(authenticator.clone()));

    // Initialize policy (passthrough)
    let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);

    // Initialize query handler
    let query_handler = Arc::new(QueryHandler::new(policy_enforcer, config.clone()));

    // Start Flight SQL server
    let flight_service = SqeFlightSqlService::new(session_manager, query_handler, config.clone());
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    tonic::transport::Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve(addr)
        .await?;

    Ok(())
}
```

- [ ] **Step 6: Verify compiles**

Run: `cargo check -p sqe-coordinator`

This is the big integration point — all crates come together. Fix any API mismatches between crates.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/
git commit -m "feat: sqe-coordinator with Flight SQL server, session management, and query pipeline"
```

---

### Task 7: Integration Test — First End-to-End Query

**Files:**
- Create: `tests/integration/flight_sql_test.rs` (or `tests/integration/mod.rs`)
- Create: `tests/sqe-test.toml` (test config pointing to quickstart stack)

- [ ] **Step 1: Create test config**

Create `tests/sqe-test.toml`:
```toml
[coordinator]
flight_sql_port = 50051

[auth]
keycloak_url = "https://auth.local"
realm = "iceberg"
client_id = "sqe-client"
client_secret = ""
ssl_verification = false

[catalog]
polaris_url = "http://localhost:8181/api/catalog"
warehouse = "iceberg"

[storage]
s3_endpoint = "http://localhost:9000"
s3_region = "us-east-1"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_path_style = true

[policy]
engine = "passthrough"
```

- [ ] **Step 2: Write integration test — authenticate and query**

Test: start coordinator, connect via Arrow Flight SQL client, authenticate as `root`/`root123`, run `SELECT 1`, verify result.

- [ ] **Step 3: Write integration test — query Iceberg table**

Test: authenticate, run `SHOW CATALOGS` or `SELECT * FROM <existing_table> LIMIT 10`, verify results come back.

- [ ] **Step 4: Run tests against quickstart stack**

Run: `cargo test --test integration -- --ignored` (integration tests marked `#[ignore]` by default, require running quickstart)

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "test: integration tests for Flight SQL auth and Iceberg query via Polaris"
```

---

## Chunk 2: Write Path + Views (Tasks 8.1-8.16)

Builds on Chunk 1. Adds CTAS, INSERT INTO, DELETE FROM, DROP TABLE, ALTER TABLE RENAME, CREATE/DROP VIEW, and MERGE INTO to the coordinator's query handler. All write operations execute on the coordinator.

### File Structure (Chunk 2)

```
crates/
  sqe-coordinator/
    src/
      write_handler.rs          # NEW: Write operation dispatcher
      writer.rs                 # NEW: Parquet writer using iceberg-rust writer API
      catalog_ops.rs            # NEW: DROP TABLE, RENAME, CREATE/DROP VIEW via REST
```

Key dependencies to add to sqe-coordinator:
- `parquet = { version = "55", features = ["async"] }` (workspace dep)
- `iceberg` (workspace dep, already declared)

---

### Task 8: Catalog DDL Operations (DROP TABLE, RENAME, CREATE/DROP VIEW)

**Files:**
- Create: `crates/sqe-coordinator/src/catalog_ops.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add module)
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (wire handlers)

- [ ] **Step 1: Implement catalog_ops.rs**

This module handles pure catalog REST operations that don't involve data writing.

```rust
use std::sync::Arc;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqe_catalog::rest_catalog::SessionCatalogBridge;
use sqe_core::{Session, SqeConfig, SqeError};
use sqlparser::ast::{ObjectName, ObjectType, Statement};
use tracing::info;

pub struct CatalogOps {
    config: SqeConfig,
}

impl CatalogOps {
    pub fn new(config: SqeConfig) -> Self {
        Self { config }
    }

    /// DROP TABLE [IF EXISTS] ns.table_name
    pub async fn drop_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        // Extract table name and if_exists from Statement::Drop
        // Parse namespace.table from ObjectName
        // Call catalog.drop_table(&table_ident)
        // If if_exists and table doesn't exist, return Ok(())
    }

    /// ALTER TABLE ns.old_name RENAME TO ns.new_name
    pub async fn rename_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        // Extract source and target names from Statement::AlterTable
        // Call catalog.rename_table(&src_ident, &dest_ident)
    }

    /// CREATE VIEW ns.view_name AS SELECT ...
    pub async fn create_view(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        // Polaris REST API doesn't have a native view API in iceberg-rust 0.5
        // For now: return NotImplemented with message about Polaris view support
        // This will be implemented when iceberg-rust adds view API support
    }

    /// DROP VIEW ns.view_name
    pub async fn drop_view(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        // Same as create_view — stub for now
    }

    // Helper: create SessionCatalogBridge for the session
    async fn catalog_bridge(&self, session: &Session) -> sqe_core::Result<Arc<SessionCatalogBridge>> {
        // Create SessionCatalog and get its bridge
    }

    // Helper: parse ObjectName into (NamespaceIdent, table_name)
    fn parse_table_ref(name: &ObjectName) -> sqe_core::Result<(NamespaceIdent, String)> {
        // Split "ns.table" into namespace and table parts
    }
}
```

- [ ] **Step 2: Wire into query_handler.rs**

Update the `execute` method to route Drop/Rename/CreateView/DropView to CatalogOps:
- `StatementKind::Drop(stmt)` → `catalog_ops.drop_table(session, &stmt)`
- `StatementKind::Rename(stmt)` → `catalog_ops.rename_table(session, &stmt)`
- `StatementKind::CreateView(stmt)` → `catalog_ops.create_view(session, &stmt)` (stub)
- `StatementKind::DropView(stmt)` → `catalog_ops.drop_view(session, &stmt)` (stub)
- Return empty RecordBatch vec for success

- [ ] **Step 3: Write unit tests for parse_table_ref**

Test parsing "ns.table", "catalog.ns.table", single-part names.

- [ ] **Step 4: Verify compiles**

Run: `cargo check -p sqe-coordinator`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: catalog DDL operations - DROP TABLE, ALTER TABLE RENAME"
```

---

### Task 9: Iceberg Parquet Writer Infrastructure

**Files:**
- Create: `crates/sqe-coordinator/src/writer.rs`
- Modify: `Cargo.toml` (add parquet workspace dep)
- Modify: `crates/sqe-coordinator/Cargo.toml` (add iceberg, parquet deps)
- Modify: `crates/sqe-coordinator/src/lib.rs` (add module)

- [ ] **Step 1: Add parquet to workspace dependencies**

In workspace `Cargo.toml`:
```toml
parquet = { version = "55", features = ["async"] }
```

In sqe-coordinator `Cargo.toml`:
```toml
iceberg = { workspace = true }
parquet = { workspace = true }
```

- [ ] **Step 2: Implement writer.rs — RecordBatch to Iceberg DataFiles**

```rust
use std::sync::Arc;
use arrow_array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use sqe_core::SqeError;

/// Write RecordBatches as Parquet data files for an Iceberg table.
///
/// Uses iceberg-rust's writer infrastructure:
/// ParquetWriterBuilder → DataFileWriterBuilder → IcebergWriter
///
/// Returns the list of DataFiles written (with paths, sizes, record counts).
pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    file_prefix: &str,
) -> sqe_core::Result<Vec<DataFile>> {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(vec![]);
    }

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Failed to create location generator: {e}")))?;

    let file_name_generator = DefaultFileNameGenerator::new(
        file_prefix.to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        None,  // no target file size
        table.metadata().default_partition_spec_id(),
    );

    let mut writer = data_file_writer_builder
        .build()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to build data file writer: {e}")))?;

    for batch in batches {
        if batch.num_rows() > 0 {
            writer
                .write(batch)
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to write batch: {e}")))?;
        }
    }

    let data_files = writer
        .close()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to close writer: {e}")))?;

    Ok(data_files)
}
```

Note: The actual iceberg-rust API may require adapting — especially around schema types (iceberg Schema vs Arrow Schema). The `write` method expects `RecordBatch` which matches our data. The writer handles partitioning, file naming, and Parquet encoding.

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p sqe-coordinator`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: Iceberg Parquet writer infrastructure for data file creation"
```

---

### Task 10: CTAS — CREATE TABLE AS SELECT

**Files:**
- Create: `crates/sqe-coordinator/src/write_handler.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add module)
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (wire CTAS handler)

- [ ] **Step 1: Implement write_handler.rs with CTAS**

```rust
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg::spec::Schema as IcebergSchema;
use sqe_catalog::rest_catalog::{SessionCatalog, SessionCatalogBridge};
use sqe_core::{Session, SqeConfig, SqeError};
use sqlparser::ast::Statement;
use tracing::info;

use crate::writer::write_data_files;

pub struct WriteHandler {
    config: SqeConfig,
}

impl WriteHandler {
    pub fn new(config: SqeConfig) -> Self {
        Self { config }
    }

    /// CTAS: Execute the SELECT, create the table, write data, commit snapshot.
    ///
    /// Steps:
    /// 1. Execute the inner SELECT query to get RecordBatches
    /// 2. Convert Arrow schema to Iceberg schema
    /// 3. Create the table in Polaris (empty, with schema)
    /// 4. Write RecordBatches as Parquet DataFiles
    /// 5. Fast-append the DataFiles via Transaction
    /// 6. Commit the transaction
    pub async fn handle_ctas(
        &self,
        session: &Session,
        stmt: &Statement,
        execute_query: impl AsyncFnOnce(&str) -> sqe_core::Result<Vec<RecordBatch>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Extract table name, or_replace flag, and inner SELECT from Statement::CreateTable
        // Execute the inner SELECT
        // Convert Arrow schema → Iceberg schema using iceberg::arrow::arrow_schema_to_schema
        // Create table via catalog.create_table(namespace, TableCreation { name, schema, ... })
        // Write data files using write_data_files()
        // If data files exist, create Transaction, fast_append, commit
        // Return empty vec (DDL success)
    }

    /// INSERT INTO SELECT: Execute SELECT, write data files, append to existing table.
    pub async fn handle_insert(
        &self,
        session: &Session,
        stmt: &Statement,
        execute_query: impl AsyncFnOnce(&str) -> sqe_core::Result<Vec<RecordBatch>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Extract target table and inner SELECT from Statement::Insert
        // Execute the inner SELECT
        // Load existing table from catalog
        // Write data files using write_data_files()
        // Create Transaction, fast_append data files, commit
        // Return empty vec
    }

    /// DELETE FROM with predicate: Scan table, find matching rows, write position deletes.
    pub async fn handle_delete(
        &self,
        session: &Session,
        stmt: &Statement,
        execute_query: impl AsyncFnOnce(&str) -> sqe_core::Result<Vec<RecordBatch>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // For MVP: execute DELETE as a rewrite:
        // 1. SELECT * FROM table WHERE NOT (delete_predicate) — get rows to keep
        // 2. Create new table snapshot with only kept rows (overwrite)
        // This is simpler than position deletes and correct for MVP
        // Future: use Iceberg position delete files for efficiency
    }

    /// MERGE INTO: Scan target+source, classify, rewrite.
    pub async fn handle_merge(
        &self,
        session: &Session,
        stmt: &Statement,
        execute_query: impl AsyncFnOnce(&str) -> sqe_core::Result<Vec<RecordBatch>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // MERGE is complex. For MVP:
        // 1. Parse the MERGE statement to extract target, source, condition, and actions
        // 2. Execute as a series of INSERT/DELETE operations
        // 3. Or return NotImplemented if too complex for initial release
    }

    // Helpers
    async fn create_catalog_bridge(&self, session: &Session) -> sqe_core::Result<Arc<SessionCatalogBridge>> { ... }
    async fn create_session_catalog(&self, session: &Session) -> sqe_core::Result<Arc<SessionCatalog>> { ... }
}
```

IMPORTANT: The exact implementation depends heavily on the iceberg-rust API. Key areas to adapt:
- `iceberg::arrow::arrow_schema_to_schema()` for Arrow → Iceberg schema conversion
- `TableCreation` builder pattern
- `Transaction::new(&table).fast_append(None, vec![])` → `.add_data_files()` → `.apply().await` → `.commit(&catalog).await`

- [ ] **Step 2: Wire CTAS into query_handler.rs**

```rust
// In execute() match:
StatementKind::Ctas(stmt) => {
    self.write_handler.handle_ctas(session, &stmt, |sql| {
        self.execute_query(session, sql)
    }).await
}
```

- [ ] **Step 3: Wire INSERT INTO into query_handler.rs**

```rust
StatementKind::Insert(stmt) => {
    self.write_handler.handle_insert(session, &stmt, |sql| {
        self.execute_query(session, sql)
    }).await
}
```

- [ ] **Step 4: Verify compiles**

Run: `cargo check -p sqe-coordinator`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: CTAS and INSERT INTO SELECT write handlers"
```

---

### Task 11: DELETE FROM and MERGE INTO

**Files:**
- Modify: `crates/sqe-coordinator/src/write_handler.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Implement DELETE FROM handler**

For MVP, use the "copy-on-write" approach:
1. Parse the DELETE predicate
2. Execute `SELECT * FROM table WHERE NOT (predicate)` to get surviving rows
3. Create a new table snapshot with an overwrite operation containing only surviving rows

This avoids the complexity of Iceberg position delete files while being functionally correct.

IMPORTANT: iceberg-rust 0.5 has `FastAppendAction` but may not have an `OverwriteAction`. Check the API. If overwrite is not available:
- Alternative A: Use `fast_append` with a `replace` operation (check if Transaction supports this)
- Alternative B: Drop and recreate the table (CTAS pattern) — works but loses table history
- Alternative C: Return NotImplemented for DELETE in MVP, implement in Chunk 3

- [ ] **Step 2: Implement MERGE INTO handler**

For MVP, MERGE INTO can be decomposed into:
1. Execute the MERGE join to classify rows (matched vs unmatched)
2. For WHEN MATCHED UPDATE: delete old rows + insert updated rows
3. For WHEN NOT MATCHED INSERT: insert new rows

If the decomposition is too complex for the iceberg-rust API:
- Return NotImplemented with a descriptive message
- Track as a Chunk 3 item

- [ ] **Step 3: Wire DELETE and MERGE into query_handler.rs**

- [ ] **Step 4: Verify compiles**

Run: `cargo check -p sqe-coordinator`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: DELETE FROM and MERGE INTO write handlers"
```

---

### Task 12: Write Path Integration Tests

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Add CTAS integration test**

```rust
#[tokio::test]
#[ignore]
async fn test_ctas_roundtrip() {
    // Authenticate as root
    // Execute: CREATE TABLE test_ns.ctas_test AS SELECT 1 as id, 'hello' as name
    // Execute: SELECT * FROM test_ns.ctas_test
    // Verify results match
    // Cleanup: DROP TABLE test_ns.ctas_test
}
```

- [ ] **Step 2: Add INSERT INTO integration test**

```rust
#[tokio::test]
#[ignore]
async fn test_insert_into() {
    // Create table via CTAS
    // INSERT INTO test_ns.insert_test SELECT 2 as id, 'world' as name
    // SELECT * → verify both rows
    // Cleanup
}
```

- [ ] **Step 3: Add DROP TABLE integration test**

```rust
#[tokio::test]
#[ignore]
async fn test_drop_table() {
    // Create table via CTAS
    // DROP TABLE test_ns.drop_test
    // Verify table is gone (SHOW TABLES should not include it)
}
```

- [ ] **Step 4: Add DELETE FROM integration test**

```rust
#[tokio::test]
#[ignore]
async fn test_delete_from() {
    // Create table with multiple rows
    // DELETE FROM test_ns.delete_test WHERE id = 1
    // SELECT * → verify row removed
    // Cleanup
}
```

- [ ] **Step 5: Verify compiles**

Run: `cargo test -p sqe-coordinator --no-run`

- [ ] **Step 6: Commit**

```bash
git commit -m "test: write path integration tests for CTAS, INSERT, DELETE, DROP"
```

## Chunk 3: Distributed Execution (Tasks 6.5-6.7, 7.5-7.13, 9.1-9.8)

Adds worker binary, fragment splitting, Arrow Flight transport with credentials, and distributed query execution.

*Plan for Chunk 3 will be written after Chunk 2.*

## Chunk 4: Trino Compat + information_schema + Observability (Tasks 10-13)

Adds Trino HTTP wire protocol, virtual information_schema, Prometheus metrics, audit logging, Docker images.

*Plan for Chunk 4 will be written after Chunk 3.*

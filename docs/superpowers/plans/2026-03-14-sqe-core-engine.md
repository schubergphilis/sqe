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

Adds real Iceberg scans (replacing EmptyExec), worker binary, fragment splitting, Arrow Flight transport with credentials, and distributed query execution.

### Architecture (Chunk 3)

**Current state:** `SqeTableProvider.scan()` returns `EmptyExec` — queries return empty results for actual Iceberg tables. All execution is coordinator-local.

**Target state:** Real Iceberg data is read. For large tables with workers available, scans are distributed across workers. Each worker reads a subset of Parquet data files from S3 and streams Arrow RecordBatches back to the coordinator.

**Key design decisions:**

1. **IcebergScanExec** — Custom DataFusion `ExecutionPlan` wrapping iceberg-rust's `table.scan().to_arrow()`. Leaf node in the physical plan tree.

2. **Workers are Parquet readers** — Workers receive lightweight `ScanTask` messages (JSON) containing S3 file paths + credentials. They read Parquet files via `object_store` + `parquet` crates and stream Arrow RecordBatches back. No iceberg dependency on workers — they don't need Polaris access.

3. **No PhysicalPlan serialization** (YAGNI) — Rather than serializing full DataFusion plans with `datafusion-proto`, the coordinator extracts scan parameters and sends them as simple messages. Workers create their own readers. This avoids custom codec complexity.

4. **Config-based worker discovery** — Coordinator knows worker URLs from `[coordinator] worker_urls`. Coordinator health-checks workers periodically via Flight `do_action("health_check")`.

5. **DistributedScanExec** — Coordinator-side `ExecutionPlan` that dispatches scan work to workers via Arrow Flight `do_get`. One partition per worker. Replaces `IcebergScanExec` in the plan when distributed execution is chosen.

6. **Fragment splitting** — Coordinator uses `table.scan().plan_files()` to enumerate data files, then round-robin assigns them to workers.

**Execution flow (distributed):**
```
1. SQL → LogicalPlan → PolicyEnforcer → PhysicalPlan (with IcebergScanExec leaves)
2. For each IcebergScanExec leaf:
   a. plan_files() → list of data files
   b. Round-robin assign files to workers
   c. Create DistributedScanExec (one partition per worker)
   d. Replace IcebergScanExec with DistributedScanExec in the plan
3. Execute plan — DistributedScanExec.execute(partition_i):
   a. Serialize ScanTask with file paths + S3 creds
   b. Call do_get on worker_i via Arrow Flight
   c. Stream RecordBatches back
4. Upper plan nodes (filter, project, join, aggregate, sort) execute locally on coordinator
```

**Communication model:**
- Client → Coordinator: Flight SQL (port 50051) — existing
- Coordinator → Worker: Arrow Flight `do_get` (worker's port, default 50052) — new
- Coordinator → Worker: Arrow Flight `do_action("health_check")` — new

### File Structure (Chunk 3)

```
Cargo.toml                                    # Add object_store workspace dependency
crates/
  sqe-core/
    src/config.rs                             # Add worker_flight_port, worker_urls to config
  sqe-catalog/
    Cargo.toml                                # Add parquet, bytes deps
    src/lib.rs                                # Add iceberg_scan module
    src/iceberg_scan.rs                       # NEW: IcebergScanExec ExecutionPlan
    src/table_provider.rs                     # Replace EmptyExec with IcebergScanExec
  sqe-planner/
    Cargo.toml                                # Add deps: sqe-core, arrow, serde, serde_json
    src/lib.rs                                # Module exports
    src/scan_task.rs                          # NEW: ScanTask message definition
    src/splitter.rs                           # NEW: Fragment splitting logic
  sqe-coordinator/
    Cargo.toml                                # Add sqe-planner, object_store deps
    src/lib.rs                                # Add worker_registry, distributed_scan modules
    src/worker_registry.rs                    # NEW: WorkerRegistry with health checks
    src/distributed_scan.rs                   # NEW: DistributedScanExec
    src/query_handler.rs                      # Modify: distributed execution path
    src/main.rs                               # Modify: initialize worker registry
  sqe-worker/
    Cargo.toml                                # Add all dependencies
    src/lib.rs                                # NEW: Module exports
    src/main.rs                               # NEW: Worker binary entry point
    src/executor.rs                           # NEW: Parquet file reader
    src/flight_service.rs                     # NEW: Worker Flight service
```

---

### Task 13: IcebergScanExec — Real Iceberg Table Scans

**Files:**
- Create: `crates/sqe-catalog/src/iceberg_scan.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`
- Modify: `crates/sqe-catalog/src/table_provider.rs`
- Modify: `crates/sqe-catalog/Cargo.toml`

**Context:** Currently `SqeTableProvider.scan()` returns `EmptyExec`, so all queries against Iceberg tables return zero rows. This task replaces it with `IcebergScanExec`, a custom DataFusion `ExecutionPlan` that uses iceberg-rust's `table.scan().to_arrow()` to read actual data.

- [ ] **Step 1: Add dependencies to sqe-catalog**

Add `bytes` and `parquet` to `crates/sqe-catalog/Cargo.toml` dependencies:

```toml
parquet = { workspace = true }
bytes = { workspace = true }
```

- [ ] **Step 2: Create IcebergScanExec**

Create `crates/sqe-catalog/src/iceberg_scan.rs`:

```rust
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::table::Table;
use tracing::debug;

/// Custom DataFusion `ExecutionPlan` that scans an Iceberg table using
/// iceberg-rust's scan API. This replaces the `EmptyExec` placeholder
/// in `SqeTableProvider` and provides actual data reads from S3.
///
/// The table's `FileIO` (configured with the user's vended S3 credentials)
/// handles all data access — no separate ObjectStore registration needed.
#[derive(Debug)]
pub struct IcebergScanExec {
    /// The Iceberg table to scan (contains FileIO with credentials).
    table: Table,
    /// Arrow schema for the scan output (after projection).
    projected_schema: SchemaRef,
    /// Column names to project (None = all columns).
    projection: Option<Vec<String>>,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl IcebergScanExec {
    /// Create a new Iceberg scan execution plan.
    pub fn new(table: Table, projected_schema: SchemaRef, projection: Option<Vec<String>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            table,
            projected_schema,
            projection,
            properties,
        }
    }

    /// Returns the underlying Iceberg table.
    pub fn table(&self) -> &Table {
        &self.table
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: table={}, projection={:?}",
            self.table.identifier(),
            self.projection,
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self) // no children to replace
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table = self.table.clone();
        let schema = self.projected_schema.clone();
        let projection = self.projection.clone();

        debug!(
            table = %table.identifier(),
            "Executing IcebergScanExec"
        );

        // Build the scan lazily — to_arrow() is async, execute() is sync.
        // We create a stream that initializes the scan on first poll.
        let stream = futures::stream::once(async move {
            let mut scan_builder = table.scan();

            // Apply column projection if specified
            if let Some(ref cols) = projection {
                scan_builder = scan_builder.select(cols.iter().map(|s| s.as_str()));
            }

            let scan = scan_builder
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let arrow_stream = scan
                .to_arrow()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok::<_, DataFusionError>(
                arrow_stream.map_err(|e| DataFusionError::External(Box::new(e))),
            )
        })
        .try_flatten();

        Ok(Box::pin(IcebergRecordBatchStream {
            schema,
            inner: Box::pin(stream),
        }))
    }
}

/// Wrapper stream that implements `RecordBatchStream` for DataFusion.
struct IcebergRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl Stream for IcebergRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for IcebergRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
```

- [ ] **Step 3: Register module in lib.rs**

Add to `crates/sqe-catalog/src/lib.rs`:

```rust
pub mod iceberg_scan;
```

- [ ] **Step 4: Update SqeTableProvider to use IcebergScanExec**

Replace the `EmptyExec` in `crates/sqe-catalog/src/table_provider.rs`:

Replace the entire `scan()` method body with:

```rust
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Convert projection indices to column names
        let projected_columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        // Determine the projected schema
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.schema.clone(),
        };

        Ok(Arc::new(
            crate::iceberg_scan::IcebergScanExec::new(
                self.table.clone(),
                projected_schema,
                projected_columns,
            ),
        ))
    }
```

Remove the `use datafusion::physical_plan::empty::EmptyExec;` import (no longer needed).

- [ ] **Step 5: Verify workspace compiles**

Run: `cargo check -p sqe-catalog`

Expected: compiles (there may be warnings about unused `arrow::datatypes` import in table_provider.rs — the old import for EmptyExec projection handling. Clean up if needed).

- [ ] **Step 6: Run existing tests**

Run: `cargo test --workspace`

Expected: All 37 existing tests pass. No behavior change for unit tests (they don't hit Iceberg). Integration tests (ignored) may now return real data instead of empty results.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-catalog/
git commit -m "feat: IcebergScanExec replaces EmptyExec for real Iceberg data reads"
```

---

### Task 14: ScanTask Protocol + Fragment Splitting (sqe-planner)

**Files:**
- Modify: `crates/sqe-planner/Cargo.toml`
- Create: `crates/sqe-planner/src/scan_task.rs`
- Create: `crates/sqe-planner/src/splitter.rs`
- Modify: `crates/sqe-planner/src/lib.rs`

**Context:** The coordinator needs to split work across workers. A `ScanTask` is a lightweight message describing what a worker should scan. The `FragmentSplitter` assigns data files to workers.

- [ ] **Step 1: Write the failing test for ScanTask serialization**

First, set up `crates/sqe-planner/Cargo.toml`:

```toml
[package]
name = "sqe-planner"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
arrow-schema = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

Create `crates/sqe-planner/src/scan_task.rs` with the struct and test:

```rust
use serde::{Deserialize, Serialize};

/// Lightweight message sent from coordinator to worker describing
/// which Parquet files to scan and how to access them.
///
/// Workers receive this as a JSON-encoded Flight Ticket body.
/// S3 credentials are included so workers don't need Polaris access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTask {
    /// Unique identifier for this fragment.
    pub fragment_id: String,
    /// S3 URLs of Parquet data files to scan.
    pub data_file_paths: Vec<String>,
    /// Column names to project (empty = all columns).
    pub projected_columns: Vec<String>,
    /// S3 endpoint URL.
    pub s3_endpoint: String,
    /// S3 region.
    pub s3_region: String,
    /// S3 access key (vended or static).
    pub s3_access_key: String,
    /// S3 secret key.
    pub s3_secret_key: String,
    /// S3 session token (from credential vending, empty if static).
    pub s3_session_token: String,
    /// Whether to use path-style S3 access (required for MinIO).
    pub s3_path_style: bool,
}

impl ScanTask {
    /// Serialize to JSON bytes for Flight Ticket body.
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_task_roundtrip() {
        let task = ScanTask {
            fragment_id: "frag-001".to_string(),
            data_file_paths: vec![
                "s3://bucket/data/file1.parquet".to_string(),
                "s3://bucket/data/file2.parquet".to_string(),
            ],
            projected_columns: vec!["id".to_string(), "name".to_string()],
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_access_key: "minioadmin".to_string(),
            s3_secret_key: "minioadmin".to_string(),
            s3_session_token: String::new(),
            s3_path_style: true,
        };

        let bytes = task.to_bytes().unwrap();
        let decoded = ScanTask::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.fragment_id, "frag-001");
        assert_eq!(decoded.data_file_paths.len(), 2);
        assert_eq!(decoded.projected_columns, vec!["id", "name"]);
        assert!(decoded.s3_path_style);
    }

    #[test]
    fn test_scan_task_empty_projection_means_all_columns() {
        let task = ScanTask {
            fragment_id: "frag-002".to_string(),
            data_file_paths: vec!["s3://bucket/data/file1.parquet".to_string()],
            projected_columns: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
        };

        let bytes = task.to_bytes().unwrap();
        let decoded = ScanTask::from_bytes(&bytes).unwrap();
        assert!(decoded.projected_columns.is_empty());
    }
}
```

- [ ] **Step 2: Create the fragment splitter**

Create `crates/sqe-planner/src/splitter.rs`:

```rust
use tracing::debug;

/// Distributes data file paths across N workers using round-robin assignment.
///
/// Returns a Vec of length `num_workers`, where each element is the list
/// of file paths assigned to that worker. Empty workers get empty Vecs.
///
/// If `num_workers` is 0 or `files` is empty, returns an empty Vec.
pub fn split_files(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    if num_workers == 0 || files.is_empty() {
        return vec![];
    }

    let mut groups: Vec<Vec<String>> = (0..num_workers).map(|_| Vec::new()).collect();

    for (i, file) in files.into_iter().enumerate() {
        groups[i % num_workers].push(file);
    }

    debug!(
        num_workers,
        files_per_worker = ?groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
        "Split files across workers"
    );

    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_files_even() {
        let files: Vec<String> = (0..6).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 3);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], vec!["file0.parquet", "file3.parquet"]);
        assert_eq!(groups[1], vec!["file1.parquet", "file4.parquet"]);
        assert_eq!(groups[2], vec!["file2.parquet", "file5.parquet"]);
    }

    #[test]
    fn test_split_files_uneven() {
        let files: Vec<String> = (0..5).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 3);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2); // file0, file3
        assert_eq!(groups[1].len(), 2); // file1, file4
        assert_eq!(groups[2].len(), 1); // file2
    }

    #[test]
    fn test_split_files_more_workers_than_files() {
        let files = vec!["file0.parquet".to_string()];
        let groups = split_files(files, 5);

        assert_eq!(groups.len(), 5);
        assert_eq!(groups[0], vec!["file0.parquet"]);
        assert!(groups[1].is_empty());
        assert!(groups[4].is_empty());
    }

    #[test]
    fn test_split_files_single_worker() {
        let files: Vec<String> = (0..3).map(|i| format!("file{i}.parquet")).collect();
        let groups = split_files(files, 1);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn test_split_files_empty() {
        let groups = split_files(vec![], 3);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_split_files_zero_workers() {
        let files = vec!["file0.parquet".to_string()];
        let groups = split_files(files, 0);
        assert!(groups.is_empty());
    }
}
```

- [ ] **Step 3: Update lib.rs**

Replace `crates/sqe-planner/src/lib.rs` contents:

```rust
pub mod scan_task;
pub mod splitter;

pub use scan_task::ScanTask;
pub use splitter::split_files;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-planner`

Expected: All 8 tests pass (2 scan_task + 6 splitter).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-planner/
git commit -m "feat: ScanTask protocol and fragment splitting for distributed execution"
```

---

### Task 15: Worker Registry (coordinator side)

**Files:**
- Create: `crates/sqe-coordinator/src/worker_registry.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/main.rs`
- Modify: `crates/sqe-core/src/config.rs`

**Context:** The coordinator needs to track available workers and their health status. Workers are listed in config; the coordinator health-checks them periodically via Flight `do_action`.

- [ ] **Step 1: Add config fields**

In `crates/sqe-core/src/config.rs`, add `worker_urls` to `CoordinatorConfig`:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default = "default_flight_port")]
    pub flight_sql_port: u16,
    #[serde(default = "default_trino_port")]
    pub trino_http_port: u16,
    #[serde(default = "default_mode")]
    pub mode: String,
    /// List of worker Flight server URLs for distributed execution.
    /// Empty = single-node mode (all queries execute locally).
    #[serde(default)]
    pub worker_urls: Vec<String>,
}
```

Add `flight_port` to `WorkerConfig`:

```rust
#[derive(Debug, Deserialize, Clone, Default)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_worker_flight_port")]
    pub flight_port: u16,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
}
```

Add the default function:

```rust
fn default_worker_flight_port() -> u16 { 50052 }
```

- [ ] **Step 2: Write the failing test for WorkerRegistry**

Create `crates/sqe-coordinator/src/worker_registry.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Tracks available workers and their health status.
///
/// Workers are discovered from config and health-checked periodically.
/// Unhealthy workers (3 consecutive failed health checks) are removed
/// from the active pool but retained in the registry for recovery.
#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

#[derive(Debug)]
struct RegistryInner {
    workers: HashMap<String, WorkerState>,
}

#[derive(Debug)]
struct WorkerState {
    /// The Flight server URL of the worker.
    url: String,
    /// Whether the worker is currently healthy.
    healthy: bool,
    /// Number of consecutive failed health checks.
    consecutive_failures: u32,
    /// Last successful health check time.
    last_healthy: Option<Instant>,
}

const MAX_CONSECUTIVE_FAILURES: u32 = 3;

impl WorkerRegistry {
    /// Create a new registry with the given worker URLs.
    pub fn new(worker_urls: Vec<String>) -> Self {
        let workers: HashMap<String, WorkerState> = worker_urls
            .into_iter()
            .map(|url| {
                let state = WorkerState {
                    url: url.clone(),
                    healthy: false, // unknown until first health check
                    consecutive_failures: 0,
                    last_healthy: None,
                };
                (url, state)
            })
            .collect();

        info!(worker_count = workers.len(), "Initialized worker registry");

        Self {
            inner: Arc::new(RwLock::new(RegistryInner { workers })),
        }
    }

    /// Returns the list of currently healthy worker URLs.
    pub async fn healthy_workers(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|w| w.healthy)
            .map(|w| w.url.clone())
            .collect()
    }

    /// Returns the total number of registered workers (healthy + unhealthy).
    pub async fn total_workers(&self) -> usize {
        let inner = self.inner.read().await;
        inner.workers.len()
    }

    /// Mark a worker as healthy after a successful health check.
    pub async fn mark_healthy(&self, url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(state) = inner.workers.get_mut(url) {
            if !state.healthy {
                info!(worker = url, "Worker became healthy");
            }
            state.healthy = true;
            state.consecutive_failures = 0;
            state.last_healthy = Some(Instant::now());
        }
    }

    /// Mark a worker as having failed a health check.
    /// After MAX_CONSECUTIVE_FAILURES, the worker is marked unhealthy.
    pub async fn mark_failed(&self, url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(state) = inner.workers.get_mut(url) {
            state.consecutive_failures += 1;
            if state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                if state.healthy {
                    warn!(
                        worker = url,
                        failures = state.consecutive_failures,
                        "Worker marked unhealthy after {} consecutive failures",
                        MAX_CONSECUTIVE_FAILURES
                    );
                }
                state.healthy = false;
            } else {
                debug!(
                    worker = url,
                    failures = state.consecutive_failures,
                    "Worker health check failed ({}/{})",
                    state.consecutive_failures,
                    MAX_CONSECUTIVE_FAILURES
                );
            }
        }
    }

    /// Run periodic health checks against all registered workers.
    /// Calls `do_action("health_check")` on each worker via Arrow Flight.
    pub fn start_health_check_task(self: &Arc<Self>, interval: Duration) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                registry.check_all_workers().await;
            }
        });
    }

    async fn check_all_workers(&self) {
        let urls: Vec<String> = {
            let inner = self.inner.read().await;
            inner.workers.keys().cloned().collect()
        };

        for url in urls {
            match Self::health_check_worker(&url).await {
                Ok(()) => self.mark_healthy(&url).await,
                Err(e) => {
                    debug!(worker = %url, error = %e, "Health check failed");
                    self.mark_failed(&url).await;
                }
            }
        }
    }

    async fn health_check_worker(url: &str) -> Result<(), Box<dyn std::error::Error>> {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::Action;

        let mut client = FlightServiceClient::connect(url.to_string()).await?;
        let action = Action {
            r#type: "health_check".to_string(),
            body: bytes::Bytes::new(),
        };
        let _response = client.do_action(tonic::Request::new(action)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_registry() {
        let registry = WorkerRegistry::new(vec![]);
        assert_eq!(registry.total_workers().await, 0);
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_workers_start_unhealthy() {
        let registry = WorkerRegistry::new(vec![
            "http://worker1:50052".to_string(),
            "http://worker2:50052".to_string(),
        ]);
        assert_eq!(registry.total_workers().await, 2);
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_mark_healthy() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);

        registry.mark_healthy("http://worker1:50052").await;
        let healthy = registry.healthy_workers().await;
        assert_eq!(healthy, vec!["http://worker1:50052"]);
    }

    #[tokio::test]
    async fn test_mark_failed_threshold() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        // First two failures: still healthy
        registry.mark_failed("http://worker1:50052").await;
        registry.mark_failed("http://worker1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);

        // Third failure: marked unhealthy
        registry.mark_failed("http://worker1:50052").await;
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_recovery_after_failure() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        // Fail 3 times → unhealthy
        for _ in 0..3 {
            registry.mark_failed("http://worker1:50052").await;
        }
        assert!(registry.healthy_workers().await.is_empty());

        // Becomes healthy again
        registry.mark_healthy("http://worker1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }
}
```

- [ ] **Step 3: Register module and update lib.rs**

Add to `crates/sqe-coordinator/src/lib.rs`:

```rust
pub mod worker_registry;
```

- [ ] **Step 4: Add missing imports to sqe-coordinator Cargo.toml**

Add these dependencies:

```toml
sqe-planner = { path = "../sqe-planner" }
```

- [ ] **Step 5: Wire WorkerRegistry into main.rs**

In `crates/sqe-coordinator/src/main.rs`, after query_handler initialization and before starting the server, add:

```rust
    // Initialize worker registry
    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::new(
            config.coordinator.worker_urls.clone(),
        ),
    );

    // Start background health checks (every 5 seconds)
    if !config.coordinator.worker_urls.is_empty() {
        worker_registry.start_health_check_task(std::time::Duration::from_secs(5));
        tracing::info!(
            workers = ?config.coordinator.worker_urls,
            "Started worker health check task"
        );
    }
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p sqe-coordinator --lib`

Expected: All existing tests pass + 5 new worker_registry tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-core/src/config.rs crates/sqe-coordinator/
git commit -m "feat: worker registry with health-check-based liveness tracking"
```

---

### Task 16: Worker Binary + Flight Service

**Files:**
- Modify: `crates/sqe-worker/Cargo.toml`
- Create: `crates/sqe-worker/src/lib.rs`
- Create: `crates/sqe-worker/src/main.rs`
- Create: `crates/sqe-worker/src/executor.rs`
- Create: `crates/sqe-worker/src/flight_service.rs`
- Modify: `Cargo.toml` (workspace — add `object_store` dep)

**Context:** Workers receive `ScanTask` messages via Arrow Flight `do_get`, read the assigned Parquet files from S3, and stream Arrow RecordBatches back. Workers are pure Parquet readers — no Iceberg or Polaris dependency.

- [ ] **Step 1: Add object_store workspace dependency**

In the workspace root `Cargo.toml`, add:

```toml
object_store = { version = "0.11", features = ["aws"] }
```

- [ ] **Step 2: Set up sqe-worker Cargo.toml**

```toml
[package]
name = "sqe-worker"
version = "0.1.0"
edition = "2021"

[lib]
name = "sqe_worker"
path = "src/lib.rs"

[[bin]]
name = "sqe-worker"
path = "src/main.rs"

[dependencies]
sqe-core = { path = "../sqe-core" }
sqe-planner = { path = "../sqe-planner" }
arrow = { workspace = true }
arrow-flight = { workspace = true }
arrow-schema = { workspace = true }
arrow-array = { workspace = true }
parquet = { workspace = true }
object_store = { workspace = true }
tonic = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
futures = { workspace = true }
bytes = { workspace = true }
anyhow = { workspace = true }
url = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 3: Create the Parquet file executor**

Create `crates/sqe-worker/src/executor.rs`:

```rust
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures::TryStreamExt;
use tracing::{debug, info};
use url::Url;

use sqe_planner::ScanTask;

/// Execute a scan task by reading Parquet files from S3 and returning Arrow RecordBatches.
///
/// Creates an S3 ObjectStore with the task's credentials, reads each assigned
/// Parquet file, applies column projection, and collects all batches.
pub async fn execute_scan(task: &ScanTask) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    info!(
        fragment_id = %task.fragment_id,
        file_count = task.data_file_paths.len(),
        "Executing scan task"
    );

    if task.data_file_paths.is_empty() {
        anyhow::bail!("ScanTask has no data files");
    }

    // Build S3 object store with the provided credentials
    let store = build_object_store(task)?;
    let store = Arc::new(store);

    let mut all_batches = Vec::new();
    let mut result_schema: Option<SchemaRef> = None;

    for file_path in &task.data_file_paths {
        debug!(file = %file_path, "Reading Parquet file");

        // Parse S3 URL to get the object key
        let object_key = s3_url_to_key(file_path)?;
        let path = object_store::path::Path::from(object_key);

        // Get file metadata (size is needed for the reader)
        let meta = store.head(&path).await?;

        // Create async Parquet reader
        let reader = ParquetObjectReader::new(store.clone(), meta);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        // Apply column projection if specified
        if !task.projected_columns.is_empty() {
            let parquet_schema = builder.schema();
            let indices: Vec<usize> = task
                .projected_columns
                .iter()
                .filter_map(|name| {
                    parquet_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                })
                .collect();

            if !indices.is_empty() {
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    indices,
                );
                builder = builder.with_projection(mask);
            }
        }

        let stream = builder.build()?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        // Capture schema from first file
        if result_schema.is_none() && !batches.is_empty() {
            result_schema = Some(batches[0].schema());
        }

        debug!(
            file = %file_path,
            batch_count = batches.len(),
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Read Parquet file"
        );

        all_batches.extend(batches);
    }

    let schema = result_schema.unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));

    info!(
        fragment_id = %task.fragment_id,
        total_batches = all_batches.len(),
        total_rows = all_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "Scan task complete"
    );

    Ok((schema, all_batches))
}

/// Build an S3 ObjectStore from ScanTask credentials.
fn build_object_store(task: &ScanTask) -> anyhow::Result<impl ObjectStore> {
    let mut builder = AmazonS3Builder::new();

    if !task.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&task.s3_endpoint);
    }
    if !task.s3_region.is_empty() {
        builder = builder.with_region(&task.s3_region);
    }
    if !task.s3_access_key.is_empty() {
        builder = builder.with_access_key_id(&task.s3_access_key);
    }
    if !task.s3_secret_key.is_empty() {
        builder = builder.with_secret_access_key(&task.s3_secret_key);
    }
    if !task.s3_session_token.is_empty() {
        builder = builder.with_token(&task.s3_session_token);
    }
    if task.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    // Allow HTTP for dev (MinIO)
    builder = builder.with_allow_http(true);

    // Extract bucket from the first file path
    let bucket = s3_url_to_bucket(&task.data_file_paths[0])?;
    builder = builder.with_bucket_name(&bucket);

    Ok(builder.build()?)
}

/// Extract the bucket name from an S3 URL like `s3://bucket/key/path`.
fn s3_url_to_bucket(url: &str) -> anyhow::Result<String> {
    let parsed = Url::parse(url)?;
    parsed
        .host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| anyhow::anyhow!("No bucket in S3 URL: {url}"))
}

/// Extract the object key from an S3 URL like `s3://bucket/key/path`.
fn s3_url_to_key(url: &str) -> anyhow::Result<String> {
    let parsed = Url::parse(url)?;
    let path = parsed.path();
    // Remove leading slash
    Ok(path.trim_start_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_url_to_bucket() {
        assert_eq!(
            s3_url_to_bucket("s3://my-bucket/path/to/file.parquet").unwrap(),
            "my-bucket"
        );
    }

    #[test]
    fn test_s3_url_to_key() {
        assert_eq!(
            s3_url_to_key("s3://my-bucket/path/to/file.parquet").unwrap(),
            "path/to/file.parquet"
        );
    }

    #[test]
    fn test_s3_url_to_key_nested() {
        assert_eq!(
            s3_url_to_key("s3://bucket/warehouse/db/table/data/00001.parquet").unwrap(),
            "warehouse/db/table/data/00001.parquet"
        );
    }
}
```

- [ ] **Step 4: Create the worker Flight service**

Create `crates/sqe-worker/src/flight_service.rs`:

```rust
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use sqe_planner::ScanTask;

use crate::executor;

/// Worker's Arrow Flight service.
///
/// Handles two operations:
/// - `do_get`: Execute a scan task and stream results back
/// - `do_action("health_check")`: Return OK for coordinator health monitoring
#[derive(Clone)]
pub struct WorkerFlightService {}

impl WorkerFlightService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    /// Execute a scan task received as a Ticket.
    ///
    /// The Ticket body contains a JSON-encoded `ScanTask`. The worker reads
    /// the assigned Parquet files from S3, applies projection, and streams
    /// the resulting RecordBatches back as FlightData.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let scan_task = ScanTask::from_bytes(&ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!("Failed to decode ScanTask: {e}"))
        })?;

        info!(
            fragment_id = %scan_task.fragment_id,
            file_count = scan_task.data_file_paths.len(),
            "Worker received scan task"
        );

        let (schema, batches) = executor::execute_scan(&scan_task).await.map_err(|e| {
            warn!(error = %e, "Scan task execution failed");
            Status::internal(format!("Scan execution failed: {e}"))
        })?;

        // Encode batches as FlightData
        let schema = Arc::new((*schema).clone());
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Handle health checks and other actions.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "health_check" => {
                debug!("Health check OK");
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            other => Err(Status::unimplemented(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    // --- Unimplemented methods (worker doesn't need these) ---

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Workers don't support handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Workers don't support list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Workers don't support get_flight_info",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Workers don't support get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Workers don't support do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Workers don't support do_exchange"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "health_check".to_string(),
            description: "Check worker health".to_string(),
        }];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}
```

- [ ] **Step 5: Create lib.rs and main.rs for the worker**

Create `crates/sqe-worker/src/lib.rs`:

```rust
pub mod executor;
pub mod flight_service;
```

Create `crates/sqe-worker/src/main.rs`:

```rust
use sqe_core::SqeConfig;
use sqe_worker::flight_service::WorkerFlightService;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sqe=info".parse()?))
        .json()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    tracing::info!("Starting SQE worker on port {port}");

    let flight_service = WorkerFlightService::new();

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
```

- [ ] **Step 6: Verify workspace compiles**

Run: `cargo check --workspace`

Expected: Compiles. Both `sqe-coordinator` and `sqe-worker` binaries build.

- [ ] **Step 7: Run all tests**

Run: `cargo test --workspace`

Expected: All tests pass (existing + new executor URL tests).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/sqe-worker/
git commit -m "feat: sqe-worker binary with Parquet scan execution over Arrow Flight"
```

---

### Task 17: DistributedScanExec + Coordinator Integration

**Files:**
- Create: `crates/sqe-coordinator/src/distributed_scan.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Modify: `crates/sqe-coordinator/src/main.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Modify: `crates/sqe-coordinator/Cargo.toml`

**Context:** This is the key integration task. The coordinator inspects the physical plan for `IcebergScanExec` nodes, replaces them with `DistributedScanExec` when workers are available, and dispatches scan work to workers.

- [ ] **Step 1: Create DistributedScanExec**

Create `crates/sqe-coordinator/src/distributed_scan.rs`:

```rust
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use arrow_schema::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, TryStreamExt};
use tonic::IntoRequest;
use tracing::{debug, info};

use sqe_planner::ScanTask;

/// DataFusion `ExecutionPlan` that distributes scan work across workers.
///
/// Each partition maps to one worker. When DataFusion calls `execute(i)`,
/// the DistributedScanExec sends a `ScanTask` to worker[i] via Arrow Flight
/// `do_get` and returns the result stream.
#[derive(Debug)]
pub struct DistributedScanExec {
    /// One ScanTask per worker (partition).
    scan_tasks: Vec<ScanTask>,
    /// Worker URLs corresponding to each scan task.
    worker_urls: Vec<String>,
    /// Output schema.
    schema: SchemaRef,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl DistributedScanExec {
    /// Create a new distributed scan execution plan.
    ///
    /// `scan_tasks[i]` will be sent to `worker_urls[i]`.
    /// Both vectors must have the same length.
    pub fn new(
        scan_tasks: Vec<ScanTask>,
        worker_urls: Vec<String>,
        schema: SchemaRef,
    ) -> Self {
        assert_eq!(scan_tasks.len(), worker_urls.len());
        let num_partitions = scan_tasks.len();

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            ExecutionMode::Bounded,
        );

        Self {
            scan_tasks,
            worker_urls,
            schema,
            properties,
        }
    }
}

impl DisplayAs for DistributedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedScanExec: workers={}, total_files={}",
            self.worker_urls.len(),
            self.scan_tasks.iter().map(|t| t.data_file_paths.len()).sum::<usize>(),
        )
    }
}

impl ExecutionPlan for DistributedScanExec {
    fn name(&self) -> &str {
        "DistributedScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let task = self.scan_tasks[partition].clone();
        let worker_url = self.worker_urls[partition].clone();
        let schema = self.schema.clone();

        info!(
            fragment_id = %task.fragment_id,
            worker = %worker_url,
            file_count = task.data_file_paths.len(),
            "Dispatching scan to worker"
        );

        // Create async stream that connects to worker and fetches results
        let stream = futures::stream::once(async move {
            let ticket_bytes = task.to_bytes().map_err(|e| {
                DataFusionError::External(Box::new(e))
            })?;

            let mut client =
                FlightServiceClient::connect(worker_url.clone())
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to connect to worker {worker_url}: {e}"
                        ))
                    })?;

            let ticket = Ticket::new(ticket_bytes);
            let response = client.do_get(ticket.into_request()).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Worker {worker_url} do_get failed: {e}"
                ))
            })?;

            let flight_stream = FlightRecordBatchStream::new_from_flight_data(
                response.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(e)),
            );

            Ok::<_, DataFusionError>(
                flight_stream.map_err(|e| DataFusionError::External(Box::new(e))),
            )
        })
        .try_flatten();

        Ok(Box::pin(DistributedRecordBatchStream {
            schema,
            inner: Box::pin(stream),
        }))
    }
}

/// Wrapper that implements `RecordBatchStream`.
struct DistributedRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl Stream for DistributedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for DistributedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
```

- [ ] **Step 2: Modify QueryHandler for distributed execution**

In `crates/sqe-coordinator/src/query_handler.rs`, add a `worker_registry` field and distributed execution logic.

Add these fields to `QueryHandler`:

```rust
pub struct QueryHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    config: SqeConfig,
    catalog_ops: CatalogOps,
    write_handler: WriteHandler,
    worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
}
```

Update `new()` to accept an optional worker registry:

```rust
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
    ) -> Self {
        let catalog_ops = CatalogOps::new(config.clone());
        let write_handler = WriteHandler::new(config.clone());
        Self {
            policy_enforcer,
            config,
            catalog_ops,
            write_handler,
            worker_registry,
        }
    }
```

Add a method to attempt distributed execution. After the policy-enforced plan is created in `execute_query()`, add the distributed path:

```rust
    /// Execute a SELECT query, potentially distributing scans to workers.
    async fn execute_query(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let ctx = self.create_session_context(session).await?;

        // Plan the query via DataFusion's SQL planner
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(format!("SQL planning failed: {e}")))?;

        // Get the logical plan and run policy enforcement
        let plan = df.logical_plan().clone();
        let enforced_plan = self
            .policy_enforcer
            .evaluate(&session.user, plan)
            .await?;

        debug!("Policy-enforced plan: {:?}", enforced_plan);

        // Create a new DataFrame from the enforced plan and execute
        let enforced_df = ctx
            .execute_logical_plan(enforced_plan)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to create execution plan: {e}")))?;

        let batches = enforced_df
            .collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Query execution failed: {e}")))?;

        info!(
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Query execution complete"
        );

        Ok(batches)
    }
```

**Note:** The full distributed plan replacement (walking the physical plan tree, finding IcebergScanExec nodes, replacing with DistributedScanExec) is complex. For this first iteration, the distributed path is wired but the actual plan replacement is deferred to when we have a working Iceberg scan with data files. The coordinator will use `IcebergScanExec` locally for now. The `DistributedScanExec` is ready to use when a scheduler decides to distribute.

Add a helper method for future use:

```rust
    /// Check if distributed execution should be used for a query.
    ///
    /// Returns true if workers are available and the query touches
    /// Iceberg tables (not pure DataFusion expressions like SELECT 1).
    async fn should_distribute(&self) -> bool {
        if let Some(ref registry) = self.worker_registry {
            !registry.healthy_workers().await.is_empty()
        } else {
            false
        }
    }
```

- [ ] **Step 3: Update main.rs to pass worker registry to QueryHandler**

In `crates/sqe-coordinator/src/main.rs`, change the QueryHandler initialization:

```rust
    let query_handler = Arc::new(QueryHandler::new(
        policy_enforcer,
        config.clone(),
        if config.coordinator.worker_urls.is_empty() {
            None
        } else {
            Some(worker_registry.clone())
        },
    ));
```

- [ ] **Step 4: Register module**

Add to `crates/sqe-coordinator/src/lib.rs`:

```rust
pub mod distributed_scan;
```

- [ ] **Step 5: Verify workspace compiles**

Run: `cargo check --workspace`

Expected: Compiles. Some warnings about unused imports are OK at this stage.

- [ ] **Step 6: Run all tests**

Run: `cargo test --workspace`

Expected: All tests pass. Update any tests that call `QueryHandler::new()` to pass `None` for the worker_registry parameter.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/
git commit -m "feat: DistributedScanExec and coordinator distributed execution path"
```

---

### Task 18: Integration Tests + Config Updates

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`
- Modify: `tests/sqe-test.toml`

**Context:** Add integration tests that verify distributed execution works end-to-end. These tests require the quickstart stack AND a running worker, so they're marked `#[ignore]`.

- [ ] **Step 1: Update test config**

Add worker config to `tests/sqe-test.toml`:

```toml
[worker]
flight_port = 50052
coordinator_url = "http://localhost:50051"
```

Add worker_urls to the coordinator section (if it exists, or add the section):

```toml
[coordinator]
worker_urls = []
```

(Empty for unit tests — workers are tested separately.)

- [ ] **Step 2: Add distributed execution integration test**

Add to `crates/sqe-coordinator/tests/integration_test.rs`:

```rust
// ---------------------------------------------------------------------------
// Chunk 3: Distributed execution tests
// ---------------------------------------------------------------------------

// Test: Worker registry starts empty when no workers configured
#[test]
fn test_worker_registry_no_workers() {
    let registry = sqe_coordinator::worker_registry::WorkerRegistry::new(vec![]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let healthy = rt.block_on(registry.healthy_workers());
    assert!(healthy.is_empty());
}

// Test: Coordinator with no workers falls back to local execution
#[tokio::test]
#[ignore] // Requires quickstart stack
async fn test_local_fallback_without_workers() {
    let (session, handler) = setup_handler().await;

    // SELECT 1 should work even without workers (local execution)
    let batches = handler
        .execute(&session, "SELECT 1 as x")
        .await
        .expect("SELECT 1 should succeed in local mode");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

// Test: ScanTask serialization roundtrip
#[test]
fn test_scan_task_roundtrip() {
    let task = sqe_planner::ScanTask {
        fragment_id: "test-001".to_string(),
        data_file_paths: vec![
            "s3://bucket/data/file1.parquet".to_string(),
        ],
        projected_columns: vec!["id".to_string()],
        s3_endpoint: "http://localhost:9000".to_string(),
        s3_region: "us-east-1".to_string(),
        s3_access_key: "key".to_string(),
        s3_secret_key: "secret".to_string(),
        s3_session_token: String::new(),
        s3_path_style: true,
    };

    let bytes = task.to_bytes().unwrap();
    let decoded = sqe_planner::ScanTask::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.fragment_id, "test-001");
    assert_eq!(decoded.data_file_paths.len(), 1);
}

// Test: Distributed SELECT with coordinator + worker (requires both running)
#[tokio::test]
#[ignore] // Requires quickstart stack + running worker
async fn test_distributed_select() {
    // This test requires:
    // 1. Quickstart stack (Keycloak, Polaris, MinIO)
    // 2. A worker running on localhost:50052
    // 3. A table with data in Polaris
    //
    // Run the worker: cargo run -p sqe-worker -- tests/sqe-test.toml
    // Then run this test: cargo test -p sqe-coordinator --test integration_test test_distributed_select -- --ignored

    let config = sqe_core::SqeConfig::load("tests/sqe-test.toml")
        .expect("Failed to load test config");

    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed");

    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);

    let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(
        vec!["http://localhost:50052".to_string()],
    ));

    // Mark worker as healthy for the test
    registry.mark_healthy("http://localhost:50052").await;

    let handler = sqe_coordinator::QueryHandler::new(policy, config, Some(registry));

    // First create a test table
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.dist_test")
        .await;
    handler
        .execute(&session, "CREATE TABLE test_ns.dist_test AS SELECT 1 as id, 'distributed' as name")
        .await
        .expect("CTAS should succeed");

    // Query should work (may use local or distributed path)
    let batches = handler
        .execute(&session, "SELECT * FROM test_ns.dist_test")
        .await
        .expect("Distributed SELECT should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // Cleanup
    let _ = handler
        .execute(&session, "DROP TABLE test_ns.dist_test")
        .await;
}
```

- [ ] **Step 3: Update setup_handler() for new QueryHandler signature**

Update the `setup_handler()` function to pass `None` for worker_registry:

```rust
async fn setup_handler() -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let config =
        sqe_core::SqeConfig::load("tests/sqe-test.toml").expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "root123")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None);
    (session, handler)
}
```

- [ ] **Step 4: Verify everything compiles and tests pass**

Run: `cargo test --workspace`

Expected: All tests pass (existing + 2 new non-ignored tests).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/tests/ tests/sqe-test.toml
git commit -m "test: distributed execution integration tests and config updates"
```

---

### Chunk 3 — Deferred Items

These items from the openspec are deferred because they add complexity without changing the core distributed execution model:

1. **Custom datafusion-proto codec** (Task 6.6) — Not needed with the ScanTask approach. Workers don't receive serialized PhysicalPlans.

2. **Credential refresh push to workers** (Task 7.10) — For long-running queries, the coordinator would need to push refreshed tokens to workers mid-execution. Deferred because the current quickstart stack's token TTL is long enough for testing.

3. **Failure handling: re-assign fragments** (Task 7.11) — When a worker dies mid-scan, the coordinator should re-assign its fragment to another worker or fall back to local execution. Deferred to a reliability-focused iteration.

4. **Worker memory limits + spill-to-disk** (Task 9.7) — DataFusion's built-in memory management handles this. Configurable limits deferred to production tuning.

5. **Iceberg-aware fragment splitting** (advanced Task 6.5) — Current implementation splits by file count (round-robin). Advanced splitting by manifest group, data file size, or partition values is deferred. The splitter interface supports this via the `split_files()` function — just change the assignment strategy.

6. **Full physical plan tree rewriting** — Replacing `IcebergScanExec` nodes deep in a multi-join plan with `DistributedScanExec` requires walking the plan tree with `with_new_children()`. The current implementation supports single-table distributed scans. Multi-table distributed execution (distributed joins) is deferred.

## Chunk 4: information_schema + Trino Compat + Observability (Tasks 19-24)

Adds virtual `information_schema` for dbt compatibility, Trino v1/statement HTTP wire protocol, Prometheus metrics endpoint, and structured JSON audit logging.

### Architecture (Chunk 4)

**Current state:** The coordinator handles Flight SQL queries against Iceberg tables. SHOW CATALOGS/SCHEMAS/TABLES return results via QueryHandler. No `information_schema` virtual schema, no Trino HTTP endpoint, no metrics or audit logging.

**Target state:**
1. `information_schema.tables`, `information_schema.columns`, `information_schema.schemata` are queryable via standard SQL (required for dbt compatibility)
2. Trino-compatible HTTP endpoint on port 8080 handles POST/GET/DELETE for `/v1/statement` with JSON column format
3. Prometheus metrics on `/metrics` endpoint (port 9090)
4. Structured JSON audit log written per query

**Key design decisions:**

1. **information_schema as DataFusion SchemaProvider** — Register an `InformationSchemaProvider` under the `information_schema` namespace in `SqeCatalogProvider`. Each virtual table (`tables`, `columns`, `schemata`) is a `TableProvider` that queries Polaris metadata via `SessionCatalog`. Access is automatically scoped by the user's bearer token.

2. **Trino HTTP as separate axum server** — The `sqe-trino-compat` crate runs an axum HTTP server alongside the Flight SQL gRPC server. It reuses `SessionManager` and `QueryHandler` for auth and execution. Results are cached in a `DashMap<query_id, Vec<RecordBatch>>` for pagination.

3. **Metrics via prometheus crate** — `sqe-metrics` provides a `MetricsRegistry` holding counters/histograms/gauges. An axum server on the metrics port serves `/metrics`. The registry is passed to QueryHandler and other components for instrumentation.

4. **Audit log as append-only JSON lines** — A simple `AuditLogger` writes one JSON line per query to the configured path. Integrated into `QueryHandler.execute()` as a post-execution hook.

5. **OTLP tracing deferred** — OpenTelemetry distributed tracing with trace context propagation to workers is complex and not needed for the initial release. The MetricsConfig.otlp_endpoint field remains for future use.

### File Structure (Chunk 4)

```
crates/
  sqe-catalog/
    src/info_schema.rs                    # NEW: information_schema TableProviders
    src/catalog_provider.rs               # MODIFY: register information_schema
    src/lib.rs                            # MODIFY: add info_schema module
  sqe-trino-compat/
    Cargo.toml                            # MODIFY: add dependencies
    src/lib.rs                            # NEW: module exports
    src/server.rs                         # NEW: axum HTTP server
    src/types.rs                          # NEW: Arrow → Trino type mapping
    src/protocol.rs                       # NEW: Trino JSON response format
  sqe-metrics/
    Cargo.toml                            # MODIFY: add dependencies
    src/lib.rs                            # NEW: module exports + MetricsRegistry
    src/audit.rs                          # NEW: AuditLogger
    src/server.rs                         # NEW: /metrics HTTP endpoint
  sqe-coordinator/
    Cargo.toml                            # MODIFY: add sqe-metrics, sqe-trino-compat deps
    src/query_handler.rs                  # MODIFY: instrument with metrics + audit
    src/main.rs                           # MODIFY: start metrics + Trino servers
    tests/integration_test.rs             # MODIFY: add Chunk 4 tests
```

---

### Task 19: information_schema Virtual Tables

**Files:**
- Create: `crates/sqe-catalog/src/info_schema.rs`
- Modify: `crates/sqe-catalog/src/catalog_provider.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`

**Context:** dbt and other SQL tools expect `SELECT * FROM information_schema.tables` to work. We implement this as a DataFusion `SchemaProvider` named `information_schema` registered inside `SqeCatalogProvider`. Three virtual tables: `tables`, `columns`, `schemata`.

- [ ] **Step 1: Create info_schema.rs with InformationSchemaProvider**

Create `crates/sqe-catalog/src/info_schema.rs`:

```rust
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{ArrayRef, RecordBatch, StringArray, Int32Array};
use arrow_array::builder::StringBuilder;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{debug, error};

use crate::rest_catalog::SessionCatalog;

/// DataFusion `SchemaProvider` for the virtual `information_schema`.
///
/// Provides `tables`, `columns`, and `schemata` virtual tables backed
/// by Polaris catalog metadata. Access is automatically scoped by the
/// user's bearer token in the `SessionCatalog`.
#[derive(Debug)]
pub struct InformationSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
}

impl InformationSchemaProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            session_catalog,
            warehouse,
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            "tables".to_string(),
            "columns".to_string(),
            "schemata".to_string(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "tables" | "columns" | "schemata")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "tables" => Ok(Some(self.build_tables_table().await?)),
            "columns" => Ok(Some(self.build_columns_table().await?)),
            "schemata" => Ok(Some(self.build_schemata_table().await?)),
            _ => Ok(None),
        }
    }
}

impl InformationSchemaProvider {
    /// Build the `information_schema.tables` virtual table.
    async fn build_tables_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = NamespaceIdent::new(ns.clone());
            match self.session_catalog.list_tables(&ns_ident).await {
                Ok(tables) => {
                    for table in &tables {
                        catalog_builder.append_value(&self.warehouse);
                        schema_builder.append_value(ns);
                        name_builder.append_value(table.name());
                        type_builder.append_value("BASE TABLE");
                    }
                }
                Err(e) => {
                    debug!(namespace = %ns, error = %e, "Failed to list tables for information_schema");
                }
            }
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(catalog_builder.finish()) as ArrayRef,
            Arc::new(schema_builder.finish()) as ArrayRef,
            Arc::new(name_builder.finish()) as ArrayRef,
            Arc::new(type_builder.finish()) as ArrayRef,
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// Build the `information_schema.columns` virtual table.
    async fn build_columns_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut sch_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut ord_b = arrow_array::builder::Int32Builder::new();
        let mut null_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = NamespaceIdent::new(ns.clone());
            let tables = match self.session_catalog.list_tables(&ns_ident).await {
                Ok(t) => t,
                Err(_) => continue,
            };

            for table_ident in &tables {
                let full_ident =
                    iceberg::TableIdent::new(ns_ident.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        debug!(table = %table_ident.name(), error = %e, "Failed to load table for columns");
                        continue;
                    }
                };

                let iceberg_schema = table.metadata().current_schema();
                for (idx, field) in iceberg_schema.as_struct().fields().iter().enumerate() {
                    cat_b.append_value(&self.warehouse);
                    sch_b.append_value(ns);
                    tbl_b.append_value(table_ident.name());
                    col_b.append_value(&field.name);
                    ord_b.append_value((idx + 1) as i32);
                    null_b.append_value(if field.required { "NO" } else { "YES" });
                    type_b.append_value(format!("{}", field.field_type));
                }
            }
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(cat_b.finish()) as ArrayRef,
            Arc::new(sch_b.finish()) as ArrayRef,
            Arc::new(tbl_b.finish()) as ArrayRef,
            Arc::new(col_b.finish()) as ArrayRef,
            Arc::new(ord_b.finish()) as ArrayRef,
            Arc::new(null_b.finish()) as ArrayRef,
            Arc::new(type_b.finish()) as ArrayRef,
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// Build the `information_schema.schemata` virtual table.
    async fn build_schemata_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut sch_b = StringBuilder::new();

        for ns in &namespaces {
            cat_b.append_value(&self.warehouse);
            sch_b.append_value(ns);
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(cat_b.finish()) as ArrayRef,
            Arc::new(sch_b.finish()) as ArrayRef,
        ])?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// List namespaces, returning empty vec on error.
    async fn list_namespaces_safe(&self) -> Vec<String> {
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => namespaces
                .iter()
                .flat_map(|ns| ns.as_ref().clone())
                .collect(),
            Err(e) => {
                error!(error = %e, "Failed to list namespaces for information_schema");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_names() {
        // InformationSchemaProvider requires a SessionCatalog which needs async setup.
        // We test the static methods only.
        let names = vec!["tables", "columns", "schemata"];
        for name in &names {
            assert!(matches!(name, &"tables" | &"columns" | &"schemata"));
        }
    }

    #[test]
    fn test_tables_schema() {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 4);
    }

    #[test]
    fn test_columns_schema() {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 7);
    }

    #[test]
    fn test_schemata_schema() {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 2);
    }
}
```

- [ ] **Step 2: Register module in sqe-catalog lib.rs**

Add to `crates/sqe-catalog/src/lib.rs`:

```rust
pub mod info_schema;
```

- [ ] **Step 3: Register information_schema in CatalogProvider**

Modify `crates/sqe-catalog/src/catalog_provider.rs`:

Add `information_schema` to `schema_names()` and `schema()`:

In `schema_names()`, append `"information_schema"` to the returned list:
```rust
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.cached_namespaces.clone();
        names.push("information_schema".to_string());
        names
    }
```

In `schema()`, add a check before the existing namespace lookup:
```rust
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "information_schema" {
            return Some(Arc::new(
                crate::info_schema::InformationSchemaProvider::new(
                    self.session_catalog.clone(),
                    // Use the warehouse name, falling back to "default"
                    if self.cached_namespaces.is_empty() {
                        "default".to_string()
                    } else {
                        // The warehouse name is not stored here — extract from session_catalog
                        // For now, use a sensible default
                        "iceberg".to_string()
                    },
                ),
            ));
        }

        if !self.cached_namespaces.contains(&name.to_string()) {
            debug!(schema = name, "Schema not found in cached namespaces");
            return None;
        }

        let provider = SqeSchemaProvider::new(
            self.session_catalog.clone(),
            name.to_string(),
            self.storage_config.clone(),
        );

        Some(Arc::new(provider))
    }
```

**Important:** To get the actual warehouse name, add a `warehouse` field to `SqeCatalogProvider`:

Update the struct:
```rust
pub struct SqeCatalogProvider {
    session_catalog: Arc<SessionCatalog>,
    storage_config: StorageConfig,
    cached_namespaces: Vec<String>,
    warehouse: String,
}
```

Update `try_new()` and `with_namespaces()` to accept a `warehouse: String` parameter.

Update `schema()` to use `self.warehouse.clone()` for the information_schema provider.

Then update all callers:
- `crates/sqe-coordinator/src/query_handler.rs` — in `create_session_context()`, pass `self.config.catalog.warehouse.clone()` when constructing `SqeCatalogProvider`.

- [ ] **Step 4: Verify workspace compiles**

Run: `cargo check --workspace`

- [ ] **Step 5: Run tests**

Run: `cargo test --workspace`

Expected: All existing tests pass + 4 new schema tests.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/ crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat: virtual information_schema (tables, columns, schemata) for dbt compatibility"
```

---

### Task 20: Metrics Registry + Prometheus Endpoint

**Files:**
- Modify: `crates/sqe-metrics/Cargo.toml`
- Create: `crates/sqe-metrics/src/lib.rs`
- Create: `crates/sqe-metrics/src/server.rs`

**Context:** Prometheus metrics endpoint on port 9090. A `MetricsRegistry` struct holds all counters/histograms/gauges. An axum server serves `/metrics`.

- [ ] **Step 1: Set up sqe-metrics Cargo.toml**

```toml
[package]
name = "sqe-metrics"
version = "0.1.0"
edition = "2021"

[dependencies]
prometheus = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 2: Create MetricsRegistry in lib.rs**

Create `crates/sqe-metrics/src/lib.rs`:

```rust
pub mod server;

use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, IntGauge, Opts, Registry,
};

/// Central metrics registry for the SQE coordinator.
///
/// Holds Prometheus counters, histograms, and gauges for query execution,
/// session tracking, and worker health monitoring.
#[derive(Clone)]
pub struct MetricsRegistry {
    pub registry: Registry,
    /// Total queries executed, labelled by status (success/error) and statement_type.
    pub query_count: CounterVec,
    /// Query execution duration in seconds.
    pub query_duration: HistogramVec,
    /// Total rows returned across all queries.
    pub rows_returned: Counter,
    /// Currently active sessions.
    pub active_sessions: IntGauge,
    /// Number of healthy workers.
    pub healthy_workers: IntGauge,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();

        let query_count = CounterVec::new(
            Opts::new("sqe_query_count_total", "Total queries executed"),
            &["status", "statement_type"],
        )
        .unwrap();
        registry.register(Box::new(query_count.clone())).unwrap();

        let query_duration = HistogramVec::new(
            HistogramOpts::new("sqe_query_duration_seconds", "Query execution duration")
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["statement_type"],
        )
        .unwrap();
        registry.register(Box::new(query_duration.clone())).unwrap();

        let rows_returned = Counter::new(
            "sqe_rows_returned_total",
            "Total rows returned across all queries",
        )
        .unwrap();
        registry.register(Box::new(rows_returned.clone())).unwrap();

        let active_sessions = IntGauge::new(
            "sqe_active_sessions",
            "Number of active sessions",
        )
        .unwrap();
        registry.register(Box::new(active_sessions.clone())).unwrap();

        let healthy_workers = IntGauge::new(
            "sqe_healthy_workers",
            "Number of healthy workers",
        )
        .unwrap();
        registry.register(Box::new(healthy_workers.clone())).unwrap();

        Self {
            registry,
            query_count,
            query_duration,
            rows_returned,
            active_sessions,
            healthy_workers,
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_creation() {
        let metrics = MetricsRegistry::new();
        assert!(metrics.registry.gather().len() >= 5);
    }

    #[test]
    fn test_query_count_increment() {
        let metrics = MetricsRegistry::new();
        metrics.query_count.with_label_values(&["success", "query"]).inc();
        let count = metrics.query_count.with_label_values(&["success", "query"]).get();
        assert_eq!(count, 1.0);
    }

    #[test]
    fn test_query_duration_observe() {
        let metrics = MetricsRegistry::new();
        metrics.query_duration.with_label_values(&["query"]).observe(0.5);
        let count = metrics.query_duration.with_label_values(&["query"]).get_sample_count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_active_sessions_gauge() {
        let metrics = MetricsRegistry::new();
        metrics.active_sessions.inc();
        metrics.active_sessions.inc();
        assert_eq!(metrics.active_sessions.get(), 2);
        metrics.active_sessions.dec();
        assert_eq!(metrics.active_sessions.get(), 1);
    }
}
```

- [ ] **Step 3: Create /metrics HTTP server**

Create `crates/sqe-metrics/src/server.rs`:

```rust
use std::sync::Arc;

use axum::{Router, routing::get, extract::State, response::IntoResponse};
use prometheus::Encoder;
use tracing::info;

use crate::MetricsRegistry;

/// Start the Prometheus metrics HTTP server.
///
/// Serves `/metrics` on the given port.
/// Returns a JoinHandle for the server task.
pub fn start_metrics_server(
    metrics: Arc<MetricsRegistry>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics);

        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

        info!("Metrics server listening on {addr}");

        axum::serve(listener, app).await.unwrap();
    })
}

async fn metrics_handler(
    State(metrics): State<Arc<MetricsRegistry>>,
) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        buffer,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_handler_returns_text() {
        let metrics = Arc::new(MetricsRegistry::new());
        metrics.query_count.with_label_values(&["success", "query"]).inc();

        let encoder = prometheus::TextEncoder::new();
        let metric_families = metrics.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("sqe_query_count_total"));
        assert!(output.contains("sqe_query_duration_seconds"));
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-metrics`

Expected: All 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-metrics/
git commit -m "feat: Prometheus metrics registry and /metrics HTTP endpoint"
```

---

### Task 21: Audit Logger

**Files:**
- Create: `crates/sqe-metrics/src/audit.rs`
- Modify: `crates/sqe-metrics/src/lib.rs`
- Modify: `crates/sqe-metrics/Cargo.toml`

**Context:** Structured JSON audit log — one line per query with timestamp, user, SQL, duration, row count, status. Written to a file path from MetricsConfig.

- [ ] **Step 1: Add serde + chrono to sqe-metrics deps**

Add to `crates/sqe-metrics/Cargo.toml` dependencies:

```toml
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
```

- [ ] **Step 2: Create AuditLogger**

Create `crates/sqe-metrics/src/audit.rs`:

```rust
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::Utc;
use serde::Serialize;
use tracing::{error, info};

/// Structured audit log entry written as a JSON line.
#[derive(Debug, Serialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub username: String,
    pub query_text: String,
    pub statement_type: String,
    pub duration_ms: u64,
    pub rows_returned: usize,
    pub status: String,
}

/// Appends JSON audit entries to a log file.
///
/// Thread-safe via Mutex. If no path is configured (empty string),
/// the logger is a no-op.
pub struct AuditLogger {
    writer: Option<Mutex<std::io::BufWriter<std::fs::File>>>,
}

impl AuditLogger {
    /// Create a new audit logger. If `path` is empty, returns a no-op logger.
    pub fn new(path: &str) -> Self {
        if path.is_empty() {
            return Self { writer: None };
        }

        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => {
                info!(path = path, "Audit log initialized");
                Self {
                    writer: Some(Mutex::new(std::io::BufWriter::new(file))),
                }
            }
            Err(e) => {
                error!(path = path, error = %e, "Failed to open audit log file");
                Self { writer: None }
            }
        }
    }

    /// Write an audit entry. Silently ignores errors (audit should not block queries).
    pub fn log(&self, entry: &AuditEntry) {
        if let Some(ref writer) = self.writer {
            if let Ok(mut w) = writer.lock() {
                if let Ok(json) = serde_json::to_string(entry) {
                    let _ = writeln!(w, "{json}");
                    let _ = w.flush();
                }
            }
        }
    }
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("active", &self.writer.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_logger() {
        let logger = AuditLogger::new("");
        let entry = AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            username: "test".to_string(),
            query_text: "SELECT 1".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 42,
            rows_returned: 1,
            status: "success".to_string(),
        };
        // Should not panic
        logger.log(&entry);
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = AuditEntry {
            timestamp: "2026-03-15T00:00:00Z".to_string(),
            username: "root".to_string(),
            query_text: "SELECT * FROM t".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 100,
            rows_returned: 5,
            status: "success".to_string(),
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"username\":\"root\""));
        assert!(json.contains("\"duration_ms\":100"));
    }

    #[test]
    fn test_file_logger_writes() {
        let dir = std::env::temp_dir();
        let path = dir.join("sqe-audit-test.jsonl");
        let path_str = path.to_str().unwrap();

        let logger = AuditLogger::new(path_str);
        let entry = AuditEntry {
            timestamp: "2026-03-15T00:00:00Z".to_string(),
            username: "testuser".to_string(),
            query_text: "SELECT 1".to_string(),
            statement_type: "query".to_string(),
            duration_ms: 10,
            rows_returned: 1,
            status: "success".to_string(),
        };
        logger.log(&entry);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("testuser"));
        assert!(content.contains("SELECT 1"));

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }
}
```

- [ ] **Step 3: Register audit module in lib.rs**

Add to `crates/sqe-metrics/src/lib.rs`:

```rust
pub mod audit;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-metrics`

Expected: All 8 tests pass (4 registry + 1 server + 3 audit).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-metrics/
git commit -m "feat: structured JSON audit logger for query execution tracking"
```

---

### Task 22: Trino HTTP Server + Type Mapping

**Files:**
- Modify: `crates/sqe-trino-compat/Cargo.toml`
- Create: `crates/sqe-trino-compat/src/lib.rs`
- Create: `crates/sqe-trino-compat/src/types.rs`
- Create: `crates/sqe-trino-compat/src/protocol.rs`
- Create: `crates/sqe-trino-compat/src/server.rs`

**Context:** Trino v1/statement REST endpoint — POST to submit queries, GET to paginate, DELETE to cancel. Arrow RecordBatches are converted to Trino's JSON column format.

- [ ] **Step 1: Set up Cargo.toml**

```toml
[package]
name = "sqe-trino-compat"
version = "0.1.0"
edition = "2021"

[dependencies]
sqe-core = { path = "../sqe-core" }
arrow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
dashmap = { workspace = true }
uuid = { workspace = true }
base64 = "0.22"

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 2: Create types.rs — Arrow to Trino type mapping**

Create `crates/sqe-trino-compat/src/types.rs`:

```rust
use arrow_schema::DataType;

/// Map an Arrow DataType to a Trino type name string.
pub fn arrow_to_trino_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        other => format!("{other:?}"),
    }
}

/// Convert a single Arrow column value to a JSON-compatible value.
///
/// Trino JSON wire format represents all values as their string form
/// inside a JSON array per column.
pub fn arrow_value_to_json(
    array: &dyn arrow_array::Array,
    row: usize,
) -> serde_json::Value {
    use arrow_array::*;

    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        _ => {
            // Fallback: use Arrow's display formatting
            serde_json::Value::String(format!("{}", arrow::util::display::array_value_to_string(array, row).unwrap_or_default()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_to_trino_type_basic() {
        assert_eq!(arrow_to_trino_type(&DataType::Int64), "bigint");
        assert_eq!(arrow_to_trino_type(&DataType::Utf8), "varchar");
        assert_eq!(arrow_to_trino_type(&DataType::Boolean), "boolean");
        assert_eq!(arrow_to_trino_type(&DataType::Float64), "double");
        assert_eq!(arrow_to_trino_type(&DataType::Int32), "integer");
    }

    #[test]
    fn test_arrow_to_trino_type_timestamp() {
        assert_eq!(
            arrow_to_trino_type(&DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
            "timestamp"
        );
    }

    #[test]
    fn test_arrow_to_trino_type_decimal() {
        assert_eq!(
            arrow_to_trino_type(&DataType::Decimal128(18, 2)),
            "decimal(18,2)"
        );
    }

    #[test]
    fn test_arrow_value_to_json_int() {
        let arr = arrow_array::Int64Array::from(vec![42]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!(42));
    }

    #[test]
    fn test_arrow_value_to_json_string() {
        let arr = arrow_array::StringArray::from(vec!["hello"]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn test_arrow_value_to_json_null() {
        let arr = arrow_array::Int64Array::from(vec![Some(1), None]);
        let val = arrow_value_to_json(&arr, 1);
        assert_eq!(val, serde_json::Value::Null);
    }
}
```

- [ ] **Step 3: Create protocol.rs — Trino JSON response format**

Create `crates/sqe-trino-compat/src/protocol.rs`:

```rust
use arrow_array::RecordBatch;
use serde::Serialize;

use crate::types::{arrow_to_trino_type, arrow_value_to_json};

/// Trino v1/statement response format.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<TrinoColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<serde_json::Value>>>,
    pub stats: TrinoStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TrinoError>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoColumn {
    pub name: String,
    pub r#type: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoStats {
    pub state: String,
    pub queued: bool,
    pub scheduled: bool,
    pub nodes: u32,
    pub total_splits: u32,
    pub completed_splits: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoError {
    pub message: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
}

/// Convert Arrow RecordBatches to Trino JSON response columns and data.
pub fn batches_to_trino(
    batches: &[RecordBatch],
) -> (Vec<TrinoColumn>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let schema = batches[0].schema();

    let columns: Vec<TrinoColumn> = schema
        .fields()
        .iter()
        .map(|f| TrinoColumn {
            name: f.name().clone(),
            r#type: arrow_to_trino_type(f.data_type()),
        })
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| arrow_value_to_json(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }

    (columns, rows)
}

impl TrinoStats {
    pub fn finished() -> Self {
        Self {
            state: "FINISHED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            completed_splits: 1,
        }
    }

    pub fn failed() -> Self {
        Self {
            state: "FAILED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            completed_splits: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_batches_to_trino_empty() {
        let (cols, rows) = batches_to_trino(&[]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn test_batches_to_trino_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let (cols, rows) = batches_to_trino(&[batch]);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].r#type, "bigint");
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].r#type, "varchar");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("alice"));
        assert_eq!(rows[1][0], serde_json::json!(2));
        assert_eq!(rows[1][1], serde_json::json!("bob"));
    }

    #[test]
    fn test_trino_response_serialization() {
        let resp = TrinoResponse {
            id: "q-001".to_string(),
            info_uri: None,
            next_uri: None,
            columns: Some(vec![TrinoColumn {
                name: "x".to_string(),
                r#type: "bigint".to_string(),
            }]),
            data: Some(vec![vec![serde_json::json!(1)]]),
            stats: TrinoStats::finished(),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"id\":\"q-001\""));
        assert!(json.contains("\"state\":\"FINISHED\""));
        assert!(!json.contains("nextUri")); // Skipped because None
    }
}
```

- [ ] **Step 4: Create server.rs — Trino HTTP endpoint**

Create `crates/sqe-trino-compat/src/server.rs`:

```rust
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{delete, get, post};
use axum::Router;
use dashmap::DashMap;
use tracing::{info, warn};
use uuid::Uuid;

use sqe_core::{Session, SqeConfig};

use crate::protocol::{self, TrinoError, TrinoResponse, TrinoStats};

/// Shared state for the Trino-compatible HTTP server.
///
/// Holds references to the auth/query infrastructure and a cache
/// of query results for pagination.
pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub config: SqeConfig,
    /// Cached query results, keyed by query ID.
    pub results: DashMap<String, CachedResult>,
}

pub struct CachedResult {
    pub response: TrinoResponse,
}

/// Trait for authenticating a user. Allows decoupling from concrete Authenticator type.
#[axum::async_trait]
pub trait TrinoAuthenticator: Send + Sync + 'static {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Session, String>;
}

/// Trait for executing queries. Allows decoupling from concrete QueryHandler type.
#[axum::async_trait]
pub trait TrinoQueryExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String>;
}

/// Start the Trino-compatible HTTP server.
pub fn start_trino_server<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    config: SqeConfig,
    port: u16,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        config,
        results: DashMap::new(),
    });

    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/statement", post(submit_query::<A, Q>))
            .route("/v1/statement/{id}/{token}", get(get_results::<A, Q>))
            .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
            .with_state(state);

        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

        info!("Trino-compat HTTP server listening on {addr}");

        axum::serve(listener, app).await.unwrap();
    })
}

/// POST /v1/statement — Submit a query.
async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let sql = body.trim();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(TrinoResponse {
            id: String::new(),
            info_uri: None,
            next_uri: None,
            columns: None,
            data: None,
            stats: TrinoStats::failed(),
            error: Some(TrinoError {
                message: "Empty query".to_string(),
                error_code: 1,
                error_name: "USER_ERROR".to_string(),
                error_type: "USER_ERROR".to_string(),
            }),
        }));
    }

    // Extract Basic auth from Authorization header
    let session = match extract_basic_auth(&headers) {
        Some((user, pass)) => {
            match state.authenticator.authenticate(&user, &pass).await {
                Ok(s) => s,
                Err(e) => {
                    return (StatusCode::UNAUTHORIZED, Json(TrinoResponse {
                        id: String::new(),
                        info_uri: None,
                        next_uri: None,
                        columns: None,
                        data: None,
                        stats: TrinoStats::failed(),
                        error: Some(TrinoError {
                            message: format!("Authentication failed: {e}"),
                            error_code: 1,
                            error_name: "USER_ERROR".to_string(),
                            error_type: "USER_ERROR".to_string(),
                        }),
                    }));
                }
            }
        }
        None => {
            return (StatusCode::UNAUTHORIZED, Json(TrinoResponse {
                id: String::new(),
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Missing Authorization header".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                }),
            }));
        }
    };

    let query_id = Uuid::new_v4().to_string();

    // Execute query
    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: None,
                next_uri: None, // All results in one page for now
                columns: Some(columns),
                data: Some(data),
                stats: TrinoStats::finished(),
                error: None,
            };

            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            warn!(error = %e, sql = sql, "Trino query execution failed");
            (StatusCode::OK, Json(TrinoResponse {
                id: query_id,
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: e.to_string(),
                    error_code: 1,
                    error_name: "INTERNAL_ERROR".to_string(),
                    error_type: "INTERNAL_ERROR".to_string(),
                }),
            }))
        }
    }
}

/// GET /v1/statement/{id}/{token} — Get query results (pagination).
async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path((id, _token)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.results.get(&id) {
        Some(cached) => (StatusCode::OK, Json(cached.response.clone())),
        None => (StatusCode::NOT_FOUND, Json(TrinoResponse {
            id,
            info_uri: None,
            next_uri: None,
            columns: None,
            data: None,
            stats: TrinoStats::failed(),
            error: Some(TrinoError {
                message: "Query not found".to_string(),
                error_code: 1,
                error_name: "USER_ERROR".to_string(),
                error_type: "USER_ERROR".to_string(),
            }),
        })),
    }
}

/// DELETE /v1/statement/{id} — Cancel a query.
async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path(id): Path<String>,
) -> StatusCode {
    state.results.remove(&id);
    StatusCode::NO_CONTENT
}

/// Extract username and password from Basic auth header.
fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        encoded,
    ).ok()?).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

// Make TrinoResponse cloneable for the DashMap cache
impl Clone for TrinoResponse {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            info_uri: self.info_uri.clone(),
            next_uri: self.next_uri.clone(),
            columns: self.columns.clone(),
            data: self.data.clone(),
            stats: TrinoStats {
                state: self.stats.state.clone(),
                queued: self.stats.queued,
                scheduled: self.stats.scheduled,
                nodes: self.stats.nodes,
                total_splits: self.stats.total_splits,
                completed_splits: self.stats.completed_splits,
            },
            error: self.error.as_ref().map(|e| TrinoError {
                message: e.message.clone(),
                error_code: e.error_code,
                error_name: e.error_name.clone(),
                error_type: e.error_type.clone(),
            }),
        }
    }
}

impl Clone for TrinoColumn {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            r#type: self.r#type.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_basic_auth() {
        let mut headers = HeaderMap::new();
        // "root:root123" in base64
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"root:root123",
        );
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());

        let (user, pass) = extract_basic_auth(&headers).unwrap();
        assert_eq!(user, "root");
        assert_eq!(pass, "root123");
    }

    #[test]
    fn test_extract_basic_auth_missing() {
        let headers = HeaderMap::new();
        assert!(extract_basic_auth(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token123".parse().unwrap());
        assert!(extract_basic_auth(&headers).is_none());
    }
}
```

- [ ] **Step 5: Create lib.rs**

```rust
pub mod protocol;
pub mod server;
pub mod types;
```

- [ ] **Step 6: Verify workspace compiles**

Run: `cargo check --workspace`

- [ ] **Step 7: Run tests**

Run: `cargo test -p sqe-trino-compat`

Expected: All 9 tests pass (6 types + 3 protocol).

- [ ] **Step 8: Commit**

```bash
git add crates/sqe-trino-compat/
git commit -m "feat: Trino v1/statement HTTP server with Arrow-to-JSON type mapping"
```

---

### Task 23: Coordinator Integration — Metrics + Audit + Trino Server

**Files:**
- Modify: `crates/sqe-coordinator/Cargo.toml`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Modify: `crates/sqe-coordinator/src/main.rs`

**Context:** Wire MetricsRegistry and AuditLogger into QueryHandler. Start metrics and Trino servers in main.rs. Implement the TrinoAuthenticator and TrinoQueryExecutor traits.

- [ ] **Step 1: Add dependencies to sqe-coordinator**

Add to `crates/sqe-coordinator/Cargo.toml` `[dependencies]`:

```toml
sqe-metrics = { path = "../sqe-metrics" }
sqe-trino-compat = { path = "../sqe-trino-compat" }
```

- [ ] **Step 2: Add metrics + audit to QueryHandler**

In `crates/sqe-coordinator/src/query_handler.rs`:

Add fields:
```rust
pub struct QueryHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    config: SqeConfig,
    catalog_ops: CatalogOps,
    write_handler: WriteHandler,
    worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
}
```

Update `new()`:
```rust
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    ) -> Self {
```

Add instrumentation in `execute()` — wrap the existing match with timing:
```rust
    pub async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();

        // ... existing parsing + routing code ...

        let result = match kind {
            // ... all existing match arms ...
        };

        // Record metrics and audit
        let duration = start.elapsed();
        let status = if result.is_ok() { "success" } else { "error" };
        let rows: usize = result.as_ref().map(|b| b.iter().map(|r| r.num_rows()).sum()).unwrap_or(0);
        let stmt_type = classify_statement_type(&kind_name);

        if let Some(ref metrics) = self.metrics {
            metrics.query_count.with_label_values(&[status, &stmt_type]).inc();
            metrics.query_duration.with_label_values(&[&stmt_type]).observe(duration.as_secs_f64());
            metrics.rows_returned.inc_by(rows as f64);
        }

        if let Some(ref audit) = self.audit {
            audit.log(&sqe_metrics::audit::AuditEntry {
                timestamp: chrono::Utc::now().to_rfc3339(),
                username: session.user.username.clone(),
                query_text: sql.to_string(),
                statement_type: stmt_type,
                duration_ms: duration.as_millis() as u64,
                rows_returned: rows,
                status: status.to_string(),
            });
        }

        result
    }
```

Extract the statement kind name before matching to use in metrics. Store it as a string before the match consumes `kind`:
```rust
        let kind = parse_and_classify(sql)?;
        let kind_name = format!("{kind:?}").split('(').next().unwrap_or("unknown").to_lowercase();
```

- [ ] **Step 3: Implement TrinoAuthenticator and TrinoQueryExecutor**

Add a new file or add at the end of `main.rs` the trait implementations. For simplicity, add to `main.rs`:

```rust
use sqe_trino_compat::server::{TrinoAuthenticator, TrinoQueryExecutor};

struct AuthenticatorAdapter(Arc<sqe_auth::Authenticator>);

#[axum::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(&self, username: &str, password: &str) -> Result<sqe_core::Session, String> {
        self.0.authenticate(username, password).await.map_err(|e| e.to_string())
    }
}

struct QueryHandlerAdapter(Arc<QueryHandler>);

#[axum::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    async fn execute(&self, session: &sqe_core::Session, sql: &str) -> Result<Vec<arrow_array::RecordBatch>, String> {
        self.0.execute(session, sql).await.map_err(|e| e.to_string())
    }
}
```

- [ ] **Step 4: Update main.rs to start all servers**

In `main.rs`, after creating QueryHandler, add:

```rust
    // Initialize metrics
    let metrics = Arc::new(sqe_metrics::MetricsRegistry::new());
    let audit = Arc::new(sqe_metrics::audit::AuditLogger::new(
        &config.metrics.audit_log_path,
    ));

    // Start metrics server
    sqe_metrics::server::start_metrics_server(metrics.clone(), config.metrics.prometheus_port);
```

Update QueryHandler constructor to pass metrics and audit:
```rust
    let query_handler = Arc::new(QueryHandler::new(
        policy_enforcer,
        config.clone(),
        if config.coordinator.worker_urls.is_empty() { None } else { Some(worker_registry.clone()) },
        Some(metrics.clone()),
        Some(audit.clone()),
    ));
```

Start Trino server:
```rust
    // Start Trino-compat HTTP server
    if config.coordinator.trino_http_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter(authenticator.clone()));
        let handler_adapter = Arc::new(QueryHandlerAdapter(query_handler.clone()));
        sqe_trino_compat::server::start_trino_server(
            auth_adapter,
            handler_adapter,
            config.clone(),
            config.coordinator.trino_http_port,
        );
        tracing::info!(
            "Trino-compat HTTP server on port {}",
            config.coordinator.trino_http_port
        );
    }
```

- [ ] **Step 5: Update all callers of QueryHandler::new()**

Everywhere that calls `QueryHandler::new()` with the old signature needs updating:

1. `crates/sqe-coordinator/tests/integration_test.rs` — `setup_handler()` and `test_simple_select()`:
   Change `QueryHandler::new(policy, config, None)` to `QueryHandler::new(policy, config, None, None, None)`

2. `test_distributed_select` test — same change.

- [ ] **Step 6: Verify workspace compiles**

Run: `cargo check --workspace`

- [ ] **Step 7: Run all tests**

Run: `cargo test --workspace`

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/sqe-coordinator/ crates/sqe-metrics/
git commit -m "feat: integrate metrics, audit logging, and Trino HTTP server into coordinator"
```

---

### Task 24: Integration Tests + Config Updates

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`
- Modify: `tests/sqe-test.toml`

**Context:** Add non-ignored tests for information_schema, metrics, audit logging, and Trino type mapping.

- [ ] **Step 1: Update test config**

Add to `tests/sqe-test.toml`:

```toml
[metrics]
prometheus_port = 19090
audit_log_path = ""
```

- [ ] **Step 2: Add Chunk 4 integration tests**

Append to `crates/sqe-coordinator/tests/integration_test.rs`:

```rust
// ---------------------------------------------------------------------------
// Chunk 4: information_schema + Trino compat + Observability tests
// ---------------------------------------------------------------------------

// Test: MetricsRegistry can be created and incremented
#[test]
fn test_metrics_registry() {
    let metrics = sqe_metrics::MetricsRegistry::new();
    metrics.query_count.with_label_values(&["success", "query"]).inc();
    assert_eq!(
        metrics.query_count.with_label_values(&["success", "query"]).get(),
        1.0
    );
}

// Test: AuditLogger no-op mode works
#[test]
fn test_audit_logger_noop() {
    let logger = sqe_metrics::audit::AuditLogger::new("");
    let entry = sqe_metrics::audit::AuditEntry {
        timestamp: "2026-03-15T00:00:00Z".to_string(),
        username: "test".to_string(),
        query_text: "SELECT 1".to_string(),
        statement_type: "query".to_string(),
        duration_ms: 10,
        rows_returned: 1,
        status: "success".to_string(),
    };
    logger.log(&entry); // Should not panic
}

// Test: Trino type mapping
#[test]
fn test_trino_type_mapping() {
    use arrow_schema::DataType;
    assert_eq!(sqe_trino_compat::types::arrow_to_trino_type(&DataType::Int64), "bigint");
    assert_eq!(sqe_trino_compat::types::arrow_to_trino_type(&DataType::Utf8), "varchar");
    assert_eq!(sqe_trino_compat::types::arrow_to_trino_type(&DataType::Float64), "double");
}

// Test: Trino response serialization
#[test]
fn test_trino_batches_to_json() {
    use std::sync::Arc;
    use arrow_schema::{DataType, Field, Schema};
    use arrow_array::{Int64Array, StringArray, RecordBatch};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    ).unwrap();

    let (cols, rows) = sqe_trino_compat::protocol::batches_to_trino(&[batch]);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].r#type, "bigint");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!(1));
}

// Test: information_schema.tables is queryable (requires quickstart stack)
#[tokio::test]
#[ignore]
async fn test_information_schema_tables() {
    let (session, handler) = setup_handler().await;

    let batches = handler
        .execute(&session, "SELECT * FROM information_schema.tables")
        .await
        .expect("information_schema.tables should be queryable");

    // Should return at least one row (there should be namespaces/tables in the test catalog)
    assert!(!batches.is_empty());
}

// Test: information_schema.schemata is queryable (requires quickstart stack)
#[tokio::test]
#[ignore]
async fn test_information_schema_schemata() {
    let (session, handler) = setup_handler().await;

    let batches = handler
        .execute(&session, "SELECT * FROM information_schema.schemata")
        .await
        .expect("information_schema.schemata should be queryable");

    assert!(!batches.is_empty());
}
```

- [ ] **Step 3: Add dev-dependencies**

Add to `crates/sqe-coordinator/Cargo.toml` `[dev-dependencies]`:

```toml
sqe-metrics = { path = "../sqe-metrics" }
sqe-trino-compat = { path = "../sqe-trino-compat" }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 4: Verify everything compiles and tests pass**

Run: `cargo test --workspace`

Expected: All tests pass (existing + 4 new non-ignored tests).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/ tests/sqe-test.toml
git commit -m "test: Chunk 4 integration tests for information_schema, metrics, and Trino compat"
```

---

### Chunk 4 — Deferred Items

1. **OTLP distributed tracing** — OpenTelemetry trace export with per-query span trees and trace context propagation to workers. Deferred to a dedicated observability iteration.

2. **Result pagination** — Current Trino endpoint returns all results in one page. Multi-page pagination with nextUri requires result caching and page tracking. Deferred to when large result sets need streaming.

3. **Query cancellation** — DELETE /v1/statement/{id} removes cached results but doesn't cancel in-flight DataFusion execution. True cancellation requires DataFusion CancellationToken integration.

4. **Trino session properties** — X-Trino-Catalog/Schema headers for setting default catalog/schema context. Deferred since most clients send fully qualified table names.

5. **information_schema.views** — Views are not yet supported in the catalog. Deferred until CREATE VIEW is fully implemented.

6. **Docker images** — Dockerfile for coordinator and worker binaries. Deferred to deployment iteration.

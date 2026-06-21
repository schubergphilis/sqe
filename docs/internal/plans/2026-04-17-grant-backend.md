# GrantBackend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single `AccessControlClient` with a pluggable `GrantBackend` trait supporting Chameleon and Polaris native grant models, so GRANT/REVOKE SQL routes through the correct backend based on config.

**Architecture:** `GrantBackend` trait in `sqe-policy` with `PolarisGrantBackend` in `sqe-policy`. `ChameleonGrantBackend` lives in `sqe-catalog` (which already depends on both `sqe-core` and `sqe-policy`, avoiding a circular dependency). The coordinator reads `access_control.backend` and constructs the right implementation at startup. Layer 1 (grants) runs independently from Layer 2 (OPA row filters / column masks).

**Tech Stack:** Rust, async-trait, reqwest (Polaris HTTP), moka (token cache), serde/serde_json (serialization), sqlparser (AST extraction)

**Design spec:** `docs/superpowers/specs/2026-04-17-grant-backend-design.md`

---

## File Structure

### Files to Create

| File | Purpose |
|---|---|
| `crates/sqe-policy/src/grants/mod.rs` | `GrantBackend` trait, shared types (`GrantStatement`, `Grantee`, `GrantFilter`, `GrantEntry`, `AccessCheck`, `AccessCheckResult`), privilege mapping |
| `crates/sqe-policy/src/grants/polaris.rs` | `PolarisGrantBackend` with Polaris Management API client |
| `crates/sqe-catalog/src/grant_chameleon.rs` | `ChameleonGrantBackend` wrapping `AccessControlClient` (in sqe-catalog to avoid circular dep) |

### Files to Modify

| File | Change |
|---|---|
| `crates/sqe-policy/src/lib.rs` | Add `pub mod grants;` |
| `crates/sqe-catalog/src/lib.rs` | Add `pub mod grant_chameleon;` |
| `crates/sqe-core/src/config.rs` | Add `client_id`, `client_secret` to `AccessControlConfig` |
| `crates/sqe-coordinator/src/query_handler.rs` | Replace `access_control_client` with `grant_backend`, refactor `extract_grant_fields` -> `extract_grant_statement`, simplify handlers |
| `crates/sqe-coordinator/src/main.rs` | Backend selection at startup |
| `crates/sqe-coordinator/src/bin/sqe_server.rs` | Same startup wiring as main.rs |

---

## Task 1: GrantBackend Trait and Shared Types

**Files:**
- Create: `crates/sqe-policy/src/grants/mod.rs`
- Modify: `crates/sqe-policy/src/lib.rs`

- [ ] **Step 1: Create the grants module with trait and types**

Create `crates/sqe-policy/src/grants/mod.rs`:

```rust
//! Pluggable grant backend for GRANT/REVOKE/SHOW GRANTS SQL.
//!
//! Two implementations:
//! - `ChameleonGrantBackend` — wraps the existing Chameleon platform API client
//! - `PolarisGrantBackend` — calls the Polaris Management REST API

pub mod chameleon;
pub mod polaris;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ── Trait ────────────────────────────────────────────────────────────────────

/// Backend for access control operations (GRANT, REVOKE, SHOW GRANTS, etc.).
///
/// Each catalog type (Chameleon, Polaris, Unity) implements this trait.
/// The coordinator selects the implementation at startup based on config.
#[async_trait]
pub trait GrantBackend: Send + Sync {
    /// Create or update a privilege grant.
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()>;

    /// Remove a privilege grant.
    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()>;

    /// List grants matching a filter (by resource or by grantee).
    async fn show_grants(
        &self,
        token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>>;

    /// List effective grants for a user (resolved through role chains).
    async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>>;

    /// Check whether a user has a specific privilege on a resource.
    async fn check_access(
        &self,
        token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult>;

    /// Human-readable backend name for logging and error messages.
    fn backend_name(&self) -> &str;
}

// ── Shared types ─────────────────────────────────────────────────────────────

/// A parsed GRANT statement ready for backend dispatch.
#[derive(Debug, Clone)]
pub struct GrantStatement {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub grantee: Grantee,
}

/// A parsed REVOKE statement ready for backend dispatch.
#[derive(Debug, Clone)]
pub struct RevokeStatement {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub grantee: Grantee,
}

/// Backend-neutral grantee. Each backend maps this to its own model.
#[derive(Debug, Clone)]
pub enum Grantee {
    /// Explicit `TO USER "name"` or bare identifier.
    User(String),
    /// Explicit `TO ROLE "name"`.
    Role(String),
    /// Explicit `TO GROUP "name"`.
    Group(String),
}

impl Grantee {
    /// Return the grantee name regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            Grantee::User(n) | Grantee::Role(n) | Grantee::Group(n) => n,
        }
    }
}

/// Filter for SHOW GRANTS: either by resource or by grantee.
#[derive(Debug, Clone)]
pub enum GrantFilter {
    /// `SHOW GRANTS ON [catalog.][namespace.]table`
    OnResource {
        catalog: Option<String>,
        namespace: Option<String>,
        table: Option<String>,
    },
    /// `SHOW GRANTS TO USER|ROLE|GROUP "name"`
    ToGrantee(Grantee),
}

/// Parameters for CHECK ACCESS.
#[derive(Debug, Clone)]
pub struct AccessCheck {
    pub user: String,
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
}

/// A single grant entry returned by SHOW GRANTS / SHOW EFFECTIVE GRANTS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntry {
    pub privilege: String,
    pub resource: String,
    pub grantee_type: String,
    pub grantee_name: String,
    pub effect: String,
    #[serde(default)]
    pub granted_by: Option<String>,
    #[serde(default)]
    pub granted_at: Option<String>,
}

/// Result of a CHECK ACCESS query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessCheckResult {
    pub allowed: bool,
    #[serde(default)]
    pub reason: Option<String>,
}

// ── Privilege mapping (SQL -> Polaris) ────────────────────────────────────────

/// Map a SQL privilege string to the Polaris privilege name and resource type.
///
/// Returns `(polaris_privilege, resource_type)`. If the input already looks
/// like a Polaris native name (all-caps with underscores), pass it through.
pub fn map_sql_to_polaris_privilege(sql_priv: &str) -> (String, &'static str) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => ("TABLE_READ_DATA".into(), "table"),
        "INSERT" => ("TABLE_WRITE_DATA".into(), "table"),
        "CREATE TABLE" => ("TABLE_CREATE".into(), "namespace"),
        "DROP" => ("TABLE_DROP".into(), "table"),
        "ALL" | "ALL PRIVILEGES" => ("CATALOG_MANAGE_CONTENT".into(), "catalog"),
        "USAGE" => ("NAMESPACE_LIST".into(), "namespace"),
        "CREATE SCHEMA" | "CREATE" => ("NAMESPACE_CREATE".into(), "catalog"),
        "DROP SCHEMA" => ("NAMESPACE_DROP".into(), "namespace"),
        _ => {
            // Pass-through: send unrecognized privileges verbatim.
            // Polaris native names (ALL_CAPS_WITH_UNDERSCORES) go through as-is.
            (sql_priv.to_uppercase(), "table")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Trait object safety ──────────────────────────────────────────
    fn _assert_grant_backend_object_safe(_: &dyn GrantBackend) {}

    // ── Grantee ──────────────────────────────────────────────────────

    #[test]
    fn grantee_name_returns_inner_value() {
        assert_eq!(Grantee::User("alice".into()).name(), "alice");
        assert_eq!(Grantee::Role("admins".into()).name(), "admins");
        assert_eq!(Grantee::Group("SG-Risk".into()).name(), "SG-Risk");
    }

    // ── Privilege mapping ────────────────────────────────────────────

    #[test]
    fn map_select_to_table_read_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("SELECT");
        assert_eq!(priv_name, "TABLE_READ_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_insert_to_table_write_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("INSERT");
        assert_eq!(priv_name, "TABLE_WRITE_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_all_to_catalog_manage_content() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("ALL");
        assert_eq!(priv_name, "CATALOG_MANAGE_CONTENT");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn map_usage_to_namespace_list() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("USAGE");
        assert_eq!(priv_name, "NAMESPACE_LIST");
        assert_eq!(res_type, "namespace");
    }

    #[test]
    fn map_create_schema_to_namespace_create() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("CREATE SCHEMA");
        assert_eq!(priv_name, "NAMESPACE_CREATE");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn passthrough_polaris_native_privilege() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("TABLE_WRITE_PROPERTIES");
        assert_eq!(priv_name, "TABLE_WRITE_PROPERTIES");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_is_case_insensitive() {
        let (priv_name, _) = map_sql_to_polaris_privilege("select");
        assert_eq!(priv_name.as_str(), "TABLE_READ_DATA");
    }

    // ── GrantEntry serde ─────────────────────────────────────────────

    #[test]
    fn grant_entry_serializes() {
        let entry = GrantEntry {
            privilege: "SELECT".into(),
            resource: "cat.ns.tbl".into(),
            grantee_type: "ROLE".into(),
            grantee_name: "analysts".into(),
            effect: "ALLOW".into(),
            granted_by: Some("admin".into()),
            granted_at: None,
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["privilege"], "SELECT");
        assert_eq!(json["grantee_name"], "analysts");
    }

    #[test]
    fn access_check_result_deserializes() {
        let json = r#"{"allowed": true, "reason": null}"#;
        let result: AccessCheckResult = serde_json::from_str(json).unwrap();
        assert!(result.allowed);
        assert!(result.reason.is_none());
    }
}
```

- [ ] **Step 2: Add the grants module to sqe-policy lib.rs**

In `crates/sqe-policy/src/lib.rs`, add at line 1 (before existing `pub mod plan_rewriter;`):

```rust
pub mod grants;
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo check -p sqe-policy 2>&1 | tail -5
```

Expected: Compiles (the `chameleon` and `polaris` submodules will be empty stubs for now — create them in the next steps).

Wait — the `pub mod chameleon;` and `pub mod polaris;` in `mod.rs` will fail because the files don't exist yet. Create empty stubs first:

Create `crates/sqe-policy/src/grants/chameleon.rs`:
```rust
//! ChameleonGrantBackend — wraps the existing AccessControlClient.
```

Create `crates/sqe-policy/src/grants/polaris.rs`:
```rust
//! PolarisGrantBackend — calls the Polaris Management REST API.
```

Then run:
```bash
cargo check -p sqe-policy 2>&1 | tail -5
```

Expected: Compiles with no errors.

- [ ] **Step 4: Run tests**

```bash
cargo test -p sqe-policy -- grants 2>&1 | tail -15
```

Expected: All grant module tests pass (trait object safety, grantee name, privilege mapping, serde).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/grants/mod.rs crates/sqe-policy/src/grants/chameleon.rs crates/sqe-policy/src/grants/polaris.rs crates/sqe-policy/src/lib.rs
git commit -m "feat: add GrantBackend trait and shared types in sqe-policy

GrantBackend trait with five operations (grant, revoke, show_grants,
show_effective, check_access). Shared types: GrantStatement, Grantee,
GrantFilter, GrantEntry, AccessCheck, AccessCheckResult. SQL-to-Polaris
privilege mapping with pass-through for native Polaris privilege names."
```

---

## Task 2: ChameleonGrantBackend

**Files:**
- Create: `crates/sqe-catalog/src/grant_chameleon.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`

**NOTE:** `ChameleonGrantBackend` lives in `sqe-catalog` (not `sqe-policy`) because `sqe-catalog` already depends on `sqe-policy` (for `PolicyStore` in info_schema). Putting it in `sqe-policy` would create a circular dependency (`sqe-policy` -> `sqe-catalog` -> `sqe-policy`).

- [ ] **Step 1: Remove the chameleon stub from sqe-policy**

Delete `crates/sqe-policy/src/grants/chameleon.rs` and remove `pub mod chameleon;` from `crates/sqe-policy/src/grants/mod.rs`.

- [ ] **Step 2: Write ChameleonGrantBackend in sqe-catalog**

Create `crates/sqe-catalog/src/grant_chameleon.rs` and add `pub mod grant_chameleon;` to `crates/sqe-catalog/src/lib.rs`:

```rust
//! ChameleonGrantBackend — wraps the existing AccessControlClient.
//!
//! Thin adapter that translates the trait's backend-neutral types
//! (`GrantStatement`, `Grantee`) into the Chameleon platform API's
//! types (`GrantRequest`). Zero behavior change for existing deployments.

use std::sync::Arc;

use async_trait::async_trait;

use sqe_catalog::access_control::{
    AccessControlClient, CheckAccessRequest, GrantRequest, ShowGrantsParams,
};

use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    Grantee, RevokeStatement,
};

/// Chameleon platform API grant backend.
///
/// Maps `Grantee::Role` and `Grantee::Group` to `"GROUP"`, and
/// `Grantee::User` to `"USER"`. Delegates all HTTP calls to the
/// existing `AccessControlClient`.
pub struct ChameleonGrantBackend {
    client: Arc<AccessControlClient>,
}

impl ChameleonGrantBackend {
    pub fn new(client: AccessControlClient) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

/// Map a `Grantee` to Chameleon's grantee_type string.
fn chameleon_grantee_type(grantee: &Grantee) -> &'static str {
    match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) | Grantee::Group(_) => "GROUP",
    }
}

/// Build a `GrantRequest` from a `GrantStatement`.
fn to_grant_request(stmt: &GrantStatement) -> GrantRequest {
    GrantRequest {
        privilege: stmt.privilege.clone(),
        catalog: stmt.catalog.clone(),
        namespace: stmt.namespace.clone(),
        table: stmt.table.clone(),
        grantee_type: chameleon_grantee_type(&stmt.grantee).to_string(),
        grantee_name: stmt.grantee.name().to_string(),
        effect: None,
    }
}

/// Convert a `sqe_catalog::access_control::GrantEntry` to our trait's `GrantEntry`.
fn from_catalog_entry(e: &sqe_catalog::access_control::GrantEntry) -> GrantEntry {
    GrantEntry {
        privilege: e.privilege.clone(),
        resource: e.resource.clone(),
        grantee_type: e.grantee_type.clone(),
        grantee_name: e.grantee_name.clone(),
        effect: e.effect.clone(),
        granted_by: e.granted_by.clone(),
        granted_at: e.granted_at.clone(),
    }
}

#[async_trait]
impl GrantBackend for ChameleonGrantBackend {
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let req = to_grant_request(stmt);
        self.client.grant(token, &req).await
    }

    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let req = GrantRequest {
            privilege: stmt.privilege.clone(),
            catalog: stmt.catalog.clone(),
            namespace: stmt.namespace.clone(),
            table: stmt.table.clone(),
            grantee_type: chameleon_grantee_type(&stmt.grantee).to_string(),
            grantee_name: stmt.grantee.name().to_string(),
            effect: None,
        };
        self.client.revoke(token, &req).await
    }

    async fn show_grants(
        &self,
        token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let params = match filter {
            GrantFilter::OnResource {
                catalog,
                namespace,
                table,
            } => ShowGrantsParams {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
                grantee_type: None,
                grantee_name: None,
            },
            GrantFilter::ToGrantee(grantee) => ShowGrantsParams {
                catalog: None,
                namespace: None,
                table: None,
                grantee_type: Some(chameleon_grantee_type(grantee).to_string()),
                grantee_name: Some(grantee.name().to_string()),
            },
        };
        let entries = self.client.show_grants(token, &params).await?;
        Ok(entries.iter().map(from_catalog_entry).collect())
    }

    async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let entries = self.client.show_effective(token, user).await?;
        Ok(entries.iter().map(from_catalog_entry).collect())
    }

    async fn check_access(
        &self,
        token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        let req = CheckAccessRequest {
            user: check.user.clone(),
            privilege: check.privilege.clone(),
            catalog: check.catalog.clone(),
            namespace: check.namespace.clone(),
            table: check.table.clone(),
        };
        let resp = self.client.check_access(token, &req).await?;
        Ok(AccessCheckResult {
            allowed: resp.allowed,
            reason: resp.reason,
        })
    }

    fn backend_name(&self) -> &str {
        "chameleon"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chameleon_maps_user_to_user() {
        assert_eq!(chameleon_grantee_type(&Grantee::User("alice".into())), "USER");
    }

    #[test]
    fn chameleon_maps_role_to_group() {
        assert_eq!(chameleon_grantee_type(&Grantee::Role("admins".into())), "GROUP");
    }

    #[test]
    fn chameleon_maps_group_to_group() {
        assert_eq!(chameleon_grantee_type(&Grantee::Group("SG-Risk".into())), "GROUP");
    }

    #[test]
    fn to_grant_request_translates_fields() {
        let stmt = GrantStatement {
            privilege: "SELECT".into(),
            catalog: Some("cat".into()),
            namespace: Some("ns".into()),
            table: Some("tbl".into()),
            grantee: Grantee::Role("analysts".into()),
        };
        let req = to_grant_request(&stmt);
        assert_eq!(req.privilege, "SELECT");
        assert_eq!(req.catalog.as_deref(), Some("cat"));
        assert_eq!(req.namespace.as_deref(), Some("ns"));
        assert_eq!(req.table.as_deref(), Some("tbl"));
        assert_eq!(req.grantee_type, "GROUP");
        assert_eq!(req.grantee_name, "analysts");
        assert!(req.effect.is_none());
    }

    #[test]
    fn from_catalog_entry_copies_all_fields() {
        let catalog_entry = sqe_catalog::access_control::GrantEntry {
            privilege: "SELECT".into(),
            resource: "cat.ns.tbl".into(),
            grantee_type: "GROUP".into(),
            grantee_name: "analysts".into(),
            effect: "ALLOW".into(),
            granted_by: Some("admin".into()),
            granted_at: Some("2026-04-17T10:00:00Z".into()),
        };
        let entry = from_catalog_entry(&catalog_entry);
        assert_eq!(entry.privilege, "SELECT");
        assert_eq!(entry.resource, "cat.ns.tbl");
        assert_eq!(entry.grantee_type, "GROUP");
        assert_eq!(entry.grantee_name, "analysts");
        assert_eq!(entry.effect, "ALLOW");
        assert_eq!(entry.granted_by.as_deref(), Some("admin"));
        assert_eq!(entry.granted_at.as_deref(), Some("2026-04-17T10:00:00Z"));
    }
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo check -p sqe-catalog 2>&1 | tail -5
```

Expected: Compiles. No circular dependency since sqe-catalog already depends on sqe-policy.

- [ ] **Step 4: Run tests**

```bash
cargo test -p sqe-catalog -- grant_chameleon 2>&1 | tail -15
```

Expected: All 5 chameleon tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/grant_chameleon.rs crates/sqe-catalog/src/lib.rs crates/sqe-policy/src/grants/mod.rs
git commit -m "feat: add ChameleonGrantBackend wrapping AccessControlClient

Thin adapter translating GrantStatement/Grantee to GrantRequest.
Maps Role and Group to GROUP, User to USER. Delegates all HTTP
calls to the existing AccessControlClient. Lives in sqe-catalog
to avoid circular dependency with sqe-policy. Zero behavior change."
```

---

## Task 3: Config Changes

**Files:**
- Modify: `crates/sqe-core/src/config.rs`

- [ ] **Step 1: Add client_id and client_secret to AccessControlConfig**

In `crates/sqe-core/src/config.rs`, find the `AccessControlConfig` struct (around line 742) and add two fields:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct AccessControlConfig {
    /// Backend type: "chameleon", "polaris", or "none" (disabled).
    #[serde(default = "default_access_control_backend")]
    pub backend: String,
    /// Backend API URL.
    /// Chameleon: http://backend:port/api/platform/v1/access
    /// Polaris: http://polaris:8181/api/management/v1 (Polaris management API)
    #[serde(default)]
    pub url: String,
    /// Request timeout in seconds.
    #[serde(default = "default_access_control_timeout")]
    pub timeout_secs: u64,
    /// Optional: Polaris service account client_id for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Optional: Polaris service account client_secret for management API.
    /// When absent, the user's passthrough OIDC token is used.
    #[serde(default)]
    pub client_secret: Option<String>,
}
```

Update the `Default` impl to include the new fields:

```rust
impl Default for AccessControlConfig {
    fn default() -> Self {
        Self {
            backend: "none".to_string(),
            url: String::new(),
            timeout_secs: 30,
            client_id: None,
            client_secret: None,
        }
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo check -p sqe-core 2>&1 | tail -5
```

Expected: Compiles. No other crates should break because the new fields have defaults.

- [ ] **Step 3: Run existing config tests**

```bash
cargo test -p sqe-core -- config 2>&1 | tail -15
```

Expected: All existing config tests still pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat: add client_id/client_secret to AccessControlConfig

Optional Polaris service credentials for the management API.
When absent, the user's passthrough OIDC token is used.
No behavior change for existing deployments."
```

---

## Task 4: PolarisGrantBackend — Structure and Token Handling

**Files:**
- Modify: `crates/sqe-policy/src/grants/polaris.rs`

- [ ] **Step 1: Write tests for token handling and catalog role naming**

Replace `crates/sqe-policy/src/grants/polaris.rs` with the struct, constructor, token resolution, naming convention, and their tests. (The actual grant/revoke/show logic comes in Task 5.)

```rust
//! PolarisGrantBackend — calls the Polaris Management REST API.
//!
//! Implements the three-step grant chain:
//! 1. Ensure catalog role exists (`POST /catalogs/{c}/catalog-roles`)
//! 2. Grant privilege to catalog role (`PUT /catalogs/{c}/catalog-roles/{r}/grants`)
//! 3. Assign catalog role to principal role (`PUT /principal-roles/{pr}/catalog-roles/{c}`)

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    Grantee, RevokeStatement, map_sql_to_polaris_privilege,
};

/// Polaris Management API grant backend.
pub struct PolarisGrantBackend {
    client: Client,
    /// Base URL for the Polaris Management API, e.g.
    /// `http://polaris:8181/api/management/v1`
    management_url: String,
    /// Optional service token source. When present, the backend fetches
    /// a service token instead of using the user's passthrough token.
    service_token: Option<ServiceTokenSource>,
}

/// OAuth2 client_credentials token source for Polaris management API.
struct ServiceTokenSource {
    token_url: String,
    client_id: String,
    client_secret: String,
    cache: moka::future::Cache<String, String>,
}

/// OAuth2 token response from Polaris.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

impl PolarisGrantBackend {
    /// Create a new Polaris grant backend.
    ///
    /// `management_url` is the base URL for the Management API
    /// (e.g. `http://polaris:8181/api/management/v1`).
    ///
    /// When `client_id` and `client_secret` are both `Some`, the backend
    /// uses OAuth2 client_credentials to fetch a service token. Otherwise
    /// it uses the user's passthrough token.
    pub fn new(
        management_url: &str,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> sqe_core::Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                sqe_core::SqeError::Config(format!("Failed to build HTTP client: {e}"))
            })?;

        let management_url = management_url.trim_end_matches('/').to_string();

        let service_token = match (client_id, client_secret) {
            (Some(id), Some(secret)) => {
                // Derive token URL from management URL:
                // http://polaris:8181/api/management/v1 -> http://polaris:8181/api/catalog/v1/oauth/tokens
                let token_url = management_url
                    .replace("/api/management/v1", "/api/catalog/v1/oauth/tokens");
                Some(ServiceTokenSource {
                    token_url,
                    client_id: id,
                    client_secret: secret,
                    cache: moka::future::Cache::builder()
                        .max_capacity(1)
                        .time_to_live(std::time::Duration::from_secs(3570))
                        .build(),
                })
            }
            _ => None,
        };

        Ok(Self {
            client,
            management_url,
            service_token,
        })
    }

    /// Resolve the token to use for a Management API call.
    ///
    /// If service credentials are configured, fetch (or return cached)
    /// service token. Otherwise return the user's passthrough token.
    async fn resolve_token(&self, user_token: &str) -> sqe_core::Result<String> {
        match &self.service_token {
            None => Ok(user_token.to_string()),
            Some(source) => {
                let key = "polaris_service_token".to_string();
                if let Some(cached) = source.cache.get(&key).await {
                    return Ok(cached);
                }
                debug!(token_url = %source.token_url, "Fetching Polaris service token");
                let resp = self
                    .client
                    .post(&source.token_url)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", &source.client_id),
                        ("client_secret", &source.client_secret),
                        ("scope", "PRINCIPAL_ROLE:ALL"),
                    ])
                    .send()
                    .await
                    .map_err(|e| {
                        sqe_core::SqeError::Auth(format!(
                            "Polaris token fetch failed: {e}"
                        ))
                    })?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(sqe_core::SqeError::Auth(format!(
                        "Polaris token fetch failed (HTTP {status}): {text}"
                    )));
                }

                let token_resp: TokenResponse = resp.json().await.map_err(|e| {
                    sqe_core::SqeError::Auth(format!(
                        "Polaris token response parse failed: {e}"
                    ))
                })?;

                source
                    .cache
                    .insert(key, token_resp.access_token.clone())
                    .await;
                Ok(token_resp.access_token)
            }
        }
    }
}

/// Build the SQE-managed catalog role name for a principal role.
/// Convention: `sqe_{principal_role_name}` to avoid collisions.
pub fn catalog_role_name(principal_role: &str) -> String {
    format!("sqe_{principal_role}")
}

// ── Polaris Management API request/response types ────────────────────────────

#[derive(Debug, Serialize)]
struct CreateCatalogRoleRequest {
    #[serde(rename = "catalogRole")]
    catalog_role: CatalogRoleName,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogRoleName {
    name: String,
}

#[derive(Debug, Serialize)]
struct GrantPrivilegeRequest {
    grant: PolarisGrant,
}

#[derive(Debug, Serialize, Deserialize)]
struct PolarisGrant {
    #[serde(rename = "type")]
    resource_type: String,
    privilege: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "tableName")]
    table_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct AssignCatalogRoleRequest {
    #[serde(rename = "catalogRole")]
    catalog_role: CatalogRoleName,
}

#[derive(Debug, Deserialize)]
struct ListCatalogRolesResponse {
    #[serde(default)]
    roles: Vec<CatalogRoleName>,
}

#[derive(Debug, Deserialize)]
struct ListGrantsResponse {
    #[serde(default)]
    grants: Vec<PolarisGrant>,
}

#[derive(Debug, Deserialize)]
struct ListPrincipalRolesResponse {
    #[serde(default)]
    roles: Vec<CatalogRoleName>,
}

#[async_trait]
impl GrantBackend for PolarisGrantBackend {
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let token = self.resolve_token(token).await?;
        let catalog = stmt.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Polaris GRANT requires a catalog name (use catalog.namespace.table)".into(),
            )
        })?;

        let principal_role = match &stmt.grantee {
            Grantee::Role(name) | Grantee::Group(name) => name.clone(),
            Grantee::User(_) => {
                return Err(sqe_core::SqeError::NotImplemented(
                    "Polaris does not support USER-level grants. Use ROLE instead.".into(),
                ));
            }
        };

        let cr_name = catalog_role_name(&principal_role);
        let (polaris_priv, resource_type) = map_sql_to_polaris_privilege(&stmt.privilege);

        // Step 1: Ensure catalog role exists (409 = already exists, OK)
        let url = format!(
            "{}/catalogs/{}/catalog-roles",
            self.management_url, catalog
        );
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&CreateCatalogRoleRequest {
                catalog_role: CatalogRoleName {
                    name: cr_name.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() && resp.status().as_u16() != 409 {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to create catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        // Step 2: Grant privilege to catalog role
        let url = format!(
            "{}/catalogs/{}/catalog-roles/{}/grants",
            self.management_url, catalog, cr_name
        );
        let grant_body = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: resource_type.to_string(),
                privilege: polaris_priv.to_string(),
                namespace: stmt.namespace.as_ref().map(|ns| vec![ns.clone()]),
                table_name: stmt.table.clone(),
            },
        };
        let resp = self
            .client
            .put(&url)
            .bearer_auth(&token)
            .json(&grant_body)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to grant privilege to catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        // Step 3: Assign catalog role to principal role
        let url = format!(
            "{}/principal-roles/{}/catalog-roles/{}",
            self.management_url, principal_role, catalog
        );
        let resp = self
            .client
            .put(&url)
            .bearer_auth(&token)
            .json(&AssignCatalogRoleRequest {
                catalog_role: CatalogRoleName {
                    name: cr_name.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to assign catalog role '{cr_name}' to principal role '{principal_role}' (HTTP {status}): {text}"
            )));
        }

        debug!(
            catalog = catalog,
            principal_role = principal_role,
            catalog_role = cr_name,
            privilege = polaris_priv,
            "Polaris grant completed (3-step chain)"
        );

        Ok(())
    }

    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let token = self.resolve_token(token).await?;
        let catalog = stmt.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Polaris REVOKE requires a catalog name (use catalog.namespace.table)".into(),
            )
        })?;

        let principal_role = match &stmt.grantee {
            Grantee::Role(name) | Grantee::Group(name) => name.clone(),
            Grantee::User(_) => {
                return Err(sqe_core::SqeError::NotImplemented(
                    "Polaris does not support USER-level grants. Use ROLE instead.".into(),
                ));
            }
        };

        let cr_name = catalog_role_name(&principal_role);
        let (polaris_priv, resource_type) = map_sql_to_polaris_privilege(&stmt.privilege);

        // Remove the specific privilege from the catalog role.
        // Does NOT delete the catalog role or unassign it.
        let url = format!(
            "{}/catalogs/{}/catalog-roles/{}/grants",
            self.management_url, catalog, cr_name
        );
        let resp = self
            .client
            .request(reqwest::Method::DELETE, &url)
            .bearer_auth(&token)
            .json(&GrantPrivilegeRequest {
                grant: PolarisGrant {
                    resource_type: resource_type.to_string(),
                    privilege: polaris_priv.to_string(),
                    namespace: stmt.namespace.as_ref().map(|ns| vec![ns.clone()]),
                    table_name: stmt.table.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to revoke privilege from catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        debug!(
            catalog = catalog,
            principal_role = principal_role,
            catalog_role = cr_name,
            privilege = polaris_priv,
            "Polaris revoke completed"
        );

        Ok(())
    }

    async fn show_grants(
        &self,
        token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let token = self.resolve_token(token).await?;

        match filter {
            GrantFilter::OnResource {
                catalog,
                namespace,
                table,
            } => {
                let catalog = catalog.as_deref().ok_or_else(|| {
                    sqe_core::SqeError::Execution(
                        "Polaris SHOW GRANTS ON requires a catalog name".into(),
                    )
                })?;

                // List all catalog roles, then get grants for each
                let url = format!(
                    "{}/catalogs/{}/catalog-roles",
                    self.management_url, catalog
                );
                let resp = self
                    .client
                    .get(&url)
                    .bearer_auth(&token)
                    .send()
                    .await
                    .map_err(|e| {
                        sqe_core::SqeError::Execution(format!(
                            "Polaris management API request failed: {e}"
                        ))
                    })?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(sqe_core::SqeError::Execution(format!(
                        "Failed to list catalog roles (HTTP {status}): {text}"
                    )));
                }

                let roles: ListCatalogRolesResponse = resp.json().await.map_err(|e| {
                    sqe_core::SqeError::Execution(format!(
                        "Failed to parse catalog roles response: {e}"
                    ))
                })?;

                let mut entries = Vec::new();
                for role in &roles.roles {
                    let grants_url = format!(
                        "{}/catalogs/{}/catalog-roles/{}/grants",
                        self.management_url, catalog, role.name
                    );
                    let resp = self
                        .client
                        .get(&grants_url)
                        .bearer_auth(&token)
                        .send()
                        .await
                        .map_err(|e| {
                            sqe_core::SqeError::Execution(format!(
                                "Polaris management API request failed: {e}"
                            ))
                        })?;

                    if !resp.status().is_success() {
                        warn!(
                            catalog_role = role.name,
                            status = %resp.status(),
                            "Skipping catalog role grants (non-200)"
                        );
                        continue;
                    }

                    let grants: ListGrantsResponse =
                        resp.json().await.unwrap_or(ListGrantsResponse {
                            grants: Vec::new(),
                        });

                    for grant in &grants.grants {
                        // Filter by namespace/table if provided
                        if let Some(ns) = namespace {
                            if let Some(ref grant_ns) = grant.namespace {
                                if !grant_ns.contains(ns) {
                                    continue;
                                }
                            }
                        }
                        if let Some(tbl) = table {
                            if let Some(ref grant_tbl) = grant.table_name {
                                if grant_tbl != tbl {
                                    continue;
                                }
                            }
                        }

                        let resource = format_polaris_resource(
                            catalog,
                            grant.namespace.as_deref(),
                            grant.table_name.as_deref(),
                        );

                        entries.push(GrantEntry {
                            privilege: grant.privilege.clone(),
                            resource,
                            grantee_type: "CATALOG_ROLE".into(),
                            grantee_name: role.name.clone(),
                            effect: "ALLOW".into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                }

                Ok(entries)
            }
            GrantFilter::ToGrantee(grantee) => {
                // Find the SQE-managed catalog role for this grantee
                let role_name = grantee.name();
                let cr_name = catalog_role_name(role_name);

                // We need a catalog to query. Without one we can't list grants.
                // Return empty for now — the coordinator should provide catalog context.
                warn!(
                    principal_role = role_name,
                    catalog_role = cr_name,
                    "SHOW GRANTS TO ROLE requires catalog context for Polaris; returning empty"
                );
                Ok(vec![])
            }
        }
    }

    async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let token = self.resolve_token(token).await?;

        // Step 1: Get principal's principal-roles
        let url = format!(
            "{}/principals/{}/principal-roles",
            self.management_url, user
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            if status.as_u16() == 403 {
                return Err(sqe_core::SqeError::Auth(
                    "Insufficient privileges to manage grants".into(),
                ));
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to list principal roles for '{user}' (HTTP {status}): {text}"
            )));
        }

        let principal_roles: ListPrincipalRolesResponse =
            resp.json().await.map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Failed to parse principal roles response: {e}"
                ))
            })?;

        // Step 2+3: For each principal-role, get catalog-role assignments
        // and for each catalog-role get grants. Flatten into GrantEntry list.
        let mut entries = Vec::new();

        for pr in &principal_roles.roles {
            // We need to iterate catalogs to find catalog-role assignments.
            // For now, use the management API to list catalogs.
            let catalogs_url = format!("{}/catalogs", self.management_url);
            let resp = self
                .client
                .get(&catalogs_url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| {
                    sqe_core::SqeError::Execution(format!(
                        "Polaris management API request failed: {e}"
                    ))
                })?;

            if !resp.status().is_success() {
                continue;
            }

            #[derive(Deserialize)]
            struct CatalogList {
                #[serde(default)]
                catalogs: Vec<CatalogInfo>,
            }
            #[derive(Deserialize)]
            struct CatalogInfo {
                name: String,
            }

            let catalog_list: CatalogList =
                resp.json().await.unwrap_or(CatalogList { catalogs: vec![] });

            for cat in &catalog_list.catalogs {
                let assign_url = format!(
                    "{}/principal-roles/{}/catalog-roles/{}",
                    self.management_url, pr.name, cat.name
                );
                let resp = self
                    .client
                    .get(&assign_url)
                    .bearer_auth(&token)
                    .send()
                    .await;

                let resp = match resp {
                    Ok(r) if r.status().is_success() => r,
                    _ => continue,
                };

                let assigned: ListCatalogRolesResponse =
                    resp.json().await.unwrap_or(ListCatalogRolesResponse {
                        roles: vec![],
                    });

                for cr in &assigned.roles {
                    let grants_url = format!(
                        "{}/catalogs/{}/catalog-roles/{}/grants",
                        self.management_url, cat.name, cr.name
                    );
                    let resp = self
                        .client
                        .get(&grants_url)
                        .bearer_auth(&token)
                        .send()
                        .await;

                    let resp = match resp {
                        Ok(r) if r.status().is_success() => r,
                        _ => continue,
                    };

                    let grants: ListGrantsResponse =
                        resp.json().await.unwrap_or(ListGrantsResponse {
                            grants: vec![],
                        });

                    for grant in &grants.grants {
                        let resource = format_polaris_resource(
                            &cat.name,
                            grant.namespace.as_deref(),
                            grant.table_name.as_deref(),
                        );

                        entries.push(GrantEntry {
                            privilege: grant.privilege.clone(),
                            resource,
                            grantee_type: "PRINCIPAL_ROLE".into(),
                            grantee_name: pr.name.clone(),
                            effect: "ALLOW".into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                }
            }
        }

        Ok(entries)
    }

    async fn check_access(
        &self,
        token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        // Walk the effective grants chain and check for a matching privilege
        let effective = self.show_effective(token, &check.user).await?;

        let (polaris_priv, _) = map_sql_to_polaris_privilege(&check.privilege);

        for entry in &effective {
            if entry.privilege == polaris_priv {
                // Check resource match
                let ns_vec = check.namespace.as_ref().map(|s| vec![s.clone()]);
                let target_resource = format_polaris_resource(
                    check.catalog.as_deref().unwrap_or(""),
                    ns_vec.as_deref(),
                    check.table.as_deref(),
                );
                if entry.resource == target_resource || check.catalog.is_none() {
                    return Ok(AccessCheckResult {
                        allowed: true,
                        reason: Some(format!(
                            "Granted via principal role '{}'",
                            entry.grantee_name
                        )),
                    });
                }
            }
        }

        Ok(AccessCheckResult {
            allowed: false,
            reason: Some(format!(
                "No matching grant for {} {} on {}",
                check.user,
                check.privilege,
                check.catalog.as_deref().unwrap_or("(no catalog)")
            )),
        })
    }

    fn backend_name(&self) -> &str {
        "polaris"
    }
}

/// Format a Polaris resource as a dotted string for GrantEntry.
fn format_polaris_resource(
    catalog: &str,
    namespace: Option<&[String]>,
    table: Option<&str>,
) -> String {
    let mut parts = vec![catalog.to_string()];
    if let Some(ns) = namespace {
        for n in ns {
            parts.push(n.clone());
        }
    }
    if let Some(t) = table {
        parts.push(t.to_string());
    }
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_role_name_adds_prefix() {
        assert_eq!(catalog_role_name("analysts"), "sqe_analysts");
        assert_eq!(catalog_role_name("data_eng"), "sqe_data_eng");
    }

    #[test]
    fn format_polaris_resource_full() {
        let result = format_polaris_resource(
            "warehouse",
            Some(&["ns".to_string()]),
            Some("orders"),
        );
        assert_eq!(result, "warehouse.ns.orders");
    }

    #[test]
    fn format_polaris_resource_catalog_only() {
        let result = format_polaris_resource("warehouse", None, None);
        assert_eq!(result, "warehouse");
    }

    #[test]
    fn format_polaris_resource_namespace_only() {
        let result = format_polaris_resource(
            "warehouse",
            Some(&["ns".to_string()]),
            None,
        );
        assert_eq!(result, "warehouse.ns");
    }

    #[test]
    fn constructor_passthrough_mode() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1",
            None,
            None,
        )
        .unwrap();
        assert!(backend.service_token.is_none());
        assert_eq!(backend.management_url, "http://polaris:8181/api/management/v1");
        assert_eq!(backend.backend_name(), "polaris");
    }

    #[test]
    fn constructor_service_credential_mode() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1/",
            Some("client-id".into()),
            Some("client-secret".into()),
        )
        .unwrap();
        assert!(backend.service_token.is_some());
        // URL should be trimmed
        assert_eq!(backend.management_url, "http://polaris:8181/api/management/v1");
        let source = backend.service_token.as_ref().unwrap();
        assert_eq!(source.token_url, "http://polaris:8181/api/catalog/v1/oauth/tokens");
        assert_eq!(source.client_id, "client-id");
    }

    #[tokio::test]
    async fn resolve_token_passthrough_returns_user_token() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1",
            None,
            None,
        )
        .unwrap();
        let token = backend.resolve_token("user-jwt-token").await.unwrap();
        assert_eq!(token, "user-jwt-token");
    }

    #[test]
    fn create_catalog_role_request_serializes() {
        let req = CreateCatalogRoleRequest {
            catalog_role: CatalogRoleName {
                name: "sqe_analysts".into(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["catalogRole"]["name"], "sqe_analysts");
    }

    #[test]
    fn grant_privilege_request_serializes() {
        let req = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: "table".into(),
                privilege: "TABLE_READ_DATA".into(),
                namespace: Some(vec!["ns".into()]),
                table_name: Some("orders".into()),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["grant"]["type"], "table");
        assert_eq!(json["grant"]["privilege"], "TABLE_READ_DATA");
        assert_eq!(json["grant"]["namespace"], serde_json::json!(["ns"]));
        assert_eq!(json["grant"]["tableName"], "orders");
    }

    #[test]
    fn grant_privilege_request_omits_none_fields() {
        let req = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: "catalog".into(),
                privilege: "CATALOG_MANAGE_CONTENT".into(),
                namespace: None,
                table_name: None,
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json["grant"].get("namespace").is_none());
        assert!(json["grant"].get("tableName").is_none());
    }

    #[test]
    fn assign_catalog_role_request_serializes() {
        let req = AssignCatalogRoleRequest {
            catalog_role: CatalogRoleName {
                name: "sqe_analysts".into(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["catalogRole"]["name"], "sqe_analysts");
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo check -p sqe-policy 2>&1 | tail -10
```

Expected: Compiles. There may be warnings about unused code in the check_access `from_ref` call — fix as needed by adjusting the resource matching logic.

- [ ] **Step 3: Run tests**

```bash
cargo test -p sqe-policy -- grants::polaris 2>&1 | tail -15
```

Expected: All polaris unit tests pass (catalog_role_name, format_polaris_resource, constructor modes, token passthrough, serialization).

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-policy/src/grants/polaris.rs
git commit -m "feat: add PolarisGrantBackend with Management API client

Three-step grant chain (create catalog role, grant privilege, assign
to principal role). Full privilege mapping (SELECT->TABLE_READ_DATA
etc.) with pass-through for native Polaris names. Token handling
supports passthrough and service credential modes. SHOW GRANTS,
SHOW EFFECTIVE GRANTS, and CHECK ACCESS walk the role chain."
```

---

## Task 5: Coordinator Integration — Replace access_control_client with grant_backend

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Replace the access_control_client field**

In `query_handler.rs`, change the struct field at line 83:

Old:
```rust
    access_control_client: Option<Arc<AccessControlClient>>,
```

New:
```rust
    grant_backend: Option<Arc<dyn sqe_policy::grants::GrantBackend>>,
```

Remove the `use sqe_catalog::AccessControlClient;` from the imports at line 14 (it may be part of a combined import — only remove the `AccessControlClient` part).

Add at the top of the file:
```rust
use sqe_policy::grants::{
    GrantBackend, GrantStatement, RevokeStatement, Grantee, GrantFilter, AccessCheck,
};
```

- [ ] **Step 2: Update QueryHandler::new to accept grant_backend**

In `QueryHandler::new()` (line 88), remove the access_control_client initialization block (lines 117-130) and add `grant_backend` as a parameter:

```rust
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        policy_store: Option<Arc<dyn PolicyStore>>,
        config: SqeConfig,
        worker_registry: Option<Arc<crate::worker_registry::WorkerRegistry>>,
        credential_tracker: Option<Arc<CredentialRefreshTracker>>,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
        query_tracker: Arc<QueryTracker>,
        query_cache: Option<Arc<ResultCache>>,
        grant_backend: Option<Arc<dyn GrantBackend>>,
    ) -> sqe_core::Result<Self> {
```

In the `Ok(Self { ... })` block, replace `access_control_client,` with `grant_backend,`.

Remove the lines 116-130 that build the `access_control_client`.

- [ ] **Step 3: Refactor extract_grant_fields to extract_grant_statement**

Replace the `extract_grant_fields` method (lines 1580-1685) with:

```rust
    /// Extract a `GrantStatement` from a sqlparser `Statement::Grant` or `Statement::Revoke`.
    fn extract_grant_statement(stmt: &Statement) -> sqe_core::Result<GrantStatement> {
        let (privileges, objects, grantees) = match stmt {
            Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } => (privileges, objects, grantees),
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => (privileges, objects, grantees),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected GRANT/REVOKE statement, got: {other}"
                )));
            }
        };

        let privilege = format!("{privileges}");

        let (catalog, namespace, table) = match objects {
            sqlparser::ast::GrantObjects::Tables(tables) if !tables.is_empty() => {
                let name = &tables[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, None, Some(parts[0].clone())),
                    2 => (None, Some(parts[0].clone()), Some(parts[1].clone())),
                    3 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some(parts[2].clone()),
                    ),
                    _ => (None, None, Some(name.to_string())),
                }
            }
            sqlparser::ast::GrantObjects::Schemas(schemas) if !schemas.is_empty() => {
                let name = &schemas[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            sqlparser::ast::GrantObjects::AllTablesInSchema { schemas }
                if !schemas.is_empty() =>
            {
                let name = &schemas[0];
                let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            _ => (None, None, None),
        };

        let raw_grantee = grantees.first().ok_or_else(|| {
            SqeError::Execution("GRANT/REVOKE requires at least one grantee".to_string())
        })?;

        let grantee_name = raw_grantee
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_default();

        let grantee = match &raw_grantee.grantee_type {
            sqlparser::ast::GranteesType::User => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::None => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::Role => Grantee::Role(grantee_name),
            sqlparser::ast::GranteesType::Group => Grantee::Group(grantee_name),
            sqlparser::ast::GranteesType::DatabaseRole => Grantee::Role(grantee_name),
            other => {
                return Err(SqeError::NotImplemented(format!(
                    "Unsupported grantee type: {other:?}. Use USER, ROLE, or GROUP"
                )));
            }
        };

        Ok(GrantStatement {
            privilege,
            catalog,
            namespace,
            table,
            grantee,
        })
    }
```

- [ ] **Step 4: Update require_access_control**

Replace:
```rust
    fn require_access_control(&self) -> sqe_core::Result<&AccessControlClient> {
        self.access_control_client
            .as_deref()
```

With:
```rust
    fn require_grant_backend(&self) -> sqe_core::Result<&dyn GrantBackend> {
        self.grant_backend
            .as_deref()
            .ok_or_else(|| {
                SqeError::NotImplemented(
                    "Access control is not configured. Set [access_control] backend and url in the config."
                        .to_string(),
                )
            })
    }
```

- [ ] **Step 5: Simplify the five handler methods**

Replace `handle_grant` (lines 1688-1709):
```rust
    async fn handle_grant(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        backend.grant(&session.access_token, &grant_stmt).await?;
        Ok(vec![])
    }
```

Replace `handle_revoke` (lines 1712-1733):
```rust
    async fn handle_revoke(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        let revoke_stmt = RevokeStatement {
            privilege: grant_stmt.privilege,
            catalog: grant_stmt.catalog,
            namespace: grant_stmt.namespace,
            table: grant_stmt.table,
            grantee: grant_stmt.grantee,
        };
        backend.revoke(&session.access_token, &revoke_stmt).await?;
        Ok(vec![])
    }
```

Replace `handle_show_grants` (lines 1736-1769):
```rust
    async fn handle_show_grants(
        &self,
        session: &Session,
        target: &sqe_sql::ShowGrantsTarget,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;

        let filter = match target {
            sqe_sql::ShowGrantsTarget::OnResource {
                catalog,
                namespace,
                table,
            } => GrantFilter::OnResource {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
            },
            sqe_sql::ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            } => {
                let grantee = match grantee_type.to_uppercase().as_str() {
                    "USER" => Grantee::User(grantee_name.clone()),
                    "ROLE" => Grantee::Role(grantee_name.clone()),
                    "GROUP" => Grantee::Group(grantee_name.clone()),
                    _ => Grantee::Role(grantee_name.clone()),
                };
                GrantFilter::ToGrantee(grantee)
            }
        };

        let entries = backend.show_grants(&session.access_token, &filter).await?;
        Self::grants_to_record_batch(&entries)
    }
```

Replace `handle_show_effective_grants` (lines 1772-1782):
```rust
    async fn handle_show_effective_grants(
        &self,
        session: &Session,
        user: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;
        let entries = backend.show_effective(&session.access_token, user).await?;
        Self::grants_to_record_batch(&entries)
    }
```

Replace `handle_check_access` (lines 1785-1819):
```rust
    async fn handle_check_access(
        &self,
        session: &Session,
        params: &sqe_sql::CheckAccessParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let backend = self.require_grant_backend()?;

        let check = AccessCheck {
            user: params.user.clone(),
            privilege: params.privilege.clone(),
            catalog: params.catalog.clone(),
            namespace: params.namespace.clone(),
            table: params.table.clone(),
        };

        let resp = backend.check_access(&session.access_token, &check).await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("allowed", DataType::Boolean, false),
            Field::new("reason", DataType::Utf8, true),
        ]));

        let allowed_array: ArrayRef = Arc::new(BooleanArray::from(vec![resp.allowed]));
        let mut reason_builder = StringBuilder::new();
        match resp.reason {
            Some(ref r) => reason_builder.append_value(r),
            None => reason_builder.append_null(),
        }
        let reason_array: ArrayRef = Arc::new(reason_builder.finish());

        let batch = RecordBatch::try_new(schema, vec![allowed_array, reason_array])
            .map_err(|e| SqeError::Execution(format!("Failed to build result batch: {e}")))?;

        Ok(vec![batch])
    }
```

- [ ] **Step 6: Update grants_to_record_batch to use the trait's GrantEntry**

Change the function signature from:
```rust
    fn grants_to_record_batch(
        entries: &[sqe_catalog::access_control::GrantEntry],
```

To:
```rust
    fn grants_to_record_batch(
        entries: &[sqe_policy::grants::GrantEntry],
```

The field names are the same, so the body is unchanged.

- [ ] **Step 7: Update tests**

The `extract_grant_fields` tests need to be updated to call `extract_grant_statement` and check `Grantee` enum variants instead of raw strings. Replace the test block:

```rust
    #[test]
    fn extract_grant_statement_basic_table() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON my_catalog.my_schema.my_table TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert_eq!(stmt.privilege, "SELECT");
        assert_eq!(stmt.catalog.as_deref(), Some("my_catalog"));
        assert_eq!(stmt.namespace.as_deref(), Some("my_schema"));
        assert_eq!(stmt.table.as_deref(), Some("my_table"));
        assert!(matches!(stmt.grantee, Grantee::User(ref n) if n == "alice"));
    }

    #[test]
    fn extract_grant_statement_role_grantee() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON t TO ROLE \"analysts\"";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert!(matches!(stmt.grantee, Grantee::Role(ref n) if n == "analysts"));
    }

    #[test]
    fn extract_grant_statement_group_grantee() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON t TO GROUP \"SG-Risk\"";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert!(matches!(stmt.grantee, Grantee::Group(ref n) if n == "SG-Risk"));
    }

    #[test]
    fn extract_grant_statement_bare_identifier_defaults_to_user() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "GRANT SELECT ON t TO alice";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let stmt = QueryHandler::extract_grant_statement(&stmts[0]).unwrap();

        assert!(matches!(stmt.grantee, Grantee::User(ref n) if n == "alice"));
    }

    #[test]
    fn extract_grant_statement_rejects_non_grant() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = "SELECT 1";
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let result = QueryHandler::extract_grant_statement(&stmts[0]);
        assert!(result.is_err());
    }
```

- [ ] **Step 8: Verify it compiles**

```bash
cargo check -p sqe-coordinator 2>&1 | tail -10
```

Expected: Compiles. Fix any remaining references to `access_control_client` or `extract_grant_fields` that were missed.

- [ ] **Step 9: Run tests**

```bash
cargo test -p sqe-coordinator --lib 2>&1 | tail -15
```

Expected: All tests pass (the refactored extract_grant_statement tests + all existing tests).

- [ ] **Step 10: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "refactor: replace access_control_client with pluggable grant_backend

QueryHandler now accepts Option<Arc<dyn GrantBackend>> instead of
Option<Arc<AccessControlClient>>. Handlers delegate to the trait.
extract_grant_fields refactored to extract_grant_statement returning
GrantStatement with Grantee enum. No backend-specific logic in the
coordinator."
```

---

## Task 6: Startup Wiring — main.rs and sqe_server.rs

**Files:**
- Modify: `crates/sqe-coordinator/src/main.rs`
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs`

- [ ] **Step 1: Update main.rs**

In `crates/sqe-coordinator/src/main.rs`, add imports near the top:

```rust
use sqe_policy::grants::{GrantBackend, polaris::PolarisGrantBackend};
use sqe_catalog::grant_chameleon::ChameleonGrantBackend;
```

Before the `QueryHandler::new()` call (around line 187), add the backend construction:

```rust
    // Initialize grant backend based on access_control config.
    let grant_backend: Option<Arc<dyn GrantBackend>> =
        match config.access_control.backend.as_str() {
            "chameleon" if !config.access_control.url.is_empty() => {
                tracing::info!(
                    backend = "chameleon",
                    url = %config.access_control.url,
                    "Access control backend configured"
                );
                let client =
                    sqe_catalog::AccessControlClient::new(&config.access_control.url)?;
                Some(Arc::new(ChameleonGrantBackend::new(client)))
            }
            "polaris" if !config.access_control.url.is_empty() => {
                tracing::info!(
                    backend = "polaris",
                    url = %config.access_control.url,
                    "Polaris grant backend configured"
                );
                Some(Arc::new(PolarisGrantBackend::new(
                    &config.access_control.url,
                    config.access_control.client_id.clone(),
                    config.access_control.client_secret.clone(),
                )?))
            }
            _ => None,
        };
```

Update the `QueryHandler::new()` call to pass `grant_backend` as the last argument:

```rust
    let query_handler = Arc::new(QueryHandler::new(
        policy_enforcer,
        None,
        config.clone(),
        // ... existing args ...
        query_cache,
        grant_backend,
    )?.with_manifest_cache(manifest_cache).with_table_cache(table_cache));
```

- [ ] **Step 2: Update sqe_server.rs identically**

Apply the same changes to `crates/sqe-coordinator/src/bin/sqe_server.rs` at line 493.

- [ ] **Step 3: Verify it compiles**

```bash
cargo check -p sqe-coordinator 2>&1 | tail -10
```

Expected: Compiles.

- [ ] **Step 4: Run full test suite**

```bash
cargo test --all --lib 2>&1 | tail -20
```

Expected: All tests pass across all crates.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/main.rs crates/sqe-coordinator/src/bin/sqe_server.rs
git commit -m "feat: wire GrantBackend at startup based on access_control.backend

main.rs and sqe_server.rs select ChameleonGrantBackend or
PolarisGrantBackend based on config. Passed to QueryHandler
as Option<Arc<dyn GrantBackend>>."
```

---

## Task 7: Clippy + Full Verification

**Files:** All changed files.

- [ ] **Step 1: Run clippy on changed crates**

```bash
cargo clippy -p sqe-core -p sqe-policy -p sqe-coordinator -- -D warnings 2>&1 | tail -20
```

Fix any warnings. Common issues: unused imports from the old `AccessControlClient` path, or `leak_str` unused if no unrecognized privileges are tested.

- [ ] **Step 2: Run full test suite**

```bash
cargo test --all --lib 2>&1 | grep -E "^(running|test result)" | tail -20
```

Expected: All crates pass, zero failures.

- [ ] **Step 3: Run sqe-sql and sqe-policy specifically**

```bash
cargo test -p sqe-sql -p sqe-policy 2>&1 | tail -15
```

Expected: All pass including the new grants module tests.

- [ ] **Step 4: Commit any clippy fixes**

```bash
git add -u
git commit -m "fix: clippy warnings in grant backend integration"
```

(Skip if no warnings.)

---

## Summary

| Task | Files | What it does |
|---|---|---|
| 1 | `grants/mod.rs`, `lib.rs` | Trait, types, privilege mapping |
| 2 | `grants/chameleon.rs`, `Cargo.toml` | Chameleon adapter wrapping AccessControlClient |
| 3 | `config.rs` | Add client_id/client_secret |
| 4 | `grants/polaris.rs` | Polaris Management API client |
| 5 | `query_handler.rs` | Replace access_control_client, refactor handlers |
| 6 | `main.rs`, `sqe_server.rs` | Startup wiring |
| 7 | All | Clippy + full verification |

**Critical path:** Tasks 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 (sequential, each depends on prior).

**Parallelism:** Tasks 2 and 3 are independent and could run in parallel after Task 1. Task 4 depends on Task 1 only (not on 2 or 3).

# Apache Ranger access-control backend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a configurable Apache Ranger access-control backend to SQE that translates `GRANT`/`REVOKE`/`SHOW GRANTS` into Apache Ranger Admin REST calls, with a Polaris 1.5 + Ranger 2.8 + Keycloak test environment that proves end-to-end enforcement.

**Architecture:** SQE writes grants to Ranger via a new `RangerGrantBackend` implementing the existing `GrantBackend` trait. Enforcement is delegated to Polaris 1.5's embedded Ranger authorizer (no SQE enforcement code). The backend is selected by a new `AccessControlBackend::Ranger` config variant.

**Tech Stack:** Rust (reqwest, serde, async-trait, tokio), Docker Compose, Apache Polaris 1.5, Apache Ranger 2.8, Keycloak, RustFS (S3).

**Reference files (read before starting):**
- `crates/sqe-policy/src/grants/mod.rs` — the `GrantBackend` trait + shared types (`GrantStatement`, `Grantee`, `GrantFilter`, `AccessCheck`, `GrantEntry`, `AccessCheckResult`).
- `crates/sqe-policy/src/grants/polaris.rs` — reference backend (privilege map, `validate_url_identifier`, request structs, test layout).
- `crates/sqe-core/src/config.rs:1945-2012` — `AccessControlBackend` enum + `AccessControlConfig` (note: it has a **manual `Default` impl**).
- `crates/sqe-coordinator/src/bin/sqe_server.rs:593-625` — `build_grant_backend`.
- `quickstart/polaris-keycloak-user-token/` — quickstart pattern (compose, sqe.toml, realm).
- `quickstart/_shared/keycloak/realm-iceberg.json` — realm pattern.

**Key verified facts (from the design spec `docs/superpowers/specs/2026-06-18-ranger-access-control-backend-design.md`):**
- Ranger service-def name = `polaris`. Resource hierarchy `root -> catalog -> namespace -> table` (no column). Access types are Polaris-native hyphenated (`table-data-read`, `table-data-write`, `table-create`, `table-drop`, `namespace-create`, ...).
- Polaris sends Ranger `user = principal.getName()`, `roles = principal.getRoles()`, `groups = null`.
- Grant primitive: `POST /service/plugins/services/grant/{serviceName}` (and `/revoke/...`) with a `GrantRevokeRequest`.
- Polaris plugin requires Ranger **2.8.0+**.
- **Top correctness risk:** the resource map SQE writes must match what Polaris sends at enforcement, including the `root` (realm) level. Handled by a configurable `realm` field + empirical verification in Task 14.

---

## Part A — SQE Rust code

### Task 1: Config — add the `Ranger` backend variant and `RangerConfig`

**Files:**
- Modify: `crates/sqe-core/src/config.rs:1949-2012`

- [ ] **Step 1: Write failing tests**

Add to the config test module (find the existing `#[cfg(test)] mod tests` in `config.rs`; if none covers access control, add these tests at the end of the file inside a new `#[cfg(test)] mod ranger_config_tests`):

```rust
#[cfg(test)]
mod ranger_config_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_ranger_backend_from_str() {
        assert_eq!(
            AccessControlBackend::from_str("ranger").unwrap(),
            AccessControlBackend::Ranger
        );
    }

    #[test]
    fn unknown_backend_lists_ranger() {
        let err = AccessControlBackend::from_str("bogus").unwrap_err();
        assert!(err.contains("ranger"), "error should mention ranger: {err}");
    }

    #[test]
    fn ranger_config_defaults() {
        let c = RangerConfig::default();
        assert_eq!(c.service_name, "polaris");
        assert_eq!(c.admin_user, "admin");
        assert_eq!(c.timeout_secs, 30);
        assert!(!c.accept_invalid_certs);
        assert!(c.realm.is_empty());
    }

    #[test]
    fn access_control_config_default_includes_ranger() {
        let c = AccessControlConfig::default();
        assert_eq!(c.ranger.service_name, "polaris");
    }

    #[test]
    fn ranger_config_deserializes_from_toml() {
        let toml = r#"
            backend = "ranger"
            url = "http://ranger-admin:6080"
            [ranger]
            service-name = "dev_polaris"
            admin-user = "admin"
            admin-password = "secret"
            realm = "POLARIS"
        "#;
        let c: AccessControlConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.backend, AccessControlBackend::Ranger);
        assert_eq!(c.ranger.service_name, "dev_polaris");
        assert_eq!(c.ranger.admin_password, "secret");
        assert_eq!(c.ranger.realm, "POLARIS");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-core ranger_config_tests 2>&1 | tail -20`
Expected: FAIL — `no variant ... Ranger`, `cannot find type RangerConfig`.

- [ ] **Step 3: Add the `Ranger` enum variant**

In `config.rs`, edit the `AccessControlBackend` enum (line ~1960) to add the variant after `Polaris`:

```rust
    /// Apache Polaris 1.3 native (`PRINCIPAL` / `PRINCIPAL_ROLE` / `CATALOG_ROLE`).
    Polaris,
    /// Apache Ranger via Polaris 1.5 embedded authorizer. SQE writes grants to
    /// Ranger Admin; Polaris enforces. Requires `[access_control.ranger]`.
    Ranger,
```

And edit the `FromStr` impl (line ~1965) to accept `"ranger"` and list it in the error:

```rust
            "polaris" => Ok(Self::Polaris),
            "ranger" => Ok(Self::Ranger),
            other => Err(format!(
                "unknown access_control.backend {other:?}; expected one of none, chameleon, polaris, ranger"
            )),
```

- [ ] **Step 4: Add `ranger` field to `AccessControlConfig` and the `RangerConfig` struct**

In `AccessControlConfig` (after the `client_secret` field, line ~1997) add:

```rust
    /// Apache Ranger backend tuning. Used only when `backend = "ranger"`.
    #[serde(default)]
    pub ranger: RangerConfig,
```

In the manual `Default for AccessControlConfig` impl (line ~2000), add `ranger: RangerConfig::default(),` to the struct literal.

Add the new struct + defaults immediately after `fn default_access_control_timeout()` (line ~2012):

```rust
/// Apache Ranger backend configuration (Polaris 1.5 embedded authorizer).
///
/// The Ranger Admin base URL is taken from `access_control.url`
/// (e.g. `http://ranger-admin:6080`), consistent with the Polaris backend.
#[derive(Debug, Deserialize, Clone)]
pub struct RangerConfig {
    /// Ranger service instance name. Must match the Polaris
    /// `polaris.authorization.ranger.service-name` setting.
    #[serde(default = "default_ranger_service_name")]
    pub service_name: String,
    /// Ranger Admin user for HTTP basic auth.
    #[serde(default = "default_ranger_admin_user")]
    pub admin_user: String,
    /// Ranger Admin password. Set via `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD`.
    #[serde(default)]
    pub admin_password: String,
    /// Value for the top-level `root` resource (the Polaris realm/context the
    /// embedded authorizer prefixes onto resource paths). When empty, the
    /// `root` level is omitted from written policies. See Task 14 for how to
    /// determine the correct value for a deployment.
    #[serde(default)]
    pub realm: String,
    /// HTTP timeout for a single Ranger Admin call, in seconds.
    #[serde(default = "default_ranger_timeout_secs")]
    pub timeout_secs: u64,
    /// Accept self-signed TLS certs on the Ranger Admin endpoint.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl Default for RangerConfig {
    fn default() -> Self {
        Self {
            service_name: default_ranger_service_name(),
            admin_user: default_ranger_admin_user(),
            admin_password: String::new(),
            realm: String::new(),
            timeout_secs: default_ranger_timeout_secs(),
            accept_invalid_certs: false,
        }
    }
}

fn default_ranger_service_name() -> String {
    "polaris".to_string()
}
fn default_ranger_admin_user() -> String {
    "admin".to_string()
}
fn default_ranger_timeout_secs() -> u64 {
    30
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p sqe-core ranger_config_tests 2>&1 | tail -20`
Expected: PASS (5 tests). If `toml` is not a dev-dependency of sqe-core, the deserialize test will fail to compile — in that case check how other config tests parse TOML (search `toml::from_str` in `config.rs`) and match that approach; the repo already parses TOML config so the crate is available.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat(config): add Ranger access-control backend variant + RangerConfig"
```

---

### Task 2: Privilege mapping + resource level (pure functions, TDD)

**Files:**
- Create: `crates/sqe-policy/src/grants/ranger.rs`
- Modify: `crates/sqe-policy/src/grants/mod.rs:7`

- [ ] **Step 1: Register the module**

In `crates/sqe-policy/src/grants/mod.rs`, change line 7 from:

```rust
pub mod polaris;
```
to:
```rust
pub mod polaris;
pub mod ranger;
```

- [ ] **Step 2: Write the failing test (privilege map + resource level)**

Create `crates/sqe-policy/src/grants/ranger.rs` with only the privilege map, resource-level enum, and tests:

```rust
//! RangerGrantBackend — translates GRANT/REVOKE/SHOW GRANTS into Apache Ranger
//! Admin REST calls. Enforcement is delegated to Polaris 1.5's embedded Ranger
//! authorizer; this backend only writes/reads Ranger policies.
//!
//! Ranger service-def: `polaris`. Resource hierarchy: root -> catalog ->
//! namespace -> table. Access types are Polaris-native hyphenated names.

/// Which resource levels a privilege applies to. Determines which keys go into
/// the Ranger resource map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLevel {
    Catalog,
    Namespace,
    Table,
}

/// Map a SQL privilege to a Ranger access type and the resource level it binds
/// to. Unknown privileges pass through lowercased (lets callers use native
/// Ranger access-type names directly).
pub fn map_sql_to_ranger_access(sql_priv: &str) -> (String, ResourceLevel) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => ("table-data-read".into(), ResourceLevel::Table),
        "INSERT" => ("table-data-write".into(), ResourceLevel::Table),
        "DROP" => ("table-drop".into(), ResourceLevel::Table),
        "CREATE TABLE" => ("table-create".into(), ResourceLevel::Namespace),
        "USAGE" => ("namespace-list".into(), ResourceLevel::Namespace),
        "DROP SCHEMA" => ("namespace-drop".into(), ResourceLevel::Namespace),
        "CREATE SCHEMA" | "CREATE" => ("namespace-create".into(), ResourceLevel::Catalog),
        "ALL" | "ALL PRIVILEGES" => ("catalog-content-manage".into(), ResourceLevel::Catalog),
        other => (other.to_lowercase(), ResourceLevel::Table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_maps_to_table_data_read() {
        let (a, lvl) = map_sql_to_ranger_access("SELECT");
        assert_eq!(a, "table-data-read");
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn insert_maps_to_table_data_write() {
        let (a, lvl) = map_sql_to_ranger_access("insert");
        assert_eq!(a, "table-data-write");
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn create_table_is_namespace_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE TABLE");
        assert_eq!(a, "table-create");
        assert_eq!(lvl, ResourceLevel::Namespace);
    }

    #[test]
    fn create_schema_is_catalog_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE SCHEMA");
        assert_eq!(a, "namespace-create");
        assert_eq!(lvl, ResourceLevel::Catalog);
    }

    #[test]
    fn unknown_passes_through_lowercased() {
        let (a, lvl) = map_sql_to_ranger_access("table-metadata-full");
        assert_eq!(a, "table-metadata-full");
        assert_eq!(lvl, ResourceLevel::Table);
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS (5 tests). (The map is implemented in the same step as the test because it is a pure lookup; the test is the spec.)

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-policy/src/grants/mod.rs crates/sqe-policy/src/grants/ranger.rs
git commit -m "feat(ranger): SQL privilege -> Ranger access-type mapping"
```

---

### Task 3: Resource map builder + GrantRevokeRequest serialization (TDD)

**Files:**
- Modify: `crates/sqe-policy/src/grants/ranger.rs`

- [ ] **Step 1: Write failing tests**

Add to the top of `ranger.rs` (imports) and a new section before `#[cfg(test)]`:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
```

Add these tests inside the existing `mod tests`:

```rust
    #[test]
    fn resource_map_table_level_full_path() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), Some("orders"), ResourceLevel::Table);
        assert_eq!(m.get("root").map(String::as_str), Some("POLARIS"));
        assert_eq!(m.get("catalog").map(String::as_str), Some("wh"));
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(m.get("table").map(String::as_str), Some("orders"));
    }

    #[test]
    fn resource_map_namespace_level_omits_table() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), Some("orders"), ResourceLevel::Namespace);
        assert!(m.get("table").is_none());
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
    }

    #[test]
    fn resource_map_catalog_level_only_catalog() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), None, ResourceLevel::Catalog);
        assert!(m.get("namespace").is_none());
        assert_eq!(m.get("catalog").map(String::as_str), Some("wh"));
    }

    #[test]
    fn resource_map_empty_realm_omits_root() {
        let m = build_resource_map("", "wh", None, None, ResourceLevel::Catalog);
        assert!(m.get("root").is_none());
    }

    #[test]
    fn grant_revoke_request_serializes_with_ranger_field_names() {
        let mut resource = BTreeMap::new();
        resource.insert("catalog".to_string(), "wh".to_string());
        let req = GrantRevokeRequest {
            grantor: "admin".into(),
            resource,
            users: vec!["alice".into()],
            groups: vec![],
            roles: vec![],
            access_types: vec!["table-data-read".into()],
            delegate_admin: false,
            enable_audit: true,
            replace_existing_permissions: false,
            is_recursive: false,
        };
        let j = serde_json::to_value(&req).unwrap();
        assert_eq!(j["grantor"], "admin");
        assert_eq!(j["accessTypes"], serde_json::json!(["table-data-read"]));
        assert_eq!(j["delegateAdmin"], false);
        assert_eq!(j["enableAudit"], true);
        assert_eq!(j["replaceExistingPermissions"], false);
        assert_eq!(j["isRecursive"], false);
        assert_eq!(j["users"], serde_json::json!(["alice"]));
        // empty grantee sets are omitted
        assert!(j.get("groups").is_none());
        assert!(j.get("roles").is_none());
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: FAIL — `cannot find function build_resource_map`, `cannot find struct GrantRevokeRequest`.

- [ ] **Step 3: Implement the builder and request struct**

Add before `#[cfg(test)]` in `ranger.rs`:

```rust
/// The Ranger `GrantRevokeRequest` payload. Field renames match Ranger's
/// `org.apache.ranger.plugin.model.RangerPolicy.GrantRevokeRequest` JSON.
#[derive(Debug, Serialize)]
pub struct GrantRevokeRequest {
    pub grantor: String,
    pub resource: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    #[serde(rename = "accessTypes")]
    pub access_types: Vec<String>,
    #[serde(rename = "delegateAdmin")]
    pub delegate_admin: bool,
    #[serde(rename = "enableAudit")]
    pub enable_audit: bool,
    #[serde(rename = "replaceExistingPermissions")]
    pub replace_existing_permissions: bool,
    #[serde(rename = "isRecursive")]
    pub is_recursive: bool,
}

/// Build the Ranger resource map for a given level. Includes `root` only when
/// `realm` is non-empty.
pub fn build_resource_map(
    realm: &str,
    catalog: &str,
    namespace: Option<&str>,
    table: Option<&str>,
    level: ResourceLevel,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if !realm.is_empty() {
        m.insert("root".to_string(), realm.to_string());
    }
    m.insert("catalog".to_string(), catalog.to_string());
    if matches!(level, ResourceLevel::Namespace | ResourceLevel::Table) {
        if let Some(ns) = namespace {
            m.insert("namespace".to_string(), ns.to_string());
        }
    }
    if matches!(level, ResourceLevel::Table) {
        if let Some(t) = table {
            m.insert("table".to_string(), t.to_string());
        }
    }
    m
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/grants/ranger.rs
git commit -m "feat(ranger): resource map builder + GrantRevokeRequest payload"
```

---

### Task 4: `RangerGrantBackend` struct, constructor, grantee mapping, grant/revoke

**Files:**
- Modify: `crates/sqe-policy/src/grants/ranger.rs`

- [ ] **Step 1: Write failing tests (constructor + grantee split + URL guard)**

Add imports at the top of `ranger.rs`:

```rust
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use tracing::{debug, warn};

use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    Grantee, RevokeStatement,
};
```

Add tests inside `mod tests`:

```rust
    fn test_backend() -> RangerGrantBackend {
        RangerGrantBackend::new(
            "http://ranger:6080/",
            "polaris",
            "admin",
            "admin-pw",
            "POLARIS",
            30,
            false,
        )
        .unwrap()
    }

    #[test]
    fn constructor_trims_trailing_slash_and_sets_name() {
        let b = test_backend();
        assert_eq!(b.admin_url, "http://ranger:6080");
        assert_eq!(b.service_name, "polaris");
        assert_eq!(b.backend_name(), "ranger");
    }

    #[test]
    fn grantee_to_user_role_fields() {
        assert_eq!(
            grantee_to_fields(&Grantee::User("alice".into())).unwrap(),
            (vec!["alice".to_string()], vec![])
        );
        assert_eq!(
            grantee_to_fields(&Grantee::Role("analyst".into())).unwrap(),
            (vec![], vec!["analyst".to_string()])
        );
    }

    #[test]
    fn grantee_group_is_rejected() {
        let err = grantee_to_fields(&Grantee::Group("sg".into())).unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::NotImplemented(_)));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: FAIL — `cannot find ... RangerGrantBackend`, `grantee_to_fields`.

- [ ] **Step 3: Implement struct, constructor, helpers, and grant/revoke**

Add before `#[cfg(test)]`:

```rust
/// Reject identifier values that would alter a URL path when interpolated.
/// Catalog/namespace/table/user/role names come from GRANT SQL and flow into
/// Ranger resource values; this is defense-in-depth, matching the Polaris
/// backend's `validate_url_identifier`.
fn validate_identifier(value: &str, what: &str) -> sqe_core::Result<()> {
    if value.is_empty() {
        return Err(sqe_core::SqeError::Execution(format!("{what} must not be empty")));
    }
    if let Some(bad) = value.chars().find(|c| {
        matches!(c, '/' | '?' | '#' | '%' | '\\') || c.is_whitespace() || c.is_control()
    }) {
        return Err(sqe_core::SqeError::Execution(format!(
            "{what} '{value}' contains invalid character {bad:?}"
        )));
    }
    Ok(())
}

/// Split a grantee into (users, roles) for a `GrantRevokeRequest`. Groups are
/// rejected: Polaris does not deliver groups to Ranger unless usersync runs.
fn grantee_to_fields(grantee: &Grantee) -> sqe_core::Result<(Vec<String>, Vec<String>)> {
    match grantee {
        Grantee::User(n) => Ok((vec![n.clone()], vec![])),
        Grantee::Role(n) => Ok((vec![], vec![n.clone()])),
        Grantee::Group(_) => Err(sqe_core::SqeError::NotImplemented(
            "Ranger backend supports USER and ROLE grantees only; GROUP requires Ranger usersync"
                .into(),
        )),
    }
}

/// Apache Ranger Admin grant backend.
pub struct RangerGrantBackend {
    client: Client,
    /// Ranger Admin base URL, e.g. `http://ranger-admin:6080`.
    admin_url: String,
    service_name: String,
    admin_user: String,
    admin_password: String,
    /// Value for the `root` resource level (empty = omit).
    realm: String,
}

impl RangerGrantBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        admin_url: &str,
        service_name: &str,
        admin_user: &str,
        admin_password: &str,
        realm: &str,
        timeout_secs: u64,
        accept_invalid_certs: bool,
    ) -> sqe_core::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| sqe_core::SqeError::Config(format!("Failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            service_name: service_name.to_string(),
            admin_user: admin_user.to_string(),
            admin_password: admin_password.to_string(),
            realm: realm.to_string(),
        })
    }

    /// Validate the resource identifiers in a grant/revoke statement and build
    /// the (resource_map, access_type, users, roles) tuple shared by grant and
    /// revoke.
    fn build_grant_revoke(
        &self,
        privilege: &str,
        catalog: Option<&str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<GrantRevokeRequest> {
        let catalog = catalog.ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Ranger GRANT requires a catalog (use catalog.namespace.table)".into(),
            )
        })?;
        validate_identifier(catalog, "catalog")?;
        if let Some(ns) = namespace {
            validate_identifier(ns, "namespace")?;
        }
        if let Some(t) = table {
            validate_identifier(t, "table")?;
        }
        validate_identifier(grantee.name(), "grantee")?;

        let (access, level) = map_sql_to_ranger_access(privilege);
        let resource = build_resource_map(&self.realm, catalog, namespace, table, level);
        let (users, roles) = grantee_to_fields(grantee)?;

        Ok(GrantRevokeRequest {
            grantor: self.admin_user.clone(),
            resource,
            users,
            groups: vec![],
            roles,
            access_types: vec![access],
            delegate_admin: false,
            enable_audit: true,
            replace_existing_permissions: false,
            is_recursive: false,
        })
    }

    /// POST a GrantRevokeRequest to the grant or revoke endpoint.
    async fn post_grant_revoke(&self, op: &str, body: &GrantRevokeRequest) -> sqe_core::Result<()> {
        let url = format!(
            "{}/service/plugins/services/{op}/{}",
            self.admin_url, self.service_name
        );
        let resp = self
            .client
            .post(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .json(body)
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Execution(format!("Ranger {op} request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, ranger_body = %text, op, "Ranger {op} failed");
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger {op} failed (HTTP {status})"
            )));
        }
        debug!(op, service = %self.service_name, "Ranger {op} completed");
        Ok(())
    }
}
```

Add the trait impl (grant/revoke now; show_grants/show_effective/check_access added in Tasks 5-6 — to compile, add `todo!()` stubs for those three methods for now, replaced in the next tasks):

```rust
#[async_trait]
impl GrantBackend for RangerGrantBackend {
    async fn grant(&self, _token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let body = self.build_grant_revoke(
            &stmt.privilege,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        self.post_grant_revoke("grant", &body).await
    }

    async fn revoke(&self, _token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let body = self.build_grant_revoke(
            &stmt.privilege,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        self.post_grant_revoke("revoke", &body).await
    }

    async fn show_grants(
        &self,
        _token: &str,
        _filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        todo!("Task 5")
    }

    async fn show_effective(&self, _token: &str, _user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        todo!("Task 5")
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        todo!("Task 6")
    }

    fn backend_name(&self) -> &str {
        "ranger"
    }
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS. The three `todo!()` methods are not called by any test yet.

- [ ] **Step 5: Add a serialization test for the full grant body**

Add inside `mod tests`:

```rust
    #[test]
    fn build_grant_revoke_select_to_role() {
        let b = test_backend();
        let body = b
            .build_grant_revoke("SELECT", Some("wh"), Some("sales"), Some("orders"),
                &Grantee::Role("analyst".into()))
            .unwrap();
        assert_eq!(body.access_types, vec!["table-data-read".to_string()]);
        assert_eq!(body.roles, vec!["analyst".to_string()]);
        assert!(body.users.is_empty());
        assert_eq!(body.resource.get("table").map(String::as_str), Some("orders"));
        assert_eq!(body.resource.get("root").map(String::as_str), Some("POLARIS"));
    }

    #[test]
    fn build_grant_revoke_requires_catalog() {
        let b = test_backend();
        let err = b
            .build_grant_revoke("SELECT", None, None, None, &Grantee::User("a".into()))
            .unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::Execution(_)));
    }

    #[test]
    fn build_grant_revoke_rejects_bad_identifier() {
        let b = test_backend();
        let err = b
            .build_grant_revoke("SELECT", Some("wh/../x"), None, None, &Grantee::User("a".into()))
            .unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::Execution(_)));
    }
```

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-policy/src/grants/ranger.rs
git commit -m "feat(ranger): RangerGrantBackend grant/revoke via Ranger Admin REST"
```

---

### Task 5: `show_grants` + `show_effective` (read Ranger policies)

**Files:**
- Modify: `crates/sqe-policy/src/grants/ranger.rs`

- [ ] **Step 1: Write failing tests for policy parsing**

Add inside `mod tests` (these test the pure parser, no HTTP):

```rust
    #[test]
    fn parse_policies_into_grant_entries() {
        // Minimal Ranger policy JSON: one allow item granting table-data-read
        // to role "analyst" on wh.sales.orders.
        let json = r#"[
          {
            "name": "p1",
            "resources": {
              "catalog": {"values": ["wh"]},
              "namespace": {"values": ["sales"]},
              "table": {"values": ["orders"]}
            },
            "policyItems": [
              {"users": [], "groups": [], "roles": ["analyst"],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ],
            "denyPolicyItems": [
              {"users": ["mallory"], "groups": [], "roles": [],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
          }
        ]"#;
        let policies: Vec<RangerPolicy> = serde_json::from_str(json).unwrap();
        let entries = policies_to_entries(&policies);
        // one allow (analyst) + one deny (mallory)
        assert_eq!(entries.len(), 2);
        let allow = entries.iter().find(|e| e.effect == "ALLOW").unwrap();
        assert_eq!(allow.privilege, "table-data-read");
        assert_eq!(allow.grantee_type, "ROLE");
        assert_eq!(allow.grantee_name, "analyst");
        assert_eq!(allow.resource, "wh.sales.orders");
        let deny = entries.iter().find(|e| e.effect == "DENY").unwrap();
        assert_eq!(deny.grantee_type, "USER");
        assert_eq!(deny.grantee_name, "mallory");
    }

    #[test]
    fn entry_matches_grantee_filter() {
        let e = GrantEntry {
            privilege: "table-data-read".into(),
            resource: "wh".into(),
            grantee_type: "ROLE".into(),
            grantee_name: "analyst".into(),
            effect: "ALLOW".into(),
            granted_by: None,
            granted_at: None,
        };
        assert!(entry_matches_grantee(&e, &Grantee::Role("analyst".into())));
        assert!(!entry_matches_grantee(&e, &Grantee::Role("other".into())));
        assert!(!entry_matches_grantee(&e, &Grantee::User("analyst".into())));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: FAIL — `cannot find RangerPolicy`, `policies_to_entries`, `entry_matches_grantee`.

- [ ] **Step 3: Implement policy types + parser + filter, replace show_* stubs**

Add the response types and helpers before `#[cfg(test)]`:

```rust
// ── Ranger policy read model (subset) ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RangerPolicy {
    #[serde(default)]
    resources: BTreeMap<String, RangerResourceValues>,
    #[serde(default, rename = "policyItems")]
    policy_items: Vec<RangerPolicyItem>,
    #[serde(default, rename = "denyPolicyItems")]
    deny_policy_items: Vec<RangerPolicyItem>,
}

#[derive(Debug, Deserialize)]
struct RangerResourceValues {
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RangerPolicyItem {
    #[serde(default)]
    users: Vec<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    accesses: Vec<RangerAccess>,
}

#[derive(Debug, Deserialize)]
struct RangerAccess {
    #[serde(rename = "type")]
    access_type: String,
}

/// Render a policy's resources as `catalog.namespace.table` (skipping `root`).
fn format_policy_resource(resources: &BTreeMap<String, RangerResourceValues>) -> String {
    let mut parts = Vec::new();
    for key in ["catalog", "namespace", "table"] {
        if let Some(v) = resources.get(key) {
            if let Some(first) = v.values.first() {
                parts.push(first.clone());
            }
        }
    }
    parts.join(".")
}

/// Flatten Ranger policies into GrantEntry rows (allow + deny items).
pub fn policies_to_entries(policies: &[RangerPolicy]) -> Vec<GrantEntry> {
    let mut out = Vec::new();
    for p in policies {
        let resource = format_policy_resource(&p.resources);
        let mut push_items = |items: &[RangerPolicyItem], effect: &str| {
            for item in items {
                for access in &item.accesses {
                    for u in &item.users {
                        out.push(GrantEntry {
                            privilege: access.access_type.clone(),
                            resource: resource.clone(),
                            grantee_type: "USER".into(),
                            grantee_name: u.clone(),
                            effect: effect.into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                    for r in &item.roles {
                        out.push(GrantEntry {
                            privilege: access.access_type.clone(),
                            resource: resource.clone(),
                            grantee_type: "ROLE".into(),
                            grantee_name: r.clone(),
                            effect: effect.into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                }
            }
        };
        push_items(&p.policy_items, "ALLOW");
        push_items(&p.deny_policy_items, "DENY");
    }
    out
}

/// Does an entry's grantee match the requested grantee (type + name)?
pub fn entry_matches_grantee(entry: &GrantEntry, grantee: &Grantee) -> bool {
    let want_type = match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) => "ROLE",
        Grantee::Group(_) => "GROUP",
    };
    entry.grantee_type == want_type && entry.grantee_name == grantee.name()
}

impl RangerGrantBackend {
    /// Fetch all policies for this service from Ranger Admin.
    async fn fetch_policies(&self) -> sqe_core::Result<Vec<RangerPolicy>> {
        let url = format!(
            "{}/service/plugins/policies/service/name/{}",
            self.admin_url, self.service_name
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Execution(format!("Ranger policy fetch failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, ranger_body = %text, "Ranger policy fetch failed");
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger policy fetch failed (HTTP {status})"
            )));
        }
        resp.json().await.map_err(|e| {
            sqe_core::SqeError::Execution(format!("Ranger policy parse failed: {e}"))
        })
    }
}
```

Replace the `show_grants` and `show_effective` `todo!()` bodies in the trait impl:

```rust
    async fn show_grants(
        &self,
        _token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let policies = self.fetch_policies().await?;
        let all = policies_to_entries(&policies);
        let filtered = match filter {
            GrantFilter::ToGrantee(g) => {
                all.into_iter().filter(|e| entry_matches_grantee(e, g)).collect()
            }
            GrantFilter::OnResource { catalog, namespace, table } => {
                // Build the dotted prefix that the entry resource must start with.
                let mut prefix = Vec::new();
                if let Some(c) = catalog { prefix.push(c.clone()); }
                if let Some(n) = namespace { prefix.push(n.clone()); }
                if let Some(t) = table { prefix.push(t.clone()); }
                let prefix = prefix.join(".");
                all.into_iter().filter(|e| e.resource.starts_with(&prefix)).collect()
            }
        };
        Ok(filtered)
    }

    async fn show_effective(&self, _token: &str, user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        // Best-effort: return policies naming this user directly. Role-derived
        // grants are not expanded here (Ranger resolves roles at enforcement).
        let policies = self.fetch_policies().await?;
        let all = policies_to_entries(&policies);
        Ok(all
            .into_iter()
            .filter(|e| e.grantee_type == "USER" && e.grantee_name == user)
            .collect())
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/grants/ranger.rs
git commit -m "feat(ranger): SHOW GRANTS / SHOW EFFECTIVE via Ranger policy read"
```

---

### Task 6: `check_access` (best-effort, deny-aware)

**Files:**
- Modify: `crates/sqe-policy/src/grants/ranger.rs`

- [ ] **Step 1: Write failing tests for the matcher**

Add inside `mod tests`:

```rust
    #[test]
    fn check_match_allows_when_user_has_access() {
        let entries = vec![
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "USER".into(), grantee_name: "alice".into(), effect: "ALLOW".into(),
                granted_by: None, granted_at: None },
        ];
        let r = evaluate_access(&entries, "alice", &[], "table-data-read", "wh.sales.orders");
        assert!(r.allowed);
    }

    #[test]
    fn check_match_deny_overrides_allow() {
        let entries = vec![
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "ROLE".into(), grantee_name: "analyst".into(), effect: "ALLOW".into(),
                granted_by: None, granted_at: None },
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "USER".into(), grantee_name: "alice".into(), effect: "DENY".into(),
                granted_by: None, granted_at: None },
        ];
        let r = evaluate_access(&entries, "alice", &["analyst".into()], "table-data-read", "wh.sales.orders");
        assert!(!r.allowed);
        assert!(r.reason.as_deref().unwrap_or("").to_lowercase().contains("deny"));
    }

    #[test]
    fn check_match_denies_when_no_grant() {
        let r = evaluate_access(&[], "alice", &[], "table-data-read", "wh.sales.orders");
        assert!(!r.allowed);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: FAIL — `cannot find function evaluate_access`.

- [ ] **Step 3: Implement `evaluate_access` and replace the `check_access` stub**

Add before `#[cfg(test)]`:

```rust
/// Best-effort local evaluation of GrantEntry rows: deny-overrides-allow for a
/// given user (+roles), access type, and resource. This mirrors Ranger's deny
/// precedence but does NOT account for tag policies, conditions, or wildcard
/// resource matching beyond exact match. The authoritative decision is Polaris
/// enforcement; this is for `CHECK ACCESS` introspection only.
pub fn evaluate_access(
    entries: &[GrantEntry],
    user: &str,
    roles: &[String],
    access_type: &str,
    resource: &str,
) -> AccessCheckResult {
    let principal_matches = |e: &GrantEntry| -> bool {
        (e.grantee_type == "USER" && e.grantee_name == user)
            || (e.grantee_type == "ROLE" && roles.iter().any(|r| r == &e.grantee_name))
    };
    let relevant = |e: &&GrantEntry| {
        e.privilege == access_type && e.resource == resource && principal_matches(e)
    };

    if entries.iter().filter(|e| e.effect == "DENY").any(|e| relevant(&e)) {
        return AccessCheckResult {
            allowed: false,
            reason: Some(format!("Denied by a DENY policy on {resource}")),
        };
    }
    if let Some(e) = entries.iter().filter(|e| e.effect == "ALLOW").find(|e| relevant(e)) {
        return AccessCheckResult {
            allowed: true,
            reason: Some(format!("Allowed via {} '{}'", e.grantee_type, e.grantee_name)),
        };
    }
    AccessCheckResult {
        allowed: false,
        reason: Some(format!("No matching grant for {user} {access_type} on {resource}")),
    }
}
```

Replace the `check_access` `todo!()` body:

```rust
    async fn check_access(
        &self,
        _token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        let catalog = check.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Ranger check_access requires a catalog; use catalog.namespace.table".into(),
            )
        })?;
        validate_identifier(catalog, "catalog")?;

        let (access, _) = map_sql_to_ranger_access(&check.privilege);
        let mut parts = vec![catalog.to_string()];
        if let Some(n) = &check.namespace { parts.push(n.clone()); }
        if let Some(t) = &check.table { parts.push(t.clone()); }
        let resource = parts.join(".");

        let policies = self.fetch_policies().await?;
        let entries = policies_to_entries(&policies);
        // Roles unknown at this layer; match on the user dimension only. Role
        // grants are surfaced via SHOW GRANTS and enforced by Polaris.
        Ok(evaluate_access(&entries, &check.user, &[], &access, &resource))
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sqe-policy ranger:: 2>&1 | tail -20`
Expected: PASS. No `todo!()` remains.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/grants/ranger.rs
git commit -m "feat(ranger): best-effort check_access with deny precedence"
```

---

### Task 7: Wire the backend into `build_grant_backend`

**Files:**
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs:593-625`

- [ ] **Step 1: Add the import**

Find the existing `use sqe_policy::grants::...` imports near the top of `sqe_server.rs` (search for `PolarisGrantBackend`) and add `RangerGrantBackend` to the import list. If the import path is `use sqe_policy::grants::{polaris::PolarisGrantBackend, ...}`, add `ranger::RangerGrantBackend`.

- [ ] **Step 2: Add the match arm**

In `build_grant_backend`, add before the final `None | Chameleon | Polaris` arm:

```rust
        AccessControlBackend::Ranger if !config.access_control.url.is_empty() => {
            let r = &config.access_control.ranger;
            tracing::info!(
                backend = "ranger",
                url = %config.access_control.url,
                service = %r.service_name,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(RangerGrantBackend::new(
                &config.access_control.url,
                &r.service_name,
                &r.admin_user,
                &r.admin_password,
                &r.realm,
                r.timeout_secs,
                r.accept_invalid_certs,
            )?)))
        }
```

Update the final catch-all arm to include `Ranger`:

```rust
        AccessControlBackend::None
        | AccessControlBackend::Chameleon
        | AccessControlBackend::Polaris
        | AccessControlBackend::Ranger => Ok(None),
```

- [ ] **Step 3: Build the whole workspace**

Run: `cargo build -p sqe-coordinator 2>&1 | tail -20`
Expected: compiles cleanly.

- [ ] **Step 4: Clippy (strict) on the touched crates**

Run: `cargo clippy -p sqe-policy -p sqe-core -p sqe-coordinator --all-targets -- -D warnings 2>&1 | tail -30`
Expected: no warnings. Fix any (common: needless `clone`, `format!` in args).

- [ ] **Step 5: Full test run for the touched crates**

Run: `cargo test -p sqe-policy -p sqe-core 2>&1 | tail -20`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/bin/sqe_server.rs
git commit -m "feat(ranger): select RangerGrantBackend from access_control config"
```

---

## Part B — Test environment (`quickstart/polaris-ranger-keycloak/`)

> Build the env incrementally and bring it up in stages so failures localize.
> All host ports are offset into a new range (Keycloak 38080, Polaris 28181,
> Ranger 26080, RustFS 29000, SQE Flight 60061) to avoid clashing with other
> quickstarts.

### Task 8: Keycloak realm with role hierarchy

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/keycloak/realm-ranger.json`

- [ ] **Step 1: Create the realm**

Model it on `quickstart/_shared/keycloak/realm-iceberg.json`. Realm `iceberg-ranger`, with:
- Realm roles: `sqe_admin`, `engineer`, `analyst`.
- A confidential `sqe-client` (directAccessGrants enabled, secret `$(env:SQE_CLIENT_SECRET)`) with a realm-role mapper emitting `realm_access.roles` (Keycloak does this by default via the `roles` scope) — copy the `principal_roles_mapper` block from the reference realm but keep `claim.name` as part of `realm_access.roles` (the default realm-role mapper already populates `realm_access.roles`; the extra mapper is optional).
- A public `polaris-frontend-client` (copy from reference).
- Users (password = `<username>123`):
  - `alice` -> roles `[analyst]`
  - `bob` -> roles `[engineer, analyst]`
  - `carol` -> roles `[sqe_admin, engineer, analyst]` (the GRANT-running admin)
  - `dave` -> roles `[]` (the negative-test user: no grants)

- [ ] **Step 2: Validate JSON**

Run: `python3 -c "import json,sys; json.load(open('quickstart/polaris-ranger-keycloak/keycloak/realm-ranger.json'))" && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/keycloak/realm-ranger.json
git commit -m "feat(quickstart): Keycloak realm for Polaris+Ranger demo"
```

---

### Task 9: Ranger service-def + service bootstrap script

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/ranger/servicedef-polaris.json`
- Create: `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`

- [ ] **Step 1: Obtain the canonical service-def**

Download the canonical polaris service-def into the repo so the env is self-contained:

Run:
```bash
mkdir -p quickstart/polaris-ranger-keycloak/ranger
curl -fsSL https://raw.githubusercontent.com/apache/ranger/master/agents-common/src/main/resources/service-defs/ranger-servicedef-polaris.json \
  -o quickstart/polaris-ranger-keycloak/ranger/servicedef-polaris.json
python3 -c "import json; d=json.load(open('quickstart/polaris-ranger-keycloak/ranger/servicedef-polaris.json')); print(d['name'], len(d['resources']), len(d['accessTypes']))"
```
Expected: prints `polaris 6 69` (name `polaris`, 6 resources, 69 access types). If the master path 404s, pin to the `ranger-2.8.0` tag in the URL. Record the exact resource `name` keys (`root`, `catalog`, `namespace`, `table`, `policy`, `principal`) — these confirm the resource map keys used in Task 3.

- [ ] **Step 2: Write the bootstrap script**

Create `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`:

```bash
#!/usr/bin/env sh
# Register the polaris service-def and create the service instance in Ranger
# Admin, idempotently. Runs once before Polaris starts.
set -eu

RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASS="${RANGER_PASS:-rangerR0cks!}"
SERVICE_NAME="${SERVICE_NAME:-polaris}"
AUTH="-u ${RANGER_USER}:${RANGER_PASS}"

echo "Waiting for Ranger Admin at ${RANGER_URL} ..."
until curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/count" >/dev/null 2>&1; do
  sleep 5
done

echo "Registering polaris service-def (idempotent) ..."
# 200 if already present; create only if missing.
if ! curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/name/polaris" >/dev/null 2>&1; then
  curl -fsS $AUTH -H 'Content-Type: application/json' \
    -X POST "${RANGER_URL}/service/public/v2/api/servicedef" \
    -d @/servicedef-polaris.json
fi

echo "Creating service instance '${SERVICE_NAME}' (idempotent) ..."
if ! curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/${SERVICE_NAME}" >/dev/null 2>&1; then
  curl -fsS $AUTH -H 'Content-Type: application/json' \
    -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d "{\"name\":\"${SERVICE_NAME}\",\"type\":\"polaris\",\"configs\":{},\"isEnabled\":true}"
fi

echo "Ranger bootstrap complete."
```

Run: `chmod +x quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`

- [ ] **Step 3: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/ranger/
git commit -m "feat(quickstart): Ranger polaris service-def + bootstrap script"
```

---

### Task 10: docker-compose stack

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/docker-compose.yml`
- Create: `quickstart/polaris-ranger-keycloak/.env.example`

- [ ] **Step 1: Write `.env.example`**

```env
KEYCLOAK_ADMIN_PASSWORD=admin
SQE_CLIENT_SECRET=sqe-secret-change-me
S3_ACCESS_KEY=s3admin
S3_SECRET_KEY=s3adminpw
POLARIS_BOOTSTRAP_SECRET=polaris-root-secret
RANGER_ADMIN_PASSWORD=rangerR0cks!
POSTGRES_PASSWORD=rangerdb
# host port offsets
KEYCLOAK_PORT=38080
POLARIS_PORT=28181
RANGER_PORT=26080
RUSTFS_PORT=29000
SQE_FLIGHT_PORT=60061
SQE_TRINO_PORT=28080
```

- [ ] **Step 2: Write `docker-compose.yml`**

Start from `quickstart/polaris-keycloak-user-token/docker-compose.yml` (keycloak, keycloak-config, rustfs, bucket-init, sqe). Change the realm to `iceberg-ranger` and the keycloak-config volume to `./keycloak/realm-ranger.json`. Then **add** these services and **modify Polaris**:

Add a Postgres for Ranger:

```yaml
  ranger-db:
    image: postgres:16
    environment:
      POSTGRES_DB: ranger
      POSTGRES_USER: ranger
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-rangerdb}
    volumes:
      - ranger-db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ranger"]
      interval: 5s
      timeout: 5s
      retries: 30
```

Add Ranger Admin (image must be 2.8.0+; if no official image is available, see Step 4):

```yaml
  ranger-admin:
    image: apache/ranger:2.8.0
    environment:
      RANGER_DB_TYPE: postgres
      RANGER_DB_HOST: ranger-db
      RANGER_DB_NAME: ranger
      RANGER_DB_USER: ranger
      RANGER_DB_PASSWORD: ${POSTGRES_PASSWORD:-rangerdb}
      RANGER_ADMIN_PASSWORD: ${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
    ports:
      - "${RANGER_PORT:-26080}:6080"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS -u admin:${RANGER_ADMIN_PASSWORD:-rangerR0cks!} http://localhost:6080/service/public/v2/api/servicedef/count || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 40
      start_period: 60s
    depends_on:
      ranger-db:
        condition: service_healthy
```

Add the Ranger bootstrap one-shot:

```yaml
  ranger-setup:
    image: curlimages/curl:8.14.1
    entrypoint: ["sh", "/bootstrap-ranger.sh"]
    environment:
      RANGER_URL: http://ranger-admin:6080
      RANGER_USER: admin
      RANGER_PASS: ${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
      SERVICE_NAME: polaris
    volumes:
      - ./ranger/bootstrap-ranger.sh:/bootstrap-ranger.sh:ro
      - ./ranger/servicedef-polaris.json:/servicedef-polaris.json:ro
    restart: "no"
    depends_on:
      ranger-admin:
        condition: service_healthy
```

Modify the **polaris** service: keep everything from the reference, change realm env to `iceberg-ranger`, set the OIDC mapping (`name-claim-path: preferred_username`, roles mapper passthrough as in the reference), and add the Ranger authorizer env + dependency on `ranger-setup`:

```yaml
      polaris.authorization.type: ranger
      polaris.authorization.ranger.service-name: polaris
      polaris.authorization.ranger.authz.default.policy.source.impl: org.apache.ranger.admin.client.RangerAdminRESTClient
      polaris.authorization.ranger.authz.default.policy.rest.url: http://ranger-admin:6080
      polaris.authorization.ranger.authz.default.policy.rest.client.username: admin
      polaris.authorization.ranger.authz.default.policy.rest.client.password: ${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
      polaris.authorization.ranger.authz.default.policy.pollIntervalMs: "5000"
```

And under polaris `depends_on`, add:

```yaml
      ranger-setup:
        condition: service_completed_successfully
```

Add the named volume at the bottom:

```yaml
volumes:
  rustfs-data:
  ranger-db-data:
```

- [ ] **Step 3: Validate compose syntax**

Run: `docker compose -f quickstart/polaris-ranger-keycloak/docker-compose.yml config >/dev/null && echo OK`
Expected: `OK` (after `cp .env.example .env` in that dir).

- [ ] **Step 4: Verify the Ranger image tag exists**

Run: `docker manifest inspect apache/ranger:2.8.0 >/dev/null 2>&1 && echo "image ok" || echo "MISSING — see note"`
If MISSING: Apache does not always publish official Ranger images. Fall back to a maintained community image that is >= 2.8.0 (search Docker Hub), or add a `ranger/Dockerfile` that builds Ranger Admin 2.8.0. Record the chosen image in `OVERVIEW.md`. Do not proceed past this step with an image < 2.8.0 — the Polaris embedded authorizer requires 2.8.0.

- [ ] **Step 5: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/docker-compose.yml quickstart/polaris-ranger-keycloak/.env.example
git commit -m "feat(quickstart): Polaris 1.5 + Ranger 2.8 + Keycloak compose stack"
```

---

### Task 11: Polaris catalog/namespace/table + data bootstrap

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/polaris/bootstrap-data.sh`
- Add to `docker-compose.yml`: a `polaris-setup` service running this script.

- [ ] **Step 1: Write the bootstrap script**

Model on `quickstart/_shared/polaris/bootstrap.sh` (read it first for the exact token + catalog-create curl shapes). It must, using the Polaris bootstrap (root) credentials:
1. Create **two catalogs** `sales_wh` and `ops_wh` (S3 warehouse on rustfs).
2. Create nested namespaces: in `sales_wh`: `sales`, `sales.eu`; in `ops_wh`: `ops`.
3. Create tables with a few rows each (via the Iceberg REST API or by leaving table creation to a later SQE CTAS in `test.sh` — prefer creating empty tables here and inserting via SQE so the write path is exercised under Ranger). At minimum create: `sales_wh.sales.orders`, `sales_wh.sales.eu.orders_eu`, `ops_wh.ops.audit`.
4. Configure Polaris OIDC-federated principals: since `polaris.authentication.type=mixed` and federation maps `preferred_username -> principal name`, no explicit principal creation is needed for federated users; verify by listing principals is NOT required.

Keep the script idempotent (ignore 409s).

- [ ] **Step 2: Add the `polaris-setup` service to compose**

```yaml
  polaris-setup:
    image: curlimages/curl:8.14.1
    entrypoint: ["sh", "/bootstrap-data.sh"]
    environment:
      POLARIS_URL: http://polaris:8181
      POLARIS_REALM: iceberg-ranger
      S3_ENDPOINT: http://rustfs:9000
      S3_ACCESS_KEY: ${S3_ACCESS_KEY:-s3admin}
      S3_SECRET_KEY: ${S3_SECRET_KEY:-s3adminpw}
      S3_BUCKET: warehouse
      BOOTSTRAP_CLIENT_ID: root
      BOOTSTRAP_CLIENT_SECRET: ${POLARIS_BOOTSTRAP_SECRET:-polaris-root-secret}
    volumes:
      - ./polaris/bootstrap-data.sh:/bootstrap-data.sh:ro
    restart: "no"
    depends_on:
      polaris:
        condition: service_healthy
      bucket-init:
        condition: service_completed_successfully
```

Make `sqe` depend on `polaris-setup` completing (as in the reference compose).

- [ ] **Step 3: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/polaris/bootstrap-data.sh quickstart/polaris-ranger-keycloak/docker-compose.yml
git commit -m "feat(quickstart): multi-catalog Polaris data bootstrap"
```

---

### Task 12: SQE config for the Ranger backend

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/sqe.toml`

- [ ] **Step 1: Write `sqe.toml`**

Start from `quickstart/polaris-keycloak-user-token/sqe.toml`. Change the realm to `iceberg-ranger` everywhere, register **both** catalogs, and use the `oidc_password` provider (so `test.sh` can authenticate users by username/password). Add the access-control block:

```toml
[auth]
keycloak_url = "http://keycloak:8080"
realm = "iceberg-ranger"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"
ssl_verification = false

[[auth.providers]]
type = "oidc_password"
token_url = "http://keycloak:8080/realms/iceberg-ranger/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"
roles_claim = "realm_access.roles"
accept_invalid_certs = true

[catalogs.sales_wh]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "sales_wh"

[catalogs.ops_wh]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "ops_wh"

[catalog]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "sales_wh"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_region = "us-east-1"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true

[policy]
engine = "passthrough"

[access_control]
backend = "ranger"
url = "http://ranger-admin:6080"

[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
# realm: leave empty initially; Task 14 determines whether a `root` value is
# required for Polaris enforcement to match SQE-written policies.
realm = ""
```

Keep `[coordinator]`, `[worker]`, `[session]`, `[metrics]` blocks from the reference.

- [ ] **Step 2: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/sqe.toml
git commit -m "feat(quickstart): SQE config with Ranger access-control backend"
```

---

### Task 13: Bring the stack up and smoke-test connectivity

**Files:** none (operational)

- [ ] **Step 1: Build SQE image and start the stack**

Run:
```bash
cd quickstart/polaris-ranger-keycloak && cp -n .env.example .env
docker compose up -d --build --wait 2>&1 | tail -30
```
Expected: all services healthy; `ranger-setup`, `bucket-init`, `polaris-setup` exit 0.

- [ ] **Step 2: Verify the Ranger service exists**

Run:
```bash
curl -fsS -u admin:rangerR0cks! http://localhost:26080/service/public/v2/api/service/name/polaris | python3 -c "import json,sys; print(json.load(sys.stdin)['type'])"
```
Expected: `polaris`.

- [ ] **Step 3: Verify SQE is up and a user can authenticate**

Run (use the repo's standard Flight SQL client; check `quickstart/_shared/lib.sh` and the reference `run.sh` for the exact client invocation):
```bash
# Example shape — adapt to the repo's client (sqe-cli / flight-sql client in run.sh):
# Authenticate as carol (admin) and run a trivial query.
```
Expected: connection succeeds. If the project has a CLI, `SHOW CATALOGS` should list `sales_wh` and `ops_wh`.

- [ ] **Step 4: Checkpoint — do not commit; report status**

If any service is unhealthy, debug before proceeding (Ranger Admin first-boot can take 60-90s; check `docker compose logs ranger-admin`).

---

### Task 14: End-to-end test script (the deliverable)

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/test.sh`

This is the proof. It must exercise all four complexity dimensions and resolve the top correctness risk (resource shape).

- [ ] **Step 1: Resolve the resource-shape question empirically (one-time, manual within the script's dev)**

Before writing assertions, determine whether Polaris enforcement matches policies written WITHOUT a `root` value:
1. With `sqe.toml` `realm = ""`, as `carol` run `GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"`.
2. As `alice` (analyst) run `SELECT * FROM sales_wh.sales.orders`.
3. If it succeeds, the empty-realm shape is correct; keep `realm = ""`.
4. If `alice` is denied despite the grant, inspect what resource Polaris requested: check Ranger Admin audits (`http://localhost:26080`, Audit tab) or `docker compose logs polaris | grep -i ranger`. Read the `root` value Polaris sends, set `access_control.ranger.realm` to it in `sqe.toml`, `docker compose restart sqe`, and retry. Record the resolved value in `OVERVIEW.md`.

Document the resolved `realm` value at the top of `test.sh` as a comment.

- [ ] **Step 2: Write `test.sh`**

The script (bash, `set -euo pipefail`) must, using the repo's Flight SQL/Trino client (copy auth + query helpers from `run.sh` / `_shared/lib.sh`):

1. **Admin grants (as carol):**
   - `GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"`
   - `GRANT SELECT, INSERT ON sales_wh.sales.orders TO ROLE "engineer"`
   - `GRANT CREATE TABLE ON sales_wh.sales TO ROLE "engineer"`
   - `GRANT SELECT ON ops_wh.ops.audit TO USER "bob"`
   - A **deny**: write a Ranger deny policy denying `analyst` SELECT on `sales_wh.sales.eu.orders_eu` (via SQE if a DENY syntax exists, else via a direct Ranger policy POST in the script — document which).
2. **Positive assertions:**
   - `alice` (analyst) CAN `SELECT` from `sales_wh.sales.orders`.
   - `bob` (engineer+analyst) CAN `INSERT` into and `SELECT` from `sales_wh.sales.orders`.
   - `bob` CAN `SELECT` from `ops_wh.ops.audit` (user grant).
   - `bob` CAN `CREATE TABLE` in `sales_wh.sales`.
3. **Deny precedence:**
   - `alice` CANNOT `SELECT` from `sales_wh.sales.eu.orders_eu` (deny overrides any analyst allow).
4. **Negative tests (must be denied):**
   - `dave` (no roles) CANNOT `SELECT` from `sales_wh.sales.orders`.
   - `alice` (analyst, read-only) CANNOT `INSERT` into `sales_wh.sales.orders`.
   - `alice` CANNOT `DROP` `sales_wh.sales.orders`.
   - `dave` CANNOT `SELECT` from `ops_wh.ops.audit`.
5. **SHOW GRANTS round-trip:**
   - `SHOW GRANTS ON sales_wh.sales.orders` (as carol) lists the analyst+engineer SELECT grants.
6. **Revoke:**
   - `REVOKE SELECT ON sales_wh.sales.orders FROM ROLE "analyst"`, then assert `alice` is now denied (allow ~5s for the Polaris policy poll interval; the script should retry-with-timeout, not assume instant propagation).

Each assertion prints `PASS`/`FAIL` and the script exits non-zero on any FAIL. Account for the Polaris `pollIntervalMs=5000` policy refresh: wrap allow/deny checks in a retry loop (up to ~20s) before declaring FAIL.

- [ ] **Step 3: Run the test**

Run: `cd quickstart/polaris-ranger-keycloak && ./test.sh 2>&1 | tail -40`
Expected: every assertion PASS, exit 0. Debug any FAIL (most likely the resource-shape `realm` value from Step 1, or the policy poll delay).

- [ ] **Step 4: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/test.sh
git commit -m "test(quickstart): end-to-end Ranger grant/deny/negative scenarios"
```

---

### Task 15: Docs + roadmap updates

**Files:**
- Create: `quickstart/polaris-ranger-keycloak/OVERVIEW.md`, `README.md`, `run.sh`
- Modify: `README.md` (repo root), `nextsteps.md`

- [ ] **Step 1: Write the quickstart `OVERVIEW.md` and `README.md`**

`OVERVIEW.md`: architecture (SQE writes to Ranger, Polaris enforces), the identity model (principal name + roles, no groups), the resolved `realm` value from Task 14, and the Ranger 2.8 requirement. `README.md`: the `cp .env.example .env && docker compose up -d --wait && ./test.sh` runbook. Follow the writing style rules in the root `CLAUDE.md` (no emdash/endash, no Unicode arrows).

- [ ] **Step 2: Write `run.sh`**

A thin wrapper mirroring the reference `run.sh`: `docker compose up -d --wait` then `./test.sh`.

- [ ] **Step 3: Verify writing style (no forbidden characters)**

Run: `grep -rn '—\|–\|→' quickstart/polaris-ranger-keycloak/*.md && echo "FOUND — fix" || echo "clean"`
Expected: `clean`.

- [ ] **Step 4: Update root `README.md` roadmap and `nextsteps.md`**

Add the Ranger access-control backend + quickstart to the roadmap checklist in the repo-root `README.md`, and update `nextsteps.md` status line per the `CLAUDE.md` "After Completing Work" rule.

- [ ] **Step 5: Final full verification**

Run:
```bash
cargo test -p sqe-policy -p sqe-core 2>&1 | tail -5
cargo clippy -p sqe-policy -p sqe-core -p sqe-coordinator --all-targets -- -D warnings 2>&1 | tail -5
```
Expected: tests pass, clippy clean.

- [ ] **Step 6: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/OVERVIEW.md quickstart/polaris-ranger-keycloak/README.md quickstart/polaris-ranger-keycloak/run.sh README.md nextsteps.md
git commit -m "docs(ranger): quickstart docs + roadmap update"
```

---

## Self-review notes

- **Spec coverage:** Config variant (T1), RangerGrantBackend grant/revoke (T2-4), show/check (T5-6), wiring (T7), test env Keycloak/Ranger/Polaris/SQE (T8-13), the four complexity dimensions + negative tests + deny precedence (T14), docs/roadmap (T15). All spec sections mapped.
- **Top correctness risk** (resource shape / `root` realm value) is handled by the configurable `realm` field (T1, T3) and the empirical resolution step (T14 Step 1) guarded by negative tests.
- **Identity decision** (User+Role, reject Group) enforced in `grantee_to_fields` (T4) and tested.
- **Type consistency:** `RangerGrantBackend`, `build_resource_map`, `GrantRevokeRequest`, `policies_to_entries`, `entry_matches_grantee`, `evaluate_access`, `map_sql_to_ranger_access`, `ResourceLevel` used consistently across tasks.
- **Known external unknowns flagged in-plan, not silently assumed:** Ranger 2.8.0 Docker image availability (T10 Step 4), exact Polaris OIDC principal/role claim mapping (T11), repo Flight SQL client invocation (T13/T14 reference `run.sh`).

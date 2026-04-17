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

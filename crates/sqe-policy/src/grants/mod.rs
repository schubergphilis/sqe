//! Pluggable grant backend for GRANT/REVOKE/SHOW GRANTS SQL.
//!
//! Two implementations:
//! - `ChameleonGrantBackend` — wraps the existing Chameleon platform API client
//! - `PolarisGrantBackend` — calls the Polaris Management REST API

pub mod profile;
pub mod polaris;
pub mod ranger;

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

    /// Write an explicit DENY.
    ///
    /// Separate from `grant` because the backends express it differently: Ranger
    /// carries deny as `denyPolicyItems` on a policy, which its grant/revoke
    /// endpoint cannot write at all, so this goes through the policy API.
    ///
    /// Defaults to unsupported so a backend without a deny concept says so
    /// instead of silently doing nothing, which for a deny would be the
    /// dangerous direction: the caller would believe access was blocked.
    async fn deny(&self, _token: &str, _stmt: &GrantStatement) -> sqe_core::Result<()> {
        Err(sqe_core::SqeError::NotImplemented(format!(
            "DENY is not supported by the {} access-control backend",
            self.backend_name()
        )))
    }

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

    /// Does this backend authorize a GRANT/REVOKE against the CALLER's own
    /// authority on the named resource?
    ///
    /// `true` means the backend performs a per-resource check of its own on the
    /// principal in [`GrantStatement::grantor`] -- so the coordinator's coarse
    /// `admin_roles` gate can be stood down (see
    /// `sqe_core::config::GrantAuthority`) without leaving the mutation
    /// unauthorized.
    ///
    /// `false` means the backend acts with SQE's identity, or with an identity the
    /// caller does not control, and the mutation is therefore only as authorized as
    /// whatever check ran before it. Defaults to `false`: a new backend has to opt
    /// in deliberately, because getting this wrong turns an ungated GRANT into
    /// self-escalation.
    fn enforces_grantor_authority(&self) -> bool {
        false
    }
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
    /// The authenticated user issuing the statement.
    ///
    /// Ranger authorizes a grant against THIS name, not against the REST
    /// credentials SQE connects with (verified: a POST carrying
    /// `grantor: "dave"` is refused 403 "User doesn't have necessary permission
    /// to grant access" even when the call authenticates as admin). So passing
    /// the real caller is what makes grant authority resource-scoped instead of
    /// role-scoped, and it makes Ranger's audit record name the human.
    ///
    /// `None` falls back to the backend's configured admin user, which is the
    /// pre-existing behaviour and keeps test doubles and non-session callers
    /// working.
    pub grantor: Option<String>,
    /// `WITH GRANT OPTION`: the grantee may re-grant what it was given. Maps to
    /// Ranger's `delegateAdmin`. Without this, only principals seeded with
    /// delegate authority at bootstrap can ever grant.
    pub with_grant_option: bool,
    /// Whether the named object is a table or a view. Selects the access-type
    /// set; the resource shape is the same either way.
    pub object: GrantObjectKind,
}

/// A parsed REVOKE statement ready for backend dispatch.
#[derive(Debug, Clone)]
pub struct RevokeStatement {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub grantee: Grantee,
    /// See `GrantStatement::grantor`. Revoking is also authorized against the
    /// grantor, so a caller can only revoke what it has authority over.
    pub grantor: Option<String>,
    /// See `GrantStatement::object`.
    pub object: GrantObjectKind,
}

/// What kind of object a GRANT names.
///
/// Polaris models views with their own access types (`view-properties-read`,
/// `view-list`, `view-create`, `view-drop`, ...) but with NO separate resource
/// level: a view is addressed by putting its name in the `table` resource slot.
/// Verified against a live Polaris 1.6 -- granting `view-properties-read` on
/// `{catalog, namespace, table: <view name>}` lets the grantee load the view.
///
/// So the resource shape is identical to a table's and only the access-type set
/// differs, which is why this is a separate axis from `ResourceLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GrantObjectKind {
    #[default]
    Table,
    View,
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

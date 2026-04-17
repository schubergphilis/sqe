//! ChameleonGrantBackend — wraps the existing AccessControlClient.
//!
//! Thin adapter that translates the trait's backend-neutral types
//! (`GrantStatement`, `Grantee`) into the Chameleon platform API's
//! types (`GrantRequest`). Zero behavior change for existing deployments.

use std::sync::Arc;

use async_trait::async_trait;

use crate::access_control::{
    AccessControlClient, CheckAccessRequest, GrantEntry as CatalogGrantEntry, GrantRequest,
    ShowGrantsParams,
};

use sqe_policy::grants::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement, Grantee,
    RevokeStatement,
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
    /// Wrap a shared `AccessControlClient`.
    ///
    /// The caller keeps ownership via `Arc` so the same client can be shared
    /// with other subsystems (coordinator session, info_schema providers) that
    /// already hold an `Arc<AccessControlClient>`.
    pub fn new(client: Arc<AccessControlClient>) -> Self {
        Self { client }
    }
}

/// Map a `Grantee` to Chameleon's grantee_type string.
fn chameleon_grantee_type(grantee: &Grantee) -> &'static str {
    match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) | Grantee::Group(_) => "GROUP",
    }
}

/// Build a `GrantRequest` from the common fields shared by `GrantStatement`
/// and `RevokeStatement`. Keeping this in one place prevents the two call
/// sites from silently diverging (e.g. swapping catalog and namespace).
fn build_grant_request(
    privilege: &str,
    catalog: &Option<String>,
    namespace: &Option<String>,
    table: &Option<String>,
    grantee: &Grantee,
) -> GrantRequest {
    GrantRequest {
        privilege: privilege.to_string(),
        catalog: catalog.clone(),
        namespace: namespace.clone(),
        table: table.clone(),
        grantee_type: chameleon_grantee_type(grantee).to_string(),
        grantee_name: grantee.name().to_string(),
        effect: None,
    }
}

/// Build a `GrantRequest` from a `GrantStatement`.
fn to_grant_request(stmt: &GrantStatement) -> GrantRequest {
    build_grant_request(
        &stmt.privilege,
        &stmt.catalog,
        &stmt.namespace,
        &stmt.table,
        &stmt.grantee,
    )
}

/// Build a `GrantRequest` from a `RevokeStatement`.
///
/// The Chameleon API accepts the same payload shape for both grant and revoke,
/// so they share the builder above.
fn to_revoke_request(stmt: &RevokeStatement) -> GrantRequest {
    build_grant_request(
        &stmt.privilege,
        &stmt.catalog,
        &stmt.namespace,
        &stmt.table,
        &stmt.grantee,
    )
}

/// Translate a `GrantFilter` into `ShowGrantsParams` for the Chameleon API.
fn filter_to_params(filter: &GrantFilter) -> ShowGrantsParams {
    match filter {
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
    }
}

/// Translate an `AccessCheck` into a `CheckAccessRequest`.
fn check_to_request(check: &AccessCheck) -> CheckAccessRequest {
    CheckAccessRequest {
        user: check.user.clone(),
        privilege: check.privilege.clone(),
        catalog: check.catalog.clone(),
        namespace: check.namespace.clone(),
        table: check.table.clone(),
    }
}

/// Convert a `CatalogGrantEntry` (from access_control) to our trait's `GrantEntry`.
///
/// Takes the entry by value so callers iterating a response `Vec` can move
/// fields out instead of cloning every string.
fn from_catalog_entry(e: CatalogGrantEntry) -> GrantEntry {
    GrantEntry {
        privilege: e.privilege,
        resource: e.resource,
        grantee_type: e.grantee_type,
        grantee_name: e.grantee_name,
        effect: e.effect,
        granted_by: e.granted_by,
        granted_at: e.granted_at,
    }
}

#[async_trait]
impl GrantBackend for ChameleonGrantBackend {
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let req = to_grant_request(stmt);
        self.client.grant(token, &req).await
    }

    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let req = to_revoke_request(stmt);
        self.client.revoke(token, &req).await
    }

    async fn show_grants(
        &self,
        token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let params = filter_to_params(filter);
        let entries = self.client.show_grants(token, &params).await?;
        Ok(entries.into_iter().map(from_catalog_entry).collect())
    }

    async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let entries = self.client.show_effective(token, user).await?;
        Ok(entries.into_iter().map(from_catalog_entry).collect())
    }

    async fn check_access(
        &self,
        token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        let req = check_to_request(check);
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

    // ── chameleon_grantee_type ───────────────────────────────────────

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

    // ── to_grant_request / to_revoke_request ─────────────────────────

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
    fn to_revoke_request_translates_fields() {
        let stmt = RevokeStatement {
            privilege: "INSERT".into(),
            catalog: Some("cat".into()),
            namespace: Some("ns".into()),
            table: Some("tbl".into()),
            grantee: Grantee::User("alice".into()),
        };
        let req = to_revoke_request(&stmt);
        assert_eq!(req.privilege, "INSERT");
        assert_eq!(req.catalog.as_deref(), Some("cat"));
        assert_eq!(req.namespace.as_deref(), Some("ns"));
        assert_eq!(req.table.as_deref(), Some("tbl"));
        assert_eq!(req.grantee_type, "USER");
        assert_eq!(req.grantee_name, "alice");
        assert!(req.effect.is_none());
    }

    // ── filter_to_params ─────────────────────────────────────────────

    #[test]
    fn filter_on_resource_populates_resource_fields_only() {
        let filter = GrantFilter::OnResource {
            catalog: Some("cat".into()),
            namespace: Some("ns".into()),
            table: Some("tbl".into()),
        };
        let params = filter_to_params(&filter);
        assert_eq!(params.catalog.as_deref(), Some("cat"));
        assert_eq!(params.namespace.as_deref(), Some("ns"));
        assert_eq!(params.table.as_deref(), Some("tbl"));
        assert!(params.grantee_type.is_none());
        assert!(params.grantee_name.is_none());
    }

    #[test]
    fn filter_to_grantee_role_populates_grantee_fields_only() {
        let filter = GrantFilter::ToGrantee(Grantee::Role("analysts".into()));
        let params = filter_to_params(&filter);
        assert!(params.catalog.is_none());
        assert!(params.namespace.is_none());
        assert!(params.table.is_none());
        assert_eq!(params.grantee_type.as_deref(), Some("GROUP"));
        assert_eq!(params.grantee_name.as_deref(), Some("analysts"));
    }

    #[test]
    fn filter_to_grantee_user_maps_grantee_type_to_user() {
        let filter = GrantFilter::ToGrantee(Grantee::User("alice".into()));
        let params = filter_to_params(&filter);
        assert_eq!(params.grantee_type.as_deref(), Some("USER"));
        assert_eq!(params.grantee_name.as_deref(), Some("alice"));
    }

    // ── check_to_request ─────────────────────────────────────────────

    #[test]
    fn check_to_request_translates_fields() {
        let check = AccessCheck {
            user: "alice".into(),
            privilege: "SELECT".into(),
            catalog: Some("cat".into()),
            namespace: Some("ns".into()),
            table: Some("tbl".into()),
        };
        let req = check_to_request(&check);
        assert_eq!(req.user, "alice");
        assert_eq!(req.privilege, "SELECT");
        assert_eq!(req.catalog.as_deref(), Some("cat"));
        assert_eq!(req.namespace.as_deref(), Some("ns"));
        assert_eq!(req.table.as_deref(), Some("tbl"));
    }

    // ── from_catalog_entry ───────────────────────────────────────────

    #[test]
    fn from_catalog_entry_copies_all_fields() {
        let catalog_entry = CatalogGrantEntry {
            privilege: "SELECT".into(),
            resource: "cat.ns.tbl".into(),
            grantee_type: "GROUP".into(),
            grantee_name: "analysts".into(),
            effect: "ALLOW".into(),
            granted_by: Some("admin".into()),
            granted_at: Some("2026-04-17T10:00:00Z".into()),
        };
        let entry = from_catalog_entry(catalog_entry);
        assert_eq!(entry.privilege, "SELECT");
        assert_eq!(entry.resource, "cat.ns.tbl");
        assert_eq!(entry.grantee_type, "GROUP");
        assert_eq!(entry.grantee_name, "analysts");
        assert_eq!(entry.effect, "ALLOW");
        assert_eq!(entry.granted_by.as_deref(), Some("admin"));
        assert_eq!(entry.granted_at.as_deref(), Some("2026-04-17T10:00:00Z"));
    }
}

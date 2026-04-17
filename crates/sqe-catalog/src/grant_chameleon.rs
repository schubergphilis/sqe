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

/// Convert a `CatalogGrantEntry` (from access_control) to our trait's `GrantEntry`.
fn from_catalog_entry(e: &CatalogGrantEntry) -> GrantEntry {
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
        let catalog_entry = CatalogGrantEntry {
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

//! Pluggable grant backend for GRANT/REVOKE/SHOW GRANTS SQL.
//!
//! Two implementations:
//! - `ChameleonGrantBackend` — wraps the existing Chameleon platform API client
//! - `PolarisGrantBackend` — calls the Polaris Management REST API

pub mod polaris;
pub mod profile;
pub mod ranger;

/// Read an HTTP error body, or a placeholder when the body itself is unreadable.
pub(crate) async fn response_body_or_unreadable(resp: reqwest::Response) -> String {
    match resp.text().await {
        Ok(b) => b,
        Err(e) => format!("<body unreadable: {e}>"),
    }
}

// The trait and the backend-neutral statement types moved to
// `sqe_policy_api::grants` (build-time item C1); re-exported so
// `sqe_policy::grants::GrantBackend` and friends keep resolving.
pub use sqe_policy_api::grants::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantObjectKind,
    GrantStatement, Grantee, RevokeStatement,
};
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

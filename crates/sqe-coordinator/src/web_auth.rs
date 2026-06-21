//! Bearer + admin guard for the operator dashboard (`web_ui`) routes.
//!
//! This module provides:
//! - `GuardReject` -- the two rejection reasons for the guard.
//! - `bearer_admin_identity` -- pure async function that validates an
//!   `Authorization: Bearer <token>` header, calls the auth provider, and
//!   checks that the resulting identity holds an admin role. Fully testable
//!   without an HTTP stack.
//! - `BearerAdminState` -- a small trait that the axum middleware is generic
//!   over. `HealthState` in `bin/sqe_server.rs` implements this trait, keeping
//!   `HealthState` and all its construction sites in the binary.
//! - `require_admin_bearer` -- the axum `from_fn_with_state` middleware that
//!   gates the `web_ui` route group.
//!
//! `/healthz`, `/readyz`, and `/api/v1/status` are NOT gated: they are
//! attached to the router before the `route_layer` is applied.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use sqe_auth::{AuthProvider, FlightCredentials, Identity};
use sqe_core::SecretString;
use sqe_metrics::audit::{Actor, AuditEvent, AuditKind, AuditLogger, Integrity, Outcome, QueryInfo};

/// Reason a bearer-admin guard check failed.
#[derive(Debug, PartialEq, Eq)]
pub enum GuardReject {
    /// No `Authorization` header, or the scheme is not `Bearer`.
    Unauthorized,
    /// Token is valid but the identity does not hold an admin role.
    Forbidden,
}

/// Validate an `Authorization: Bearer <token>` header against the bearer
/// provider and require an admin role. Reuses `config.auth.has_admin_role`.
///
/// Returns the authenticated `Identity` on success, or a `GuardReject` on
/// failure. The function is pure (no HTTP types) so it can be unit-tested
/// without an axum router.
pub async fn bearer_admin_identity(
    provider: &Arc<dyn AuthProvider>,
    auth_cfg: &sqe_core::config::AuthConfig,
    header: Option<&str>,
) -> Result<Identity, GuardReject> {
    let token = match header.and_then(|h| h.strip_prefix("Bearer ")) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => return Err(GuardReject::Unauthorized),
    };
    let creds = FlightCredentials {
        bearer_token: Some(SecretString::new(token)),
        ..Default::default()
    };
    let identity = provider
        .authenticate(&creds)
        .await
        .map_err(|_| GuardReject::Unauthorized)?;
    if auth_cfg.has_admin_role(&identity.roles) {
        Ok(identity)
    } else {
        Err(GuardReject::Forbidden)
    }
}

/// Trait implemented by any state struct that carries the bearer provider and
/// auth config needed by the admin guard middleware.
///
/// Using a trait keeps `HealthState` and all its construction sites in
/// `bin/sqe_server.rs` (a binary crate) while `require_admin_bearer` lives
/// here in the lib. Option (a) -- moving `HealthState` into the lib -- would
/// force every field public and add cross-crate imports for all handlers.
pub trait BearerAdminState: Send + Sync + 'static {
    fn bearer_provider(&self) -> Option<&Arc<dyn AuthProvider>>;
    fn auth_config(&self) -> Option<&sqe_core::config::AuthConfig>;
    /// Returns the audit logger for emitting dashboard-access events, if wired.
    fn audit(&self) -> Option<&Arc<AuditLogger>>;
}

/// Build an `AuditEvent` for a dashboard access attempt.
///
/// Called from `require_admin_bearer` after the guard result is known.
/// The bearer token is NEVER included: only the resolved identity fields
/// (on success) or a placeholder actor (on denial) are recorded.
///
/// `client_ip` is `None` for now because the health router is served
/// without `into_make_service_with_connect_info`; wired in Phase C2.
pub fn dashboard_audit_event(
    result: &Result<Identity, GuardReject>,
    client_ip: Option<String>,
) -> AuditEvent {
    let (actor, outcome) = match result {
        Ok(identity) => {
            let actor = Actor::from_parts(
                identity.user_id.clone(),
                identity.subject.clone(),
                identity.email.clone(),
                identity.roles.clone(),
                identity.groups.clone(),
            );
            (actor, Outcome::Success)
        }
        Err(GuardReject::Unauthorized) => {
            let actor = Actor::from_parts(
                "unknown".into(),
                None,
                None,
                vec![],
                vec![],
            );
            let outcome = Outcome::Failure {
                error_type: Some("DashboardAccessDenied".into()),
                error_code: None,
                message: Some("missing or invalid bearer token".into()),
            };
            (actor, outcome)
        }
        Err(GuardReject::Forbidden) => {
            let actor = Actor::from_parts(
                "unknown".into(),
                None,
                None,
                vec![],
                vec![],
            );
            let outcome = Outcome::Failure {
                error_type: Some("DashboardAccessDenied".into()),
                error_code: None,
                message: Some("admin role required".into()),
            };
            (actor, outcome)
        }
    };

    AuditEvent {
        time: chrono::Utc::now(),
        kind: AuditKind::Auth,
        actor,
        outcome,
        resources: vec![],
        policy: None,
        timing: None,
        stats: None,
        query: Some(QueryInfo {
            text: Some("dashboard_access".into()),
            query_hash: String::new(),
            statement_type: "dashboard_access".into(),
        }),
        session_id: None,
        client_ip,
        integrity: Integrity::default(),
    }
}

/// Axum middleware that gates a route group behind bearer + admin auth.
///
/// Attach via `route_layer(axum::middleware::from_fn_with_state(...))` to
/// the `web_ui` sub-router only. The health routes (`/healthz`, `/readyz`,
/// `/api/v1/status`) must NOT go through this layer.
pub async fn require_admin_bearer<S>(
    State(state): State<Arc<S>>,
    request: Request,
    next: Next,
) -> Response
where
    S: BearerAdminState,
{
    let provider = match state.bearer_provider() {
        Some(p) => p,
        None => {
            return (StatusCode::UNAUTHORIZED, "auth not configured").into_response();
        }
    };
    let auth_cfg = match state.auth_config() {
        Some(c) => c,
        None => {
            return (StatusCode::UNAUTHORIZED, "auth not configured").into_response();
        }
    };
    let header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let result = bearer_admin_identity(provider, auth_cfg, header).await;

    // Emit an audit event when a logger is present. client_ip is None until
    // Phase C2 wires connect_info into the health router.
    if let Some(audit) = state.audit() {
        audit.log_event(dashboard_audit_event(&result, None));
    }

    match result {
        Ok(identity) => {
            let _ = identity;
            next.run(request).await
        }
        Err(GuardReject::Unauthorized) => {
            (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
        }
        Err(GuardReject::Forbidden) => {
            (StatusCode::FORBIDDEN, "admin role required").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct StubProvider {
        roles: Vec<String>,
        ok: bool,
    }

    #[async_trait::async_trait]
    impl sqe_auth::AuthProvider for StubProvider {
        async fn authenticate(
            &self,
            creds: &sqe_auth::FlightCredentials,
        ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
            assert!(
                creds.bearer_token.is_some(),
                "guard must pass the bearer token"
            );
            if self.ok {
                Ok(sqe_auth::Identity {
                    user_id: "u".into(),
                    display_name: "u".into(),
                    roles: self.roles.clone(),
                    subject: None,
                    email: None,
                    groups: vec![],
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                })
            } else {
                Err(sqe_auth::AuthError::AuthFailed("bad".into()))
            }
        }
    }

    fn auth_cfg(admin_roles: &[&str]) -> sqe_core::config::AuthConfig {
        // AuthConfig does not derive Default; parse via TOML with an empty section
        // so all serde(default) fields take their defaults, then override admin_roles.
        let mut c: sqe_core::config::AuthConfig = toml::from_str("").expect("empty auth config");
        c.admin_roles = admin_roles.iter().map(|s| s.to_string()).collect();
        c
    }

    #[tokio::test]
    async fn missing_header_is_unauthorized() {
        let p: Arc<dyn sqe_auth::AuthProvider> =
            Arc::new(StubProvider { roles: vec![], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), None).await;
        assert!(matches!(r, Err(GuardReject::Unauthorized)));
    }

    #[tokio::test]
    async fn non_bearer_scheme_is_unauthorized() {
        let p: Arc<dyn sqe_auth::AuthProvider> =
            Arc::new(StubProvider { roles: vec![], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Basic abc")).await;
        assert!(matches!(r, Err(GuardReject::Unauthorized)));
    }

    #[tokio::test]
    async fn valid_bearer_non_admin_is_forbidden() {
        let p: Arc<dyn sqe_auth::AuthProvider> =
            Arc::new(StubProvider { roles: vec!["analyst".into()], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Bearer tok")).await;
        assert!(matches!(r, Err(GuardReject::Forbidden)));
    }

    #[tokio::test]
    async fn valid_bearer_admin_is_ok() {
        let p: Arc<dyn sqe_auth::AuthProvider> =
            Arc::new(StubProvider { roles: vec!["admin".into()], ok: true });
        let id = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Bearer tok"))
            .await
            .unwrap();
        assert_eq!(id.roles, vec!["admin".to_string()]);
    }

    // ── Audit emit tests ──────────────────────────────────────────────────────

    /// Helper: write one event via `dashboard_audit_event`, flush, read back JSONL.
    fn write_and_read(
        result: Result<sqe_auth::Identity, GuardReject>,
    ) -> (sqe_metrics::audit::AuditLogger, std::path::PathBuf, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let logger =
            sqe_metrics::audit::AuditLogger::new(path.to_str().unwrap()).expect("audit logger");
        let event = dashboard_audit_event(&result, None);
        logger.log_event(event);
        logger.flush();
        (logger, path, dir)
    }

    fn admin_identity() -> sqe_auth::Identity {
        sqe_auth::Identity {
            user_id: "alice".into(),
            display_name: "alice".into(),
            roles: vec!["admin".into()],
            subject: Some("sub-alice".into()),
            email: Some("alice@corp.example".into()),
            groups: vec!["ops".into()],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        }
    }

    #[test]
    fn audit_success_emits_auth_event_with_username() {
        let result: Result<sqe_auth::Identity, GuardReject> = Ok(admin_identity());
        let (_logger, path, _dir) = write_and_read(result);
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "expected exactly one audit line; got: {content}");
        let v: serde_json::Value =
            serde_json::from_str(lines[0]).expect("line must be valid JSON");
        // kind must be "auth" (OCSF Authentication)
        assert_eq!(
            v["kind"].as_str(),
            Some("auth"),
            "kind must be 'auth'; got: {v}"
        );
        // status must be "success" (from Outcome::Success serialization)
        assert_eq!(
            v["status"].as_str(),
            Some("success"),
            "status must be 'success'; got: {v}"
        );
        // actor.username must be the admin's user_id
        assert_eq!(
            v["actor"]["username"].as_str(),
            Some("alice"),
            "actor.username must be 'alice'; got: {v}"
        );
        // No bearer token in the event (the token is never put in the event struct)
        assert!(
            !content.contains("Bearer"),
            "bearer token must not appear in audit line: {content}"
        );
        assert!(
            !content.contains("tok"),
            "bearer token value must not appear in audit line: {content}"
        );
    }

    #[test]
    fn audit_unauthorized_emits_failure_with_reason() {
        let result: Result<sqe_auth::Identity, GuardReject> = Err(GuardReject::Unauthorized);
        let (_logger, path, _dir) = write_and_read(result);
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "expected exactly one audit line; got: {content}");
        let v: serde_json::Value =
            serde_json::from_str(lines[0]).expect("line must be valid JSON");
        assert_eq!(v["kind"].as_str(), Some("auth"), "kind must be 'auth'; got: {v}");
        assert_eq!(v["status"].as_str(), Some("failure"), "status must be 'failure'; got: {v}");
        assert_eq!(
            v["error_type"].as_str(),
            Some("DashboardAccessDenied"),
            "error_type must be 'DashboardAccessDenied'; got: {v}"
        );
        // Denial reason must be present and non-sensitive
        let msg = v["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("bearer"),
            "message must mention bearer; got: {msg}"
        );
        // No bearer token in the event
        assert!(
            !content.contains("Bearer "),
            "bearer scheme must not appear in audit line: {content}"
        );
    }

    #[test]
    fn audit_forbidden_emits_failure_admin_role_reason() {
        let result: Result<sqe_auth::Identity, GuardReject> = Err(GuardReject::Forbidden);
        let (_logger, path, _dir) = write_and_read(result);
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "expected exactly one audit line; got: {content}");
        let v: serde_json::Value =
            serde_json::from_str(lines[0]).expect("line must be valid JSON");
        assert_eq!(v["kind"].as_str(), Some("auth"), "kind must be 'auth'; got: {v}");
        assert_eq!(v["status"].as_str(), Some("failure"), "status must be 'failure'; got: {v}");
        let msg = v["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("admin"),
            "message must mention admin role; got: {msg}"
        );
    }
}

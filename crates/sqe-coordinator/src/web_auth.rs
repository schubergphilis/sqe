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
    match bearer_admin_identity(provider, auth_cfg, header).await {
        Ok(_identity) => next.run(request).await,
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
}

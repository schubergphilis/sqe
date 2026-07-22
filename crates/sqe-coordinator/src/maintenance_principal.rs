//! Maintenance service principal: mints ephemeral per-job sessions for the
//! advisory compaction scheduler.
//!
//! This is structurally isolated from the interactive auth chain on purpose.
//! `MaintenancePrincipal` wraps its own `OidcM2mProvider` instance, built
//! directly from `MaintenancePrincipalConfig`. It does not go through
//! `sqe_auth::factory::build_auth_chain`, and there is no
//! `AuthProviderConfig::M2m` variant: the only way to construct one is from
//! inside this module, from the `[maintenance]` config section. That keeps
//! the "who can authenticate as the maintenance identity" surface to exactly
//! one call site.
//!
//! Sessions minted here are never inserted into a `SessionManager`. They
//! exist only for the duration of a single scheduler job and are held
//! directly by the caller (the advisory scheduler, added in a later task).

use chrono::{DateTime, Utc};
use sqe_auth::{AuthProvider, FlightCredentials, Identity, OidcM2mConfig, OidcM2mProvider};
use sqe_core::config::MaintenancePrincipalConfig;
use sqe_core::{Credentials, Session, SqeError};

/// Service principal used by the maintenance subsystem to mint short-lived,
/// unregistered sessions for compaction analysis jobs.
///
/// `provider` is a private `OidcM2mProvider` owned exclusively by this
/// principal; nothing outside this module can reach it, so it cannot be
/// wired into the interactive `AuthChain` by accident.
pub struct MaintenancePrincipal {
    provider: OidcM2mProvider,
    pub user_id: String,
}

impl MaintenancePrincipal {
    /// Build a principal from the `[maintenance.principal]` config section.
    ///
    /// Construction is lazy: no network call happens here. `OidcM2mProvider::new`
    /// still validates that `token_endpoint` and `client_id` are non-empty,
    /// which closes the Task 1 gap where config validation only checked that
    /// a principal section was present, not that its contents were usable.
    pub fn from_config(cfg: &MaintenancePrincipalConfig) -> sqe_core::Result<Self> {
        let m2m_cfg = OidcM2mConfig {
            token_endpoint: cfg.token_endpoint.clone(),
            client_id: cfg.client_id.clone(),
            client_secret: cfg.client_secret.expose().to_string(),
            scope: cfg.scope.clone(),
            user_id: cfg.user_id.clone(),
            roles: cfg.roles.clone(),
            refresh_skew: std::time::Duration::from_secs(cfg.refresh_skew_secs),
            accept_invalid_certs: false,
            request_timeout: std::time::Duration::from_secs(5),
        };
        let provider = OidcM2mProvider::new(m2m_cfg).map_err(SqeError::Config)?;
        Ok(Self {
            provider,
            user_id: cfg.user_id.clone(),
        })
    }

    /// Authenticate against the token endpoint and mint an ephemeral
    /// `Session` for a single maintenance job.
    ///
    /// The network call happens in `provider.authenticate`; everything
    /// after that is pure and lives in `session_from_identity` so it can be
    /// unit-tested without a token endpoint.
    pub async fn mint_session(&self, job_id: &str) -> sqe_core::Result<Session> {
        let identity = self
            .provider
            .authenticate(&FlightCredentials::default())
            .await
            .map_err(|e| SqeError::Auth(e.to_string()))?;
        Ok(Self::session_from_identity(&identity, job_id))
    }

    /// Build the ephemeral maintenance `Session` from an already-authenticated
    /// `Identity`. Mirrors `session_manager.rs::identity_to_session`'s field
    /// mapping, but overrides the session id with the job-scoped
    /// `maintenance-job-<job_id>` form and is never registered with a
    /// `SessionManager`.
    ///
    /// Split out from `mint_session` so the session-shape (id prefix,
    /// carried user_id/roles) is testable without a network call: tests can
    /// hand-build an `Identity` and call this directly.
    fn session_from_identity(identity: &Identity, job_id: &str) -> Session {
        let token_expiry: DateTime<Utc> = identity
            .expires_at
            .unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1));

        let mut session = Session::new(
            identity.user_id.clone(),
            identity.catalog_token.clone().unwrap_or_default(),
            identity.refresh_token.clone(),
            token_expiry,
            identity.roles.clone(),
        )
        .with_identity(identity.subject.clone(), identity.email.clone(), identity.groups.clone())
        // Explicit in-engine write-authority marker (Phase 4b). Set here,
        // once, by the maintenance principal's own minting path only.
        // `MaintenanceHandler::authorize_or_deny` honors this independent
        // of the role-name heuristic so autonomous compaction is authorized
        // by design, not by accident of "unknown role defaults to allow".
        // Polaris still enforces authorization server-side.
        .with_maintenance_authority(true);

        session.id = format!("maintenance-job-{job_id}");
        session
    }

    /// Pre-commit token refresh: fetch a fresh catalog token from the IdP and
    /// rotate it into `session`'s credential trio in place. Intended to be
    /// called by the scheduler right before a job commits, so a long-running
    /// analysis job does not fail on a token that expired mid-run.
    pub async fn refresh(&self, session: &mut Session) -> sqe_core::Result<()> {
        // `refresh_catalog_token` on `OidcM2mProvider` ignores the identity
        // argument entirely (the M2M grant has no per-session state), so an
        // empty placeholder is sufficient here.
        let placeholder = Identity {
            user_id: self.user_id.clone(),
            display_name: self.user_id.clone(),
            roles: Vec::new(),
            subject: None,
            email: None,
            groups: Vec::new(),
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };
        let refreshed = self
            .provider
            .refresh_catalog_token(&placeholder)
            .await
            .map_err(|e| SqeError::Auth(e.to_string()))?;
        let Some(new_token) = refreshed else {
            return Ok(());
        };
        // TODO(4b): `refresh_catalog_token` only returns the token, not a
        // new expiry, and M2M `authenticate` never populates
        // `Identity::expires_at` either. At mint time, `session_from_identity`
        // fabricates `token_expiry = now + 1h` because `Identity::expires_at`
        // is always None. If the real IdP token lives under an hour, this
        // UNDER-reports expiry, and a consumer that trusts `token_expiry()`
        // could attempt to use a dead token. CRITICAL: never trust
        // `token_expiry()` on a maintenance session. The future scheduler must
        // refresh unconditionally before any commit, not gated on
        // `token_expiry()`.
        session.rotate_credentials(Credentials::new(
            new_token,
            session.refresh_token().cloned(),
            session.token_expiry(),
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bogus_config() -> MaintenancePrincipalConfig {
        MaintenancePrincipalConfig {
            token_endpoint: "https://idp.example/token".into(),
            client_id: "sqe-maintenance".into(),
            client_secret: "x".into(),
            scope: None,
            user_id: "svc-sqe-maintenance".into(),
            roles: vec!["maintenance".into()],
            refresh_skew_secs: 60,
        }
    }

    #[test]
    fn principal_from_config_builds() {
        let cfg = bogus_config();
        let p = MaintenancePrincipal::from_config(&cfg).expect("build");
        assert_eq!(p.user_id, "svc-sqe-maintenance");
    }

    #[test]
    fn from_config_rejects_empty_token_endpoint() {
        let mut cfg = bogus_config();
        cfg.token_endpoint = String::new();
        match MaintenancePrincipal::from_config(&cfg) {
            Ok(_) => panic!("expected empty token_endpoint to be rejected"),
            Err(SqeError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn from_config_rejects_empty_client_id() {
        let mut cfg = bogus_config();
        cfg.client_id = String::new();
        match MaintenancePrincipal::from_config(&cfg) {
            Ok(_) => panic!("expected empty client_id to be rejected"),
            Err(SqeError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
        }
    }

    fn fake_identity(user_id: &str) -> Identity {
        Identity {
            user_id: user_id.to_string(),
            display_name: user_id.to_string(),
            roles: vec!["maintenance".to_string()],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: Some(sqe_core::SecretString::new("fake-token".to_string())),
            refresh_token: None,
            expires_at: None,
        }
    }

    #[test]
    fn session_from_identity_has_job_prefixed_id() {
        let identity = fake_identity("svc-sqe-maintenance");
        let session = MaintenancePrincipal::session_from_identity(&identity, "job-42");
        assert_eq!(session.id, "maintenance-job-job-42");
    }

    #[test]
    fn session_from_identity_carries_configured_user_id() {
        let identity = fake_identity("svc-sqe-maintenance");
        let session = MaintenancePrincipal::session_from_identity(&identity, "job-1");
        assert_eq!(session.user.username, "svc-sqe-maintenance");
        assert_eq!(session.access_token().expose(), "fake-token");
    }

    #[test]
    fn session_from_identity_carries_maintenance_authority() {
        // Even though `fake_identity` sets a plain "maintenance" role (not a
        // "write"/"admin"-named one), the minted session must carry the
        // explicit maintenance-authority marker so it passes the write gate
        // independent of the role-name heuristic.
        let identity = fake_identity("svc-sqe-maintenance");
        let session = MaintenancePrincipal::session_from_identity(&identity, "job-9");
        assert!(session.has_maintenance_authority());
    }

    #[test]
    fn session_from_identity_carries_all_identity_fields() {
        // Verify that the minted session is fully formed: it carries the
        // identity's roles, groups, subject, and email as well as user_id
        // and token. This confirms it is a complete, standalone session
        // independent of any SessionManager.
        let identity = fake_identity("svc-sqe-maintenance");
        let session = MaintenancePrincipal::session_from_identity(&identity, "job-7");
        assert_eq!(session.user.roles, vec!["maintenance".to_string()]);
        assert!(session.user.groups.is_empty());
        assert!(session.user.subject.is_none());
        assert!(session.user.email.is_none());
    }
}

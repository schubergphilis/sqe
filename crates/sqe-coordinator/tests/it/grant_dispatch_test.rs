//! Admin-gate dispatch tests for GRANT / REVOKE (issue #204).
//!
//! GRANT and REVOKE mutate access control. In production the Polaris grant
//! backend swaps the caller's bearer for a service token scoped
//! PRINCIPAL_ROLE:ALL, so an ungated GRANT let any authenticated user
//! self-escalate. These tests prove:
//!   - a non-admin caller is rejected BEFORE the backend is ever called
//!     (the service-token path is unreachable), and
//!   - an admin caller reaches the backend.
//!
//! A recording stub `GrantBackend` flags whether `grant()` / `revoke()`
//! actually ran, so the non-admin assertion covers "rejected before
//! dispatch", not merely "returned an error".

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::grants::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    RevokeStatement,
};
use sqe_policy::PassthroughEnforcer;

// ---------------------------------------------------------------------------
// Recording stub backend
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingBackend {
    granted: AtomicBool,
    revoked: AtomicBool,
}

#[async_trait]
impl GrantBackend for RecordingBackend {
    async fn grant(&self, _token: &str, _stmt: &GrantStatement) -> sqe_core::Result<()> {
        self.granted.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn revoke(&self, _token: &str, _stmt: &RevokeStatement) -> sqe_core::Result<()> {
        self.revoked.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn show_grants(
        &self,
        _token: &str,
        _filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        Ok(vec![])
    }

    async fn show_effective(&self, _token: &str, _user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        Ok(vec![])
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        Ok(AccessCheckResult {
            allowed: true,
            reason: None,
        })
    }

    fn backend_name(&self) -> &str {
        "recording"
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MINIMAL_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59999"
"#;

fn minimal_config() -> SqeConfig {
    toml::from_str(MINIMAL_TOML).expect("minimal config parses")
}

fn session_with_roles(roles: Vec<&str>) -> Session {
    Session::new(
        "tester".to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles.into_iter().map(String::from).collect(),
    )
}

fn make_handler(backend: Arc<RecordingBackend>) -> QueryHandler {
    let config = minimal_config();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        Some(backend),
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grant_rejected_without_admin_role_and_backend_not_called() {
    let backend = Arc::new(RecordingBackend::default());
    let handler = make_handler(backend.clone());
    let session = session_with_roles(vec!["analyst"]);

    let err = handler
        .execute(
            &session,
            "GRANT SELECT ON sales.orders TO ROLE analyst",
            None,
        )
        .await
        .expect_err("non-admin must not be able to GRANT");

    let msg = err.to_string();
    assert!(msg.contains("403"), "expected 403, got: {msg}");
    assert!(msg.contains("admin"), "expected admin role mention: {msg}");

    // The critical assertion: the backend (and thus the service-token
    // PRINCIPAL_ROLE:ALL path) was never reached.
    assert!(
        !backend.granted.load(Ordering::SeqCst),
        "backend.grant() must not run for a non-admin caller"
    );
}

#[tokio::test]
async fn revoke_rejected_without_admin_role_and_backend_not_called() {
    let backend = Arc::new(RecordingBackend::default());
    let handler = make_handler(backend.clone());
    let session = session_with_roles(vec![]);

    let err = handler
        .execute(
            &session,
            "REVOKE SELECT ON sales.orders FROM ROLE analyst",
            None,
        )
        .await
        .expect_err("non-admin must not be able to REVOKE");

    assert!(err.to_string().contains("403"), "expected 403: {err}");
    assert!(
        !backend.revoked.load(Ordering::SeqCst),
        "backend.revoke() must not run for a non-admin caller"
    );
}

#[tokio::test]
async fn grant_allowed_for_admin_reaches_backend() {
    let backend = Arc::new(RecordingBackend::default());
    let handler = make_handler(backend.clone());
    // Default admin_roles includes "service_admin".
    let session = session_with_roles(vec!["service_admin"]);

    handler
        .execute(
            &session,
            "GRANT SELECT ON sales.orders TO ROLE analyst",
            None,
        )
        .await
        .expect("admin GRANT should succeed");

    assert!(
        backend.granted.load(Ordering::SeqCst),
        "backend.grant() must run for an admin caller"
    );
}

#[tokio::test]
async fn revoke_allowed_for_admin_reaches_backend() {
    let backend = Arc::new(RecordingBackend::default());
    let handler = make_handler(backend.clone());
    let session = session_with_roles(vec!["catalog_admin"]);

    handler
        .execute(
            &session,
            "REVOKE SELECT ON sales.orders FROM ROLE analyst",
            None,
        )
        .await
        .expect("admin REVOKE should succeed");

    assert!(
        backend.revoked.load(Ordering::SeqCst),
        "backend.revoke() must run for an admin caller"
    );
}

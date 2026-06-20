//! Tests for the policy-cache flush on GRANT/REVOKE (issue #207) and the
//! self-or-admin gate on read-path grant introspection (issue #260).
//!
//! Both share `query_handler.rs` and a recording stub `GrantBackend`. A
//! recording `PolicyStore` flags whether `invalidate_all()` ran, so the #207
//! assertion covers "the cache was flushed after the mutation succeeded", not
//! merely "the statement returned Ok".

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SessionUser, SqeConfig};
use sqe_policy::grants::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    RevokeStatement,
};
use sqe_policy::{PassthroughEnforcer, PolicyStore, ResolvedPolicy};

// ---------------------------------------------------------------------------
// Recording stub backend (mirrors grant_dispatch_test.rs, plus read-path flags)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingBackend {
    granted: AtomicBool,
    revoked: AtomicBool,
    show_effective_called: AtomicBool,
    check_access_called: AtomicBool,
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

    async fn show_effective(
        &self,
        _token: &str,
        _user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        self.show_effective_called.store(true, Ordering::SeqCst);
        Ok(vec![])
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        self.check_access_called.store(true, Ordering::SeqCst);
        Ok(AccessCheckResult { allowed: true, reason: None })
    }

    fn backend_name(&self) -> &str {
        "recording"
    }
}

// ---------------------------------------------------------------------------
// Recording stub policy store: counts invalidate_all() calls
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingStore {
    invalidations: AtomicU32,
}

#[async_trait]
impl PolicyStore for RecordingStore {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    fn invalidate_all(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
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

fn session_for(username: &str, roles: Vec<&str>) -> Session {
    Session::new(
        username.to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles.into_iter().map(String::from).collect(),
    )
}

fn make_handler(backend: Arc<RecordingBackend>, store: Arc<RecordingStore>) -> QueryHandler {
    let config = minimal_config();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        Some(store),
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
// #207 - cache flush on GRANT/REVOKE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grant_flushes_policy_cache_after_mutation() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("admin", vec!["service_admin"]);

    handler
        .execute(&session, "GRANT SELECT ON sales.orders TO ROLE analyst")
        .await
        .expect("admin GRANT should succeed");

    assert!(
        backend.granted.load(Ordering::SeqCst),
        "backend.grant() must run for an admin caller"
    );
    assert_eq!(
        store.invalidations.load(Ordering::SeqCst),
        1,
        "GRANT must flush the policy cache exactly once after the mutation"
    );
}

#[tokio::test]
async fn revoke_flushes_policy_cache_after_mutation() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("admin", vec!["catalog_admin"]);

    handler
        .execute(&session, "REVOKE SELECT ON sales.orders FROM ROLE analyst")
        .await
        .expect("admin REVOKE should succeed");

    assert!(
        backend.revoked.load(Ordering::SeqCst),
        "backend.revoke() must run for an admin caller"
    );
    assert_eq!(
        store.invalidations.load(Ordering::SeqCst),
        1,
        "REVOKE must flush the policy cache exactly once after the mutation"
    );
}

#[tokio::test]
async fn rejected_grant_does_not_flush_policy_cache() {
    // A non-admin GRANT is rejected before the mutation, so the cache must
    // not be flushed (the stale decision is never the issue if nothing
    // changed).
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(&session, "GRANT SELECT ON sales.orders TO ROLE analyst")
        .await
        .expect_err("non-admin must not be able to GRANT");
    assert!(err.to_string().contains("403"), "expected 403: {err}");

    assert!(
        !backend.granted.load(Ordering::SeqCst),
        "backend.grant() must not run for a non-admin caller"
    );
    assert_eq!(
        store.invalidations.load(Ordering::SeqCst),
        0,
        "a rejected GRANT must not flush the policy cache"
    );
}

// ---------------------------------------------------------------------------
// #260 - self-or-admin gate on read-path introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_effective_grants_self_is_allowed() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    handler
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"alice\"")
        .await
        .expect("self-introspection must be allowed");

    assert!(
        backend.show_effective_called.load(Ordering::SeqCst),
        "self-introspection must reach the backend"
    );
}

#[tokio::test]
async fn show_effective_grants_other_principal_requires_admin() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"bob\"")
        .await
        .expect_err("non-admin must not introspect another principal");
    assert!(err.to_string().contains("403"), "expected 403: {err}");

    assert!(
        !backend.show_effective_called.load(Ordering::SeqCst),
        "cross-principal introspection must be rejected before the service-token backend runs"
    );
}

#[tokio::test]
async fn show_effective_grants_admin_can_introspect_anyone() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("root", vec!["service_admin"]);

    handler
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"bob\"")
        .await
        .expect("admin must be able to introspect any principal");

    assert!(
        backend.show_effective_called.load(Ordering::SeqCst),
        "admin introspection must reach the backend"
    );
}

#[tokio::test]
async fn check_access_self_is_allowed() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    handler
        .execute(&session, "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"alice\"")
        .await
        .expect("self access check must be allowed");

    assert!(
        backend.check_access_called.load(Ordering::SeqCst),
        "self access check must reach the backend"
    );
}

#[tokio::test]
async fn check_access_other_principal_requires_admin() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(&session, "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"")
        .await
        .expect_err("non-admin must not check another principal's access");
    assert!(err.to_string().contains("403"), "expected 403: {err}");

    assert!(
        !backend.check_access_called.load(Ordering::SeqCst),
        "cross-principal access check must be rejected before the service-token backend runs"
    );
}

#[tokio::test]
async fn show_effective_policy_other_principal_requires_admin() {
    // SHOW EFFECTIVE POLICY mirrors SHOW EFFECTIVE GRANTS: the self-or-admin
    // gate runs BEFORE any catalog load or policy resolution, so a non-admin
    // targeting another principal is rejected with a 403 without the resolver
    // ever running. The rejection does not depend on a live catalog.
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(
            &session,
            "SHOW EFFECTIVE POLICY FOR USER \"bob\" ON cat.ns.orders",
        )
        .await
        .expect_err("non-admin must not introspect another principal's policy");
    assert!(err.to_string().contains("403"), "expected 403: {err}");
}

#[tokio::test]
async fn check_access_admin_can_check_anyone() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("root", vec!["catalog_admin"]);

    handler
        .execute(&session, "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"")
        .await
        .expect("admin must be able to check any principal's access");

    assert!(
        backend.check_access_called.load(Ordering::SeqCst),
        "admin access check must reach the backend"
    );
}

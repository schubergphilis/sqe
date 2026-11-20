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
    show_grants_called: AtomicBool,
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
        self.show_grants_called.store(true, Ordering::SeqCst);
        Ok(vec![])
    }

    async fn show_effective(&self, _token: &str, _user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        self.show_effective_called.store(true, Ordering::SeqCst);
        Ok(vec![])
    }

    async fn check_access(
        &self,
        _token: &str,
        _check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        self.check_access_called.store(true, Ordering::SeqCst);
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
        .execute(
            &session,
            "GRANT SELECT ON sales.orders TO ROLE analyst",
            None,
        )
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
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"alice\"", None)
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
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"bob\"", None)
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
        .execute(&session, "SHOW EFFECTIVE GRANTS FOR USER \"bob\"", None)
        .await
        .expect("admin must be able to introspect any principal");

    assert!(
        backend.show_effective_called.load(Ordering::SeqCst),
        "admin introspection must reach the backend"
    );
}

// ---------------------------------------------------------------------------
// #721 - SHOW GRANTS gets the same self-or-admin gate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_grants_to_self_is_allowed() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    handler
        .execute(&session, "SHOW GRANTS TO USER \"alice\"", None)
        .await
        .expect("self-introspection must be allowed");
    assert!(backend.show_grants_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn show_grants_on_other_targets_requires_admin() {
    for sql in [
        "SHOW GRANTS TO USER \"bob\"",
        "SHOW GRANTS TO ROLE \"analyst\"",
        "SHOW GRANTS ON cat.ns.tbl",
    ] {
        let backend = Arc::new(RecordingBackend::default());
        let store = Arc::new(RecordingStore::default());
        let handler = make_handler(backend.clone(), store.clone());
        let session = session_for("alice", vec!["analyst"]);

        let err = handler
            .execute(&session, sql, None)
            .await
            .expect_err("non-admin must not list other principals' grants");
        assert!(
            err.to_string().contains("403"),
            "{sql}: expected 403: {err}"
        );
        assert!(
            !backend.show_grants_called.load(Ordering::SeqCst),
            "{sql}: must be rejected before the service-identity backend runs"
        );
    }
}

// ---------------------------------------------------------------------------
// #747 - SET/UNSET TAGS is policy DDL, admin only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_tags_requires_admin() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend, store);
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(&session, "ALTER TABLE cat.ns.tbl UNSET TAGS (ssn)", None)
        .await
        .expect_err("non-admin must not rewrite column tags");
    assert!(err.to_string().contains("403"), "expected 403: {err}");
}

// ---------------------------------------------------------------------------
// #911 - tag_authority = "ranger-delegate"
// ---------------------------------------------------------------------------

/// A handler under `tag_authority = "ranger-delegate"` over `store`.
fn make_delegate_handler(store: Arc<dyn PolicyStore>) -> QueryHandler {
    let mut config = minimal_config();
    config.access_control.tag_authority = sqe_core::config::TagAuthority::RangerDelegate;
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
        Some(Arc::new(RecordingBackend::default())),
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
}

/// The statement got past the tag gate (it may fail later: no catalog is wired).
fn assert_passed_tag_gate(res: sqe_core::Result<Vec<arrow_array::RecordBatch>>, sql: &str) {
    let e = res.expect_err("no catalog is wired, so the write itself cannot succeed");
    let msg = e.to_string();
    assert!(
        !msg.contains("403 Forbidden"),
        "{sql}: the tag gate must pass: {msg}"
    );
    assert!(
        !msg.contains("expected a"),
        "{sql}: the statement must parse: {msg}"
    );
}

fn assert_refused_by_tag_gate(res: sqe_core::Result<Vec<arrow_array::RecordBatch>>, sql: &str) {
    let e = res.expect_err("must be refused");
    assert!(
        e.to_string().contains("403 Forbidden"),
        "{sql}: expected 403: {e}"
    );
}

const TAG_STMTS: [&str; 3] = [
    "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))",
    "ALTER TABLE ns.tbl UNSET TAGS (ssn)",
    "ALTER TABLE ns.tbl MODIFY COLUMN ssn UNSET MASKING POLICY",
];

#[tokio::test]
async fn tag_authority_delegate_passes_with_a_grant() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .grant_table_privilege("ns", "tbl", "alice", sqe_policy::TablePrivilege::Alter)
        .await;
    let handler = make_delegate_handler(store);
    let session = session_for("alice", vec!["analyst"]);
    for sql in TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&session, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_authority_delegate_refuses_without_a_grant() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    // Alter on a different table, and Drop on this one: neither counts.
    store
        .grant_table_privilege("ns", "other", "alice", sqe_policy::TablePrivilege::Alter)
        .await;
    store
        .grant_table_privilege("ns", "tbl", "alice", sqe_policy::TablePrivilege::Drop)
        .await;
    let handler = make_delegate_handler(store);
    let session = session_for("alice", vec!["analyst"]);
    for sql in TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&session, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_authority_delegate_refuses_on_a_backend_without_the_model() {
    // RecordingStore keeps the default `grants_column_privilege`: nothing.
    let handler = make_delegate_handler(Arc::new(RecordingStore::default()));
    let session = session_for("alice", vec!["analyst"]);
    for sql in TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&session, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_authority_delegate_admin_still_passes() {
    let handler = make_delegate_handler(Arc::new(RecordingStore::default()));
    let session = session_for("root", vec!["service_admin"]);
    for sql in TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&session, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_authority_default_mode_refuses_every_tag_spelling() {
    let handler = make_handler(
        Arc::new(RecordingBackend::default()),
        Arc::new(RecordingStore::default()),
    );
    let session = session_for("alice", vec!["analyst"]);
    for sql in TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&session, sql, None).await, sql);
    }
}

#[tokio::test]
async fn set_reserved_tblproperties_requires_admin() {
    // `sqe.column-tags` is the same property SET TAGS writes, so writing it
    // through SET TBLPROPERTIES needs the same gate. The gate runs before
    // catalog resolution, so the unknown `cat` qualifier still yields 403.
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend, store);
    let session = session_for("alice", vec!["analyst"]);

    let err = handler
        .execute(
            &session,
            "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('SQE.column-tags' = '{}')",
            None,
        )
        .await
        .expect_err("non-admin must not write reserved sqe. properties");
    assert!(err.to_string().contains("403"), "expected 403: {err}");

    // A non-reserved key is not admin-gated (it fails later, on the missing
    // catalog, not on the admin gate).
    let err = handler
        .execute(
            &session,
            "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('write.format.default' = 'parquet')",
            None,
        )
        .await
        .expect_err("no catalog is wired");
    assert!(
        !err.to_string().contains("requires one of admin roles"),
        "unexpected admin gate: {err}"
    );
}

#[tokio::test]
async fn create_or_replace_with_reserved_tblproperties_requires_admin() {
    // #894: an in-place CREATE OR REPLACE sets the statement's properties on
    // the live table, whose older snapshots still hold the data its tags
    // mask, so naming `sqe.column-tags` there is gated like SET TBLPROPERTIES.
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend, store);
    let session = session_for("alice", vec!["analyst"]);

    let sql = "CREATE OR REPLACE TABLE cat.ns.tbl \
               TBLPROPERTIES ('sqe.column-tags' = '{}') AS SELECT 1 AS id";
    let err = handler
        .execute(&session, sql, None)
        .await
        .expect_err("non-admin must not replace column tags");
    assert!(err.to_string().contains("403"), "expected 403: {err}");

    let sql = "CREATE OR REPLACE TABLE cat.ns.tbl \
               TBLPROPERTIES ('write.merge.mode' = 'copy-on-write') AS SELECT 1 AS id";
    let err = handler
        .execute(&session, sql, None)
        .await
        .expect_err("no catalog is wired");
    assert!(
        !err.to_string().contains("403"),
        "unexpected admin gate: {err}"
    );
}

#[tokio::test]
async fn set_maintenance_tblproperties_stays_owner_settable() {
    // Table owners opt into autonomous compaction with `sqe.maintenance.*`
    // keys; only policy properties are admin-gated, not every `sqe.` key.
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend, store);
    let session = session_for("alice", vec!["analyst"]);

    for sql in [
        "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('sqe.maintenance.enabled' = 'true')",
        "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES \
         ('sqe.maintenance.compaction.min-input-files' = '8')",
    ] {
        // Gets past the gate, then fails on the missing catalog.
        let err = handler
            .execute(&session, sql, None)
            .await
            .expect_err("no catalog is wired");
        let msg = err.to_string();
        assert!(!msg.contains("403"), "unexpected 403 for {sql}: {msg}");
        assert!(
            msg.contains("unknown catalog"),
            "expected catalog error for {sql}: {msg}"
        );
    }
}

#[tokio::test]
async fn check_access_self_is_allowed() {
    let backend = Arc::new(RecordingBackend::default());
    let store = Arc::new(RecordingStore::default());
    let handler = make_handler(backend.clone(), store.clone());
    let session = session_for("alice", vec!["analyst"]);

    handler
        .execute(
            &session,
            "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"alice\"",
            None,
        )
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
        .execute(
            &session,
            "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"",
            None,
        )
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
            None,
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
        .execute(
            &session,
            "CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"",
            None,
        )
        .await
        .expect("admin must be able to check any principal's access");

    assert!(
        backend.check_access_called.load(Ordering::SeqCst),
        "admin access check must reach the backend"
    );
}

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

// ---------------------------------------------------------------------------
// #917 - tag_privilege, and every writer of `sqe.column-tags`
// ---------------------------------------------------------------------------

/// A store that grants a column privilege per (user, column, privilege),
/// whatever the table: the gate asks per column, so per-column answers are
/// what these tests need. Deny evaluation is the Ranger store's (its unit
/// tests); to the gate a deny is a `false` here.
#[derive(Default)]
struct ColumnGrants(Vec<(&'static str, &'static str, sqe_policy::TablePrivilege)>);

#[async_trait]
impl PolicyStore for ColumnGrants {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    fn invalidate_all(&self) {}

    async fn grants_column_privilege(
        &self,
        user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
        column: &str,
        privilege: sqe_policy::TablePrivilege,
    ) -> sqe_core::Result<bool> {
        Ok(self
            .0
            .iter()
            .any(|(u, c, p)| *u == user.username && *c == column && *p == privilege))
    }
}

fn tag_handler(
    config: SqeConfig,
    store: Arc<dyn PolicyStore>,
    authority: sqe_core::config::TagAuthority,
    privilege: sqe_core::config::TagPrivilege,
) -> QueryHandler {
    let mut config = config;
    config.access_control.tag_authority = authority;
    config.access_control.tag_privilege = privilege;
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

fn apply_tag_handler(grants: ColumnGrants) -> QueryHandler {
    tag_handler(
        minimal_config(),
        Arc::new(grants),
        sqe_core::config::TagAuthority::RangerDelegate,
        sqe_core::config::TagPrivilege::ApplyTag,
    )
}

#[tokio::test]
async fn apply_tag_privilege_gates_the_tag_statements() {
    use sqe_policy::TablePrivilege::{Alter, ApplyTag};
    let handler = apply_tag_handler(ColumnGrants(vec![
        ("steward", "ssn", ApplyTag),
        ("owner", "ssn", Alter),
    ]));
    for sql in TAG_STMTS {
        let steward = session_for("steward", vec!["analyst"]);
        assert_passed_tag_gate(handler.execute(&steward, sql, None).await, sql);
        // `alter` is not `apply_tag`.
        let owner = session_for("owner", vec!["analyst"]);
        let err = handler
            .execute(&owner, sql, None)
            .await
            .expect_err("refused");
        assert!(
            err.to_string().contains("requires apply_tag on ns.tbl.ssn"),
            "{sql}: {err}"
        );
        let root = session_for("root", vec!["service_admin"]);
        assert_passed_tag_gate(handler.execute(&root, sql, None).await, sql);
    }
    // The default privilege stays `alter`.
    let handler = tag_handler(
        minimal_config(),
        Arc::new(ColumnGrants(vec![("owner", "ssn", Alter)])),
        sqe_core::config::TagAuthority::RangerDelegate,
        sqe_core::config::TagPrivilege::default(),
    );
    let owner = session_for("owner", vec!["analyst"]);
    for sql in TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&owner, sql, None).await, sql);
    }
}

#[tokio::test]
async fn plain_create_naming_tags_needs_the_privilege_on_every_named_column() {
    use sqe_policy::TablePrivilege::ApplyTag;
    let handler = apply_tag_handler(ColumnGrants(vec![
        ("steward", "ssn", ApplyTag),
        ("steward", "email", ApplyTag),
    ]));
    let steward = session_for("steward", vec!["analyst"]);
    for sql in [
        r#"CREATE TABLE ns.tbl (ssn INT, email INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"],"email":["PII"]}')"#,
        r#"CREATE TABLE ns.tbl TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"]}') AS SELECT 1 AS ssn"#,
        r#"CREATE TABLE ns.tbl (ssn INT) WITH (extra_properties = MAP(ARRAY['sqe.column-tags'], ARRAY['{"ssn":["PII"]}']))"#,
        // A new table with an empty map tags nothing.
        "CREATE TABLE ns.tbl (ssn INT) TBLPROPERTIES ('sqe.column-tags' = '{}')",
    ] {
        assert_passed_tag_gate(handler.execute(&steward, sql, None).await, sql);
    }
    for sql in [
        r#"CREATE TABLE ns.tbl (ssn INT, phone INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"],"phone":["PII"]}')"#,
        r#"CREATE TABLE ns.tbl TBLPROPERTIES ('sqe.column-tags' = '{"phone":["PII"]}') AS SELECT 1 AS phone"#,
    ] {
        let err = handler
            .execute(&steward, sql, None)
            .await
            .expect_err("refused");
        assert!(
            err.to_string()
                .contains("requires apply_tag on ns.tbl.phone"),
            "{sql}: {err}"
        );
    }
    let root = session_for("root", vec!["service_admin"]);
    let sql = r#"CREATE TABLE ns.tbl (phone INT) TBLPROPERTIES ('sqe.column-tags' = '{"phone":["PII"]}')"#;
    assert_passed_tag_gate(handler.execute(&root, sql, None).await, sql);
}

#[tokio::test]
async fn raw_tag_writes_refuse_a_malformed_value_or_an_off_case_key_before_the_catalog() {
    use sqe_policy::TablePrivilege::ApplyTag;
    let handler = apply_tag_handler(ColumnGrants(vec![("steward", "ssn", ApplyTag)]));
    let steward = session_for("steward", vec!["analyst"]);
    // `cat` is no catalog: a 403 here was decided before catalog resolution.
    for sql in [
        "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('sqe.column-tags' = 'not-json')",
        r#"ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('sqe.column-tags' = '{"ssn":"PII"}')"#,
        r#"ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('SQE.column-tags' = '{"ssn":["PII"]}')"#,
        "CREATE OR REPLACE TABLE cat.ns.tbl TBLPROPERTIES ('sqe.column-tags' = '[]') AS SELECT 1 AS ssn",
        r#"CREATE TABLE cat.ns.tbl (ssn INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":')"#,
    ] {
        let err = handler.execute(&steward, sql, None).await.expect_err("refused");
        assert!(err.to_string().contains("403 Forbidden"), "{sql}: {err}");
        assert!(!err.to_string().contains("unknown catalog"), "{sql}: {err}");
    }
    // An admin is not gated on the value at all.
    let root = session_for("root", vec!["service_admin"]);
    let sql = "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('sqe.column-tags' = 'not-json')";
    let err = handler
        .execute(&root, sql, None)
        .await
        .expect_err("no catalog");
    assert!(!err.to_string().contains("403"), "{err}");
}

#[tokio::test]
async fn admin_role_mode_is_unchanged_but_sees_extra_properties() {
    // Under the default `admin-role` a non-admin may not write the key
    // through ALTER or CREATE OR REPLACE, may through a plain CREATE (a new
    // table), and a grant changes nothing.
    use sqe_policy::TablePrivilege::{Alter, ApplyTag};
    let handler = tag_handler(
        minimal_config(),
        Arc::new(ColumnGrants(vec![
            ("alice", "ssn", ApplyTag),
            ("alice", "ssn", Alter),
        ])),
        sqe_core::config::TagAuthority::AdminRole,
        sqe_core::config::TagPrivilege::ApplyTag,
    );
    let alice = session_for("alice", vec!["analyst"]);
    for sql in [
        r#"ALTER TABLE cat.ns.tbl SET TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"]}')"#,
        // The Trino spelling of the same write was not seen by the 0.55 gate.
        "ALTER TABLE cat.ns.tbl SET TBLPROPERTIES (extra_properties = MAP(ARRAY['sqe.column-tags'], ARRAY['{}']))",
        "CREATE OR REPLACE TABLE cat.ns.tbl WITH (extra_properties = MAP(ARRAY['sqe.column-tags'], ARRAY['{}'])) AS SELECT 1 AS ssn",
    ] {
        let err = handler.execute(&alice, sql, None).await.expect_err("refused");
        assert!(err.to_string().contains("requires one of admin roles"), "{sql}: {err}");
    }
    for sql in TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&alice, sql, None).await, sql);
    }
    let sql =
        r#"CREATE TABLE ns.tbl (ssn INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"]}')"#;
    assert_passed_tag_gate(handler.execute(&alice, sql, None).await, sql);
}

// ---------------------------------------------------------------------------
// #926 - tag_names = "ranger-tagdefs": refuse undefined tag names
// ---------------------------------------------------------------------------

/// Column grants plus a tag-definition list. `defs: None` is a backend whose
/// list cannot be read; `late` is defined only on a fresh read (a tag the
/// portal defined after the list was cached).
#[derive(Default)]
struct Tagdefs {
    grants: ColumnGrants,
    /// (user, table) pairs granted every privilege on the whole table.
    tables: Vec<(&'static str, &'static str)>,
    defs: Option<Vec<&'static str>>,
    late: Option<&'static str>,
    scoped: bool,
    reads: AtomicU32,
    fresh_reads: AtomicU32,
}

#[async_trait]
impl PolicyStore for Tagdefs {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    fn invalidate_all(&self) {}

    fn catalog_scoped(&self) -> bool {
        self.scoped
    }

    async fn grants_table_privilege(
        &self,
        user: &SessionUser,
        table_name: &str,
        _namespace: &str,
        _privilege: sqe_policy::TablePrivilege,
    ) -> sqe_core::Result<bool> {
        Ok(self
            .tables
            .iter()
            .any(|(u, t)| *u == user.username && *t == table_name))
    }

    async fn grants_column_privilege(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
        column: &str,
        privilege: sqe_policy::TablePrivilege,
    ) -> sqe_core::Result<bool> {
        self.grants
            .grants_column_privilege(user, table_name, namespace, column, privilege)
            .await
    }

    async fn tag_definitions(
        &self,
        fresh: bool,
    ) -> sqe_core::Result<Arc<std::collections::HashSet<String>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if fresh {
            self.fresh_reads.fetch_add(1, Ordering::SeqCst);
        }
        let defs = self.defs.as_ref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "could not read the Ranger tag definitions: HTTP 503".into(),
            )
        })?;
        let mut out: std::collections::HashSet<String> =
            defs.iter().map(|d| d.to_string()).collect();
        if fresh {
            out.extend(self.late.map(String::from));
        }
        Ok(Arc::new(out))
    }
}

/// A handler with `tag_names = "ranger-tagdefs"` and `authority` over `store`.
fn tagdef_handler(store: Arc<Tagdefs>, authority: sqe_core::config::TagAuthority) -> QueryHandler {
    let mut config = minimal_config();
    config.access_control.tag_names = sqe_core::config::TagNames::RangerTagdefs;
    tag_handler(
        config,
        store,
        authority,
        sqe_core::config::TagPrivilege::Alter,
    )
}

fn steward_tagdefs(defs: Option<Vec<&'static str>>) -> Tagdefs {
    use sqe_policy::TablePrivilege::Alter;
    Tagdefs {
        grants: ColumnGrants(vec![("steward", "ssn", Alter)]),
        defs,
        ..Default::default()
    }
}

/// Got past both the privilege and the name check.
fn assert_passed_name_check(res: sqe_core::Result<Vec<arrow_array::RecordBatch>>, sql: &str) {
    if let Err(e) = &res {
        assert!(!e.to_string().contains("not defined"), "{sql}: {e}");
        assert!(!e.to_string().contains("tag names"), "{sql}: {e}");
    }
    assert_passed_tag_gate(res, sql);
}

fn assert_undefined(res: sqe_core::Result<Vec<arrow_array::RecordBatch>>, sql: &str, tag: &str) {
    let e = res.expect_err("an undefined tag must be refused");
    assert!(
        e.to_string()
            .contains(&format!("tag '{tag}' is not defined in Ranger")),
        "{sql}: {e}"
    );
    assert!(
        !e.to_string().contains("403"),
        "{sql}: a name error, not a denial: {e}"
    );
}

#[tokio::test]
async fn tag_names_refuses_an_undefined_tag_and_accepts_a_defined_one() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    let store = Arc::new(steward_tagdefs(Some(vec!["live.pii", "PII"])));
    let handler = tagdef_handler(store.clone(), RangerDelegate);
    let steward = session_for("steward", vec!["analyst"]);

    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('live.pi'))";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "live.pi");
    // A cached miss is re-read once before the refusal.
    assert_eq!(store.fresh_reads.load(Ordering::SeqCst), 1);

    for sql in [
        "ALTER TABLE ns.tbl SET TAGS (ssn = ('live.pii'))",
        "ALTER TABLE ns.tbl ALTER COLUMN ssn SET TAGS ('live.pii' = 'high', 'PII')",
        "ALTER TABLE ns.tbl MODIFY COLUMN ssn SET TAG live.pii = 'x'",
    ] {
        assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    }
    // The native list has no value syntax: refused by the parser, not stored
    // (and name-checked) as the key `live.pii' = 'high`.
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('live.pii' = 'high', 'PII'))";
    let err = handler.execute(&steward, sql, None).await.expect_err(sql);
    assert!(
        err.to_string().contains("takes tag names only"),
        "{sql}: {err}"
    );
    // Exact match: tag names are case-sensitive, as tag policies match them.
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('pii'))";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "pii");
    // One undefined name in the list refuses the statement.
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII', 'live.pi'))";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "live.pi");
}

#[tokio::test]
async fn tag_names_accepts_a_tag_defined_since_the_list_was_cached() {
    let store = Arc::new(Tagdefs {
        late: Some("live.new"),
        ..steward_tagdefs(Some(vec!["PII"]))
    });
    let handler = tagdef_handler(
        store.clone(),
        sqe_core::config::TagAuthority::RangerDelegate,
    );
    let steward = session_for("steward", vec!["analyst"]);
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('live.new'))";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    assert_eq!(store.fresh_reads.load(Ordering::SeqCst), 1);
    // A defined name is answered from the cached list alone.
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    assert_eq!(store.fresh_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tag_names_fails_closed_but_never_blocks_an_untag() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    // The list cannot be read.
    let store = Arc::new(steward_tagdefs(None));
    let handler = tagdef_handler(store.clone(), RangerDelegate);
    let steward = session_for("steward", vec!["analyst"]);
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))";
    let err = handler
        .execute(&steward, sql, None)
        .await
        .expect_err("fail closed");
    assert!(err.to_string().contains("403 Forbidden"), "{err}");
    assert!(
        err.to_string().contains("cannot check the tag names"),
        "{err}"
    );
    // Removing a tag names nothing new, so it needs no list.
    for sql in [
        "ALTER TABLE ns.tbl UNSET TAGS (ssn)",
        "ALTER TABLE ns.tbl UNSET TAGS (ssn = ('PII'))",
        "ALTER TABLE ns.tbl MODIFY COLUMN ssn UNSET MASKING POLICY",
    ] {
        assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    }
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        1,
        "only the SET read the list"
    );

    // A backend without tag definitions (the trait default) refuses too.
    use sqe_policy::TablePrivilege::Alter;
    let handler = {
        let mut config = minimal_config();
        config.access_control.tag_names = sqe_core::config::TagNames::RangerTagdefs;
        tag_handler(
            config,
            Arc::new(ColumnGrants(vec![("steward", "ssn", Alter)])),
            RangerDelegate,
            sqe_core::config::TagPrivilege::Alter,
        )
    };
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))";
    let err = handler
        .execute(&steward, sql, None)
        .await
        .expect_err("fail closed");
    assert!(
        err.to_string().contains("cannot list tag definitions"),
        "{err}"
    );
}

#[tokio::test]
async fn tag_names_exempts_admins_and_checks_only_after_the_privilege() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    let store = Arc::new(steward_tagdefs(None));
    let handler = tagdef_handler(store.clone(), RangerDelegate);
    // An admin is not checked, even with an unreadable list.
    let root = session_for("root", vec!["service_admin"]);
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('anything.at.all'))";
    assert_passed_name_check(handler.execute(&root, sql, None).await, sql);
    // A caller without the privilege is refused for that, before the list is
    // read: the refusal tells them nothing about which tags exist.
    let mallory = session_for("mallory", vec!["analyst"]);
    let err = handler
        .execute(&mallory, sql, None)
        .await
        .expect_err("refused");
    assert!(
        err.to_string().contains("requires alter on ns.tbl.ssn"),
        "{err}"
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tag_names_any_is_the_default_and_reads_nothing() {
    let store = Arc::new(steward_tagdefs(None));
    let handler = tag_handler(
        minimal_config(),
        store.clone(),
        sqe_core::config::TagAuthority::RangerDelegate,
        sqe_core::config::TagPrivilege::Alter,
    );
    let steward = session_for("steward", vec!["analyst"]);
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('live.pi'))";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tag_names_under_catalog_scoped_accepts_the_catalog_qualified_definition() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    let steward = session_for("steward", vec!["analyst"]);
    let store = Arc::new(Tagdefs {
        scoped: true,
        ..steward_tagdefs(Some(vec!["sales.pii", "PII"]))
    });
    let handler = tagdef_handler(store, RangerDelegate);
    // `sales.pii` defines `pii` in catalog `sales`...
    let sql = "ALTER TABLE sales.ns.tbl SET TAGS (ssn = ('pii'))";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    // ...and not in another catalog.
    let sql = "ALTER TABLE ops.ns.tbl SET TAGS (ssn = ('pii'))";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "pii");
    // A bare definition still defines the tag in every catalog.
    for sql in [
        "ALTER TABLE sales.ns.tbl SET TAGS (ssn = ('PII'))",
        "ALTER TABLE ops.ns.tbl SET TAGS (ssn = ('PII'))",
    ] {
        assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    }
    // The qualified name itself is a defined tag, as any name in the list is.
    let sql = "ALTER TABLE ops.ns.tbl SET TAGS (ssn = ('sales.pii'))";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);

    // Without catalog_scoped a qualified definition defines only itself.
    let store = Arc::new(steward_tagdefs(Some(vec!["sales.pii"])));
    let handler = tagdef_handler(store, RangerDelegate);
    let sql = "ALTER TABLE sales.ns.tbl SET TAGS (ssn = ('pii'))";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "pii");
}

#[tokio::test]
async fn tag_names_checks_the_tags_a_new_table_names() {
    use sqe_core::config::TagAuthority::{AdminRole, RangerDelegate};
    let ok =
        r#"CREATE TABLE ns.tbl (ssn INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"]}')"#;
    let typo = r#"CREATE TABLE ns.tbl (ssn INT) TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII",{"live.pi":"x"}]}')"#;
    let alice = session_for("steward", vec!["analyst"]);
    // `admin-role` lets a non-admin name tags on a new table; their names are
    // still checked.
    for authority in [AdminRole, RangerDelegate] {
        let handler = tagdef_handler(Arc::new(steward_tagdefs(Some(vec!["PII"]))), authority);
        assert_passed_name_check(handler.execute(&alice, ok, None).await, ok);
        assert_undefined(handler.execute(&alice, typo, None).await, typo, "live.pi");
        let root = session_for("root", vec!["service_admin"]);
        assert_passed_name_check(handler.execute(&root, typo, None).await, typo);
    }
    // Off, the admin-role plain CREATE is ungated as before.
    let handler = tag_handler(
        minimal_config(),
        Arc::new(steward_tagdefs(None)),
        AdminRole,
        sqe_core::config::TagPrivilege::Alter,
    );
    assert_passed_tag_gate(handler.execute(&alice, typo, None).await, typo);
}

/// The routes that diff against a live table, on a sqlite catalog.
#[cfg(feature = "test-sqlite")]
mod live_tag_maps {
    use super::*;
    use sqe_core::config::{TagAuthority, TagPrivilege};
    use sqe_policy::TablePrivilege::{Alter, ApplyTag};
    use sqe_write::test_support::sqlite_rig::SqliteRig;
    use std::collections::HashMap;

    struct Rig {
        rig: SqliteRig,
        handler: QueryHandler,
    }

    impl Rig {
        /// Table `t (ssn, email, phone)` with `ssn` and `email` tagged.
        async fn new(authority: TagAuthority, grants: ColumnGrants) -> Self {
            Self::with_removal(authority, grants, Default::default()).await
        }

        async fn with_removal(
            authority: TagAuthority,
            grants: ColumnGrants,
            removal: sqe_core::config::TagRemoval,
        ) -> Self {
            let rig = SqliteRig::new().await;
            let mut config = rig.config.clone();
            config.access_control.tag_removal = removal;
            let handler = tag_handler(config, Arc::new(grants), authority, TagPrivilege::ApplyTag);
            let me = Self { rig, handler };
            me.run("root", "CREATE TABLE t (ssn INT, email INT, phone INT)")
                .await
                .expect("create");
            me.run(
                "root",
                r#"ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"],"email":["PII"]}')"#,
            )
            .await
            .expect("seed tags");
            me
        }

        async fn run(
            &self,
            user: &str,
            sql: &str,
        ) -> sqe_core::Result<Vec<arrow_array::RecordBatch>> {
            let roles = if user == "root" {
                vec!["service_admin"]
            } else {
                vec!["analyst"]
            };
            let mut session = session_for(user, roles);
            session.default_schema = Some(self.rig.session().default_schema.unwrap());
            self.handler.execute(&session, sql, None).await
        }

        async fn tags(&self) -> HashMap<String, Vec<String>> {
            let table = self.rig.load(&self.rig.ident("t")).await;
            sqe_write::tag_source_impl::parse_column_tags(table.metadata().properties()).unwrap()
        }
    }

    fn set(json: &str) -> String {
        format!("ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = '{json}')")
    }

    fn refused_on(err: sqe_core::SqeError, column: &str) {
        let msg = err.to_string();
        assert!(msg.contains("403 Forbidden"), "{msg}");
        let column = if column.contains('.') {
            column.to_string()
        } else {
            format!("t.{column}")
        };
        assert!(
            msg.contains(&format!("requires apply_tag on default.{column}")),
            "{msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn set_tblproperties_checks_the_changed_columns_only() {
        let r = Rig::new(
            TagAuthority::RangerDelegate,
            ColumnGrants(vec![
                ("steward", "phone", ApplyTag),
                ("owner", "ssn", Alter),
            ]),
        )
        .await;
        // Add `phone`, keep the rest as is (in another order): only `phone` changes.
        r.run(
            "steward",
            &set(r#"{"email":["PII"],"phone":["PII"],"ssn":["PII"]}"#),
        )
        .await
        .expect("phone is granted");
        assert_eq!(r.tags().await.len(), 3);
        // Dropping `email` from the map is an untag of `email`.
        refused_on(
            r.run("steward", &set(r#"{"ssn":["PII"],"phone":["PII"]}"#))
                .await
                .unwrap_err(),
            "email",
        );
        // Changing `ssn`'s tags.
        refused_on(
            r.run(
                "steward",
                &set(r#"{"ssn":["SECRET"],"email":["PII"],"phone":["PII"]}"#),
            )
            .await
            .unwrap_err(),
            "ssn",
        );
        // Clearing the map, the stand-in for UNSET TBLPROPERTIES.
        refused_on(r.run("steward", &set("{}")).await.unwrap_err(), "email");
        // `alter` on the column is not `apply_tag`.
        refused_on(
            r.run("owner", &set(r#"{"email":["PII"],"phone":["PII"]}"#))
                .await
                .unwrap_err(),
            "ssn",
        );
        assert_eq!(r.tags().await.len(), 3, "no refused write landed");
        // The steward may untag what they may tag.
        r.run("steward", &set(r#"{"ssn":["PII"],"email":["PII"]}"#))
            .await
            .expect("untag phone");
        assert!(!r.tags().await.contains_key("phone"));
        // An admin may do anything.
        r.run("root", &set("{}")).await.expect("admin");
        assert!(r.tags().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tag_removal_gates_the_keys_a_raw_write_drops() {
        use sqe_policy::TablePrivilege::RemoveTag;
        let mut grants: Vec<_> = ["ssn", "email", "phone"]
            .map(|c| ("steward", c, ApplyTag))
            .into();
        grants.push(("steward", "email", RemoveTag));
        let removal = sqe_core::config::TagRemoval::RemoveTag;
        let r =
            Rig::with_removal(TagAuthority::RangerDelegate, ColumnGrants(grants), removal).await;
        // Adding a column or a key removes nothing.
        let sql = set(r#"{"ssn":["PII","GDPR"],"email":["PII"],"phone":["PII"]}"#);
        r.run("steward", &sql).await.expect("adds only");
        // Dropping a key from `ssn` needs `remove_tag` there.
        let err = r
            .run(
                "steward",
                &set(r#"{"ssn":["PII"],"email":["PII"],"phone":["PII"]}"#),
            )
            .await;
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("requires remove_tag on default.t.ssn"),
            "{msg}"
        );
        assert_eq!(
            r.tags().await["ssn"].len(),
            2,
            "the refused write did not land"
        );
        // `email` is granted it.
        let sql = set(r#"{"ssn":["PII","GDPR"],"phone":["PII"]}"#);
        r.run("steward", &sql).await.expect("email may be untagged");
        assert!(!r.tags().await.contains_key("email"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unset_tblproperties_does_not_reach_the_tags() {
        let r = Rig::new(TagAuthority::RangerDelegate, ColumnGrants::default()).await;
        for sql in [
            "ALTER TABLE t UNSET TBLPROPERTIES ('sqe.column-tags')",
            "ALTER TABLE t UNSET TBLPROPERTIES IF EXISTS ('sqe.column-tags')",
        ] {
            assert!(r.run("steward", sql).await.is_err(), "{sql}");
        }
        assert_eq!(r.tags().await.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_or_replace_diffs_against_the_live_table() {
        let r = Rig::new(
            TagAuthority::RangerDelegate,
            ColumnGrants(vec![("steward", "phone", ApplyTag)]),
        )
        .await;
        let replace = |json: &str| {
            format!(
                "CREATE OR REPLACE TABLE t TBLPROPERTIES ('sqe.column-tags' = '{json}') \
                 AS SELECT 1 AS ssn, 2 AS email, 3 AS phone"
            )
        };
        refused_on(
            r.run("steward", &replace(r#"{"phone":["PII"]}"#))
                .await
                .unwrap_err(),
            "email",
        );
        assert_eq!(r.tags().await.len(), 2, "nothing replaced");
        r.run(
            "steward",
            &replace(r#"{"ssn":["PII"],"email":["PII"],"phone":["PII"]}"#),
        )
        .await
        .expect("only phone changes");
        assert_eq!(r.tags().await.len(), 3);
        // Replacing without naming the key keeps the tags and needs nothing.
        r.run(
            "steward",
            "CREATE OR REPLACE TABLE t AS SELECT 1 AS ssn, 2 AS email, 3 AS phone",
        )
        .await
        .expect("tags untouched");
        assert_eq!(r.tags().await.len(), 3);
        // A replace that creates the table checks every column named.
        refused_on(
            r.run(
                "steward",
                r#"CREATE OR REPLACE TABLE u TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["PII"]}') AS SELECT 1 AS ssn"#,
            )
            .await
            .unwrap_err(),
            "u.ssn",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_live_map_is_an_admins_to_overwrite() {
        let r = Rig::new(
            TagAuthority::RangerDelegate,
            ColumnGrants(vec![("steward", "phone", ApplyTag)]),
        )
        .await;
        r.run("root", &set("not-json"))
            .await
            .expect("admin writes anything");
        let err = r
            .run("steward", &set(r#"{"phone":["PII"]}"#))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not a tag map"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_role_still_refuses_set_tblproperties() {
        let r = Rig::new(
            TagAuthority::AdminRole,
            ColumnGrants(vec![("steward", "phone", ApplyTag)]),
        )
        .await;
        let err = r
            .run(
                "steward",
                &set(r#"{"ssn":["PII"],"email":["PII"],"phone":["PII"]}"#),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("requires one of admin roles"),
            "{err}"
        );
        assert_eq!(r.tags().await.len(), 2);
    }

    /// `(tag_name, tag_value)` rows of a tag view, as the caller sees them.
    async fn view_tags(r: &Rig, view: &str) -> Vec<(String, String)> {
        let batches = r
            .run(
                "root",
                &format!(
                    "SELECT tag_name, tag_value FROM information_schema.{view} \
                     ORDER BY tag_name"
                ),
            )
            .await
            .expect("read tag view");
        let mut out = Vec::new();
        for batch in batches {
            let names = arrow::array::as_string_array(batch.column(0));
            let values = arrow::array::as_string_array(batch.column(1));
            for i in 0..batch.num_rows() {
                out.push((names.value(i).to_string(), values.value(i).to_string()));
            }
        }
        out
    }

    /// #922: a tag written by `ALTER ... SET TAGS` shows in the matching
    /// `information_schema` view in the next statement, and an `UNSET`
    /// takes it out again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_tags_read_back_through_the_tag_views() {
        let r = Rig::new(TagAuthority::AdminRole, ColumnGrants(vec![])).await;
        let pair = |k: &str, v: &str| (k.to_string(), v.to_string());

        r.run(
            "root",
            "ALTER TABLE t SET TAGS ('gold', 'owner' = 'finance')",
        )
        .await
        .expect("set table tags");
        assert_eq!(
            view_tags(&r, "table_tags").await,
            vec![pair("gold", ""), pair("owner", "finance")]
        );
        r.run("root", "ALTER TABLE t UNSET TAGS ('gold')")
            .await
            .expect("unset table tag");
        assert_eq!(
            view_tags(&r, "table_tags").await,
            vec![pair("owner", "finance")]
        );

        let schema = r.rig.session().default_schema.unwrap();
        r.run(
            "root",
            &format!("ALTER SCHEMA \"{schema}\" SET TAGS ('domain' = 'hr')"),
        )
        .await
        .expect("set schema tags");
        assert_eq!(
            view_tags(&r, "schema_tags").await,
            vec![pair("domain", "hr")]
        );
        r.run(
            "root",
            &format!("ALTER SCHEMA \"{schema}\" UNSET TAGS ('domain')"),
        )
        .await
        .expect("unset schema tag");
        assert!(view_tags(&r, "schema_tags").await.is_empty());
    }
}

// ---------------------------------------------------------------------------
// #922 - tags on a table or schema itself
// ---------------------------------------------------------------------------

const TABLE_TAG_STMTS: [&str; 2] = [
    "ALTER TABLE ns.tbl SET TAGS ('gold', 'owner' = 'finance')",
    "ALTER TABLE ns.tbl UNSET TAGS ('gold')",
];

const SCHEMA_TAG_STMTS: [&str; 2] = [
    "ALTER SCHEMA ns SET TAGS ('gold', 'owner' = 'finance')",
    "ALTER SCHEMA ns UNSET TAGS ('gold')",
];

#[tokio::test]
async fn object_tags_admin_role_refuses_a_steward_and_passes_an_admin() {
    let handler = make_handler(
        Arc::new(RecordingBackend::default()),
        Arc::new(RecordingStore::default()),
    );
    let steward = session_for("alice", vec!["analyst"]);
    let admin = session_for("root", vec!["service_admin"]);
    for sql in TABLE_TAG_STMTS.into_iter().chain(SCHEMA_TAG_STMTS) {
        assert_refused_by_tag_gate(handler.execute(&steward, sql, None).await, sql);
        assert_passed_tag_gate(handler.execute(&admin, sql, None).await, sql);
    }
}

#[tokio::test]
async fn object_tags_delegate_needs_the_table_grant() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .grant_table_privilege("ns", "tbl", "alice", sqe_policy::TablePrivilege::Alter)
        .await;
    let handler = make_delegate_handler(store);
    let alice = session_for("alice", vec!["analyst"]);
    let bob = session_for("bob", vec!["analyst"]);
    for sql in TABLE_TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&alice, sql, None).await, sql);
        assert_refused_by_tag_gate(handler.execute(&bob, sql, None).await, sql);
    }
    // A table grant is not a schema grant.
    for sql in SCHEMA_TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&alice, sql, None).await, sql);
    }
}

#[tokio::test]
async fn object_tags_column_grant_is_not_a_table_grant() {
    use sqe_policy::TablePrivilege::Alter;
    let handler = apply_tag_handler(ColumnGrants(vec![("steward", "ssn", Alter)]));
    let steward = session_for("steward", vec!["analyst"]);
    for sql in TABLE_TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&steward, sql, None).await, sql);
    }
}

#[tokio::test]
async fn object_tags_schema_needs_the_database_wildcard_grant() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .grant_table_privilege("ns", "*", "alice", sqe_policy::TablePrivilege::Alter)
        .await;
    let handler = make_delegate_handler(store);
    let alice = session_for("alice", vec!["analyst"]);
    for sql in SCHEMA_TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&alice, sql, None).await, sql);
    }
}

#[tokio::test]
async fn object_tags_refuse_an_undefined_key() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    let store = Arc::new(Tagdefs {
        tables: vec![("steward", "tbl"), ("steward", "*")],
        defs: Some(vec!["gold"]),
        ..Default::default()
    });
    let handler = tagdef_handler(store, RangerDelegate);
    let steward = session_for("steward", vec!["analyst"]);
    for sql in [
        "ALTER TABLE ns.tbl SET TAGS ('gold', 'silver')",
        "ALTER SCHEMA ns SET TAGS ('silver' = 'x')",
    ] {
        assert_undefined(handler.execute(&steward, sql, None).await, sql, "silver");
    }
    // UNSET removes names; it does not need them defined.
    for sql in [
        "ALTER TABLE ns.tbl SET TAGS ('gold' = 'x')",
        "ALTER SCHEMA ns UNSET TAGS ('silver')",
    ] {
        assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    }
}

#[tokio::test]
async fn object_tags_raw_table_property_is_reserved_for_admins() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .grant_table_privilege("ns", "tbl", "alice", sqe_policy::TablePrivilege::Alter)
        .await;
    let delegate = make_delegate_handler(store);
    let default = make_handler(
        Arc::new(RecordingBackend::default()),
        Arc::new(RecordingStore::default()),
    );
    let alice = session_for("alice", vec!["analyst"]);
    for sql in [
        "ALTER TABLE ns.tbl SET TBLPROPERTIES ('sqe.table-tags' = '[\"gold\"]')",
        "ALTER TABLE ns.tbl SET TBLPROPERTIES ('SQE.Table-Tags' = '[]')",
    ] {
        for handler in [&delegate, &default] {
            let err = handler.execute(&alice, sql, None).await.expect_err(sql);
            assert!(
                err.to_string().contains("sqe.table-tags table property"),
                "{sql}: {err}"
            );
        }
    }
}

const CATALOG_TAG_STMTS: [&str; 2] = [
    "ALTER CATALOG iceberg SET TAGS ('gold', 'owner' = 'finance')",
    "ALTER CATALOG iceberg UNSET TAGS ('gold')",
];

#[tokio::test]
async fn catalog_tags_admin_role_refuses_a_steward_and_passes_an_admin() {
    let handler = make_handler(
        Arc::new(RecordingBackend::default()),
        Arc::new(RecordingStore::default()),
    );
    let steward = session_for("alice", vec!["analyst"]);
    let admin = session_for("root", vec!["service_admin"]);
    for sql in CATALOG_TAG_STMTS {
        assert_refused_by_tag_gate(handler.execute(&steward, sql, None).await, sql);
        assert_passed_tag_gate(handler.execute(&admin, sql, None).await, sql);
    }
}

/// Under `ranger-delegate` the query service has no catalog resource to
/// grant on: the write goes to Polaris on the caller's token, which decides.
#[tokio::test]
async fn catalog_tags_delegate_leaves_the_privilege_to_polaris() {
    let handler = make_delegate_handler(Arc::new(RecordingStore::default()));
    let steward = session_for("alice", vec!["analyst"]);
    for sql in CATALOG_TAG_STMTS {
        assert_passed_tag_gate(handler.execute(&steward, sql, None).await, sql);
    }
}

#[tokio::test]
async fn catalog_tags_refuse_an_undefined_key_and_a_dotted_name() {
    use sqe_core::config::TagAuthority::RangerDelegate;
    let store = Arc::new(Tagdefs {
        defs: Some(vec!["gold"]),
        ..Default::default()
    });
    let handler = tagdef_handler(store, RangerDelegate);
    let steward = session_for("steward", vec!["analyst"]);
    let sql = "ALTER CATALOG iceberg SET TAGS ('gold', 'silver')";
    assert_undefined(handler.execute(&steward, sql, None).await, sql, "silver");
    let sql = "ALTER CATALOG iceberg UNSET TAGS ('silver')";
    assert_passed_name_check(handler.execute(&steward, sql, None).await, sql);
    let sql = "ALTER CATALOG a.b SET TAGS ('gold')";
    let err = handler.execute(&steward, sql, None).await.expect_err(sql);
    assert!(
        err.to_string().contains("single catalog name"),
        "{sql}: {err}"
    );
}

// ---------------------------------------------------------------------------
// #927 - tag_removal: removing a tag can need more than adding one
// ---------------------------------------------------------------------------

fn removal_handler(
    store: Arc<dyn PolicyStore>,
    removal: sqe_core::config::TagRemoval,
    privilege: sqe_core::config::TagPrivilege,
) -> QueryHandler {
    let mut config = minimal_config();
    config.access_control.tag_removal = removal;
    let authority = sqe_core::config::TagAuthority::RangerDelegate;
    tag_handler(config, store, authority, privilege)
}

const UNTAG_STMTS: [&str; 2] = [
    "ALTER TABLE ns.tbl UNSET TAGS (ssn)",
    "ALTER TABLE ns.tbl MODIFY COLUMN ssn UNSET MASKING POLICY",
];

#[tokio::test]
async fn tag_removal_remove_tag_gates_the_untags_only() {
    use sqe_core::config::{TagPrivilege, TagRemoval};
    use sqe_policy::TablePrivilege::{ApplyTag, RemoveTag};
    let grants = ColumnGrants(vec![
        ("steward", "ssn", ApplyTag),
        ("remover", "ssn", ApplyTag),
        ("remover", "ssn", RemoveTag),
        ("bare", "ssn", RemoveTag),
    ]);
    let handler = removal_handler(
        Arc::new(grants),
        TagRemoval::RemoveTag,
        TagPrivilege::ApplyTag,
    );
    let session = |u: &str| session_for(u, vec!["analyst"]);
    // Adding a tag needs the tag privilege only.
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))";
    assert_passed_tag_gate(handler.execute(&session("steward"), sql, None).await, sql);
    for sql in UNTAG_STMTS {
        let err = handler
            .execute(&session("steward"), sql, None)
            .await
            .expect_err(sql);
        assert!(
            err.to_string()
                .contains("requires remove_tag on ns.tbl.ssn"),
            "{sql}: {err}"
        );
        assert_passed_tag_gate(handler.execute(&session("remover"), sql, None).await, sql);
        // `remove_tag` stacks on the tag privilege; it does not replace it.
        let err = handler
            .execute(&session("bare"), sql, None)
            .await
            .expect_err(sql);
        assert!(
            err.to_string().contains("requires apply_tag on ns.tbl.ssn"),
            "{sql}: {err}"
        );
        let root = session_for("root", vec!["service_admin"]);
        assert_passed_tag_gate(handler.execute(&root, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_removal_admin_and_same() {
    use sqe_core::config::{TagPrivilege, TagRemoval};
    use sqe_policy::TablePrivilege::{Alter, RemoveTag};
    let grants = || {
        Arc::new(ColumnGrants(vec![
            ("owner", "ssn", Alter),
            ("owner", "ssn", RemoveTag),
        ]))
    };
    let owner = session_for("owner", vec!["analyst"]);
    let root = session_for("root", vec!["service_admin"]);
    let admin = removal_handler(grants(), TagRemoval::Admin, TagPrivilege::Alter);
    let sql = "ALTER TABLE ns.tbl SET TAGS (ssn = ('PII'))";
    assert_passed_tag_gate(admin.execute(&owner, sql, None).await, sql);
    for sql in UNTAG_STMTS {
        let err = admin.execute(&owner, sql, None).await.expect_err(sql);
        assert!(
            err.to_string().contains("tag_removal = \"admin\""),
            "{sql}: {err}"
        );
        assert_passed_tag_gate(admin.execute(&root, sql, None).await, sql);
    }
    // The default leaves an untag to the tag privilege, as before.
    let same = removal_handler(grants(), TagRemoval::default(), TagPrivilege::Alter);
    for sql in UNTAG_STMTS {
        assert_passed_tag_gate(same.execute(&owner, sql, None).await, sql);
    }
}

#[tokio::test]
async fn tag_removal_gates_object_untags() {
    use sqe_core::config::{TagPrivilege, TagRemoval};
    use sqe_policy::TablePrivilege::{Alter, RemoveTag};
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .grant_table_privilege("ns", "tbl", "alice", Alter)
        .await;
    store.grant_table_privilege("ns", "tbl", "bob", Alter).await;
    store
        .grant_table_privilege("ns", "tbl", "bob", RemoveTag)
        .await;
    let handler = removal_handler(store.clone(), TagRemoval::RemoveTag, TagPrivilege::Alter);
    let [set, unset] = TABLE_TAG_STMTS;
    let alice = session_for("alice", vec!["analyst"]);
    let bob = session_for("bob", vec!["analyst"]);
    assert_passed_tag_gate(handler.execute(&alice, set, None).await, set);
    let err = handler.execute(&alice, unset, None).await.expect_err(unset);
    assert!(
        err.to_string().contains("requires remove_tag on ns.tbl"),
        "{err}"
    );
    assert_passed_tag_gate(handler.execute(&bob, unset, None).await, unset);
    // Polaris has no `remove_tag`: a catalog untag is an admin's.
    let [set, unset] = CATALOG_TAG_STMTS;
    assert_passed_tag_gate(handler.execute(&bob, set, None).await, set);
    assert_refused_by_tag_gate(handler.execute(&bob, unset, None).await, unset);
    let admin = removal_handler(store, TagRemoval::Admin, TagPrivilege::Alter);
    let [_, unset] = TABLE_TAG_STMTS;
    assert_refused_by_tag_gate(admin.execute(&bob, unset, None).await, unset);
}

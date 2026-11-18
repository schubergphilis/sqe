use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::catalog_scope::{
    catalog_not_found, catalog_read_only, scope_required, CatalogAccess, CatalogScope,
    TokenBinding, SCOPE_EXEMPT_CATALOGS,
};
use crate::snapshot_properties::SnapshotProperties;
use crate::SecretString;

/// Atomic credential bundle. The three fields rotate together: the access
/// token, its matching refresh token (when the IdP issued one), and the
/// expiry timestamp that pairs with the access token. Constructing a
/// `Credentials` value forces the caller to set all three at the same call
/// site, which is the bug class issue #89 closes: three independent
/// `session.access_token = ...; session.refresh_token = ...; session.token_expiry = ...`
/// assignment sequences let a fourth call site forget one of the three and
/// leave a stale expiry attached to a fresh token.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_token: SecretString,
    pub refresh_token: Option<SecretString>,
    pub expiry: DateTime<Utc>,
}

impl Credentials {
    pub fn new(
        access_token: SecretString,
        refresh_token: Option<SecretString>,
        expiry: DateTime<Utc>,
    ) -> Self {
        Self {
            access_token,
            refresh_token,
            expiry,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Session {
    pub id: String,
    pub user: SessionUser,
    // The credential trio is private (issue #89). Mutate via
    // `rotate_credentials`; read via `access_token()`, `refresh_token()`,
    // `token_expiry()`. Direct field access would let any caller leave
    // `token_expiry` stale relative to `access_token`.
    access_token: SecretString,
    refresh_token: Option<SecretString>,
    token_expiry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// Timestamp of last activity (query execution). Used for idle session expiry.
    pub last_activity: DateTime<Utc>,
    /// Default catalog for unqualified table references (from X-Trino-Catalog).
    pub default_catalog: Option<String>,
    /// Default schema for unqualified table references (from X-Trino-Schema).
    pub default_schema: Option<String>,
    /// Client source identifier (from X-Trino-Source).
    pub source: Option<String>,
    /// The address the caller connected from, when the front door knows it.
    /// MCP sets it so that in the modes where every caller is one configured
    /// user (`static`, `anonymous`) the client registry and the query rate
    /// limit still tell callers apart (#680).
    pub peer_ip: Option<String>,
    /// Named Iceberg branch for writes in this session. When set via
    /// `SET WRITE_BRANCH = '<name>'`, INSERT/UPDATE/DELETE/MERGE statements
    /// route their snapshot ref updates to this branch instead of `main`.
    pub write_branch: Option<String>,
    /// Iceberg snapshot summary properties that this session's autocommit
    /// writes stamp into their commit, set by `SET SNAPSHOT_PROPERTIES`
    /// outside a transaction. Inside a transaction the properties live on the
    /// transaction's scope instead; these do not apply there.
    pub snapshot_properties: Option<Arc<SnapshotProperties>>,
    /// Per-session Parquet compression codec, carried by the client in the
    /// `iceberg.compression_codec` Trino session property (`X-Trino-Session`).
    /// When set, write paths use it instead of `config.catalog.parquet_compression`.
    pub compression_codec: Option<String>,
    /// The explicit transaction this session is in, if any. Trino carries it
    /// in `X-Trino-Transaction-Id`; Flight SQL sets it on the stored session
    /// at `BEGIN`, or a Flight SQL request carries the id `BeginTransaction`
    /// returned. Inside a transaction DML writes to the transaction's branch
    /// (#821); DDL is refused, or runs autocommit in a client-held one.
    pub transaction_id: Option<String>,
    /// `transaction_id` came with the request (Flight SQL
    /// `BeginTransaction`), not from `BEGIN`. DDL inside such a transaction
    /// runs autocommit instead of being refused (#824). Private so only
    /// [`Self::with_client_transaction_id`] sets it.
    client_transaction: bool,
    /// Explicit, internal write-authority marker for the in-process
    /// maintenance principal (Phase 4b active auto-compaction).
    ///
    /// This is set to `true` ONLY by `MaintenancePrincipal::session_from_identity`
    /// (`sqe-coordinator/src/maintenance_principal.rs`), never by any
    /// auth/wire/config deserialization path. It grants IN-ENGINE write
    /// authorization for `MaintenanceHandler::authorize_or_deny`
    /// (`session_has_write_privilege`) so autonomous compaction is
    /// authorized explicitly rather than by accident of the "unknown role
    /// defaults to allow" heuristic. Polaris remains the real server-side
    /// enforcer; this field does not bypass it.
    maintenance_authority: bool,
    /// The `azp` / `jti` / `sub` of the bearer this session authenticated
    /// with, when the provider verified a JWT. A catalog scope (#908) binds
    /// to it. `None` for sessions without a JWT (password grant without
    /// claims, static users).
    token_binding: Option<Arc<TokenBinding>>,
    /// The verified catalog scope of this request (#908). Set on a
    /// per-request clone by the front door, never on the stored session.
    catalog_scope: Option<Arc<CatalogScope>>,
}

#[derive(Debug, Clone)]
pub struct SessionUser {
    pub username: String,
    pub roles: Vec<String>,
    /// JWT `sub` claim forwarded from the auth provider's `Identity`.
    /// Distinct from `username` which may use a custom `user_claim`.
    pub subject: Option<String>,
    /// Email address forwarded from the auth provider's `Identity`.
    pub email: Option<String>,
    /// Group memberships forwarded from the auth provider's `Identity`.
    /// Separate from `roles`: different claim path, different semantics.
    pub groups: Vec<String>,
}

impl SessionUser {
    /// Stable identity of the principal: the IdP subject when the provider
    /// forwarded one, else the display username. Ownership checks compare
    /// this, not the username, so a second provider in the chain that mints
    /// the same display name is a different owner (issue #622).
    pub fn principal_id(&self) -> &str {
        self.subject.as_deref().unwrap_or(&self.username)
    }
}

impl Session {
    pub fn new(
        username: String,
        access_token: SecretString,
        refresh_token: Option<SecretString>,
        token_expiry: DateTime<Utc>,
        roles: Vec<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            user: SessionUser {
                username,
                roles,
                subject: None,
                email: None,
                groups: Vec::new(),
            },
            access_token,
            refresh_token,
            token_expiry,
            created_at: now,
            last_activity: now,
            default_catalog: None,
            default_schema: None,
            source: None,
            peer_ip: None,
            write_branch: None,
            snapshot_properties: None,
            compression_codec: None,
            transaction_id: None,
            client_transaction: false,
            maintenance_authority: false,
            token_binding: None,
            catalog_scope: None,
        }
    }

    /// Returns a new session bound to the bearer claims a catalog scope is
    /// checked against (#908). An empty binding is stored as `None`.
    #[must_use = "with_token_binding consumes self; bind the returned Session"]
    pub fn with_token_binding(mut self, binding: Option<TokenBinding>) -> Self {
        self.token_binding = binding.filter(|b| !b.is_empty()).map(Arc::new);
        self
    }

    pub fn token_binding(&self) -> Option<&TokenBinding> {
        self.token_binding.as_deref()
    }

    /// Returns a new session limited to `scope` (#908). The front door
    /// calls this on a per-request clone after verifying the scope.
    #[must_use = "with_catalog_scope consumes self; bind the returned Session"]
    pub fn with_catalog_scope(mut self, scope: Option<Arc<CatalogScope>>) -> Self {
        self.catalog_scope = scope;
        self
    }

    pub fn catalog_scope(&self) -> Option<&CatalogScope> {
        self.catalog_scope.as_deref()
    }

    /// The shared handle to this session's scope, for catalogs that carry
    /// it as a backstop.
    pub fn catalog_scope_arc(&self) -> Option<Arc<CatalogScope>> {
        self.catalog_scope.clone()
    }

    /// The scope that governs this session, checked against its bearer
    /// (#908). Fails closed:
    /// - a scope whose `sub` / `jti` differ from the bearer's, or that has
    ///   expired, is refused;
    /// - a session of the scoped client without a scope is refused, on
    ///   every protocol, whether or not its door could carry one.
    ///
    /// `Ok(None)` is an unscoped session of any other client.
    pub fn effective_scope(&self) -> Result<Option<&CatalogScope>, crate::SqeError> {
        self.effective_scope_for(crate::catalog_scope::scoped_client().as_deref(), Utc::now())
    }

    /// [`Session::effective_scope`] with the scoped client and the clock
    /// passed in, for tests.
    pub fn effective_scope_for(
        &self,
        scoped_client: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<Option<&CatalogScope>, crate::SqeError> {
        let binding = self.token_binding.as_deref();
        match self.catalog_scope.as_deref() {
            Some(scope) => {
                let bound = binding.is_some_and(|b| {
                    b.subject.as_deref() == Some(scope.subject())
                        && b.token_id.as_deref() == Some(scope.token_id())
                });
                if !bound {
                    return Err(scope_required("the scope is for another token"));
                }
                if scope.is_expired(now) {
                    return Err(scope_required("the scope has expired"));
                }
                Ok(Some(scope))
            }
            None => {
                let is_scoped_client = scoped_client.is_some_and(|client| {
                    binding.and_then(|b| b.client_id.as_deref()) == Some(client)
                });
                if is_scoped_client {
                    Err(scope_required("this client must present a catalog scope"))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Whether `catalog` is visible to this session. Always true without a
    /// scope; with one, true for the scoped catalogs and SQE's own
    /// [`SCOPE_EXEMPT_CATALOGS`]. False when the scope check fails.
    pub fn catalog_visible(&self, catalog: &str) -> bool {
        match self.effective_scope() {
            Ok(None) => true,
            Ok(Some(scope)) => {
                scope.access(catalog).is_some()
                    || SCOPE_EXEMPT_CATALOGS
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(catalog))
            }
            Err(_) => false,
        }
    }

    /// `Ok` when this session may read `catalog`; "catalog not found" when
    /// a scope hides it. A no-op without a scope.
    pub fn check_catalog_read(&self, catalog: &str) -> Result<(), crate::SqeError> {
        self.effective_scope()?;
        if self.catalog_visible(catalog) {
            Ok(())
        } else {
            Err(catalog_not_found(catalog))
        }
    }

    /// `Ok` when this session may write into `catalog`: "catalog not found"
    /// outside the scope, refused for a `read_only` one. A no-op without a
    /// scope.
    pub fn check_catalog_write(&self, catalog: &str) -> Result<(), crate::SqeError> {
        let Some(scope) = self.effective_scope()? else {
            return Ok(());
        };
        match scope.access(catalog) {
            Some(CatalogAccess::ReadWrite) => Ok(()),
            Some(CatalogAccess::ReadOnly) => Err(catalog_read_only(catalog)),
            None => Err(catalog_not_found(catalog)),
        }
    }

    /// Borrow the current access token. Returns `&SecretString` so the
    /// caller has to go through `.expose()` to read the raw value.
    pub fn access_token(&self) -> &SecretString {
        &self.access_token
    }

    /// Borrow the current refresh token, if the IdP issued one.
    pub fn refresh_token(&self) -> Option<&SecretString> {
        self.refresh_token.as_ref()
    }

    /// Return the expiry instant of the current access token.
    pub fn token_expiry(&self) -> DateTime<Utc> {
        self.token_expiry
    }

    /// Atomically replace the credential trio. The three fields move
    /// together so a `Credentials` value always represents a consistent
    /// (token, refresh, expiry) tuple. No external caller can leave
    /// `token_expiry` mismatched against `access_token` because no
    /// individual setter exists.
    pub fn rotate_credentials(&mut self, creds: Credentials) {
        self.access_token = creds.access_token;
        self.refresh_token = creds.refresh_token;
        self.token_expiry = creds.expiry;
    }

    /// Set the write branch for this session. Passing `None` clears the value
    /// and sends subsequent writes to `main`.
    pub fn set_write_branch(&mut self, branch: Option<String>) {
        self.write_branch = branch;
    }

    /// Set the snapshot properties for this session's autocommit writes.
    /// Passing `None` clears them.
    pub fn set_snapshot_properties(&mut self, properties: Option<Arc<SnapshotProperties>>) {
        self.snapshot_properties = properties;
    }

    /// `USE [catalog.]schema`: set the scope unqualified table references
    /// resolve in. `catalog` `None` keeps the current default catalog.
    pub fn set_default_scope(&mut self, catalog: Option<String>, schema: String) {
        if catalog.is_some() {
            self.default_catalog = catalog;
        }
        self.default_schema = Some(schema);
    }

    /// Returns a new session with the given default catalog.
    #[must_use = "with_catalog consumes self; bind the returned Session"]
    pub fn with_catalog(mut self, catalog: Option<String>) -> Self {
        self.default_catalog = catalog;
        self
    }

    /// Returns a new session with the given default schema.
    #[must_use = "with_schema consumes self; bind the returned Session"]
    pub fn with_schema(mut self, schema: Option<String>) -> Self {
        self.default_schema = schema;
        self
    }

    /// Returns a new session with the given source.
    #[must_use = "with_source consumes self; bind the returned Session"]
    pub fn with_source(mut self, source: Option<String>) -> Self {
        self.source = source;
        self
    }

    pub fn with_peer_ip(mut self, peer_ip: Option<String>) -> Self {
        self.peer_ip = peer_ip;
        self
    }

    /// Returns a new session with the given per-session Parquet compression
    /// codec (from the `iceberg.compression_codec` Trino session property).
    #[must_use = "with_compression_codec consumes self; bind the returned Session"]
    pub fn with_compression_codec(mut self, codec: Option<String>) -> Self {
        self.compression_codec = codec;
        self
    }

    /// Returns a new session in the given explicit transaction (`None` is
    /// autocommit).
    #[must_use = "with_transaction_id consumes self; bind the returned Session"]
    pub fn with_transaction_id(mut self, transaction_id: Option<String>) -> Self {
        self.transaction_id = transaction_id;
        self.client_transaction = false;
        self
    }

    /// Returns a new session in the transaction a client holds the id of
    /// (Flight SQL `BeginTransaction`, #824). Unlike [`Self::with_transaction_id`]
    /// DDL in it runs autocommit: a DB-API driver opens such a transaction on
    /// every connection, and refusing `CREATE TABLE` there would break every
    /// default connection.
    #[must_use = "with_client_transaction_id consumes self; bind the returned Session"]
    pub fn with_client_transaction_id(mut self, transaction_id: String) -> Self {
        self.transaction_id = Some(transaction_id);
        self.client_transaction = true;
        self
    }

    /// Whether the transaction id came with the request rather than from
    /// `BEGIN`; see [`Self::with_client_transaction_id`].
    pub fn in_client_transaction(&self) -> bool {
        self.client_transaction && self.transaction_id.is_some()
    }

    /// Enrich the session with identity fields from the auth provider.
    ///
    /// Called once after `Session::new` on the auth path so existing call
    /// sites that do not pass enriched identity data keep compiling unchanged.
    #[must_use = "with_identity consumes self; bind the returned Session"]
    pub fn with_identity(
        mut self,
        subject: Option<String>,
        email: Option<String>,
        groups: Vec<String>,
    ) -> Self {
        self.user.subject = subject;
        self.user.email = email;
        self.user.groups = groups;
        self
    }

    /// Returns a new session with the explicit maintenance write-authority
    /// marker set. Intended to be called exactly once, by
    /// `MaintenancePrincipal::session_from_identity`, when minting the
    /// ephemeral session the compaction scheduler runs under. No other call
    /// site should invoke this: the marker is never derived from a token,
    /// wire request, or config value, so nothing an external caller sends can
    /// set it.
    #[must_use = "with_maintenance_authority consumes self; bind the returned Session"]
    pub fn with_maintenance_authority(mut self, yes: bool) -> Self {
        self.maintenance_authority = yes;
        self
    }

    /// Returns `true` if this session carries the explicit maintenance
    /// write-authority marker set by `with_maintenance_authority`. Consulted
    /// by `sqe_coordinator::maintenance::session_has_write_privilege` to
    /// grant write access to the maintenance principal's session independent
    /// of the role-name heuristic. Polaris still enforces authorization
    /// server-side; this only affects in-engine gating.
    pub fn has_maintenance_authority(&self) -> bool {
        self.maintenance_authority
    }

    pub fn token_fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let hash = hex::encode(Sha256::digest(self.access_token.expose_bytes()));
        format!("{}-{}", self.user.username, &hash[..16])
    }

    /// Returns `true` if the access token will expire within `buffer`.
    ///
    /// Use `std::time::Duration::from_secs(n)` at the call site so the
    /// unit can't be misread (issue #116).
    pub fn is_token_expiring(&self, buffer: std::time::Duration) -> bool {
        let buf = chrono::Duration::from_std(buffer).unwrap_or(chrono::Duration::zero());
        Utc::now() + buf >= self.token_expiry
    }

    /// Update the last activity timestamp to the current time.
    pub fn touch(&mut self) {
        self.last_activity = Utc::now();
    }

    /// Returns `true` if the session has been idle for longer than `timeout`.
    pub fn is_idle(&self, timeout: std::time::Duration) -> bool {
        let tmo = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::zero());
        Utc::now() - self.last_activity > tmo
    }

    /// Returns `true` if the session has exceeded its absolute lifetime.
    pub fn is_absolute_expired(&self, timeout: std::time::Duration) -> bool {
        let tmo = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::zero());
        Utc::now() - self.created_at > tmo
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_session() -> Session {
        Session::new(
            "alice".to_string(),
            SecretString::new("tok_abc".to_string()),
            None,
            Utc::now() + Duration::hours(1),
            vec!["analyst".to_string()],
        )
    }

    fn bound(client: &str, sub: &str, jti: &str) -> Session {
        make_session().with_token_binding(Some(crate::catalog_scope::TokenBinding {
            client_id: Some(client.to_string()),
            token_id: Some(jti.to_string()),
            subject: Some(sub.to_string()),
        }))
    }

    fn scope_for(sub: &str, jti: &str, expires: DateTime<Utc>) -> Option<Arc<CatalogScope>> {
        Some(Arc::new(CatalogScope::new(
            sub,
            jti,
            expires,
            [(
                "sales".to_string(),
                crate::catalog_scope::CatalogAccess::ReadOnly,
            )],
        )))
    }

    #[test]
    fn effective_scope_accepts_a_bound_live_scope() {
        let now = Utc::now();
        let s = bound("portal", "alice", "j1").with_catalog_scope(scope_for(
            "alice",
            "j1",
            now + Duration::minutes(5),
        ));
        let scope = s.effective_scope_for(Some("portal"), now).unwrap();
        assert!(scope.is_some_and(|sc| sc.access("SALES").is_some()));
    }

    #[test]
    fn effective_scope_refuses_another_tokens_scope() {
        let now = Utc::now();
        let exp = now + Duration::minutes(5);
        let other_jti =
            bound("portal", "alice", "j1").with_catalog_scope(scope_for("alice", "j2", exp));
        assert!(other_jti.effective_scope_for(Some("portal"), now).is_err());
        let other_sub =
            bound("portal", "alice", "j1").with_catalog_scope(scope_for("bob", "j1", exp));
        assert!(other_sub.effective_scope_for(Some("portal"), now).is_err());
        let unbound = make_session().with_catalog_scope(scope_for("alice", "j1", exp));
        assert!(unbound.effective_scope_for(None, now).is_err());
    }

    #[test]
    fn effective_scope_refuses_an_expired_scope() {
        let now = Utc::now();
        let s = bound("portal", "alice", "j1").with_catalog_scope(scope_for("alice", "j1", now));
        assert!(s.effective_scope_for(Some("portal"), now).is_err());
    }

    #[test]
    fn effective_scope_refuses_the_scoped_client_without_a_scope() {
        let s = bound("portal", "alice", "j1");
        assert!(s.effective_scope_for(Some("portal"), Utc::now()).is_err());
    }

    #[test]
    fn effective_scope_leaves_other_clients_alone() {
        let now = Utc::now();
        assert!(matches!(
            bound("dbt", "alice", "j1").effective_scope_for(Some("portal"), now),
            Ok(None)
        ));
        assert!(matches!(
            make_session().effective_scope_for(Some("portal"), now),
            Ok(None)
        ));
        assert!(matches!(
            bound("portal", "alice", "j1").effective_scope_for(None, now),
            Ok(None)
        ));
    }

    #[test]
    fn test_new_session_initialises_last_activity() {
        let before = Utc::now();
        let session = make_session();
        let after = Utc::now();
        assert!(session.last_activity >= before);
        assert!(session.last_activity <= after);
        assert!(session.created_at >= before);
        assert!(session.created_at <= after);
    }

    #[test]
    fn test_is_idle_returns_false_immediately() {
        let session = make_session();
        // 15-minute idle timeout: a fresh session should not be idle
        assert!(!session.is_idle(std::time::Duration::from_secs(900)));
    }

    #[test]
    fn test_is_idle_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago
        session.last_activity = Utc::now() - Duration::seconds(1200);
        // 15-minute (900s) idle timeout - session should be idle
        assert!(session.is_idle(std::time::Duration::from_secs(900)));
    }

    #[test]
    fn test_is_absolute_expired_returns_false_immediately() {
        let session = make_session();
        // 8-hour absolute timeout: a fresh session should not be expired
        assert!(!session.is_absolute_expired(std::time::Duration::from_secs(28800)));
    }

    #[test]
    fn test_is_absolute_expired_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate session created 9 hours ago
        session.created_at = Utc::now() - Duration::hours(9);
        // 8-hour (28800s) absolute timeout: session should be expired
        assert!(session.is_absolute_expired(std::time::Duration::from_secs(28800)));
    }

    #[test]
    fn test_touch_resets_idle_timer() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago, idle
        session.last_activity = Utc::now() - Duration::seconds(1200);
        assert!(session.is_idle(std::time::Duration::from_secs(900)));

        // Touch should reset last_activity to now
        session.touch();
        assert!(!session.is_idle(std::time::Duration::from_secs(900)));
    }

    #[test]
    fn test_touch_does_not_affect_absolute_expiry() {
        let mut session = make_session();
        // Simulate session created 9 hours ago
        session.created_at = Utc::now() - Duration::hours(9);
        assert!(session.is_absolute_expired(std::time::Duration::from_secs(28800)));

        // Touch should not reset created_at
        session.touch();
        assert!(session.is_absolute_expired(std::time::Duration::from_secs(28800)));
    }

    #[test]
    fn test_active_session_survives_idle_timeout() {
        let mut session = make_session();
        // Simulate last activity 10 minutes ago
        session.last_activity = Utc::now() - Duration::seconds(600);
        // 15-minute (900s) idle timeout: session is still active
        assert!(!session.is_idle(std::time::Duration::from_secs(900)));
    }

    #[test]
    fn test_debug_includes_last_activity() {
        let session = make_session();
        let debug_str = format!("{:?}", session);
        assert!(
            debug_str.contains("last_activity"),
            "Debug output should include last_activity, got: {debug_str}"
        );
    }

    #[test]
    fn only_a_request_carried_transaction_is_client_held() {
        let session = make_session();
        assert!(!session.in_client_transaction());
        let held = session.clone().with_client_transaction_id("t1".to_string());
        assert!(held.in_client_transaction());
        assert_eq!(held.transaction_id.as_deref(), Some("t1"));
        let begun = held.with_transaction_id(Some("t2".to_string()));
        assert!(
            !begun.in_client_transaction(),
            "BEGIN's id is not client-held"
        );
        assert!(!session.with_transaction_id(None).in_client_transaction());
    }

    #[test]
    fn rotate_credentials_replaces_all_three_fields_atomically() {
        let mut session = make_session();
        let original_expiry = session.token_expiry();
        let new_expiry = Utc::now() + Duration::hours(2);

        session.rotate_credentials(Credentials::new(
            SecretString::new("new-tok".to_string()),
            Some(SecretString::new("new-refresh".to_string())),
            new_expiry,
        ));

        assert_eq!(session.access_token().expose(), "new-tok");
        assert_eq!(
            session.refresh_token().map(|t| t.expose()),
            Some("new-refresh"),
        );
        assert_eq!(session.token_expiry(), new_expiry);
        assert_ne!(session.token_expiry(), original_expiry);
    }

    #[test]
    fn session_carries_enriched_identity() {
        let s = Session::new(
            "alice".into(),
            SecretString::new("t".into()),
            None,
            Utc::now(),
            vec!["analyst".into()],
        )
        .with_identity(
            Some("u-1".into()),
            Some("alice@x.io".into()),
            vec!["hr".into()],
        );
        assert_eq!(s.user.subject.as_deref(), Some("u-1"));
        assert_eq!(s.user.email.as_deref(), Some("alice@x.io"));
        assert_eq!(s.user.groups, vec!["hr".to_string()]);
    }

    #[test]
    fn snapshot_properties_default_to_none_and_can_be_set_and_cleared() {
        let mut session = make_session();
        assert!(session.snapshot_properties.is_none());

        let props = Arc::new(
            SnapshotProperties::try_new([("copy.run".to_string(), "42".to_string())]).unwrap(),
        );
        session.set_snapshot_properties(Some(props.clone()));
        assert_eq!(session.snapshot_properties, Some(props));

        session.set_snapshot_properties(None);
        assert!(session.snapshot_properties.is_none());
    }

    #[test]
    fn maintenance_authority_defaults_to_false() {
        let session = make_session();
        assert!(!session.has_maintenance_authority());
    }

    #[test]
    fn with_maintenance_authority_true_is_reported_by_accessor() {
        let session = make_session().with_maintenance_authority(true);
        assert!(session.has_maintenance_authority());
    }

    #[test]
    fn with_maintenance_authority_false_is_reported_by_accessor() {
        let session = make_session()
            .with_maintenance_authority(true)
            .with_maintenance_authority(false);
        assert!(!session.has_maintenance_authority());
    }

    #[test]
    fn debug_does_not_leak_access_or_refresh_token() {
        let mut session = make_session();
        session.rotate_credentials(Credentials::new(
            SecretString::new("a-very-secret-token".to_string()),
            Some(SecretString::new("a-very-secret-refresh".to_string())),
            Utc::now() + Duration::hours(1),
        ));
        let d = format!("{:?}", session);
        assert!(!d.contains("a-very-secret-token"), "leaked access: {d}");
        assert!(!d.contains("a-very-secret-refresh"), "leaked refresh: {d}");
    }
}

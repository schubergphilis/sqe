use chrono::{DateTime, Utc};
use uuid::Uuid;

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
    /// Named Iceberg branch for writes in this session. When set via
    /// `SET WRITE_BRANCH = '<name>'`, INSERT/UPDATE/DELETE/MERGE statements
    /// route their snapshot ref updates to this branch instead of `main`.
    pub write_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SessionUser {
    pub username: String,
    pub roles: Vec<String>,
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
            user: SessionUser { username, roles },
            access_token,
            refresh_token,
            token_expiry,
            created_at: now,
            last_activity: now,
            default_catalog: None,
            default_schema: None,
            source: None,
            write_branch: None,
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

    pub fn token_fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(self.access_token.expose_bytes()));
        format!("{}-{}", self.user.username, &hash[..16])
    }

    pub fn is_token_expiring(&self, buffer_secs: u64) -> bool {
        let buffer = chrono::Duration::seconds(buffer_secs as i64);
        Utc::now() + buffer >= self.token_expiry
    }

    /// Update the last activity timestamp to the current time.
    pub fn touch(&mut self) {
        self.last_activity = Utc::now();
    }

    /// Returns `true` if the session has been idle for longer than `idle_timeout_secs`.
    pub fn is_idle(&self, idle_timeout_secs: u64) -> bool {
        let timeout = chrono::Duration::seconds(idle_timeout_secs as i64);
        Utc::now() - self.last_activity > timeout
    }

    /// Returns `true` if the session has exceeded its absolute lifetime.
    pub fn is_absolute_expired(&self, absolute_timeout_secs: u64) -> bool {
        let timeout = chrono::Duration::seconds(absolute_timeout_secs as i64);
        Utc::now() - self.created_at > timeout
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
        assert!(!session.is_idle(900));
    }

    #[test]
    fn test_is_idle_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago
        session.last_activity = Utc::now() - Duration::seconds(1200);
        // 15-minute (900s) idle timeout - session should be idle
        assert!(session.is_idle(900));
    }

    #[test]
    fn test_is_absolute_expired_returns_false_immediately() {
        let session = make_session();
        // 8-hour absolute timeout: a fresh session should not be expired
        assert!(!session.is_absolute_expired(28800));
    }

    #[test]
    fn test_is_absolute_expired_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate session created 9 hours ago
        session.created_at = Utc::now() - Duration::hours(9);
        // 8-hour (28800s) absolute timeout: session should be expired
        assert!(session.is_absolute_expired(28800));
    }

    #[test]
    fn test_touch_resets_idle_timer() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago, idle
        session.last_activity = Utc::now() - Duration::seconds(1200);
        assert!(session.is_idle(900));

        // Touch should reset last_activity to now
        session.touch();
        assert!(!session.is_idle(900));
    }

    #[test]
    fn test_touch_does_not_affect_absolute_expiry() {
        let mut session = make_session();
        // Simulate session created 9 hours ago
        session.created_at = Utc::now() - Duration::hours(9);
        assert!(session.is_absolute_expired(28800));

        // Touch should not reset created_at
        session.touch();
        assert!(session.is_absolute_expired(28800));
    }

    #[test]
    fn test_active_session_survives_idle_timeout() {
        let mut session = make_session();
        // Simulate last activity 10 minutes ago
        session.last_activity = Utc::now() - Duration::seconds(600);
        // 15-minute (900s) idle timeout: session is still active
        assert!(!session.is_idle(900));
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

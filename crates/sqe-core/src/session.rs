use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone)]
pub struct Session {
    pub id: String,
    pub user: SessionUser,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_expiry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// Timestamp of last activity (query execution). Used for idle session expiry.
    pub last_activity: DateTime<Utc>,
    /// Default catalog for unqualified table references (from X-Trino-Catalog).
    pub default_catalog: Option<String>,
    /// Default schema for unqualified table references (from X-Trino-Schema).
    pub default_schema: Option<String>,
    /// Client source identifier (from X-Trino-Source).
    pub source: Option<String>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("user", &self.user)
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("token_expiry", &self.token_expiry)
            .field("created_at", &self.created_at)
            .field("last_activity", &self.last_activity)
            .field("default_catalog", &self.default_catalog)
            .field("default_schema", &self.default_schema)
            .field("source", &self.source)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct SessionUser {
    pub username: String,
    pub roles: Vec<String>,
}

impl Session {
    pub fn new(username: String, access_token: String, refresh_token: Option<String>, token_expiry: DateTime<Utc>, roles: Vec<String>) -> Self {
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
        }
    }

    /// Returns a new session with the given default catalog.
    pub fn with_catalog(mut self, catalog: Option<String>) -> Self {
        self.default_catalog = catalog;
        self
    }

    /// Returns a new session with the given default schema.
    pub fn with_schema(mut self, schema: Option<String>) -> Self {
        self.default_schema = schema;
        self
    }

    /// Returns a new session with the given source.
    pub fn with_source(mut self, source: Option<String>) -> Self {
        self.source = source;
        self
    }

    pub fn token_fingerprint(&self) -> String {
        let token = &self.access_token;
        let tail = &token[token.len().saturating_sub(8)..];
        format!("{}-{}", self.user.username, tail)
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
            "tok_abc".to_string(),
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
        // 15-minute idle timeout — a fresh session should not be idle
        assert!(!session.is_idle(900));
    }

    #[test]
    fn test_is_idle_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago
        session.last_activity = Utc::now() - Duration::seconds(1200);
        // 15-minute (900s) idle timeout — session should be idle
        assert!(session.is_idle(900));
    }

    #[test]
    fn test_is_absolute_expired_returns_false_immediately() {
        let session = make_session();
        // 8-hour absolute timeout — a fresh session should not be expired
        assert!(!session.is_absolute_expired(28800));
    }

    #[test]
    fn test_is_absolute_expired_returns_true_after_timeout() {
        let mut session = make_session();
        // Simulate session created 9 hours ago
        session.created_at = Utc::now() - Duration::hours(9);
        // 8-hour (28800s) absolute timeout — session should be expired
        assert!(session.is_absolute_expired(28800));
    }

    #[test]
    fn test_touch_resets_idle_timer() {
        let mut session = make_session();
        // Simulate last activity 20 minutes ago — idle
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
        // 15-minute (900s) idle timeout — session is still active
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
}

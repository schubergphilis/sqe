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
        Self {
            id: Uuid::new_v4().to_string(),
            user: SessionUser { username, roles },
            access_token,
            refresh_token,
            token_expiry,
            created_at: Utc::now(),
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
}

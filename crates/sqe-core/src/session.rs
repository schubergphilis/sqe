use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub user: SessionUser,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_expiry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
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
        }
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

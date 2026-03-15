use chrono::{DateTime, Utc};
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct CachedToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expiry: DateTime<Utc>,
}

pub struct TokenCache {
    tokens: DashMap<String, CachedToken>,
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
        }
    }

    pub fn get(&self, session_id: &str) -> Option<CachedToken> {
        self.tokens.get(session_id).map(|entry| entry.clone())
    }

    pub fn insert(&self, session_id: &str, token: CachedToken) {
        self.tokens.insert(session_id.to_string(), token);
    }

    pub fn remove(&self, session_id: &str) {
        self.tokens.remove(session_id);
    }

    /// Returns session IDs whose tokens will expire within `buffer_secs` seconds.
    pub fn expiring_sessions(&self, buffer_secs: u64) -> Vec<String> {
        let threshold = Utc::now() + chrono::Duration::seconds(buffer_secs as i64);
        self.tokens
            .iter()
            .filter(|entry| entry.value().expiry <= threshold)
            .map(|entry| entry.key().clone())
            .collect()
    }
}

impl Default for TokenCache {
    fn default() -> Self {
        Self::new()
    }
}

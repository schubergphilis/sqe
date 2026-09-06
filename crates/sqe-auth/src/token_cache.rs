use std::fmt;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use sqe_core::SecretString;

/// A cached access/refresh token pair for one session.
///
/// The token material is held in [`SecretString`] so a `debug!(?cached)` or a
/// panic message cannot print the bearer, and so the bytes are zeroized on
/// drop. Reach for `expose()` only where the value must cross into an HTTP
/// header. Finding 208.
#[derive(Clone)]
pub struct CachedToken {
    pub access_token: SecretString,
    pub refresh_token: Option<SecretString>,
    pub expiry: DateTime<Utc>,
}

impl fmt::Debug for CachedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedToken")
            .field("access_token", &self.access_token)
            .field("refresh_token", &self.refresh_token)
            .field("expiry", &self.expiry)
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_token(access: &str, refresh: Option<&str>, expiry: DateTime<Utc>) -> CachedToken {
        CachedToken {
            access_token: SecretString::new(access.to_string()),
            refresh_token: refresh.map(|r| SecretString::new(r.to_string())),
            expiry,
        }
    }

    #[test]
    fn insert_and_get() {
        let cache = TokenCache::new();
        let expiry = Utc::now() + Duration::hours(1);
        cache.insert("s1", make_token("tok1", Some("ref1"), expiry));

        let cached = cache.get("s1").expect("should find cached token");
        assert_eq!(cached.access_token.expose(), "tok1");
        assert_eq!(
            cached.refresh_token.as_ref().map(|t| t.expose()),
            Some("ref1")
        );
    }

    #[test]
    fn get_missing_returns_none() {
        let cache = TokenCache::new();
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn remove_deletes_entry() {
        let cache = TokenCache::new();
        let expiry = Utc::now() + Duration::hours(1);
        cache.insert("s1", make_token("tok1", None, expiry));

        cache.remove("s1");
        assert!(cache.get("s1").is_none());
    }

    #[test]
    fn insert_overwrites_existing() {
        let cache = TokenCache::new();
        let expiry = Utc::now() + Duration::hours(1);
        cache.insert("s1", make_token("tok1", None, expiry));
        cache.insert("s1", make_token("tok2", None, expiry));

        let cached = cache.get("s1").unwrap();
        assert_eq!(cached.access_token.expose(), "tok2");
    }

    /// Finding 208: formatting a `CachedToken` must never print the bearer.
    /// Mirrors `identity_debug_does_not_leak_token` on the provider side.
    #[test]
    fn debug_does_not_leak_token_material() {
        let token = make_token(
            "eyJhbGciOiJSUzI1NiJ9.super-secret-access",
            Some("super-secret-refresh"),
            Utc::now() + Duration::hours(1),
        );

        let rendered = format!("{token:?}");

        assert!(
            !rendered.contains("super-secret-access"),
            "access token leaked into Debug output: {rendered}"
        );
        assert!(
            !rendered.contains("super-secret-refresh"),
            "refresh token leaked into Debug output: {rendered}"
        );
        assert!(
            !rendered.contains("eyJ"),
            "JWT prefix leaked into Debug output: {rendered}"
        );
        assert!(
            rendered.contains("<set>"),
            "expected the SecretString presence sentinel: {rendered}"
        );
    }

    #[test]
    fn expiring_sessions_finds_soon_expiring() {
        let cache = TokenCache::new();
        // Token expiring in 30 seconds
        let soon = Utc::now() + Duration::seconds(30);
        // Token expiring in 2 hours
        let later = Utc::now() + Duration::hours(2);

        cache.insert("expiring", make_token("t1", None, soon));
        cache.insert("fresh", make_token("t2", None, later));

        // With 60s buffer, the "expiring" session (30s left) should be found
        let result = cache.expiring_sessions(60);
        assert!(result.contains(&"expiring".to_string()));
        assert!(!result.contains(&"fresh".to_string()));
    }

    #[test]
    fn expiring_sessions_includes_already_expired() {
        let cache = TokenCache::new();
        let past = Utc::now() - Duration::seconds(10);
        cache.insert("expired", make_token("t1", None, past));

        let result = cache.expiring_sessions(0);
        assert!(result.contains(&"expired".to_string()));
    }

    #[test]
    fn expiring_sessions_empty_cache() {
        let cache = TokenCache::new();
        assert!(cache.expiring_sessions(60).is_empty());
    }

    #[test]
    fn default_creates_empty_cache() {
        let cache = TokenCache::default();
        assert!(cache.get("anything").is_none());
        assert!(cache.expiring_sessions(0).is_empty());
    }
}

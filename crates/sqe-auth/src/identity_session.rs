//! The one conversion from an authenticated [`Identity`] to a core
//! [`Session`].
//!
//! Every wire protocol (Flight SQL, Trino HTTP, MCP, Quack, the maintenance
//! principal) used to carry its own copy of this mapping, and the copies
//! drifted: the Quack copy dropped `subject`, `email` and `groups`, so a
//! group-only Ranger row filter that applied over Flight SQL did not apply
//! to the same user over Quack (review finding 204). Keeping the mapping
//! here, next to `Identity`, gives every entry point the same session shape
//! and gives the compiler something to complain about when a new entry point
//! forgets a field.

use chrono::{DateTime, Duration, Utc};
use sqe_core::{SecretString, Session};

use crate::provider::Identity;

/// Fallback lifetime for providers without a token lifetime concept
/// (anonymous, mTLS). Issue #26.
pub const DEFAULT_SESSION_LIFETIME: Duration = Duration::hours(1);

impl Identity {
    /// Absolute expiry the session should carry: the provider's `expires_at`
    /// when it has one, otherwise [`DEFAULT_SESSION_LIFETIME`] from now.
    pub fn session_expiry(&self) -> DateTime<Utc> {
        self.expires_at
            .unwrap_or_else(|| Utc::now() + DEFAULT_SESSION_LIFETIME)
    }

    /// Convert this identity into a [`Session`], consuming it.
    ///
    /// `fallback_token` is used as the catalog token when the provider
    /// supplied none (for example the raw bearer JWT the client presented,
    /// which is forwarded to Polaris as-is). Subject, email, groups, roles,
    /// refresh token and expiry all carry over.
    pub fn into_session(self, fallback_token: Option<&str>) -> Session {
        let token_expiry = self.session_expiry();
        let catalog_token = self
            .catalog_token
            .unwrap_or_else(|| SecretString::new(fallback_token.unwrap_or_default().to_string()));
        Session::new(
            self.user_id,
            catalog_token,
            self.refresh_token,
            token_expiry,
            self.roles,
        )
        .with_identity(self.subject, self.email, self.groups)
    }

    /// Borrowing form of [`Identity::into_session`].
    pub fn to_session(&self, fallback_token: Option<&str>) -> Session {
        self.clone().into_session(fallback_token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> Identity {
        Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec!["analyst".to_string()],
            subject: Some("sub-123".to_string()),
            email: Some("alice@example.com".to_string()),
            groups: vec!["eu-analysts".to_string(), "finance".to_string()],
            catalog_token: Some(SecretString::new("catalog-tok".to_string())),
            refresh_token: Some(SecretString::new("refresh-tok".to_string())),
            expires_at: Some(Utc::now() + Duration::minutes(7)),
        }
    }

    /// 204: every identity field the policy layer keys on must survive the
    /// conversion. Groups in particular: Ranger items match on
    /// `users || roles || groups`, so a dropped group means a missing filter.
    #[test]
    fn all_identity_fields_survive() {
        let id = identity();
        let expires = id.expires_at.unwrap();
        let session = id.into_session(None);

        assert_eq!(session.user.username, "alice");
        assert_eq!(session.user.roles, vec!["analyst"]);
        assert_eq!(session.user.subject.as_deref(), Some("sub-123"));
        assert_eq!(session.user.email.as_deref(), Some("alice@example.com"));
        assert_eq!(session.user.groups, vec!["eu-analysts", "finance"]);
        assert_eq!(session.access_token().expose(), "catalog-tok");
        assert_eq!(session.refresh_token().unwrap().expose(), "refresh-tok");
        assert_eq!(session.token_expiry(), expires);
    }

    #[test]
    fn fallback_token_used_only_when_provider_supplied_none() {
        let mut id = identity();
        let with_provider_token = id.to_session(Some("raw-jwt"));
        assert_eq!(with_provider_token.access_token().expose(), "catalog-tok");

        id.catalog_token = None;
        let with_fallback = id.to_session(Some("raw-jwt"));
        assert_eq!(with_fallback.access_token().expose(), "raw-jwt");

        let with_nothing = id.into_session(None);
        assert_eq!(with_nothing.access_token().expose(), "");
    }

    #[test]
    fn missing_expiry_falls_back_to_default_lifetime() {
        let mut id = identity();
        id.expires_at = None;
        let before = Utc::now();
        let session = id.into_session(None);
        let lower = before + DEFAULT_SESSION_LIFETIME - Duration::seconds(5);
        let upper = Utc::now() + DEFAULT_SESSION_LIFETIME + Duration::seconds(5);
        assert!(session.token_expiry() > lower && session.token_expiry() < upper);
    }
}

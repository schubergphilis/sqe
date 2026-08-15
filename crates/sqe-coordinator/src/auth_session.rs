//! Shared conversion from an authenticated `Identity` to a `Session`.
//!
//! Used by the Trino-compat auth adapters in both coordinator binaries so the
//! Basic-auth and bearer paths build sessions identically (and identically to
//! `SessionManager`'s Flight SQL path).

use sqe_auth::Identity;
use sqe_core::{SecretString, Session};

/// Convert an authenticated `Identity` into a `Session`.
///
/// Prefers the provider-supplied `expires_at` (so the session evicts when the
/// access token actually expires), falling back to 1 hour. When the provider
/// supplied no `catalog_token`, `fallback_token` is used (e.g. the raw bearer
/// JWT, which is forwarded to Polaris). Subject/email/groups are carried through.
pub fn identity_to_session(identity: Identity, fallback_token: Option<&str>) -> Session {
    let token_expiry = identity
        .expires_at
        .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(1));
    let catalog_token = identity
        .catalog_token
        .clone()
        .unwrap_or_else(|| SecretString::new(fallback_token.unwrap_or_default().to_string()));
    Session::new(
        identity.user_id,
        catalog_token,
        identity.refresh_token,
        token_expiry,
        identity.roles,
    )
    .with_identity(identity.subject, identity.email, identity.groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(token: Option<&str>, expires: Option<chrono::DateTime<chrono::Utc>>) -> Identity {
        Identity {
            user_id: "sp-reader".to_string(),
            display_name: "sp-reader".to_string(),
            roles: vec!["service".to_string()],
            subject: Some("sub-123".to_string()),
            email: None,
            groups: vec!["g1".to_string()],
            catalog_token: token.map(|t| SecretString::new(t.to_string())),
            refresh_token: None,
            expires_at: expires,
        }
    }

    #[test]
    fn maps_core_fields() {
        let exp = chrono::Utc::now() + chrono::Duration::minutes(30);
        let s = identity_to_session(identity(Some("cat-token"), Some(exp)), None);
        assert_eq!(s.user.username, "sp-reader");
        assert_eq!(s.user.roles, vec!["service".to_string()]);
        assert_eq!(s.access_token().expose(), "cat-token");
        assert_eq!(s.token_expiry(), exp);
    }

    #[test]
    fn uses_fallback_token_when_provider_has_none() {
        let s = identity_to_session(identity(None, None), Some("raw-jwt"));
        assert_eq!(s.access_token().expose(), "raw-jwt");
    }

    #[test]
    fn defaults_expiry_when_absent() {
        let before = chrono::Utc::now();
        let s = identity_to_session(identity(Some("t"), None), None);
        assert!(s.token_expiry() > before + chrono::Duration::minutes(50));
    }
}

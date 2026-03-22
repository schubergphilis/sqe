pub mod oidc_password;
pub mod oauth;
pub mod token_cache;
pub mod authenticator;

/// Deprecated: use `oidc_password` instead.
#[deprecated(note = "renamed to oidc_password")]
pub mod keycloak {
    pub use crate::oidc_password::*;
}

pub use authenticator::Authenticator;

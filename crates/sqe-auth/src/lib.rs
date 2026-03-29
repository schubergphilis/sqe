pub mod oidc_password;
pub mod oidc_provider;
pub mod oauth;
pub mod anonymous;
pub mod aws_iam;
pub mod bearer_token;
pub mod token_exchange;
pub mod token_cache;
pub mod authenticator;
pub mod provider;
pub mod chain;
pub mod factory;

/// Deprecated: use `oidc_password` instead.
#[deprecated(note = "renamed to oidc_password")]
pub mod keycloak {
    pub use crate::oidc_password::*;
}

pub use authenticator::Authenticator;
pub use provider::{AuthProvider, AuthError, FlightCredentials, Identity};
pub use chain::AuthChain;
pub use oidc_provider::{OidcPasswordProvider, OidcPasswordProviderConfig};
pub use anonymous::{AnonymousProvider, AnonymousProviderConfig};
pub use aws_iam::{AwsIamProvider, AwsIamProviderConfig, KeyMapping as AwsKeyMapping};
pub use bearer_token::{BearerTokenProvider, BearerTokenProviderConfig};
pub use token_exchange::{TokenExchangeProvider, TokenExchangeConfig};
pub use factory::build_auth_chain;

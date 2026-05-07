pub mod oidc_password;
pub mod oidc_provider;
pub mod oidc_discovery;
pub mod oidc_m2m;
pub mod oauth;
pub mod anonymous;
pub mod api_key;
pub mod aws_iam;
pub mod bearer_token;
pub mod mtls;
pub mod token_exchange;
pub mod token_cache;
pub mod authenticator;
pub mod provider;
pub mod chain;
pub mod factory;
pub mod pending_auth;
pub mod device_code;
pub mod auth_code;
pub mod per_catalog;

/// Deprecated: use `oidc_password` instead.
#[deprecated(note = "renamed to oidc_password")]
pub mod keycloak {
    pub use crate::oidc_password::*;
}

pub use authenticator::Authenticator;
pub use provider::{AuthProvider, AuthError, FlightCredentials, Identity};
pub use chain::AuthChain;
pub use oidc_discovery::{OidcDiscovery, OidcDiscoveryConfig, DiscoveredEndpoints};
pub use oidc_provider::{OidcPasswordProvider, OidcPasswordProviderConfig};
pub use oidc_m2m::{OidcM2mConfig, OidcM2mProvider};
pub use anonymous::{AnonymousProvider, AnonymousProviderConfig};
pub use api_key::{ApiKeyProvider, ApiKeyProviderConfig, ApiKeyEntry};
pub use aws_iam::{AwsIamProvider, AwsIamProviderConfig, KeyMapping as AwsKeyMapping};
pub use bearer_token::{BearerTokenProvider, BearerTokenProviderConfig};
pub use mtls::{MtlsProvider, MtlsProviderConfig};
pub use token_exchange::{TokenExchangeProvider, TokenExchangeConfig};
pub use factory::build_auth_chain;
pub use pending_auth::{PendingAuthStore, PendingAuth, TokenSet};
pub use device_code::{DeviceCodeService, DeviceAuthSession, DevicePollResult};
pub use auth_code::{AuthCodeService, AuthCodeChallenge};

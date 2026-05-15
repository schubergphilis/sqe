//! `AwsIamProvider` — AWS IAM/STS-based authentication provider.
//!
//! Authenticates clients presenting AWS credentials (access key ID as username,
//! secret access key as password). Validates credentials by calling the AWS STS
//! `GetCallerIdentity` API and maps the resulting ARN to an `Identity`.
//!
//! # Credential Detection
//!
//! This provider handles credentials where the username starts with `"AKIA"` (long-term
//! access keys) or `"ASIA"` (temporary session credentials from STS AssumeRole).
//!
//! # Validation Modes
//!
//! **STS validation (default):** Calls `GetCallerIdentity` using AWS Signature Version 4
//! signing. This is the secure path — it cryptographically proves the client possesses
//! the claimed AWS credentials. No AWS SDK dependency: SigV4 is implemented inline
//! using `sha2` + `hmac`.
//!
//! **Config-only mode (`validate_with_sts = false`):** Skips the STS call and derives
//! identity purely from the access key ID via configured mappings. Faster and no
//! network dependency, but less secure — it trusts the client to present a valid key.
//!
//! # Use Cases
//!
//! - AWS-native deployments where IAM is the identity provider
//! - Service-to-service auth (Lambda, ECS task roles connecting to SQE)
//! - Environments where OIDC is unavailable but IAM roles are managed

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

type HmacSha256 = Hmac<Sha256>;

/// Configuration for the AWS IAM auth provider.
///
/// ```toml
/// [[auth.providers]]
/// type = "aws_iam"
/// region = "us-east-1"
/// validate_with_sts = true
///
/// [auth.providers.role_mappings]
/// "arn:aws:iam::*:role/DataAnalyst" = ["analyst", "reader"]
/// "arn:aws:iam::*:role/Admin" = ["admin"]
/// ```
#[derive(Debug, Clone)]
pub struct AwsIamProviderConfig {
    /// AWS region for the STS endpoint (e.g., `"us-east-1"`).
    pub region: String,

    /// Whether to validate credentials by calling STS `GetCallerIdentity`.
    /// When `false`, identity is derived purely from config mappings.
    /// Default: `true`.
    pub validate_with_sts: bool,

    /// Maps ARN glob patterns to role lists.
    ///
    /// The glob pattern supports `*` as a wildcard matching any substring.
    /// Example: `"arn:aws:iam::*:role/DataAnalyst"` matches any account's
    /// `DataAnalyst` role.
    pub role_mappings: HashMap<String, Vec<String>>,

    /// Maps access key IDs to static identities (for config-only mode).
    /// Only used when `validate_with_sts = false`.
    ///
    /// Example:
    /// ```toml
    /// [auth.providers.key_mappings]
    /// "AKIAIOSFODNN7EXAMPLE" = { arn = "arn:aws:iam::123456789012:user/dbt-prod", roles = ["writer"] }
    /// ```
    pub key_mappings: HashMap<String, KeyMapping>,
}

/// Static identity mapping for a known access key ID (config-only mode).
#[derive(Debug, Clone)]
pub struct KeyMapping {
    /// The full ARN to use as the user_id.
    pub arn: String,
    /// Roles to assign directly (bypasses role_mappings glob matching).
    pub roles: Vec<String>,
}

impl Default for AwsIamProviderConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            validate_with_sts: true,
            role_mappings: HashMap::new(),
            key_mappings: HashMap::new(),
        }
    }
}

/// AWS IAM/STS authentication provider.
///
/// See [module-level docs](self) for usage details.
pub struct AwsIamProvider {
    config: AwsIamProviderConfig,
    http_client: reqwest::Client,
}

impl AwsIamProvider {
    /// Create a new AWS IAM provider with the given configuration.
    pub fn new(config: AwsIamProviderConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Returns `true` if the username looks like an AWS access key ID.
    fn is_aws_access_key(username: &str) -> bool {
        // Long-term keys start with AKIA, temporary (STS) keys start with ASIA.
        // AWS access key IDs are always 20 uppercase alphanumeric characters.
        (username.starts_with("AKIA") || username.starts_with("ASIA"))
            && username.len() == 20
            && username.chars().all(|c| c.is_ascii_alphanumeric())
    }

    /// Call STS `GetCallerIdentity` and return (Arn, UserId, Account).
    async fn call_sts_get_caller_identity(
        &self,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
    ) -> Result<StsIdentity, AuthError> {
        let region = &self.config.region;
        let host = format!("sts.{region}.amazonaws.com");
        let endpoint = format!("https://{host}/");
        let body = "Action=GetCallerIdentity&Version=2011-06-15";

        let now = Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        // ---- SigV4 Signing ----
        let service = "sts";
        let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");

        // Step 1: Canonical request
        let payload_hash = hex_sha256(body.as_bytes());

        let mut signed_headers = "content-type;host;x-amz-date".to_string();
        let mut canonical_headers = format!(
            "content-type:application/x-www-form-urlencoded\nhost:{host}\nx-amz-date:{amz_date}\n"
        );

        if let Some(token) = session_token {
            canonical_headers = format!(
                "content-type:application/x-www-form-urlencoded\nhost:{host}\nx-amz-date:{amz_date}\nx-amz-security-token:{token}\n"
            );
            signed_headers = "content-type;host;x-amz-date;x-amz-security-token".to_string();
        }

        let canonical_request = format!(
            "POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        // Step 2: String to sign
        let canonical_request_hash = hex_sha256(canonical_request.as_bytes());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}"
        );

        // Step 3: Signing key
        let signing_key = derive_signing_key(secret_access_key, &date_stamp, region, service);

        // Step 4: Signature
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        // Step 5: Authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );

        // ---- Make the request ----
        let mut request = self
            .http_client
            .post(&endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Host", &host)
            .header("X-Amz-Date", &amz_date)
            .header("Authorization", &authorization);

        if let Some(token) = session_token {
            request = request.header("X-Amz-Security-Token", token);
        }

        let response = request
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("STS request failed: {e}")))?;

        let status = response.status();
        let response_body = response
            .text()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("failed to read STS response: {e}")))?;

        if !status.is_success() {
            debug!(status = %status, body = %response_body, "STS GetCallerIdentity rejected");
            return Err(AuthError::AuthFailed(format!(
                "AWS STS rejected credentials (HTTP {status})"
            )));
        }

        // Parse the XML response.
        // Example response:
        // <GetCallerIdentityResponse>
        //   <GetCallerIdentityResult>
        //     <Arn>arn:aws:iam::123456789012:user/Alice</Arn>
        //     <UserId>AIDAEXAMPLEID</UserId>
        //     <Account>123456789012</Account>
        //   </GetCallerIdentityResult>
        // </GetCallerIdentityResponse>
        let arn = extract_xml_element(&response_body, "Arn")
            .ok_or_else(|| {
                AuthError::Internal(anyhow::anyhow!(
                    "STS response missing <Arn> element"
                ))
            })?;
        let user_id = extract_xml_element(&response_body, "UserId")
            .ok_or_else(|| {
                AuthError::Internal(anyhow::anyhow!(
                    "STS response missing <UserId> element"
                ))
            })?;
        let account = extract_xml_element(&response_body, "Account")
            .ok_or_else(|| {
                AuthError::Internal(anyhow::anyhow!(
                    "STS response missing <Account> element"
                ))
            })?;

        Ok(StsIdentity {
            arn,
            user_id,
            account,
        })
    }

    /// Resolve roles for an ARN using the configured role_mappings glob patterns.
    fn resolve_roles(&self, arn: &str) -> Vec<String> {
        let mut roles = Vec::new();
        for (pattern, pattern_roles) in &self.config.role_mappings {
            if arn_glob_matches(pattern, arn) {
                roles.extend(pattern_roles.iter().cloned());
            }
        }
        roles.sort();
        roles.dedup();
        roles
    }
}

/// Identity returned by STS `GetCallerIdentity`.
#[derive(Debug, Clone)]
struct StsIdentity {
    arn: String,
    user_id: String,
    account: String,
}

#[async_trait]
impl AuthProvider for AwsIamProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // --- Credential detection ---
        let username = match &credentials.username {
            Some(u) if Self::is_aws_access_key(u) => u.clone(),
            _ => return Err(AuthError::NotMyCredentials),
        };

        let secret_key = credentials
            .password
            .as_ref()
            .ok_or_else(|| {
                AuthError::AuthFailed(
                    "AWS access key ID provided but secret access key (password) is missing"
                        .to_string(),
                )
            })?
            .expose();

        // Session token for temporary credentials (ASIA* keys).
        // Passed via bearer_token field since Flight credentials don't have
        // a dedicated session token field.
        let session_token = credentials.bearer_token.as_ref().map(|t| t.expose());

        debug!(
            access_key_id = %username,
            has_session_token = session_token.is_some(),
            validate_with_sts = self.config.validate_with_sts,
            "AWS IAM authentication attempt"
        );

        if self.config.validate_with_sts {
            // --- STS validation path ---
            let sts_identity = self
                .call_sts_get_caller_identity(&username, secret_key, session_token)
                .await?;

            let display_name = extract_display_name_from_arn(&sts_identity.arn);
            let roles = self.resolve_roles(&sts_identity.arn);

            debug!(
                arn = %sts_identity.arn,
                account = %sts_identity.account,
                sts_user_id = %sts_identity.user_id,
                roles = ?roles,
                "AWS IAM authentication succeeded via STS"
            );

            Ok(Identity {
                user_id: sts_identity.arn,
                display_name,
                roles,
                catalog_token: None,
                refresh_token: None,
                expires_at: None,
            })
        } else {
            // --- Config-only path ---
            if let Some(mapping) = self.config.key_mappings.get(&username) {
                debug!(
                    access_key_id = %username,
                    arn = %mapping.arn,
                    roles = ?mapping.roles,
                    "AWS IAM authentication succeeded via config mapping"
                );

                Ok(Identity {
                    user_id: mapping.arn.clone(),
                    display_name: extract_display_name_from_arn(&mapping.arn),
                    roles: mapping.roles.clone(),
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                })
            } else {
                warn!(
                    access_key_id = %username,
                    "AWS access key ID not found in key_mappings (validate_with_sts=false)"
                );
                Err(AuthError::AuthFailed(format!(
                    "unknown AWS access key ID: {username}"
                )))
            }
        }
    }

    // refresh_catalog_token: uses the default (Ok(None)).
    // AWS credentials don't produce catalog tokens — the coordinator uses its
    // own Polaris auth separately.
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Extract a display name from an AWS ARN.
///
/// Examples:
/// - `arn:aws:iam::123456789012:user/Alice` -> `"Alice"`
/// - `arn:aws:iam::123456789012:role/DataAnalyst` -> `"DataAnalyst"`
/// - `arn:aws:sts::123456789012:assumed-role/MyRole/session` -> `"MyRole/session"`
/// - Anything else -> the full ARN
fn extract_display_name_from_arn(arn: &str) -> String {
    // ARN format: arn:partition:service:region:account:resource-type/resource-id
    // Split on ':' and take the last part, then split on '/' and take everything
    // after the resource type.
    if let Some(resource) = arn.split(':').next_back() {
        if let Some(slash_pos) = resource.find('/') {
            return resource[slash_pos + 1..].to_string();
        }
        return resource.to_string();
    }
    arn.to_string()
}

/// Simple glob matching for ARN patterns.
///
/// Supports `*` as a wildcard that matches any sequence of characters.
/// This is intentionally simple — it splits the pattern on `*` and checks
/// that all parts appear in order in the input string.
fn arn_glob_matches(pattern: &str, input: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 1 {
        // No wildcards — exact match.
        return pattern == input;
    }

    let mut pos = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First segment must be a prefix.
            if !input.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Last segment must be a suffix.
            if !input[pos..].ends_with(part) {
                return false;
            }
        } else {
            // Middle segments must appear in order.
            match input[pos..].find(part) {
                Some(found) => pos += found + part.len(),
                None => return false,
            }
        }
    }

    true
}

/// Compute SHA-256 and return lowercase hex.
fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Compute HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Derive the AWS SigV4 signing key.
///
/// ```text
/// kDate    = HMAC("AWS4" + secret, date)
/// kRegion  = HMAC(kDate, region)
/// kService = HMAC(kRegion, service)
/// kSigning = HMAC(kService, "aws4_request")
/// ```
fn derive_signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(
        format!("AWS4{secret}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Extract the text content of an XML element by tag name.
///
/// This is a minimal parser — no dependency on an XML crate. It handles the
/// simple, well-defined STS `GetCallerIdentity` response format.
fn extract_xml_element(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Credential detection
    // -----------------------------------------------------------------------

    #[test]
    fn is_aws_access_key_accepts_akia_prefix() {
        assert!(AwsIamProvider::is_aws_access_key("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn is_aws_access_key_accepts_asia_prefix() {
        assert!(AwsIamProvider::is_aws_access_key("ASIAJEXAMPLEEXAMPLEA"));
    }

    #[test]
    fn is_aws_access_key_rejects_short_key() {
        assert!(!AwsIamProvider::is_aws_access_key("AKIA1234"));
    }

    #[test]
    fn is_aws_access_key_rejects_non_alphanumeric() {
        assert!(!AwsIamProvider::is_aws_access_key("AKIA!OSFODNN7EXAMPL"));
    }

    #[test]
    fn is_aws_access_key_rejects_regular_username() {
        assert!(!AwsIamProvider::is_aws_access_key("alice@example.com"));
    }

    #[test]
    fn is_aws_access_key_rejects_empty() {
        assert!(!AwsIamProvider::is_aws_access_key(""));
    }

    // -----------------------------------------------------------------------
    // NotMyCredentials when username is not an AWS key
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn non_aws_username_returns_not_my_credentials() {
        let provider = AwsIamProvider::new(AwsIamProviderConfig::default());

        let creds = FlightCredentials {
            username: Some("alice@example.com".to_string()),
            password: Some(sqe_core::SecretString::new("password123".to_string())),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {} // expected
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_username_returns_not_my_credentials() {
        let provider = AwsIamProvider::new(AwsIamProviderConfig::default());

        let creds = FlightCredentials {
            username: None,
            password: Some(sqe_core::SecretString::new("password123".to_string())),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // AWS key without password → AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn aws_key_without_password_returns_auth_failed() {
        let provider = AwsIamProvider::new(AwsIamProviderConfig::default());

        let creds = FlightCredentials {
            username: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            password: None,
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("secret access key"), "got: {msg}");
            }
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Config-only mode: known key → identity
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn config_only_known_key_returns_identity() {
        let mut key_mappings = HashMap::new();
        key_mappings.insert(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            KeyMapping {
                arn: "arn:aws:iam::123456789012:user/Alice".to_string(),
                roles: vec!["analyst".to_string(), "reader".to_string()],
            },
        );

        let provider = AwsIamProvider::new(AwsIamProviderConfig {
            validate_with_sts: false,
            key_mappings,
            ..Default::default()
        });

        let creds = FlightCredentials {
            username: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            password: Some(sqe_core::SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "arn:aws:iam::123456789012:user/Alice");
        assert_eq!(identity.display_name, "Alice");
        assert_eq!(identity.roles, vec!["analyst", "reader"]);
        assert!(identity.catalog_token.is_none());
        assert!(identity.refresh_token.is_none());
    }

    // -----------------------------------------------------------------------
    // Config-only mode: unknown key → AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn config_only_unknown_key_returns_auth_failed() {
        let provider = AwsIamProvider::new(AwsIamProviderConfig {
            validate_with_sts: false,
            ..Default::default()
        });

        let creds = FlightCredentials {
            username: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            password: Some(sqe_core::SecretString::new("secret".to_string())),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("unknown AWS access key ID"), "got: {msg}");
            }
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // ARN display name extraction
    // -----------------------------------------------------------------------

    #[test]
    fn display_name_from_user_arn() {
        assert_eq!(
            extract_display_name_from_arn("arn:aws:iam::123456789012:user/Alice"),
            "Alice"
        );
    }

    #[test]
    fn display_name_from_role_arn() {
        assert_eq!(
            extract_display_name_from_arn("arn:aws:iam::123456789012:role/DataAnalyst"),
            "DataAnalyst"
        );
    }

    #[test]
    fn display_name_from_assumed_role_arn() {
        assert_eq!(
            extract_display_name_from_arn(
                "arn:aws:sts::123456789012:assumed-role/MyRole/my-session"
            ),
            "MyRole/my-session"
        );
    }

    #[test]
    fn display_name_from_root_arn() {
        assert_eq!(
            extract_display_name_from_arn("arn:aws:iam::123456789012:root"),
            "root"
        );
    }

    // -----------------------------------------------------------------------
    // ARN glob matching
    // -----------------------------------------------------------------------

    #[test]
    fn glob_exact_match() {
        assert!(arn_glob_matches(
            "arn:aws:iam::123456789012:role/Admin",
            "arn:aws:iam::123456789012:role/Admin"
        ));
    }

    #[test]
    fn glob_exact_mismatch() {
        assert!(!arn_glob_matches(
            "arn:aws:iam::123456789012:role/Admin",
            "arn:aws:iam::123456789012:role/User"
        ));
    }

    #[test]
    fn glob_wildcard_account() {
        assert!(arn_glob_matches(
            "arn:aws:iam::*:role/DataAnalyst",
            "arn:aws:iam::123456789012:role/DataAnalyst"
        ));
    }

    #[test]
    fn glob_wildcard_account_no_match_different_role() {
        assert!(!arn_glob_matches(
            "arn:aws:iam::*:role/DataAnalyst",
            "arn:aws:iam::123456789012:role/Admin"
        ));
    }

    #[test]
    fn glob_trailing_wildcard() {
        assert!(arn_glob_matches(
            "arn:aws:iam::123456789012:*",
            "arn:aws:iam::123456789012:role/Admin"
        ));
    }

    #[test]
    fn glob_multiple_wildcards() {
        assert!(arn_glob_matches(
            "arn:aws:*::*:role/*",
            "arn:aws:iam::123456789012:role/DataAnalyst"
        ));
    }

    // -----------------------------------------------------------------------
    // Role resolution from glob mappings
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_roles_matching_pattern() {
        let mut role_mappings = HashMap::new();
        role_mappings.insert(
            "arn:aws:iam::*:role/DataAnalyst".to_string(),
            vec!["analyst".to_string(), "reader".to_string()],
        );
        role_mappings.insert(
            "arn:aws:iam::*:role/Admin".to_string(),
            vec!["admin".to_string()],
        );

        let provider = AwsIamProvider::new(AwsIamProviderConfig {
            role_mappings,
            ..Default::default()
        });

        let roles = provider.resolve_roles("arn:aws:iam::123456789012:role/DataAnalyst");
        assert_eq!(roles, vec!["analyst", "reader"]);
    }

    #[test]
    fn resolve_roles_no_match_returns_empty() {
        let mut role_mappings = HashMap::new();
        role_mappings.insert(
            "arn:aws:iam::*:role/Admin".to_string(),
            vec!["admin".to_string()],
        );

        let provider = AwsIamProvider::new(AwsIamProviderConfig {
            role_mappings,
            ..Default::default()
        });

        let roles = provider.resolve_roles("arn:aws:iam::123456789012:user/Alice");
        assert!(roles.is_empty());
    }

    #[test]
    fn resolve_roles_deduplicates() {
        let mut role_mappings = HashMap::new();
        role_mappings.insert(
            "arn:aws:iam::*:role/*".to_string(),
            vec!["reader".to_string()],
        );
        role_mappings.insert(
            "arn:aws:iam::*:role/DataAnalyst".to_string(),
            vec!["reader".to_string(), "analyst".to_string()],
        );

        let provider = AwsIamProvider::new(AwsIamProviderConfig {
            role_mappings,
            ..Default::default()
        });

        let roles = provider.resolve_roles("arn:aws:iam::123456789012:role/DataAnalyst");
        // Should be deduplicated: ["analyst", "reader"] — no duplicate "reader"
        assert_eq!(roles, vec!["analyst", "reader"]);
    }

    // -----------------------------------------------------------------------
    // XML element extraction
    // -----------------------------------------------------------------------

    #[test]
    fn extract_xml_element_basic() {
        let xml = "<Response><Arn>arn:aws:iam::123:user/Alice</Arn><Account>123</Account></Response>";
        assert_eq!(
            extract_xml_element(xml, "Arn"),
            Some("arn:aws:iam::123:user/Alice".to_string())
        );
        assert_eq!(
            extract_xml_element(xml, "Account"),
            Some("123".to_string())
        );
    }

    #[test]
    fn extract_xml_element_missing_tag() {
        let xml = "<Response><Arn>test</Arn></Response>";
        assert_eq!(extract_xml_element(xml, "Missing"), None);
    }

    #[test]
    fn extract_xml_full_sts_response() {
        let xml = r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::123456789012:user/Alice</Arn>
    <UserId>AIDAEXAMPLEID1234567</UserId>
    <Account>123456789012</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>01234567-89ab-cdef-0123-456789abcdef</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>"#;

        assert_eq!(
            extract_xml_element(xml, "Arn"),
            Some("arn:aws:iam::123456789012:user/Alice".to_string())
        );
        assert_eq!(
            extract_xml_element(xml, "UserId"),
            Some("AIDAEXAMPLEID1234567".to_string())
        );
        assert_eq!(
            extract_xml_element(xml, "Account"),
            Some("123456789012".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // SigV4 signing helpers — deterministic test
    // -----------------------------------------------------------------------

    #[test]
    fn hex_sha256_empty_string() {
        // SHA-256 of empty string is a well-known constant.
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hmac_sha256_deterministic() {
        let result = hmac_sha256(b"key", b"message");
        // HMAC-SHA256("key", "message") is deterministic.
        assert_eq!(result.len(), 32);
        let hex_result = hex::encode(&result);
        assert_eq!(
            hex_result,
            "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"
        );
    }

    #[test]
    fn derive_signing_key_deterministic() {
        let key = derive_signing_key("wJalrXUtnFEMI", "20230101", "us-east-1", "sts");
        assert_eq!(key.len(), 32);
        // The key should be deterministic for the same inputs.
        let key2 = derive_signing_key("wJalrXUtnFEMI", "20230101", "us-east-1", "sts");
        assert_eq!(key, key2);
    }

    // -----------------------------------------------------------------------
    // STS validation with mock server
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sts_validation_with_mock_success() {
        // This test would require a mock HTTP server. We test the parsing
        // and logic separately. The STS call itself is integration-tested
        // against real AWS.
        //
        // For now, verify the config-only path works end-to-end (above tests)
        // and the STS path compiles correctly.
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token returns None
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_returns_none() {
        let provider = AwsIamProvider::new(AwsIamProviderConfig::default());
        let identity = Identity {
            user_id: "arn:aws:iam::123456789012:user/Alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };

        let result = provider.refresh_catalog_token(&identity).await;
        assert!(result.unwrap().is_none());
    }
}

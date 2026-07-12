//! `ApiKeyProvider` — static API key authentication provider.
//!
//! Authenticates clients presenting a pre-shared API key in the password field.
//! Keys are loaded from a TOML file and can be hot-reloaded without restart.
//!
//! # Credential Detection
//!
//! This provider handles credentials where the password field starts with a
//! configurable prefix (default: `"sqe_"`). This prevents collision with OIDC
//! passwords and bearer tokens.
//!
//! # Security
//!
//! - Key comparison uses constant-time equality (`subtle`) to prevent timing attacks.
//! - Keys are stored in memory; the keys file should be permission-restricted.
//!
//! # Hot-Reload
//!
//! A background task polls the keys file's modification time and reloads when
//! it changes. Reload errors are logged but do not interrupt running auth.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// A single API key entry as it appears in the keys TOML file.
///
/// ```toml
/// [[keys]]
/// key = "sqe_abc123def456"
/// description = "CI pipeline read-only"
/// user = "ci-bot"
/// groups = ["readers"]
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyEntry {
    /// The full API key string (including prefix).
    pub key: String,
    /// Human-readable description (for audit logs).
    #[serde(default)]
    pub description: String,
    /// The user identity to assign when this key is used.
    #[serde(default = "default_api_key_user")]
    pub user: String,
    /// Groups assigned to this key, mapped to roles via `role_mappings`.
    #[serde(default)]
    pub groups: Vec<String>,
}

fn default_api_key_user() -> String {
    "api-key-user".to_string()
}

/// Root structure of the keys TOML file.
#[derive(Debug, Clone, Deserialize)]
struct KeysFile {
    #[serde(default)]
    keys: Vec<ApiKeyEntry>,
}

/// Configuration for the API key auth provider.
#[derive(Debug, Clone)]
pub struct ApiKeyProviderConfig {
    /// Path to the TOML file containing API key entries.
    pub keys_file: PathBuf,
    /// Prefix that identifies an API key in the password field (default: `"sqe_"`).
    pub key_prefix: String,
    /// Group → roles mapping. Keys are group names, values are role lists.
    pub role_mappings: HashMap<String, Vec<String>>,
    /// How often to check the keys file for changes (default: 30s).
    pub reload_interval: Duration,
}

impl Default for ApiKeyProviderConfig {
    fn default() -> Self {
        Self {
            keys_file: PathBuf::from("api-keys.toml"),
            key_prefix: "sqe_".to_string(),
            role_mappings: HashMap::new(),
            reload_interval: Duration::from_secs(30),
        }
    }
}

/// API key authentication provider.
pub struct ApiKeyProvider {
    config: ApiKeyProviderConfig,
    keys: Arc<RwLock<Vec<ApiKeyEntry>>>,
}

impl ApiKeyProvider {
    /// Create a new provider, loading keys from the configured file.
    ///
    /// Returns an error if the keys file cannot be read or parsed.
    pub fn new(config: ApiKeyProviderConfig) -> Result<Self, anyhow::Error> {
        let keys = load_keys_from_file(&config.keys_file)?;
        info!(
            keys_file = %config.keys_file.display(),
            key_count = keys.len(),
            "API key provider initialized"
        );
        Ok(Self {
            config,
            keys: Arc::new(RwLock::new(keys)),
        })
    }

    /// Create a provider with pre-loaded keys (for testing).
    pub fn with_keys(config: ApiKeyProviderConfig, keys: Vec<ApiKeyEntry>) -> Self {
        Self {
            config,
            keys: Arc::new(RwLock::new(keys)),
        }
    }

    /// Spawn a background task that polls the keys file for changes.
    ///
    /// The task runs until the returned `tokio::task::JoinHandle` is aborted.
    pub fn spawn_reload_watcher(&self) -> tokio::task::JoinHandle<()> {
        let keys = Arc::clone(&self.keys);
        let path = self.config.keys_file.clone();
        let interval = self.config.reload_interval;

        tokio::spawn(async move {
            let mut last_modified = file_mtime(&path);
            loop {
                tokio::time::sleep(interval).await;
                let current_mtime = file_mtime(&path);
                if current_mtime != last_modified {
                    match load_keys_from_file_async(&path).await {
                        Ok(new_keys) => {
                            info!(
                                keys_file = %path.display(),
                                key_count = new_keys.len(),
                                "API keys reloaded"
                            );
                            *keys.write().await = new_keys;
                            last_modified = current_mtime;
                        }
                        Err(e) => {
                            warn!(
                                keys_file = %path.display(),
                                error = %e,
                                "Failed to reload API keys — keeping previous set"
                            );
                        }
                    }
                }
            }
        })
    }

    /// Resolve groups to roles via the configured role mappings.
    fn resolve_roles(&self, groups: &[String]) -> Vec<String> {
        let mut roles = Vec::new();
        for group in groups {
            if let Some(mapped) = self.config.role_mappings.get(group) {
                roles.extend(mapped.iter().cloned());
            }
        }
        roles.sort();
        roles.dedup();
        roles
    }
}

#[async_trait]
impl AuthProvider for ApiKeyProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // Detect: password must be present and start with the configured prefix.
        let password = match &credentials.password {
            Some(p) if p.expose().starts_with(&self.config.key_prefix) => p.expose(),
            _ => return Err(AuthError::NotMyCredentials),
        };

        let keys = self.keys.read().await;

        // Hash the candidate once to a fixed-length digest. All subsequent
        // comparisons are over 32-byte buffers, so length is never an input
        // to the timing of the comparison.
        let input_digest = sha256_digest(password.as_bytes());

        // Walk every entry. ct_eq returns a Choice; we accumulate matches in
        // a single byte and capture the first matching index without ever
        // breaking out of the loop, so iteration order is not observable.
        let mut matched: u8 = 0;
        let mut match_index: usize = 0;
        for (idx, entry) in keys.iter().enumerate() {
            let entry_digest = sha256_digest(entry.key.as_bytes());
            let eq: u8 = input_digest.ct_eq(&entry_digest).unwrap_u8();
            // Latch the first index where eq == 1 without branching on it.
            let take = eq & (1 - matched);
            match_index = (take as usize) * idx + (1 - take as usize) * match_index;
            matched |= eq;
        }

        if matched == 1 {
            let entry = &keys[match_index];
            let roles = self.resolve_roles(&entry.groups);
            debug!(
                user = %entry.user,
                description = %entry.description,
                roles = ?roles,
                "API key authentication succeeded"
            );
            return Ok(Identity {
                user_id: entry.user.clone(),
                display_name: entry.user.clone(),
                roles,
                subject: None,
                email: None,
                groups: vec![],
                catalog_token: None,
                refresh_token: None,
                expires_at: None,
            });
        }

        Err(AuthError::AuthFailed("invalid API key".to_string()))
    }

    // refresh_catalog_token: returns Ok(None) — API keys have no catalog token.
}

/// Load and parse keys from a TOML file (blocking -- use for startup only).
fn load_keys_from_file(path: &Path) -> Result<Vec<ApiKeyEntry>, anyhow::Error> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read keys file {}: {e}", path.display()))?;
    let keys_file: KeysFile = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("cannot parse keys file {}: {e}", path.display()))?;
    Ok(keys_file.keys)
}

/// Async variant of [`load_keys_from_file`] -- safe to call from Tokio worker threads.
async fn load_keys_from_file_async(path: &Path) -> Result<Vec<ApiKeyEntry>, anyhow::Error> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow::anyhow!("cannot read keys file {}: {e}", path.display()))?;
    let keys_file: KeysFile = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("cannot parse keys file {}: {e}", path.display()))?;
    Ok(keys_file.keys)
}

/// Get file modification time as an opaque comparable value.
fn file_mtime(path: &Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ApiKeyProviderConfig {
        let mut role_mappings = HashMap::new();
        role_mappings.insert("readers".to_string(), vec!["read-only".to_string()]);
        role_mappings.insert(
            "admins".to_string(),
            vec!["admin".to_string(), "read-only".to_string()],
        );
        ApiKeyProviderConfig {
            keys_file: PathBuf::from("/nonexistent"),
            key_prefix: "sqe_".to_string(),
            role_mappings,
            reload_interval: Duration::from_secs(30),
        }
    }

    fn test_keys() -> Vec<ApiKeyEntry> {
        vec![
            ApiKeyEntry {
                key: "sqe_key_alpha".to_string(),
                description: "CI pipeline".to_string(),
                user: "ci-bot".to_string(),
                groups: vec!["readers".to_string()],
            },
            ApiKeyEntry {
                key: "sqe_key_bravo".to_string(),
                description: "Admin key".to_string(),
                user: "admin-svc".to_string(),
                groups: vec!["admins".to_string()],
            },
        ]
    }

    // -----------------------------------------------------------------------
    // Correct key authenticates
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn correct_key_returns_identity() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_key_alpha".to_string())),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "ci-bot");
        assert_eq!(identity.roles, vec!["read-only"]);
        assert!(identity.catalog_token.is_none());
    }

    // -----------------------------------------------------------------------
    // Wrong key returns AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wrong_key_returns_auth_failed() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_wrong_key".to_string())),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => assert!(msg.contains("invalid API key")),
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // No prefix → NotMyCredentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_prefix_returns_not_my_credentials() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("regular_password".to_string())),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // No password → NotMyCredentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_password_returns_not_my_credentials() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // JWT-looking password → NotMyCredentials (no sqe_ prefix)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn jwt_password_returns_not_my_credentials() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new(
                "eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string(),
            )),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Group → role mapping
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admin_key_maps_groups_to_roles() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_key_bravo".to_string())),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "admin-svc");
        // Roles are sorted and deduped.
        assert_eq!(identity.roles, vec!["admin", "read-only"]);
    }

    // -----------------------------------------------------------------------
    // Unmapped groups produce empty roles
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unmapped_groups_produce_empty_roles() {
        let keys = vec![ApiKeyEntry {
            key: "sqe_unmapped".to_string(),
            description: "test".to_string(),
            user: "test-user".to_string(),
            groups: vec!["unknown-group".to_string()],
        }];
        let provider = ApiKeyProvider::with_keys(test_config(), keys);
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_unmapped".to_string())),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert!(identity.roles.is_empty());
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token returns None
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_returns_none() {
        let provider = ApiKeyProvider::with_keys(test_config(), test_keys());
        let identity = Identity {
            user_id: "test".to_string(),
            display_name: "test".to_string(),
            roles: vec![],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };

        assert!(provider
            .refresh_catalog_token(&identity)
            .await
            .unwrap()
            .is_none());
    }

    // -----------------------------------------------------------------------
    // File loading (roundtrip)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_keys_toml() {
        let toml_str = r#"
[[keys]]
key = "sqe_test_key_1"
description = "Test key"
user = "tester"
groups = ["readers", "testers"]

[[keys]]
key = "sqe_test_key_2"
description = "Another key"
user = "other"
"#;
        let keys_file: KeysFile = toml::from_str(toml_str).unwrap();
        assert_eq!(keys_file.keys.len(), 2);
        assert_eq!(keys_file.keys[0].user, "tester");
        assert_eq!(keys_file.keys[0].groups, vec!["readers", "testers"]);
        assert_eq!(keys_file.keys[1].user, "other");
        assert!(keys_file.keys[1].groups.is_empty());
    }

    // -----------------------------------------------------------------------
    // Hot-reload: writing new file updates keys
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reload_picks_up_new_key() {
        let dir = std::env::temp_dir().join(format!("sqe-api-key-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let keys_path = dir.join("keys.toml");

        // Write initial file with one key.
        std::fs::write(
            &keys_path,
            r#"
[[keys]]
key = "sqe_original"
user = "original-user"
"#,
        )
        .unwrap();

        let config = ApiKeyProviderConfig {
            keys_file: keys_path.clone(),
            key_prefix: "sqe_".to_string(),
            role_mappings: HashMap::new(),
            reload_interval: Duration::from_millis(50),
        };

        let provider = ApiKeyProvider::new(config).unwrap();
        let watcher = provider.spawn_reload_watcher();

        // Original key works.
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_original".to_string())),
            ..Default::default()
        };
        let id = provider.authenticate(&creds).await.unwrap();
        assert_eq!(id.user_id, "original-user");

        // Sleep to ensure mtime differs, then write new file.
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(
            &keys_path,
            r#"
[[keys]]
key = "sqe_original"
user = "original-user"

[[keys]]
key = "sqe_newkey"
user = "new-user"
"#,
        )
        .unwrap();

        // Wait for reload.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // New key should work now.
        let creds2 = FlightCredentials {
            password: Some(sqe_core::SecretString::new("sqe_newkey".to_string())),
            ..Default::default()
        };
        let id2 = provider.authenticate(&creds2).await.unwrap();
        assert_eq!(id2.user_id, "new-user");

        watcher.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------
    // Constant-time: different-length keys don't match
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn different_length_key_does_not_match() {
        let keys = vec![ApiKeyEntry {
            key: "sqe_short".to_string(),
            description: "".to_string(),
            user: "test".to_string(),
            groups: vec![],
        }];
        let provider = ApiKeyProvider::with_keys(test_config(), keys);

        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new(
                "sqe_short_but_longer".to_string(),
            )),
            ..Default::default()
        };
        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(_)) => {}
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }
}

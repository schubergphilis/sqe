use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use moka::future::Cache;
use tracing::debug;

use sqe_core::config::StorageConfig;

/// Vended S3 credentials extracted from Polaris table load responses.
///
/// When Polaris is configured with credential vending, it returns short-lived
/// S3 credentials scoped to the specific table being accessed. These credentials
/// are included in the table config returned by the REST catalog's load_table response.
#[derive(Debug, Clone)]
pub struct VendedCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub expiry: Option<DateTime<Utc>>,
}

/// Cache for vended S3 credentials, keyed by table identifier.
///
/// Uses moka's async cache with TTL derived from credential expiry.
/// When credentials expire or are not available, the system falls back
/// to static S3 credentials from `StorageConfig`.
pub struct CredentialCache {
    cache: Cache<String, VendedCredentials>,
    storage_config: StorageConfig,
}

impl CredentialCache {
    /// Create a new credential cache with a default TTL.
    ///
    /// The TTL is set conservatively to 50 minutes, which is shorter than
    /// the typical 1-hour STS credential lifetime. Individual entries may
    /// have shorter TTLs based on their actual expiry.
    pub fn new(storage_config: StorageConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(50 * 60))
            .build();

        Self {
            cache,
            storage_config,
        }
    }

    /// Get cached credentials for a table, or extract them from the config.
    ///
    /// If credentials are cached and still valid, returns them directly.
    /// Otherwise attempts to extract from the provided table config.
    /// Falls back to static credentials from StorageConfig.
    pub async fn get_or_extract(
        &self,
        table_key: &str,
        table_config: &HashMap<String, String>,
    ) -> VendedCredentials {
        // Check cache first
        if let Some(creds) = self.cache.get(table_key).await {
            if let Some(expiry) = &creds.expiry {
                if *expiry > Utc::now() {
                    debug!(table = table_key, "Using cached vended credentials");
                    return creds;
                }
            } else {
                debug!(table = table_key, "Using cached vended credentials (no expiry)");
                return creds;
            }
        }

        // Try to extract from table config
        if let Some(creds) = extract_from_table_config(table_config) {
            debug!(
                table = table_key,
                has_session_token = creds.session_token.is_some(),
                has_expiry = creds.expiry.is_some(),
                "Extracted vended credentials from table config"
            );
            self.cache
                .insert(table_key.to_string(), creds.clone())
                .await;
            return creds;
        }

        // Fallback to static storage config
        debug!(table = table_key, "Using static S3 credentials from StorageConfig");
        VendedCredentials {
            access_key: self.storage_config.s3_access_key.clone(),
            secret_key: self.storage_config.s3_secret_key.expose().to_string(),
            session_token: None,
            expiry: None,
        }
    }

    /// Invalidate cached credentials for a specific table.
    pub async fn invalidate(&self, table_key: &str) {
        self.cache.invalidate(table_key).await;
    }

    /// Invalidate all cached credentials.
    pub async fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Return an Arc-wrapped version of this cache.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Extract vended S3 credentials from a table config returned by the REST catalog.
///
/// Polaris returns credentials via the `config` field of the LoadTableResponse.
/// The standard Iceberg REST spec uses these property keys:
/// - `s3.access-key-id`
/// - `s3.secret-access-key`
/// - `s3.session-token`
pub fn extract_from_table_config(config: &HashMap<String, String>) -> Option<VendedCredentials> {
    let access_key = config.get("s3.access-key-id").cloned()?;
    let secret_key = config.get("s3.secret-access-key").cloned()?;

    let session_token = config.get("s3.session-token").cloned();

    // Try to parse expiry from the config if present
    let expiry = config
        .get("s3.session-token-expiry")
        .or_else(|| config.get("s3.token-expiry"))
        .and_then(|s| {
            // Try parsing as RFC3339 first, then as epoch millis
            DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
                .or_else(|| {
                    s.parse::<i64>().ok().and_then(|millis| {
                        DateTime::from_timestamp_millis(millis)
                    })
                })
        });

    Some(VendedCredentials {
        access_key,
        secret_key,
        session_token,
        expiry,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_full_credentials() {
        let mut config = HashMap::new();
        config.insert("s3.access-key-id".to_string(), "AKID123".to_string());
        config.insert("s3.secret-access-key".to_string(), "SECRET456".to_string());
        config.insert("s3.session-token".to_string(), "TOKEN789".to_string());

        let creds = extract_from_table_config(&config).unwrap();
        assert_eq!(creds.access_key, "AKID123");
        assert_eq!(creds.secret_key, "SECRET456");
        assert_eq!(creds.session_token.unwrap(), "TOKEN789");
        assert!(creds.expiry.is_none());
    }

    #[test]
    fn test_extract_without_session_token() {
        let mut config = HashMap::new();
        config.insert("s3.access-key-id".to_string(), "AKID123".to_string());
        config.insert("s3.secret-access-key".to_string(), "SECRET456".to_string());

        let creds = extract_from_table_config(&config).unwrap();
        assert_eq!(creds.access_key, "AKID123");
        assert_eq!(creds.secret_key, "SECRET456");
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn test_extract_missing_access_key() {
        let mut config = HashMap::new();
        config.insert("s3.secret-access-key".to_string(), "SECRET456".to_string());

        assert!(extract_from_table_config(&config).is_none());
    }

    #[test]
    fn test_extract_empty_config() {
        let config = HashMap::new();
        assert!(extract_from_table_config(&config).is_none());
    }

    #[test]
    fn test_extract_with_epoch_expiry() {
        let mut config = HashMap::new();
        config.insert("s3.access-key-id".to_string(), "AKID123".to_string());
        config.insert("s3.secret-access-key".to_string(), "SECRET456".to_string());
        config.insert(
            "s3.session-token-expiry".to_string(),
            "1700000000000".to_string(),
        );

        let creds = extract_from_table_config(&config).unwrap();
        assert!(creds.expiry.is_some());
    }
}

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct SqeConfig {
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    pub auth: AuthConfig,
    pub catalog: CatalogConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default = "default_flight_port")]
    pub flight_sql_port: u16,
    #[serde(default = "default_trino_port")]
    pub trino_http_port: u16,
    #[serde(default = "default_mode")]
    pub mode: String,
    /// List of worker Flight server URLs for distributed execution.
    /// Empty = single-node mode (all queries execute locally).
    #[serde(default)]
    pub worker_urls: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_worker_flight_port")]
    pub flight_port: u16,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_true")]
    pub spill_to_disk: bool,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            coordinator_url: String::new(),
            flight_port: default_worker_flight_port(),
            heartbeat_interval_secs: default_heartbeat(),
            memory_limit: default_memory(),
            spill_to_disk: true,
            spill_dir: default_spill_dir(),
        }
    }
}

/// Parse a human-readable memory size string (e.g. "1GB", "512MB", "1024") into bytes.
///
/// Supported suffixes (case-insensitive): `B`, `KB`/`K`, `MB`/`M`, `GB`/`G`, `TB`/`T`.
/// A bare number without a suffix is interpreted as bytes.
pub fn parse_memory_limit(s: &str) -> crate::error::Result<usize> {
    let s = s.trim();
    if s.is_empty() {
        return Err(crate::error::SqeError::Config(
            "Empty memory limit string".to_string(),
        ));
    }

    // Find where the numeric part ends and the suffix begins
    let (num_str, suffix) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(idx) => (&s[..idx], s[idx..].trim().to_uppercase()),
        None => (s, String::new()),
    };

    let num: f64 = num_str.parse().map_err(|e| {
        crate::error::SqeError::Config(format!("Invalid memory limit number '{num_str}': {e}"))
    })?;

    let multiplier: f64 = match suffix.as_str() {
        "" | "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => {
            return Err(crate::error::SqeError::Config(format!(
                "Unknown memory limit suffix '{other}' in '{s}'"
            )))
        }
    };

    Ok((num * multiplier) as usize)
}

#[derive(Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(default)]
    pub keycloak_url: String,
    #[serde(default)]
    pub realm: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Generic OAuth2 token endpoint for client_credentials grant.
    /// When set (and keycloak_url is empty), the engine uses client_credentials mode.
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("keycloak_url", &self.keycloak_url)
            .field("realm", &self.realm)
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .field("token_endpoint", &self.token_endpoint)
            .field("token_refresh_buffer_secs", &self.token_refresh_buffer_secs)
            .field("ssl_verification", &self.ssl_verification)
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    pub polaris_url: String,
    #[serde(default)]
    pub warehouse: String,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
    /// Default Iceberg table format version for new tables (2 or 3).
    #[serde(default = "default_table_format_version")]
    pub default_table_format_version: u8,
}

#[derive(Deserialize, Clone, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_region: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
    #[serde(default)]
    pub s3_path_style: bool,
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_path_style", &self.s3_path_style)
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PolicyConfig {
    #[serde(default = "default_passthrough")]
    pub engine: String,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self { engine: "passthrough".to_string() }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    #[serde(default)]
    pub otlp_endpoint: String,
    #[serde(default)]
    pub audit_log_path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_port: 9090,
            otlp_endpoint: String::new(),
            audit_log_path: String::new(),
        }
    }
}

fn default_flight_port() -> u16 { 50051 }
fn default_trino_port() -> u16 { 8080 }
fn default_mode() -> String { "hybrid".to_string() }
fn default_worker_flight_port() -> u16 { 50052 }
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
fn default_refresh_buffer() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_cache_ttl() -> u64 { 30 }
fn default_table_format_version() -> u8 { 2 }
fn default_passthrough() -> String { "passthrough".to_string() }
fn default_prometheus_port() -> u16 { 9090 }

impl SqeConfig {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to read {path}: {e}")))?;
        let mut config: Self = toml::from_str(&content)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to parse config: {e}")))?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Apply environment variable overrides. Convention: `SQE_<SECTION>__<FIELD>`.
    /// E.g. `SQE_AUTH__KEYCLOAK_URL=https://...` overrides `auth.keycloak_url`.
    fn apply_env_overrides(&mut self) {
        // Coordinator
        env_override_u16("SQE_COORDINATOR__FLIGHT_SQL_PORT", &mut self.coordinator.flight_sql_port);
        env_override_u16("SQE_COORDINATOR__TRINO_HTTP_PORT", &mut self.coordinator.trino_http_port);
        env_override_str("SQE_COORDINATOR__MODE", &mut self.coordinator.mode);

        // Worker
        env_override_str("SQE_WORKER__COORDINATOR_URL", &mut self.worker.coordinator_url);
        env_override_u16("SQE_WORKER__FLIGHT_PORT", &mut self.worker.flight_port);
        env_override_u64("SQE_WORKER__HEARTBEAT_INTERVAL_SECS", &mut self.worker.heartbeat_interval_secs);
        env_override_str("SQE_WORKER__MEMORY_LIMIT", &mut self.worker.memory_limit);
        env_override_bool("SQE_WORKER__SPILL_TO_DISK", &mut self.worker.spill_to_disk);
        env_override_str("SQE_WORKER__SPILL_DIR", &mut self.worker.spill_dir);

        // Auth
        env_override_str("SQE_AUTH__KEYCLOAK_URL", &mut self.auth.keycloak_url);
        env_override_str("SQE_AUTH__REALM", &mut self.auth.realm);
        env_override_str("SQE_AUTH__CLIENT_ID", &mut self.auth.client_id);
        env_override_str("SQE_AUTH__CLIENT_SECRET", &mut self.auth.client_secret);
        env_override_str("SQE_AUTH__TOKEN_ENDPOINT", &mut self.auth.token_endpoint);
        env_override_u64("SQE_AUTH__TOKEN_REFRESH_BUFFER_SECS", &mut self.auth.token_refresh_buffer_secs);
        env_override_bool("SQE_AUTH__SSL_VERIFICATION", &mut self.auth.ssl_verification);

        // Catalog
        env_override_str("SQE_CATALOG__POLARIS_URL", &mut self.catalog.polaris_url);
        env_override_str("SQE_CATALOG__WAREHOUSE", &mut self.catalog.warehouse);
        env_override_u64("SQE_CATALOG__METADATA_CACHE_TTL_SECS", &mut self.catalog.metadata_cache_ttl_secs);
        env_override_u8("SQE_CATALOG__DEFAULT_TABLE_FORMAT_VERSION", &mut self.catalog.default_table_format_version);

        // Storage
        env_override_str("SQE_STORAGE__S3_ENDPOINT", &mut self.storage.s3_endpoint);
        env_override_str("SQE_STORAGE__S3_REGION", &mut self.storage.s3_region);
        env_override_str("SQE_STORAGE__S3_ACCESS_KEY", &mut self.storage.s3_access_key);
        env_override_str("SQE_STORAGE__S3_SECRET_KEY", &mut self.storage.s3_secret_key);
        env_override_bool("SQE_STORAGE__S3_PATH_STYLE", &mut self.storage.s3_path_style);

        // Policy
        env_override_str("SQE_POLICY__ENGINE", &mut self.policy.engine);

        // Metrics
        env_override_u16("SQE_METRICS__PROMETHEUS_PORT", &mut self.metrics.prometheus_port);
        env_override_str("SQE_METRICS__OTLP_ENDPOINT", &mut self.metrics.otlp_endpoint);
        env_override_str("SQE_METRICS__AUDIT_LOG_PATH", &mut self.metrics.audit_log_path);
    }
}

fn env_override_str(key: &str, target: &mut String) {
    if let Ok(val) = std::env::var(key) {
        *target = val;
    }
}

fn env_override_u8(key: &str, target: &mut u8) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u8, ignoring");
        }
    }
}

fn env_override_u16(key: &str, target: &mut u16) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u16, ignoring");
        }
    }
}

fn env_override_u64(key: &str, target: &mut u64) {
    if let Ok(val) = std::env::var(key) {
        if let Ok(parsed) = val.parse() {
            *target = parsed;
        } else {
            tracing::warn!("{key}={val:?} is not a valid u64, ignoring");
        }
    }
}

fn env_override_bool(key: &str, target: &mut bool) {
    if let Ok(val) = std::env::var(key) {
        match val.to_lowercase().as_str() {
            "true" | "1" | "yes" => *target = true,
            "false" | "0" | "no" => *target = false,
            _ => tracing::warn!("{key}={val:?} is not a valid bool, ignoring"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_limit_bytes() {
        assert_eq!(parse_memory_limit("1024").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024B").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1024b").unwrap(), 1024);
    }

    #[test]
    fn test_parse_memory_limit_kilobytes() {
        assert_eq!(parse_memory_limit("1K").unwrap(), 1024);
        assert_eq!(parse_memory_limit("1KB").unwrap(), 1024);
        assert_eq!(parse_memory_limit("2kb").unwrap(), 2048);
    }

    #[test]
    fn test_parse_memory_limit_megabytes() {
        assert_eq!(parse_memory_limit("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("512MB").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_gigabytes() {
        assert_eq!(parse_memory_limit("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("8GB").unwrap(), 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_terabytes() {
        assert_eq!(
            parse_memory_limit("1TB").unwrap(),
            1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_parse_memory_limit_whitespace() {
        assert_eq!(parse_memory_limit("  1GB  ").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_limit_empty() {
        assert!(parse_memory_limit("").is_err());
        assert!(parse_memory_limit("   ").is_err());
    }

    #[test]
    fn test_parse_memory_limit_invalid() {
        assert!(parse_memory_limit("abc").is_err());
        assert!(parse_memory_limit("GB").is_err());
    }

    #[test]
    fn test_parse_memory_limit_unknown_suffix() {
        assert!(parse_memory_limit("100XB").is_err());
    }

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.memory_limit, "8GB");
        assert!(config.spill_to_disk);
        assert_eq!(config.spill_dir, "/tmp/sqe-spill");
        assert_eq!(config.flight_port, 50052);
        assert_eq!(config.heartbeat_interval_secs, 5);
    }
}

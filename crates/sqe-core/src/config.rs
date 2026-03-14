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
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct WorkerConfig {
    #[serde(default)]
    pub coordinator_url: String,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_memory")]
    pub memory_limit: String,
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub keycloak_url: String,
    pub realm: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default = "default_refresh_buffer")]
    pub token_refresh_buffer_secs: u64,
    #[serde(default = "default_true")]
    pub ssl_verification: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    pub polaris_url: String,
    #[serde(default)]
    pub warehouse: String,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
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
fn default_heartbeat() -> u64 { 5 }
fn default_memory() -> String { "8GB".to_string() }
fn default_spill_dir() -> String { "/tmp/sqe-spill".to_string() }
fn default_refresh_buffer() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_cache_ttl() -> u64 { 30 }
fn default_passthrough() -> String { "passthrough".to_string() }
fn default_prometheus_port() -> u16 { 9090 }

impl SqeConfig {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to read {path}: {e}")))?;
        toml::from_str(&content)
            .map_err(|e| crate::error::SqeError::Config(format!("Failed to parse config: {e}")))
    }
}

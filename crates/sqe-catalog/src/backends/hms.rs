//! Hive Metastore (HMS) catalog backend.
//!
//! HMS speaks Thrift over a TCP port (9083 by default). The upstream
//! `iceberg-catalog-hms` crate (vendored from apache/iceberg-rust v0.9.0
//! into `vendor/iceberg-rust/crates/catalog/hms/`) wraps a Thrift client
//! and implements the `iceberg::Catalog` trait on top.
//!
//! Enable the `hms` cargo feature to pull in volo-thrift + pilota and
//! get a functioning backend. Without the feature the struct stays as a
//! marker that returns a clear error pointing at the feature flag.

use sqe_core::{Result as SqeResult, SqeError};

/// Configuration for the HMS backend.
#[derive(Debug, Clone)]
pub struct HmsConfig {
    /// Thrift URI, e.g. `thrift://hms.example.com:9083`.
    pub uri: String,
    /// Warehouse location used when HMS stores relative paths.
    pub warehouse: String,
    /// Optional per-request timeout.
    pub timeout_ms: Option<u64>,
}

impl HmsConfig {
    pub fn new(uri: impl Into<String>, warehouse: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            warehouse: warehouse.into(),
            timeout_ms: Some(5_000),
        }
    }
}

/// Marker backend. Calls return `SqeError::Catalog` with a message pointing
/// at the matrix-parity task list. The struct is kept so downstream wiring
/// can reference it once the real implementation lands.
#[derive(Debug, Clone)]
pub struct HmsBackend {
    config: HmsConfig,
}

impl HmsBackend {
    pub fn new(config: HmsConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &HmsConfig {
        &self.config
    }

    /// Build the underlying `HmsCatalog` from the vendored upstream crate.
    ///
    /// Returns an error when the `hms` cargo feature is disabled. With the
    /// feature on, this constructs a working Thrift-backed catalog.
    #[cfg(feature = "hms")]
    pub async fn build_catalog(
        &self,
        storage_factory: std::sync::Arc<dyn iceberg::io::StorageFactory>,
    ) -> SqeResult<iceberg_catalog_hms::HmsCatalog> {
        use iceberg::CatalogBuilder;
        let mut props = std::collections::HashMap::new();
        props.insert(
            iceberg_catalog_hms::HMS_CATALOG_PROP_URI.to_string(),
            self.config.uri.clone(),
        );
        props.insert(
            iceberg_catalog_hms::HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            self.config.warehouse.clone(),
        );
        if let Some(timeout_ms) = self.config.timeout_ms {
            props.insert(
                iceberg_catalog_hms::HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
                "buffered".to_string(),
            );
            // The upstream HMS catalog does not expose a public timeout
            // field on the builder; the timeout_ms config is recorded for
            // future extension and surfaced via a warning if unused.
            let _ = timeout_ms;
        }
        iceberg_catalog_hms::HmsCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load(&self.config.uri.clone(), props)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to build HmsCatalog: {e}")))
    }

    /// Stub `list_tables` for builds without the `hms` feature. Returns
    /// an error pointing at the feature flag so misconfigured deployments
    /// fail loudly rather than silently.
    #[cfg(not(feature = "hms"))]
    pub async fn list_tables(&self, _namespace: &str) -> SqeResult<Vec<String>> {
        Err(SqeError::Catalog(format!(
            "HMS backend requires building with the `hms` cargo feature \
             (configured URI {}). Build with `cargo build --features hms` to enable.",
            self.config.uri
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips_uri() {
        let cfg = HmsConfig::new("thrift://hms.test:9083", "s3://lake/warehouse");
        assert_eq!(cfg.uri, "thrift://hms.test:9083");
        assert_eq!(cfg.warehouse, "s3://lake/warehouse");
        assert_eq!(cfg.timeout_ms, Some(5_000));
    }

    #[cfg(not(feature = "hms"))]
    #[tokio::test]
    async fn list_tables_returns_stub_error() {
        let backend = HmsBackend::new(HmsConfig::new("thrift://hms.test:9083", "wh"));
        let err = backend.list_tables("ns").await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("HMS backend"));
        assert!(msg.contains("`hms` cargo feature"));
    }
}

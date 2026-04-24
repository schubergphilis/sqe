//! Hive Metastore (HMS) catalog backend.
//!
//! HMS speaks Thrift over a TCP port (9083 by default). The upstream
//! `iceberg-catalog-hms` crate wraps a Thrift client and implements the
//! `iceberg::Catalog` trait on top; SQE adopts that crate once the vendored
//! RisingWave fork rebases onto an apache/iceberg-rust release that exports
//! compatible types. Until then this module carries a configuration type and
//! a marker struct so that the `hms` Cargo feature is a no-op with a clear
//! error message.
//!
//! The HMS write path acquires a table-level lock via `lock`/`unlock` RPCs
//! before committing a new metadata pointer. The spec in
//! `openspec/changes/iceberg-matrix-parity/specs/catalog-backends/spec.md`
//! requires this; the real implementation tracks Phase A of the matrix plan.

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

    /// List tables in a namespace. Today this returns an error; the real
    /// implementation lands in Phase A task 2.7.
    pub async fn list_tables(&self, _namespace: &str) -> SqeResult<Vec<String>> {
        Err(SqeError::Catalog(format!(
            "HMS backend not yet wired to upstream iceberg-catalog-hms (configured URI {}); \
             tracked in openspec/changes/iceberg-matrix-parity/tasks.md task 2.7",
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

    #[tokio::test]
    async fn list_tables_returns_stub_error() {
        let backend = HmsBackend::new(HmsConfig::new("thrift://hms.test:9083", "wh"));
        let err = backend.list_tables("ns").await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("HMS backend"));
        assert!(msg.contains("task 2.7"));
    }
}

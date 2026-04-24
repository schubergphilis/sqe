//! AWS Glue Data Catalog backend.
//!
//! Glue is a hosted Iceberg-compatible catalog. The upstream
//! `iceberg-catalog-glue` crate drives it via the AWS SDK; SQE adopts it
//! once the vendored fork rebases onto a release that exports compatible
//! types. Today this module is a marker with a typed config struct so the
//! catalog registry can accept `catalog.type = "glue"` in `sqe.toml` and
//! return a clear error.
//!
//! Real implementation lands in Phase A task 2.3. Until then, the `glue`
//! Cargo feature pulls in this module but no AWS SDK; builds stay small.

use sqe_core::{Result as SqeResult, SqeError};

/// Configuration for the Glue backend.
#[derive(Debug, Clone)]
pub struct GlueConfig {
    /// AWS region, e.g. `eu-west-1`. Required by the SDK.
    pub region: String,
    /// Warehouse path for new-table default locations, e.g. `s3://lake/wh`.
    pub warehouse: String,
    /// Optional custom endpoint (for LocalStack or a VPC endpoint).
    pub endpoint: Option<String>,
}

impl GlueConfig {
    pub fn new(region: impl Into<String>, warehouse: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            warehouse: warehouse.into(),
            endpoint: None,
        }
    }
}

/// Marker backend. Methods return errors pointing at the Phase A task list.
#[derive(Debug, Clone)]
pub struct GlueBackend {
    config: GlueConfig,
}

impl GlueBackend {
    pub fn new(config: GlueConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &GlueConfig {
        &self.config
    }

    /// List databases (Glue calls namespaces `Database`). Stub.
    pub async fn list_databases(&self) -> SqeResult<Vec<String>> {
        Err(SqeError::Catalog(format!(
            "Glue backend not yet wired to upstream iceberg-catalog-glue (region {}); \
             tracked in openspec/changes/iceberg-matrix-parity/tasks.md task 2.3",
            self.config.region
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_endpoint_to_none() {
        let cfg = GlueConfig::new("eu-west-1", "s3://lake/wh");
        assert_eq!(cfg.region, "eu-west-1");
        assert!(cfg.endpoint.is_none());
    }

    #[tokio::test]
    async fn list_databases_returns_stub_error() {
        let backend = GlueBackend::new(GlueConfig::new("eu-west-1", "s3://lake/wh"));
        let err = backend.list_databases().await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Glue backend"));
        assert!(msg.contains("task 2.3"));
    }
}

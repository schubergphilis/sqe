//! AWS Glue Data Catalog backend.
//!
//! Glue is a hosted Iceberg-compatible catalog. The upstream
//! `iceberg-catalog-glue` crate (vendored from apache/iceberg-rust v0.9.0
//! into `vendor/iceberg-rust/crates/catalog/glue/`) drives Glue via the
//! AWS SDK and implements the `iceberg::Catalog` trait.
//!
//! Enable the `glue` cargo feature to pull in aws-sdk-glue + aws-config
//! and get a functioning backend. Without the feature the struct stays
//! a marker that fails loudly with a pointer at the feature flag.

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

    /// Build the underlying `GlueCatalog` from the vendored upstream crate.
    ///
    /// Returns an error when the `glue` cargo feature is disabled. With the
    /// feature on, this constructs a working AWS SDK-backed catalog.
    #[cfg(feature = "glue")]
    pub async fn build_catalog(
        &self,
        storage_factory: std::sync::Arc<dyn iceberg::io::StorageFactory>,
    ) -> SqeResult<iceberg_catalog_glue::GlueCatalog> {
        use iceberg::CatalogBuilder;
        let mut props = std::collections::HashMap::new();
        props.insert(
            iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            self.config.warehouse.clone(),
        );
        props.insert(
            iceberg_catalog_glue::AWS_REGION_NAME.to_string(),
            self.config.region.clone(),
        );
        if let Some(endpoint) = self.config.endpoint.as_ref() {
            props.insert(
                iceberg_catalog_glue::GLUE_CATALOG_PROP_URI.to_string(),
                endpoint.clone(),
            );
        }
        iceberg_catalog_glue::GlueCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("glue", props)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to build GlueCatalog: {e}")))
    }

    /// Stub `list_databases` for builds without the `glue` feature.
    #[cfg(not(feature = "glue"))]
    pub async fn list_databases(&self) -> SqeResult<Vec<String>> {
        Err(SqeError::Catalog(format!(
            "Glue backend requires building with the `glue` cargo feature \
             (region {}). Build with `cargo build --features glue` to enable.",
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

    #[cfg(not(feature = "glue"))]
    #[tokio::test]
    async fn list_databases_returns_stub_error() {
        let backend = GlueBackend::new(GlueConfig::new("eu-west-1", "s3://lake/wh"));
        let err = backend.list_databases().await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Glue backend"));
        assert!(msg.contains("`glue` cargo feature"));
    }
}

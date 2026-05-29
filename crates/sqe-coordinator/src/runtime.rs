//! Coordinator runtime configuration for DataFusion.
//!
//! Configures a shared DataFusion [`RuntimeEnv`] with memory limits and
//! spill-to-disk support based on [`CoordinatorConfig`]. The runtime is built
//! once at coordinator startup and reused across all queries so the memory pool
//! is shared and enforced globally.

use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tracing::info;

use sqe_core::config::{CoordinatorConfig, StorageConfig};
use sqe_core::parse_memory_limit;

/// Build a DataFusion [`RuntimeEnv`] for the coordinator with memory limits
/// and spill-to-disk support.
///
/// - Memory is managed via a [`FairSpillPool`] set on the runtime via
///   `RuntimeEnvBuilder::with_memory_pool`.
/// - When `spill_to_disk` is `true`, the spill directory is set via
///   `RuntimeEnvBuilder::with_temp_file_path`.
/// - When `spill_to_disk` is `false`, the disk manager is disabled via
///   `DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)`.
///
/// `storage` is the coordinator-wide `[storage]` block; it is handed to the
/// object-store registry so file-reader TVFs (`read_csv` / `read_parquet`) can
/// lazily build `s3://` stores for ad-hoc buckets that were never
/// pre-registered as Iceberg catalogs.
pub fn build_coordinator_runtime(
    config: &CoordinatorConfig,
    storage: &StorageConfig,
) -> anyhow::Result<Arc<RuntimeEnv>> {
    let memory_bytes = parse_memory_limit(&config.memory_limit).map_err(|e| {
        anyhow::anyhow!(
            "Invalid coordinator memory_limit '{}': {e}",
            config.memory_limit
        )
    })?;

    info!(
        memory_limit = %config.memory_limit,
        memory_bytes = memory_bytes,
        spill_to_disk = config.spill_to_disk,
        spill_dir = %config.spill_dir,
        spill_compression = %config.spill_compression,
        "Configuring coordinator DataFusion runtime"
    );

    // Use FairSpillPool directly — it divides memory fairly among spillable
    // operators and triggers spill when the limit is reached.
    let memory_pool = Arc::new(FairSpillPool::new(memory_bytes));

    // V10 httpfs: wrap the default ObjectStoreRegistry so http(s) URLs in
    // file-format TVFs (read_csv / read_json / read_parquet) get a backing
    // HttpStore built lazily on first request. `with_s3_fallback` additionally
    // builds `s3://` stores from the coordinator `[storage]` config on demand,
    // so the same TVFs can read ad-hoc S3 buckets that were never
    // pre-registered as Iceberg catalogs. `file` paths use the default
    // registry's existing path.
    let registry =
        Arc::new(sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::with_s3_fallback(
            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
            storage.clone(),
        ));
    let mut builder = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_object_store_registry(registry);

    if config.spill_to_disk {
        // Create spill directory if it doesn't exist
        std::fs::create_dir_all(&config.spill_dir).map_err(|e| {
            anyhow::anyhow!(
                "Failed to create coordinator spill directory '{}': {e}",
                config.spill_dir
            )
        })?;
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        // Disable disk manager — any attempt to spill will return an error.
        let disk_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    Ok(Arc::new(builder.build()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryLimit;

    /// Helper: create a CoordinatorConfig with spill disabled.
    fn config_no_spill(memory_limit: &str) -> CoordinatorConfig {
        let toml_src = format!(
            r#"
            memory_limit = "{memory_limit}"
            spill_to_disk = false
            spill_dir = "/tmp/sqe-test-coordinator-spill"
            "#
        );
        toml::from_str(&toml_src).expect("valid CoordinatorConfig toml")
    }

    #[test]
    fn test_default_memory_limit_applied() {
        let config = config_no_spill("8GB");
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default()).expect("should build");

        // 8GB = 8 * 1024^3 = 8_589_934_592 bytes
        let expected_bytes = 8 * 1024 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_custom_memory_limit_512mb() {
        let config = config_no_spill("512MB");
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default()).expect("should build with 512MB limit");

        let expected_bytes = 512 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_spill_disabled() {
        let config = config_no_spill("1GB");
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default()).expect("should build with spill disabled");

        assert!(
            !runtime.disk_manager.tmp_files_enabled(),
            "DiskManager should be disabled when spill_to_disk is false"
        );
    }

    #[test]
    fn test_spill_enabled_creates_dir_and_enables_disk_manager() {
        let tmpdir = std::env::temp_dir().join("sqe-test-coordinator-spill-enabled");
        // Clean up from previous test runs
        let _ = std::fs::remove_dir_all(&tmpdir);

        let config = CoordinatorConfig {
            memory_limit: "1GB".to_string(),
            spill_to_disk: true,
            spill_dir: tmpdir.to_string_lossy().to_string(),
            ..config_no_spill("1GB")
        };
        let runtime =
            build_coordinator_runtime(&config, &StorageConfig::default()).expect("should build with spill enabled");

        assert!(
            runtime.disk_manager.tmp_files_enabled(),
            "DiskManager should be enabled when spill_to_disk is true"
        );
        assert!(tmpdir.exists(), "Spill directory should have been created");

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmpdir);
    }

    #[test]
    fn test_invalid_memory_limit_errors() {
        let config = config_no_spill("not_a_number");
        let result = build_coordinator_runtime(&config, &StorageConfig::default());
        assert!(result.is_err(), "Should error on invalid memory limit");
    }
}

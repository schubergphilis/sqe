//! Worker runtime configuration for DataFusion.
//!
//! Configures the DataFusion [`SessionContext`] with memory limits and
//! spill-to-disk support based on [`WorkerConfig`].

use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use tracing::info;

use sqe_core::config::WorkerConfig;
use sqe_core::parse_memory_limit;

/// Build a DataFusion [`SessionContext`] configured with the memory limit
/// and spill-to-disk settings from [`WorkerConfig`].
///
/// - Memory is managed via a [`FairSpillPool`] set directly on the runtime
///   via `RuntimeEnvBuilder::with_memory_pool`.
/// - When `spill_to_disk` is `true`, the spill directory is set to
///   `config.spill_dir` via `RuntimeEnvBuilder::with_temp_file_path`.
/// - When `spill_to_disk` is `false`, the disk manager is disabled via
///   `DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)`.
pub fn build_session_context(config: &WorkerConfig) -> anyhow::Result<SessionContext> {
    let memory_bytes = parse_memory_limit(&config.memory_limit).map_err(|e| {
        anyhow::anyhow!("Invalid worker memory_limit '{}': {e}", config.memory_limit)
    })?;

    info!(
        memory_limit = %config.memory_limit,
        memory_bytes = memory_bytes,
        spill_to_disk = config.spill_to_disk,
        spill_dir = %config.spill_dir,
        "Configuring DataFusion runtime"
    );

    // Use FairSpillPool directly — it divides memory fairly among spillable
    // operators and triggers spill when the limit is reached.
    let memory_pool = Arc::new(FairSpillPool::new(memory_bytes));

    let mut builder = RuntimeEnvBuilder::new().with_memory_pool(memory_pool);

    if config.spill_to_disk {
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        // Disable disk manager — any attempt to spill will return an error.
        let disk_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    let runtime = Arc::new(builder.build()?);
    let session_config = SessionConfig::new()
        // Parquet filter pushdown DISABLED: DF epic #20324 causes
        // "Invalid comparison: Utf8 >= Int32" on mixed-type predicates.
        // SQE's own late materialization path handles this safely.
        ;
    let ctx = SessionContext::new_with_config_rt(session_config, runtime);

    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryLimit;

    /// Helper: create a WorkerConfig with spill disabled so tests that only
    /// care about memory limits don't need to create temporary directories.
    fn config_no_spill(memory_limit: &str) -> WorkerConfig {
        WorkerConfig {
            memory_limit: memory_limit.to_string(),
            spill_to_disk: false,
            ..Default::default()
        }
    }

    #[test]
    fn test_default_memory_limit_applied() {
        // Use spill_to_disk=false to avoid filesystem side effects.
        let config = config_no_spill("8GB");
        let ctx = build_session_context(&config).expect("should build");
        let runtime = ctx.runtime_env();

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
        let ctx = build_session_context(&config).expect("should build with 512MB limit");
        let runtime = ctx.runtime_env();

        let expected_bytes = 512 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_memory_limit_1gb() {
        let config = config_no_spill("1GB");
        let ctx = build_session_context(&config).expect("should build with 1GB limit");
        let runtime = ctx.runtime_env();

        let expected_bytes = 1024 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_spill_disabled() {
        let config = WorkerConfig {
            spill_to_disk: false,
            ..Default::default()
        };
        let ctx = build_session_context(&config).expect("should build with spill disabled");
        let runtime = ctx.runtime_env();

        assert!(
            !runtime.disk_manager.tmp_files_enabled(),
            "DiskManager should be disabled when spill_to_disk is false"
        );
    }

    #[test]
    fn test_spill_enabled_uses_temp_dir() {
        let tmpdir = std::env::temp_dir().join("sqe-test-spill-enabled");
        let config = WorkerConfig {
            spill_to_disk: true,
            spill_dir: tmpdir.to_string_lossy().to_string(),
            ..Default::default()
        };
        let ctx = build_session_context(&config).expect("should build with spill enabled");
        let runtime = ctx.runtime_env();

        assert!(
            runtime.disk_manager.tmp_files_enabled(),
            "DiskManager should be enabled when spill_to_disk is true"
        );
    }

    #[test]
    fn test_invalid_memory_limit_errors() {
        let config = WorkerConfig {
            memory_limit: "not_a_number".to_string(),
            spill_to_disk: false,
            ..Default::default()
        };
        let result = build_session_context(&config);
        assert!(result.is_err(), "Should error on invalid memory limit");
    }
}

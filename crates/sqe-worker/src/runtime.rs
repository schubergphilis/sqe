//! Worker runtime configuration for DataFusion.
//!
//! Configures the DataFusion [`SessionContext`] with memory limits and
//! spill-to-disk support based on [`WorkerConfig`]. Uses a
//! [`ResizableFairSpillPool`] so hot config reload can change the limit.

use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sqe_spill::ResizableFairSpillPool;
use tracing::info;

use sqe_core::config::WorkerConfig;
use sqe_core::parse_memory_limit;

/// Build a DataFusion [`SessionContext`] and the shared resizable memory pool.
///
/// The pool is returned so bootstrap can wire hot-reload without downcasting
/// through `dyn MemoryPool`.
pub fn build_session_context(
    config: &WorkerConfig,
) -> anyhow::Result<(SessionContext, Arc<ResizableFairSpillPool>)> {
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

    let memory_pool = Arc::new(ResizableFairSpillPool::new(memory_bytes));
    let ctx = build_session_context_with_pool(config, memory_pool.clone())?;
    Ok((ctx, memory_pool))
}

/// Build a session context using an existing resizable pool (tests / custom wiring).
pub fn build_session_context_with_pool(
    config: &WorkerConfig,
    memory_pool: Arc<ResizableFairSpillPool>,
) -> anyhow::Result<SessionContext> {
    let memory_bytes = memory_pool.pool_size();
    let pool: Arc<dyn MemoryPool> = memory_pool;
    let mut builder = RuntimeEnvBuilder::new().with_memory_pool(pool);

    if config.spill_to_disk {
        builder = builder.with_temp_file_path(&config.spill_dir);
    } else {
        let disk_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        builder = builder.with_disk_manager_builder(disk_builder);
    }

    let runtime = Arc::new(builder.build()?);

    let sort_spill_reservation = (memory_bytes / 4).max(1024 * 1024);
    let sort_in_place = (memory_bytes / 16).max(64 * 1024);
    let mut session_config = SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);
    {
        let opts = session_config.options_mut();
        opts.execution.sort_spill_reservation_bytes = sort_spill_reservation;
        opts.execution.sort_in_place_threshold_bytes = sort_in_place;
    }
    info!(
        sort_spill_reservation_bytes = sort_spill_reservation,
        sort_in_place_threshold_bytes = sort_in_place,
        "Configured DataFusion sort spill reservation (merge headroom)"
    );

    Ok(SessionContext::new_with_config_rt(session_config, runtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryLimit;

    fn config_no_spill(memory_limit: &str) -> WorkerConfig {
        WorkerConfig {
            memory_limit: memory_limit.to_string(),
            spill_to_disk: false,
            ..Default::default()
        }
    }

    #[test]
    fn test_default_memory_limit_applied() {
        let config = config_no_spill("8GB");
        let (ctx, pool) = build_session_context(&config).expect("should build");
        let runtime = ctx.runtime_env();

        let expected_bytes = 8 * 1024 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
        assert_eq!(pool.pool_size(), expected_bytes);
    }

    #[test]
    fn test_custom_memory_limit_512mb() {
        let config = config_no_spill("512MB");
        let (ctx, _) = build_session_context(&config).expect("should build with 512MB limit");
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
        let (ctx, _) = build_session_context(&config).expect("should build with 1GB limit");
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
        let (ctx, _) = build_session_context(&config).expect("should build with spill disabled");
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
        let (ctx, _) = build_session_context(&config).expect("should build with spill enabled");
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

    #[test]
    fn hot_resize_pool_visible_on_runtime() {
        let config = config_no_spill("64MB");
        let (ctx, pool) = build_session_context(&config).expect("build");
        pool.set_pool_size(128 * 1024 * 1024);
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(n) => assert_eq!(n, 128 * 1024 * 1024),
            _ => panic!("expected finite"),
        }
    }
}

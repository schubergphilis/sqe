//! Coordinator runtime configuration for DataFusion.
//!
//! Configures a shared DataFusion [`RuntimeEnv`] with memory limits and
//! spill-to-disk support based on [`CoordinatorConfig`]. The runtime is built
//! once at coordinator startup and reused across all queries so the memory pool
//! is shared and enforced globally.

use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tracing::info;

use sqe_core::config::{
    CoordinatorConfig, MemoryPoolKind, StorageConfig, DEFAULT_COORDINATOR_MEMORY_LIMIT,
};
use sqe_core::{parse_memory_limit, RuntimeMemoryInfo};

/// Resolve `coordinator.memory_limit` before anything reads it.
///
/// `"auto"` becomes enforced cgroup limit x `memory_limit_auto_fraction` -
/// `process_headroom`, or [`DEFAULT_COORDINATOR_MEMORY_LIMIT`] when no limit
/// is enforced. The resolved byte count is written back into
/// `config.memory_limit`, because the runtime, admission, write and
/// maintenance paths each parse that string on their own.
///
/// Fails closed when `memory_limit + process_headroom` exceeds the enforced
/// limit. Returns that sum, the need the runtime memory watch compares the
/// live limit against.
pub fn resolve_coordinator_memory(config: &mut CoordinatorConfig) -> anyhow::Result<u64> {
    resolve_coordinator_memory_with(config, &sqe_core::runtime_memory_info())
}

fn resolve_coordinator_memory_with(
    config: &mut CoordinatorConfig,
    mem: &RuntimeMemoryInfo,
) -> anyhow::Result<u64> {
    let headroom = parse_memory_limit(&config.process_headroom).map_err(|e| {
        anyhow::anyhow!(
            "Invalid coordinator process_headroom '{}': {e}",
            config.process_headroom
        )
    })? as u64;
    let auto = sqe_core::is_auto_memory_limit(&config.memory_limit);
    let limit = sqe_core::resolve_memory_limit(
        &config.memory_limit,
        DEFAULT_COORDINATOR_MEMORY_LIMIT,
        config.memory_limit_auto_fraction,
        headroom,
        mem.enforced_memory_limit_bytes,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "Invalid coordinator memory_limit '{}': {e}",
            config.memory_limit
        )
    })?;
    if auto {
        info!(
            memory_limit_bytes = limit,
            enforced_limit_bytes = ?mem.enforced_memory_limit_bytes,
            enforced_limit_source = mem.enforced_memory_limit_source,
            fraction = config.memory_limit_auto_fraction,
            process_headroom_bytes = headroom,
            "Resolved coordinator memory_limit = \"auto\""
        );
    }
    sqe_core::check_process_headroom("coordinator", limit, headroom, mem)
        .map_err(|e| anyhow::anyhow!(e))?;
    config.memory_limit = limit.to_string();
    Ok(limit.saturating_add(headroom))
}

/// Build the runtime memory pool from the `coordinator.memory_pool` knob.
///
/// `"greedy"` (default) hands out memory first-come first-served up to the
/// limit; spillable operators spill when the pool is genuinely full, and
/// [`TrackConsumersPool`] names the top consumers in exhaustion errors.
/// `"fair"` restores the previous [`FairSpillPool`], which statically divides
/// the pool across every registered spillable consumer. Wide plans register
/// dozens of consumers (operators x partitions), capping each at pool/N even
/// while most allocate nothing: TPC-DS q39 at SF10 registered ~90, capping
/// every consumer at ~95 MB of an 8 GB pool and failing a partial aggregate
/// that DataFusion 53 cannot emit early under a constant GROUP BY ordering
/// key. Unknown values warn and fall back to greedy.
///
/// The greedy pool keeps `headroom_percent` of itself for non-spillable
/// consumers ([`crate::memory_headroom`]); `fair` divides memory itself and
/// is not wrapped.
fn build_memory_pool(
    kind: MemoryPoolKind,
    memory_bytes: usize,
    headroom_percent: u8,
) -> Arc<dyn MemoryPool> {
    const TOP_CONSUMERS_IN_ERROR: usize = 5;
    let tracked_greedy = || {
        let pool = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_bytes),
            std::num::NonZeroUsize::new(TOP_CONSUMERS_IN_ERROR).expect("non-zero const"),
        ));
        // Keep the concrete handle for per-query consumer reporting
        // (crate::memory::tracked_pool_report).
        crate::memory::set_tracked_pool(Arc::clone(&pool));
        pool as Arc<dyn MemoryPool>
    };
    match kind {
        MemoryPoolKind::Fair => Arc::new(FairSpillPool::new(memory_bytes)),
        MemoryPoolKind::Greedy => crate::memory_headroom::HeadroomPool::wrap(
            tracked_greedy(),
            memory_bytes,
            headroom_percent,
        ),
    }
}

/// Build a DataFusion [`RuntimeEnv`] for the coordinator with memory limits
/// and spill-to-disk support.
///
/// - The memory pool comes from [`build_memory_pool`]: greedy behind the
///   unspillable headroom by default, `fair` on request.
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
        memory_pool = %config.memory_pool,
        unspillable_headroom_percent = config.unspillable_headroom_percent,
        spill_to_disk = config.spill_to_disk,
        spill_dir = %config.spill_dir,
        spill_compression = %config.spill_compression,
        "Configuring coordinator DataFusion runtime"
    );

    let memory_pool = build_memory_pool(
        config.memory_pool,
        memory_bytes,
        config.unspillable_headroom_percent,
    );

    // V10 httpfs: wrap the default ObjectStoreRegistry so http(s) URLs in
    // file-format TVFs (read_csv / read_json / read_parquet) get a backing
    // HttpStore built lazily on first request. `with_s3_fallback` additionally
    // builds `s3://` stores from the coordinator `[storage]` config on demand,
    // so the same TVFs can read ad-hoc S3 buckets that were never
    // pre-registered as Iceberg catalogs. `file` paths use the default
    // registry's existing path.
    let registry = Arc::new(
        sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::with_s3_fallback(
            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
            storage.clone(),
        ),
    );
    let mut builder = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_object_store_registry(registry)
        // #363: disable DataFusion 54's on-by-default, infinite-TTL
        // `list_files_cache` so directory reads of external mutable buckets
        // (read_parquet / read_csv over raw S3 that dbt writes into) always
        // list fresh instead of computing footer offsets from a stale size.
        .with_cache_manager(sqe_catalog::lazy_object_store::external_store_cache_config());

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
        let disk_builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
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
        let runtime =
            build_coordinator_runtime(&config, &StorageConfig::default()).expect("should build");

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
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default())
            .expect("should build with 512MB limit");

        let expected_bytes = 512 * 1024 * 1024;
        match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, expected_bytes),
            _ => panic!("Expected Finite memory limit"),
        }
    }

    #[test]
    fn test_memory_pool_knob() {
        // Default is greedy: a single consumer may take the whole pool.
        let config = config_no_spill("1GB");
        assert_eq!(config.memory_pool, MemoryPoolKind::Greedy);

        for kind in [MemoryPoolKind::Greedy, MemoryPoolKind::Fair] {
            let pool = build_memory_pool(kind, 1024 * 1024 * 1024, 20);
            match pool.memory_limit() {
                MemoryLimit::Finite(limit) => assert_eq!(limit, 1024 * 1024 * 1024),
                _ => panic!("Expected Finite memory limit for kind {kind}"),
            }
        }
        // The unspillable headroom wraps greedy only; fair divides the pool
        // itself, and 0 leaves greedy unwrapped.
        assert_eq!(
            build_memory_pool(MemoryPoolKind::Greedy, 1 << 30, 20).name(),
            "HeadroomPool"
        );
        assert_ne!(
            build_memory_pool(MemoryPoolKind::Fair, 1 << 30, 20).name(),
            "HeadroomPool"
        );
        assert_ne!(
            build_memory_pool(MemoryPoolKind::Greedy, 1 << 30, 0).name(),
            "HeadroomPool"
        );
    }

    #[test]
    fn test_spill_disabled() {
        let config = config_no_spill("1GB");
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default())
            .expect("should build with spill disabled");

        assert!(
            !runtime.disk_manager.tmp_files_enabled(),
            "DiskManager should be disabled when spill_to_disk is false"
        );
    }

    #[test]
    fn test_spill_enabled_creates_dir_and_enables_disk_manager() {
        // nosemgrep: opt.aikido.semgrep-rules.rust.lang.security.temp-dir -- test-only spill dir; production spill_dir is operator-configured
        let tmpdir = std::env::temp_dir().join("sqe-test-coordinator-spill-enabled");
        // Clean up from previous test runs
        let _ = std::fs::remove_dir_all(&tmpdir);

        let config = CoordinatorConfig {
            memory_limit: "1GB".to_string(),
            spill_to_disk: true,
            spill_dir: tmpdir.to_string_lossy().to_string(),
            ..config_no_spill("1GB")
        };
        let runtime = build_coordinator_runtime(&config, &StorageConfig::default())
            .expect("should build with spill enabled");

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

    const GIB: u64 = 1024 * 1024 * 1024;

    fn mem_with_enforced(enforced: Option<u64>) -> RuntimeMemoryInfo {
        let mut mem = sqe_core::runtime_memory_info();
        mem.enforced_memory_limit_bytes = enforced;
        mem
    }

    #[test]
    fn auto_memory_limit_is_written_back_as_bytes() {
        let mut config = config_no_spill("auto");
        config.memory_limit_auto_fraction = 0.5;
        config.process_headroom = "1GB".to_string();
        let need = resolve_coordinator_memory_with(&mut config, &mem_with_enforced(Some(16 * GIB)))
            .expect("auto resolves");
        assert_eq!(config.memory_limit, (7 * GIB).to_string());
        assert_eq!(need, 8 * GIB);
        // Every downstream reader re-parses the string; it must still parse.
        assert_eq!(
            parse_memory_limit(&config.memory_limit).unwrap() as u64,
            7 * GIB
        );
        build_coordinator_runtime(&config, &StorageConfig::default())
            .expect("runtime builds from the resolved limit");
    }

    #[test]
    fn auto_memory_limit_falls_back_without_a_cgroup_limit() {
        let mut config = config_no_spill("auto");
        resolve_coordinator_memory_with(&mut config, &mem_with_enforced(None))
            .expect("auto falls back");
        assert_eq!(
            config.memory_limit,
            (parse_memory_limit(DEFAULT_COORDINATOR_MEMORY_LIMIT).unwrap() as u64).to_string()
        );
    }

    #[test]
    fn explicit_memory_limit_over_the_cgroup_refuses_to_start() {
        let mut config = config_no_spill("8GB");
        config.process_headroom = "2GB".to_string();
        let err = resolve_coordinator_memory_with(&mut config, &mem_with_enforced(Some(8 * GIB)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("coordinator.memory_limit"), "{err}");
        // A refused config is left as written.
        assert_eq!(config.memory_limit, "8GB");
    }
}

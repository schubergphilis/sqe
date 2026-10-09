//! One integration-test binary for sqe-catalog.
//!
//! Each module used to be its own `tests/*.rs` binary, and every one of them
//! linked DataFusion, iceberg and the AWS SDK: 27 link units, about 130 CPU-s
//! of a cold `cargo test --workspace --no-run` (2026-09-25). One harness is one
//! link. Test names gain the module prefix (`hive_store_test::...`); substring
//! filters still match: `cargo test -p sqe-catalog --test it hive_store`.
//!
//! Feature-gated modules keep their own `#![cfg(feature = ...)]` line, so a
//! build without the feature compiles them out exactly as before.

#[cfg(feature = "hive-external")]
#[allow(dead_code)] // each consumer uses a different slice of the fixture
mod hive_fixture;

mod authorization_revalidation_test;
mod aws_config_test;
mod backends_integration;
mod bloom_probe_369;
mod catalog_http_timeout_test;
mod concurrent_bearer_test;
mod glue_source_live;
mod hive_manifest_test;
mod hive_mount_test;
mod hive_partition_bench;
mod hive_partition_test;
mod hive_plan_io_test;
mod hive_provider_test;
mod hive_scan_exec_test;
mod hive_scan_mode_test;
mod hive_serde_map_test;
mod hive_store_test;
mod mount_glue_test;
mod mount_hms_test;
mod mount_jdbc_test;
mod mount_rest_test;
mod mount_s3tables_test;
mod mount_sqlite_test;
mod native_trino_syntax;
mod schema_provider_concurrency_test;
mod sqlite_rs_catalog_test;
mod table_cache_state_machine_test;
mod trino_colname_compat;
mod trino_row_compat;

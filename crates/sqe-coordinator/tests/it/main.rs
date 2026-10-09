//! Consolidated integration-test harness for sqe-coordinator.
//!
//! Every module here used to be its own `tests/*.rs` binary. Twenty separate
//! binaries each linked DataFusion plus the whole engine: a one-line change
//! to the coordinator cost ~341s of mostly-serial link time before a single
//! test ran (measured 2026-06-12). One harness means one link. By 2026-09-25
//! seventeen new standalone files had grown back (about 145 CPU-s of a cold
//! test build); they are modules here now. Add new tests as a module, not as
//! a new `tests/*.rs` file.
//!
//! Test NAMES gain a module prefix (`integration_test::test_inner_join`),
//! but substring filters like `cargo test ... test_inner_join` still match.
//!
//! Modules gated on `test-sqlite` keep their `#![cfg(feature = "test-sqlite")]`
//! line and compile out without the feature; CI runs `--all-features`.
//!
//! Deliberately NOT consolidated:
//! - `tests/in_subquery_or_stack_overflow.rs`: its release-only deep-OR
//!   guards die by SIGABRT (OS-level stack overflow) on regression, which
//!   tears down the whole process; the file's own docs require one process
//!   per size.

mod common;
mod hive_diff;

mod access_control_e2e;
mod analyze_statement_test;
mod attach_dispatch_test;
mod attach_rest_test;
mod audit_e2e_test;
mod audit_planning_failure_test;
mod catalog_discovery_test;
mod compaction_benchmark;
mod compaction_distributed_benchmark;
mod ctas_write_modes_e2e;
mod distributed_failure_e2e;
mod equality_delete_integration;
mod flight_sql_smoke;
mod full_outer_window_join_test;
mod grant_dispatch_test;
mod grant_introspection_gate_test;
mod hive_ddl_test;
mod hive_differential_test;
mod hive_external_e2e;
mod hive_info_schema_policy_test;
mod hive_partition_ddl_test;
mod hive_scan_tasks_test;
mod hive_tag_source_test;
mod hive_write_test;
mod in_subquery_view_rewrite;
mod incremental_scan_e2e;
mod insert_overwrite_e2e;
mod integration_test;
mod join_fallback_e2e;
mod lease_cas_spike_test;
mod maintenance_lease_test;
mod maintenance_log_test;
mod maintenance_procedures_test;
mod maintenance_scheduler_test;
mod mcp_access_control_e2e;
mod mcp_describe_schema_e2e;
mod mcp_listener_supervision_e2e;
mod mor_update_merge_integration;
mod multi_catalog_routing_test;
mod namespace_staleness_test;
mod partition_e2e;
mod partition_evolution_e2e;
mod quack_e2e;
mod rewrite_data_files_deletes;
mod rewrite_data_files_distributed_parity;
mod rewrite_data_files_real;
mod rewrite_files_new_delete_conflict_test;
mod rewrite_require_mode_test;
mod rewrite_snapshot_properties_test;
mod runtime_catalog_test;
mod show_stats_policy_e2e;
mod spark_access_control_e2e;
mod spark_mask_parity_e2e;
mod sql_compat_test;
mod table_health_test;
mod trino_cli_test;
mod trino_describe_test;
mod trino_metadata_test;
mod trino_session_catalog_test;
mod v3_e2e;
mod v3_types_integration;
mod vended_credentials_e2e;
mod write_shape_e2e;

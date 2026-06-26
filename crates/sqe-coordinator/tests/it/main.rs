//! Consolidated integration-test harness for sqe-coordinator.
//!
//! Every module here used to be its own `tests/*.rs` binary. Twenty separate
//! binaries each linked DataFusion plus the whole engine: a one-line change
//! to the coordinator cost ~341s of mostly-serial link time before a single
//! test ran (measured 2026-06-12). One harness means one link.
//!
//! Test NAMES gain a module prefix (`integration_test::test_inner_join`),
//! but substring filters like `cargo test ... test_inner_join` still match.
//!
//! Deliberately NOT consolidated:
//! - `tests/runtime_catalog_test.rs`: gated behind `--features test-sqlite`
//!   with a crate-level `#![cfg]`, invoked separately in CI.
//! - `tests/in_subquery_or_stack_overflow.rs`: its release-only deep-OR
//!   guards die by SIGABRT (OS-level stack overflow) on regression, which
//!   tears down the whole process; the file's own docs require one process
//!   per size.

mod common;

mod attach_dispatch_test;
mod attach_rest_test;
mod audit_e2e_test;
mod catalog_discovery_test;
mod equality_delete_integration;
mod grant_dispatch_test;
mod grant_introspection_gate_test;
mod in_subquery_view_rewrite;
mod incremental_scan_e2e;
mod integration_test;
mod maintenance_procedures_test;
mod mor_update_merge_integration;
mod multi_catalog_routing_test;
mod partition_e2e;
mod partition_evolution_e2e;
mod quack_e2e;
mod rewrite_data_files_real;
mod sql_compat_test;
mod trino_metadata_test;
mod v3_e2e;
mod v3_types_integration;

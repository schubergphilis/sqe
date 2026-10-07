//! SQE write path, split out of `sqe-coordinator` (build-time item C2, plan
//! in `docs/internal/plans/2026-09-07-coordinator-crate-split.md`).
//!
//! This crate owns everything between a parsed write statement and a
//! committed Iceberg snapshot: [`write_handler`] (CTAS, INSERT, INSERT
//! OVERWRITE, UPDATE, DELETE, MERGE), [`writer`] (Parquet data and position
//! delete files), [`merge_sql`] and [`merge_target_provider`] (MERGE
//! rewriting), [`puffin_carry`] (statistics carry-forward), [`hive_write`]
//! (Hive external tables), plus the [`ident`] and [`commit`] helpers those
//! share with the coordinator's maintenance code.
//!
//! It depends on the shared tier (sqe-core, sqe-catalog, sqe-policy,
//! sqe-compaction, ...) but not on the coordinator. The coordinator reaches
//! back in through [`write_handler::CatalogDiscovery`], the only seam that
//! touches coordinator-owned session state. The coordinator re-exports every
//! module here under its old `sqe_coordinator::` path, so downstream code
//! compiles unchanged.

pub mod commit;
pub mod hive_write;
pub mod ident;
pub mod merge_sql;
pub mod merge_target_provider;
mod policy_view;
pub mod puffin_carry;
pub mod write_handler;
pub mod writer;

/// In-process test rigs (architecture review 513). Compiled for this crate's
/// own unit tests and, as public library code, for dependents' tests under
/// `--features test-sqlite`; never part of a production build.
#[cfg(any(test, feature = "test-sqlite"))]
pub mod test_support;

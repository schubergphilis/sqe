//! Placeholder for the Puffin-backed `StatisticsSource` consumer.
//!
//! Tasks 7.12-7.14 of the iceberg-matrix-parity Phase F deliver the CBO
//! wiring that reads theta-sketch NDV estimates from the Puffin sidecar
//! written by `crates/sqe-catalog/src/puffin_stats.rs`. The consumer side
//! waits for DataFusion 54 where the `StatisticsSource` trait lands:
//!
//! - Tracking issue: <https://github.com/apache/datafusion/issues/21157>
//! - SQE constraint: the RisingWave iceberg-rust fork still pins DataFusion
//!   52.1; SQE's DF 53 upgrade unblocks this once the fork rebases onto
//!   `main`. See `docs/matrix-parity-tracking-issue.md` for the schedule.
//!
//! ## Planned shape
//!
//! ```ignore
//! pub struct PuffinStatisticsSource {
//!     table: iceberg::table::Table,
//!     cache: moka::sync::Cache<i64, Arc<FileMetadata>>,
//! }
//!
//! #[async_trait]
//! impl StatisticsSource for PuffinStatisticsSource {
//!     async fn statistics(
//!         &self,
//!         snapshot_id: i64,
//!     ) -> Result<Statistics, DataFusionError> {
//!         // 1. Load the StatisticsFile entry for snapshot_id from
//!         //    table.metadata().statistics()
//!         // 2. Open the Puffin sidecar via table.file_io()
//!         // 3. For each blob, read the `ndv` property
//!         // 4. Build Statistics { num_rows, column_statistics: ColumnStatistics[] }
//!     }
//! }
//! ```
//!
//! See also `crates/sqe-catalog/src/puffin_stats.rs` for the writer side of
//! this round-trip.
//
// TODO(matrix-f): Phase F tasks 7.12-7.14 — implement PuffinStatisticsSource
// once DataFusion 54 (apache/datafusion#21157) lands in the SQE toolchain.

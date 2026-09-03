//! Placeholder for a Puffin-backed DataFusion statistics consumer.
//!
//! Tasks 7.12-7.14 of the iceberg-matrix-parity Phase F deliver the CBO
//! wiring that reads theta-sketch NDV estimates from the Puffin sidecar
//! written by `crates/sqe-catalog/src/puffin_stats.rs`. The consumer side
//! DataFusion 55 now exposes two relevant building blocks, but neither is a
//! drop-in consumer for Iceberg Puffin files:
//!
//! - `StatisticsRequest` is threaded through logical `TableScan` / `ScanArgs`,
//!   but SQE still needs an optimizer rule that requests the statistics and an
//!   Iceberg provider that reads the requested Puffin blobs.
//! - `StatisticsRegistry` / `StatisticsProvider` propagate statistics through
//!   physical operators. The registry is opt-in and affects join selection, so
//!   it must be enabled only after plan and benchmark validation.
//!
//! The implementation should load the snapshot's `StatisticsFile`, map blob
//! field IDs to columns, expose NDV/min/max through an Iceberg statistics
//! provider, add the matching request rule, and then validate join plans and
//! benchmark suites before enabling the physical registry. See
//! `crates/sqe-catalog/src/puffin_stats.rs` for the writer side.
//
// TODO(matrix-f): Phase F tasks 7.12-7.14 — implement the StatisticsRequest
// producer and Iceberg/Puffin provider, then benchmark StatisticsRegistry.

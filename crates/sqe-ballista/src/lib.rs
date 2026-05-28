//! SQE-on-ballista integration.
//!
//! This crate is the seam between SQE and Apache Ballista 53. It hosts the
//! pieces ballista needs to run SQE's iceberg + OIDC bearer-passthrough
//! workload without forking ballista:
//!
//! - [`codec`] — the iceberg logical + physical extension codecs that let
//!   ballista serialize an `IcebergTableProvider` / `IcebergTableScan`
//!   across the scheduler -> executor boundary. `iceberg-datafusion` ships
//!   neither; see the divergence ledger (D1/D2) in the cutover design doc.
//!
//! Later phases add a coordinator-side submission facade and the executor
//! bootstrap. See
//! `docs/superpowers/specs/2026-05-28-sqe-on-ballista-cutover-design.md`.

pub mod codec;

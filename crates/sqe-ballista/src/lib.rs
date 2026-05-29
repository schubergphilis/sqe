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

pub mod auth_ext;
pub mod cluster;
pub mod codec;
pub mod sqe_codec;

/// Drive an async future to completion from a sync context that is itself
/// running on a tokio worker thread (codec decode happens inside the
/// ballista executor's runtime).
///
/// `futures::executor::block_on` deadlocks here: it parks the worker thread
/// without pumping the tokio reactor, so the iceberg REST client's HTTP
/// future never makes progress. `block_in_place` hands the thread back to
/// the runtime while we block, and the current `Handle` drives our future on
/// the same multi-threaded runtime. Requires a multi-threaded runtime (the
/// ballista executor has one).
pub(crate) fn block_on_in_runtime<F: std::future::Future>(fut: F) -> F::Output {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(move || handle.block_on(fut))
}

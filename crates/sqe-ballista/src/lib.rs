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

use std::sync::Arc;

use datafusion::prelude::SessionContext;

/// Register the scalar UDFs an SQE plan may reference, so a policy-rewritten or
/// function-bearing logical plan survives the ballista codec round-trip: the
/// scheduler resolves them when it physical-plans the submitted plan, and the
/// executor can run them. Scope is the scalar UDF set the coordinator registers
/// for query *execution*: `sha256` (column masks), Trino-compat functions, and
/// JSON functions. Table-valued functions (read_parquet, iceberg metadata) are
/// deliberately excluded: they resolve at parse time on the coordinator, so the
/// plan reaching ballista already holds concrete scans, not TVF calls.
///
/// Called at every SQE-built planning context (the coordinator, the ballista
/// scheduler session builder, and the executor via
/// `BallistaFunctionRegistry::from`) so the registered set cannot drift between
/// them. The drift is exactly what broke column masks on the ballista path
/// before this existed (cutover parity criterion #2).
pub fn register_sqe_session_udfs(
    ctx: &mut SessionContext,
    mask_key: Option<Arc<Vec<u8>>>,
) -> datafusion::error::Result<()> {
    // sha256() column-mask UDF. With a key it runs as HMAC-SHA256 (issue #37);
    // the cluster reads the same `[policy] mask_key` as the coordinator, so a
    // masked value hashes identically on either engine.
    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf(mask_key));
    // Trino-compat scalar functions (year(), month(), day_of_week(), ...) and
    // the extended set (soundex, regexp_extract, ...). A &mut reborrows to &.
    sqe_trino_functions::register_trino_functions(ctx);
    sqe_trino_functions::register_extended_trino_functions(ctx);
    // JSON functions (json_get, json_contains, ...).
    datafusion_functions_json::register_all(ctx)?;
    Ok(())
}

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

#[cfg(test)]
mod udf_tests {
    use super::*;
    use ballista_core::registry::BallistaFunctionRegistry;

    /// The cluster catalog before this fix registered no SQE UDFs on the
    /// scheduler/executor, so `sha256` column masks (and JSON funcs) failed to
    /// resolve on the ballista path. Assert the helper registers the mask UDF
    /// beyond the DataFusion defaults, and that it flows into the
    /// `BallistaFunctionRegistry` the executor is configured with. Regression
    /// guard for parity criterion #2.
    #[tokio::test]
    async fn register_sqe_session_udfs_adds_mask_and_extra_scalars() {
        let default_count = SessionContext::new().state().scalar_functions().len();

        let mut ctx = SessionContext::new();
        register_sqe_session_udfs(&mut ctx, None).expect("register SQE UDFs");
        let funcs = ctx.state().scalar_functions().clone();

        assert!(
            funcs.contains_key("sha256"),
            "sha256 column-mask UDF must be registered (the parity #2 gap)"
        );
        assert!(
            funcs.len() > default_count,
            "Trino + JSON scalar UDFs must be added beyond DataFusion defaults"
        );

        // The executor is configured from a BallistaFunctionRegistry; confirm
        // the mask UDF survives that conversion (this is what executors run).
        let registry = BallistaFunctionRegistry::from(&ctx.state());
        assert!(
            registry.scalar_functions.contains_key("sha256"),
            "executor function registry must carry the mask UDF"
        );
    }
}

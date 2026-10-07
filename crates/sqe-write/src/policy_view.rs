//! Separate mask context for the write path.
//!
//! DML evaluation reads the target through the caller's column masks. The
//! masks reach the write path as SQL text
//! ([`WritePolicyPredicates::column_mask_sqls`]), and a HASH mask renders as
//! the internal keyed function ([`sqe_policy::sha256_udf::MASK_HASH_UDF_NAME`]).
//! That function is never registered in a user's session: if it were, user
//! SQL could hash candidate values with the mask key and match them against
//! masked output.
//!
//! So the masked relation is planned here, in a context cloned from the
//! user's session state with the internal mask functions added, and only
//! ever fed engine-generated SQL (column names, the scratch table, and the
//! policy's own mask and row-filter text). The resulting plan is registered
//! in the user's session as a view; the user-facing statement names the
//! view and never sees the function. The view's plan holds the function
//! itself, so nothing has to resolve it by name later.
//!
//! The clone shares the user's catalog list (the scratch table registered
//! there resolves), but its function registry is its own: registering the
//! mask function on it does not register it on the user's session.

use datafusion::prelude::SessionContext;
use sqe_core::{SqeError, SqeErrorCode};
use sqe_policy::write_predicates::WritePolicyPredicates;

/// Plan `relation_sql` (engine-generated SQL only, never user text) in the
/// separate mask context and register the result in `ctx` as the view
/// `view_name`, replacing an earlier registration under that name.
///
/// Fails closed: a mask that does not plan here is an error, never a fallback
/// to an unkeyed hash or to raw values.
pub(crate) async fn register_policy_view(
    ctx: &SessionContext,
    view_name: &str,
    relation_sql: &str,
    predicates: &WritePolicyPredicates,
) -> sqe_core::Result<()> {
    let mask_ctx = SessionContext::new_with_state(ctx.state());
    for udf in &predicates.mask_udfs {
        mask_ctx.register_udf(udf.as_ref().clone());
    }
    let view = sqe_catalog::file_tvf_prepare::plan_sql(&mask_ctx, relation_sql)
        .await
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::AccessDenied,
                format!("write refused: the column masks cannot be applied to this statement: {e}"),
            )
        })?
        .into_view();
    // A retry re-registers the view over the fresh target; the name is
    // per-invocation, so this only ever replaces our own registration.
    let _ = ctx.deregister_table(view_name);
    ctx.register_table(view_name, view).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Failed to register policy view: {e}"),
        )
    })?;
    Ok(())
}

//! Coordinator-side submission facade.
//!
//! [`submit_standalone`] takes a policy-rewritten DataFusion `LogicalPlan`
//! and runs it through an in-process ballista standalone cluster
//! (scheduler + executor), returning the result stream. This is the Phase 2
//! shape: one standalone cluster per query (correctness-first). Phase 3
//! replaces it with a shared scheduler + remote executors.
//!
//! The caller (sqe-coordinator) builds the plan against its own
//! `SessionContext` with the per-session `SqeCatalogProvider` and bearer.
//! We re-register that catalog on the ballista context and install the SQE
//! codecs so the plan round-trips across the scheduler -> executor boundary.
//!
//! See `docs/superpowers/specs/2026-05-28-sqe-on-ballista-cutover-design.md`.

use std::sync::Arc;

use anyhow::{Context, Result};
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::CatalogProvider;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use sqe_catalog::SessionCatalog;

use crate::sqe_codec::{SqeLogicalCodec, SqePhysicalCodec};

/// Run a policy-rewritten `LogicalPlan` through an in-process ballista
/// standalone cluster and open the result stream.
///
/// - `plan` — the enforced (policy-rewritten) logical plan from the
///   coordinator's planning pipeline.
/// - `catalog_name` / `catalog_provider` — the per-session
///   `SqeCatalogProvider` to register on the ballista context (under the
///   same name the plan references) so the logical codec can rehydrate
///   tables on the executor.
/// - `session_catalog` — the per-session `SessionCatalog`; the physical
///   codec uses it to reload the iceberg `Table` on the executor with the
///   session's vended credentials.
/// - `target_partitions` — scan/execute fan-out.
///
/// Returns the output schema and the opened record-batch stream.
pub async fn submit_standalone(
    plan: LogicalPlan,
    catalog_name: &str,
    catalog_provider: Arc<dyn CatalogProvider>,
    session_catalog: Arc<SessionCatalog>,
    target_partitions: usize,
) -> Result<(SchemaRef, SendableRecordBatchStream)> {
    let logical_codec = Arc::new(SqeLogicalCodec::new(catalog_provider.clone()));
    let physical_codec = Arc::new(SqePhysicalCodec::new(session_catalog));

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_ballista_standalone_parallelism(target_partitions)
        .with_ballista_logical_extension_codec(logical_codec)
        .with_ballista_physical_extension_codec(physical_codec);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state)
        .await
        .context("starting ballista standalone session")?;

    ctx.register_catalog(catalog_name, catalog_provider);

    let df = ctx
        .execute_logical_plan(plan)
        .await
        .context("submitting logical plan to ballista")?;

    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let stream = df
        .execute_stream()
        .await
        .context("opening ballista result stream")?;

    Ok((schema, stream))
}

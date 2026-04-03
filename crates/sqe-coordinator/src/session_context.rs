use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use tracing::debug;

use sqe_catalog::{SessionCatalog, SqeCatalogProvider};
use sqe_core::{Session, SqeConfig};
use sqe_policy::PolicyStore;

use crate::query_tracker::QueryTracker;

/// Build a DataFusion [`SessionContext`] for the given session.
///
/// The context is wired up with:
/// - The user's Polaris catalog (via their bearer token)
/// - The `system` catalog for Trino JDBC metadata + runtime query tables
/// - The `sha256()` UDF for column masking
/// - Trino-compatible function aliases (`year()`, `month()`, …)
/// - The `read_parquet()` table-valued function
#[tracing::instrument(skip(config, session, policy_store, query_tracker), fields(username = %session.user.username))]
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    policy_store: Option<&Arc<dyn PolicyStore>>,
    query_tracker: &Arc<QueryTracker>,
) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
    let catalog_name = if config.catalog.warehouse.is_empty() {
        "default".to_string()
    } else {
        config.catalog.warehouse.clone()
    };

    // Configure per-query memory limit via DataFusion's memory pool
    let max_memory = sqe_core::parse_memory_limit(&config.query.max_query_memory).unwrap_or(256 * 1024 * 1024);
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(&catalog_name, "default");

    let ctx = if max_memory > 0 {
        let pool = Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(max_memory));
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .map_err(|e| sqe_core::SqeError::Config(format!("Failed to create runtime env: {e}")))?;
        SessionContext::new_with_config_rt(session_config, runtime)
    } else {
        SessionContext::new_with_config(session_config)
    };

    // Create a per-session catalog connected to Polaris with the user's bearer token
    let session_catalog = Arc::new(
        SessionCatalog::new(
            &config.catalog.polaris_url,
            &config.catalog.warehouse,
            &session.access_token,
            &config.storage,
            None, None,
        )
        .await?,
    );

    // Clone before moving into SqeCatalogProvider (which consumes the Arc)
    let session_catalog_for_return = session_catalog.clone();
    let session_catalog_for_system = session_catalog.clone();

    // Create the DataFusion CatalogProvider from the session catalog,
    // passing policy store and session user for information_schema column filtering.
    let catalog_provider = SqeCatalogProvider::try_new_with_policy(
        session_catalog,
        config.storage.clone(),
        config.catalog.warehouse.clone(),
        policy_store.cloned(),
        Some(session.user.clone()),
    )
    .await?;

    ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));

    // Register the system catalog for Trino JDBC metadata browsing
    // (system.jdbc.types, system.jdbc.catalogs, system.jdbc.schemas, etc.)
    // and the system.runtime.* virtual tables for query/node/task info.
    let tracker = Arc::clone(query_tracker);
    let records_fn: sqe_catalog::system_runtime::QueryRecordsFn = Arc::new(move || {
        tracker
            .records()
            .into_iter()
            .map(|r| sqe_catalog::system_runtime::RuntimeQueryRecord {
                query_id: r.query_id.to_string(),
                state: match r.state {
                    crate::query_tracker::QueryState::Queued => {
                        sqe_catalog::system_runtime::RuntimeQueryState::Queued
                    }
                    crate::query_tracker::QueryState::Running => {
                        sqe_catalog::system_runtime::RuntimeQueryState::Running
                    }
                    crate::query_tracker::QueryState::Finished => {
                        sqe_catalog::system_runtime::RuntimeQueryState::Finished
                    }
                    crate::query_tracker::QueryState::Failed => {
                        sqe_catalog::system_runtime::RuntimeQueryState::Failed
                    }
                    crate::query_tracker::QueryState::Canceled => {
                        sqe_catalog::system_runtime::RuntimeQueryState::Canceled
                    }
                },
                user: r.user.clone(),
                source: r.source.clone(),
                sql: r.sql.clone(),
                created: r.created,
                started: r.started,
                ended: r.ended,
                queued_ms: r.queued_ms,
                planning_ms: r.planning_ms,
                execution_ms: r.execution_ms,
                output_rows: r.output_rows,
                error_type: r.error_type.clone(),
                error_code: r.error_code.clone(),
                fragments: r
                    .fragments
                    .iter()
                    .map(|f| sqe_catalog::system_runtime::RuntimeFragmentInfo {
                        task_id: f.task_id.clone(),
                        worker_url: f.worker_url.clone(),
                        state: f.state.to_string(),
                        elapsed_ms: f.elapsed_ms,
                        input_rows: f.input_rows,
                        output_rows: f.output_rows,
                    })
                    .collect(),
            })
            .collect()
    });
    let coordinator_uri = format!(
        "http://localhost:{}",
        config.coordinator.flight_sql_port
    );
    let runtime_schema = Arc::new(sqe_catalog::system_runtime::RuntimeSchemaProvider::new(
        records_fn,
        config.catalog.warehouse.clone(),
        coordinator_uri,
        config.coordinator.worker_urls.clone(),
    ));
    let system_catalog = sqe_catalog::SystemCatalogProvider::new(
        session_catalog_for_system,
        config.catalog.warehouse.clone(),
    )
    .with_runtime(runtime_schema);
    ctx.register_catalog("system", Arc::new(system_catalog));

    // Register the sha256() scalar function for column masking.
    // DataFusion does not ship a built-in sha256 — we provide one via sqe-policy.
    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf());

    // Register Trino-compatible function aliases (year(), month(), day_of_week(), etc.)
    // so Trino SQL and dbt models work without modification.
    crate::trino_functions::register_trino_functions(&ctx);

    // Register the read_parquet() table-valued function so users can
    // query external Parquet files directly from SQL:
    //   SELECT * FROM read_parquet('s3://bucket/path/*.parquet', ...)
    ctx.register_udtf(
        "read_parquet",
        Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
            config.storage.clone(),
        )),
    );

    debug!(
        catalog_name = %catalog_name,
        username = %session.user.username,
        "Registered session catalog in DataFusion context"
    );

    Ok((ctx, session_catalog_for_return))
}

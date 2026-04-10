use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use tracing::debug;

use sqe_catalog::{ManifestCache, SessionCatalog, SqeCatalogProvider};
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
///
/// When a shared `runtime` is provided (built once at coordinator startup via
/// [`crate::runtime::build_coordinator_runtime`]), it is used for all sessions
/// so the FairSpillPool memory limit is enforced globally. When `None`, a
/// per-query runtime is created using the legacy `max_query_memory` setting.
#[tracing::instrument(skip(config, session, policy_store, query_tracker, runtime, prom_metrics, manifest_cache), fields(username = %session.user.username))]
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    policy_store: Option<&Arc<dyn PolicyStore>>,
    query_tracker: &Arc<QueryTracker>,
    runtime: Option<&Arc<RuntimeEnv>>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    manifest_cache: Option<&ManifestCache>,
) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
    let catalog_name = if config.catalog.warehouse.is_empty() {
        "default".to_string()
    } else {
        config.catalog.warehouse.clone()
    };

    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(&catalog_name, "default");

    let mut ctx = if let Some(rt) = runtime {
        // Use the shared coordinator runtime (FairSpillPool with spill-to-disk)
        SessionContext::new_with_config_rt(session_config, Arc::clone(rt))
    } else {
        // Legacy path: per-query GreedyMemoryPool (no spill)
        let max_memory = sqe_core::parse_memory_limit(&config.query.max_query_memory)
            .unwrap_or(256 * 1024 * 1024);
        if max_memory > 0 {
            let pool = Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(max_memory),
            );
            let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_memory_pool(pool)
                .build_arc()
                .map_err(|e| {
                    sqe_core::SqeError::Config(format!("Failed to create runtime env: {e}"))
                })?;
            SessionContext::new_with_config_rt(session_config, rt)
        } else {
            SessionContext::new_with_config(session_config)
        }
    };

    // Register DataFusion's built-in in-memory catalog so DML helpers can register
    // temporary MemTables under `datafusion.public.<name>` without hitting the
    // Iceberg catalog which does not support dynamic table registration.
    let df_catalog = Arc::new(MemoryCatalogProvider::new());
    let df_schema = Arc::new(MemorySchemaProvider::new());
    df_catalog
        .register_schema("public", df_schema)
        .expect("MemoryCatalogProvider always accepts schema registration");
    ctx.register_catalog("datafusion", df_catalog);

    // Create a per-session catalog connected to Polaris with the user's bearer token
    let session_catalog = Arc::new(
        SessionCatalog::new(
            &config.catalog.polaris_url,
            &config.catalog.warehouse,
            &session.access_token,
            &config.storage,
            config.catalog.metadata_cache_ttl_secs,
            None, None,
        )
        .await?,
    );

    // Clone before moving into SqeCatalogProvider (which consumes the Arc)
    let session_catalog_for_return = session_catalog.clone();
    let session_catalog_for_system = session_catalog.clone();

    // Create the DataFusion CatalogProvider from the session catalog,
    // passing policy store and session user for information_schema column filtering.
    let mut catalog_provider = SqeCatalogProvider::try_new_with_policy(
        session_catalog,
        config.storage.clone(),
        config.catalog.warehouse.clone(),
        policy_store.cloned(),
        Some(session.user.clone()),
    )
    .await?;
    if let Some(m) = prom_metrics {
        catalog_provider = catalog_provider.with_metrics(Arc::clone(m));
    }
    if let Some(mc) = manifest_cache {
        catalog_provider = catalog_provider.with_manifest_cache(mc.clone());
    }

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

    // Register extended Trino-compatible functions (soundex, regexp_extract, word_stem, etc.)
    crate::trino_functions_ext::register_extended_trino_functions(&ctx);

    // Register JSON functions from datafusion-functions-json crate.
    // Provides: json_get, json_get_str, json_get_int, json_get_float, json_get_bool,
    //           json_get_json, json_get_array, json_contains, json_as_text, json_length
    datafusion_functions_json::register_all(&mut ctx)
        .expect("Failed to register JSON functions");

    // Register the read_parquet() table-valued function so users can
    // query external Parquet files directly from SQL:
    //   SELECT * FROM read_parquet('s3://bucket/path/*.parquet', ...)
    ctx.register_udtf(
        "read_parquet",
        Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
            config.storage.clone(),
        )),
    );

    // Register Iceberg metadata TVFs:
    //   SELECT * FROM table_snapshots('schema', 'table')
    //   SELECT * FROM table_manifests('schema', 'table')
    //   SELECT * FROM table_history('schema', 'table')
    //   SELECT * FROM table_files('schema', 'table')
    //   SELECT * FROM table_partitions('schema', 'table')
    //   SELECT * FROM table_refs('schema', 'table')
    ctx.register_udtf(
        "table_snapshots",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TableSnapshotsFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );
    ctx.register_udtf(
        "table_manifests",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TableManifestsFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );
    ctx.register_udtf(
        "table_history",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TableHistoryFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );
    ctx.register_udtf(
        "table_files",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TableFilesFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );
    ctx.register_udtf(
        "table_partitions",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TablePartitionsFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );
    ctx.register_udtf(
        "table_refs",
        Arc::new(sqe_catalog::iceberg_metadata_tvf::TableRefsFunction::new(
            Arc::clone(&session_catalog_for_return),
        )),
    );

    debug!(
        catalog_name = %catalog_name,
        username = %session.user.username,
        "Registered session catalog in DataFusion context"
    );

    Ok((ctx, session_catalog_for_return))
}

use std::sync::{Arc, LazyLock};

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use moka::future::Cache;
use sha2::{Digest, Sha256};
use tracing::debug;

use sqe_catalog::{SessionCatalog, SqeCatalogProvider, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use sqe_policy::PolicyStore;

use crate::query_tracker::QueryTracker;

/// Per-user SessionContext cache keyed by token fingerprint.
///
/// The cache holds `(SessionContext, Arc<SessionCatalog>)` pairs so that warm
/// queries skip the ~50 ms registration overhead (UDFs, TVFs, catalog setup).
/// Entries expire after 5 minutes (matching default idle session TTL) and the
/// cache holds at most 100 entries to bound memory usage.
///
/// DataFusion's `SessionContext` is `Clone` and wraps an `Arc<SessionState>`
/// internally, so a clone is O(1) — only the Arc ref-count changes.
static SESSION_CONTEXT_CACHE: LazyLock<Cache<String, (SessionContext, Arc<SessionCatalog>)>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(100)
            .time_to_live(std::time::Duration::from_secs(300))
            .build()
    });

/// Build a DataFusion [`SessionContext`] for the given session.
///
/// The context is wired up with:
/// - The user's Polaris catalog (via their bearer token)
/// - The `system` catalog for Trino JDBC metadata + runtime query tables
/// - The `sha256()` UDF for column masking
/// - Trino-compatible function aliases (`year()`, `month()`, ...)
/// - The `read_parquet()` table-valued function
///
/// When a shared `runtime` is provided (built once at coordinator startup via
/// [`crate::runtime::build_coordinator_runtime`]), it is used for all sessions
/// so the FairSpillPool memory limit is enforced globally. When `None`, a
/// per-query runtime is created using the legacy `max_query_memory` setting.
///
/// Results are cached per token fingerprint (5-minute TTL, max 100 entries).
/// On a cache hit the `SessionContext` and `SessionCatalog` are cloned in O(1)
/// and returned immediately, skipping all registration work.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(config, session, policy_store, query_tracker, runtime, prom_metrics, table_cache), fields(username = %session.user.username))]
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    policy_store: Option<&Arc<dyn PolicyStore>>,
    query_tracker: &Arc<QueryTracker>,
    runtime: Option<&Arc<RuntimeEnv>>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    table_cache: Option<&TableMetadataCache>,
) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
    // --- Cache key: username + token fingerprint ---
    // Different tokens from the same user must not share a stale SessionCatalog.
    // We key by username + first 16 hex chars of the SHA-256 of the access token.
    let token_hash = format!("{:x}", Sha256::digest(session.access_token.as_bytes()));
    let cache_key = format!("{}:{}", session.user.username, &token_hash[..16]);

    // --- Atomic cache lookup / build via try_get_with ---
    // Eliminates the TOCTOU race where two concurrent requests for the same key
    // both miss the cache and build redundant SessionContexts. moka coalesces
    // concurrent callers into a single init future.
    let username = session.user.username.clone();
    let result = SESSION_CONTEXT_CACHE
        .try_get_with(cache_key.clone(), async {
            debug!(
                username = %username,
                "SessionContext cache miss — building new context"
            );

            let catalog_name = if config.catalog.warehouse.is_empty() {
                "default".to_string()
            } else {
                config.catalog.warehouse.clone()
            };

            let session_config = SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema(&catalog_name, "default")
                // Parse numeric literals like 0.06 as DECIMAL instead of DOUBLE.
                // Matches Trino/SQL standard behavior: 0.06 - 0.01 = 0.05 (exact),
                // not 0.049999999999999996 (floating-point). Critical for correct
                // BETWEEN predicates and aggregate precision.
                .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
                // Broadcast threshold for hash joins: dimension tables below this
                // size use CollectLeft mode (build entire table in memory, broadcast
                // to probe side). Default 1MB is too low for TPC-DS dimension tables
                // like date_dim (73K rows ~5MB), customer_demographics (1.9M ~80MB).
                // 64MB matches Trino/Spark's broadcast join threshold.
                .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 64 * 1024 * 1024)
                // Dynamic filter pushdown: hash join build-side min/max values
                // pushed to probe-side scans at runtime.
                .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
                // Parquet filter pushdown: DataFusion pushes predicates into
                // the Parquet reader as RowFilters. Type mismatches (Utf8 >= Int32)
                // are handled gracefully by PhysicalExprPredicate (returns all-true
                // on error, lets parent FilterExec handle the coercion).
                .set_bool("datafusion.execution.parquet.pushdown_filters", true)
                .set_bool("datafusion.execution.parquet.reorder_filters", true);

            let mut ctx = if let Some(rt) = runtime {
                // Use the shared coordinator runtime (FairSpillPool with spill-to-disk)
                SessionContext::new_with_config_rt(session_config, Arc::clone(rt))
            } else {
                // Fallback path (tests, one-shot helpers): FairSpillPool with spill disabled.
                // Still prevents OOM by dividing memory fairly among operators.
                let max_memory = sqe_core::parse_memory_limit(&config.query.max_query_memory)
                    .unwrap_or(256 * 1024 * 1024);
                if max_memory > 0 {
                    let pool = Arc::new(
                        datafusion::execution::memory_pool::FairSpillPool::new(max_memory),
                    );
                    let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                        .with_memory_pool(pool)
                        .build_arc()
                        .map_err(|e| {
                            Arc::new(SqeError::Config(format!("Failed to create runtime env: {e}")))
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

            // Create a per-session catalog connected to Polaris with the user's bearer token.
            // Pass the shared global table metadata cache so Polaris REST round-trips are
            // skipped for tables that have already been loaded within the TTL window.
            let session_catalog = Arc::new(
                SessionCatalog::new(
                    &config.catalog.polaris_url,
                    &config.catalog.warehouse,
                    &session.access_token,
                    &config.storage,
                    table_cache.cloned(),
                    None, None,
                )
                .await
                .map_err(Arc::new)?,
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
            .await
            .map_err(Arc::new)?;
            if let Some(m) = prom_metrics {
                catalog_provider = catalog_provider.with_metrics(Arc::clone(m));
            }
            // Apply the small-file direct-read threshold from catalog config.
            // Convert MB to bytes; 0 MB disables the fast path.
            let small_file_threshold_bytes = config.catalog.small_file_threshold_mb
                .saturating_mul(1024 * 1024);
            catalog_provider = catalog_provider.with_small_file_threshold(small_file_threshold_bytes);
            catalog_provider = catalog_provider
                .with_manifest_concurrency(config.catalog.manifest_concurrency);

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
                        bytes_scanned: r.bytes_scanned,
                        rows_scanned: r.rows_scanned,
                        spill_bytes: r.spill_bytes,
                        peak_memory_bytes: r.peak_memory_bytes,
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
            sqe_trino_functions::register_trino_functions(&ctx);

            // Register extended Trino-compatible functions (soundex, regexp_extract, word_stem, etc.)
            sqe_trino_functions::register_extended_trino_functions(&ctx);

            // Register JSON functions from datafusion-functions-json crate.
            // Provides: json_get, json_get_str, json_get_int, json_get_float, json_get_bool,
            //           json_get_json, json_get_array, json_contains, json_as_text, json_length
            datafusion_functions_json::register_all(&mut ctx)
                .map_err(|e| Arc::new(SqeError::Config(format!("Failed to register JSON functions: {e}"))))?;

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
                "Registered session catalog in DataFusion context"
            );

            Ok((ctx, session_catalog_for_return)) as Result<_, Arc<SqeError>>
        })
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to build session context: {e}")))?;

    Ok(result)
}

/// Invalidate the cached SessionContext for a specific user.
///
/// Because cache keys are now `username:token_hash`, we iterate and remove all
/// entries whose key starts with the given username prefix.
///
/// Must be called after DDL/DML operations (CTAS, DROP TABLE, INSERT, etc.)
/// that modify the catalog so subsequent queries see the new schema state.
#[allow(dead_code)]
pub async fn invalidate_session_cache(username: &str) {
    let prefix = format!("{username}:");
    let keys_to_remove: Vec<String> = SESSION_CONTEXT_CACHE
        .iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(k, _)| k.to_string())
        .collect();
    for key in &keys_to_remove {
        SESSION_CONTEXT_CACHE.remove(key.as_str()).await;
    }
    debug!(username = %username, count = keys_to_remove.len(), "SessionContext cache invalidated for user after schema change");
}

/// Invalidate all cached SessionContexts.
///
/// Used when a DDL/DML operation modifies the catalog, ensuring all users
/// (not just the current user) see the updated schema state.
pub fn invalidate_all_session_caches() {
    SESSION_CONTEXT_CACHE.invalidate_all();
    debug!("All SessionContext caches invalidated");
}

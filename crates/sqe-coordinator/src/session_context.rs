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
use crate::runtime_catalog::RuntimeCatalogRegistry;

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
#[tracing::instrument(skip(config, session, policy_store, query_tracker, runtime, prom_metrics, table_cache, runtime_catalogs), fields(username = %session.user.username))]
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    policy_store: Option<&Arc<dyn PolicyStore>>,
    query_tracker: &Arc<QueryTracker>,
    runtime: Option<&Arc<RuntimeEnv>>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    table_cache: Option<&TableMetadataCache>,
    runtime_catalogs: &RuntimeCatalogRegistry,
) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
    // --- Cache key: username + token fingerprint ---
    // Different tokens from the same user must not share a stale SessionCatalog.
    // We key by username + first 16 hex chars of the SHA-256 of the access token.
    let token_hash = format!("{:x}", Sha256::digest(session.access_token.expose_bytes()));
    let cache_key = format!("{}:{}", session.user.username, &token_hash[..16]);

    // --- Atomic cache lookup / build via try_get_with ---
    // Eliminates the TOCTOU race where two concurrent requests for the same key
    // both miss the cache and build redundant SessionContexts. moka coalesces
    // concurrent callers into a single init future.
    let username = session.user.username.clone();
    let attached_providers = runtime_catalogs.providers();
    let result = SESSION_CONTEXT_CACHE
        .try_get_with(cache_key.clone(), async {
            debug!(
                username = %username,
                "SessionContext cache miss — building new context"
            );

            // Multi-catalog: build the named list from
            // `flattened_catalogs()` (legacy `[catalog]` block joins
            // under `iceberg` when no `[catalogs.*]` are set; otherwise
            // the named map drives, alphabetically sorted, with the
            // legacy block folded in only when `default_catalog` names
            // it explicitly). The "default" catalog DataFusion uses
            // for unqualified names is `resolve_default_catalog()`.
            let flattened: Vec<(String, sqe_core::config::CatalogConfig)> = config
                .flattened_catalogs()
                .into_iter()
                .map(|(n, c)| (n, c.clone()))
                .collect();
            let catalog_name = config.resolve_default_catalog();

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

            let ctx = if let Some(rt) = runtime {
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
                    // V10 httpfs: lazy http(s) ObjectStoreRegistry mirrors the
                    // primary coordinator runtime so the fallback path
                    // (tests, one-shot helpers) also accepts URL-shaped
                    // file paths in read_* TVFs.
                    let registry = Arc::new(
                        sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::new(
                            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
                        ),
                    );
                    let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                        .with_memory_pool(pool)
                        .with_object_store_registry(registry)
                        .build_arc()
                        .map_err(|e| {
                            Arc::new(SqeError::Config(format!("Failed to create runtime env: {e}")))
                        })?;
                    SessionContext::new_with_config_rt(session_config, rt)
                } else {
                    SessionContext::new_with_config(session_config)
                }
            };

            // V8: enable DuckDB-style `SELECT * FROM 'file.parquet'` auto-detection.
            // DataFusion's DynamicFileCatalog wraps the current catalog list in a
            // factory that resolves quoted-string table names against ListingTableFactory
            // when nothing else matches. Extension drives the format (parquet / csv /
            // json / avro). Must run before catalog registrations because it
            // replaces the catalog list pointer; subsequent register_catalog calls
            // attach to the wrapped list.
            let ctx = ctx.enable_url_table();
            let mut ctx = ctx;

            // Register DataFusion's built-in in-memory catalog so DML helpers can register
            // temporary MemTables under `datafusion.public.<name>` without hitting the
            // Iceberg catalog which does not support dynamic table registration.
            let df_catalog = Arc::new(MemoryCatalogProvider::new());
            let df_schema = Arc::new(MemorySchemaProvider::new());
            df_catalog
                .register_schema("public", df_schema)
                .expect("MemoryCatalogProvider always accepts schema registration");
            ctx.register_catalog("datafusion", df_catalog);

            // Build one SessionCatalog + SqeCatalogProvider per
            // entry from `flattened`. Each catalog gets its own
            // per-session connection. Two per-catalog overrides
            // resolved here (V7):
            //
            //   * `cat_cfg.auth` — when present, replaces the
            //     session bearer for this catalog only. Variants:
            //     SessionBearer (default), Static, Anonymous,
            //     ClientCredentials, Aws. Federated deployments
            //     where one catalog speaks Polaris (session token)
            //     and another speaks a partner Iceberg REST endpoint
            //     behind its own OAuth client now configure both in
            //     one TOML.
            //
            //   * `cat_cfg.storage` — when present, overrides the
            //     coordinator-wide `[storage]` block for this
            //     catalog. The override flows into `for_session_with`
            //     and into `SqeCatalogProvider`, so scan / write
            //     paths for this catalog hit the right S3 endpoint
            //     and region. Iceberg credential vending from REST
            //     catalogs still wins per-table over both.
            //
            // 3-part SQL identifiers work without session-state setup
            // because every catalog registers under its declared SQL
            // name. Cross-catalog joins like `polaris.sales.orders
            // LEFT JOIN nessie.archive.orders` flow through the
            // standard DataFusion path.
            //
            // The first entry in `flattened` is treated as the
            // "primary" — its `SessionCatalog` is what
            // `system.runtime.*` introspection and the legacy
            // `session_catalog_for_return` path use. Operators
            // running mixed Polaris+Glue (or whatever) deployments
            // pick the primary by name via `query.default_catalog`.
            let global_storage = config.storage.clone();
            let mut primary_session_catalog: Option<Arc<SessionCatalog>> = None;

            for (cat_name, cat_cfg) in &flattened {
                let auth = cat_cfg.auth.clone().unwrap_or_default();
                let bearer = sqe_auth::per_catalog::resolve_bearer(
                    &auth,
                    session.access_token.expose(),
                )
                .await
                .map_err(Arc::new)?;
                let storage = cat_cfg
                    .storage
                    .clone()
                    .unwrap_or_else(|| global_storage.clone());

                let session_catalog = Arc::new(
                    SessionCatalog::for_session_with(
                        cat_cfg,
                        &storage,
                        table_cache.cloned(),
                        &bearer,
                    )
                    .await
                    .map_err(Arc::new)?,
                );

                let mut catalog_provider = SqeCatalogProvider::try_new_with_policy(
                    session_catalog.clone(),
                    storage.clone(),
                    cat_cfg.warehouse.clone(),
                    policy_store.cloned(),
                    Some(session.user.clone()),
                )
                .await
                .map_err(Arc::new)?;
                if let Some(m) = prom_metrics {
                    catalog_provider = catalog_provider.with_metrics(Arc::clone(m));
                }
                let small_file_threshold_bytes =
                    cat_cfg.small_file_threshold_mb.saturating_mul(1024 * 1024);
                catalog_provider =
                    catalog_provider.with_small_file_threshold(small_file_threshold_bytes);
                catalog_provider =
                    catalog_provider.with_manifest_concurrency(cat_cfg.manifest_concurrency);

                ctx.register_catalog(cat_name, Arc::new(catalog_provider));

                if primary_session_catalog.is_none() {
                    primary_session_catalog = Some(session_catalog);
                }
            }

            // Hold onto the primary for downstream consumers that
            // expect a single SessionCatalog (system.runtime.*,
            // SessionCatalog return value). Multi-catalog access
            // for queries flows through DataFusion's CatalogProvider
            // registration above; this is purely the legacy
            // bookkeeping handle.
            let session_catalog = primary_session_catalog.ok_or_else(|| {
                Arc::new(SqeError::Config(
                    "no catalogs configured; populate `[catalog]` or `[catalogs.*]`".into(),
                ))
            })?;
            let session_catalog_for_return = session_catalog.clone();
            let session_catalog_for_system = session_catalog.clone();

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

            // Register any catalogs attached at runtime via ATTACH.
            // The providers snapshot was taken before the cache future was
            // entered, so it reflects the registry state at the moment of
            // the cache miss.
            for (name, provider) in &attached_providers {
                ctx.register_catalog(name.clone(), Arc::clone(provider));
            }

            // Register the sha256() scalar function for column masking.
            // DataFusion does not ship a built-in sha256, we provide one via sqe-policy.
            // When `coordinator.policy.mask_key` is set the UDF runs as
            // HMAC-SHA256 with that key, blocking offline rainbow-table
            // attacks against low-entropy masked columns (issue #37).
            let mask_key = if config.policy.mask_key.is_empty() {
                None
            } else {
                Some(std::sync::Arc::new(config.policy.mask_key.as_bytes().to_vec()))
            };
            ctx.register_udf(sqe_policy::sha256_udf::sha256_udf(mask_key));

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

            // V8: read_csv() and read_json() TVFs alongside read_parquet().
            // Same calling convention (positional path + named kw args);
            // CSV-specific args: delimiter, has_header, quote, escape,
            // comment, null_regex, file_extension. JSON-specific args:
            // newline_delimited, file_extension. S3 credentials + endpoint
            // overrides flow through the shared file_tvf_common helpers.
            ctx.register_udtf(
                "read_csv",
                Arc::new(sqe_catalog::read_csv::ReadCsvFunction::new(
                    config.storage.clone(),
                )),
            );
            ctx.register_udtf(
                "read_json",
                Arc::new(sqe_catalog::read_json::ReadJsonFunction::new(
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
/// Also called after CALL system.<maintenance procedure>(...) so the
/// cached DataFusion TVF MemTables (table_files, table_snapshots, etc.)
/// rebuild from the post-rewrite Polaris snapshot.
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
    // Moka's remove is queued as a pending task; flush so the cache sees
    // the removal on the very next try_get_with in the same tokio tick.
    // Without this, a CALL system.<proc> followed by SELECT in the same
    // session within one test tick still hits the stale cached entry.
    SESSION_CONTEXT_CACHE.run_pending_tasks().await;
    debug!(
        username = %username,
        count = keys_to_remove.len(),
        "SessionContext cache invalidated for user after schema change"
    );
}

/// Invalidate all cached SessionContexts.
///
/// Used when a DDL/DML operation modifies the catalog, ensuring all users
/// (not just the current user) see the updated schema state.
pub fn invalidate_all_session_caches() {
    SESSION_CONTEXT_CACHE.invalidate_all();
    debug!("All SessionContext caches invalidated");
}

//! Embedded SQE engine for the CLI.
//!
//! Boots a single-process [`SessionContext`] with the same DataFusion
//! tuning as the cluster-mode coordinator: `parse_float_as_decimal`,
//! 64MB hash-join broadcast threshold, dynamic filter pushdown, Parquet
//! filter pushdown. Registers all the same scalar / aggregate / table
//! functions (Trino aliases, JSON, sha256, `read_parquet`, etc.) so the
//! same SQL text runs against the embedded engine as against a remote
//! coordinator.
//!
//! ## Persistence (V2)
//!
//! When [`WarehouseMode::Persistent`] is selected, embedded mode
//! attaches a SQLite-backed Iceberg catalog at `<path>/sqe.db` with
//! data files under `<path>/iceberg/`. `CREATE TABLE` writes Iceberg
//! metadata + Parquet to disk and a fresh process re-attaches and
//! sees the tables. The default warehouse is `~/.sqe/warehouse/`;
//! users override with `--warehouse <path>` or skip persistence
//! entirely with `--memory`.
//!
//! In [`WarehouseMode::Memory`], only DataFusion's default in-memory
//! catalog is registered. `CREATE TABLE foo AS SELECT ...` works
//! within the session but the table is gone on next start. No auth,
//! no policy, no metrics endpoint in either mode.
//!
//! ## Why duplicate the registration code from `sqe-coordinator`?
//!
//! The coordinator's `create_session_context` takes a full `SqeConfig`
//! plus an authenticated `Session`, a `PolicyStore`, a `QueryTracker`,
//! and a `MetricsRegistry`. None of those exist in embedded mode and
//! plumbing them as `Option`s through the builder would bloat the
//! cluster path for the embedded use case. A small targeted helper
//! here is cleaner; if both paths ever diverge meaningfully we
//! refactor at that point.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::CatalogBuilder;
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use sqe_core::config::StorageConfig;

use crate::writable_iceberg_catalog::WritableIcebergCatalog;

use crate::client::{QueryResult, SqlClient};

/// One persistent Iceberg catalog the embedded engine attaches.
///
/// Every entry produces an independent SQLite catalog at
/// `<path>/sqe.db` plus data files under `<path>/iceberg/`,
/// registered with DataFusion under `name`. Cross-catalog joins
/// like `SELECT * FROM prod.sales.orders JOIN stage.sales.orders`
/// work as long as both catalogs are attached.
#[derive(Debug, Clone)]
pub struct EmbeddedCatalog {
    /// Catalog identifier used in 3-part SQL names. Must be a valid
    /// SQL identifier; we don't currently quote it on the wire.
    pub name: String,
    /// Filesystem path to the warehouse root.
    pub path: PathBuf,
}

/// What the embedded engine attaches at startup.
#[derive(Debug, Clone)]
pub enum WarehouseMode {
    /// Ephemeral. `CREATE TABLE foo AS SELECT ...` works within the
    /// session via DataFusion's default in-memory catalog, but
    /// nothing persists across processes.
    Memory,
    /// Attach one or more named persistent catalogs. Order is
    /// preserved for the welcome banner. Empty Vec is equivalent
    /// to [`WarehouseMode::Memory`] (handled at attach time so
    /// callers don't need to special-case it).
    Persistent { catalogs: Vec<EmbeddedCatalog> },
}

impl WarehouseMode {
    /// Default: a single catalog named `iceberg` at
    /// `~/.sqe/warehouse/`. Falls back to a process-local
    /// `./.sqe-warehouse/` when `HOME` is unset (some CI runners).
    pub fn default_persistent() -> Self {
        let path = std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".sqe").join("warehouse"))
            .unwrap_or_else(|| PathBuf::from("./.sqe-warehouse"));
        WarehouseMode::Persistent {
            catalogs: vec![EmbeddedCatalog {
                name: "iceberg".to_string(),
                path,
            }],
        }
    }

    /// Build a `Persistent` mode from a single warehouse path,
    /// keeping the legacy `iceberg` catalog name. Used by the
    /// `--warehouse <path>` CLI flag for backwards compatibility.
    pub fn single(path: PathBuf) -> Self {
        WarehouseMode::Persistent {
            catalogs: vec![EmbeddedCatalog {
                name: "iceberg".to_string(),
                path,
            }],
        }
    }
}

/// Build a [`SessionContext`] suitable for embedded queries.
///
/// `memory_limit_bytes` caps the per-process query memory; values
/// below 64MB are clamped to that floor because DataFusion's hash
/// joins cannot make forward progress with smaller pools.
///
/// Use [`build_embedded_context_with_warehouse`] when you also want
/// a persistent Iceberg catalog attached.
pub fn build_embedded_context(memory_limit_bytes: usize) -> anyhow::Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "default")
        // Same DataFusion tuning the cluster coordinator applies. See
        // the comments in `sqe-coordinator/src/session_context.rs` for
        // the rationale on each flag.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            64 * 1024 * 1024,
        )
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);

    let pool_size = memory_limit_bytes.max(64 * 1024 * 1024);
    let pool = Arc::new(FairSpillPool::new(pool_size));
    // V10: wrap DataFusion's default ObjectStoreRegistry so
    // `read_csv('https://...')`, `read_parquet('https://...')`, etc. lazily
    // build an HttpStore for the URL's scheme://host on first request. Keeps
    // s3 / file behaviour unchanged; only http/https get the lazy build.
    let registry = Arc::new(sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::new(
        datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
    ));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_object_store_registry(registry)
        .build_arc()
        .map_err(|e| anyhow::anyhow!("failed to build runtime env: {e}"))?;

    let ctx = SessionContext::new_with_config_rt(session_config, runtime);

    // V8: enable `SELECT * FROM 'file.parquet'` auto-detection. The wrapped
    // catalog list resolves quoted-string table names against
    // ListingTableFactory by extension (parquet / csv / json / avro).
    let ctx = ctx.enable_url_table();
    let mut ctx = ctx;

    // Scalar UDFs.
    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf());

    // Trino dialect compatibility — year(), month(), day_of_week(),
    // url_extract_*, etc. plus the extended set (regexp_extract,
    // word_stem, soundex). These are what dbt models and Trino-shape
    // queries rely on.
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);

    // JSON functions: json_get, json_get_str, json_contains, etc.
    datafusion_functions_json::register_all(&mut ctx)
        .map_err(|e| anyhow::anyhow!("failed to register JSON functions: {e}"))?;

    // File-format TVFs for direct file access. Embedded mode passes a
    // default `StorageConfig` so users can still hit S3 by supplying inline
    // credentials in the TVF call. Filesystem paths work without any storage
    // config.
    ctx.register_udtf(
        "read_parquet",
        Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
            StorageConfig::default(),
        )),
    );
    ctx.register_udtf(
        "read_csv",
        Arc::new(sqe_catalog::read_csv::ReadCsvFunction::new(
            StorageConfig::default(),
        )),
    );
    ctx.register_udtf(
        "read_json",
        Arc::new(sqe_catalog::read_json::ReadJsonFunction::new(
            StorageConfig::default(),
        )),
    );
    // V11: Delta Lake reader. Wraps deltalake::open_table so users can
    // query a Delta table root directly:
    //   SELECT count(*) FROM read_delta('/data/delta/sales');
    //   SELECT * FROM read_delta('s3://bucket/delta/orders',
    //     access_key => 'AKIA...', secret_key => '...');
    // Time-travel via version => '<int>' or timestamp => '<RFC3339>'.
    // The CLI's Cargo.toml enables the sqe-catalog `delta` feature
    // unconditionally, so this registration always runs.
    ctx.register_udtf(
        "read_delta",
        Arc::new(sqe_catalog::read_delta::ReadDeltaFunction::new(
            StorageConfig::default(),
        )),
    );

    Ok(ctx)
}

/// Async variant of [`build_embedded_context`] that also attaches
/// any persistent Iceberg catalogs declared in `mode`. Each catalog
/// becomes a top-level SQL identifier so `<catalog>.<schema>.<table>`
/// resolves the right one and cross-catalog joins work without any
/// session-state setup.
///
/// Returns the [`SessionContext`] paired with a map of attached
/// iceberg catalogs keyed by user-facing name. The map lets the SQL
/// DDL interceptor route `CREATE SCHEMA <cat>.<ns>` directly to
/// `iceberg::Catalog::create_namespace` rather than DataFusion's
/// CatalogProvider (which `iceberg-datafusion` doesn't implement).
///
/// Side effect: creates `<path>` and `<path>/iceberg/` for each
/// catalog if missing. The SQLite database itself is created on
/// first connect by `iceberg-catalog-sql`.
pub async fn build_embedded_context_with_warehouse(
    memory_limit_bytes: usize,
    mode: &WarehouseMode,
) -> anyhow::Result<(SessionContext, IcebergCatalogMap)> {
    let ctx = build_embedded_context(memory_limit_bytes)?;
    let mut iceberg_catalogs: IcebergCatalogMap = HashMap::new();

    let catalogs = match mode {
        WarehouseMode::Memory => return Ok((ctx, iceberg_catalogs)),
        WarehouseMode::Persistent { catalogs } if catalogs.is_empty() => {
            return Ok((ctx, iceberg_catalogs));
        }
        WarehouseMode::Persistent { catalogs } => catalogs,
    };

    // Reject duplicate catalog names early — DataFusion's
    // `register_catalog` overwrites silently and the user would lose
    // a catalog without a clear error.
    let mut seen = std::collections::HashSet::new();
    for c in catalogs {
        if !seen.insert(c.name.clone()) {
            return Err(anyhow::anyhow!(
                "catalog name `{}` repeated — pick a unique name per --catalog",
                c.name
            ));
        }
    }

    for c in catalogs {
        let handle = attach_sqlite_catalog(&ctx, &c.name, &c.path)
            .await
            .map_err(|e| anyhow::anyhow!("failed to attach catalog `{}`: {e}", c.name))?;
        iceberg_catalogs.insert(c.name.clone(), handle);
    }
    Ok((ctx, iceberg_catalogs))
}

/// Map of iceberg catalogs keyed by user-facing name, available to
/// the DDL interceptor for write operations.
pub type IcebergCatalogMap = HashMap<String, Arc<dyn iceberg::Catalog>>;

/// Initialise `<path>/sqe.db` as the Iceberg metadata store and
/// `<path>/iceberg/` as the data file root, then register the result
/// with `ctx` under the given catalog `name`.
/// Returns the `Arc<dyn iceberg::Catalog>` after wiring it into the
/// DataFusion session. Caller stores the returned handle so DDL
/// interceptors (CREATE SCHEMA / CREATE TABLE on the iceberg catalog
/// surface) can route directly to the catalog API instead of through
/// DataFusion's CatalogProvider, which doesn't implement writes for
/// `iceberg-datafusion`'s provider.
async fn attach_sqlite_catalog(
    ctx: &SessionContext,
    name: &str,
    path: &Path,
) -> anyhow::Result<Arc<dyn iceberg::Catalog>> {
    std::fs::create_dir_all(path)
        .map_err(|e| anyhow::anyhow!("failed to create warehouse dir {}: {e}", path.display()))?;
    let data_root = path.join("iceberg");
    std::fs::create_dir_all(&data_root).map_err(|e| {
        anyhow::anyhow!("failed to create data dir {}: {e}", data_root.display())
    })?;

    // SQLite URI is `sqlite://<absolute path>` per sqlx's parsing.
    // We canonicalise so relative paths in `--warehouse` work even
    // after later `cd` calls.
    let abs = path.canonicalize().map_err(|e| {
        anyhow::anyhow!("failed to canonicalise warehouse path {}: {e}", path.display())
    })?;
    let db_path = abs.join("sqe.db");
    // `mode=rwc` tells SQLite to create the file if missing; without it
    // sqlx fails with "unable to open database file" on the first run
    // because SQLite defaults to read-write without create.
    let uri = format!("sqlite://{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", abs.join("iceberg").display());

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), uri);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri);

    // The builder's `name` parameter is NOT just diagnostic — it
    // becomes the `catalog_name` row key inside the SQLite
    // `iceberg_namespace_properties` and `iceberg_tables` tables, so
    // namespaces and tables are scoped per-name. We deliberately keep
    // the same fixed name across every embedded warehouse so each
    // SQLite file holds a single coherent scope. Catalog separation
    // for the user comes from each catalog living in its own SQLite
    // file (separate `path`); the user-facing identifier is set by
    // `ctx.register_catalog(name, ...)` below.
    let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        SqlCatalogBuilder::default()
            .load("sqe-embedded".to_string(), props)
            .await
            .map_err(|e| anyhow::anyhow!("SQLite catalog open failed: {e}"))?,
    );

    let provider = WritableIcebergCatalog::try_new(catalog.clone()).await?;

    ctx.register_catalog(name, Arc::new(provider));
    Ok(catalog)
}

/// `SqlClient` impl backed by an in-process [`SessionContext`].
///
/// Mirrors the network clients (`flight.rs`, `http.rs`) so the CLI's
/// REPL and one-shot paths don't need to special-case embedded mode.
/// The `iceberg_catalogs` map keeps strong references to the
/// catalog handles even though `WritableIcebergCatalog` already
/// holds its own clone — this stays around so a future CTAS path
/// can reach the iceberg API for the Parquet-write + commit step
/// without going through `Arc::downgrade` gymnastics on the
/// CatalogProvider trait object.
pub struct EmbeddedClient {
    ctx: SessionContext,
    #[allow(dead_code)]
    iceberg_catalogs: IcebergCatalogMap,
}

impl EmbeddedClient {
    /// Build a memory-only embedded client. Sufficient for ad-hoc
    /// `read_parquet` queries; `CREATE TABLE` lives only for the
    /// session. Used by tests; production paths go through
    /// [`Self::with_warehouse`].
    #[allow(dead_code)]
    pub fn new(memory_limit_bytes: usize) -> anyhow::Result<Self> {
        Ok(Self {
            ctx: build_embedded_context(memory_limit_bytes)?,
            iceberg_catalogs: HashMap::new(),
        })
    }

    /// Build an embedded client with a chosen warehouse mode.
    /// `Persistent` attaches a SQLite-backed Iceberg catalog;
    /// `Memory` matches the legacy `new()` behaviour with no
    /// iceberg catalogs attached.
    pub async fn with_warehouse(
        memory_limit_bytes: usize,
        mode: &WarehouseMode,
    ) -> anyhow::Result<Self> {
        let (ctx, iceberg_catalogs) =
            build_embedded_context_with_warehouse(memory_limit_bytes, mode).await?;
        Ok(Self {
            ctx,
            iceberg_catalogs,
        })
    }
}

#[async_trait]
impl SqlClient for EmbeddedClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        // No SQL interceptor needed: WritableIcebergCatalog routes
        // CREATE SCHEMA / DROP SCHEMA / CREATE TABLE / DROP TABLE
        // through the standard DataFusion CatalogProvider trait, which
        // dispatches to the underlying iceberg::Catalog. Reads also
        // go through the same provider since it composes the upstream
        // IcebergSchemaProvider for namespace contents.
        //
        // V12: pre-rewrite `'hf://...'` quoted literals to their HTTPS
        // equivalent. This makes `SELECT * FROM 'hf://...'` (URL-table
        // auto-detect) flow through V10's LazyHttpObjectStoreRegistry the
        // same way `SELECT * FROM 'https://...'` already does. The TVF
        // path (`read_csv('hf://...')`) is unaffected; it does its own
        // rewrite via `rewrite_hf_path_in_place` inside the TVF.
        let rewritten = sqe_catalog::file_tvf_common::rewrite_hf_urls_in_sql(sql)?;
        let df = self.ctx.sql(rewritten.as_ref()).await?;
        // Snapshot the DataFrame schema before collecting so we still
        // emit column names when the query produces zero batches (an
        // optimizer collapse like `WHERE FALSE` yields an EmptyExec).
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        Ok(record_batches_to_query_result(&schema, &batches))
    }
}

/// Render a sequence of [`RecordBatch`]es into the CLI's column-name +
/// stringified-row shape. Column names come from the input `schema`
/// even when `batches` is empty, matching what the network clients do.
fn record_batches_to_query_result(
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> QueryResult {
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default()))
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = formatters.iter().map(|f| f.value(row_idx).to_string()).collect();
            rows.push(row);
        }
    }
    QueryResult { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_client_executes_select_literal() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client.execute("SELECT 42 AS answer").await.expect("query");
        assert_eq!(result.columns, vec!["answer".to_string()]);
        assert_eq!(result.rows, vec![vec!["42".to_string()]]);
    }

    #[tokio::test]
    async fn embedded_client_runs_trino_function_year() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client
            .execute("SELECT year(DATE '2026-05-07') AS y")
            .await
            .expect("query");
        assert_eq!(result.columns, vec!["y".to_string()]);
        assert_eq!(result.rows, vec![vec!["2026".to_string()]]);
    }

    #[tokio::test]
    async fn embedded_client_returns_zero_rows_for_empty_select() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client
            .execute("SELECT 1 WHERE FALSE")
            .await
            .expect("query");
        assert_eq!(result.columns, vec!["Int64(1)".to_string()]);
        assert!(result.rows.is_empty());
    }

    /// V9: `SELECT * EXCLUDE (col)` removes the named column from the
    /// projection. DataFusion 53.1 supports this natively under the
    /// generic dialect; the test pins behaviour so a future DF upgrade
    /// can not silently regress.
    #[tokio::test]
    async fn select_star_exclude_drops_columns() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let r = client
            .execute(
                "WITH t(id, name, secret) AS \
                 (VALUES (1, 'alice', 'xyz'), (2, 'bob', 'abc')) \
                 SELECT * EXCLUDE (secret) FROM t",
            )
            .await
            .expect("query");
        assert_eq!(r.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(r.rows.len(), 2);
    }

    /// V9: `SELECT * REPLACE (expr AS col)` substitutes a column with a
    /// computed expression while keeping the column ordering. Native in
    /// DataFusion 53.1.
    #[tokio::test]
    async fn select_star_replace_substitutes_columns() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let r = client
            .execute(
                "WITH t(id, name) AS \
                 (VALUES (1, 'alice'), (2, 'bob')) \
                 SELECT * REPLACE (UPPER(name) AS name) FROM t \
                 ORDER BY id",
            )
            .await
            .expect("query");
        assert_eq!(r.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(r.rows[0][1], "ALICE");
        assert_eq!(r.rows[1][1], "BOB");
    }

    /// V9: DESCRIBE returns a (column_name, data_type, is_nullable)
    /// projection. DataFusion-native; no SQE-side wiring needed.
    #[tokio::test]
    async fn describe_table_returns_column_metadata() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client
            .execute("CREATE TABLE t AS SELECT 1::INT AS x, 'hi' AS y")
            .await
            .expect("create table");

        let r = client.execute("DESCRIBE t").await.expect("describe");
        assert_eq!(
            r.columns,
            vec![
                "column_name".to_string(),
                "data_type".to_string(),
                "is_nullable".to_string(),
            ]
        );
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], "x");
        assert_eq!(r.rows[1][0], "y");
    }

    /// Memory limit below the floor (64 MB) is clamped, not rejected.
    #[tokio::test]
    async fn embedded_client_clamps_tiny_memory_limit() {
        let mut client = EmbeddedClient::new(1).expect("build client even with tiny limit");
        let result = client.execute("SELECT 1").await.expect("query");
        assert_eq!(result.rows, vec![vec!["1".to_string()]]);
    }

    /// Default persistent path lives somewhere off `HOME` (or a fallback
    /// when `HOME` is unset). The caller doesn't actually need to write
    /// to it for this test; we just want the construction to succeed
    /// without panicking on environment shape.
    #[test]
    fn default_persistent_returns_a_path() {
        match WarehouseMode::default_persistent() {
            WarehouseMode::Persistent { catalogs } => {
                assert_eq!(catalogs.len(), 1);
                assert_eq!(catalogs[0].name, "iceberg");
                assert!(
                    !catalogs[0].path.as_os_str().is_empty(),
                    "warehouse path must not be empty"
                );
            }
            WarehouseMode::Memory => panic!("default_persistent must not be Memory"),
        }
    }

    /// `single(path)` keeps the legacy single-catalog name `iceberg`.
    /// Locks the backwards-compat contract for `--warehouse <path>`.
    #[test]
    fn single_warehouse_uses_iceberg_name() {
        let m = WarehouseMode::single(PathBuf::from("/tmp/foo"));
        match m {
            WarehouseMode::Persistent { catalogs } => {
                assert_eq!(catalogs.len(), 1);
                assert_eq!(catalogs[0].name, "iceberg");
            }
            _ => panic!("single must be Persistent"),
        }
    }

    /// Build is rejected when two `--catalog` entries pick the same name.
    /// Without this guard DataFusion's `register_catalog` would silently
    /// overwrite and the user would lose data without a clear error.
    #[tokio::test]
    async fn duplicate_catalog_names_are_rejected() {
        let tmp1 = tempfile::tempdir().expect("tempdir1");
        let tmp2 = tempfile::tempdir().expect("tempdir2");
        let mode = WarehouseMode::Persistent {
            catalogs: vec![
                EmbeddedCatalog {
                    name: "shared".into(),
                    path: tmp1.path().to_path_buf(),
                },
                EmbeddedCatalog {
                    name: "shared".into(),
                    path: tmp2.path().to_path_buf(),
                },
            ],
        };
        let result = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode).await;
        let err = result
            .map(|_| ())
            .expect_err("duplicate names must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("repeated"),
            "expected duplicate-name error, got: {msg}"
        );
    }

    /// Two catalogs registered side by side must both be visible via
    /// information_schema.schemata. The smoke for cross-catalog
    /// access; locks the contract that the DataFusion catalog name
    /// matches what the user passed on `--catalog NAME=PATH`.
    #[tokio::test]
    async fn two_catalogs_both_visible() {
        use iceberg::{Catalog, NamespaceIdent, TableCreation};
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

        let tmp_a = tempfile::tempdir().expect("tempdir a");
        let tmp_b = tempfile::tempdir().expect("tempdir b");

        // Bootstrap a namespace + table in each via the iceberg API.
        for (name, dir) in &[("a", tmp_a.path()), ("b", tmp_b.path())] {
            std::fs::create_dir_all(dir.join("iceberg")).expect("data dir");
            let abs = dir.canonicalize().expect("canonicalize");
            let mut props = HashMap::new();
            props.insert(
                SQL_CATALOG_PROP_URI.to_string(),
                format!("sqlite://{}?mode=rwc", abs.join("sqe.db").display()),
            );
            props.insert(
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                format!("file://{}", abs.join("iceberg").display()),
            );
            // Use the same fixed name as production code so the
            // bootstrap writes into the same `catalog_name` scope
            // the attach path reads. See the comment in
            // `attach_sqlite_catalog`.
            let cat = SqlCatalogBuilder::default()
                .load("sqe-embedded".to_string(), props)
                .await
                .expect("bootstrap catalog");
            let ns = NamespaceIdent::new(format!("ns_{name}"));
            cat.create_namespace(&ns, HashMap::new())
                .await
                .expect("create_namespace");
            let schema = IcebergSchema::builder()
                .with_fields(vec![NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .expect("schema");
            cat.create_table(
                &ns,
                TableCreation::builder()
                    .name(format!("t_{name}"))
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("create_table");
        }

        let mode = WarehouseMode::Persistent {
            catalogs: vec![
                EmbeddedCatalog {
                    name: "left".into(),
                    path: tmp_a.path().to_path_buf(),
                },
                EmbeddedCatalog {
                    name: "right".into(),
                    path: tmp_b.path().to_path_buf(),
                },
            ],
        };

        let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode)
            .await
            .expect("two-catalog client builds");

        let r = c
            .execute(
                "SELECT table_catalog, table_schema, table_name \
                 FROM information_schema.tables \
                 WHERE table_catalog IN ('left', 'right') \
                 ORDER BY table_catalog, table_schema, table_name",
            )
            .await
            .expect("information_schema");
        // Each catalog produces one user table plus iceberg metadata
        // pseudo-tables. We only assert the user tables are there
        // under the right catalog name.
        let has_left = r
            .rows
            .iter()
            .any(|row| row[0] == "left" && row[1] == "ns_a" && row[2] == "t_a");
        let has_right = r
            .rows
            .iter()
            .any(|row| row[0] == "right" && row[1] == "ns_b" && row[2] == "t_b");
        assert!(has_left, "left catalog missing; rows: {:?}", r.rows);
        assert!(has_right, "right catalog missing; rows: {:?}", r.rows);
    }

    /// Persistence smoke: create a namespace and table via SQL,
    /// drop the client, build a fresh one against the same warehouse,
    /// and confirm both are visible. End-to-end exercise of the
    /// V5 `WritableIcebergCatalog` write path: every operation goes
    /// through DataFusion's SQL surface, no out-of-band bootstrap.
    #[tokio::test]
    async fn persistent_warehouse_survives_client_restart() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mode = WarehouseMode::single(tmp.path().to_path_buf());

        // Phase 1: create namespace + table via SQL.
        {
            let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode)
                .await
                .expect("first client");
            c.execute("CREATE SCHEMA iceberg.test_ns")
                .await
                .expect("CREATE SCHEMA");
            c.execute(
                "CREATE TABLE iceberg.test_ns.greetings (id BIGINT, msg VARCHAR)",
            )
            .await
            .expect("CREATE TABLE");
        }

        // Phase 2: build the embedded client, confirm the iceberg
        // catalog is registered and the namespace + table are visible
        // via DataFusion's information_schema.
        let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode)
            .await
            .expect("client builds against existing warehouse");
        let r = c
            .execute(
                "SELECT table_schema, table_name \
                 FROM information_schema.tables \
                 WHERE table_catalog = 'iceberg' AND table_schema = 'test_ns' \
                 ORDER BY table_name",
            )
            .await
            .expect("information_schema.tables");
        assert_eq!(r.columns, vec!["table_schema".to_string(), "table_name".to_string()]);
        // The iceberg-datafusion bridge exposes the user table plus
        // metadata pseudo-tables ($snapshots, $manifests). We only
        // require the main table is visible — the pseudo-tables are
        // a useful side benefit but their exact set is upstream's
        // concern, not ours.
        assert!(
            r.rows
                .iter()
                .any(|row| row == &vec!["test_ns".to_string(), "greetings".to_string()]),
            "fresh client should see the pre-existing greetings table; got rows: {:?}",
            r.rows,
        );
    }

    /// Memory mode never touches disk: the warehouse path is unused.
    /// We pass an obviously-invalid path and assert the build still
    /// succeeds because the SQLite branch is bypassed.
    #[tokio::test]
    async fn memory_mode_skips_warehouse_setup() {
        let mode = WarehouseMode::Memory;
        let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode)
            .await
            .expect("memory client builds without disk");
        // Sanity: the session works.
        let r = c.execute("SELECT 1").await.expect("query");
        assert_eq!(r.rows, vec![vec!["1".to_string()]]);
    }

    /// V8: `read_csv()` round-trips a local CSV file. The TVF wraps a
    /// `ListingTable` over `CsvFormat` and the embedded mode passes the
    /// default `StorageConfig`, so filesystem paths just work.
    /// Multi-thread flavor required: schema inference uses `block_in_place`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_csv_tvf_local_file() {
        use std::io::Write;
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("data.csv");
        let mut f = std::fs::File::create(&path).expect("create csv");
        writeln!(f, "id,name").expect("header");
        writeln!(f, "1,alice").expect("row1");
        writeln!(f, "2,bob").expect("row2");
        drop(f);

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = format!(
            "SELECT count(*) AS n FROM read_csv('{}')",
            path.display()
        );
        let result = client.execute(&sql).await.expect("query");
        assert_eq!(result.columns, vec!["n".to_string()]);
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
    }

    /// V8: `read_json()` over an NDJSON file. Each line is one record;
    /// schema inference picks both columns up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_json_tvf_local_file() {
        use std::io::Write;
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("events.json");
        let mut f = std::fs::File::create(&path).expect("create json");
        writeln!(f, "{{\"id\": 1, \"name\": \"alice\"}}").expect("row1");
        writeln!(f, "{{\"id\": 2, \"name\": \"bob\"}}").expect("row2");
        drop(f);

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = format!(
            "SELECT count(*) AS n FROM read_json('{}')",
            path.display()
        );
        let result = client.execute(&sql).await.expect("query");
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
    }

    /// V8: `SELECT * FROM 'file.csv'` auto-detect via DataFusion's
    /// `enable_url_table()`. The `DynamicFileCatalog` resolves the
    /// quoted-string table name against `ListingTableFactory`, picking
    /// `CsvFormat` from the `.csv` extension.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_from_quoted_csv_path_auto_detects() {
        use std::io::Write;
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("auto.csv");
        let mut f = std::fs::File::create(&path).expect("create csv");
        writeln!(f, "x").expect("header");
        writeln!(f, "10").expect("row1");
        writeln!(f, "20").expect("row2");
        writeln!(f, "30").expect("row3");
        drop(f);

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = format!("SELECT count(*) AS n FROM '{}'", path.display());
        let result = client.execute(&sql).await.expect("query");
        assert_eq!(result.rows, vec![vec!["3".to_string()]]);
    }

    /// V8: `COPY (SELECT ...) TO 'file.parquet'` writes a Parquet file via
    /// DataFusion's native `CopyTo` planner node. Round-trip via
    /// `read_parquet()` proves the data lands and the format is correct.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_to_parquet_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("out.parquet");

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let copy_sql = format!(
            "COPY (SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b') TO '{}'",
            path.display()
        );
        client.execute(&copy_sql).await.expect("copy");

        let count_sql = format!(
            "SELECT count(*) AS n FROM read_parquet('{}')",
            path.display()
        );
        let result = client.execute(&count_sql).await.expect("read");
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
    }

    /// V8: `COPY ... TO 'file.csv'` writes CSV. Round-trip via `read_csv()`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_to_csv_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("out.csv");

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let copy_sql = format!(
            "COPY (SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b') TO '{}'",
            path.display()
        );
        client.execute(&copy_sql).await.expect("copy");

        let count_sql = format!(
            "SELECT count(*) AS n FROM read_csv('{}')",
            path.display()
        );
        let result = client.execute(&count_sql).await.expect("read");
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
    }

    /// V12: `SELECT * FROM 'hf://...'` (URL-table auto-detect) rewrites the
    /// hf:// URL to its HTTPS equivalent before DataFusion sees the SQL.
    /// Proof: the resulting error mentions `https://huggingface.co/...`
    /// rather than `'hf://...' table not found`. We don't actually fetch
    /// from HuggingFace in a unit test; we only assert the rewrite ran.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_from_hf_url_rewrites_to_https() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        // Use a path that almost certainly does not exist so the fetch
        // itself fails. The rewrite must have happened first; otherwise
        // we'd see "table not found" with the literal hf:// URL.
        let sql = "SELECT * FROM 'hf://datasets/sqe-tests-not-real/v12-rewrite/data.csv'";
        let err = client.execute(sql).await.expect_err("hf:// URL must rewrite");
        let msg = format!("{err}");
        assert!(
            !msg.contains("'hf://"),
            "rewrite must consume the hf:// literal; error: {msg}"
        );
    }

    /// V12: malformed hf:// URLs surface a parse error early, before the
    /// network roundtrip. Without the rewrite, DataFusion would emit
    /// "table not found" with no hint about the URL shape.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn select_from_malformed_hf_url_errors_with_diagnostic() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = "SELECT * FROM 'hf://malformed'";
        let err = client.execute(sql).await.expect_err("malformed hf:// must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("malformed HuggingFace URL"),
            "expected diagnostic from rewriter, got: {msg}"
        );
    }

    /// V8: `read_csv()` honours the `delimiter` named argument. We use `;`
    /// as the separator (tab characters round-trip through the DataFusion
    /// SQL parser awkwardly).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_csv_tvf_custom_delimiter() {
        use std::io::Write;
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("semi.csv");
        let mut f = std::fs::File::create(&path).expect("create csv");
        writeln!(f, "id;name").expect("header");
        writeln!(f, "1;alice").expect("row1");
        writeln!(f, "2;bob").expect("row2");
        drop(f);

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = format!(
            "SELECT count(*) AS n FROM read_csv('{}', delimiter => ';')",
            path.display()
        );
        let result = client.execute(&sql).await.expect("query");
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
    }
}

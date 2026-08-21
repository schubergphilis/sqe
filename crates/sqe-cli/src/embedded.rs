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

use sqe_core::SecretStore;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::CatalogBuilder;
use iceberg_catalog_sql::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
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

/// How the embedded engine reaches S3-compatible object storage.
///
/// Embedded mode never loads an `sqe.toml`, so every `StorageConfig` it
/// built used to be `StorageConfig::default()`: empty endpoint, empty
/// keys. An `s3://` read then went to the real AWS endpoint and, with no
/// static key, on to the instance-metadata service, so
/// `--hive-root lake=s3://bucket/wh/` against MinIO, RustFS, Ceph RGW, or
/// Garage failed with a `169.254.169.254` connect timeout that names
/// nothing a user can act on. `AmazonS3Builder::new()` reads no
/// environment variables of its own, so the usual `AWS_*` exports did not
/// rescue it either: all three endpoint spellings (`AWS_ENDPOINT_URL`,
/// `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT`) were measured against a running
/// RustFS and all three produced the same IMDS timeout.
///
/// [`EmbeddedS3Options::resolve`] is where flags and environment meet, and
/// [`EmbeddedS3Options::to_storage_config`] is the single place the result
/// becomes a [`StorageConfig`], so the file TVFs (`read_parquet`,
/// `read_csv`, `read_json`) and Hive external-table roots always read the
/// same store with the same credentials. Wiring one and not the other
/// would produce an engine where `SELECT * FROM lake.sales.orders` works
/// and `read_csv('s3://…')` mysteriously does not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddedS3Options {
    /// Endpoint override, e.g. `http://localhost:9000`. Unset means real
    /// AWS S3.
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: Option<String>,
    /// Force virtual-hosted addressing (`bucket.host`) even when an
    /// endpoint override is set. See [`Self::to_storage_config`] for why
    /// an endpoint override otherwise implies path-style.
    pub virtual_hosted: bool,
}

impl EmbeddedS3Options {
    /// Resolve each setting from the CLI flag first, then the
    /// `SQE_STORAGE__S3_*` variables the coordinator's own config honors,
    /// then the standard `AWS_*` variables.
    ///
    /// `env` is a lookup function rather than a direct `std::env::var`
    /// call so this stays testable: process environment is global state,
    /// and a test that mutates it races every other test in the binary.
    pub fn resolve<F>(flags: &EmbeddedS3Options, env: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let pick = |flag: &Option<String>, names: &[&str]| -> Option<String> {
            if let Some(v) = flag.as_deref().filter(|v| !v.is_empty()) {
                return Some(v.to_string());
            }
            names
                .iter()
                .filter_map(|n| env(n))
                .find(|v| !v.is_empty())
        };

        Self {
            endpoint: pick(
                &flags.endpoint,
                &[
                    "SQE_STORAGE__S3_ENDPOINT",
                    "AWS_ENDPOINT_URL_S3",
                    "AWS_ENDPOINT_URL",
                ],
            ),
            access_key: pick(
                &flags.access_key,
                &["SQE_STORAGE__S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"],
            ),
            secret_key: pick(
                &flags.secret_key,
                &["SQE_STORAGE__S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"],
            ),
            region: pick(
                &flags.region,
                &["SQE_STORAGE__S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
            ),
            virtual_hosted: flags.virtual_hosted,
        }
    }

    /// Same as [`Self::resolve`] against the real process environment.
    pub fn resolve_from_env(flags: &EmbeddedS3Options) -> Self {
        Self::resolve(flags, |name| std::env::var(name).ok())
    }

    /// The engine-wide [`StorageConfig`] for this process.
    ///
    /// Local paths and `http(s)://` are allowed here for the same reason
    /// [`build_embedded_context`] used to allow them inline: the embedded
    /// CLI runs as the local user, who already owns this config, its
    /// credentials, and a shell. The server fails closed instead, because
    /// there the caller is a remote authenticated identity, not the owner
    /// of the process.
    ///
    /// An endpoint override implies path-style addressing unless
    /// `virtual_hosted` says otherwise. A custom endpoint in practice
    /// means a self-hosted store reached as `host:port`, where
    /// `bucket.host` does not resolve at all; defaulting the other way
    /// turns every MinIO or RustFS quickstart into a DNS puzzle. Real AWS,
    /// with no endpoint override, keeps virtual-hosted addressing.
    pub fn to_storage_config(&self) -> StorageConfig {
        let mut storage = StorageConfig::default();
        storage.tvf.allow_local_paths = true;
        storage.tvf.allow_http = true;

        if let Some(endpoint) = &self.endpoint {
            storage.s3_endpoint = endpoint.clone();
        }
        if let Some(key) = &self.access_key {
            storage.s3_access_key = key.clone();
        }
        if let Some(key) = &self.secret_key {
            storage.s3_secret_key = sqe_core::SecretString::new(key.clone());
        }
        if let Some(region) = &self.region {
            storage.s3_region = region.clone();
        }
        storage.s3_path_style = self.endpoint.is_some() && !self.virtual_hosted;
        storage
    }
}

/// Build a [`SessionContext`] suitable for embedded queries.
///
/// `memory_limit_bytes` caps the per-process query memory; values
/// below 64MB are clamped to that floor because DataFusion's hash
/// joins cannot make forward progress with smaller pools.
///
/// `storage` is the engine's object-store configuration, normally from
/// [`EmbeddedS3Options::to_storage_config`]. The file TVFs are registered
/// with it, and [`EmbeddedClient::attach_hive_root`] reads Hive manifests
/// and data with the same one.
///
/// Use [`build_embedded_context_with_warehouse`] when you also want
/// a persistent Iceberg catalog attached.
pub fn build_embedded_context(
    memory_limit_bytes: usize,
    storage: &StorageConfig,
) -> anyhow::Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "default")
        // Same DataFusion tuning the cluster coordinator applies. See
        // the comments in `sqe-coordinator/src/session_context.rs` for
        // the rationale on each flag.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        // DuckDB dialect: adds `x -> expr` lambda syntax for the Trino
        // higher-order array functions without changing identifier/quoting
        // behavior. See sqe-coordinator/src/session_context.rs and #354.
        .set_str("datafusion.sql_parser.dialect", "DuckDB")
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
    let registry = Arc::new(
        sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::new(
            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
        ),
    );
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_object_store_registry(registry)
        // #363: disable DataFusion 54's infinite-TTL list_files_cache so
        // directory reads of mutable external buckets always list fresh
        // (stale cached ObjectMeta.size -> "Corrupt footer" on grown files).
        .with_cache_manager(sqe_catalog::lazy_object_store::external_store_cache_config())
        .build_arc()
        .map_err(|e| anyhow::anyhow!("failed to build runtime env: {e}"))?;

    let ctx = SessionContext::new_with_config_rt(session_config, runtime);

    // V8: enable `SELECT * FROM 'file.parquet'` auto-detection. The wrapped
    // catalog list resolves quoted-string table names against
    // ListingTableFactory by extension (parquet / csv / json / avro).
    let ctx = ctx.enable_url_table();
    let mut ctx = ctx;

    // Scalar UDFs. SQE_POLICY__MASK_KEY enables HMAC-SHA256 in embedded
    // mode (issue #37). Without it the UDF stays plain SHA-256 and prints
    // a one-shot warning on registration.
    let mask_key = std::env::var("SQE_POLICY__MASK_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| Arc::new(s.into_bytes()));
    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf(mask_key));

    // Trino dialect compatibility — year(), month(), day_of_week(),
    // url_extract_*, etc. plus the extended set (regexp_extract,
    // word_stem, soundex). These are what dbt models and Trino-shape
    // queries rely on.
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);

    // JSON functions: json_get, json_get_str, json_contains, etc.
    datafusion_functions_json::register_all(&mut ctx)
        .map_err(|e| anyhow::anyhow!("failed to register JSON functions: {e}"))?;

    // File-format TVFs for direct file access. The embedded CLI runs
    // in-process as the local user, who already has shell + filesystem
    // access, so local paths are enabled here. The server defaults
    // `allow_local_paths` to false so a remote, authenticated user cannot
    // read the host's /etc/shadow or a mounted token; that threat model does
    // not apply to a local CLI, where rejecting local files would break the
    // tool's whole point. Users can still hit S3 by supplying inline
    // credentials in the TVF call.
    //
    // For the same reason, arbitrary http(s) fetches are enabled here. The
    // server defaults `allow_http` to false because a worker pod can reach the
    // cloud metadata service (http://169.254.169.254) with privileges higher
    // than the querying user's — an SSRF/credential-exfil vector. The embedded
    // CLI has no such privilege gap: it runs as the local user, who can already
    // `curl` any URL, so reading `read_csv('https://host/data.csv')` should just
    // work like it does in DuckDB. Operators who want a locked-down embedded
    // build can still gate egress at the network layer.
    // Object-store gating (E2E-identity item 1) is likewise skipped here:
    // `TvfCaller::trusted_local()` marks the embedded, in-process caller as
    // the owner of this config and its credentials, so `s3://` reads with
    // the locally-configured key keep working without an
    // `allowed_object_store_prefixes` list. The server constructs TVFs with
    // the session's authenticated identity instead and fails closed.
    //
    // `storage` arrives from `EmbeddedS3Options::to_storage_config`, which
    // is also what `attach_hive_root` passes to the Hive manifest store and
    // data-store factory. One config, so `read_csv('s3://…')` and
    // `SELECT * FROM <hive-root>.db.table` cannot disagree about which
    // endpoint or key to use.
    let tvf_storage = storage.clone();
    let tvf_caller = sqe_core::config::TvfCaller::trusted_local();
    // `with_runtime_env` is not optional for object-store paths. Each TVF
    // builds its S3/Azure/GCS client during schema inference and registers
    // it on a throwaway context; without the session's own `RuntimeEnv`
    // getting that same registration, planning succeeds and EXECUTION fails
    // with DataFusion's "No suitable object store found for s3://…", which
    // reads like an engine bug and is really a missing registration. The
    // coordinator already wires it (`session_context.rs`); embedded mode did
    // not, so every `read_csv('s3://…')` in embedded mode failed this way,
    // inline credentials or not.
    let tvf_runtime = ctx.runtime_env();
    ctx.register_udtf(
        "read_parquet",
        Arc::new(
            sqe_catalog::read_parquet::ReadParquetFunction::with_caller(
                tvf_storage.clone(),
                tvf_caller.clone(),
            )
            .with_runtime_env(Arc::clone(&tvf_runtime)),
        ),
    );
    ctx.register_udtf(
        "read_csv",
        Arc::new(
            sqe_catalog::read_csv::ReadCsvFunction::with_caller(
                tvf_storage.clone(),
                tvf_caller.clone(),
            )
            .with_runtime_env(Arc::clone(&tvf_runtime)),
        ),
    );
    ctx.register_udtf(
        "read_json",
        Arc::new(
            sqe_catalog::read_json::ReadJsonFunction::with_caller(
                tvf_storage.clone(),
                tvf_caller.clone(),
            )
            .with_runtime_env(Arc::clone(&tvf_runtime)),
        ),
    );
    // V11 Delta Lake reader (`read_delta` TVF) is temporarily disabled for the
    // DataFusion 54 bump: delta-rs (deltalake-core) has no DF 54 release yet, so
    // its `DeltaTableProvider` implements an older DataFusion's `TableProvider`.
    // The optional `delta` feature and the deltalake-core dependency are removed
    // until delta-rs ships a DF 54-compatible release. Re-enable by restoring the
    // `delta` feature (sqe-cli + sqe-catalog Cargo.toml), the `read_delta` module
    // in sqe-catalog/src/lib.rs, and this registration. read_delta.rs is kept on
    // disk unchanged for that purpose.
    let _ = (tvf_storage, tvf_caller);

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
    storage: &StorageConfig,
) -> anyhow::Result<(SessionContext, IcebergCatalogMap)> {
    let ctx = build_embedded_context(memory_limit_bytes, storage)?;
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
    std::fs::create_dir_all(&data_root)
        .map_err(|e| anyhow::anyhow!("failed to create data dir {}: {e}", data_root.display()))?;

    // SQLite URI is `sqlite://<absolute path>` per sqlx's parsing.
    // We canonicalise so relative paths in `--warehouse` work even
    // after later `cd` calls.
    let abs = path.canonicalize().map_err(|e| {
        anyhow::anyhow!(
            "failed to canonicalise warehouse path {}: {e}",
            path.display()
        )
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

/// Build an embedded context with a non-REST cloud catalog (Glue or
/// S3 Tables) attached read-only under `catalog_name`.
///
/// The catalog is built through the shared `sqe_catalog` backend
/// constructor and wrapped in `iceberg-datafusion`'s read-only
/// `IcebergCatalogProvider`, so `SELECT` works but writes do not (use
/// the server for writes). Credentials come from the AWS provider
/// chain (`AWS_PROFILE`, SSO, instance profile). Requires the `aws`
/// cargo feature on `sqe-cli`; without it the backend constructor
/// returns a clear "requires the ... cargo feature" error.
pub async fn build_embedded_context_with_backend(
    memory_limit_bytes: usize,
    backend: &sqe_core::config::CatalogBackend,
    catalog_name: &str,
    storage: &StorageConfig,
) -> anyhow::Result<SessionContext> {
    let ctx = build_embedded_context(memory_limit_bytes, storage)?;
    let catalog = sqe_catalog::SessionCatalog::build_backend_catalog(backend)
        .await
        .map_err(|e| anyhow::anyhow!("failed to build `{catalog_name}` catalog: {e}"))?;
    let provider = iceberg_datafusion::IcebergCatalogProvider::try_new(catalog)
        .await
        .map_err(|e| anyhow::anyhow!("failed to attach `{catalog_name}` catalog: {e}"))?;
    ctx.register_catalog(catalog_name, Arc::new(provider));
    Ok(ctx)
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
    /// In-memory secret store for `CREATE SECRET` / `DROP SECRET` / `SHOW SECRETS`.
    secrets: SecretStore,
    /// Names of catalogs attached at runtime via SQL `ATTACH`, paired with the
    /// optional secret name they reference. Used by `DROP SECRET` to guard
    /// against removing secrets while catalogs depend on them.
    attached_catalogs: HashMap<String, Option<String>>,
    /// Manifest cache shared by every Hive root attached in this process,
    /// the single-process embedded equivalent of the coordinator's one
    /// `HiveMetadataCache` instance (`main.rs`). One instance per
    /// `EmbeddedClient`, not per root: the cache key already includes the
    /// database and table name, not the root, so two roots' manifests
    /// never collide in it.
    hive_manifest_cache: Arc<sqe_catalog::hive::provider::HiveMetadataCache>,
    /// Every Hive root attached in this process, by the catalog name it was
    /// registered under. `attach_hive_root` populates this at the same time
    /// it registers the provider with DataFusion; DDL that needs to reach
    /// the manifest store directly (`CREATE EXTERNAL TABLE`, `DROP TABLE`,
    /// `SHOW CREATE TABLE`) resolves through here rather than downcasting
    /// the `Arc<dyn CatalogProvider>` DataFusion holds. Also doubles as the
    /// answer to "is `spec.catalog` a Hive root at all": a name absent from
    /// this map is either not attached or is some other kind of catalog.
    hive_roots: HashMap<String, Arc<sqe_catalog::hive::provider::HiveCatalogProvider>>,
    /// The engine's object-store configuration, as resolved once from
    /// `--s3-*` flags and the environment. Held so a Hive root attached
    /// after start-up reads with the same endpoint and credentials the file
    /// TVFs were registered with; see [`EmbeddedS3Options`].
    storage: StorageConfig,
}

impl EmbeddedClient {
    /// Build a memory-only embedded client. Sufficient for ad-hoc
    /// `read_parquet` queries; `CREATE TABLE` lives only for the
    /// session. Used by tests; production paths go through
    /// [`Self::with_warehouse`].
    #[allow(dead_code)]
    pub fn new(memory_limit_bytes: usize) -> anyhow::Result<Self> {
        let storage = EmbeddedS3Options::default().to_storage_config();
        Ok(Self {
            ctx: build_embedded_context(memory_limit_bytes, &storage)?,
            iceberg_catalogs: HashMap::new(),
            secrets: SecretStore::new(),
            attached_catalogs: HashMap::new(),
            hive_manifest_cache: Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
            hive_roots: HashMap::new(),
            storage,
        })
    }

    /// Build an embedded client with a chosen warehouse mode.
    /// `Persistent` attaches a SQLite-backed Iceberg catalog;
    /// `Memory` matches the legacy `new()` behaviour with no
    /// iceberg catalogs attached.
    pub async fn with_warehouse(
        memory_limit_bytes: usize,
        mode: &WarehouseMode,
        s3: &EmbeddedS3Options,
    ) -> anyhow::Result<Self> {
        let storage = s3.to_storage_config();
        let (ctx, iceberg_catalogs) =
            build_embedded_context_with_warehouse(memory_limit_bytes, mode, &storage).await?;
        Ok(Self {
            ctx,
            iceberg_catalogs,
            secrets: SecretStore::new(),
            attached_catalogs: HashMap::new(),
            hive_manifest_cache: Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
            hive_roots: HashMap::new(),
            storage,
        })
    }

    /// Build an embedded client backed by a cloud catalog (Glue or
    /// S3 Tables), attached read-only and mounted under `catalog_name`.
    /// Auth uses the AWS credential chain. Requires the `aws` cargo
    /// feature; without it the backend constructor returns a clear
    /// error naming the missing feature.
    pub async fn with_backend(
        memory_limit_bytes: usize,
        backend: sqe_core::config::CatalogBackend,
        catalog_name: &str,
        s3: &EmbeddedS3Options,
    ) -> anyhow::Result<Self> {
        let storage = s3.to_storage_config();
        let ctx = build_embedded_context_with_backend(
            memory_limit_bytes,
            &backend,
            catalog_name,
            &storage,
        )
        .await?;
        Ok(Self {
            ctx,
            iceberg_catalogs: HashMap::new(),
            secrets: SecretStore::new(),
            attached_catalogs: HashMap::new(),
            hive_manifest_cache: Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
            hive_roots: HashMap::new(),
            storage,
        })
    }

    /// Attach a Hive-style external-table warehouse root under `name`.
    ///
    /// Registers before the REPL starts (called from `main.rs` for
    /// `--hive-root NAME=URL`), but nothing about it is start-up-only: the
    /// same two calls (`build_hive_root` then `session_hive_catalog`) are
    /// what a later `ATTACH <location> AS <name> (TYPE hive_external)`
    /// would run in embedded mode too, once that SQL form is intercepted
    /// here the way `ATTACH ... TYPE iceberg_rest` already is.
    ///
    /// Embedded mode authenticates object-storage reads with
    /// `TvfCaller::trusted_local()`: the local user already owns this
    /// process's config and credentials, the same trust boundary the file
    /// TVFs (`read_parquet`, ...) use. Local paths and `http(s)://` are
    /// allowed for the same reason `build_embedded_context` allows them.
    ///
    /// Manifest caps use `ManifestLimits::default()`: `sqe-cli` never loads
    /// a `SqeConfig`/TOML (unlike the coordinator, which reads
    /// `[hive].manifest_max_bytes` et al. -- both at `[[hive.roots]]`
    /// startup and on `ATTACH ... TYPE hive_external`, see
    /// `query_handler.rs`'s `handle_attach`), so there is no `HiveConfig`
    /// here to source an override from. `ManifestLimits::default()` is
    /// exactly what `HiveConfig`'s own defaults resolve to, so this is not
    /// a weaker cap, just one with no CLI flag to change it yet.
    pub async fn attach_hive_root(&mut self, name: &str, location: &str) -> anyhow::Result<()> {
        if self.ctx.catalog(name).is_some() {
            anyhow::bail!("catalog '{name}' is already attached; DETACH it first");
        }

        let storage = self.storage.clone();
        let caller = sqe_core::config::TvfCaller::trusted_local();

        let root = sqe_catalog::mount::build_hive_root(
            location,
            &std::collections::BTreeMap::new(),
            &storage,
            sqe_catalog::hive::manifest::ManifestLimits::default(),
            caller.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("--hive-root {name}={location}: {e}"))?;

        let provider = sqe_catalog::mount::session_hive_catalog(
            name,
            root,
            &storage,
            &self.secrets,
            caller,
            self.ctx.runtime_env(),
            self.hive_manifest_cache.clone(),
            std::time::Duration::from_secs(60),
        );
        // Tracked separately from `ctx.register_catalog` (which only wants
        // the trait object) so DDL can reach `HiveCatalogProvider::store()`
        // and `invalidate_table_names()` directly instead of downcasting.
        self.hive_roots.insert(name.to_string(), provider.clone());
        self.ctx.register_catalog(name.to_string(), provider);
        Ok(())
    }
}

#[async_trait]
impl SqlClient for EmbeddedClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        // Intercept ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS
        // before DataFusion's parser sees the SQL — these are SQE-specific
        // syntax extensions that DataFusion has no AST for.
        match sqe_sql::parse_and_classify(sql) {
            Ok(kind) => match kind {
                sqe_sql::StatementKind::Attach(stmt) => {
                    return self.embedded_handle_attach(&stmt).await;
                }
                sqe_sql::StatementKind::Detach(stmt) => {
                    return self.embedded_handle_detach(&stmt);
                }
                sqe_sql::StatementKind::CreateSecret(stmt) => {
                    return self.embedded_handle_create_secret(&stmt);
                }
                sqe_sql::StatementKind::DropSecret(stmt) => {
                    return self.embedded_handle_drop_secret(&stmt);
                }
                sqe_sql::StatementKind::ShowSecrets => {
                    return self.embedded_handle_show_secrets();
                }
                sqe_sql::StatementKind::CreateExternalTable(spec) => {
                    return self.embedded_handle_create_external_table(&spec).await;
                }
                // DROP TABLE / SHOW CREATE TABLE are NOT SQE-specific syntax
                // (DataFusion parses both natively), so unlike the arms
                // above these only intercept when the target is a Hive
                // root; anything else falls through to the existing path a
                // few lines down, unchanged. That existing path is real for
                // non-Hive tables: `ctx.sql("DROP TABLE ...")` reaches
                // `IcebergSchemaProvider::deregister_table`, which really
                // calls `catalog.drop_table` (`vendor/iceberg-rust/.../
                // schema.rs`). For a Hive table it would not be a silent
                // no-op, but it would still be wrong: `HiveSchemaProvider`
                // inherits the default `SchemaProvider::deregister_table`,
                // and DataFusion 54.1's default for that is a loud
                // `exec_err!("schema provider does not support
                // deregistering tables")`, not `Ok(None)`. `SessionContext::
                // drop_table` then discards that error and reports
                // `Table '{name}' doesn't exist.` instead (`if_exists` is
                // false, so the match falls to its generic arm), which is a
                // real table given a false, misleading error rather than
                // ever actually being dropped. That is why a Hive drop must
                // be handled here instead of being allowed to fall through:
                // not to prevent a silent success, but to make DROP TABLE
                // work at all for a Hive table instead of always failing
                // with the wrong reason.
                sqe_sql::StatementKind::Drop(ref stmt) => {
                    if let Some(result) = self.embedded_handle_drop_hive_table(stmt).await? {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::ShowCreateTable(ref stmt) => {
                    if let Some(result) =
                        self.embedded_handle_show_create_hive_table(stmt).await?
                    {
                        return Ok(result);
                    }
                }
                _ => {}
            },
            // Our own classifier rejected this statement with a specific,
            // useful reason (e.g. `hive_ddl::column_of`'s "nested type ...
            // on column ..." naming both). Falling through silently would
            // hand the SQL to DataFusion's own `CREATE EXTERNAL TABLE`
            // grammar (a structurally different one, driven by its own
            // `DFParser`, not this crate's extractor), which either cannot
            // parse Hive-only clauses at all or, in the narrow case where it
            // can, fails registration against a Hive schema provider with
            // "schema provider does not support registering tables" --
            // correct on its face, but actively misleading about why.
            // Narrowed on purpose to the one shape this crate actually
            // classifies: an ordinary statement our classifier cannot parse
            // for an unrelated reason (a typo, a dialect feature we don't
            // handle) must keep reaching DataFusion's parser unchanged, not
            // get reframed as a Hive diagnostic that has nothing to do with
            // it.
            Err(e) if looks_like_create_external_table(sql) => {
                return Err(e.into());
            }
            Err(_) => {}
        }

        // V12: pre-rewrite `'hf://...'` quoted literals to their HTTPS
        // equivalent. This makes `SELECT * FROM 'hf://...'` (URL-table
        // auto-detect) flow through V10's LazyHttpObjectStoreRegistry the
        // same way `SELECT * FROM 'https://...'` already does. The TVF
        // path (`read_csv('hf://...')`) is unaffected; it does its own
        // rewrite via `rewrite_hf_path_in_place` inside the TVF.
        let rewritten = sqe_catalog::file_tvf_common::rewrite_hf_urls_in_sql(sql)?;
        // DF54: named TVF arguments (`delimiter => ';'`) are no longer
        // accepted by DataFusion directly.  Convert them to the positional
        // `'key=value'` form that `parse_file_tvf_args` understands, exactly
        // as the coordinator does before handing SQL to DataFusion.
        let rewritten = sqe_sql::rewrite_named_tvf_args(rewritten.as_ref());
        let df = self.ctx.sql(&rewritten).await?;
        // Snapshot the DataFrame schema before collecting so we still
        // emit column names when the query produces zero batches (an
        // optimizer collapse like `WHERE FALSE` yields an EmptyExec).
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        Ok(record_batches_to_query_result(&schema, &batches))
    }
}

impl EmbeddedClient {
    async fn embedded_handle_attach(
        &mut self,
        stmt: &sqe_sql::AttachStatement,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        if self.attached_catalogs.contains_key(&stmt.name) {
            return Err(format!(
                "catalog '{}' is already attached; DETACH it first",
                stmt.name
            )
            .into());
        }

        let catalog = sqe_catalog::mount::build_catalog(
            &stmt.location,
            stmt.kind,
            &stmt.options,
            &self.secrets,
        )
        .await
        .map_err(|e| format!("ATTACH failed: {e}"))?;

        let provider =
            sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog::try_new(catalog.clone())
                .await
                .map_err(|e| format!("ATTACH: failed to wrap catalog '{}': {e}", stmt.name))?;

        self.ctx
            .register_catalog(stmt.name.clone(), Arc::new(provider));

        let secret_ref = stmt
            .options
            .get("SECRET")
            .and_then(|v| v.as_secret_ref())
            .map(|s| s.to_string());
        self.attached_catalogs.insert(stmt.name.clone(), secret_ref);

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    fn embedded_handle_detach(
        &mut self,
        stmt: &sqe_sql::DetachStatement,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        if self.attached_catalogs.remove(&stmt.name).is_none() {
            return Err(format!("catalog '{}' is not attached", stmt.name).into());
        }
        // DataFusion 53.1 has no deregister_catalog. In embedded mode
        // enable_url_table() wraps the catalog list in DynamicFileCatalog
        // so the downcast to MemoryCatalogProviderList always fails.
        // The catalog stays in DataFusion's list until the session ends,
        // but our tracking no longer considers it attached — queries that
        // try to resolve it will fail at plan time once the provider is
        // removed from our accounting.
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    fn embedded_handle_create_secret(
        &mut self,
        stmt: &sqe_sql::CreateSecretStatement,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        // Embedded mode is a single-user binary, so there is no admin gate to
        // apply here; see #106 for the rationale. The shared helper keeps the
        // kind dispatch and option extraction symmetric with the coordinator
        // path so a new SecretKind variant lands once.
        let secret = sqe_sql::build_secret_from_stmt(stmt)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        self.secrets
            .create(&stmt.name, secret)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    fn embedded_handle_drop_secret(
        &mut self,
        stmt: &sqe_sql::DropSecretStatement,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let in_use: Vec<String> = self
            .attached_catalogs
            .iter()
            .filter(|(_, secret_ref)| secret_ref.as_deref() == Some(&stmt.name))
            .map(|(name, _)| name.clone())
            .collect();

        self.secrets
            .drop_secret(&stmt.name, &in_use)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    fn embedded_handle_show_secrets(&self) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let listed = self
            .secrets
            .list()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        Ok(QueryResult {
            columns: vec!["name".to_string(), "type".to_string()],
            rows: listed
                .into_iter()
                .map(|(name, type_name)| vec![name, type_name.to_string()])
                .collect(),
        })
    }

    /// `CREATE EXTERNAL TABLE` in embedded mode: write `spec`'s sidecar
    /// manifest into the resolved Hive root's warehouse.
    ///
    /// There is no "current" Hive root a session falls back to (unlike a
    /// DataFusion default catalog/schema), so `spec.catalog` must name an
    /// attached root explicitly; a 1- or 2-part table name errors rather
    /// than guessing.
    async fn embedded_handle_create_external_table(
        &mut self,
        spec: &sqe_sql::hive_ddl::HiveTableSpec,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let catalog_name = spec.catalog.as_deref().ok_or_else(
            || -> Box<dyn std::error::Error> {
                "CREATE EXTERNAL TABLE needs a catalog-qualified name in embedded mode \
                 (e.g. `lake.sales.orders`); there is no \"current\" Hive root to fall \
                 back to"
                    .into()
            },
        )?;
        let provider = self
            .hive_roots
            .get(catalog_name)
            .cloned()
            .ok_or_else(|| -> Box<dyn std::error::Error> {
                format!(
                    "hive root '{catalog_name}' is not attached; attach it with \
                     --hive-root {catalog_name}=<url> (or ATTACH ... TYPE hive_external) \
                     before creating tables in it"
                )
                .into()
            })?;

        // "default", not a value read back from the session config: every
        // `WarehouseMode` in `build_embedded_context` hardcodes
        // "default"/"default", so there is no other schema this could ever
        // resolve to.
        let database = spec
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let owner = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "embedded".to_string());
        let created_at = chrono::Utc::now().to_rfc3339();
        let manifest = sqe_catalog::hive::manifest::HiveManifest::from_spec(
            spec,
            &database,
            &owner,
            &created_at,
        );

        provider.store().put(&manifest, spec.if_not_exists).await?;
        // So a `SHOW TABLES` (or a plan that calls `table_exist`) issued
        // right after sees the new table instead of a stale listing cached
        // for up to `cache_ttl` (60s in `attach_hive_root`).
        provider.invalidate_table_names(&database);

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    /// `DROP TABLE` against a Hive root: delete the manifest, never the
    /// data. `Ok(None)` means "not a Hive drop" (no catalog qualifier, an
    /// unattached or non-Hive catalog, or more than one name), so the
    /// caller falls through to DataFusion's own drop path unchanged.
    ///
    /// That fall-through path is real, not inert, for a non-Hive table:
    /// `ctx.sql("DROP TABLE ...")` reaches
    /// `IcebergSchemaProvider::deregister_table`
    /// (`vendor/iceberg-rust/crates/integrations/datafusion/src/schema.rs`),
    /// which calls `catalog.drop_table` and genuinely drops it. It would be
    /// a silent no-op for a Hive table, though: `HiveSchemaProvider`
    /// inherits DataFusion's default `deregister_table`, which just returns
    /// `Ok(None)` without touching the manifest. That is exactly why a Hive
    /// drop must be intercepted here rather than allowed to fall through.
    async fn embedded_handle_drop_hive_table(
        &mut self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        let sqlparser::ast::Statement::Drop {
            names,
            if_exists,
            purge,
            ..
        } = stmt
        else {
            return Ok(None);
        };
        // Only a single, 3-part name resolving to an attached Hive root is
        // handled here. `DROP TABLE a, b` naming a Hive table alongside
        // anything else has no single manifest to delete, so it is left to
        // fall through rather than silently handling only the first name.
        let [name] = names.as_slice() else {
            return Ok(None);
        };
        let parts = object_name_parts(name);
        let [catalog, database, table] = parts.as_slice() else {
            return Ok(None);
        };
        let Some(provider) = self.hive_roots.get(catalog).cloned() else {
            return Ok(None);
        };

        if *purge {
            return Err(format!(
                "DROP TABLE ... PURGE is not supported for Hive external tables \
                 ({database}.{table}): PURGE deletes the underlying data, which an \
                 external table's DROP never does"
            )
            .into());
        }

        match provider.store().delete(database, table).await {
            Ok(()) => {}
            Err(sqe_catalog::hive::HiveError::NotFound(_)) if *if_exists => {}
            Err(e) => return Err(e.into()),
        }
        provider.invalidate_table_names(database);

        Ok(Some(QueryResult {
            columns: vec![],
            rows: vec![],
        }))
    }

    /// `SHOW CREATE TABLE` against a Hive root: render Athena/Glue-style DDL
    /// from the manifest. `Ok(None)` means "not a Hive table", so the
    /// caller falls through to whatever embedded mode already does for
    /// other tables (today, DataFusion has no native `SHOW CREATE TABLE`;
    /// unrelated to this task).
    async fn embedded_handle_show_create_hive_table(
        &mut self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        let sqlparser::ast::Statement::ShowCreate { obj_name, .. } = stmt else {
            return Ok(None);
        };
        let parts = object_name_parts(obj_name);
        let [catalog, database, table] = parts.as_slice() else {
            return Ok(None);
        };
        let Some(provider) = self.hive_roots.get(catalog).cloned() else {
            return Ok(None);
        };

        let manifest = provider.store().get(database, table).await?;
        let spec = sqe_catalog::hive::hive_spec_from_manifest(catalog, &manifest);
        let ddl = sqe_catalog::hive::render_hive_create_table_ddl(&spec);

        Ok(Some(QueryResult {
            columns: vec!["Create Table".to_string()],
            rows: vec![vec![ddl]],
        }))
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
            .map(|col| {
                arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default())
            })
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row_idx).to_string())
                .collect();
            rows.push(row);
        }
    }
    QueryResult { columns, rows }
}

/// True when `sql`, after skipping leading whitespace and SQL comments,
/// begins with the words `CREATE EXTERNAL TABLE` (case-insensitive, any
/// amount of whitespace between them). Used only to decide whether a
/// `sqe_sql::parse_and_classify` failure is "ours" and should be surfaced
/// directly instead of falling through to DataFusion's own, structurally
/// different `CREATE EXTERNAL TABLE` grammar. Deliberately the narrowest
/// check that does the job: a textual prefix match, not a parse attempt,
/// so a statement this crate cannot classify for any other reason is never
/// mistaken for one of ours.
fn looks_like_create_external_table(sql: &str) -> bool {
    sqe_sql::is_create_external_table(sql)
}

/// Lowercase an unquoted identifier, preserve a quoted one. Mirrors the
/// private `ident_text` in `sqe_sql::hive_ddl` (issue #337 folding), which
/// this module cannot call directly since it is not exported; kept
/// consistent so a `DROP TABLE`/`SHOW CREATE TABLE` reference folds the same
/// way the `CREATE EXTERNAL TABLE` for the same object did.
fn ident_text(i: &sqlparser::ast::Ident) -> String {
    match i.quote_style {
        Some(_) => i.value.clone(),
        None => i.value.to_ascii_lowercase(),
    }
}

/// The dotted parts of an object name, folded via [`ident_text`].
fn object_name_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(ident_text))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stub environment, so these tests never touch process-global
    /// `std::env` and cannot race the rest of the binary.
    fn env_of<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn s3_flag_beats_sqe_env_which_beats_aws_env() {
        let env = env_of(&[
            ("SQE_STORAGE__S3_ENDPOINT", "http://from-sqe-env:9000"),
            ("AWS_ENDPOINT_URL", "http://from-aws-env:9000"),
            ("SQE_STORAGE__S3_ACCESS_KEY", "sqe-key"),
            ("AWS_ACCESS_KEY_ID", "aws-key"),
            ("AWS_SECRET_ACCESS_KEY", "aws-secret"),
            ("AWS_DEFAULT_REGION", "eu-west-1"),
        ]);
        let resolved = EmbeddedS3Options::resolve(
            &EmbeddedS3Options {
                endpoint: Some("http://from-flag:9000".to_string()),
                ..EmbeddedS3Options::default()
            },
            env,
        );
        // Flag wins where given.
        assert_eq!(resolved.endpoint.as_deref(), Some("http://from-flag:9000"));
        // `SQE_STORAGE__*` wins over `AWS_*` for the same setting.
        assert_eq!(resolved.access_key.as_deref(), Some("sqe-key"));
        // `AWS_*` still fills what neither a flag nor `SQE_STORAGE__*` set.
        assert_eq!(resolved.secret_key.as_deref(), Some("aws-secret"));
        assert_eq!(resolved.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn an_empty_flag_value_falls_through_to_the_environment() {
        // `--s3-endpoint ""` is a user saying nothing, not a user asking for
        // an empty endpoint: an empty string in `StorageConfig` means "real
        // AWS" and would silently ignore the environment they did set.
        let resolved = EmbeddedS3Options::resolve(
            &EmbeddedS3Options {
                endpoint: Some(String::new()),
                ..EmbeddedS3Options::default()
            },
            env_of(&[("AWS_ENDPOINT_URL", "http://rustfs:9000")]),
        );
        assert_eq!(resolved.endpoint.as_deref(), Some("http://rustfs:9000"));
    }

    #[test]
    fn aws_endpoint_url_s3_is_preferred_over_the_generic_spelling() {
        let resolved = EmbeddedS3Options::resolve(
            &EmbeddedS3Options::default(),
            env_of(&[
                ("AWS_ENDPOINT_URL", "http://generic:9000"),
                ("AWS_ENDPOINT_URL_S3", "http://s3-specific:9000"),
            ]),
        );
        assert_eq!(resolved.endpoint.as_deref(), Some("http://s3-specific:9000"));
    }

    #[test]
    fn an_endpoint_override_implies_path_style_addressing() {
        let storage = EmbeddedS3Options {
            endpoint: Some("http://localhost:19100".to_string()),
            ..EmbeddedS3Options::default()
        }
        .to_storage_config();
        // `bucket.localhost:19100` does not resolve; path-style is the only
        // addressing that reaches a self-hosted store.
        assert!(storage.s3_path_style);
        assert_eq!(storage.s3_endpoint, "http://localhost:19100");
    }

    #[test]
    fn virtual_hosted_opts_back_out_of_path_style() {
        let storage = EmbeddedS3Options {
            endpoint: Some("http://minio.example:9000".to_string()),
            virtual_hosted: true,
            ..EmbeddedS3Options::default()
        }
        .to_storage_config();
        assert!(!storage.s3_path_style);
    }

    #[test]
    fn no_endpoint_override_keeps_virtual_hosted_addressing_for_real_aws() {
        let storage = EmbeddedS3Options::default().to_storage_config();
        assert!(!storage.s3_path_style);
        assert!(storage.s3_endpoint.is_empty());
    }

    #[test]
    fn credentials_land_in_the_storage_config_and_the_secret_stays_wrapped() {
        let storage = EmbeddedS3Options {
            access_key: Some("AKIAEXAMPLE".to_string()),
            secret_key: Some("s3cr3t".to_string()),
            region: Some("us-east-1".to_string()),
            ..EmbeddedS3Options::default()
        }
        .to_storage_config();
        assert_eq!(storage.s3_access_key, "AKIAEXAMPLE");
        assert_eq!(storage.s3_secret_key.expose(), "s3cr3t");
        assert_eq!(storage.s3_region, "us-east-1");
        // The secret must not leak through Debug: the CLI prints its
        // resolved config on some error paths, and `ps` already saw the flag.
        assert!(
            !format!("{:?}", storage.s3_secret_key).contains("s3cr3t"),
            "SecretString must redact on Debug"
        );
    }

    #[test]
    fn embedded_storage_trusts_the_local_user_for_files_and_http() {
        // Embedded mode runs as the local user, who owns this config and a
        // shell already. The server fails closed instead; if this ever
        // flips, `read_csv('/tmp/x.csv')` stops working in the CLI.
        let storage = EmbeddedS3Options::default().to_storage_config();
        assert!(storage.tvf.allow_local_paths);
        assert!(storage.tvf.allow_http);
    }

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
        let result = client.execute("SELECT 1 WHERE FALSE").await.expect("query");
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
        let result = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default()).await;
        let err = result.map(|_| ()).expect_err("duplicate names must error");
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
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use iceberg::{Catalog, NamespaceIdent, TableCreation};

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

        let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default())
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
    //
    // #195: this test wedged the 1h `cargo-test` job twice. The per-phase
    // `tokio::time::timeout` guard added after the first wedge never fired on
    // the second one (pipeline 1482960: no guard panic in the log, 35min
    // stall): the hang blocks the current-thread runtime without yielding, so
    // no timer can run. The only guard that works against a blocking wedge is
    // an OS-level one: run the whole test on a sacrificial thread with its own
    // runtime and join it with a hard deadline. On deadline the test fails in
    // minutes naming the issue; the wedged thread stays detached and dies with
    // the test process. The async per-phase bounds are kept inside the body so
    // an async-visible stall still names its phase.
    #[test]
    fn persistent_warehouse_survives_client_restart() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rt.block_on(persistent_warehouse_body());
            }));
            let _ = tx.send(result);
        });
        match rx.recv_timeout(std::time::Duration::from_secs(300)) {
            Ok(Ok(())) => {}
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!(
                "#195 guard: test wedged for 300s in a blocking call the async \
                 timeouts cannot see; failing fast instead of stalling the job"
            ),
        }
    }

    async fn persistent_warehouse_body() {
        use std::time::Duration;
        // Run `fut`, failing the test fast if it doesn't finish within `secs`,
        // so an async-visible stall names its phase.
        async fn bounded<F: std::future::Future>(phase: &str, secs: u64, fut: F) -> F::Output {
            match tokio::time::timeout(Duration::from_secs(secs), fut).await {
                Ok(v) => v,
                Err(_) => panic!("#195 guard: phase '{phase}' did not complete within {secs}s"),
            }
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let mode = WarehouseMode::single(tmp.path().to_path_buf());

        // Phase 1: create namespace + table via SQL.
        {
            let mut c = bounded(
                "phase1_open",
                90,
                EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default()),
            )
            .await
            .expect("first client");
            bounded(
                "create_schema",
                90,
                c.execute("CREATE SCHEMA iceberg.test_ns"),
            )
            .await
            .expect("CREATE SCHEMA");
            bounded(
                "create_table",
                90,
                c.execute("CREATE TABLE iceberg.test_ns.greetings (id BIGINT, msg VARCHAR)"),
            )
            .await
            .expect("CREATE TABLE");
        }

        // Phase 2: build the embedded client, confirm the iceberg
        // catalog is registered and the namespace + table are visible
        // via DataFusion's information_schema.
        let mut c = bounded(
            "phase2_open",
            90,
            EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default()),
        )
        .await
        .expect("client builds against existing warehouse");
        let r = bounded(
            "phase2_query",
            90,
            c.execute(
                "SELECT table_schema, table_name \
                 FROM information_schema.tables \
                 WHERE table_catalog = 'iceberg' AND table_schema = 'test_ns' \
                 ORDER BY table_name",
            ),
        )
        .await
        .expect("information_schema.tables");
        assert_eq!(
            r.columns,
            vec!["table_schema".to_string(), "table_name".to_string()]
        );
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
        let mut c = EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default())
            .await
            .expect("memory client builds without disk");
        // Sanity: the session works.
        let r = c.execute("SELECT 1").await.expect("query");
        assert_eq!(r.rows, vec![vec!["1".to_string()]]);
    }

    /// V8: `read_csv()` round-trips a local CSV file. The TVF wraps a
    /// `ListingTable` over `CsvFormat` and the embedded mode enables
    /// `tvf.allow_local_paths`, so local filesystem paths just work.
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
        let sql = format!("SELECT count(*) AS n FROM read_csv('{}')", path.display());
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
        let sql = format!("SELECT count(*) AS n FROM read_json('{}')", path.display());
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

        let count_sql = format!("SELECT count(*) AS n FROM read_csv('{}')", path.display());
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
        let err = client
            .execute(sql)
            .await
            .expect_err("hf:// URL must rewrite");
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
        let err = client
            .execute(sql)
            .await
            .expect_err("malformed hf:// must error");
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

    // ------------------------------------------------------------------
    // Phase F: ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn create_bearer_secret_then_show() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        client
            .execute("CREATE SECRET tok1 (TYPE bearer, TOKEN 'abc')")
            .await
            .expect("create");
        let r = client.execute("SHOW SECRETS").await.expect("show");
        assert_eq!(r.columns, vec!["name".to_string(), "type".to_string()]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], "tok1");
        assert_eq!(r.rows[0][1], "bearer");
        assert!(
            !r.rows[0].iter().any(|v| v.contains("abc")),
            "token must not appear in output"
        );
    }

    #[tokio::test]
    async fn create_aws_secret_all_optional() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        client
            .execute("CREATE SECRET s3cred (TYPE aws, ACCESS_KEY_ID 'AKIA1', SECRET_ACCESS_KEY 'sk', REGION 'eu-west-1')")
            .await
            .expect("create aws secret");
        let r = client.execute("SHOW SECRETS").await.expect("show");
        assert_eq!(r.rows[0][1], "aws");
    }

    #[tokio::test]
    async fn create_duplicate_secret_errors() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        client
            .execute("CREATE SECRET dup (TYPE bearer, TOKEN 't')")
            .await
            .expect("first");
        let err = client
            .execute("CREATE SECRET dup (TYPE bearer, TOKEN 't2')")
            .await
            .expect_err("duplicate should fail");
        assert!(err.to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn create_bearer_missing_token_errors() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let err = client
            .execute("CREATE SECRET bad (TYPE bearer)")
            .await
            .expect_err("missing TOKEN should fail");
        assert!(err.to_string().contains("TOKEN"));
    }

    #[tokio::test]
    async fn drop_secret_removes_it() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        client
            .execute("CREATE SECRET eph (TYPE bearer, TOKEN 't')")
            .await
            .expect("create");
        client.execute("DROP SECRET eph").await.expect("drop");
        let r = client.execute("SHOW SECRETS").await.expect("show");
        assert_eq!(r.rows.len(), 0);
    }

    #[tokio::test]
    async fn drop_missing_secret_errors() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let err = client
            .execute("DROP SECRET nope")
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn show_secrets_empty() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let r = client.execute("SHOW SECRETS").await.expect("show");
        assert_eq!(r.columns, vec!["name".to_string(), "type".to_string()]);
        assert_eq!(r.rows.len(), 0);
    }

    #[tokio::test]
    async fn attach_sqlite_catalog_and_create_schema() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");

        client
            .execute(&format!("ATTACH '{path}' AS mycat (TYPE sqlite)"))
            .await
            .expect("ATTACH should succeed");

        // Create a namespace in the attached catalog — proves the catalog is
        // reachable and WritableIcebergCatalog is wired correctly.
        client
            .execute("CREATE SCHEMA mycat.myns")
            .await
            .expect("CREATE SCHEMA in attached catalog should succeed");

        // The schema must now appear in information_schema.schemata
        let r = client
            .execute(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE catalog_name = 'mycat' AND schema_name = 'myns'",
            )
            .await
            .expect("information_schema query");
        assert!(
            r.rows.iter().any(|row| row[0] == "myns"),
            "namespace 'myns' must appear in information_schema; got {:?}",
            r.rows
        );
    }

    #[tokio::test]
    async fn attach_duplicate_catalog_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");

        let sql = format!("ATTACH '{path}' AS dupcat (TYPE sqlite)");
        client.execute(&sql).await.expect("first attach");
        let err = client
            .execute(&sql)
            .await
            .expect_err("duplicate attach should fail");
        assert!(err.to_string().contains("already attached"));
    }

    #[tokio::test]
    async fn detach_removes_tracking() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");

        client
            .execute(&format!("ATTACH '{path}' AS detachme (TYPE sqlite)"))
            .await
            .expect("attach");
        client.execute("DETACH detachme").await.expect("detach");

        // After detach, attaching with the same name must succeed (tracking cleared)
        client
            .execute(&format!("ATTACH '{path}' AS detachme (TYPE sqlite)"))
            .await
            .expect("re-attach after detach");
    }

    #[tokio::test]
    async fn detach_unknown_catalog_errors() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let err = client
            .execute("DETACH nope")
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("not attached"));
    }

    #[tokio::test]
    async fn drop_secret_in_use_by_attach_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");

        client
            .execute("CREATE SECRET tok (TYPE bearer, TOKEN 'xyz')")
            .await
            .expect("create");
        // ATTACH with SECRET ref. SQLite doesn't use the bearer token for
        // auth but the parser still records the secret_ref in options.
        client
            .execute(&format!(
                "ATTACH '{path}' AS guarded (TYPE sqlite, SECRET tok)"
            ))
            .await
            .expect("attach with secret ref");

        let err = client
            .execute("DROP SECRET tok")
            .await
            .expect_err("in-use drop should fail");
        let msg = err.to_string();
        assert!(msg.contains("tok"), "error should name the secret: {msg}");
        assert!(
            msg.contains("guarded"),
            "error should name the catalog: {msg}"
        );
    }

    /// Build an embedded client with a Hive root on a temp dir holding a
    /// partitioned CSV tree (two partitions, three rows total). No manifest
    /// is written here; each test creates the table itself via `CREATE
    /// EXTERNAL TABLE`. Files are literally named `a.csv` so
    /// `drop_removes_the_manifest_and_leaves_the_data` can assert on a
    /// stable path. Returns the client and the temp dir, whose `data` and
    /// `wh` subdirectories are the fixture's data and warehouse roots.
    async fn client_with_hive_root() -> (EmbeddedClient, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("data");
        for (dt, rows) in [
            ("2026-01-01", "1,10.5\n2,20.0\n"),
            ("2026-01-02", "3,30.5\n"),
        ] {
            let partition = data.join(format!("dt={dt}"));
            std::fs::create_dir_all(&partition).expect("mkdir partition");
            std::fs::write(partition.join("a.csv"), format!("id,amount\n{rows}"))
                .expect("write csv");
        }
        let client = client_over_root(dir.path()).await;
        (client, dir)
    }

    /// A fresh `EmbeddedClient` with `lake` attached at `<root_dir>/wh`, the
    /// same warehouse-root convention `client_with_hive_root` uses. Shares
    /// no state with any other client returned by this function: a new
    /// `SessionContext`, a new process-local `HiveMetadataCache`, and a new
    /// `HiveManifestStore` handle, all independently pointed at the same
    /// on-disk root.
    async fn client_over_root(root_dir: &std::path::Path) -> EmbeddedClient {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client
            .attach_hive_root("lake", &format!("file://{}/wh", root_dir.display()))
            .await
            .expect("attach hive root");
        client
    }

    #[tokio::test]
    async fn create_external_table_then_select_round_trips_in_embedded_mode() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        let sql = format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            data.display()
        );
        c.execute(&sql).await.expect("create");
        let r = c
            .execute("SELECT count(*) AS n FROM lake.sales.orders")
            .await
            .expect("select");
        assert!(format!("{r:?}").contains('3'), "expected 3 rows, got {r:?}");
    }

    #[tokio::test]
    async fn the_definition_survives_a_new_client_because_it_is_in_the_root() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            data.display()
        ))
        .await
        .expect("create");

        // A fresh client over the same root, which is the whole point of the
        // sidecar manifest: no shared process state, no metastore.
        let mut c2 = client_over_root(dir.path()).await;
        let r = c2
            .execute("SELECT count(*) AS n FROM lake.sales.orders")
            .await
            .expect("select");
        assert!(
            format!("{r:?}").contains('3'),
            "definition did not survive: {r:?}"
        );
    }

    #[tokio::test]
    async fn drop_removes_the_manifest_and_leaves_the_data() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}'",
            data.display()
        ))
        .await
        .expect("create");
        c.execute("DROP TABLE lake.sales.orders")
            .await
            .expect("drop");
        assert!(
            c.execute("SELECT 1 FROM lake.sales.orders").await.is_err(),
            "table should be gone"
        );
        assert!(
            data.join("dt=2026-01-01/a.csv").exists(),
            "DROP TABLE must never delete data"
        );
    }

    #[tokio::test]
    async fn drop_table_purge_is_rejected_by_name() {
        // Not in the original brief; added because "PURGE is rejected"
        // (Step 3 of the task brief) had no test pinning it.
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}'",
            data.display()
        ))
        .await
        .expect("create");

        let err = c
            .execute("DROP TABLE lake.sales.orders PURGE")
            .await
            .expect_err("PURGE must be rejected");
        assert!(
            format!("{err}").contains("PURGE"),
            "error should name PURGE: {err}"
        );
        assert!(
            dir.path().join("wh/sales/orders/_sqe/table.json").exists(),
            "a rejected PURGE must not drop the manifest as a side effect"
        );
    }

    #[tokio::test]
    async fn show_create_table_renders_athena_compatible_ddl() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}'",
            data.display()
        ))
        .await
        .expect("create");

        let r = c
            .execute("SHOW CREATE TABLE lake.sales.orders")
            .await
            .expect("show");
        assert_eq!(r.rows.len(), 1, "expected exactly one DDL row: {r:?}");
        let ddl = r.rows[0][0].clone();
        for needle in ["CREATE EXTERNAL TABLE", "PARTITIONED BY", "LOCATION"] {
            assert!(ddl.contains(needle), "missing {needle} in: {ddl}");
        }

        // Round trip, read against the manifest by its literal on-disk path
        // (not by rebuilding a `HiveManifestStore`, which would only prove
        // the renderer agrees with itself): the rendered DDL must re-parse
        // to a spec equal to what the manifest on disk actually holds.
        let manifest_bytes =
            std::fs::read(dir.path().join("wh/sales/orders/_sqe/table.json"))
                .expect("read manifest from disk");
        let manifest: sqe_catalog::hive::manifest::HiveManifest =
            serde_json::from_slice(&manifest_bytes).expect("parse manifest json");
        let expected = sqe_catalog::hive::hive_spec_from_manifest("lake", &manifest);

        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &ddl)
                .expect("rendered DDL must parse");
        let reparsed = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
            .expect("rendered DDL must extract")
            .expect("rendered DDL must still be CREATE EXTERNAL TABLE");

        assert_eq!(reparsed, expected, "SHOW CREATE TABLE did not round-trip");

        // Literal anchors independent of `assert_eq!` above: a bug shared
        // between the renderer and `hive_spec_from_manifest` (e.g. both
        // silently dropping partition_keys) would satisfy that assertion
        // without actually rendering the real definition.
        assert_eq!(
            reparsed
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "amount"]
        );
        assert_eq!(
            reparsed
                .partition_keys
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["dt"]
        );
        assert_eq!(reparsed.location, format!("file://{}", data.display()));
    }

    #[tokio::test]
    async fn create_external_table_as_select_fails_at_the_parser_not_at_our_handler() {
        // sqlparser dispatches `CREATE EXTERNAL TABLE` to its own
        // `parse_create_external_table`, which has no `AS`/`parse_query`
        // path in any dialect: `ct.query` is unconditionally `None`
        // whenever `ct.external` is true. The rejection this test pins
        // happens at the parser boundary, first in
        // `sqe_sql::parse_and_classify` (which then fails the whole `if
        // let Ok(kind) = ...` in `execute`), then again in DataFusion's own
        // parser once the SQL falls through to `ctx.sql()`, not in any
        // handler this task adds. No code in this task can ever see this
        // shape.
        let (mut c, dir) = client_with_hive_root().await;
        let err = c
            .execute(&format!(
                "CREATE EXTERNAL TABLE lake.sales.t \
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
                 LOCATION 'file://{}' AS SELECT 1",
                dir.path().display()
            ))
            .await
            .expect_err("must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("AS") || msg.to_lowercase().contains("parse"),
            "expected a parser-level rejection, got: {msg}"
        );
    }

    #[tokio::test]
    async fn creating_a_table_in_a_root_that_is_not_attached_names_the_missing_root() {
        let (mut c, _dir) = client_with_hive_root().await;
        let err = c
            .execute(
                "CREATE EXTERNAL TABLE nosuch.sales.t (id INT) \
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
                 LOCATION 'file:///tmp/'",
            )
            .await
            .expect_err("must reject");
        assert!(
            format!("{err}").contains("nosuch"),
            "should name the root: {err}"
        );
    }

    #[tokio::test]
    async fn create_external_table_with_nested_type_surfaces_our_error_not_datafusions() {
        // The narrowing's whole point: our own extractor already computes
        // the specific, useful diagnostic (naming the type and the
        // column). Without the guard this reaches DataFusion's own,
        // structurally different `CREATE EXTERNAL TABLE` grammar instead,
        // which either can't parse the shape at all or, if it can (a bare
        // `STORED AS ... LOCATION ...` with no Hive-only clause), fails
        // with "schema provider does not support registering tables" --
        // technically true, actively misleading, and not the reason at all.
        let mut c = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let err = c
            .execute(
                "CREATE EXTERNAL TABLE lake.sales.badtype (tags ARRAY<STRING>) \
                 STORED AS PARQUET LOCATION 'file:///tmp/does-not-matter'",
            )
            .await
            .expect_err("must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("nested type") && msg.contains("tags"),
            "expected our own nested-type diagnostic naming the column, got: {msg}"
        );
        assert!(
            !msg.contains("schema provider") && !msg.contains("registering"),
            "must not be DataFusion's misleading schema-provider error: {msg}"
        );
    }

    #[tokio::test]
    async fn an_ordinary_unparseable_statement_still_falls_through_to_datafusion() {
        // The narrowing must not widen into swallowing every classifier
        // failure: something that isn't shaped like `CREATE EXTERNAL
        // TABLE` at all must still reach DataFusion's own parser and
        // surface DataFusion's own error, unchanged.
        let mut c = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let err = c.execute("SELEC 1").await.expect_err("must reject");
        let msg = format!("{err}");
        assert!(
            !msg.contains("hive_external") && !msg.contains("Hive external table"),
            "an unrelated parse failure must not be reframed as a Hive diagnostic: {msg}"
        );
    }
}

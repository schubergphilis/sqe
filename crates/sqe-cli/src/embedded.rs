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

use async_trait::async_trait;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;
use sqe_core::config::StorageConfig;

use sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog;

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
    /// STS session token. Never a CLI flag: the only credentials that need
    /// one are temporary, and they arrive through the environment.
    pub session_token: Option<String>,
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
            names.iter().filter_map(|n| env(n)).find(|v| !v.is_empty())
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
            session_token: pick(
                &flags.session_token,
                &["SQE_STORAGE__S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"],
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
        if let Some(token) = &self.session_token {
            storage.s3_session_token = sqe_core::SecretString::new(token.clone());
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
    build_embedded_context_with_mask(memory_limit_bytes, storage, None)
}

/// The memory catalog and schema DataFusion creates as the session default.
/// They stay registered when a session switches its default to an Iceberg
/// catalog, so time travel registers its one-statement pinned providers
/// here.
const TT_CATALOG: &str = "default";
const TT_SCHEMA: &str = "default";

/// The embedded engine's memory pool: the coordinator's default, greedy with
/// an unspillable headroom ([`sqe_core::memory_headroom`]).
///
/// It used to be a `FairSpillPool`, which splits the pool minus every
/// unspillable byte evenly across the registered spillable consumers. The
/// Iceberg scan's decode admissions are unspillable, so each aggregate
/// partition's cap shrank with them: q3 of the engine bench (150M rows,
/// 3 GB) spilled its final aggregate 88-96 times where the coordinator, on
/// the same data and limit, did not spill at all.
///
/// The coordinator also caps each sort at a share of the spillable bytes,
/// in the per-query pool it builds for every statement. This engine has no
/// per-query pool, so the share sits on this one: a greedy pool without it
/// lets the first sort partitions take every spillable byte and fails the
/// query on a sibling that cannot start.
fn embedded_memory_pool(memory_limit_bytes: usize) -> Arc<dyn MemoryPool> {
    const TOP_CONSUMERS_IN_ERROR: usize = 5;
    let pool_size = memory_limit_bytes.max(64 * 1024 * 1024);
    let tracked = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(pool_size),
        std::num::NonZeroUsize::new(TOP_CONSUMERS_IN_ERROR).expect("non-zero const"),
    ));
    sqe_core::memory_headroom::HeadroomPool::wrap_single_query(
        tracked,
        pool_size,
        sqe_core::memory_headroom::DEFAULT_UNSPILLABLE_HEADROOM_PERCENT,
        "sqe-cli --memory-limit",
    )
}

/// Like [`build_embedded_context`], with an explicit HMAC mask key (from
/// `[policy] mask_key` in a loaded `sqe.toml`).
pub fn build_embedded_context_with_mask(
    memory_limit_bytes: usize,
    storage: &StorageConfig,
    mask_key: Option<&str>,
) -> anyhow::Result<SessionContext> {
    let session_config = sqe_catalog::engine_session_config()
        .with_information_schema(true)
        .with_default_catalog_and_schema(TT_CATALOG, TT_SCHEMA);

    let pool = embedded_memory_pool(memory_limit_bytes);
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

    let ctx = sqe_catalog::engine_context(session_config, runtime);
    // Iceberg scans fan out the way the coordinator's post-planning pass fans
    // them out: an `IcebergScanExec` starts as one partition (see the #131
    // note in `SqeTableProvider::scan`), and these two rules bump it to
    // `target_partitions` after join modes are decided, so no hash build is
    // coalesced. Appended after DataFusion's own physical rules. No byte
    // threshold: the coordinator's `distribution_threshold` exists to keep
    // small scans off workers, and in-process there is no hand-off to save.
    let ctx = SessionContext::new_with_state(
        datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state())
            .with_physical_optimizer_rule(Arc::new(
                sqe_catalog::parallel_probe_scan::ParallelProbeScanRule::new(),
            ))
            .with_physical_optimizer_rule(Arc::new(
                sqe_catalog::parallel_scan::ParallelScanRule::new(0),
            ))
            // Re-attach the scan filters those two orphaned (#787).
            .with_physical_optimizer_rule(Arc::new(
                sqe_catalog::dynamic_filter_wiring::SettleDynamicFiltersRule::new(),
            ))
            .build(),
    );

    // V8: enable `SELECT * FROM 'file.parquet'` auto-detection. The wrapped
    // catalog list resolves quoted-string table names against
    // ListingTableFactory by extension (parquet / csv / json / avro).
    let ctx = ctx.enable_url_table();
    let mut ctx = ctx;

    // Scalar UDFs. SQE_POLICY__MASK_KEY enables HMAC-SHA256 in embedded
    // mode (issue #37). Without it the UDF stays plain SHA-256 and prints
    // a one-shot warning on registration.
    let mask_key = mask_key
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Arc::new(s.as_bytes().to_vec()));
    // Embedded mode applies no masks, so a keyed user-callable sha256 is safe.
    ctx.register_udf(sqe_policy::sha256_udf::user_sha256_udf(mask_key));

    // Trino dialect compatibility — year(), month(), day_of_week(),
    // url_extract_*, etc. plus the extended set (regexp_extract,
    // word_stem, soundex). These are what dbt models and Trino-shape
    // queries rely on.
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);

    // JSON functions: json_get, json_get_str, json_contains, etc.
    datafusion_functions_json::register_all(&mut ctx)
        .map_err(|e| anyhow::anyhow!("failed to register JSON functions: {e}"))?;
    // A JSON column on a format v3 table is a variant; the `json*`
    // functions read it as its JSON text (#849). After every JSON function
    // is registered, so none is missed.
    sqe_core::variant_json_args::accept_variant_in_json_functions(&ctx);
    sqe_core::variant_get::register_variant_functions(&ctx);
    sqe_geo::register_geo_functions(&ctx);

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
    // `FileTableFunctions::register`, not `ctx.register_udtf`: the readers
    // build their provider on the async path (`prepare_statement` in
    // `execute`, right before `ctx.sql`) and their synchronous `call` only
    // looks it up (architecture review 806).
    use sqe_catalog::file_tvf_prepare::FileTableFunctions;
    FileTableFunctions::register(
        &ctx,
        "read_parquet",
        Arc::new(
            sqe_catalog::read_parquet::ReadParquetFunction::with_caller(
                tvf_storage.clone(),
                tvf_caller.clone(),
            )
            .with_runtime_env(Arc::clone(&tvf_runtime)),
        ),
    );
    FileTableFunctions::register(
        &ctx,
        "read_csv",
        Arc::new(
            sqe_catalog::read_csv::ReadCsvFunction::with_caller(
                tvf_storage.clone(),
                tvf_caller.clone(),
            )
            .with_runtime_env(Arc::clone(&tvf_runtime)),
        ),
    );
    FileTableFunctions::register(
        &ctx,
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
    mask_key: Option<&str>,
) -> anyhow::Result<(SessionContext, IcebergCatalogMap)> {
    let ctx = build_embedded_context_with_mask(memory_limit_bytes, storage, mask_key)?;
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

/// The writable Iceberg catalogs of this process keyed by user-facing name:
/// the `--catalog` SQLite warehouses and every `ATTACH`ed Iceberg catalog.
/// The DDL interceptor routes `CREATE SCHEMA` / `DROP SCHEMA` through these
/// handles (review 806): `SessionContext::catalog` hands back DataFusion's
/// `DynamicFileCatalogProvider` wrapper once URL tables are enabled, so the
/// provider cannot be recovered from the context by downcasting.
pub type IcebergCatalogMap = HashMap<String, Arc<WritableIcebergCatalog>>;

/// Open the embedded SQLite catalog at `db_path` (created if absent) with
/// `warehouse_uri` as the data root, under the fixed `sqe-embedded` scope.
///
/// Pure-Rust engine (`sqe_catalog::sqlite_rs_catalog`, always compiled in)
/// unless sqe-cli is built with the `sqlite-c` feature, which selects the
/// sqlx + libsqlite3 engine instead. Not gated on a default feature: the
/// release job, the Dockerfile and the Makefile build sqe-cli with
/// `--no-default-features`. Both engines read and write the same file, so a
/// warehouse survives switching.
#[cfg(not(feature = "sqlite-c"))]
async fn open_embedded_sqlite_catalog(
    db_path: &Path,
    warehouse_uri: &str,
) -> anyhow::Result<Arc<dyn iceberg::Catalog>> {
    use sqe_catalog::sqlite_rs_catalog::SqliteRsCatalog;
    let catalog = SqliteRsCatalog::open("sqe-embedded", db_path, warehouse_uri)
        .await
        .map_err(|e| anyhow::anyhow!("SQLite catalog open failed: {e}"))?;
    Ok(Arc::new(catalog))
}

#[cfg(feature = "sqlite-c")]
async fn open_embedded_sqlite_catalog(
    db_path: &Path,
    warehouse_uri: &str,
) -> anyhow::Result<Arc<dyn iceberg::Catalog>> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::{
        SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
    };
    // `mode=rwc` tells SQLite to create the file if missing; without it
    // sqlx fails with "unable to open database file" on the first run
    // because SQLite defaults to read-write without create.
    let mut props = HashMap::new();
    props.insert(
        SQL_CATALOG_PROP_URI.to_string(),
        format!("sqlite://{}?mode=rwc", db_path.display()),
    );
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse_uri.to_string(),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(sqe_catalog::storage_factory())
        .load("sqe-embedded".to_string(), props)
        .await
        .map_err(|e| anyhow::anyhow!("SQLite catalog open failed: {e}"))?;
    Ok(Arc::new(catalog))
}

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
) -> anyhow::Result<Arc<WritableIcebergCatalog>> {
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
    let warehouse_uri = format!("file://{}", abs.join("iceberg").display());

    // The catalog's `name` is NOT just diagnostic — it becomes the
    // `catalog_name` row key inside the SQLite
    // `iceberg_namespace_properties` and `iceberg_tables` tables, so
    // namespaces and tables are scoped per-name. We deliberately keep
    // the same fixed name across every embedded warehouse so each
    // SQLite file holds a single coherent scope. Catalog separation
    // for the user comes from each catalog living in its own SQLite
    // file (separate `path`); the user-facing identifier is set by
    // `ctx.register_catalog(name, ...)` below.
    let catalog = open_embedded_sqlite_catalog(&db_path, &warehouse_uri).await?;

    let provider = Arc::new(WritableIcebergCatalog::try_new(catalog).await?);

    ctx.register_catalog(
        name,
        Arc::clone(&provider) as Arc<dyn datafusion::catalog::CatalogProvider>,
    );
    Ok(provider)
}

/// Build an embedded context with a non-REST cloud catalog (Glue or
/// S3 Tables) attached read-only under `catalog_name`.
///
/// The catalog is built through the shared `sqe_catalog` backend
/// constructor and mounted through `iceberg-datafusion`'s
/// `IcebergCatalogProvider`, wrapped in `read_only_catalog::ReadOnlyCatalog` so `SELECT`
/// works and every write is refused (use the server for writes); the
/// provider alone would create, drop and insert into the real tables.
/// Credentials come from the AWS provider
/// chain (`AWS_PROFILE`, SSO, instance profile). Requires the `aws`
/// cargo feature on `sqe-cli`; without it the backend constructor
/// returns a clear "requires the ... cargo feature" error.
pub async fn build_embedded_context_with_backend(
    memory_limit_bytes: usize,
    backend: &sqe_core::config::CatalogBackend,
    catalog_name: &str,
    storage: &StorageConfig,
    mask_key: Option<&str>,
) -> anyhow::Result<SessionContext> {
    let ctx = build_embedded_context_with_mask(memory_limit_bytes, storage, mask_key)?;
    let catalog = sqe_catalog::SessionCatalog::build_backend_catalog(backend)
        .await
        .map_err(|e| anyhow::anyhow!("failed to build `{catalog_name}` catalog: {e}"))?;
    let provider = datafusion_iceberg::IcebergCatalogProvider::try_new(catalog)
        .await
        .map_err(|e| anyhow::anyhow!("failed to attach `{catalog_name}` catalog: {e}"))?;
    ctx.register_catalog(
        catalog_name,
        crate::read_only_catalog::ReadOnlyCatalog::wrap(catalog_name, Arc::new(provider)),
    );
    Ok(ctx)
}

/// `SqlClient` impl backed by an in-process [`SessionContext`].
///
/// Mirrors the network clients (`flight.rs`, `http.rs`) so the CLI's
/// REPL and one-shot paths don't need to special-case embedded mode.
/// The `iceberg_catalogs` map holds the `WritableIcebergCatalog` of every
/// writable Iceberg catalog registered with `ctx`, so `CREATE SCHEMA` /
/// `DROP SCHEMA` reach their async namespace methods without going through
/// the CatalogProvider trait object DataFusion holds (see
/// [`IcebergCatalogMap`]).
pub struct EmbeddedClient {
    ctx: SessionContext,
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
    /// The `iceberg_catalogs` attached with `TYPE iceberg_rest`. A REST
    /// `CREATE TABLE` carries its format version as a table property, which
    /// a local catalog refuses. The value is the REST `SessionCatalog` the
    /// view DDL runs on (the `iceberg::Catalog` trait has no view
    /// operations).
    rest_catalogs: HashMap<String, Arc<sqe_catalog::SessionCatalog>>,
    /// The `--catalog-backend glue|s3tables` catalog, mounted through
    /// `read_only_catalog::ReadOnlyCatalog`. Held by name because the
    /// context's `DynamicFileCatalog` wraps every catalog, so a downcast
    /// cannot find the read-only wrapper.
    read_only_catalog: Option<String>,
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
            rest_catalogs: HashMap::new(),
            read_only_catalog: None,
            storage,
        })
    }

    /// Build an embedded client with a chosen warehouse mode.
    /// `Persistent` attaches a SQLite-backed Iceberg catalog;
    /// `Memory` matches the legacy `new()` behaviour with no
    /// iceberg catalogs attached.
    #[allow(dead_code)]
    pub async fn with_warehouse(
        memory_limit_bytes: usize,
        mode: &WarehouseMode,
        s3: &EmbeddedS3Options,
    ) -> anyhow::Result<Self> {
        Self::with_warehouse_and_policy(memory_limit_bytes, mode, s3, None).await
    }

    /// Like [`Self::with_warehouse`], applying `[policy] mask_key` from a
    /// loaded `sqe.toml` so HMAC-SHA256 matches the server.
    pub async fn with_warehouse_and_policy(
        memory_limit_bytes: usize,
        mode: &WarehouseMode,
        s3: &EmbeddedS3Options,
        mask_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let storage = s3.to_storage_config();
        let (ctx, iceberg_catalogs) =
            build_embedded_context_with_warehouse(memory_limit_bytes, mode, &storage, mask_key)
                .await?;
        let client = Self {
            ctx,
            iceberg_catalogs,
            secrets: SecretStore::new(),
            attached_catalogs: HashMap::new(),
            hive_manifest_cache: Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
            hive_roots: HashMap::new(),
            rest_catalogs: HashMap::new(),
            read_only_catalog: None,
            storage,
        };
        client.register_metadata_tvfs();
        Ok(client)
    }

    /// (Re)register the Iceberg metadata TVFs the `"t$snapshots"` rewrite
    /// calls (`table_snapshots`, `table_files`, ...) over the current
    /// writable catalogs. Called again after ATTACH and DETACH, since the
    /// resolver holds a copy of the catalog map.
    ///
    /// `cat.ns."t$snapshots"` reads catalog `cat`. A bare `ns."t$snapshots"`
    /// reads the only Iceberg catalog, and is refused when there are several
    /// rather than guessed. No metadata policy: embedded mode has no policy
    /// engine.
    fn register_metadata_tvfs(&self) {
        use datafusion::catalog::TableFunctionImpl;
        use sqe_catalog::iceberg_metadata_tvf::{
            MetadataCatalogResolver, TableFilesFunction, TableHistoryFunction,
            TableManifestsFunction, TablePartitionsFunction, TableRefsFunction,
            TableSnapshotsFunction,
        };
        let catalogs = self.iceberg_catalogs.clone();
        let storage = self.storage.clone();
        let resolve: MetadataCatalogResolver = Arc::new(move |name: Option<&str>| {
            let (name, writable) = match name {
                Some(name) => catalogs.get_key_value(name).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "metadata table: '{name}' is not an Iceberg catalog"
                    ))
                })?,
                None if catalogs.len() == 1 => catalogs.iter().next().expect("one catalog"),
                None => {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "metadata table: qualify the table with its catalog, \
                         e.g. iceberg.ns.\"t$snapshots\""
                            .to_string(),
                    ))
                }
            };
            Ok(Arc::new(sqe_catalog::SessionCatalog::local(
                writable.client(),
                name,
                &storage,
            )))
        });
        let r = || resolve.clone();
        let functions: [(&str, Arc<dyn TableFunctionImpl>); 6] = [
            (
                "table_snapshots",
                Arc::new(TableSnapshotsFunction::with_catalog_resolver(r())),
            ),
            (
                "table_manifests",
                Arc::new(TableManifestsFunction::with_catalog_resolver(r())),
            ),
            (
                "table_history",
                Arc::new(TableHistoryFunction::with_catalog_resolver(r())),
            ),
            (
                "table_files",
                Arc::new(TableFilesFunction::with_catalog_resolver(r())),
            ),
            (
                "table_partitions",
                Arc::new(TablePartitionsFunction::with_catalog_resolver(r())),
            ),
            (
                "table_refs",
                Arc::new(TableRefsFunction::with_catalog_resolver(r())),
            ),
        ];
        for (name, function) in functions {
            self.ctx.register_udtf(name, function);
        }
    }

    pub async fn with_backend_and_policy(
        memory_limit_bytes: usize,
        backend: sqe_core::config::CatalogBackend,
        catalog_name: &str,
        s3: &EmbeddedS3Options,
        mask_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let storage = s3.to_storage_config();
        let ctx = build_embedded_context_with_backend(
            memory_limit_bytes,
            &backend,
            catalog_name,
            &storage,
            mask_key,
        )
        .await?;
        Ok(Self {
            ctx,
            iceberg_catalogs: HashMap::new(),
            secrets: SecretStore::new(),
            attached_catalogs: HashMap::new(),
            hive_manifest_cache: Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new()),
            hive_roots: HashMap::new(),
            rest_catalogs: HashMap::new(),
            read_only_catalog: Some(catalog_name.to_string()),
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
        // The SQL front door (finding 700): the same normalization the
        // coordinator applies over Flight SQL and the Trino server applies
        // over HTTP, so `INSERT INTO t VALUES 1`, `TABLE t` and `SELECT 1+1`
        // behave (and name their columns) identically in `--embedded`. The
        // text handed to DataFusion below is the normalized text when the
        // front door parsed the statement; a statement it cannot parse falls
        // through verbatim so DataFusion's own grammar (`CREATE EXTERNAL
        // TABLE ... STORED AS`, `COPY`) and error messages still apply.
        // `FOR {VERSION|TIMESTAMP|SYSTEM_TIME} AS OF` parses in neither the
        // front door nor DataFusion ("Expected: one of UPDATE or SHARE"), so
        // it is stripped first and the rest of the statement runs against
        // snapshot-pinned providers.
        let (stripped, specs) = sqe_sql::extract_time_travel_spec(sql)?;
        if !specs.is_empty() {
            return self.embedded_time_travel(&stripped, &specs).await;
        }
        let mut exec_sql = sql.to_string();
        // Intercept ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS
        // before DataFusion's parser sees the SQL — these are SQE-specific
        // syntax extensions that DataFusion has no AST for.
        match sqe_sql::SqlFrontDoor::new(sqe_sql::WireDialect::Embedded)
            .normalize(sql)
            .and_then(|normalized| {
                exec_sql = normalized.sql().to_string();
                sqe_sql::classify_normalized(normalized)
            }) {
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
                    self.refuse_drop_on_read_only(stmt)?;
                    if let Some(result) = self.embedded_handle_drop_hive_table(stmt).await? {
                        return Ok(result);
                    }
                    if let Some(result) = self.embedded_handle_iceberg_schema_ddl(stmt).await? {
                        return Ok(result);
                    }
                }
                // CREATE SCHEMA / DROP SCHEMA on a `WritableIcebergCatalog`
                // go to its async namespace methods; DataFusion's own
                // synchronous `register_schema` hook refuses them (review
                // 806). Any other catalog falls through to `ctx.sql`.
                sqe_sql::StatementKind::CreateSchema(ref stmt)
                | sqe_sql::StatementKind::DropSchema(ref stmt) => {
                    self.refuse_drop_on_read_only(stmt)?;
                    if let Some(result) = self.embedded_handle_iceberg_schema_ddl(stmt).await? {
                        return Ok(result);
                    }
                }
                // DataFusion's CREATE VIEW hands a `ViewTable` to the schema
                // provider's `register_table`, and the Iceberg one turns any
                // provider into a new, empty Iceberg *table*. So a view in an
                // Iceberg catalog never reaches `ctx.sql`: a REST catalog
                // gets a real Iceberg view, any other is refused.
                sqe_sql::StatementKind::CreateView(ref stmt)
                | sqe_sql::StatementKind::DropView(ref stmt) => {
                    self.refuse_drop_on_read_only(stmt)?;
                    if let Some(result) = self.embedded_handle_iceberg_view_ddl(stmt).await? {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::ShowCreateTable(ref stmt) => {
                    if let Some(result) = self.embedded_handle_show_create_hive_table(stmt).await? {
                        return Ok(result);
                    }
                    if let Some(result) =
                        self.embedded_handle_show_create_iceberg_table(stmt).await?
                    {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::Ctas(ref stmt) => {
                    if let Some(result) = self.embedded_handle_iceberg_ctas(stmt).await? {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::CreateTable(ref stmt) => {
                    if let Some(result) = self.embedded_handle_iceberg_create_table(stmt).await? {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::ShowTables(_) => {
                    if let Some(result) = self.embedded_handle_show_tables_in(&exec_sql).await? {
                        return Ok(result);
                    }
                }
                // DataFusion's DML hooks are not implemented by the Iceberg
                // table provider ("DELETE not supported for Base table"), so
                // DELETE / UPDATE / MERGE on a writable Iceberg catalog run
                // through the server's write path instead.
                sqe_sql::StatementKind::Insert(ref stmt) => {
                    if let Some(result) = self.embedded_handle_iceberg_insert(stmt).await? {
                        return Ok(result);
                    }
                }
                sqe_sql::StatementKind::Delete(ref stmt)
                | sqe_sql::StatementKind::Update(ref stmt)
                | sqe_sql::StatementKind::Merge(ref stmt) => {
                    if let Some(result) = self.embedded_handle_iceberg_dml(stmt).await? {
                        return Ok(result);
                    }
                }
                ref alter @ (sqe_sql::StatementKind::Rename(_)
                | sqe_sql::StatementKind::AlterSchema(_)
                | sqe_sql::StatementKind::AlterTableProps(_)
                | sqe_sql::StatementKind::DropNestedColumn { .. }
                | sqe_sql::StatementKind::AlterNestedColumnType { .. }
                | sqe_sql::StatementKind::RefDdl(_)
                | sqe_sql::StatementKind::PartitionEvolution(_)
                | sqe_sql::StatementKind::IdentifierFields(_)) => {
                    if let Some(result) = self.embedded_handle_iceberg_alter(alter).await? {
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

        self.run_datafusion(&exec_sql).await
    }
}

impl EmbeddedClient {
    /// Plan and run `exec_sql` (front-door normalized) on DataFusion.
    async fn run_datafusion(
        &self,
        exec_sql: &str,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let df = self.plan_datafusion(exec_sql).await?;
        // Snapshot the DataFrame schema before collecting so we still
        // emit column names when the query produces zero batches (an
        // optimizer collapse like `WHERE FALSE` yields an EmptyExec).
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        Ok(QueryResult::from_batches(&schema, &batches)?)
    }

    /// Plan one statement the way [`Self::run_datafusion`] runs it, without
    /// executing it.
    async fn plan_datafusion(
        &self,
        exec_sql: &str,
    ) -> Result<datafusion::dataframe::DataFrame, Box<dyn std::error::Error>> {
        // V12: pre-rewrite `'hf://...'` quoted literals to their HTTPS
        // equivalent. This makes `SELECT * FROM 'hf://...'` (URL-table
        // auto-detect) flow through V10's LazyHttpObjectStoreRegistry the
        // same way `SELECT * FROM 'https://...'` already does. The TVF
        // path (`read_csv('hf://...')`) is unaffected; it does its own
        // rewrite via `rewrite_hf_path_in_place` inside the TVF.
        let rewritten = sqe_catalog::file_tvf_common::rewrite_hf_urls_in_sql(exec_sql)?;
        // DF54: named TVF arguments (`delimiter => ';'`) are no longer
        // accepted by DataFusion directly.  Convert them to the positional
        // `'key=value'` form that `parse_file_tvf_args` understands, exactly
        // as the coordinator does before handing SQL to DataFusion. This is a
        // DataFusion version shim, not dialect normalization, which is why it
        // sits outside the front door.
        let rewritten = sqe_sql::rewrite_named_tvf_args(rewritten.as_ref());
        // Review 806: fill the Hive listing snapshots DataFusion's synchronous
        // `schema_names()` / `table_names()` hooks serve while planning, the
        // same way the coordinator's `refresh_listing_snapshots` does. A Hive
        // table is always 3-part in `--embedded` (no session default catalog
        // points at a root), so only `cat.db.t` references count.
        for provider in self.hive_roots.values() {
            provider.refresh_referenced(&rewritten, None, None).await;
        }
        // Same review: build the file table functions' providers (schema
        // inference over object storage) here, before DataFusion's
        // synchronous `call` asks for them.
        sqe_catalog::file_tvf_prepare::prepare_statement(&self.ctx, &rewritten).await?;
        // Embedded mode has no policy, and `CREATE TABLE ... AS` /
        // `CREATE EXTERNAL TABLE` here are DataFusion's own DDL.
        Ok(sqe_catalog::file_tvf_prepare::plan_sql_without_policy(&self.ctx, &rewritten).await?)
    }
}

/// The ATTACH an `iceberg_rest` catalog opens with in embedded mode. An
/// ATTACH that names no `S3_*` option reads the engine's own object store,
/// the one `--s3-endpoint` / `SQE_STORAGE__S3_*` / `AWS_*` configured, so
/// those settings fill the `S3_*` options; without them the catalog's FileIO
/// fell back to the ambient AWS chain and failed with "region is missing"
/// against a custom endpoint. Any `S3_*` option means the ATTACH describes
/// its own store and nothing is mixed in. Credentials the catalog vends
/// still replace these keys.
///
/// The server does not do this: there the configured keys are the
/// coordinator's, and a user's ATTACH must not borrow them.
fn with_storage_fallback<'a>(
    stmt: &'a sqe_sql::AttachStatement,
    storage: &StorageConfig,
) -> std::borrow::Cow<'a, sqe_sql::AttachStatement> {
    use sqe_sql::OptionValue;
    use std::borrow::Cow;

    if stmt.kind != sqe_sql::CatalogKind::IcebergRest
        || stmt.options.keys().any(|k| k.starts_with("S3_"))
    {
        return Cow::Borrowed(stmt);
    }
    let path_style = if storage.s3_path_style { "true" } else { "" };
    let fill = [
        ("S3_ENDPOINT", storage.s3_endpoint.as_str()),
        ("S3_REGION", storage.s3_region.as_str()),
        ("S3_ACCESS_KEY", storage.s3_access_key.as_str()),
        ("S3_SECRET_KEY", storage.s3_secret_key.expose()),
        ("S3_SESSION_TOKEN", storage.s3_session_token.expose()),
        ("S3_PATH_STYLE", path_style),
    ];
    if fill.iter().all(|(_, v)| v.is_empty()) {
        return Cow::Borrowed(stmt);
    }
    let mut filled = stmt.clone();
    for (key, value) in fill {
        if !value.is_empty() {
            filled
                .options
                .insert(key.to_string(), OptionValue::String(value.to_string()));
        }
    }
    Cow::Owned(filled)
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

        let filled = with_storage_fallback(stmt, &self.storage);
        let stmt: &sqe_sql::AttachStatement = &filled;
        let catalog = sqe_catalog::mount::build_catalog(
            &stmt.location,
            stmt.kind,
            &stmt.options,
            &self.secrets,
        )
        .await
        .map_err(|e| format!("ATTACH failed: {e}"))?;

        let mut provider =
            sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog::try_new(catalog.clone())
                .await
                .map_err(|e| format!("ATTACH: failed to wrap catalog '{}': {e}", stmt.name))?;
        let views = if stmt.kind == sqe_sql::CatalogKind::IcebergRest {
            let views = self.rest_view_catalog(stmt).await?;
            provider = provider.with_views(Arc::clone(&views), &stmt.name);
            Some(views)
        } else {
            None
        };
        // So SHOW TABLES lists the views. A listing the server refuses only
        // leaves them unlisted: each still loads by name.
        let _ = provider.refresh_all_views().await;
        let provider = Arc::new(provider);

        self.ctx.register_catalog(
            stmt.name.clone(),
            Arc::clone(&provider) as Arc<dyn datafusion::catalog::CatalogProvider>,
        );
        // So CREATE SCHEMA / DROP SCHEMA on this catalog reach its async
        // namespace methods (review 806); removed again on DETACH.
        self.iceberg_catalogs.insert(stmt.name.clone(), provider);
        if let Some(views) = views {
            self.rest_catalogs.insert(stmt.name.clone(), views);
        }
        self.register_metadata_tvfs();

        let secret_ref = stmt
            .options
            .get("SECRET")
            .and_then(|v| v.as_secret_ref())
            .map(|s| s.to_string());
        self.attached_catalogs.insert(stmt.name.clone(), secret_ref);

        Ok(QueryResult::empty())
    }

    /// The REST `SessionCatalog` view DDL and view reads of an
    /// `iceberg_rest` attach run on, built from the same properties as the
    /// attached catalog (URI, warehouse, token or OAuth2 credential, prefix,
    /// S3 options). With an OAuth2 secret the raw view calls carry the token
    /// the catalog's own client exchanged.
    async fn rest_view_catalog(
        &self,
        stmt: &sqe_sql::AttachStatement,
    ) -> Result<Arc<sqe_catalog::SessionCatalog>, Box<dyn std::error::Error>> {
        let mut props =
            sqe_catalog::mount::rest_attach_props(&stmt.location, &stmt.options, &self.secrets)
                .map_err(|e| format!("ATTACH failed: {e}"))?;
        let uri = props.remove("uri").unwrap_or_default();
        let warehouse = props.remove("warehouse").unwrap_or_default();
        let token = props.remove("token").unwrap_or_default();
        let catalog = sqe_catalog::SessionCatalog::new_with_props(
            &uri,
            &warehouse,
            &token,
            &self.storage,
            None,
            None,
            None,
            false,
            props,
        )
        .await
        .map_err(|e| format!("ATTACH: views of catalog '{}': {e}", stmt.name))?;
        Ok(Arc::new(catalog))
    }

    fn embedded_handle_detach(
        &mut self,
        stmt: &sqe_sql::DetachStatement,
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        if self.attached_catalogs.contains_key(&stmt.name) {
            self.iceberg_catalogs.remove(&stmt.name);
            self.rest_catalogs.remove(&stmt.name);
            self.register_metadata_tvfs();
        }
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
        Ok(QueryResult::empty())
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
        Ok(QueryResult::empty())
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
        Ok(QueryResult::empty())
    }

    fn embedded_handle_show_secrets(&self) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let listed = self
            .secrets
            .list()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        Ok(QueryResult::text(
            vec!["name".to_string(), "type".to_string()],
            listed
                .into_iter()
                .map(|(name, type_name)| vec![name, type_name.to_string()])
                .collect(),
        ))
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
        let catalog_name =
            spec.catalog
                .as_deref()
                .ok_or_else(|| -> Box<dyn std::error::Error> {
                    "CREATE EXTERNAL TABLE needs a catalog-qualified name in embedded mode \
                 (e.g. `lake.sales.orders`); there is no \"current\" Hive root to fall \
                 back to"
                        .into()
                })?;
        let provider = self.hive_roots.get(catalog_name).cloned().ok_or_else(
            || -> Box<dyn std::error::Error> {
                format!(
                    "hive root '{catalog_name}' is not attached; attach it with \
                     --hive-root {catalog_name}=<url> (or ATTACH ... TYPE hive_external) \
                     before creating tables in it"
                )
                .into()
            },
        )?;

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

        writable_store(&provider, catalog_name)?
            .put(&manifest, spec.if_not_exists)
            .await?;
        // So a `SHOW TABLES` (or a plan that calls `table_exist`) issued
        // right after sees the new table instead of a stale listing cached
        // for up to `cache_ttl` (60s in `attach_hive_root`).
        provider.invalidate_table_names(&database);

        Ok(QueryResult::empty())
    }

    /// `CREATE SCHEMA <cat>.<ns>` / `DROP SCHEMA <cat>.<ns>` where `<cat>` is
    /// a `WritableIcebergCatalog` (a `--catalog` SQLite warehouse or an
    /// attached Iceberg catalog): routed to its async namespace methods, the
    /// way the coordinator's `catalog_ops` does, instead of DataFusion's
    /// synchronous `register_schema` hook, which would have to block the
    /// planner on the catalog and now refuses (architecture review 806).
    /// `Ok(None)` means "not ours": a bare name, a catalog that is not
    /// registered, or one of another kind, all fall through to `ctx.sql`.
    async fn embedded_handle_iceberg_schema_ddl(
        &self,
        stmt: &datafusion::sql::sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use datafusion::sql::sqlparser::ast::{ObjectType, SchemaName, Statement};

        enum Ddl<'a> {
            Create {
                name: &'a datafusion::sql::sqlparser::ast::ObjectName,
                if_not_exists: bool,
            },
            Drop {
                name: &'a datafusion::sql::sqlparser::ast::ObjectName,
                if_exists: bool,
                cascade: bool,
            },
        }
        let ddl = match stmt {
            Statement::CreateSchema {
                schema_name: SchemaName::Simple(name),
                if_not_exists,
                ..
            } => Ddl::Create {
                name,
                if_not_exists: *if_not_exists,
            },
            Statement::Drop {
                object_type: ObjectType::Schema,
                names,
                if_exists,
                cascade,
                ..
            } if names.len() == 1 => Ddl::Drop {
                name: &names[0],
                if_exists: *if_exists,
                cascade: *cascade,
            },
            _ => return Ok(None),
        };

        let parts: Vec<String> = match &ddl {
            Ddl::Create { name, .. } | Ddl::Drop { name, .. } => name
                .0
                .iter()
                .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
                .collect(),
        };
        let [catalog_name, schema_name] = parts.as_slice() else {
            return Ok(None);
        };
        // Not through `self.ctx.catalog(..)`: with URL tables enabled
        // DataFusion hands back its `DynamicFileCatalogProvider` wrapper, so
        // the registered provider cannot be recovered by downcasting.
        let Some(catalog) = self.iceberg_catalogs.get(catalog_name.as_str()) else {
            return Ok(None);
        };

        match ddl {
            Ddl::Create { if_not_exists, .. } => {
                catalog.create_namespace(schema_name, if_not_exists).await?
            }
            Ddl::Drop {
                if_exists, cascade, ..
            } => {
                catalog
                    .drop_namespace(schema_name, if_exists, cascade)
                    .await?
            }
        }
        Ok(Some(QueryResult::empty()))
    }

    /// The session's default catalog, which a one- or two-part name resolves in.
    fn default_catalog(&self) -> String {
        self.ctx
            .state()
            .config()
            .options()
            .catalog
            .default_catalog
            .clone()
    }

    /// `CREATE TABLE <cat>.<ns>.<t> AS <query>` where `<cat>` is a
    /// `WritableIcebergCatalog`. DataFusion's CTAS registers a filled
    /// `MemTable`, which the Iceberg schema provider refuses ("register_table
    /// does not support tables with data"). It accepts an empty one, so the
    /// table is created from the query's schema with `LIMIT 0` and filled by
    /// an `INSERT`, the path a plain INSERT already takes. The two steps are
    /// not one commit: a failing INSERT drops the empty table again.
    /// `Ok(None)` means "not ours" and falls through to `ctx.sql`.
    async fn embedded_handle_iceberg_ctas(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            return Ok(None);
        };
        let Some(query) = ct.query.as_ref() else {
            return Ok(None);
        };
        let parts: Vec<&str> = ct
            .name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
            .collect();
        let catalog = match parts.as_slice() {
            [catalog, _, _] => catalog.to_string(),
            [_] | [_, _] => self.default_catalog(),
            _ => return Ok(None),
        };
        if ct.temporary || !ct.columns.is_empty() || !self.iceberg_catalogs.contains_key(&catalog) {
            return Ok(None);
        }
        let name = &ct.name;
        if ct.or_replace {
            return Err(format!(
                "CREATE OR REPLACE TABLE ... AS is not supported for Iceberg catalog \
                 `{catalog}` in embedded mode; DROP TABLE {name} first"
            )
            .into());
        }
        // The CTAS below runs through DataFusion's CREATE TABLE, which would
        // drop both silently.
        if ct.partition_by.is_some()
            || ct.cluster_by.is_some()
            || !matches!(
                ct.hive_distribution,
                sqlparser::ast::HiveDistributionStyle::NONE
            )
            || !matches!(ct.table_options, sqlparser::ast::CreateTableOptions::None)
        {
            return Err(format!(
                "CREATE TABLE ... AS with PARTITIONED BY, WITH or TBLPROPERTIES is not \
                 supported for Iceberg catalog `{catalog}` in embedded mode; CREATE TABLE \
                 {name} with a column list first, then INSERT INTO {name} SELECT ..."
            )
            .into());
        }
        let empty = QueryResult::empty();
        if self.ctx.table_exist(name.to_string().as_str())? {
            if ct.if_not_exists {
                return Ok(Some(empty));
            }
            return Err(format!("table {name} already exists").into());
        }
        // Iceberg stores no second or millisecond timestamps, and a Parquet
        // `timestamp(s)` column reads as Timestamp(ms), so `CREATE TABLE AS
        // SELECT * FROM read_parquet(...)` was refused with "Unsupported
        // Arrow data type: Timestamp(ms)". Create the empty table with those
        // columns widened to microseconds instead; the INSERT below casts the
        // rows onto the table schema, which loses nothing.
        let probe = self
            .plan_datafusion(&format!("SELECT * FROM ({query}) AS __sqe_ctas LIMIT 0"))
            .await?;
        match widen_coarse_timestamps(probe.schema().as_arrow()) {
            Some(widened) => {
                self.ctx.register_table(
                    name.to_string().as_str(),
                    Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
                        widened,
                    ))),
                )?;
            }
            None => {
                self.run_datafusion(&format!(
                    "CREATE TABLE {name} AS SELECT * FROM ({query}) AS __sqe_ctas LIMIT 0"
                ))
                .await?;
            }
        }
        // The rows go through the same writer as a plain INSERT. The error
        // is not `Send`, so only its text crosses the cleanup await.
        let insert_sql = format!("INSERT INTO {name} {query}");
        let inserted = match sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            &insert_sql,
        ) {
            Ok(stmts) if stmts.len() == 1 => self
                .embedded_handle_iceberg_insert(&stmts[0])
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            _ => self
                .run_datafusion(&insert_sql)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        };
        let failure = match inserted {
            Ok(()) => return Ok(Some(empty)),
            Err(e) => e,
        };
        let _ = self
            .run_datafusion(&format!("DROP TABLE IF EXISTS {name}"))
            .await;
        Err(failure.into())
    }

    /// The write handlers register their scratch tables under DataFusion's
    /// built-in `datafusion.public`, which the embedded context renames to
    /// `default.default`. Add it back, empty, on the first write.
    fn ensure_write_scratch_catalog(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.ctx.catalog("datafusion").is_none() {
            use datafusion::catalog::{
                CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider,
            };
            let scratch = MemoryCatalogProvider::new();
            scratch.register_schema("public", Arc::new(MemorySchemaProvider::new()))?;
            self.ctx.register_catalog("datafusion", Arc::new(scratch));
        }
        Ok(())
    }

    /// INSERT into a table in a `WritableIcebergCatalog`, through
    /// `sqe-write`'s streaming writer, as on a coordinator. The source is
    /// planned as a standalone query and the writer matches it to the table
    /// by position. DataFusion's own INSERT types a `VALUES` list against
    /// the table schema, whose list fields carry `PARQUET:field_id`
    /// metadata, and an `ARRAY[...]` literal then fails to build its batch.
    /// `Ok(None)`: not a writable Iceberg target, DataFusion answers.
    /// The config the write and DDL handlers run under: the embedded
    /// defaults with `query.max_query_memory` set to this engine's pool
    /// (`--memory-limit`), so a sort-on-write is admitted against the memory
    /// the engine has, not the server's 256 MB default.
    fn handler_config(&self) -> sqe_core::SqeConfig {
        use datafusion::execution::memory_pool::MemoryLimit;
        let mut config = sqe_core::SqeConfig::embedded_defaults();
        if let MemoryLimit::Finite(bytes) = self.ctx.runtime_env().memory_pool.memory_limit() {
            config.query.max_query_memory = bytes.to_string();
        }
        config
    }

    async fn embedded_handle_iceberg_insert(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use sqlparser::ast::{Statement, TableObject};

        let Statement::Insert(ins) = stmt else {
            return Ok(None);
        };
        let TableObject::TableName(name) = &ins.table else {
            return Ok(None);
        };
        let Some(source) = ins.source.as_ref() else {
            return Ok(None);
        };
        let Some((catalog_name, catalog)) = self.writable_target(name) else {
            return Ok(None);
        };
        self.ensure_write_scratch_catalog()?;

        let select_sql = sqe_sql::rewrite_named_tvf_args(&source.to_string());
        let session = self.write_session(catalog_name);
        let handler = sqe_write::write_handler::WriteHandler::new(self.handler_config());
        let mut plan_out = None;
        let mut policy_summary = None;
        let batches = handler
            .handle_insert_streaming_on(
                &session,
                stmt,
                catalog,
                &self.ctx,
                &select_sql,
                &mut plan_out,
                &mut policy_summary,
            )
            .await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        Ok(Some(QueryResult::from_batches(&schema, &batches)?))
    }

    /// DELETE / UPDATE / MERGE on a table in a `WritableIcebergCatalog`,
    /// through `sqe-write`'s handlers: the same copy-on-write and
    /// merge-on-read paths (`write.delete.mode` / `write.update.mode` /
    /// `write.merge.mode`) the server runs. Embedded mode has no policy
    /// enforcer, so no row filter or mask applies. `Ok(None)`: the target
    /// is not in a writable Iceberg catalog, DataFusion answers.
    async fn embedded_handle_iceberg_dml(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use sqe_write::write_handler::{
            delete_target_object_name, update_target_object_name, WriteHandler,
        };
        use sqlparser::ast::{Statement, TableFactor};

        let name = match stmt {
            Statement::Delete(_) => delete_target_object_name(stmt)?,
            Statement::Update(_) => update_target_object_name(stmt)?,
            Statement::Merge(m) => match &m.table {
                TableFactor::Table { name, .. } => name,
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let Some((catalog_name, catalog)) = self.writable_target(name) else {
            return Ok(None);
        };
        self.ensure_write_scratch_catalog()?;

        let session = self.write_session(catalog_name);
        let handler = WriteHandler::new(self.handler_config());
        let mut plan_out = None;
        let mut policy_summary = None;
        let batches = match stmt {
            Statement::Delete(_) => {
                handler
                    .dispatch_delete_on(
                        &session,
                        stmt,
                        catalog,
                        &self.ctx,
                        &mut plan_out,
                        &mut policy_summary,
                    )
                    .await?
            }
            Statement::Update(_) => {
                handler
                    .dispatch_update_on(
                        &session,
                        stmt,
                        catalog,
                        &self.ctx,
                        &mut plan_out,
                        &mut policy_summary,
                    )
                    .await?
            }
            Statement::Merge(m) => {
                let source_sql = sqe_write::merge_sql::merge_source_sql(&m.source)?;
                // Through the wrappers, so a `read_parquet(...)` source has its
                // provider built before planning.
                sqe_catalog::file_tvf_prepare::prepare_statement(&self.ctx, &source_sql).await?;
                let source =
                    sqe_catalog::file_tvf_prepare::plan_sql_without_policy(&self.ctx, &source_sql)
                        .await?;
                let source_plan = source.logical_plan().clone();
                let source_schema = Arc::new(source_plan.schema().as_arrow().clone());
                let source_batches = source.collect().await?;
                handler
                    .dispatch_merge_on(
                        &session,
                        stmt,
                        source_batches,
                        Some(source_schema),
                        catalog,
                        &self.ctx,
                        Some(source_plan),
                        &mut plan_out,
                        &mut policy_summary,
                    )
                    .await?
            }
            _ => return Ok(None),
        };
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        Ok(Some(QueryResult::from_batches(&schema, &batches)?))
    }

    /// `CREATE TABLE` with a column list in a `WritableIcebergCatalog`,
    /// through `sqe-write`'s handler: `PARTITIONED BY` transforms,
    /// `TBLPROPERTIES` / `WITH` options, nested types and the format version
    /// all as on a coordinator. DataFusion's own CREATE TABLE drops the
    /// partitioning and the properties. `Ok(None)`: not an Iceberg target,
    /// DataFusion answers.
    async fn embedded_handle_iceberg_create_table(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            return Ok(None);
        };
        if ct.temporary || ct.external || ct.query.is_some() {
            return Ok(None);
        }
        let Some((catalog_name, catalog)) = self.writable_target(&ct.name) else {
            return Ok(None);
        };
        let rest_catalog = self.rest_catalogs.contains_key(&catalog_name);
        let session = self.write_session(catalog_name.clone());
        let handler = sqe_write::write_handler::WriteHandler::new(self.handler_config());
        handler
            .handle_create_table_on(&session, stmt, catalog, rest_catalog)
            .await?;

        // The new table (and a namespace the handler created) is not in the
        // catalog's cached listing yet.
        let ident = sqe_write::ident::resolve_table_ident(&ct.name, &session)?;
        let namespace = ident.namespace().as_ref().join(".");
        if let Some(writable) = self.iceberg_catalogs.get(&catalog_name) {
            writable.refresh_namespace(&namespace).await?;
        }
        Ok(Some(QueryResult::empty()))
    }

    /// `CREATE [OR REPLACE] VIEW` / `DROP VIEW [IF EXISTS]` naming a
    /// `WritableIcebergCatalog`. On an `iceberg_rest` attach this is an
    /// Iceberg view, written through `CatalogOps` as on a coordinator: the
    /// body's analyzed schema, its normalized SQL, `OR REPLACE` as one
    /// `replaceView` commit. Any other Iceberg catalog (the SQLite
    /// warehouse) has no view operations, so CREATE is refused and DROP
    /// finds nothing. `Ok(None)`: not an Iceberg target, DataFusion's own
    /// session view applies.
    async fn embedded_handle_iceberg_view_ddl(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use sqlparser::ast::{ObjectType, Statement};

        let (name, create) = match stmt {
            Statement::CreateView(cv) => (&cv.name, Some(cv)),
            Statement::Drop {
                object_type: ObjectType::View,
                names,
                ..
            } => match names.as_slice() {
                [name] => (name, None),
                _ if names.iter().any(|n| self.iceberg_target(n).is_some()) => {
                    return Err("DROP VIEW names one Iceberg view at a time".into());
                }
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let Some(catalog_name) = self.iceberg_target(name) else {
            return Ok(None);
        };
        if let Some(cv) = create {
            if cv.materialized {
                return Err("materialized views are not supported".into());
            }
            if cv.temporary {
                return Err(format!(
                    "CREATE TEMPORARY VIEW {name}: a temporary view lives in the session; \
                     name it without the Iceberg catalog `{catalog_name}`"
                )
                .into());
            }
        }
        let views = match self.rest_catalogs.get(&catalog_name) {
            Some(views) => Arc::clone(views),
            None => {
                return match stmt {
                    Statement::Drop {
                        if_exists: true, ..
                    } => Ok(Some(QueryResult::empty())),
                    Statement::Drop { .. } => Err(format!(
                        "view {name} does not exist: catalog `{catalog_name}` stores no views"
                    )
                    .into()),
                    _ => Err(format!(
                        "CREATE VIEW {name}: catalog `{catalog_name}` stores no Iceberg views; \
                         create the view in an ATTACHed iceberg_rest catalog"
                    )
                    .into()),
                };
            }
        };

        let warehouse = views.warehouse().to_string();
        let session = self.write_session(catalog_name.clone());
        let ops = sqe_write::catalog_ops::CatalogOps::new(sqe_core::SqeConfig::embedded_defaults())
            .with_session_catalog(views);
        match create {
            Some(cv) => {
                let schema = self.view_body_schema(&cv.query.to_string()).await?;
                let schema_json = sqe_write::view_schema::arrow_schema_to_iceberg_json(&schema);
                // The body names this catalog by its attach name, which only
                // this session knows; store it under the warehouse name,
                // which a coordinator or another engine resolves.
                let mut stored = stmt.clone();
                if let Statement::CreateView(cv) = &mut stored {
                    sqe_sql::rename_catalog(&mut cv.query, &catalog_name, &warehouse);
                }
                ops.create_view(&session, &stored, &schema_json).await?;
            }
            None => ops.drop_view(&session, stmt).await?,
        }

        // SHOW TABLES serves the catalog's view listing; the statement
        // succeeded even if the re-list does not.
        let ident = sqe_write::ident::resolve_table_ident(name, &session)?;
        if let Some(writable) = self.iceberg_catalogs.get(&catalog_name) {
            let _ = writable
                .refresh_views(&ident.namespace().as_ref().join("."))
                .await;
        }
        Ok(Some(QueryResult::empty()))
    }

    /// The schema a view body produces, planned as a query would be: the
    /// analyzed (type-coerced) schema, not the planned one, as the
    /// coordinator's `get_schema` derives it.
    async fn view_body_schema(
        &self,
        body: &str,
    ) -> Result<arrow_schema::SchemaRef, Box<dyn std::error::Error>> {
        let sql = sqe_sql::rewrite_named_tvf_args(body);
        for provider in self.hive_roots.values() {
            provider.refresh_referenced(&sql, None, None).await;
        }
        sqe_catalog::file_tvf_prepare::prepare_statement(&self.ctx, &sql).await?;
        let df = sqe_catalog::file_tvf_prepare::plan_sql_without_policy(&self.ctx, &sql).await?;
        let schema = sqe_catalog::file_tvf_prepare::analyzed_schema(&self.ctx, df.logical_plan())?;
        Ok(Arc::new(schema.as_arrow().clone()))
    }

    /// `ALTER TABLE` on a writable Iceberg catalog: rename, column and nested
    /// column changes, table properties, branches and tags, partition
    /// evolution and identifier fields run on the cluster's `CatalogOps`.
    /// `None` for any other target, which falls through to DataFusion.
    /// `SET TAGS` and masking policies stay cluster-only: embedded mode has
    /// no policy engine to enforce them.
    async fn embedded_handle_iceberg_alter(
        &self,
        kind: &sqe_sql::StatementKind,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use sqe_sql::{PartitionEvolution, RefDdl, StatementKind as K};
        let table = match kind {
            K::Rename(stmt) | K::AlterSchema(stmt) | K::AlterTableProps(stmt) => {
                match stmt.as_ref() {
                    sqlparser::ast::Statement::AlterTable(at) => at.name.clone(),
                    _ => return Ok(None),
                }
            }
            K::DropNestedColumn { table, .. } | K::AlterNestedColumnType { table, .. } => {
                sqe_write::catalog_ops::parse_object_name(table)?
            }
            K::RefDdl(ddl) => match ddl.as_ref() {
                RefDdl::CreateBranch { table, .. }
                | RefDdl::CreateTag { table, .. }
                | RefDdl::DropBranch { table, .. }
                | RefDdl::DropTag { table, .. } => {
                    sqe_write::catalog_ops::parse_object_name(table)?
                }
            },
            K::PartitionEvolution(evolution) => match evolution.as_ref() {
                PartitionEvolution::AddField { table, .. }
                | PartitionEvolution::DropField { table, .. }
                | PartitionEvolution::ReplaceField { table, .. } => {
                    sqe_write::catalog_ops::parse_object_name(table)?
                }
            },
            K::IdentifierFields(ddl) => sqe_write::catalog_ops::parse_object_name(&ddl.table)?,
            _ => return Ok(None),
        };
        let Some((catalog_name, catalog)) = self.writable_target(&table) else {
            return Ok(None);
        };
        let session = self.write_session(catalog_name.clone());
        let ops = sqe_write::catalog_ops::CatalogOps::new(sqe_core::SqeConfig::embedded_defaults())
            .with_session_catalog(catalog);

        // Namespaces whose cached listing the statement changes.
        let source = sqe_write::ident::resolve_table_ident(&table, &session)?;
        let mut namespaces = vec![source.namespace().as_ref().join(".")];
        match kind {
            K::Rename(stmt) => {
                if let Some(dest) = rename_destination(stmt) {
                    // The rename runs inside the source catalog; a target
                    // in another catalog would land there silently.
                    if let [dest_catalog, _, _] = dest.0.as_slice() {
                        let dest_catalog = dest_catalog.as_ident().map(|i| i.value.as_str());
                        if dest_catalog != Some(catalog_name.as_str()) {
                            return Err(format!(
                                "ALTER TABLE ... RENAME cannot move a table to another \
                                 catalog: {table} -> {dest}"
                            )
                            .into());
                        }
                    }
                    if dest.0.len() > 1 {
                        let dest = sqe_write::ident::resolve_table_ident(dest, &session)?;
                        namespaces.push(dest.namespace().as_ref().join("."));
                    }
                }
                ops.rename_table(&session, stmt).await?;
            }
            K::AlterSchema(stmt) => ops.alter_table_schema(&session, stmt).await?,
            K::AlterTableProps(stmt) => ops.set_table_properties(&session, stmt).await?,
            K::DropNestedColumn {
                table,
                path,
                if_exists,
            } => {
                ops.drop_nested_column(&session, table, path, *if_exists)
                    .await?
            }
            K::AlterNestedColumnType {
                table,
                path,
                data_type,
            } => {
                ops.alter_nested_column_type(&session, table, path, data_type)
                    .await?
            }
            K::RefDdl(ddl) => ops.apply_ref_ddl(&session, ddl).await?,
            K::PartitionEvolution(evolution) => {
                ops.apply_partition_evolution(&session, evolution).await?
            }
            K::IdentifierFields(ddl) => ops.apply_identifier_fields(&session, ddl).await?,
            _ => return Ok(None),
        }

        // The table providers hold the metadata they loaded; reload them.
        if let Some(writable) = self.iceberg_catalogs.get(&catalog_name) {
            namespaces.dedup();
            for namespace in &namespaces {
                writable.refresh_namespace(namespace).await?;
            }
        }
        Ok(Some(QueryResult::empty()))
    }

    /// The catalog a one- to three-part table name lands in, when that is a
    /// writable Iceberg catalog: its name and a `SessionCatalog` over it.
    fn writable_target(
        &self,
        name: &sqlparser::ast::ObjectName,
    ) -> Option<(String, Arc<sqe_catalog::SessionCatalog>)> {
        let catalog_name = self.iceberg_target(name)?;
        let writable = self.iceberg_catalogs.get(&catalog_name)?;
        let catalog = Arc::new(sqe_catalog::SessionCatalog::local(
            writable.client(),
            &catalog_name,
            &self.storage,
        ));
        Some((catalog_name, catalog))
    }

    /// Refuse `DROP TABLE` / `DROP VIEW` / `DROP SCHEMA` on a read-only
    /// backend catalog. Its schemas refuse `deregister_table`, but DataFusion
    /// reports any error there as "Table '...' doesn't exist", which is
    /// false, and `DynamicFileCatalog` answers `DROP SCHEMA` with "not
    /// implemented" before the catalog sees it.
    fn refuse_drop_on_read_only(
        &self,
        stmt: &datafusion::sql::sqlparser::ast::Statement,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use datafusion::sql::sqlparser::ast::{ObjectType, Statement};
        let Statement::Drop {
            object_type, names, ..
        } = stmt
        else {
            return Ok(());
        };
        let catalog_parts = match object_type {
            ObjectType::Table | ObjectType::View => 3,
            ObjectType::Schema => 2,
            _ => return Ok(()),
        };
        for name in names {
            let catalog = match name.0.as_slice() {
                [catalog, ..] if name.0.len() == catalog_parts => match catalog.as_ident() {
                    Some(ident) => ident.value.clone(),
                    None => continue,
                },
                _ => self.default_catalog(),
            };
            if self.read_only_catalog.as_deref() == Some(catalog.as_str()) {
                return Err(format!(
                    "catalog `{catalog}` is attached read-only; use the server for writes"
                )
                .into());
            }
        }
        Ok(())
    }

    /// The catalog a one- to three-part name lands in, when that is one of
    /// `iceberg_catalogs`.
    fn iceberg_target(&self, name: &sqlparser::ast::ObjectName) -> Option<String> {
        let parts: Vec<&str> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.as_str()))
            .collect();
        let catalog_name = match parts.as_slice() {
            [catalog, _, _] => catalog.to_string(),
            [_] | [_, _] => self.default_catalog(),
            _ => return None,
        };
        self.iceberg_catalogs
            .contains_key(&catalog_name)
            .then_some(catalog_name)
    }

    /// Run `stripped`, a statement whose time-travel clauses were removed,
    /// with each `specs` table swapped for a provider pinned to the snapshot
    /// its clause names. The pinned providers live in the memory catalog for
    /// this one statement and are deregistered on success and on error, so a
    /// later plain read sees the current table again.
    async fn embedded_time_travel(
        &mut self,
        stripped: &str,
        specs: &[sqe_sql::TimeTravelSpec],
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let mut sql = stripped.to_string();
        let mut pinned = Vec::with_capacity(specs.len());
        for spec in specs {
            match self.pin_time_travel_table(spec).await {
                Ok(alias) => {
                    let qualified = format!("\"{TT_CATALOG}\".\"{TT_SCHEMA}\".{alias}");
                    sql = sqe_sql::replace_table_reference(&sql, &spec.table, &qualified);
                    pinned.push(alias);
                }
                Err(e) => {
                    self.unpin_time_travel_tables(pinned);
                    return Err(e);
                }
            }
        }
        // The stripped statement goes back through `execute`, so an
        // `INSERT ... SELECT ... FOR VERSION AS OF` still reaches the
        // Iceberg write path.
        let result = Box::pin(self.execute(&sql)).await;
        self.unpin_time_travel_tables(pinned);
        result
    }

    fn unpin_time_travel_tables(&self, aliases: Vec<String>) {
        for alias in aliases {
            let _ = self
                .ctx
                .deregister_table(datafusion::common::TableReference::full(
                    TT_CATALOG, TT_SCHEMA, alias,
                ));
        }
    }

    /// Load the Iceberg table `spec` names, resolve its clause to a snapshot
    /// and register a provider pinned to that snapshot. Returns the alias it
    /// is registered under in `TT_CATALOG.TT_SCHEMA`.
    async fn pin_time_travel_table(
        &self,
        spec: &sqe_sql::TimeTravelSpec,
    ) -> Result<String, Box<dyn std::error::Error>> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        static NEXT_ALIAS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let name = Parser::new(&GenericDialect {})
            .try_with_sql(&spec.table)?
            .parse_object_name(false)?;
        let parts = object_name_parts(&name);
        let (catalog, schema, table) = match parts.as_slice() {
            [catalog, schema, table] => (catalog.clone(), schema.clone(), table.clone()),
            [schema, table] => (self.default_catalog(), schema.clone(), table.clone()),
            [table] => {
                let schema = self
                    .ctx
                    .state()
                    .config()
                    .options()
                    .catalog
                    .default_schema
                    .clone();
                (self.default_catalog(), schema, table.clone())
            }
            _ => return Err(format!("time travel: cannot resolve table {}", spec.table).into()),
        };
        let Some(writable) = self.iceberg_catalogs.get(&catalog) else {
            return Err(format!(
                "time travel reads Iceberg tables only; {} is not in an Iceberg catalog",
                spec.table
            )
            .into());
        };
        let ident = iceberg::TableIdent::new(iceberg::NamespaceIdent::new(schema), table.clone());
        let iceberg_table = iceberg::Catalog::load_table(writable.client().as_ref(), &ident)
            .await
            .map_err(|e| format!("time travel: cannot load {}: {e}", spec.table))?;
        let snapshot_id = sqe_catalog::time_travel::resolve_version_snapshot(
            iceberg_table.metadata(),
            &spec.version,
            &spec.table,
        )?;
        let provider = datafusion_iceberg::IcebergStaticTableProvider::try_new_from_table_snapshot(
            iceberg_table,
            snapshot_id,
        )
        .await?;
        let n = NEXT_ALIAS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let alias = format!("__sqe_ver_{n}");
        self.ctx.register_table(
            datafusion::common::TableReference::full(TT_CATALOG, TT_SCHEMA, alias.as_str()),
            Arc::new(provider),
        )?;
        Ok(alias)
    }

    /// The session the write handlers run as: the Unix user, no token, the
    /// target catalog and the session's default schema.
    fn write_session(&self, catalog_name: String) -> sqe_core::Session {
        let options = self.ctx.state().config().options().clone();
        let mut session = sqe_core::Session::new(
            std::env::var("USER").unwrap_or_else(|_| "sqe-embedded".to_string()),
            sqe_core::SecretString::new(String::new()),
            None,
            chrono::DateTime::<chrono::Utc>::MAX_UTC,
            Vec::new(),
        );
        session.default_catalog = Some(catalog_name);
        session.default_schema = Some(options.catalog.default_schema.clone());
        session
    }

    /// `SHOW TABLES {IN|FROM} [<catalog>.]<schema> [LIKE '<pattern>']`.
    /// DataFusion only answers the unscoped form ("SHOW TABLES IN not
    /// supported"), so the scoped form is answered from
    /// `information_schema.tables` in the same four columns. `Ok(None)`: no
    /// scope, DataFusion's own `SHOW TABLES` applies.
    async fn embedded_handle_show_tables_in(
        &self,
        sql: &str,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        use sqlparser::ast::{ShowStatementFilter, ShowStatementFilterPosition, Statement};

        let Ok(mut stmts) =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, sql)
        else {
            return Ok(None);
        };
        let Some(Statement::ShowTables { show_options, .. }) = stmts.pop() else {
            return Ok(None);
        };
        let Some(parent) = show_options.show_in.and_then(|si| si.parent_name) else {
            return Ok(None);
        };
        let parts: Vec<String> = parent
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
            .collect();
        let (catalog, schema) = match parts.as_slice() {
            [schema] => (self.default_catalog(), schema.clone()),
            [catalog, schema] => (catalog.clone(), schema.clone()),
            _ => return Err(format!("SHOW TABLES IN takes [catalog.]schema, got {parent}").into()),
        };
        if self.ctx.catalog(&catalog).is_none() {
            return Err(format!("catalog `{catalog}` not found").into());
        }
        let literal = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let mut query = format!(
            "SELECT table_catalog, table_schema, table_name, table_type \
             FROM information_schema.tables WHERE table_catalog = {} AND table_schema = {}",
            literal(&catalog),
            literal(&schema)
        );
        let filter = match show_options.filter_position {
            Some(
                ShowStatementFilterPosition::Infix(f) | ShowStatementFilterPosition::Suffix(f),
            ) => Some(f),
            None => None,
        };
        match filter {
            Some(ShowStatementFilter::Like(p)) => {
                query.push_str(&format!(" AND table_name LIKE {}", literal(&p)));
            }
            Some(ShowStatementFilter::ILike(p)) => {
                query.push_str(&format!(" AND table_name ILIKE {}", literal(&p)));
            }
            Some(other) => return Err(format!("SHOW TABLES: unsupported filter {other}").into()),
            None => {}
        }
        query.push_str(" ORDER BY table_name");
        self.run_datafusion(&query).await.map(Some)
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

        match writable_store(&provider, catalog)?
            .delete(database, table)
            .await
        {
            Ok(()) => {}
            Err(sqe_catalog::hive::HiveError::NotFound(_)) if *if_exists => {}
            Err(e) => return Err(e.into()),
        }
        provider.invalidate_table_names(database);

        Ok(Some(QueryResult::empty()))
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

        let manifest = writable_store(&provider, catalog)?
            .get(database, table)
            .await?;
        let spec = sqe_catalog::hive::hive_spec_from_manifest(catalog, &manifest);
        let ddl = sqe_catalog::hive::render_hive_create_table_ddl(&spec);

        Ok(Some(QueryResult::text(
            vec!["Create Table".to_string()],
            vec![vec![ddl]],
        )))
    }

    /// `SHOW CREATE TABLE` for a table in an Iceberg catalog, rendered as
    /// the coordinator renders it: the columns from the table's schema, then
    /// its `PARTITIONED BY`, sort order and `TBLPROPERTIES` from the Iceberg
    /// metadata. `Ok(None)` when the name is not in an Iceberg catalog.
    async fn embedded_handle_show_create_iceberg_table(
        &mut self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<Option<QueryResult>, Box<dyn std::error::Error>> {
        let sqlparser::ast::Statement::ShowCreate { obj_type, obj_name } = stmt else {
            return Ok(None);
        };
        let want_view = match obj_type {
            sqlparser::ast::ShowCreateObject::View => true,
            sqlparser::ast::ShowCreateObject::Table => false,
            _ => return Ok(None),
        };
        let parts = object_name_parts(obj_name);
        let (catalog, schema, table) = match parts.as_slice() {
            [c, s, t] => (c.clone(), s.clone(), t.clone()),
            [s, t] => (self.default_catalog(), s.clone(), t.clone()),
            [t] => {
                let options = self.ctx.state().config().options().clone();
                (
                    options.catalog.default_catalog.clone(),
                    options.catalog.default_schema.clone(),
                    t.clone(),
                )
            }
            _ => return Ok(None),
        };
        let Some(writable) = self.iceberg_catalogs.get(&catalog).cloned() else {
            return Ok(None);
        };

        let provider = match self.ctx.catalog(&catalog).and_then(|c| c.schema(&schema)) {
            Some(s) => s.table(&table).await?,
            None => None,
        };
        let Some(provider) = provider else {
            let kind = if want_view { "view" } else { "table" };
            return Err(format!("{kind} not found: {}", parts.join(".")).into());
        };
        let is_view = provider.table_type() == datafusion::datasource::TableType::View;
        match (want_view, is_view) {
            (true, true) => {
                return self
                    .embedded_show_create_iceberg_view(&catalog, &schema, &table, &parts)
                    .await
                    .map(Some);
            }
            (false, true) => {
                return Err(
                    format!("Relation '{}' is a view, not a table", parts.join(".")).into(),
                );
            }
            (true, false) => {
                return Err(
                    format!("Relation '{}' is a table, not a view", parts.join(".")).into(),
                );
            }
            (false, false) => {}
        }
        let columns: Vec<sqe_catalog::show_create::DdlColumn> = provider
            .schema()
            .fields()
            .iter()
            .map(|field| sqe_catalog::show_create::DdlColumn {
                name: field.name().clone(),
                // A geospatial column's type lives in its field metadata;
                // its storage type would print as `varbinary`.
                ddl_type: sqe_geo::ddl_type(field).unwrap_or_else(|| {
                    sqe_trino_protocol::encode::arrow_to_ddl_trino_type(field.data_type())
                }),
                nullable: field.is_nullable(),
            })
            .collect();

        let ident = iceberg::TableIdent::new(iceberg::NamespaceIdent::new(schema), table);
        let metadata_table =
            iceberg::Catalog::load_table(writable.client().as_ref(), &ident).await?;
        let ddl = sqe_catalog::show_create::render_create_table(
            &parts,
            &columns,
            Some(metadata_table.metadata()),
        );

        Ok(Some(QueryResult::text(
            vec!["Create Table".to_string()],
            vec![vec![ddl]],
        )))
    }

    /// `SHOW CREATE VIEW` on an Iceberg view: its stored SQL, with the
    /// warehouse name it is stored under (see
    /// [`Self::embedded_handle_iceberg_view_ddl`]) shown as this session's
    /// name for the catalog again.
    async fn embedded_show_create_iceberg_view(
        &self,
        catalog: &str,
        schema: &str,
        view: &str,
        parts: &[String],
    ) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let Some(views) = self.rest_catalogs.get(catalog) else {
            return Err(format!("view not found: {}", parts.join(".")).into());
        };
        let ns = iceberg::NamespaceIdent::new(schema.to_string());
        let Some(body) = views.load_view_sql(&ns, view).await? else {
            return Err(format!("view not found: {}", parts.join(".")).into());
        };
        // A body another engine wrote may not parse here; show it as stored.
        let dialect = sqlparser::dialect::GenericDialect {};
        let body = match sqlparser::parser::Parser::parse_sql(&dialect, &body).as_deref_mut() {
            Ok([query]) => {
                sqe_sql::rename_catalog(query, views.warehouse(), catalog);
                query.to_string()
            }
            _ => body,
        };
        Ok(QueryResult::text(
            vec!["Create View".to_string()],
            vec![vec![format!("CREATE VIEW {} AS\n{body}", parts.join("."))]],
        ))
    }
}

/// `schema` with its second and millisecond timestamp columns widened to
/// microseconds, the coarsest precision Iceberg stores, or `None` when it has
/// none. Nanosecond columns stay: the Iceberg schema provider upgrades the
/// table to v3 for them.
// ponytail: top-level columns only, like the coordinator's
// `timestamps_for_format_version`.
fn widen_coarse_timestamps(schema: &arrow_schema::Schema) -> Option<arrow_schema::Schema> {
    use arrow_schema::{DataType, TimeUnit};
    let coarse = |f: &arrow_schema::Field| {
        matches!(
            f.data_type(),
            DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, _)
        )
    };
    if !schema.fields().iter().any(|f| coarse(f)) {
        return None;
    }
    let fields: Vec<arrow_schema::Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(_, tz) if coarse(f) => f
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            _ => f.as_ref().clone(),
        })
        .collect();
    Some(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
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
/// The new name in `ALTER TABLE ... RENAME TO <name>`.
fn rename_destination(stmt: &sqlparser::ast::Statement) -> Option<&sqlparser::ast::ObjectName> {
    let sqlparser::ast::Statement::AlterTable(at) = stmt else {
        return None;
    };
    at.operations.iter().find_map(|op| match op {
        sqlparser::ast::AlterTableOperation::RenameTable { table_name } => match table_name {
            sqlparser::ast::RenameTableNameKind::As(name)
            | sqlparser::ast::RenameTableNameKind::To(name) => Some(name),
        },
        _ => None,
    })
}

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

/// The writable sidecar store behind a Hive root, or an error naming the
/// catalog. A root reading a metastore SQE does not own (Glue, HMS) has no
/// manifest to write, and DDL against it must say so rather than appear to
/// succeed.
fn writable_store(
    provider: &sqe_catalog::hive::provider::HiveCatalogProvider,
    catalog: &str,
) -> anyhow::Result<std::sync::Arc<sqe_catalog::hive::store::HiveManifestStore>> {
    provider.store().ok_or_else(|| {
        anyhow::anyhow!(
            "catalog `{catalog}` reads an external metastore, so SQE cannot write its table \
             definitions; change the table where the metastore owns it"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rest_attach(options: &[(&str, &str)]) -> sqe_sql::AttachStatement {
        sqe_sql::AttachStatement {
            name: "polaris".to_string(),
            location: "http://localhost:8181/api/catalog".to_string(),
            kind: sqe_sql::CatalogKind::IcebergRest,
            options: options
                .iter()
                .map(|(k, v)| (k.to_string(), sqe_sql::OptionValue::String(v.to_string())))
                .collect(),
        }
    }

    fn minio_storage() -> StorageConfig {
        StorageConfig {
            s3_endpoint: "http://127.0.0.1:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_access_key: "ak".to_string(),
            s3_secret_key: sqe_core::SecretString::new("sk".to_string()),
            s3_path_style: true,
            ..Default::default()
        }
    }

    fn opt<'a>(stmt: &'a sqe_sql::AttachStatement, key: &str) -> Option<&'a str> {
        stmt.options.get(key).and_then(sqe_sql::OptionValue::as_str)
    }

    #[test]
    fn rest_attach_without_s3_options_reads_the_engine_store() {
        let stmt = rest_attach(&[("WAREHOUSE", "bench")]);
        let filled = with_storage_fallback(&stmt, &minio_storage());
        assert_eq!(opt(&filled, "S3_ENDPOINT"), Some("http://127.0.0.1:9000"));
        assert_eq!(opt(&filled, "S3_REGION"), Some("us-east-1"));
        assert_eq!(opt(&filled, "S3_ACCESS_KEY"), Some("ak"));
        assert_eq!(opt(&filled, "S3_SECRET_KEY"), Some("sk"));
        assert_eq!(opt(&filled, "S3_PATH_STYLE"), Some("true"));
        // Unset settings stay absent rather than becoming empty props.
        assert_eq!(opt(&filled, "S3_SESSION_TOKEN"), None);
        assert_eq!(opt(&filled, "WAREHOUSE"), Some("bench"));
    }

    #[test]
    fn rest_attach_with_any_s3_option_is_left_alone() {
        let stmt = rest_attach(&[
            ("WAREHOUSE", "bench"),
            ("S3_ENDPOINT", "https://s3.eu-west-1.amazonaws.com"),
        ]);
        let filled = with_storage_fallback(&stmt, &minio_storage());
        assert!(matches!(filled, std::borrow::Cow::Borrowed(_)));
        assert_eq!(opt(&filled, "S3_ACCESS_KEY"), None);
        assert_eq!(opt(&filled, "S3_REGION"), None);
    }

    #[test]
    fn unconfigured_store_and_other_kinds_are_left_alone() {
        let stmt = rest_attach(&[("WAREHOUSE", "bench")]);
        let filled = with_storage_fallback(&stmt, &StorageConfig::default());
        assert!(matches!(filled, std::borrow::Cow::Borrowed(_)));

        let mut glue = rest_attach(&[]);
        glue.kind = sqe_sql::CatalogKind::Glue;
        let filled = with_storage_fallback(&glue, &minio_storage());
        assert!(matches!(filled, std::borrow::Cow::Borrowed(_)));
    }

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

    /// The embedded pool is the coordinator's: a busy aggregate is not capped
    /// at a share by idle spillable siblings or by the scan's unspillable
    /// bytes, stops at the headroom line, and the scan keeps the headroom.
    #[test]
    fn embedded_pool_is_greedy_with_unspillable_headroom() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        const MIB: usize = 1024 * 1024;
        let pool = embedded_memory_pool(100 * MIB);
        let _idle: Vec<_> = (0..8)
            .map(|i| {
                MemoryConsumer::new(format!("idle-agg-{i}"))
                    .with_can_spill(true)
                    .register(&pool)
            })
            .collect();
        let scan = MemoryConsumer::new("iceberg-scan-decode:t").register(&pool);
        scan.try_grow(15 * MIB).expect("the scan admits");
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        // A FairSpillPool would cap this at (100 - 15) / 9 MiB.
        agg.try_grow(75 * MIB)
            .expect("one aggregate may take the spillable bytes");
        let err = agg.try_grow(6 * MIB).expect_err("spillable stops at 80%");
        assert!(
            matches!(
                err,
                datafusion::error::DataFusionError::ResourcesExhausted(_)
            ),
            "the refusal is the one operators spill on: {err}"
        );
        scan.try_grow(10 * MIB)
            .expect("the scan still has the unspillable headroom");
    }

    /// The coordinator's per-query sort share: each partition's sort buffer
    /// stops at its share of the spillable bytes, so the last partition to
    /// start still finds room instead of failing the query.
    #[test]
    fn embedded_pool_keeps_a_sort_share_for_every_partition() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        const MIB: usize = 1024 * 1024;
        let pool = embedded_memory_pool(100 * MIB);
        let sorters: Vec<_> = (0..4)
            .map(|i| {
                MemoryConsumer::new(format!("ExternalSorter[{i}]"))
                    .with_can_spill(true)
                    .register(&pool)
            })
            .collect();
        // 80 MiB spillable over 4 sorts.
        for sorter in &sorters[..3] {
            sorter.try_grow(MIB).expect("first reservation");
            let err = sorter
                .try_grow(60 * MIB)
                .expect_err("a sort stops at its 20 MiB share");
            let refusal = err.to_string();
            assert!(
                refusal.contains("sqe-cli --memory-limit sort share"),
                "the refusal names the embedded limit: {refusal}"
            );
            sorter.try_grow(19 * MIB).expect("up to the share");
        }
        sorters[3]
            .try_grow(20 * MIB)
            .expect("the last partition still gets its share");
        let agg = MemoryConsumer::new("agg")
            .with_can_spill(true)
            .register(&pool);
        agg.try_grow(1)
            .expect_err("the shares filled the spillable bytes");
    }

    /// Writes plan their sort-on-write gate against `--memory-limit`, not
    /// the server's 256 MB `query.max_query_memory` default.
    #[tokio::test]
    async fn handler_config_takes_the_memory_limit() {
        let client = EmbeddedClient::new(3 * 1024 * 1024 * 1024).unwrap();
        let config = client.handler_config();
        assert_eq!(
            sqe_core::parse_memory_limit(&config.query.max_query_memory).unwrap(),
            3 * 1024 * 1024 * 1024
        );
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
    fn a_session_token_reaches_the_storage_config() {
        // Temporary credentials (SSO, assumed role) are useless without the
        // token: S3 rejects the request as `InvalidAccessKeyId`, which reads
        // like a wrong key rather than a missing token. There is no flag for
        // it on purpose, so the environment is the only path in.
        let resolved = EmbeddedS3Options::resolve(
            &EmbeddedS3Options::default(),
            env_of(&[
                ("AWS_ACCESS_KEY_ID", "ASIAEXAMPLE"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
                ("AWS_SESSION_TOKEN", "the-token"),
            ]),
        );
        assert_eq!(resolved.session_token.as_deref(), Some("the-token"));
        assert_eq!(
            resolved.to_storage_config().s3_session_token.expose(),
            "the-token"
        );
    }

    #[test]
    fn the_sqe_session_token_spelling_wins_over_the_aws_one() {
        let resolved = EmbeddedS3Options::resolve(
            &EmbeddedS3Options::default(),
            env_of(&[
                ("SQE_STORAGE__S3_SESSION_TOKEN", "sqe-token"),
                ("AWS_SESSION_TOKEN", "aws-token"),
            ]),
        );
        assert_eq!(resolved.session_token.as_deref(), Some("sqe-token"));
    }

    #[test]
    fn no_session_token_leaves_the_storage_config_empty() {
        // The provider chain supplies its own token when the engine has no
        // static key pair. Defaulting to a non-empty value here would sign
        // anonymous reads with a token that does not match the keys.
        let resolved = EmbeddedS3Options::resolve(&EmbeddedS3Options::default(), env_of(&[]));
        assert_eq!(resolved.session_token, None);
        assert!(resolved
            .to_storage_config()
            .s3_session_token
            .expose()
            .is_empty());
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
        assert_eq!(
            resolved.endpoint.as_deref(),
            Some("http://s3-specific:9000")
        );
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
        // The SQL front door names anonymous columns `_colN` on every wire,
        // embedded included (finding 700); DataFusion's `Int64(1)` is gone.
        assert_eq!(result.columns, vec!["_col0".to_string()]);
        assert!(result.rows.is_empty());
    }

    /// A NULL cell is marked, so `-f json` prints `null` and an empty string
    /// stays `""`. Both used to render as `""`.
    #[tokio::test]
    async fn embedded_client_marks_null_cells() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let sql = "SELECT CAST(NULL AS DECIMAL(7,2)) AS n, '' AS e, 1 AS one \
                   UNION ALL SELECT 2.50, NULL, 2 ORDER BY one";
        let result = client.execute(sql).await.expect("query");
        assert_eq!(result.rows.len(), 2);
        let marked: Vec<Vec<bool>> = (0..2)
            .map(|r| (0..3).map(|c| result.is_null(r, c)).collect())
            .collect();
        assert_eq!(
            marked,
            vec![vec![true, false, false], vec![false, true, false]]
        );
        assert_eq!(result.rows[0][1], "");
    }

    /// Finding 700: Trino shapes that only the Trino HTTP door used to accept
    /// work in `--embedded` too, through the same front door.
    #[tokio::test]
    async fn embedded_client_accepts_trino_shapes_through_the_front_door() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client
            .execute("CREATE TABLE fd_t (id BIGINT)")
            .await
            .expect("create");
        client
            .execute("INSERT INTO fd_t VALUES 1")
            .await
            .expect("paren-less VALUES insert");
        let result = client.execute("TABLE fd_t").await.expect("bare TABLE");
        assert_eq!(result.columns, vec!["id".to_string()]);
        assert_eq!(result.rows, vec![vec!["1".to_string()]]);
        let result = client.execute("SELECT 1+1").await.expect("query");
        assert_eq!(result.columns, vec!["_col0".to_string()]);
        assert_eq!(result.rows, vec![vec!["2".to_string()]]);
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
        let result =
            EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default())
                .await;
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
        use iceberg::{NamespaceIdent, TableCreation};

        let tmp_a = tempfile::tempdir().expect("tempdir a");
        let tmp_b = tempfile::tempdir().expect("tempdir b");

        // Bootstrap a namespace + table in each via the iceberg API.
        for (name, dir) in &[("a", tmp_a.path()), ("b", tmp_b.path())] {
            std::fs::create_dir_all(dir.join("iceberg")).expect("data dir");
            let abs = dir.canonicalize().expect("canonicalize");
            // Bootstrap through the same opener production uses, so the
            // rows land in the same `catalog_name` scope the attach path
            // reads and the same engine (sqlx or sqlite-rs) writes them.
            let cat = open_embedded_sqlite_catalog(
                &abs.join("sqe.db"),
                &format!("file://{}", abs.join("iceberg").display()),
            )
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

        let mut c =
            EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default())
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
                EmbeddedClient::with_warehouse(
                    64 * 1024 * 1024,
                    &mode,
                    &EmbeddedS3Options::default(),
                ),
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
        // Metadata pseudo-tables ($snapshots, $manifests) are not
        // listed (`WritableIcebergCatalog` drops them); only the main
        // table has to be visible here.
        assert!(
            r.rows
                .iter()
                .any(|row| row == &vec!["test_ns".to_string(), "greetings".to_string()]),
            "fresh client should see the pre-existing greetings table; got rows: {:?}",
            r.rows,
        );
        assert!(
            r.rows.iter().all(|row| !row[1].contains('$')),
            "metadata tables must not be listed; got rows: {:?}",
            r.rows,
        );
    }

    /// Memory mode never touches disk: the warehouse path is unused.
    /// We pass an obviously-invalid path and assert the build still
    /// succeeds because the SQLite branch is bypassed.
    #[tokio::test]
    async fn memory_mode_skips_warehouse_setup() {
        let mode = WarehouseMode::Memory;
        let mut c =
            EmbeddedClient::with_warehouse(64 * 1024 * 1024, &mode, &EmbeddedS3Options::default())
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

    /// #849: a JSON column on a format v3 table is a variant. The JSON
    /// functions read it as JSON text in embedded mode too, as they do on
    /// the server.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_functions_read_a_variant_column() {
        let text: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(vec![Some(
            r#"{"a": {"b": 2}, "s": "x"}"#,
        )]));
        let variant =
            sqe_core::variant::from_arrow(text.as_ref(), &sqe_core::variant::storage_type())
                .expect("variant");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("doc", variant.data_type().clone(), true),
        ]));
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema, vec![variant]).expect("batch");

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client.ctx.register_batch("docs", batch).expect("register");
        let result = client
            .execute(
                "SELECT json_get_int(doc, 'a', 'b') AS b, json_extract_scalar(doc, '$.s') AS s, \
                 json_extract(doc, '$.a') AS a FROM docs",
            )
            .await
            .expect("query");
        assert_eq!(
            result.rows,
            vec![vec![
                "2".to_string(),
                "x".to_string(),
                r#"{"b":2}"#.to_string()
            ]]
        );
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
    // Iceberg views on an ATTACHed iceberg_rest catalog
    // ------------------------------------------------------------------

    /// A stored view `ns.v` with body `body`, as the REST catalog's
    /// LoadViewResponse returns it.
    fn load_view_response(body: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata-location": "s3://bucket/ns/v/metadata/00001.json",
            "metadata": {
                "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
                "format-version": 1,
                "location": "s3://bucket/ns/v",
                "current-version-id": 1,
                "properties": {},
                "versions": [{
                    "version-id": 1,
                    "timestamp-ms": 1_573_518_431_292_i64,
                    "schema-id": 0,
                    "summary": {"engine-name": "sqe"},
                    "default-namespace": ["ns"],
                    "default-catalog": "wh",
                    "representations": [{"type": "sql", "sql": body, "dialect": "sqe"}]
                }],
                "version-log": [{"version-id": 1, "timestamp-ms": 1_573_518_431_292_i64}],
                "schemas": [{
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]
                }]
            },
            "config": {}
        })
    }

    /// CREATE VIEW, a read through the view, CREATE OR REPLACE VIEW and
    /// DROP VIEW against a mock REST catalog attached as `pol` over
    /// warehouse `wh`, so the view's `default-catalog` differs from the
    /// name the body is read under.
    #[tokio::test]
    async fn iceberg_rest_view_lifecycle() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "overrides": {}, "defaults": {}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"namespaces": [["ns"]]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"identifiers": []})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/views"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(load_view_response("SELECT 1 AS x")),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/views/v"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(load_view_response("SELECT 1 AS x")),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/views/v"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(load_view_response("SELECT 2 AS x")),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/ns/views/v"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        // `v2` is stored the way this session writes a body over `pol.ns.v`:
        // under the warehouse name.
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/views/v2"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(load_view_response("SELECT x FROM wh.ns.v")),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/views/selfie"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(load_view_response("SELECT * FROM wh.ns.selfie")),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/views"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [
                    {"namespace": ["ns"], "name": "v"},
                    {"namespace": ["ns"], "name": "v2"}
                ]
            })))
            .mount(&server)
            .await;

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client
            .execute(&format!(
                "ATTACH '{}' AS pol (TYPE iceberg_rest, WAREHOUSE 'wh', TOKEN 't0k')",
                server.uri()
            ))
            .await
            .expect("attach");

        let tables = client
            .execute("SHOW TABLES IN pol.ns")
            .await
            .expect("show tables");
        let mut listed: Vec<(String, String)> = tables
            .rows
            .iter()
            .map(|r| (r[2].clone(), r[3].clone()))
            .collect();
        listed.sort();
        assert_eq!(
            listed,
            vec![
                ("v".to_string(), "VIEW".to_string()),
                ("v2".to_string(), "VIEW".to_string())
            ],
            "the attach lists the namespace's views"
        );

        let rows = client
            .execute("SELECT x FROM pol.ns.v2")
            .await
            .expect("a view over a view, named by the warehouse");
        assert_eq!(rows.rows, vec![vec!["1".to_string()]]);
        let ddl = client
            .execute("SHOW CREATE VIEW pol.ns.v2")
            .await
            .expect("show create view");
        assert_eq!(ddl.columns, vec!["Create View".to_string()]);
        assert_eq!(
            ddl.rows,
            vec![vec![
                "CREATE VIEW pol.ns.v2 AS\nSELECT x FROM pol.ns.v".to_string()
            ]],
            "the stored warehouse name shows as the attach name"
        );
        let err = client
            .execute("SHOW CREATE TABLE pol.ns.v2")
            .await
            .expect_err("SHOW CREATE TABLE on a view");
        assert!(
            err.to_string()
                .contains("Relation 'pol.ns.v2' is a view, not a table"),
            "{err}"
        );
        let err = client
            .execute("SELECT * FROM pol.ns.selfie")
            .await
            .expect_err("a view over itself");
        assert!(
            err.to_string().contains(
                "view wh.ns.selfie references itself through wh.ns.selfie -> wh.ns.selfie"
            ),
            "{err}"
        );

        client
            .execute("CREATE VIEW pol.ns.v3 AS SELECT x FROM pol.ns.v")
            .await
            .expect("create a view over a view");
        let requests = server.received_requests().await.expect("recording on");
        let stored = requests
            .iter()
            .filter(|r| r.method.as_str() == "POST" && r.url.path() == "/v1/namespaces/ns/views")
            .map(|r| r.body_json::<serde_json::Value>().expect("json body"))
            .find(|b| b["name"] == "v3")
            .expect("CREATE VIEW v3 posted");
        assert_eq!(
            stored["view-version"]["representations"][0]["sql"], "SELECT x FROM wh.ns.v",
            "the body is stored under the warehouse name"
        );

        client
            .execute("CREATE VIEW pol.ns.v AS SELECT CAST(1 AS BIGINT) AS x")
            .await
            .expect("create view");
        let requests = server.received_requests().await.expect("recording on");
        let create = requests
            .iter()
            .find(|r| {
                r.method.as_str() == "POST"
                    && r.url.path() == "/v1/namespaces/ns/views"
                    && r.body_json::<serde_json::Value>().unwrap()["name"] == "v"
            })
            .expect("CREATE VIEW posts to the views endpoint");
        assert_eq!(
            create
                .headers
                .get("authorization")
                .map(|v| v.to_str().unwrap()),
            Some("Bearer t0k"),
            "view calls carry the attach's bearer token"
        );
        let body: serde_json::Value = create.body_json().expect("json body");
        assert_eq!(body["name"], "v");
        assert_eq!(body["schema"]["fields"][0]["name"], "x");
        assert_eq!(body["schema"]["fields"][0]["type"], "long");
        let version = &body["view-version"];
        assert_eq!(version["default-catalog"], "wh");
        // The schema the body's unqualified names resolve in: the creating
        // session's (no USE here, so `default`), not the view's own
        // namespace (ai-portal-data#357).
        assert_eq!(version["default-namespace"], serde_json::json!(["default"]));
        assert_eq!(version["representations"][0]["dialect"], "sqe");
        assert_eq!(
            version["representations"][0]["sql"],
            "SELECT CAST(1 AS BIGINT) AS x"
        );

        let rows = client
            .execute("SELECT x FROM pol.ns.v")
            .await
            .expect("read through the view");
        assert_eq!(rows.rows, vec![vec!["1".to_string()]]);

        client
            .execute("CREATE OR REPLACE VIEW pol.ns.v AS SELECT CAST(2 AS BIGINT) AS x")
            .await
            .expect("replace view");
        let requests = server.received_requests().await.expect("recording on");
        let commit = requests
            .iter()
            .find(|r| r.method.as_str() == "POST" && r.url.path() == "/v1/namespaces/ns/views/v")
            .expect("OR REPLACE commits to the existing view");
        let body: serde_json::Value = commit.body_json().expect("json body");
        assert_eq!(body["requirements"][0]["type"], "assert-view-uuid");
        assert_eq!(
            body["requirements"][0]["uuid"],
            "fa6506c3-7681-40c8-86dc-e36561f83385"
        );

        client
            .execute("DROP VIEW pol.ns.v")
            .await
            .expect("drop view");
        let requests = server.received_requests().await.expect("recording on");
        assert!(
            requests
                .iter()
                .any(|r| r.method.as_str() == "DELETE"
                    && r.url.path() == "/v1/namespaces/ns/views/v"),
            "DROP VIEW deletes the view"
        );

        let err = client
            .execute("SELECT * FROM pol.ns.missing")
            .await
            .expect_err("an unknown name is still not found");
        assert!(err.to_string().contains("not found"), "{err}");

        let err = client
            .execute("CREATE MATERIALIZED VIEW pol.ns.m AS SELECT 1")
            .await
            .expect_err("materialized views are refused");
        assert!(err.to_string().contains("materialized"), "{err}");
    }

    // ------------------------------------------------------------------
    // Phase F: ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS
    // ------------------------------------------------------------------

    /// An attach without TOKEN (a local Nessie, an open dev Polaris) sends
    /// its view calls with no `Authorization` header, not `Bearer `.
    #[tokio::test]
    async fn anonymous_rest_attach_sends_no_authorization_on_views() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"overrides": {}, "defaults": {}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"namespaces": [["ns"]]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"identifiers": []})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/views"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(load_view_response("SELECT 1 AS x")),
            )
            .mount(&server)
            .await;

        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        client
            .execute(&format!(
                "ATTACH '{}' AS anon (TYPE iceberg_rest, WAREHOUSE 'wh')",
                server.uri()
            ))
            .await
            .expect("attach");
        client
            .execute("CREATE VIEW anon.ns.v AS SELECT CAST(1 AS BIGINT) AS x")
            .await
            .expect("create view");
        let requests = server.received_requests().await.expect("recording on");
        let create = requests
            .iter()
            .find(|r| r.method.as_str() == "POST" && r.url.path() == "/v1/namespaces/ns/views")
            .expect("CREATE VIEW posts to the views endpoint");
        assert!(
            create.headers.get("authorization").is_none(),
            "anonymous attach sent {:?}",
            create.headers.get("authorization")
        );
    }

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

    /// CTAS into an Iceberg catalog used to fail with "register_table does
    /// not support tables with data"; IF NOT EXISTS skips an existing table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_into_an_iceberg_catalog_writes_the_rows() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        for sql in [
            format!("ATTACH '{path}' AS ice (TYPE sqlite)"),
            "CREATE SCHEMA ice.s1".to_string(),
            "CREATE TABLE ice.s1.t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, name)"
                .to_string(),
            "CREATE TABLE IF NOT EXISTS ice.s1.t AS SELECT 9 AS id, 'z' AS name".to_string(),
        ] {
            client
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let r = client
            .execute("SELECT id, name FROM ice.s1.t ORDER BY id")
            .await
            .expect("select");
        assert_eq!(
            r.rows,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()]
            ]
        );
        let err = client
            .execute("CREATE TABLE ice.s1.t AS SELECT 1 AS id")
            .await
            .expect_err("an existing table without IF NOT EXISTS");
        assert!(err.to_string().contains("already exists"), "{err}");

        // A failing INSERT leaves no empty table behind. The cast only fails
        // on a row, so the empty CREATE (`LIMIT 0`) succeeds first.
        let err = client
            .execute("CREATE TABLE ice.s1.bad AS SELECT CAST(v AS INT) AS id FROM (VALUES ('x')) AS t(v)")
            .await
            .expect_err("the cast fails");
        assert!(err.to_string().to_lowercase().contains("cast"), "{err}");
        let r = client
            .execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'bad'")
            .await
            .expect("count");
        assert_eq!(r.rows, vec![vec!["0".to_string()]]);
    }

    /// A Parquet `timestamp(s)` / `timestamp(ms)` column reads as
    /// Timestamp(ms), which Iceberg cannot store: CTAS from such a file
    /// failed with "Unsupported Arrow data type: Timestamp(ms)". The column
    /// now lands as a microsecond timestamp with every value intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_widens_millisecond_timestamps_to_micros() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let file = tmp.path().join("coarse.parquet");
        let file = file.to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        for sql in [
            format!(
                "COPY (SELECT 1 AS id, arrow_cast(TIMESTAMP '2026-06-01 12:34:56.789', \
                 'Timestamp(Millisecond, None)') AS ts) TO '{file}'"
            ),
            format!("ATTACH '{path}/wh' AS ice (TYPE sqlite)"),
            "CREATE SCHEMA ice.s1".to_string(),
            format!("CREATE TABLE ice.s1.t AS SELECT * FROM read_parquet('{file}')"),
        ] {
            client
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let r = client
            .execute("SELECT id, CAST(ts AS VARCHAR), arrow_typeof(ts) FROM ice.s1.t")
            .await
            .expect("select");
        assert_eq!(r.rows.len(), 1, "{:?}", r.rows);
        assert_eq!(r.rows[0][0], "1");
        assert!(
            r.rows[0][1].starts_with("2026-06-01T12:34:56.789"),
            "{:?}",
            r.rows
        );
        assert!(
            r.rows[0][2].contains("Microsecond") || r.rows[0][2].contains("µs"),
            "{:?}",
            r.rows
        );
    }

    /// `SHOW TABLES IN|FROM [catalog.]schema [LIKE ...]` was "SHOW TABLES IN
    /// not supported" in embedded mode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn show_tables_in_a_schema() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        for sql in [
            format!("ATTACH '{path}' AS ice (TYPE sqlite)"),
            "CREATE SCHEMA ice.s1".to_string(),
            "CREATE SCHEMA ice.s2".to_string(),
            "CREATE TABLE ice.s1.orders (id BIGINT)".to_string(),
            "CREATE TABLE ice.s1.items (id BIGINT)".to_string(),
            "CREATE TABLE ice.s2.other (id BIGINT)".to_string(),
        ] {
            client
                .execute(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let names = |r: QueryResult| -> Vec<String> {
            r.rows
                .into_iter()
                .map(|row| row[2].clone())
                .filter(|n| !n.contains('$'))
                .collect()
        };
        for sql in ["SHOW TABLES IN ice.s1", "SHOW TABLES FROM ice.s1"] {
            let r = client
                .execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(r.columns[2], "table_name");
            assert_eq!(names(r), vec!["items", "orders"], "{sql}");
        }
        let r = client
            .execute("SHOW TABLES IN ice.s1 LIKE 'ord%'")
            .await
            .expect("like");
        assert_eq!(names(r), vec!["orders"]);
        let err = client
            .execute("SHOW TABLES IN nope.s1")
            .await
            .expect_err("unknown catalog");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    /// A Unity Catalog namespace also lists Delta tables, which fail to load
    /// as Iceberg. One table that does not load must not fail the ATTACH.
    #[tokio::test]
    async fn attach_skips_a_table_that_does_not_load() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().to_str().unwrap();
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build");
        let attach = format!("ATTACH '{path}' AS skipcat (TYPE sqlite)");

        for sql in [
            attach.as_str(),
            "CREATE SCHEMA skipcat.ns",
            "CREATE TABLE skipcat.ns.good (id INT)",
            "INSERT INTO skipcat.ns.good VALUES (7)",
            "CREATE TABLE skipcat.ns.bad (id INT)",
            "DETACH skipcat",
        ] {
            client.execute(sql).await.expect(sql);
        }

        // Remove the metadata of `bad`, so loading it fails.
        let mut dirs = vec![tmp.path().to_path_buf()];
        let mut removed = 0;
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let p = entry.expect("entry").path();
                if p.is_dir() {
                    dirs.push(p);
                } else if p.to_string_lossy().contains("/bad/")
                    && p.to_string_lossy().ends_with(".metadata.json")
                {
                    std::fs::remove_file(&p).expect("remove metadata");
                    removed += 1;
                }
            }
        }
        assert!(removed > 0, "found the metadata of `bad`");

        client
            .execute(&attach)
            .await
            .expect("ATTACH skips the broken table");
        let r = client
            .execute("SELECT id FROM skipcat.ns.good")
            .await
            .expect("good reads");
        assert_eq!(r.rows, vec![vec!["7".to_string()]]);
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
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT COMMENT 'order''s id', \
             amount DOUBLE, `select` STRING, `Ship To` STRING) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 'file://{}' TBLPROPERTIES ('comment' = 'all orders')",
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
        let manifest_bytes = std::fs::read(dir.path().join("wh/sales/orders/_sqe/table.json"))
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
            vec!["id", "amount", "select", "Ship To"]
        );
        // Names a bare identifier would fold or reject come back quoted;
        // the column and table comments survive the round trip.
        for needle in [
            "`select` string",
            "`Ship To` string",
            "COMMENT 'order''s id'",
        ] {
            assert!(ddl.contains(needle), "missing {needle} in: {ddl}");
        }
        assert_eq!(reparsed.columns[0].comment.as_deref(), Some("order's id"));
        assert_eq!(reparsed.comment.as_deref(), Some("all orders"));
        assert!(!reparsed.table_properties.contains_key("comment"));
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

    /// A `--catalog-backend glue|s3tables` catalog is shared with every other
    /// engine, so embedded mode only reads it. Unwrapped, the datafusion-iceberg
    /// provider turned `CREATE TABLE` / `CREATE VIEW` into new Glue tables,
    /// `DROP TABLE` into a real drop and `INSERT` into a commit.
    #[tokio::test]
    async fn backend_catalog_refuses_every_write() {
        use datafusion::catalog::{
            CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
        };
        use datafusion::datasource::MemTable;

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let s = Arc::new(MemorySchemaProvider::new());
        s.register_table(
            "t".into(),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("s", s).unwrap();

        let mut c = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        c.ctx.register_catalog(
            "glue",
            crate::read_only_catalog::ReadOnlyCatalog::wrap("glue", catalog),
        );
        c.read_only_catalog = Some("glue".into());

        for sql in [
            "CREATE TABLE glue.s.x (id BIGINT)",
            "CREATE TABLE glue.s.x AS SELECT 1 AS id",
            "CREATE VIEW glue.s.v AS SELECT 1 AS id",
            "DROP TABLE glue.s.t",
            "DROP TABLE IF EXISTS glue.s.t",
            "DROP VIEW IF EXISTS glue.s.t",
            "INSERT INTO glue.s.t VALUES (2)",
            "DELETE FROM glue.s.t",
            "UPDATE glue.s.t SET id = 3",
            "TRUNCATE TABLE glue.s.t",
            "CREATE SCHEMA glue.y",
            "DROP SCHEMA glue.s CASCADE",
        ] {
            let err = c.execute(sql).await.expect_err(sql);
            assert!(
                format!("{err}").contains("catalog `glue` is attached read-only"),
                "{sql}: {err}"
            );
        }

        let rows = c.execute("SELECT id FROM glue.s.t").await.expect("select");
        assert_eq!(rows.rows, vec![vec!["1".to_string()]]);
        let tables = c
            .execute("SHOW TABLES IN glue.s")
            .await
            .expect("show tables");
        assert_eq!(tables.rows.len(), 1, "{:?}", tables.rows);
    }
}

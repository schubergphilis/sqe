//! `CatalogProvider`/`SchemaProvider` over the Hive manifest, and the
//! `TableProvider` wrapper that gives a `ListingTable` size-derived
//! statistics and Hive's NULL-partition semantics.
//!
//! # Two lifetimes, deliberately different
//!
//! [`HiveCatalogProvider`] is built PER SESSION. Two answers to "why"
//! looked right and were not; both are recorded in `mount.rs`'s module doc
//! above `build_hive_root` so this does not silently disagree with the
//! corrected one. It is NOT because a Hive read needs per-user vended
//! credentials: reads use the engine's own `StorageConfig` credentials, or
//! a named `SECRET`, exactly like the file TVFs. It is NOT because each
//! session needs its own `RuntimeEnv`: production shares ONE `RuntimeEnv`/
//! `ObjectStoreRegistry` across every session
//! (`SessionContext::new_with_config_rt`), so a shared provider would not
//! be resolving against a stale one. The real reason: `data_store_factory`
//! carries this session's `TvfCaller`, and `TvfPolicy::check_path` gates
//! every table load on it, including the `{user}`-substituted
//! `allowed_object_store_prefixes` check. A provider built once and reused
//! would carry the FIRST session's caller into every later session's table
//! loads. `runtime` is still that session's `RuntimeEnv` (there being only
//! one in production does not make passing it optional; a fallback/test
//! path can build its own), matching how `ListingTable` resolves its
//! object store from the session's registry at scan time.
//!
//! [`HiveMetadataCache`] is the opposite: ONE process-wide instance, injected
//! into every per-session provider. It has to be shared because the policy
//! layer's `TagSource` is constructed once at coordinator startup and is
//! synchronous, so it can only read a cache that outlives any one session.
//! Sharing is safe: a manifest's content does not depend on who read it, and
//! reading one grants no data access; the mask decision is still made per
//! user.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, ScanArgs, ScanResult, SchemaProvider, Session};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Constraints, ScalarValue, Statistics};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    lit, Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::expressions::{
    binary as phys_binary, case as phys_case, col as phys_col, lit as phys_lit,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use futures::future::BoxFuture;
use futures::TryStreamExt;
use moka::sync::Cache as SyncCache;
use object_store::{ObjectMeta, ObjectStore};
use tracing::{debug, error, warn};

use super::manifest::{HiveManifest, PartitionIndex, PartitionIndexEntry};
use super::partition::{
    evaluate_projection, parse_projection, prune_entries_owned, ProjectionConfig,
};
use super::scan_exec::{HiveScanExec, HiveScanSpec};
use super::serde_map::{arrow_type, listing_options, resolve_format, ResolvedFormat};
use super::stats::{analyzed_statistics, row_width_hint, size_only_from_index, size_only_statistics};
use super::source::HiveMetadataSource;
use super::store::HiveManifestStore;
use super::HiveError;
use crate::schema_provider::{merge_listing, NamespaceListing};
use crate::send_erased::SendErased;

/// Capacity bound for a catalog's per-database table-name snapshots
/// ([`HiveListings::tables`]). Generous: a warehouse with more than this
/// many distinct databases referenced by one session is not the case this
/// is sized for, and an evicted snapshot just costs one more listing on the
/// next pre-plan refresh.
const TABLE_NAMES_CACHE_CAPACITY: u64 = 1024;

/// Last known database and table listings of one Hive catalog (architecture
/// review 806).
///
/// DataFusion's `CatalogProvider::schema_names()` and
/// `SchemaProvider::table_names()` are synchronous and have no error channel.
/// They used to bridge onto the metastore from inside the hook and turn a
/// listing failure into "this catalog has no databases" (finding P1-7). They
/// now serve these snapshots and never touch the network; the snapshots are
/// filled by [`HiveCatalogProvider::refresh_referenced`] from the
/// coordinator's (and the embedded CLI's) async pre-plan path. A failed
/// refresh keeps the previous names and records why in
/// [`NamespaceListing::stale_error`], so readers can tell "empty" from
/// "unavailable".
///
/// Shared by the catalog provider and every [`HiveSchemaProvider`] it
/// issues, so a listing refreshed for one query's planning serves the next
/// one within `ttl`. Per provider, not process-global like the Iceberg
/// snapshots: a Hive listing is not principal-filtered, and the provider is
/// itself session-scoped, so there is no cross-session sharing to gain.
pub struct HiveListings {
    /// How long a snapshot is served without being re-listed. A snapshot
    /// older than this is still served (last known) until the next refresh
    /// replaces it.
    ttl: Duration,
    databases: RwLock<Option<Arc<NamespaceListing>>>,
    tables: SyncCache<String, Arc<NamespaceListing>>,
}

impl HiveListings {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            databases: RwLock::new(None),
            tables: SyncCache::builder()
                .max_capacity(TABLE_NAMES_CACHE_CAPACITY)
                .time_to_idle(Duration::from_secs(3600))
                .build(),
        }
    }

    /// The database snapshot, if a refresh has run.
    pub fn databases(&self) -> Option<Arc<NamespaceListing>> {
        self.databases
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The table snapshot of `database`, if a refresh has run for it.
    pub fn tables(&self, database: &str) -> Option<Arc<NamespaceListing>> {
        self.tables.get(database)
    }

    fn is_fresh(&self, listing: Option<&Arc<NamespaceListing>>) -> bool {
        listing.is_some_and(|l| l.checked_at.elapsed() < self.ttl)
    }

    fn databases_fresh(&self) -> bool {
        self.is_fresh(self.databases().as_ref())
    }

    fn tables_fresh(&self, database: &str) -> bool {
        self.is_fresh(self.tables.get(database).as_ref())
    }

    fn store_databases(&self, outcome: Result<Vec<String>, (String, bool)>) -> Arc<NamespaceListing> {
        let mut slot = self.databases.write().unwrap_or_else(PoisonError::into_inner);
        let merged = Arc::new(merge_listing(slot.as_deref(), outcome));
        *slot = Some(Arc::clone(&merged));
        merged
    }

    fn store_tables(
        &self,
        database: &str,
        outcome: Result<Vec<String>, (String, bool)>,
    ) -> Arc<NamespaceListing> {
        let previous = self.tables.get(database);
        let merged = Arc::new(merge_listing(previous.as_deref(), outcome));
        self.tables.insert(database.to_string(), Arc::clone(&merged));
        merged
    }
}

impl fmt::Debug for HiveListings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveListings")
            .field("ttl", &self.ttl)
            .field("databases", &self.databases().map(|l| l.names.len()))
            .field("tables", &self.tables.entry_count())
            .finish()
    }
}

/// Turn a listing failure into the `(message, denied)` pair the snapshot
/// records. A `Forbidden` is a legitimate (empty) answer for this caller;
/// anything else means the metastore could not be listed.
fn listing_failure(e: &HiveError) -> (String, bool) {
    (e.to_string(), matches!(e, HiveError::Forbidden(_)))
}

/// Default capacity for [`HiveMetadataCache`]. Process-wide and long-lived,
/// so sized for "every Hive table this deployment has ever queried since
/// boot," not one session.
const MANIFEST_CACHE_CAPACITY: u64 = 16_384;

/// The directory name Hive itself writes for a NULL partition value.
///
/// DataFusion 54 has no native concept of this. Its own
/// `parse_partitions_for_path`/`try_into_partitioned_file`
/// (`datafusion-catalog-listing`) read the raw path segment after `=` and
/// pass it straight through `ScalarValue::try_from_string`, which is just
/// `ScalarValue::from(value).cast_to(target_type)`. No string casts to a
/// null scalar that way, and the crate's own doc comment on
/// `try_into_partitioned_file` states outright that "hive-style partition
/// values are never null" as a design assumption. So a `dt=
/// __HIVE_DEFAULT_PARTITION__` directory is read as the literal string
/// `__HIVE_DEFAULT_PARTITION__`, not NULL, and nothing upstream will ever
/// fix that: this module is what owns the translation, and it does it by
/// rewriting `IS NULL`/`IS NOT NULL` filters on partition columns into a
/// comparison against this literal before they reach `ListingTable::scan`
/// (see [`normalize_default_partition_filters`]). `ListingTable`'s own
/// `supports_filters_pushdown` already classifies both forms as `Exact`
/// pruning-eligible (its `can_be_evaluated_for_partition_pruning` accepts any
/// expression referencing only partition columns, `IsNull`/`IsNotNull`
/// included), so DataFusion trusts the scan's pruning entirely and never
/// re-checks with a `Filter` node above it. That is exactly why the
/// rewrite has to happen before the file listing, not after.
pub use super::manifest::HIVE_DEFAULT_PARTITION;

/// The codec the table's data files are wrapped in, read off the first
/// object under `prefix`.
///
/// Glue has no trustworthy field for this: `compressionType` is absent on
/// most tables and reads "none" on every table that carries it, gzipped
/// ones included. So the key suffix is the only signal, and one listing per
/// manifest-cache miss is what it costs.
///
/// Falls back to `current` when the prefix is empty, unreachable, or the
/// first object names no codec. A listing failure is not fatal here: the
/// scan itself will surface it with a better message than this would.
async fn detect_compression(
    store: &Arc<dyn ObjectStore>,
    prefix: &object_store::path::Path,
    current: datafusion::datasource::file_format::file_compression_type::FileCompressionType,
) -> datafusion::datasource::file_format::file_compression_type::FileCompressionType {
    let mut stream = store.list(Some(prefix));
    loop {
        match stream.try_next().await {
            Ok(Some(meta)) => {
                // Directory markers and the manifest sidecar say nothing
                // about how the data is encoded.
                let key = meta.location.as_ref();
                if meta.size == 0 || key.ends_with('/') || key.contains("/_sqe/") {
                    continue;
                }
                return super::serde_map::compression_from_key(key).unwrap_or(current);
            }
            Ok(None) => return current,
            Err(e) => {
                warn!(prefix = %prefix, error = %e, "hive_external: could not list files to detect compression; assuming uncompressed");
                return current;
            }
        }
    }
}

/// Builds an object store for a table's data location. The implementation
/// holds the session's credentials; `secret` is a name resolved against the
/// SecretStore, never material carried in a manifest.
pub trait HiveDataStoreFactory: Send + Sync + fmt::Debug {
    fn store_for(
        &self,
        location: &str,
        secret: Option<&str>,
    ) -> Result<Arc<dyn ObjectStore>, HiveError>;
}

/// A shared, process-wide cache of loaded manifests, keyed by
/// `"<database>.<table>"`.
///
/// One instance is built at coordinator startup and injected into every
/// per-session [`HiveCatalogProvider`]. It holds no credentials and grants no
/// access by itself: reading it only tells a caller what a table's columns
/// and tags are, and the policy layer still decides per user whether to mask
/// or deny based on that. There is deliberately no invalidation here beyond
/// the moka TTL/capacity eviction; a manifest change (e.g. `ALTER TABLE`)
/// self-heals the next time `HiveSchemaProvider::table` reloads it, which is
/// on every uncached call.
/// Who currently owns a bucket-level object-store registration: the table
/// that last (successfully) claimed it, and the `sqe.secret` name it read
/// under (`None` for engine-default credentials).
struct ObjectStoreOwner {
    table_ref: String,
    secret: Option<String>,
}

#[derive(Clone)]
struct CachedManifest {
    manifest: HiveManifest,
    etag: Option<String>,
    loaded_at: Instant,
}

pub struct HiveMetadataCache {
    inner: SyncCache<String, CachedManifest>,
    ttl: Duration,
    /// Guards against the collision `register_object_store` cannot see on
    /// its own: DataFusion's `ObjectStoreRegistry` keys purely on
    /// `scheme://host:port` (see `object_store_bucket_key`), discarding the
    /// path, so two Hive tables in the SAME bucket with DIFFERENT
    /// `sqe.secret` credentials would otherwise silently collide --
    /// whichever table is scanned last in this process wins the
    /// registration for every table in that bucket until something
    /// re-registers it. `claim_object_store_bucket` is called before every
    /// `register_object_store`, so the collision is a loud refusal instead
    /// of quietly-wrong data (where the mismatched credential happens to
    /// still work) or a confusing 403 (where it does not).
    object_store_owners: Mutex<HashMap<String, ObjectStoreOwner>>,
}

impl fmt::Debug for HiveMetadataCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveMetadataCache")
            .field("entry_count", &self.inner.entry_count())
            .finish()
    }
}

impl Default for HiveMetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

impl HiveMetadataCache {
    pub fn new() -> Self {
        Self::with_ttl(Duration::from_secs(60))
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            inner: SyncCache::builder()
                .max_capacity(MANIFEST_CACHE_CAPACITY)
                .build(),
            ttl,
            object_store_owners: Mutex::new(HashMap::new()),
        }
    }

    fn key(database: &str, table: &str) -> String {
        format!("{database}.{table}")
    }

    pub fn get(&self, database: &str, table: &str) -> Option<HiveManifest> {
        self.inner
            .get(&Self::key(database, table))
            .map(|c| c.manifest)
    }

    pub fn insert(&self, database: &str, table: &str, m: HiveManifest) {
        self.insert_versioned(database, table, m, None);
    }

    pub fn insert_versioned(
        &self,
        database: &str,
        table: &str,
        m: HiveManifest,
        etag: Option<String>,
    ) {
        self.inner.insert(
            Self::key(database, table),
            CachedManifest {
                manifest: m,
                etag,
                loaded_at: Instant::now(),
            },
        );
    }

    /// Use the cached manifest when the store's current ETag matches, or
    /// when the store has no ETag and the TTL has not elapsed. A changed
    /// ETag returns `None` so the caller reloads.
    pub fn get_if_fresh(
        &self,
        database: &str,
        table: &str,
        etag: Option<&str>,
    ) -> Option<HiveManifest> {
        let cached = self.inner.get(&Self::key(database, table))?;
        match etag {
            Some(now) => {
                if cached.etag.as_deref() == Some(now) {
                    Some(cached.manifest)
                } else {
                    None
                }
            }
            None => {
                if cached.loaded_at.elapsed() < self.ttl {
                    Some(cached.manifest)
                } else {
                    None
                }
            }
        }
    }

    pub fn invalidate(&self, database: &str, table: &str) {
        self.inner.invalidate(&Self::key(database, table));
    }

    /// Claim `bucket_key` for `table_ref` reading under `secret`, or refuse
    /// naming both tables and both secrets when a DIFFERENT table already
    /// claimed this bucket under a DIFFERENT secret.
    ///
    /// A successful claim (no mismatch) ALWAYS updates the stored record to
    /// `(table_ref, secret)`, even when the claimant is the SAME table as
    /// before: `register_object_store` is about to overwrite whatever
    /// store DataFusion had registered for this bucket with the one built
    /// from THIS claim, so the record has to track "whichever claim most
    /// recently succeeded", the same last-write-wins semantics the
    /// registry itself has, or it goes stale and starts comparing future
    /// claims against a secret nothing is actually registered under
    /// anymore. An earlier version of this function returned `Ok(())` on a
    /// same-table match WITHOUT updating the record, which reopened the
    /// exact hole this guard exists to close: table A claims under `s1`,
    /// A's manifest changes to `s2` and re-claims (allowed, but the record
    /// stayed `(A, s1)`), then table B claims under `s1` -- table differs
    /// but the STALE secret matches, so B is wrongly allowed and its `s1`
    /// store silently replaces A's `s2` store in the registry. Updating
    /// the record on every non-refused claim closes that, and as a side
    /// effect also removes the mirror-image bug (a table legitimately
    /// re-claiming under a new secret being refused because the record
    /// still showed the old one).
    ///
    /// Known gap, accepted rather than fixed this round: this never
    /// evicts on `DROP TABLE`, so a table dropped and never re-created
    /// leaves a stale-but-harmless record (nothing will ever claim its
    /// exact `table_ref` again, and a genuinely different table sharing
    /// its bucket is still compared against the last REAL claim, which is
    /// correct); only a same-table re-claim needed the fix above, and it
    /// has it.
    pub fn claim_object_store_bucket(
        &self,
        bucket_key: &str,
        table_ref: &str,
        secret: Option<&str>,
    ) -> Result<(), String> {
        let mut owners = self
            .object_store_owners
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = owners.get(bucket_key) {
            if existing.table_ref != table_ref && existing.secret.as_deref() != secret {
                return Err(format!(
                    "hive_external: table '{table_ref}' would register object-store \
                     credentials for bucket '{bucket_key}' under secret {secret:?}, but \
                     table '{}' already registered that bucket under secret {:?}. \
                     DataFusion's object-store registry keys by bucket, not by table, so two \
                     Hive tables in the same bucket must read under the same credentials, or a \
                     scan spanning both would silently read one table's data under the other's \
                     credentials",
                    existing.table_ref, existing.secret
                ));
            }
        }
        owners.insert(
            bucket_key.to_string(),
            ObjectStoreOwner {
                table_ref: table_ref.to_string(),
                secret: secret.map(str::to_string),
            },
        );
        Ok(())
    }
}

/// The bucket-only key `datafusion_execution::object_store`'s registry
/// computes internally (`format!("{scheme}://{host}:{port}")`, discarding
/// the path) before looking up or inserting a store. Not exported by
/// DataFusion, so duplicated here deliberately: `claim_object_store_bucket`
/// needs to detect a collision on the SAME key `register_object_store`
/// will use, or the check and the actual registration could disagree about
/// what counts as "the same bucket".
fn object_store_bucket_key(url: &url::Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort]
    )
}

/// `HiveError` from the manifest/data-store path becomes a `DataFusionError`
/// at the `TableProvider`/`SchemaProvider` boundary, since that is the only
/// error type those traits can return.
fn to_df(e: HiveError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Hive's `LOCATION` always names a directory, even for a table backed by a
/// single file: every object directly under the prefix is a data file.
/// `ListingTableUrl::is_collection()` disagrees for a scheme URL with no
/// trailing `/` (it is exactly `self.url.path().ends_with('/')`), treating
/// it as a pointer to one object instead of a directory listing. Nothing in
/// Hive DDL requires a trailing slash on `LOCATION`, so a table declared
/// without one would otherwise make every file below it invisible to the
/// scan. This is the layer that owns normalizing that gap.
pub(crate) fn table_location_url(location: &str) -> Result<ListingTableUrl, HiveError> {
    let normalized = if location.ends_with('/') {
        location.to_string()
    } else {
        format!("{location}/")
    };
    ListingTableUrl::parse(&normalized)
        .map_err(|e| HiveError::Validation(format!("invalid location `{location}`: {e}")))
}

/// The Arrow schema for the columns physically stored in the data files:
/// `storage_descriptor.columns` only, NOT `partition_keys`.
///
/// `ListingTable::try_new` appends the partition columns itself (as
/// non-nullable fields, after this schema) when building its combined
/// `table_schema`; passing a schema that already includes them here would
/// double them up. That append is also what gives "partition keys last",
/// Hive's own column order, without this module needing to build the
/// combined schema by hand.
///
/// All fields nullable: CSV/JSON have no NOT NULL enforcement to trust, and
/// this is never inferred from the data, only from the declared column list,
/// so a renamed or reordered column keeps its ordinal position (the CSV
/// reader binds fields by position against this schema, never by name).
fn file_schema_of(manifest: &HiveManifest) -> Result<Schema, HiveError> {
    let fields = manifest
        .storage_descriptor
        .columns
        .iter()
        .map(|c| arrow_type(&c.r#type).map(|dt| Field::new(&c.name, dt, true)))
        .collect::<Result<Vec<Field>, HiveError>>()?;
    Ok(Schema::new(fields))
}

/// `(name, type)` pairs for `manifest.partition_keys`, in declared order:
/// what `ListingOptions::with_table_partition_cols` and, in turn,
/// `ListingTable::try_new`'s schema-building append, consume.
fn partition_columns_of(manifest: &HiveManifest) -> Result<Vec<(String, DataType)>, HiveError> {
    manifest
        .partition_keys
        .iter()
        .map(|k| arrow_type(&k.r#type).map(|dt| (k.name.clone(), dt)))
        .collect()
}

/// Rewrite `col IS NULL` / `col IS NOT NULL` into a comparison against
/// [`HIVE_DEFAULT_PARTITION`] for every `col` in `partition_cols`, anywhere
/// in the expression tree. See the doc comment on [`HIVE_DEFAULT_PARTITION`]
/// for why this has to happen before the filters reach `ListingTable::scan`,
/// not after.
///
/// Every other expression, including `IS NULL`/`IS NOT NULL` on a
/// non-partition column, passes through unchanged.
fn normalize_default_partition_filters(
    filters: &[Expr],
    partition_cols: &[String],
) -> DFResult<Vec<Expr>> {
    filters
        .iter()
        .cloned()
        .map(|e| {
            e.transform(|node| rewrite_default_partition_node(node, partition_cols))
                .data()
        })
        .collect()
}

fn rewrite_default_partition_node(
    expr: Expr,
    partition_cols: &[String],
) -> DFResult<Transformed<Expr>> {
    match expr {
        Expr::IsNull(inner) => match *inner {
            Expr::Column(c) if partition_cols.contains(&c.name) => Ok(Transformed::yes(
                Expr::Column(c).eq(lit(HIVE_DEFAULT_PARTITION)),
            )),
            other => Ok(Transformed::no(Expr::IsNull(Box::new(other)))),
        },
        Expr::IsNotNull(inner) => match *inner {
            Expr::Column(c) if partition_cols.contains(&c.name) => Ok(Transformed::yes(
                Expr::Column(c).not_eq(lit(HIVE_DEFAULT_PARTITION)),
            )),
            other => Ok(Transformed::no(Expr::IsNotNull(Box::new(other)))),
        },
        other => Ok(Transformed::no(other)),
    }
}

/// Wraps `plan`'s output in a `ProjectionExec` that turns the literal
/// sentinel value into a real NULL for every string-typed partition column,
/// so `SELECT dt` and `GROUP BY dt` agree with the `IS NULL` filter rewrite
/// above about what the value actually is.
///
/// Filter rewriting alone was not enough: it made `WHERE dt IS NULL` return
/// the right rows, but a plain `SELECT dt` on the same table still returned
/// the literal string `__HIVE_DEFAULT_PARTITION__`, and a `GROUP BY dt` would
/// have bucketed those rows under that string instead of folding them into
/// the NULL group. That mix, correct filtering but a wrong projected value,
/// is worse than either consistent choice, since two clauses of the same
/// query would disagree about what the value is. This closes the gap on the
/// projection side to match.
///
/// Scoped to string-typed partition columns (`Utf8`/`LargeUtf8`/`Utf8View`):
/// Hive's sentinel is always that fixed string literal, written to the
/// directory name regardless of the partition column's declared type, but a
/// non-string partition column carrying that literal directory name would
/// already have failed earlier, in `ListingTable`'s own file-listing
/// (`ScalarValue::try_from_string("__HIVE_DEFAULT_PARTITION__", non_utf8_ty)`
/// does not parse), so this function never actually sees one.
///
/// A no-op (returns `plan` unchanged) when no partition column appears in
/// the output schema at all, which happens whenever a query's projection
/// does not select or need one.
fn nullify_default_partition_columns(
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: &[String],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let is_rewritable_partition_column = |name: &str, dt: &DataType| {
        partition_columns.contains(&name.to_string())
            && matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
    };

    if !schema
        .fields()
        .iter()
        .any(|f| is_rewritable_partition_column(f.name(), f.data_type()))
    {
        return Ok(plan);
    }

    let exprs = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().clone();
            let base = phys_col(&name, schema.as_ref())?;
            let expr: Arc<dyn PhysicalExpr> =
                if is_rewritable_partition_column(&name, f.data_type()) {
                    let is_sentinel = phys_binary(
                        Arc::clone(&base),
                        Operator::Eq,
                        phys_lit(HIVE_DEFAULT_PARTITION),
                        schema.as_ref(),
                    )?;
                    phys_case(
                        None,
                        vec![(is_sentinel, phys_lit(ScalarValue::Utf8(None)))],
                        Some(base),
                    )?
                } else {
                    base
                };
            Ok((expr, name))
        })
        .collect::<DFResult<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;

    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

/// Wraps a `ListingTable`, delegating every `TableProvider` method to it
/// except [`TableProvider::schema`] (partition columns forced nullable; see
/// below), [`TableProvider::statistics`] (size-derived; see `stats.rs`), and
/// [`TableProvider::scan`]/[`TableProvider::scan_with_args`] (rewritten per
/// [`normalize_default_partition_filters`] before delegating).
///
/// `with_collect_stat(false)` in `serde_map::listing_options` makes
/// `ListingTable::statistics()` report `None` (DataFusion's own optimizer
/// treats that as fully Unknown), and nothing else in `ListingTable` would
/// ever inject the size-derived statistics this design computes at `table()`
/// time. `JoinStrategyRule` and `star_schema_reorder` both plan from
/// statistics; Unknown row counts are what caused the five-query SMJ
/// regression fixed by MR !724. Wrapping rather than subclassing is the only
/// option: `ListingTable` is not open for extension (its fields are private
/// and it has no hook to override just one method).
///
/// # Why `schema()` cannot just delegate
///
/// `ListingTable::try_new` hardcodes every partition column as
/// `Field::new(name, ty, false)`, NOT NULLABLE, unconditionally, with no
/// config knob to change it. Probing confirmed the consequence: DataFusion's
/// own logical optimizer folds `dt IS NULL` to a constant `false`
/// (`EmptyRelation`, seen directly in `EXPLAIN`) using exactly that
/// nullability fact, before the query ever reaches this wrapper's `scan()`.
/// [`normalize_default_partition_filters`]'s rewrite runs too late to see
/// a filter the optimizer already deleted. So this wrapper's `schema()`
/// returns `self.inner.schema()` with every column in `partition_columns`
/// remarked nullable, purely so the optimizer keeps the filter instead of
/// deleting it before the rewrite gets a chance to run. That alone does not
/// make the projected value correct: see
/// [`nullify_default_partition_columns`] for the other half, which turns the
/// literal sentinel into a real NULL in `scan()`'s physical output so
/// `SELECT dt` and `WHERE dt IS NULL` agree on what the value is.
struct HiveTableProvider {
    /// The wrapped `ListingTable`, held as a trait object on purpose. Every
    /// `async_trait` method below proves `HiveTableProvider: Sync` for its own
    /// `Send` future, and with a concrete `ListingTable` here that proof
    /// walked DataFusion's whole plan and expression type graph
    /// (`ListingOptions` -> `SortExpr` -> `Expr` -> `LogicalPlan`) once per
    /// method, about 3.5 s each (`-Zself-profile`, 2026-09-07). `dyn
    /// TableProvider` is `Send + Sync` by declaration, so the walk stops here.
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
    statistics: Statistics,
    partition_columns: Vec<String>,
    partition_types: Vec<(String, String)>,
    index: Option<PartitionIndex>,
    projection: Option<ProjectionConfig>,
    resolved: ResolvedFormat,
    file_schema: SchemaRef,
    data_store_factory: Arc<dyn HiveDataStoreFactory>,
    secret: Option<String>,
    runtime: Arc<RuntimeEnv>,
    max_index_entries: usize,
    table_location: String,
}

impl fmt::Debug for HiveTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveTableProvider")
            .field("partition_columns", &self.partition_columns)
            .field("has_index", &self.index.is_some())
            .finish()
    }
}

impl HiveTableProvider {
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: ListingTable,
        statistics: Statistics,
        partition_columns: Vec<String>,
        partition_types: Vec<(String, String)>,
        index: Option<PartitionIndex>,
        projection: Option<ProjectionConfig>,
        resolved: ResolvedFormat,
        file_schema: SchemaRef,
        data_store_factory: Arc<dyn HiveDataStoreFactory>,
        secret: Option<String>,
        runtime: Arc<RuntimeEnv>,
        max_index_entries: usize,
        table_location: String,
    ) -> Self {
        let schema = nullable_partition_schema(&inner.schema(), &partition_columns);
        Self {
            inner: Arc::new(inner),
            schema,
            statistics,
            partition_columns,
            partition_types,
            index,
            projection,
            resolved,
            file_schema,
            data_store_factory,
            secret,
            runtime,
            max_index_entries,
            table_location,
        }
    }

    fn uses_index_scan(&self) -> bool {
        self.projection
            .as_ref()
            .map(|p| p.enabled)
            .unwrap_or(false)
            || self.index.is_some()
    }

    fn planned_entries(&self, filters: &[Expr]) -> Result<Vec<PartitionIndexEntry>, HiveError> {
        if let Some(proj) = self.projection.as_ref().filter(|p| p.enabled) {
            let virtuals = evaluate_projection(
                proj,
                &self.table_location,
                filters,
                &self.partition_types,
                self.max_index_entries,
            )?;
            return Ok(prune_entries_owned(
                &virtuals,
                filters,
                &self.partition_types,
            ));
        }
        if let Some(index) = &self.index {
            return Ok(prune_entries_owned(
                &index.entries,
                filters,
                &self.partition_types,
            ));
        }
        Ok(Vec::new())
    }

    /// Boxed at the definition (compile time, see
    /// docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the
    /// coroutine owns `filters: Vec<Expr>`, so its `Send` proof walks
    /// DataFusion's expression graph; done once here rather than again in
    /// each `scan` variant that awaits it. Heap-allocating it is also what
    /// keeps the per-partition `ListingTable` scans off a 2 MB tokio worker
    /// stack during SELECT.
    fn scan_from_entries<'a>(
        &'a self,
        state: &'a dyn Session,
        projection: Option<&'a [usize]>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> BoxFuture<'a, DFResult<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.scan_from_entries_inner(state, projection, filters, limit))
    }

    async fn scan_from_entries_inner(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let entries = self.planned_entries(&filters).map_err(to_df)?;
        let out_schema = projected_schema(&self.schema, projection);
        if entries.is_empty() {
            return Ok(Arc::new(EmptyExec::new(out_schema)));
        }

        let n_file = self.file_schema.fields().len();
        let file_proj = file_column_projection(projection, n_file);
        let data_filters: Vec<Expr> = filters
            .iter()
            .filter(|f| !references_only_partition_cols(f, &self.partition_columns))
            .cloned()
            .collect();

        let mut plans = Vec::with_capacity(entries.len());
        for entry in &entries {
            let url = table_location_url(&entry.location).map_err(to_df)?;
            let store = self
                .data_store_factory
                .store_for(&entry.location, self.secret.as_deref())
                .map_err(to_df)?;
            self.runtime
                .register_object_store(url.as_ref(), store);

            let options = listing_options(&self.resolved, Vec::new()).map_err(to_df)?;
            let config = ListingTableConfig::new(url)
                .with_listing_options(options)
                .with_schema(Arc::clone(&self.file_schema));
            // As a trait object for the same compile-time reason as
            // `HiveTableProvider::inner`: this future holds it across the
            // `.await`, and `dyn TableProvider::scan` is an opaque `BoxFuture`.
            let listing: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
            let plan = listing
                .scan(state, file_proj.as_ref(), &data_filters, limit)
                .await?;
            let plan = attach_partition_values(
                plan,
                &self.file_schema,
                &self.partition_columns,
                &entry.values,
                projection,
                &self.schema,
            )?;
            let plan = nullify_default_partition_columns(plan, &self.partition_columns)?;
            plans.push(plan);
        }

        let inner: Arc<dyn ExecutionPlan> = if plans.len() == 1 {
            plans.pop().unwrap()
        } else {
            UnionExec::try_new(plans)?
        };

        // Wrap in the one node a Hive scan can be recognized by. The coordinator
        // finds what to distribute with `downcast_ref::<IcebergScanExec>()`, and
        // until this existed a Hive scan was a UnionExec of projections over
        // ListingTable scans with nothing in it naming the table. `execute`
        // delegates, so this changes nothing about what a query returns.
        Ok(Arc::new(HiveScanExec::new(
            inner,
            HiveScanSpec {
                entries,
                resolved: self.resolved.clone(),
                partition_columns: self.partition_columns.clone(),
                full_schema: Arc::clone(&self.schema),
                secret: self.secret.clone(),
                data_store_factory: Arc::clone(&self.data_store_factory),
            },
        )))
    }
}

fn projected_schema(schema: &Schema, projection: Option<&[usize]>) -> SchemaRef {
    match projection {
        Some(idx) => {
            let fields: Vec<Field> = idx.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields).with_metadata(schema.metadata().clone()))
        }
        None => Arc::new(schema.clone()),
    }
}

fn file_column_projection(projection: Option<&[usize]>, n_file: usize) -> Option<Vec<usize>> {
    let sel = projection?;
    let file_idx: Vec<usize> = sel.iter().copied().filter(|&i| i < n_file).collect();
    if file_idx.is_empty() {
        None
    } else {
        Some(file_idx)
    }
}

fn references_only_partition_cols(expr: &Expr, partition_cols: &[String]) -> bool {
    let mut only = true;
    let mut any = false;
    let _ = expr.apply(|node| {
        if let Expr::Column(c) = node {
            any = true;
            if !partition_cols.iter().any(|p| p == &c.name) {
                only = false;
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    any && only
}

fn attach_partition_values(
    plan: Arc<dyn ExecutionPlan>,
    file_schema: &Schema,
    partition_columns: &[String],
    values: &std::collections::BTreeMap<String, String>,
    projection: Option<&[usize]>,
    full_schema: &Schema,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let selected: Vec<usize> = match projection {
        Some(p) => p.to_vec(),
        None => (0..full_schema.fields().len()).collect(),
    };
    let inner_schema = plan.schema();
    let n_file = file_schema.fields().len();
    let exprs = selected
        .iter()
        .map(|&i| {
            let field = full_schema.field(i);
            let name = field.name().clone();
            let expr: Arc<dyn PhysicalExpr> = if i < n_file {
                phys_col(&name, inner_schema.as_ref())?
            } else {
                let key = &partition_columns[i - n_file];
                let raw = values
                    .get(key)
                    .map(String::as_str)
                    .unwrap_or(HIVE_DEFAULT_PARTITION);
                phys_lit(partition_scalar(raw, field.data_type()))
            };
            Ok((expr, name))
        })
        .collect::<DFResult<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

/// Type a directory-encoded partition value against the column's declared
/// Arrow type.
///
/// `pub` because cycle 5 runs the same materialization on a WORKER, from a
/// `HiveScanDescriptor`, and two copies of this rule would drift. The symptom
/// of drift is not a crash: it is a partition column silently changing type
/// between coordinator-local and distributed execution of the same query.
pub fn partition_scalar(raw: &str, dt: &DataType) -> ScalarValue {
    if raw == HIVE_DEFAULT_PARTITION {
        return match dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                ScalarValue::Utf8(Some(HIVE_DEFAULT_PARTITION.to_string()))
            }
            other => ScalarValue::try_from(other).unwrap_or(ScalarValue::Null),
        };
    }
    ScalarValue::try_from_string(raw.to_string(), dt)
        .unwrap_or_else(|_| ScalarValue::Utf8(Some(raw.to_string())))
}

/// `schema` with every field named in `partition_columns` remarked nullable.
/// See the "Why `schema()` cannot just delegate" section on
/// [`HiveTableProvider`] for why this exists.
fn nullable_partition_schema(schema: &Schema, partition_columns: &[String]) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if partition_columns.iter().any(|p| p == f.name()) {
                f.as_ref().clone().with_nullable(true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

#[async_trait]
impl TableProvider for HiveTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    // `scan` and `scan_with_args` are written in `async_trait`'s desugared
    // form (compile time, see
    // docs/internal/plans/2026-09-07-compile-time-async-hubs.md). The
    // rewritten filters must outlive the inner scan that borrows them, so
    // the async block has to own the `Vec<Expr>`; it holds it as a
    // `SendErased`, which states the (true) `Send` fact once instead of
    // making rustc re-prove it over DataFusion's whole expression graph for
    // this method's parameter environment (2 s apiece). The filter rewrite
    // itself runs synchronously before the block, so a bad filter still
    // fails before any I/O, as it did.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let rewritten = match normalize_default_partition_filters(filters, &self.partition_columns)
        {
            Ok(r) => r,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        if self.uses_index_scan() {
            return self.scan_from_entries(state, projection.map(Vec::as_slice), rewritten, limit);
        }
        let held = SendErased::new(rewritten);
        Box::pin(async move {
            let plan = self
                .inner
                .scan(state, projection, held.get(), limit)
                .await?;
            nullify_default_partition_columns(plan, &self.partition_columns)
        })
    }

    fn scan_with_args<'a, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        args: ScanArgs<'a>,
    ) -> BoxFuture<'async_trait, DFResult<ScanResult>>
    where
        'a: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let rewritten = match args.filters() {
            Some(f) => match normalize_default_partition_filters(f, &self.partition_columns) {
                Ok(r) => Some(r),
                Err(e) => return Box::pin(async move { Err(e) }),
            },
            None => None,
        };
        let projection = args.projection();
        let limit = args.limit();
        if self.uses_index_scan() {
            use futures::TryFutureExt;
            return Box::pin(
                self.scan_from_entries(state, projection, rewritten.unwrap_or_default(), limit)
                    .map_ok(ScanResult::new),
            );
        }
        let held = SendErased::new(rewritten.unwrap_or_default());
        let has_filters = args.filters().is_some();
        Box::pin(async move {
            let mut new_args = ScanArgs::default()
                .with_projection(projection)
                .with_limit(limit);
            if has_filters {
                new_args = new_args.with_filters(Some(held.get().as_slice()));
            }
            let result = self.inner.scan_with_args(state, new_args).await?;
            let plan =
                nullify_default_partition_columns(result.into_inner(), &self.partition_columns)?;
            Ok(ScanResult::new(plan))
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.statistics.clone())
    }

    // The four DML hooks below are written in `async_trait`'s desugared form
    // on purpose (compile time, see
    // docs/internal/plans/2026-09-07-compile-time-async-hubs.md): an
    // `async fn` wrapper owns a coroutine capturing `Vec<Expr>`, and proving
    // it `Send` walks DataFusion's whole `Expr` graph once more under this
    // method's parameter environment (2 s apiece). The inner table's future
    // is already boxed, so it is handed straight through.
    fn insert_into<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.insert_into(state, input, insert_op)
    }

    fn delete_from<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        filters: Vec<Expr>,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.delete_from(state, filters)
    }

    fn update<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update(state, assignments, filters)
    }

    fn truncate<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.truncate(state)
    }
}

/// DataFusion `CatalogProvider` over the Hive manifest store. Built PER
/// SESSION; see the module doc comment for why.
pub struct HiveCatalogProvider {
    name: String,
    /// Where table metadata is read from. A sidecar warehouse and a
    /// metastore that already exists (Glue, HMS) differ only here.
    source: Arc<dyn HiveMetadataSource>,
    /// The same object as `source` when this catalog is backed by a sidecar
    /// warehouse SQE owns, and `None` when it reads a metastore it does not.
    /// Only DDL needs it; reads go through `source`.
    sidecar: Option<Arc<HiveManifestStore>>,
    data_store_factory: Arc<dyn HiveDataStoreFactory>,
    runtime: Arc<RuntimeEnv>,
    manifest_cache: Arc<HiveMetadataCache>,
    /// Database and per-database table-name snapshots, shared with every
    /// `HiveSchemaProvider` this catalog issues via `schema()`; see
    /// [`HiveListings`].
    listings: Arc<HiveListings>,
}

impl fmt::Debug for HiveCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `HiveManifestStore` has no `Debug` impl (it holds an
        // `Arc<dyn ObjectStore>`), so this cannot derive; the fields that
        // matter for a log line are the name and cache occupancy, not the
        // store or the session's credentialed factory.
        f.debug_struct("HiveCatalogProvider")
            .field("name", &self.name)
            .field("manifest_cache", &self.manifest_cache)
            .finish()
    }
}

impl HiveCatalogProvider {
    /// Built PER SESSION. The binding reason is `data_store_factory`, which
    /// carries that session's `TvfCaller` (see `mount.rs`'s module doc
    /// comment above `build_hive_root` for why reusing it across sessions
    /// would be a per-user authorization bug, not a plumbing one); `runtime`
    /// is also session-scoped but is not itself why the provider must be
    /// rebuilt (production shares one `RuntimeEnv` across every session).
    /// `manifest_cache` is the one exception: a process-wide singleton,
    /// shared across every session.
    pub fn new(
        name: String,
        store: Arc<HiveManifestStore>,
        data_store_factory: Arc<dyn HiveDataStoreFactory>,
        runtime: Arc<RuntimeEnv>,
        manifest_cache: Arc<HiveMetadataCache>,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            name,
            source: Arc::clone(&store) as Arc<dyn HiveMetadataSource>,
            sidecar: Some(store),
            data_store_factory,
            runtime,
            manifest_cache,
            listings: Arc::new(HiveListings::new(cache_ttl)),
        }
    }

    /// Same catalog over a metastore SQE does not own, so there is no
    /// sidecar to write and DDL is refused rather than silently dropped.
    pub fn with_source(
        name: String,
        source: Arc<dyn HiveMetadataSource>,
        data_store_factory: Arc<dyn HiveDataStoreFactory>,
        runtime: Arc<RuntimeEnv>,
        manifest_cache: Arc<HiveMetadataCache>,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            name,
            source,
            sidecar: None,
            data_store_factory,
            runtime,
            manifest_cache,
            listings: Arc::new(HiveListings::new(cache_ttl)),
        }
    }

    /// The catalog's name as registered with DataFusion.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The listing snapshots this provider and its schema providers serve.
    pub fn listings(&self) -> &Arc<HiveListings> {
        &self.listings
    }

    /// List the databases from the metastore and store the result as the
    /// catalog's snapshot. On failure the previous names are kept and the
    /// error recorded, then returned so an async caller can surface it.
    pub async fn refresh_databases(&self) -> Result<Arc<NamespaceListing>, HiveError> {
        match self.source.list_databases().await {
            Ok(mut names) => {
                names.sort();
                Ok(self.listings.store_databases(Ok(names)))
            }
            Err(e) => {
                let kept = self.listings.store_databases(Err(listing_failure(&e)));
                error!(catalog = %self.name, error = %e, kept = kept.names.len(), "hive_external: failed to list databases; keeping previous snapshot");
                Err(e)
            }
        }
    }

    /// List the tables of `database` and store the result as its snapshot;
    /// same failure contract as [`Self::refresh_databases`].
    pub async fn refresh_tables(&self, database: &str) -> Result<Arc<NamespaceListing>, HiveError> {
        match self.source.list_tables(database).await {
            Ok(mut names) => {
                names.sort();
                Ok(self.listings.store_tables(database, Ok(names)))
            }
            Err(e) => {
                let kept = self.listings.store_tables(database, Err(listing_failure(&e)));
                error!(catalog = %self.name, database, error = %e, kept = kept.names.len(), "hive_external: failed to list tables; keeping previous snapshot");
                Err(e)
            }
        }
    }

    /// Refresh the snapshots a statement is about to plan against, off the
    /// async path (review 806): the database list, plus the tables of every
    /// database the statement references in this catalog (`cat.db.t`, and
    /// `db.t` / the session schema when this catalog is `default_catalog`).
    /// A statement that reads `information_schema` enumerates every
    /// database, so it refreshes them all. Snapshots younger than the TTL
    /// are skipped, so the steady-state cost is one listing per database
    /// per window, not per query. Failures are recorded in the snapshot,
    /// never propagated: planning proceeds on the last known names and
    /// `table()` reports the precise error for the table it actually needs.
    pub async fn refresh_referenced(
        &self,
        sql: &str,
        default_catalog: Option<&str>,
        default_schema: Option<&str>,
    ) {
        let is_default = default_catalog.is_some_and(|c| c.eq_ignore_ascii_case(&self.name));
        let mut databases: Vec<String> = Vec::new();
        if is_default {
            if let Some(schema) = default_schema {
                databases.push(schema.to_string());
            }
        }
        for (catalog, schema) in sqe_sql::extract_schema_qualifiers_from_sql(sql) {
            let ours = match catalog {
                Some(c) => c.eq_ignore_ascii_case(&self.name),
                None => is_default,
            };
            // `information_schema` is DataFusion's own virtual schema, not a
            // Hive database to list.
            if ours
                && !schema.eq_ignore_ascii_case("information_schema")
                && !databases.iter().any(|d| d.eq_ignore_ascii_case(&schema))
            {
                databases.push(schema);
            }
        }

        if !self.listings.databases_fresh() {
            // Errors are already folded into the snapshot with their reason.
            let _ = self.refresh_databases().await;
        }

        if sql_reads_information_schema(sql) {
            if let Some(listing) = self.listings.databases() {
                for db in &listing.names {
                    if !databases.iter().any(|d| d.eq_ignore_ascii_case(db)) {
                        databases.push(db.clone());
                    }
                }
            }
        }

        for db in databases {
            if self.listings.tables_fresh(&db) {
                continue;
            }
            let _ = self.refresh_tables(&db).await;
        }
    }

    /// Refresh the database list and every database's tables, regardless of
    /// age. For callers that will enumerate the whole catalog next (tests,
    /// tooling); a query path should use [`Self::refresh_referenced`].
    pub async fn refresh_all(&self) {
        let Ok(databases) = self.refresh_databases().await else {
            return;
        };
        for db in &databases.names {
            let _ = self.refresh_tables(db).await;
        }
    }

    pub fn manifest_cache(&self) -> Arc<HiveMetadataCache> {
        Arc::clone(&self.manifest_cache)
    }

    /// The shared manifest store backing this catalog. Embedded-mode DDL
    /// (`CREATE EXTERNAL TABLE` / `DROP TABLE` / `SHOW CREATE TABLE`) writes,
    /// deletes, and reads manifests directly through this handle rather than
    /// through a DataFusion `SchemaProvider` method, since none of those
    /// three statements has a DataFusion-native path that reaches a Hive
    /// manifest.
    /// `None` when this catalog reads a metastore SQE does not own, where
    /// there is nothing to write a manifest to. Callers turn that into a
    /// refusal naming the catalog rather than a silent no-op.
    pub fn store(&self) -> Option<Arc<HiveManifestStore>> {
        self.sidecar.clone()
    }

    /// Drop the table-name snapshot of `database` (and the database list,
    /// since the first table of a new database creates it) so the next
    /// pre-plan refresh re-lists instead of waiting out the TTL. Call after
    /// DDL that creates or drops a table. Mirrors
    /// `SqeSchemaProvider::invalidate_table_names` (`schema_provider.rs`).
    ///
    /// `HiveSchemaProvider::table()` itself needs no matching invalidation:
    /// it calls `store.get()` unconditionally on every lookup (see its own
    /// doc comment on cache-before-build ordering) rather than reading
    /// through `manifest_cache` first, so a manifest write or delete is
    /// visible to the next `table()` call with no cache to invalidate. The
    /// listing snapshots are the only stale-listing risk.
    pub fn invalidate_table_names(&self, database: &str) {
        self.listings.tables.invalidate(database);
        *self
            .listings
            .databases
            .write()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// True when `sql` reads `information_schema`, whose `schemata` / `tables` /
/// `columns` views enumerate every schema of every catalog and so need every
/// database's table snapshot, not just the referenced ones.
fn sql_reads_information_schema(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("information_schema")
}

impl CatalogProvider for HiveCatalogProvider {
    // Synchronous and without an error channel, so it serves the last known
    // database snapshot and never touches the metastore (review 806); the
    // snapshot is refreshed by `refresh_referenced` on the async pre-plan
    // path. A stale snapshot is served with a warning, an absent one as an
    // empty list with a debug line, so a listing outage cannot masquerade
    // as an empty catalog in the logs even though DataFusion sees a list.
    fn schema_names(&self) -> Vec<String> {
        match self.listings.databases() {
            Some(listing) => {
                if let Some(err) = &listing.stale_error {
                    warn!(
                        catalog = %self.name,
                        error = %err,
                        denied = listing.denied,
                        age_secs = listing.checked_at.elapsed().as_secs(),
                        known = listing.names.len(),
                        "hive_external: serving last known database list; metastore listing is stale"
                    );
                }
                listing.names.clone()
            }
            None => {
                debug!(
                    catalog = %self.name,
                    "hive_external: no database snapshot yet; refresh_referenced has not run for this catalog"
                );
                Vec::new()
            }
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Always lazy, never a live existence check: a database directory
        // with no tables yet is indistinguishable from one that does not
        // exist at all until a specific `table()` lookup resolves (or
        // fails to), the same way a bare directory is not a namespace until
        // something is written under it. `table_names()`/`table()` on the
        // returned provider answer that lazily, same as Hive's own
        // directory-based emptiness.
        Some(Arc::new(HiveSchemaProvider {
            database: name.to_string(),
            source: Arc::clone(&self.source),
            data_store_factory: Arc::clone(&self.data_store_factory),
            runtime: Arc::clone(&self.runtime),
            cache: Arc::clone(&self.manifest_cache),
            listings: Arc::clone(&self.listings),
        }))
    }
}

/// DataFusion `SchemaProvider` over one database directory in the Hive
/// manifest store.
pub struct HiveSchemaProvider {
    database: String,
    source: Arc<dyn HiveMetadataSource>,
    data_store_factory: Arc<dyn HiveDataStoreFactory>,
    runtime: Arc<RuntimeEnv>,
    cache: Arc<HiveMetadataCache>,
    listings: Arc<HiveListings>,
}

impl fmt::Debug for HiveSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveSchemaProvider")
            .field("database", &self.database)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for HiveSchemaProvider {
    // DataFusion's `SchemaProvider::table_names()` is synchronous while the
    // manifest store is async, exactly the constraint `SqeSchemaProvider`
    // documents in `schema_provider.rs`. Same answer as there (review 806):
    // serve the database's last known snapshot and never touch the network;
    // `HiveCatalogProvider::refresh_referenced` fills it on the async
    // pre-plan path. A stale snapshot is served with a warning.
    fn table_names(&self) -> Vec<String> {
        match self.listings.tables(&self.database) {
            Some(listing) => {
                if let Some(err) = &listing.stale_error {
                    warn!(
                        database = %self.database,
                        error = %err,
                        denied = listing.denied,
                        age_secs = listing.checked_at.elapsed().as_secs(),
                        known = listing.names.len(),
                        "hive_external: serving last known table list; metastore listing is stale"
                    );
                }
                listing.names.clone()
            }
            None => {
                debug!(
                    database = %self.database,
                    "hive_external: no table-name snapshot yet; refresh_referenced has not run for this database"
                );
                Vec::new()
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        match self.listings.tables(&self.database) {
            Some(listing) if listing.stale_error.is_none() => {
                listing.names.iter().any(|n| n == name)
            }
            // No usable snapshot must not read as "table does not exist": a
            // `DROP TABLE IF EXISTS` would silently no-op. Unknown is
            // answered as present so the caller reaches `table()`, which asks
            // the source and reports the real answer or the real error.
            _ => true,
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let etag = match self.source.etag(&self.database, name).await {
            Ok(e) => e,
            Err(HiveError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(to_df(e)),
        };
        let manifest = if let Some(cached) =
            self.cache
                .get_if_fresh(&self.database, name, etag.as_deref())
        {
            cached
        } else {
            match self.source.get(&self.database, name).await {
                Ok(m) => m,
                Err(HiveError::NotFound(_)) => return Ok(None),
                Err(e) => return Err(to_df(e)),
            }
        };

        // Cache BEFORE building the table. The policy layer's `TagSource` is
        // synchronous and can only answer from this cache, and plan
        // rewriting runs after the `LogicalPlan` is built, which is after
        // this call returns. If the insert moved below the table build and
        // the build failed partway, a later retry could rewrite against an
        // empty cache and deny rather than mask.
        //
        // `a_failed_table_build_still_leaves_the_manifest_cached` (see
        // hive_provider_test.rs) pins this ordering: moving this insert to
        // just before the final `Ok` return, as a mutation check, makes that
        // test fail while leaving every happy-path test (including
        // `loading_a_table_populates_the_metadata_cache_before_returning`)
        // green, which is exactly why the happy-path test alone was not
        // enough.
        self.cache
            .insert_versioned(&self.database, name, manifest.clone(), etag);

        // A table this release cannot read is invisible, not fatal. Glue
        // holds formats SQE has no reader for (ORC, Avro) and types it
        // cannot map, and `information_schema.tables` calls this for every
        // table in the catalog: erroring here means one unreadable table
        // out of 692 takes down the whole listing, which is the same
        // enumerate-and-fail shape that makes the Iceberg Glue provider
        // useless against a Hive catalog. The reason is logged rather than
        // discarded, and a direct `SELECT` on such a table reports it as
        // not found, matching how a denied column is absent rather than an
        // error.
        let unreadable = |what: &str, e: HiveError| {
            warn!(
                database = %self.database, table = %name,
                error = %e,
                "hive_external: table hidden from this catalog: its {what} is not readable by \
                 this release"
            );
        };
        let mut resolved = match resolve_format(&manifest) {
            Ok(r) => r,
            Err(e) => {
                unreadable("storage format", e);
                return Ok(None);
            }
        };
        let file_schema = match file_schema_of(&manifest) {
            Ok(s) => s,
            Err(e) => {
                unreadable("column schema", e);
                return Ok(None);
            }
        };
        let partition_cols = match partition_columns_of(&manifest) {
            Ok(p) => p,
            Err(e) => {
                unreadable("partition schema", e);
                return Ok(None);
            }
        };
        let partition_names: Vec<String> = partition_cols.iter().map(|(n, _)| n.clone()).collect();

        let url = table_location_url(&manifest.storage_descriptor.location).map_err(to_df)?;

        let data_store = self
            .data_store_factory
            .store_for(&manifest.storage_descriptor.location, manifest.secret())
            .map_err(to_df)?;

        // Refuse a credential collision BEFORE registering: DataFusion's
        // `ObjectStoreRegistry` keys purely on `scheme://host:port`
        // (`object_store_bucket_key`, matching what `register_object_store`
        // itself keys on), discarding the path, so two Hive tables in this
        // same bucket under different `sqe.secret` credentials would
        // otherwise silently collide on this single key -- whichever is
        // scanned last in this process wins the registration for every
        // table in the bucket. See `HiveMetadataCache::claim_object_store_bucket`'s
        // own doc comment for the full case and its accepted gap.
        let bucket_key = object_store_bucket_key(url.as_ref());
        let table_ref = format!("{}.{name}", self.database);
        self.cache
            .claim_object_store_bucket(&bucket_key, &table_ref, manifest.secret())
            .map_err(HiveError::Forbidden)
            .map_err(to_df)?;

        // The session's own runtime, not a fresh one: `ListingTable`
        // resolves its object store from the session's registry at scan
        // time, so the store has to live where the query will look for it.
        // Compression has to come from a real object key: Glue records
        // nothing usable (`compressionType` is absent on most tables and
        // says "none" on every table carrying it, gzipped ones included).
        // One listing per manifest-cache miss, and only for the text
        // formats, where guessing wrong feeds gzip bytes to a parser and
        // fails as `Encountered unexpected ''` rather than as a codec
        // problem.
        if matches!(
            resolved.kind,
            super::serde_map::HiveFileFormat::Json | super::serde_map::HiveFileFormat::Csv
        ) {
            resolved.compression =
                detect_compression(&data_store, url.prefix(), resolved.compression).await;
        }
        let projection_cfg =
            parse_projection(&manifest.parameters, &manifest.partition_keys).map_err(to_df)?;
        let index = manifest.sqe.partition_index.clone();
        let partition_types: Vec<(String, String)> = manifest
            .partition_keys
            .iter()
            .map(|k| (k.name.clone(), k.r#type.clone()))
            .collect();

        // The one remaining await of this method runs BEFORE the
        // `ListingTable` exists (compile time, see
        // docs/internal/plans/2026-09-07-compile-time-async-hubs.md): its
        // `ListingOptions` carry `Expr`s (`file_sort_order`), and a coroutine
        // that holds them across an await pays a `Send` proof over
        // DataFusion's whole expression graph. Listing the files first keeps
        // every later step synchronous.
        let needs_listing = manifest.hive_statistics().is_none()
            && index.is_none()
            && !projection_cfg.as_ref().map(|p| p.enabled).unwrap_or(false);
        let listed_files: Option<Vec<ObjectMeta>> = if needs_listing {
            match data_store
                .list(Some(url.prefix()))
                .try_collect::<Vec<ObjectMeta>>()
                .await
            {
                Ok(files) => Some(files),
                Err(e) => {
                    warn!(
                        database = %self.database, table = %name, error = %e,
                        "hive_external: could not list files for size-only statistics; \
                         table remains queryable with unknown statistics"
                    );
                    None
                }
            }
        } else {
            None
        };

        let options = listing_options(&resolved, partition_cols).map_err(to_df)?;

        let file_schema_ref = Arc::new(file_schema);
        let width_hint = row_width_hint(&file_schema_ref);
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(options)
            .with_schema(Arc::clone(&file_schema_ref));
        let listing_table = ListingTable::try_new(config)?;
        let full_schema = listing_table.schema();

        let statistics = if let Some(hs) = manifest.hive_statistics() {
            let bytes = index
                .as_ref()
                .map(|i| i.entries.iter().map(|e| e.total_bytes).sum());
            analyzed_statistics(hs.row_count, bytes, &full_schema)
        } else if let Some(idx) = &index {
            let bytes: u64 = idx.entries.iter().map(|e| e.total_bytes).sum();
            size_only_from_index(bytes, width_hint, &full_schema)
        } else {
            match listed_files {
                Some(files) => size_only_statistics(&files, width_hint, &full_schema),
                None => Statistics::new_unknown(&full_schema),
            }
        };

        Ok(Some(Arc::new(HiveTableProvider::new(
            listing_table,
            statistics,
            partition_names,
            partition_types,
            index,
            projection_cfg,
            resolved,
            file_schema_ref,
            Arc::clone(&self.data_store_factory),
            manifest.secret().map(str::to_string),
            Arc::clone(&self.runtime),
            self.source.limits().max_partition_index_entries,
            manifest.storage_descriptor.location.clone(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    #[test]
    fn location_without_trailing_slash_is_normalized_to_a_directory() {
        let url = table_location_url("file:///tmp/x/data").expect("parse");
        assert!(url.is_collection(), "must be treated as a directory");
    }

    #[test]
    fn location_with_trailing_slash_is_left_alone() {
        let url = table_location_url("file:///tmp/x/data/").expect("parse");
        assert!(url.is_collection());
    }

    #[test]
    fn is_null_on_a_partition_column_becomes_a_sentinel_comparison() {
        let filters = vec![col("dt").is_null()];
        let out = normalize_default_partition_filters(&filters, &["dt".to_string()]).unwrap();
        assert_eq!(out, vec![col("dt").eq(lit(HIVE_DEFAULT_PARTITION))]);
    }

    #[test]
    fn is_not_null_on_a_partition_column_becomes_a_sentinel_inequality() {
        let filters = vec![col("dt").is_not_null()];
        let out = normalize_default_partition_filters(&filters, &["dt".to_string()]).unwrap();
        assert_eq!(out, vec![col("dt").not_eq(lit(HIVE_DEFAULT_PARTITION))]);
    }

    #[test]
    fn is_null_on_a_non_partition_column_is_untouched() {
        let filters = vec![col("amount").is_null()];
        let out = normalize_default_partition_filters(&filters, &["dt".to_string()]).unwrap();
        assert_eq!(out, filters);
    }

    #[test]
    fn is_null_nested_inside_another_expression_is_still_rewritten() {
        let filters = vec![col("dt").is_null().or(col("amount").gt(lit(5.0)))];
        let out = normalize_default_partition_filters(&filters, &["dt".to_string()]).unwrap();
        let expected = col("dt")
            .eq(lit(HIVE_DEFAULT_PARTITION))
            .or(col("amount").gt(lit(5.0)));
        assert_eq!(out, vec![expected]);
    }

    #[test]
    fn manifest_cache_round_trips_by_database_and_table() {
        let cache = HiveMetadataCache::new();
        assert!(cache.get("sales", "orders").is_none());
        let m = HiveManifest {
            manifest_version: super::super::manifest::MANIFEST_VERSION,
            table_type: "EXTERNAL_TABLE".to_string(),
            database_name: "sales".to_string(),
            name: "orders".to_string(),
            owner: None,
            created_at: None,
            description: None,
            partition_keys: Vec::new(),
            parameters: Default::default(),
            storage_descriptor: super::super::manifest::StorageDescriptor {
                location: "file:///tmp/x".to_string(),
                input_format: None,
                output_format: None,
                compressed: false,
                serde_info: Default::default(),
                columns: Vec::new(),
                sort_columns: Vec::new(),
            },
            sqe: Default::default(),
        };
        cache.insert("sales", "orders", m.clone());
        assert_eq!(cache.get("sales", "orders").unwrap().name, m.name);
        assert!(cache.get("sales", "other").is_none());
        assert!(cache.get("other", "orders").is_none());
    }

    #[test]
    fn claim_object_store_bucket_allows_a_different_table_under_the_same_secret() {
        let cache = HiveMetadataCache::new();
        cache
            .claim_object_store_bucket("s3://bucket", "sales.orders", Some("s1"))
            .expect("first claim");
        cache
            .claim_object_store_bucket("s3://bucket", "sales.shipments", Some("s1"))
            .expect("same secret, different table, must not be refused");
    }

    #[test]
    fn claim_object_store_bucket_refuses_a_different_table_under_a_different_secret() {
        let cache = HiveMetadataCache::new();
        cache
            .claim_object_store_bucket("s3://bucket", "sales.orders", Some("s1"))
            .expect("first claim");
        let err = cache
            .claim_object_store_bucket("s3://bucket", "sales.shipments", Some("s2"))
            .expect_err("different table, different secret, must be refused");
        assert!(err.contains("sales.orders"), "{err}");
        assert!(err.contains("sales.shipments"), "{err}");
        assert!(err.contains("s1"), "{err}");
        assert!(err.contains("s2"), "{err}");
    }

    /// The sequence the review's fix-round found: a same-table re-claim
    /// under a NEW secret must update the stored record, not leave it
    /// stale. Pins both symptoms the review traced from that staleness.
    ///
    /// Mutation-checked: reverting the fix (returning early on a
    /// same-table match without updating the record, the function's exact
    /// shape before this fix) makes this test fail -- in practice on the
    /// "B under A's current secret must be allowed" assertion first (the
    /// premature-refusal half surfaces before the wrongly-allowed-old-
    /// secret half gets a chance to run), which is itself proof the
    /// record went stale on the first re-claim.
    #[test]
    fn a_same_table_reclaim_under_a_new_secret_updates_the_record() {
        let cache = HiveMetadataCache::new();

        // A claims under s1.
        cache
            .claim_object_store_bucket("s3://bucket", "sales.a", Some("s1"))
            .expect("A's first claim");

        // A's manifest changes to s2 and re-claims. Must succeed (same
        // table), and must actually update the record -- not a no-op.
        cache
            .claim_object_store_bucket("s3://bucket", "sales.a", Some("s2"))
            .expect("A re-claiming under a new secret must not be refused");

        // A legitimate table B arriving under A's NEW secret (s2) must not
        // be refused because of a stale record still showing s1 (the
        // "premature refusal" half of the bug).
        cache
            .claim_object_store_bucket("s3://bucket", "sales.b_same_secret", Some("s2"))
            .expect("B under A's current secret must be allowed");

        // Reset to a clean two-claimant state to isolate the other half:
        // table B arriving under A's OLD secret (s1) must now be REFUSED,
        // because what is actually registered for this bucket is s2's
        // store, not s1's. A stale record showing (A, s1) would wrongly
        // allow this and let B's s1 store silently replace A's s2 store.
        let cache2 = HiveMetadataCache::new();
        cache2
            .claim_object_store_bucket("s3://bucket", "sales.a", Some("s1"))
            .expect("A's first claim");
        cache2
            .claim_object_store_bucket("s3://bucket", "sales.a", Some("s2"))
            .expect("A re-claiming under a new secret must not be refused");
        let err = cache2
            .claim_object_store_bucket("s3://bucket", "sales.b_old_secret", Some("s1"))
            .expect_err(
                "B claiming under A's OLD, no-longer-registered secret must be refused, \
                 not silently allowed to overwrite A's current store",
            );
        assert!(err.contains("sales.a"), "{err}");
        assert!(err.contains("sales.b_old_secret"), "{err}");
    }
}

/// The listing snapshots behind `schema_names()` / `table_names()` (review
/// 806): what the synchronous hooks serve before, after and across a failed
/// refresh, and which databases `refresh_referenced` lists for a statement.
#[cfg(test)]
mod listing_tests {
    use super::*;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    /// A metastore whose listings are canned and whose failure is a switch,
    /// recording every `list_tables` call so a test can pin what a statement
    /// caused to be listed.
    struct ScriptedSource {
        databases: Vec<String>,
        tables: HashMap<String, Vec<String>>,
        fail_with: Mutex<Option<HiveError>>,
        list_tables_calls: Mutex<Vec<String>>,
        limits: super::super::manifest::ManifestLimits,
    }

    impl ScriptedSource {
        fn new(tables: &[(&str, &[&str])]) -> Arc<Self> {
            let mut databases: Vec<String> = tables.iter().map(|(d, _)| d.to_string()).collect();
            databases.sort();
            Arc::new(Self {
                databases,
                tables: tables
                    .iter()
                    .map(|(d, t)| (d.to_string(), t.iter().map(|s| s.to_string()).collect()))
                    .collect(),
                fail_with: Mutex::new(None),
                list_tables_calls: Mutex::new(Vec::new()),
                limits: Default::default(),
            })
        }

        fn fail_with(&self, e: Option<HiveError>) {
            *self.fail_with.lock().unwrap() = e;
        }

        fn calls(&self) -> Vec<String> {
            self.list_tables_calls.lock().unwrap().clone()
        }

        fn failure(&self) -> Result<(), HiveError> {
            match &*self.fail_with.lock().unwrap() {
                Some(HiveError::Store(m)) => Err(HiveError::Store(m.clone())),
                Some(HiveError::Forbidden(m)) => Err(HiveError::Forbidden(m.clone())),
                Some(other) => Err(HiveError::Validation(other.to_string())),
                None => Ok(()),
            }
        }
    }

    #[async_trait]
    impl HiveMetadataSource for ScriptedSource {
        async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
            self.failure()?;
            Ok(self.databases.clone())
        }

        async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
            self.list_tables_calls.lock().unwrap().push(database.to_string());
            self.failure()?;
            Ok(self.tables.get(database).cloned().unwrap_or_default())
        }

        async fn get(&self, _database: &str, table: &str) -> Result<HiveManifest, HiveError> {
            Err(HiveError::NotFound(table.to_string()))
        }

        async fn etag(&self, _database: &str, _table: &str) -> Result<Option<String>, HiveError> {
            Ok(None)
        }

        fn limits(&self) -> &super::super::manifest::ManifestLimits {
            &self.limits
        }
    }

    #[derive(Debug)]
    struct UnusedFactory;

    impl HiveDataStoreFactory for UnusedFactory {
        fn store_for(
            &self,
            _location: &str,
            _secret: Option<&str>,
        ) -> Result<Arc<dyn ObjectStore>, HiveError> {
            unreachable!("listing tests never scan")
        }
    }

    fn provider(source: Arc<ScriptedSource>, ttl: Duration) -> HiveCatalogProvider {
        HiveCatalogProvider::with_source(
            "lake".to_string(),
            source,
            Arc::new(UnusedFactory),
            RuntimeEnvBuilder::new().build_arc().expect("runtime"),
            Arc::new(HiveMetadataCache::new()),
            ttl,
        )
    }

    #[tokio::test]
    async fn sync_hooks_serve_nothing_until_a_refresh_runs_and_unknown_is_not_absent() {
        let source = ScriptedSource::new(&[("sales", &["orders"])]);
        let p = provider(source, Duration::from_secs(60));

        assert!(p.schema_names().is_empty(), "no snapshot yet: no databases");
        let schema = p.schema("sales").expect("schemas are lazy");
        assert!(schema.table_names().is_empty(), "no snapshot yet: no tables");
        assert!(
            schema.table_exist("orders") && schema.table_exist("nope"),
            "without a snapshot existence is unknown, and unknown must not read as absent"
        );

        p.refresh_all().await;
        assert_eq!(p.schema_names(), vec!["sales".to_string()]);
        assert_eq!(schema.table_names(), vec!["orders".to_string()]);
        assert!(schema.table_exist("orders"));
        assert!(!schema.table_exist("nope"), "a fresh snapshot answers absence for real");
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_previous_names_and_marks_the_snapshot_stale() {
        let source = ScriptedSource::new(&[("sales", &["orders"])]);
        let p = provider(Arc::clone(&source), Duration::from_secs(60));
        p.refresh_all().await;

        source.fail_with(Some(HiveError::Store("bucket unreachable".into())));
        p.refresh_tables("sales").await.expect_err("the failure must reach the async caller");
        p.refresh_databases().await.expect_err("the failure must reach the async caller");

        let schema = p.schema("sales").expect("schema");
        assert_eq!(
            schema.table_names(),
            vec!["orders".to_string()],
            "the last known names are served, not an empty list"
        );
        assert_eq!(p.schema_names(), vec!["sales".to_string()]);
        let tables = p.listings().tables("sales").expect("snapshot");
        assert!(tables.unavailable(), "an outage is recorded as unavailable, not denied");
        assert!(tables.stale_error.as_deref().is_some_and(|e| e.contains("bucket unreachable")));
        assert!(
            schema.table_exist("nope"),
            "a stale snapshot cannot vouch for absence; unknown reads as present"
        );

        source.fail_with(Some(HiveError::Forbidden("not yours".into())));
        let _ = p.refresh_tables("sales").await;
        let tables = p.listings().tables("sales").expect("snapshot");
        assert!(tables.denied && !tables.unavailable(), "a denial is a legitimate answer");
    }

    #[tokio::test]
    async fn refresh_referenced_lists_only_this_catalogs_referenced_databases() {
        let source = ScriptedSource::new(&[("sales", &["orders"]), ("hr", &["people"])]);
        let p = provider(Arc::clone(&source), Duration::from_secs(60));

        // `lake.sales.orders` is ours; `other.x.y` belongs to another
        // catalog and `bare.t` resolves against the default catalog, which
        // is not this root.
        p.refresh_referenced(
            "SELECT * FROM lake.sales.orders o JOIN other.x.y ON true JOIN bare.t ON true",
            Some("main_warehouse"),
            Some("gold"),
        )
        .await;
        assert_eq!(source.calls(), vec!["sales".to_string()]);
        assert_eq!(p.schema_names(), vec!["hr".to_string(), "sales".to_string()]);
        assert!(p.listings().tables("hr").is_none(), "hr was not referenced");

        // Within the TTL the same statement costs nothing.
        p.refresh_referenced("SELECT * FROM lake.sales.orders", Some("main_warehouse"), None)
            .await;
        assert_eq!(source.calls().len(), 1);
    }

    #[tokio::test]
    async fn two_part_names_and_the_session_schema_count_when_this_root_is_the_default_catalog() {
        let source = ScriptedSource::new(&[("sales", &["orders"]), ("hr", &["people"])]);
        let p = provider(Arc::clone(&source), Duration::from_secs(60));

        p.refresh_referenced("SELECT * FROM sales.orders", Some("LAKE"), Some("hr"))
            .await;
        let mut calls = source.calls();
        calls.sort();
        assert_eq!(calls, vec!["hr".to_string(), "sales".to_string()]);
    }

    #[tokio::test]
    async fn an_information_schema_read_refreshes_every_database() {
        let source = ScriptedSource::new(&[("sales", &["orders"]), ("hr", &["people"])]);
        let p = provider(Arc::clone(&source), Duration::from_secs(60));

        p.refresh_referenced(
            "SELECT table_name FROM lake.information_schema.tables",
            Some("main_warehouse"),
            None,
        )
        .await;
        let mut calls = source.calls();
        calls.sort();
        assert_eq!(calls, vec!["hr".to_string(), "sales".to_string()]);
    }

    #[tokio::test]
    async fn invalidation_and_ttl_expiry_force_a_relist() {
        let source = ScriptedSource::new(&[("sales", &["orders"])]);
        let p = provider(Arc::clone(&source), Duration::from_millis(0));
        p.refresh_referenced("SELECT * FROM lake.sales.orders", None, None).await;
        assert_eq!(source.calls().len(), 1);

        // A zero TTL means every pre-plan refresh re-lists.
        p.refresh_referenced("SELECT * FROM lake.sales.orders", None, None).await;
        assert_eq!(source.calls().len(), 2);

        // DDL invalidation drops both the table and the database snapshot so
        // the next refresh cannot serve a stale answer even within the TTL.
        let p = provider(Arc::clone(&source), Duration::from_secs(60));
        p.refresh_all().await;
        p.invalidate_table_names("sales");
        assert!(p.listings().tables("sales").is_none());
        assert!(p.listings().databases().is_none());
        assert!(p.schema_names().is_empty());
    }
}

//! `CatalogProvider`/`SchemaProvider` over the Hive manifest, and the
//! `TableProvider` wrapper that gives a `ListingTable` size-derived
//! statistics and Hive's NULL-partition semantics.
//!
//! # Two lifetimes, deliberately different
//!
//! [`HiveCatalogProvider`] is built PER SESSION: a Hive read must run as the
//! calling user, and [`SchemaProvider::table`] takes no session argument, so
//! the credentials (carried by `data_store_factory`) have to be baked into
//! the provider instance. `runtime` is that session's `RuntimeEnv` for the
//! same reason `ListingTable` resolves its object store from the session's
//! registry at scan time; registering on a fresh runtime would produce a
//! scan that cannot find its bucket.
//!
//! [`HiveMetadataCache`] is the opposite: ONE process-wide instance, injected
//! into every per-session provider. It has to be shared because the policy
//! layer's `TagSource` is constructed once at coordinator startup and is
//! synchronous, so it can only read a cache that outlives any one session.
//! Sharing is safe: a manifest's content does not depend on who read it, and
//! reading one grants no data access; the mask decision is still made per
//! user.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use futures::TryStreamExt;
use moka::sync::Cache as SyncCache;
use object_store::{ObjectMeta, ObjectStore};
use tracing::{error, warn};

use super::manifest::HiveManifest;
use super::serde_map::{arrow_type, listing_options, resolve_format};
use super::stats::{row_width_hint, size_only_statistics};
use super::store::HiveManifestStore;
use super::HiveError;

/// TTL-independent capacity bound for a schema's `table_names()` cache and a
/// catalog's `schema()`-issued caches (they share one moka instance; see
/// [`HiveCatalogProvider::new`]). Generous: a warehouse with more than this
/// many distinct databases across one session is not the case this cache is
/// sized for, and a miss just costs one more listing.
const TABLE_NAMES_CACHE_CAPACITY: u64 = 1024;

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
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

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
pub struct HiveMetadataCache {
    inner: SyncCache<String, HiveManifest>,
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
        Self {
            inner: SyncCache::builder()
                .max_capacity(MANIFEST_CACHE_CAPACITY)
                .build(),
        }
    }

    fn key(database: &str, table: &str) -> String {
        format!("{database}.{table}")
    }

    pub fn get(&self, database: &str, table: &str) -> Option<HiveManifest> {
        self.inner.get(&Self::key(database, table))
    }

    pub fn insert(&self, database: &str, table: &str, m: HiveManifest) {
        self.inner.insert(Self::key(database, table), m);
    }
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
fn table_location_url(location: &str) -> Result<ListingTableUrl, HiveError> {
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
#[derive(Debug)]
struct HiveTableProvider {
    inner: Arc<ListingTable>,
    schema: SchemaRef,
    statistics: Statistics,
    partition_columns: Vec<String>,
}

impl HiveTableProvider {
    fn new(inner: ListingTable, statistics: Statistics, partition_columns: Vec<String>) -> Self {
        let schema = nullable_partition_schema(&inner.schema(), &partition_columns);
        Self {
            inner: Arc::new(inner),
            schema,
            statistics,
            partition_columns,
        }
    }
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let rewritten = normalize_default_partition_filters(filters, &self.partition_columns)?;
        let plan = self
            .inner
            .scan(state, projection, &rewritten, limit)
            .await?;
        nullify_default_partition_columns(plan, &self.partition_columns)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        let rewritten = match args.filters() {
            Some(f) => Some(normalize_default_partition_filters(
                f,
                &self.partition_columns,
            )?),
            None => None,
        };
        let mut new_args = ScanArgs::default()
            .with_projection(args.projection())
            .with_limit(args.limit());
        new_args = new_args.with_filters(rewritten.as_deref());
        let result = self.inner.scan_with_args(state, new_args).await?;
        let plan = nullify_default_partition_columns(result.into_inner(), &self.partition_columns)?;
        Ok(ScanResult::new(plan))
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

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

/// DataFusion `CatalogProvider` over the Hive manifest store. Built PER
/// SESSION; see the module doc comment for why.
pub struct HiveCatalogProvider {
    name: String,
    store: Arc<HiveManifestStore>,
    data_store_factory: Arc<dyn HiveDataStoreFactory>,
    runtime: Arc<RuntimeEnv>,
    manifest_cache: Arc<HiveMetadataCache>,
    /// `"<database>"` -> table names, shared with every `HiveSchemaProvider`
    /// this catalog issues via `schema()` (`moka::sync::Cache` clones cheaply
    /// and share their backing map), so a listing done for one query's
    /// planning is not repeated for the next one within `cache_ttl`.
    table_names_cache: SyncCache<String, Arc<Vec<String>>>,
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
        let table_names_cache = SyncCache::builder()
            .max_capacity(TABLE_NAMES_CACHE_CAPACITY)
            .time_to_live(cache_ttl)
            .build();
        Self {
            name,
            store,
            data_store_factory,
            runtime,
            manifest_cache,
            table_names_cache,
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
    pub fn store(&self) -> Arc<HiveManifestStore> {
        Arc::clone(&self.store)
    }

    /// Drop any cached `table_names()` result for `database`. Call after DDL
    /// that creates or drops a table so the next `SHOW TABLES` / planning
    /// lookup sees the change without waiting for the cache TTL. Mirrors
    /// `SqeSchemaProvider::invalidate_table_names` (`schema_provider.rs`).
    ///
    /// `HiveSchemaProvider::table()` itself needs no matching invalidation:
    /// it calls `store.get()` unconditionally on every lookup (see its own
    /// doc comment on cache-before-build ordering) rather than reading
    /// through `manifest_cache` first, so a manifest write or delete is
    /// visible to the next `table()` call with no cache to invalidate.
    /// `table_names_cache` is the only stale-listing risk.
    pub fn invalidate_table_names(&self, database: &str) {
        self.table_names_cache.invalidate(database);
    }
}

impl CatalogProvider for HiveCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let store = Arc::clone(&self.store);
        match crate::runtime_bridge::block_on_compat(async move { store.list_databases().await }) {
            Ok(Ok(names)) => names,
            Ok(Err(e)) => {
                error!(catalog = %self.name, error = %e, "hive_external: failed to list databases");
                Vec::new()
            }
            Err(bridge) => {
                error!(catalog = %self.name, error = %bridge, "hive_external: could not list databases");
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
            store: Arc::clone(&self.store),
            data_store_factory: Arc::clone(&self.data_store_factory),
            runtime: Arc::clone(&self.runtime),
            cache: Arc::clone(&self.manifest_cache),
            table_names_cache: self.table_names_cache.clone(),
        }))
    }
}

/// DataFusion `SchemaProvider` over one database directory in the Hive
/// manifest store.
pub struct HiveSchemaProvider {
    database: String,
    store: Arc<HiveManifestStore>,
    data_store_factory: Arc<dyn HiveDataStoreFactory>,
    runtime: Arc<RuntimeEnv>,
    cache: Arc<HiveMetadataCache>,
    table_names_cache: SyncCache<String, Arc<Vec<String>>>,
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
    // documents at `schema_provider.rs:184`. This uses the identical bridge
    // (`crate::runtime_bridge::block_on_compat`, itself `block_in_place` +
    // `Handle::block_on` on a multi-thread runtime, a dedicated thread with a
    // hard deadline on a current-thread one) rather than a second style.
    fn table_names(&self) -> Vec<String> {
        if let Some(cached) = self.table_names_cache.get(&self.database) {
            return (*cached).clone();
        }

        let store = Arc::clone(&self.store);
        let database = self.database.clone();
        let names = match crate::runtime_bridge::block_on_compat(async move {
            store.list_tables(&database).await
        }) {
            Ok(Ok(names)) => names,
            Ok(Err(e)) => {
                error!(database = %self.database, error = %e, "hive_external: failed to list tables");
                Vec::new()
            }
            Err(bridge) => {
                error!(database = %self.database, error = %bridge, "hive_external: could not list tables");
                Vec::new()
            }
        };

        self.table_names_cache
            .insert(self.database.clone(), Arc::new(names.clone()));
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|n| n == name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let manifest = match self.store.get(&self.database, name).await {
            Ok(m) => m,
            Err(HiveError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(to_df(e)),
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
        self.cache.insert(&self.database, name, manifest.clone());

        let resolved = resolve_format(&manifest).map_err(to_df)?;
        let file_schema = file_schema_of(&manifest).map_err(to_df)?;
        let partition_cols = partition_columns_of(&manifest).map_err(to_df)?;
        let partition_names: Vec<String> = partition_cols.iter().map(|(n, _)| n.clone()).collect();
        let options = listing_options(&resolved, partition_cols).map_err(to_df)?;

        let url = table_location_url(&manifest.storage_descriptor.location).map_err(to_df)?;

        let data_store = self
            .data_store_factory
            .store_for(&manifest.storage_descriptor.location, manifest.secret())
            .map_err(to_df)?;

        // The session's own runtime, not a fresh one: `ListingTable`
        // resolves its object store from the session's registry at scan
        // time, so the store has to live where the query will look for it.
        self.runtime
            .register_object_store(url.as_ref(), Arc::clone(&data_store));

        let width_hint = row_width_hint(&file_schema);
        let config = ListingTableConfig::new(url.clone())
            .with_listing_options(options)
            .with_schema(Arc::new(file_schema));
        let listing_table = ListingTable::try_new(config)?;
        let full_schema = listing_table.schema();

        let statistics = match data_store
            .list(Some(url.prefix()))
            .try_collect::<Vec<ObjectMeta>>()
            .await
        {
            Ok(files) => size_only_statistics(&files, width_hint, &full_schema),
            Err(e) => {
                warn!(
                    database = %self.database, table = %name, error = %e,
                    "hive_external: could not list files for size-only statistics; \
                     table remains queryable with unknown statistics"
                );
                Statistics::new_unknown(&full_schema)
            }
        };

        Ok(Some(Arc::new(HiveTableProvider::new(
            listing_table,
            statistics,
            partition_names,
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
}

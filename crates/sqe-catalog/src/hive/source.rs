//! Where a Hive external table's metadata comes from.
//!
//! The read path needs five things from a metastore, and no more: list the
//! databases, list a database's tables, fetch one table's [`HiveManifest`],
//! answer whether a cached manifest is still current, and report the caps a
//! manifest must satisfy. [`HiveManifestStore`] answers all five out of
//! `_sqe/table.json` sidecars in object storage, which is the right shape
//! for a warehouse that has no metastore of its own.
//!
//! It is the wrong shape when a metastore already exists. Against AWS Glue,
//! sidecars mean maintaining a second copy of a catalog someone else owns,
//! and a copy of a catalog is a cache with no invalidation: a snapshot table
//! that rotates its directory nightly reads correctly one day and returns
//! zero rows the next, because the copied `Location` names a prefix that has
//! since been deleted. Zero rows is also what a legitimately empty table
//! looks like, so the failure is silent.
//!
//! This trait is the seam that lets the same provider read either. It is
//! deliberately read-only: `put` and `delete` stay on the concrete
//! [`HiveManifestStore`], because SQE owns a sidecar warehouse and does not
//! own Glue.
//!
//! ## Implementations must resolve lazily
//!
//! Never enumerate a namespace and load every table in it. That is exactly
//! what `IcebergSchemaProvider::try_new` does upstream, and it is why
//! `--catalog-backend glue` fails the entire attach when one table in a
//! 692-table catalog cannot be loaded. A real catalog always contains a
//! table this engine cannot read; the rest must stay queryable.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::manifest::{HiveManifest, ManifestLimits};
use super::store::HiveManifestStore;
use super::HiveError;

/// The read surface a [`super::provider::HiveCatalogProvider`] needs from a
/// Hive metastore.
#[async_trait]
pub trait HiveMetadataSource: Send + Sync {
    /// Databases visible to this source. Ordering is not significant.
    async fn list_databases(&self) -> Result<Vec<String>, HiveError>;

    /// Table names in `database`. An unknown database is an empty list
    /// rather than an error: DataFusion asks about schemas it has merely
    /// seen named, and a hard error there fails the whole query.
    async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError>;

    /// Every manifest in `database` in one round trip, for sources whose
    /// listing call already returns full table definitions (Glue's
    /// `GetTables` does; a sidecar store's directory listing does not).
    ///
    /// `Ok(None)` means "no cheaper than one `get` per table", which is the
    /// default. A `Some` answer lets the catalog warm the manifest cache
    /// from a single listing, so `SHOW TABLES`, `information_schema` and
    /// the first `SELECT` after them pay one paginated call per database
    /// instead of one fetch per table (issue #666: 618 `GetTable` calls
    /// behind one `SHOW TABLES`). Same not-found contract as
    /// [`Self::list_tables`]: an unknown database is `Some(empty)`.
    async fn list_manifests(
        &self,
        database: &str,
    ) -> Result<Option<Vec<HiveManifest>>, HiveError> {
        let _ = database;
        Ok(None)
    }

    /// One table's manifest. [`HiveError::NotFound`] when it does not exist.
    async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError>;

    /// A cheap token that changes when the table's metadata changes, used to
    /// revalidate a cached manifest without re-fetching it.
    ///
    /// `Ok(None)` means "cannot tell cheaply", which callers must treat as
    /// "assume stale". A source whose only read is the full fetch should
    /// return `None` and lean on the cache TTL rather than pay for the
    /// fetch twice.
    async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError>;

    /// Caps every manifest from this source must satisfy.
    fn limits(&self) -> &ManifestLimits;
}

#[async_trait]
impl HiveMetadataSource for HiveManifestStore {
    async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
        HiveManifestStore::list_databases(self).await
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
        HiveManifestStore::list_tables(self, database).await
    }

    async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
        HiveManifestStore::get(self, database, table).await
    }

    async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
        HiveManifestStore::manifest_etag(self, database, table).await
    }

    fn limits(&self) -> &ManifestLimits {
        HiveManifestStore::limits(self)
    }
}

/// One attached Hive root: where its table metadata comes from, plus the
/// sidecar store behind it when SQE owns one.
///
/// The pair exists because reads and DDL want different things. Reads only
/// ever need [`HiveMetadataSource`]. `CREATE EXTERNAL TABLE` and `DROP
/// TABLE` need a store they can write, which a root over someone else's
/// metastore does not have, so `sidecar` is `None` there and the DDL path
/// refuses by name rather than appearing to succeed.
#[derive(Clone)]
pub struct HiveRoot {
    source: Arc<dyn HiveMetadataSource>,
    sidecar: Option<Arc<HiveManifestStore>>,
}

impl HiveRoot {
    /// A root over a sidecar warehouse SQE owns: readable and writable.
    pub fn from_store(store: Arc<HiveManifestStore>) -> Self {
        Self {
            source: Arc::clone(&store) as Arc<dyn HiveMetadataSource>,
            sidecar: Some(store),
        }
    }

    /// A root over a metastore SQE does not own: readable only.
    pub fn from_source(source: Arc<dyn HiveMetadataSource>) -> Self {
        Self {
            source,
            sidecar: None,
        }
    }

    pub fn source(&self) -> Arc<dyn HiveMetadataSource> {
        Arc::clone(&self.source)
    }

    /// The writable sidecar store, or `None` when this root reads an
    /// external metastore.
    pub fn sidecar(&self) -> Option<Arc<HiveManifestStore>> {
        self.sidecar.clone()
    }

    /// Bound every read on this root's metadata source (issue #666). The
    /// sidecar handle, used by DDL only, is left as is: a write that stalls
    /// should fail through its own object-store error, not be cut short.
    #[must_use = "with_metadata_timeout consumes self; bind the returned root"]
    pub fn with_metadata_timeout(mut self, timeout: Duration) -> Self {
        self.source = Arc::new(TimeoutSource::new(Arc::clone(&self.source), timeout));
        self
    }
}

/// A [`HiveMetadataSource`] whose every call is cut at `timeout` (issue
/// #666).
///
/// Nothing SQE-side bounded a metastore read before this: a throttled Glue
/// `GetTable` or a `GetTables` page that never came back stalled the query
/// in planning with no log line and no way to cancel. The cut is reported as
/// [`HiveError::Timeout`], which `HiveSchemaProvider` turns into "serve the
/// stale entry with a warning" when it has one and into a named error when
/// it does not. Listings are bounded per call, so a multi-database refresh
/// skips the slow database and keeps its previous snapshot, the way a failed
/// listing already did.
pub struct TimeoutSource {
    inner: Arc<dyn HiveMetadataSource>,
    timeout: Duration,
}

impl TimeoutSource {
    pub fn new(inner: Arc<dyn HiveMetadataSource>, timeout: Duration) -> Self {
        Self { inner, timeout }
    }

    async fn bounded<T, F>(&self, op: &str, what: &str, fut: F) -> Result<T, HiveError>
    where
        F: std::future::Future<Output = Result<T, HiveError>>,
    {
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(HiveError::Timeout(format!(
                "{op} {what} did not answer within {}s ([hive] metadata_timeout_secs)",
                self.timeout.as_secs_f64()
            ))),
        }
    }
}

#[async_trait]
impl HiveMetadataSource for TimeoutSource {
    async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
        self.bounded("list_databases", "", self.inner.list_databases())
            .await
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
        self.bounded("list_tables", database, self.inner.list_tables(database))
            .await
    }

    async fn list_manifests(
        &self,
        database: &str,
    ) -> Result<Option<Vec<HiveManifest>>, HiveError> {
        self.bounded(
            "list_manifests",
            database,
            self.inner.list_manifests(database),
        )
        .await
    }

    async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
        self.bounded(
            "get",
            &format!("{database}.{table}"),
            self.inner.get(database, table),
        )
        .await
    }

    async fn etag(&self, database: &str, table: &str) -> Result<Option<String>, HiveError> {
        self.bounded(
            "etag",
            &format!("{database}.{table}"),
            self.inner.etag(database, table),
        )
        .await
    }

    fn limits(&self) -> &ManifestLimits {
        self.inner.limits()
    }
}

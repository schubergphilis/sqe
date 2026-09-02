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

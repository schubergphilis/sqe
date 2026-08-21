//! The manifest store: reads, writes, deletes, and lists Hive-style
//! external-table manifests in object storage.
//!
//! The manifest is untrusted input: anyone who can write to the warehouse
//! prefix can plant one. That is why [`HiveManifestStore::get`] re-checks
//! `storage_descriptor.location` against the `[storage.tvf]` allowlist on
//! every load, not only when the manifest is created. See the comment on
//! that check for why the guard cannot be moved to create time.

use std::sync::Arc;

use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use tracing::warn;

use sqe_core::config::{TvfCaller, TvfPolicy};

use super::manifest::{HiveManifest, ManifestLimits};
use super::HiveError;

/// The sidecar filename under `<root>/<database>/<table>/_sqe/`.
const MANIFEST_FILENAME: &str = "table.json";
/// The directory name that holds the manifest, sibling to the table's data.
const MANIFEST_DIR: &str = "_sqe";

/// Reads, writes, deletes, and lists Hive-style external-table manifests
/// under a warehouse root in object storage.
pub struct HiveManifestStore {
    root: ListingTableUrl,
    store: Arc<dyn ObjectStore>,
    limits: ManifestLimits,
    tvf: TvfPolicy,
    caller: TvfCaller,
}

impl HiveManifestStore {
    pub fn new(
        root: ListingTableUrl,
        store: Arc<dyn ObjectStore>,
        limits: ManifestLimits,
        tvf: TvfPolicy,
        caller: TvfCaller,
    ) -> Self {
        Self {
            root,
            store,
            limits,
            tvf,
            caller,
        }
    }

    /// `<root_path>/<database>/<table>/_sqe/table.json`.
    pub fn manifest_path(&self, database: &str, table: &str) -> Path {
        self.database_prefix(database)
            .join(table)
            .join(MANIFEST_DIR)
            .join(MANIFEST_FILENAME)
    }

    /// The database-level prefix, `<root_path>/<database>`.
    fn database_prefix(&self, database: &str) -> Path {
        self.root.prefix().clone().join(database)
    }

    /// Write a manifest. When `if_not_exists` is `true`, an existing
    /// manifest at the same path is left untouched and this returns
    /// `HiveError::Validation` naming the table.
    ///
    /// `storage_descriptor.location` is validated up front, same as
    /// [`HiveManifestStore::get`]: a `CREATE EXTERNAL TABLE` naming a
    /// forbidden location should not even get as far as writing the
    /// manifest. This does NOT replace the load-time check below; a
    /// manifest can reach the store by any other path (a direct write to the
    /// warehouse prefix), and the create-time check has no way to see that.
    pub async fn put(&self, m: &HiveManifest, if_not_exists: bool) -> Result<(), HiveError> {
        self.tvf
            .check_path(&m.storage_descriptor.location, &self.caller, false)
            .map_err(HiveError::Forbidden)?;

        let path = self.manifest_path(&m.database_name, &m.name);
        let bytes = Bytes::from(m.to_bytes()?);
        let table_ref = format!("{}.{}", m.database_name, m.name);

        if !if_not_exists {
            return self
                .store
                .put(&path, PutPayload::from(bytes))
                .await
                .map(|_| ())
                .map_err(|e| store_err(&path, e));
        }

        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(bytes.clone()), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(HiveError::Validation(format!(
                "table `{table_ref}` already exists"
            ))),
            // The backing store does not implement a conditional put at all
            // (some object_store backends only support PutMode::Overwrite).
            // The design's documented behavior for that case is
            // last-writer-wins, not a hard failure, so fall back to a
            // head-then-put. This is not atomic: a second writer could land
            // between the head and the put. That race is accepted here
            // because it is the documented behavior for stores without
            // conditional-put support, not a silent weakening of a
            // guarantee this store claims to hold elsewhere.
            Err(object_store::Error::NotImplemented { .. })
            | Err(object_store::Error::NotSupported { .. }) => {
                warn!(
                    table = %table_ref,
                    path = %path,
                    "hive_external: backing object store has no conditional put; \
                     falling back to a non-atomic head-then-put (last-writer-wins)"
                );
                match self.store.head(&path).await {
                    Ok(_) => Err(HiveError::Validation(format!(
                        "table `{table_ref}` already exists"
                    ))),
                    Err(object_store::Error::NotFound { .. }) => self
                        .store
                        .put(&path, PutPayload::from(bytes))
                        .await
                        .map(|_| ())
                        .map_err(|e| store_err(&path, e)),
                    Err(e) => Err(store_err(&path, e)),
                }
            }
            Err(e) => Err(store_err(&path, e)),
        }
    }

    /// Load a manifest, validating its location on every call.
    pub async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
        let path = self.manifest_path(database, table);
        let bytes = match self.store.get(&path).await {
            Ok(result) => result.bytes().await.map_err(|e| store_err(&path, e))?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(HiveError::NotFound(format!("{database}.{table}")));
            }
            Err(e) => return Err(store_err(&path, e)),
        };

        let manifest = HiveManifest::parse(&bytes, &self.limits)?;

        // SECURITY: the manifest is untrusted input. Anyone who can write to
        // the warehouse prefix can plant one pointing `storage_descriptor.
        // location` at `file:///var/run/secrets/kubernetes.io/serviceaccount/
        // token` or an internal HTTP endpoint (the same bypass CAT-01 closed
        // for `ListingTableFactory`, which is why the coordinator does not
        // call `enable_url_table()`). This check MUST run here, on every
        // load, and cannot be hoisted to create time: a manifest written by
        // any path other than this store's own `put` (a direct write to the
        // object store, a different engine version, a hand-edited file)
        // never goes through create-time validation at all, so a guard that
        // only runs at create protects nothing against the actual attack.
        self.tvf
            .check_path(&manifest.storage_descriptor.location, &self.caller, false)
            .map_err(HiveError::Forbidden)?;

        Ok(manifest)
    }

    /// Delete a manifest. `HiveError::NotFound` if it does not exist.
    pub async fn delete(&self, database: &str, table: &str) -> Result<(), HiveError> {
        let path = self.manifest_path(database, table);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => {
                Err(HiveError::NotFound(format!("{database}.{table}")))
            }
            Err(e) => Err(store_err(&path, e)),
        }
    }

    /// List database names one level under the warehouse root.
    ///
    /// A candidate directory only counts as a database if it has at least
    /// one table with a manifest under it, same principle as `list_tables`
    /// not inferring a table from a bare directory: the warehouse root can
    /// hold things other than database directories (a table's own data
    /// directory, a staging area, ...), and a bare directory listing cannot
    /// tell those apart from a database.
    ///
    /// This uses [`HiveManifestStore::has_any_table`] rather than
    /// `list_tables(..).is_empty()`: `list_databases` only needs one bit per
    /// candidate ("does this hold at least one table"), not every table
    /// name, so it stops as soon as it finds the first manifest instead of
    /// enumerating and `head`-ing every table under every candidate.
    pub async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
        let prefix = self.root.prefix();
        let list_prefix = if prefix.as_ref().is_empty() {
            None
        } else {
            Some(prefix)
        };
        let result = self
            .store
            .list_with_delimiter(list_prefix)
            .await
            .map_err(|e| store_err(prefix, e))?;

        let mut names = Vec::new();
        for candidate in &result.common_prefixes {
            let Some(name) = candidate.filename() else {
                continue;
            };
            if self.has_any_table(name).await? {
                names.push(name.to_string());
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// `true` as soon as `database` has at least one table with a manifest
    /// under it. Stops at the first match rather than enumerating every
    /// candidate, unlike `list_tables`, which genuinely needs every name.
    async fn has_any_table(&self, database: &str) -> Result<bool, HiveError> {
        let db_prefix = self.database_prefix(database);
        let result = self
            .store
            .list_with_delimiter(Some(&db_prefix))
            .await
            .map_err(|e| store_err(&db_prefix, e))?;

        for candidate in &result.common_prefixes {
            let Some(name) = candidate.filename() else {
                continue;
            };
            let manifest_path = self.manifest_path(database, name);
            match self.store.head(&manifest_path).await {
                Ok(_) => return Ok(true),
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(store_err(&manifest_path, e)),
            }
        }
        Ok(false)
    }

    /// List table names one level under `<root>/<database>` that actually
    /// have a manifest at `<db>/<table>/_sqe/table.json`. A bare directory
    /// under the database prefix (a data directory, say) is not a table and
    /// is not reported.
    pub async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
        let db_prefix = self.database_prefix(database);
        let result = self
            .store
            .list_with_delimiter(Some(&db_prefix))
            .await
            .map_err(|e| store_err(&db_prefix, e))?;

        let mut names = Vec::new();
        for candidate in &result.common_prefixes {
            let Some(name) = candidate.filename() else {
                continue;
            };
            let manifest_path = self.manifest_path(database, name);
            match self.store.head(&manifest_path).await {
                Ok(_) => names.push(name.to_string()),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(store_err(&manifest_path, e)),
            }
        }
        names.sort();
        Ok(names)
    }
}

/// Every `object_store` error becomes `HiveError::Store` naming the object
/// path, since the operator's next step is to look at that object.
fn store_err(path: &Path, err: object_store::Error) -> HiveError {
    HiveError::Store(format!("{path}: {err}"))
}

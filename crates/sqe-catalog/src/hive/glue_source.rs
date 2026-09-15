//! [`HiveMetadataSource`] backed by the AWS Glue Data Catalog.
//!
//! Glue already is the metastore, so SQE reads it rather than keeping a copy.
//! The mapping is mechanical because `manifest.rs` models Glue's `TableInput`
//! and `StorageDescriptor` field for field, which is what it was built for.
//!
//! Distinct from `--catalog-backend glue`, which builds an *Iceberg* Glue
//! catalog. That one resolves a table through `Parameters.metadata_location`
//! and cannot see a Hive external table at all; against a catalog of them it
//! fails the whole attach rather than returning nothing.
//!
//! ## Read-only, and lazy
//!
//! No `put`/`delete`: SQE does not own this catalog. And a table is fetched
//! only when something names it. Enumerating a namespace and loading every
//! table in it is what makes the Iceberg provider fail whole-catalog on one
//! unreadable table, and a real catalog always has one.

use std::collections::BTreeMap;

use async_trait::async_trait;
use aws_sdk_glue::types::{Column as GlueSdkColumn, StorageDescriptor as GlueSdkSd, Table};

use super::manifest::{
    GlueColumn, HiveManifest, ManifestLimits, SerdeInfo, SqeExtensions, StorageDescriptor,
    MANIFEST_VERSION,
};
use super::source::HiveMetadataSource;
use super::HiveError;

/// Reads table metadata straight out of Glue.
pub struct GlueMetadataSource {
    client: aws_sdk_glue::Client,
    /// Glue `CatalogId`, i.e. the owning account. `None` means the caller's
    /// own account, which is what almost every deployment wants.
    catalog_id: Option<String>,
    /// When non-empty, the root is restricted to these databases: only they
    /// are listed, and a table in any other database is not found. Empty
    /// exposes the whole catalog.
    databases: Vec<String>,
    limits: ManifestLimits,
}

impl std::fmt::Debug for GlueMetadataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueMetadataSource")
            .field("catalog_id", &self.catalog_id)
            .finish()
    }
}

impl GlueMetadataSource {
    pub fn new(
        client: aws_sdk_glue::Client,
        catalog_id: Option<String>,
        databases: Vec<String>,
        limits: ManifestLimits,
    ) -> Self {
        Self {
            client,
            catalog_id,
            databases,
            limits,
        }
    }

    /// `true` when `database` is outside the configured allow-list. An empty
    /// list allows everything.
    fn database_blocked(&self, database: &str) -> bool {
        !self.databases.is_empty() && !self.databases.iter().any(|d| d == database)
    }

    /// One paginated `GetTables` for `database`, mapped to manifests. Backs
    /// both `list_tables` and `list_manifests`.
    async fn list_glue_manifests(&self, database: &str) -> Result<Vec<HiveManifest>, HiveError> {
        // A database outside the allow-list lists no tables, the same empty
        // answer an unknown database gives.
        if self.database_blocked(database) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut pages = self
            .client
            .get_tables()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = match page {
                Ok(p) => p,
                // DataFusion asks about schemas it has only seen named, so an
                // unknown database is an empty list. Erroring here fails the
                // whole query over a namespace nobody selected from.
                Err(e) if is_not_found(&e) => return Ok(Vec::new()),
                Err(e) => return Err(glue_err("get_tables", e)),
            };
            out.extend(
                page.table_list()
                    .iter()
                    .map(|t| manifest_from_glue_table(database, t)),
            );
        }
        Ok(out)
    }

    /// Build a client from the ambient AWS configuration: the same provider
    /// chain the AWS CLI uses, so `AWS_PROFILE`, SSO, an instance profile and
    /// a task role all work, and credentials refresh rather than being
    /// captured once.
    pub async fn from_env(
        region: Option<&str>,
        catalog_id: Option<String>,
        databases: Vec<String>,
        limits: ManifestLimits,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        // The shared loader carries the ring HTTPS client; a bare
        // `aws_config::defaults` has none and panics on the first request.
        let mut loader = crate::aws_config::sdk_config_loader();
        if let Some(region) = region.filter(|r| !r.is_empty()) {
            loader = loader.region(aws_config::Region::new(region.to_string()));
        }
        let conf = loader.load().await;
        let mut builder = aws_sdk_glue::config::Builder::from(&conf);
        // Bound the SDK's own connect and operation clocks too (issue #666):
        // the `TimeoutSource` wrapper cuts the await, but an SDK call with
        // no timeout of its own would keep a connection and a retry loop
        // alive underneath it.
        if let Some(t) = timeout {
            builder = builder.timeout_config(
                aws_sdk_glue::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(t.min(std::time::Duration::from_secs(5)))
                    .operation_attempt_timeout(t)
                    .operation_timeout(t)
                    .build(),
            );
        }
        Self::new(
            aws_sdk_glue::Client::from_conf(builder.build()),
            catalog_id,
            databases,
            limits,
        )
    }
}

fn column_of(c: &GlueSdkColumn) -> GlueColumn {
    GlueColumn {
        name: c.name().to_string(),
        r#type: c.r#type().unwrap_or("string").to_string(),
        comment: c.comment().map(str::to_string),
    }
}

fn storage_descriptor_of(sd: Option<&GlueSdkSd>) -> StorageDescriptor {
    let Some(sd) = sd else {
        return StorageDescriptor {
            location: String::new(),
            input_format: None,
            output_format: None,
            compressed: false,
            serde_info: SerdeInfo::default(),
            columns: Vec::new(),
            sort_columns: Vec::new(),
        };
    };
    let serde_info = sd
        .serde_info()
        .map(|si| SerdeInfo {
            serialization_library: si.serialization_library().map(str::to_string),
            parameters: si
                .parameters()
                .map(|p| {
                    p.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
        })
        .unwrap_or_default();

    StorageDescriptor {
        // A trailing slash, so a prefix listing does not also match a
        // sibling whose name merely starts with this one.
        location: match sd.location() {
            Some(l) if !l.is_empty() => format!("{}/", l.trim_end_matches('/')),
            _ => String::new(),
        },
        input_format: sd.input_format().map(str::to_string),
        output_format: sd.output_format().map(str::to_string),
        compressed: sd.compressed(),
        serde_info,
        columns: sd.columns().iter().map(column_of).collect(),
        sort_columns: Vec::new(),
    }
}

/// Glue's `Table` as a [`HiveManifest`].
///
/// Partition keys stay in their own list rather than being folded into
/// `columns`: Glue never puts them in the `StorageDescriptor`, and the
/// provider appends them at the tail of the query schema itself.
pub fn manifest_from_glue_table(database: &str, t: &Table) -> HiveManifest {
    HiveManifest {
        manifest_version: MANIFEST_VERSION,
        table_type: t
            .table_type()
            .unwrap_or("EXTERNAL_TABLE")
            .to_string(),
        database_name: database.to_string(),
        name: t.name().to_string(),
        owner: t.owner().map(str::to_string),
        created_at: t.create_time().map(|d| d.to_string()),
        description: t.description().map(str::to_string),
        view_original_text: t.view_original_text().map(str::to_string),
        partition_keys: t.partition_keys().iter().map(column_of).collect(),
        // Carries projection.* verbatim, which is what makes a projected
        // table readable without any partitions registered in Glue.
        parameters: t
            .parameters()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default(),
        storage_descriptor: storage_descriptor_of(t.storage_descriptor()),
        sqe: SqeExtensions::default(),
    }
}

fn glue_err(op: &str, e: impl std::fmt::Display) -> HiveError {
    HiveError::Store(format!("glue {op} failed: {e}"))
}

#[async_trait]
impl HiveMetadataSource for GlueMetadataSource {
    async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
        // With an allow-list, skip the catalog scan and return it directly:
        // the databases the operator named are the answer, and a name that
        // does not exist in Glue simply lists no tables later.
        if !self.databases.is_empty() {
            return Ok(self.databases.clone());
        }
        let mut out = Vec::new();
        let mut pages = self
            .client
            .get_databases()
            .set_catalog_id(self.catalog_id.clone())
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| glue_err("get_databases", e))?;
            out.extend(page.database_list().iter().map(|d| d.name().to_string()));
        }
        Ok(out)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
        // `GetTables` returns whole `Table` objects either way, so the names
        // are the manifests' names; the paginated call is the cost, not the
        // mapping.
        Ok(self
            .list_glue_manifests(database)
            .await?
            .into_iter()
            .map(|m| m.name)
            .collect())
    }

    /// `GetTables` already carries every table's full definition, so the
    /// listing IS the bulk fetch: one paginated call per database instead of
    /// one `GetTable` per table (issue #666).
    async fn list_manifests(
        &self,
        database: &str,
    ) -> Result<Option<Vec<HiveManifest>>, HiveError> {
        self.list_glue_manifests(database).await.map(Some)
    }

    async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
        // A database outside the allow-list is not found, so a direct
        // `SELECT FROM glue.other_db.t` cannot reach past the scoping.
        if self.database_blocked(database) {
            return Err(HiveError::NotFound(format!("{database}.{table}")));
        }
        let resp = self
            .client
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .name(table)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) if is_not_found(&e) => {
                return Err(HiveError::NotFound(format!("{database}.{table}")))
            }
            Err(e) => return Err(glue_err("get_table", e)),
        };
        let t = resp
            .table()
            .ok_or_else(|| HiveError::NotFound(format!("{database}.{table}")))?;
        Ok(manifest_from_glue_table(database, t))
    }

    /// Glue exposes no change token cheaper than the fetch itself, so this
    /// always reports "cannot tell" and freshness comes from the cache TTL.
    /// `UpdateTime` would serve, but reading it means `GetTable`, which is
    /// the very call the token exists to avoid.
    async fn etag(&self, _database: &str, _table: &str) -> Result<Option<String>, HiveError> {
        Ok(None)
    }

    fn limits(&self) -> &ManifestLimits {
        &self.limits
    }
}

fn is_not_found<E: std::fmt::Debug>(e: &E) -> bool {
    // The SDK models EntityNotFound per-operation, so matching the rendered
    // error keeps one predicate for every call rather than three.
    format!("{e:?}").contains("EntityNotFound")
}

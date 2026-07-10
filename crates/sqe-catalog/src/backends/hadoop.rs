//! Hadoop storage-only catalog backend.
//!
//! Iceberg's Hadoop catalog stores table metadata as `metadata/v<N>.metadata.json`
//! files under a warehouse path. There is no separate catalog service; the
//! "catalog" is the directory layout itself. SQE implements this backend by
//! walking the warehouse with `object_store` and picking the highest version
//! file per table.
//!
//! This backend is read-oriented. Writing through the Hadoop layout is
//! possible but race-prone (no atomic rename on S3-compatible stores), so
//! production workloads should use REST, HMS, or Glue.

use std::str::FromStr;
use std::sync::Arc;

use futures::stream::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore};
use regex::Regex;
use tracing::{debug, instrument, warn};

use sqe_core::{Result as SqeResult, SqeError};

/// A discovered table in a Hadoop-layout warehouse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HadoopTable {
    /// Namespace parts (e.g. `["sales", "q1"]` for `sales.q1`).
    pub namespace: Vec<String>,
    /// Table name.
    pub name: String,
    /// Absolute path to the highest-version metadata file.
    pub metadata_location: String,
    /// Version number extracted from the filename (v<N>.metadata.json).
    pub version: u32,
}

/// A backend that discovers Iceberg tables by scanning a warehouse path.
pub struct HadoopBackend {
    store: Arc<dyn ObjectStore>,
    /// The warehouse root path inside the object store (without the scheme).
    /// For `s3://lake/warehouse`, this is `warehouse`. For a local filesystem
    /// backed warehouse at `/tmp/wh`, this is the empty string.
    warehouse_root: ObjectPath,
    /// Pre-compiled regex for `v<N>.metadata.json` filenames.
    metadata_re: Regex,
}

impl std::fmt::Debug for HadoopBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HadoopBackend")
            .field("warehouse_root", &self.warehouse_root)
            .finish()
    }
}

impl HadoopBackend {
    /// Create a new Hadoop backend over an existing `ObjectStore`.
    ///
    /// `warehouse_root` is the path inside the store where namespaces live.
    /// Pass an empty `ObjectPath::default()` if the store root IS the warehouse.
    pub fn new(store: Arc<dyn ObjectStore>, warehouse_root: ObjectPath) -> Self {
        Self {
            store,
            warehouse_root,
            // Filenames look like `v00001.metadata.json`, `v42.metadata.json`, or
            // `v123-abcdef.metadata.json` when the writer appended a random suffix.
            // We only pull the numeric version prefix; any suffix is ignored.
            metadata_re: Regex::new(r"^v(\d+)(?:-[A-Za-z0-9-]+)?\.metadata\.json$")
                .expect("static regex compiles"),
        }
    }

    /// List all tables discovered under the warehouse path.
    ///
    /// Returns tables grouped by namespace, with only the highest-version
    /// metadata file per table. Non-Iceberg subdirectories are ignored.
    #[instrument(skip(self))]
    pub async fn list_tables(&self) -> SqeResult<Vec<HadoopTable>> {
        let prefix = if self.warehouse_root.as_ref().is_empty() {
            None
        } else {
            Some(&self.warehouse_root)
        };
        let mut stream = self.store.list(prefix);
        let mut candidates: std::collections::HashMap<(Vec<String>, String), HadoopTable> =
            std::collections::HashMap::new();

        while let Some(next) = stream.next().await {
            let meta: ObjectMeta = match next {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "object_store list error; continuing");
                    continue;
                }
            };

            // Look for `.../<ns1>/<ns2?>/<table>/metadata/v<N>.metadata.json`.
            // PathPart borrows from the Path, so we copy into owned Strings to
            // keep the borrow checker happy across the helper call.
            let owned_parts: Vec<String> = meta
                .location
                .parts()
                .map(|p| p.as_ref().to_string())
                .collect();
            let parts: Vec<&str> = owned_parts.iter().map(|s| s.as_str()).collect();
            let Some((ns, tbl, version)) = self.extract_table_parts(&parts) else {
                continue;
            };

            let key = (ns.clone(), tbl.clone());
            let abs = meta.location.to_string();

            candidates
                .entry(key)
                .and_modify(|existing| {
                    if version > existing.version {
                        existing.version = version;
                        existing.metadata_location = abs.clone();
                    }
                })
                .or_insert_with(|| HadoopTable {
                    namespace: ns,
                    name: tbl,
                    metadata_location: abs,
                    version,
                });
        }

        let mut tables: Vec<HadoopTable> = candidates.into_values().collect();
        // Stable output: namespace first, then table, alphabetical.
        tables.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.name.cmp(&b.name))
        });
        debug!(count = tables.len(), "Hadoop backend listed tables");
        Ok(tables)
    }

    /// Extract `(namespace_parts, table_name, version)` from a path like
    /// `warehouse/sales/orders/metadata/v00012.metadata.json`.
    ///
    /// Returns `None` if the path does not match the Iceberg Hadoop layout.
    fn extract_table_parts(&self, parts: &[&str]) -> Option<(Vec<String>, String, u32)> {
        // Must end with `metadata/v<N>.metadata.json`.
        if parts.len() < 3 {
            return None;
        }
        let filename = parts[parts.len() - 1];
        let metadata_dir = parts[parts.len() - 2];
        if metadata_dir != "metadata" {
            return None;
        }

        let caps = self.metadata_re.captures(filename)?;
        let version = u32::from_str(caps.get(1)?.as_str()).ok()?;

        // The parts before `metadata/` are `[...warehouse, ...namespace, table]`.
        // Strip the warehouse root prefix if present.
        let prelude = &parts[..parts.len() - 2];

        // Collect warehouse-root parts as owned Strings to keep borrows simple.
        let root_owned: Vec<String> = if self.warehouse_root.as_ref().is_empty() {
            Vec::new()
        } else {
            self.warehouse_root
                .parts()
                .map(|p| p.as_ref().to_string())
                .collect()
        };
        if prelude.len() < root_owned.len() {
            return None;
        }
        for (i, rp) in root_owned.iter().enumerate() {
            if prelude[i] != rp.as_str() {
                return None;
            }
        }
        let relative = &prelude[root_owned.len()..];
        if relative.is_empty() {
            return None;
        }

        let table = (*relative.last()?).to_string();
        let namespace: Vec<String> = relative[..relative.len() - 1]
            .iter()
            .map(|s| (*s).to_string())
            .collect();

        Some((namespace, table, version))
    }

    /// Find a single table's current metadata location.
    ///
    /// `namespace` is a dotted path (e.g. `"sales.q1"`); empty string means
    /// the warehouse root.
    pub async fn find_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> SqeResult<Option<HadoopTable>> {
        let tables = self.list_tables().await?;
        let wanted_ns: Vec<String> = if namespace.is_empty() {
            Vec::new()
        } else {
            namespace.split('.').map(|s| s.to_string()).collect()
        };
        Ok(tables
            .into_iter()
            .find(|t| t.namespace == wanted_ns && t.name == table))
    }
}

/// Convenience wrapper around `object_store` errors that maps them to
/// `SqeError::Catalog` consistently.
#[allow(dead_code)]
fn map_err<E: std::fmt::Display>(ctx: &str, err: E) -> SqeError {
    SqeError::Catalog(format!("{ctx}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    async fn seed_store(pairs: &[(&str, &str)]) -> Arc<dyn ObjectStore> {
        let concrete = InMemory::new();
        for (path, body) in pairs {
            let p = ObjectPath::from(*path);
            concrete
                .put(&p, PutPayload::from(bytes::Bytes::from(body.to_string())))
                .await
                .unwrap();
        }
        Arc::new(concrete) as Arc<dyn ObjectStore>
    }

    #[tokio::test]
    async fn discovers_table_with_highest_version() {
        let store = seed_store(&[
            ("warehouse/ns/t/metadata/v00001.metadata.json", "{}"),
            ("warehouse/ns/t/metadata/v00002.metadata.json", "{}"),
        ])
        .await;
        let backend = HadoopBackend::new(store, ObjectPath::from("warehouse"));
        let tables = backend.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].namespace, vec!["ns".to_string()]);
        assert_eq!(tables[0].name, "t");
        assert_eq!(tables[0].version, 2);
        assert!(tables[0].metadata_location.ends_with("v00002.metadata.json"));
    }

    #[tokio::test]
    async fn discovers_tables_across_nested_namespaces() {
        let store = seed_store(&[
            ("warehouse/a/t1/metadata/v00001.metadata.json", "{}"),
            ("warehouse/a/b/t2/metadata/v00003.metadata.json", "{}"),
            ("warehouse/a/b/t2/metadata/v00001.metadata.json", "{}"),
        ])
        .await;
        let backend = HadoopBackend::new(store, ObjectPath::from("warehouse"));
        let tables = backend.list_tables().await.unwrap();
        assert_eq!(tables.len(), 2);
        // Alphabetical: ns=[a] comes before ns=[a, b].
        assert_eq!(tables[0].namespace, vec!["a".to_string()]);
        assert_eq!(tables[0].name, "t1");
        assert_eq!(tables[1].namespace, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(tables[1].name, "t2");
        assert_eq!(tables[1].version, 3);
    }

    #[tokio::test]
    async fn ignores_non_metadata_files() {
        let store = seed_store(&[
            ("warehouse/ns/t/metadata/v00001.metadata.json", "{}"),
            ("warehouse/ns/t/data/part-00000.parquet", "parquet"),
            ("warehouse/ns/t/README.txt", "hello"),
        ])
        .await;
        let backend = HadoopBackend::new(store, ObjectPath::from("warehouse"));
        let tables = backend.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
    }

    #[tokio::test]
    async fn find_table_returns_none_when_missing() {
        let store = seed_store(&[
            ("warehouse/ns/t/metadata/v00001.metadata.json", "{}"),
        ])
        .await;
        let backend = HadoopBackend::new(store, ObjectPath::from("warehouse"));
        let missing = backend.find_table("ns", "other").await.unwrap();
        assert!(missing.is_none());
        let found = backend.find_table("ns", "t").await.unwrap().unwrap();
        assert_eq!(found.version, 1);
    }

    #[tokio::test]
    async fn metadata_filename_with_random_suffix() {
        let store = seed_store(&[
            (
                "warehouse/ns/t/metadata/v00001-abc123.metadata.json",
                "{}",
            ),
            ("warehouse/ns/t/metadata/v00002.metadata.json", "{}"),
        ])
        .await;
        let backend = HadoopBackend::new(store, ObjectPath::from("warehouse"));
        let tables = backend.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].version, 2);
    }
}

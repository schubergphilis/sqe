use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::rest_catalog::SessionCatalog;
use crate::system_jdbc::JdbcSchemaProvider;
use crate::system_metadata::MetadataSchemaProvider;

/// One catalog the session can reach, paired with the `SessionCatalog` used to
/// enumerate it. `system.jdbc.*` and `system.metadata.*` iterate these so JDBC
/// metadata browsing (`getCatalogs`/`getTables`/`getColumns`) sees every
/// reachable catalog -- the configured ones plus the session's own
/// (X-Trino-Catalog) Polaris warehouse -- not just the default. (#5)
#[derive(Clone)]
pub struct SystemCatalogEntry {
    pub name: String,
    pub catalog: Arc<SessionCatalog>,
}

impl std::fmt::Debug for SystemCatalogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemCatalogEntry")
            .field("name", &self.name)
            .finish()
    }
}

/// DataFusion `CatalogProvider` for the virtual `system` catalog.
///
/// Provides the following schemas:
///
/// - `jdbc`     — JDBC metadata tables required by Trino JDBC drivers
/// - `metadata` — catalog/table/schema property tables
/// - `runtime`  — live query, node, and task information (optional). The
///   coordinator supplies this one (`sqe_coordinator::system_runtime`): it
///   renders coordinator state, so the catalog crate only holds the slot.
pub struct SystemCatalogProvider {
    jdbc_schema: Arc<JdbcSchemaProvider>,
    metadata_schema: Arc<MetadataSchemaProvider>,
    runtime_schema: Option<Arc<dyn SchemaProvider>>,
}

impl std::fmt::Debug for SystemCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemCatalogProvider")
            .field("jdbc_schema", &self.jdbc_schema)
            .field("metadata_schema", &self.metadata_schema)
            .field("runtime_schema", &self.runtime_schema.is_some())
            .finish()
    }
}

impl SystemCatalogProvider {
    /// Build the `system` catalog over every reachable catalog. `entries[0]`
    /// is the primary/default warehouse (first in JDBC catalog listings);
    /// `entries` also include the session's own catalog so a JDBC client that
    /// connected with `X-Trino-Catalog=<ws>` enumerates `<ws>`.
    pub fn new(entries: Vec<SystemCatalogEntry>) -> Self {
        Self {
            jdbc_schema: Arc::new(JdbcSchemaProvider::new(entries.clone())),
            metadata_schema: Arc::new(MetadataSchemaProvider::new(entries)),
            runtime_schema: None,
        }
    }

    /// Set the schema provider for the `system.runtime.*` virtual tables.
    #[must_use = "with_runtime consumes self; bind the returned provider"]
    pub fn with_runtime(mut self, runtime: Arc<dyn SchemaProvider>) -> Self {
        self.runtime_schema = Some(runtime);
        self
    }
}

impl CatalogProvider for SystemCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names = vec!["jdbc".to_string(), "metadata".to_string()];
        if self.runtime_schema.is_some() {
            names.push("runtime".to_string());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "jdbc" => Some(self.jdbc_schema.clone()),
            "metadata" => Some(self.metadata_schema.clone()),
            "runtime" => self
                .runtime_schema
                .as_ref()
                .map(|s| s.clone() as Arc<dyn SchemaProvider>),
            _ => None,
        }
    }
}

/// The catalog registered as `system` (#901): SQE's virtual schemas, plus a
/// Polaris catalog named `system` for every other schema when one exists.
///
/// Routing uses [`sqe_core::system_catalog::is_reserved_system_schema`], the
/// same predicate the policy rewriter uses to skip tag lookups, so a schema
/// the policy layer treats as virtual can never come from Polaris. A Polaris
/// namespace with a reserved name is hidden.
///
/// The Polaris part is set at most once: at session build when the catalog is
/// configured or is the session's discovered catalog, or later by lazy
/// discovery on a cached session ([`Self::attach_polaris`]).
#[derive(Debug)]
pub struct SystemCompositeProvider {
    virtual_: Arc<dyn CatalogProvider>,
    polaris: std::sync::OnceLock<Arc<dyn CatalogProvider>>,
}

impl SystemCompositeProvider {
    pub fn new(
        virtual_: Arc<dyn CatalogProvider>,
        polaris: Option<Arc<dyn CatalogProvider>>,
    ) -> Self {
        let cell = std::sync::OnceLock::new();
        if let Some(p) = polaris {
            let _ = cell.set(p);
        }
        Self {
            virtual_,
            polaris: cell,
        }
    }

    /// Set the Polaris part. False when one was already set (the first wins).
    pub fn attach_polaris(&self, polaris: Arc<dyn CatalogProvider>) -> bool {
        self.polaris.set(polaris).is_ok()
    }

    pub fn has_polaris(&self) -> bool {
        self.polaris.get().is_some()
    }

    /// The Polaris catalog `system`, when one is attached.
    pub fn polaris(&self) -> Option<&Arc<dyn CatalogProvider>> {
        self.polaris.get()
    }
}

fn reserved_schema_error(name: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Plan(format!(
        "schema name `{name}` is reserved in catalog `system` (reserved: {})",
        sqe_core::system_catalog::RESERVED_SYSTEM_SCHEMAS.join(", ")
    ))
}

impl CatalogProvider for SystemCompositeProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.virtual_.schema_names();
        if let Some(p) = self.polaris.get() {
            names.extend(
                p.schema_names()
                    .into_iter()
                    .filter(|n| !sqe_core::system_catalog::is_reserved_system_schema(n)),
            );
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if sqe_core::system_catalog::is_reserved_system_schema(name) {
            return self.virtual_.schema(name);
        }
        self.polaris.get().and_then(|p| p.schema(name))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        if sqe_core::system_catalog::is_reserved_system_schema(name) {
            return Err(reserved_schema_error(name));
        }
        match self.polaris.get() {
            Some(p) => p.register_schema(name, schema),
            None => datafusion::common::not_impl_err!(
                "catalog `system` has no Polaris catalog to create schema `{name}` in"
            ),
        }
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        if sqe_core::system_catalog::is_reserved_system_schema(name) {
            return Err(reserved_schema_error(name));
        }
        match self.polaris.get() {
            Some(p) => p.deregister_schema(name, cascade),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod composite_tests {
    use super::*;
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};

    fn polaris_with(names: &[&str]) -> Arc<dyn CatalogProvider> {
        let p = MemoryCatalogProvider::new();
        for n in names {
            p.register_schema(n, Arc::new(MemorySchemaProvider::new()))
                .unwrap();
        }
        Arc::new(p)
    }

    fn virtual_with_runtime() -> (Arc<dyn CatalogProvider>, Arc<dyn SchemaProvider>) {
        let runtime: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
        let v = SystemCatalogProvider::new(vec![]).with_runtime(Arc::clone(&runtime));
        (Arc::new(v), runtime)
    }

    #[test]
    fn reserved_names_go_to_the_virtual_part_only() {
        let (v, runtime) = virtual_with_runtime();
        // Polaris carries a namespace for every reserved name and its case
        // variants: none may ever be returned.
        let mut shadow: Vec<String> = Vec::new();
        for r in sqe_core::system_catalog::RESERVED_SYSTEM_SCHEMAS {
            shadow.push(r.to_string());
            shadow.push(r.to_ascii_uppercase());
        }
        let shadow_refs: Vec<&str> = shadow.iter().map(String::as_str).collect();
        let mut all = shadow_refs.clone();
        all.push("access");
        let polaris = polaris_with(&all);
        let c = SystemCompositeProvider::new(v, Some(Arc::clone(&polaris)));

        let got = c.schema("runtime").expect("runtime");
        assert!(
            Arc::ptr_eq(&got, &runtime),
            "runtime must be the virtual one"
        );
        for name in &shadow_refs {
            if let Some(s) = c.schema(name) {
                let from_polaris = polaris.schema(name).expect("seeded");
                assert!(!Arc::ptr_eq(&s, &from_polaris), "{name} came from Polaris");
            }
        }
        assert!(c.schema("access").is_some());

        let names = c.schema_names();
        assert!(names.contains(&"access".to_string()), "{names:?}");
        assert!(names.contains(&"runtime".to_string()), "{names:?}");
        assert!(
            !names.iter().any(|n| n == "RUNTIME" || n == "JDBC"),
            "{names:?}"
        );
        assert_eq!(
            names.iter().filter(|n| *n == "runtime").count(),
            1,
            "{names:?}"
        );
    }

    #[test]
    fn without_polaris_it_is_the_virtual_catalog() {
        let (v, _) = virtual_with_runtime();
        let expected = v.schema_names();
        let c = SystemCompositeProvider::new(v, None);
        assert_eq!(c.schema_names(), expected);
        assert!(c.schema("access").is_none());
        assert!(c.schema("jdbc").is_some());
    }

    #[test]
    fn reserved_schemas_cannot_be_registered_or_dropped() {
        let (v, _) = virtual_with_runtime();
        let c = SystemCompositeProvider::new(v, Some(polaris_with(&[])));
        let err = c
            .register_schema("Runtime", Arc::new(MemorySchemaProvider::new()))
            .unwrap_err();
        assert!(
            err.to_string().contains("reserved in catalog `system`"),
            "{err}"
        );
        assert!(c.deregister_schema("jdbc", true).is_err());
        assert!(c
            .register_schema("access", Arc::new(MemorySchemaProvider::new()))
            .is_ok());
        assert!(c.schema("access").is_some());
    }

    #[test]
    fn polaris_is_attached_once() {
        let (v, _) = virtual_with_runtime();
        let c = SystemCompositeProvider::new(v, None);
        assert!(!c.has_polaris());
        assert!(c.attach_polaris(polaris_with(&["access"])));
        assert!(!c.attach_polaris(polaris_with(&["other"])));
        assert!(c.schema("access").is_some());
        assert!(c.schema("other").is_none());
    }
}

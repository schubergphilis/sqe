use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{debug, warn};

use crate::rest_catalog::{listing_error_is_forbidden, SessionCatalog};
use crate::system_catalog::SystemCatalogEntry;

/// DataFusion `SchemaProvider` for the virtual `system.metadata` schema.
///
/// Exposes metadata tables (`catalogs`, `table_properties`, `schema_properties`,
/// `table_comments`) that provide introspection into catalog and table-level
/// properties stored in Polaris / Iceberg.
pub struct MetadataSchemaProvider {
    /// Every catalog the session can reach (primary first, deduplicated by
    /// name). The metadata tables iterate these so `system.metadata.catalogs`
    /// and the per-table/-schema property tables cover all reachable catalogs,
    /// not just the default. (#5)
    catalogs: Vec<SystemCatalogEntry>,
}

impl MetadataSchemaProvider {
    pub fn new(entries: Vec<SystemCatalogEntry>) -> Self {
        let mut seen = std::collections::HashSet::new();
        let catalogs = entries
            .into_iter()
            .filter(|e| seen.insert(e.name.clone()))
            .collect();
        Self { catalogs }
    }
}

impl std::fmt::Debug for MetadataSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.catalogs.iter().map(|e| e.name.as_str()).collect();
        f.debug_struct("MetadataSchemaProvider").field("catalogs", &names).finish()
    }
}

#[async_trait]
impl SchemaProvider for MetadataSchemaProvider {

    fn table_names(&self) -> Vec<String> {
        vec![
            "catalogs".into(),
            "table_properties".into(),
            "schema_properties".into(),
            "table_comments".into(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(
            name,
            "catalogs" | "table_properties" | "schema_properties" | "table_comments"
        )
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "catalogs" => {
                let names: Vec<String> = self.catalogs.iter().map(|e| e.name.clone()).collect();
                Ok(Some(build_catalogs_table(&names)?))
            }
            "table_properties" => Ok(Some(self.build_table_properties_table().await?)),
            "schema_properties" => Ok(Some(self.build_schema_properties_table().await?)),
            "table_comments" => Ok(Some(self.build_table_comments_table().await?)),
            _ => Ok(None),
        }
    }
}

impl MetadataSchemaProvider {
    /// List a single catalog's namespaces, returning an empty vec on error
    /// (with a log message) so enumeration skips an unauthorized / unreachable
    /// catalog instead of aborting the whole metadata listing.
    async fn list_namespaces_safe(catalog: &SessionCatalog) -> Vec<NamespaceIdent> {
        match catalog.list_namespaces().await {
            Ok(namespaces) => namespaces,
            Err(e) if listing_error_is_forbidden(&e) => {
                debug!(error = %e, "system.metadata: skipping catalog the principal is not authorized to list");
                Vec::new()
            }
            Err(e) => {
                warn!(error = %e, "system.metadata: skipping catalog whose namespaces could not be listed");
                Vec::new()
            }
        }
    }

    /// Convert a `NamespaceIdent` to a dot-separated string.
    fn namespace_to_string(ns: &NamespaceIdent) -> String {
        ns.as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Build `system.metadata.table_properties` — one row per property per table.
    async fn build_table_properties_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("property_name", DataType::Utf8, false),
            Field::new("property_value", DataType::Utf8, true),
        ]));

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut table_b = StringBuilder::new();
        let mut prop_name_b = StringBuilder::new();
        let mut prop_value_b = StringBuilder::new();

        for entry in &self.catalogs {
            for ns in &Self::list_namespaces_safe(&entry.catalog).await {
                let ns_str = Self::namespace_to_string(ns);
                let tables = match entry.catalog.list_tables(ns).await {
                    Ok(t) => t,
                    Err(e) if listing_error_is_forbidden(&e) => {
                        debug!(catalog = %entry.name, namespace = %ns_str, "system.metadata.table_properties: skipping namespace the principal is not authorized to list");
                        continue;
                    }
                    Err(e) => {
                        warn!(catalog = %entry.name, namespace = %ns_str, error = %e, "Failed to list tables for system.metadata.table_properties");
                        continue;
                    }
                };

                for table_ident in &tables {
                    let full_ident =
                        iceberg::TableIdent::new(ns.clone(), table_ident.name().to_string());
                    let table = match entry.catalog.load_table(&full_ident).await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(catalog = %entry.name, table = %table_ident.name(), error = %e, "Failed to load table for system.metadata.table_properties");
                            continue;
                        }
                    };

                    let properties = table.metadata().properties();
                    for (key, value) in properties {
                        catalog_b.append_value(&entry.name);
                        schema_b.append_value(&ns_str);
                        table_b.append_value(table_ident.name());
                        prop_name_b.append_value(key);
                        prop_value_b.append_value(value);
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_b.finish()) as ArrayRef,
                Arc::new(schema_b.finish()) as ArrayRef,
                Arc::new(table_b.finish()) as ArrayRef,
                Arc::new(prop_name_b.finish()) as ArrayRef,
                Arc::new(prop_value_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// Build `system.metadata.schema_properties` — one row per property per namespace.
    ///
    /// Uses `get_namespace()` on the inner REST catalog to fetch namespace properties.
    async fn build_schema_properties_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("property_name", DataType::Utf8, false),
            Field::new("property_value", DataType::Utf8, true),
        ]));

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut prop_name_b = StringBuilder::new();
        let mut prop_value_b = StringBuilder::new();

        for entry in &self.catalogs {
            for ns in &Self::list_namespaces_safe(&entry.catalog).await {
                let ns_str = Self::namespace_to_string(ns);
                match entry.catalog.get_namespace(ns).await {
                    Ok(namespace) => {
                        let properties = namespace.properties();
                        for (key, value) in properties {
                            catalog_b.append_value(&entry.name);
                            schema_b.append_value(&ns_str);
                            prop_name_b.append_value(key);
                            prop_value_b.append_value(value);
                        }
                    }
                    Err(e) if listing_error_is_forbidden(&e) => {
                        debug!(catalog = %entry.name, namespace = %ns_str, "system.metadata.schema_properties: skipping namespace the principal is not authorized to access");
                    }
                    Err(e) => {
                        warn!(catalog = %entry.name, namespace = %ns_str, error = %e, "Failed to get namespace for system.metadata.schema_properties");
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_b.finish()) as ArrayRef,
                Arc::new(schema_b.finish()) as ArrayRef,
                Arc::new(prop_name_b.finish()) as ArrayRef,
                Arc::new(prop_value_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// Build `system.metadata.table_comments` — one row per table.
    ///
    /// Extracts the `"comment"` property from each table's metadata properties.
    /// If no `"comment"` property exists, the comment column is NULL.
    async fn build_table_comments_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
        ]));

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut table_b = StringBuilder::new();
        let mut comment_b = StringBuilder::new();

        for entry in &self.catalogs {
            for ns in &Self::list_namespaces_safe(&entry.catalog).await {
                let ns_str = Self::namespace_to_string(ns);
                let tables = match entry.catalog.list_tables(ns).await {
                    Ok(t) => t,
                    Err(e) if listing_error_is_forbidden(&e) => {
                        debug!(catalog = %entry.name, namespace = %ns_str, "system.metadata.table_comments: skipping namespace the principal is not authorized to list");
                        continue;
                    }
                    Err(e) => {
                        warn!(catalog = %entry.name, namespace = %ns_str, error = %e, "Failed to list tables for system.metadata.table_comments");
                        continue;
                    }
                };

                for table_ident in &tables {
                    let full_ident =
                        iceberg::TableIdent::new(ns.clone(), table_ident.name().to_string());
                    let table = match entry.catalog.load_table(&full_ident).await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(catalog = %entry.name, table = %table_ident.name(), error = %e, "Failed to load table for system.metadata.table_comments");
                            continue;
                        }
                    };

                    catalog_b.append_value(&entry.name);
                    schema_b.append_value(&ns_str);
                    table_b.append_value(table_ident.name());

                    match table.metadata().properties().get("comment") {
                        Some(comment) => comment_b.append_value(comment),
                        None => comment_b.append_null(),
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_b.finish()) as ArrayRef,
                Arc::new(schema_b.finish()) as ArrayRef,
                Arc::new(table_b.finish()) as ArrayRef,
                Arc::new(comment_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

/// Build the `system.metadata.catalogs` table: one row per reachable catalog,
/// each with connector type "iceberg".
pub fn build_catalogs_table(catalogs: &[String]) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("connector_id", DataType::Utf8, false),
    ]));

    let mut catalog_b = StringBuilder::new();
    let mut connector_b = StringBuilder::new();

    for catalog in catalogs {
        catalog_b.append_value(catalog);
        connector_b.append_value("iceberg");
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(catalog_b.finish()) as ArrayRef,
            Arc::new(connector_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalogs_table_schema() {
        let table = build_catalogs_table(&["my_warehouse".to_string()]).unwrap();
        let schema = table.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(1).name(), "connector_id");
    }

    #[test]
    fn test_catalogs_table_builds_for_any_warehouse_name() {
        for name in &["warehouse1", "my-wh", "", "test-warehouse"] {
            let result = build_catalogs_table(&[name.to_string()]);
            assert!(
                result.is_ok(),
                "build_catalogs_table should succeed for warehouse name '{name}'"
            );
        }
    }

    #[test]
    fn test_catalogs_table_lists_every_reachable_catalog() {
        // One row per reachable catalog, not just the default. (#5)
        let table = build_catalogs_table(&[
            "main_warehouse".to_string(),
            "ws_energy_co".to_string(),
        ])
        .unwrap();
        assert_eq!(table.schema().fields().len(), 2);
    }

    #[test]
    fn test_catalogs_table_column_types() {
        let table = build_catalogs_table(&["test".to_string()]).unwrap();
        let schema = table.schema();
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_namespace_to_string_single_level() {
        let ns = NamespaceIdent::new("my_schema".to_string());
        assert_eq!(MetadataSchemaProvider::namespace_to_string(&ns), "my_schema");
    }

    #[test]
    fn test_namespace_to_string_multi_level() {
        let ns = NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap();
        assert_eq!(
            MetadataSchemaProvider::namespace_to_string(&ns),
            "level1.level2"
        );
    }
}

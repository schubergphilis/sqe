use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{error, warn};

use crate::rest_catalog::SessionCatalog;

/// DataFusion `SchemaProvider` for the virtual `system.metadata` schema.
///
/// Exposes metadata tables (`catalogs`, `table_properties`, `schema_properties`,
/// `table_comments`) that provide introspection into catalog and table-level
/// properties stored in Polaris / Iceberg.
pub struct MetadataSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
}

impl MetadataSchemaProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            session_catalog,
            warehouse,
        }
    }
}

impl std::fmt::Debug for MetadataSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSchemaProvider")
            .field("warehouse", &self.warehouse)
            .finish()
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
            "catalogs" => Ok(Some(build_catalogs_table(&self.warehouse)?)),
            "table_properties" => Ok(Some(self.build_table_properties_table().await?)),
            "schema_properties" => Ok(Some(self.build_schema_properties_table().await?)),
            "table_comments" => Ok(Some(self.build_table_comments_table().await?)),
            _ => Ok(None),
        }
    }
}

impl MetadataSchemaProvider {
    /// List namespaces, returning an empty vec on error (with a log message).
    async fn list_namespaces_safe(&self) -> Vec<NamespaceIdent> {
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => namespaces,
            Err(e) => {
                error!(error = %e, "Failed to list namespaces for system.metadata");
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

        let namespaces = self.list_namespaces_safe().await;

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut table_b = StringBuilder::new();
        let mut prop_name_b = StringBuilder::new();
        let mut prop_value_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_str = Self::namespace_to_string(ns);
            let tables = match self.session_catalog.list_tables(ns).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(namespace = %ns_str, error = %e, "Failed to list tables for system.metadata.table_properties");
                    continue;
                }
            };

            for table_ident in &tables {
                let full_ident =
                    iceberg::TableIdent::new(ns.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(table = %table_ident.name(), error = %e, "Failed to load table for system.metadata.table_properties");
                        continue;
                    }
                };

                let properties = table.metadata().properties();
                for (key, value) in properties {
                    catalog_b.append_value(&self.warehouse);
                    schema_b.append_value(&ns_str);
                    table_b.append_value(table_ident.name());
                    prop_name_b.append_value(key);
                    prop_value_b.append_value(value);
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

        let namespaces = self.list_namespaces_safe().await;

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut prop_name_b = StringBuilder::new();
        let mut prop_value_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_str = Self::namespace_to_string(ns);
            match self.session_catalog.get_namespace(ns).await {
                Ok(namespace) => {
                    let properties = namespace.properties();
                    for (key, value) in properties {
                        catalog_b.append_value(&self.warehouse);
                        schema_b.append_value(&ns_str);
                        prop_name_b.append_value(key);
                        prop_value_b.append_value(value);
                    }
                }
                Err(e) => {
                    warn!(namespace = %ns_str, error = %e, "Failed to get namespace for system.metadata.schema_properties");
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

        let namespaces = self.list_namespaces_safe().await;

        let mut catalog_b = StringBuilder::new();
        let mut schema_b = StringBuilder::new();
        let mut table_b = StringBuilder::new();
        let mut comment_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_str = Self::namespace_to_string(ns);
            let tables = match self.session_catalog.list_tables(ns).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(namespace = %ns_str, error = %e, "Failed to list tables for system.metadata.table_comments");
                    continue;
                }
            };

            for table_ident in &tables {
                let full_ident =
                    iceberg::TableIdent::new(ns.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(table = %table_ident.name(), error = %e, "Failed to load table for system.metadata.table_comments");
                        continue;
                    }
                };

                catalog_b.append_value(&self.warehouse);
                schema_b.append_value(&ns_str);
                table_b.append_value(table_ident.name());

                match table.metadata().properties().get("comment") {
                    Some(comment) => comment_b.append_value(comment),
                    None => comment_b.append_null(),
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

/// Build the static `system.metadata.catalogs` table with a single row.
///
/// Contains the warehouse name and connector type ("iceberg").
pub fn build_catalogs_table(warehouse: &str) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("connector_id", DataType::Utf8, false),
    ]));

    let mut catalog_b = StringBuilder::new();
    let mut connector_b = StringBuilder::new();

    catalog_b.append_value(warehouse);
    connector_b.append_value("iceberg");

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
        let table = build_catalogs_table("my_warehouse").unwrap();
        let schema = table.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(1).name(), "connector_id");
    }

    #[test]
    fn test_catalogs_table_builds_for_any_warehouse_name() {
        for name in &["warehouse1", "my-wh", "", "test-warehouse"] {
            let result = build_catalogs_table(name);
            assert!(
                result.is_ok(),
                "build_catalogs_table should succeed for warehouse name '{name}'"
            );
        }
    }

    #[test]
    fn test_catalogs_table_column_types() {
        let table = build_catalogs_table("test").unwrap();
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

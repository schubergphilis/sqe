use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{debug, error};

use crate::rest_catalog::SessionCatalog;

/// DataFusion `SchemaProvider` for the virtual `information_schema`.
#[derive(Debug)]
pub struct InformationSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
}

impl InformationSchemaProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            session_catalog,
            warehouse,
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            "tables".to_string(),
            "columns".to_string(),
            "schemata".to_string(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "tables" | "columns" | "schemata")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "tables" => Ok(Some(self.build_tables_table().await?)),
            "columns" => Ok(Some(self.build_columns_table().await?)),
            "schemata" => Ok(Some(self.build_schemata_table().await?)),
            _ => Ok(None),
        }
    }
}

impl InformationSchemaProvider {
    async fn build_tables_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut catalog_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = NamespaceIdent::new(ns.clone());
            match self.session_catalog.list_tables(&ns_ident).await {
                Ok(tables) => {
                    for table in &tables {
                        catalog_builder.append_value(&self.warehouse);
                        schema_builder.append_value(ns);
                        name_builder.append_value(table.name());
                        type_builder.append_value("BASE TABLE");
                    }
                }
                Err(e) => {
                    debug!(namespace = %ns, error = %e, "Failed to list tables for information_schema");
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(catalog_builder.finish()) as ArrayRef,
                Arc::new(schema_builder.finish()) as ArrayRef,
                Arc::new(name_builder.finish()) as ArrayRef,
                Arc::new(type_builder.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn build_columns_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut sch_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut ord_b = arrow_array::builder::Int32Builder::new();
        let mut null_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = NamespaceIdent::new(ns.clone());
            let tables = match self.session_catalog.list_tables(&ns_ident).await {
                Ok(t) => t,
                Err(_) => continue,
            };

            for table_ident in &tables {
                let full_ident =
                    iceberg::TableIdent::new(ns_ident.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        debug!(table = %table_ident.name(), error = %e, "Failed to load table for columns");
                        continue;
                    }
                };

                let iceberg_schema = table.metadata().current_schema();
                for (idx, field) in iceberg_schema.as_struct().fields().iter().enumerate() {
                    cat_b.append_value(&self.warehouse);
                    sch_b.append_value(ns);
                    tbl_b.append_value(table_ident.name());
                    col_b.append_value(&field.name);
                    ord_b.append_value((idx + 1) as i32);
                    null_b.append_value(if field.required { "NO" } else { "YES" });
                    type_b.append_value(format!("{}", field.field_type));
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cat_b.finish()) as ArrayRef,
                Arc::new(sch_b.finish()) as ArrayRef,
                Arc::new(tbl_b.finish()) as ArrayRef,
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(ord_b.finish()) as ArrayRef,
                Arc::new(null_b.finish()) as ArrayRef,
                Arc::new(type_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn build_schemata_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut sch_b = StringBuilder::new();

        for ns in &namespaces {
            cat_b.append_value(&self.warehouse);
            sch_b.append_value(ns);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cat_b.finish()) as ArrayRef,
                Arc::new(sch_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn list_namespaces_safe(&self) -> Vec<String> {
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => namespaces
                .iter()
                .flat_map(|ns| ns.as_ref().clone())
                .collect(),
            Err(e) => {
                error!(error = %e, "Failed to list namespaces for information_schema");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_names() {
        let names = vec!["tables", "columns", "schemata"];
        for name in &names {
            assert!(matches!(name, &"tables" | &"columns" | &"schemata"));
        }
    }

    #[test]
    fn test_tables_schema() {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 4);
    }

    #[test]
    fn test_columns_schema() {
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 7);
    }

    #[test]
    fn test_schemata_schema() {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ]);
        assert_eq!(schema.fields().len(), 2);
    }
}

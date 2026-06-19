use std::fmt;
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

use sqe_core::SessionUser;
use sqe_policy::PolicyStore;

use crate::rest_catalog::SessionCatalog;

/// DataFusion `SchemaProvider` for the virtual `information_schema`.
///
/// When a `PolicyStore` and `SessionUser` are provided, restricted columns
/// are filtered out of `information_schema.columns` so that users cannot
/// discover column names they are not allowed to see.
pub struct InformationSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
    policy_store: Option<Arc<dyn PolicyStore>>,
    session_user: Option<SessionUser>,
    /// Namespace names resolved (and visibility-filtered) by the owning
    /// `SqeCatalogProvider` at construction. When present, `schemata`,
    /// `tables`, and `columns` derive from this list instead of issuing a
    /// second, unfiltered `listNamespaces` — keeping every metadata
    /// surface consistent with `SHOW SCHEMAS`. `None` falls back to a
    /// live listing (test/loose construction paths).
    cached_namespaces: Option<Vec<String>>,
}

impl fmt::Debug for InformationSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InformationSchemaProvider")
            .field("warehouse", &self.warehouse)
            .field("has_policy_store", &self.policy_store.is_some())
            .field("session_user", &self.session_user)
            .finish()
    }
}

impl InformationSchemaProvider {
    pub fn new(
        session_catalog: Arc<SessionCatalog>,
        warehouse: String,
        policy_store: Option<Arc<dyn PolicyStore>>,
        session_user: Option<SessionUser>,
    ) -> Self {
        Self {
            session_catalog,
            warehouse,
            policy_store,
            session_user,
            cached_namespaces: None,
        }
    }

    /// Use a pre-resolved (visibility-filtered) namespace list instead of
    /// re-listing live. See the `cached_namespaces` field doc.
    #[must_use = "with_cached_namespaces consumes self; bind the returned provider"]
    pub fn with_cached_namespaces(mut self, namespaces: Vec<String>) -> Self {
        self.cached_namespaces = Some(namespaces);
        self
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {

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
                    warn!(namespace = %ns, error = %e, "Failed to list tables for information_schema");
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
            Field::new("character_maximum_length", DataType::Int32, true),
            Field::new("numeric_precision", DataType::Int32, true),
            Field::new("numeric_scale", DataType::Int32, true),
            Field::new("datetime_precision", DataType::Int32, true),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("udt_name", DataType::Utf8, true),
        ]));

        let namespaces = self.list_namespaces_safe().await;

        let mut cat_b = StringBuilder::new();
        let mut sch_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut ord_b = arrow_array::builder::Int32Builder::new();
        let mut null_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        let mut char_max_b = arrow_array::builder::Int32Builder::new();
        let mut num_prec_b = arrow_array::builder::Int32Builder::new();
        let mut num_scale_b = arrow_array::builder::Int32Builder::new();
        let mut dt_prec_b = arrow_array::builder::Int32Builder::new();
        let mut default_b = StringBuilder::new();
        let mut udt_b = StringBuilder::new();

        for ns in &namespaces {
            let ns_ident = NamespaceIdent::new(ns.clone());
            let tables = match self.session_catalog.list_tables(&ns_ident).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(namespace = ?ns, error = %e, "Failed to list tables for columns");
                    continue;
                }
            };

            for table_ident in &tables {
                let full_ident =
                    iceberg::TableIdent::new(ns_ident.clone(), table_ident.name().to_string());
                let table = match self.session_catalog.load_table(&full_ident).await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(table = %table_ident.name(), error = %e, "Failed to load table for columns");
                        continue;
                    }
                };

                // Resolve restricted columns for this table when policy is active.
                // Fail closed on policy errors: skip the table entirely rather than leak
                // the column list when the policy backend is degraded.
                //
                // Match plan_rewriter's policy key: the namespace is the LAST dotted
                // component (resolve_policy_key uses rsplit('.').next()). Passing the
                // full dotted namespace here would miss the policy and leak restricted
                // column names in information_schema for multi-level namespaces.
                let policy_ns = ns.rsplit('.').next().unwrap_or(ns);
                let restricted_columns = match (&self.policy_store, &self.session_user) {
                    (Some(store), Some(user)) => {
                        match store.resolve(user, table_ident.name(), policy_ns).await {
                            Ok(policy) => policy.restricted_columns,
                            Err(e) => {
                                warn!(
                                    table = %table_ident.name(),
                                    namespace = %ns,
                                    error = %e,
                                    "Policy resolution failed for information_schema.columns; omitting table (fail-closed)"
                                );
                                continue;
                            }
                        }
                    }
                    _ => Vec::new(),
                };

                let iceberg_schema = table.metadata().current_schema();
                let mut ordinal = 0i32;
                for field in iceberg_schema.as_struct().fields().iter() {
                    // Filter out restricted columns so they are invisible
                    if restricted_columns.contains(&field.name) {
                        continue;
                    }
                    ordinal += 1;
                    let info = iceberg_to_sql_type_info(&field.field_type);
                    cat_b.append_value(&self.warehouse);
                    sch_b.append_value(ns);
                    tbl_b.append_value(table_ident.name());
                    col_b.append_value(&field.name);
                    ord_b.append_value(ordinal);
                    null_b.append_value(if field.required { "NO" } else { "YES" });
                    type_b.append_value(&info.data_type);
                    match info.character_maximum_length {
                        Some(v) => char_max_b.append_value(v),
                        None => char_max_b.append_null(),
                    }
                    match info.numeric_precision {
                        Some(v) => num_prec_b.append_value(v),
                        None => num_prec_b.append_null(),
                    }
                    match info.numeric_scale {
                        Some(v) => num_scale_b.append_value(v),
                        None => num_scale_b.append_null(),
                    }
                    match info.datetime_precision {
                        Some(v) => dt_prec_b.append_value(v),
                        None => dt_prec_b.append_null(),
                    }
                    default_b.append_null();
                    udt_b.append_value(&info.udt_name);
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
                Arc::new(char_max_b.finish()) as ArrayRef,
                Arc::new(num_prec_b.finish()) as ArrayRef,
                Arc::new(num_scale_b.finish()) as ArrayRef,
                Arc::new(dt_prec_b.finish()) as ArrayRef,
                Arc::new(default_b.finish()) as ArrayRef,
                Arc::new(udt_b.finish()) as ArrayRef,
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
        if let Some(ref cached) = self.cached_namespaces {
            return cached.clone();
        }
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => namespaces
                .iter()
                .map(|ns| ns.as_ref().iter().map(|s| s.as_str()).collect::<Vec<_>>().join("."))
                .collect(),
            Err(e) => {
                error!(error = %e, "Failed to list namespaces for information_schema");
                Vec::new()
            }
        }
    }
}

/// Resolved metadata for one `information_schema.columns` row.
struct SqlTypeInfo {
    data_type: String,
    udt_name: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    datetime_precision: Option<i32>,
}

/// Map an Iceberg `Type` to its SQL-standard `data_type` rendering and
/// the related precision / length attributes.
///
/// dbt-trino's `adapter.get_columns_in_relation` and any SQL-conforming
/// JDBC driver expect names like `bigint`, `varchar`, `decimal(p,s)`.
/// The previous handler emitted Iceberg's native display (`long`,
/// `string`, `timestamp`) which the dbt-trino dialect plugin did not
/// recognise, silently falling back to `VARCHAR` for every column.
fn iceberg_to_sql_type_info(t: &iceberg::spec::Type) -> SqlTypeInfo {
    use iceberg::spec::{PrimitiveType, Type};
    match t {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => SqlTypeInfo {
                data_type: "boolean".to_string(),
                udt_name: "boolean".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Int => SqlTypeInfo {
                data_type: "integer".to_string(),
                udt_name: "int4".to_string(),
                character_maximum_length: None,
                numeric_precision: Some(32),
                numeric_scale: Some(0),
                datetime_precision: None,
            },
            PrimitiveType::Long => SqlTypeInfo {
                data_type: "bigint".to_string(),
                udt_name: "int8".to_string(),
                character_maximum_length: None,
                numeric_precision: Some(64),
                numeric_scale: Some(0),
                datetime_precision: None,
            },
            PrimitiveType::Float => SqlTypeInfo {
                data_type: "real".to_string(),
                udt_name: "float4".to_string(),
                character_maximum_length: None,
                numeric_precision: Some(24),
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Double => SqlTypeInfo {
                data_type: "double precision".to_string(),
                udt_name: "float8".to_string(),
                character_maximum_length: None,
                numeric_precision: Some(53),
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Decimal { precision, scale } => SqlTypeInfo {
                data_type: format!("decimal({precision},{scale})"),
                udt_name: "numeric".to_string(),
                character_maximum_length: None,
                numeric_precision: Some(*precision as i32),
                numeric_scale: Some(*scale as i32),
                datetime_precision: None,
            },
            PrimitiveType::Date => SqlTypeInfo {
                data_type: "date".to_string(),
                udt_name: "date".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Time => SqlTypeInfo {
                data_type: "time".to_string(),
                udt_name: "time".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: Some(6),
            },
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => SqlTypeInfo {
                data_type: "timestamp".to_string(),
                udt_name: "timestamp".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: Some(6),
            },
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => SqlTypeInfo {
                data_type: "timestamp with time zone".to_string(),
                udt_name: "timestamptz".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: Some(6),
            },
            PrimitiveType::String => SqlTypeInfo {
                data_type: "varchar".to_string(),
                udt_name: "varchar".to_string(),
                // Iceberg has no enforced max length; use i32::MAX so dbt
                // round-trips treat columns as unbounded varchar.
                character_maximum_length: Some(i32::MAX),
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Uuid => SqlTypeInfo {
                data_type: "uuid".to_string(),
                udt_name: "uuid".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Fixed(size) => SqlTypeInfo {
                data_type: format!("varbinary({size})"),
                udt_name: "bytea".to_string(),
                character_maximum_length: Some(*size as i32),
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
            PrimitiveType::Binary => SqlTypeInfo {
                data_type: "varbinary".to_string(),
                udt_name: "bytea".to_string(),
                character_maximum_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
            },
        },
        Type::Struct(_) => SqlTypeInfo {
            data_type: "row".to_string(),
            udt_name: "row".to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        },
        Type::List(_) => SqlTypeInfo {
            data_type: "array".to_string(),
            udt_name: "array".to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        },
        Type::Map(_) => SqlTypeInfo {
            data_type: "map".to_string(),
            udt_name: "map".to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        },
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
        // 7 base columns plus 6 extension columns required by dbt-trino
        // and SQL-conforming JDBC drivers (issue #99).
        let schema = Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::Int32, true),
            Field::new("numeric_precision", DataType::Int32, true),
            Field::new("numeric_scale", DataType::Int32, true),
            Field::new("datetime_precision", DataType::Int32, true),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("udt_name", DataType::Utf8, true),
        ]);
        assert_eq!(schema.fields().len(), 13);
    }

    #[test]
    fn test_iceberg_to_sql_type_primitives() {
        use iceberg::spec::{PrimitiveType, Type};
        let cases: &[(Type, &str)] = &[
            (Type::Primitive(PrimitiveType::Boolean), "boolean"),
            (Type::Primitive(PrimitiveType::Int), "integer"),
            (Type::Primitive(PrimitiveType::Long), "bigint"),
            (Type::Primitive(PrimitiveType::Float), "real"),
            (Type::Primitive(PrimitiveType::Double), "double precision"),
            (Type::Primitive(PrimitiveType::String), "varchar"),
            (Type::Primitive(PrimitiveType::Date), "date"),
            (Type::Primitive(PrimitiveType::Time), "time"),
            (Type::Primitive(PrimitiveType::Timestamp), "timestamp"),
            (
                Type::Primitive(PrimitiveType::Timestamptz),
                "timestamp with time zone",
            ),
            (Type::Primitive(PrimitiveType::Binary), "varbinary"),
            (Type::Primitive(PrimitiveType::Uuid), "uuid"),
        ];
        for (ty, expected) in cases {
            let info = super::iceberg_to_sql_type_info(ty);
            assert_eq!(&info.data_type, expected, "type {ty:?}");
        }
    }

    #[test]
    fn test_iceberg_to_sql_type_decimal() {
        use iceberg::spec::{PrimitiveType, Type};
        let info = super::iceberg_to_sql_type_info(&Type::Primitive(PrimitiveType::Decimal {
            precision: 18,
            scale: 2,
        }));
        assert_eq!(info.data_type, "decimal(18,2)");
        assert_eq!(info.numeric_precision, Some(18));
        assert_eq!(info.numeric_scale, Some(2));
    }

    #[test]
    fn test_iceberg_to_sql_type_string_has_max_length() {
        use iceberg::spec::{PrimitiveType, Type};
        let info = super::iceberg_to_sql_type_info(&Type::Primitive(PrimitiveType::String));
        assert_eq!(info.character_maximum_length, Some(i32::MAX));
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

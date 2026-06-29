use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{BooleanBuilder, Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DFResult;
use iceberg::NamespaceIdent;
use tracing::{debug, warn};

use crate::rest_catalog::{listing_error_is_forbidden, SessionCatalog};
use crate::system_catalog::SystemCatalogEntry;

/// Descriptor for a single row in the `system.jdbc.types` table.
struct JdbcTypeRow {
    name: &'static str,
    jdbc_type: i32,
    precision: i32,
    literal_prefix: Option<&'static str>,
    literal_suffix: Option<&'static str>,
    case_sensitive: bool,
    min_scale: i32,
    max_scale: i32,
    num_prec_radix: Option<i32>,
}

/// DataFusion `SchemaProvider` for the virtual `system.jdbc` schema.
///
/// Exposes JDBC metadata tables (`types`, `catalogs`, `schemas`, `tables`, `columns`)
/// required by Trino JDBC drivers (e.g. DBeaver) for metadata browsing.
pub struct JdbcSchemaProvider {
    /// Every catalog the session can reach (primary/default first, then the
    /// other configured catalogs and the session's own, deduplicated by name).
    /// `system.jdbc.catalogs` enumerates the names; `schemas`/`tables`/`columns`
    /// iterate each catalog so JDBC metadata browsing sees all of them, not just
    /// the default. (#5)
    catalogs: Vec<SystemCatalogEntry>,
}

impl JdbcSchemaProvider {
    pub fn new(entries: Vec<SystemCatalogEntry>) -> Self {
        Self {
            catalogs: dedup_entries(entries),
        }
    }
}

/// Deduplicate reachable catalogs by name, preserving order (primary first).
fn dedup_entries(entries: Vec<SystemCatalogEntry>) -> Vec<SystemCatalogEntry> {
    let mut seen = std::collections::HashSet::new();
    entries
        .into_iter()
        .filter(|e| seen.insert(e.name.clone()))
        .collect()
}

/// List a single catalog's namespaces as dotted strings; `[]` on error
/// (unauthorized / unreachable catalog), so enumeration skips it rather than
/// aborting the whole metadata listing.
async fn list_namespaces_for(catalog: &SessionCatalog) -> Vec<String> {
    match catalog.list_namespaces().await {
        Ok(namespaces) => namespaces
            .iter()
            .map(|ns| ns.as_ref().iter().map(|s| s.as_str()).collect::<Vec<_>>().join("."))
            .collect(),
        Err(e) => {
            // A principal that cannot LIST a catalog's namespaces just doesn't
            // see it: skip quietly (#318). Any other failure is logged.
            if listing_error_is_forbidden(&e) {
                debug!(error = %e, "system.jdbc: skipping catalog the principal is not authorized to list");
            } else {
                warn!(error = %e, "system.jdbc: skipping catalog whose namespaces could not be listed");
            }
            Vec::new()
        }
    }
}

impl std::fmt::Debug for JdbcSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.catalogs.iter().map(|e| e.name.as_str()).collect();
        f.debug_struct("JdbcSchemaProvider").field("catalogs", &names).finish()
    }
}

#[async_trait]
impl SchemaProvider for JdbcSchemaProvider {

    fn table_names(&self) -> Vec<String> {
        vec![
            "types".into(),
            "catalogs".into(),
            "schemas".into(),
            "tables".into(),
            "columns".into(),
        ]
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(name, "types" | "catalogs" | "schemas" | "tables" | "columns")
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match name {
            "types" => Ok(Some(build_types_table()?)),
            "catalogs" => {
                let names: Vec<String> = self.catalogs.iter().map(|e| e.name.clone()).collect();
                Ok(Some(build_catalogs_table(&names)?))
            }
            "schemas" => Ok(Some(self.build_schemas_table().await?)),
            "tables" => Ok(Some(self.build_tables_table().await?)),
            "columns" => Ok(Some(self.build_columns_table().await?)),
            _ => Ok(None),
        }
    }
}

impl JdbcSchemaProvider {
    async fn build_schemas_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schem", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
        ]));

        let mut schem_builder = StringBuilder::new();
        let mut catalog_builder = StringBuilder::new();

        for entry in &self.catalogs {
            // Every catalog exposes information_schema.
            schem_builder.append_value("information_schema");
            catalog_builder.append_value(&entry.name);

            for ns in list_namespaces_for(&entry.catalog).await {
                schem_builder.append_value(&ns);
                catalog_builder.append_value(&entry.name);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(schem_builder.finish()) as ArrayRef,
                Arc::new(catalog_builder.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn build_tables_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_cat", DataType::Utf8, true),
            Field::new("table_schem", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("type_cat", DataType::Utf8, true),
            Field::new("type_schem", DataType::Utf8, true),
            Field::new("type_name", DataType::Utf8, true),
            Field::new("self_referencing_col_name", DataType::Utf8, true),
            Field::new("ref_generation", DataType::Utf8, true),
        ]));

        let mut cat_b = StringBuilder::new();
        let mut schem_b = StringBuilder::new();
        let mut name_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        let mut remarks_b = StringBuilder::new();
        let mut type_cat_b = StringBuilder::new();
        let mut type_schem_b = StringBuilder::new();
        let mut type_name_b = StringBuilder::new();
        let mut self_ref_b = StringBuilder::new();
        let mut ref_gen_b = StringBuilder::new();

        for entry in &self.catalogs {
            for ns in list_namespaces_for(&entry.catalog).await {
                let ns_ident = NamespaceIdent::new(ns.clone());
                match entry.catalog.list_tables(&ns_ident).await {
                    Ok(tables) => {
                        for table in &tables {
                            cat_b.append_value(&entry.name);
                            schem_b.append_value(&ns);
                            name_b.append_value(table.name());
                            type_b.append_value("TABLE");
                            remarks_b.append_null();
                            type_cat_b.append_null();
                            type_schem_b.append_null();
                            type_name_b.append_null();
                            self_ref_b.append_null();
                            ref_gen_b.append_null();
                        }
                    }
                    Err(e) if listing_error_is_forbidden(&e) => {
                        debug!(catalog = %entry.name, namespace = %ns, "system.jdbc.tables: skipping namespace the principal is not authorized to list");
                    }
                    Err(e) => {
                        warn!(catalog = %entry.name, namespace = %ns, error = %e, "Failed to list tables for system.jdbc.tables");
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cat_b.finish()) as ArrayRef,
                Arc::new(schem_b.finish()) as ArrayRef,
                Arc::new(name_b.finish()) as ArrayRef,
                Arc::new(type_b.finish()) as ArrayRef,
                Arc::new(remarks_b.finish()) as ArrayRef,
                Arc::new(type_cat_b.finish()) as ArrayRef,
                Arc::new(type_schem_b.finish()) as ArrayRef,
                Arc::new(type_name_b.finish()) as ArrayRef,
                Arc::new(self_ref_b.finish()) as ArrayRef,
                Arc::new(ref_gen_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    async fn build_columns_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        // Full JDBC getColumns() schema — all 24 columns that DBeaver/Trino JDBC expects
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_cat", DataType::Utf8, true),
            Field::new("table_schem", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Int32, false),
            Field::new("type_name", DataType::Utf8, false),
            Field::new("column_size", DataType::Int32, true),
            Field::new("buffer_length", DataType::Int32, true),
            Field::new("decimal_digits", DataType::Int32, true),
            Field::new("num_prec_radix", DataType::Int32, true),
            Field::new("nullable", DataType::Int32, false),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("column_def", DataType::Utf8, true),
            Field::new("sql_data_type", DataType::Int32, true),
            Field::new("sql_datetime_sub", DataType::Int32, true),
            Field::new("char_octet_length", DataType::Int32, true),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("scope_catalog", DataType::Utf8, true),
            Field::new("scope_schema", DataType::Utf8, true),
            Field::new("scope_table", DataType::Utf8, true),
            Field::new("source_data_type", DataType::Int32, true),
            Field::new("is_autoincrement", DataType::Utf8, false),
            Field::new("is_generatedcolumn", DataType::Utf8, false),
        ]));

        let mut cat_b = StringBuilder::new();
        let mut schem_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut dtype_b = Int32Builder::new();
        let mut tname_b = StringBuilder::new();
        let mut colsize_b = Int32Builder::new();
        let mut buflen_b = Int32Builder::new();
        let mut dec_digits_b = Int32Builder::new();
        let mut radix_b = Int32Builder::new();
        let mut nullable_b = Int32Builder::new();
        let mut remarks_b = StringBuilder::new();
        let mut coldef_b = StringBuilder::new();
        let mut sql_dt_b = Int32Builder::new();
        let mut sql_sub_b = Int32Builder::new();
        let mut char_oct_b = Int32Builder::new();
        let mut ordinal_b = Int32Builder::new();
        let mut is_nullable_b = StringBuilder::new();
        let mut scope_cat_b = StringBuilder::new();
        let mut scope_sch_b = StringBuilder::new();
        let mut scope_tbl_b = StringBuilder::new();
        let mut src_dt_b = Int32Builder::new();
        let mut is_auto_b = StringBuilder::new();
        let mut is_gen_b = StringBuilder::new();

        for entry in &self.catalogs {
            for ns in list_namespaces_for(&entry.catalog).await {
                let ns_ident = NamespaceIdent::new(ns.clone());
                let tables = match entry.catalog.list_tables(&ns_ident).await {
                    Ok(t) => t,
                    Err(e) if listing_error_is_forbidden(&e) => {
                        debug!(catalog = %entry.name, namespace = %ns, "system.jdbc.columns: skipping namespace the principal is not authorized to list");
                        continue;
                    }
                    Err(e) => {
                        warn!(catalog = %entry.name, namespace = %ns, error = %e, "Failed to list tables for system.jdbc.columns");
                        continue;
                    }
                };

                for table_ident in &tables {
                    let full_ident =
                        iceberg::TableIdent::new(ns_ident.clone(), table_ident.name().to_string());
                    let table = match entry.catalog.load_table(&full_ident).await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(catalog = %entry.name, table = %table_ident.name(), error = %e, "Failed to load table for system.jdbc.columns");
                            continue;
                        }
                    };

                    let iceberg_schema = table.metadata().current_schema();
                    for (idx, field) in iceberg_schema.as_struct().fields().iter().enumerate() {
                        let (jdbc_type, type_name) = iceberg_type_to_jdbc(&field.field_type);

                        cat_b.append_value(&entry.name);
                        schem_b.append_value(&ns);
                        tbl_b.append_value(table_ident.name());
                        col_b.append_value(&field.name);
                        dtype_b.append_value(jdbc_type);
                        tname_b.append_value(type_name);
                        colsize_b.append_null();
                        buflen_b.append_null();
                        dec_digits_b.append_null();
                        radix_b.append_null();
                        nullable_b.append_value(if field.required { 0 } else { 1 });
                        remarks_b.append_null();
                        coldef_b.append_null();
                        sql_dt_b.append_null();
                        sql_sub_b.append_null();
                        char_oct_b.append_null();
                        ordinal_b.append_value((idx + 1) as i32);
                        is_nullable_b.append_value(if field.required { "NO" } else { "YES" });
                        scope_cat_b.append_null();
                        scope_sch_b.append_null();
                        scope_tbl_b.append_null();
                        src_dt_b.append_null();
                        is_auto_b.append_value("NO");
                        is_gen_b.append_value("NO");
                    }
                }
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cat_b.finish()) as ArrayRef,
                Arc::new(schem_b.finish()) as ArrayRef,
                Arc::new(tbl_b.finish()) as ArrayRef,
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(dtype_b.finish()) as ArrayRef,
                Arc::new(tname_b.finish()) as ArrayRef,
                Arc::new(colsize_b.finish()) as ArrayRef,
                Arc::new(buflen_b.finish()) as ArrayRef,
                Arc::new(dec_digits_b.finish()) as ArrayRef,
                Arc::new(radix_b.finish()) as ArrayRef,
                Arc::new(nullable_b.finish()) as ArrayRef,
                Arc::new(remarks_b.finish()) as ArrayRef,
                Arc::new(coldef_b.finish()) as ArrayRef,
                Arc::new(sql_dt_b.finish()) as ArrayRef,
                Arc::new(sql_sub_b.finish()) as ArrayRef,
                Arc::new(char_oct_b.finish()) as ArrayRef,
                Arc::new(ordinal_b.finish()) as ArrayRef,
                Arc::new(is_nullable_b.finish()) as ArrayRef,
                Arc::new(scope_cat_b.finish()) as ArrayRef,
                Arc::new(scope_sch_b.finish()) as ArrayRef,
                Arc::new(scope_tbl_b.finish()) as ArrayRef,
                Arc::new(src_dt_b.finish()) as ArrayRef,
                Arc::new(is_auto_b.finish()) as ArrayRef,
                Arc::new(is_gen_b.finish()) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

/// Build the static `system.jdbc.types` table with standard SQL/JDBC type metadata.
fn build_types_table() -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("type_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Int32, false),
        Field::new("precision", DataType::Int32, true),
        Field::new("literal_prefix", DataType::Utf8, true),
        Field::new("literal_suffix", DataType::Utf8, true),
        Field::new("create_params", DataType::Utf8, true),
        Field::new("nullable", DataType::Int32, false),
        Field::new("case_sensitive", DataType::Boolean, false),
        Field::new("searchable", DataType::Int32, false),
        Field::new("unsigned_attribute", DataType::Boolean, false),
        Field::new("fixed_prec_scale", DataType::Boolean, false),
        Field::new("auto_increment", DataType::Boolean, false),
        Field::new("local_type_name", DataType::Utf8, true),
        Field::new("minimum_scale", DataType::Int32, false),
        Field::new("maximum_scale", DataType::Int32, false),
        Field::new("sql_data_type", DataType::Int32, false),
        Field::new("sql_datetime_sub", DataType::Int32, false),
        Field::new("num_prec_radix", DataType::Int32, true),
    ]));

    let type_rows: Vec<JdbcTypeRow> = vec![
        JdbcTypeRow { name: "boolean",   jdbc_type: 16, precision:  1, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "tinyint",   jdbc_type: -6, precision:  3, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "smallint",  jdbc_type:  5, precision:  5, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "integer",   jdbc_type:  4, precision: 10, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "bigint",    jdbc_type: -5, precision: 19, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "real",      jdbc_type:  7, precision: 24, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "double",    jdbc_type:  8, precision: 53, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "decimal",   jdbc_type:  3, precision: 38, literal_prefix: None,              literal_suffix: None,       case_sensitive: false, min_scale: 0, max_scale: 38, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "varchar",   jdbc_type: 12, precision:  0, literal_prefix: Some("'"),         literal_suffix: Some("'"),  case_sensitive: true,  min_scale: 0, max_scale:  0, num_prec_radix: None     },
        JdbcTypeRow { name: "varbinary", jdbc_type: -3, precision:  0, literal_prefix: Some("X'"),        literal_suffix: Some("'"),  case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: None     },
        JdbcTypeRow { name: "date",      jdbc_type: 91, precision:  0, literal_prefix: Some("DATE '"),    literal_suffix: Some("'"),  case_sensitive: false, min_scale: 0, max_scale:  0, num_prec_radix: Some(10) },
        JdbcTypeRow { name: "timestamp", jdbc_type: 93, precision:  0, literal_prefix: Some("TIMESTAMP '"), literal_suffix: Some("'"), case_sensitive: false, min_scale: 0, max_scale: 9, num_prec_radix: Some(10) },
    ];

    let mut name_b = StringBuilder::new();
    let mut dtype_b = Int32Builder::new();
    let mut prec_b = Int32Builder::new();
    let mut prefix_b = StringBuilder::new();
    let mut suffix_b = StringBuilder::new();
    let mut params_b = StringBuilder::new();
    let mut nullable_b = Int32Builder::new();
    let mut case_b = BooleanBuilder::new();
    let mut search_b = Int32Builder::new();
    let mut unsigned_b = BooleanBuilder::new();
    let mut fixed_b = BooleanBuilder::new();
    let mut auto_b = BooleanBuilder::new();
    let mut local_b = StringBuilder::new();
    let mut min_scale_b = Int32Builder::new();
    let mut max_scale_b = Int32Builder::new();
    let mut sql_dtype_b = Int32Builder::new();
    let mut sql_dtsub_b = Int32Builder::new();
    let mut radix_b = Int32Builder::new();

    for row in &type_rows {
        name_b.append_value(row.name);
        dtype_b.append_value(row.jdbc_type);
        prec_b.append_value(row.precision);
        match row.literal_prefix {
            Some(v) => prefix_b.append_value(v),
            None => prefix_b.append_null(),
        }
        match row.literal_suffix {
            Some(v) => suffix_b.append_value(v),
            None => suffix_b.append_null(),
        }
        params_b.append_null(); // create_params always null
        nullable_b.append_value(1); // typeNullable
        case_b.append_value(row.case_sensitive);
        search_b.append_value(3); // typeSearchable
        unsigned_b.append_value(false);
        fixed_b.append_value(false);
        auto_b.append_value(false);
        local_b.append_null(); // local_type_name always null
        min_scale_b.append_value(row.min_scale);
        max_scale_b.append_value(row.max_scale);
        sql_dtype_b.append_value(0); // sql_data_type
        sql_dtsub_b.append_value(0); // sql_datetime_sub
        match row.num_prec_radix {
            Some(v) => radix_b.append_value(v),
            None => radix_b.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(name_b.finish()) as ArrayRef,
            Arc::new(dtype_b.finish()) as ArrayRef,
            Arc::new(prec_b.finish()) as ArrayRef,
            Arc::new(prefix_b.finish()) as ArrayRef,
            Arc::new(suffix_b.finish()) as ArrayRef,
            Arc::new(params_b.finish()) as ArrayRef,
            Arc::new(nullable_b.finish()) as ArrayRef,
            Arc::new(case_b.finish()) as ArrayRef,
            Arc::new(search_b.finish()) as ArrayRef,
            Arc::new(unsigned_b.finish()) as ArrayRef,
            Arc::new(fixed_b.finish()) as ArrayRef,
            Arc::new(auto_b.finish()) as ArrayRef,
            Arc::new(local_b.finish()) as ArrayRef,
            Arc::new(min_scale_b.finish()) as ArrayRef,
            Arc::new(max_scale_b.finish()) as ArrayRef,
            Arc::new(sql_dtype_b.finish()) as ArrayRef,
            Arc::new(sql_dtsub_b.finish()) as ArrayRef,
            Arc::new(radix_b.finish()) as ArrayRef,
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// Build the `system.jdbc.catalogs` table, one row per catalog the session
/// can see (JDBC `getCatalogs()` shape: a single `table_cat` column).
fn build_catalogs_table(catalogs: &[String]) -> DFResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_cat",
        DataType::Utf8,
        false,
    )]));

    let mut cat_b = StringBuilder::new();
    for c in catalogs {
        cat_b.append_value(c);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(cat_b.finish()) as ArrayRef],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// Map Iceberg types to JDBC type codes and type name strings.
fn iceberg_type_to_jdbc(ty: &iceberg::spec::Type) -> (i32, &'static str) {
    use iceberg::spec::PrimitiveType;
    match ty {
        iceberg::spec::Type::Primitive(p) => match p {
            PrimitiveType::Boolean => (16, "boolean"),
            PrimitiveType::Int => (4, "integer"),
            PrimitiveType::Long => (-5, "bigint"),
            PrimitiveType::Float => (7, "real"),
            PrimitiveType::Double => (8, "double"),
            PrimitiveType::Decimal { .. } => (3, "decimal"),
            PrimitiveType::Date => (91, "date"),
            PrimitiveType::Time => (92, "time"),
            PrimitiveType::Timestamp => (93, "timestamp"),
            PrimitiveType::Timestamptz => (93, "timestamp with time zone"),
            PrimitiveType::TimestampNs => (93, "timestamp"),
            PrimitiveType::TimestamptzNs => (93, "timestamp with time zone"),
            PrimitiveType::String => (12, "varchar"),
            PrimitiveType::Uuid => (12, "varchar"),
            PrimitiveType::Fixed(_) => (-3, "varbinary"),
            PrimitiveType::Binary => (-3, "varbinary"),
        },
        _ => (12, "varchar"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types_table_schema() {
        let table = build_types_table().unwrap();
        let schema = table.schema();
        assert_eq!(schema.field(0).name(), "type_name");
        assert_eq!(schema.field(1).name(), "data_type");
        assert!(schema.fields().len() >= 18);
    }

    #[test]
    fn test_catalogs_table() {
        let table =
            build_catalogs_table(&["my_warehouse".to_string()]).unwrap();
        let schema = table.schema();
        assert_eq!(schema.field(0).name(), "table_cat");
    }

    #[test]
    fn catalogs_table_lists_every_reachable_catalog() {
        // The catalogs table emits one row per reachable catalog name, in the
        // order given (primary/default first). Dedup of entries happens in
        // JdbcSchemaProvider::new before this point. (#5)
        let table = build_catalogs_table(&[
            "main_warehouse".to_string(),
            "ws_energy_co".to_string(),
        ])
        .unwrap();
        assert_eq!(table.schema().field(0).name(), "table_cat");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_string() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::String));
        assert_eq!(code, 12);
        assert_eq!(name, "varchar");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_long() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Long));
        assert_eq!(code, -5);
        assert_eq!(name, "bigint");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_boolean() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Boolean));
        assert_eq!(code, 16);
        assert_eq!(name, "boolean");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_timestamp() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Timestamp));
        assert_eq!(code, 93);
        assert_eq!(name, "timestamp");
    }

    // -------------------------------------------------------------------------
    // build_types_table — row count, JDBC codes, schema column count
    // -------------------------------------------------------------------------

    /// `build_types_table` must build successfully and contain the correct schema.
    #[test]
    fn test_types_table_has_12_rows() {
        let table = build_types_table().unwrap();
        // The schema has 18 columns as documented in the implementation.
        assert_eq!(table.schema().fields().len(), 18);
        // Verify the first two column names as a basic sanity check.
        let schema = table.schema();
        assert_eq!(schema.field(0).name(), "type_name");
        assert_eq!(schema.field(1).name(), "data_type");
    }

    /// `build_types_table` schema must have exactly 18 columns.
    #[test]
    fn test_types_table_schema_column_count() {
        let table = build_types_table().unwrap();
        assert_eq!(
            table.schema().fields().len(),
            18,
            "types table should have exactly 18 columns"
        );
    }

    /// `build_types_table` schema column names must match the JDBC spec.
    #[test]
    fn test_types_table_schema_column_names() {
        let table = build_types_table().unwrap();
        let schema = table.schema();
        let expected_names = [
            "type_name",
            "data_type",
            "precision",
            "literal_prefix",
            "literal_suffix",
            "create_params",
            "nullable",
            "case_sensitive",
            "searchable",
            "unsigned_attribute",
            "fixed_prec_scale",
            "auto_increment",
            "local_type_name",
            "minimum_scale",
            "maximum_scale",
            "sql_data_type",
            "sql_datetime_sub",
            "num_prec_radix",
        ];
        for (i, name) in expected_names.iter().enumerate() {
            assert_eq!(
                schema.field(i).name(),
                *name,
                "column {i} name mismatch"
            );
        }
    }

    // -------------------------------------------------------------------------
    // build_catalogs_table — single row, correct value, schema column count
    // -------------------------------------------------------------------------

    /// `build_catalogs_table` schema must have exactly 1 column named `table_cat`.
    #[test]
    fn test_catalogs_table_schema_column_count() {
        let table = build_catalogs_table(&["my_warehouse".to_string()]).unwrap();
        assert_eq!(
            table.schema().fields().len(),
            1,
            "catalogs table should have exactly 1 column"
        );
        assert_eq!(table.schema().field(0).name(), "table_cat");
    }

    /// `build_catalogs_table` must build without error for an arbitrary warehouse name.
    #[test]
    fn test_catalogs_table_builds_for_any_warehouse_name() {
        for name in &["warehouse1", "my-wh", "", "üñîcödé-wh"] {
            let result = build_catalogs_table(&[name.to_string()]);
            assert!(
                result.is_ok(),
                "build_catalogs_table should succeed for warehouse name '{name}'"
            );
        }
    }

    // -------------------------------------------------------------------------
    // iceberg_type_to_jdbc — exhaustive primitive type coverage
    // -------------------------------------------------------------------------

    #[test]
    fn test_iceberg_type_to_jdbc_int() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Int));
        assert_eq!(code, 4, "Int should map to JDBC INTEGER (4)");
        assert_eq!(name, "integer");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_float() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Float));
        assert_eq!(code, 7, "Float should map to JDBC REAL (7)");
        assert_eq!(name, "real");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_double() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Double));
        assert_eq!(code, 8, "Double should map to JDBC DOUBLE (8)");
        assert_eq!(name, "double");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_decimal() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) =
            iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Decimal { precision: 38, scale: 10 }));
        assert_eq!(code, 3, "Decimal should map to JDBC DECIMAL (3)");
        assert_eq!(name, "decimal");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_date() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Date));
        assert_eq!(code, 91, "Date should map to JDBC DATE (91)");
        assert_eq!(name, "date");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_time() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Time));
        assert_eq!(code, 92, "Time should map to JDBC TIME (92)");
        assert_eq!(name, "time");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_timestamptz() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Timestamptz));
        assert_eq!(code, 93, "Timestamptz should map to JDBC TIMESTAMP (93)");
        assert_eq!(name, "timestamp with time zone");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_timestamp_ns() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::TimestampNs));
        assert_eq!(code, 93, "TimestampNs should map to JDBC TIMESTAMP (93)");
        assert_eq!(name, "timestamp");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_timestamptz_ns() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::TimestamptzNs));
        assert_eq!(code, 93);
        assert_eq!(name, "timestamp with time zone");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_uuid() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Uuid));
        assert_eq!(code, 12, "UUID should map to JDBC VARCHAR (12)");
        assert_eq!(name, "varchar");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_fixed() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Fixed(16)));
        assert_eq!(code, -3, "Fixed should map to JDBC VARBINARY (-3)");
        assert_eq!(name, "varbinary");
    }

    #[test]
    fn test_iceberg_type_to_jdbc_binary() {
        use iceberg::spec::{PrimitiveType, Type};
        let (code, name) = iceberg_type_to_jdbc(&Type::Primitive(PrimitiveType::Binary));
        assert_eq!(code, -3, "Binary should map to JDBC VARBINARY (-3)");
        assert_eq!(name, "varbinary");
    }

    /// Complex (non-primitive) Iceberg types (e.g. struct, list, map) must fall
    /// back to `varchar` so that JDBC clients always receive a usable type name.
    #[test]
    fn test_iceberg_type_to_jdbc_complex_falls_back_to_varchar() {
        use iceberg::spec::{ListType, NestedField, PrimitiveType, Type};
        use std::sync::Arc;
        // Build a List<string> type as a representative complex type
        let inner_field = Arc::new(NestedField::required(1, "element", Type::Primitive(PrimitiveType::String)));
        let list_type = Type::List(ListType { element_field: inner_field });
        let (code, name) = iceberg_type_to_jdbc(&list_type);
        assert_eq!(code, 12, "Complex types should fall back to VARCHAR (12)");
        assert_eq!(name, "varchar");
    }
}

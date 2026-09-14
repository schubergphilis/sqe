use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::common::ScalarValue;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::NamespaceIdent;
use tracing::{debug, warn};

use crate::rest_catalog::{listing_error_is_forbidden, SessionCatalog};

/// DataFusion `SchemaProvider` for the virtual `information_schema`.
///
/// Restricted columns are NOT filtered here (#665). The plan rewriter's
/// contract for a restricted column is "nullified in place, the column
/// survives in the schema", so the name is visible in every query result
/// and hiding it from `columns` would make the two surfaces disagree. An
/// earlier version filtered anyway, but it was dead code: with
/// `with_information_schema(true)` on every session, DataFusion's built-in
/// provider answers `information_schema.*` and this provider is only ever
/// listed, never queried for columns.
#[derive(Clone)]
pub struct InformationSchemaProvider {
    session_catalog: Arc<SessionCatalog>,
    warehouse: String,
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
            .finish()
    }
}

impl InformationSchemaProvider {
    pub fn new(session_catalog: Arc<SessionCatalog>, warehouse: String) -> Self {
        Self {
            session_catalog,
            warehouse,
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
            // `columns` is built lazily at scan time so a `table_schema = 'x'`
            // predicate lists one namespace instead of the whole catalog
            // (review 307).
            "columns" => Ok(Some(Arc::new(InformationSchemaColumns {
                inner: self.clone(),
            }))),
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

        let namespaces = self.list_namespaces_safe().await?;

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
                Err(e) if listing_error_is_forbidden(&e) => {
                    debug!(namespace = %ns, "information_schema.tables: skipping namespace the principal is not authorized to list");
                }
                Err(e) => {
                    return Err(catalog_unavailable("information_schema.tables", ns, &e));
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

    /// Arrow schema of `information_schema.columns`.
    fn columns_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
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
        ]))
    }

    /// Build the `columns` rows. With `namespace_filter` set only that
    /// namespace is listed (one `list_tables` instead of one per namespace).
    /// Each table is read through `SessionCatalog::load_table`, which serves
    /// a fresh metadata-cache hit without a round trip inside the
    /// authorization recheck window, so a warm catalog costs zero REST calls
    /// and a cold one costs one `load_table` per miss (review 307).
    ///
    /// Boxed at the definition (compile time, see
    /// docs/internal/plans/2026-09-07-compile-time-async-hubs.md) so the
    /// `TableProvider::scan` that awaits it owns no coroutine over
    /// DataFusion's `Expr` graph.
    fn build_columns_batch<'a>(
        &'a self,
        namespace_filter: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, DFResult<RecordBatch>> {
        Box::pin(self.build_columns_batch_inner(namespace_filter))
    }

    async fn build_columns_batch_inner(
        &self,
        namespace_filter: Option<&str>,
    ) -> DFResult<RecordBatch> {
        let schema = Self::columns_schema();

        let namespaces: Vec<String> = match namespace_filter {
            Some(ns) => self
                .list_namespaces_safe()
                .await?
                .into_iter()
                .filter(|n| n == ns)
                .collect(),
            None => self.list_namespaces_safe().await?,
        };

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
                Err(e) if listing_error_is_forbidden(&e) => {
                    debug!(namespace = ?ns, "information_schema.columns: skipping namespace the principal is not authorized to list");
                    continue;
                }
                Err(e) => {
                    return Err(catalog_unavailable("information_schema.columns", ns, &e));
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

                let iceberg_schema = table.metadata().current_schema();
                let mut ordinal = 0i32;
                for field in iceberg_schema.as_struct().fields().iter() {
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

        Ok(batch)
    }

    async fn build_schemata_table(&self) -> DFResult<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ]));

        let namespaces = self.list_namespaces_safe().await?;

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

    async fn list_namespaces_safe(&self) -> DFResult<Vec<String>> {
        if let Some(ref cached) = self.cached_namespaces {
            return Ok(cached.clone());
        }
        match self.session_catalog.list_namespaces().await {
            Ok(namespaces) => Ok(namespaces
                .iter()
                .map(|ns| {
                    ns.as_ref()
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
                .collect()),
            Err(e) if listing_error_is_forbidden(&e) => {
                debug!(error = %e, "information_schema: skipping catalog the principal is not authorized to list");
                Ok(Vec::new())
            }
            // An unreachable catalog contributes nothing, the same as one the
            // caller cannot see. information_schema spans every attached
            // catalog, so propagating here lets a single dead endpoint blind
            // the caller to every healthy one: a deployment that keeps an
            // Iceberg anchor configured but unreachable, and reads its real
            // tables from elsewhere, could not run `information_schema.tables`
            // at all. Warn rather than debug, because unreachable is worth
            // noticing even though it is survivable.
            Err(e) => {
                warn!(
                    error = %e,
                    "information_schema: catalog could not be listed and contributes no rows"
                );
                Ok(Vec::new())
            }
        }
    }
}

/// `information_schema.columns` as a filter-aware `TableProvider`.
///
/// DataFusion hands `scan()` the pushed-down predicates, so a
/// `WHERE table_schema = 'x'` (the dbt / JDBC `get_columns_in_relation`
/// shape) builds rows for one namespace instead of every namespace in the
/// catalog. The pushdown is `Inexact`: DataFusion still applies the filter
/// above the scan, so a wider result is never wrong, only slower.
#[derive(Debug)]
struct InformationSchemaColumns {
    inner: InformationSchemaProvider,
}

/// The namespace an `information_schema.columns` filter set pins, when a
/// conjunct is exactly `table_schema = '<literal>'` (either operand order).
/// Any other shape returns `None` and the full catalog is listed.
fn namespace_filter(filters: &[Expr]) -> Option<String> {
    fn literal_eq(col: &Expr, lit: &Expr) -> Option<String> {
        match (col, lit) {
            (Expr::Column(c), Expr::Literal(ScalarValue::Utf8(Some(v)), _))
            | (Expr::Column(c), Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _))
            | (Expr::Column(c), Expr::Literal(ScalarValue::Utf8View(Some(v)), _))
                if c.name == "table_schema" =>
            {
                Some(v.clone())
            }
            _ => None,
        }
    }
    filters.iter().find_map(|f| match f {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => literal_eq(left, right).or_else(|| literal_eq(right, left)),
        _ => None,
    })
}

#[async_trait]
impl TableProvider for InformationSchemaColumns {
    fn schema(&self) -> Arc<Schema> {
        InformationSchemaProvider::columns_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if namespace_filter(&[(*f).clone()]).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    // `async_trait`'s desugared form (compile time, see
    // docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the
    // namespace filter is extracted synchronously, so the async block below
    // captures no `Expr` and its `Send` proof does not walk DataFusion's
    // expression graph. Behaviour is unchanged.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> futures::future::BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let ns = namespace_filter(filters);
        if let Some(ns) = &ns {
            debug!(namespace = %ns, "information_schema.columns: namespace filter pushed down");
        }
        let schema = self.schema();
        Box::pin(async move {
            let batch = self.inner.build_columns_batch(ns.as_deref()).await?;
            // Type-erased before the await: a `MemTable` carries `Expr`
            // (column defaults), and holding one across `scan` would put
            // the expression graph back into this coroutine's `Send` proof.
            let mem: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
            mem.scan(state, projection, &[], limit).await
        })
    }
}

/// A namespace that could not be listed for a reason other than
/// authorization is an outage, not an empty schema: surface it as
/// `CatalogUnavailable` so clients and the audit log can tell the two apart
/// (review 806). Denied namespaces are skipped by the callers before this.
fn catalog_unavailable(surface: &str, namespace: &str, e: &sqe_core::SqeError) -> DataFusionError {
    DataFusionError::External(Box::new(sqe_core::SqeError::with_code(
        sqe_core::SqeErrorCode::CatalogUnavailable,
        format!("{surface}: namespace '{namespace}' could not be listed: {e}"),
    )))
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
        Type::Variant(_) => SqlTypeInfo {
            data_type: "variant".to_string(),
            udt_name: "variant".to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        },
    }
}

/// `InformationSchemaProvider` driven against a wiremock Polaris
/// (architecture review 313): rows and catalog call counts per table.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, StringArray};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{col, lit, SessionContext};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::rest_catalog::TableMetadataCache;

    const WAREHOUSE: &str = "test-warehouse";
    const NAMESPACES_PATH: &str = "/v1/test-warehouse/namespaces";

    fn tables_path(ns: &str) -> String {
        format!("/v1/test-warehouse/namespaces/{ns}/tables")
    }

    fn table_path(ns: &str, table: &str) -> String {
        format!("/v1/test-warehouse/namespaces/{ns}/tables/{table}")
    }

    /// `loadTable` body: `id` (required int) and `data` (optional string).
    fn load_table_body(table: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata-location": format!("s3://warehouse/{table}/metadata/00001.metadata.json"),
            "metadata": {
                "format-version": 2,
                "table-uuid": uuid::Uuid::new_v4().to_string(),
                "location": format!("s3://warehouse/{table}"),
                "last-sequence-number": 0,
                "last-updated-ms": 1_700_000_000_000_i64,
                "last-column-id": 2,
                "current-schema-id": 0,
                "schemas": [{
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "int"},
                        {"id": 2, "name": "data", "required": false, "type": "string"}
                    ]
                }],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {},
                "current-snapshot-id": -1,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": []
            },
            "config": {}
        })
    }

    fn identifiers(ns: &str, names: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "identifiers": names
                .iter()
                .map(|n| serde_json::json!({"namespace": [ns], "name": n}))
                .collect::<Vec<_>>()
        })
    }

    fn forbidden() -> ResponseTemplate {
        ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": {"message": "not authorized", "type": "NotAuthorizedException", "code": 403}
        }))
    }

    fn unavailable() -> ResponseTemplate {
        ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": {"message": "backend down", "type": "ServiceUnavailable", "code": 503}
        }))
    }

    async fn mount_json(server: &MockServer, url: &str, body: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path(url))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    /// Two namespaces, three tables, every table loadable: `sales.orders`,
    /// `sales.customers`, `hr.employees`.
    async fn polaris(server: &MockServer) {
        mount_json(
            server,
            "/v1/config",
            serde_json::json!({"overrides": {"prefix": WAREHOUSE}, "defaults": {}}),
        )
        .await;
        mount_json(
            server,
            NAMESPACES_PATH,
            serde_json::json!({"namespaces": [["sales"], ["hr"]]}),
        )
        .await;
        mount_json(
            server,
            &tables_path("sales"),
            identifiers("sales", &["orders", "customers"]),
        )
        .await;
        mount_json(
            server,
            &tables_path("hr"),
            identifiers("hr", &["employees"]),
        )
        .await;
        for (ns, table) in [
            ("sales", "orders"),
            ("sales", "customers"),
            ("hr", "employees"),
        ] {
            mount_json(server, &table_path(ns, table), load_table_body(table)).await;
        }
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(200))
            .mount(server)
            .await;
    }

    async fn session(
        server: &MockServer,
        token: &str,
        cache: TableMetadataCache,
    ) -> Arc<SessionCatalog> {
        Arc::new(
            SessionCatalog::new(
                &server.uri(),
                WAREHOUSE,
                token,
                &sqe_core::config::StorageConfig::default(),
                Some(cache),
                None,
                None,
                false,
            )
            .await
            .expect("construct session catalog"),
        )
    }

    async fn provider_for(server: &MockServer, token: &str) -> InformationSchemaProvider {
        let catalog = session(server, token, TableMetadataCache::new(0)).await;
        InformationSchemaProvider::new(catalog, WAREHOUSE.to_string())
    }

    /// Scan `table` of `provider` with `filters` and collect every row.
    async fn scan_rows(
        provider: &InformationSchemaProvider,
        table: &str,
        filters: &[Expr],
    ) -> DFResult<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let table = provider
            .table(table)
            .await?
            .expect("information_schema table exists");
        let plan = table.scan(&ctx.state(), None, filters, None).await?;
        collect(plan, ctx.task_ctx()).await
    }

    fn strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let array = b
                    .column_by_name(column)
                    .expect("column present")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 column");
                (0..array.len())
                    .map(|i| array.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn ints(batches: &[RecordBatch], column: &str) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|b| {
                let array = b
                    .column_by_name(column)
                    .expect("column present")
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 column");
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Requests the mock saw for `method_name` on `url_path`.
    async fn calls(server: &MockServer, method_name: &str, url_path: &str) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.method.as_str() == method_name && r.url.path() == url_path)
            .count()
    }

    #[tokio::test]
    async fn provider_exposes_exactly_the_three_tables() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-names").await;

        assert_eq!(provider.table_names(), ["tables", "columns", "schemata"]);
        for name in ["tables", "columns", "schemata"] {
            assert!(provider.table_exist(name));
            assert!(provider.table(name).await.unwrap().is_some());
        }
        assert!(!provider.table_exist("routines"));
        assert!(provider.table("routines").await.unwrap().is_none());
        assert_eq!(calls(&server, "GET", NAMESPACES_PATH).await, 2);
    }

    /// `schemata`: one row per namespace, one `listNamespaces` call, and no
    /// call at all when the provider carries a pre-resolved list.
    #[tokio::test]
    async fn schemata_lists_every_namespace_once() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-schemata").await;

        let batches = scan_rows(&provider, "schemata", &[]).await.unwrap();
        assert_eq!(row_count(&batches), 2);
        assert_eq!(strings(&batches, "schema_name"), ["sales", "hr"]);
        assert_eq!(strings(&batches, "catalog_name"), [WAREHOUSE, WAREHOUSE]);
        assert_eq!(batches[0].schema().fields().len(), 2);
        assert_eq!(calls(&server, "GET", NAMESPACES_PATH).await, 1);

        let cached = provider.with_cached_namespaces(vec!["only_this".to_string()]);
        let batches = scan_rows(&cached, "schemata", &[]).await.unwrap();
        assert_eq!(strings(&batches, "schema_name"), ["only_this"]);
        assert_eq!(
            calls(&server, "GET", NAMESPACES_PATH).await,
            1,
            "a cached namespace list must not re-list"
        );
    }

    /// `tables`: one row per table across every namespace, one `listTables`
    /// per namespace, no `loadTable`.
    #[tokio::test]
    async fn tables_lists_one_row_per_table_with_one_list_call_per_namespace() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-tables").await;

        let batches = scan_rows(&provider, "tables", &[]).await.unwrap();

        assert_eq!(row_count(&batches), 3);
        assert_eq!(batches[0].schema().fields().len(), 4);
        assert_eq!(
            strings(&batches, "table_name"),
            ["orders", "customers", "employees"]
        );
        assert_eq!(strings(&batches, "table_schema"), ["sales", "sales", "hr"]);
        assert!(strings(&batches, "table_type")
            .iter()
            .all(|t| t == "BASE TABLE"));
        assert_eq!(calls(&server, "GET", NAMESPACES_PATH).await, 1);
        assert_eq!(calls(&server, "GET", &tables_path("sales")).await, 1);
        assert_eq!(calls(&server, "GET", &tables_path("hr")).await, 1);
        assert_eq!(
            calls(&server, "GET", &table_path("sales", "orders")).await,
            0
        );
    }

    /// `tables`: a namespace the principal may not list is skipped; an
    /// outage on one namespace fails the whole table as `CatalogUnavailable`.
    /// (wiremock serves the first matching mount, so the override goes in
    /// before the baseline fixture.)
    #[tokio::test]
    async fn tables_skips_forbidden_namespaces_and_fails_on_outages() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(tables_path("hr")))
            .respond_with(forbidden())
            .mount(&server)
            .await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-tables-403").await;

        let batches = scan_rows(&provider, "tables", &[]).await.unwrap();
        assert_eq!(strings(&batches, "table_name"), ["orders", "customers"]);

        server.reset().await;
        Mock::given(method("GET"))
            .and(path(tables_path("hr")))
            .respond_with(unavailable())
            .mount(&server)
            .await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-tables-503").await;

        let err = scan_rows(&provider, "tables", &[])
            .await
            .expect_err("an outage is not an empty schema");
        let DataFusionError::External(inner) = err else {
            panic!("expected External, got {err:?}");
        };
        assert_eq!(
            inner
                .downcast_ref::<sqe_core::SqeError>()
                .expect("SqeError")
                .error_code(),
            sqe_core::SqeErrorCode::CatalogUnavailable
        );
    }

    /// `columns`: every column of every table with SQL-standard types, one
    /// `loadTable` per table (the metadata cache is disabled here).
    #[tokio::test]
    async fn columns_lists_every_column_and_loads_each_table_once() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-columns").await;

        let batches = scan_rows(&provider, "columns", &[]).await.unwrap();

        assert_eq!(row_count(&batches), 6);
        assert_eq!(batches[0].schema().fields().len(), 13);
        assert_eq!(
            strings(&batches, "table_name"),
            [
                "orders",
                "orders",
                "customers",
                "customers",
                "employees",
                "employees"
            ]
        );
        assert_eq!(
            strings(&batches, "column_name"),
            ["id", "data", "id", "data", "id", "data"]
        );
        assert_eq!(
            ints(&batches, "ordinal_position"),
            [Some(1), Some(2), Some(1), Some(2), Some(1), Some(2)]
        );
        assert_eq!(
            strings(&batches, "is_nullable"),
            ["NO", "YES", "NO", "YES", "NO", "YES"]
        );
        assert_eq!(
            strings(&batches, "data_type"),
            ["integer", "varchar", "integer", "varchar", "integer", "varchar"]
        );
        assert_eq!(ints(&batches, "numeric_precision")[..2], [Some(32), None]);
        assert_eq!(
            ints(&batches, "character_maximum_length")[..2],
            [None, Some(i32::MAX)]
        );
        for (ns, table) in [
            ("sales", "orders"),
            ("sales", "customers"),
            ("hr", "employees"),
        ] {
            assert_eq!(calls(&server, "GET", &table_path(ns, table)).await, 1);
        }
    }

    /// Review 307: `WHERE table_schema = 'hr'` lists only that namespace,
    /// so `listTables` and `loadTable` run for `hr` alone.
    #[tokio::test]
    async fn columns_with_table_schema_filter_lists_one_namespace() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-columns-filter").await;

        let batches = scan_rows(&provider, "columns", &[col("table_schema").eq(lit("hr"))])
            .await
            .unwrap();

        assert_eq!(row_count(&batches), 2);
        assert_eq!(strings(&batches, "table_schema"), ["hr", "hr"]);
        assert_eq!(calls(&server, "GET", &tables_path("hr")).await, 1);
        assert_eq!(calls(&server, "GET", &tables_path("sales")).await, 0);
        assert_eq!(
            calls(&server, "GET", &table_path("sales", "orders")).await,
            0
        );
        assert_eq!(
            calls(&server, "GET", &table_path("hr", "employees")).await,
            1
        );
    }

    /// Review 307: with a shared metadata cache inside the authorization
    /// recheck window, a second `columns` build costs zero `loadTable` GETs.
    #[tokio::test]
    async fn warm_cache_columns_build_costs_no_load_table_round_trips() {
        let server = MockServer::start().await;
        polaris(&server).await;
        let cache = TableMetadataCache::new(300).with_authorization_recheck(300);
        let catalog = session(&server, "info-schema-columns-warm", cache).await;
        let provider = InformationSchemaProvider::new(catalog, WAREHOUSE.to_string());

        scan_rows(&provider, "columns", &[]).await.unwrap();
        let cold = calls(&server, "GET", &table_path("sales", "orders")).await;
        assert_eq!(cold, 1);

        let batches = scan_rows(&provider, "columns", &[]).await.unwrap();
        assert_eq!(row_count(&batches), 6);
        assert_eq!(
            calls(&server, "GET", &table_path("sales", "orders")).await,
            cold
        );
        assert_eq!(
            calls(&server, "GET", &table_path("hr", "employees")).await,
            1
        );
    }

    /// `columns`: a table whose `loadTable` fails is omitted; the others are
    /// still listed.
    #[tokio::test]
    async fn columns_omits_a_table_whose_load_fails() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(table_path("sales", "customers")))
            .respond_with(unavailable())
            .mount(&server)
            .await;
        polaris(&server).await;
        let provider = provider_for(&server, "info-schema-columns-load-fail").await;

        let batches = scan_rows(&provider, "columns", &[]).await.unwrap();

        assert_eq!(
            strings(&batches, "table_name"),
            ["orders", "orders", "employees", "employees"]
        );
    }

    /// A catalog that cannot list namespaces contributes no rows to any of
    /// the three tables instead of failing them.
    #[tokio::test]
    async fn unlistable_catalog_contributes_no_rows() {
        let server = MockServer::start().await;
        mount_json(
            &server,
            "/v1/config",
            serde_json::json!({"overrides": {"prefix": WAREHOUSE}, "defaults": {}}),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(NAMESPACES_PATH))
            .respond_with(unavailable())
            .mount(&server)
            .await;
        let provider = provider_for(&server, "info-schema-unlistable").await;

        for table in ["schemata", "tables", "columns"] {
            let batches = scan_rows(&provider, table, &[]).await.unwrap();
            assert_eq!(row_count(&batches), 0, "{table}");
        }

        server.reset().await;
        mount_json(
            &server,
            "/v1/config",
            serde_json::json!({"overrides": {"prefix": WAREHOUSE}, "defaults": {}}),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(NAMESPACES_PATH))
            .respond_with(forbidden())
            .mount(&server)
            .await;
        let provider = provider_for(&server, "info-schema-forbidden").await;
        let batches = scan_rows(&provider, "schemata", &[]).await.unwrap();
        assert_eq!(row_count(&batches), 0);
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

    /// Review 307: `WHERE table_schema = 'x'` pins one namespace in either
    /// operand order; anything else lists the whole catalog.
    #[test]
    fn namespace_filter_recognises_table_schema_equality() {
        use datafusion::logical_expr::{col, lit};
        assert_eq!(
            namespace_filter(&[col("table_schema").eq(lit("analytics"))]),
            Some("analytics".to_string())
        );
        assert_eq!(
            namespace_filter(&[lit("analytics").eq(col("table_schema"))]),
            Some("analytics".to_string())
        );
        assert_eq!(namespace_filter(&[col("table_name").eq(lit("t"))]), None);
        assert_eq!(
            namespace_filter(&[col("table_schema").not_eq(lit("analytics"))]),
            None
        );
        assert_eq!(namespace_filter(&[]), None);
    }

    /// Review 806: a non-auth listing failure is `CatalogUnavailable`, never
    /// zero rows.
    #[test]
    fn listing_failure_maps_to_catalog_unavailable() {
        let err = catalog_unavailable(
            "information_schema.tables",
            "ns",
            &sqe_core::SqeError::Catalog("503 service unavailable".into()),
        );
        let DataFusionError::External(inner) = err else {
            panic!("expected External");
        };
        let sqe = inner
            .downcast_ref::<sqe_core::SqeError>()
            .expect("SqeError");
        assert_eq!(sqe.error_code(), sqe_core::SqeErrorCode::CatalogUnavailable);
    }
}

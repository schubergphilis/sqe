//! Unity Catalog's `information_schema` tag views:
//! `catalog_tags`, `schema_tags`, `table_tags` and `column_tags`.
//!
//! DataFusion answers every `information_schema.*` name with its own
//! built-in provider (`SessionState::schema_for_ref`), so these views cannot
//! be registered as tables. Instead the SQL front door rewrites
//! `cat.information_schema.column_tags` into a call of the table function
//! [`TAG_VIEW_FUNCTION`] (`sqe_sql::trino_compat`), the same way the
//! `"t$snapshots"` rewrite reaches the Iceberg metadata functions:
//!
//! ```sql
//! SELECT * FROM cat.information_schema.column_tags
//! -- becomes
//! SELECT * FROM sqe_information_schema_tags('column_tags', 'cat') AS column_tags
//! ```
//!
//! ## Rows
//!
//! `column_tags` reads the `sqe.column-tags` property of every table the
//! caller can load, through the session catalog and so with the caller's
//! own token: a table Polaris does not let the caller load is skipped, as
//! `SHOW TAGS ON` refuses it. A table the policy store denies the caller
//! entirely is skipped too ([`TagViewAccess`]), so the view never shows more
//! than a query of the table would. A key-only tag has `tag_value = ''`,
//! the form Databricks uses, and the form `SET TAGS ('k' = '')` stores.
//! A table whose `sqe.column-tags` cannot be read is left out with a
//! warning rather than failing the whole listing.
//!
//! `catalog_tags`, `schema_tags` and `table_tags` return no rows yet: SQE
//! keeps tags on columns only. They have Databricks' columns, so a client
//! that reads all four works today.
//!
//! Equality filters on `schema_name` and `table_name` limit which
//! namespaces are listed and which tables are loaded; DataFusion applies
//! them again (`Inexact`). Hive tables and Iceberg views are not listed.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{BinaryExpr, Expr};
use futures::{StreamExt, TryStreamExt};
use iceberg::{NamespaceIdent, TableIdent};
use tracing::{debug, warn};

use crate::iceberg_metadata_tvf::MetadataCatalogResolver;
use crate::rest_catalog::SessionCatalog;

/// The table function the front door rewrites the tag views into.
pub const TAG_VIEW_FUNCTION: &str = "sqe_information_schema_tags";

/// How many tables `column_tags` loads at once.
const LOAD_CONCURRENCY: usize = 8;

/// One of the four tag views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagView {
    Catalog,
    Schema,
    Table,
    Column,
}

impl TagView {
    /// Every view, in the order Databricks lists them.
    pub const ALL: [TagView; 4] = [Self::Catalog, Self::Schema, Self::Table, Self::Column];

    /// The view's name in `information_schema`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Catalog => "catalog_tags",
            Self::Schema => "schema_tags",
            Self::Table => "table_tags",
            Self::Column => "column_tags",
        }
    }

    /// The view called `name`, ignoring ASCII case.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.name().eq_ignore_ascii_case(name))
    }

    /// Databricks' columns for the view: the object's name parts, then
    /// `tag_name` and `tag_value`.
    pub fn schema(self) -> SchemaRef {
        let parts: &[&str] = match self {
            Self::Catalog => &["catalog_name"],
            Self::Schema => &["catalog_name", "schema_name"],
            Self::Table => &["catalog_name", "schema_name", "table_name"],
            Self::Column => &["catalog_name", "schema_name", "table_name", "column_name"],
        };
        let fields = parts
            .iter()
            .chain(&["tag_name", "tag_value"])
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>();
        Arc::new(Schema::new(fields))
    }
}

type AccessFuture = futures::future::BoxFuture<'static, bool>;
type AccessFn = dyn Fn(String, String, String, std::collections::HashMap<String, Vec<String>>) -> AccessFuture
    + Send
    + Sync;

/// Whether the caller may see a table at all, given
/// `(catalog, namespace, table, column tag keys)`. The coordinator builds it from its
/// policy store: `false` when the policy denies the caller every row, the
/// rule that makes a query of the table say "table not found".
#[derive(Clone)]
pub struct TagViewAccess(Arc<AccessFn>);

impl TagViewAccess {
    pub fn new<F>(visible: F) -> Self
    where
        F: Fn(
                String,
                String,
                String,
                std::collections::HashMap<String, Vec<String>>,
            ) -> AccessFuture
            + Send
            + Sync
            + 'static,
    {
        Self(Arc::new(visible))
    }
}

impl std::fmt::Debug for TagViewAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TagViewAccess")
    }
}

/// `sqe_information_schema_tags('<view>' [, '<catalog>'])`.
pub struct TagViewFunction {
    catalogs: MetadataCatalogResolver,
    access: Option<TagViewAccess>,
}

impl TagViewFunction {
    /// List tables from the catalog `catalogs` picks for the catalog
    /// argument (the session catalog when there is none).
    pub fn new(catalogs: MetadataCatalogResolver) -> Self {
        Self {
            catalogs,
            access: None,
        }
    }

    /// Skip the tables `access` says the caller may not see.
    pub fn with_access(mut self, access: Option<TagViewAccess>) -> Self {
        self.access = access;
        self
    }
}

impl std::fmt::Debug for TagViewFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TagViewFunction")
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

impl TableFunctionImpl for TagViewFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let (view, catalog_name) = match exprs {
            [view] => (string_literal(view), None),
            [view, catalog] => match string_literal(catalog) {
                Some(c) => (string_literal(view), Some(c)),
                None => (None, None),
            },
            _ => (None, None),
        };
        let Some(view) = view else {
            return plan_err!(
                "{TAG_VIEW_FUNCTION} expects a view name and an optional catalog name \
                 as string literals"
            );
        };
        let Some(view) = TagView::from_name(view) else {
            return plan_err!("{TAG_VIEW_FUNCTION}: unknown tag view '{view}'");
        };
        // The name the catalog is registered under, which policy keys on.
        let (catalog, catalog_name) = (self.catalogs)(catalog_name)?;
        Ok(Arc::new(TagViewProvider {
            view,
            schema: view.schema(),
            catalog,
            catalog_name,
            access: self.access.clone(),
        }))
    }
}

/// The provider behind one tag view. The listing happens in `scan`, which
/// is async; `call` only captures the catalog.
#[derive(Debug)]
pub struct TagViewProvider {
    view: TagView,
    schema: SchemaRef,
    catalog: Arc<SessionCatalog>,
    catalog_name: String,
    access: Option<TagViewAccess>,
}

impl TagViewProvider {
    /// The catalog this view lists, as the query named it.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// The view this provider answers.
    pub fn view(&self) -> TagView {
        self.view
    }
}

/// Whether `provider` answers a tag view. Its columns are catalog metadata,
/// not a table's columns, so it carries no column tags of its own; the
/// policy rewriter uses this to treat the scan as untagged.
pub fn is_tag_view_provider(provider: &dyn TableProvider) -> bool {
    tag_view_of(provider).is_some()
}

/// `(catalog, view name)` of a tag view scan, so the audit record and the
/// result cache see `cat.information_schema.column_tags` rather than the
/// function name.
pub fn tag_view_resource(provider: &dyn TableProvider) -> Option<(String, &'static str)> {
    tag_view_of(provider).map(|p| (p.catalog_name.clone(), p.view.name()))
}

fn tag_view_of(provider: &dyn TableProvider) -> Option<&TagViewProvider> {
    (provider as &dyn std::any::Any).downcast_ref::<TagViewProvider>()
}

/// The column a pushed-down equality filter compares, and the literal.
fn equality_filter(expr: &Expr) -> Option<(&str, &str)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), other) | (other, Expr::Column(c)) => {
            string_literal(other).map(|v| (c.name.as_str(), v))
        }
        _ => None,
    }
}

/// The `schema_name` and `table_name` an equality filter pins, if any.
fn pinned_names(filters: &[Expr]) -> (Option<String>, Option<String>) {
    let mut schema = None;
    let mut table = None;
    for (column, value) in filters.iter().filter_map(equality_filter) {
        match column {
            "schema_name" => schema = Some(value.to_string()),
            "table_name" => table = Some(value.to_string()),
            _ => {}
        }
    }
    (schema, table)
}

/// Not-found and forbidden: the caller cannot see the object, so it is left
/// out of the listing. Anything else fails the query rather than returning
/// a listing that is silently short.
fn hidden_from_caller(e: &sqe_core::SqeError) -> bool {
    e.is_not_found() || e.error_code() == sqe_core::SqeErrorCode::AccessDenied
}

/// One `column_tags` row.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnTagRow {
    schema: String,
    table: String,
    column: String,
    tag: String,
    value: String,
}

#[async_trait]
impl TableProvider for TagViewProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
            .map(|f| match equality_filter(f) {
                Some(("schema_name" | "table_name", _)) => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    // `async_trait`'s desugared form, as in `iceberg_metadata_tvf`: the
    // filters are read before the async block, so it captures no `Expr`
    // and its `Send` proof does not walk DataFusion's expression graph.
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
        let (schema_name, table_name) = pinned_names(filters);
        Box::pin(async move {
            let rows = match self.view {
                TagView::Column => self.column_tag_rows(schema_name, table_name).await?,
                // SQE keeps tags on columns only (until catalog, schema and
                // table tags land).
                TagView::Catalog | TagView::Schema | TagView::Table => Vec::new(),
            };
            let batch = self.batch(&rows)?;
            let mem: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(self.schema.clone(), vec![vec![batch]])?);
            mem.scan(state, projection, &[], limit).await
        })
    }
}

impl TagViewProvider {
    /// Every (column, tag) of every table the caller can load, sorted.
    async fn column_tag_rows(
        &self,
        schema_name: Option<String>,
        table_name: Option<String>,
    ) -> DFResult<Vec<ColumnTagRow>> {
        let fail = |what: &str, e: sqe_core::SqeError| {
            DataFusionError::Execution(format!("information_schema.column_tags: {what}: {e}"))
        };
        let namespaces = match schema_name {
            Some(name) => vec![NamespaceIdent::new(name)],
            None => self
                .catalog
                .list_namespaces()
                .await
                .map_err(|e| fail("failed to list schemas", e))?,
        };

        let mut idents: Vec<TableIdent> = Vec::new();
        for namespace in &namespaces {
            match self.catalog.list_tables(namespace).await {
                Ok(tables) => idents.extend(
                    tables
                        .into_iter()
                        .filter(|t| table_name.as_deref().is_none_or(|n| t.name() == n)),
                ),
                Err(e) if hidden_from_caller(&e) => {
                    debug!(namespace = ?namespace, error = %e, "column_tags: schema not listed");
                }
                Err(e) => return Err(fail("failed to list tables", e)),
            }
        }

        let per_table: Vec<Vec<ColumnTagRow>> = futures::stream::iter(idents)
            .map(|ident| self.table_rows(ident))
            .buffer_unordered(LOAD_CONCURRENCY)
            .try_collect()
            .await?;
        let mut rows: Vec<ColumnTagRow> = per_table.into_iter().flatten().collect();
        rows.sort_unstable();
        Ok(rows)
    }

    /// The rows of one table, or none when the caller cannot see it.
    fn table_rows(
        &self,
        ident: TableIdent,
    ) -> futures::future::BoxFuture<'_, DFResult<Vec<ColumnTagRow>>> {
        Box::pin(async move {
            let schema = ident.namespace().as_ref().join(".");
            let table_name = ident.name().to_string();
            let table = match self.catalog.load_table(&ident).await {
                Ok(table) => table,
                Err(e) if hidden_from_caller(&e) => {
                    debug!(table = %ident, error = %e, "column_tags: table not loaded");
                    return Ok(Vec::new());
                }
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "information_schema.column_tags: failed to load table '{ident}': {e}"
                    )))
                }
            };
            let Some(raw) = table
                .metadata()
                .properties()
                .get(sqe_core::column_tags::PROP_KEY)
            else {
                return Ok(Vec::new());
            };
            let tags = match sqe_core::column_tags::parse(raw) {
                Ok(tags) => tags,
                Err(e) => {
                    warn!(table = %ident, error = %e, "column_tags: unreadable sqe.column-tags, table left out");
                    return Ok(Vec::new());
                }
            };
            if let Some(access) = &self.access {
                let keys = sqe_core::column_tags::keys(&tags);
                if !(access.0)(
                    self.catalog_name.clone(),
                    schema.clone(),
                    table_name.clone(),
                    keys,
                )
                .await
                {
                    return Ok(Vec::new());
                }
            }
            Ok(tags
                .into_iter()
                .flat_map(|(column, tags)| {
                    let (schema, table_name) = (schema.clone(), table_name.clone());
                    tags.into_iter().map(move |tag| ColumnTagRow {
                        schema: schema.clone(),
                        table: table_name.clone(),
                        column: column.clone(),
                        tag: tag.key,
                        value: tag.value.unwrap_or_default(),
                    })
                })
                .collect())
        })
    }

    fn batch(&self, rows: &[ColumnTagRow]) -> DFResult<RecordBatch> {
        let mut builders: Vec<StringBuilder> = (0..self.schema.fields().len())
            .map(|_| StringBuilder::new())
            .collect();
        for row in rows {
            let values = [
                self.catalog_name.as_str(),
                &row.schema,
                &row.table,
                &row.column,
                &row.tag,
                &row.value,
            ];
            for (builder, value) in builders.iter_mut().zip(values) {
                builder.append_value(value);
            }
        }
        let columns: Vec<ArrayRef> = builders
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

#[cfg(test)]
// The tests call `TableFunctionImpl::call` directly, as DataFusion's planner
// does for this function.
#[allow(deprecated)]
mod tests {
    use std::collections::HashMap;

    use datafusion::prelude::{col, lit, SessionContext};
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{Catalog, CatalogBuilder, TableCreation};

    use super::*;

    async fn catalog_with(tables: &[(&str, &str, Option<&str>)]) -> Arc<SessionCatalog> {
        let catalog = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "memory",
                [("warehouse".to_string(), "memory:///wh".to_string())].into(),
            )
            .await
            .unwrap();
        for (ns, table, tags) in tables {
            let ns = NamespaceIdent::new(ns.to_string());
            if !catalog.namespace_exists(&ns).await.unwrap() {
                catalog
                    .create_namespace(&ns, Default::default())
                    .await
                    .unwrap();
            }
            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "card", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "email", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap();
            let properties: HashMap<String, String> = tags
                .map(|t| {
                    HashMap::from([(sqe_core::column_tags::PROP_KEY.to_string(), t.to_string())])
                })
                .unwrap_or_default();
            catalog
                .create_table(
                    &ns,
                    TableCreation::builder()
                        .name(table.to_string())
                        .schema(schema)
                        .properties(properties)
                        .build(),
                )
                .await
                .unwrap();
        }
        Arc::new(SessionCatalog::local(
            Arc::new(catalog),
            "lake",
            &Default::default(),
        ))
    }

    fn function(catalog: Arc<SessionCatalog>) -> TagViewFunction {
        TagViewFunction::new(Arc::new(move |name: Option<&str>| {
            let registered = name.unwrap_or("lake").to_string();
            Ok((Arc::clone(&catalog), registered))
        }))
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in batches {
            for r in 0..batch.num_rows() {
                out.push(
                    (0..batch.num_columns())
                        .map(|c| {
                            datafusion::arrow::util::display::array_value_to_string(
                                batch.column(c),
                                r,
                            )
                            .unwrap()
                        })
                        .collect(),
                );
            }
        }
        out
    }

    fn context(function: TagViewFunction) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf(TAG_VIEW_FUNCTION, Arc::new(function));
        ctx
    }

    #[test]
    fn views_have_databricks_columns() {
        let names = |v: TagView| {
            v.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(TagView::Column),
            [
                "catalog_name",
                "schema_name",
                "table_name",
                "column_name",
                "tag_name",
                "tag_value"
            ]
        );
        assert_eq!(
            names(TagView::Catalog),
            ["catalog_name", "tag_name", "tag_value"]
        );
        assert_eq!(
            names(TagView::Schema),
            ["catalog_name", "schema_name", "tag_name", "tag_value"]
        );
        assert_eq!(
            names(TagView::Table),
            [
                "catalog_name",
                "schema_name",
                "table_name",
                "tag_name",
                "tag_value"
            ]
        );
        assert_eq!(TagView::from_name("COLUMN_TAGS"), Some(TagView::Column));
        assert_eq!(TagView::from_name("columns"), None);
    }

    #[tokio::test]
    async fn column_tags_lists_keys_and_values() {
        let catalog = catalog_with(&[
            (
                "sales",
                "cards",
                Some(r#"{"card":[{"pci":"card"},"pii"],"email":["pii"]}"#),
            ),
            ("sales", "plain", None),
            ("hr", "people", Some(r#"{"email":[{"pii":"email"}]}"#)),
        ])
        .await;
        let ctx = context(function(catalog));
        let got = rows(
            &ctx,
            "SELECT * FROM sqe_information_schema_tags('column_tags', 'lake')",
        )
        .await;
        let want = [
            ["lake", "hr", "people", "email", "pii", "email"],
            ["lake", "sales", "cards", "card", "pci", "card"],
            ["lake", "sales", "cards", "card", "pii", ""],
            ["lake", "sales", "cards", "email", "pii", ""],
        ];
        assert_eq!(got, want.map(|r| r.map(str::to_string).to_vec()).to_vec());
    }

    #[tokio::test]
    async fn schema_and_table_filters_narrow_the_listing() {
        let catalog = catalog_with(&[
            ("sales", "cards", Some(r#"{"card":["pci"]}"#)),
            ("hr", "people", Some(r#"{"email":["pii"]}"#)),
        ])
        .await;
        let ctx = context(function(catalog));
        let got = rows(
            &ctx,
            "SELECT table_name, column_name FROM sqe_information_schema_tags('column_tags') \
             WHERE schema_name = 'hr' AND table_name = 'people'",
        )
        .await;
        assert_eq!(got, vec![vec!["people".to_string(), "email".to_string()]]);
        // A schema that does not exist is an empty listing, not an error.
        let none = rows(
            &ctx,
            "SELECT * FROM sqe_information_schema_tags('column_tags') WHERE schema_name = 'nope'",
        )
        .await;
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn equality_filters_are_inexact() {
        let catalog = catalog_with(&[]).await;
        let provider = function(catalog).call(&[lit("column_tags")]).unwrap();
        let schema_eq = col("schema_name").eq(lit("hr"));
        let tag_eq = col("tag_name").eq(lit("pii"));
        assert_eq!(
            provider
                .supports_filters_pushdown(&[&schema_eq, &tag_eq])
                .unwrap(),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported
            ]
        );
    }

    #[tokio::test]
    async fn a_denied_table_is_left_out() {
        let catalog = catalog_with(&[
            ("sales", "cards", Some(r#"{"card":["pci"]}"#)),
            ("hr", "people", Some(r#"{"email":["pii"]}"#)),
        ])
        .await;
        let access = TagViewAccess::new(|catalog, _ns, table, keys| {
            assert_eq!(catalog, "lake");
            assert!(keys.values().all(|tags| !tags.is_empty()));
            Box::pin(async move { table != "people" })
        });
        let ctx = context(function(catalog).with_access(Some(access)));
        let got = rows(
            &ctx,
            "SELECT schema_name, table_name FROM sqe_information_schema_tags('column_tags')",
        )
        .await;
        assert_eq!(got, vec![vec!["sales".to_string(), "cards".to_string()]]);
    }

    #[tokio::test]
    async fn an_unreadable_tag_property_leaves_the_table_out() {
        let catalog = catalog_with(&[
            ("sales", "bad", Some(r#"{"card":[{"a":"1","b":"2"}]}"#)),
            ("sales", "good", Some(r#"{"card":["pci"]}"#)),
        ])
        .await;
        let ctx = context(function(catalog));
        let got = rows(
            &ctx,
            "SELECT table_name FROM sqe_information_schema_tags('column_tags')",
        )
        .await;
        assert_eq!(got, vec![vec!["good".to_string()]]);
    }

    #[tokio::test]
    async fn object_tag_views_are_empty_with_their_columns() {
        let catalog = catalog_with(&[("sales", "cards", Some(r#"{"card":["pci"]}"#))]).await;
        let ctx = context(function(catalog));
        for view in ["catalog_tags", "schema_tags", "table_tags"] {
            let df = ctx
                .sql(&format!(
                    "SELECT * FROM sqe_information_schema_tags('{view}', 'lake')"
                ))
                .await
                .unwrap();
            assert_eq!(
                df.schema().fields().len(),
                TagView::from_name(view).unwrap().schema().fields().len()
            );
            assert_eq!(df.count().await.unwrap(), 0, "{view}");
        }
    }

    #[tokio::test]
    async fn bad_arguments_are_refused() {
        let catalog = catalog_with(&[]).await;
        let f = function(catalog);
        assert!(f.call(&[lit("columns")]).is_err());
        assert!(f.call(&[]).is_err());
        assert!(f.call(&[lit(1)]).is_err());
    }

    #[tokio::test]
    async fn the_marker_names_the_view() {
        let catalog = catalog_with(&[]).await;
        let provider = function(catalog)
            .call(&[lit("column_tags"), lit("lake")])
            .unwrap();
        assert!(is_tag_view_provider(provider.as_ref()));
        assert_eq!(
            tag_view_resource(provider.as_ref()),
            Some(("lake".to_string(), "column_tags"))
        );
    }
}

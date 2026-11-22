//! Lazy, filter-scoped tables for the catalog-walking `system.jdbc` and
//! `system.metadata` tables (#939).
//!
//! Those tables list namespaces and load tables in every reachable catalog.
//! Building their rows in `SchemaProvider::table()` made every lookup pay for
//! the whole walk: DataFusion's built-in `information_schema` asks each
//! listed table of every schema, `system` included, for its type or its
//! provider, so a dbt listing filtered to one schema loaded every Iceberg
//! table in the warehouse, and one table that could not be loaded stalled
//! it. A [`LazyMetaTable`] only knows its schema until it is scanned, and the
//! scan hands the equality filters on its catalog, schema and table columns
//! to the builder as a [`MetaScope`], so only the matching namespaces are
//! listed and only the matching tables loaded.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, Like, Operator};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use futures::future::BoxFuture;

use crate::system_catalog::SystemCatalogEntry;

/// The columns of a metadata table that name its catalog, schema and (for
/// per-table tables) table.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScopeColumns {
    pub catalog: &'static str,
    pub schema: &'static str,
    pub table: Option<&'static str>,
}

/// The exact catalogs, schemas and tables a query restricts a metadata
/// table to. `None` places no restriction. Values are only taken from a
/// filter that admits those strings and nothing else, so a scan built for
/// the scope holds every row the query can return; DataFusion still applies
/// the filters on top (they are pushed down as `Inexact`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct MetaScope {
    pub catalog: Option<BTreeSet<String>>,
    pub schema: Option<BTreeSet<String>>,
    pub table: Option<BTreeSet<String>>,
}

impl MetaScope {
    /// The scope a set of conjunct filters puts on `columns`: for each
    /// column, the intersection of the values every filter on it admits.
    pub(crate) fn from_filters(filters: &[Expr], columns: ScopeColumns) -> Self {
        let pick = |column: &str| {
            filters
                .iter()
                .filter_map(|f| exact_values(f, column))
                .reduce(|a, b| a.intersection(&b).cloned().collect())
        };
        Self {
            catalog: pick(columns.catalog),
            schema: pick(columns.schema),
            table: columns.table.and_then(pick),
        }
    }

    pub(crate) fn admits_catalog(&self, name: &str) -> bool {
        self.catalog.as_ref().is_none_or(|c| c.contains(name))
    }

    pub(crate) fn admits_schema(&self, name: &str) -> bool {
        self.schema.as_ref().is_none_or(|s| s.contains(name))
    }

    pub(crate) fn admits_table(&self, name: &str) -> bool {
        self.table.as_ref().is_none_or(|t| t.contains(name))
    }
}

/// The strings `filter` admits for `column`, or `None` when it admits
/// anything else:
/// - `column = 'v'` (either operand order);
/// - a case-sensitive, non-negated `column LIKE 'v'` whose pattern holds no
///   unescaped wildcard (the Trino JDBC driver sends schema and table
///   patterns as `LIKE '<escaped>' ESCAPE '\'`);
/// - a non-negated `column IN ('a', 'b')` of literals only (dbt's
///   `get_catalog` sends the schema list this way);
/// - an OR of those on the same column (DataFusion's simplifier turns a
///   short IN list into one).
fn exact_values(filter: &Expr, column: &str) -> Option<BTreeSet<String>> {
    match filter {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            let value = if column_name(left) == Some(column) {
                string_literal(right)
            } else if column_name(right) == Some(column) {
                string_literal(left)
            } else {
                None
            };
            Some(BTreeSet::from([value?]))
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let mut values = exact_values(left, column)?;
            values.extend(exact_values(right, column)?);
            Some(values)
        }
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) if column_name(expr) == Some(column) => list.iter().map(string_literal).collect(),
        Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            escape_char,
            case_insensitive: false,
        }) if column_name(expr) == Some(column) => Some(BTreeSet::from([unescape_like(
            &string_literal(pattern)?,
            *escape_char,
        )?])),
        _ => None,
    }
}

/// The column an expression reads, seen through a cast to another string
/// type (which keeps string equality).
fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Cast(Cast { expr, field }) if is_string_type(field.data_type()) => column_name(expr),
        _ => None,
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)),
            _,
        ) => Some(s.clone()),
        Expr::Cast(Cast { expr, field }) if is_string_type(field.data_type()) => {
            string_literal(expr)
        }
        _ => None,
    }
}

/// The literal a LIKE pattern matches when it matches exactly one string,
/// else `None`. Without an ESCAPE clause a backslash is left to the engine's
/// own reading, so such a pattern is not taken.
fn unescape_like(pattern: &str, escape: Option<char>) -> Option<String> {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            out.push(chars.next()?);
        } else if c == '%' || c == '_' || (escape.is_none() && c == '\\') {
            return None;
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// Builds a metadata table's rows for one scope.
pub(crate) type ScopedBuild =
    Arc<dyn Fn(MetaScope) -> BoxFuture<'static, DFResult<RecordBatch>> + Send + Sync>;

/// A [`LazyMetaTable`] whose rows `build` makes from `catalogs` and the
/// scan's scope.
pub(crate) fn lazy_table<F, Fut>(
    schema: SchemaRef,
    columns: ScopeColumns,
    catalogs: &[SystemCatalogEntry],
    build: F,
) -> Arc<dyn TableProvider>
where
    F: Fn(Vec<SystemCatalogEntry>, MetaScope) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = DFResult<RecordBatch>> + Send + 'static,
{
    let catalogs = catalogs.to_vec();
    let build: ScopedBuild = Arc::new(move |scope| Box::pin(build(catalogs.clone(), scope)));
    Arc::new(LazyMetaTable::new(schema, columns, build))
}

/// A metadata table whose rows are built when it is scanned, for the scope
/// the scan's filters set. `table()` and `schema()` do no catalog I/O.
pub(crate) struct LazyMetaTable {
    schema: SchemaRef,
    columns: ScopeColumns,
    build: ScopedBuild,
}

impl LazyMetaTable {
    pub(crate) fn new(schema: SchemaRef, columns: ScopeColumns, build: ScopedBuild) -> Self {
        Self {
            schema,
            columns,
            build,
        }
    }
}

impl std::fmt::Debug for LazyMetaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyMetaTable")
            .field("columns", &self.columns)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LazyMetaTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Every filter is `Inexact`: the scope only narrows what is built, and
    /// DataFusion applies each filter to the built rows as well.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    // Desugared on purpose, as in `read_json_buffer`: the scope is read from
    // `filters` before the future is made, so the future never captures an
    // `Expr` and its `Send` proof stays off DataFusion's expression graph.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let rows = (self.build)(MetaScope::from_filters(filters, self.columns));
        let schema = Arc::clone(&self.schema);
        Box::pin(async move {
            let batch = rows.await?;
            let table = MemTable::try_new(schema, vec![vec![batch]])?;
            table.scan(state, projection, &[], limit).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    const JDBC: ScopeColumns = ScopeColumns {
        catalog: "table_cat",
        schema: "table_schem",
        table: Some("table_name"),
    };

    fn set(values: &[&str]) -> Option<BTreeSet<String>> {
        Some(values.iter().map(|v| v.to_string()).collect())
    }

    fn like(column: &str, pattern: &str, escape: Option<char>) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(col(column)),
            Box::new(lit(pattern)),
            escape,
            false,
        ))
    }

    #[test]
    fn equality_in_either_order_sets_the_scope() {
        let scope = MetaScope::from_filters(
            &[
                col("table_schem").eq(lit("dbt897")),
                lit("orders").eq(col("table_name")),
                lit("lake").eq(col("table_cat")),
            ],
            JDBC,
        );
        assert_eq!(
            scope,
            MetaScope {
                catalog: set(&["lake"]),
                schema: set(&["dbt897"]),
                table: set(&["orders"]),
            }
        );
    }

    #[test]
    fn in_lists_and_same_column_ors_admit_each_value() {
        let schema = |filters: &[Expr]| MetaScope::from_filters(filters, JDBC).schema;
        let ab = set(&["a", "b"]);
        assert_eq!(
            schema(&[col("table_schem").in_list(vec![lit("a"), lit("b")], false)]),
            ab
        );
        assert_eq!(
            schema(&[col("table_schem")
                .eq(lit("a"))
                .or(lit("b").eq(col("table_schem")))]),
            ab
        );
        // Conjuncts on one column intersect.
        assert_eq!(
            schema(&[
                col("table_schem").in_list(vec![lit("a"), lit("b"), lit("c")], false),
                col("table_schem").in_list(vec![lit("b"), lit("d")], false),
            ]),
            set(&["b"])
        );
        // Contradicting conjuncts admit nothing.
        assert_eq!(
            schema(&[
                col("table_schem").eq(lit("a")),
                col("table_schem").eq(lit("b")),
            ]),
            Some(BTreeSet::new())
        );
    }

    #[test]
    fn escaped_like_is_exact_and_wildcards_are_not() {
        let exact = |f: Expr| MetaScope::from_filters(&[f], JDBC).schema;
        // The Trino JDBC driver escapes `_` in getTables / getColumns.
        assert_eq!(
            exact(like("table_schem", "dbt\\_897", Some('\\'))),
            set(&["dbt_897"])
        );
        assert_eq!(exact(like("table_schem", "sales", None)), set(&["sales"]));
        assert_eq!(exact(like("table_schem", "dbt_897", Some('\\'))), None);
        assert_eq!(exact(like("table_schem", "dbt%", Some('\\'))), None);
        assert_eq!(exact(like("table_schem", "a\\_b", None)), None);
        assert_eq!(exact(like("table_schem", "trailing\\", Some('\\'))), None);
    }

    #[test]
    fn other_shapes_leave_the_scope_open() {
        let open = |f: Expr| MetaScope::from_filters(&[f], JDBC);
        let ilike = Expr::Like(Like::new(
            false,
            Box::new(col("table_schem")),
            Box::new(lit("a")),
            None,
            true,
        ));
        let not_like = Expr::Like(Like::new(
            true,
            Box::new(col("table_schem")),
            Box::new(lit("a")),
            None,
            false,
        ));
        for filter in [
            ilike,
            not_like,
            col("table_schem").not_eq(lit("a")),
            col("table_schem")
                .eq(lit("a"))
                .or(col("table_name").eq(lit("b"))),
            col("table_schem").in_list(vec![lit("a"), lit("b")], true),
            col("table_schem").in_list(vec![lit("a"), col("table_name")], false),
            col("remarks").eq(lit("a")),
            col("table_schem").eq(col("table_name")),
        ] {
            assert_eq!(open(filter.clone()), MetaScope::default(), "{filter}");
        }
    }

    #[test]
    fn a_table_column_is_only_read_when_the_table_has_one() {
        let schemas = ScopeColumns {
            catalog: "table_catalog",
            schema: "table_schem",
            table: None,
        };
        let scope = MetaScope::from_filters(&[col("table_name").eq(lit("t"))], schemas);
        assert_eq!(scope, MetaScope::default());
    }

    /// End to end through SQL, over an in-memory Iceberg catalog that
    /// records every `list_tables` and `load_table` call.
    mod listing {
        use std::collections::HashMap;
        use std::sync::Mutex;

        use datafusion::arrow::util::display::array_value_to_string;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use iceberg::memory::MemoryCatalog;
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use iceberg::table::Table;
        use iceberg::{
            Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation,
            TableIdent,
        };

        use super::*;
        use crate::rest_catalog::SessionCatalog;
        use crate::system_catalog::SystemCatalogProvider;

        /// Delegates to a memory catalog, records `list:<ns>` and
        /// `load:<ns>.<table>`, and fails what the test names with a
        /// non-access error.
        #[derive(Debug)]
        struct RecordingCatalog {
            inner: MemoryCatalog,
            calls: Arc<Mutex<Vec<String>>>,
            broken_table: Option<&'static str>,
            broken_listing: Option<&'static str>,
        }

        fn unavailable() -> iceberg::Error {
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "metadata file is gone")
        }

        fn dotted(ns: &NamespaceIdent) -> String {
            ns.as_ref().join(".")
        }

        #[async_trait]
        impl Catalog for RecordingCatalog {
            async fn list_namespaces(
                &self,
                parent: Option<&NamespaceIdent>,
            ) -> iceberg::Result<Vec<NamespaceIdent>> {
                self.inner.list_namespaces(parent).await
            }
            async fn create_namespace(
                &self,
                namespace: &NamespaceIdent,
                properties: HashMap<String, String>,
            ) -> iceberg::Result<Namespace> {
                self.inner.create_namespace(namespace, properties).await
            }
            async fn get_namespace(
                &self,
                namespace: &NamespaceIdent,
            ) -> iceberg::Result<Namespace> {
                self.inner.get_namespace(namespace).await
            }
            async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
                self.inner.namespace_exists(namespace).await
            }
            async fn update_namespace(
                &self,
                namespace: &NamespaceIdent,
                properties: HashMap<String, String>,
            ) -> iceberg::Result<()> {
                self.inner.update_namespace(namespace, properties).await
            }
            async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
                self.inner.drop_namespace(namespace).await
            }
            async fn list_tables(
                &self,
                namespace: &NamespaceIdent,
            ) -> iceberg::Result<Vec<TableIdent>> {
                let ns = dotted(namespace);
                self.calls.lock().unwrap().push(format!("list:{ns}"));
                if self.broken_listing == Some(ns.as_str()) {
                    return Err(unavailable());
                }
                self.inner.list_tables(namespace).await
            }
            async fn create_table(
                &self,
                namespace: &NamespaceIdent,
                creation: TableCreation,
            ) -> iceberg::Result<Table> {
                self.inner.create_table(namespace, creation).await
            }
            async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
                let name = format!("{}.{}", dotted(table.namespace()), table.name());
                self.calls.lock().unwrap().push(format!("load:{name}"));
                if self.broken_table == Some(name.as_str()) {
                    return Err(unavailable());
                }
                self.inner.load_table(table).await
            }
            async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
                self.inner.drop_table(table).await
            }
            async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
                self.inner.purge_table(table).await
            }
            async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
                self.inner.table_exists(table).await
            }
            async fn rename_table(
                &self,
                src: &TableIdent,
                dest: &TableIdent,
            ) -> iceberg::Result<()> {
                self.inner.rename_table(src, dest).await
            }
            async fn register_table(
                &self,
                table: &TableIdent,
                metadata_location: String,
            ) -> iceberg::Result<Table> {
                self.inner.register_table(table, metadata_location).await
            }
            async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
                self.inner.update_table(commit).await
            }
        }

        /// The fixture's tables: `a_x` and `abx` tell an escaped `_` from
        /// the LIKE wildcard.
        const TABLES: &[(&str, &str)] = &[
            ("a", "t1"),
            ("a", "t2"),
            ("a_x", "t3"),
            ("abx", "t4"),
            ("b", "t5"),
        ];

        struct Fixture {
            ctx: SessionContext,
            calls: Arc<Mutex<Vec<String>>>,
        }

        impl Fixture {
            /// `warehouse` must be unique per test: the circuit breaker of a
            /// local catalog is shared per warehouse name, and the failing
            /// loads here must not trip another test's.
            async fn new(
                warehouse: &str,
                broken_table: Option<&'static str>,
                broken_listing: Option<&'static str>,
            ) -> Self {
                let inner = iceberg::memory::MemoryCatalogBuilder::default()
                    .load(
                        "memory",
                        [("warehouse".to_string(), format!("memory:///{warehouse}"))].into(),
                    )
                    .await
                    .unwrap();
                for (ns, table) in TABLES {
                    let ns = NamespaceIdent::new(ns.to_string());
                    if !inner.namespace_exists(&ns).await.unwrap() {
                        let props = [("owner".to_string(), "data".to_string())].into();
                        inner.create_namespace(&ns, props).await.unwrap();
                    }
                    let schema = IcebergSchema::builder()
                        .with_fields(vec![NestedField::required(
                            1,
                            "id",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()])
                        .build()
                        .unwrap();
                    let properties: HashMap<String, String> =
                        [("comment".to_string(), format!("{table} rows"))].into();
                    let creation = TableCreation::builder()
                        .name(table.to_string())
                        .schema(schema)
                        .properties(properties)
                        .build();
                    inner.create_table(&ns, creation).await.unwrap();
                }
                let calls = Arc::new(Mutex::new(Vec::new()));
                let recording = RecordingCatalog {
                    inner,
                    calls: Arc::clone(&calls),
                    broken_table,
                    broken_listing,
                };
                let catalog =
                    SessionCatalog::local(Arc::new(recording), warehouse, &Default::default());
                let entry = SystemCatalogEntry {
                    name: "lake".to_string(),
                    catalog: Arc::new(catalog),
                };
                let ctx = SessionContext::new_with_config(
                    SessionConfig::new().with_information_schema(true),
                );
                ctx.register_catalog("system", Arc::new(SystemCatalogProvider::new(vec![entry])));
                Self { ctx, calls }
            }

            async fn rows(&self, sql: &str) -> DFResult<Vec<String>> {
                let batches = self.ctx.sql(sql).await?.collect().await?;
                let mut out = Vec::new();
                for batch in batches {
                    for r in 0..batch.num_rows() {
                        let row: Vec<String> = (0..batch.num_columns())
                            .map(|c| array_value_to_string(batch.column(c), r).unwrap())
                            .collect();
                        out.push(row.join("|"));
                    }
                }
                Ok(out)
            }

            /// The recorded calls, sorted, cleared for the next query.
            fn take_calls(&self) -> Vec<String> {
                let mut calls = std::mem::take(&mut *self.calls.lock().unwrap());
                calls.sort();
                calls
            }
        }

        fn loads(calls: &[String]) -> Vec<&str> {
            calls
                .iter()
                .filter_map(|c| c.strip_prefix("load:"))
                .collect()
        }

        /// The dbt-trino listing (#939): `information_schema` filtered to one
        /// schema used to build `system.jdbc.columns` and the metadata
        /// tables on lookup, loading every table in every namespace.
        #[tokio::test]
        async fn information_schema_lookup_loads_no_table() {
            let f = Fixture::new("sys_scope_939_info", None, None).await;
            let sql = "SELECT table_catalog, table_schema, table_name \
                       FROM information_schema.tables WHERE table_schema = 'jdbc' \
                       ORDER BY table_name LIMIT 3";
            let rows = f.rows(sql).await.unwrap();
            assert_eq!(
                rows,
                [
                    "system|jdbc|attributes",
                    "system|jdbc|catalogs",
                    "system|jdbc|columns"
                ]
            );
            let sql = "SELECT count(*) FROM information_schema.columns \
                       WHERE table_catalog = 'system'";
            assert_ne!(f.rows(sql).await.unwrap(), ["0"]);
            assert_eq!(f.take_calls(), Vec::<String>::new());
        }

        #[tokio::test]
        async fn columns_filtered_by_schema_load_only_that_schema() {
            let f = Fixture::new("sys_scope_939_cols", None, None).await;
            let sql = "SELECT table_schem, table_name, column_name, type_name \
                       FROM system.jdbc.columns WHERE table_schem = 'a' ORDER BY table_name";
            let rows = f.rows(sql).await.unwrap();
            assert_eq!(rows, ["a|t1|id|bigint", "a|t2|id|bigint"]);
            let calls = f.take_calls();
            assert_eq!(calls, ["list:a", "load:a.t1", "load:a.t2"]);

            // The Trino JDBC driver's getColumns shape: escaped LIKE patterns.
            let sql = "SELECT table_schem, table_name FROM system.jdbc.columns \
                       WHERE table_cat = 'lake' AND table_schem LIKE 'a\\_x' ESCAPE '\\' \
                       AND table_name LIKE 't3' ESCAPE '\\'";
            assert_eq!(f.rows(sql).await.unwrap(), ["a_x|t3"]);
            assert_eq!(f.take_calls(), ["list:a_x", "load:a_x.t3"]);

            // A wildcard is no scope: every namespace is read and the filter
            // still decides the rows.
            let sql = "SELECT table_schem, table_name FROM system.jdbc.columns \
                       WHERE table_schem LIKE 'a_x' ORDER BY 1";
            assert_eq!(f.rows(sql).await.unwrap(), ["a_x|t3", "abx|t4"]);
            assert_eq!(loads(&f.take_calls()).len(), TABLES.len());

            // Another catalog name: nothing is listed at all.
            let sql = "SELECT * FROM system.jdbc.columns WHERE table_cat = 'other'";
            assert!(f.rows(sql).await.unwrap().is_empty());
            assert_eq!(f.take_calls(), Vec::<String>::new());
        }

        #[tokio::test]
        async fn tables_and_schemas_filter_without_loading() {
            let f = Fixture::new("sys_scope_939_tables", None, None).await;
            let sql = "SELECT table_schem, table_name, table_type FROM system.jdbc.tables \
                       WHERE table_schem = 'a' AND table_name = 't2'";
            assert_eq!(f.rows(sql).await.unwrap(), ["a|t2|TABLE"]);
            assert_eq!(f.take_calls(), ["list:a"]);

            let sql = "SELECT table_catalog, table_schem FROM system.jdbc.schemas \
                       WHERE table_schem = 'b'";
            assert_eq!(f.rows(sql).await.unwrap(), ["lake|b"]);
            let sql = "SELECT table_schem FROM system.jdbc.schemas \
                       WHERE table_schem = 'information_schema'";
            assert_eq!(f.rows(sql).await.unwrap(), ["information_schema"]);
            assert_eq!(f.take_calls(), Vec::<String>::new());

            let sql = "SELECT schema_name, property_name, property_value \
                       FROM system.metadata.schema_properties WHERE schema_name = 'abx'";
            assert_eq!(f.rows(sql).await.unwrap(), ["abx|owner|data"]);
        }

        #[tokio::test]
        async fn table_comments_load_only_the_named_table() {
            let f = Fixture::new("sys_scope_939_comments", None, None).await;
            let sql =
                "SELECT schema_name, table_name, comment FROM system.metadata.table_comments \
                       WHERE schema_name = 'a' AND table_name = 't1'";
            assert_eq!(f.rows(sql).await.unwrap(), ["a|t1|t1 rows"]);
            assert_eq!(f.take_calls(), ["list:a", "load:a.t1"]);

            let sql = "SELECT table_name, property_value FROM system.metadata.table_properties \
                       WHERE schema_name = 'b' AND property_name = 'comment'";
            assert_eq!(f.rows(sql).await.unwrap(), ["t5|t5 rows"]);
            assert_eq!(f.take_calls(), ["list:b", "load:b.t5"]);
        }

        /// A table that cannot be loaded inside the filtered schema is
        /// skipped (and logged); its neighbours are still listed.
        #[tokio::test]
        async fn unloadable_table_in_scope_is_skipped() {
            let f = Fixture::new("sys_scope_939_broken", Some("a.t1"), None).await;
            let sql = "SELECT table_name FROM system.jdbc.columns WHERE table_schem = 'a'";
            assert_eq!(f.rows(sql).await.unwrap(), ["t2"]);
            assert_eq!(f.take_calls(), ["list:a", "load:a.t1", "load:a.t2"]);
            let sql = "SELECT table_name FROM system.metadata.table_comments \
                       WHERE schema_name = 'a'";
            assert_eq!(f.rows(sql).await.unwrap(), ["t2"]);
        }

        /// A namespace whose listing fails with anything but an access
        /// error still fails a listing that reaches it, and no longer
        /// fails one filtered to another schema.
        #[tokio::test]
        async fn failed_listing_fails_only_queries_that_reach_it() {
            let f = Fixture::new("sys_scope_939_listing", None, Some("b")).await;
            let sql = "SELECT table_name FROM system.jdbc.tables WHERE table_schem = 'a' \
                       ORDER BY 1";
            assert_eq!(f.rows(sql).await.unwrap(), ["t1", "t2"]);
            let sql = "SELECT count(*) FROM system.jdbc.tables";
            let err = f.rows(sql).await.unwrap_err().to_string();
            assert!(err.contains("metadata file is gone"), "{err}");
        }
    }
}

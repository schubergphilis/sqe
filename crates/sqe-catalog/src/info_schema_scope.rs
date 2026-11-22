//! A scoped walk for `information_schema.{tables,columns,views,schemata}`
//! (OpenSpec `information-schema-visible-catalogs`, D4 and D6).
//!
//! With `with_information_schema(true)`, DataFusion answers every
//! `information_schema` reference with its built-in provider, whatever the
//! qualifier. That provider walks every catalog, lists every schema and asks
//! every table for its type or its provider, then lets the query's filters
//! drop the rows it did not need. A dbt `get_columns_in_relation` for one
//! table in one catalog loaded every table of every catalog, and one table
//! the user may not read failed the whole listing.
//!
//! [`scope_information_schema`] swaps the source of those four scans for a
//! [`ScopedInformationSchema`] right after planning. At scan time it reads
//! the catalog, schema and table conjuncts the optimizer pushed into it
//! ([`MetaScope`]), adds the reference's own catalog qualifier, and runs the
//! same built-in provider over a narrowed catalog list: only the admitted
//! catalogs, schemas and tables are listed or loaded. The filters stay
//! `Inexact`, so DataFusion still applies them to the rows built, and the
//! result is the rows the unscoped walk returned for the same query.
//!
//! The qualifier counts as a filter, as in Trino: `"db".information_schema.x`
//! lists catalog `db` only. The front door qualifies a bare reference with
//! the session catalog, so a bare read under a session catalog lists that
//! catalog only; a bare read with no session catalog still walks them all.
//!
//! D6: while enumerating, a table the user may not read
//! ([`SqeErrorCode::AccessDenied`]) is left out of the listing quietly.
//! Every other error (an unreachable catalog, a schema SQE cannot convert)
//! still fails the query (#890).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::information_schema::{InformationSchemaProvider, INFORMATION_SCHEMA};
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::{provider_as_source, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableScan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;
use futures::future::BoxFuture;
use sqe_core::{SqeError, SqeErrorCode};
use tracing::debug;

use crate::system_scope::{MetaScope, ScopeColumns};

/// The `information_schema` views whose rows come from walking the catalogs.
/// `df_settings`, `routines` and `parameters` walk nothing and are left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkedView {
    Tables,
    Columns,
    Views,
    Schemata,
}

impl WalkedView {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "tables" => Some(Self::Tables),
            "columns" => Some(Self::Columns),
            "views" => Some(Self::Views),
            "schemata" => Some(Self::Schemata),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Tables => "tables",
            Self::Columns => "columns",
            Self::Views => "views",
            Self::Schemata => "schemata",
        }
    }

    fn columns(self) -> ScopeColumns {
        match self {
            Self::Tables | Self::Columns | Self::Views => ScopeColumns {
                catalog: "table_catalog",
                schema: "table_schema",
                table: Some("table_name"),
            },
            Self::Schemata => ScopeColumns {
                catalog: "catalog_name",
                schema: "schema_name",
                table: None,
            },
        }
    }
}

/// Swap the source of every walked `information_schema` scan in `plan` for a
/// [`ScopedInformationSchema`]. A no-op when the session has no
/// `information_schema` (the scans then name a real schema).
pub(crate) fn scope_information_schema(
    plan: LogicalPlan,
    information_schema_enabled: bool,
) -> DFResult<LogicalPlan> {
    if !information_schema_enabled {
        return Ok(plan);
    }
    Ok(plan
        .transform_up_with_subqueries(|node| {
            let LogicalPlan::TableScan(scan) = node else {
                return Ok(Transformed::no(node));
            };
            let view = scan
                .table_name
                .schema()
                .filter(|s| s.eq_ignore_ascii_case(INFORMATION_SCHEMA))
                .and_then(|_| WalkedView::from_name(&scan.table_name.table().to_ascii_lowercase()));
            let Some(view) = view else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            let provider = ScopedInformationSchema {
                view,
                qualifier: scan.table_name.catalog().map(str::to_string),
                schema: scan.source.schema(),
                table_type: scan.source.table_type(),
            };
            Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
                source: provider_as_source(Arc::new(provider)),
                ..scan
            })))
        })?
        .data)
}

/// One walked `information_schema` view, built at scan time over the
/// catalogs, schemas and tables its filters and qualifier admit.
#[derive(Debug)]
pub(crate) struct ScopedInformationSchema {
    view: WalkedView,
    /// The reference's catalog (`"db".information_schema.x`); `None` for a
    /// bare reference, which walks every catalog the filters admit.
    qualifier: Option<String>,
    schema: SchemaRef,
    table_type: TableType,
}

#[async_trait]
impl TableProvider for ScopedInformationSchema {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        self.table_type
    }

    /// `Inexact`: the scope only narrows the walk; DataFusion applies each
    /// filter to the rows built as well.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    // Desugared on purpose, as in `LazyMetaTable::scan`: the scope is read
    // from `filters` before the future is made, so the future never captures
    // an `Expr` and its `Send` proof stays off DataFusion's expression graph.
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
        let mut scope = MetaScope::from_filters(filters, self.view.columns());
        if let Some(qualifier) = &self.qualifier {
            let admitted = scope.admits_catalog(qualifier);
            scope.catalog = Some(
                std::iter::once(qualifier.clone())
                    .filter(|_| admitted)
                    .collect(),
            );
        }
        let catalogs = ScopedCatalogList::new(state.catalog_list(), scope);
        let provider = InformationSchemaProvider::new(Arc::new(catalogs));
        let view = self.view.name();
        Box::pin(async move {
            let table = provider.table(view).await?.ok_or_else(|| {
                DataFusionError::Internal(format!("information_schema.{view} is not built in"))
            })?;
            table.scan(state, projection, &[], limit).await
        })
    }
}

/// The session's catalogs, narrowed to what a [`MetaScope`] admits.
#[derive(Debug)]
pub(crate) struct ScopedCatalogList {
    inner: Arc<dyn CatalogProviderList>,
    scope: Arc<MetaScope>,
}

impl ScopedCatalogList {
    pub(crate) fn new(inner: Arc<dyn CatalogProviderList>, scope: MetaScope) -> Self {
        Self {
            inner,
            scope: Arc::new(scope),
        }
    }
}

impl CatalogProviderList for ScopedCatalogList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner
            .catalog_names()
            .into_iter()
            .filter(|c| self.scope.admits_catalog(c))
            .collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let inner = self.inner.catalog(name)?;
        Some(Arc::new(ScopedCatalog {
            inner,
            scope: Arc::clone(&self.scope),
        }))
    }
}

/// One catalog, listing only the schemas the scope admits.
#[derive(Debug)]
struct ScopedCatalog {
    inner: Arc<dyn CatalogProvider>,
    scope: Arc<MetaScope>,
}

impl CatalogProvider for ScopedCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.inner
            .schema_names()
            .into_iter()
            .filter(|s| self.scope.admits_schema(s))
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let inner = self.inner.schema(name)?;
        Some(Arc::new(EnumerationSchemaProvider {
            inner,
            scope: Arc::clone(&self.scope),
        }))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.deregister_schema(name, cascade)
    }
}

/// D6: a schema as an `information_schema` walk sees it. It lists only the
/// tables the scope admits, and a table the user may not read is skipped
/// (`Ok(None)`) instead of failing the listing. Every other error passes
/// through.
#[derive(Debug)]
pub(crate) struct EnumerationSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    scope: Arc<MetaScope>,
}

impl EnumerationSchemaProvider {
    #[cfg(test)]
    pub(crate) fn new(inner: Arc<dyn SchemaProvider>, scope: MetaScope) -> Self {
        Self {
            inner,
            scope: Arc::new(scope),
        }
    }
}

#[async_trait]
impl SchemaProvider for EnumerationSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        self.inner
            .table_names()
            .into_iter()
            .filter(|t| self.scope.admits_table(t))
            .collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        skip_access_denied(name, self.inner.table(name).await)
    }

    async fn table_type(&self, name: &str) -> DFResult<Option<TableType>> {
        skip_access_denied(name, self.inner.table_type(name).await)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

fn skip_access_denied<T>(table: &str, result: DFResult<Option<T>>) -> DFResult<Option<T>> {
    match result {
        Err(e) if is_access_denied(&e) => {
            debug!(table, error = %e, "information_schema: no access, table left out");
            Ok(None)
        }
        other => other,
    }
}

/// Whether `error` is the catalog refusing the user (a 403): a typed
/// [`SqeError`] whose code is [`SqeErrorCode::AccessDenied`], under any
/// number of DataFusion `External`, `Context` and `Shared` wrappers.
fn is_access_denied(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::External(inner) => {
            if let Some(sqe) = inner.downcast_ref::<SqeError>() {
                return sqe.error_code() == SqeErrorCode::AccessDenied;
            }
            inner
                .downcast_ref::<DataFusionError>()
                .is_some_and(is_access_denied)
        }
        DataFusionError::Context(_, inner) => is_access_denied(inner),
        DataFusionError::Shared(inner) => is_access_denied(inner),
        _ => false,
    }
}

#[cfg(test)]
#[path = "info_schema_scope_tests.rs"]
mod tests;

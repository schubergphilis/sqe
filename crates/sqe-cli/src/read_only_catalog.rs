//! Read-only view of a catalog attached with `--catalog-backend glue|s3tables`.
//!
//! `IcebergCatalogProvider` is writable: its schemas create an Iceberg table on
//! `register_table` (so `CREATE TABLE` and `CREATE VIEW` create tables), drop
//! the real table on `deregister_table`, and its tables accept `insert_into`.
//! These wrappers keep every read and refuse every write, so embedded mode
//! never changes a shared Glue or S3 Tables catalog.
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, ScanArgs, ScanResult, SchemaProvider, Session, TableProvider,
};
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

fn refused(catalog: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "catalog `{catalog}` is attached read-only; use the server for writes"
    ))
}

#[derive(Debug)]
pub(crate) struct ReadOnlyCatalog {
    name: Arc<str>,
    inner: Arc<dyn CatalogProvider>,
}

impl ReadOnlyCatalog {
    pub(crate) fn wrap(name: &str, inner: Arc<dyn CatalogProvider>) -> Arc<dyn CatalogProvider> {
        Arc::new(Self {
            name: name.into(),
            inner,
        })
    }
}

impl CatalogProvider for ReadOnlyCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let inner = self.inner.schema(name)?;
        Some(Arc::new(ReadOnlySchema {
            catalog: Arc::clone(&self.name),
            inner,
        }))
    }
    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(refused(&self.name))
    }
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(refused(&self.name))
    }
}

#[derive(Debug)]
struct ReadOnlySchema {
    catalog: Arc<str>,
    inner: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl SchemaProvider for ReadOnlySchema {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.inner.table(name).await?.map(|inner| {
            Arc::new(ReadOnlyTable {
                catalog: Arc::clone(&self.catalog),
                inner,
            }) as Arc<dyn TableProvider>
        }))
    }
    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        self.inner.table_type(name).await
    }
    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(refused(&self.catalog))
    }
    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(refused(&self.catalog))
    }
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

#[derive(Debug)]
struct ReadOnlyTable {
    catalog: Arc<str>,
    inner: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for ReadOnlyTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }
    fn get_logical_plan(&'_ self) -> Option<std::borrow::Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(refused(&self.catalog))
    }
    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(refused(&self.catalog))
    }
    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(refused(&self.catalog))
    }
    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        Err(refused(&self.catalog))
    }
}

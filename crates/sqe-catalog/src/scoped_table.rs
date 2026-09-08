//! Bind file-reader inference, planning and execution to an immutable store scope.
//! Pools remain shared, but credentials and file metadata caches never cross TVFs.
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::sync::Arc;

pub(crate) fn inference_context(parent: Option<&RuntimeEnv>) -> Result<SessionContext> {
    let mut builder = RuntimeEnvBuilder::new()
        .with_cache_manager(crate::lazy_object_store::external_store_cache_config());
    if let Some(parent) = parent {
        builder.memory_pool = Some(Arc::clone(&parent.memory_pool));
        builder.disk_manager = Some(Arc::clone(&parent.disk_manager));
    }
    Ok(SessionContext::new_with_config_rt(
        Default::default(),
        Arc::new(builder.build()?),
    ))
}

#[derive(Debug)]
pub(crate) struct ScopedTable {
    inner: Arc<dyn TableProvider>,
    runtime: Arc<RuntimeEnv>,
}
impl ScopedTable {
    pub(crate) fn bind(
        inner: Arc<dyn TableProvider>,
        runtime: Arc<RuntimeEnv>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self { inner, runtime })
    }
}
#[async_trait]
impl TableProvider for ScopedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    // `async_trait`'s desugared form (compile time, see
    // docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the block
    // must own the scoped `SessionState` the inner scan borrows and needs
    // the `&[Expr]`, and proving either `Send` walks DataFusion's whole type
    // graph under this method's parameter environment (2.5 s). Both are
    // held as `SendErased`, which states the fact once. The downcast and the
    // state build stay synchronous, before the block, as before.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> futures::future::BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let Some(state) = state.as_any().downcast_ref::<SessionState>() else {
            return Box::pin(async {
                Err(DataFusionError::Internal(
                    "File reader requires a SessionState".into(),
                ))
            });
        };
        let scoped = crate::send_erased::SendErased::new(
            SessionStateBuilder::new_from_existing(state.clone())
                .with_runtime_env(Arc::clone(&self.runtime))
                .build(),
        );
        let filters = crate::send_erased::SendErased::new(filters);
        Box::pin(async move {
            let inner = self
                .inner
                .scan(scoped.get(), projection, filters.get(), limit)
                .await?;
            Ok(Arc::new(ScopedExec {
                inner,
                runtime: Arc::clone(&self.runtime),
            }) as Arc<dyn ExecutionPlan>)
        })
    }
}
#[derive(Debug)]
struct ScopedExec {
    inner: Arc<dyn ExecutionPlan>,
    runtime: Arc<RuntimeEnv>,
}
impl DisplayAs for ScopedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ScopedFileScan")
    }
}
impl ExecutionPlan for ScopedExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn name(&self) -> &str {
        "ScopedFileScan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ScopedFileScan requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            inner: Arc::clone(&children[0]),
            runtime: Arc::clone(&self.runtime),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let scoped = TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            Arc::clone(&self.runtime),
        );
        self.inner.execute(partition, Arc::new(scoped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use object_store::{memory::InMemory, ObjectStoreExt};

    async fn table(parent: &RuntimeEnv, value: i64) -> Arc<dyn TableProvider> {
        let ctx = inference_context(Some(parent)).unwrap();
        assert!(Arc::ptr_eq(
            &ctx.runtime_env().memory_pool,
            &parent.memory_pool
        ));
        assert!(Arc::ptr_eq(
            &ctx.runtime_env().disk_manager,
            &parent.disk_manager
        ));
        assert!(!Arc::ptr_eq(
            &ctx.runtime_env().cache_manager,
            &parent.cache_manager
        ));
        let store = Arc::new(InMemory::new());
        store
            .put(
                &"data.csv".into(),
                format!("value\n{value}\n").into_bytes().into(),
            )
            .await
            .unwrap();
        ctx.register_object_store(&url::Url::parse("s3://same-bucket").unwrap(), store);
        let path = ListingTableUrl::parse("s3://same-bucket/data.csv").unwrap();
        let options = ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
            .with_file_extension(".csv");
        let schema = options.infer_schema(&ctx.state(), &path).await.unwrap();
        let table = ListingTable::try_new(
            ListingTableConfig::new(path)
                .with_listing_options(options)
                .with_schema(schema),
        )
        .unwrap();
        ScopedTable::bind(Arc::new(table), ctx.runtime_env())
    }

    #[tokio::test]
    async fn identical_bucket_paths_keep_their_own_stores_through_execution() {
        let ctx = SessionContext::new();
        let first = table(&ctx.runtime_env(), 11).await;
        let second = table(&ctx.runtime_env(), 22).await;
        assert!(ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse("s3://same-bucket/").unwrap())
            .is_err());
        ctx.register_table("first", first).unwrap();
        ctx.register_table("second", second).unwrap();
        let batches = ctx
            .sql("SELECT value FROM first UNION ALL SELECT value FROM second ORDER BY value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![11, 22]);
    }
}

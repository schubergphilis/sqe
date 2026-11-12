//! DataFusion [`TableProvider`] that wraps a [`QuackClient`].
//!
//! The provider eagerly fetches the query result at construction time and
//! caches the `RecordBatch`es alongside the inferred schema. `scan()` then
//! delegates to an in-memory `MemTable`. This keeps the implementation
//! simple at the cost of buffering the whole result; for the TVF /
//! interactive workloads we target that trade-off is fine, and the
//! design leaves room to add a streaming variant later.
//!
//! Schema inference comes from the first batch DuckDB sends back, which
//! always exists for non-empty queries (every PrepareResponse carries
//! `result_names` plus the first DataChunk).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::{DataSourceExec, MemorySourceConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;

use crate::{ClientError, QuackClient, QuackTargetPolicy};

/// Wraps a remote Quack query as a DataFusion table.
///
/// Use [`QuackTableProvider::new`] to build one directly from Rust; the
/// [`crate::QuackQueryTvf`] does the same thing from SQL via a TVF.
#[derive(Debug)]
pub struct QuackTableProvider {
    schema: SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
}

impl QuackTableProvider {
    /// Connect to `uri`, run `sql`, cache the result. The destination must
    /// pass `policy` before a socket is opened and the decoded result is capped
    /// by `policy.max_result_bytes`. This is the constructor the SQL-facing
    /// TVF uses (review finding 604).
    pub fn new_with_policy(
        uri: &str,
        token: Option<&str>,
        sql: &str,
        policy: QuackTargetPolicy,
    ) -> Result<Self, ClientError> {
        let mut client = QuackClient::connect_with_policy(uri, token, policy)?;
        let result = client.execute(sql)?;
        let _ = client.disconnect();
        // Schema comes from `PrepareResponse.result_types`, not from the
        // first batch — that lets zero-row results (e.g. WHERE 1=0) still
        // expose a valid table to DataFusion.
        Ok(Self {
            schema: result.schema,
            batches: result.batches,
        })
    }
}

#[async_trait]
impl TableProvider for QuackTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // `async_trait`'s desugared form (compile time, see
    // docs/internal/plans/2026-09-07-compile-time-async-hubs.md): the plan is
    // built synchronously, the same way `MemTable::scan` builds it, and handed
    // back as a ready future. No coroutine holds `&dyn Session` or `&[Expr]`,
    // so rustc never proves `Expr: Sync` for this crate. `filters` and `limit`
    // are unused, as in `MemTable::scan`; a fresh `MemTable` has no sort order.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [Expr],
        _limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = DFResult<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let plan = MemorySourceConfig::try_new(
            std::slice::from_ref(&self.batches),
            Arc::clone(&self.schema),
            projection.cloned(),
        )
        .map(|source| {
            let show_sizes = state.config_options().explain.show_sizes;
            DataSourceExec::from_data_source(source.with_show_sizes(show_sizes))
                as Arc<dyn ExecutionPlan>
        });
        Box::pin(std::future::ready(plan))
    }
}

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

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;

use crate::{ClientError, QuackClient};

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
    /// Connect to `uri`, run `sql`, cache the result. Schema is inferred
    /// from the first `RecordBatch`; an empty result set is reported as an
    /// error because we can't synthesize an Arrow schema with no data.
    pub fn new(uri: &str, token: Option<&str>, sql: &str) -> Result<Self, ClientError> {
        let mut client = QuackClient::connect(uri, token)?;
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

    /// Number of batches captured.
    pub fn batches(&self) -> &[arrow_array::RecordBatch] {
        &self.batches
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![self.batches.clone()])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        mem.scan(state, projection, filters, limit).await
    }
}

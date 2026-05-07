//! Embedded SQE engine for the CLI.
//!
//! Boots a single-process [`SessionContext`] with the same DataFusion
//! tuning as the cluster-mode coordinator: `parse_float_as_decimal`,
//! 64MB hash-join broadcast threshold, dynamic filter pushdown, Parquet
//! filter pushdown. Registers all the same scalar / aggregate / table
//! functions (Trino aliases, JSON, sha256, `read_parquet`, etc.) so the
//! same SQL text runs against the embedded engine as against a remote
//! coordinator.
//!
//! ## Scope (V1)
//!
//! No persistent catalog — users query files directly via `read_parquet`
//! or in-memory tables. No auth, no policy, no metrics endpoint. A
//! Hadoop / SQLite catalog at `~/.sqe/warehouse/` is the planned V2
//! addition.
//!
//! ## Why duplicate the registration code from `sqe-coordinator`?
//!
//! The coordinator's `create_session_context` takes a full `SqeConfig`
//! plus an authenticated `Session`, a `PolicyStore`, a `QueryTracker`,
//! and a `MetricsRegistry`. None of those exist in embedded mode and
//! plumbing them as `Option`s through the builder would bloat the
//! cluster path for the embedded use case. A small targeted helper
//! here is cleaner; if both paths ever diverge meaningfully we
//! refactor at that point.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sqe_core::config::StorageConfig;

use crate::client::{QueryResult, SqlClient};

/// Build a [`SessionContext`] suitable for embedded queries.
///
/// `memory_limit_bytes` caps the per-process query memory; values
/// below 64MB are clamped to that floor because DataFusion's hash
/// joins cannot make forward progress with smaller pools.
pub fn build_embedded_context(memory_limit_bytes: usize) -> anyhow::Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "default")
        // Same DataFusion tuning the cluster coordinator applies. See
        // the comments in `sqe-coordinator/src/session_context.rs` for
        // the rationale on each flag.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            64 * 1024 * 1024,
        )
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);

    let pool_size = memory_limit_bytes.max(64 * 1024 * 1024);
    let pool = Arc::new(FairSpillPool::new(pool_size));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .map_err(|e| anyhow::anyhow!("failed to build runtime env: {e}"))?;

    let mut ctx = SessionContext::new_with_config_rt(session_config, runtime);

    // Scalar UDFs.
    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf());

    // Trino dialect compatibility — year(), month(), day_of_week(),
    // url_extract_*, etc. plus the extended set (regexp_extract,
    // word_stem, soundex). These are what dbt models and Trino-shape
    // queries rely on.
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);

    // JSON functions: json_get, json_get_str, json_contains, etc.
    datafusion_functions_json::register_all(&mut ctx)
        .map_err(|e| anyhow::anyhow!("failed to register JSON functions: {e}"))?;

    // `read_parquet(path, ...)` TVF for direct file access. Embedded
    // mode passes a default `StorageConfig` so users can still hit S3
    // by supplying inline credentials in the TVF call. Filesystem
    // paths work without any storage config.
    ctx.register_udtf(
        "read_parquet",
        Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
            StorageConfig::default(),
        )),
    );

    Ok(ctx)
}

/// `SqlClient` impl backed by an in-process [`SessionContext`].
///
/// Mirrors the network clients (`flight.rs`, `http.rs`) so the CLI's
/// REPL and one-shot paths don't need to special-case embedded mode.
pub struct EmbeddedClient {
    ctx: SessionContext,
}

impl EmbeddedClient {
    pub fn new(memory_limit_bytes: usize) -> anyhow::Result<Self> {
        Ok(Self {
            ctx: build_embedded_context(memory_limit_bytes)?,
        })
    }
}

#[async_trait]
impl SqlClient for EmbeddedClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let df = self.ctx.sql(sql).await?;
        // Snapshot the DataFrame schema before collecting so we still
        // emit column names when the query produces zero batches (an
        // optimizer collapse like `WHERE FALSE` yields an EmptyExec).
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        Ok(record_batches_to_query_result(&schema, &batches))
    }
}

/// Render a sequence of [`RecordBatch`]es into the CLI's column-name +
/// stringified-row shape. Column names come from the input `schema`
/// even when `batches` is empty, matching what the network clients do.
fn record_batches_to_query_result(
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> QueryResult {
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default()))
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = formatters.iter().map(|f| f.value(row_idx).to_string()).collect();
            rows.push(row);
        }
    }
    QueryResult { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_client_executes_select_literal() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client.execute("SELECT 42 AS answer").await.expect("query");
        assert_eq!(result.columns, vec!["answer".to_string()]);
        assert_eq!(result.rows, vec![vec!["42".to_string()]]);
    }

    #[tokio::test]
    async fn embedded_client_runs_trino_function_year() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client
            .execute("SELECT year(DATE '2026-05-07') AS y")
            .await
            .expect("query");
        assert_eq!(result.columns, vec!["y".to_string()]);
        assert_eq!(result.rows, vec![vec!["2026".to_string()]]);
    }

    #[tokio::test]
    async fn embedded_client_returns_zero_rows_for_empty_select() {
        let mut client = EmbeddedClient::new(64 * 1024 * 1024).expect("build client");
        let result = client
            .execute("SELECT 1 WHERE FALSE")
            .await
            .expect("query");
        assert_eq!(result.columns, vec!["Int64(1)".to_string()]);
        assert!(result.rows.is_empty());
    }

    /// Memory limit below the floor (64 MB) is clamped, not rejected.
    #[tokio::test]
    async fn embedded_client_clamps_tiny_memory_limit() {
        let mut client = EmbeddedClient::new(1).expect("build client even with tiny limit");
        let result = client.execute("SELECT 1").await.expect("query");
        assert_eq!(result.rows, vec![vec!["1".to_string()]]);
    }
}

//! Handlers for EXPLAIN, EXPLAIN ANALYZE, and EXPLAIN FULL.
//!
//! All three apply policy enforcement before producing output — the plan
//! shown is the plan that actually executes.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;

use sqe_catalog::IcebergScanExec;
use sqe_core::{Session, SqeError};
use sqe_policy::PolicyEnforcer;

pub struct ExplainHandler {
    pub policy_enforcer: Arc<dyn PolicyEnforcer>,
}

impl ExplainHandler {
    pub fn new(policy_enforcer: Arc<dyn PolicyEnforcer>) -> Self {
        Self { policy_enforcer }
    }

    /// EXPLAIN <query> — returns logical and physical plan as text, no execution.
    pub async fn plan(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let df = ctx
            .sql(inner_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN planning failed: {e}")))?;

        let logical = df.logical_plan().clone();
        let enforced = self
            .policy_enforcer
            .evaluate(&session.user, logical)
            .await?;

        let logical_str = format!("{}", enforced.display_indent());

        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

        let physical_str = format!("{}", displayable(physical.as_ref()).indent(true));

        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        let types: ArrayRef = Arc::new(StringArray::from(vec!["logical_plan", "physical_plan"]));
        let plans: ArrayRef = Arc::new(StringArray::from(vec![
            logical_str.as_str(),
            physical_str.as_str(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![types, plans])
            .map_err(|e| SqeError::Execution(format!("Failed to build explain batch: {e}")))?;

        Ok(vec![batch])
    }

    /// EXPLAIN ANALYZE <query> — executes the query and returns per-operator metrics.
    /// Output schema: (step INT32, operation TEXT, output_rows INT64, elapsed_ms FLOAT64)
    pub async fn analyze(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let df = ctx
            .sql(inner_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN ANALYZE planning failed: {e}")))?;
        let logical = df.logical_plan().clone();
        let enforced = self
            .policy_enforcer
            .evaluate(&session.user, logical)
            .await?;

        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

        // Execute — populates metrics on each node in-place
        collect(physical.clone(), ctx.task_ctx())
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN ANALYZE execution failed: {e}")))?;

        let mut rows: Vec<AnalyzeRow> = Vec::new();
        walk_analyze(&physical, &mut rows);

        let schema = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("output_rows", DataType::Int64, true),
            Field::new("elapsed_ms", DataType::Float64, true),
        ]));

        let steps: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|r| r.step).collect::<Vec<_>>(),
        ));
        let ops: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.operation.as_str()).collect::<Vec<_>>(),
        ));

        let mut output_rows_b = arrow_array::builder::Int64Builder::new();
        for r in &rows {
            match r.output_rows {
                Some(v) => output_rows_b.append_value(v),
                None => output_rows_b.append_null(),
            }
        }
        let output_rows_arr: ArrayRef = Arc::new(output_rows_b.finish());

        let mut elapsed_b = arrow_array::builder::Float64Builder::new();
        for r in &rows {
            match r.elapsed_ms {
                Some(v) => elapsed_b.append_value(v),
                None => elapsed_b.append_null(),
            }
        }
        let elapsed_arr: ArrayRef = Arc::new(elapsed_b.finish());

        let batch = RecordBatch::try_new(schema, vec![steps, ops, output_rows_arr, elapsed_arr])
            .map_err(|e| SqeError::Execution(format!("Failed to build analyze batch: {e}")))?;

        Ok(vec![batch])
    }

    /// EXPLAIN FULL <query> — plan + Iceberg statistics, no execution.
    /// Output schema: (step INT32, operation TEXT, estimated_rows INT64,
    ///                 estimated_bytes INT64, files_scanned INT32, files_total INT32)
    pub async fn full(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let df = ctx
            .sql(inner_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN FULL planning failed: {e}")))?;
        let logical = df.logical_plan().clone();
        let enforced = self
            .policy_enforcer
            .evaluate(&session.user, logical)
            .await?;

        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

        let mut rows: Vec<FullRow> = Vec::new();
        walk_full(&physical, &mut rows);

        let schema = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("estimated_rows", DataType::Int64, true),
            Field::new("estimated_bytes", DataType::Int64, true),
            Field::new("files_scanned", DataType::Int32, true),
            Field::new("files_total", DataType::Int32, true),
        ]));

        let steps: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|r| r.step).collect::<Vec<_>>(),
        ));
        let ops: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.operation.as_str()).collect::<Vec<_>>(),
        ));

        macro_rules! nullable_array {
            ($builder:ty, $rows:expr, $field:ident) => {{
                let mut b = <$builder>::new();
                for r in $rows {
                    match r.$field {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }};
        }

        let est_rows = nullable_array!(arrow_array::builder::Int64Builder, &rows, estimated_rows);
        let est_bytes = nullable_array!(arrow_array::builder::Int64Builder, &rows, estimated_bytes);
        let f_scanned = nullable_array!(arrow_array::builder::Int32Builder, &rows, files_scanned);
        let f_total = nullable_array!(arrow_array::builder::Int32Builder, &rows, files_total);

        let batch = RecordBatch::try_new(
            schema,
            vec![steps, ops, est_rows, est_bytes, f_scanned, f_total],
        )
        .map_err(|e| SqeError::Execution(format!("Failed to build full explain batch: {e}")))?;

        Ok(vec![batch])
    }
}

// ---------------------------------------------------------------------------
// Private row types and free-function tree walkers
// ---------------------------------------------------------------------------

struct AnalyzeRow {
    step: i32,
    operation: String,
    output_rows: Option<i64>,
    elapsed_ms: Option<f64>,
}

struct FullRow {
    step: i32,
    operation: String,
    estimated_rows: Option<i64>,
    estimated_bytes: Option<i64>,
    files_scanned: Option<i32>,
    files_total: Option<i32>,
}

fn walk_analyze(node: &Arc<dyn ExecutionPlan>, rows: &mut Vec<AnalyzeRow>) {
    for child in node.children() {
        walk_analyze(child, rows);
    }
    let step = rows.len() as i32;
    let operation = node.name().to_string();
    let metrics = node.metrics();
    let output_rows = metrics
        .as_ref()
        .and_then(|m| m.output_rows())
        .map(|r| r as i64);
    let elapsed_ms = metrics
        .as_ref()
        .and_then(|m| m.elapsed_compute())
        .map(|ns| ns as f64 / 1_000_000.0);
    rows.push(AnalyzeRow { step, operation, output_rows, elapsed_ms });
}

fn walk_full(node: &Arc<dyn ExecutionPlan>, rows: &mut Vec<FullRow>) {
    for child in node.children() {
        walk_full(child, rows);
    }
    let step = rows.len() as i32;
    let operation = node.name().to_string();

    if let Some(scan) = node.as_any().downcast_ref::<IcebergScanExec>() {
        let table = scan.table();
        let snap = table.metadata().current_snapshot();
        let props = snap.map(|s| s.summary().additional_properties.clone());

        let parse_i64 = |key: &str| -> Option<i64> {
            props.as_ref()?.get(key)?.parse::<i64>()
                .map_err(|e| {
                    tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                    e
                })
                .ok()
        };
        let parse_i32 = |key: &str| -> Option<i32> {
            props.as_ref()?.get(key)?.parse::<i32>()
                .map_err(|e| {
                    tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                    e
                })
                .ok()
        };

        let estimated_rows = parse_i64("total-records");
        let estimated_bytes = parse_i64("total-files-size");
        let files_total = parse_i32("total-data-files");
        let files_scanned = files_total;

        rows.push(FullRow {
            step,
            operation,
            estimated_rows,
            estimated_bytes,
            files_scanned,
            files_total,
        });
    } else {
        use datafusion::common::stats::Precision;
        let estimated_rows = node
            .partition_statistics(None)
            .ok()
            .and_then(|s| match s.num_rows {
                Precision::Exact(v) | Precision::Inexact(v) => Some(v as i64),
                Precision::Absent => None,
            });

        rows.push(FullRow {
            step,
            operation,
            estimated_rows,
            estimated_bytes: None,
            files_scanned: None,
            files_total: None,
        });
    }
}

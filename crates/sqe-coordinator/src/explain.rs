//! Handlers for EXPLAIN, EXPLAIN ANALYZE, and EXPLAIN FULL.
//!
//! All three apply policy enforcement before producing output — the plan
//! shown is the plan that actually executes.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{displayable, execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;

use sqe_catalog::IcebergScanExec;
use sqe_core::{Session, SqeError, SqeErrorCode};
use sqe_policy::PolicyEnforcer;

pub struct ExplainHandler {
    pub policy_enforcer: Arc<dyn PolicyEnforcer>,
    /// The same `QueryConfig` `query_handler` plans with, so EXPLAIN output
    /// and EXPLAIN ANALYZE execution run the identical post-planning pipeline
    /// (`crate::post_planning`).
    query_config: sqe_core::config::QueryConfig,
    /// Rule failures are counted on `sqe_plan_rule_failures_total{rule}` here
    /// too, so an EXPLAIN that hits a broken rule is as visible as a query.
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

impl ExplainHandler {
    pub fn new(
        policy_enforcer: Arc<dyn PolicyEnforcer>,
        query_config: &sqe_core::config::QueryConfig,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    ) -> Self {
        Self {
            policy_enforcer,
            query_config: query_config.clone(),
            metrics,
        }
    }

    /// Apply the post-planning rule pipeline `query_handler` applies, so
    /// EXPLAIN shows (and EXPLAIN ANALYZE measures) the executed plan rather
    /// than the raw planner output. Distribution (`try_distribute`) is the
    /// one step not reflected here: it needs a live worker registry and a
    /// query id, so EXPLAIN prints the pre-distribution coordinator plan.
    fn apply_plan_shape_rules(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> sqe_core::Result<Arc<dyn ExecutionPlan>> {
        crate::post_planning::apply_post_planning_rules(
            ctx,
            &self.query_config,
            self.metrics.as_ref(),
            plan,
        )
    }

    /// EXPLAIN <query> — returns logical and physical plan as text, no execution.
    pub async fn plan(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        require_query(inner_sql)?;
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, inner_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("EXPLAIN planning failed: {e}")))?;

        let logical = df.logical_plan().clone();
        let (enforced, _policy_summary) = self
            .policy_enforcer
            .evaluate(&session.user, &sqe_policy::TableScope::of(ctx), logical)
            .await?;

        let logical_str = format!("{}", enforced.display_indent());

        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Physical planning failed: {e}")))?;
        let physical = self.apply_plan_shape_rules(ctx, physical)?;

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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build explain batch: {e}")))?;

        Ok(vec![batch])
    }

    /// EXPLAIN ANALYZE <query> — executes the query and returns per-operator
    /// metrics, prefixed with phase-level timings (parse + logical plan,
    /// policy evaluation, physical plan, execution, total).
    ///
    /// Output schema: (step INT32, operation TEXT, output_rows INT64, elapsed_ms FLOAT64,
    ///                 output_bytes INT64, output_batches INT64,
    ///                 spill_count INT64, spilled_bytes INT64, spilled_rows INT64)
    ///
    /// Phase rows have negative `step` values so they sort before the
    /// physical-plan operator rows (which start at step 0). Operation
    /// names are prefixed with `[phase] ` to make them easy to filter.
    pub async fn analyze(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        require_query(inner_sql)?;
        let total_start = std::time::Instant::now();

        let parse_start = std::time::Instant::now();
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, inner_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("EXPLAIN ANALYZE planning failed: {e}")))?;
        let logical = df.logical_plan().clone();
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        let policy_start = std::time::Instant::now();
        let (enforced, policy_summary) = self
            .policy_enforcer
            .evaluate(&session.user, &sqe_policy::TableScope::of(ctx), logical)
            .await?;
        refuse_metrics_under_row_filter(&policy_summary, "EXPLAIN ANALYZE")?;
        let policy_ms = policy_start.elapsed().as_secs_f64() * 1000.0;

        let physical_plan_start = std::time::Instant::now();
        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Physical planning failed: {e}")))?;
        let physical = self.apply_plan_shape_rules(ctx, physical)?;
        let physical_plan_ms = physical_plan_start.elapsed().as_secs_f64() * 1000.0;

        // Execute — populates metrics on each node in-place
        let execute_start = std::time::Instant::now();
        drain_capped(&physical, ctx, self.query_config.max_result_rows, "EXPLAIN ANALYZE").await?;
        let execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;

        let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
        // The "framework" overhead is everything outside per-operator
        // execution: parse + logical plan + policy + physical plan +
        // result materialization. This is the value that dominates SSB
        // SF1 timings where each query has tiny actual work but a fixed
        // setup cost.
        let framework_ms = total_ms - execute_ms;

        // Phase rows go first in the output. Negative step keeps them
        // visually distinct from the per-operator step numbers (0..N).
        let mut rows: Vec<AnalyzeRow> = vec![
            phase_row(-5, "[phase] parse + logical plan", parse_ms),
            phase_row(-4, "[phase] policy evaluate", policy_ms),
            phase_row(-3, "[phase] physical plan", physical_plan_ms),
            phase_row(-2, "[phase] execute (per-op detail below)", execute_ms),
            phase_row(
                -1,
                "[phase] framework overhead (parse + plan + policy + result)",
                framework_ms,
            ),
        ];
        walk_analyze(&physical, &mut rows);

        let schema = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("output_rows", DataType::Int64, true),
            Field::new("elapsed_ms", DataType::Float64, true),
            Field::new("output_bytes", DataType::Int64, true),
            Field::new("output_batches", DataType::Int64, true),
            Field::new("spill_count", DataType::Int64, true),
            Field::new("spilled_bytes", DataType::Int64, true),
            Field::new("spilled_rows", DataType::Int64, true),
        ]));

        let steps: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|r| r.step).collect::<Vec<_>>(),
        ));
        let ops: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.operation.as_str())
                .collect::<Vec<_>>(),
        ));

        macro_rules! nullable_i64 {
            ($rows:expr, $field:ident) => {{
                let mut b = arrow_array::builder::Int64Builder::new();
                for r in $rows {
                    match r.$field {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }};
        }

        let output_rows_arr = nullable_i64!(&rows, output_rows);

        let mut elapsed_b = arrow_array::builder::Float64Builder::new();
        for r in &rows {
            match r.elapsed_ms {
                Some(v) => elapsed_b.append_value(v),
                None => elapsed_b.append_null(),
            }
        }
        let elapsed_arr: ArrayRef = Arc::new(elapsed_b.finish());

        let output_bytes_arr = nullable_i64!(&rows, output_bytes);
        let output_batches_arr = nullable_i64!(&rows, output_batches);
        let spill_count_arr = nullable_i64!(&rows, spill_count);
        let spilled_bytes_arr = nullable_i64!(&rows, spilled_bytes);
        let spilled_rows_arr = nullable_i64!(&rows, spilled_rows);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                steps,
                ops,
                output_rows_arr,
                elapsed_arr,
                output_bytes_arr,
                output_batches_arr,
                spill_count_arr,
                spilled_bytes_arr,
                spilled_rows_arr,
            ],
        )
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build analyze batch: {e}")))?;

        Ok(vec![batch])
    }

    /// EXPLAIN FULL <query> — executes the query and returns Iceberg statistics
    /// combined with actual per-operator metrics.
    /// Output schema: (step INT32, operation TEXT, estimated_rows INT64,
    ///                 estimated_bytes INT64, files_scanned INT32, files_total INT32,
    ///                 output_rows INT64, elapsed_ms FLOAT64, output_bytes INT64,
    ///                 output_batches INT64, spill_count INT64, spilled_bytes INT64,
    ///                 spilled_rows INT64)
    pub async fn full(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        require_query(inner_sql)?;
        let total_start = std::time::Instant::now();

        let parse_start = std::time::Instant::now();
        let df = sqe_catalog::file_tvf_prepare::plan_sql(ctx, inner_sql)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("EXPLAIN FULL planning failed: {e}")))?;
        let logical = df.logical_plan().clone();
        let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

        let policy_start = std::time::Instant::now();
        let (enforced, policy_summary) = self
            .policy_enforcer
            .evaluate(&session.user, &sqe_policy::TableScope::of(ctx), logical)
            .await?;
        refuse_metrics_under_row_filter(&policy_summary, "EXPLAIN FULL")?;
        let policy_ms = policy_start.elapsed().as_secs_f64() * 1000.0;

        let physical_plan_start = std::time::Instant::now();
        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Physical planning failed: {e}")))?;
        let physical = self.apply_plan_shape_rules(ctx, physical)?;
        let physical_plan_ms = physical_plan_start.elapsed().as_secs_f64() * 1000.0;

        // Execute — populates per-node metrics in-place
        let execute_start = std::time::Instant::now();
        drain_capped(&physical, ctx, self.query_config.max_result_rows, "EXPLAIN FULL").await?;
        let execute_ms = execute_start.elapsed().as_secs_f64() * 1000.0;

        let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
        let framework_ms = total_ms - execute_ms;

        let mut rows: Vec<FullRow> = vec![
            full_phase_row(-5, "[phase] parse + logical plan", parse_ms),
            full_phase_row(-4, "[phase] policy evaluate", policy_ms),
            full_phase_row(-3, "[phase] physical plan", physical_plan_ms),
            full_phase_row(-2, "[phase] execute (per-op detail below)", execute_ms),
            full_phase_row(
                -1,
                "[phase] framework overhead (parse + plan + policy + result)",
                framework_ms,
            ),
        ];
        walk_full(&physical, &mut rows);

        let schema = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("estimated_rows", DataType::Int64, true),
            Field::new("estimated_bytes", DataType::Int64, true),
            Field::new("files_scanned", DataType::Int32, true),
            Field::new("files_total", DataType::Int32, true),
            Field::new("output_rows", DataType::Int64, true),
            Field::new("elapsed_ms", DataType::Float64, true),
            Field::new("output_bytes", DataType::Int64, true),
            Field::new("output_batches", DataType::Int64, true),
            Field::new("spill_count", DataType::Int64, true),
            Field::new("spilled_bytes", DataType::Int64, true),
            Field::new("spilled_rows", DataType::Int64, true),
        ]));

        let steps: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|r| r.step).collect::<Vec<_>>(),
        ));
        let ops: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.operation.as_str())
                .collect::<Vec<_>>(),
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
        let out_rows = nullable_array!(arrow_array::builder::Int64Builder, &rows, output_rows);
        let elapsed = nullable_array!(arrow_array::builder::Float64Builder, &rows, elapsed_ms);
        let out_bytes = nullable_array!(arrow_array::builder::Int64Builder, &rows, output_bytes);
        let out_batches =
            nullable_array!(arrow_array::builder::Int64Builder, &rows, output_batches);
        let spill_cnt = nullable_array!(arrow_array::builder::Int64Builder, &rows, spill_count);
        let spill_bytes = nullable_array!(arrow_array::builder::Int64Builder, &rows, spilled_bytes);
        let spill_rows = nullable_array!(arrow_array::builder::Int64Builder, &rows, spilled_rows);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                steps,
                ops,
                est_rows,
                est_bytes,
                f_scanned,
                f_total,
                out_rows,
                elapsed,
                out_bytes,
                out_batches,
                spill_cnt,
                spill_bytes,
                spill_rows,
            ],
        )
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build full explain batch: {e}")))?;

        Ok(vec![batch])
    }
}

// ---------------------------------------------------------------------------
// Private row types and free-function tree walkers
// ---------------------------------------------------------------------------

/// EXPLAIN in every form plans the inner statement through `ctx.sql`, and
/// ANALYZE / FULL then `collect()` it. DataFusion executes DDL eagerly in
/// `sql()` and DML on `collect()`, so an inner `INSERT` / `DROP` would run
/// against the Hive provider with none of SQE's write gates (#711). Only a
/// query may be explained; the same allow-list MCP's `explain_query` uses.
fn require_query(inner_sql: &str) -> sqe_core::Result<()> {
    let kind = sqe_sql::classifier::parse_and_classify(inner_sql)?;
    if matches!(kind, sqe_sql::classifier::StatementKind::Query(_)) {
        return Ok(());
    }
    Err(SqeError::with_code(
        SqeErrorCode::ExecutionFailed,
        format!(
            "EXPLAIN only accepts a query; {} statements are not explained (run them directly)",
            kind.name()
        ),
    ))
}

/// EXPLAIN ANALYZE / FULL report per-operator `output_rows` (and FULL the
/// snapshot's `total-records`), including operators below the injected row
/// filter, so they would reveal how many rows the policy hides. Refuse both
/// whenever a row filter or deny-all applies, for every caller: the filter
/// binds whoever it was resolved for, admin or not. Plain EXPLAIN (no
/// execution, no counts) stays available.
fn refuse_metrics_under_row_filter(
    summary: &sqe_policy::PolicySummary,
    what: &str,
) -> sqe_core::Result<()> {
    if summary.row_filters_applied == 0 && !summary.denied {
        return Ok(());
    }
    Err(SqeError::with_code(
        SqeErrorCode::ExecutionFailed,
        format!(
            "{what} is not available for a query governed by a row filter policy \
             (operator metrics would expose filtered row counts); use EXPLAIN instead"
        ),
    ))
}

/// Build an `AnalyzeRow` for a phase-level timing entry. Phase rows
/// only carry `elapsed_ms`; the other metric columns are `None` so
/// downstream filtering can ignore them or render them as blanks.
fn phase_row(step: i32, operation: &str, elapsed_ms: f64) -> AnalyzeRow {
    AnalyzeRow {
        step,
        operation: operation.to_string(),
        output_rows: None,
        elapsed_ms: Some(elapsed_ms),
        output_bytes: None,
        output_batches: None,
        spill_count: None,
        spilled_bytes: None,
        spilled_rows: None,
    }
}

struct AnalyzeRow {
    step: i32,
    operation: String,
    output_rows: Option<i64>,
    elapsed_ms: Option<f64>,
    output_bytes: Option<i64>,
    output_batches: Option<i64>,
    spill_count: Option<i64>,
    spilled_bytes: Option<i64>,
    spilled_rows: Option<i64>,
}

fn full_phase_row(step: i32, operation: &str, elapsed_ms: f64) -> FullRow {
    FullRow {
        step,
        operation: operation.to_string(),
        estimated_rows: None,
        estimated_bytes: None,
        files_scanned: None,
        files_total: None,
        output_rows: None,
        elapsed_ms: Some(elapsed_ms),
        output_bytes: None,
        output_batches: None,
        spill_count: None,
        spilled_bytes: None,
        spilled_rows: None,
    }
}

struct FullRow {
    step: i32,
    operation: String,
    estimated_rows: Option<i64>,
    estimated_bytes: Option<i64>,
    files_scanned: Option<i32>,
    files_total: Option<i32>,
    output_rows: Option<i64>,
    elapsed_ms: Option<f64>,
    output_bytes: Option<i64>,
    output_batches: Option<i64>,
    spill_count: Option<i64>,
    spilled_bytes: Option<i64>,
    spilled_rows: Option<i64>,
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
    let output_bytes = metrics
        .as_ref()
        .and_then(|m| m.sum(|metric| matches!(metric.value(), MetricValue::OutputBytes(_))))
        .map(|v| v.as_usize() as i64);
    let output_batches = metrics
        .as_ref()
        .and_then(|m| m.sum(|metric| matches!(metric.value(), MetricValue::OutputBatches(_))))
        .map(|v| v.as_usize() as i64);
    let spill_count = metrics
        .as_ref()
        .and_then(|m| m.spill_count())
        .map(|v| v as i64);
    let spilled_bytes = metrics
        .as_ref()
        .and_then(|m| m.spilled_bytes())
        .map(|v| v as i64);
    let spilled_rows = metrics
        .as_ref()
        .and_then(|m| m.spilled_rows())
        .map(|v| v as i64);
    rows.push(AnalyzeRow {
        step,
        operation,
        output_rows,
        elapsed_ms,
        output_bytes,
        output_batches,
        spill_count,
        spilled_bytes,
        spilled_rows,
    });
}

fn walk_full(node: &Arc<dyn ExecutionPlan>, rows: &mut Vec<FullRow>) {
    for child in node.children() {
        walk_full(child, rows);
    }
    let step = rows.len() as i32;
    let operation = node.name().to_string();

    // Actual execution metrics (populated after collect())
    let metrics = node.metrics();
    let output_rows = metrics
        .as_ref()
        .and_then(|m| m.output_rows())
        .map(|r| r as i64);
    let elapsed_ms = metrics
        .as_ref()
        .and_then(|m| m.elapsed_compute())
        .map(|ns| ns as f64 / 1_000_000.0);
    let output_bytes = metrics
        .as_ref()
        .and_then(|m| m.sum(|metric| matches!(metric.value(), MetricValue::OutputBytes(_))))
        .map(|v| v.as_usize() as i64);
    let output_batches = metrics
        .as_ref()
        .and_then(|m| m.sum(|metric| matches!(metric.value(), MetricValue::OutputBatches(_))))
        .map(|v| v.as_usize() as i64);
    let spill_count = metrics
        .as_ref()
        .and_then(|m| m.spill_count())
        .map(|v| v as i64);
    let spilled_bytes = metrics
        .as_ref()
        .and_then(|m| m.spilled_bytes())
        .map(|v| v as i64);
    let spilled_rows = metrics
        .as_ref()
        .and_then(|m| m.spilled_rows())
        .map(|v| v as i64);

    if let Some(scan) = node.downcast_ref::<IcebergScanExec>() {
        let table = scan.table();
        let snap = scan
            .snapshot_id()
            .and_then(|id| table.metadata().snapshot_by_id(id))
            .or_else(|| table.metadata().current_snapshot());
        let props = snap.map(|s| s.summary().additional_properties.clone());

        let parse_i64 = |key: &str| -> Option<i64> {
            props
                .as_ref()?
                .get(key)?
                .parse::<i64>()
                .map_err(|e| {
                    tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                    e
                })
                .ok()
        };
        let parse_i32 = |key: &str| -> Option<i32> {
            props
                .as_ref()?
                .get(key)?
                .parse::<i32>()
                .map_err(|e| {
                    tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                    e
                })
                .ok()
        };

        let estimated_rows = parse_i64("total-records");
        let estimated_bytes = parse_i64("total-files-size");
        let files_total = parse_i32("total-data-files");
        let files_planned = metrics
            .as_ref()
            .and_then(|m| m.sum_by_name("data_files_planned"))
            .map(|v| v.as_usize());
        let files_pruned_dynamic = metrics
            .as_ref()
            .and_then(|m| m.sum_by_name("files_pruned_dynamic"))
            .map(|v| v.as_usize())
            .unwrap_or(0);
        let files_scanned =
            files_planned.map(|planned| planned.saturating_sub(files_pruned_dynamic) as i32);

        rows.push(FullRow {
            step,
            operation,
            estimated_rows,
            estimated_bytes,
            files_scanned,
            files_total,
            output_rows,
            elapsed_ms,
            output_bytes,
            output_batches,
            spill_count,
            spilled_bytes,
            spilled_rows,
        });
    } else {
        use datafusion::common::stats::Precision;
        let estimated_rows = StatisticsContext::new().compute(node.as_ref(), &StatisticsArgs::new())
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
            output_rows,
            elapsed_ms,
            output_bytes,
            output_batches,
            spill_count,
            spilled_bytes,
            spilled_rows,
        });
    }
}

/// Run `plan` for its operator metrics only, discarding every batch, and stop
/// with the SELECT path's error once more than `max_rows` rows have streamed
/// (`0` = uncapped). EXPLAIN ANALYZE / FULL used `collect()`, which buffered
/// the entire result the buffered SELECT path refuses to buffer (#734).
async fn drain_capped(
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
    max_rows: usize,
    what: &str,
) -> sqe_core::Result<()> {
    use futures::TryStreamExt;
    let fail = |e: datafusion::error::DataFusionError| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("{what} execution failed: {e}"),
        )
    };
    let mut stream = execute_stream(Arc::clone(plan), ctx.task_ctx()).map_err(fail)?;
    let mut rows: usize = 0;
    while let Some(batch) = stream.try_next().await.map_err(fail)? {
        rows = rows.saturating_add(batch.num_rows());
        if max_rows > 0 && rows > max_rows {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!(
                    "{what} result exceeds maximum allowed rows ({rows} > {max_rows}). \
                     Reason: configured max_result_rows={max_rows}. Use LIMIT to reduce \
                     output or raise the limit in config."
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{drain_capped, require_query, ExplainHandler};
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use sqe_policy::{PolicyEnforcer, PolicySummary, TableScope};
    use std::sync::Arc;

    /// Returns the plan unchanged but reports the given policy summary, as a
    /// rewriter that injected a row filter would.
    struct SummaryEnforcer(PolicySummary);

    #[async_trait::async_trait]
    impl PolicyEnforcer for SummaryEnforcer {
        async fn evaluate(
            &self,
            _user: &sqe_core::SessionUser,
            _scope: &TableScope,
            plan: LogicalPlan,
        ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
            Ok((plan, self.0.clone()))
        }
    }

    #[tokio::test]
    async fn explain_metrics_refused_under_row_filter() {
        let session = sqe_core::Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        let ctx = SessionContext::new();
        for summary in [
            PolicySummary {
                row_filters_applied: 1,
                ..Default::default()
            },
            PolicySummary {
                denied: true,
                ..Default::default()
            },
        ] {
            let handler = ExplainHandler::new(
                Arc::new(SummaryEnforcer(summary)),
                &sqe_core::config::QueryConfig::default(),
                None,
            );
            let err = handler
                .analyze(&session, "SELECT 1", &ctx)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("EXPLAIN ANALYZE is not available"), "{err}");
            let err = handler
                .full(&session, "SELECT 1", &ctx)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("EXPLAIN FULL is not available"), "{err}");
        }
        assert!(super::refuse_metrics_under_row_filter(&PolicySummary::default(), "x").is_ok());
    }

    #[tokio::test]
    async fn explain_execution_stops_at_max_result_rows() {
        let ctx = SessionContext::new();
        let plan = ctx
            .sql("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(n)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        drain_capped(&plan, &ctx, 0, "EXPLAIN ANALYZE")
            .await
            .unwrap();
        drain_capped(&plan, &ctx, 5, "EXPLAIN ANALYZE")
            .await
            .unwrap();
        let err = drain_capped(&plan, &ctx, 2, "EXPLAIN ANALYZE")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_result_rows=2"), "{err}");
    }

    #[test]
    fn explain_refuses_anything_but_a_query() {
        assert!(require_query("SELECT 1").is_ok());
        assert!(require_query("WITH t AS (SELECT 1 AS n) SELECT n FROM t").is_ok());
        for sql in [
            "INSERT INTO lake.sales.orders SELECT 1, 2.0, 'x'",
            "DELETE FROM lake.sales.orders WHERE id = 1",
            "DROP TABLE lake.sales.orders",
            "CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION 's3://b/p'",
        ] {
            let err = require_query(sql).expect_err(sql).to_string();
            assert!(err.contains("EXPLAIN only accepts a query"), "{sql}: {err}");
        }
    }
}

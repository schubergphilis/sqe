//! `ROW_NUMBER()` over input already sorted by its partition keys, computed
//! in one vectorised pass per batch.
//!
//! DataFusion's `BoundedWindowAggExec` keeps one state per window partition,
//! keyed by the partition values as a `Vec<ScalarValue>` in an `IndexMap`,
//! and slices and evaluates every partition separately. With many small
//! partitions that bookkeeping is the whole cost: "latest row per customer"
//! over 150M rows and 15M customers spent ~34 s of CPU in the window, against
//! ~4 s for the same rows in 1,000 partitions.
//!
//! When the input arrives sorted by the partition keys
//! (`InputOrderMode::Sorted`), a row number is a counter that restarts where
//! the keys change, so [`SortedRowNumberExec`] finds the boundaries with
//! `arrow::compute::partition` and fills the counter, carrying only the last
//! row's keys and count from one batch to the next. It wraps the original
//! window node and answers every planning question (properties, required
//! input ordering and distribution, statistics) from it; only `execute`
//! differs. Output batches are the input batches with the row-number column
//! appended, as before.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::compute::{concat, partition};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::window::StandardWindowExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::sort_expr::OrderingRequirements;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, InputOrderMode, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, StatisticsArgs,
};
use futures::StreamExt;

/// Escape hatch: set to `0`, `false`, or `off` to keep DataFusion's own
/// window exec (read once per process).
const ENABLED_ENV: &str = "SQE_SORTED_ROW_NUMBER";

fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var(ENABLED_ENV).as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        )
    })
}

/// Swap every eligible `BoundedWindowAggExec` for a [`SortedRowNumberExec`].
///
/// Eligible: no state observer, `InputOrderMode::Sorted`, a non-empty
/// `PARTITION BY`, and every window expression is `row_number()` over the
/// same partition keys with a `UInt64` output. Anything else is left alone.
#[derive(Debug, Default)]
pub struct SortedRowNumberRule;

impl PhysicalOptimizerRule for SortedRowNumberRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !enabled() {
            return Ok(plan);
        }
        plan.transform_up(|node| {
            Ok(match SortedRowNumberExec::try_wrap(&node) {
                Some(exec) => Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>),
                None => Transformed::no(node),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "sorted_row_number"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `ROW_NUMBER()` per partition over input sorted by the partition keys.
#[derive(Debug)]
pub struct SortedRowNumberExec {
    /// The window node this replaces; the source of every planning answer.
    window: Arc<dyn ExecutionPlan>,
    /// The `PARTITION BY` expressions, evaluated against the input.
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    /// How many row-number columns to append (one per window expression).
    outputs: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl SortedRowNumberExec {
    /// `Some` when `node` is a `BoundedWindowAggExec` this exec computes
    /// exactly.
    pub fn try_wrap(node: &Arc<dyn ExecutionPlan>) -> Option<Self> {
        let window = node.downcast_ref::<BoundedWindowAggExec>()?;
        if window.state_observer().is_some() || window.input_order_mode != InputOrderMode::Sorted {
            return None;
        }
        let exprs = window.window_expr();
        let partition_by = exprs.first()?.partition_by().to_vec();
        if partition_by.is_empty() {
            return None;
        }
        let input_width = window.input().schema().fields().len();
        let schema = window.schema();
        for (i, expr) in exprs.iter().enumerate() {
            if !is_row_number(expr.as_any()) || !same_exprs(expr.partition_by(), &partition_by) {
                return None;
            }
            if schema.field(input_width + i).data_type() != &DataType::UInt64 {
                return None;
            }
        }
        Some(Self {
            window: Arc::clone(node),
            partition_by,
            outputs: exprs.len(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.window.children()[0]
    }
}

fn is_row_number(expr: &dyn std::any::Any) -> bool {
    expr.downcast_ref::<StandardWindowExpr>()
        .and_then(|e| {
            e.get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
        })
        .is_some_and(|udf| udf.fun().name() == "row_number")
}

fn same_exprs(a: &[Arc<dyn PhysicalExpr>], b: &[Arc<dyn PhysicalExpr>]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq(y))
}

impl DisplayAs for SortedRowNumberExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SortedRowNumberExec: ")?;
        self.window.fmt_as(t, f)
    }
}

impl ExecutionPlan for SortedRowNumberExec {
    fn name(&self) -> &str {
        "SortedRowNumberExec"
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.window.apply_expressions(f)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.window.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.input()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.window.required_input_ordering()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.window.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.window.benefits_from_input_partitioning()
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let window = Arc::clone(&self.window).replace_children(children, options)?;
        // A new child can change the input order mode; re-check, and fall
        // back to the plain window node when it no longer qualifies.
        Ok(match Self::try_wrap(&window) {
            Some(exec) => Arc::new(exec),
            None => window,
        })
    }

    fn execute(
        &self,
        partition_idx: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input().execute(partition_idx, context)?;
        let schema = self.window.schema();
        let mut state = RowNumberState {
            schema: Arc::clone(&schema),
            partition_by: self.partition_by.clone(),
            outputs: self.outputs,
            last_keys: None,
            last_count: 0,
            baseline: BaselineMetrics::new(&self.metrics, partition_idx),
        };
        let stream = input.map(move |batch| {
            let out = state.next_batch(batch?)?;
            state.baseline.record_output(out.num_rows());
            Ok(out)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.window.statistics_from_inputs(input_stats, args)
    }
}

/// Per-stream state: the last row's partition keys and its row number.
struct RowNumberState {
    schema: SchemaRef,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    outputs: usize,
    /// One-row arrays holding the previous batch's last partition key.
    last_keys: Option<Vec<ArrayRef>>,
    last_count: u64,
    baseline: BaselineMetrics,
}

impl RowNumberState {
    fn next_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let _timer = self.baseline.elapsed_compute().timer();
        let rows = batch.num_rows();
        let mut numbers: Vec<u64> = Vec::with_capacity(rows);
        if rows > 0 {
            let keys = self
                .partition_by
                .iter()
                .map(|e| e.evaluate(&batch).and_then(|v| v.into_array(rows)))
                .collect::<Result<Vec<_>>>()?;
            let mut first_start = 1;
            if let Some(last) = &self.last_keys {
                if continues(last, &keys)? {
                    first_start = self.last_count + 1;
                }
            }
            for (i, range) in partition(&keys)?.ranges().into_iter().enumerate() {
                let start = if i == 0 { first_start } else { 1 };
                numbers.extend(start..start + range.len() as u64);
            }
            self.last_count = numbers[rows - 1];
            // Copy the one-row slice so the carried key does not pin the batch.
            self.last_keys = Some(
                keys.iter()
                    .map(|k| concat(&[k.slice(rows - 1, 1).as_ref()]))
                    .collect::<std::result::Result<_, _>>()?,
            );
        }
        let number: ArrayRef = Arc::new(UInt64Array::from(numbers));
        let mut columns = batch.columns().to_vec();
        columns.extend(std::iter::repeat_n(number, self.outputs));
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// Whether the batch's first row has the same partition key as the previous
/// batch's last row, by the same equality `partition` uses.
fn continues(last: &[ArrayRef], keys: &[ArrayRef]) -> Result<bool> {
    let pairs = last
        .iter()
        .zip(keys)
        .map(|(l, k)| concat(&[l.as_ref(), k.slice(0, 1).as_ref()]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(partition(&pairs)?.len() == 1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::SortedRowNumberRule;

    /// Two input partitions of three-row batches, so window partitions
    /// straddle batch boundaries. Keys repeat, include NULLs, and carry a
    /// second key column.
    fn table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("g", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let rows: Vec<(Option<i64>, Option<&str>, i64)> = (0..97)
            .map(|i| {
                let k = if i % 11 == 0 { None } else { Some(i % 7) };
                let g = if i % 5 == 0 {
                    None
                } else {
                    Some(["x", "y"][i as usize % 2])
                };
                (k, g, i * 13 % 101)
            })
            .collect();
        let batch = |chunk: &[(Option<i64>, Option<&str>, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        chunk.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        chunk.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        chunk.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };
        let (left, right) = rows.split_at(50);
        let partitions = vec![
            left.chunks(3).map(batch).collect(),
            right.chunks(3).map(batch).collect(),
        ];
        Arc::new(MemTable::try_new(schema, partitions).unwrap())
    }

    fn ctx(with_rule: bool) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(3)
            .with_batch_size(4);
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if with_rule {
            builder = builder.with_physical_optimizer_rule(Arc::new(SortedRowNumberRule));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("t", table()).unwrap();
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> (String, String) {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let shown = displayable(plan.as_ref()).indent(false).to_string();
        let batches = df.collect().await.unwrap();
        (pretty_format_batches(&batches).unwrap().to_string(), shown)
    }

    /// Same rows as DataFusion's own window, and the rewrite actually ran.
    async fn assert_same(sql: &str, expect_rewrite: bool) {
        let (want, _) = run(&ctx(false), sql).await;
        let (got, plan) = run(&ctx(true), sql).await;
        assert_eq!(
            plan.contains("SortedRowNumberExec"),
            expect_rewrite,
            "rewrite expectation for {sql}\n{plan}"
        );
        assert_eq!(got, want, "{sql}\n{plan}");
    }

    #[tokio::test]
    async fn row_number_matches_datafusion() {
        assert_same(
            "SELECT k, ts, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) AS rn \
             FROM t ORDER BY k NULLS FIRST, ts DESC",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn latest_per_key_filter_matches_datafusion() {
        assert_same(
            "SELECT k, g, ts FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k, g \
             ORDER BY ts DESC) AS rn FROM t) s WHERE rn = 1 ORDER BY k, g, ts",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn two_row_numbers_over_same_keys_match_datafusion() {
        assert_same(
            "SELECT g, ts, ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) AS a, \
             ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) AS b \
             FROM t ORDER BY g, ts",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn rank_and_unpartitioned_row_number_are_left_alone() {
        assert_same(
            "SELECT k, ts, RANK() OVER (PARTITION BY k ORDER BY ts) AS r FROM t ORDER BY k, ts",
            false,
        )
        .await;
        assert_same(
            "SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS r FROM t ORDER BY ts",
            false,
        )
        .await;
    }
}

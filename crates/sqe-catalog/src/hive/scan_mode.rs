//! Marks a Hive scan that ran coordinator-local, and says why.
//!
//! Cycle 5 dispatches Hive scans to workers when it can. When it cannot, the
//! query still succeeds -- falling back is always correct -- and that is
//! exactly the problem: a silent fallback is indistinguishable from success.
//! The feature never engages, the rows are right, and nobody notices.
//!
//! So the fallback is a plan node. It does nothing to the data (it delegates
//! `execute` straight through) and exists purely so `EXPLAIN` carries the
//! reason to whoever is wondering why their cluster is idle.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};

/// Pass-through wrapper recording that this Hive scan did not distribute.
#[derive(Debug)]
pub struct HiveLocalScanExec {
    input: Arc<dyn ExecutionPlan>,
    /// Why the scan stayed on the coordinator, from
    /// [`super::worker_scan::Unservable::reason`].
    reason: String,
}

impl HiveLocalScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, reason: String) -> Self {
        Self { input, reason }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl DisplayAs for HiveLocalScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            // The reason belongs in the DEFAULT format, not only in verbose.
            // Someone asking why a query is slow runs plain EXPLAIN.
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "HiveLocalScanExec: coordinator-local, {}", self.reason)
            }
            DisplayFormatType::TreeRender => write!(f, "HiveLocalScanExec"),
        }
    }
}

impl ExecutionPlan for HiveLocalScanExec {
    fn name(&self) -> &str {
        "HiveLocalScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Identical to the input: this node changes nothing about the data,
        // its partitioning, or its ordering.
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// SQE PATCH (DataFusion 55): owns no expressions of its own; the wrapped
    /// scan is a child and is visited in its own right.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// DataFusion 55 deprecated this but kept it required; upstream's own
    /// migration shim. Real logic is in `replace_children`.
    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Preserve the reason across replacement. Dropping it here would make
        // the marker vanish the first time an optimizer rule rebuilt the plan,
        // which is the silent-fallback bug wearing a different hat.
        let child = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "HiveLocalScanExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(HiveLocalScanExec::new(child, self.reason.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

//! TopK pushdown verification for ORDER BY ... LIMIT N.
//!
//! DataFusion's `SortExec` uses heap-based TopK mode when `fetch` is set,
//! providing O(N) memory usage regardless of input size. This module
//! provides a verification utility to confirm TopK is active in a plan.

use datafusion::physical_plan::ExecutionPlan;

/// Check whether a physical plan tree contains a SortExec with TopK (fetch) set.
///
/// Renders the full plan display and checks for the "TopK" marker that
/// DataFusion emits when SortExec operates in heap-based TopK mode.
pub fn plan_uses_topk(plan: &dyn ExecutionPlan) -> bool {
    let display = format!(
        "{}",
        datafusion::physical_plan::displayable(plan).indent(true)
    );
    display.contains("TopK")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_topk_in_sort_limit_plan() {
        let ctx = SessionContext::new();

        ctx.sql("CREATE TABLE t (id INT, name VARCHAR) AS VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
            .await
            .unwrap();

        let df = ctx
            .sql("SELECT * FROM t ORDER BY id LIMIT 3")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );

        // Confirm TopK(fetch=3) appears in the plan
        assert!(
            display.contains("TopK(fetch=3)"),
            "Expected TopK with fetch=3 in plan:\n{display}"
        );

        // Our helper should also detect it
        assert!(plan_uses_topk(plan.as_ref()));
    }

    #[tokio::test]
    async fn test_no_topk_without_limit() {
        let ctx = SessionContext::new();

        ctx.sql("CREATE TABLE t2 (id INT) AS VALUES (1), (2), (3)")
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM t2 ORDER BY id").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        assert!(!plan_uses_topk(plan.as_ref()));
    }
}

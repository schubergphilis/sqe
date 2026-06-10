//! Scan-pushdown helpers for distributed execution (#233).
//!
//! The coordinator pushes the scan's filter predicate and, when it is provably
//! safe, the query LIMIT into each `ScanTask` so workers prune rows before
//! shipping them over Arrow Flight. Both are pure optimizations: the
//! coordinator keeps the authoritative `FilterExec` and `GlobalLimitExec`
//! above `DistributedScanExec`, so a worker that double-filters or over-counts
//! a per-fragment limit cannot change the query result.
//!
//! # Predicate serialization
//!
//! We serialize the scan's logical `df_filters` (conjunction-folded into one
//! `Expr`) with `datafusion_proto`'s `Serializeable::to_bytes`. The logical
//! form references columns by name, so the worker rebuilds a `PhysicalExpr`
//! against the parquet file schema without depending on projection order.
//!
//! # Limit safety
//!
//! Per-fragment truncation is only correct when every operator between the
//! limit node and the scan is row-count-PRESERVING and order-insensitive. A
//! filter removes rows, so a limit cannot pass through it (the worker would
//! count pre-filter rows and truncate before enough matches were found).
//! `SortExec`, `AggregateExec`, joins, and windows likewise break the property.
//! The accepted passthroughs are `ProjectionExec`, `CoalescePartitionsExec`,
//! and `RepartitionExec`. So `SELECT * FROM t LIMIT n` pushes the limit, while
//! `SELECT ... WHERE ... LIMIT n` pushes only the predicate.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_proto::bytes::Serializeable;

/// Serialize the scan's filter predicate to proto bytes for transport in a
/// `ScanTask`. Returns `None` when there is no filter to push.
///
/// The individual `df_filters` are conjunction-folded with `AND` so the worker
/// reconstructs a single predicate. Serialization failures are non-fatal: the
/// caller treats `None` as "ship every projected row" and relies on the
/// coordinator's `FilterExec`.
pub fn serialize_scan_predicate(
    df_filters: &[datafusion::logical_expr::Expr],
) -> Option<Vec<u8>> {
    if df_filters.is_empty() {
        return None;
    }
    let combined = df_filters.iter().cloned().reduce(|a, b| a.and(b))?;
    match combined.to_bytes() {
        Ok(bytes) => Some(bytes.to_vec()),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Failed to serialize scan predicate for pushdown; \
                 falling back to coordinator-side filtering"
            );
            None
        }
    }
}

/// Returns `true` if `node` is row-count-PRESERVING and order-insensitive, so a
/// per-fragment limit may pass through it unchanged.
///
/// `FilterExec` is deliberately NOT here. A filter removes rows, so capping its
/// *input* to N is not the same as capping its *output* to N. The worker only
/// applies the predicate when late materialization is beneficial (predicate
/// columns a proper subset of the projection); for `SELECT <pred-cols> WHERE
/// <pred> LIMIT n` it ships raw rows and would count pre-filter rows toward the
/// limit, truncating before enough matches were found. Pushing a limit through
/// a filter is therefore unsafe and DataFusion itself never does it.
fn is_limit_safe_passthrough(node: &dyn ExecutionPlan) -> bool {
    let any = node.as_any();
    any.is::<ProjectionExec>()
        || any.is::<CoalescePartitionsExec>()
        || any.is::<RepartitionExec>()
}

/// Walk down from `node` toward `scan`; return `true` if every intermediate
/// operator is a limit-safe passthrough and `scan` is reachable.
fn path_to_scan_is_limit_safe(
    node: &Arc<dyn ExecutionPlan>,
    scan: &Arc<dyn ExecutionPlan>,
) -> bool {
    if Arc::ptr_eq(node, scan) {
        return true;
    }
    if !is_limit_safe_passthrough(node.as_ref()) {
        return false;
    }
    // Passthrough operators have exactly one child in the shapes we accept;
    // recurse into all children to be safe (still must reach the scan).
    node.children()
        .iter()
        .any(|child| path_to_scan_is_limit_safe(child, scan))
}

/// Extract a per-fragment row LIMIT to push into each `ScanTask`, or `None`
/// when no limit can be safely pushed.
///
/// Searches the plan for a `GlobalLimitExec` or `LocalLimitExec` that sits above
/// `scan` through a limit-safe path. The pushed cap is `skip + fetch` (an
/// over-approximation: each fragment may emit up to that many rows, and the
/// coordinator's limit node applies the true global skip/fetch). A `GlobalLimit`
/// with no `fetch` (offset-only) yields `None`.
///
/// Mutual-exclusion invariant with the pushed predicate (#233): a limit is
/// pushed only when no `FilterExec` lies between it and the scan. A non-empty
/// `predicate_proto` requires non-empty `df_filters`, and because
/// `IcebergScanExec` rejects static filter pushdown the `FilterExec` always
/// survives in the plan (SQL `LIMIT` sits above `WHERE`, so on the path). Hence
/// limit-pushed implies filterless scan, so the worker's early-stop counts
/// exactly the rows the coordinator's `GlobalLimitExec` counts. Limit and
/// predicate are never both pushed for the same scan.
pub fn extract_pushable_limit(
    plan: &Arc<dyn ExecutionPlan>,
    scan: &Arc<dyn ExecutionPlan>,
) -> Option<usize> {
    let mut stack: Vec<&Arc<dyn ExecutionPlan>> = vec![plan];
    while let Some(node) = stack.pop() {
        if let Some(gl) = node.as_any().downcast_ref::<GlobalLimitExec>() {
            if let Some(fetch) = gl.fetch() {
                let cap = fetch.saturating_add(gl.skip());
                // The single child of the limit is the start of the path.
                if node
                    .children()
                    .iter()
                    .any(|child| path_to_scan_is_limit_safe(child, scan))
                {
                    return Some(cap);
                }
            }
        } else if let Some(ll) = node.as_any().downcast_ref::<LocalLimitExec>() {
            let cap = ll.fetch();
            if node
                .children()
                .iter()
                .any(|child| path_to_scan_is_limit_safe(child, scan))
            {
                return Some(cap);
            }
        }
        for child in node.children() {
            stack.push(child);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::expressions::{col as pcol, lit as plit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::expressions::BinaryExpr;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    /// Stand-in "scan" leaf used as the pushdown target in these unit tests.
    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    #[test]
    fn predicate_serializes_and_is_non_empty() {
        let f = col("a").gt(lit(5i64));
        let bytes = serialize_scan_predicate(&[f]).expect("should serialize");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn predicate_none_when_no_filters() {
        assert!(serialize_scan_predicate(&[]).is_none());
    }

    #[test]
    fn predicate_conjunction_roundtrips_to_expr() {
        // Two filters fold into one AND expr that decodes back identically.
        let filters = vec![col("a").gt(lit(5i64)), col("b").lt(lit(10i64))];
        let bytes = serialize_scan_predicate(&filters).unwrap();
        let decoded =
            datafusion::logical_expr::Expr::from_bytes(&bytes).expect("decode");
        let expected = col("a").gt(lit(5i64)).and(col("b").lt(lit(10i64)));
        assert_eq!(decoded, expected);
    }

    #[test]
    fn limit_pushed_directly_above_scan() {
        // GlobalLimit -> scan (e.g. SELECT * FROM t LIMIT 10 OFFSET 3):
        // safe, cap = skip + fetch.
        let scan = leaf();
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(scan.clone(), 3, Some(10)));
        assert_eq!(extract_pushable_limit(&limit, &scan), Some(13));
    }

    #[test]
    fn limit_pushed_through_projection_path() {
        // GlobalLimit -> Projection -> scan : projection preserves row count,
        // so the limit is safe to push.
        let scan = leaf();
        let proj_expr: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(pcol("a", &schema()).unwrap(), "a".to_string())];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_expr, scan.clone()).unwrap());
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(projection, 0, Some(10)));
        assert_eq!(extract_pushable_limit(&limit, &scan), Some(10));
    }

    #[test]
    fn limit_not_pushed_through_filter() {
        // GlobalLimit -> Filter -> scan : a filter removes rows, so capping the
        // filter's INPUT to N is not the same as capping its OUTPUT. The worker
        // would count pre-filter rows and truncate too early. Refuse to push
        // (the predicate is still pushed separately; the limit is not).
        let scan = leaf();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            pcol("a", &schema()).unwrap(),
            Operator::Gt,
            plit(5i64),
        ));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, scan.clone()).unwrap());
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(filter, 3, Some(10)));
        assert_eq!(extract_pushable_limit(&limit, &scan), None);
    }

    #[test]
    fn limit_not_pushed_through_sort() {
        // GlobalLimit -> Sort -> scan : unsafe (global top-N), refuse.
        let scan = leaf();
        let sort_expr = PhysicalSortExpr::new_default(pcol("a", &schema()).unwrap());
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            LexOrdering::new(vec![sort_expr]).unwrap(),
            scan.clone(),
        ));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));
        assert_eq!(extract_pushable_limit(&limit, &scan), None);
    }

    #[test]
    fn no_limit_node_yields_none() {
        let scan = leaf();
        assert_eq!(extract_pushable_limit(&scan, &scan), None);
    }

    #[test]
    fn offset_only_global_limit_yields_none() {
        let scan = leaf();
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(scan.clone(), 5, None));
        assert_eq!(extract_pushable_limit(&limit, &scan), None);
    }
}

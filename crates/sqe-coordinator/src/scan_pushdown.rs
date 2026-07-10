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

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, expr::InList};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysBinaryExpr, Column as PhysColumn, InListExpr, IsNotNullExpr, IsNullExpr,
    Literal as PhysLiteral, NotExpr,
};
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

/// Returns `true` when the expression is a bare `true` literal — the state a
/// `DynamicFilterPhysicalExpr` snapshot has before its hash-join build side
/// completes. Pushing it to a worker would be pure overhead.
pub fn is_trivially_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<PhysLiteral>()
        .is_some_and(|l| matches!(l.value(), ScalarValue::Boolean(Some(true))))
}

/// Convert a dynamic-filter snapshot, keeping whatever top-level AND
/// conjuncts are expressible as logical `Expr`s and dropping the rest.
///
/// DataFusion 53's hash-join filter snapshots look like
/// `lo_partkey >= 8 AND lo_partkey <= 79984 AND hash_lookup` — min/max key
/// bounds plus an opaque hash-set membership probe that has no logical
/// equivalent. All-or-nothing conversion threw away the usable range bounds
/// because of that last term. Dropping a CONJUNCT only widens a filter, so
/// pushing the survivors to a worker is always sound (the coordinator's
/// join stays authoritative); dropping inside OR/NOT would not be, which is
/// why the split happens only at top-level ANDs and the strict converter
/// handles everything below.
pub fn physical_filter_to_logical_lenient(expr: &Arc<dyn PhysicalExpr>) -> Option<Expr> {
    if let Some(b) = expr.downcast_ref::<PhysBinaryExpr>() {
        if *b.op() == datafusion::logical_expr::Operator::And {
            let left = physical_filter_to_logical_lenient(b.left());
            let right = physical_filter_to_logical_lenient(b.right());
            return match (left, right) {
                (Some(l), Some(r)) => Some(l.and(r)),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            };
        }
    }
    physical_filter_to_logical(expr)
}

/// Convert a *snapshot* of a dynamic join filter (a `PhysicalExpr`) back into
/// a logical `Expr` so it can ride the existing `ScanTask::predicate_proto`
/// channel to workers (which rebuild physical predicates by column NAME
/// against each parquet file schema).
///
/// Hash-join runtime filters only ever take simple shapes — `col >= lit AND
/// col <= lit` range bounds, `col IN (...)` lists, null checks, and boolean
/// combinations thereof — so this handles exactly those and returns `None`
/// for anything else. `None` is non-fatal: the scan ships unfiltered, which
/// is what happened unconditionally before this conversion existed.
pub fn physical_filter_to_logical(expr: &Arc<dyn PhysicalExpr>) -> Option<Expr> {
    let any: &dyn PhysicalExpr = expr.as_ref();
    if let Some(c) = any.downcast_ref::<PhysColumn>() {
        return Some(Expr::Column(datafusion::common::Column::from_name(c.name())));
    }
    if let Some(l) = any.downcast_ref::<PhysLiteral>() {
        return Some(Expr::Literal(l.value().clone(), None));
    }
    if let Some(b) = any.downcast_ref::<PhysBinaryExpr>() {
        let left = physical_filter_to_logical(b.left())?;
        let right = physical_filter_to_logical(b.right())?;
        return Some(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(left),
            *b.op(),
            Box::new(right),
        )));
    }
    if let Some(il) = any.downcast_ref::<InListExpr>() {
        let e = physical_filter_to_logical(il.expr())?;
        let list = il
            .list()
            .iter()
            .map(physical_filter_to_logical)
            .collect::<Option<Vec<_>>>()?;
        return Some(Expr::InList(InList::new(Box::new(e), list, il.negated())));
    }
    if let Some(n) = any.downcast_ref::<IsNullExpr>() {
        return Some(Expr::IsNull(Box::new(physical_filter_to_logical(n.arg())?)));
    }
    if let Some(n) = any.downcast_ref::<IsNotNullExpr>() {
        return Some(Expr::IsNotNull(Box::new(physical_filter_to_logical(n.arg())?)));
    }
    if let Some(n) = any.downcast_ref::<NotExpr>() {
        return Some(Expr::Not(Box::new(physical_filter_to_logical(n.arg())?)));
    }
    None
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
    node.is::<ProjectionExec>()
        || node.is::<CoalescePartitionsExec>()
        || node.is::<RepartitionExec>()
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
        if let Some(gl) = node.downcast_ref::<GlobalLimitExec>() {
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
        } else if let Some(ll) = node.downcast_ref::<LocalLimitExec>() {
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

    /// A hash-join range snapshot (`a >= 3 AND a <= 17`) — the shape a
    /// `DynamicFilterPhysicalExpr` materializes after its build side
    /// completes — must convert to the equivalent logical Expr so it can
    /// ride `ScanTask::predicate_proto` to a worker.
    #[test]
    fn physical_range_filter_converts_to_logical() {
        let s = schema();
        let ge: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            pcol("a", &s).unwrap(),
            Operator::GtEq,
            plit(3i64),
        ));
        let le: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            pcol("a", &s).unwrap(),
            Operator::LtEq,
            plit(17i64),
        ));
        let both: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ge, Operator::And, le));
        let logical = physical_filter_to_logical(&both).expect("convertible");
        assert_eq!(
            logical,
            col("a").gt_eq(lit(3i64)).and(col("a").lt_eq(lit(17i64)))
        );
    }

    #[test]
    fn physical_in_list_converts_to_logical() {
        let s = schema();
        let in_list = datafusion::physical_expr::expressions::in_list(
            pcol("b", &s).unwrap(),
            vec![plit(1i64), plit(2i64)],
            &false,
            &s,
        )
        .unwrap();
        let logical = physical_filter_to_logical(&in_list).expect("convertible");
        assert_eq!(logical, col("b").in_list(vec![lit(1i64), lit(2i64)], false));
    }

    /// Unsupported shapes (e.g. a CASE expression) must yield None — the
    /// scan then ships unfiltered, exactly the pre-conversion behavior.
    #[test]
    fn unconvertible_physical_filter_yields_none() {
        use datafusion::physical_expr::expressions::CaseExpr;
        let case: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(None, vec![(plit(true), plit(1i64))], Some(plit(0i64)))
                .unwrap(),
        );
        assert!(physical_filter_to_logical(&case).is_none());
    }

    /// DF 53 snapshots end with an opaque hash-set probe; the lenient
    /// converter must keep the range conjuncts and drop only that term.
    #[test]
    fn lenient_conversion_keeps_convertible_conjuncts() {
        use datafusion::physical_expr::expressions::CaseExpr;
        let s = schema();
        let ge: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            pcol("a", &s).unwrap(),
            Operator::GtEq,
            plit(3i64),
        ));
        // stand-in for the unconvertible hash_lookup term
        let opaque: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(None, vec![(plit(true), plit(true))], Some(plit(false)))
                .unwrap(),
        );
        let both: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ge, Operator::And, opaque));
        let logical = physical_filter_to_logical_lenient(&both).expect("partial conversion");
        assert_eq!(logical, col("a").gt_eq(lit(3i64)));
        // strict conversion still refuses the same expr
        assert!(physical_filter_to_logical(&both).is_none());
    }

    #[test]
    fn trivially_true_literal_is_detected() {
        assert!(is_trivially_true(&plit(true)));
        assert!(!is_trivially_true(&plit(false)));
        let s = schema();
        assert!(!is_trivially_true(&pcol("a", &s).unwrap()));
    }
}

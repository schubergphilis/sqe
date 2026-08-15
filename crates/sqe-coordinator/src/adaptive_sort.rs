//! Adaptive sort stripping for physical query plans.
//!
//! Walks the physical plan tree and selectively removes `SortExec` nodes
//! based on the configured [`SortMode`], current [`MemoryPressure`], and
//! whether the sort keys match Iceberg partition columns.
//!
//! The key insight: sorting by non-partition columns is a convenience, not
//! a structural requirement. When memory is scarce, returning unsorted data
//! is better than spilling, timing out, or crashing.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use tracing::info;

use sqe_catalog::IcebergScanExec;
use sqe_core::SortMode;

use crate::memory::MemoryPressure;

/// Record of a single sort-stripping decision made during plan rewriting.
#[derive(Debug, Clone)]
pub struct SortDecision {
    /// Column names from the ORDER BY clause.
    pub sort_columns: Vec<String>,
    /// Whether the sort was kept or stripped.
    pub kept: bool,
    /// Human-readable reason for the decision.
    pub reason: String,
}

/// Decide whether a specific sort should be kept or stripped.
///
/// Implements the decision matrix:
/// ```text
/// (strict, _, _)                          → KEEP
/// (partition_only, _, true)               → KEEP
/// (partition_only, _, false)              → STRIP
/// (adaptive, Green, _)                    → KEEP
/// (adaptive, Yellow|Orange|Red, true)     → KEEP
/// (adaptive, Yellow|Orange|Red, false)    → STRIP
/// ```
///
/// TopK sorts (with LIMIT/fetch) are always kept regardless of mode.
pub fn decide_sort(
    sort_mode: SortMode,
    pressure: MemoryPressure,
    is_partition_sort: bool,
    has_fetch: bool,
) -> bool {
    // TopK sorts are cheap — always keep
    if has_fetch {
        return true;
    }

    match sort_mode {
        SortMode::Strict => true,
        SortMode::PartitionOnly => is_partition_sort,
        SortMode::Adaptive => {
            if pressure == MemoryPressure::Green {
                true
            } else {
                is_partition_sort
            }
        }
    }
}

/// Walk the physical plan tree and apply adaptive sort stripping.
///
/// Returns the (possibly modified) plan and a list of decisions made.
/// Each `SortExec` node in the tree is evaluated against the decision
/// matrix and either kept or replaced with its input child.
pub fn apply_adaptive_sort(
    plan: Arc<dyn ExecutionPlan>,
    sort_mode: SortMode,
    pressure: MemoryPressure,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
) -> (Arc<dyn ExecutionPlan>, Vec<SortDecision>) {
    // Strict mode with green pressure: nothing to do, skip tree walk
    if sort_mode == SortMode::Strict {
        return (plan, vec![]);
    }

    let decisions = std::cell::RefCell::new(Vec::new());
    let fallback = Arc::clone(&plan);

    let result = plan.transform_down(|node| {
        if let Some(sort_exec) = node.downcast_ref::<SortExec>() {
            let has_fetch = sort_exec.fetch().is_some();
            let sort_cols = extract_sort_column_names(sort_exec);
            let input = Arc::clone(sort_exec.children()[0]);

            let partition_cols = find_partition_columns(&input);
            let is_partition_sort =
                !sort_cols.is_empty() && sort_cols.iter().all(|c| partition_cols.contains(c));

            let keep = decide_sort(sort_mode, pressure, is_partition_sort, has_fetch);

            let reason = if has_fetch {
                "TopK sort (has LIMIT) — always kept".to_string()
            } else if keep {
                match sort_mode {
                    SortMode::Strict => "strict mode — always keep".to_string(),
                    SortMode::PartitionOnly => {
                        "partition-only mode — sort keys match partition columns".to_string()
                    }
                    SortMode::Adaptive => {
                        format!("adaptive mode — pressure={pressure}, partition sort")
                    }
                }
            } else {
                match sort_mode {
                    SortMode::PartitionOnly => format!(
                        "partition-only mode — sort keys [{}] are not partition columns [{}]",
                        sort_cols.join(", "),
                        partition_cols.join(", "),
                    ),
                    SortMode::Adaptive => {
                        format!("adaptive mode — pressure={pressure}, non-partition sort stripped",)
                    }
                    SortMode::Strict => unreachable!(),
                }
            };

            decisions.borrow_mut().push(SortDecision {
                sort_columns: sort_cols.clone(),
                kept: keep,
                reason,
            });

            if keep {
                Ok(Transformed::no(node))
            } else {
                // Record metric for stripped sorts
                if let Some(m) = metrics {
                    let mode_label = match sort_mode {
                        SortMode::Strict => "strict",
                        SortMode::PartitionOnly => "partition_only",
                        SortMode::Adaptive => "adaptive",
                    };
                    let reason_label = match sort_mode {
                        SortMode::PartitionOnly => "partition_only",
                        SortMode::Adaptive => "memory_pressure",
                        SortMode::Strict => "strict",
                    };
                    m.sorts_stripped_total
                        .with_label_values(&[mode_label, reason_label])
                        .inc();
                }

                info!(
                    sort_columns = ?sort_cols,
                    sort_mode = ?sort_mode,
                    pressure = %pressure,
                    "ORDER BY stripped — sort keys are not partition columns. \
                     Set sort_mode = \"strict\" to force all sorts."
                );

                Ok(Transformed::yes(input))
            }
        } else {
            Ok(Transformed::no(node))
        }
    });

    let final_plan = match result {
        Ok(transformed) => transformed.data,
        Err(_) => fallback,
    };

    let decisions = decisions.into_inner();
    (final_plan, decisions)
}

/// Walk a subtree to find `IcebergScanExec` and extract partition column names.
///
/// Returns an empty vec if no `IcebergScanExec` is found (e.g., metadata queries
/// or memory-backed plans).
fn find_partition_columns(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut stack: Vec<Arc<dyn ExecutionPlan>> = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if let Some(iceberg_scan) = node.downcast_ref::<IcebergScanExec>() {
            return iceberg_scan.partition_column_names();
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    vec![]
}

/// Extract column names from `SortExec` sort expressions.
///
/// Each `PhysicalSortExpr` wraps a physical expression. When the expression
/// is a `Column`, we extract its name directly. For complex expressions
/// (e.g., functions), we fall back to the `Display` representation.
fn extract_sort_column_names(sort_exec: &SortExec) -> Vec<String> {
    use datafusion::physical_plan::expressions::Column;

    sort_exec
        .expr()
        .iter()
        .map(|sort_expr| {
            if let Some(col) = sort_expr.expr.downcast_ref::<Column>() {
                col.name().to_string()
            } else {
                // Fallback: use Display representation for complex expressions
                format!("{}", sort_expr.expr)
            }
        })
        .collect()
}

/// Format a user-facing warning string when sorts were stripped.
///
/// Returns `None` if no sorts were stripped (all kept or no sorts in plan).
pub fn format_sort_warning(decisions: &[SortDecision], sort_mode: SortMode) -> Option<String> {
    let stripped: Vec<&SortDecision> = decisions.iter().filter(|d| !d.kept).collect();
    if stripped.is_empty() {
        return None;
    }

    let all_cols: Vec<String> = stripped
        .iter()
        .flat_map(|d| d.sort_columns.iter().cloned())
        .collect();

    let mode_str = match sort_mode {
        SortMode::Strict => "strict",
        SortMode::PartitionOnly => "partition_only",
        SortMode::Adaptive => "adaptive",
    };

    Some(format!(
        "ORDER BY [{}] was removed (sort_mode={mode_str}). \
         Results are returned in partition order. \
         To force sorting, set sort_mode=strict or remove ORDER BY if ordering is not required.",
        all_cols.join(", "),
    ))
}

/// Format an actionable rejection message for Red memory pressure.
///
/// When memory is critical and a query with ORDER BY is rejected,
/// this provides specific guidance on what the user can do.
pub fn format_pressure_rejection(sort_columns: &[String], pressure: MemoryPressure) -> String {
    if sort_columns.is_empty() {
        return format!(
            "Query rejected: server memory is {pressure} (>95% utilized). Please retry later."
        );
    }

    format!(
        "Query rejected: server memory is >95% utilized. \
         Your query includes ORDER BY [{}] which requires sort buffers. Options:\n  \
         1. Remove ORDER BY from your query (data returns in partition order)\n  \
         2. Add LIMIT to reduce sort memory (e.g., ORDER BY {} LIMIT 1000)\n  \
         3. Retry later when memory pressure decreases\n  \
         4. Ask your administrator to set sort_mode=adaptive for automatic sort management",
        sort_columns.join(", "),
        sort_columns.first().unwrap_or(&"col".to_string()),
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::memory::LazyMemoryExec;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("year", DataType::Int32, false),
            Field::new("month", DataType::Int32, false),
        ]))
    }

    fn make_memory_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    fn make_sort_plan(
        input: Arc<dyn ExecutionPlan>,
        col_names: &[&str],
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let sort_exprs: Vec<PhysicalSortExpr> = col_names
            .iter()
            .map(|name| {
                PhysicalSortExpr::new(
                    col(name, &schema).unwrap(),
                    datafusion::arrow::compute::SortOptions::default(),
                )
            })
            .collect();
        let ordering = LexOrdering::new(sort_exprs).unwrap();
        let mut sort = SortExec::new(ordering, input);
        if let Some(f) = fetch {
            sort = sort.with_fetch(Some(f));
        }
        Arc::new(sort)
    }

    // ---- decide_sort unit tests ----

    #[test]
    fn test_strict_always_keeps() {
        assert!(decide_sort(
            SortMode::Strict,
            MemoryPressure::Green,
            false,
            false
        ));
        assert!(decide_sort(
            SortMode::Strict,
            MemoryPressure::Red,
            false,
            false
        ));
        assert!(decide_sort(
            SortMode::Strict,
            MemoryPressure::Orange,
            true,
            false
        ));
    }

    #[test]
    fn test_partition_only_keeps_partition_sort() {
        assert!(decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            true,
            false
        ));
        assert!(decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Red,
            true,
            false
        ));
    }

    #[test]
    fn test_partition_only_strips_non_partition() {
        assert!(!decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            false,
            false
        ));
        assert!(!decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Red,
            false,
            false
        ));
    }

    #[test]
    fn test_adaptive_green_keeps_all() {
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Green,
            false,
            false
        ));
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Green,
            true,
            false
        ));
    }

    #[test]
    fn test_adaptive_pressure_keeps_partition_sort() {
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            true,
            false
        ));
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Orange,
            true,
            false
        ));
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Red,
            true,
            false
        ));
    }

    #[test]
    fn test_adaptive_pressure_strips_non_partition() {
        assert!(!decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            false,
            false
        ));
        assert!(!decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Orange,
            false,
            false
        ));
        assert!(!decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Red,
            false,
            false
        ));
    }

    #[test]
    fn test_topk_always_kept() {
        // TopK (fetch=true) should always be kept regardless of mode/pressure
        assert!(decide_sort(
            SortMode::Adaptive,
            MemoryPressure::Red,
            false,
            true
        ));
        assert!(decide_sort(
            SortMode::PartitionOnly,
            MemoryPressure::Green,
            false,
            true
        ));
        assert!(decide_sort(
            SortMode::Strict,
            MemoryPressure::Red,
            false,
            true
        ));
    }

    // ---- apply_adaptive_sort integration tests ----

    #[test]
    fn test_strict_mode_no_op() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input, &["name"], None);

        let (result, decisions) =
            apply_adaptive_sort(plan.clone(), SortMode::Strict, MemoryPressure::Red, None);

        // Strict mode skips the tree walk entirely
        assert!(decisions.is_empty());
        // Plan should be unchanged (same Arc)
        assert!(Arc::ptr_eq(&result, &plan));
    }

    #[test]
    fn test_partition_only_strips_non_partition_sort() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input.clone(), &["name"], None);

        let (result, decisions) =
            apply_adaptive_sort(plan, SortMode::PartitionOnly, MemoryPressure::Green, None);

        // "name" is not a partition column (no IcebergScanExec in the tree)
        assert_eq!(decisions.len(), 1);
        assert!(!decisions[0].kept);
        // Result should be the input (sort stripped)
        assert_eq!(result.name(), "LazyMemoryExec");
    }

    #[test]
    fn test_adaptive_green_keeps_sort() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input, &["name"], None);

        let (result, decisions) =
            apply_adaptive_sort(plan, SortMode::Adaptive, MemoryPressure::Green, None);

        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].kept);
        assert_eq!(result.name(), "SortExec");
    }

    #[test]
    fn test_adaptive_yellow_strips_non_partition() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input, &["name"], None);

        let (result, decisions) =
            apply_adaptive_sort(plan, SortMode::Adaptive, MemoryPressure::Yellow, None);

        assert_eq!(decisions.len(), 1);
        assert!(!decisions[0].kept);
        assert_eq!(result.name(), "LazyMemoryExec");
    }

    #[test]
    fn test_topk_always_kept_in_plan() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input, &["name"], Some(100));

        let (result, decisions) =
            apply_adaptive_sort(plan, SortMode::Adaptive, MemoryPressure::Red, None);

        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].kept);
        // DataFusion names SortExec with LIMIT as "SortExec(TopK)"
        assert!(
            result.name().starts_with("SortExec"),
            "Expected SortExec, got {}",
            result.name()
        );
    }

    #[test]
    fn test_no_sort_in_plan() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);

        let (result, decisions) =
            apply_adaptive_sort(plan.clone(), SortMode::Adaptive, MemoryPressure::Red, None);

        assert!(decisions.is_empty());
        assert!(Arc::ptr_eq(&result, &plan));
    }

    #[test]
    fn test_metrics_incremented_on_strip() {
        let schema = test_schema();
        let input = make_memory_plan(schema);
        let plan = make_sort_plan(input, &["name"], None);
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());

        let (_result, decisions) = apply_adaptive_sort(
            plan,
            SortMode::Adaptive,
            MemoryPressure::Yellow,
            Some(&metrics),
        );

        assert_eq!(decisions.len(), 1);
        assert!(!decisions[0].kept);
        assert_eq!(
            metrics
                .sorts_stripped_total
                .with_label_values(&["adaptive", "memory_pressure"])
                .get(),
            1
        );
    }

    // ---- format_sort_warning tests ----

    #[test]
    fn test_warning_none_when_all_kept() {
        let decisions = vec![SortDecision {
            sort_columns: vec!["name".to_string()],
            kept: true,
            reason: "test".to_string(),
        }];

        assert!(format_sort_warning(&decisions, SortMode::Adaptive).is_none());
    }

    #[test]
    fn test_warning_present_when_stripped() {
        let decisions = vec![SortDecision {
            sort_columns: vec!["name".to_string(), "id".to_string()],
            kept: false,
            reason: "test".to_string(),
        }];

        let warning = format_sort_warning(&decisions, SortMode::Adaptive).unwrap();
        assert!(warning.contains("ORDER BY [name, id]"));
        assert!(warning.contains("sort_mode=adaptive"));
        assert!(warning.contains("partition order"));
    }

    #[test]
    fn test_warning_empty_decisions() {
        assert!(format_sort_warning(&[], SortMode::Adaptive).is_none());
    }

    // ---- format_pressure_rejection tests ----

    #[test]
    fn test_pressure_rejection_with_sort_columns() {
        let cols = vec!["col1".to_string(), "col2".to_string()];
        let msg = format_pressure_rejection(&cols, MemoryPressure::Red);
        assert!(msg.contains("ORDER BY [col1, col2]"));
        assert!(msg.contains("Remove ORDER BY"));
        assert!(msg.contains("LIMIT"));
        assert!(msg.contains("sort_mode=adaptive"));
    }

    #[test]
    fn test_pressure_rejection_no_sort_columns() {
        let msg = format_pressure_rejection(&[], MemoryPressure::Red);
        assert!(msg.contains("server memory is red"));
        assert!(msg.contains("retry later"));
    }

    // ---- extract_sort_column_names tests ----

    #[test]
    fn test_extract_sort_column_names() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_exprs = vec![
            PhysicalSortExpr::new(
                col("name", &schema).unwrap(),
                datafusion::arrow::compute::SortOptions::default(),
            ),
            PhysicalSortExpr::new(
                col("id", &schema).unwrap(),
                datafusion::arrow::compute::SortOptions::default(),
            ),
        ];
        let ordering = LexOrdering::new(sort_exprs).unwrap();
        let sort_exec = SortExec::new(ordering, input);

        let names = extract_sort_column_names(&sort_exec);
        assert_eq!(names, vec!["name", "id"]);
    }

    // ---- find_partition_columns tests ----

    #[test]
    fn test_find_partition_columns_no_iceberg_scan() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);
        let cols = find_partition_columns(&plan);
        assert!(cols.is_empty());
    }
}

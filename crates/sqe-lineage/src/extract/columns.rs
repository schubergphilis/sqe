//! Column-level lineage extraction from a DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.
//!
//! Each node in the plan tree maps a `ColumnTrace`: for output column ordinal i,
//! the list of leaf-column dependencies. Tasks E4-E12 wire up per-node rules.

use crate::event::Transformation;
use datafusion::common::Column;
use datafusion::logical_expr::{
    Aggregate, Expr, Join, LogicalPlan, Projection, Sort, TableScan, Union,
};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct ColumnDep {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub field: String,
    pub transformation: Transformation,
}

/// `Trace[i]` is the list of leaf-column dependencies of the i-th output
/// column of a plan node.
pub type ColumnTrace = Vec<Vec<ColumnDep>>;

// ---------------------------------------------------------------------------
// Transformation factories
//
// OL TransformationType taxonomy (spec §5.2 + §5.3):
//   DIRECT   - data flows through the column (IDENTITY, TRANSFORMATION, AGGREGATION,
//              WINDOW, MERGE_INSERT, MERGE_UPDATE, MASKED)
//   INDIRECT - used in filter/join/group-by/sort but doesn't produce data values
//              (FILTER, JOIN, GROUP_BY, SORT, WINDOW, CONDITIONAL)
// ---------------------------------------------------------------------------

pub fn direct_identity()       -> Transformation { make("DIRECT",   "IDENTITY",       false) }
pub fn direct_transformation() -> Transformation { make("DIRECT",   "TRANSFORMATION", false) }
pub fn direct_aggregation()    -> Transformation { make("DIRECT",   "AGGREGATION",    false) }
pub fn direct_window()         -> Transformation { make("DIRECT",   "WINDOW",         false) }
pub fn indirect_filter()       -> Transformation { make("INDIRECT", "FILTER",         false) }
pub fn indirect_join()         -> Transformation { make("INDIRECT", "JOIN",           false) }
pub fn indirect_groupby()      -> Transformation { make("INDIRECT", "GROUP_BY",       false) }
pub fn indirect_sort()         -> Transformation { make("INDIRECT", "SORT",           false) }
pub fn indirect_window()       -> Transformation { make("INDIRECT", "WINDOW",         false) }
pub fn indirect_conditional()  -> Transformation { make("INDIRECT", "CONDITIONAL",    false) }
pub fn masked()                -> Transformation { make("DIRECT",   "MASKED",         true)  }
pub fn merge_insert()          -> Transformation { make("DIRECT",   "MERGE_INSERT",   false) }
pub fn merge_update()          -> Transformation { make("DIRECT",   "MERGE_UPDATE",   false) }

fn make(kind: &str, subtype: &str, masking: bool) -> Transformation {
    Transformation {
        kind: kind.into(),
        subtype: subtype.into(),
        description: String::new(),
        masking,
    }
}

// ---------------------------------------------------------------------------
// Per-node trace rules. `trace_plan` walks the plan bottom-up and dispatches
// by node kind. Unknown nodes return an empty trace (conservative for v1).
// ---------------------------------------------------------------------------

/// Parse a (possibly quoted) qualified table name into (catalog, schema, table)
/// applying the same fallback rules as `extract::datasets::parse_table_ref`.
fn parse_table_ref(name: &str) -> (String, String, String) {
    let parts: Vec<String> = name
        .split('.')
        .map(|s| s.trim_matches('"').to_string())
        .collect();
    match parts.len() {
        3 => (parts[0].clone(), parts[1].clone(), parts[2].clone()),
        2 => ("default".to_string(), parts[0].clone(), parts[1].clone()),
        1 => (
            "default".to_string(),
            "default".to_string(),
            parts[0].clone(),
        ),
        _ => (String::new(), String::new(), name.to_string()),
    }
}

/// TableScan column trace rule (E4): each scan column emits one
/// `ColumnDep` with `direct_identity()`. Terminal node, no recursion.
fn trace_table_scan(ts: &TableScan) -> ColumnTrace {
    let (catalog, schema, table) = parse_table_ref(&ts.table_name.to_string());
    ts.source
        .schema()
        .fields()
        .iter()
        .map(|f| {
            vec![ColumnDep {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                field: f.name().clone(),
                transformation: direct_identity(),
            }]
        })
        .collect()
}

/// Strip an outer `Expr::Alias` to inspect the underlying expression shape.
fn unwrap_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => unwrap_alias(&a.expr),
        other => other,
    }
}

/// Look up the index of a `Column` ref inside an input plan's schema.
fn column_index(plan: &LogicalPlan, col: &Column) -> Option<usize> {
    plan.schema().maybe_index_of_column(col)
}

/// Collect all column-ref deps for an expression by mapping each `&Column`
/// referenced in the expression through the child trace.
fn deps_for_refs(
    refs: &HashSet<&Column>,
    input: &LogicalPlan,
    child_trace: &ColumnTrace,
    transformation: &Transformation,
) -> Vec<ColumnDep> {
    let mut out = Vec::new();
    for c in refs {
        let Some(idx) = column_index(input, c) else {
            continue;
        };
        let Some(child_deps) = child_trace.get(idx) else {
            continue;
        };
        for dep in child_deps {
            out.push(ColumnDep {
                catalog: dep.catalog.clone(),
                schema: dep.schema.clone(),
                table: dep.table.clone(),
                field: dep.field.clone(),
                transformation: transformation.clone(),
            });
        }
    }
    out
}

/// Projection column trace rule (E5).
///
/// Bare column refs (`Expr::Column` after stripping aliases) preserve the
/// upstream transformation (typically IDENTITY). Any other shape is treated
/// as a real computation and gets `direct_transformation()`.
fn trace_projection(p: &Projection, child_trace: ColumnTrace) -> ColumnTrace {
    p.expr
        .iter()
        .map(|e| {
            let bare = unwrap_alias(e);
            if let Expr::Column(c) = bare {
                // Pure passthrough: copy the child trace at the column's index
                if let Some(idx) = column_index(p.input.as_ref(), c) {
                    if let Some(deps) = child_trace.get(idx) {
                        return deps.clone();
                    }
                }
                Vec::new()
            } else {
                let refs = e.column_refs();
                deps_for_refs(
                    &refs,
                    p.input.as_ref(),
                    &child_trace,
                    &direct_transformation(),
                )
            }
        })
        .collect()
}

/// Append the deps of `expr`'s column refs (mapped through the input plan)
/// to every output column in `child_trace`, tagged with `transformation`.
fn attach_indirect(
    child_trace: &mut ColumnTrace,
    input: &LogicalPlan,
    expr: &Expr,
    transformation: Transformation,
) {
    let refs = expr.column_refs();
    let extras = deps_for_refs(&refs, input, child_trace, &transformation);
    if extras.is_empty() {
        return;
    }
    for out in child_trace.iter_mut() {
        out.extend(extras.iter().cloned());
    }
}

/// Aggregate column trace rule (E7).
///
/// Output schema is `group_expr` columns followed by `aggr_expr` columns.
/// - Group-by column: bare ref preserves IDENTITY; computed expr -> TRANSFORMATION.
/// - Aggregate column: AGGREGATION on its argument columns plus INDIRECT/GROUP_BY
///   for every group-by column referenced.
fn trace_aggregate(a: &Aggregate, child_trace: ColumnTrace) -> ColumnTrace {
    let input = a.input.as_ref();

    // 1. Trace each group-by column.
    let group_traces: Vec<Vec<ColumnDep>> = a
        .group_expr
        .iter()
        .map(|e| {
            let bare = unwrap_alias(e);
            if let Expr::Column(c) = bare {
                if let Some(idx) = column_index(input, c) {
                    if let Some(deps) = child_trace.get(idx) {
                        return deps.clone();
                    }
                }
                Vec::new()
            } else {
                let refs = e.column_refs();
                deps_for_refs(&refs, input, &child_trace, &direct_transformation())
            }
        })
        .collect();

    // 2. Build the INDIRECT/GROUP_BY deps to attach to every aggregated column.
    let mut group_indirect: Vec<ColumnDep> = Vec::new();
    for e in &a.group_expr {
        let refs = e.column_refs();
        group_indirect.extend(deps_for_refs(
            &refs,
            input,
            &child_trace,
            &indirect_groupby(),
        ));
    }

    // 3. Trace each aggregate column: AGGREGATION on argument columns + GROUP_BY indirect.
    let agg_traces: Vec<Vec<ColumnDep>> = a
        .aggr_expr
        .iter()
        .map(|e| {
            let refs = e.column_refs();
            let mut deps =
                deps_for_refs(&refs, input, &child_trace, &direct_aggregation());
            deps.extend(group_indirect.iter().cloned());
            deps
        })
        .collect();

    let mut out: ColumnTrace = group_traces;
    out.extend(agg_traces);
    out
}

/// Join column trace rule (E8).
///
/// Output schema is left's columns followed by right's. Each output passes
/// through its source side's lineage. Every column referenced by the join
/// predicate (`on` pairs and optional `filter`) adds an INDIRECT/JOIN dep
/// to all output columns.
fn trace_join(j: &Join) -> ColumnTrace {
    let left_trace = trace_plan(j.left.as_ref());
    let right_trace = trace_plan(j.right.as_ref());

    let mut out: ColumnTrace = left_trace.clone();
    out.extend(right_trace.clone());

    // Collect deps from every column referenced in the join predicate (both
    // left and right sides), classified by which child plan owns the column.
    let mut indirect: Vec<ColumnDep> = Vec::new();
    let join_t = indirect_join();

    let mut add_from_expr = |e: &Expr| {
        let refs = e.column_refs();
        for c in &refs {
            if let Some(idx) = column_index(j.left.as_ref(), c) {
                if let Some(deps) = left_trace.get(idx) {
                    for dep in deps {
                        indirect.push(ColumnDep {
                            catalog: dep.catalog.clone(),
                            schema: dep.schema.clone(),
                            table: dep.table.clone(),
                            field: dep.field.clone(),
                            transformation: join_t.clone(),
                        });
                    }
                }
            } else if let Some(idx) = column_index(j.right.as_ref(), c) {
                if let Some(deps) = right_trace.get(idx) {
                    for dep in deps {
                        indirect.push(ColumnDep {
                            catalog: dep.catalog.clone(),
                            schema: dep.schema.clone(),
                            table: dep.table.clone(),
                            field: dep.field.clone(),
                            transformation: join_t.clone(),
                        });
                    }
                }
            }
        }
    };

    for (l, r) in &j.on {
        add_from_expr(l);
        add_from_expr(r);
    }
    if let Some(f) = &j.filter {
        add_from_expr(f);
    }

    if !indirect.is_empty() {
        for col_trace in out.iter_mut() {
            col_trace.extend(indirect.iter().cloned());
        }
    }

    out
}

/// Union column trace rule (E9).
///
/// Output column i merges deps from each input child's column at position i.
fn trace_union(u: &Union) -> ColumnTrace {
    let child_traces: Vec<ColumnTrace> =
        u.inputs.iter().map(|child| trace_plan(child)).collect();
    let width = child_traces
        .iter()
        .map(|t| t.len())
        .max()
        .unwrap_or(0);

    (0..width)
        .map(|i| {
            let mut merged: Vec<ColumnDep> = Vec::new();
            for ct in &child_traces {
                if let Some(deps) = ct.get(i) {
                    merged.extend(deps.iter().cloned());
                }
            }
            merged
        })
        .collect()
}

/// Sort column trace rule (E9): passthrough on outputs; every column
/// referenced by a sort key adds an INDIRECT/SORT dep to all outputs.
fn trace_sort(s: &Sort, mut child_trace: ColumnTrace) -> ColumnTrace {
    for sort_expr in &s.expr {
        attach_indirect(
            &mut child_trace,
            s.input.as_ref(),
            &sort_expr.expr,
            indirect_sort(),
        );
    }
    child_trace
}

/// Walk a `LogicalPlan` bottom-up and emit per-output-column lineage
/// (`ColumnTrace[i]` lists leaf-column deps for output column i).
pub fn trace_plan(plan: &LogicalPlan) -> ColumnTrace {
    match plan {
        LogicalPlan::TableScan(ts) => trace_table_scan(ts),
        LogicalPlan::Projection(p) => {
            let child = trace_plan(p.input.as_ref());
            trace_projection(p, child)
        }
        LogicalPlan::Filter(f) => {
            let mut t = trace_plan(f.input.as_ref());
            attach_indirect(&mut t, f.input.as_ref(), &f.predicate, indirect_filter());
            t
        }
        LogicalPlan::Aggregate(a) => {
            let child = trace_plan(a.input.as_ref());
            trace_aggregate(a, child)
        }
        LogicalPlan::Join(j) => trace_join(j),
        LogicalPlan::Union(u) => trace_union(u),
        LogicalPlan::Sort(s) => {
            let child = trace_plan(s.input.as_ref());
            trace_sort(s, child)
        }
        LogicalPlan::Limit(l) => trace_plan(l.input.as_ref()),
        LogicalPlan::Distinct(d) => trace_plan(d.input().as_ref()),
        LogicalPlan::SubqueryAlias(s) => trace_plan(s.input.as_ref()),
        // Unknown nodes -> empty trace. Subsequent E10 task fills in Window.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_produce_expected_taxonomy() {
        let t = direct_identity();
        assert_eq!(t.kind, "DIRECT");
        assert_eq!(t.subtype, "IDENTITY");
        assert!(!t.masking);

        let m = masked();
        assert_eq!(m.kind, "DIRECT");
        assert_eq!(m.subtype, "MASKED");
        assert!(m.masking);

        let f = indirect_filter();
        assert_eq!(f.kind, "INDIRECT");
        assert_eq!(f.subtype, "FILTER");
    }
}

//! Column-level lineage extraction from a DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.
//!
//! Each node in the plan tree maps a `ColumnTrace`: for output column ordinal i,
//! the list of leaf-column dependencies. Tasks E4-E12 wire up per-node rules.

use crate::event::Transformation;
use datafusion::logical_expr::{LogicalPlan, TableScan};

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

/// Walk a `LogicalPlan` bottom-up and emit per-output-column lineage
/// (`ColumnTrace[i]` lists leaf-column deps for output column i).
pub fn trace_plan(plan: &LogicalPlan) -> ColumnTrace {
    match plan {
        LogicalPlan::TableScan(ts) => trace_table_scan(ts),
        // Unknown nodes -> empty trace. Subsequent E5-E10 tasks fill these in.
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

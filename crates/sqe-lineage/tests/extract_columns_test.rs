//! Per-node column trace rule tests for `extract::columns::trace_plan`.
//!
//! Tasks E4-E10 add one rule at a time. Tests cover the behaviour each rule
//! is supposed to encode (IDENTITY/TRANSFORMATION/AGGREGATION/etc).

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{
    col, lit, Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder,
};
use sqe_lineage::extract::columns;
use std::sync::Arc;

/// Build a TableScan over a MemTable with a 3-part qualified name.
fn build_simple_scan(
    catalog: &str,
    schema: &str,
    table: &str,
    cols: &[(&str, DataType)],
) -> LogicalPlan {
    let arrow_schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t)| Field::new(*n, t.clone(), false))
            .collect::<Vec<_>>(),
    ));
    let mem = MemTable::try_new(arrow_schema, vec![vec![]]).unwrap();
    let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(mem);
    let table_ref = TableReference::full(catalog, schema, table);
    LogicalPlanBuilder::scan(table_ref, provider_as_source(provider), None)
        .unwrap()
        .build()
        .unwrap()
}

#[test]
fn table_scan_emits_one_identity_dep_per_column() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let trace = columns::trace_plan(&plan);

    assert_eq!(trace.len(), 2, "two output columns");

    // id column
    assert_eq!(trace[0].len(), 1);
    let dep = &trace[0][0];
    assert_eq!(dep.catalog, "polaris");
    assert_eq!(dep.schema, "sales");
    assert_eq!(dep.table, "orders");
    assert_eq!(dep.field, "id");
    assert_eq!(dep.transformation.kind, "DIRECT");
    assert_eq!(dep.transformation.subtype, "IDENTITY");

    // amount column
    assert_eq!(trace[1].len(), 1);
    let dep = &trace[1][0];
    assert_eq!(dep.catalog, "polaris");
    assert_eq!(dep.schema, "sales");
    assert_eq!(dep.table, "orders");
    assert_eq!(dep.field, "amount");
    assert_eq!(dep.transformation.kind, "DIRECT");
    assert_eq!(dep.transformation.subtype, "IDENTITY");
}

#[test]
fn projection_passthrough_is_identity() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("id"), col("amount")])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // Bare column refs preserve the upstream IDENTITY
    assert_eq!(trace[0].len(), 1);
    assert_eq!(trace[0][0].field, "id");
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");
    assert_eq!(trace[1].len(), 1);
    assert_eq!(trace[1][0].field, "amount");
    assert_eq!(trace[1][0].transformation.subtype, "IDENTITY");
}

#[test]
fn projection_expr_is_transformation() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let doubled: Expr = (col("amount") * lit(2_i64)).alias("doubled");
    let plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("id"), doubled])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // id passthrough remains IDENTITY
    assert_eq!(trace[0][0].field, "id");
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");

    // doubled column references `amount` with TRANSFORMATION (computation)
    assert_eq!(trace[1].len(), 1, "doubled has one input dep: amount");
    assert_eq!(trace[1][0].field, "amount");
    assert_eq!(trace[1][0].transformation.kind, "DIRECT");
    assert_eq!(trace[1][0].transformation.subtype, "TRANSFORMATION");
}

#[test]
fn filter_adds_indirect_to_all_outputs() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("id"), col("amount")])
        .unwrap()
        .filter(col("amount").gt(lit(100_i64)))
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // trace[0] = id: still has direct IDENTITY + an INDIRECT/FILTER on `amount`
    let id_subtypes: Vec<&str> = trace[0]
        .iter()
        .map(|d| d.transformation.subtype.as_str())
        .collect();
    assert!(
        id_subtypes.contains(&"IDENTITY"),
        "id keeps IDENTITY through filter"
    );
    let id_filter_dep = trace[0]
        .iter()
        .find(|d| d.transformation.subtype == "FILTER")
        .expect("id has INDIRECT/FILTER dep");
    assert_eq!(id_filter_dep.transformation.kind, "INDIRECT");
    assert_eq!(id_filter_dep.field, "amount");

    // trace[1] = amount: same pattern, plus a self-FILTER on amount
    let amount_filter_dep = trace[1]
        .iter()
        .find(|d| d.transformation.subtype == "FILTER")
        .expect("amount has INDIRECT/FILTER dep");
    assert_eq!(amount_filter_dep.transformation.kind, "INDIRECT");
    assert_eq!(amount_filter_dep.field, "amount");
}

#[test]
fn aggregate_groupby_preserves_identity() {
    use datafusion::functions_aggregate::expr_fn::sum;

    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .aggregate(vec![col("id")], vec![sum(col("amount")).alias("total")])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2, "id (group) + total (agg)");

    // group-by id keeps IDENTITY
    assert_eq!(trace[0].len(), 1);
    assert_eq!(trace[0][0].field, "id");
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");

    // total: AGGREGATION on amount + INDIRECT/GROUP_BY on id
    let agg_dep = trace[1]
        .iter()
        .find(|d| d.transformation.subtype == "AGGREGATION")
        .expect("total has DIRECT/AGGREGATION");
    assert_eq!(agg_dep.transformation.kind, "DIRECT");
    assert_eq!(agg_dep.field, "amount");

    let gb_dep = trace[1]
        .iter()
        .find(|d| d.transformation.subtype == "GROUP_BY")
        .expect("total has INDIRECT/GROUP_BY");
    assert_eq!(gb_dep.transformation.kind, "INDIRECT");
    assert_eq!(gb_dep.field, "id");
}

#[test]
fn aggregate_groupby_adds_indirect_to_all_aggs() {
    use datafusion::functions_aggregate::expr_fn::{count, sum};

    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .aggregate(
            vec![col("id")],
            vec![
                sum(col("amount")).alias("total"),
                count(col("amount")).alias("n"),
            ],
        )
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 3);

    // Both aggregates carry an INDIRECT/GROUP_BY referencing `id`
    for (label, idx) in [("total", 1usize), ("n", 2usize)] {
        let gb = trace[idx]
            .iter()
            .find(|d| d.transformation.subtype == "GROUP_BY")
            .unwrap_or_else(|| panic!("{label} should have GROUP_BY dep"));
        assert_eq!(gb.transformation.kind, "INDIRECT");
        assert_eq!(gb.field, "id");
    }

    // The group-by column itself should NOT have a self-GROUP_BY dep
    assert!(
        trace[0]
            .iter()
            .all(|d| d.transformation.subtype != "GROUP_BY"),
        "group-by column should not have its own GROUP_BY dep"
    );
}

#[test]
fn join_passes_through_each_side_with_indirect_join_on_predicate() {
    let left = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let right = build_simple_scan(
        "polaris",
        "sales",
        "customers",
        &[("id", DataType::Int64), ("region", DataType::Utf8)],
    );

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (
                vec![Column::new(Some("orders"), "id")],
                vec![Column::new(Some("customers"), "id")],
            ),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 4, "left.id, left.amount, right.id, right.region");

    // Each output column has an IDENTITY dep from its source side
    let id_left = &trace[0];
    let id_left_identity = id_left
        .iter()
        .find(|d| d.transformation.subtype == "IDENTITY")
        .expect("left.id has IDENTITY dep");
    assert_eq!(id_left_identity.table, "orders");
    assert_eq!(id_left_identity.field, "id");

    let amount = &trace[1];
    let amount_identity = amount
        .iter()
        .find(|d| d.transformation.subtype == "IDENTITY")
        .expect("amount has IDENTITY dep");
    assert_eq!(amount_identity.table, "orders");

    let id_right = &trace[2];
    let id_right_identity = id_right
        .iter()
        .find(|d| d.transformation.subtype == "IDENTITY")
        .expect("right.id has IDENTITY dep");
    assert_eq!(id_right_identity.table, "customers");

    let region = &trace[3];
    let region_identity = region
        .iter()
        .find(|d| d.transformation.subtype == "IDENTITY")
        .expect("region has IDENTITY dep");
    assert_eq!(region_identity.table, "customers");

    // Every output column has INDIRECT/JOIN deps from join predicate columns
    for (idx, name) in [(0, "id_left"), (1, "amount"), (2, "id_right"), (3, "region")] {
        let join_deps: Vec<&str> = trace[idx]
            .iter()
            .filter(|d| d.transformation.subtype == "JOIN")
            .map(|d| d.field.as_str())
            .collect();
        assert!(
            !join_deps.is_empty(),
            "{name} should have INDIRECT/JOIN deps"
        );
        assert!(
            join_deps.contains(&"id"),
            "{name} should reference `id` via JOIN"
        );
    }
}

#[test]
fn union_merges_traces_by_position() {
    let left = build_simple_scan(
        "polaris",
        "sales",
        "orders_2024",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let right = build_simple_scan(
        "polaris",
        "sales",
        "orders_2025",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(left)
        .union(right)
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // Output[0] (id) merges deps from both inputs at position 0
    let id_tables: Vec<&str> = trace[0].iter().map(|d| d.table.as_str()).collect();
    assert!(
        id_tables.contains(&"orders_2024"),
        "id should include left source"
    );
    assert!(
        id_tables.contains(&"orders_2025"),
        "id should include right source"
    );

    // Output[1] (amount) same
    let amount_tables: Vec<&str> = trace[1].iter().map(|d| d.table.as_str()).collect();
    assert!(amount_tables.contains(&"orders_2024"));
    assert!(amount_tables.contains(&"orders_2025"));
}

#[test]
fn sort_adds_indirect_sort_deps() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .sort(vec![col("amount").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // Both outputs keep IDENTITY plus pick up an INDIRECT/SORT on `amount`
    for (idx, name) in [(0, "id"), (1, "amount")] {
        let sort_dep = trace[idx]
            .iter()
            .find(|d| d.transformation.subtype == "SORT")
            .unwrap_or_else(|| panic!("{name} should have SORT dep"));
        assert_eq!(sort_dep.transformation.kind, "INDIRECT");
        assert_eq!(sort_dep.field, "amount");
    }
}

#[test]
fn limit_passes_through() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");
    assert_eq!(trace[1][0].transformation.subtype, "IDENTITY");
}

#[test]
fn distinct_passes_through() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .distinct()
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");
    assert_eq!(trace[1][0].transformation.subtype, "IDENTITY");
}

#[test]
fn window_args_direct_partition_indirect() {
    use datafusion::functions_window::expr_fn::row_number;

    let plan = build_simple_scan(
        "polaris",
        "events",
        "logins",
        &[
            ("user_id", DataType::Int64),
            ("ts", DataType::Int64),
            ("amount", DataType::Float64),
        ],
    );

    let win = row_number()
        .partition_by(vec![col("user_id")])
        .order_by(vec![col("ts").sort(true, false)])
        .build()
        .unwrap()
        .alias("rn");

    let plan = LogicalPlanBuilder::from(plan)
        .window(vec![win])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    // Output schema is input + window_expr columns: user_id, ts, amount, rn
    assert_eq!(trace.len(), 4);

    // Inputs (positions 0..3) pass through with IDENTITY
    for (idx, deps) in trace.iter().enumerate().take(3) {
        let identity = deps
            .iter()
            .find(|d| d.transformation.subtype == "IDENTITY")
            .unwrap_or_else(|| panic!("input column {idx} keeps IDENTITY"));
        assert_eq!(identity.transformation.kind, "DIRECT");

        // Each input also picks up an INDIRECT/WINDOW dep on user_id
        let win_dep = deps
            .iter()
            .find(|d| {
                d.transformation.subtype == "WINDOW" && d.transformation.kind == "INDIRECT"
            })
            .unwrap_or_else(|| panic!("input column {idx} picks up INDIRECT/WINDOW"));
        // The user_id and ts are both referenced by partition/order; at minimum
        // the partition column must show up
        let _ = win_dep;
    }

    // The collected INDIRECT/WINDOW deps over all inputs reference both
    // user_id (partition) and ts (order_by)
    let win_field_set: std::collections::HashSet<&str> = trace[0]
        .iter()
        .filter(|d| {
            d.transformation.subtype == "WINDOW" && d.transformation.kind == "INDIRECT"
        })
        .map(|d| d.field.as_str())
        .collect();
    assert!(win_field_set.contains("user_id"));
    assert!(win_field_set.contains("ts"));

    // Output[3] is the window function result: DIRECT/WINDOW on its arg columns
    // (row_number has no args, so the only deps come from partition/order_by);
    // we still expect WINDOW deps and no IDENTITY deps for the new column.
    let rn = &trace[3];
    let rn_subtypes: std::collections::HashSet<&str> =
        rn.iter().map(|d| d.transformation.subtype.as_str()).collect();
    assert!(
        rn_subtypes.contains("WINDOW"),
        "row_number column has WINDOW deps"
    );
}

#[test]
fn window_with_args_marks_args_direct_window() {
    use datafusion::functions_window::expr_fn::lag;

    let plan = build_simple_scan(
        "polaris",
        "events",
        "logins",
        &[
            ("user_id", DataType::Int64),
            ("ts", DataType::Int64),
            ("amount", DataType::Float64),
        ],
    );

    // LAG(amount) OVER (PARTITION BY user_id ORDER BY ts)
    let win = lag(col("amount"), None, None)
        .partition_by(vec![col("user_id")])
        .order_by(vec![col("ts").sort(true, false)])
        .build()
        .unwrap()
        .alias("prev_amount");

    let plan = LogicalPlanBuilder::from(plan)
        .window(vec![win])
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 4);

    // The window output column has a DIRECT/WINDOW dep on the arg `amount`
    let direct_window = trace[3]
        .iter()
        .find(|d| {
            d.transformation.subtype == "WINDOW"
                && d.transformation.kind == "DIRECT"
                && d.field == "amount"
        })
        .expect("prev_amount has DIRECT/WINDOW on `amount`");
    let _ = direct_window;
}

/// Minimal `UserDefinedLogicalNodeCore` impl that wraps a single child plan
/// and exposes its schema unchanged. Used to verify that `Extension` nodes
/// are handled as passthrough by `trace_plan`.
#[derive(Debug, Hash, PartialEq, Eq)]
struct PassthroughExt {
    name: &'static str,
    input: LogicalPlan,
    schema: datafusion::common::DFSchemaRef,
}

// `DFSchema` does not implement `PartialOrd`; provide a trivial impl so the
// node satisfies the `UserDefinedLogicalNodeCore` bounds.
impl PartialOrd for PassthroughExt {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.name.partial_cmp(other.name)? {
            std::cmp::Ordering::Equal => self.input.partial_cmp(&other.input),
            other => Some(other),
        }
    }
}

impl datafusion::logical_expr::UserDefinedLogicalNodeCore for PassthroughExt {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            name: self.name,
            input: inputs.into_iter().next().expect("one input"),
            schema: self.schema.clone(),
        })
    }
}

#[test]
fn extension_node_passes_through() {
    let scan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let schema = scan.schema().clone();
    let ext_node = PassthroughExt {
        name: "sqe_policy_mask",
        input: scan,
        schema,
    };
    let plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
        node: Arc::new(ext_node),
    });

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);

    // Passthrough preserves IDENTITY for both columns.
    assert_eq!(trace[0][0].field, "id");
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");
    assert_eq!(trace[1][0].field, "amount");
    assert_eq!(trace[1][0].transformation.subtype, "IDENTITY");
}

#[test]
fn subquery_alias_passes_through() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let plan = LogicalPlanBuilder::from(plan)
        .alias("o")
        .unwrap()
        .build()
        .unwrap();

    let trace = columns::trace_plan(&plan);
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0][0].field, "id");
    assert_eq!(trace[0][0].transformation.subtype, "IDENTITY");
    assert_eq!(trace[1][0].field, "amount");
    assert_eq!(trace[1][0].transformation.subtype, "IDENTITY");
}

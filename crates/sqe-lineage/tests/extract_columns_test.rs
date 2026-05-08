//! Per-node column trace rule tests for `extract::columns::trace_plan`.
//!
//! Tasks E4-E10 add one rule at a time. Tests cover the behaviour each rule
//! is supposed to encode (IDENTITY/TRANSFORMATION/AGGREGATION/etc).

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
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

//! Per-node column trace rule tests for `extract::columns::trace_plan`.
//!
//! Tasks E4-E10 add one rule at a time. Tests cover the behaviour each rule
//! is supposed to encode (IDENTITY/TRANSFORMATION/AGGREGATION/etc).

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
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

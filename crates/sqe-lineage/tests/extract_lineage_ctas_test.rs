//! Regression test for CTAS-style write plans synthesised by the coordinator.
//!
//! The OL coordinator wraps a SELECT logical plan in a synthetic
//! `INSERT INTO target` to give the extractor a write-shaped plan it can use
//! to recover output dataset and column lineage. This test mirrors that
//! wrapping shape and asserts every target column receives a `DIRECT/IDENTITY`
//! link to its matching source column.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, dml::InsertOp, LogicalPlanBuilder};
use sqe_lineage::extract;
use std::sync::Arc;

fn lookup() -> extract::CatalogLookup {
    Arc::new(|n: &str| match n {
        "polaris" => "https://polaris.example/api/catalog".into(),
        other => format!("sqe://{other}"),
    })
}

#[test]
fn ctas_wrapper_yields_inputs_outputs_and_column_lineage() {
    // Mimic what `WriteHandler::handle_ctas_streaming` builds:
    //   CREATE TABLE polaris.sales.archive AS
    //     SELECT id, amount FROM polaris.sales.orders
    // → wrapped as INSERT INTO polaris.sales.archive (SELECT id, amount FROM polaris.sales.orders)
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let source_mem = Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap());
    let target_mem = Arc::new(MemTable::try_new(arrow_schema, vec![vec![]]).unwrap());

    let source_ref = TableReference::full("polaris", "sales", "orders");
    let scan = LogicalPlanBuilder::scan(source_ref, provider_as_source(source_mem), None)
        .unwrap()
        .project(vec![col("id"), col("amount")])
        .unwrap()
        .build()
        .unwrap();

    let target_ref = TableReference::full("polaris", "sales", "archive");
    let plan = LogicalPlanBuilder::insert_into(
        scan,
        target_ref,
        provider_as_source(target_mem),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();

    let (inputs, outputs) = extract::extract_lineage(&plan, &lookup());

    assert_eq!(inputs.len(), 1, "one source dataset (the SELECT's TableScan)");
    assert_eq!(inputs[0].name, "sales.orders");
    assert_eq!(inputs[0].namespace, "https://polaris.example/api/catalog");

    assert_eq!(outputs.len(), 1, "one target dataset (the CTAS target)");
    assert_eq!(outputs[0].name, "sales.archive");
    assert_eq!(outputs[0].namespace, "https://polaris.example/api/catalog");

    let cl = outputs[0]
        .facets
        .columnLineage
        .as_ref()
        .expect("columnLineage facet must be present for CTAS write");

    assert!(
        cl.fields.contains_key("id"),
        "target column 'id' is mapped"
    );
    assert!(
        cl.fields.contains_key("amount"),
        "target column 'amount' is mapped"
    );

    for col_name in ["id", "amount"] {
        let entry = &cl.fields[col_name];
        assert_eq!(
            entry.inputFields.len(),
            1,
            "{col_name} has exactly one input field"
        );
        let src = &entry.inputFields[0];
        assert_eq!(src.namespace, "https://polaris.example/api/catalog");
        assert_eq!(src.name, "sales.orders");
        assert_eq!(src.field, col_name);
        assert_eq!(entry.inputFields[0].transformations.len(), 1);
        assert_eq!(entry.inputFields[0].transformations[0].kind, "DIRECT");
        assert_eq!(entry.inputFields[0].transformations[0].subtype, "IDENTITY");
    }
}

#[test]
fn ctas_wrapper_with_aggregation_uses_correct_transformation() {
    // CREATE TABLE polaris.sales.totals AS
    //   SELECT customer_id, SUM(amount) AS total FROM polaris.sales.orders
    //   GROUP BY customer_id
    // The aggregated column should record DIRECT/AGGREGATION, the group-by
    // column should record DIRECT/IDENTITY.
    use datafusion::functions_aggregate::expr_fn::sum;

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let source_mem = Arc::new(MemTable::try_new(arrow_schema, vec![vec![]]).unwrap());
    let source_ref = TableReference::full("polaris", "sales", "orders");

    let agg = LogicalPlanBuilder::scan(source_ref, provider_as_source(source_mem), None)
        .unwrap()
        .aggregate(vec![col("customer_id")], vec![sum(col("amount"))])
        .unwrap()
        .build()
        .unwrap();

    // The synthetic CTAS wrapper target schema mirrors the source's output
    // (group-by + agg expression). We only need the wrapper for shape; the
    // MemTable schema stub uses the same fields.
    let target_arrow = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("sum(orders.amount)", DataType::Float64, true),
    ]));
    let target_mem = Arc::new(MemTable::try_new(target_arrow, vec![vec![]]).unwrap());
    let target_ref = TableReference::full("polaris", "sales", "totals");

    let plan = LogicalPlanBuilder::insert_into(
        agg,
        target_ref,
        provider_as_source(target_mem),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();

    let (inputs, outputs) = extract::extract_lineage(&plan, &lookup());
    assert_eq!(inputs.len(), 1);
    assert_eq!(outputs.len(), 1);

    let cl = outputs[0]
        .facets
        .columnLineage
        .as_ref()
        .expect("columnLineage facet must be present");
    // customer_id passes through aggregation unchanged: IDENTITY.
    let cust_entry = cl
        .fields
        .get("customer_id")
        .expect("customer_id mapped");
    assert_eq!(
        cust_entry.inputFields[0].transformations[0].subtype,
        "IDENTITY",
        "GROUP BY column passes through with IDENTITY"
    );
    // The aggregated column carries DIRECT/AGGREGATION.
    let sum_key = cl
        .fields
        .keys()
        .find(|k| k.contains("sum"))
        .expect("aggregated column present");
    let sum_entry = &cl.fields[sum_key];
    assert_eq!(
        sum_entry.inputFields[0].transformations[0].subtype,
        "AGGREGATION",
        "aggregated column carries AGGREGATION subtype"
    );
}

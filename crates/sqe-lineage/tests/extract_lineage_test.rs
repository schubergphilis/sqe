//! E14 — full `extract_lineage` integration: inputs + outputs + columnLineage facet.

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
fn insert_into_yields_inputs_outputs_and_column_lineage() {
    // INSERT INTO polaris.sales.archive SELECT id FROM polaris.sales.orders
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mem_source = Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap());
    let mem_target = Arc::new(MemTable::try_new(arrow_schema, vec![vec![]]).unwrap());

    let source_ref = TableReference::full("polaris", "sales", "orders");
    let scan = LogicalPlanBuilder::scan(source_ref, provider_as_source(mem_source), None)
        .unwrap()
        .project(vec![col("id")])
        .unwrap()
        .build()
        .unwrap();

    let target_ref = TableReference::full("polaris", "sales", "archive");
    let plan = LogicalPlanBuilder::insert_into(
        scan,
        target_ref,
        provider_as_source(mem_target),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();

    let (inputs, outputs) = extract::extract_lineage(&plan, &lookup());

    assert_eq!(inputs.len(), 1, "one source dataset");
    assert_eq!(inputs[0].name, "sales.orders");
    assert_eq!(inputs[0].namespace, "https://polaris.example/api/catalog");

    assert_eq!(outputs.len(), 1, "one target dataset");
    assert_eq!(outputs[0].name, "sales.archive");

    let cl = outputs[0]
        .outputFacets
        .columnLineage
        .as_ref()
        .expect("columnLineage facet present");
    assert!(cl.fields.contains_key("id"), "target column 'id' is mapped");
    let entry = &cl.fields["id"];
    assert_eq!(entry.inputFields.len(), 1);
    let src = &entry.inputFields[0];
    assert_eq!(src.namespace, "https://polaris.example/api/catalog");
    assert_eq!(src.name, "sales.orders");
    assert_eq!(src.field, "id");
    assert_eq!(entry.inputFields[0].transformations.len(), 1);
    assert_eq!(entry.inputFields[0].transformations[0].subtype, "IDENTITY");
}

#[test]
fn select_only_plan_yields_inputs_no_outputs() {
    // SELECT id FROM polaris.sales.orders → inputs but no outputs (read-only).
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mem = Arc::new(MemTable::try_new(arrow_schema, vec![vec![]]).unwrap());

    let plan = LogicalPlanBuilder::scan(
        TableReference::full("polaris", "sales", "orders"),
        provider_as_source(mem),
        None,
    )
    .unwrap()
    .project(vec![col("id")])
    .unwrap()
    .build()
    .unwrap();

    let (inputs, outputs) = extract::extract_lineage(&plan, &lookup());
    assert_eq!(inputs.len(), 1);
    assert!(outputs.is_empty(), "SELECT-only plan has no output target");
}

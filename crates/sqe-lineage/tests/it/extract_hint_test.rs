//! E13 — output dataset extraction from `LineageHint::DdlSchema`.
//!
//! DDL paths (CREATE TABLE / DROP / ALTER) carry no source plan but do
//! carry the target schema. The hint route lets the coordinator emit a
//! lineage event for those without a LogicalPlan.

use sqe_lineage::extract;
use sqe_lineage::LineageHint;
use std::sync::Arc;

#[test]
fn ddl_hint_yields_output_dataset_with_schema() {
    let hint = LineageHint::DdlSchema {
        catalog: "polaris".into(),
        schema: "sales".into(),
        table: "new_table".into(),
        columns: vec![
            ("id".into(), "Int64".into()),
            ("amount".into(), "Float64".into()),
        ],
    };
    let lookup: extract::CatalogLookup = Arc::new(|n: &str| match n {
        "polaris" => "https://polaris.example/api/catalog".into(),
        other => format!("sqe://{other}"),
    });

    let (inputs, outputs) = extract::extract_from_hint(&hint, &lookup);

    assert!(inputs.is_empty(), "DDL hint has no input datasets");
    assert_eq!(outputs.len(), 1);

    let out = &outputs[0];
    assert_eq!(out.name, "sales.new_table");
    assert_eq!(out.namespace, "https://polaris.example/api/catalog");

    let schema = out.facets.schema.as_ref().expect("schema facet");
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[0].field_type, "Int64");
    assert_eq!(schema.fields[1].name, "amount");
    assert_eq!(schema.fields[1].field_type, "Float64");

    let ds = out.facets.dataSource.as_ref().expect("dataSource facet");
    assert_eq!(ds.name, "polaris");
    assert_eq!(ds.uri, "https://polaris.example/api/catalog");
}

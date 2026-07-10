use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use sqe_lineage::extract::{datasets, CatalogLookup};
use std::sync::Arc;

fn lookup_polaris() -> CatalogLookup {
    Arc::new(|name: &str| match name {
        "polaris" => "https://polaris.example/api/catalog".into(),
        other => format!("sqe://{other}"),
    })
}

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
fn table_scan_yields_one_input_dataset_with_multi_catalog_namespace() {
    let plan = build_simple_scan(
        "polaris",
        "sales",
        "orders",
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let lookup = lookup_polaris();
    let inputs = datasets::extract_inputs(&plan, &lookup);

    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input.namespace, "https://polaris.example/api/catalog");
    assert_eq!(input.name, "sales.orders");
    let schema = input.facets.schema.as_ref().expect("schema facet present");
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].name, "id");
    let ds = input
        .facets
        .dataSource
        .as_ref()
        .expect("dataSource facet");
    assert_eq!(ds.name, "polaris");
    assert_eq!(ds.uri, "https://polaris.example/api/catalog");
}

#[test]
fn unknown_catalog_falls_back_to_sqe_namespace() {
    let plan = build_simple_scan(
        "nessie",
        "archive",
        "orders",
        &[("id", DataType::Int64)],
    );
    let lookup = lookup_polaris();
    let inputs = datasets::extract_inputs(&plan, &lookup);

    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].namespace, "sqe://nessie");
    assert_eq!(inputs[0].name, "archive.orders");
}

#[test]
fn two_part_table_name_uses_default_catalog() {
    // TableReference::partial(schema, table) => 2-part
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mem = MemTable::try_new(arrow_schema, vec![vec![]]).unwrap();
    let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(mem);
    let table_ref = TableReference::partial("public", "t");
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(provider), None)
        .unwrap()
        .build()
        .unwrap();

    let lookup = lookup_polaris();
    let inputs = datasets::extract_inputs(&plan, &lookup);

    assert_eq!(inputs.len(), 1);
    // 2-part falls back to default catalog
    assert_eq!(inputs[0].namespace, "sqe://default");
    assert_eq!(inputs[0].name, "public.t");
}

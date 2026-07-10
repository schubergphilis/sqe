use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use sqe_lineage::extract::{datasets, CatalogLookup};
use std::sync::Arc;

fn lookup_polaris() -> CatalogLookup {
    Arc::new(|name: &str| match name {
        "polaris" => "https://polaris.example/api/catalog".into(),
        other => format!("sqe://{other}"),
    })
}

/// Build an INSERT INTO `<catalog>.<schema>.<table>` SELECT * FROM source plan.
fn build_insert_plan(
    target_catalog: &str,
    target_schema: &str,
    target_table: &str,
    source_ref: TableReference,
    cols: &[(&str, DataType)],
) -> LogicalPlan {
    let arrow_schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t)| Field::new(*n, t.clone(), false))
            .collect::<Vec<_>>(),
    ));
    let source_mem = MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap();
    let source_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(source_mem);
    let source_scan = LogicalPlanBuilder::scan(
        source_ref,
        provider_as_source(source_provider),
        None,
    )
    .unwrap()
    .build()
    .unwrap();

    let target_mem = MemTable::try_new(arrow_schema, vec![vec![]]).unwrap();
    let target_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(target_mem);
    let target_ref =
        TableReference::full(target_catalog, target_schema, target_table);

    LogicalPlanBuilder::insert_into(
        source_scan,
        target_ref,
        provider_as_source(target_provider),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap()
}

#[test]
fn insert_plan_yields_one_output_dataset() {
    let plan = build_insert_plan(
        "polaris",
        "sales",
        "archive",
        TableReference::full("polaris", "sales", "orders"),
        &[("id", DataType::Int64), ("amount", DataType::Float64)],
    );
    let lookup = lookup_polaris();
    let outputs = datasets::extract_outputs(&plan, &lookup);

    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.namespace, "https://polaris.example/api/catalog");
    assert_eq!(output.name, "sales.archive");
    let ds = output
        .facets
        .dataSource
        .as_ref()
        .expect("dataSource facet");
    assert_eq!(ds.name, "polaris");
    assert_eq!(ds.uri, "https://polaris.example/api/catalog");
    // Schema facet is filled in E14, not E2.
    assert!(output.facets.schema.is_none());
}

#[test]
fn insert_with_unknown_catalog_uses_sqe_fallback() {
    let plan = build_insert_plan(
        "nessie",
        "archive",
        "orders",
        TableReference::full("nessie", "sales", "orders"),
        &[("id", DataType::Int64)],
    );
    let lookup = lookup_polaris();
    let outputs = datasets::extract_outputs(&plan, &lookup);

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].namespace, "sqe://nessie");
    assert_eq!(outputs[0].name, "archive.orders");
}

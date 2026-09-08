//! Column lineage under policy, and honesty about incomplete traces
//! (architecture review 610, #617).
//!
//! A CTAS over a masked column used to say `t2.email <- hr.employees.email,
//! IDENTITY`, which lets a catalog propagate the source's PII tag onto a
//! column that holds hashes. And any plan node without a trace rule made its
//! columns silently vanish from the facet, indistinguishable from a constant.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, dml::InsertOp, LogicalPlan, LogicalPlanBuilder};
use sqe_lineage::extract;
use std::sync::Arc;

fn lookup() -> extract::CatalogLookup {
    Arc::new(|n: &str| format!("sqe://{n}"))
}

/// `INSERT INTO polaris.hr.archive (SELECT <exprs> FROM polaris.hr.employees)`,
/// the shape `WriteHandler::handle_ctas_streaming` builds.
fn ctas_over_employees(
    build_source: impl FnOnce(LogicalPlanBuilder) -> LogicalPlanBuilder,
) -> LogicalPlan {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));
    let source = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap());
    let scan = LogicalPlanBuilder::scan(
        TableReference::full("polaris", "hr", "employees"),
        provider_as_source(source),
        None,
    )
    .unwrap();
    let select = build_source(scan).build().unwrap();
    let target_schema = Arc::new(Schema::new(
        select
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let target = Arc::new(MemTable::try_new(target_schema, vec![vec![]]).unwrap());
    LogicalPlanBuilder::insert_into(
        select,
        TableReference::full("polaris", "hr", "archive"),
        provider_as_source(target),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap()
}

#[test]
fn a_masked_source_column_is_reported_as_masked_not_identity() {
    let plan = ctas_over_employees(|b| b.project(vec![col("id"), col("email")]).unwrap());

    // What the policy enforcer recorded for this statement: `email` on the
    // scan of `polaris.hr.employees` was masked. The key is the scan's
    // TableReference display, as `PolicyPlanRewriter` keys its summary.
    let masked = vec![("polaris.hr.employees".to_string(), "email".to_string())];
    let extracted = extract::extract_lineage_governed(&plan, &lookup(), &masked);

    let cl = extracted.outputs[0]
        .facets
        .columnLineage
        .as_ref()
        .expect("column lineage on the CTAS target");
    let email = &cl.fields["email"].inputFields[0];
    assert_eq!(email.field, "email");
    assert_eq!(email.transformations[0].subtype, "MASKED", "{email:?}");
    assert!(email.transformations[0].masking, "{email:?}");

    let id = &cl.fields["id"].inputFields[0];
    assert_eq!(id.transformations[0].subtype, "IDENTITY", "unmasked columns are untouched: {id:?}");
    assert!(extracted.quality.complete, "every node here has a trace rule");
    assert!(extracted.quality.untraced.is_empty());
}

#[test]
fn the_namespace_only_table_key_matches_the_same_scan() {
    // A scan planned as `hr.employees` (session default catalog) keys the
    // enforcer's summary without the catalog; the tracer parses both the
    // same way, so the mask still lands.
    let plan = ctas_over_employees(|b| b.project(vec![col("email")]).unwrap());
    let masked = vec![("hr.employees".to_string(), "email".to_string())];
    let extracted = extract::extract_lineage_governed(&plan, &lookup(), &masked);
    let email = &extracted.outputs[0].facets.columnLineage.as_ref().unwrap().fields["email"]
        .inputFields[0];
    assert_eq!(email.transformations[0].subtype, "MASKED");

    // A different table's `email` is not.
    let other = vec![("hr.contractors".to_string(), "email".to_string())];
    let extracted = extract::extract_lineage_governed(&plan, &lookup(), &other);
    let email = &extracted.outputs[0].facets.columnLineage.as_ref().unwrap().fields["email"]
        .inputFields[0];
    assert_eq!(email.transformations[0].subtype, "IDENTITY");
}

#[test]
fn an_untraced_node_is_reported_in_the_quality_facet() {
    // `Unnest` has no column-trace rule: its outputs get no dependencies.
    // Before 610 the facet simply omitted them; now the run says why.
    let plan = ctas_over_employees(|b| {
        b.project(vec![col("id"), col("tags")])
            .unwrap()
            .unnest_column("tags")
            .unwrap()
    });
    let extracted = extract::extract_lineage_governed(&plan, &lookup(), &[]);
    assert!(!extracted.quality.complete);
    assert_eq!(extracted.quality.untraced, vec!["Unnest".to_string()]);
    let cl = extracted.outputs[0]
        .facets
        .columnLineage
        .as_ref()
        .expect("facet is still emitted for what could be traced");
    assert!(
        cl.fields.is_empty(),
        "nothing below the Unnest is traced through it; the facet says so instead of guessing: {:?}",
        cl.fields.keys().collect::<Vec<_>>()
    );
}

#[test]
fn the_plain_entry_point_is_unchanged() {
    let plan = ctas_over_employees(|b| b.project(vec![col("id"), col("email")]).unwrap());
    let (inputs, outputs) = extract::extract_lineage(&plan, &lookup());
    assert_eq!(inputs.len(), 1);
    let cl = outputs[0].facets.columnLineage.as_ref().unwrap();
    assert_eq!(cl.fields["email"].inputFields[0].transformations[0].subtype, "IDENTITY");
}

//! Column lineage through a view and through an optimized scan.
//!
//! SQE optimizes a view's body before wrapping it in a `ViewTable`, so the
//! base scans a CTAS over the view inlines carry a pushed-down projection.
//! The tracer used to name the provider's full schema by position, which
//! shifted every column after the first one the projection dropped:
//! `total` came out as `items.id, items.label, orders.id` instead of
//! `items.amount, orders.qty`.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable, ViewTable};
use datafusion::logical_expr::{dml::InsertOp, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use sqe_lineage::extract::{self, CatalogInfo, CatalogLookup};
use std::collections::BTreeSet;
use std::sync::Arc;

fn lookup() -> CatalogLookup {
    Arc::new(|name: &str| CatalogInfo::new(name, None))
}

fn mem(cols: &[(&str, DataType)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t)| Field::new(*n, t.clone(), false))
            .collect::<Vec<_>>(),
    ));
    Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())
}

/// A session over `items(id, label, amount)` and
/// `orders(id, item_id, qty, region_id)` with `q_view` registered the way
/// `sqe_catalog::schema_provider::view_table` builds it: the body planned,
/// optimized, then wrapped in a `ViewTable`.
async fn session_with_view() -> SessionContext {
    let config = SessionConfig::new().with_default_catalog_and_schema("lake", "shop");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(
        "items",
        mem(&[
            ("id", DataType::Int64),
            ("label", DataType::Utf8),
            ("amount", DataType::Int64),
        ]),
    )
    .unwrap();
    ctx.register_table(
        "orders",
        mem(&[
            ("id", DataType::Int64),
            ("item_id", DataType::Int64),
            ("qty", DataType::Int64),
            ("region_id", DataType::Int64),
        ]),
    )
    .unwrap();
    let sql = "SELECT i.id, i.amount * o.qty AS total \
               FROM items i JOIN orders o ON o.item_id = i.id";
    let body = ctx.sql(sql).await.unwrap();
    let (state, plan) = body.into_parts();
    let plan = state.optimize(&plan).unwrap();
    ctx.register_table("q_view", Arc::new(ViewTable::new(plan, Some(sql.into()))))
        .unwrap();
    ctx
}

/// The synthetic INSERT wrapper the CTAS path captures for lineage.
fn wrap(source: LogicalPlan, target: Schema) -> LogicalPlan {
    let stub = MemTable::try_new(Arc::new(target), vec![vec![]]).unwrap();
    LogicalPlanBuilder::insert_into(
        source,
        TableReference::full("lake", "shop", "q_vctas"),
        provider_as_source(Arc::new(stub)),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap()
}

/// `(table.field, kind)` pairs of one output column's input fields.
fn inputs_of(plan: &LogicalPlan, column: &str) -> BTreeSet<(String, String)> {
    let (_, outputs) = extract::extract_lineage(plan, &lookup());
    let cl = outputs[0]
        .facets
        .columnLineage
        .as_ref()
        .expect("CTAS carries column lineage");
    cl.fields[column]
        .inputFields
        .iter()
        .flat_map(|f| {
            let name = format!("{}.{}", f.name.rsplit('.').next().unwrap(), f.field);
            f.transformations
                .iter()
                .map(move |t| (name.clone(), t.kind.clone()))
        })
        .collect()
}

fn fields(set: &BTreeSet<(String, String)>) -> BTreeSet<String> {
    set.iter().map(|(f, _)| f.clone()).collect()
}

#[tokio::test]
async fn ctas_from_view_over_join_names_the_view_body_columns() {
    let ctx = session_with_view().await;
    let select = ctx
        .sql("SELECT id, total FROM q_view")
        .await
        .unwrap()
        .logical_plan()
        .clone();
    let plan = wrap(
        select,
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
        ]),
    );

    let total = inputs_of(&plan, "total");
    // Data flows from amount and qty; the join keys are the only other
    // columns the value depends on. label and region_id never reach it.
    assert!(
        total.contains(&("items.amount".into(), "DIRECT".into())),
        "total <- items.amount DIRECT, got {total:?}"
    );
    assert!(
        total.contains(&("orders.qty".into(), "DIRECT".into())),
        "total <- orders.qty DIRECT, got {total:?}"
    );
    assert_eq!(
        fields(&total),
        ["items.amount", "items.id", "orders.item_id", "orders.qty"]
            .into_iter()
            .map(String::from)
            .collect(),
        "total names the view body's columns, not positions in the base tables"
    );

    let id = inputs_of(&plan, "id");
    assert!(
        id.contains(&("items.id".into(), "DIRECT".into())),
        "id <- items.id DIRECT, got {id:?}"
    );
    assert!(
        !fields(&id).contains("items.label") && !fields(&id).contains("orders.id"),
        "id never names label or orders.id, got {id:?}"
    );
}

#[test]
fn scan_with_pushed_down_projection_traces_its_output_columns() {
    let items = mem(&[
        ("id", DataType::Int64),
        ("label", DataType::Utf8),
        ("amount", DataType::Int64),
    ]);
    let plan = LogicalPlanBuilder::scan(
        TableReference::full("lake", "shop", "items"),
        provider_as_source(items),
        Some(vec![0, 2]),
    )
    .unwrap()
    .build()
    .unwrap();

    let trace = extract::columns::trace_plan(&plan);
    let names: Vec<&str> = trace.iter().map(|d| d[0].field.as_str()).collect();
    assert_eq!(names, ["id", "amount"]);
}

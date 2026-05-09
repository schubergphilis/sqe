//! End-to-end test: when a write plan IS captured, column lineage flows
//! through the full ChannelObserver -> spawn_emitter -> wiremock pipeline.
//!
//! This guards against the regression where write paths (CTAS, INSERT,
//! DELETE, UPDATE, MERGE) used to ship `plan: None` to the OL emitter and
//! produced events with empty inputs/outputs and no column lineage.
//!
//! The fix wires `&mut Option<PlanOrHint>` through the write handlers in
//! `sqe-coordinator`. To verify the emitter side honours a captured plan
//! without standing up a full coordinator, we build the same INSERT-shaped
//! plan the coordinator now synthesises and push it through the pipeline.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, dml::InsertOp, LogicalPlan, LogicalPlanBuilder};
use sqe_lineage::sink::Sink;
use sqe_lineage::sinks::http::{AuthMode, HttpConfig, HttpSink};
use sqe_lineage::*;
use std::sync::Arc;
use std::time::Duration;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

fn lookup() -> extract::CatalogLookup {
    Arc::new(|n: &str| match n {
        "polaris" => "https://polaris.example/api/catalog".into(),
        other => format!("sqe://{other}"),
    })
}

fn cfg() -> Arc<EmitterConfig> {
    Arc::new(EmitterConfig {
        job_namespace: "sqe-test".into(),
        producer: "https://test/v0".into(),
        catalog_lookup: lookup(),
    })
}

/// Build the same shape `WriteHandler::handle_ctas_streaming` synthesises:
/// a SELECT plan wrapped in INSERT INTO target.
fn build_ctas_wrapped_plan() -> LogicalPlan {
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
    LogicalPlanBuilder::insert_into(
        scan,
        target_ref,
        provider_as_source(target_mem),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap()
}

/// COMPLETE event for a CTAS write whose plan IS captured emits non-empty
/// inputs/outputs and a populated columnLineage facet on the target dataset.
#[tokio::test]
async fn ctas_complete_event_with_captured_plan_carries_column_lineage() {
    let collector = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&collector)
        .await;

    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(http) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter =
        prometheus::IntCounter::new("ctas_emit_test_1", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    let plan = build_ctas_wrapped_plan();

    let mut ctx = QueryCompleteCtx::dummy();
    ctx.statement_kind = "ctas".into();
    ctx.sql = "CREATE TABLE polaris.sales.archive AS SELECT id, amount FROM polaris.sales.orders"
        .into();
    ctx.plan = Some(PlanOrHint::Plan(Box::new(plan)));

    obs.on_query_complete(ctx);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 1, "expected one COMPLETE event");

    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(body["eventType"], "COMPLETE");

    // The fix: outputs[0].outputFacets.columnLineage is populated because
    // the write plan was captured. Without the fix, plan = None and this
    // facet would be missing entirely.
    let outputs = body["outputs"]
        .as_array()
        .expect("outputs array present");
    assert_eq!(outputs.len(), 1, "exactly one output dataset");
    assert_eq!(outputs[0]["name"], "sales.archive");

    let cl = &outputs[0]["outputFacets"]["columnLineage"];
    assert!(
        cl.is_object(),
        "columnLineage facet must be an object, got: {cl}"
    );
    let fields = &cl["fields"];
    assert!(
        fields.is_object(),
        "columnLineage.fields must be an object, got: {fields}"
    );
    assert!(
        fields["id"].is_object(),
        "target column 'id' is mapped via columnLineage"
    );
    assert!(
        fields["amount"].is_object(),
        "target column 'amount' is mapped via columnLineage"
    );

    // Verify the input dependency is the source dataset and the
    // transformation type is DIRECT/IDENTITY.
    let id_inputs = fields["id"]["inputFields"]
        .as_array()
        .expect("inputFields array");
    assert_eq!(id_inputs.len(), 1);
    assert_eq!(id_inputs[0]["name"], "sales.orders");
    assert_eq!(id_inputs[0]["field"], "id");
    let txform = &id_inputs[0]["transformations"][0];
    assert_eq!(txform["type"], "DIRECT");
    assert_eq!(txform["subtype"], "IDENTITY");

    // Inputs must also be populated.
    let inputs = body["inputs"]
        .as_array()
        .expect("inputs array present");
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0]["name"], "sales.orders");
}

/// Sanity guard: a COMPLETE event with `plan = None` (the regression state)
/// emits empty inputs/outputs and no columnLineage facet. This is the
/// "before" state; we keep it as a contrast to make the regression
/// signature explicit.
#[tokio::test]
async fn ctas_complete_event_without_plan_has_empty_lineage() {
    let collector = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v1/lineage"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&collector)
        .await;

    let http = HttpSink::new(HttpConfig {
        endpoint: format!("{}/api/v1/lineage", collector.uri()),
        auth: AuthMode::None,
        timeout_ms: 5000,
        retry_attempts: 0,
    })
    .unwrap();
    let multi = Arc::new(MultiSink::new(vec![Arc::new(http) as Arc<dyn Sink>]));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _emitter = spawn_emitter(rx, multi, cfg());

    let counter =
        prometheus::IntCounter::new("ctas_emit_test_2", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter);

    let mut ctx = QueryCompleteCtx::dummy();
    ctx.statement_kind = "ctas".into();
    ctx.plan = None;

    obs.on_query_complete(ctx);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);

    let body: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    let inputs = body["inputs"].as_array().expect("inputs is array");
    let outputs = body["outputs"].as_array().expect("outputs is array");
    assert!(inputs.is_empty(), "no plan -> no inputs");
    assert!(outputs.is_empty(), "no plan -> no outputs");
}

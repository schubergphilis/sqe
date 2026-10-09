//! Golden tests for the emitted event JSON, built from the statements the AI
//! portal recorded against SQE v0.48.1 (`sqe_lineage_v0_48_1.jsonl` in the
//! portal repo). Each statement is planned the way SQE plans it (a session
//! whose default catalog is the workspace catalog, the write paths' synthetic
//! INSERT wrapper) and pushed through `ChannelObserver -> spawn_emitter`; the
//! test compares the whole event, timestamps aside.
//!
//! What they pin:
//! - one dataset namespace, `sqe://<iceberg catalog>`, for inputs, outputs
//!   and column-lineage `inputFields`, with a `symlinks` TABLE identifier
//!   under the catalog's REST URI (v0.48.1: inputs `sqe://default`, outputs
//!   the bare Polaris URL);
//! - the `sqe_query` run facet carrying the query id, equal to `runId`;
//! - INSERT output fields named after the target table's columns
//!   (v0.48.1: `country, segment, Int64(1)` for a `country, seg, n` table);
//! - DROP TABLE emitting the table with `lifecycleStateChange: DROP`
//!   (v0.48.1: no datasets) and a CTAS carrying `CREATE`;
//! - the committed Iceberg snapshot id as the OL `version` facet.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{dml::InsertOp, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::{json, Value};
use sqe_lineage::extract::{CatalogInfo, CatalogLookup, SNAPSHOT_ID_METADATA_KEY};
use sqe_lineage::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const POLARIS: &str = "http://polaris:8181/api/catalog";

/// The lookup `sqe-server` builds for the portal's config: the legacy
/// `[catalog]` block (SQL name `iceberg`, warehouse `workspace1`) and
/// Polaris-discovered workspace catalogs, all on one Polaris.
fn lookup() -> CatalogLookup {
    Arc::new(|name: &str| match name {
        "iceberg" => CatalogInfo::new("workspace1", Some(POLARIS.into())),
        other => CatalogInfo::new(other, Some(POLARIS.into())),
    })
}

fn scope() -> LineageScope {
    LineageScope {
        catalog: "workspace1".into(),
        schema: "default".into(),
    }
}

fn query_id() -> uuid::Uuid {
    uuid::Uuid::parse_str("6f1c2a4e-8d3b-4c5a-9e7f-0a1b2c3d4e5f").unwrap()
}

fn customers_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("segment", DataType::Utf8, false),
        Field::new("signup_date", DataType::Date32, false),
    ]))
}

fn lin_probe_schema() -> Schema {
    Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("seg", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
    ])
}

/// Plan `select` in a session whose default catalog/schema is
/// `workspace1.default`, as SQE does for a portal connection
/// (`X-Trino-Catalog: workspace1`). The scan keeps the bare `customers`.
async fn plan_select(select: &str) -> LogicalPlan {
    let config = SessionConfig::new().with_default_catalog_and_schema("workspace1", "default");
    let ctx = SessionContext::new_with_config(config);
    let customers = MemTable::try_new(customers_schema(), vec![vec![]]).unwrap();
    ctx.register_table("customers", Arc::new(customers))
        .unwrap();
    ctx.sql(select).await.unwrap().logical_plan().clone()
}

/// The synthetic INSERT wrapper the write paths capture for lineage
/// (`sqe_write::write_handler::build_lineage_insert_plan`): `target` is the
/// target stub's schema.
fn wrap(source: LogicalPlan, table: &str, target: Schema) -> LogicalPlan {
    let stub = MemTable::try_new(Arc::new(target), vec![vec![]]).unwrap();
    LogicalPlanBuilder::insert_into(
        source,
        TableReference::full("workspace1", "default", table),
        provider_as_source(Arc::new(stub)),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap()
}

struct Capture(Mutex<Vec<event::RunEvent>>);

#[async_trait::async_trait]
impl Sink for Capture {
    async fn send(&self, event: &event::RunEvent) -> Result<(), SinkError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
    fn name(&self) -> &'static str {
        "capture"
    }
}

/// Push one COMPLETE through the real emitter and return its JSON with the
/// wall-clock fields blanked.
async fn emit(ctx: QueryCompleteCtx) -> Value {
    let capture = Arc::new(Capture(Mutex::new(Vec::new())));
    let multi = Arc::new(MultiSink::new(vec![capture.clone() as Arc<dyn Sink>]));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let cfg = Arc::new(EmitterConfig {
        job_namespace: "portal-dev".into(),
        producer: "https://github.com/sbp/sqe/vtest".into(),
        catalog_lookup: lookup(),
    });
    let _emitter = spawn_emitter(rx, multi, cfg);
    let counter = prometheus::IntCounter::new("portal_gaps", "test").unwrap();
    ChannelObserver::new(tx, counter).on_query_complete(ctx);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let event = loop {
        if let Some(e) = capture.0.lock().unwrap().first().cloned() {
            break e;
        }
        assert!(std::time::Instant::now() < deadline, "no event emitted");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let mut v = serde_json::to_value(event).unwrap();
    v["eventTime"] = Value::Null;
    v["run"]["facets"]["nominalTime"] = Value::Null;
    v
}

fn complete(kind: &str, sql: &str, plan: Option<PlanOrHint>) -> QueryCompleteCtx {
    let mut c = QueryCompleteCtx::dummy();
    c.run_id = query_id();
    c.session_id = "sess-portal".into(); // not a UUID: no parent facet
    c.statement_kind = kind.into();
    c.sql = sql.into();
    c.plan = plan;
    c.scope = scope();
    c
}

fn run_json() -> Value {
    json!({
        "runId": "6f1c2a4e-8d3b-4c5a-9e7f-0a1b2c3d4e5f",
        "facets": {
            "nominalTime": null,
            "sqe_query": { "queryId": "6f1c2a4e-8d3b-4c5a-9e7f-0a1b2c3d4e5f" }
        }
    })
}

fn identity(table: &str) -> (Value, Value) {
    (
        json!({ "name": "workspace1", "uri": POLARIS }),
        json!({ "identifiers": [{
            "namespace": POLARIS,
            "name": format!("workspace1.default.{table}"),
            "type": "TABLE"
        }]}),
    )
}

fn customers_input() -> Value {
    let (data_source, symlinks) = identity("customers");
    json!({
        "namespace": "sqe://workspace1",
        "name": "default.customers",
        "facets": {
            "schema": { "fields": [
                { "name": "id", "type": "Int32" },
                { "name": "name", "type": "Utf8" },
                { "name": "country", "type": "Utf8" },
                { "name": "segment", "type": "Utf8" },
                { "name": "signup_date", "type": "Date32" }
            ]},
            "dataSource": data_source,
            "symlinks": symlinks
        }
    })
}

fn dep(field: &str, kind: &str, subtype: &str) -> Value {
    json!({
        "namespace": "sqe://workspace1",
        "name": "default.customers",
        "field": field,
        "transformations": [{ "type": kind, "subtype": subtype, "description": "", "masking": false }]
    })
}

#[tokio::test]
async fn insert_names_output_fields_after_the_target_columns() {
    let source = plan_select("SELECT country, segment, 1 FROM customers").await;
    let mut target = lin_probe_schema();
    // The write path records the snapshot its commit produced.
    target = target.with_metadata(HashMap::from([(
        SNAPSHOT_ID_METADATA_KEY.to_string(),
        "4205761418235361718".to_string(),
    )]));
    let plan = wrap(source, "lin_probe", target);
    let sql = "INSERT INTO lin_probe SELECT country, segment, 1 FROM customers";
    let got = emit(complete(
        "insert",
        sql,
        Some(PlanOrHint::Plan(Box::new(plan))),
    ))
    .await;

    let (data_source, symlinks) = identity("lin_probe");
    let expected = json!({
        "eventType": "COMPLETE",
        "eventTime": null,
        "producer": "https://github.com/sbp/sqe/vtest",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        "run": run_json(),
        "job": {
            "namespace": "portal-dev",
            "name": format!("insert:{}", sqe_metrics::audit::query_hash(sql)),
            "facets": { "sql": {
                "query": "INSERT INTO lin_probe SELECT country, segment, ? FROM customers",
                "dialect": "sqe"
            }}
        },
        "inputs": [customers_input()],
        "outputs": [{
            "namespace": "sqe://workspace1",
            "name": "default.lin_probe",
            "facets": {
                "schema": { "fields": [
                    { "name": "country", "type": "Utf8" },
                    { "name": "seg", "type": "Utf8" },
                    { "name": "n", "type": "Int64" }
                ]},
                "dataSource": data_source,
                "columnLineage": { "fields": {
                    "country": { "inputFields": [dep("country", "DIRECT", "IDENTITY")] },
                    "n": { "inputFields": [] },
                    "seg": { "inputFields": [dep("segment", "DIRECT", "IDENTITY")] }
                }},
                "symlinks": symlinks,
                "version": { "datasetVersion": "4205761418235361718" }
            },
            "outputFacets": {}
        }]
    });
    assert_eq!(
        got,
        expected,
        "\n{}",
        serde_json::to_string_pretty(&got).unwrap()
    );
}

#[tokio::test]
async fn ctas_uses_one_namespace_and_marks_the_target_created() {
    let select = "WITH c AS (SELECT country, segment, id FROM customers) \
                  SELECT country, upper(segment) AS seg, count(id) AS n FROM c \
                  GROUP BY country, upper(segment)";
    let source = plan_select(select).await;
    // CTAS: the target is made from the SELECT, so the stub has its schema.
    let target = source.schema().as_arrow().clone();
    let plan = wrap(source, "lin_probe", target);
    let mut ctx = complete(
        "ctas",
        &format!("CREATE TABLE lin_probe AS {select}"),
        Some(PlanOrHint::Plan(Box::new(plan))),
    );
    ctx.output_lifecycle = Some(event::lifecycle::CREATE.into());
    let got = emit(ctx).await;

    assert_eq!(got["inputs"], json!([customers_input()]));
    let out = &got["outputs"][0];
    assert_eq!(out["namespace"], "sqe://workspace1");
    assert_eq!(out["name"], "default.lin_probe");
    assert_eq!(
        out["facets"]["lifecycleStateChange"],
        json!({ "lifecycleStateChange": "CREATE" })
    );
    assert_eq!(
        out["facets"]["columnLineage"]["fields"]["n"]["inputFields"],
        json!([
            dep("id", "DIRECT", "AGGREGATION"),
            dep("country", "INDIRECT", "GROUP_BY"),
            dep("segment", "INDIRECT", "GROUP_BY"),
        ])
    );
    assert_eq!(
        out["facets"]["columnLineage"]["fields"]["seg"]["inputFields"],
        json!([dep("segment", "DIRECT", "TRANSFORMATION")])
    );
    assert!(
        out["facets"].get("version").is_none(),
        "no snapshot recorded"
    );
    assert_eq!(got["run"], run_json());
}

#[tokio::test]
async fn drop_emits_the_dropped_table_with_lifecycle_drop() {
    let hint = LineageHint::TableLifecycle {
        catalog: "workspace1".into(),
        schema: "default".into(),
        table: "lin_probe".into(),
        change: event::lifecycle::DROP.into(),
    };
    let got = emit(complete(
        "drop",
        "DROP TABLE IF EXISTS lin_probe",
        Some(PlanOrHint::Hint(hint)),
    ))
    .await;
    let (data_source, symlinks) = identity("lin_probe");
    assert_eq!(got["inputs"], json!([]));
    assert_eq!(
        got["outputs"],
        json!([{
            "namespace": "sqe://workspace1",
            "name": "default.lin_probe",
            "facets": {
                "dataSource": data_source,
                "symlinks": symlinks,
                "lifecycleStateChange": { "lifecycleStateChange": "DROP" }
            },
            "outputFacets": {}
        }])
    );
    assert_eq!(got["run"], run_json());
}

/// A scan the user qualified with the legacy SQL catalog name `iceberg` is
/// the same Polaris catalog as `workspace1`: one namespace for both.
#[tokio::test]
async fn legacy_catalog_name_resolves_to_its_warehouse() {
    let config = SessionConfig::new().with_default_catalog_and_schema("iceberg", "default");
    let ctx = SessionContext::new_with_config(config);
    let customers = MemTable::try_new(customers_schema(), vec![vec![]]).unwrap();
    ctx.register_table("customers", Arc::new(customers))
        .unwrap();
    let source = ctx
        .sql("SELECT country FROM iceberg.default.customers")
        .await
        .unwrap()
        .logical_plan()
        .clone();
    let target = source.schema().as_arrow().clone();
    let plan = wrap(source, "lin_probe2", target);
    let got = emit(complete(
        "ctas",
        "CREATE TABLE lin_probe2 AS SELECT country FROM iceberg.default.customers",
        Some(PlanOrHint::Plan(Box::new(plan))),
    ))
    .await;
    assert_eq!(got["inputs"][0]["namespace"], "sqe://workspace1");
    assert_eq!(
        got["inputs"][0]["facets"]["symlinks"]["identifiers"][0]["name"],
        "workspace1.default.customers"
    );
    assert_eq!(got["outputs"][0]["namespace"], "sqe://workspace1");
    assert_eq!(
        got["outputs"][0]["facets"]["columnLineage"]["fields"]["country"]["inputFields"][0]
            ["namespace"],
        "sqe://workspace1"
    );
}

//! Vector distance functions over an Iceberg `list<real>` column (#907).
//!
//! An embedding lives in an ordinary Iceberg list column and rows are ranked
//! with DataFusion's `cosine_distance`, `inner_product` and `array_distance`.
//! These tests keep the three functions registered, check that a bound list
//! is accepted as either argument over Flight SQL, and check that a row
//! filter and a column mask still apply when a query orders by distance.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use datafusion::logical_expr::{col, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{MaskType, ResolvedPolicy};

const NS: &str = "default";

/// Distance from `[1, 0]`, closest first: 1 (0), 6, 2, 4, 3 (1), 5 (2).
const ROWS: &str = "(1, 'EU', make_array(1.0, 0.0)), \
                    (2, 'EU', make_array(0.9, 0.1)), \
                    (3, 'US', make_array(0.0, 1.0)), \
                    (4, 'US', make_array(0.7, 0.7)), \
                    (5, 'EU', make_array(-1.0, 0.0)), \
                    (6, 'US', make_array(0.95, 0.05))";

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A fresh `default.<table>` holding [`ROWS`], `emb` as `array(real)`. Each
/// test uses its own table: the tests run in parallel.
async fn seed(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> String {
    let t = format!("{NS}.{table}");
    exec(handler, session, &format!("DROP TABLE IF EXISTS {t}")).await;
    let create = format!("CREATE TABLE {t} (id BIGINT, region VARCHAR, emb array(real))");
    exec(handler, session, &create).await;
    exec(handler, session, &format!("INSERT INTO {t} VALUES {ROWS}")).await;
    t
}

/// The `id` column of `batches`, in order.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b.column(0).as_primitive::<Int64Type>();
            (0..b.num_rows()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// Column `c` of `batches` as `f64`s, `None` for NULL.
fn floats(batches: &[RecordBatch], c: usize) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b.column(c).as_primitive::<Float64Type>();
            (0..b.num_rows())
                .map(|i| (!a.is_null(i)).then(|| a.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `[x, y]` as the `List<Float64>` a client binds for a float array.
fn vector(x: f64, y: f64) -> ScalarValue {
    let values = ScalarValue::new_list_nullable(
        &[ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))],
        &arrow_schema::DataType::Float64,
    );
    ScalarValue::List(values)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn distance_functions_rank_an_iceberg_list_column() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = seed(&handler, &session, "vector_distance_rank").await;

    let sql = format!(
        "SELECT id, cosine_distance(emb, make_array(1.0, 0.0)), \
         inner_product(emb, make_array(1.0, 0.0)), \
         array_distance(emb, make_array(1.0, 0.0)) \
         FROM {t} ORDER BY 2, id"
    );
    let batches = handler.execute(&session, &sql, None).await.expect(&sql);
    assert_eq!(ids(&batches), vec![1, 6, 2, 4, 3, 5], "{sql}");

    // Row 5 is [-1, 0]: opposite, so distance 2, product -1, length 2.
    let last = |c| *floats(&batches, c).last().expect("six rows");
    assert_eq!(last(1), Some(2.0), "cosine_distance");
    assert_eq!(last(2), Some(-1.0), "inner_product");
    assert_eq!(last(3), Some(2.0), "array_distance");

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {t}")).await;
}

/// The portal's query: a prepared statement over Flight SQL, the query
/// vector bound as a `List<Float64>` against a `list<real>` column, on
/// either side of `cosine_distance`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bound_vector_ranks_over_flight_sql_in_either_argument() {
    let server = crate::flight_ingest_e2e::IngestServer::start().await;
    let (session, handler) = crate::common::setup_handler().await;
    let t = seed(&handler, &session, "vector_distance_flight").await;
    let mut client = server.client().await;

    for sql in [
        format!("SELECT id FROM {t} ORDER BY cosine_distance(emb, ?), id LIMIT 3"),
        format!("SELECT id FROM {t} ORDER BY cosine_distance(?, emb), id LIMIT 3"),
        format!("SELECT id FROM {t} ORDER BY cosine_distance(emb, $1), id LIMIT 3"),
    ] {
        let mut prepared = client
            .prepare(sql.clone(), None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: prepare: {e}"));
        let param = vector(1.0, 0.0).to_array().expect("list array");
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "$1",
            param.data_type().clone(),
            true,
        )]));
        let params = RecordBatch::try_new(schema, vec![param]).expect("parameter batch");
        prepared
            .set_parameters(params)
            .unwrap_or_else(|e| panic!("{sql}: set_parameters: {e}"));
        let info = prepared
            .execute()
            .await
            .unwrap_or_else(|e| panic!("{sql}: execute: {e}"));
        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint.ticket.clone().expect("endpoint carries a ticket");
            let stream = client
                .do_get(ticket)
                .await
                .unwrap_or_else(|e| panic!("{sql}: do_get: {e}"));
            batches.extend(
                stream
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: stream: {e}")),
            );
        }
        assert_eq!(ids(&batches), vec![1, 6, 2], "{sql}");
    }

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {t}")).await;
}

/// Ranked under `policy`, with the query vector bound: the path Flight SQL
/// prepared statements take. Returns `(id, distance)` rows.
async fn ranked_under(table: &str, policy: ResolvedPolicy) -> Vec<(i64, Option<f64>)> {
    let (session, plain) = crate::common::setup_handler().await;
    let t = seed(&plain, &session, table).await;
    let store = Arc::new(InMemoryPolicyStore::new());
    store.add_table_policy(NS, table, policy).await;
    let (session, governed) = crate::common::setup_handler_with_policy_store(store).await;

    let sql = format!("SELECT id, cosine_distance(emb, $1) AS d FROM {t} ORDER BY d, id LIMIT 5");
    let batches = governed
        .execute_bound_with_id(&session, &sql, vec![vector(1.0, 0.0)], None, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    exec(&plain, &session, &format!("DROP TABLE IF EXISTS {t}")).await;
    ids(&batches).into_iter().zip(floats(&batches, 1)).collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn row_filter_limits_the_nearest_rows_to_allowed_ones() {
    let rows = ranked_under(
        "vector_distance_filtered",
        ResolvedPolicy {
            row_filters: vec![col("region").eq(lit("EU"))],
            ..Default::default()
        },
    )
    .await;
    let seen: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(seen, vec![1, 2, 5], "only EU rows, nearest first: {rows:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn masked_or_restricted_vector_gives_null_distance() {
    let masked = ResolvedPolicy {
        column_masks: [("emb".to_string(), MaskType::Nullify)].into(),
        ..Default::default()
    };
    let restricted = ResolvedPolicy {
        restricted_columns: vec!["emb".to_string()],
        ..Default::default()
    };
    for (name, policy) in [
        ("vector_distance_masked", masked),
        ("vector_distance_restricted", restricted),
    ] {
        let rows = ranked_under(name, policy).await;
        assert_eq!(
            rows.len(),
            5,
            "{name}: LIMIT 5 still returns rows: {rows:?}"
        );
        assert!(
            rows.iter().all(|(_, d)| d.is_none()),
            "{name}: every distance must be NULL, not computed from the raw vector: {rows:?}"
        );
    }
}

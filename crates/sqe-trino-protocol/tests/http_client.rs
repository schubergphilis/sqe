//! `TrinoHttpClient` against a mocked `/v1/statement` server (review 705).
//!
//! Runs with `--features client,arrow` (the clippy / test gates use
//! `--all-features`); without them the file compiles to nothing.
#![cfg(all(feature = "client", feature = "arrow"))]

use arrow_schema::DataType;
use serde_json::{json, Value};
use sqe_trino_protocol::client::{TrinoClientError, TrinoHttpClient};
use sqe_trino_protocol::decode::rows_to_record_batch;
use sqe_trino_protocol::types::TrinoResponse;
use wiremock::matchers::{header, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const PAGES: usize = 1500;
const ROWS_PER_PAGE: usize = 3;

fn page_json(base: &str, n: usize, total: usize) -> Value {
    let rows: Vec<Value> = (0..ROWS_PER_PAGE)
        .map(|i| json!([(n * ROWS_PER_PAGE + i) as i64, format!("r{n}-{i}")]))
        .collect();
    let mut page = json!({
        "id": "q-1",
        "infoUri": null,
        "data": rows,
        "stats": { "state": if n + 1 == total { "FINISHED" } else { "RUNNING" } },
        "warnings": []
    });
    if n == 0 {
        page["columns"] = json!([
            {"name": "id", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}},
            {"name": "label", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}}
        ]);
    }
    if n + 1 < total {
        page["nextUri"] = json!(format!("{base}/v1/statement/q-1/{}", n + 1));
    }
    page
}

/// Serves page `n` for `GET /v1/statement/q-1/{n}`.
struct PageResponder {
    base: String,
    total: usize,
}

impl Respond for PageResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let n: usize = request
            .url
            .path_segments()
            .and_then(|mut s| s.next_back())
            .and_then(|s| s.parse().ok())
            .expect("page number in path");
        ResponseTemplate::new(200).set_body_json(page_json(&self.base, n, self.total))
    }
}

async fn paged_server(total: usize) -> MockServer {
    let server = MockServer::start().await;
    let base = server.uri();
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_json(&base, 0, total)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/v1/statement/q-1/\d+$"))
        .respond_with(PageResponder {
            base: base.clone(),
            total,
        })
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/v1/statement/q-1/\d+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn fifteen_hundred_pages_deliver_every_row() {
    let server = paged_server(PAGES).await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();

    let mut pages_seen = 0usize;
    let collected = {
        let mut rows = Vec::new();
        let summary = client
            .execute("SELECT 1", |page: &TrinoResponse| {
                pages_seen += 1;
                if let Some(d) = &page.data {
                    rows.extend(d.iter().cloned());
                }
                Ok(())
            })
            .await
            .expect("query succeeds");
        assert_eq!(summary.pages, PAGES);
        assert_eq!(summary.rows, PAGES * ROWS_PER_PAGE);
        assert_eq!(summary.stats.state, "FINISHED");
        assert_eq!(summary.columns.as_ref().map(Vec::len), Some(2));
        rows
    };
    assert_eq!(pages_seen, PAGES, "callback ran once per page");
    assert_eq!(
        collected.len(),
        PAGES * ROWS_PER_PAGE,
        "no page was dropped"
    );
    // Rows arrive in page order.
    assert_eq!(collected[0][0], json!(0));
    assert_eq!(
        collected.last().unwrap()[0],
        json!((PAGES * ROWS_PER_PAGE - 1) as i64)
    );

    // `collect` sees the same total.
    let all = client.collect("SELECT 1").await.unwrap();
    assert_eq!(all.rows.len(), PAGES * ROWS_PER_PAGE);
    assert_eq!(all.columns.len(), 2);
}

#[tokio::test]
async fn page_limit_is_an_error_not_a_truncation() {
    let server = paged_server(20).await;
    let client = TrinoHttpClient::builder(server.uri())
        .max_pages(5)
        .build()
        .unwrap();
    let err = client.collect("SELECT 1").await.expect_err("must fail");
    assert!(
        matches!(err, TrinoClientError::PageLimitExceeded { limit: 5 }),
        "{err}"
    );
    // The guard cancels the server-side query. Give the spawned DELETE a
    // moment to land.
    for _ in 0..50 {
        if server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.method == "DELETE")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let deletes = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method == "DELETE")
        .count();
    assert_eq!(deletes, 1, "exactly one DELETE nextUri on early exit");
}

#[tokio::test]
async fn foreign_origin_next_uri_is_rejected() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-2",
            "infoUri": null,
            "nextUri": "http://attacker.invalid:1/v1/statement/q-2/1",
            "data": [[1]],
            "stats": {"state": "RUNNING"},
            "warnings": []
        })))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();
    let err = client.collect("SELECT 1").await.expect_err("must fail");
    assert!(
        matches!(err, TrinoClientError::ForeignNextUri { .. }),
        "{err}"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "only the initial POST left the client");
}

#[tokio::test]
async fn error_is_surfaced_before_data_is_consumed() {
    let server = MockServer::start().await;
    // A page that carries BOTH data and error: the client must report the
    // error and never hand the data to the caller.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-3",
            "infoUri": null,
            "columns": [{"name": "x", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
            "data": [[1], [2]],
            "error": {
                "message": "Division by zero",
                "errorCode": 65540,
                "errorName": "DIVISION_BY_ZERO",
                "errorType": "USER_ERROR"
            },
            "stats": {"state": "FAILED"},
            "warnings": []
        })))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();
    let mut callback_calls = 0usize;
    let err = client
        .execute("SELECT 1/0", |_| {
            callback_calls += 1;
            Ok(())
        })
        .await
        .expect_err("must fail");
    assert_eq!(callback_calls, 0, "no page reached the caller");
    match err {
        TrinoClientError::Query(e) => {
            assert_eq!(e.error_name, "DIVISION_BY_ZERO");
            assert_eq!(e.message, "Division by zero");
        }
        other => panic!("expected Query error, got {other}"),
    }
}

#[tokio::test]
async fn headers_carry_auth_user_catalog_and_schema() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("X-Trino-Catalog", "iceberg"))
        .and(header("X-Trino-Schema", "tpch_sf1"))
        .and(header("X-Trino-User", "admin"))
        .and(header("Authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-4", "infoUri": null, "stats": {"state": "FINISHED"}, "warnings": []
        })))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri())
        .bearer_auth("tok")
        .user("admin")
        .catalog("iceberg")
        .schema("tpch_sf1")
        .build()
        .unwrap();
    let out = client.collect("SELECT 1").await.expect("headers matched");
    assert_eq!(out.rows.len(), 0);

    // Basic auth variant.
    let server2 = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("Authorization", "Basic YWxpY2U6c2VjcmV0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-5", "infoUri": null, "stats": {"state": "FINISHED"}, "warnings": []
        })))
        .mount(&server2)
        .await;
    let client2 = TrinoHttpClient::builder(server2.uri())
        .basic_auth("alice", "secret")
        .build()
        .unwrap();
    client2.collect("SELECT 1").await.expect("basic auth sent");
}

#[tokio::test]
async fn http_503_is_retried_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-6", "infoUri": null, "data": [[1]],
            "columns": [{"name": "x", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
            "stats": {"state": "FINISHED"}, "warnings": []
        })))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();
    let out = client.collect("SELECT 1").await.expect("retried past 503");
    assert_eq!(out.rows.len(), 1);
}

#[tokio::test]
async fn non_success_status_is_an_error_with_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(401).set_body_string("nope"))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();
    let err = client.collect("SELECT 1").await.err().unwrap();
    match err {
        TrinoClientError::Status { status, body, .. } => {
            assert_eq!(status, 401);
            assert_eq!(body, "nope");
        }
        other => panic!("expected Status, got {other}"),
    }
}

#[tokio::test]
async fn decimal_18_2_round_trips_through_client_and_decoder() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "q-7", "infoUri": null,
            "columns": [{
                "name": "total", "type": "decimal(18,2)",
                "typeSignature": {"rawType": "decimal", "arguments": [
                    {"kind": "LONG", "value": 18}, {"kind": "LONG", "value": 2}]}
            }],
            "data": [["12345.67"], ["-0.05"], [null]],
            "stats": {"state": "FINISHED"}, "warnings": []
        })))
        .mount(&server)
        .await;
    let client = TrinoHttpClient::builder(server.uri()).build().unwrap();
    let out = client.collect("SELECT total FROM t").await.unwrap();
    let batch = rows_to_record_batch(&out.columns, &out.rows).unwrap();
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Decimal128(18, 2)
    );
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Decimal128Array>()
        .unwrap();
    assert_eq!(arr.value_as_string(0), "12345.67");
    assert_eq!(arr.value_as_string(1), "-0.05");
    assert!(arrow_array::Array::is_null(arr, 2));
}

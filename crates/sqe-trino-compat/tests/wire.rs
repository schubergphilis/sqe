//! Wire-level golden tests for the Trino-compat HTTP server (issue #630).
//!
//! Every other test of `server.rs` calls the axum handlers directly with a
//! mock `TrinoState`. Nothing exercised the listener the way a client does:
//! a real TCP round trip, `nextUri` followed verbatim (including the queued
//! route), `DELETE` on the URI the client is holding, prepared statements
//! replayed through `X-Trino-Prepared-Statement`, and the `X-Trino-Set-*`
//! header echo that drives `USE` / `SET SESSION`.
//!
//! This file starts the real server on an ephemeral port with an in-process
//! DataFusion executor (no Polaris, no stack) and a mock authenticator, drives
//! it with `reqwest`, and records each hop as a step in a transcript. The
//! transcript is normalised (query ids, poll tokens, host:port, timings) and
//! compared byte-for-byte against a golden under `tests/fixtures/wire/`.
//!
//! Re-generate goldens after an intentional protocol change with
//! `UPDATE_GOLDENS=1 cargo test -p sqe-trino-compat --test wire`, then review
//! the diff: the golden IS the protocol contract.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use serde_json::{json, Value};
use sqe_core::{SecretString, Session, SqeError};
use sqe_trino_compat::server::{
    start_trino_server_with_options, HttpsListener, NodeContext, TrinoAuthenticator,
    TrinoQueryExecutor, TrinoResultStream, TrinoServerOptions,
};

// ── Test doubles ─────────────────────────────────────────────────────────────

const BASIC_PASSWORD: &str = "pw";
const GOOD_BEARER: &str = "good-token";

fn session_for(user: &str) -> Session {
    Session::new(
        user.to_string(),
        SecretString::new("catalog-token".to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec![],
    )
}

/// Basic auth accepts `alice`/`bob` with password `pw`; bearer accepts one
/// fixed token and maps it to `bearer-user`. Everything else is rejected.
struct MockAuth;

#[async_trait::async_trait]
impl TrinoAuthenticator for MockAuth {
    async fn authenticate(&self, user: &str, pass: &str, _peer_ip: &str) -> Result<Session, SqeError> {
        if pass == BASIC_PASSWORD && matches!(user, "alice" | "bob") {
            Ok(session_for(user))
        } else {
            Err(SqeError::Auth("bad credentials".to_string()))
        }
    }

    async fn authenticate_bearer(&self, token: &str, _peer_ip: &str) -> Result<Session, SqeError> {
        if token == GOOD_BEARER {
            Ok(session_for("bearer-user"))
        } else {
            Err(SqeError::Auth("bad token".to_string()))
        }
    }
}

/// Rows in the in-memory table `t`. 2500 rows at the default page size of
/// 1000 gives two full pages plus a short last page, so the last page is
/// recognised as such without an extra empty fetch.
const T_ROWS: i64 = 2500;

/// Set when the `/* stall */` query's stream is dropped.
static STALL_DROPPED: AtomicBool = AtomicBool::new(false);

/// Sets its flag on drop.
struct DropFlag(&'static AtomicBool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// In-process DataFusion executor. Two SQL-comment markers steer timing so
/// the queued and cancel routes can be reached deterministically:
/// `/* slow */` finishes after the POST's 1s bounded wait has elapsed,
/// `/* hang */` never finishes on its own.
///
/// The markers only survive to the executor when every projected column is
/// aliased: `alias_anonymous_select_columns` re-serializes the AST for a
/// SELECT with unaliased expressions, which drops comments.
struct DfExecutor {
    ctx: SessionContext,
}

impl DfExecutor {
    fn new() -> Self {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let ids: Vec<i64> = (0..T_ROWS).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("fixture batch");
        ctx.register_batch("t", batch).expect("register t");
        Self { ctx }
    }
}

#[async_trait::async_trait]
impl TrinoQueryExecutor for DfExecutor {
    async fn execute(&self, _session: &Session, sql: &str) -> Result<TrinoResultStream, SqeError> {
        if sql.contains("/* slow */") {
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        if sql.contains("/* hang */") {
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
        // Planning returns at once, but the stream never yields a batch: a
        // page fetch blocks pulling it, as a long aggregate does. The stream
        // sets STALL_DROPPED when dropped, which is how a cancel stops it.
        if sql.contains("/* stall */") {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "one",
                DataType::Int64,
                false,
            )]));
            let guard = DropFlag(&STALL_DROPPED);
            let stalled =
                futures::stream::pending::<Result<RecordBatch, SqeError>>().map(move |r| {
                    let _keep = &guard;
                    r
                });
            return Ok(TrinoResultStream::new(schema, Box::pin(stalled)));
        }
        // The server hands USE through to the executor and applies the
        // session headers itself; the coordinator answers it with an empty
        // result, which is what this stands in for.
        if sql.trim_start().to_ascii_uppercase().starts_with("USE ") {
            return Ok(TrinoResultStream::from_batches(vec![]));
        }
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))?;
        let schema = stream.schema();
        let mapped = stream.map(|r| r.map_err(|e| SqeError::Execution(e.to_string())));
        Ok(TrinoResultStream::new(schema, Box::pin(mapped)))
    }

    fn memory_pool(&self) -> Option<Arc<dyn MemoryPool>> {
        Some(self.ctx.runtime_env().memory_pool.clone())
    }
}

// ── Server + client harness ──────────────────────────────────────────────────

struct Harness {
    base: String,
    http: reqwest::Client,
    transcript: Vec<Value>,
    _server: tokio::task::JoinHandle<()>,
}

/// One recorded HTTP exchange.
struct Exchange {
    status: u16,
    headers: reqwest::header::HeaderMap,
    /// Parsed JSON body, or `Null` when the body was not JSON.
    body: Value,
    /// Raw body text (for non-JSON responses such as a bare 401).
    text: String,
}

impl Exchange {
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    fn header_all(&self, name: &str) -> Vec<String> {
        self.headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok().map(str::to_string))
            .collect()
    }

    fn next_uri(&self) -> Option<String> {
        self.body
            .get("nextUri")
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    fn state(&self) -> &str {
        self.body
            .pointer("/stats/state")
            .and_then(Value::as_str)
            .unwrap_or("")
    }

    fn rows(&self) -> Vec<Value> {
        self.body
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }
}

/// All `data` rows across a list of pages, in order.
fn all_rows(pages: &[Exchange]) -> Vec<Value> {
    pages.iter().flat_map(Exchange::rows).collect()
}

/// Response headers worth pinning in the golden: everything the Trino
/// protocol drives through headers, plus the rate-limit hint.
const RECORDED_HEADERS: &[&str] = &[
    "x-trino-set-catalog",
    "x-trino-set-schema",
    "x-trino-set-session",
    "x-trino-clear-session",
    "x-trino-added-prepare",
    "x-trino-deallocated-prepare",
    "retry-after",
    "www-authenticate",
];

impl Harness {
    async fn start() -> Self {
        // The server binds a caller-chosen port and reports nothing back, so
        // pick a free one first. The bind-then-release window is the usual
        // ephemeral-port race; on a dev box it never bites.
        let port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
            probe.local_addr().expect("probe addr").port()
        };
        let node = NodeContext {
            version: "test-version".to_string(),
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
        };
        let server = start_trino_server_with_options(
            Arc::new(MockAuth),
            Arc::new(DfExecutor::new()),
            port,
            node,
            None,
            TrinoServerOptions {
                expose_version: true,
                ..Default::default()
            },
        );
        let base = format!("http://127.0.0.1:{port}");
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("reqwest client");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if http.get(format!("{base}/v1/info")).send().await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Trino-compat server did not come up on {base}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Self {
            base,
            http,
            transcript: Vec::new(),
            _server: server,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn post(&self, sql: &str) -> reqwest::RequestBuilder {
        self.http
            .post(self.url("/v1/statement"))
            .header("X-Trino-User", "alice")
            .body(sql.to_string())
    }

    fn post_as(&self, user: &str, sql: &str) -> reqwest::RequestBuilder {
        self.post(sql).basic_auth(user, Some(BASIC_PASSWORD))
    }

    /// Send a request and append the normalised exchange to the transcript.
    async fn step(&mut self, name: &str, req: reqwest::RequestBuilder) -> Exchange {
        let resp = req
            .send()
            .await
            .unwrap_or_else(|e| panic!("step {name}: {e}"));
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let text = resp
            .text()
            .await
            .unwrap_or_else(|e| panic!("step {name} body: {e}"));
        let body = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);

        let mut recorded_headers = BTreeMap::new();
        for header_name in RECORDED_HEADERS {
            let values: Vec<String> = headers
                .get_all(*header_name)
                .iter()
                .filter_map(|v| v.to_str().ok().map(str::to_string))
                .collect();
            if !values.is_empty() {
                recorded_headers.insert((*header_name).to_string(), json!(values));
            }
        }
        let mut entry = serde_json::Map::new();
        entry.insert("step".into(), json!(name));
        entry.insert("status".into(), json!(status));
        if !recorded_headers.is_empty() {
            entry.insert(
                "headers".into(),
                Value::Object(recorded_headers.into_iter().collect()),
            );
        }
        if body.is_null() {
            entry.insert("text".into(), json!(text));
        } else {
            entry.insert("body".into(), body.clone());
        }
        self.transcript
            .push(normalize(&Value::Object(entry), &self.base));

        Exchange {
            status,
            headers,
            body,
            text,
        }
    }

    /// Follow `nextUri` from `first` until a page without one, recording each
    /// hop as `<name>-<n>`. Returns the pages in order, `first` excluded. The
    /// POST never carries columns or data (#767): the first page does.
    async fn follow(&mut self, name: &str, first: &Exchange) -> Vec<Exchange> {
        let mut pages = Vec::new();
        let mut next = first.next_uri();
        while let Some(uri) = next {
            let page = self
                .step(&format!("{name}-{}", pages.len()), self.http.get(&uri))
                .await;
            next = page.next_uri();
            pages.push(page);
        }
        pages
    }

    /// POST a statement as `user` and follow it to its terminal page.
    /// Returns the POST exchange and the pages after it.
    async fn run_as(&mut self, name: &str, user: &str, sql: &str) -> (Exchange, Vec<Exchange>) {
        let req = self.post_as(user, sql);
        let first = self.step(name, req).await;
        let pages = self.follow(name, &first).await;
        (first, pages)
    }

    /// Compare the recorded transcript against `tests/fixtures/wire/<name>.json`.
    fn assert_golden(&self, name: &str) {
        let path: PathBuf = [
            env!("CARGO_MANIFEST_DIR"),
            "tests",
            "fixtures",
            "wire",
            &format!("{name}.json"),
        ]
        .iter()
        .collect();
        let mut rendered =
            serde_json::to_string_pretty(&Value::Array(self.transcript.clone())).expect("render");
        rendered.push('\n');
        if std::env::var_os("UPDATE_GOLDENS").is_some() {
            std::fs::create_dir_all(path.parent().expect("fixture dir")).expect("mkdir fixtures");
            std::fs::write(&path, &rendered).expect("write golden");
            return;
        }
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "missing golden {}: {e}\nrun with UPDATE_GOLDENS=1 to create it; actual transcript:\n{rendered}",
                path.display()
            )
        });
        assert!(
            rendered == expected,
            "transcript for `{name}` drifted from {}\n--- expected ---\n{expected}\n--- actual ---\n{rendered}\nIf the change is intentional, re-run with UPDATE_GOLDENS=1 and review the diff.",
            path.display()
        );
    }
}

// ── Normalisation ────────────────────────────────────────────────────────────

/// Stats fields that measure time or bytes and so vary run to run.
const VOLATILE_NUMERIC_KEYS: &[&str] = &[
    "elapsedTimeMillis",
    "wallTimeMillis",
    "cpuTimeMillis",
    "queuedTimeMillis",
    "processedBytes",
    "physicalInputBytes",
    "peakMemoryBytes",
];

/// Result pages longer than this are summarised as `"<N rows>"` so the
/// pagination golden stays readable; the test asserts the rows themselves.
const MAX_GOLDEN_ROWS: usize = 10;

fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// Poll tokens are 32 random bytes as unpadded base64url: 43 URL-safe chars.
fn is_poll_token(s: &str) -> bool {
    s.len() == 43
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn normalize_string(s: &str, base: &str) -> String {
    let s = s.replace(base, "http://<host>");
    if is_uuid(&s) {
        return "<query-id>".to_string();
    }
    if s.starts_with("http://<host>/") {
        return s
            .split('/')
            .map(|seg| {
                if is_uuid(seg) {
                    "<query-id>"
                } else if is_poll_token(seg) {
                    "<poll-token>"
                } else {
                    seg
                }
            })
            .collect::<Vec<_>>()
            .join("/");
    }
    s
}

/// Recursively rebuild a JSON value with sorted keys, stable ids, a fixed
/// host, zeroed timings and summarised long pages.
fn normalize(v: &Value, base: &str) -> Value {
    match v {
        Value::String(s) => Value::String(normalize_string(s, base)),
        Value::Array(items) => Value::Array(items.iter().map(|i| normalize(i, base)).collect()),
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                let value = &map[k];
                let normalized = if VOLATILE_NUMERIC_KEYS.contains(&k.as_str()) && value.is_number()
                {
                    json!(0)
                } else if k == "uptime" && value.is_string() {
                    json!("<uptime>")
                } else if k == "data" {
                    match value.as_array() {
                        Some(rows) if rows.len() > MAX_GOLDEN_ROWS => {
                            json!(format!("<{} rows>", rows.len()))
                        }
                        _ => normalize(value, base),
                    }
                } else {
                    normalize(value, base)
                };
                out.insert(k.clone(), normalized);
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn info_endpoints() {
    let mut h = Harness::start().await;
    let info = h.step("info", h.http.get(h.url("/v1/info"))).await;
    assert_eq!(info.status, 200);
    assert_eq!(info.body["coordinator"], json!(true));
    let state = h.step("state", h.http.get(h.url("/v1/info/state"))).await;
    assert_eq!(state.status, 200);
    assert_eq!(state.text, "ACTIVE");
    h.assert_golden("info_endpoints");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_pages_columns_and_rows_via_next_uri() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "SELECT 1 AS one, 'x' AS s, 1 + 1");
    let posted = h.step("post", req).await;
    assert_eq!(posted.status, 200);
    // The POST never inlines a result (#767, trino-go-client): RUNNING plus a
    // nextUri to results page 0, no columns, no data.
    assert_eq!(posted.state(), "RUNNING");
    assert!(posted.body.get("columns").is_none(), "POST carries no columns");
    assert!(posted.body.get("data").is_none(), "POST carries no data");
    let next = posted.next_uri().expect("POST carries nextUri");
    assert!(next.ends_with("/0") && !next.contains("/queued/"), "{next}");

    let pages = h.follow("page", &posted).await;
    assert_eq!(pages.len(), 1, "single result page");
    let page = &pages[0];
    assert_eq!(page.status, 200);
    assert_eq!(page.state(), "FINISHED");
    assert!(page.next_uri().is_none(), "last page must not carry nextUri");
    let names: Vec<&str> = page.body["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .map(|c| c["name"].as_str().expect("name"))
        .collect();
    // Trino names an unaliased expression after its position, not its rank
    // among the unaliased ones: the third column is `_col2`.
    assert_eq!(names, ["one", "s", "_col2"]);
    assert_eq!(page.rows(), vec![json!([1, "x", 2])]);
    h.assert_golden("select_inline");
}

/// Nested columns go out the way Trino sends them, which is what the JDBC
/// driver decodes against: the type signature nests the element types (never
/// a parameterized `rawType`), arrays and rows are JSON arrays, maps JSON
/// objects, and a row field name that is not a plain identifier is quoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_columns_carry_nested_signatures_and_structured_values() {
    let mut h = Harness::start().await;
    let req = h.post_as(
        "alice",
        "SELECT make_array(1, NULL, 3) AS arr, \
         map(['a', 'b'], [1, NULL]) AS m, \
         named_struct('a', 1, 'My Field', 'x') AS r, \
         make_array(named_struct('k', 2, 'v', make_array(4))) AS deep",
    );
    let posted = h.step("post", req).await;
    assert_eq!(posted.status, 200);
    let pages = h.follow("page", &posted).await;
    let page = pages.last().expect("a result page");
    assert_eq!(page.state(), "FINISHED");
    let columns = page.body["columns"].as_array().expect("columns");
    let raw: Vec<&str> = columns
        .iter()
        .map(|c| c["typeSignature"]["rawType"].as_str().expect("rawType"))
        .collect();
    assert_eq!(raw, ["array", "map", "row", "array"]);
    assert_eq!(columns[2]["type"], "row(a bigint,\"My Field\" varchar)");
    assert_eq!(
        columns[2]["typeSignature"]["arguments"][1]["value"]["fieldName"]["name"],
        "My Field"
    );
    assert_eq!(
        page.rows(),
        vec![json!([[1, null, 3], {"a": 1, "b": null}, [1, "x"], [[2, [4]]]])]
    );
    h.assert_golden("nested_types");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paginated_select_follows_next_uri_to_finished() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "SELECT id, name FROM t ORDER BY id");
    let posted = h.step("post", req).await;
    assert_eq!(posted.status, 200);
    assert_eq!(posted.state(), "RUNNING");
    assert!(posted.rows().is_empty(), "POST carries no data");
    let mut next = posted.next_uri().expect("POST has nextUri");

    let first = h.step("page-0", h.http.get(&next)).await;
    assert_eq!(first.status, 200);
    assert_eq!(first.state(), "RUNNING");
    assert_eq!(first.rows().len(), 1000);
    let mut all_rows = first.rows();
    next = first.next_uri().expect("first page has nextUri");

    let second = h.step("page-1", h.http.get(&next)).await;
    assert_eq!(second.status, 200);
    assert_eq!(second.rows().len(), 1000);
    all_rows.extend(second.rows());
    next = second.next_uri().expect("second page has nextUri");

    // Skipping ahead is refused: pages are served in order.
    let skipped = next
        .rsplit_once('/')
        .map(|(head, _)| format!("{head}/7"))
        .expect("uri");
    let out_of_range = h.step("page-7-out-of-range", h.http.get(&skipped)).await;
    assert_eq!(out_of_range.status, 404);
    assert_eq!(out_of_range.state(), "FAILED");

    let last = h.step("page-2", h.http.get(&next)).await;
    assert_eq!(last.status, 200);
    assert_eq!(last.state(), "FINISHED");
    assert!(last.next_uri().is_none());
    assert_eq!(last.rows().len(), 500);
    all_rows.extend(last.rows());

    let ids: Vec<i64> = all_rows
        .iter()
        .map(|r| r[0].as_i64().expect("id"))
        .collect();
    assert_eq!(ids, (0..T_ROWS).collect::<Vec<_>>());
    h.assert_golden("paginated_select");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_query_goes_through_queued_route() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "/* slow */ SELECT 42 AS answer");
    let started = h.step("post", req).await;
    assert_eq!(started.status, 200);
    assert_eq!(started.state(), "QUEUED");
    let queued = started
        .next_uri()
        .expect("started response points at the queued route");
    assert!(queued.contains("/v1/statement/queued/"), "{queued}");

    // A long maxWait rides out the executor's delay in one hop; the poll
    // token in the URI is the credential, so no Authorization header.
    let redirect = h
        .step("poll", h.http.get(format!("{queued}?maxWait=5s")))
        .await;
    assert_eq!(redirect.status, 200);
    assert_eq!(redirect.state(), "RUNNING");
    let results = redirect
        .next_uri()
        .expect("finished poll redirects to results");
    assert!(results.ends_with("/0"), "{results}");
    assert!(!results.contains("/queued/"), "{results}");

    let page = h.step("results", h.http.get(&results)).await;
    assert_eq!(page.state(), "FINISHED");
    assert_eq!(page.rows(), vec![json!([42])]);
    h.assert_golden("queued_route");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_on_next_uri_cancels_a_running_query() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "/* hang */ SELECT 1 AS one");
    let started = h.step("post", req).await;
    assert_eq!(started.state(), "QUEUED");
    let next = started.next_uri().expect("nextUri");

    let cancelled = h.step("delete", h.http.delete(&next)).await;
    assert_eq!(cancelled.status, 204);

    // Cancel drops the registry entry, so the poll token in the URI no longer
    // proves anything: without a credential the poll is a bare 401 (no
    // query-id oracle), with the owner's credential it is "Query not found".
    let anonymous = h
        .step("poll-after-cancel-no-creds", h.http.get(&next))
        .await;
    assert_eq!(anonymous.status, 401);

    let req = h.http.get(&next).basic_auth("alice", Some(BASIC_PASSWORD));
    let after = h.step("poll-after-cancel", req).await;
    assert_eq!(after.status, 404);
    assert_eq!(after.state(), "FAILED");
    assert_eq!(after.body["error"]["message"], json!("Query not found"));

    // Idempotent for the owner: a second DELETE on a gone query is still 204.
    let req = h
        .http
        .delete(&next)
        .basic_auth("alice", Some(BASIC_PASSWORD));
    let again = h.step("delete-again", req).await;
    assert_eq!(again.status, 204);
    h.assert_golden("cancel");
}

/// A DELETE while a poll is blocked inside the page fetch stops the
/// execution. The fetch holds the cursor lock while it pulls the stream,
/// which is where a query actually runs, so a cancel that only drops an
/// unlocked cursor left the query running to completion (#788: TPC-H q09
/// cancelled at 3 s kept 1.2 GB reserved and returned its rows at 6 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_during_an_in_flight_fetch_stops_the_stream() {
    use std::sync::atomic::Ordering::SeqCst;
    let h = Harness::start().await;
    let started = h
        .post_as("alice", "/* stall */ SELECT 1 AS one")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let next = started["nextUri"].as_str().expect("nextUri").to_string();
    // Follow nextUri the way a client does; the request that fetches the
    // first page blocks pulling the stalled stream.
    let http = h.http.clone();
    let mut uri = next.clone();
    let poll = tokio::spawn(async move {
        loop {
            let body = http
                .get(&uri)
                .basic_auth("alice", Some(BASIC_PASSWORD))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            match body["nextUri"].as_str() {
                Some(n) if body["error"].is_null() => uri = n.to_string(),
                _ => return body,
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !poll.is_finished(),
        "the page fetch should block on the stalled stream"
    );
    assert!(
        !STALL_DROPPED.load(SeqCst),
        "stream dropped before the cancel"
    );

    let cancelled = h.http.delete(&next).send().await.unwrap();
    assert_eq!(cancelled.status().as_u16(), 204);

    let body = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("the blocked poll returns once the query is cancelled")
        .unwrap();
    assert_eq!(
        body["error"]["message"],
        json!("Query was canceled"),
        "{body}"
    );
    assert!(
        STALL_DROPPED.load(SeqCst),
        "cancel must drop the stream, which stops the execution"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_statement_round_trips_through_headers() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "PREPARE q1 FROM SELECT ? AS v");
    let prepared = h.step("prepare", req).await;
    assert_eq!(prepared.status, 200);
    assert_eq!(prepared.body["updateType"], json!("PREPARE"));
    let added = prepared
        .header("x-trino-added-prepare")
        .expect("PREPARE echoes x-trino-added-prepare");
    assert_eq!(added, "q1=SELECT+%3F+AS+v");

    // The client replays the header value verbatim on the EXECUTE.
    let req = h
        .post_as("alice", "EXECUTE q1 USING 42")
        .header("X-Trino-Prepared-Statement", &added);
    let posted = h.step("execute", req).await;
    assert_eq!(posted.status, 200);
    assert_eq!(posted.state(), "RUNNING");
    let pages = h.follow("execute-page", &posted).await;
    let executed = pages.last().expect("EXECUTE has a result page");
    assert_eq!(executed.state(), "FINISHED");
    assert_eq!(all_rows(&pages), vec![json!([42])]);
    assert_eq!(pages[0].body["columns"][0]["name"], json!("v"));

    let req = h
        .post_as("alice", "DESCRIBE OUTPUT q1")
        .header("X-Trino-Prepared-Statement", &added);
    let described = h.step("describe-output", req).await;
    assert_eq!(
        described.state(),
        "FAILED",
        "executor without DESCRIBE support"
    );

    // DEALLOCATE of a name the client never prepared is NOT_FOUND, as in
    // Trino (review 708); the header carries the prepared set, so a
    // DEALLOCATE without it names nothing.
    let req = h.post_as("alice", "DEALLOCATE PREPARE q1");
    let unknown = h.step("deallocate-unknown", req).await;
    assert_eq!(unknown.status, 200);
    assert_eq!(unknown.state(), "FAILED");
    assert_eq!(unknown.body["error"]["errorType"], json!("USER_ERROR"));
    assert_eq!(unknown.body["error"]["errorName"], json!("NOT_FOUND"));
    assert!(unknown.header("x-trino-deallocated-prepare").is_none());

    let req = h
        .post_as("alice", "DEALLOCATE PREPARE q1")
        .header("X-Trino-Prepared-Statement", &added);
    let deallocated = h.step("deallocate", req).await;
    assert_eq!(deallocated.body["updateType"], json!("DEALLOCATE"));
    assert_eq!(
        deallocated.header_all("x-trino-deallocated-prepare"),
        vec!["q1".to_string()]
    );

    // Without the header the name is unknown: USER_ERROR body, HTTP 200.
    let req = h.post_as("alice", "EXECUTE q1 USING 1");
    let missing = h.step("execute-unknown", req).await;
    assert_eq!(missing.status, 200);
    assert_eq!(missing.state(), "FAILED");
    assert_eq!(missing.body["error"]["errorType"], json!("USER_ERROR"));
    h.assert_golden("prepared_statement");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn use_and_session_statements_echo_headers() {
    let mut h = Harness::start().await;
    // USE reaches the executor, so it pages like a query. It finished inside
    // the POST's bounded wait, so the POST is the hop that knows the outcome
    // and carries the headers; the results page does not repeat them.
    let (posted, pages) = h.run_as("use", "alice", "USE lake.gold").await;
    assert_eq!(posted.status, 200);
    assert_eq!(posted.state(), "RUNNING");
    assert_eq!(posted.header("x-trino-set-catalog").as_deref(), Some("lake"));
    assert_eq!(posted.header("x-trino-set-schema").as_deref(), Some("gold"));
    let used = pages.last().expect("USE has a terminal page");
    assert_eq!(used.state(), "FINISHED");
    assert!(used.header("x-trino-set-catalog").is_none());
    assert_eq!(used.body["columns"], json!([]), "USE has no result columns");
    assert!(
        used.body.get("data").is_none(),
        "no data key for a column-less update"
    );

    // A property nothing in the engine reads is rejected, as Trino rejects
    // an unknown property (review 708): no header, INVALID_SESSION_PROPERTY.
    let req = h.post_as("alice", "SET SESSION query_max_run_time = '1h'");
    let rejected = h.step("set-session-unknown", req).await;
    assert_eq!(rejected.status, 200);
    assert_eq!(rejected.state(), "FAILED");
    assert_eq!(rejected.body["error"]["errorType"], json!("USER_ERROR"));
    assert_eq!(
        rejected.body["error"]["errorName"],
        json!("INVALID_SESSION_PROPERTY")
    );
    assert!(rejected.header("x-trino-set-session").is_none());

    let req = h.post_as("alice", "SET SESSION iceberg.compression_codec = 'ZSTD'");
    let set = h.step("set-session", req).await;
    assert_eq!(set.body["updateType"], json!("SET SESSION"));
    let set_session = set
        .header("x-trino-set-session")
        .expect("SET SESSION echoes x-trino-set-session");
    assert_eq!(set_session, "iceberg.compression_codec=ZSTD");

    // The client replays the property on the next request.
    let req = h
        .post_as("alice", "SHOW SESSION")
        .header("X-Trino-Session", &set_session);
    let posted = h.step("show-session", req).await;
    assert_eq!(posted.state(), "RUNNING");
    let pages = h.follow("show-session-page", &posted).await;
    assert_eq!(pages.last().expect("page").state(), "FINISHED");
    let rows = all_rows(&pages);
    assert_eq!(rows.len(), 1, "SHOW SESSION lists exactly the allow-list");
    assert_eq!(rows[0][0], json!("iceberg.compression_codec"));
    assert_eq!(rows[0][1], json!("ZSTD"), "replayed value is in effect");
    assert_eq!(rows[0][3], json!("varchar"));

    let req = h.post_as("alice", "RESET SESSION iceberg.compression_codec");
    let reset = h.step("reset-session", req).await;
    assert_eq!(reset.body["updateType"], json!("RESET SESSION"));
    assert_eq!(
        reset.header_all("x-trino-clear-session"),
        vec!["iceberg.compression_codec".to_string()]
    );
    h.assert_golden("session_headers");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_and_bearer_auth_gate_submit_and_cancel() {
    let mut h = Harness::start().await;

    let anonymous = h.step("no-auth", h.post("SELECT 1")).await;
    assert_eq!(anonymous.status, 401);

    let req = h.post("SELECT 1").basic_auth("alice", Some("wrong"));
    let bad_basic = h.step("bad-basic", req).await;
    assert_eq!(bad_basic.status, 401);

    let req = h.post("SELECT 1").bearer_auth("not-a-token");
    let bad_bearer = h.step("bad-bearer", req).await;
    assert_eq!(bad_bearer.status, 401);

    let (good_basic, pages) = h.run_as("basic", "alice", "SELECT 1").await;
    assert_eq!(good_basic.state(), "RUNNING");
    assert_eq!(pages.last().expect("result page").state(), "FINISHED");

    // Bearer-owned running query: cancel by another principal is refused,
    // cancel by the owner (legacy DELETE, credential path) succeeds.
    let req = h
        .post("/* hang */ SELECT 1 AS one")
        .bearer_auth(GOOD_BEARER);
    let started = h.step("bearer", req).await;
    assert_eq!(started.state(), "QUEUED");
    let id = started.body["id"].as_str().expect("id").to_string();
    let next = started.next_uri().expect("nextUri");

    // A poll whose slug is not the minted token falls back to credentials,
    // and without any it is a bare 401 (no query-id oracle).
    let tampered = {
        let mut segs: Vec<&str> = next.split('/').collect();
        let n = segs.len();
        segs[n - 2] = "not-the-poll-token";
        segs.join("/")
    };
    let no_creds = h
        .step("poll-tampered-slug-no-creds", h.http.get(&tampered))
        .await;
    assert_eq!(no_creds.status, 401);

    let req = h
        .http
        .delete(h.url(&format!("/v1/statement/{id}")))
        .basic_auth("bob", Some(BASIC_PASSWORD));
    let forbidden = h.step("delete-as-bob", req).await;
    assert_eq!(forbidden.status, 403);

    let req = h
        .http
        .delete(h.url(&format!("/v1/statement/{id}")))
        .bearer_auth(GOOD_BEARER);
    let cancelled = h.step("delete-as-owner", req).await;
    assert_eq!(cancelled.status, 204);
    h.assert_golden("auth");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failures_are_error_bodies_not_http_errors() {
    let mut h = Harness::start().await;
    let req = h.post_as("alice", "SELECT * FROM missing_table");
    let missing = h.step("missing-table", req).await;
    assert_eq!(
        missing.status, 200,
        "execution failures are HTTP 200 + error body"
    );
    assert_eq!(missing.state(), "FAILED");
    assert!(missing.body["error"]["message"]
        .as_str()
        .expect("message")
        .contains("missing_table"));

    let req = h.post_as("alice", "SELEC 1");
    let syntax = h.step("syntax-error", req).await;
    assert_eq!(syntax.status, 200);
    assert_eq!(syntax.state(), "FAILED");

    let empty = h.step("empty-body", h.post_as("alice", "   ")).await;
    assert_eq!(empty.status, 400);

    let unknown = h
        .step(
            "unknown-id",
            h.http.get(h.url(
                "/v1/statement/00000000-0000-0000-0000-000000000000/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/0",
            ))
            .basic_auth("alice", Some(BASIC_PASSWORD)),
        )
        .await;
    assert_eq!(unknown.status, 404);
    assert_eq!(unknown.body["error"]["message"], json!("Query not found"));
    h.assert_golden("failures");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn information_schema_is_qualified_with_the_session_catalog() {
    let mut h = Harness::start().await;
    let req = h
        .post_as(
            "alice",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        )
        .header("X-Trino-Catalog", "datafusion");
    let posted = h.step("information-schema", req).await;
    assert_eq!(posted.status, 200);
    assert_eq!(posted.state(), "RUNNING");
    let pages = h.follow("information-schema-page", &posted).await;
    assert_eq!(pages.last().expect("result page").state(), "FINISHED");
    assert_eq!(all_rows(&pages), vec![json!(["t"])]);
    h.assert_golden("information_schema");
}

// ── HTTPS listener ([coordinator] trino_https_port) ─────────────────────────

fn tls_fixture(name: &str) -> String {
    format!(
        "{}/../sqe-core/tests/fixtures/tls/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr").port()
}

/// Start the server with a plain port and an HTTPS port serving the fixture
/// certificate (signed by the private "SQE Test CA", SAN `localhost`, the
/// shape a cert-manager internal issuer produces). Returns both ports once
/// the HTTPS port answers.
async fn start_with_https() -> (u16, u16, tokio::task::JoinHandle<()>) {
    let tls_config = sqe_core::config::TlsConfig {
        cert_file: tls_fixture("server.crt"),
        key_file: tls_fixture("server.key"),
        ..Default::default()
    };
    let material = sqe_core::ServerTlsMaterial::load(&tls_config).expect("load fixture cert");
    let tls = axum_server::tls_rustls::RustlsConfig::from_config(material.rustls_server_config());
    let (plain, https) = (free_port(), free_port());
    let server = start_trino_server_with_options(
        Arc::new(MockAuth),
        Arc::new(DfExecutor::new()),
        plain,
        NodeContext {
            version: "test-version".to_string(),
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
        },
        None,
        TrinoServerOptions {
            https: Some(HttpsListener { port: https, tls }),
            ..Default::default()
        },
    );
    let client = trusting_client();
    let deadline = Instant::now() + Duration::from_secs(10);
    while client
        .get(format!("https://localhost:{https}/v1/info"))
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "HTTPS listener did not come up on {https}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (plain, https, server)
}

/// A client that trusts only the fixture CA, the way a pod with a
/// trust-manager bundle mounted trusts only the internal issuer.
fn trusting_client() -> reqwest::Client {
    let ca = std::fs::read(tls_fixture("ca.crt")).expect("read fixture CA");
    reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only([reqwest::Certificate::from_pem(&ca).expect("parse CA")])
        .https_only(true)
        .build()
        .expect("reqwest client")
}

/// Round trip over TLS: Basic auth is accepted, every `nextUri` stays on
/// https and the HTTPS port, and the rows come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_listener_round_trip_with_private_ca() {
    let (_plain, https, _server) = start_with_https().await;
    let client = trusting_client();
    let base = format!("https://localhost:{https}");

    let posted = client
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "alice")
        .basic_auth("alice", Some(BASIC_PASSWORD))
        .body("SELECT count(*) FROM t")
        .send()
        .await
        .expect("POST over TLS");
    assert_eq!(posted.status(), 200);
    let mut body: Value = posted.json().await.expect("json");
    let mut rows = Vec::new();
    while let Some(next) = body.get("nextUri").and_then(Value::as_str).map(str::to_string) {
        assert!(
            next.starts_with(&format!("{base}/v1/statement/")),
            "nextUri must stay on the HTTPS listener: {next}"
        );
        body = client
            .get(&next)
            .send()
            .await
            .expect("GET nextUri over TLS")
            .json()
            .await
            .expect("json");
        rows.extend(body.get("data").and_then(Value::as_array).cloned().unwrap_or_default());
    }
    assert_eq!(body.pointer("/stats/state"), Some(&json!("FINISHED")));
    assert_eq!(rows, vec![json!([T_ROWS])]);

    // Bearer auth is accepted over TLS too.
    let bearer = client
        .post(format!("{base}/v1/statement"))
        .bearer_auth(GOOD_BEARER)
        .body("SELECT 1")
        .send()
        .await
        .expect("bearer POST over TLS");
    assert_eq!(bearer.status(), 200);

    // A bad password is still a 401 on the TLS port.
    let rejected = client
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "alice")
        .basic_auth("alice", Some("wrong"))
        .body("SELECT 1")
        .send()
        .await
        .expect("POST over TLS");
    assert_eq!(rejected.status(), 401);
}

/// The plain listener keeps serving next to the HTTPS one, with http URIs,
/// and a client that does not trust the private CA fails the handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_listener_keeps_plain_port_and_rejects_untrusted_clients() {
    let (plain, https, _server) = start_with_https().await;

    let http = reqwest::Client::builder().no_proxy().build().expect("client");
    let posted: Value = http
        .post(format!("http://127.0.0.1:{plain}/v1/statement"))
        .header("X-Trino-User", "alice")
        .basic_auth("alice", Some(BASIC_PASSWORD))
        .body("SELECT 1")
        .send()
        .await
        .expect("POST over plain HTTP")
        .json()
        .await
        .expect("json");
    let next = posted["nextUri"].as_str().expect("nextUri");
    assert!(
        next.starts_with(&format!("http://127.0.0.1:{plain}/")),
        "plain listener keeps http URIs: {next}"
    );

    // The default client trusts no private CA: no handshake, no request.
    let untrusted = http
        .get(format!("https://localhost:{https}/v1/info"))
        .send()
        .await;
    assert!(untrusted.is_err(), "untrusted CA must fail the TLS handshake");

    // Plain HTTP on the TLS port is not answered with a Trino response.
    let plain_on_tls = http
        .get(format!("http://127.0.0.1:{https}/v1/info"))
        .send()
        .await;
    assert!(
        plain_on_tls.map(|r| !r.status().is_success()).unwrap_or(true),
        "plain HTTP must not be served on the TLS port"
    );
}

//! `/v1/statement` reports the bytes a statement read in `stats`:
//! `physicalInputBytes` (fetched from storage) and `processedBytes`
//! (decoded by the scans), on the last page.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};

/// Every request authenticates as the session `setup_handler` built.
struct FixedAuth(sqe_core::Session);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoAuthenticator for FixedAuth {
    async fn authenticate(
        &self,
        _username: &str,
        _password: &str,
        _peer_ip: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        Ok(self.0.clone())
    }
}

/// The coordinator adapter's query path (`sqe-server` `execute_as`):
/// SELECTs stream under the Trino query id, with the bytes probe attached.
struct StreamingQuery(Arc<sqe_coordinator::QueryHandler>);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoQueryExecutor for StreamingQuery {
    async fn execute(
        &self,
        _session: &sqe_core::Session,
        _sql: &str,
    ) -> Result<sqe_trino_compat::server::TrinoResultStream, sqe_core::SqeError> {
        unreachable!("the server always passes the query id")
    }

    async fn execute_with_query_id(
        &self,
        session: &sqe_core::Session,
        sql: &str,
        query_id: &str,
    ) -> Result<sqe_trino_compat::server::TrinoResultStream, sqe_core::SqeError> {
        use futures::StreamExt;
        let id = uuid::Uuid::parse_str(query_id).expect("server mints UUID ids");
        let (schema, stream) = self
            .0
            .execute_stream_with_id(session, sql, None, Some(id))
            .await?;
        let stream = stream.map(|res| res.map_err(sqe_core::SqeError::from));
        Ok(
            sqe_trino_compat::server::TrinoResultStream::new(schema, Box::pin(stream))
                .with_bytes(self.0.statement_bytes_probe(id)),
        )
    }
}

/// Run `sql` and return the `stats` of its last page.
async fn last_stats(base_url: &str, sql: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let mut body: serde_json::Value = client
        .post(format!("{base_url}/v1/statement"))
        .basic_auth("root", Some("s3cr3t"))
        .body(sql.to_string())
        .send()
        .await
        .expect("POST /v1/statement")
        .json()
        .await
        .expect("statement json");
    while let Some(next) = body.get("nextUri").and_then(|v| v.as_str()) {
        body = client
            .get(next)
            .basic_auth("root", Some("s3cr3t"))
            .send()
            .await
            .expect("GET nextUri")
            .json()
            .await
            .expect("page json");
    }
    assert!(body.get("error").is_none(), "{sql}: {body}");
    assert_eq!(body["stats"]["state"], "FINISHED", "{sql}: {body}");
    body["stats"].clone()
}

/// Start the Trino endpoint on a free port; returns its base URL.
async fn start_server(
    session: &sqe_core::Session,
    handler: &Arc<sqe_coordinator::QueryHandler>,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let node = sqe_trino_compat::server::NodeContext {
        version: "test".to_string(),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        started_at: std::time::Instant::now(),
    };
    let _server = sqe_trino_compat::server::start_trino_server(
        Arc::new(FixedAuth(session.clone())),
        Arc::new(StreamingQuery(Arc::clone(handler))),
        port,
        node,
        None,
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://127.0.0.1:{port}")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_stats_report_the_bytes_a_select_read() {
    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let t = "default.trino_stats_bytes";
    for sql in [
        format!("DROP TABLE IF EXISTS {t}"),
        format!("CREATE TABLE {t} (id BIGINT, s VARCHAR)"),
        format!("INSERT INTO {t} SELECT value, repeat('x', 64) FROM generate_series(1, 5000)"),
    ] {
        handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let files_sql =
        "SELECT CAST(SUM(file_size_in_bytes) AS BIGINT) FROM table_files('default', 'trino_stats_bytes')";
    let batches = handler
        .execute(&session, files_sql, None)
        .await
        .expect("table_files");
    let file_bytes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0) as u64;

    let base_url = start_server(&session, &handler).await;

    let stats = last_stats(&base_url, &format!("SELECT * FROM {t}")).await;
    let physical = stats["physicalInputBytes"]
        .as_u64()
        .expect("physicalInputBytes");
    let processed = stats["processedBytes"].as_u64().expect("processedBytes");
    assert!(
        physical > 0 && physical <= file_bytes,
        "physicalInputBytes {physical} not within the {file_bytes} bytes of data files"
    );
    assert!(processed > 0, "processedBytes is 0: {stats}");
    assert_eq!(stats["processedRows"], 5000);

    let stats = last_stats(&base_url, "SELECT 1").await;
    assert_eq!(stats["physicalInputBytes"], 0, "{stats}");
    assert_eq!(stats["processedBytes"], 0, "{stats}");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}

/// File TVFs scan through DataFusion's `DataSourceExec`, which the stats
/// walk did not count as a scan: `read_parquet` reported 0 bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_stats_report_bytes_for_read_parquet() {
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("stats.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (1..=5000).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("ArrowWriter");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let base_url = start_server(&session, &handler).await;
    let sql = format!("SELECT * FROM read_parquet('{}')", path.display());
    let stats = last_stats(&base_url, &sql).await;
    let processed = stats["processedBytes"].as_u64().expect("processedBytes");
    assert!(processed > 0, "processedBytes is 0: {stats}");
    assert_eq!(stats["processedRows"], 5000);
}

/// A distributed scan reports the file bytes its fragments covered as
/// physical input and the rows the workers returned as processed bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_stats_report_bytes_for_a_distributed_scan() {
    use crate::common::chaos_worker::{spawn_chaos_worker, ChaosMode};

    let (setup_session, setup) = crate::common::setup_handler().await;
    let t = "default.trino_stats_bytes_dist";
    for sql in [
        format!("DROP TABLE IF EXISTS {t}"),
        format!("CREATE TABLE {t} (id BIGINT, s VARCHAR)"),
        format!("INSERT INTO {t} SELECT value, repeat('x', 64) FROM generate_series(1, 5000)"),
        format!("INSERT INTO {t} SELECT value, repeat('y', 64) FROM generate_series(5001, 9000)"),
    ] {
        setup
            .execute(&setup_session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let files_sql = "SELECT CAST(SUM(file_size_in_bytes) AS BIGINT) \
                     FROM table_files('default', 'trino_stats_bytes_dist')";
    let batches = setup
        .execute(&setup_session, files_sql, None)
        .await
        .expect("table_files");
    let file_bytes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0) as u64;

    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let rig = crate::distributed_failure_e2e::rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let base_url = start_server(&rig.session, &rig.handler).await;

    let stats = last_stats(&base_url, &format!("SELECT * FROM {t}")).await;
    assert!(worker.do_gets() >= 1, "the scan never reached the worker");
    let physical = stats["physicalInputBytes"]
        .as_u64()
        .expect("physicalInputBytes");
    let processed = stats["processedBytes"].as_u64().expect("processedBytes");
    assert!(
        physical > 0 && physical <= file_bytes,
        "physicalInputBytes {physical} not within the {file_bytes} bytes of data files"
    );
    assert!(processed > 0, "processedBytes is 0: {stats}");
    assert_eq!(stats["processedRows"], 9000);

    let _ = setup
        .execute(&setup_session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}

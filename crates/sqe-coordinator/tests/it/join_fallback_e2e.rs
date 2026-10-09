//! Issue #784: a hash join whose build side outgrows `query.max_query_memory`
//! fails with ResourcesExhausted before its first batch, because
//! DataFusion's hash-join build cannot spill. With `query.join_fallback` on,
//! the coordinator replans the query once with sort-merge joins, which sort
//! and spill, and the query completes.
//!
//! Runs the real `QueryHandler` paths (buffered `execute` and streaming
//! `execute_stream`) over two Hive external CSV tables, so no Polaris or
//! object store is needed. The answer is computed in closed form.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::PassthroughEnforcer;

/// Rows per side. Every key matches exactly once, so the hash build holds
/// all of one side: far more than [`CAP`].
const ROWS: i64 = 1_500_000;
const CAP: &str = "16MB";
const SQL: &str = "SELECT COUNT(*), SUM(a.x + b.y) FROM lake.t.a a JOIN lake.t.b b ON a.k = b.k";

/// `a(k, x = k)`, `b(k, y = 2k)`: the join returns ROWS rows and
/// `SUM(x + y) = 3 * (0 + 1 + ... + ROWS-1)`.
fn expected() -> (i64, i64) {
    (ROWS, 3 * (ROWS * (ROWS - 1) / 2))
}

fn answer(batches: &[RecordBatch]) -> (i64, i64) {
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    (
        batch.column(0).as_primitive::<Int64Type>().value(0),
        batch.column(1).as_primitive::<Int64Type>().value(0),
    )
}

fn write_csv_table(dir: &std::path::Path, col: &str, factor: i64) {
    let partition = dir.join("dt=2026-01-01");
    std::fs::create_dir_all(&partition).expect("mkdir partition");
    let mut body = format!("k,{col}\n");
    for k in 0..ROWS {
        body.push_str(&format!("{k},{}\n", k * factor));
    }
    std::fs::write(partition.join("part.csv"), body).expect("write csv");
}

async fn mock_polaris() -> wiremock::MockServer {
    let server = crate::common::mock_server().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/config"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/namespaces"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

struct Fixture {
    _mock: wiremock::MockServer,
    _dirs: Vec<tempfile::TempDir>,
    handler: QueryHandler,
    session: Session,
}

async fn fixture(join_fallback: bool) -> Fixture {
    crate::common::init_tracing();
    let root = tempfile::tempdir().expect("tempdir");
    let data_a = tempfile::tempdir().expect("tempdir");
    let data_b = tempfile::tempdir().expect("tempdir");
    let spill = tempfile::tempdir().expect("tempdir");
    write_csv_table(data_a.path(), "x", 1);
    write_csv_table(data_b.path(), "y", 2);

    let mock = mock_polaris().await;
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\n",
        mock.uri()
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("minimal config parses");
    config.storage.tvf.allow_local_paths = true;
    config.query.max_query_memory = CAP.to_string();
    config.query.per_user_memory_budget = "0".to_string();
    config.query.join_fallback = join_fallback;
    config.coordinator.spill_dir = spill.path().display().to_string();
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );
    let session = Session::new(
        format!("tester-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    );

    handler
        .execute(
            &session,
            &format!(
                "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
                root.path().display()
            ),
            None,
        )
        .await
        .expect("attach hive root");
    for (name, col, dir) in [("a", "x", &data_a), ("b", "y", &data_b)] {
        handler
            .execute(
                &session,
                &format!(
                    "CREATE EXTERNAL TABLE lake.t.{name} (k BIGINT, {col} BIGINT) \
                     PARTITIONED BY (dt string) \
                     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
                     LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
                    dir.path().display()
                ),
                None,
            )
            .await
            .expect("create external table");
    }
    Fixture {
        _mock: mock,
        _dirs: vec![root, data_a, data_b, spill],
        handler,
        session,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn without_the_fallback_the_hash_build_exhausts_the_query_cap() {
    let f = fixture(false).await;
    let err = f
        .handler
        .execute(&f.session, SQL, None)
        .await
        .expect_err("a hash build over the cap must fail without the fallback");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("resources exhausted"),
        "expected a memory error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fallback_completes_the_join_on_the_buffered_path() {
    let f = fixture(true).await;
    let batches = f
        .handler
        .execute(&f.session, SQL, None)
        .await
        .expect("the sort-merge retry completes the join");
    assert_eq!(answer(&batches), expected());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fallback_completes_the_join_on_the_streaming_path() {
    let f = fixture(true).await;
    let (_, stream) = f
        .handler
        .execute_stream(&f.session, SQL, None)
        .await
        .expect("plan the streaming query");
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .expect("the sort-merge retry completes the join");
    assert_eq!(answer(&batches), expected());
}

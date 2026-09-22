//! DataFusion's built-in `information_schema` answers for a Hive root through
//! `HiveSchemaProvider::table_names()`, which serves the process-wide,
//! principal-agnostic listing snapshot. #716: the per-session provider gates
//! it with the SAME policy resolution `SHOW TABLES` uses. #701: a
//! `WHERE table_schema = '<db>'` conjunct refreshes only that database.
//!
//! Harness trimmed from `hive_ddl_test.rs` (`handler_with_root_and_policy`).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{PassthroughEnforcer, PolicyStore, ResolvedPolicy};

async fn mock_polaris() -> wiremock::MockServer {
    let server = wiremock::MockServer::start().await;
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

fn create_table_ddl(data_dir: &std::path::Path, table: &str) -> String {
    format!(
        "CREATE EXTERNAL TABLE {table} (id BIGINT, amount DOUBLE) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
        data_dir.display()
    )
}

fn table_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn hive_information_schema_hides_a_denied_table_and_pins_to_one_database() {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let mock = mock_polaris().await;
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\n",
        mock.uri()
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("minimal config parses");
    config.storage.tvf.allow_local_paths = true;

    let store = Arc::new(InMemoryPolicyStore::new());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        Some(store.clone() as Arc<dyn PolicyStore>),
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

    // Fresh username/token per run: `SESSION_CONTEXT_CACHE` is process-wide.
    let session = Session::new(
        format!("tester-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    );
    let run = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move { handler.execute(session, &sql, None).await }
    };

    run(format!(
        "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
        root_dir.path().display()
    ))
    .await
    .expect("attach hive root");
    let data_dir = root_dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir");
    std::fs::write(data_dir.join("a.csv"), "id,amount\n1,10.5\n").expect("write csv");
    for table in ["lake.sales.orders", "lake.sales.returns", "lake.hr.people"] {
        run(create_table_ddl(&data_dir, table))
            .await
            .expect("create table");
    }

    let sales_sql = "SELECT table_name FROM lake.information_schema.tables \
                     WHERE table_schema = 'sales' ORDER BY table_name"
        .to_string();

    // Positive control: everything is visible before the deny.
    let names = table_names(&run(sales_sql.clone()).await.expect("info schema"));
    assert_eq!(names, vec!["orders".to_string(), "returns".to_string()]);

    // #716: deny-all on the table, keyed `{root}.{database}` like SELECT (#715).
    store
        .add_table_policy(
            "lake.sales",
            "orders",
            ResolvedPolicy {
                row_filters: vec![datafusion::logical_expr::lit(false)],
                ..Default::default()
            },
        )
        .await;
    let names = table_names(&run(sales_sql).await.expect("info schema"));
    assert_eq!(
        names,
        vec!["returns".to_string()],
        "a fully-denied Hive table must not appear in information_schema.tables"
    );

    let err = run("SELECT * FROM lake.sales.orders".to_string())
        .await
        .expect_err("SELECT on a fully-denied table must refuse");
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("not found"),
        "denial must read like a missing table: {msg}"
    );

    // #701: a pinned table_schema lists only that database.
    let names = table_names(
        &run(
            "SELECT table_name FROM lake.information_schema.tables WHERE table_schema = 'hr'"
                .to_string(),
        )
        .await
        .expect("info schema"),
    );
    assert_eq!(names, vec!["people".to_string()]);
}

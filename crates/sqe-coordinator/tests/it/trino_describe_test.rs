//! `DESCRIBE OUTPUT <prepared>` / `DESCRIBE INPUT <prepared>` (#3).
//!
//! Trino JDBC issues these for `PreparedStatement.getMetaData()` /
//! `getParameterMetaData()`. They describe a PREPAREd statement: OUTPUT returns
//! one row per output column, INPUT one row per bind parameter. These tests
//! exercise the coordinator's `describe_prepared`, which numbers `?` ->
//! `$1..$N`, plans the statement (no execution / no bound values), and builds
//! the synthetic result set. A tableless `SELECT ... WHERE ... = ?` is used so
//! planning needs no table metadata -- only a minimal Polaris mock for the
//! session-context build.

use std::sync::Arc;

use chrono::{Duration, Utc};
use sqe_core::{Session, SqeConfig};
use sqe_trino_compat::protocol::DescribeKind;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_empty_polaris() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

fn handler(catalog_url: &str) -> sqe_coordinator::QueryHandler {
    let toml = format!(
        "[coordinator]\n\n[auth]\ntoken_endpoint = \"http://127.0.0.1:9/unused\"\nclient_id = \"c\"\n\n\
         [catalog]\ncatalog_url = \"{catalog_url}\"\nwarehouse = \"main_warehouse\"\n\n\
         [storage]\ns3_endpoint = \"http://127.0.0.1:9\"\ns3_access_key = \"_\"\ns3_secret_key = \"_\"\ns3_region = \"us-east-1\"\ns3_path_style = true\n"
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker =
        Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history));
    sqe_coordinator::QueryHandler::new(
        policy,
        None,
        config,
        None,
        None,
        None,
        None,
        query_tracker,
        None,
        None,
        None,
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("QueryHandler::new")
}

fn session() -> Session {
    Session::new(
        "describer".to_string(),
        sqe_core::SecretString::new("tok".to_string()),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    )
}

fn cell(batch: &arrow_array::RecordBatch, col: usize, row: usize) -> String {
    arrow::util::display::array_value_to_string(batch.column(col), row).unwrap_or_default()
}

/// DESCRIBE OUTPUT: one row per output column with the Trino type name.
#[tokio::test]
async fn describe_output_lists_columns_and_types() {
    let server = mount_empty_polaris().await;
    let h = handler(&server.uri());
    let sql = "SELECT 1 AS id, 'hi' AS name WHERE 1 = ?";

    let batches = h
        .describe_prepared(&session(), sql, DescribeKind::Output)
        .await
        .expect("describe output");
    let b = &batches[0];
    assert_eq!(b.schema().field(0).name(), "Column Name");
    assert_eq!(b.schema().field(4).name(), "Type");
    assert_eq!(b.num_rows(), 2, "two output columns");
    // Column names in order.
    assert_eq!(cell(b, 0, 0), "id");
    assert_eq!(cell(b, 0, 1), "name");
    // Trino type names: bigint (Int64 literal) and varchar.
    assert_eq!(cell(b, 4, 0), "bigint");
    assert_eq!(cell(b, 4, 1), "varchar");
}

/// DESCRIBE INPUT: one row per bind parameter with its inferred type.
#[tokio::test]
async fn describe_input_lists_parameters_and_types() {
    let server = mount_empty_polaris().await;
    let h = handler(&server.uri());
    // Two params: `id = ?` (Int64) and `name = ?` (Utf8).
    let sql = "SELECT 1 AS id, 'hi' AS name WHERE 1 = ? AND 'x' = ?";

    let batches = h
        .describe_prepared(&session(), sql, DescribeKind::Input)
        .await
        .expect("describe input");
    let b = &batches[0];
    assert_eq!(b.schema().field(0).name(), "Position");
    assert_eq!(b.schema().field(1).name(), "Type");
    assert_eq!(b.num_rows(), 2, "two bind parameters");
    // Positions are 0-based (Trino convention).
    assert_eq!(cell(b, 0, 0), "0");
    assert_eq!(cell(b, 0, 1), "1");
    // Inferred types: bigint and varchar (unknown is acceptable if DataFusion
    // cannot infer, but these comparisons against typed literals should infer).
    assert_eq!(cell(b, 1, 0), "bigint");
    assert_eq!(cell(b, 1, 1), "varchar");
}

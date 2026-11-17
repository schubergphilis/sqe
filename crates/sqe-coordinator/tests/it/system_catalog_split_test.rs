//! The `system` catalog split (#901): SQE serves the reserved schemas
//! (`runtime`, `metadata`, `jdbc`, `information_schema`) and a Polaris
//! catalog named `system` serves every other schema.
//!
//! `scripts/bootstrap-test.sh` creates the Polaris catalog `system` with a
//! namespace `access`. The split only applies with
//! `catalog_discovery = "polaris-auto"`, so every test here uses that mode.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use sqe_core::SqeConfig;

/// The tests share the Polaris `system` catalog and the process-wide
/// session-context cache; run them one at a time.
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn polaris_auto_config() -> SqeConfig {
    let mut config = SqeConfig::load(&crate::common::test_config_path()).expect("load test config");
    config.query.catalog_discovery = sqe_core::config::CatalogDiscovery::PolarisAuto;
    config
}

fn polaris_auto_handler() -> sqe_coordinator::QueryHandler {
    let config = polaris_auto_config();
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
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
    .expect("QueryHandler::new succeeds")
}

async fn root_session() -> sqe_core::Session {
    let config = SqeConfig::load(&crate::common::test_config_path()).expect("load test config");
    let auth = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("create authenticator");
    auth.authenticate("root", "")
        .await
        .expect("authenticate as root")
}

/// Every value of the first column, as strings.
fn first_column(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        for i in 0..col.len() {
            out.push(crate::common::fmt_val(col.as_ref(), i));
        }
    }
    out
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// `SHOW SCHEMAS FROM system` lists the virtual schemas and the Polaris
/// namespace `access`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_show_schemas_lists_both_parts() {
    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    let batches = handler
        .execute(&session, "SHOW SCHEMAS FROM system", None)
        .await
        .expect("SHOW SCHEMAS FROM system");
    let schemas = first_column(&batches);
    for want in ["access", "runtime", "metadata", "jdbc"] {
        assert!(
            schemas.iter().any(|s| s == want),
            "SHOW SCHEMAS FROM system must list {want}: {schemas:?}"
        );
    }
}

/// The virtual tables answer as before with a Polaris `system` present.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_virtual_tables_unchanged() {
    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    handler
        .execute(&session, "SELECT * FROM system.runtime.queries", None)
        .await
        .expect("system.runtime.queries must still be served by SQE");
    let batches = handler
        .execute(&session, "SHOW TABLES FROM system.runtime", None)
        .await
        .expect("SHOW TABLES FROM system.runtime");
    let tables = first_column(&batches);
    assert!(
        tables.iter().any(|t| t == "queries"),
        "SHOW TABLES FROM system.runtime must list queries: {tables:?}"
    );
}

/// A table in the Polaris `system` catalog is created, written, listed,
/// described, read and dropped like a table in any other catalog.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_polaris_table_round_trip() {
    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS system.access.audit", None)
        .await;
    let create = "CREATE TABLE system.access.audit (id BIGINT, who VARCHAR)";
    handler
        .execute(&session, create, None)
        .await
        .expect("CREATE TABLE in the Polaris system catalog");
    let insert = "INSERT INTO system.access.audit VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')";
    handler
        .execute(&session, insert, None)
        .await
        .expect("INSERT into system.access.audit");
    handler
        .execute(
            &session,
            "DELETE FROM system.access.audit WHERE id = 2",
            None,
        )
        .await
        .expect("DELETE from system.access.audit");

    let select = "SELECT who FROM system.access.audit ORDER BY id";
    let batches = handler
        .execute(&session, select, None)
        .await
        .expect("SELECT from system.access.audit");
    assert_eq!(first_column(&batches), vec!["alice", "carol"]);

    let batches = handler
        .execute(&session, "SHOW TABLES FROM system.access", None)
        .await
        .expect("SHOW TABLES FROM system.access");
    let tables = first_column(&batches);
    assert!(
        tables.iter().any(|t| t == "audit"),
        "SHOW TABLES FROM system.access must list audit: {tables:?}"
    );

    let batches = handler
        .execute(&session, "SHOW COLUMNS FROM system.access.audit", None)
        .await
        .expect("SHOW COLUMNS FROM system.access.audit");
    let columns = first_column(&batches);
    assert_eq!(columns, vec!["id", "who"], "columns of system.access.audit");

    handler
        .execute(&session, "DROP TABLE system.access.audit", None)
        .await
        .expect("DROP TABLE system.access.audit");
}

/// A Flight SQL ingest naming catalog `system` and schema `access` appends
/// to the Polaris table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_flight_ingest_appends_to_polaris_table() {
    use arrow_flight::sql::{
        CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
    };

    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    let _ = handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS system.access.ingest_t",
            None,
        )
        .await;
    let create = "CREATE TABLE system.access.ingest_t (id BIGINT, name VARCHAR)";
    handler
        .execute(&session, create, None)
        .await
        .expect("CREATE TABLE system.access.ingest_t");

    let server = crate::flight_ingest_e2e::IngestServer::serve(
        polaris_auto_handler(),
        polaris_auto_config(),
        None,
    )
    .await;
    let mut client = server.client().await;
    let command = CommandStatementIngest {
        table_definition_options: Some(TableDefinitionOptions {
            if_not_exist: TableNotExistOption::Fail as i32,
            if_exists: TableExistsOption::Append as i32,
        }),
        table: "ingest_t".to_string(),
        schema: Some("access".to_string()),
        catalog: Some("system".to_string()),
        temporary: false,
        transaction_id: None,
        options: std::collections::HashMap::new(),
    };
    let sent = crate::flight_ingest_e2e::batch(&[(1, "alpha"), (2, "beta")]);
    let count = crate::flight_ingest_e2e::ingest(&mut client, command, vec![sent])
        .await
        .expect("ingest into system.access.ingest_t");
    assert_eq!(count, 2, "the server reports the rows it wrote");

    let select = "SELECT name FROM system.access.ingest_t ORDER BY id";
    let batches = handler
        .execute(&session, select, None)
        .await
        .expect("read back system.access.ingest_t");
    assert_eq!(first_column(&batches), vec!["alpha", "beta"]);

    handler
        .execute(&session, "DROP TABLE system.access.ingest_t", None)
        .await
        .expect("DROP TABLE system.access.ingest_t");
}

/// A reserved schema name cannot be created in catalog `system`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_create_reserved_schema_refused() {
    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    for schema in ["runtime", "METADATA"] {
        let sql = format!("CREATE SCHEMA system.{schema}");
        let err = handler
            .execute(&session, &sql, None)
            .await
            .expect_err("a reserved schema must be refused in catalog system");
        let msg = err.to_string();
        assert!(
            msg.contains("reserved") && msg.contains(&schema.to_ascii_lowercase()),
            "{sql}: the refusal must name the reserved schema: {msg}"
        );
    }
}

/// `CALL system.<procedure>` keeps resolving to the built-in procedure with a
/// Polaris catalog `system` present, and runs on the session catalog's table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn system_split_call_procedure_unchanged() {
    let _lock = LOCK.lock().await;
    crate::common::init_tracing();
    let handler = polaris_auto_handler();
    let session = root_session().await;

    let _ = handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS test_ns.sys_split_call_t",
            None,
        )
        .await;
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.sys_split_call_t (id BIGINT)",
            None,
        )
        .await
        .expect("create test_ns.sys_split_call_t");
    for v in [1, 2] {
        let sql = format!("INSERT INTO test_ns.sys_split_call_t VALUES ({v})");
        handler
            .execute(&session, &sql, None)
            .await
            .expect("insert into test_ns.sys_split_call_t");
    }

    let call = "CALL system.rewrite_data_files(table => 'test_ns.sys_split_call_t')";
    handler
        .execute(&session, call, None)
        .await
        .expect("CALL system.rewrite_data_files must run the built-in procedure");

    let batches = handler
        .execute(&session, "SELECT id FROM test_ns.sys_split_call_t", None)
        .await
        .expect("read back after rewrite");
    assert_eq!(row_count(&batches), 2, "the rewrite keeps both rows");

    let _ = handler
        .execute(
            &session,
            "DROP TABLE IF EXISTS test_ns.sys_split_call_t",
            None,
        )
        .await;
}

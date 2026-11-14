//! #897: Trino `WITH (...)` table properties through `/v1/statement`, in
//! the statement shapes stock dbt-trino sends.

use std::sync::Arc;

use iceberg::spec::FormatVersion;

const NS: &str = "default";

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

/// The handler's `execute`, which takes DDL and DML as well as queries.
struct Execute(Arc<sqe_coordinator::QueryHandler>);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoQueryExecutor for Execute {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<sqe_trino_compat::server::TrinoResultStream, sqe_core::SqeError> {
        self.0
            .execute(session, sql, None)
            .await
            .map(sqe_trino_compat::server::TrinoResultStream::from_batches)
    }
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
        Arc::new(Execute(Arc::clone(handler))),
        port,
        node,
        None,
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://127.0.0.1:{port}")
}

/// Run `sql` through `/v1/statement` to its last page; the error message
/// when it failed.
async fn run(base_url: &str, sql: &str) -> Result<(), String> {
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
    match body.get("error") {
        Some(err) => Err(err["message"].as_str().unwrap_or_default().to_string()),
        None => Ok(()),
    }
}

async fn ok(base_url: &str, sql: &str) {
    run(base_url, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The table as Polaris holds it.
async fn load(session: &sqe_core::Session, name: &str) -> iceberg::table::Table {
    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("config");
    let catalog = sqe_catalog::rest_catalog::SessionCatalog::for_session(
        &config,
        None,
        session.access_token().expose(),
    )
    .await
    .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs([NS, name]).expect("ident");
    catalog.load_table(&ident).await.expect("load table")
}

/// dbt-trino's table materialization: `create table ... comment '...'
/// WITH (format_version = N) as (select ...)`, both versions whatever the
/// server default is, then `ALTER TABLE ... SET PROPERTIES format_version`
/// upgrades and refuses a downgrade.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_format_version_property_897() {
    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let base_url = start_server(&session, &handler).await;
    let (v2, v3) = ("trino_props_v2_897", "trino_props_v3_897");
    for t in [v2, v3] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }

    for (t, version, want) in [(v2, "2", FormatVersion::V2), (v3, "'3'", FormatVersion::V3)] {
        let sql = format!(
            "create table {NS}.{t}\n      comment 'a mart'\n      WITH (\n          \
             format_version = {version}\n      )\n    as (\n      \
             select 1 as id, 'a' as name\n    );"
        );
        ok(&base_url, &sql).await;
        let table = load(&session, t).await;
        assert_eq!(table.metadata().format_version(), want, "{sql}");
        let props = table.metadata().properties();
        assert!(!props.contains_key("format_version"), "{t}: {props:?}");
        assert_eq!(
            props.get("comment").map(String::as_str),
            Some("a mart"),
            "{t}"
        );
    }

    ok(
        &base_url,
        &format!("ALTER TABLE {NS}.{v2} SET PROPERTIES format_version = 3"),
    )
    .await;
    assert_eq!(
        load(&session, v2).await.metadata().format_version(),
        FormatVersion::V3
    );
    let err = run(
        &base_url,
        &format!("ALTER TABLE {NS}.{v2} SET PROPERTIES format_version = 2"),
    )
    .await
    .expect_err("a downgrade is refused");
    assert!(err.contains("cannot lower the format version"), "{err}");
    // The upgraded table takes every kind of write.
    for sql in [
        format!("INSERT INTO {NS}.{v2} VALUES (2, 'b'), (3, 'c')"),
        format!("UPDATE {NS}.{v2} SET name = 'z' WHERE id = 2"),
        format!("DELETE FROM {NS}.{v2} WHERE id = 3"),
    ] {
        ok(&base_url, &sql).await;
    }

    // An enforced dbt contract: `create table (cols) comment ... WITH (...)`,
    // then `insert into`.
    let contract = "trino_props_contract_897";
    ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{contract}")).await;
    let sql = format!(
        "create table {NS}.{contract} (\n  id integer,\n  name varchar\n)\n\
         comment 'a contract'\nWITH (\n  format_version = 3\n)"
    );
    ok(&base_url, &sql).await;
    ok(
        &base_url,
        &format!("insert into {NS}.{contract} (id, name) values (1, 'a')"),
    )
    .await;
    let table = load(&session, contract).await;
    assert_eq!(
        table.metadata().format_version(),
        FormatVersion::V3,
        "{sql}"
    );
    let props = table.metadata().properties();
    assert_eq!(props.get("comment").map(String::as_str), Some("a contract"));

    for t in [v2, v3, contract] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }
}

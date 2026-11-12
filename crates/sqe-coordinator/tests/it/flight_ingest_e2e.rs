//! Flight SQL bulk ingest (`DoPut` + `CommandStatementIngest`) over a real gRPC
//! socket against the Polaris + RustFS test stack.
//!
//! This is what ADBC `adbc_ingest` / `cursor.adbc_ingest` sends. The server
//! side is `do_put_statement_ingest` -> `handle_ingest_streaming`. Until now
//! nothing drove that arm through the wire protocol: `flight_sql_smoke.rs`
//! covers the transport with a catalog-free mock, and the write tests call
//! `QueryHandler` in process, which never decodes a `DoPut` stream.
//!
//! The server here is the production service on an OS-assigned port, wired the
//! way `sqe-server` wires it (one `SessionManager` shared by the handler and
//! the service), with the real `Authenticator` from `tests/sqe-test.toml`. A
//! client authenticates by replaying the session id as the bearer token, the
//! way `distributed_failure_e2e` does, so no handshake is involved.
//!
//! Every test uses its own table name, so tests do not share state.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandStatementIngest, EndTransaction, TableDefinitionOptions, TableExistsOption,
    TableNotExistOption,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::TryStreamExt;
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::session_manager::SessionManager;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

/// A running Flight SQL server over the real stack. Dropping it stops it.
struct IngestServer {
    addr: std::net::SocketAddr,
    sessions: Arc<SessionManager>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl IngestServer {
    async fn start() -> Self {
        // `setup_handler` builds the handler the other tests use; the session
        // manager is attached afterwards, as `sqe-server` does.
        let (_root, handler) = crate::common::setup_handler().await;
        let config =
            sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("test config");
        let authenticator = Arc::new(
            sqe_auth::Authenticator::new(&config.auth)
                .await
                .expect("authenticator"),
        );
        let sessions = Arc::new(SessionManager::new(authenticator));
        let handler = handler.with_session_manager(Arc::clone(&sessions));
        let service = SqeFlightSqlService::new(Arc::clone(&sessions), Arc::new(handler), config);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = rx.await;
                })
                .await
                .expect("flight sql server runs");
        });
        Self {
            addr,
            sessions,
            shutdown: Some(tx),
            task: Some(task),
        }
    }

    /// A client on its own session (own token), authenticated as the test
    /// stack's root principal. Two clients are two sessions of the same user.
    async fn client(&self) -> FlightSqlServiceClient<Channel> {
        let channel = Channel::from_shared(format!("http://{}", self.addr))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to the flight sql server");
        let session = self
            .sessions
            .authenticate("root", "")
            .await
            .expect("root session");
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_token(session.id.clone());
        client
    }
}

impl Drop for IngestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

const SCHEMA: &str = "default";

fn fq(table: &str) -> String {
    format!("{SCHEMA}.{table}")
}

/// Arrow schema of the rows every test sends.
fn ingest_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// One batch of `(id, name)` rows.
fn batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0)));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
    RecordBatch::try_new(ingest_schema(), vec![ids, names]).expect("batch")
}

fn ingest_command(
    table: &str,
    if_not_exist: TableNotExistOption,
    if_exists: TableExistsOption,
    transaction_id: Option<Bytes>,
) -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: Some(TableDefinitionOptions {
            if_not_exist: if_not_exist as i32,
            if_exists: if_exists as i32,
        }),
        table: table.to_string(),
        schema: Some(SCHEMA.to_string()),
        catalog: None,
        temporary: false,
        transaction_id,
        options: HashMap::new(),
    }
}

/// Send `batches` as one `DoPut` ingest stream; returns the server's count.
async fn ingest(
    client: &mut FlightSqlServiceClient<Channel>,
    command: CommandStatementIngest,
    batches: Vec<RecordBatch>,
) -> Result<i64, arrow_flight::error::FlightError> {
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    client.execute_ingest(command, stream).await
}

/// Run `sql` through `GetFlightInfo` + `DoGet`.
async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, arrow_flight::error::FlightError> {
    let info = client.execute(sql.to_string(), None).await?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.clone().expect("endpoint carries a ticket");
        let stream = client.do_get(ticket).await?;
        batches.extend(stream.try_collect::<Vec<_>>().await?);
    }
    Ok(batches)
}

/// `id|name` rows of `table`, ordered by id.
async fn rows(client: &mut FlightSqlServiceClient<Channel>, table: &str) -> Vec<String> {
    let sql = format!("SELECT id, name FROM {} ORDER BY id", fq(table));
    let batches = query(client, &sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            let cells: Vec<String> = (0..2)
                .map(|c| crate::common::fmt_val(b.column(c).as_ref(), r))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// `DROP TABLE IF EXISTS` then `CREATE TABLE`, so a rerun starts clean.
async fn fresh_table(client: &mut FlightSqlServiceClient<Channel>, table: &str) {
    drop_table(client, table).await;
    client
        .execute_update(
            format!("CREATE TABLE {} (id BIGINT, name STRING)", fq(table)),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("create {table}: {e}"));
}

async fn drop_table(client: &mut FlightSqlServiceClient<Channel>, table: &str) {
    client
        .execute_update(format!("DROP TABLE IF EXISTS {}", fq(table)), None)
        .await
        .unwrap_or_else(|e| panic!("drop {table}: {e}"));
}

fn strings(rows: &[&str]) -> Vec<String> {
    rows.iter().map(|s| s.to_string()).collect()
}

/// An ingest into an existing table with append semantics returns the number
/// of rows sent and the rows are readable, value for value.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_append_into_existing_table_returns_count_and_rows() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_append_one";
    fresh_table(&mut client, table).await;

    let cmd = ingest_command(
        table,
        TableNotExistOption::Fail,
        TableExistsOption::Append,
        None,
    );
    let sent = batch(&[(1, "alpha"), (2, "beta"), (3, "gamma")]);
    let count = ingest(&mut client, cmd, vec![sent]).await.expect("ingest");

    assert_eq!(count, 3, "the server reports the rows it wrote");
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|alpha", "2|beta", "3|gamma"])
    );
    drop_table(&mut client, table).await;
}

/// The create-if-not-exists mode ADBC's `mode="create"` / `"create_append"`
/// sends: the table does not exist, the ingest creates it from the stream
/// schema and loads the rows.
///
/// Known bug #881: `do_put_statement_ingest` never reads
/// `table_definition_options`, and `handle_ingest_streaming` starts with
/// `load_table`, so a missing table answers `NOT_FOUND` / `TABLE_NOT_FOUND`
/// instead of being created.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "bug:#881; needs:polaris; ingest ignores TableNotExistOption::Create"]
async fn ingest_create_if_not_exists_creates_the_table() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_create_new";
    drop_table(&mut client, table).await;

    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Fail,
        None,
    );
    let sent = batch(&[(10, "ten"), (20, "twenty")]);
    let count = ingest(&mut client, cmd, vec![sent])
        .await
        .expect("create-if-not-exists ingest into a missing table");

    assert_eq!(count, 2, "the server reports the rows it wrote");
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["10|ten", "20|twenty"])
    );
    drop_table(&mut client, table).await;
}

/// Two append ingests: both batches are there, the second count is its own.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_append_twice_keeps_both_batches() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_append_two";
    fresh_table(&mut client, table).await;

    let append = || {
        ingest_command(
            table,
            TableNotExistOption::Fail,
            TableExistsOption::Append,
            None,
        )
    };
    let first = ingest(
        &mut client,
        append(),
        vec![batch(&[(1, "one"), (2, "two")])],
    )
    .await
    .expect("first ingest");
    assert_eq!(first, 2);
    assert_eq!(rows(&mut client, table).await, strings(&["1|one", "2|two"]));

    let second = ingest(&mut client, append(), vec![batch(&[(3, "three")])])
        .await
        .expect("second ingest");
    assert_eq!(
        second, 1,
        "the count is this ingest's rows, not the table's"
    );
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|one", "2|two", "3|three"])
    );
    drop_table(&mut client, table).await;
}

/// `TABLE_EXISTS_OPTION_FAIL` on an existing table is an error, and the table
/// keeps the rows it had: a refused ingest writes nothing.
///
/// Known bug #881: `TableExistsOption` is ignored, so every ingest into an
/// existing table appends. The call returns `Ok(1)` and the row is written.
/// (`Replace` is ignored the same way.)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "bug:#881; needs:polaris; ingest ignores TableExistsOption::Fail"]
async fn ingest_fail_if_exists_refuses_an_existing_table() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_fail_exists";
    fresh_table(&mut client, table).await;
    client
        .execute_update(
            format!("INSERT INTO {} VALUES (1, 'seed')", fq(table)),
            None,
        )
        .await
        .expect("seed row");

    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Fail,
        None,
    );
    let result = ingest(&mut client, cmd, vec![batch(&[(2, "refused")])]).await;

    assert!(
        result.is_err(),
        "fail-if-exists on an existing table must error, got {result:?}"
    );
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|seed"]),
        "a refused ingest must not write"
    );
    drop_table(&mut client, table).await;
}

/// A stream of several batches arrives complete and in value.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_multi_batch_stream_arrives_complete() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_multi_batch";
    fresh_table(&mut client, table).await;

    // 4 batches of different sizes; 2 + 1 + 3 + 5 rows.
    let batches = vec![
        batch(&[(1, "a"), (2, "b")]),
        batch(&[(3, "c")]),
        batch(&[(4, "d"), (5, "e"), (6, "f")]),
        batch(&[(7, "g"), (8, "h"), (9, "i"), (10, "j"), (11, "k")]),
    ];
    let cmd = ingest_command(
        table,
        TableNotExistOption::Fail,
        TableExistsOption::Append,
        None,
    );
    let count = ingest(&mut client, cmd, batches).await.expect("ingest");

    assert_eq!(count, 11);
    let expected: Vec<String> = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{}|{n}", i + 1))
        .collect();
    assert_eq!(rows(&mut client, table).await, expected);
    drop_table(&mut client, table).await;
}

/// An ingest carrying a `BeginTransaction` id is not visible to another
/// session until `EndTransaction(COMMIT)`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris; two concurrent sessions"]
async fn ingest_in_transaction_is_invisible_until_commit() {
    let server = IngestServer::start().await;
    let mut writer = server.client().await;
    let mut reader = server.client().await;
    let table = "flight_ingest_txn_commit";
    fresh_table(&mut writer, table).await;

    let txn = writer.begin_transaction().await.expect("begin transaction");
    let cmd = ingest_command(
        table,
        TableNotExistOption::Fail,
        TableExistsOption::Append,
        Some(txn.clone()),
    );
    let count = ingest(
        &mut writer,
        cmd,
        vec![batch(&[(1, "pending"), (2, "pending")])],
    )
    .await
    .expect("ingest inside the transaction");
    assert_eq!(count, 2);

    assert!(
        rows(&mut reader, table).await.is_empty(),
        "another session must not see an uncommitted ingest"
    );

    writer
        .end_transaction(txn, EndTransaction::Commit)
        .await
        .expect("commit");

    assert_eq!(
        rows(&mut reader, table).await,
        strings(&["1|pending", "2|pending"]),
        "the committed rows are visible"
    );
    drop_table(&mut writer, table).await;
}

/// An ingest inside a transaction that is rolled back leaves no rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris; two concurrent sessions"]
async fn ingest_in_transaction_is_gone_after_rollback() {
    let server = IngestServer::start().await;
    let mut writer = server.client().await;
    let mut reader = server.client().await;
    let table = "flight_ingest_txn_rollback";
    fresh_table(&mut writer, table).await;
    writer
        .execute_update(
            format!("INSERT INTO {} VALUES (1, 'kept')", fq(table)),
            None,
        )
        .await
        .expect("seed row outside the transaction");

    let txn = writer.begin_transaction().await.expect("begin transaction");
    let cmd = ingest_command(
        table,
        TableNotExistOption::Fail,
        TableExistsOption::Append,
        Some(txn.clone()),
    );
    ingest(&mut writer, cmd, vec![batch(&[(2, "discarded")])])
        .await
        .expect("ingest inside the transaction");

    writer
        .end_transaction(txn, EndTransaction::Rollback)
        .await
        .expect("rollback");

    for (who, client) in [("writer", &mut writer), ("reader", &mut reader)] {
        assert_eq!(
            rows(client, table).await,
            strings(&["1|kept"]),
            "{who}: the rolled-back ingest is gone, the seed row stays"
        );
    }
    drop_table(&mut writer, table).await;
}

// Policy case (ingest refused for a principal without write privilege on the
// target) is not here: the default Polaris + RustFS stack has one principal,
// `root`, which owns everything. A second principal with narrower grants needs
// the Ranger stack (`make test-access-control`, `SQE_AC_E2E=1`), which is a
// different docker stack and not part of this suite's group.

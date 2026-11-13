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
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandStatementIngest, DoPutUpdateResult, EndTransaction, ProstMessageExt,
    TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use arrow_flight::FlightDescriptor;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
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

/// Like [`ingest`], but the stream announces `schema` even with no batches.
/// `execute_ingest` encodes without a schema, so an empty upload carries none;
/// ADBC drivers send the schema first.
async fn ingest_announcing_schema(
    client: &mut FlightSqlServiceClient<Channel>,
    command: CommandStatementIngest,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
) -> Result<i64, arrow_flight::error::FlightError> {
    let descriptor = FlightDescriptor::new_cmd(prost::Message::encode_to_vec(&command.as_any()));
    let data = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .with_schema(schema)
        .build(futures::stream::iter(batches.into_iter().map(Ok)))
        .map(|frame| frame.expect("the batches encode"));
    let mut responses = client.do_put(data).await?;
    let result = responses
        .message()
        .await
        .map_err(arrow_flight::error::FlightError::from)?
        .expect("a PutResult");
    let update: DoPutUpdateResult = prost::Message::decode(&*result.app_metadata)
        .map_err(|e| arrow_flight::error::FlightError::DecodeError(e.to_string()))?;
    Ok(update.record_count)
}

/// The gRPC code of a failed ingest, if the error carries one.
fn status_code<T>(result: &Result<T, arrow_flight::error::FlightError>) -> Option<tonic::Code> {
    match result {
        Err(arrow_flight::error::FlightError::Tonic(status)) => Some(status.code()),
        _ => None,
    }
}

/// `snapshot_id|operation` of every snapshot of `table`, oldest first.
async fn snapshots(client: &mut FlightSqlServiceClient<Channel>, table: &str) -> Vec<String> {
    let sql = format!(
        "SELECT snapshot_id, operation FROM table_snapshots('{SCHEMA}', '{table}') \
         ORDER BY committed_at, snapshot_id"
    );
    let batches = query(client, &sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            out.push(format!(
                "{}|{}",
                crate::common::fmt_val(b.column(0).as_ref(), r),
                crate::common::fmt_val(b.column(1).as_ref(), r)
            ));
        }
    }
    out
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
/// schema and loads the rows (#881).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
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

/// `TABLE_EXISTS_OPTION_FAIL` on an existing table is an error (gRPC
/// `AlreadyExists`), and the table keeps the rows it had: a refused ingest
/// writes nothing (#881).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
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

    assert_eq!(
        status_code(&result),
        Some(tonic::Code::AlreadyExists),
        "fail-if-exists on an existing table must refuse, got {result:?}"
    );
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|seed"]),
        "a refused ingest must not write"
    );
    drop_table(&mut client, table).await;
}

/// `create_append` (CREATE, APPEND) twice: the first call creates the table,
/// the second appends to it. Exact contents after each.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_create_append_twice_creates_then_appends() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_create_append";
    drop_table(&mut client, table).await;

    let create_append = || {
        ingest_command(
            table,
            TableNotExistOption::Create,
            TableExistsOption::Append,
            None,
        )
    };
    let first = ingest(
        &mut client,
        create_append(),
        vec![batch(&[(1, "one"), (2, "two")])],
    )
    .await
    .expect("first create_append creates the table");
    assert_eq!(first, 2);
    assert_eq!(rows(&mut client, table).await, strings(&["1|one", "2|two"]));

    let second = ingest(&mut client, create_append(), vec![batch(&[(3, "three")])])
        .await
        .expect("second create_append appends");
    assert_eq!(second, 1, "the count is this ingest's rows");
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|one", "2|two", "3|three"])
    );
    drop_table(&mut client, table).await;
}

/// `replace` (CREATE, REPLACE) on an existing table: the rows are exactly the
/// new ones, and the old snapshot is still in the table's history (one new
/// overwrite snapshot, not a drop and recreate).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_replace_swaps_the_rows_and_keeps_history() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_replace";
    fresh_table(&mut client, table).await;
    let seed = ingest_command(
        table,
        TableNotExistOption::Fail,
        TableExistsOption::Append,
        None,
    );
    ingest(&mut client, seed, vec![batch(&[(1, "old"), (2, "old")])])
        .await
        .expect("seed");
    assert_eq!(rows(&mut client, table).await, strings(&["1|old", "2|old"]));
    let before = snapshots(&mut client, table).await;
    assert_eq!(before.len(), 1, "the seed is one snapshot: {before:?}");

    let replace = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Replace,
        None,
    );
    let count = ingest(
        &mut client,
        replace,
        vec![batch(&[(5, "new"), (6, "new"), (7, "new")])],
    )
    .await
    .expect("replace");
    assert_eq!(count, 3);
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["5|new", "6|new", "7|new"]),
        "the old rows are gone, the new ones are all there"
    );

    let after = snapshots(&mut client, table).await;
    assert_eq!(after.len(), 2, "one new snapshot: {after:?}");
    assert_eq!(
        after[0], before[0],
        "the previous snapshot is still in the history"
    );
    assert!(
        after[1].ends_with("|overwrite") || after[1].ends_with("|replace"),
        "the replace is one overwrite snapshot, got {}",
        after[1]
    );
    drop_table(&mut client, table).await;
}

/// A replace whose stream does not fit the table fails and leaves the table
/// as it was: same rows, same snapshots.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_replace_with_a_mismatched_stream_leaves_the_table_unchanged() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_replace_mismatch";
    fresh_table(&mut client, table).await;
    client
        .execute_update(
            format!("INSERT INTO {} VALUES (1, 'kept')", fq(table)),
            None,
        )
        .await
        .expect("seed row");
    let before = snapshots(&mut client, table).await;

    // Three columns into a two-column table.
    let wide_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let wide = RecordBatch::try_new(
        wide_schema,
        vec![
            Arc::new(Int64Array::from_iter_values([9])) as ArrayRef,
            Arc::new(StringArray::from_iter_values(["x"])) as ArrayRef,
            Arc::new(StringArray::from_iter_values(["y"])) as ArrayRef,
        ],
    )
    .expect("wide batch");
    let replace = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Replace,
        None,
    );
    let result = ingest(&mut client, replace, vec![wide]).await;

    assert!(
        result.is_err(),
        "a mismatched replace must fail, got {result:?}"
    );
    assert_eq!(
        rows(&mut client, table).await,
        strings(&["1|kept"]),
        "the table keeps its rows"
    );
    assert_eq!(
        snapshots(&mut client, table).await,
        before,
        "and its snapshots: a failed replace commits nothing"
    );
    drop_table(&mut client, table).await;
}

/// A create from a stream that announces its schema but carries no rows: the
/// table exists with that schema and has no rows. (The Flight encoder sends
/// the schema of an empty stream only when told to, so this sends it
/// explicitly, as ADBC drivers do.)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_empty_stream_creates_an_empty_table() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_empty_create";
    drop_table(&mut client, table).await;

    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Fail,
        None,
    );
    let count = ingest_announcing_schema(&mut client, cmd, vec![], ingest_schema())
        .await
        .expect("create from an empty stream");
    assert_eq!(count, 0);

    assert!(
        rows(&mut client, table).await.is_empty(),
        "the table exists and has no rows"
    );
    let columns = query(
        &mut client,
        &format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = '{SCHEMA}' AND table_name = '{table}' \
             ORDER BY ordinal_position"
        ),
    )
    .await
    .expect("columns");
    let mut described = Vec::new();
    for b in &columns {
        for r in 0..b.num_rows() {
            described.push(format!(
                "{}|{}",
                crate::common::fmt_val(b.column(0).as_ref(), r),
                crate::common::fmt_val(b.column(1).as_ref(), r)
            ));
        }
    }
    assert_eq!(
        described,
        strings(&["id|Int64", "name|Utf8View"]),
        "the table has the stream's schema"
    );
    drop_table(&mut client, table).await;
}

/// A create with a stream that announces no schema fails with
/// `InvalidArgument` and creates no table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_create_from_a_stream_without_a_schema_is_refused() {
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_no_schema";
    drop_table(&mut client, table).await;

    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Fail,
        None,
    );
    // One message carrying the command descriptor and no schema or batches:
    // nothing to create the table from. (A client with no descriptor message
    // at all never reaches the ingest handler, so the descriptor is sent by
    // hand.)
    let descriptor = FlightDescriptor::new_cmd(prost::Message::encode_to_vec(&cmd.as_any()));
    let frame = arrow_flight::FlightData {
        flight_descriptor: Some(descriptor),
        ..Default::default()
    };
    let result: Result<(), arrow_flight::error::FlightError> =
        match client.do_put(futures::stream::iter([frame])).await {
            Err(e) => Err(e),
            Ok(mut responses) => responses
                .message()
                .await
                .map(|_| ())
                .map_err(arrow_flight::error::FlightError::from),
        };
    assert_eq!(
        status_code(&result),
        Some(tonic::Code::InvalidArgument),
        "got {result:?}"
    );
    let probe = query(&mut client, &format!("SELECT id FROM {}", fq(table))).await;
    assert!(probe.is_err(), "no table was created");
}

/// An ingest that creates a table inside a Flight transaction creates it
/// autocommit (as SQL DDL does there) and writes the rows to the transaction:
/// a rollback leaves the table, empty.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris; two concurrent sessions"]
async fn ingest_create_in_a_transaction_then_rollback_leaves_an_empty_table() {
    let server = IngestServer::start().await;
    let mut writer = server.client().await;
    let mut reader = server.client().await;
    let table = "flight_ingest_txn_create_rollback";
    drop_table(&mut writer, table).await;

    let txn = writer.begin_transaction().await.expect("begin transaction");
    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Append,
        Some(txn.clone()),
    );
    let count = ingest(&mut writer, cmd, vec![batch(&[(1, "discarded")])])
        .await
        .expect("create-and-ingest inside the transaction");
    assert_eq!(count, 1);

    writer
        .end_transaction(txn, EndTransaction::Rollback)
        .await
        .expect("rollback");

    for (who, client) in [("writer", &mut writer), ("reader", &mut reader)] {
        assert!(
            rows(client, table).await.is_empty(),
            "{who}: the table exists and the rolled-back rows are gone"
        );
    }
    drop_table(&mut writer, table).await;
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

/// #888: a create from a stream with an unsigned 64-bit column makes a
/// `long` column instead of failing; Iceberg has no unsigned integers. A
/// value above `i64::MAX` fails the ingest rather than wrapping.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ingest_create_maps_uint64_to_long() {
    use arrow_array::UInt64Array;
    let server = IngestServer::start().await;
    let mut client = server.client().await;
    let table = "flight_ingest_uint64_create";
    drop_table(&mut client, table).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let unsigned = |id: u64, name: &str| {
        let ids: ArrayRef = Arc::new(UInt64Array::from(vec![id]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![name]));
        RecordBatch::try_new(schema.clone(), vec![ids, names]).expect("batch")
    };
    let cmd = ingest_command(
        table,
        TableNotExistOption::Create,
        TableExistsOption::Append,
        None,
    );
    let count = ingest(&mut client, cmd.clone(), vec![unsigned(7, "seven")])
        .await
        .expect("create from a UInt64 stream");
    assert_eq!(count, 1);
    assert_eq!(rows(&mut client, table).await, strings(&["7|seven"]));

    let too_big = ingest(&mut client, cmd, vec![unsigned(u64::MAX, "max")]).await;
    assert!(too_big.is_err(), "u64::MAX must not fit a long column");
    assert_eq!(rows(&mut client, table).await, strings(&["7|seven"]));
    drop_table(&mut client, table).await;
}

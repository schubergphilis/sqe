//! Smoke test for the Arrow Flight SQL server over a real gRPC socket.
//!
//! Every other test in this tier drives `QueryHandler` in process. That leaves
//! the wire protocol -- the only way a client ever reaches SQE -- uncovered:
//! handshake, bearer propagation, `GetFlightInfo` ticket issuance, and the
//! `DoGet` stream that carries the Arrow batches. `audit_e2e_test.rs` gets
//! closest by calling service methods with hand-built `tonic::Request`s, and
//! says so in its own comment: `do_handshake` cannot be reached that way at all,
//! because `tonic::Streaming<HandshakeRequest>` is not constructible without the
//! server machinery.
//!
//! So this file starts the server. It binds `127.0.0.1:0` and serves through
//! `serve_with_incoming`, because both production entry points hand tonic a
//! `SocketAddr` and let it bind, which cannot express "pick a free port and tell
//! me which one". Everything else -- the service, its auth, its statement path
//! -- is the production code.
//!
//! Deliberately no docker stack: an `AnonymousProvider` supplies identity and
//! the queries are catalog-free, so this runs on a bare `cargo test` and keeps
//! the transport as the only thing under test.

use std::sync::Arc;

use arrow_array::Array;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use sqe_auth::{AnonymousProvider, AnonymousProviderConfig};
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::{
    query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry, SessionManager,
};
use sqe_core::{SecretStore, SqeConfig};
use sqe_policy::PassthroughEnforcer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

/// `catalog_url` points at an in-process Polaris mock that serves an empty
/// catalog. The queries never touch a table, so an empty catalog keeps the
/// transport as the only thing under test. This used to be a closed port: since
/// finding 306 (#657) an unreachable catalog fails the session build loudly
/// instead of degrading to an empty namespace list, so a dead URL now proves
/// the fail-closed rule rather than the statement path.
fn smoke_config(catalog_url: &str) -> SqeConfig {
    let toml = format!("[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n");
    toml::from_str(&toml).expect("smoke config parses")
}

/// Polaris mock with a config endpoint and zero namespaces.
async fn mount_empty_polaris() -> wiremock::MockServer {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};
    let server = crate::common::mock_server().await;
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

/// A running Flight SQL server. Dropping it shuts the server task down.
struct FlightServer {
    addr: std::net::SocketAddr,
    /// The server's query tracker, so tests can observe query state the way
    /// the admin API does.
    tracker: Arc<QueryTracker>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
    /// The Polaris mock the session build talks to; dropped with the server.
    _polaris: wiremock::MockServer,
}

impl FlightServer {
    async fn start() -> Self {
        let polaris = mount_empty_polaris().await;
        let config = smoke_config(&polaris.uri());
        let tracker = Arc::new(QueryTracker::new(&config.query_history));
        let handler = QueryHandler::new(
            Arc::new(PassthroughEnforcer),
            None, // policy_store
            config.clone(),
            None, // worker_registry
            None, // credential_tracker
            None, // metrics
            None, // audit
            Arc::clone(&tracker),
            None, // query_cache
            None, // grant_backend
            None, // lineage
            RuntimeCatalogRegistry::new(),
            SecretStore::new(),
        )
        .expect("QueryHandler::new");

        // Any credentials authenticate. The subject here is the transport, not
        // the identity provider, which has its own tests.
        let provider = Arc::new(AnonymousProvider::new(AnonymousProviderConfig::default()));
        let session_manager = Arc::new(SessionManager::with_provider(provider));
        let service = SqeFlightSqlService::new(session_manager, Arc::new(handler), config);

        // Port 0 plus `serve_with_incoming`: the OS picks a free port and the
        // listener reports it, so concurrent test binaries cannot collide the
        // way a fixed port would.
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
            tracker,
            shutdown: Some(tx),
            task: Some(task),
            _polaris: polaris,
        }
    }

    /// A gRPC channel to this server. `connect` retries internally until the
    /// server accepts, so no sleep is needed after `start`.
    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .expect("valid endpoint")
            .connect()
            .await
            .expect("connect to the flight sql server")
    }
}

impl Drop for FlightServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Run `sql` through a client and return the concatenated batches.
async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<arrow_array::RecordBatch>, arrow_flight::error::FlightError> {
    let info = client.execute(sql.to_string(), None).await?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.clone().expect("endpoint carries a ticket");
        let stream = client.do_get(ticket).await?;
        batches.extend(stream.try_collect::<Vec<_>>().await?);
    }
    Ok(batches)
}

/// The full client sequence: handshake for a token, then a statement whose
/// result value is checked after a round trip through Arrow IPC.
///
/// `SELECT 1 + 1` rather than `SELECT 1`: a literal the planner could fold from
/// the SQL text alone would not prove the value came back from execution.
#[tokio::test(flavor = "multi_thread")]
async fn handshake_then_statement_returns_rows_over_grpc() {
    let server = FlightServer::start().await;
    let mut client = FlightSqlServiceClient::new(server.channel().await);

    let token = client
        .handshake("smoke", "smoke")
        .await
        .expect("handshake succeeds against the anonymous provider");
    assert!(
        !token.is_empty(),
        "handshake must return a session token for the client to send back"
    );
    client.set_token(String::from_utf8(token.to_vec()).expect("token is utf-8"));

    let batches = query(&mut client, "SELECT 1 + 1 AS answer")
        .await
        .expect("statement executes over the wire");

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "expected exactly one row, got {rows}");
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a batch");
    let col = batch
        .column_by_name("answer")
        .expect("the projected alias survives the round trip");
    let values = col
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("1 + 1 is an Int64");
    assert_eq!(values.value(0), 2);
}

/// Finding 403: the ADBC/JDBC cancel sequence over the wire. `GetFlightInfo`
/// for a long `SELECT`, `DoGet` left streaming, then `CancelQuery` with the
/// returned `FlightInfo`. Before the fix the ticket carried the SQL, so the
/// server could not map it to a tracked query and answered UNSPECIFIED (0).
#[tokio::test(flavor = "multi_thread")]
async fn cancel_query_with_flight_info_cancels_the_running_statement() {
    use arrow_flight::sql::{ActionCancelQueryRequest, ActionCancelQueryResult, ProstMessageExt};
    use arrow_flight::Action;
    use prost::Message;
    use sqe_coordinator::query_tracker::QueryState;

    let server = FlightServer::start().await;
    let mut client = FlightSqlServiceClient::new(server.channel().await);
    let token = client.handshake("smoke", "smoke").await.expect("handshake");
    client.set_token(String::from_utf8(token.to_vec()).expect("token is utf-8"));

    // Effectively unbounded work, catalog-free.
    let info = client
        .execute(
            "SELECT count(*) FROM generate_series(1, 100000000000)".to_string(),
            None,
        )
        .await
        .expect("GetFlightInfo");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");

    // Start DoGet on a second client and leave it draining in the background.
    let mut reader = FlightSqlServiceClient::new(server.channel().await);
    reader.set_token(client.token().expect("token set").to_string());
    let drain = tokio::spawn(async move {
        let stream = reader.do_get(ticket).await.expect("DoGet opens");
        stream.try_collect::<Vec<_>>().await
    });

    // Wait until the tracker shows the query running.
    let tracker = Arc::clone(&server.tracker);
    let running_id = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(r) = tracker
                .records()
                .into_iter()
                .find(|r| r.state == QueryState::Running)
            {
                break r.query_id;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("query must reach Running");

    let request = ActionCancelQueryRequest {
        info: info.encode_to_vec().into(),
    };
    let action = Action {
        r#type: "CancelQuery".to_string(),
        body: request.as_any().encode_to_vec().into(),
    };
    let mut results = client
        .do_action(action)
        .await
        .expect("DoAction CancelQuery");
    let first = results
        .try_next()
        .await
        .expect("action stream")
        .expect("one result");
    let any = arrow_flight::sql::Any::decode(&*first.body).expect("Any");
    let result: ActionCancelQueryResult = any.unpack().expect("unpack").expect("typed result");
    assert_eq!(result.result, 1, "CancelResult must be CANCELLED (1)");

    // The tracker shows Canceled within a second.
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if tracker
                .get(&running_id)
                .is_some_and(|r| r.state == QueryState::Canceled)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("tracker must show Canceled within one second");

    // And the DoGet stream is released rather than pinned until the idle timer.
    // The stream may end with a Cancelled status or cleanly; only its
    // termination is asserted here.
    let _stream_outcome = tokio::time::timeout(std::time::Duration::from_secs(5), drain)
        .await
        .expect("cancel must end the DoGet stream")
        .expect("drain task");
}

/// A statement without a bearer token must be refused by the transport.
///
/// The control matters as much as the assertion: the SAME client sequence with
/// a token succeeds in the test above, so this proves the refusal comes from the
/// missing credential and not from a malformed request or a dead server.
#[tokio::test(flavor = "multi_thread")]
async fn statement_without_a_token_is_unauthenticated() {
    let server = FlightServer::start().await;
    let mut client = FlightSqlServiceClient::new(server.channel().await);

    let err = query(&mut client, "SELECT 1")
        .await
        .expect_err("a tokenless statement must not execute");

    // Assert the gRPC status CODE, not a substring of the message. A message
    // match would also accept an Internal or Unavailable error that happened to
    // mention authorization, which is how a broken server reads as a working
    // deny. Observed here: Unauthenticated / "No authorization header".
    match err {
        arrow_flight::error::FlightError::Tonic(status) => assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "expected Unauthenticated, got {:?}: {}",
            status.code(),
            status.message()
        ),
        other => panic!("expected a tonic status, got: {other}"),
    }
}

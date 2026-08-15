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

/// `catalog_url` points at a closed port on purpose. These queries never touch
/// a catalog, and a dead URL proves it: if the statement path grew a dependency
/// on catalog reachability, this test would fail rather than quietly pass
/// against whatever happens to be listening on a developer's machine.
const SMOKE_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://127.0.0.1:59997"
"#;

fn smoke_config() -> SqeConfig {
    toml::from_str(SMOKE_TOML).expect("smoke config parses")
}

/// A running Flight SQL server. Dropping it shuts the server task down.
struct FlightServer {
    addr: std::net::SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FlightServer {
    async fn start() -> Self {
        let config = smoke_config();
        let tracker = Arc::new(QueryTracker::new(&config.query_history));
        let handler = QueryHandler::new(
            Arc::new(PassthroughEnforcer),
            None, // policy_store
            config.clone(),
            None, // worker_registry
            None, // credential_tracker
            None, // metrics
            None, // audit
            tracker,
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
            shutdown: Some(tx),
            task: Some(task),
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

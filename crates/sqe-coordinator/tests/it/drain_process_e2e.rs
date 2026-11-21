//! Acceptance 4 of `openspec/changes/admission-queue-and-drain` (#902), end to
//! end through a real `sqe-server` process: SIGTERM during a running query.
//!
//! Every in-process test of the drain drives `flip_ready_and_drain` or the
//! handler's `draining` flag directly. None of them proves the pieces meet in
//! the binary: the signal handler, `stop_admitting` reaching the handler the
//! Flight service uses, the tracker count the drain polls, the close path, and
//! the exit. This test starts the binary and sends it a real SIGTERM.
//!
//! No docker stack: an anonymous auth provider supplies identity, the query is
//! catalog-free, and the catalog is a wiremock Polaris that serves an empty
//! catalog (the same shape `flight_sql_smoke.rs` uses).
//!
//! The binary is `target/<profile>/sqe-server`, built by `cargo dev-server`
//! (override with `SQE_SERVER_BIN`). A missing binary FAILS the test with the
//! build command; it is never skipped silently.
//!
//! `cargo test -p sqe-coordinator --test it -- --ignored drain_process`

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::Channel;

/// `shutdown_drain_secs` for the server under test. The exit must come far
/// sooner than this: it is the deadline the drain must NOT run to.
const DRAIN_SECS: u64 = 60;

/// A CPU-bound, catalog-free statement that runs for several seconds on the
/// server. Both sides are `generate_series`, so nothing is read from storage,
/// and the modulo filter keeps the optimiser from folding the join away.
const LONG_QUERY: &str = "SELECT count(*) AS n FROM generate_series(1, 9000) a \
     CROSS JOIN generate_series(1, 9000) b WHERE (a.value * b.value) % 7 = 3";

fn server_binary() -> PathBuf {
    if let Ok(path) = std::env::var("SQE_SERVER_BIN") {
        return PathBuf::from(path);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| manifest.join("../../target"));
    let bin = target.join("debug").join("sqe-server");
    assert!(
        bin.exists(),
        "{} is missing: build it with `cargo dev-server` (or set SQE_SERVER_BIN)",
        bin.display()
    );
    bin
}

/// A free local port. Bound and released at once, so a race with another
/// process is possible but unlikely on loopback; the server reports a bind
/// failure loudly if it happens.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

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

/// The server process. Killed on drop so a failed assertion leaves nothing
/// running.
struct Server {
    child: Child,
    flight_port: u16,
    log: PathBuf,
}

impl Server {
    fn start(dir: &std::path::Path, catalog_url: &str) -> Self {
        let flight_port = free_port();
        let config = format!(
            "[coordinator]\n\
             flight_sql_port = {flight_port}\n\
             trino_http_port = {trino}\n\
             shutdown_drain_secs = {DRAIN_SECS}\n\n\
             [metrics]\n\
             prometheus_port = {prom}\n\n\
             [auth]\n\
             [[auth.providers]]\n\
             type = \"anonymous\"\n\n\
             [catalog]\n\
             catalog_url = \"{catalog_url}\"\n",
            trino = free_port(),
            prom = free_port(),
        );
        let config_path = dir.join("sqe.toml");
        std::fs::write(&config_path, config).expect("write the config");

        // Output goes to a file, not a terminal: with a terminal on stderr the
        // server drains nothing (`effective_drain_secs`), which would defeat
        // the test.
        let log = dir.join("server.log");
        let out = std::fs::File::create(&log).expect("create the log");
        let child = Command::new(server_binary())
            .arg("--config")
            .arg(&config_path)
            .arg("--mode")
            .arg("coordinator")
            .stdin(Stdio::null())
            .stdout(out.try_clone().expect("clone log handle"))
            .stderr(out)
            .spawn()
            .expect("spawn sqe-server");
        Self {
            child,
            flight_port,
            log,
        }
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    /// A handshaken client, retrying until the server accepts.
    async fn client(&mut self) -> FlightSqlServiceClient<Channel> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                panic!(
                    "sqe-server exited during startup ({status}):\n{}",
                    self.log_tail()
                );
            }
            let attempt = async {
                let channel =
                    Channel::from_shared(format!("http://127.0.0.1:{}", self.flight_port))
                        .expect("valid endpoint")
                        .connect()
                        .await
                        .map_err(|e| e.to_string())?;
                let mut client = FlightSqlServiceClient::new(channel);
                let token = client
                    .handshake("drain", "drain")
                    .await
                    .map_err(|e| e.to_string())?;
                client.set_token(String::from_utf8(token.to_vec()).expect("utf-8 token"));
                Ok::<_, String>(client)
            };
            match attempt.await {
                Ok(client) => return client,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(e) => panic!(
                    "sqe-server never accepted a client: {e}\n{}",
                    self.log_tail()
                ),
            }
        }
    }

    fn sigterm(&self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -TERM failed: {status}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Run `sql` and collect the batches, as a Flight SQL client does.
async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<arrow_array::RecordBatch>, FlightError> {
    let info = client.execute(sql.to_string(), None).await?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.clone().expect("endpoint carries a ticket");
        let stream = client.do_get(ticket).await?;
        batches.extend(stream.try_collect::<Vec<_>>().await?);
    }
    Ok(batches)
}

/// The refusal reason a Flight client reads from a failed statement.
fn refusal_reason(err: &FlightError) -> Option<String> {
    match err {
        FlightError::Tonic(status) => status
            .metadata()
            .get("x-sqe-refusal-reason")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        _ => None,
    }
}

/// SIGTERM during a running query: the query finishes and returns its rows, a
/// statement sent after the signal is refused with reason `draining`, and the
/// process exits shortly after the query ends, not at `shutdown_drain_secs`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:sqe-server; run `cargo dev-server` first"]
async fn drain_process_sigterm_lets_the_running_query_finish() {
    let polaris = mount_empty_polaris().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = Server::start(dir.path(), &polaris.uri());

    // Both clients handshake BEFORE the signal, so the late statement meets
    // the drain gate and not a closed door.
    let mut runner = server.client().await;
    let mut late = server.client().await;
    // A statement the server answers while healthy, so the refusal below is
    // the drain and not a client that never worked.
    let ok = query(&mut late, "SELECT 1 + 1 AS two")
        .await
        .expect("a statement before SIGTERM runs");
    assert_eq!(ok.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let long = tokio::spawn(async move {
        let batches = query(&mut runner, LONG_QUERY).await;
        (batches, Instant::now())
    });

    // Let the statement get going on the server, then require that it is
    // still running: a query that already finished would prove nothing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !long.is_finished(),
        "LONG_QUERY ended before SIGTERM; make it heavier on this machine\n{}",
        server.log_tail()
    );

    let signalled = Instant::now();
    server.sigterm();

    // A statement after the signal is refused with `draining`. The listener
    // stays open during the drain, so this reaches the admission gate.
    let mut refusal = None;
    for _ in 0..20 {
        match query(&mut late, "SELECT 2 + 2 AS four").await {
            Err(e) => {
                refusal = Some(e);
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    let refusal = refusal.unwrap_or_else(|| {
        panic!(
            "a statement after SIGTERM was never refused\n{}",
            server.log_tail()
        )
    });
    assert_eq!(
        refusal_reason(&refusal).as_deref(),
        Some("draining"),
        "refusal after SIGTERM must carry reason `draining`, got: {refusal}"
    );
    assert!(
        !long.is_finished(),
        "the long query must still be running while the late statement is refused"
    );

    // The running query completes and returns its rows.
    let (batches, finished_at) = tokio::time::timeout(Duration::from_secs(DRAIN_SECS), long)
        .await
        .expect("the long query finishes within the drain bound")
        .expect("query task did not panic");
    let batches = batches.unwrap_or_else(|e| {
        panic!(
            "the running query must complete during the drain, got: {e}\n{}",
            server.log_tail()
        )
    });
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    drop(late);

    // The process exits shortly after the query finished, not at the deadline.
    let exit_deadline = finished_at + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = server.child.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            Instant::now() < exit_deadline,
            "sqe-server still running 15 s after the query finished\n{}",
            server.log_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let exited_after = signalled.elapsed();
    assert!(status.success(), "clean exit expected, got {status}");
    assert!(
        exited_after < Duration::from_secs(DRAIN_SECS / 2),
        "exit took {exited_after:?} after SIGTERM; the drain ran to its deadline"
    );
    println!(
        "query finished {:?} after SIGTERM; process exited {:?} after SIGTERM",
        finished_at.duration_since(signalled),
        exited_after
    );
}

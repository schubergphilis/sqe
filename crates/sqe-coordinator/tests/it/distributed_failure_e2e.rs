//! Distributed failure paths against real workers (finding 411, #591).
//!
//! Until this file, the coordinator's worker-loss, retry, straggler, cancel
//! and timeout behaviour was covered only by unit tests with a scripted
//! dispatcher; the one live distributed test ran a successful query. The
//! regressions in findings 402 and 403 (no retry mid-stream, a cancel that
//! did not cancel, a deadline that never fired) all sat in the gap.
//!
//! Every test here plans a real query against the Polaris + RustFS test
//! stack and dispatches to in-process workers from
//! `common::chaos_worker`: a production `WorkerFlightService` whose `do_get`
//! response can be cut, hung or slowed. Nothing is mocked on the
//! coordinator side; the registry, scheduler, retry loop, tracker and
//! streaming deadline are the production ones.
//!
//! Stack: `docker compose -f docker-compose.test.yml up -d && scripts/bootstrap-test.sh`,
//! or `make test-integration FILTER=distributed_failure_e2e`. No external
//! worker processes are needed.
//!
//! The six cases and the behaviour each pins down:
//!
//! 1. A worker that refuses connections at dispatch: the query succeeds on
//!    the other worker and the registry marks the dead one unhealthy.
//! 2. A worker that dies after two frames: the query fails with a
//!    worker-named error (rows were already yielded, so no re-dispatch:
//!    finding 402) and the worker is marked unhealthy.
//! 3. A worker that hangs: the query fails at `worker_rpc_timeout_secs`,
//!    not at `stream_idle_timeout_secs`.
//! 4. `CancelQuery` on a running streamed query: the tracker records
//!    `Canceled` within a second and the worker's stream is closed.
//! 5. A streamed query still producing rows past `query.timeout_secs` is
//!    aborted with the timeout error (finding 403).
//! 6. One worker five times slower than the others: the query completes
//!    and `sqe_scheduler_stragglers_total` increments.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use futures::StreamExt;

use crate::common::chaos_worker::{spawn_chaos_worker, ChaosHandle, ChaosMode};

const ROWS_PER_FILE: i64 = 40_000;

/// The coordinator side for one test: root session, handler wired to
/// `worker_urls`, and the tracker and registry the assertions read.
struct Rig {
    session: sqe_core::Session,
    handler: Arc<sqe_coordinator::QueryHandler>,
    registry: Arc<sqe_coordinator::worker_registry::WorkerRegistry>,
    tracker: Arc<sqe_coordinator::query_tracker::QueryTracker>,
    metrics: Arc<sqe_metrics::MetricsRegistry>,
    config: sqe_core::SqeConfig,
}

/// Build the coordinator. `tune` edits the test config before the handler is
/// created: this is where a case sets the timeout it wants to see fire.
///
/// `distribution_threshold` and `distribution_file_threshold` are zeroed so a
/// small test table is distributed at all; production keeps small scans local.
async fn rig(worker_urls: &[String], tune: impl FnOnce(&mut sqe_core::SqeConfig)) -> Rig {
    crate::common::init_tracing();
    let mut config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    config.query.distribution_threshold = "0".to_string();
    config.query.distribution_file_threshold = 0;
    tune(&mut config);

    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");

    // Same construction as `sqe_server`: the pooled channels carry the
    // configured worker timeouts. `WorkerRegistry::new` would use the pool's
    // 30 s default and a case that sets `worker_rpc_timeout_secs` would be
    // measuring the wrong knob.
    let pool = sqe_coordinator::channel_pool::ChannelPool::shared_with_timeouts(
        Duration::from_secs(config.coordinator.worker_connect_timeout_secs),
        Duration::from_secs(config.coordinator.worker_rpc_timeout_secs),
        &config.coordinator.transport,
    );
    let registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::with_channel_pool(
            worker_urls.to_vec(),
            pool,
        ),
    );
    for url in worker_urls {
        registry.mark_healthy(url).await;
    }
    let tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().expect("metrics registry"));
    let handler = sqe_coordinator::QueryHandler::new(
        Arc::new(sqe_policy::PassthroughEnforcer),
        None,
        config.clone(),
        Some(Arc::clone(&registry)),
        None,
        Some(Arc::clone(&metrics)),
        None,
        Arc::clone(&tracker),
        None,
        None,
        None,
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("Failed to create QueryHandler");
    Rig {
        session,
        handler: Arc::new(handler),
        registry,
        tracker,
        metrics,
        config,
    }
}

/// A fresh table with `files` data files of `ROWS_PER_FILE` rows each. One
/// INSERT per file, so the scan has one fragment per file to spread over the
/// workers. Created through a no-worker handler so the setup itself never
/// touches the chaos path.
async fn seed_table(name: &str, files: usize) -> String {
    let (session, handler) = crate::common::setup_handler().await;
    let table = format!("default.{name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {table} (id BIGINT, v BIGINT)"),
            None,
        )
        .await
        .expect("CREATE TABLE");
    for f in 0..files as i64 {
        let lo = f * ROWS_PER_FILE + 1;
        let hi = (f + 1) * ROWS_PER_FILE;
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {table} SELECT value AS id, value * 2 AS v \
                     FROM generate_series({lo}, {hi}) AS t(value)"
                ),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("INSERT file {f}: {e}"));
    }
    table
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn sum_col0(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("BIGINT column")
                .iter()
                .flatten()
                .sum::<i64>()
        })
        .sum()
}

/// A loopback port nothing listens on: bind, read the port, drop the listener.
fn dead_worker_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

// ── 1. refused connection at dispatch ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn refused_connection_is_retried_elsewhere_and_the_worker_is_evicted() {
    let table = seed_table("chaos_refused", 3).await;
    let live: ChaosHandle = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let dead = dead_worker_url();
    // One fragment per file, so the scheduler has three to balance over two
    // workers and the dead one is certain to be handed at least one.
    let rig = rig(&[dead.clone(), live.url.clone()], |c| {
        // Files here are 220 to 280 KB: one fits a 300 KB bin, two do not,
        // and a file smaller than the bin is never split into byte-range
        // morsels. Exactly one fragment per file.
        c.query.target_task_size = "300KB".to_string();
    })
    .await;

    let batches = rig
        .handler
        .execute(&rig.session, &format!("SELECT id FROM {table}"), None)
        .await
        .expect("query must succeed on the surviving worker");
    assert_eq!(total_rows(&batches), 3 * ROWS_PER_FILE as usize);

    assert!(live.do_gets() >= 1, "the live worker must have served the scan");
    let healthy = rig.registry.healthy_workers().await;
    assert!(
        !healthy.contains(&dead),
        "the worker that refused the connection must be marked unhealthy, healthy = {healthy:?}"
    );
    assert!(healthy.contains(&live.url), "the live worker stays healthy");
}

// ── 2. worker dies mid-stream ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn worker_lost_after_two_frames_fails_the_query_and_is_evicted() {
    let table = seed_table("chaos_midstream", 2).await;
    // Frame 0 is the schema, frame 1 the first batch: rows have reached the
    // coordinator when the stream is cut, so a re-dispatch would duplicate
    // them (finding 402) and the documented behaviour is to fail.
    let flaky = spawn_chaos_worker(ChaosMode::FailAfterFrames(2)).await;
    let rig = rig(std::slice::from_ref(&flaky.url), |_| {}).await;

    let err = rig
        .handler
        .execute(&rig.session, &format!("SELECT id, v FROM {table}"), None)
        .await
        .expect_err("a worker lost mid-stream must fail the query, not return a prefix");
    let text = err.to_string();
    assert!(
        text.contains(&flaky.url) || text.contains("Worker") || text.contains("worker"),
        "error must name the worker: {text}"
    );
    assert!(flaky.frames_sent() >= 2, "frames sent: {}", flaky.frames_sent());
    let healthy = rig.registry.healthy_workers().await;
    assert!(
        healthy.is_empty(),
        "the worker that died mid-stream must be marked unhealthy, healthy = {healthy:?}"
    );
}

// ── 3. hung worker ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn hung_worker_fails_at_the_rpc_timeout_not_the_idle_timeout() {
    let table = seed_table("chaos_hang", 1).await;
    let hung = spawn_chaos_worker(ChaosMode::Hang).await;
    let rig = rig(std::slice::from_ref(&hung.url), |c| {
        c.coordinator.worker_rpc_timeout_secs = 3;
        c.coordinator.worker_connect_timeout_secs = 3;
        // Both far away, so only the rpc timeout can be what fired.
        c.query.stream_idle_timeout_secs = 120;
        c.query.timeout_secs = 120;
    })
    .await;

    let started = Instant::now();
    let err = rig
        .handler
        .execute(&rig.session, &format!("SELECT id FROM {table}"), None)
        .await
        .expect_err("a hung worker must fail the query");
    let elapsed = started.elapsed();
    // Two layers can fire, and both are the rpc timeout: the pooled channel's
    // gRPC deadline ("Timeout expired", Cancelled) or the coordinator's own
    // `tokio::time::timeout` wrap ("do_get exceeded Ns rpc timeout").
    let text = err.to_string().to_lowercase();
    assert!(
        text.contains("timeout") || text.contains("timed out"),
        "error must be the rpc timeout: {text}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "must fail near worker_rpc_timeout_secs (3s), took {elapsed:?}"
    );
    assert!(hung.do_gets() >= 1, "the coordinator must have reached the worker");
}

// ── 4. CancelQuery over Flight SQL ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cancel_query_stops_the_stream_and_records_canceled() {
    use arrow_flight::sql::server::FlightSqlService;
    use arrow_flight::sql::{ActionCancelQueryRequest, Any, CommandStatementQuery, ProstMessageExt};
    use arrow_flight::{Action, FlightDescriptor};
    use prost::Message;

    let table = seed_table("chaos_cancel", 3).await;
    // 700 ms per frame: the query is still streaming when the cancel lands.
    let slow = spawn_chaos_worker(ChaosMode::DelayPerFrame(Duration::from_millis(700))).await;
    let rig = rig(std::slice::from_ref(&slow.url), |c| {
        c.query.timeout_secs = 120;
        c.query.stream_idle_timeout_secs = 120;
    })
    .await;

    let authenticator = Arc::new(
        sqe_auth::Authenticator::new(&rig.config.auth)
            .await
            .expect("authenticator"),
    );
    let session_manager = Arc::new(sqe_coordinator::session_manager::SessionManager::new(
        authenticator,
    ));
    let session = session_manager
        .authenticate("root", "")
        .await
        .expect("Flight SQL session");
    let token = session.id.clone();
    let service = Arc::new(sqe_coordinator::flight_sql::SqeFlightSqlService::new(
        session_manager,
        Arc::clone(&rig.handler),
        rig.config.clone(),
    ));
    let authed = |body: Action| {
        let mut req = tonic::Request::new(body);
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("header"),
        );
        req
    };
    let mut descriptor_req = tonic::Request::new(FlightDescriptor::new_cmd(vec![]));
    descriptor_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("header"),
    );

    let info = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: format!("SELECT id, v FROM {table}"),
                transaction_id: None,
            },
            descriptor_req,
        )
        .await
        .expect("get_flight_info_statement")
        .into_inner();
    let ticket = info.endpoint[0].ticket.as_ref().expect("ticket").clone();
    let any = Any::decode(&*ticket.ticket).expect("ticket is an Any");
    let fetch = <sqe_coordinator::flight_sql::FetchResults as Message>::decode(&*any.value)
        .expect("FetchResults");
    let query_id = uuid::Uuid::parse_str(&fetch.handle).expect("ticket handle is a query id");

    // Start streaming in the background; the stream is slow by construction.
    let consumer = {
        let service = Arc::clone(&service);
        let token = token.clone();
        tokio::spawn(async move {
            let mut req = tonic::Request::new(ticket);
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}").parse().expect("header"),
            );
            let response = service.do_get_fallback(req, fetch.as_any()).await;
            let mut stream = match response {
                Ok(r) => r.into_inner(),
                Err(status) => return Err(status),
            };
            let mut frames = 0usize;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(_) => frames += 1,
                    Err(status) => return Err(status),
                }
            }
            Ok(frames)
        })
    };

    // Let it get going: the first batch takes ~700 ms to arrive.
    let started = Instant::now();
    while slow.do_gets() == 0 && started.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(slow.do_gets() >= 1, "the query never reached the worker");
    tokio::time::sleep(Duration::from_millis(900)).await;

    let cancel_at = Instant::now();
    let result = service
        .do_action_cancel_query(
            ActionCancelQueryRequest {
                info: info.encode_to_vec().into(),
            },
            authed(Action::new("CancelQuery", vec![])),
        )
        .await
        .expect("do_action_cancel_query");
    assert_eq!(result.result, 1, "CancelResult must be CANCELLED (1)");

    // Tracker: Canceled within a second.
    let mut state = None;
    while cancel_at.elapsed() < Duration::from_secs(1) {
        state = rig.tracker.get(&query_id).map(|r| r.state.clone());
        if state == Some(sqe_coordinator::query_tracker::QueryState::Canceled) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        state,
        Some(sqe_coordinator::query_tracker::QueryState::Canceled),
        "tracker must record Canceled within 1 s of CancelQuery"
    );

    // The consumer's stream ends (with an error or EOF), and the worker sees
    // its stream closed: the cancel reached the fragment, not just the row.
    let consumed = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .expect("consumer must finish after cancel")
        .expect("consumer task");
    assert!(
        consumed.is_err() || consumed.as_ref().is_ok_and(|frames| *frames < 3 * 8),
        "the stream must not run to completion after cancel: {consumed:?}"
    );
    assert!(
        slow.wait_streams_closed(Duration::from_secs(5)).await,
        "worker still has {} open stream(s) after cancel",
        slow.active_streams()
    );
}

// ── 5. streamed query past query.timeout_secs ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn streaming_query_past_the_deadline_is_aborted_with_the_timeout_error() {
    let table = seed_table("chaos_deadline", 3).await;
    // Frames keep flowing every 700 ms, so an idle timer alone would never
    // fire; only the absolute deadline can end this (finding 403).
    let slow = spawn_chaos_worker(ChaosMode::DelayPerFrame(Duration::from_millis(700))).await;
    let rig = rig(std::slice::from_ref(&slow.url), |c| {
        c.query.timeout_secs = 2;
        c.query.stream_idle_timeout_secs = 60;
        c.coordinator.worker_rpc_timeout_secs = 60;
    })
    .await;

    let started = Instant::now();
    let (_schema, mut stream) = rig
        .handler
        .execute_stream(&rig.session, &format!("SELECT id, v FROM {table}"), None)
        .await
        .expect("execute_stream starts");
    let mut rows = 0usize;
    let mut error = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => rows += batch.num_rows(),
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }
    let elapsed = started.elapsed();
    // What a Flight SQL client does once the stream errors: let go of it. The
    // worker-side assertion below is about the coordinator releasing the
    // fragment when its consumer does, not about a consumer that holds on.
    drop(stream);
    let err = error.expect("the stream must end with the timeout error, not a clean EOF");
    let text = err.to_string();
    assert!(
        text.contains("timed out") && text.contains("timeout"),
        "must be the query deadline error: {text}"
    );
    assert!(
        rows < 3 * ROWS_PER_FILE as usize,
        "the deadline must cut the result short, got all {rows} rows"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "deadline of 2 s must fire promptly, took {elapsed:?}"
    );
    assert!(
        slow.wait_streams_closed(Duration::from_secs(5)).await,
        "worker still has {} open stream(s) after the deadline",
        slow.active_streams()
    );
}

// ── 6. straggler ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn a_slow_worker_is_counted_as_a_straggler_and_the_query_completes() {
    // Six fragments over three workers: the slow one gets about two, so the
    // median fragment time stays fast and the slow ones exceed 3x median.
    let table = seed_table("chaos_straggler", 6).await;
    let fast_a = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let fast_b = spawn_chaos_worker(ChaosMode::PassThrough).await;
    // A fast fragment here is 150 to 350 ms; six frames at 400 ms each puts
    // the slow one well past 3x the median without approaching any timeout.
    let slow = spawn_chaos_worker(ChaosMode::DelayPerFrame(Duration::from_millis(400))).await;
    let rig = rig(
        &[fast_a.url.clone(), fast_b.url.clone(), slow.url.clone()],
        |c| {
            c.query.timeout_secs = 120;
            c.query.stream_idle_timeout_secs = 120;
            c.coordinator.worker_rpc_timeout_secs = 60;
            // The planner bin-packs files up to `target_task_size` per
            // fragment (default 128 MB), which would fold these six small
            // files into one fragment on one worker. Force one file per bin so
            // there is something to spread and something to straggle.
            // Files here are 220 to 280 KB: one fits a 300 KB bin, two do not,
        // and a file smaller than the bin is never split into byte-range
        // morsels. Exactly one fragment per file.
        c.query.target_task_size = "300KB".to_string();
        },
    )
    .await;

    let before = rig.metrics.scheduler_stragglers.get();
    let batches = rig
        .handler
        .execute(&rig.session, &format!("SELECT id FROM {table}"), None)
        .await
        .expect("a straggler slows the query, it does not fail it");
    assert_eq!(total_rows(&batches), 6 * ROWS_PER_FILE as usize);
    let n = 6 * ROWS_PER_FILE;
    assert_eq!(sum_col0(&batches), n * (n + 1) / 2, "every row exactly once");

    assert!(
        slow.do_gets() >= 1,
        "the slow worker must have received at least one fragment (fast_a={}, fast_b={}, slow={})",
        fast_a.do_gets(),
        fast_b.do_gets(),
        slow.do_gets()
    );
    // The straggler summary runs when the last fragment reports; give it a
    // moment after the result is collected.
    let deadline = Instant::now() + Duration::from_secs(5);
    while rig.metrics.scheduler_stragglers.get() == before && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        rig.metrics.scheduler_stragglers.get() > before,
        "sqe_scheduler_stragglers_total must increment for the slow worker's fragment(s)"
    );
}

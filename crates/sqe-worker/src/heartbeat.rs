use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use sqe_planner::{CoordinatorHello, WorkerHello};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, error, warn};

use crate::status::WorkerStatus;

/// Metadata header name shared with the coordinator's heartbeat handler.
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Starts a background task that sends periodic heartbeat signals to the coordinator.
///
/// The heartbeat is an Arrow Flight `do_action("heartbeat")` whose body is a
/// JSON [`WorkerHello`] (finding 609): the worker's routable Flight URL, its
/// per-process `instance_id`, build, the newest `ScanTask` version it
/// implements, the load vector from `status`, and the worker-attributable
/// failures since the previous heartbeat. When `worker_secret` is non-empty it
/// is attached as the `x-sqe-worker-secret` metadata header so the coordinator
/// can authenticate the heartbeat.
///
/// The reply is parsed as a [`CoordinatorHello`]. `accepted == false` is
/// logged at `error!` on every tick with the coordinator's reason; the worker
/// keeps serving tickets, so a coordinator that flaps does not take every
/// worker with it, but `status.coordinator_accepted()` reports the state. A
/// pre-hello reply (bare `ok` or the 601 `{"status":"ok",...}` object) is
/// logged once at `warn!` and otherwise accepted. The reply's
/// `ticket_public_key` is pinned into `ticket_pin` either way (finding 601).
///
/// The `tonic::Channel` is built once at task start and reused across ticks.
/// On connection-level failure it is dropped and rebuilt on the next tick;
/// HTTP/2 multiplexing keeps the connection alive across heartbeats.
///
/// `client_tls` is applied whenever `coordinator_url` uses the `https` scheme.
/// Without it tonic refuses the URI outright ("Connecting to HTTPS without TLS
/// enabled") and a TLS coordinator could never be heartbeated at all, which is
/// half of finding 603. Build it once with `sqe_core::tls::tonic_client` and
/// hand it in; the SNI domain comes from the coordinator URL's own host.
///
/// On failure the error is logged and the next heartbeat is attempted after the
/// normal interval. There is no exponential back-off because the coordinator's
/// worker registry already tolerates a configurable number of consecutive misses
/// before marking a worker unhealthy.
///
/// Returns the task handle so a host (or a test) can stop heartbeating and
/// let the worker age out of the registry. Dropping the handle does NOT stop
/// the task.
pub fn start_heartbeat_task(
    coordinator_url: String,
    worker_url: String,
    interval: Duration,
    worker_secret: String,
    client_tls: Option<ClientTlsConfig>,
    ticket_pin: Option<Arc<crate::ticket_pin::TicketKeyPin>>,
    status: Arc<WorkerStatus>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick completes immediately; consume it so the first real
        // heartbeat fires after one full interval, giving the coordinator time
        // to start.
        ticker.tick().await;

        let mut cached: Option<Channel> = None;
        let legacy_warned = AtomicBool::new(false);

        loop {
            ticker.tick().await;
            let failures = status.failures_since_last();
            let hello = WorkerHello::current(
                worker_url.clone(),
                status.instance_id(),
                crate::status::build_identifier(),
                datafusion::DATAFUSION_VERSION,
            )
            .with_capacity(status.capacity())
            .with_failures_since_last(failures);
            match send_heartbeat(
                &coordinator_url,
                &hello,
                &worker_secret,
                client_tls.as_ref(),
                ticket_pin.as_deref(),
                &mut cached,
            )
            .await
            {
                Ok(reply) => {
                    // The coordinator saw this count; anything recorded since
                    // stays pending for the next tick.
                    status.acknowledge_failures(failures);
                    handle_reply(&coordinator_url, reply.as_ref(), &status, &legacy_warned);
                }
                Err(e) => {
                    cached = None;
                    match e.downcast_ref::<tonic::Status>() {
                        // The coordinator answered and refused us: version
                        // skew, bad secret. Retrying does not fix it; say so
                        // loudly on every tick.
                        Some(refusal)
                            if matches!(
                                refusal.code(),
                                tonic::Code::FailedPrecondition
                                    | tonic::Code::PermissionDenied
                                    | tonic::Code::Unauthenticated
                                    | tonic::Code::InvalidArgument
                            ) =>
                        {
                            status.set_coordinator_accepted(false);
                            error!(
                                coordinator = %coordinator_url,
                                code = ?refusal.code(),
                                reason = %refusal.message(),
                                "Coordinator refused this worker's heartbeat; \
                                 this worker is NOT in the cluster"
                            );
                        }
                        _ => warn!(
                            coordinator = %coordinator_url,
                            error = %e,
                            "Heartbeat to coordinator failed, will retry next interval"
                        ),
                    }
                }
            }
        }
    })
}

/// Log and record what the coordinator said. Split out so the policy is
/// testable without a network.
fn handle_reply(
    coordinator_url: &str,
    reply: Option<&CoordinatorHello>,
    status: &WorkerStatus,
    legacy_warned: &AtomicBool,
) {
    match reply {
        None => {
            // The RPC succeeded but the body was not a hello in any
            // generation. Treat as accepted (the old code did) but say so.
            status.set_coordinator_accepted(true);
            warn!(
                coordinator = %coordinator_url,
                "Heartbeat reply was not a CoordinatorHello; assuming an older coordinator"
            );
        }
        Some(hello) if !hello.accepted => {
            status.set_coordinator_accepted(false);
            error!(
                coordinator = %coordinator_url,
                cluster_id = %hello.cluster_id,
                coordinator_build = %hello.build,
                reason = hello.reason.as_deref().unwrap_or("no reason given"),
                "Coordinator did not admit this worker to the registry"
            );
        }
        Some(hello) if hello.is_legacy() => {
            status.set_coordinator_accepted(true);
            if !legacy_warned.swap(true, Ordering::AcqRel) {
                warn!(
                    coordinator = %coordinator_url,
                    "Coordinator answered with a pre-hello reply (no cluster id, build or \
                     admission decision); upgrade the coordinator to complete the handshake"
                );
            }
            debug!(coordinator = %coordinator_url, "Heartbeat sent (legacy reply)");
        }
        Some(hello) => {
            status.set_coordinator_accepted(true);
            debug!(
                coordinator = %coordinator_url,
                cluster_id = %hello.cluster_id,
                coordinator_build = %hello.build,
                "Heartbeat sent"
            );
        }
    }
}

/// One heartbeat round trip. `Ok(Some(hello))` when the coordinator replied
/// with a body any [`CoordinatorHello::parse`] generation understands,
/// `Ok(None)` when the RPC succeeded with an unparseable body.
async fn send_heartbeat(
    coordinator_url: &str,
    hello: &WorkerHello,
    worker_secret: &str,
    client_tls: Option<&ClientTlsConfig>,
    ticket_pin: Option<&crate::ticket_pin::TicketKeyPin>,
    cached: &mut Option<Channel>,
) -> Result<Option<CoordinatorHello>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = match cached.as_ref() {
        Some(ch) => ch.clone(),
        None => {
            let mut endpoint = Endpoint::new(coordinator_url.to_string())?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10));
            if sqe_core::tls::is_https_url(coordinator_url) {
                let mut tls = client_tls.cloned().unwrap_or_default();
                if let Some(domain) = sqe_core::tls::sni_domain(coordinator_url) {
                    tls = tls.domain_name(domain);
                }
                endpoint = endpoint.tls_config(tls)?;
            }
            let ch = endpoint.connect().await?;
            *cached = Some(ch.clone());
            ch
        }
    };
    let mut client = FlightServiceClient::new(channel);

    // Findings 408 and 609: the body is a JSON `WorkerHello`. It is a strict
    // superset of the #655 `HeartbeatPayload` (`url` aliases `worker_url`),
    // so a coordinator still decoding the older type reads the URL and both
    // protocol versions from it. Coordinators predating the handshake never
    // read the body as JSON; they see it as a bare URL and reject it, which is
    // the loud failure a mixed rollout should produce.
    let action = Action {
        r#type: "heartbeat".to_string(),
        body: bytes::Bytes::from(hello.encode()),
    };

    let mut request = tonic::Request::new(action);
    if !worker_secret.is_empty() {
        let value = worker_secret.parse().map_err(|e| {
            format!("worker_secret cannot be encoded as a metadata header value: {e}")
        })?;
        request.metadata_mut().insert(WORKER_SECRET_HEADER, value);
    }

    let mut response = client.do_action(request).await?.into_inner();
    let Some(result) = futures::StreamExt::next(&mut response).await else {
        return Ok(None);
    };
    let result = result?;
    let reply = CoordinatorHello::parse(&result.body);
    // Finding 601: the coordinator returns its scan-ticket public key in the
    // response body over this secret-authenticated channel; pin it. Older
    // coordinators reply with a bare "ok", which carries no key.
    if let (Some(pin), Some(key)) = (
        ticket_pin,
        reply.as_ref().and_then(|h| h.ticket_public_key.as_deref()),
    ) {
        if let Err(e) = pin.pin_b64(key) {
            warn!(error = %e, "coordinator sent an unusable ticket public key");
        }
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello() -> WorkerHello {
        WorkerHello::current(
            "http://127.0.0.1:50052",
            uuid::Uuid::now_v7(),
            "test",
            datafusion::DATAFUSION_VERSION,
        )
    }

    #[tokio::test]
    async fn heartbeat_to_unreachable_coordinator_returns_error() {
        // Attempting to heartbeat a non-existent coordinator should return an
        // error (connection refused), not panic.
        let mut cached: Option<Channel> = None;
        let result = send_heartbeat(
            "http://127.0.0.1:19999",
            &hello(),
            "",
            None,
            None,
            &mut cached,
        )
        .await;
        assert!(result.is_err());
        assert!(cached.is_none(), "failed connect should leave cache empty");
    }

    #[tokio::test]
    async fn heartbeat_to_https_coordinator_is_not_refused_for_missing_tls() {
        // Regression for finding 603. Before the fix tonic rejected the URI
        // before dialling ("Connecting to HTTPS without TLS enabled"), so a
        // worker could never heartbeat a TLS coordinator. The failure must now
        // be a network failure instead.
        let mut cached: Option<Channel> = None;
        let err = send_heartbeat(
            "https://127.0.0.1:19999",
            &hello(),
            "",
            None,
            None,
            &mut cached,
        )
        .await
        .expect_err("nothing listens on port 19999");

        let mut chain = err.to_string();
        let mut source = err.source();
        while let Some(cause) = source {
            chain.push_str(&format!(" | {cause}"));
            source = cause.source();
        }
        assert!(
            !chain.to_lowercase().contains("without tls"),
            "https coordinator still refused for want of a ClientTlsConfig: {chain}"
        );
    }

    #[tokio::test]
    async fn heartbeat_uses_the_supplied_client_tls() {
        // Same as above but with the config the bootstrap path actually builds.
        let tls = sqe_core::tls::tonic_client(
            &sqe_core::config::TlsConfig {
                ca_file: format!(
                    "{}/../sqe-core/tests/fixtures/tls/ca.crt",
                    env!("CARGO_MANIFEST_DIR")
                ),
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let mut cached: Option<Channel> = None;
        let result = send_heartbeat(
            "https://127.0.0.1:19999",
            &hello(),
            "",
            Some(&tls),
            None,
            &mut cached,
        )
        .await;
        assert!(result.is_err(), "nothing listens on port 19999");
        assert!(cached.is_none(), "failed connect should leave cache empty");
    }

    #[tokio::test]
    async fn start_heartbeat_task_does_not_panic() {
        // The background task should be spawnable even when the coordinator
        // is unreachable, it simply logs warnings and retries.
        let handle = start_heartbeat_task(
            "http://127.0.0.1:19999".to_string(),
            "http://127.0.0.1:50052".to_string(),
            Duration::from_millis(50),
            String::new(),
            None,
            None,
            WorkerStatus::new(),
        );
        // Give it a moment to run a couple of iterations.
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
    }

    #[tokio::test]
    async fn start_heartbeat_task_with_secret_does_not_panic() {
        let handle = start_heartbeat_task(
            "http://127.0.0.1:19999".to_string(),
            "http://127.0.0.1:50052".to_string(),
            Duration::from_millis(50),
            "shared-secret".to_string(),
            None,
            None,
            WorkerStatus::new(),
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
    }

    #[test]
    fn reply_policy_records_admission_and_warns_legacy_once() {
        let status = WorkerStatus::new();
        let legacy_warned = AtomicBool::new(false);

        let rejected = CoordinatorHello::rejected("c", "b", "single-node mode");
        handle_reply("http://c", Some(&rejected), &status, &legacy_warned);
        assert!(!status.coordinator_accepted());
        assert!(!legacy_warned.load(Ordering::Acquire));

        let accepted = CoordinatorHello::accepted("c", "b");
        handle_reply("http://c", Some(&accepted), &status, &legacy_warned);
        assert!(status.coordinator_accepted());
        assert!(!legacy_warned.load(Ordering::Acquire));

        let legacy = CoordinatorHello::parse(b"ok").unwrap();
        assert!(legacy.is_legacy());
        handle_reply("http://c", Some(&legacy), &status, &legacy_warned);
        assert!(status.coordinator_accepted());
        assert!(
            legacy_warned.load(Ordering::Acquire),
            "first legacy reply warns"
        );

        // An unparseable body is tolerated (old coordinator), not a rejection.
        status.set_coordinator_accepted(false);
        handle_reply("http://c", None, &status, &legacy_warned);
        assert!(status.coordinator_accepted());
    }
}

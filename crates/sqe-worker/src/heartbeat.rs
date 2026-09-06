use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, warn};

/// Metadata header name shared with the coordinator's heartbeat handler.
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Starts a background task that sends periodic heartbeat signals to the coordinator.
///
/// The heartbeat is an Arrow Flight `do_action("heartbeat")` call where the body
/// contains the worker's own Flight service URL so the coordinator can identify
/// which worker sent the heartbeat. When `worker_secret` is non-empty it is
/// attached as the `x-sqe-worker-secret` metadata header so the coordinator
/// can authenticate the heartbeat.
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
pub fn start_heartbeat_task(
    coordinator_url: String,
    worker_url: String,
    interval: Duration,
    worker_secret: String,
    client_tls: Option<ClientTlsConfig>,
    ticket_pin: Option<std::sync::Arc<crate::ticket_pin::TicketKeyPin>>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick completes immediately; consume it so the first real
        // heartbeat fires after one full interval, giving the coordinator time
        // to start.
        ticker.tick().await;

        let mut cached: Option<Channel> = None;

        loop {
            ticker.tick().await;
            match send_heartbeat(
                &coordinator_url,
                &worker_url,
                &worker_secret,
                client_tls.as_ref(),
                ticket_pin.as_deref(),
                &mut cached,
            )
            .await
            {
                Ok(()) => debug!(coordinator = %coordinator_url, "Heartbeat sent"),
                Err(e) => {
                    cached = None;
                    warn!(
                        coordinator = %coordinator_url,
                        error = %e,
                        "Heartbeat to coordinator failed, will retry next interval"
                    );
                }
            }
        }
    });
}

async fn send_heartbeat(
    coordinator_url: &str,
    worker_url: &str,
    worker_secret: &str,
    client_tls: Option<&ClientTlsConfig>,
    ticket_pin: Option<&crate::ticket_pin::TicketKeyPin>,
    cached: &mut Option<Channel>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    // Finding 408: the body is a JSON `HeartbeatPayload` carrying
    // `sqe.proto.version` and this build's DataFusion version, so a
    // coordinator/worker skew is refused at registration instead of failing
    // at `Expr` decode time mid-query. Coordinators predating the handshake
    // never read the body as JSON; they see it as a bare URL and reject it,
    // which is the loud failure a mixed rollout should produce.
    let payload =
        sqe_planner::HeartbeatPayload::current(worker_url, datafusion::DATAFUSION_VERSION);
    let action = Action {
        r#type: "heartbeat".to_string(),
        body: bytes::Bytes::from(payload.to_json()),
    };

    let mut request = tonic::Request::new(action);
    if !worker_secret.is_empty() {
        let value = worker_secret.parse().map_err(|e| {
            format!("worker_secret cannot be encoded as a metadata header value: {e}")
        })?;
        request.metadata_mut().insert(WORKER_SECRET_HEADER, value);
    }

    let mut response = client.do_action(request).await?.into_inner();
    // Finding 601: the coordinator returns its scan-ticket public key in the
    // response body over this secret-authenticated channel; pin it. Older
    // coordinators reply with a bare "ok", which carries no key.
    if let Some(pin) = ticket_pin {
        if let Some(Ok(result)) = futures::StreamExt::next(&mut response).await {
            if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&result.body) {
                if let Some(key) = body.get("ticket_public_key").and_then(|k| k.as_str()) {
                    if let Err(e) = pin.pin_b64(key) {
                        warn!(error = %e, "coordinator sent an unusable ticket public key");
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn heartbeat_to_unreachable_coordinator_returns_error() {
        // Attempting to heartbeat a non-existent coordinator should return an
        // error (connection refused), not panic.
        let mut cached: Option<Channel> = None;
        let result = send_heartbeat(
            "http://127.0.0.1:19999",
            "http://127.0.0.1:50052",
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
            "https://worker-1.sqe.test:50052",
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
            "https://worker-1.sqe.test:50052",
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
        start_heartbeat_task(
            "http://127.0.0.1:19999".to_string(),
            "http://127.0.0.1:50052".to_string(),
            Duration::from_millis(50),
            String::new(),
            None,
            None,
        );
        // Give it a moment to run a couple of iterations.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn start_heartbeat_task_with_secret_does_not_panic() {
        start_heartbeat_task(
            "http://127.0.0.1:19999".to_string(),
            "http://127.0.0.1:50052".to_string(),
            Duration::from_millis(50),
            "shared-secret".to_string(),
            None,
            None,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

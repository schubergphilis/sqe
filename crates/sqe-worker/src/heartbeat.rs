use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use tonic::transport::Endpoint;
use tracing::{debug, warn};

/// Starts a background task that sends periodic heartbeat signals to the coordinator.
///
/// The heartbeat is an Arrow Flight `do_action("heartbeat")` call where the body
/// contains the worker's own Flight service URL so the coordinator can identify
/// which worker sent the heartbeat.
///
/// On failure the error is logged and the next heartbeat is attempted after the
/// normal interval — there is no exponential back-off because the coordinator's
/// worker registry already tolerates a configurable number of consecutive misses
/// before marking a worker unhealthy.
pub fn start_heartbeat_task(coordinator_url: String, worker_url: String, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick completes immediately; consume it so the first real
        // heartbeat fires after one full interval, giving the coordinator time
        // to start.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            if let Err(e) = send_heartbeat(&coordinator_url, &worker_url).await {
                warn!(
                    coordinator = %coordinator_url,
                    error = %e,
                    "Heartbeat to coordinator failed, will retry next interval"
                );
            } else {
                debug!(coordinator = %coordinator_url, "Heartbeat sent");
            }
        }
    });
}

async fn send_heartbeat(
    coordinator_url: &str,
    worker_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = Endpoint::new(coordinator_url.to_string())?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let action = Action {
        r#type: "heartbeat".to_string(),
        body: bytes::Bytes::from(worker_url.to_string()),
    };

    let _response = client.do_action(tonic::Request::new(action)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn heartbeat_to_unreachable_coordinator_returns_error() {
        // Attempting to heartbeat a non-existent coordinator should return an
        // error (connection refused), not panic.
        let result =
            send_heartbeat("http://127.0.0.1:19999", "http://127.0.0.1:50052").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn start_heartbeat_task_does_not_panic() {
        // The background task should be spawnable even when the coordinator
        // is unreachable — it simply logs warnings and retries.
        start_heartbeat_task(
            "http://127.0.0.1:19999".to_string(),
            "http://127.0.0.1:50052".to_string(),
            Duration::from_millis(50),
        );
        // Give it a moment to run a couple of iterations.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

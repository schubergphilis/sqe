//! Credential refresh push — monitors vended S3 credentials during distributed
//! query execution and pushes refreshed credentials to workers before expiry.
//!
//! When the coordinator dispatches scan fragments with short-lived STS
//! credentials, those credentials may expire before a long-running scan
//! completes.  This module:
//!
//! 1. Tracks which workers are executing which fragments.
//! 2. Monitors credential expiry times.
//! 3. Refreshes credentials from Polaris before they expire.
//! 4. Pushes the new credentials to the appropriate workers via
//!    Arrow Flight `do_action("refresh_credentials")`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Refreshed S3 credential payload pushed from coordinator to worker.
///
/// This mirrors `sqe_worker::credential_channel::RefreshableCredentials` but is
/// defined here to avoid a circular dependency.  Both sides serialize/deserialize
/// the same JSON schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshableCredentials {
    /// The fragment this credential update applies to.
    pub fragment_id: String,
    /// S3 access key ID.
    pub access_key_id: String,
    /// S3 secret access key.
    pub secret_access_key: String,
    /// S3 session token (STS).
    pub session_token: String,
    /// When these credentials expire (RFC 3339).
    pub expiry: DateTime<Utc>,
}

/// Tracks an active fragment dispatched to a worker.
#[derive(Debug, Clone)]
pub struct ActiveFragment {
    pub fragment_id: String,
    pub worker_url: String,
    pub credential_expiry: Option<DateTime<Utc>>,
}

/// Default buffer before credential expiry at which a refresh is triggered.
///
/// If credentials expire in less than this duration, they are considered
/// "approaching expiry" and a refresh is initiated.
const REFRESH_BUFFER_SECS: i64 = 300; // 5 minutes

/// Tracks active fragments and their credential state.
///
/// The coordinator registers fragments when they are dispatched and removes
/// them when they complete.  A background task (or explicit call) checks
/// for approaching expiry and triggers refresh pushes.
#[derive(Debug, Clone)]
pub struct CredentialRefreshTracker {
    /// Map from fragment_id to active fragment info.
    fragments: Arc<RwLock<HashMap<String, ActiveFragment>>>,
    /// How many seconds before expiry to trigger a refresh.
    refresh_buffer_secs: i64,
}

impl CredentialRefreshTracker {
    pub fn new() -> Self {
        Self {
            fragments: Arc::new(RwLock::new(HashMap::new())),
            refresh_buffer_secs: REFRESH_BUFFER_SECS,
        }
    }

    /// Create a tracker with a custom refresh buffer (useful for testing).
    pub fn with_refresh_buffer(refresh_buffer_secs: i64) -> Self {
        Self {
            fragments: Arc::new(RwLock::new(HashMap::new())),
            refresh_buffer_secs,
        }
    }

    /// Register a fragment that has been dispatched to a worker.
    pub async fn register(
        &self,
        fragment_id: String,
        worker_url: String,
        credential_expiry: Option<DateTime<Utc>>,
    ) {
        let mut fragments = self.fragments.write().await;
        debug!(
            fragment_id = %fragment_id,
            worker_url = %worker_url,
            credential_expiry = ?credential_expiry,
            "Registered active fragment for credential tracking"
        );
        fragments.insert(
            fragment_id.clone(),
            ActiveFragment {
                fragment_id,
                worker_url,
                credential_expiry,
            },
        );
    }

    /// Remove a fragment (scan completed or failed).
    pub async fn unregister(&self, fragment_id: &str) {
        let mut fragments = self.fragments.write().await;
        if fragments.remove(fragment_id).is_some() {
            debug!(fragment_id = %fragment_id, "Unregistered fragment from credential tracking");
        }
    }

    /// Return fragments whose credentials are approaching expiry.
    ///
    /// A fragment is "approaching expiry" if its credential expiry is within
    /// `refresh_buffer_secs` of the current time.
    pub async fn fragments_needing_refresh(&self) -> Vec<ActiveFragment> {
        let now = Utc::now();
        let buffer = chrono::Duration::seconds(self.refresh_buffer_secs);
        let threshold = now + buffer;

        let fragments = self.fragments.read().await;
        fragments
            .values()
            .filter(|f| {
                if let Some(expiry) = f.credential_expiry {
                    expiry <= threshold
                } else {
                    // No expiry known — cannot determine if refresh is needed
                    false
                }
            })
            .cloned()
            .collect()
    }

    /// Update the credential expiry for a fragment after a successful refresh.
    pub async fn update_expiry(&self, fragment_id: &str, new_expiry: DateTime<Utc>) {
        let mut fragments = self.fragments.write().await;
        if let Some(fragment) = fragments.get_mut(fragment_id) {
            fragment.credential_expiry = Some(new_expiry);
            debug!(
                fragment_id = %fragment_id,
                new_expiry = %new_expiry,
                "Updated credential expiry after refresh"
            );
        }
    }

    /// Return the number of currently tracked fragments.
    pub async fn active_count(&self) -> usize {
        let fragments = self.fragments.read().await;
        fragments.len()
    }
}

impl Default for CredentialRefreshTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Push refreshed credentials to a single worker via Arrow Flight `do_action`.
///
/// Returns `Ok(())` if the worker accepted the credentials, or an error if the
/// Flight call failed.
pub async fn push_credentials_to_worker(
    worker_url: &str,
    credentials: &RefreshableCredentials,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::to_vec(credentials)?;

    let channel = tonic::transport::Endpoint::new(worker_url.to_string())?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let action = Action {
        r#type: "refresh_credentials".to_string(),
        body: bytes::Bytes::from(body),
    };

    let response = client
        .do_action(tonic::Request::new(action))
        .await?;

    // Consume the response stream to ensure the action completed
    let mut stream = response.into_inner();
    while let Some(_result) = stream.message().await? {
        // Response body is informational (e.g. "accepted" or "no_active_scan")
    }

    info!(
        worker = %worker_url,
        fragment_id = %credentials.fragment_id,
        "Successfully pushed refreshed credentials to worker"
    );

    Ok(())
}

/// Push refreshed credentials to all workers that have fragments needing refresh.
///
/// `get_fresh_credentials` is a callback that obtains new credentials for a given
/// fragment.  This allows the caller to plug in their own credential vending logic
/// (e.g. re-loading the table from Polaris).
///
/// Returns the number of successful pushes.
pub async fn refresh_expiring_credentials<F, Fut>(
    tracker: &CredentialRefreshTracker,
    get_fresh_credentials: F,
) -> usize
where
    F: Fn(ActiveFragment) -> Fut,
    Fut: std::future::Future<Output = Option<RefreshableCredentials>>,
{
    let needing_refresh = tracker.fragments_needing_refresh().await;

    if needing_refresh.is_empty() {
        return 0;
    }

    info!(
        count = needing_refresh.len(),
        "Found fragments with credentials approaching expiry"
    );

    let mut success_count = 0;

    for fragment in needing_refresh {
        let fragment_id = fragment.fragment_id.clone();
        let worker_url = fragment.worker_url.clone();

        match get_fresh_credentials(fragment).await {
            Some(creds) => {
                let new_expiry = creds.expiry;
                match push_credentials_to_worker(&worker_url, &creds).await {
                    Ok(()) => {
                        tracker.update_expiry(&fragment_id, new_expiry).await;
                        success_count += 1;
                    }
                    Err(e) => {
                        warn!(
                            fragment_id = %fragment_id,
                            worker = %worker_url,
                            error = %e,
                            "Failed to push refreshed credentials to worker"
                        );
                    }
                }
            }
            None => {
                warn!(
                    fragment_id = %fragment_id,
                    "Failed to obtain fresh credentials for fragment"
                );
            }
        }
    }

    success_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let tracker = CredentialRefreshTracker::new();
        assert_eq!(tracker.active_count().await, 0);

        tracker
            .register(
                "frag-001".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::hours(1)),
            )
            .await;
        assert_eq!(tracker.active_count().await, 1);

        tracker.unregister("frag-001").await;
        assert_eq!(tracker.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_unregister_unknown_fragment_is_noop() {
        let tracker = CredentialRefreshTracker::new();
        tracker.unregister("nonexistent").await;
        assert_eq!(tracker.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_fragments_needing_refresh_expired() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        // Credential expires in 2 minutes — within the 5-minute buffer
        tracker
            .register(
                "frag-soon".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::seconds(120)),
            )
            .await;

        let needing = tracker.fragments_needing_refresh().await;
        assert_eq!(needing.len(), 1);
        assert_eq!(needing[0].fragment_id, "frag-soon");
    }

    #[tokio::test]
    async fn test_fragments_needing_refresh_not_yet() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        // Credential expires in 1 hour — well outside the 5-minute buffer
        tracker
            .register(
                "frag-safe".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::hours(1)),
            )
            .await;

        let needing = tracker.fragments_needing_refresh().await;
        assert!(needing.is_empty());
    }

    #[tokio::test]
    async fn test_fragments_without_expiry_not_refreshed() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        tracker
            .register(
                "frag-no-expiry".to_string(),
                "http://w1:50052".to_string(),
                None,
            )
            .await;

        let needing = tracker.fragments_needing_refresh().await;
        assert!(needing.is_empty());
    }

    #[tokio::test]
    async fn test_update_expiry() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        // Register with expiry in 2 minutes (needs refresh)
        tracker
            .register(
                "frag-001".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::seconds(120)),
            )
            .await;

        assert_eq!(tracker.fragments_needing_refresh().await.len(), 1);

        // Update expiry to 1 hour from now (no longer needs refresh)
        tracker
            .update_expiry("frag-001", Utc::now() + Duration::hours(1))
            .await;

        assert!(tracker.fragments_needing_refresh().await.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_fragments_mixed_expiry() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        // Fragment 1: expires soon
        tracker
            .register(
                "frag-soon".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::seconds(60)),
            )
            .await;

        // Fragment 2: expires later
        tracker
            .register(
                "frag-later".to_string(),
                "http://w2:50052".to_string(),
                Some(Utc::now() + Duration::hours(1)),
            )
            .await;

        // Fragment 3: already expired
        tracker
            .register(
                "frag-expired".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() - Duration::seconds(60)),
            )
            .await;

        let needing = tracker.fragments_needing_refresh().await;
        assert_eq!(needing.len(), 2);

        let ids: Vec<&str> = needing.iter().map(|f| f.fragment_id.as_str()).collect();
        assert!(ids.contains(&"frag-soon"));
        assert!(ids.contains(&"frag-expired"));
        assert!(!ids.contains(&"frag-later"));
    }

    #[test]
    fn test_refreshable_credentials_json_roundtrip() {
        let creds = RefreshableCredentials {
            fragment_id: "frag-001".to_string(),
            access_key_id: "AKID_NEW".to_string(),
            secret_access_key: "SECRET_NEW".to_string(),
            session_token: "TOKEN_NEW".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        };

        let json = serde_json::to_vec(&creds).unwrap();
        let decoded: RefreshableCredentials = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.fragment_id, "frag-001");
        assert_eq!(decoded.access_key_id, "AKID_NEW");
        assert_eq!(decoded.secret_access_key, "SECRET_NEW");
        assert_eq!(decoded.session_token, "TOKEN_NEW");
    }

    #[tokio::test]
    async fn test_push_to_unreachable_worker_returns_error() {
        let creds = RefreshableCredentials {
            fragment_id: "frag-001".to_string(),
            access_key_id: "AKID".to_string(),
            secret_access_key: "SECRET".to_string(),
            session_token: "TOKEN".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        };

        let result =
            push_credentials_to_worker("http://127.0.0.1:19999", &creds).await;
        assert!(result.is_err(), "push to unreachable worker should fail");
    }

    #[tokio::test]
    async fn test_refresh_expiring_credentials_with_no_fragments() {
        let tracker = CredentialRefreshTracker::new();
        let count = refresh_expiring_credentials(&tracker, |_| async { None }).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_refresh_expiring_credentials_callback_returns_none() {
        let tracker = CredentialRefreshTracker::with_refresh_buffer(300);

        tracker
            .register(
                "frag-001".to_string(),
                "http://w1:50052".to_string(),
                Some(Utc::now() + Duration::seconds(60)),
            )
            .await;

        // Callback cannot obtain fresh credentials
        let count = refresh_expiring_credentials(&tracker, |_| async { None }).await;
        assert_eq!(count, 0);
    }
}

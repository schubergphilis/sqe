//! Credential update channel for receiving refreshed S3 credentials from the coordinator.
//!
//! Uses a `tokio::sync::watch` channel per fragment so that a long-running scan
//! can pick up new credentials between file reads without blocking.
//!
//! The coordinator pushes refreshed credentials via Arrow Flight `do_action("refresh_credentials")`.
//! The worker's Flight service deserializes the payload and sends it into the
//! appropriate watch channel.  The executor polls the receiver before each file
//! read and transparently switches to the new credentials.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, RwLock};
use tracing::{debug, info, warn};

/// Refreshed S3 credential payload sent from coordinator to worker.
///
/// Serialized as JSON in the Arrow Flight action body.
#[derive(Clone, Serialize, Deserialize)]
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

impl std::fmt::Debug for RefreshableCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableCredentials")
            .field("fragment_id", &self.fragment_id)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field("session_token", &"[REDACTED]")
            .field("expiry", &self.expiry)
            .finish()
    }
}

/// Manages per-fragment credential watch channels.
///
/// The Flight service inserts updates via [`publish`], and executors subscribe
/// via [`subscribe`] before starting a scan.
#[derive(Debug, Clone)]
pub struct CredentialStore {
    inner: Arc<RwLock<HashMap<String, watch::Sender<Option<RefreshableCredentials>>>>>,
}

impl CredentialStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a fragment and return a receiver that the executor can watch.
    ///
    /// If the fragment already has a channel (e.g. from a previous registration),
    /// a new receiver is created from the existing sender.
    pub async fn subscribe(&self, fragment_id: &str) -> watch::Receiver<Option<RefreshableCredentials>> {
        let mut map = self.inner.write().await;
        if let Some(sender) = map.get(fragment_id) {
            debug!(fragment_id = %fragment_id, "Reusing existing credential channel");
            sender.subscribe()
        } else {
            let (tx, rx) = watch::channel(None);
            map.insert(fragment_id.to_string(), tx);
            debug!(fragment_id = %fragment_id, "Created new credential channel");
            rx
        }
    }

    /// Publish refreshed credentials for a fragment.
    ///
    /// Returns `true` if the fragment had an active channel, `false` if the
    /// fragment was unknown (credentials are dropped).
    pub async fn publish(&self, creds: RefreshableCredentials) -> bool {
        let map = self.inner.read().await;
        if let Some(sender) = map.get(&creds.fragment_id) {
            let fragment_id = creds.fragment_id.clone();
            match sender.send(Some(creds)) {
                Ok(()) => {
                    info!(
                        fragment_id = %fragment_id,
                        "Published refreshed credentials to executor"
                    );
                    true
                }
                Err(_) => {
                    warn!(
                        fragment_id = %fragment_id,
                        "Credential channel closed — executor may have finished"
                    );
                    false
                }
            }
        } else {
            warn!(
                fragment_id = %creds.fragment_id,
                "No credential channel for fragment — scan may not be active"
            );
            false
        }
    }

    /// Remove the channel for a completed fragment to free resources.
    pub async fn remove(&self, fragment_id: &str) {
        let mut map = self.inner.write().await;
        if map.remove(fragment_id).is_some() {
            debug!(fragment_id = %fragment_id, "Removed credential channel");
        }
    }
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_creds(fragment_id: &str) -> RefreshableCredentials {
        RefreshableCredentials {
            fragment_id: fragment_id.to_string(),
            access_key_id: "AKID_NEW".to_string(),
            secret_access_key: "SECRET_NEW".to_string(),
            session_token: "TOKEN_NEW".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        }
    }

    #[tokio::test]
    async fn test_subscribe_creates_channel_with_none() {
        let store = CredentialStore::new();
        let rx = store.subscribe("frag-001").await;
        assert!(rx.borrow().is_none(), "initial value should be None");
    }

    #[tokio::test]
    async fn test_publish_and_receive() {
        let store = CredentialStore::new();
        let mut rx = store.subscribe("frag-001").await;

        let creds = make_creds("frag-001");
        let published = store.publish(creds.clone()).await;
        assert!(published, "publish should succeed");

        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.access_key_id, "AKID_NEW");
        assert_eq!(received.secret_access_key, "SECRET_NEW");
        assert_eq!(received.session_token, "TOKEN_NEW");
    }

    #[tokio::test]
    async fn test_publish_unknown_fragment_returns_false() {
        let store = CredentialStore::new();
        let creds = make_creds("unknown-fragment");
        let published = store.publish(creds).await;
        assert!(!published);
    }

    #[tokio::test]
    async fn test_remove_cleans_up() {
        let store = CredentialStore::new();
        let _rx = store.subscribe("frag-001").await;
        store.remove("frag-001").await;

        // Publishing after removal should fail
        let creds = make_creds("frag-001");
        let published = store.publish(creds).await;
        assert!(!published);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let store = CredentialStore::new();
        let mut rx1 = store.subscribe("frag-001").await;
        let mut rx2 = store.subscribe("frag-001").await;

        let creds = make_creds("frag-001");
        store.publish(creds).await;

        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();

        assert_eq!(
            rx1.borrow().as_ref().unwrap().access_key_id,
            "AKID_NEW"
        );
        assert_eq!(
            rx2.borrow().as_ref().unwrap().access_key_id,
            "AKID_NEW"
        );
    }

    #[tokio::test]
    async fn test_credential_json_roundtrip() {
        let creds = make_creds("frag-001");
        let json = serde_json::to_vec(&creds).unwrap();
        let decoded: RefreshableCredentials = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.fragment_id, "frag-001");
        assert_eq!(decoded.access_key_id, "AKID_NEW");
        assert_eq!(decoded.secret_access_key, "SECRET_NEW");
        assert_eq!(decoded.session_token, "TOKEN_NEW");
    }

    #[tokio::test]
    async fn test_multiple_updates_receiver_gets_latest() {
        let store = CredentialStore::new();
        let mut rx = store.subscribe("frag-001").await;

        // Publish twice rapidly
        let creds1 = RefreshableCredentials {
            fragment_id: "frag-001".to_string(),
            access_key_id: "AKID_V1".to_string(),
            secret_access_key: "SECRET_V1".to_string(),
            session_token: "TOKEN_V1".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        };
        let creds2 = RefreshableCredentials {
            fragment_id: "frag-001".to_string(),
            access_key_id: "AKID_V2".to_string(),
            secret_access_key: "SECRET_V2".to_string(),
            session_token: "TOKEN_V2".to_string(),
            expiry: Utc::now() + Duration::hours(2),
        };

        store.publish(creds1).await;
        store.publish(creds2).await;

        // watch channel always delivers the latest value
        rx.changed().await.unwrap();
        let received = rx.borrow().clone().unwrap();
        assert_eq!(received.access_key_id, "AKID_V2");
    }
}

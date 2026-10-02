//! HTTP POST sink for an OpenLineage collector.
//!
//! Collector authentication modes:
//! - `AuthMode::None` -- no Authorization header (Marquez default).
//! - `AuthMode::Bearer(token)` -- static API key.
//!
//! User session tokens are never accepted by the lineage observer.
//!
//! Retries 5xx and timeouts up to `retry_attempts` times with exponential backoff
//! (250ms x 2^n). 4xx responses are not retried.

use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum AuthMode {
    None,
    Bearer(sqe_core::SecretString),
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub endpoint: String,
    pub auth: AuthMode,
    pub timeout_ms: u64,
    pub retry_attempts: u32,
}

pub struct HttpSink {
    client: reqwest::Client,
    cfg: HttpConfig,
}

impl HttpSink {
    pub fn new(cfg: HttpConfig) -> Result<Self, SinkError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .build()
            .map_err(|e| SinkError::Http(e.to_string()))?;
        Ok(Self { client, cfg })
    }
}

#[async_trait::async_trait]
impl Sink for HttpSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let body = serde_json::to_vec(ev)?;
        let mut attempt = 0u32;
        loop {
            let mut req = self
                .client
                .post(&self.cfg.endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone());

            req = match &self.cfg.auth {
                AuthMode::None => req,
                AuthMode::Bearer(t) => req.bearer_auth(t.expose()),
            };

            match req.send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) if r.status().is_server_error() && attempt < self.cfg.retry_attempts => {
                    let backoff = 250u64 << attempt;
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                    continue;
                }
                Ok(r) => return Err(SinkError::Http(format!("status {}", r.status()))),
                Err(_e) if attempt < self.cfg.retry_attempts => {
                    let backoff = 250u64 << attempt;
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(SinkError::Http(e.to_string())),
            }
        }
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_credential_is_redacted_from_auth_and_config_debug() {
        let secret = "collector-credential-canary";
        let auth = AuthMode::Bearer(secret.into());
        let cfg = HttpConfig {
            endpoint: "https://collector.example/lineage".into(),
            auth: auth.clone(),
            timeout_ms: 5000,
            retry_attempts: 1,
        };
        for rendered in [format!("{auth:?}"), format!("{cfg:?}")] {
            assert!(
                !rendered.contains(secret),
                "credential leaked in debug output"
            );
        }
    }
}

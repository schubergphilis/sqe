//! Flight SQL benchmark client over the shared
//! `sqe_trino_protocol::flight::FlightSqlClient` (review 705).
//!
//! Creates a fresh gRPC connection per query to avoid HTTP/2 stream
//! accumulation issues on long-running benchmark sessions.
//!
//! No default schema: the coordinator's Flight path never populates
//! `Session::default_schema` and treats `USE` as a logged no-op
//! (`query_handler.rs`, `StatementKind::Use`), so `default_schema()` reports
//! `None` and `query::QualifyTables::Auto` qualifies table names for this
//! client. When the coordinator honours a Flight session schema, return it
//! here and the rewrite switches itself off.

use std::time::Duration;

use arrow_array::RecordBatch;
use sqe_trino_protocol::flight::{ChannelOptions, FlightSqlClient};

pub struct FlightSqlBenchClient {
    host: String,
    token: Option<String>,
}

impl FlightSqlBenchClient {
    /// Connect to a Flight SQL server and obtain auth credentials.
    pub async fn connect(
        host: &str,
        username: Option<&str>,
        password: Option<&str>,
        token_endpoint: Option<&str>,
        client_id: Option<&str>,
        client_secret: Option<&str>,
    ) -> anyhow::Result<Self> {
        let token = if let (Some(endpoint), Some(cid), Some(secret)) =
            (token_endpoint, client_id, client_secret)
        {
            Some(fetch_client_credentials_token(endpoint, cid, secret).await?)
        } else if let (Some(user), Some(pass)) = (username, password) {
            let client =
                FlightSqlClient::connect_with_handshake(host, user, pass, &channel_options())
                    .await?;
            client.token().map(str::to_string)
        } else {
            None
        };

        Ok(Self {
            host: host.to_string(),
            token,
        })
    }

    /// Connect using a pre-obtained bearer token, sent as the Flight
    /// Authorization on every request. Used by the `run` verb, whose golden
    /// bearer both authenticates the session (via the coordinator's
    /// bearer_passthrough provider) and is forwarded to the golden Polaris.
    pub fn with_token(host: &str, token: &str) -> Self {
        Self {
            host: host.to_string(),
            token: Some(token.to_string()),
        }
    }

    /// Open a fresh connection carrying the stored token.
    async fn new_client(&self) -> anyhow::Result<FlightSqlClient> {
        let opts = channel_options();
        Ok(match &self.token {
            Some(token) => FlightSqlClient::connect_with_token(&self.host, token, &opts).await?,
            None => FlightSqlClient::connect(&self.host, &opts).await?,
        })
    }
}

/// Bench channel tuning. Per-request timeout: long CTAS loads (e.g. TPC-H
/// SF100 lineitem = 28 GiB / 600M rows) can take well over the old 5-minute
/// hard-coded value. Override via `BENCH_CLIENT_TIMEOUT_SECS` when running at
/// large scale factors; the default of 1800 s (30 min) covers SF100 on a
/// modest box without artificially capping SF1000 runs either.
fn channel_options() -> ChannelOptions {
    let timeout_secs: u64 = std::env::var("BENCH_CLIENT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1800);
    ChannelOptions {
        timeout: Some(Duration::from_secs(timeout_secs)),
        connect_timeout: Some(Duration::from_secs(10)),
        http2_keep_alive_interval: Some(Duration::from_secs(10)),
        keep_alive_timeout: Some(Duration::from_secs(20)),
        keep_alive_while_idle: true,
    }
}

/// Fetch an access token via OAuth2 client_credentials grant.
async fn fetch_client_credentials_token(
    endpoint: &str,
    client_id: &str,
    client_secret: &str,
) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(endpoint)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ])
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Token request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Token endpoint returned {status}: {body}");
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse token response: {e}"))?;

    body["access_token"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| anyhow::anyhow!("No access_token in token response"))
}

#[async_trait::async_trait]
impl super::BenchClient for FlightSqlBenchClient {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        // Fresh connection per query — avoids HTTP/2 stream accumulation
        let mut client = self.new_client().await?;

        let debug = std::env::var("BENCH_DEBUG").is_ok();
        if debug {
            eprintln!("[flight] execute + do_get...");
        }
        let batches = client.execute_collect(sql).await?;
        if debug {
            eprintln!("[flight] got {} batches", batches.len());
        }
        Ok(batches)
    }

    async fn execute_update(&self, sql: &str) -> anyhow::Result<()> {
        let _ = self.execute(sql).await?;
        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "flight"
    }
}

use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::Channel;

/// Flight SQL benchmark client.
///
/// Creates a fresh gRPC connection per query to avoid HTTP/2 stream
/// accumulation issues on long-running benchmark sessions.
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
            let channel = build_channel(host).await?;
            let mut client = FlightSqlServiceClient::new(channel);
            let handshake_token = client
                .handshake(user, pass)
                .await
                .map_err(|e| anyhow::anyhow!("Authentication failed: {e}"))?;
            Some(
                String::from_utf8(handshake_token.to_vec())
                    .map_err(|e| anyhow::anyhow!("Token is not valid UTF-8: {e}"))?,
            )
        } else {
            None
        };

        Ok(Self {
            host: host.to_string(),
            token,
        })
    }

    /// Create a fresh FlightSqlServiceClient with the stored token.
    async fn new_client(&self) -> anyhow::Result<FlightSqlServiceClient<Channel>> {
        let channel = build_channel(&self.host).await?;
        let mut client = FlightSqlServiceClient::new(channel);
        if let Some(ref token) = self.token {
            client.set_token(token.clone());
        }
        Ok(client)
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

async fn build_channel(host: &str) -> anyhow::Result<Channel> {
    use std::time::Duration;

    let url = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    };
    // Per-request timeout. Long CTAS loads (e.g. TPC-H SF100 lineitem =
    // 28 GiB / 600M rows) can take well over the old 5-minute hard-coded
    // value. Override via `BENCH_CLIENT_TIMEOUT_SECS` when running at
    // large scale factors; the default of 1800 s (30 min) covers SF100
    // on a modest box without artificially capping SF1000 runs either.
    let timeout_secs: u64 = std::env::var("BENCH_CLIENT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1800);

    let channel = Channel::from_shared(url.clone())
        .map_err(|e| anyhow::anyhow!("Invalid endpoint URI '{url}': {e}"))?
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to '{url}': {e}"))?;

    Ok(channel)
}

#[async_trait::async_trait]
impl super::BenchClient for FlightSqlBenchClient {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        // Fresh connection per query — avoids HTTP/2 stream accumulation
        let mut client = self.new_client().await?;

        let debug = std::env::var("BENCH_DEBUG").is_ok();
        if debug { eprintln!("[flight] get_flight_info..."); }
        let flight_info = client
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?;
        if debug { eprintln!("[flight] got {} endpoints", flight_info.endpoint.len()); }

        let mut batches = Vec::new();

        for (i, endpoint) in flight_info.endpoint.iter().enumerate() {
            let ticket = endpoint
                .ticket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Flight endpoint returned no ticket"))?;

            if debug { eprintln!("[flight] do_get endpoint {i}..."); }
            let stream = client
                .do_get(ticket)
                .await
                .map_err(|e| anyhow::anyhow!("do_get failed: {e}"))?;

            if debug { eprintln!("[flight] collecting batches from endpoint {i}..."); }
            let endpoint_batches: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to collect record batches: {e}"))?;

            if debug {
                eprintln!(
                    "[flight] got {} batches from endpoint {i}",
                    endpoint_batches.len()
                );
            }
            batches.extend(endpoint_batches);
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

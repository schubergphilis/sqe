use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// Flight SQL benchmark client.
///
/// Wraps `FlightSqlServiceClient` in a `Mutex` because the upstream
/// client methods take `&mut self`, while `BenchClient::execute` only
/// receives a shared `&self` reference.
pub struct FlightSqlBenchClient {
    client: Mutex<FlightSqlServiceClient<Channel>>,
}

impl FlightSqlBenchClient {
    /// Connect to a Flight SQL server.
    ///
    /// Auth modes (tried in order):
    /// 1. If `token_endpoint` + `client_id` + `client_secret` are provided,
    ///    fetch a bearer token via OAuth2 client_credentials grant.
    /// 2. If `username` + `password` are provided, do a Flight SQL handshake.
    /// 3. Otherwise, connect without auth.
    pub async fn connect(
        host: &str,
        username: Option<&str>,
        password: Option<&str>,
        token_endpoint: Option<&str>,
        client_id: Option<&str>,
        client_secret: Option<&str>,
    ) -> anyhow::Result<Self> {
        let channel = build_channel(host).await?;
        let mut inner = FlightSqlServiceClient::new(channel);

        if let (Some(endpoint), Some(cid), Some(secret)) = (token_endpoint, client_id, client_secret) {
            // OAuth2 client_credentials → bearer token
            let token = fetch_client_credentials_token(endpoint, cid, secret).await?;
            inner.set_token(token);
        } else if let (Some(user), Some(pass)) = (username, password) {
            // Flight SQL handshake (OIDC password grant)
            let token = inner
                .handshake(user, pass)
                .await
                .map_err(|e| anyhow::anyhow!("Authentication failed: {e}"))?;

            inner.set_token(
                String::from_utf8(token.to_vec())
                    .map_err(|e| anyhow::anyhow!("Token is not valid UTF-8: {e}"))?,
            );
        }

        Ok(Self {
            client: Mutex::new(inner),
        })
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

    let body: serde_json::Value = resp.json().await
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
    let channel = Channel::from_shared(url.clone())
        .map_err(|e| anyhow::anyhow!("Invalid endpoint URI '{url}': {e}"))?
        // Keep the connection alive during long queries
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        // Per-request timeout (5 minutes max per query)
        .timeout(Duration::from_secs(300))
        // Connection timeout
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to '{url}': {e}"))?;

    Ok(channel)
}

#[async_trait::async_trait]
impl super::BenchClient for FlightSqlBenchClient {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        // Acquire the lock, call execute, then release before looping over endpoints.
        let flight_info = {
            let mut guard = self.client.lock().await;
            guard
                .execute(sql.to_string(), None)
                .await
                .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        };

        let mut batches = Vec::new();

        for endpoint in flight_info.endpoint {
            let ticket = endpoint
                .ticket
                .ok_or_else(|| anyhow::anyhow!("Flight endpoint returned no ticket"))?;

            // Re-acquire the lock for each do_get call; release between iterations.
            let stream = {
                let mut guard = self.client.lock().await;
                guard
                    .do_get(ticket)
                    .await
                    .map_err(|e| anyhow::anyhow!("do_get failed: {e}"))?
            };

            let endpoint_batches: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to collect record batches: {e}"))?;

            batches.extend(endpoint_batches);
        }

        Ok(batches)
    }

    async fn execute_update(&self, sql: &str) -> anyhow::Result<()> {
        // SQE doesn't implement do_put_statement_update — route through
        // the regular execute path which handles DDL/DML via query routing.
        let _ = self.execute(sql).await?;
        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "flight"
    }
}

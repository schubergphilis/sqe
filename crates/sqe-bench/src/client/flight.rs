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
    /// Connect to a Flight SQL server, performing the handshake if
    /// username and password are provided.
    pub async fn connect(
        host: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> anyhow::Result<Self> {
        let channel = build_channel(host).await?;
        let mut inner = FlightSqlServiceClient::new(channel);

        if let (Some(user), Some(pass)) = (username, password) {
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

async fn build_channel(url: &str) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid endpoint URI '{url}': {e}"))?
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
        let mut guard = self.client.lock().await;
        guard
            .execute_update(sql.to_string(), None)
            .await
            .map_err(|e| anyhow::anyhow!("Update failed: {e}"))?;

        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "flight"
    }
}

use arrow_array::RecordBatch;

/// Stub Trino HTTP client — not yet implemented.
pub struct TrinoBenchClient {
    host: String,
    #[allow(dead_code)]
    username: Option<String>,
    #[allow(dead_code)]
    password: Option<String>,
}

impl TrinoBenchClient {
    pub fn new(host: &str, username: Option<&str>, password: Option<&str>) -> Self {
        Self {
            host: host.to_string(),
            username: username.map(str::to_string),
            password: password.map(str::to_string),
        }
    }
}

#[async_trait::async_trait]
impl super::BenchClient for TrinoBenchClient {
    async fn execute(&self, _sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        anyhow::bail!("Trino client not yet implemented (host={})", self.host)
    }

    async fn execute_update(&self, _sql: &str) -> anyhow::Result<()> {
        anyhow::bail!("Trino client not yet implemented (host={})", self.host)
    }

    fn protocol_name(&self) -> &str {
        "trino"
    }
}

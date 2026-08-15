// Client implementations are used by the `test` and `load` commands which are
// not yet wired into the CLI; suppress dead_code warnings until they are.
#![allow(dead_code)]

pub mod flight;
pub mod trino;

use arrow_array::RecordBatch;
use async_trait::async_trait;

#[async_trait]
pub trait BenchClient: Send + Sync {
    /// Execute a SQL query and return results as RecordBatches.
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>>;

    /// Execute a SQL statement that returns no rows (DDL, DML).
    async fn execute_update(&self, sql: &str) -> anyhow::Result<()>;

    /// Protocol name for reporting.
    fn protocol_name(&self) -> &str;
}

pub async fn create_client(
    protocol: &str,
    host: &str,
    username: Option<&str>,
    password: Option<&str>,
    token_endpoint: Option<&str>,
    client_id: Option<&str>,
    client_secret: Option<&str>,
) -> anyhow::Result<Box<dyn BenchClient>> {
    match protocol {
        "flight" => {
            let client = flight::FlightSqlBenchClient::connect(
                host,
                username,
                password,
                token_endpoint,
                client_id,
                client_secret,
            )
            .await?;
            Ok(Box::new(client))
        }
        "trino" => {
            let client = trino::TrinoBenchClient::new(host, username, password);
            Ok(Box::new(client))
        }
        _ => anyhow::bail!("Unknown protocol: {protocol}. Supported: flight, trino"),
    }
}

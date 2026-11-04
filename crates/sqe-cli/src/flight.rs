//! Flight SQL backend for the CLI, over the shared
//! `sqe_trino_protocol::flight::FlightSqlClient` (review 705).

use arrow_array::RecordBatch;
use sqe_trino_protocol::flight::{ChannelOptions, FlightSqlClient};

use crate::client::{QueryResult, SqlClient};

pub struct FlightClient {
    inner: FlightSqlClient,
}

impl FlightClient {
    pub async fn connect(
        url: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = FlightSqlClient::connect_with_handshake(
            url,
            username,
            password,
            &ChannelOptions::default(),
        )
        .await?;
        Ok(Self { inner })
    }

    pub async fn connect_with_token(
        url: &str,
        token: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inner =
            FlightSqlClient::connect_with_token(url, token, &ChannelOptions::default()).await?;
        Ok(Self { inner })
    }
}

#[async_trait::async_trait]
impl SqlClient for FlightClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let batches = self.inner.execute_collect(sql).await?;
        batches_to_result(&batches)
    }
}

fn batches_to_result(batches: &[RecordBatch]) -> Result<QueryResult, Box<dyn std::error::Error>> {
    match batches.first() {
        None => Ok(QueryResult::empty()),
        Some(first) => QueryResult::from_batches(&first.schema(), batches)
            .map_err(|e| format!("Cannot format the result: {e}").into()),
    }
}

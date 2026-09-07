//! Flight SQL backend for the CLI, over the shared
//! `sqe_trino_protocol::flight::FlightSqlClient` (review 705).

use arrow::util::display::{ArrayFormatter, FormatOptions};
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
    if batches.is_empty() {
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        });
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let opts = FormatOptions::default();

    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<ArrayFormatter> = (0..batch.num_columns())
            .map(|i| {
                ArrayFormatter::try_new(batch.column(i).as_ref(), &opts)
                    .map_err(|e| format!("Cannot format column {i}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters
                .iter()
                .map(|fmt| fmt.value(row).to_string())
                .collect();
            rows.push(cells);
        }
    }

    Ok(QueryResult { columns, rows })
}

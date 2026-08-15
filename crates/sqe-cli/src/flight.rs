use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Channel, ClientTlsConfig};

use crate::client::{QueryResult, SqlClient};

pub struct FlightClient {
    inner: FlightSqlServiceClient<Channel>,
}

impl FlightClient {
    pub async fn connect(
        url: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = build_channel(url).await?;
        let mut inner = FlightSqlServiceClient::new(channel);

        let token = inner
            .handshake(username, password)
            .await
            .map_err(|e| format!("Authentication failed: {e}"))?;

        inner.set_token(String::from_utf8(token.to_vec())?);

        Ok(Self { inner })
    }

    pub async fn connect_with_token(
        url: &str,
        token: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = build_channel(url).await?;
        let mut inner = FlightSqlServiceClient::new(channel);
        inner.set_token(token.to_string());
        Ok(Self { inner })
    }
}

async fn build_channel(url: &str) -> Result<Channel, Box<dyn std::error::Error>> {
    let mut endpoint = Channel::from_shared(url.to_string())?;
    if url.starts_with("https://") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("Failed to connect to {url}: {e}"))?;
    Ok(channel)
}

#[async_trait::async_trait]
impl SqlClient for FlightClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let info = self
            .inner
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| format!("Query failed: {e}"))?;

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for endpoint in info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let stream = self
                    .inner
                    .do_get(ticket)
                    .await
                    .map_err(|e| format!("Failed to fetch results: {e}"))?;

                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                all_batches.extend(batches);
            }
        }

        batches_to_result(&all_batches)
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

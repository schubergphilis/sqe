use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::Channel;

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
        let channel = Channel::from_shared(url.to_string())?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {url}: {e}"))?;

        let mut inner = FlightSqlServiceClient::new(channel);

        let token = inner
            .handshake(username, password)
            .await
            .map_err(|e| format!("Authentication failed: {e}"))?;

        inner.set_token(String::from_utf8(token.to_vec())?);

        Ok(Self { inner })
    }
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

        Ok(batches_to_result(&all_batches))
    }
}

fn batches_to_result(batches: &[RecordBatch]) -> QueryResult {
    if batches.is_empty() {
        return QueryResult {
            columns: vec![],
            rows: vec![],
        };
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let opts = FormatOptions::default();

    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<ArrayFormatter> = (0..batch.num_columns())
            .map(|i| ArrayFormatter::try_new(batch.column(i).as_ref(), &opts).unwrap())
            .collect();

        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters.iter().map(|fmt| fmt.value(row).to_string()).collect();
            rows.push(cells);
        }
    }

    QueryResult { columns, rows }
}

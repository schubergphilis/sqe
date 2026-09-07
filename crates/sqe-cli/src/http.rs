//! Trino-protocol (HTTP) backend for the CLI, over the shared
//! `sqe_trino_protocol::client::TrinoHttpClient`.
//!
//! The previous hand-rolled client capped pagination at 1000 rounds and
//! returned whatever it had collected as success, so `sqe-cli -e ... > out.csv`
//! silently produced a short file for results over ~1M rows (review 705).
//! The shared client paginates without a cap and treats every failure mode
//! (foreign `nextUri`, server `error`, HTTP status) as an error.

use sqe_trino_protocol::client::TrinoHttpClient;

use crate::client::{QueryResult, SqlClient};

pub struct HttpClient {
    inner: TrinoHttpClient,
}

impl HttpClient {
    pub fn with_bearer(
        base_url: &str,
        token: &str,
        accept_invalid_certs: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = TrinoHttpClient::builder(base_url)
            .bearer_auth(token)
            .source("sqe-cli")
            .accept_invalid_certs(accept_invalid_certs)
            .build()?;
        Ok(Self { inner })
    }

    pub fn new(
        base_url: &str,
        username: &str,
        password: &str,
        accept_invalid_certs: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = TrinoHttpClient::builder(base_url)
            .basic_auth(username, password)
            .source("sqe-cli")
            .accept_invalid_certs(accept_invalid_certs)
            .build()?;
        Ok(Self { inner })
    }
}

#[async_trait::async_trait]
impl SqlClient for HttpClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();

        self.inner
            .execute(sql, |page| {
                if columns.is_empty() {
                    if let Some(cols) = &page.columns {
                        columns = cols.iter().map(|c| c.name.clone()).collect();
                    }
                }
                if let Some(data) = &page.data {
                    rows.extend(
                        data.iter()
                            .map(|row| row.iter().map(json_to_display).collect::<Vec<String>>()),
                    );
                }
                Ok(())
            })
            .await?;

        Ok(QueryResult { columns, rows })
    }
}

fn json_to_display(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

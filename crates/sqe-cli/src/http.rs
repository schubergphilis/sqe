use base64::Engine;
use serde::Deserialize;
use url::Url;

use crate::client::{QueryResult, SqlClient};

pub struct HttpClient {
    base_url: String,
    auth_header: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoResponse {
    #[allow(dead_code)]
    id: String,
    #[serde(default)]
    next_uri: Option<String>,
    #[serde(default)]
    columns: Option<Vec<TrinoColumn>>,
    #[serde(default)]
    data: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(default)]
    error: Option<TrinoError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoColumn {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoError {
    message: String,
}

impl HttpClient {
    pub fn new(base_url: &str, username: &str, password: &str, accept_invalid_certs: bool) -> Self {
        let credentials = base64::engine::general_purpose::STANDARD
            .encode(format!("{username}:{password}"));
        let auth_header = format!("Basic {credentials}");

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_header,
            client,
        }
    }

    async fn fetch_response(
        &self,
        url: &str,
        body: Option<&str>,
    ) -> Result<TrinoResponse, Box<dyn std::error::Error>> {
        let req = if let Some(sql) = body {
            self.client
                .post(url)
                .header("Authorization", &self.auth_header)
                .header("Content-Type", "text/plain")
                .body(sql.to_string())
        } else {
            self.client
                .get(url)
                .header("Authorization", &self.auth_header)
        };

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            return Err(format!("HTTP {status}: {text}").into());
        }

        let parsed: TrinoResponse = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse response: {e}\nBody: {text}"))?;

        Ok(parsed)
    }
}

const MAX_PAGINATION_ROUNDS: usize = 1000;

#[async_trait::async_trait]
impl SqlClient for HttpClient {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
        let url = format!("{}/v1/statement", self.base_url);

        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();

        // Initial POST
        let mut resp = self.fetch_response(&url, Some(sql)).await?;

        for _ in 0..MAX_PAGINATION_ROUNDS {
            // Check for errors
            if let Some(err) = &resp.error {
                return Err(err.message.clone().into());
            }

            // Collect column names from first response that has them
            if columns.is_empty() {
                if let Some(cols) = &resp.columns {
                    columns = cols.iter().map(|c| c.name.clone()).collect();
                }
            }

            // Collect rows
            if let Some(data) = &resp.data {
                for row in data {
                    let cells: Vec<String> = row.iter().map(json_to_display).collect();
                    rows.push(cells);
                }
            }

            // Follow next_uri for paginated results (validate same origin to prevent SSRF)
            match resp.next_uri {
                Some(ref next) => {
                    if !same_origin(&self.base_url, next) {
                        return Err(format!(
                            "Server returned next_uri with different origin: {next}"
                        )
                        .into());
                    }
                    resp = self.fetch_response(next, None).await?;
                }
                None => break,
            }
        }

        Ok(QueryResult { columns, rows })
    }
}

fn same_origin(base: &str, next: &str) -> bool {
    match (Url::parse(base), Url::parse(next)) {
        (Ok(b), Ok(n)) => b.origin() == n.origin(),
        _ => false,
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

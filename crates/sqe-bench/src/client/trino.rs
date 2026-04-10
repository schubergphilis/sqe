use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;

/// Trino HTTP v1 statement protocol benchmark client.
pub struct TrinoBenchClient {
    client: Client,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
    catalog: Option<String>,
}

#[derive(Deserialize)]
struct TrinoResponse {
    #[allow(dead_code)]
    id: String,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    columns: Option<Vec<TrinoColumn>>,
    data: Option<Vec<Vec<serde_json::Value>>>,
    stats: TrinoStats,
    error: Option<TrinoError>,
}

#[derive(Deserialize)]
struct TrinoColumn {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Deserialize)]
struct TrinoStats {
    state: String,
}

#[derive(Deserialize)]
struct TrinoError {
    message: String,
}

impl TrinoBenchClient {
    pub fn new(host: &str, username: Option<&str>, password: Option<&str>) -> Self {
        // Normalise the base URL: strip any trailing slash so we can always
        // append `/v1/statement` unconditionally.
        let base_url = host.trim_end_matches('/').to_string();
        Self {
            client: Client::new(),
            base_url,
            username: username.map(str::to_string),
            password: password.map(str::to_string),
            catalog: None,
        }
    }

    /// Set the default Trino catalog for queries.
    pub fn with_catalog(mut self, catalog: &str) -> Self {
        self.catalog = Some(catalog.to_string());
        self
    }

    /// Apply credentials to a request builder.
    ///
    /// - Both username and password present: HTTP Basic auth.
    /// - Username only: treated as a Bearer token.
    /// - Neither: request is sent unauthenticated.
    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.username, &self.password) {
            (Some(user), Some(pass)) => req.basic_auth(user, Some(pass)),
            (Some(token), None) => req.bearer_auth(token),
            _ => req,
        }
    }

    /// Submit the SQL statement and poll `nextUri` until the query finishes,
    /// collecting all data pages along the way.  Returns the column schema
    /// (from the first response that carries one) and all accumulated rows.
    async fn run_query(
        &self,
        sql: &str,
    ) -> anyhow::Result<(Option<Vec<TrinoColumn>>, Vec<Vec<serde_json::Value>>)> {
        let submit_url = format!("{}/v1/statement", self.base_url);

        // Build the initial POST request.
        let mut req = self
            .client
            .post(&submit_url)
            .header("Content-Type", "text/plain")
            .body(sql.to_string());

        req = self.apply_auth(req);

        // Add standard Trino protocol headers.
        if let Some(user) = &self.username {
            req = req.header("X-Trino-User", user);
        }
        if let Some(catalog) = &self.catalog {
            req = req.header("X-Trino-Catalog", catalog);
        }

        // Execute the first request.
        let resp: TrinoResponse = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to submit query: {e}"))?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("HTTP error on query submit: {e}"))?
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse Trino response: {e}"))?;

        let mut columns: Option<Vec<TrinoColumn>> = resp.columns;
        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();

        if let Some(rows) = resp.data {
            all_rows.extend(rows);
        }

        if let Some(err) = resp.error {
            anyhow::bail!("Trino query error: {}", err.message);
        }

        let mut next_uri = resp.next_uri;

        // Poll until there is no more `nextUri` or the state is FINISHED.
        while let Some(uri) = next_uri {
            let req = self.apply_auth(self.client.get(&uri));

            let page: TrinoResponse = req
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to poll Trino: {e}"))?
                .error_for_status()
                .map_err(|e| anyhow::anyhow!("HTTP error while polling Trino: {e}"))?
                .json()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to parse Trino page: {e}"))?;

            // A later page may carry the column metadata when the first didn't.
            if columns.is_none() {
                columns = page.columns;
            }

            if let Some(rows) = page.data {
                all_rows.extend(rows);
            }

            if let Some(err) = page.error {
                anyhow::bail!("Trino query error: {}", err.message);
            }

            // Stop if the query finished, even if `nextUri` is still present
            // (defensive — the protocol says nextUri is absent on FINISHED,
            // but we guard both conditions).
            if page.stats.state == "FINISHED" {
                break;
            }

            next_uri = page.next_uri;
        }

        Ok((columns, all_rows))
    }
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

fn trino_type_to_arrow(type_name: &str) -> DataType {
    // Trino type names may carry precision/scale qualifiers, e.g.
    // "varchar(255)" or "decimal(18,2)".  Strip the parenthesised part
    // before matching so the arms stay simple.
    let base = type_name
        .split('(')
        .next()
        .unwrap_or(type_name)
        .trim()
        .to_lowercase();

    match base.as_str() {
        "boolean" => DataType::Boolean,
        "integer" | "int" | "tinyint" | "smallint" => DataType::Int32,
        "bigint" => DataType::Int64,
        "double" | "real" | "float" => DataType::Float64,
        "varchar" | "char" | "varbinary" | "json" | "uuid" | "interval year to month"
        | "interval day to second" => DataType::Utf8,
        "date" => DataType::Date32,
        "decimal" => DataType::Float64, // simplified — no fixed-point in arrow_array basic API
        _ => DataType::Utf8,            // safe fallback: stringify unknown types
    }
}

/// Build an Arrow `ArrayRef` for one column from the collected JSON rows.
fn build_array(
    col_idx: usize,
    data_type: &DataType,
    rows: &[Vec<serde_json::Value>],
) -> anyhow::Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let vals: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_bool()))
                .collect();
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        DataType::Int32 => {
            let vals: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|v| v.as_i64())
                        .map(|n| n as i32)
                })
                .collect();
            Ok(Arc::new(Int32Array::from(vals)))
        }
        DataType::Int64 => {
            let vals: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_i64()))
                .collect();
            Ok(Arc::new(Int64Array::from(vals)))
        }
        DataType::Float64 => {
            let vals: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_f64()))
                .collect();
            Ok(Arc::new(Float64Array::from(vals)))
        }
        DataType::Date32 => {
            // Trino returns dates as "YYYY-MM-DD" strings.  Convert to the
            // number of days since the Unix epoch (1970-01-01) which is what
            // Arrow's Date32 stores.
            let vals: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .and_then(|v| v.as_str())
                        .and_then(parse_date_to_days)
                })
                .collect();
            Ok(Arc::new(Date32Array::from(vals)))
        }
        // DataType::Utf8 and everything else: stringify the JSON value.
        _ => {
            let vals: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(col_idx).map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => String::new(),
                        other => other.to_string(),
                    })
                })
                .collect();
            let refs: Vec<Option<&str>> = vals
                .iter()
                .map(|o| o.as_deref())
                .collect();
            Ok(Arc::new(StringArray::from(refs)))
        }
    }
}

/// Parse an ISO 8601 date string ("YYYY-MM-DD") into days since Unix epoch.
fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;

    // Use chrono for the calendar arithmetic.
    use chrono::NaiveDate;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let days = date.signed_duration_since(epoch).num_days();
    i32::try_from(days).ok()
}

/// Convert accumulated Trino JSON pages into a single Arrow `RecordBatch`.
///
/// Returns an empty `Vec` when there were no columns (DDL/DML statements).
fn build_record_batches(
    columns: Option<Vec<TrinoColumn>>,
    rows: Vec<Vec<serde_json::Value>>,
) -> anyhow::Result<Vec<RecordBatch>> {
    let columns = match columns {
        Some(c) if !c.is_empty() => c,
        _ => return Ok(vec![]),
    };

    if rows.is_empty() {
        // Return an empty RecordBatch that still carries the correct schema.
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, trino_type_to_arrow(&c.type_name), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let empty_arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow_array::new_empty_array(f.data_type()))
            .collect();
        let batch = RecordBatch::try_new(schema, empty_arrays)
            .map_err(|e| anyhow::anyhow!("Failed to build empty RecordBatch: {e}"))?;
        return Ok(vec![batch]);
    }

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, trino_type_to_arrow(&c.type_name), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let dt = trino_type_to_arrow(&col.type_name);
            build_array(i, &dt, &rows)
        })
        .collect::<anyhow::Result<_>>()?;

    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| anyhow::anyhow!("Failed to build RecordBatch: {e}"))?;

    Ok(vec![batch])
}

// ---------------------------------------------------------------------------
// BenchClient implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl super::BenchClient for TrinoBenchClient {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let (columns, rows) = self.run_query(sql).await?;
        build_record_batches(columns, rows)
    }

    async fn execute_update(&self, sql: &str) -> anyhow::Result<()> {
        self.run_query(sql).await?;
        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "trino"
    }
}

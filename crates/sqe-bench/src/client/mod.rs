//! Bench clients: one trait, two transports, both over the shared
//! `sqe-trino-protocol` clients (review 705).

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

    /// The default schema this connection carries for unqualified table
    /// names, when the protocol can express one (Trino: `X-Trino-Schema`).
    /// `None` means the query text must qualify its tables itself
    /// (`query::QualifyTables::Auto` does that for such clients).
    fn default_schema(&self) -> Option<&str> {
        None
    }
}

/// Everything needed to open a bench connection.
pub struct ConnectArgs<'a> {
    /// `"flight"` or `"trino"`.
    pub protocol: &'a str,
    pub endpoint: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub token_endpoint: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub client_secret: Option<&'a str>,
    /// Default catalog (Trino protocol only: `X-Trino-Catalog`).
    pub catalog: Option<&'a str>,
    /// Default schema (Trino protocol only: `X-Trino-Schema`).
    pub schema: Option<&'a str>,
}

pub async fn create_client(args: &ConnectArgs<'_>) -> anyhow::Result<Box<dyn BenchClient>> {
    match args.protocol {
        "flight" => {
            let client = flight::FlightSqlBenchClient::connect(
                args.endpoint,
                args.username,
                args.password,
                args.token_endpoint,
                args.client_id,
                args.client_secret,
            )
            .await?;
            // The coordinator scopes `USE [catalog.]schema` on the token's
            // session (review 707), so the Flight client carries a default
            // schema the same way the Trino client carries `X-Trino-Schema`.
            let client = match args.schema {
                Some(schema) => {
                    let scope = match args.catalog {
                        Some(catalog) => format!("{catalog}.{schema}"),
                        None => schema.to_string(),
                    };
                    client.with_schema(&scope)
                }
                None => client,
            };
            Ok(Box::new(client))
        }
        "trino" => {
            let mut client =
                trino::TrinoBenchClient::new(args.endpoint, args.username, args.password)?;
            if let Some(c) = args.catalog {
                client = client.with_catalog(c);
            }
            if let Some(s) = args.schema {
                client = client.with_schema(s);
            }
            Ok(Box::new(client))
        }
        other => anyhow::bail!("Unknown protocol: {other}. Supported: flight, trino"),
    }
}

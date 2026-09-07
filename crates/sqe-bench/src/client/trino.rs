//! Trino HTTP v1 statement protocol benchmark client, over the shared
//! `sqe_trino_protocol::client::TrinoHttpClient` (review 705).
//!
//! Compared with the client it replaces: pages are checked for `error`
//! before `data`, `nextUri` must share the server's origin, the final
//! `nextUri` is always fetched (no early `FINISHED` break), and rows are
//! decoded with faithful Arrow types (`decimal(p,s)` -> `Decimal128(p,s)`,
//! integer widths, timestamps) so the compare harness diffs equal types.

use arrow_array::RecordBatch;
use sqe_trino_protocol::client::TrinoHttpClient;
use sqe_trino_protocol::decode::rows_to_record_batch;

pub struct TrinoBenchClient {
    inner: TrinoHttpClient,
}

impl TrinoBenchClient {
    /// `username` + `password` -> HTTP Basic. `username` alone -> the
    /// protocol's `X-Trino-User` header (what an unauthenticated Trino
    /// requires). Neither -> unauthenticated.
    pub fn new(host: &str, username: Option<&str>, password: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = TrinoHttpClient::builder(host).source("sqe-bench");
        match (username, password) {
            (Some(user), Some(pass)) => builder = builder.basic_auth(user, pass).user(user),
            (Some(user), None) => builder = builder.user(user),
            _ => {}
        }
        Ok(Self {
            inner: builder.build()?,
        })
    }

    /// Set the default Trino catalog for queries.
    #[must_use = "with_catalog consumes self; bind the returned client"]
    pub fn with_catalog(mut self, catalog: &str) -> Self {
        self.inner.set_catalog(Some(catalog.to_string()));
        self
    }

    /// Set the default schema for unqualified table names (`X-Trino-Schema`).
    #[must_use = "with_schema consumes self; bind the returned client"]
    pub fn with_schema(mut self, schema: &str) -> Self {
        self.inner.set_schema(Some(schema.to_string()));
        self
    }
}

#[async_trait::async_trait]
impl super::BenchClient for TrinoBenchClient {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let out = self.inner.collect(sql).await?;
        if out.columns.is_empty() {
            // DDL / DML: no result set.
            return Ok(vec![]);
        }
        let batch = rows_to_record_batch(&out.columns, &out.rows)?;
        Ok(vec![batch])
    }

    async fn execute_update(&self, sql: &str) -> anyhow::Result<()> {
        self.inner.collect(sql).await?;
        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "trino"
    }

    fn default_schema(&self) -> Option<&str> {
        self.inner.schema()
    }
}

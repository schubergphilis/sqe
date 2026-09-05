//! Thin abstraction over the SQE query path so the Quack server can call into
//! either the real `sqe-coordinator` `QueryHandler` or a test stub without
//! pulling the coordinator's heavy machinery into unit tests.
//!
//! The production wrapper around `QueryHandler` lives in a separate
//! integration crate (or in the binary that wires the coordinator and the
//! Quack server together). This trait keeps the server crate buildable on
//! its own.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use sqe_core::Session;

/// A query result the server pages out to the client, rather than a
/// fully-materialised `Vec<RecordBatch>` (finding 606).
///
/// The schema is available *before* the first batch, which matters twice: a
/// zero-row result still reports its column names and types, and the server
/// can answer `PrepareResponse` without draining the whole stream.
pub struct QueryStream {
    /// Result schema, known at plan time.
    pub schema: SchemaRef,
    /// Result batches. Boxed and `'static` so a partially-drained stream can be
    /// parked in the pending-results store between `FetchRequest`s.
    pub batches: BoxStream<'static, Result<RecordBatch, QueryError>>,
}

impl QueryStream {
    /// Wrap an already-materialised result. Convenience for test stubs and for
    /// the buffered fallback path (DML, which the coordinator's streaming API
    /// does not serve).
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: Box::pin(stream::iter(batches.into_iter().map(Ok))),
        }
    }
}

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Parse, plan, and start executing `sql` for the authenticated `session`.
    /// Returns as soon as the schema is known; batches are pulled lazily so the
    /// server can bound each response (and the total result) in bytes.
    async fn execute(&self, session: &Session, sql: &str) -> Result<QueryStream, QueryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// SQL parse failure. Surfaces as `SQE-PARSE` on the wire.
    #[error("parse error: {0}")]
    Parse(String),
    /// Policy denied the query (row filter, column mask, or visibility
    /// rejection). Surfaces as `SQE-POLICY` on the wire.
    #[error("policy denial")]
    Policy(String),
    /// Plan or execution failure (table not found, type mismatch, etc.).
    /// Surfaces as `SQE-EXEC` on the wire.
    #[error("execution error: {0}")]
    Execution(String),
    /// Unexpected internal error. Surfaces as `SQE-EXEC` with a generic
    /// message; the underlying detail stays in the warn log.
    #[error("internal: {0}")]
    Internal(String),
}

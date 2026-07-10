//! Thin abstraction over the SQE query path so the Quack server can call into
//! either the real `sqe-coordinator` `QueryHandler` or a test stub without
//! pulling the coordinator's heavy machinery into unit tests.
//!
//! The production wrapper around `QueryHandler` lives in a separate
//! integration crate (or in the binary that wires the coordinator and the
//! Quack server together). This trait keeps the server crate buildable on
//! its own.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use sqe_core::Session;

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Parse, plan, and execute `sql` for the authenticated `session`. Returns
    /// all result batches in memory; result streaming follows in a later
    /// iteration once we have a need for it (CSV-export-scale queries).
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, QueryError>;
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

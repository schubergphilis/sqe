//! Production `QueryExecutor` implementation that delegates to
//! `sqe_coordinator::QueryHandler`.
//!
//! Available only when this crate is built with the `coordinator-executor`
//! feature so the default build does not pull in DataFusion, the catalog
//! layer, or any of the other coordinator dependencies.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use sqe_coordinator::QueryHandler;
use sqe_core::{Session, SqeError};

use crate::query_executor::{QueryError, QueryExecutor};

pub struct CoordinatorExecutor {
    query_handler: Arc<QueryHandler>,
}

impl CoordinatorExecutor {
    pub fn new(query_handler: Arc<QueryHandler>) -> Self {
        Self { query_handler }
    }
}

#[async_trait]
impl QueryExecutor for CoordinatorExecutor {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
        self.query_handler
            .execute(session, sql)
            .await
            .map_err(sqe_error_to_query_error)
    }
}

/// Map a `sqe_core::SqeError` to the closest `QueryError` variant.
///
/// We intentionally do **not** grep the error string to recover sub-variants
/// (`SqeError::Catalog` could in principle be a parse failure if iceberg-rust
/// reshaped its messages — but matching English is exactly the bug class the
/// structured error variants were designed to remove). Anything that is not a
/// clearly-internal error maps to `QueryError::Execution`.
pub fn sqe_error_to_query_error(err: SqeError) -> QueryError {
    let msg = err.to_string();
    match err {
        // Mid-query auth failure (e.g., token expired during a long catalog
        // call). Surface as Execution so the client sees the same path as
        // any other engine-side failure; ConnectionRequest's SQE-AUTH wire
        // contract is reserved for handshake-time auth.
        SqeError::Auth(_) | SqeError::ExecutionAuth { .. } => QueryError::Execution(msg),

        // Catalog / iceberg / S3 failures all surface as Execution. Retries
        // are the engine's responsibility before reaching this layer.
        SqeError::Catalog(_)
        | SqeError::CatalogHttp { .. }
        | SqeError::IcebergCommitConflict(_)
        | SqeError::S3Throttled(_) => QueryError::Execution(msg),

        // The catch-all execution variant.
        SqeError::Execution(_) | SqeError::NotImplemented(_) => QueryError::Execution(msg),

        // Misconfiguration is an operator problem, not a query author problem.
        // Treat as Internal so the wire surfaces a generic message and the
        // detail stays in the warn log.
        SqeError::Config(_) => QueryError::Internal(msg),

        // Anything that bubbled out as anyhow::Error is by definition
        // unexpected; same treatment as Config.
        SqeError::Internal(_) => QueryError::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anyhow_msg() -> anyhow::Error {
        anyhow::anyhow!("plan failed to compile")
    }

    #[test]
    fn auth_failures_map_to_execution() {
        let mapped = sqe_error_to_query_error(SqeError::Auth("token expired".to_string()));
        assert!(matches!(mapped, QueryError::Execution(_)));
        let mapped = sqe_error_to_query_error(SqeError::ExecutionAuth {
            status: 401,
            body: "unauthorized".to_string(),
        });
        assert!(matches!(mapped, QueryError::Execution(_)));
    }

    #[test]
    fn catalog_failures_map_to_execution() {
        let mapped = sqe_error_to_query_error(SqeError::Catalog("namespace not found".to_string()));
        assert!(matches!(mapped, QueryError::Execution(_)));
        let mapped = sqe_error_to_query_error(SqeError::CatalogHttp {
            status: 503,
            op: sqe_core::CatalogOp::ListNamespaces,
            body: "service unavailable".to_string(),
        });
        assert!(matches!(mapped, QueryError::Execution(_)));
    }

    #[test]
    fn iceberg_conflict_maps_to_execution() {
        let mapped = sqe_error_to_query_error(SqeError::IcebergCommitConflict("retry".to_string()));
        assert!(matches!(mapped, QueryError::Execution(_)));
    }

    #[test]
    fn s3_throttle_maps_to_execution() {
        let mapped = sqe_error_to_query_error(SqeError::S3Throttled("503 slow down".to_string()));
        assert!(matches!(mapped, QueryError::Execution(_)));
    }

    #[test]
    fn plain_execution_maps_to_execution() {
        let mapped =
            sqe_error_to_query_error(SqeError::Execution("table 'foo' not found".to_string()));
        match mapped {
            QueryError::Execution(msg) => assert!(msg.contains("table 'foo' not found")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn not_implemented_maps_to_execution() {
        let mapped = sqe_error_to_query_error(SqeError::NotImplemented("PIVOT".to_string()));
        match mapped {
            QueryError::Execution(msg) => assert!(msg.contains("PIVOT")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn config_maps_to_internal() {
        let mapped = sqe_error_to_query_error(SqeError::Config("missing catalog URL".to_string()));
        assert!(matches!(mapped, QueryError::Internal(_)));
    }

    #[test]
    fn internal_anyhow_maps_to_internal() {
        let mapped = sqe_error_to_query_error(SqeError::Internal(anyhow_msg()));
        assert!(matches!(mapped, QueryError::Internal(_)));
    }
}

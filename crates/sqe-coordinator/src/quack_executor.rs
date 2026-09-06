//! `QueryExecutor` implementation that delegates to this crate's
//! `QueryHandler`. Lives here (rather than in `sqe-quack-server`) because
//! `sqe-quack-server` does not depend on `sqe-coordinator` — the trait is
//! defined there but the production implementation has to live on this
//! side of the dependency edge to avoid a cycle.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use sqe_core::{Session, SqeError};
use sqe_quack_server::{QueryError, QueryExecutor, QueryStream};

use crate::QueryHandler;

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
    /// Streaming by default (finding 606): `execute_stream` returns as soon as
    /// the schema is known, so the Quack server can page the result out under a
    /// byte cap instead of materialising it whole. `execute_stream` also emits
    /// the same `AuditEvent` as `execute`, via the stream finalizer.
    ///
    /// It only serves `SELECT`, though — DML and metadata statements come back
    /// as `NotImplemented` with an explicit instruction to fall back to
    /// `execute`, which we do so `CREATE TABLE`/`INSERT` over Quack keep
    /// working. The fallback is cheap: `execute_stream` rejects at classify
    /// time, before any planning or execution.
    async fn execute(&self, session: &Session, sql: &str) -> Result<QueryStream, QueryError> {
        match self.query_handler.execute_stream(session, sql, None).await {
            Ok((schema, stream)) => Ok(QueryStream {
                schema,
                batches: stream
                    .map(|item| {
                        item.map_err(|e: datafusion::error::DataFusionError| {
                            QueryError::Execution(e.to_string())
                        })
                    })
                    .boxed(),
            }),
            Err(SqeError::NotImplemented(_)) => {
                let batches = self
                    .query_handler
                    .execute(session, sql, None)
                    .await
                    .map_err(sqe_error_to_query_error)?;
                let schema = match batches.first() {
                    Some(b) => b.schema(),
                    None => Arc::new(arrow_schema::Schema::empty()),
                };
                Ok(QueryStream::from_batches(schema, batches))
            }
            Err(e) => Err(sqe_error_to_query_error(e)),
        }
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

        // Source-preserving boundary errors: route by code. InternalError
        // (e.g. config_src) is an operator problem -> Internal; AccessDenied is
        // a policy decision -> Policy, which is what makes the server's
        // "SQE-POLICY: access denied" masking branch live (606: it used to fall
        // through to Execution, so the full denial reason reached the client);
        // everything else (catalog/exec/storage boundaries) surfaces as
        // Execution.
        SqeError::Sourced { ref code, .. } => match *code {
            sqe_core::SqeErrorCode::InternalError => QueryError::Internal(msg),
            sqe_core::SqeErrorCode::AccessDenied => QueryError::Policy(msg),
            _ => QueryError::Execution(msg),
        },
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
            sqe_error_to_query_error(SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, "table 'foo' not found".to_string()));
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

    fn sourced(code: sqe_core::SqeErrorCode, message: &str) -> SqeError {
        SqeError::Sourced {
            code,
            message: message.to_string(),
            source: Box::new(std::io::Error::other("cause")),
        }
    }

    /// 606: the Quack server masks `QueryError::Policy` down to the generic
    /// "SQE-POLICY: access denied". That branch was dead because every
    /// `Sourced` error, `AccessDenied` included, routed to `Execution` — so the
    /// client received the full denial reason, which is the row-filter /
    /// column-mask payload the masking exists to hide.
    #[test]
    fn access_denied_maps_to_policy() {
        let mapped = sqe_error_to_query_error(sourced(
            sqe_core::SqeErrorCode::AccessDenied,
            "user 'bob' lacks SELECT on hr.salaries (row filter dept='HR')",
        ));
        match mapped {
            QueryError::Policy(msg) => assert!(
                msg.contains("hr.salaries"),
                "the detail must survive into the server log: {msg}"
            ),
            other => panic!("expected Policy, got {other:?}"),
        }
    }

    /// The neighbouring codes must not follow AccessDenied into Policy:
    /// InternalError stays Internal, everything else stays Execution.
    #[test]
    fn other_sourced_codes_keep_their_mapping() {
        let mapped =
            sqe_error_to_query_error(sourced(sqe_core::SqeErrorCode::InternalError, "bad config"));
        assert!(matches!(mapped, QueryError::Internal(_)));

        let mapped = sqe_error_to_query_error(sourced(
            sqe_core::SqeErrorCode::AuthenticationFailed,
            "token rejected",
        ));
        assert!(matches!(mapped, QueryError::Execution(_)));

        let mapped = sqe_error_to_query_error(sourced(
            sqe_core::SqeErrorCode::TableNotFound,
            "no such table",
        ));
        assert!(matches!(mapped, QueryError::Execution(_)));
    }
}

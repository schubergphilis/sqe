//! Quack RPC server. Accepts HTTP/1.1 POST /quack from DuckDB clients and
//! translates them into SQE coordinator sessions.
//!
//! See `docs/quack-protocol.md` for the wire reference.

pub mod app;
pub mod pending;
pub mod query_executor;
pub mod session;

pub use app::{router, QuackServerState, ResultLimits};
pub use pending::{PendingResult, PendingResults};
pub use query_executor::{QueryError, QueryExecutor, QueryStream};
pub use session::{Session, SessionRejection, SessionStore};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error(transparent)]
    Wire(#[from] sqe_quack_wire::WireError),
}

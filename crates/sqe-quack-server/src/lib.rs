//! Quack RPC server. Accepts HTTP/1.1 POST /quack from DuckDB clients and
//! translates them into SQE coordinator sessions.
//!
//! See `docs/quack-protocol.md` for the wire reference.

pub mod app;
pub mod session;

pub use app::{router, QuackServerState};
pub use session::{Session, SessionStore};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error(transparent)]
    Wire(#[from] sqe_quack_wire::WireError),
}

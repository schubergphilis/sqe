//! Pure-Rust port of DuckDB's BinarySerializer for the Quack RPC protocol.
//!
//! Pinned to `quack_version = 1` and DuckDB extension `v1.5-variegata`.
//! See `docs/quack-protocol.md` for the wire format reference.

pub mod varint;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("varint overflow: too many continuation bytes")]
    VarintOverflow,
}

pub type Result<T> = std::result::Result<T, WireError>;

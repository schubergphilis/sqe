//! Pure-Rust port of DuckDB's BinarySerializer for the Quack RPC protocol.
//!
//! Pinned to `quack_version = 1` and DuckDB extension `v1.5-variegata`.
//! See `docs/quack-protocol.md` for the wire format reference.

pub mod codec;
pub mod message;
pub mod varint;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("varint overflow: too many continuation bytes")]
    VarintOverflow,
    #[error("expected field_id {expected:#06x}, got {actual:#06x}")]
    UnexpectedField { expected: u16, actual: u16 },
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    #[error("unknown MessageType discriminant {0}")]
    UnknownMessageType(u8),
    #[error("message type {0:?} not yet supported by sqe-quack-wire (DataChunk-carrying messages deferred)")]
    UnsupportedMessageType(crate::message::MessageType),
}

pub type Result<T> = std::result::Result<T, WireError>;

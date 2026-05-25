//! Pure-Rust port of DuckDB's BinarySerializer for the Quack RPC protocol.
//!
//! Pinned to `quack_version = 1` and DuckDB extension `v1.5-variegata`.
//! See `docs/quack-protocol.md` for the wire format reference.

pub mod arrow_bridge;
pub mod codec;
pub mod data_chunk;
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
    #[error("unknown LogicalTypeId discriminant {0}")]
    UnknownLogicalTypeId(u8),
    #[error(
        "LogicalType {0:?} is recognised but not yet supported (likely nested or parameterised)"
    )]
    UnsupportedLogicalType(crate::data_chunk::LogicalTypeId),
    #[error("VectorType {0} (compressed format) not yet supported by sqe-quack-wire")]
    UnsupportedVectorType(u8),
    #[error("Arrow data type {0} not yet supported by sqe-quack-wire::arrow_bridge")]
    UnsupportedArrowType(String),
    #[error("DataChunkWrapper nullable byte was false (null), which the protocol does not emit in practice")]
    NullDataChunkWrapper,
}

pub type Result<T> = std::result::Result<T, WireError>;

//! Trino wire protocol: the `/v1/statement` JSON types and the shared clients.
//!
//! Review 705 found three hand-rolled Trino HTTP clients and two Flight SQL
//! clients across `sqe-cli`, `sqe-bench` and `sqe-trino-compat`, each with a
//! different bug (silent pagination truncation, `data` consumed before `error`,
//! no `nextUri` origin check, lossy `decimal` decoding). This crate is the one
//! place those semantics live:
//!
//! - [`types`] -- `TrinoResponse`, `TrinoColumn`, `TrinoError`, `TrinoStats`
//!   and the type-signature helpers. serde only; the server crate depends on
//!   this and nothing else here.
//! - [`client`] (feature `client`) -- `TrinoHttpClient`: Basic or Bearer auth,
//!   `X-Trino-Catalog` / `X-Trino-Schema`, same-origin `nextUri`, `error`
//!   checked before `data`, unbounded pagination with a per-page callback,
//!   `DELETE nextUri` when a query is dropped mid-flight.
//! - [`decode`] (feature `arrow`) -- Trino JSON pages to Arrow `RecordBatch`
//!   with faithful types.
//! - [`encode`] (feature `arrow`) -- Arrow cells to Trino JSON values, the
//!   inverse, shared by the Trino server and the MCP endpoint (review 803).
//! - [`flight`] (feature `flight`) -- `FlightSqlClient` wrapping
//!   `FlightSqlServiceClient` (channel, handshake, token, collect or stream).

pub mod types;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "arrow")]
pub mod decode;

#[cfg(feature = "arrow")]
pub mod encode;

#[cfg(feature = "flight")]
pub mod flight;

pub use types::{
    parse_trino_type, type_signature_for, ExecutionMetrics, TrinoColumn, TrinoError,
    TrinoErrorLocation, TrinoFailureInfo, TrinoResponse, TrinoStats, TrinoType, TrinoTypeArgument,
    TrinoTypeSignature, TrinoWarning, TrinoWarningCode,
};

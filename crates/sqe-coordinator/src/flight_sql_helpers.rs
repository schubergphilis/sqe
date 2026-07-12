//! Helper types and functions for the Flight SQL service.
//!
//! Extracted from `flight_sql.rs` to keep the main service file focused on
//! protocol handler logic.  All items here are `pub(crate)` unless they are
//! part of the public API surface (`FetchResults`).

use std::pin::Pin;

use arrow_flight::sql::Any;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::FlightData;
use futures::Stream;
use tonic::Status;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Boxed, pinned stream of `FlightData` items used as the body of most
/// Flight responses.
pub(crate) type FlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

// ---------------------------------------------------------------------------
// FetchResults — custom protobuf ticket payload
// ---------------------------------------------------------------------------

/// Custom protobuf message to carry query handles in tickets.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

/// Convert an `SqeError` to a gRPC `Status` with the correct status code.
///
/// Maps SQE error codes to the most semantically appropriate gRPC code,
/// attaches `x-sqe-error-code` and (optionally) `x-sqe-query-id` metadata,
/// and logs the error at WARN level.
pub(crate) fn sqe_error_to_status(
    e: &sqe_core::SqeError,
    query_id: Option<&uuid::Uuid>,
) -> tonic::Status {
    let code = e.error_code();
    let grpc_code = match code {
        sqe_core::SqeErrorCode::SyntaxError
        | sqe_core::SqeErrorCode::ParseError
        | sqe_core::SqeErrorCode::SemanticError
        | sqe_core::SqeErrorCode::TypeMismatch
        | sqe_core::SqeErrorCode::InvalidArguments
        | sqe_core::SqeErrorCode::DivisionByZero
        | sqe_core::SqeErrorCode::InvalidCast
        | sqe_core::SqeErrorCode::ColumnNotFound
        | sqe_core::SqeErrorCode::DuplicateColumn => tonic::Code::InvalidArgument,

        sqe_core::SqeErrorCode::TableNotFound
        | sqe_core::SqeErrorCode::SchemaNotFound
        | sqe_core::SqeErrorCode::CatalogNotFound
        | sqe_core::SqeErrorCode::ViewNotFound
        | sqe_core::SqeErrorCode::FunctionNotFound => tonic::Code::NotFound,

        sqe_core::SqeErrorCode::DuplicateTable => tonic::Code::AlreadyExists,
        sqe_core::SqeErrorCode::AuthenticationFailed | sqe_core::SqeErrorCode::SessionExpired => {
            tonic::Code::Unauthenticated
        }
        sqe_core::SqeErrorCode::AccessDenied => tonic::Code::PermissionDenied,
        sqe_core::SqeErrorCode::NotSupported => tonic::Code::Unimplemented,
        sqe_core::SqeErrorCode::QueryTimeout => tonic::Code::DeadlineExceeded,
        sqe_core::SqeErrorCode::QueryCancelled => tonic::Code::Cancelled,
        sqe_core::SqeErrorCode::ResourceExhausted => tonic::Code::ResourceExhausted,

        // Transient catalog/storage failures (issue #12): map to gRPC
        // codes that clients and operators recognise as retryable. Without
        // these arms, every 5xx / network reset / circuit-open from
        // Polaris arrived at the client as opaque `INTERNAL` with no
        // retry hint, sending operators hunting for coordinator bugs.
        sqe_core::SqeErrorCode::CatalogUnavailable | sqe_core::SqeErrorCode::StorageError => {
            tonic::Code::Unavailable
        }
        sqe_core::SqeErrorCode::CircuitBreakerOpen => tonic::Code::FailedPrecondition,
        sqe_core::SqeErrorCode::CommitConflict => tonic::Code::Aborted,
        // Explicit arm for `CatalogError` (previously fell through to the
        // catch-all `_ => Internal`). Operators still see Internal — but
        // it's now documented as a real catalog error, distinct from the
        // transient ones above.
        sqe_core::SqeErrorCode::CatalogError => tonic::Code::Internal,

        _ => tonic::Code::Internal,
    };

    let message = e.client_message();
    let mut status = tonic::Status::new(grpc_code, &message);

    if let Ok(val) = code.name().parse() {
        status.metadata_mut().insert("x-sqe-error-code", val);
    }
    if let Some(qid) = query_id {
        if let Ok(val) = qid.to_string().parse() {
            status.metadata_mut().insert("x-sqe-query-id", val);
        }
    }

    tracing::warn!(
        error_code = %code,
        query_id = ?query_id,
        grpc_code = ?grpc_code,
        client_message = %message,
        internal_detail = %e,
        "Flight SQL error"
    );

    status
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_flight::sql::ProstMessageExt;
    use prost::Message;

    // -----------------------------------------------------------------------
    // FetchResults: encode / decode roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn fetch_results_roundtrip_via_prost() {
        let original = FetchResults {
            handle: "SELECT 1".to_string(),
        };

        let bytes = original.encode_to_vec();
        let decoded = FetchResults::decode(bytes.as_slice()).expect("decode should succeed");

        assert_eq!(original, decoded);
        assert_eq!(decoded.handle, "SELECT 1");
    }

    #[test]
    fn fetch_results_roundtrip_empty_handle() {
        let original = FetchResults {
            handle: String::new(),
        };

        let bytes = original.encode_to_vec();
        let decoded = FetchResults::decode(bytes.as_slice()).expect("decode should succeed");

        assert_eq!(original, decoded);
        assert_eq!(decoded.handle, "");
    }

    #[test]
    fn fetch_results_roundtrip_unicode_handle() {
        let sql = "SELECT '日本語' AS lang, 42 AS n FROM tbl WHERE x > 0";
        let original = FetchResults {
            handle: sql.to_string(),
        };

        let bytes = original.encode_to_vec();
        let decoded = FetchResults::decode(bytes.as_slice()).expect("decode should succeed");

        assert_eq!(decoded.handle, sql);
    }

    // -----------------------------------------------------------------------
    // FetchResults: type_url and as_any
    // -----------------------------------------------------------------------

    #[test]
    fn fetch_results_type_url() {
        assert_eq!(
            FetchResults::type_url(),
            "type.googleapis.com/arrow.flight.protocol.sql.FetchResults"
        );
    }

    #[test]
    fn fetch_results_as_any_roundtrip() {
        let original = FetchResults {
            handle: "SELECT COUNT(*) FROM orders".to_string(),
        };

        let any = original.as_any();

        assert_eq!(any.type_url, FetchResults::type_url());

        // The Any.value bytes must decode back to the same message.
        let decoded =
            FetchResults::decode(&*any.value).expect("decode from Any.value should succeed");
        assert_eq!(decoded.handle, original.handle);
    }

    #[test]
    fn fetch_results_as_any_type_url_matches_constant() {
        let msg = FetchResults {
            handle: "x".to_string(),
        };
        let any = msg.as_any();
        // as_any() must embed the canonical type URL so that do_get_fallback
        // can match on it.
        assert_eq!(any.type_url, FetchResults::type_url());
    }
}

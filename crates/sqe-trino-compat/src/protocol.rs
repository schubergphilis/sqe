//! Trino protocol types for the server.
//!
//! The wire structs (`TrinoResponse`, `TrinoColumn`, `TrinoError`,
//! `TrinoStats`, type-signature helpers) live in the serde-only
//! `sqe-trino-protocol` crate so `sqe-cli` and `sqe-bench` can deserialize a
//! page without linking this server crate (review 705). They are re-exported
//! here unchanged. What stays in this module is server-only: the `/v1/info`
//! shape, the `SqeError`-aware `TrinoError` constructors (as an extension
//! trait, since `sqe-core` pulls DataFusion and must not reach the clients),
//! Arrow-to-JSON row conversion, and the session-state echo types.

use arrow_array::RecordBatch;
use serde::Serialize;

pub use sqe_trino_protocol::types::{
    type_signature_for, ExecutionMetrics, TrinoColumn, TrinoError, TrinoErrorLocation,
    TrinoFailureInfo, TrinoResponse, TrinoStats, TrinoTypeArgument, TrinoTypeSignature,
    TrinoWarning, TrinoWarningCode,
};

use crate::types::{arrow_to_trino_type, arrow_value_to_json};

// ── Trino /v1/info response ──────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub node_version: NodeVersion,
    pub environment: String,
    pub coordinator: bool,
    pub starting: bool,
    pub uptime: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeVersion {
    pub version: String,
}

/// `TrinoError` constructors that map SQE's own error model onto the wire.
///
/// An extension trait because `TrinoError` is defined in `sqe-trino-protocol`
/// (which has no `sqe-core` dependency by design) while these need
/// `SqeError` / `SqeErrorCode`. Import the trait and call them as before:
/// `TrinoError::from_sqe_error(..)`, `TrinoError::user_error(..)`, etc.
pub trait TrinoErrorExt: Sized {
    fn from_sqe_error(e: &sqe_core::SqeError, query_id: Option<&str>) -> Self;

    /// Build a Trino USER_ERROR for a malformed request (e.g. an `EXECUTE`
    /// naming a prepared statement the client never sent). USER_ERROR is
    /// returned as HTTP 200 with an error body, per the Trino protocol.
    fn user_error(message: impl Into<String>, query_id: Option<&str>) -> Self;

    fn internal_error(message: impl Into<String>, query_id: Option<&str>) -> Self;

    /// Build a USER_ERROR carrying one of Trino's own `StandardErrorCode`
    /// names that has no `SqeErrorCode` counterpart (`INVALID_SESSION_PROPERTY`,
    /// `NOT_FOUND`). Trino clients dispatch on `errorName`, so the protocol
    /// layer's session and prepared-statement errors must use Trino's names
    /// rather than the nearest engine code.
    fn trino_user_error(
        error_code: i32,
        error_name: &str,
        message: impl Into<String>,
        query_id: Option<&str>,
    ) -> Self;

    /// `INVALID_SESSION_PROPERTY` (Trino `StandardErrorCode` 14).
    fn invalid_session_property(message: impl Into<String>, query_id: Option<&str>) -> Self;

    /// `NOT_FOUND` (Trino `StandardErrorCode` 5): what Trino returns for
    /// `EXECUTE`, `DEALLOCATE PREPARE` and `DESCRIBE OUTPUT|INPUT` naming a
    /// prepared statement the session does not hold.
    fn prepared_statement_not_found(name: &str, query_id: Option<&str>) -> Self;

    fn from_code(
        code: sqe_core::SqeErrorCode,
        message: impl Into<String>,
        query_id: Option<&str>,
    ) -> Self;
}

impl TrinoErrorExt for TrinoError {
    fn from_sqe_error(e: &sqe_core::SqeError, query_id: Option<&str>) -> Self {
        let code = e.error_code();
        let message = e.client_message();
        let error_type = code.trino_error_type().to_string();
        Self {
            message: message.clone(),
            error_code: code.trino_error_code(),
            error_name: code.name().to_string(),
            error_type: error_type.clone(),
            query_id: query_id.map(|s| s.to_string()),
            failure_info: Some(TrinoFailureInfo {
                r#type: format!("io.trino.spi.{error_type}"),
                message,
                suppressed: Vec::new(),
                cause: None,
                stack: Vec::new(),
            }),
            error_location: None,
        }
    }

    fn user_error(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::from_code(sqe_core::SqeErrorCode::SyntaxError, message, query_id)
    }

    fn internal_error(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::from_code(sqe_core::SqeErrorCode::InternalError, message, query_id)
    }

    fn trino_user_error(
        error_code: i32,
        error_name: &str,
        message: impl Into<String>,
        query_id: Option<&str>,
    ) -> Self {
        let message = message.into();
        Self {
            message: message.clone(),
            error_code,
            error_name: error_name.to_string(),
            error_type: "USER_ERROR".to_string(),
            query_id: query_id.map(|s| s.to_string()),
            failure_info: Some(TrinoFailureInfo {
                r#type: "io.trino.spi.USER_ERROR".to_string(),
                message,
                suppressed: Vec::new(),
                cause: None,
                stack: Vec::new(),
            }),
            error_location: None,
        }
    }

    fn invalid_session_property(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::trino_user_error(14, "INVALID_SESSION_PROPERTY", message, query_id)
    }

    fn prepared_statement_not_found(name: &str, query_id: Option<&str>) -> Self {
        Self::trino_user_error(
            5,
            "NOT_FOUND",
            format!("Prepared statement not found: {name}"),
            query_id,
        )
    }

    fn from_code(
        code: sqe_core::SqeErrorCode,
        message: impl Into<String>,
        query_id: Option<&str>,
    ) -> Self {
        let message = message.into();
        Self {
            message: message.clone(),
            error_code: code.trino_error_code(),
            error_name: code.name().to_string(),
            error_type: code.trino_error_type().to_string(),
            query_id: query_id.map(|s| s.to_string()),
            failure_info: Some(TrinoFailureInfo {
                r#type: format!("io.trino.spi.{}", code.trino_error_type()),
                message,
                suppressed: Vec::new(),
                cause: None,
                stack: Vec::new(),
            }),
            error_location: None,
        }
    }
}

pub fn batches_to_trino(
    batches: &[RecordBatch],
) -> (Vec<TrinoColumn>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let columns = columns_for_schema(batches[0].schema().as_ref());
    let rows = batches_to_rows(batches);
    (columns, rows)
}

/// Trino column metadata for an Arrow schema. Split out of
/// [`batches_to_trino`] so the streaming result cursor can emit `columns`
/// from the schema before (or without) any batch arriving.
pub fn columns_for_schema(schema: &arrow_schema::Schema) -> Vec<TrinoColumn> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let trino_type = arrow_to_trino_type(f.data_type());
            let type_signature = type_signature_for(&trino_type);
            TrinoColumn {
                name: f.name().clone(),
                r#type: trino_type,
                type_signature,
            }
        })
        .collect()
}

/// Row-major JSON conversion of a slice of batches (one page's worth). CPU
/// bound; the server runs it under `spawn_blocking`.
pub fn batches_to_rows(batches: &[RecordBatch]) -> Vec<Vec<serde_json::Value>> {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut rows = Vec::with_capacity(total);
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| arrow_value_to_json(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }
    rows
}

/// Session-state mutations the server must echo back to the client.
///
/// Trino's wire protocol drives `USE` and `SET SESSION` by emitting
/// `X-Trino-Set-*` / `X-Trino-Clear-*` headers on the response. Clients
/// observe the headers and replay them on the next request to retain
/// the new session state. The headers go on the terminal response of the
/// statement that caused them only (finding 708): a `USE` that fails must
/// never switch the client's replayed schema, and a paging response never
/// repeats them.
#[derive(Debug, Clone, Default)]
pub struct UpdatedSessionState {
    pub set_catalog: Option<String>,
    pub set_schema: Option<String>,
    pub set_session: Vec<(String, String)>,
    pub clear_session: Vec<String>,
    pub added_prepare: Vec<(String, String)>,
    pub deallocated_prepare: Vec<String>,
}

impl UpdatedSessionState {
    pub fn is_empty(&self) -> bool {
        self.set_catalog.is_none()
            && self.set_schema.is_none()
            && self.set_session.is_empty()
            && self.clear_session.is_empty()
            && self.added_prepare.is_empty()
            && self.deallocated_prepare.is_empty()
    }
}

/// Which prepared-statement metadata a `DESCRIBE` requests. Trino JDBC issues
/// `DESCRIBE OUTPUT <name>` for `PreparedStatement.getMetaData()` and
/// `DESCRIBE INPUT <name>` for `getParameterMetaData()`. Both are recognised by
/// the SQL front door (`sqe_sql::StatementKind::DescribeOutput` /
/// `DescribeInput`); nothing here parses statement text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeKind {
    Output,
    Input,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_batches_to_trino_empty() {
        let (cols, rows) = batches_to_trino(&[]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn test_batches_to_trino_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let (cols, rows) = batches_to_trino(&[batch]);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].r#type, "bigint");
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].r#type, "varchar");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("alice"));
        assert_eq!(rows[1][0], serde_json::json!(2));
        assert_eq!(rows[1][1], serde_json::json!("bob"));
    }

    #[test]
    fn test_server_info_serialization() {
        let info = ServerInfo {
            node_version: NodeVersion {
                version: "0.1.0".to_string(),
            },
            environment: "production".to_string(),
            coordinator: true,
            starting: false,
            uptime: "5.00m".to_string(),
        };

        let json = serde_json::to_string(&info).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["nodeVersion"]["version"], "0.1.0");
        assert_eq!(parsed["environment"], "production");
        assert_eq!(parsed["coordinator"], true);
        assert_eq!(parsed["starting"], false);
        assert_eq!(parsed["uptime"], "5.00m");
    }

    #[test]
    fn test_server_info_starting_state() {
        let info = ServerInfo {
            node_version: NodeVersion {
                version: "0.1.0".to_string(),
            },
            environment: "production".to_string(),
            coordinator: true,
            starting: true,
            uptime: "0.00s".to_string(),
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
        assert_eq!(parsed["starting"], true);
    }

    #[test]
    fn test_trino_response_serialization() {
        let resp = TrinoResponse {
            id: "q-001".to_string(),
            info_uri: None,
            columns: Some(vec![TrinoColumn {
                name: "x".to_string(),
                r#type: "bigint".to_string(),
                type_signature: type_signature_for("bigint"),
            }]),
            data: Some(vec![vec![serde_json::json!(1)]]),
            stats: TrinoStats::finished(),
            ..Default::default()
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"id\":\"q-001\""));
        assert!(json.contains("\"state\":\"FINISHED\""));
        assert!(!json.contains("nextUri")); // Skipped because None
        assert!(
            json.contains("\"typeSignature\""),
            "typeSignature must be present, got: {json}"
        );
        assert!(
            json.contains("\"rawType\":\"bigint\""),
            "rawType must be present, got: {json}"
        );
        assert!(
            json.contains("\"warnings\":[]"),
            "warnings array must always be present"
        );
        assert!(
            json.contains("\"cpuTimeMillis\""),
            "stats must include cpuTimeMillis"
        );
        assert!(
            json.contains("\"processedRows\""),
            "stats must include processedRows"
        );
    }

    #[test]
    fn test_type_signature_timestamp_precision_in_arguments() {
        // #345: timestamp(p) must serialize rawType "timestamp" + precision in
        // arguments, not rawType "timestamp(6)". The Trino JDBC driver's
        // ClientTypeSignature parser rejects a parameterized base in rawType.
        let sig = type_signature_for("timestamp(6)");
        let json = serde_json::to_value(&sig).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "rawType": "timestamp",
                "arguments": [{ "kind": "LONG", "value": 6 }]
            }),
            "timestamp(6) signature, got: {json}"
        );
    }

    #[test]
    fn test_type_signature_timestamp_with_tz_precision_in_arguments() {
        let sig = type_signature_for("timestamp(6) with time zone");
        let json = serde_json::to_value(&sig).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "rawType": "timestamp with time zone",
                "arguments": [{ "kind": "LONG", "value": 6 }]
            }),
            "timestamp(6) with time zone signature, got: {json}"
        );
    }

    #[test]
    fn test_type_signature_time_precision_in_arguments() {
        // The helper also covers time(p), even though arrow_to_trino_type
        // currently emits a bare "time".
        let sig = type_signature_for("time(3)");
        let json = serde_json::to_value(&sig).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "rawType": "time",
                "arguments": [{ "kind": "LONG", "value": 3 }]
            }),
            "time(3) signature, got: {json}"
        );
    }

    #[test]
    fn test_type_signature_bare_temporal_fall_through() {
        // Non-parameterized forms still hit the exact-match arms with a
        // default precision of 6.
        assert_eq!(type_signature_for("timestamp").raw_type, "timestamp");
        assert_eq!(type_signature_for("timestamp").arguments.len(), 1);
        assert_eq!(
            type_signature_for("timestamp with time zone").raw_type,
            "timestamp with time zone"
        );
    }

    #[test]
    fn test_type_signature_decimal_unchanged() {
        // Guard the pre-existing decimal shape the parameterized-temporal
        // handler must not disturb.
        let json = serde_json::to_value(type_signature_for("decimal(10,2)")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "rawType": "decimal",
                "arguments": [
                    { "kind": "LONG", "value": 10 },
                    { "kind": "LONG", "value": 2 }
                ]
            })
        );
    }

    #[test]
    fn test_trino_response_always_includes_info_uri() {
        let resp = TrinoResponse {
            id: "q-001".to_string(),
            info_uri: None,
            stats: TrinoStats::finished(),
            ..Default::default()
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(
            json.contains("\"infoUri\":null"),
            "infoUri must always be present, got: {json}"
        );
    }

    #[test]
    fn test_trino_response_includes_update_type_count_for_writes() {
        let resp = TrinoResponse {
            id: "q-002".to_string(),
            info_uri: None,
            stats: TrinoStats::finished(),
            update_type: Some("INSERT".to_string()),
            update_count: Some(42),
            ..Default::default()
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"updateType\":\"INSERT\""));
        assert!(json.contains("\"updateCount\":42"));
    }

    #[test]
    fn error_from_code_maps_auth_and_internal() {
        let unauthorized = TrinoError::from_code(
            sqe_core::SqeErrorCode::AuthenticationFailed,
            "authentication failed",
            None,
        );
        assert_eq!(unauthorized.error_type, "USER_ERROR");
        assert_eq!(unauthorized.error_name, "AUTHENTICATION_FAILED");

        let missing = TrinoError::from_code(
            sqe_core::SqeErrorCode::CatalogNotFound,
            "unknown query",
            None,
        );
        assert_eq!(missing.error_name, "CATALOG_NOT_FOUND");

        let exhausted = TrinoError::from_code(
            sqe_core::SqeErrorCode::ResourceExhausted,
            "rate limit",
            None,
        );
        assert_eq!(exhausted.error_name, "RESOURCE_EXHAUSTED");

        let panic = TrinoError::internal_error("query task terminated unexpectedly", None);
        assert_eq!(panic.error_type, "INTERNAL_ERROR");
    }

    #[test]
    fn test_trino_error_includes_failure_info() {
        let err = TrinoError {
            message: "broken".to_string(),
            error_code: 1,
            error_name: "USER_ERROR".to_string(),
            error_type: "USER_ERROR".to_string(),
            query_id: None,
            failure_info: Some(TrinoFailureInfo {
                r#type: "io.trino.spi.USER_ERROR".to_string(),
                message: "broken".to_string(),
                suppressed: Vec::new(),
                cause: None,
                stack: Vec::new(),
            }),
            error_location: Some(TrinoErrorLocation {
                line_number: 1,
                column_number: 8,
            }),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"failureInfo\""));
        assert!(json.contains("\"errorLocation\""));
        assert!(json.contains("\"lineNumber\":1"));
    }
}

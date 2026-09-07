use arrow_array::RecordBatch;
use serde::Serialize;

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

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoResponse {
    pub id: String,
    pub info_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<TrinoColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<serde_json::Value>>>,
    pub stats: TrinoStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TrinoError>,
    /// `INSERT`, `UPDATE`, `DELETE`, etc. Read by dbt-trino to mark write
    /// statements; absent for read queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_type: Option<String>,
    /// Row count for write statements. dbt-trino's `adapter.execute` reads
    /// this to populate `rows_affected`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_count: Option<i64>,
    /// Optional URI clients can call to abort the query partway through.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_cancel_uri: Option<String>,
    /// Always emitted (Trino spec mandates an empty array when no warnings).
    #[serde(default)]
    pub warnings: Vec<TrinoWarning>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoWarning {
    pub warning_code: TrinoWarningCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoWarningCode {
    pub code: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoColumn {
    pub name: String,
    pub r#type: String,
    pub type_signature: TrinoTypeSignature,
}

/// Trino column type signature — required by the Trino JDBC driver.
///
/// The driver calls `ClientTypeSignature.getRawType()` on every column;
/// if `typeSignature` is missing from the JSON the field deserializes as
/// `null` and the driver throws NPE.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoTypeSignature {
    pub raw_type: String,
    #[serde(default)]
    pub arguments: Vec<TrinoTypeArgument>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoTypeArgument {
    pub kind: String,
    pub value: serde_json::Value,
}

/// Statistics emitted on every page of a Trino query response.
///
/// dbt-trino, the Trino UI, Datadog's Trino integration, and the JDBC
/// driver's `QueryStatusInfo` read these. Missing fields render as 0
/// or blank for the user.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoStats {
    pub state: String,
    pub queued: bool,
    pub scheduled: bool,
    pub nodes: u32,
    pub total_splits: u32,
    pub queued_splits: u32,
    pub running_splits: u32,
    pub completed_splits: u32,
    pub cpu_time_millis: u64,
    pub wall_time_millis: u64,
    pub queued_time_millis: u64,
    pub elapsed_time_millis: u64,
    pub processed_rows: u64,
    pub processed_bytes: u64,
    pub physical_input_bytes: u64,
    pub peak_memory_bytes: u64,
    pub spilled_bytes: u64,
    /// `rootStage` is always present but `null` until distributed staging
    /// is exposed via this layer.
    pub root_stage: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoError {
    pub message: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    /// Java exception chain. The Trino CLI and JDBC `QueryError.deserialize`
    /// read `failureInfo` to render stack traces and the original cause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_info: Option<TrinoFailureInfo>,
    /// Line and column for IDE highlighting on syntax errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_location: Option<TrinoErrorLocation>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoFailureInfo {
    pub r#type: String,
    pub message: String,
    #[serde(default)]
    pub suppressed: Vec<TrinoFailureInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<TrinoFailureInfo>>,
    /// Frames are formatted as `package.Class.method(File:line)`. Empty
    /// when the source did not produce a stack.
    #[serde(default)]
    pub stack: Vec<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoErrorLocation {
    pub line_number: i32,
    pub column_number: i32,
}

impl TrinoError {
    pub fn from_sqe_error(e: &sqe_core::SqeError, query_id: Option<&str>) -> Self {
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

    /// Build a Trino USER_ERROR for a malformed request (e.g. an `EXECUTE`
    /// naming a prepared statement the client never sent). USER_ERROR is
    /// returned as HTTP 200 with an error body, per the Trino protocol.
    pub fn user_error(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::from_code(sqe_core::SqeErrorCode::SyntaxError, message, query_id)
    }

    pub fn internal_error(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::from_code(sqe_core::SqeErrorCode::InternalError, message, query_id)
    }

    /// Build a USER_ERROR carrying one of Trino's own `StandardErrorCode`
    /// names that has no `SqeErrorCode` counterpart (`INVALID_SESSION_PROPERTY`,
    /// `NOT_FOUND`). Trino clients dispatch on `errorName`, so the protocol
    /// layer's session and prepared-statement errors must use Trino's names
    /// rather than the nearest engine code.
    pub fn trino_user_error(
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

    /// `INVALID_SESSION_PROPERTY` (Trino `StandardErrorCode` 14).
    pub fn invalid_session_property(message: impl Into<String>, query_id: Option<&str>) -> Self {
        Self::trino_user_error(14, "INVALID_SESSION_PROPERTY", message, query_id)
    }

    /// `NOT_FOUND` (Trino `StandardErrorCode` 5): what Trino returns for
    /// `EXECUTE`, `DEALLOCATE PREPARE` and `DESCRIBE OUTPUT|INPUT` naming a
    /// prepared statement the session does not hold.
    pub fn prepared_statement_not_found(name: &str, query_id: Option<&str>) -> Self {
        Self::trino_user_error(
            5,
            "NOT_FOUND",
            format!("Prepared statement not found: {name}"),
            query_id,
        )
    }

    pub fn from_code(
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

/// Build a `TrinoTypeSignature` from a Trino type string.
///
/// For parameterized types like `decimal(18,2)`, the precision and scale
/// are extracted into `arguments`. For simple types, `arguments` is empty.
pub fn type_signature_for(trino_type: &str) -> TrinoTypeSignature {
    // The Trino JDBC driver accesses arguments[0] for varchar, varbinary,
    // and decimal types. Missing arguments cause ArrayIndexOutOfBoundsException.

    // Handle "decimal(p,s)" → rawType "decimal", arguments [{LONG,p},{LONG,s}]
    if let Some(rest) = trino_type.strip_prefix("decimal(") {
        if let Some(params) = rest.strip_suffix(')') {
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                let args: Vec<TrinoTypeArgument> = parts
                    .iter()
                    .filter_map(|p| p.trim().parse::<i64>().ok())
                    .map(|v| TrinoTypeArgument {
                        kind: "LONG".to_string(),
                        value: serde_json::json!(v),
                    })
                    .collect();
                return TrinoTypeSignature {
                    raw_type: "decimal".to_string(),
                    arguments: args,
                };
            }
        }
    }

    let long_arg = |v: i64| TrinoTypeArgument {
        kind: "LONG".to_string(),
        value: serde_json::json!(v),
    };

    // Handle parameterized temporal types: `timestamp(p)`, `timestamp(p) with
    // time zone`, `time(p)`, `time(p) with time zone`. `arrow_to_trino_type`
    // emits the precision baked into the type string (e.g. "timestamp(6)"),
    // but the Trino JDBC driver's ClientTypeSignature parser rejects a
    // parameterized base in `rawType` and throws on the whole response. The
    // precision must live in `arguments` with `rawType` holding only the base
    // name -- the same shape `decimal` already uses. (#345)
    if let Some(sig) = parameterized_temporal_signature(trino_type, &long_arg) {
        return sig;
    }

    match trino_type {
        // varchar/varbinary: driver reads arguments[0] for display size
        "varchar" => TrinoTypeSignature {
            raw_type: "varchar".to_string(),
            arguments: vec![long_arg(2147483647)],
        },
        "varbinary" => TrinoTypeSignature {
            raw_type: "varbinary".to_string(),
            arguments: vec![long_arg(2147483647)],
        },
        // Bare (non-parameterized) temporal types: driver reads arguments[0]
        // for precision; default to 6 (microsecond) to match Iceberg. The
        // parameterized forms are handled above.
        "timestamp" => TrinoTypeSignature {
            raw_type: "timestamp".to_string(),
            arguments: vec![long_arg(6)],
        },
        "timestamp with time zone" => TrinoTypeSignature {
            raw_type: "timestamp with time zone".to_string(),
            arguments: vec![long_arg(6)],
        },
        _ => TrinoTypeSignature {
            raw_type: trino_type.to_string(),
            arguments: vec![],
        },
    }
}

/// Build a type signature for a parameterized temporal type, extracting the
/// precision into `arguments` with the base name in `rawType`.
///
/// Recognizes `timestamp(p)`, `timestamp(p) with time zone`, `time(p)`, and
/// `time(p) with time zone`. Returns `None` for any other (or non-parameterized)
/// input, so bare `timestamp` / `time` fall through to the exact-match arms.
fn parameterized_temporal_signature(
    trino_type: &str,
    long_arg: &impl Fn(i64) -> TrinoTypeArgument,
) -> Option<TrinoTypeSignature> {
    // Split off a trailing " with time zone" so the precision parse sees the
    // bare `base(p)` form.
    let (base, tz_suffix) = match trino_type.strip_suffix(" with time zone") {
        Some(prefix) => (prefix, " with time zone"),
        None => (trino_type, ""),
    };

    let open = base.find('(')?;
    let name = &base[..open];
    if name != "timestamp" && name != "time" {
        return None;
    }
    let precision: i64 = base[open + 1..].strip_suffix(')')?.trim().parse().ok()?;

    Some(TrinoTypeSignature {
        raw_type: format!("{name}{tz_suffix}"),
        arguments: vec![long_arg(precision)],
    })
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

/// Metrics collected from one execution that feed into a `TrinoStats` row.
///
/// Populated best-effort from DataFusion / QueryHandler counters. Zero
/// values are valid and render as `0` on the wire, which is correct for
/// queries that never reach the data path (e.g. `USE` statements).
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    pub elapsed_millis: u64,
    pub queued_millis: u64,
    pub cpu_time_millis: u64,
    pub processed_rows: u64,
    pub processed_bytes: u64,
    pub physical_input_bytes: u64,
    pub peak_memory_bytes: u64,
    pub spilled_bytes: u64,
}

impl TrinoStats {
    pub fn finished() -> Self {
        Self::finished_with_metrics(&ExecutionMetrics::default())
    }

    pub fn finished_with_metrics(m: &ExecutionMetrics) -> Self {
        Self {
            state: "FINISHED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            queued_splits: 0,
            running_splits: 0,
            completed_splits: 1,
            cpu_time_millis: m.cpu_time_millis,
            wall_time_millis: m.elapsed_millis,
            queued_time_millis: m.queued_millis,
            elapsed_time_millis: m.elapsed_millis,
            processed_rows: m.processed_rows,
            processed_bytes: m.processed_bytes,
            physical_input_bytes: m.physical_input_bytes,
            peak_memory_bytes: m.peak_memory_bytes,
            spilled_bytes: m.spilled_bytes,
            root_stage: None,
        }
    }

    /// Stats for a query that has been accepted but not yet started/finished.
    pub fn queued() -> Self {
        Self {
            state: "QUEUED".to_string(),
            queued: true,
            scheduled: false,
            nodes: 0,
            total_splits: 0,
            queued_splits: 0,
            running_splits: 0,
            completed_splits: 0,
            cpu_time_millis: 0,
            wall_time_millis: 0,
            queued_time_millis: 0,
            elapsed_time_millis: 0,
            processed_rows: 0,
            processed_bytes: 0,
            physical_input_bytes: 0,
            peak_memory_bytes: 0,
            spilled_bytes: 0,
            root_stage: None,
        }
    }

    pub fn failed() -> Self {
        Self {
            state: "FAILED".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: 1,
            queued_splits: 0,
            running_splits: 0,
            completed_splits: 0,
            cpu_time_millis: 0,
            wall_time_millis: 0,
            queued_time_millis: 0,
            elapsed_time_millis: 0,
            processed_rows: 0,
            processed_bytes: 0,
            physical_input_bytes: 0,
            peak_memory_bytes: 0,
            spilled_bytes: 0,
            root_stage: None,
        }
    }

    /// Stats for an in-progress paginated result.
    pub fn running(completed_pages: usize, total_pages: usize) -> Self {
        Self::running_with_metrics(completed_pages, total_pages, &ExecutionMetrics::default())
    }

    pub fn running_with_metrics(
        completed_pages: usize,
        total_pages: usize,
        m: &ExecutionMetrics,
    ) -> Self {
        let remaining = total_pages.saturating_sub(completed_pages);
        Self {
            state: "RUNNING".to_string(),
            queued: false,
            scheduled: true,
            nodes: 1,
            total_splits: total_pages as u32,
            queued_splits: 0,
            running_splits: remaining.min(1) as u32,
            completed_splits: completed_pages as u32,
            cpu_time_millis: m.cpu_time_millis,
            wall_time_millis: m.elapsed_millis,
            queued_time_millis: m.queued_millis,
            elapsed_time_millis: m.elapsed_millis,
            processed_rows: m.processed_rows,
            processed_bytes: m.processed_bytes,
            physical_input_bytes: m.physical_input_bytes,
            peak_memory_bytes: m.peak_memory_bytes,
            spilled_bytes: m.spilled_bytes,
            root_stage: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn trino_stats_queued_state() {
        let s = TrinoStats::queued();
        assert_eq!(s.state, "QUEUED");
        assert!(s.queued);
        assert!(!s.scheduled);
    }

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

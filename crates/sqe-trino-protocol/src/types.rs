//! Trino `/v1/statement` wire types.
//!
//! Moved verbatim from `sqe-trino-compat::protocol` (review 705) so the two
//! client binaries can deserialize a page without linking the server crate.
//! Everything here is plain serde; the `sqe_core`-aware constructors on
//! `TrinoError` stay in `sqe-trino-compat` as an extension trait.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Always serialized. Defaulted on deserialization so a client can read a
    /// minimal page (a proxy error body, a test double) without failing.
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoWarning {
    pub warning_code: TrinoWarningCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoWarningCode {
    pub code: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoTypeSignature {
    pub raw_type: String,
    #[serde(default)]
    pub arguments: Vec<TrinoTypeArgument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl std::fmt::Display for TrinoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.error_name, self.error_code, self.message
        )
    }
}

impl std::error::Error for TrinoError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoErrorLocation {
    pub line_number: i32,
    pub column_number: i32,
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

/// A parsed Trino type name, as it appears in `TrinoColumn::type`.
///
/// This is the client-side counterpart of the server's `arrow_to_trino_type`:
/// the string a Trino (or SQE) server emits, split into its base name and
/// parameters so a decoder can pick a faithful Arrow type. Nested types keep
/// their parameter text verbatim; the decoder renders those as JSON strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrinoType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    /// `decimal(p, s)`. Trino defaults to `decimal(38, 0)` when unparameterized.
    Decimal {
        precision: u8,
        scale: i8,
    },
    Varchar,
    Char,
    Varbinary,
    Json,
    Uuid,
    Date,
    /// `time(p)` / `time(p) with time zone`.
    Time {
        precision: u8,
        with_time_zone: bool,
    },
    /// `timestamp(p)` / `timestamp(p) with time zone`. Trino's default
    /// precision is 3.
    Timestamp {
        precision: u8,
        with_time_zone: bool,
    },
    IntervalYearToMonth,
    IntervalDayToSecond,
    /// `array(...)`, `map(...)`, `row(...)` and anything else not listed;
    /// carries the full original type string.
    Other(String),
}

/// Parse a Trino type string (`"decimal(18,2)"`, `"timestamp(6) with time
/// zone"`, `"varchar(255)"`, `"array(bigint)"`).
pub fn parse_trino_type(type_name: &str) -> TrinoType {
    let lower = type_name.trim().to_ascii_lowercase();
    let (head, with_tz) = match lower.strip_suffix(" with time zone") {
        Some(h) => (h.trim_end().to_string(), true),
        None => (lower.clone(), false),
    };
    let (base, params) = match head.find('(') {
        Some(open) if head.ends_with(')') => (
            head[..open].trim().to_string(),
            Some(head[open + 1..head.len() - 1].to_string()),
        ),
        _ => (head.clone(), None),
    };
    let first_param = |default: u8| -> u8 {
        params
            .as_deref()
            .and_then(|p| p.split(',').next())
            .and_then(|p| p.trim().parse::<u8>().ok())
            .unwrap_or(default)
    };
    match base.as_str() {
        "boolean" => TrinoType::Boolean,
        "tinyint" => TrinoType::TinyInt,
        "smallint" => TrinoType::SmallInt,
        "integer" | "int" => TrinoType::Integer,
        "bigint" => TrinoType::BigInt,
        "real" => TrinoType::Real,
        "double" => TrinoType::Double,
        "decimal" => {
            let mut it = params.as_deref().unwrap_or("").split(',');
            let precision = it
                .next()
                .and_then(|p| p.trim().parse::<u8>().ok())
                .unwrap_or(38);
            let scale = it
                .next()
                .and_then(|s| s.trim().parse::<i8>().ok())
                .unwrap_or(0);
            TrinoType::Decimal { precision, scale }
        }
        "varchar" => TrinoType::Varchar,
        "char" => TrinoType::Char,
        "varbinary" => TrinoType::Varbinary,
        "json" => TrinoType::Json,
        "uuid" => TrinoType::Uuid,
        "date" => TrinoType::Date,
        "time" => TrinoType::Time {
            precision: first_param(3),
            with_time_zone: with_tz,
        },
        "timestamp" => TrinoType::Timestamp {
            precision: first_param(3),
            with_time_zone: with_tz,
        },
        "interval year to month" => TrinoType::IntervalYearToMonth,
        "interval day to second" => TrinoType::IntervalDayToSecond,
        _ => TrinoType::Other(type_name.trim().to_string()),
    }
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

    #[test]
    fn trino_stats_queued_state() {
        let s = TrinoStats::queued();
        assert_eq!(s.state, "QUEUED");
        assert!(s.queued);
        assert!(!s.scheduled);
    }

    #[test]
    fn response_round_trips_without_stats() {
        // A client must tolerate a page that omits `stats` (proxy error
        // bodies, test doubles); the server always emits it.
        let r: TrinoResponse = serde_json::from_str(r#"{"id":"q","infoUri":null}"#).unwrap();
        assert_eq!(r.id, "q");
        assert_eq!(r.stats.state, "");
        let json = serde_json::to_string(&r).unwrap();
        assert!(
            json.contains("\"stats\""),
            "stats always serialized: {json}"
        );
    }

    #[test]
    fn type_signature_decimal_and_temporal() {
        let d = serde_json::to_value(type_signature_for("decimal(10,2)")).unwrap();
        assert_eq!(
            d,
            serde_json::json!({
                "rawType": "decimal",
                "arguments": [
                    { "kind": "LONG", "value": 10 },
                    { "kind": "LONG", "value": 2 }
                ]
            })
        );
        let t = type_signature_for("timestamp(6) with time zone");
        assert_eq!(t.raw_type, "timestamp with time zone");
        assert_eq!(t.arguments.len(), 1);
        assert_eq!(type_signature_for("time(3)").raw_type, "time");
        assert_eq!(type_signature_for("timestamp").arguments.len(), 1);
    }

    #[test]
    fn parse_trino_type_covers_parameterized_forms() {
        assert_eq!(
            parse_trino_type("decimal(18,2)"),
            TrinoType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        assert_eq!(
            parse_trino_type("decimal"),
            TrinoType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(parse_trino_type("varchar(255)"), TrinoType::Varchar);
        assert_eq!(parse_trino_type("VARCHAR"), TrinoType::Varchar);
        assert_eq!(
            parse_trino_type("timestamp(6) with time zone"),
            TrinoType::Timestamp {
                precision: 6,
                with_time_zone: true
            }
        );
        assert_eq!(
            parse_trino_type("timestamp"),
            TrinoType::Timestamp {
                precision: 3,
                with_time_zone: false
            }
        );
        assert_eq!(
            parse_trino_type("time(3)"),
            TrinoType::Time {
                precision: 3,
                with_time_zone: false
            }
        );
        assert_eq!(parse_trino_type("tinyint"), TrinoType::TinyInt);
        assert_eq!(parse_trino_type("smallint"), TrinoType::SmallInt);
        assert_eq!(parse_trino_type("integer"), TrinoType::Integer);
        assert_eq!(parse_trino_type("real"), TrinoType::Real);
        assert_eq!(
            parse_trino_type("array(bigint)"),
            TrinoType::Other("array(bigint)".to_string())
        );
        assert_eq!(
            parse_trino_type("interval day to second"),
            TrinoType::IntervalDayToSecond
        );
    }

    #[test]
    fn trino_error_display_is_name_code_message() {
        let e = TrinoError {
            message: "boom".into(),
            error_code: 1,
            error_name: "SYNTAX_ERROR".into(),
            error_type: "USER_ERROR".into(),
            query_id: None,
            failure_info: None,
            error_location: None,
        };
        assert_eq!(e.to_string(), "SYNTAX_ERROR (1): boom");
    }
}

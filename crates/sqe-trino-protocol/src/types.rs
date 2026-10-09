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
/// are extracted into `arguments`. Nested types (`array`, `map`, `row`) carry
/// their element signatures as `TYPE` / `NAMED_TYPE` arguments, recursively.
/// For simple types, `arguments` is empty.
pub fn type_signature_for(trino_type: &str) -> TrinoTypeSignature {
    // The Trino JDBC driver accesses arguments[0] for varchar, varbinary,
    // and decimal types. Missing arguments cause ArrayIndexOutOfBoundsException.

    if let Some(sig) = nested_signature(trino_type) {
        return sig;
    }

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
        // Any other `base(n, ...)` with integer parameters (`varchar(255)`,
        // `char(3)`) splits the same way: base name, LONG arguments.
        _ => integer_parameter_signature(trino_type, &long_arg).unwrap_or_else(|| {
            TrinoTypeSignature {
                raw_type: trino_type.to_string(),
                arguments: vec![],
            }
        }),
    }
}

/// Build the signature of `array(T)`, `map(K,V)` or `row(name T, ...)`.
///
/// Trino's client signature never parameterizes `rawType` (the JDBC driver
/// rejects a `rawType` containing `,`, and maps columns by the bare base
/// name): an array carries one `TYPE` argument, a map two, and a row one
/// `NAMED_TYPE` argument per field, `{fieldName: {name}, typeSignature}`.
/// Returns `None` for anything else, including unbalanced input.
fn nested_signature(trino_type: &str) -> Option<TrinoTypeSignature> {
    let t = trino_type.trim();
    let open = t.find('(')?;
    let inner = t[open + 1..].strip_suffix(')')?;
    let base = t[..open].trim().to_ascii_lowercase();
    let parts = split_top_level(inner)?;

    let type_arg = |s: &str| TrinoTypeArgument {
        kind: "TYPE".to_string(),
        value: serde_json::to_value(type_signature_for(s)).unwrap_or_default(),
    };
    let arguments = match (base.as_str(), parts.len()) {
        ("array", 1) | ("map", 2) => parts.iter().map(|p| type_arg(p)).collect(),
        ("row", n) if n > 0 => parts
            .iter()
            .map(|p| {
                let (name, ty) = split_row_field(p);
                TrinoTypeArgument {
                    kind: "NAMED_TYPE".to_string(),
                    value: serde_json::json!({
                        "fieldName": name.map(|n| serde_json::json!({ "name": n })),
                        "typeSignature": serde_json::to_value(type_signature_for(ty))
                            .unwrap_or_default(),
                    }),
                }
            })
            .collect(),
        _ => return None,
    };
    Some(TrinoTypeSignature {
        raw_type: base,
        arguments,
    })
}

/// Split a type parameter list on the commas outside parentheses and double
/// quotes. `None` when the parentheses or quotes do not balance.
fn split_top_level(params: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let (mut depth, mut quoted, mut start) = (0usize, false, 0);
    for (i, c) in params.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.checked_sub(1)?,
            ',' if !quoted && depth == 0 => {
                parts.push(params[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 || quoted {
        return None;
    }
    parts.push(params[start..].trim());
    Some(parts)
}

/// Split one `row(...)` field into its name and type.
///
/// A field is `name type`, `"quoted name" type` (with `""` escaping a quote),
/// or an anonymous bare type (`row(integer, varchar)`), which Trino sends
/// with a null `fieldName`.
fn split_row_field(field: &str) -> (Option<String>, &str) {
    let f = field.trim();
    if let Some(rest) = f.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = rest.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c != '"' {
                name.push(c);
            } else if chars.peek().map(|&(_, n)| n) == Some('"') {
                name.push('"');
                chars.next();
            } else {
                return (Some(name), rest[i + 1..].trim());
            }
        }
        return (None, f);
    }
    // A multi-word scalar (`timestamp with time zone`) is a bare type, not a
    // field called `timestamp`; a parameterized head is a bare type too.
    match f.split_once(char::is_whitespace) {
        Some((name, ty))
            if !name.contains('(') && matches!(parse_trino_type(f), TrinoType::Other(_)) =>
        {
            (Some(name.to_string()), ty.trim())
        }
        _ => (None, f),
    }
}

/// `base(n, ...)` with only integer parameters -> rawType `base`, LONG
/// arguments. `None` if any parameter is not an integer.
fn integer_parameter_signature(
    trino_type: &str,
    long_arg: &impl Fn(i64) -> TrinoTypeArgument,
) -> Option<TrinoTypeSignature> {
    let open = trino_type.find('(')?;
    let params = trino_type[open + 1..].strip_suffix(')')?;
    let arguments = params
        .split(',')
        .map(|p| p.trim().parse::<i64>().ok().map(long_arg))
        .collect::<Option<Vec<_>>>()?;
    Some(TrinoTypeSignature {
        raw_type: trino_type[..open].trim().to_string(),
        arguments,
    })
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

    fn sig_json(t: &str) -> serde_json::Value {
        serde_json::to_value(type_signature_for(t)).unwrap()
    }

    #[test]
    fn type_signature_array_nests_one_type_argument() {
        assert_eq!(
            sig_json("array(bigint)"),
            serde_json::json!({
                "rawType": "array",
                "arguments": [
                    { "kind": "TYPE", "value": { "rawType": "bigint", "arguments": [] } }
                ]
            })
        );
    }

    #[test]
    fn type_signature_map_nests_key_and_value() {
        assert_eq!(
            sig_json("map(varchar,decimal(10,2))"),
            serde_json::json!({
                "rawType": "map",
                "arguments": [
                    { "kind": "TYPE", "value": {
                        "rawType": "varchar",
                        "arguments": [{ "kind": "LONG", "value": 2147483647 }]
                    } },
                    { "kind": "TYPE", "value": {
                        "rawType": "decimal",
                        "arguments": [
                            { "kind": "LONG", "value": 10 },
                            { "kind": "LONG", "value": 2 }
                        ]
                    } }
                ]
            })
        );
    }

    #[test]
    fn type_signature_row_nests_named_fields() {
        assert_eq!(
            sig_json("row(a integer,b timestamp(6) with time zone)"),
            serde_json::json!({
                "rawType": "row",
                "arguments": [
                    { "kind": "NAMED_TYPE", "value": {
                        "fieldName": { "name": "a" },
                        "typeSignature": { "rawType": "integer", "arguments": [] }
                    } },
                    { "kind": "NAMED_TYPE", "value": {
                        "fieldName": { "name": "b" },
                        "typeSignature": {
                            "rawType": "timestamp with time zone",
                            "arguments": [{ "kind": "LONG", "value": 6 }]
                        }
                    } }
                ]
            })
        );
    }

    #[test]
    fn type_signature_row_quoted_and_anonymous_fields() {
        let s = sig_json(r#"row("my, ""odd"" field" bigint, integer, timestamp with time zone)"#);
        let args = s["arguments"].as_array().unwrap();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0]["value"]["fieldName"]["name"], r#"my, "odd" field"#);
        assert_eq!(args[0]["value"]["typeSignature"]["rawType"], "bigint");
        assert!(args[1]["value"]["fieldName"].is_null());
        assert_eq!(args[1]["value"]["typeSignature"]["rawType"], "integer");
        assert!(args[2]["value"]["fieldName"].is_null());
        assert_eq!(
            args[2]["value"]["typeSignature"]["rawType"],
            "timestamp with time zone"
        );
    }

    #[test]
    fn type_signature_deep_nesting_recurses() {
        let s = sig_json("array(row(a integer, b map(varchar, array(bigint))))");
        assert_eq!(s["rawType"], "array");
        let row = &s["arguments"][0]["value"];
        assert_eq!(row["rawType"], "row");
        let b = &row["arguments"][1]["value"];
        assert_eq!(b["fieldName"]["name"], "b");
        let map = &b["typeSignature"];
        assert_eq!(map["rawType"], "map");
        assert_eq!(map["arguments"][0]["value"]["rawType"], "varchar");
        let arr = &map["arguments"][1]["value"];
        assert_eq!(arr["rawType"], "array");
        assert_eq!(arr["arguments"][0]["value"]["rawType"], "bigint");
    }

    #[test]
    fn type_signature_integer_parameters_and_unbalanced_input() {
        assert_eq!(
            sig_json("varchar(255)"),
            serde_json::json!({
                "rawType": "varchar",
                "arguments": [{ "kind": "LONG", "value": 255 }]
            })
        );
        // Not a type we can split: passes through rather than guessing.
        assert_eq!(type_signature_for("array(bigint").raw_type, "array(bigint");
        assert_eq!(type_signature_for("map(a)").raw_type, "map(a)");
    }

    /// A small recursive type tree, rendered the way the server's
    /// `arrow_to_trino_type` renders nested types.
    #[derive(Debug)]
    enum Gen {
        Leaf(&'static str),
        Array(Box<Gen>),
        Map(Box<Gen>, Box<Gen>),
        Row(Vec<(String, Gen)>),
    }

    impl Gen {
        fn render(&self) -> String {
            match self {
                Gen::Leaf(s) => s.to_string(),
                Gen::Array(e) => format!("array({})", e.render()),
                Gen::Map(k, v) => format!("map({},{})", k.render(), v.render()),
                Gen::Row(fs) => {
                    let cols: Vec<String> = fs
                        .iter()
                        .map(|(n, t)| {
                            if n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                                format!("{n} {}", t.render())
                            } else {
                                format!("\"{}\" {}", n.replace('"', "\"\""), t.render())
                            }
                        })
                        .collect();
                    format!("row({})", cols.join(","))
                }
            }
        }

        fn random(rng: &mut u64, depth: u32) -> Gen {
            const LEAVES: &[&str] = &[
                "boolean",
                "bigint",
                "integer",
                "double",
                "varchar",
                "varbinary",
                "date",
                "decimal(10,2)",
                "decimal(38,0)",
                "timestamp",
                "timestamp(6)",
                "timestamp(3) with time zone",
                "time(3)",
                "varchar(12)",
                "interval day to second",
            ];
            let mut next = |n: u64| {
                *rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (*rng >> 33) % n
            };
            let pick = if depth == 0 { 0 } else { next(4) };
            match pick {
                0 => Gen::Leaf(LEAVES[next(LEAVES.len() as u64) as usize]),
                1 => Gen::Array(Box::new(Gen::random(rng, depth - 1))),
                2 => Gen::Map(
                    Box::new(Gen::random(rng, depth - 1)),
                    Box::new(Gen::random(rng, depth - 1)),
                ),
                _ => {
                    let n = 1 + next(3);
                    Gen::Row(
                        (0..n)
                            .map(|i| {
                                let name = if i == 1 {
                                    format!("f {i}, \"x\"")
                                } else {
                                    format!("f{i}")
                                };
                                (name, Gen::random(rng, depth - 1))
                            })
                            .collect(),
                    )
                }
            }
        }

        /// The signature matches the tree: no parameterized `rawType`
        /// anywhere, containers carry their children as typed arguments.
        fn check(&self, sig: &serde_json::Value) {
            let raw = sig["rawType"].as_str().unwrap();
            assert!(
                !raw.contains(['(', ')', ',', '<', '>']),
                "{} -> rawType {raw:?}",
                self.render()
            );
            let args = sig["arguments"].as_array().unwrap();
            match self {
                Gen::Leaf(_) => {
                    assert!(args.iter().all(|a| a["kind"] == "LONG"), "{sig}");
                }
                Gen::Array(e) => {
                    assert_eq!(raw, "array");
                    assert_eq!(args.len(), 1);
                    assert_eq!(args[0]["kind"], "TYPE");
                    e.check(&args[0]["value"]);
                }
                Gen::Map(k, v) => {
                    assert_eq!(raw, "map");
                    assert_eq!(args.len(), 2);
                    assert!(args.iter().all(|a| a["kind"] == "TYPE"));
                    k.check(&args[0]["value"]);
                    v.check(&args[1]["value"]);
                }
                Gen::Row(fs) => {
                    assert_eq!(raw, "row");
                    assert_eq!(args.len(), fs.len());
                    for ((name, t), a) in fs.iter().zip(args) {
                        assert_eq!(a["kind"], "NAMED_TYPE");
                        assert_eq!(a["value"]["fieldName"]["name"], name.as_str());
                        t.check(&a["value"]["typeSignature"]);
                    }
                }
            }
        }
    }

    /// The nested counterpart of the server's scalar signature property:
    /// for any generated nested type, `rawType` never carries parameters
    /// and the arguments recurse into the children. Deterministic seed, so
    /// a failure reproduces.
    #[test]
    fn nested_type_signatures_keep_parameters_out_of_raw_type() {
        let mut rng = 0x5eed_u64;
        for _ in 0..2000 {
            let t = Gen::random(&mut rng, 4);
            t.check(&sig_json(&t.render()));
        }
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

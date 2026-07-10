//! Wire types for the audit export path: [`Severity`] and the [`ShipRecord`]
//! payload sent to downstream sinks.

use serde_json::Value;

/// Severity tier for a shipped log record. Derived from OCSF `status_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    /// Maps to OTel `Severity::Error` (level 17) when emitting OTLP log records.
    Error,
}

/// A normalised, shippable audit record produced from one OCSF JSON line.
/// The `body` field holds the complete parsed OCSF `Value` for the SIEM.
#[derive(Debug, Clone)]
pub struct ShipRecord {
    pub seq: u64,
    pub time_unix_ms: i64,
    pub severity: Severity,
    pub body: Value,
    pub class_uid: i64,
    pub category_uid: i64,
    pub kind: String,
    pub status_id: i64,
    pub user_name: String,
}

/// Injection seam for the OTLP log shipper (Task 8). Dyn-compatible via
/// `async_trait`. A stub implementation is used in Task 7 proof tests.
#[async_trait::async_trait]
pub trait LogShipExporter: Send + Sync {
    async fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String>;
}

/// Parse one OCSF JSON line into a `ShipRecord`.
///
/// Returns `None` if the line cannot be parsed as JSON; the caller should
/// log and skip such lines (non-fatal). All indexed fields default to
/// zero/empty when absent from the OCSF payload.
pub fn ocsf_to_ship_record(ocsf_line: &str) -> Option<ShipRecord> {
    let v: Value = serde_json::from_str(ocsf_line).ok()?;

    let seq = v
        .pointer("/metadata/sequence")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let time_unix_ms = v.get("time").and_then(Value::as_i64).unwrap_or(0);

    let status_id = v.get("status_id").and_then(Value::as_i64).unwrap_or(0);
    let severity = match status_id {
        2 => Severity::Warn,
        _ => Severity::Info,
    };

    let class_uid = v.get("class_uid").and_then(Value::as_i64).unwrap_or(0);
    let category_uid = v.get("category_uid").and_then(Value::as_i64).unwrap_or(0);

    // `kind` lives under `metadata` in the SQE OCSF shape; fall back to a
    // top-level `kind` field, then empty string.
    let kind = v
        .pointer("/metadata/kind")
        .or_else(|| v.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let user_name = v
        .pointer("/actor/user/name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    Some(ShipRecord {
        seq,
        time_unix_ms,
        severity,
        body: v,
        class_uid,
        category_uid,
        kind,
        status_id,
        user_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{sample_query_event, Outcome, AuditKind};
    use crate::audit::ocsf::to_ocsf;

    fn success_ocsf_line(seq: u64) -> String {
        let mut ev = sample_query_event();
        ev.integrity.seq = seq;
        serde_json::to_string(&to_ocsf(&ev)).unwrap()
    }

    fn failure_ocsf_line() -> String {
        let mut ev = sample_query_event();
        ev.outcome = Outcome::Failure {
            error_type: Some("PlanError".into()),
            error_code: Some("E1".into()),
            message: Some("bad plan".into()),
        };
        serde_json::to_string(&to_ocsf(&ev)).unwrap()
    }

    #[test]
    fn success_line_maps_to_info_with_correct_fields() {
        let line = success_ocsf_line(77);
        let rec = ocsf_to_ship_record(&line).expect("should parse");
        assert_eq!(rec.seq, 77);
        assert_eq!(rec.severity, Severity::Info);
        assert_eq!(rec.status_id, 1);
        assert_eq!(rec.class_uid, 6005);
        assert_eq!(rec.category_uid, 6);
        assert_eq!(rec.user_name, "alice");
        // body holds the full OCSF object
        assert_eq!(rec.body["actor"]["user"]["name"], "alice");
        assert!(rec.time_unix_ms > 0);
    }

    #[test]
    fn failure_line_maps_to_warn() {
        let line = failure_ocsf_line();
        let rec = ocsf_to_ship_record(&line).expect("should parse");
        assert_eq!(rec.severity, Severity::Warn);
        assert_eq!(rec.status_id, 2);
    }

    #[test]
    fn garbage_line_returns_none() {
        assert!(ocsf_to_ship_record("not json at all {{{").is_none());
        assert!(ocsf_to_ship_record("").is_none());
    }

    #[test]
    fn missing_optional_fields_default_gracefully() {
        // Minimal valid JSON that is parseable but has no OCSF fields.
        let rec = ocsf_to_ship_record("{}").expect("empty object should parse");
        assert_eq!(rec.seq, 0);
        assert_eq!(rec.time_unix_ms, 0);
        assert_eq!(rec.severity, Severity::Info);
        assert_eq!(rec.class_uid, 0);
        assert_eq!(rec.user_name, "");
        assert_eq!(rec.kind, "");
    }

    #[test]
    fn kind_field_from_audit_kind_variant() {
        // Verify a known AuditKind produces the expected class_uid via to_ocsf
        // (kind is not a top-level OCSF field; class_uid is the discriminant).
        let mut ev = sample_query_event();
        ev.kind = AuditKind::Auth;
        let line = serde_json::to_string(&to_ocsf(&ev)).unwrap();
        let rec = ocsf_to_ship_record(&line).expect("should parse");
        assert_eq!(rec.class_uid, 3002);
    }
}

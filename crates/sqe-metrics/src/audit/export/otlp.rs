use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{Logger as _, LogRecord as _, LoggerProvider as _};
use opentelemetry::logs::Severity as OtelSeverity;
use opentelemetry::logs::AnyValue;
use opentelemetry_otlp::{LogExporter, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::Resource;

use super::record::{LogShipExporter, ShipRecord, Severity};

/// Map an OCSF `class_uid` to a human-readable kind string.
fn class_uid_to_kind(class_uid: i64) -> &'static str {
    match class_uid {
        6005 => "datastore_activity",
        3002 => "authentication",
        3003 => "authorize_session",
        3001 => "account_change",
        3004 => "entity_management",
        _ => "unknown",
    }
}

/// OTLP log exporter that ships `ShipRecord` batches to a collector.
///
/// Uses a DEDICATED `SdkLoggerProvider` (not the tracing bridge global).
/// Records are emitted synchronously via `logger.emit`; `provider.force_flush`
/// drains the batch processor before returning so every batch is acknowledged.
#[derive(Debug)]
pub struct OtlpExporter {
    provider: SdkLoggerProvider,
    logger: opentelemetry_sdk::logs::SdkLogger,
}

impl OtlpExporter {
    /// Build an exporter targeting `endpoint` (e.g. `http://localhost:4317`).
    ///
    /// Returns an error string if the OTLP exporter or provider cannot be built.
    pub fn new(endpoint: &str) -> Result<Self, String> {
        let log_exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("failed to build OTLP log exporter: {e}"))?;

        let resource = Resource::builder()
            .with_service_name("sqe-audit")
            .build();

        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter)
            .build();

        let logger = provider.logger("sqe-audit");

        Ok(Self { provider, logger })
    }
}

#[async_trait::async_trait]
impl LogShipExporter for OtlpExporter {
    async fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String> {
        for r in records {
            let mut rec = self.logger.create_log_record();

            // Convert milliseconds-since-epoch to SystemTime.
            let ts = if r.time_unix_ms >= 0 {
                UNIX_EPOCH + Duration::from_millis(r.time_unix_ms as u64)
            } else {
                SystemTime::now()
            };
            rec.set_timestamp(ts);

            rec.set_severity_number(match r.severity {
                Severity::Info => OtelSeverity::Info,
                Severity::Warn => OtelSeverity::Warn,
                // Error maps to OTel Error severity (level 17 in the OTel log data model).
                Severity::Error => OtelSeverity::Error,
            });

            // Body: full OCSF JSON as a string so the SIEM receives the complete record.
            rec.set_body(AnyValue::from(r.body.to_string()));

            rec.add_attribute("ocsf.class_uid", r.class_uid);
            rec.add_attribute("ocsf.category_uid", r.category_uid);
            rec.add_attribute("audit.kind", class_uid_to_kind(r.class_uid));
            rec.add_attribute("audit.status_id", r.status_id);
            rec.add_attribute("user.name", r.user_name.clone());
            rec.add_attribute("audit.seq", r.seq as i64);

            self.logger.emit(rec);
        }

        // Flush the batch processor and treat any error as a failed export.
        self.provider
            .force_flush()
            .map_err(|e| format!("otlp export failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Construction smoke test: builds OtlpExporter with a dummy endpoint without
    /// panicking. No collector is started; delivery is proven by the Task 7 stub tests.
    /// Requires a Tokio runtime because tonic's batch processor spawns an async task.
    #[tokio::test]
    async fn otlp_exporter_constructs_without_panic() {
        // Use a syntactically valid endpoint; connection is lazy so no network activity.
        let exporter = OtlpExporter::new("http://localhost:4317");
        assert!(exporter.is_ok(), "OtlpExporter::new should succeed: {exporter:?}");
    }
}

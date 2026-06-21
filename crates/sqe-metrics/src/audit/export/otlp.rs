use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{Logger as _, LogRecord as _, LoggerProvider as _};
use opentelemetry::logs::Severity as OtelSeverity;
use opentelemetry::logs::AnyValue;
use opentelemetry::InstrumentationScope;
use opentelemetry_otlp::{LogExporter, WithExportConfig};
// Bring the LogExporter trait into scope for `.export()` without a name clash
// against `opentelemetry_otlp::LogExporter` (the struct).
use opentelemetry_sdk::logs::LogExporter as _;
use opentelemetry_sdk::logs::{LogBatch, SdkLogger, SdkLoggerProvider};
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
/// Uses a DEDICATED `opentelemetry_otlp::LogExporter` driven directly (not via a
/// `BatchLogProcessor`). The per-batch `OTelSdkResult` from `export().await` IS the
/// ack: `export_batch` returns `Ok(())` only if the collector accepted the batch, and
/// `Err` otherwise. No background timer can drain the buffer before we check the result.
///
/// An `SdkLoggerProvider` with NO processors is kept solely as a record factory:
/// it gives us `SdkLogRecord` instances via `logger.create_log_record()`.
/// No data flows through the provider's processor chain.
#[derive(Debug)]
pub struct OtlpExporter {
    /// Bare OTLP exporter. Called directly so the export Result is observed.
    log_exporter: LogExporter,
    /// Factory-only logger (provider has no processor). Used only to mint `SdkLogRecord`s.
    factory_logger: SdkLogger,
    /// Instrumentation scope attached to every batch.
    scope: InstrumentationScope,
}

impl OtlpExporter {
    /// Build an exporter targeting `endpoint` (e.g. `http://localhost:4317`).
    ///
    /// Returns an error string if the OTLP exporter or provider cannot be built.
    pub fn new(endpoint: &str) -> Result<Self, String> {
        let resource = Resource::builder()
            .with_service_name("sqe-audit")
            .build();

        let mut log_exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("failed to build OTLP log exporter: {e}"))?;

        // Attach the resource (service.name etc.) to the bare exporter so the
        // collector receives it on every batch, matching the old provider-path behaviour.
        log_exporter.set_resource(&resource);

        // Factory-only provider: no processors, so no data flows through it.
        // Its sole purpose is to let us call `logger.create_log_record()`.
        let factory_provider = SdkLoggerProvider::builder().build();
        let factory_logger = factory_provider.logger("sqe-audit");

        let scope = InstrumentationScope::builder("sqe-audit").build();

        Ok(Self { log_exporter, factory_logger, scope })
    }
}

#[async_trait::async_trait]
impl LogShipExporter for OtlpExporter {
    async fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String> {
        let now = SystemTime::now();

        // Mint one SdkLogRecord per ShipRecord using the factory logger.
        let mut sdk_records: Vec<opentelemetry_sdk::logs::SdkLogRecord> =
            Vec::with_capacity(records.len());

        for r in records {
            let mut rec = self.factory_logger.create_log_record();

            // Convert milliseconds-since-epoch to SystemTime.
            let ts = if r.time_unix_ms >= 0 {
                UNIX_EPOCH + Duration::from_millis(r.time_unix_ms as u64)
            } else {
                now
            };
            rec.set_timestamp(ts);
            // Set observed_timestamp explicitly since we bypass logger.emit().
            rec.set_observed_timestamp(now);

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

            sdk_records.push(rec);
        }

        // Build the borrow slice that LogBatch::new expects.
        let pairs: Vec<(&opentelemetry_sdk::logs::SdkLogRecord, &InstrumentationScope)> =
            sdk_records.iter().map(|r| (r, &self.scope)).collect();

        let batch = LogBatch::new(&pairs);

        // Drive the exporter DIRECTLY. The returned OTelSdkResult IS the ack:
        // Ok(()) means the collector accepted; Err means it did not.
        // No background timer can drain the buffer before we check the result.
        self.log_exporter
            .export(batch)
            .await
            .map_err(|e| format!("otlp export failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Construction smoke test: builds OtlpExporter with a dummy endpoint without
    /// panicking. No collector is started; delivery is proven by the Task 7 stub tests.
    #[tokio::test]
    async fn otlp_exporter_constructs_without_panic() {
        // Use a syntactically valid endpoint; connection is lazy so no network activity.
        let exporter = OtlpExporter::new("http://localhost:4317");
        assert!(exporter.is_ok(), "OtlpExporter::new should succeed: {exporter:?}");
    }

    /// Regression guard for MUST-FIX 1 (at-least-once guarantee).
    ///
    /// Proves that when the collector is unreachable, `export_batch` returns `Err`
    /// rather than `Ok`. With the old `BatchLogProcessor` path this could spuriously
    /// return `Ok` because the background timer would drain the buffer before
    /// `force_flush` ran, hiding the transport error.
    ///
    /// Port 1 on loopback is reserved/non-routable on all major platforms and will
    /// produce a connection-refused error quickly. The exporter timeout is set to 2 s
    /// so the test completes fast even if the OS takes a moment to refuse.
    #[tokio::test]
    async fn export_batch_returns_err_when_collector_unreachable() {
        use serde_json::json;
        use crate::audit::export::record::ShipRecord;

        let resource = Resource::builder().with_service_name("sqe-audit").build();
        let mut log_exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint("http://127.0.0.1:1")
            .with_timeout(Duration::from_secs(2))
            .build()
            .expect("build should succeed even for unreachable endpoint");
        log_exporter.set_resource(&resource);
        let factory_provider = SdkLoggerProvider::builder().build();
        let factory_logger = factory_provider.logger("sqe-audit");
        let scope = InstrumentationScope::builder("sqe-audit").build();
        let exporter = OtlpExporter { log_exporter, factory_logger, scope };

        let record = ShipRecord {
            body: json!({"class_uid": 6005, "activity_name": "Query"}),
            class_uid: 6005,
            category_uid: 6,
            kind: "datastore_activity".into(),
            status_id: 1,
            user_name: "test-user".into(),
            seq: 1,
            time_unix_ms: 1_700_000_000_000,
            severity: Severity::Info,
        };

        let result = exporter.export_batch(&[record]).await;
        assert!(
            result.is_err(),
            "export_batch MUST return Err when the collector is unreachable, got Ok. \
             This would indicate the transport error is being swallowed (regression of \
             the BatchLogProcessor silent-loss bug)."
        );
    }
}

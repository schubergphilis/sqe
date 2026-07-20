//! OpenTelemetry initialization: tracing subscriber + OTLP span export, and
//! the [`OtelGuard`] that flushes on shutdown.

use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider,
    propagation::TraceContextPropagator, trace::SdkTracerProvider, Resource,
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initialize the full observability stack.
///
/// - Always: `tracing-subscriber` with JSON formatting + env filter
/// - When `otlp_endpoint` is non-empty: adds OTel trace, metrics, and log
///   exporters via OTLP/gRPC
///
/// Returns an [`OtelGuard`] that flushes and shuts down providers on drop.
pub fn init_telemetry(service_name: &str, otlp_endpoint: &str) -> OtelGuard {
    init_telemetry_with_sampling(service_name, otlp_endpoint, "", 0.01)
}

/// Initialize the full observability stack with a configurable trace sampling rate.
///
/// `trace_sample_rate` controls the fraction of traces exported (0.0–1.0).
/// Use `1.0` to capture all traces (development/debugging) or `0.01` for 1%
/// sampling in production.
pub fn init_telemetry_with_sampling(
    service_name: &str,
    otlp_endpoint: &str,
    traces_otlp_endpoint: &str,
    trace_sample_rate: f64,
) -> OtelGuard {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("sqe=info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true);

    let trace_endpoint = if traces_otlp_endpoint.is_empty() { otlp_endpoint } else { traces_otlp_endpoint };

    if trace_endpoint.is_empty() {
        // No OTel — just structured JSON logs
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init();

        return OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        };
    }

    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();

    // ── Traces ───────────────────────────────────────────────
    let trace_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(trace_endpoint)
        .build()
        .expect("Failed to create OTLP span exporter");

    // Use ParentBased sampling: if the parent span is sampled, always sample
    // the child; otherwise use the configured ratio for root spans.
    let sampler = opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
        opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(trace_sample_rate),
    ));
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(sampler)
        .with_batch_exporter(trace_exporter)
        .build();

    let tracer = tracer_provider.tracer(service_name.to_string());
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Register W3C TraceContext propagator so inject/extract helpers work
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otel_trace_layer = OpenTelemetryLayer::new(tracer);

    // Logs and metrics retain the legacy all-signals endpoint. Configuring
    // only traces_otlp_endpoint avoids sending unsupported signals to a
    // trace-only collector pipeline.
    let (logger_provider, meter_provider) = if otlp_endpoint.is_empty() {
        (None, None)
    } else {
        let log_exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint)
            .build()
            .expect("Failed to create OTLP log exporter");
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(log_exporter)
            .build();
        let metric_exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint)
            .build()
            .expect("Failed to create OTLP metric exporter");
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(metric_exporter)
            .build();
        opentelemetry::global::set_meter_provider(meter_provider.clone());
        (Some(logger_provider), Some(meter_provider))
    };

    let otel_log_layer = logger_provider.as_ref().map(|provider| {
        let filter = EnvFilter::new("info")
            .add_directive("hyper=off".parse().unwrap())
            .add_directive("tonic=off".parse().unwrap())
            .add_directive("h2=off".parse().unwrap())
            .add_directive("reqwest=off".parse().unwrap())
            .add_directive("tower=off".parse().unwrap())
            .add_directive("tower_http=off".parse().unwrap());
        OpenTelemetryTracingBridge::new(provider).with_filter(filter)
    });

    // ── Compose subscriber ───────────────────────────────────
    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init();

    tracing::info!(
        traces_otlp_endpoint = trace_endpoint,
        all_signals_otlp_endpoint = otlp_endpoint,
        service = service_name,
        logs_and_metrics = !otlp_endpoint.is_empty(),
        "OpenTelemetry initialized"
    );

    OtelGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider,
        logger_provider,
    }
}

/// RAII guard that shuts down OTel providers on drop.
pub struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Shutdown order: meter → tracer → logger (logger last so flush logs are captured)
        if let Some(mp) = self.meter_provider.take() {
            let _ = mp.shutdown();
        }
        if let Some(tp) = self.tracer_provider.take() {
            let _ = tp.shutdown();
        }
        if let Some(lp) = self.logger_provider.take() {
            let _ = lp.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(self.0.clone())
        }
    }

    #[test]
    fn test_guard_drop_without_otel() {
        let guard = OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        };
        drop(guard);
    }

    #[test]
    fn json_events_include_trace_span_and_safe_correlation_fields() {
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId,
        };
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let capture = Capture::default();
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("json-correlation-test");
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_span_list(true)
                    .with_writer(capture.clone()),
            )
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let remote = SpanContext::new(
                TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
                SpanId::from_hex("b7ad6b7169203331").unwrap(),
                TraceFlags::SAMPLED,
                true,
                Default::default(),
            );
            let span = tracing::info_span!(
                "flight_sql.request",
                trace_id = tracing::field::Empty,
                span_id = tracing::field::Empty,
                request_id = "request-42",
                session_id = "session-42",
                query_id = "query-42",
            );
            span.set_parent(opentelemetry::Context::new().with_remote_span_context(remote))
                .unwrap();
            crate::propagation::record_trace_fields(&span);
            span.in_scope(|| tracing::info!("ordinary tracing event"));
        });

        let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(output.contains("0af7651916cd43dd8448eb211c80319c"));
        assert!(output.contains("\"span_id\":"));
        assert!(output.contains("request-42"));
        assert!(output.contains("session-42"));
        assert!(output.contains("query-42"));
        provider.shutdown().unwrap();
    }
}

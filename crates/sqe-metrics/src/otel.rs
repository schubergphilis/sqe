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
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("sqe=info"));

    let fmt_layer = tracing_subscriber::fmt::layer().json();

    if otlp_endpoint.is_empty() {
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
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("Failed to create OTLP span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(trace_exporter)
        .build();

    let tracer = tracer_provider.tracer(service_name.to_string());
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Register W3C TraceContext propagator so inject/extract helpers work
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otel_trace_layer = OpenTelemetryLayer::new(tracer);

    // ── Logs ─────────────────────────────────────────────────
    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("Failed to create OTLP log exporter");

    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();

    // Filter to prevent telemetry-induced-telemetry loops
    let otel_log_filter = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap())
        .add_directive("tower=off".parse().unwrap())
        .add_directive("tower_http=off".parse().unwrap());

    let otel_log_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_log_filter);

    // ── Metrics ──────────────────────────────────────────────
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

    // ── Compose subscriber ───────────────────────────────────
    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init();

    tracing::info!(
        otlp_endpoint = otlp_endpoint,
        service = service_name,
        "OpenTelemetry initialized (traces + metrics + logs)"
    );

    OtelGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
        logger_provider: Some(logger_provider),
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

    #[test]
    fn test_guard_drop_without_otel() {
        let guard = OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        };
        drop(guard);
    }
}

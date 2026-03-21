//! Trace context propagation helpers for Arrow Flight / gRPC metadata.
//!
//! Provides [`inject_trace_context`] and [`extract_trace_context`] to carry
//! W3C TraceContext (`traceparent` / `tracestate`) across coordinator-worker
//! boundaries via tonic [`MetadataMap`].

use opentelemetry::propagation::{Extractor, Injector};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};

/// Adapter that implements [`Injector`] for tonic's [`MetadataMap`].
///
/// Used by the coordinator to inject the current trace context into
/// outgoing gRPC (Arrow Flight) request metadata.
pub struct MetadataInjector<'a>(pub &'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            MetadataKey::from_bytes(key.as_bytes()),
            MetadataValue::try_from(&value),
        ) {
            self.0.insert(k, v);
        }
    }
}

/// Adapter that implements [`Extractor`] for tonic's [`MetadataMap`].
///
/// Used by workers to extract the trace context from incoming gRPC
/// (Arrow Flight) request metadata.
pub struct MetadataExtractor<'a>(pub &'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(key) => Some(key.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Inject the current trace context into a tonic [`MetadataMap`].
///
/// Uses the globally registered [`TextMapPropagator`] (typically
/// [`TraceContextPropagator`]) to serialize `traceparent` and `tracestate`
/// headers from the provided OpenTelemetry [`Context`].
///
/// # Example
///
/// ```ignore
/// let mut request = tonic::Request::new(ticket);
/// let cx = tracing::Span::current().context();  // from OpenTelemetrySpanExt
/// inject_trace_context(&cx, request.metadata_mut());
/// ```
pub fn inject_trace_context(cx: &opentelemetry::Context, metadata: &mut MetadataMap) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(cx, &mut MetadataInjector(metadata));
    });
}

/// Extract the trace context from a tonic [`MetadataMap`].
///
/// Uses the globally registered [`TextMapPropagator`] to deserialize
/// `traceparent` and `tracestate` headers into an OpenTelemetry [`Context`].
///
/// # Example
///
/// ```ignore
/// let parent_cx = extract_trace_context(request.metadata());
/// let span = tracing::info_span!("worker_do_get");
/// span.set_parent(parent_cx);
/// ```
pub fn extract_trace_context(metadata: &MetadataMap) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(metadata))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId};
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    /// Install a TraceContextPropagator for the duration of these tests.
    fn install_propagator() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    }

    #[test]
    fn test_inject_and_extract_roundtrip() {
        install_propagator();

        // Build a context with a known span context
        let span_context = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            TraceFlags::SAMPLED,
            true,
            Default::default(),
        );
        let cx =
            opentelemetry::Context::new().with_remote_span_context(span_context.clone());

        // Inject into metadata
        let mut metadata = MetadataMap::new();
        inject_trace_context(&cx, &mut metadata);

        // traceparent must be present
        let tp = metadata
            .get("traceparent")
            .expect("traceparent header should be set");
        let tp_str = tp.to_str().unwrap();
        assert!(
            tp_str.contains("0af7651916cd43dd8448eb211c80319c"),
            "traceparent should contain trace-id: {tp_str}"
        );
        assert!(
            tp_str.contains("b7ad6b7169203331"),
            "traceparent should contain span-id: {tp_str}"
        );

        // Extract back
        let extracted_cx = extract_trace_context(&metadata);
        let extracted_span = extracted_cx.span().span_context().clone();

        assert_eq!(extracted_span.trace_id(), span_context.trace_id());
        assert_eq!(extracted_span.span_id(), span_context.span_id());
        assert!(extracted_span.trace_flags().is_sampled());
    }

    #[test]
    fn test_extract_empty_metadata_returns_empty_context() {
        install_propagator();

        let metadata = MetadataMap::new();
        let cx = extract_trace_context(&metadata);

        // No span context should be present
        assert!(!cx.span().span_context().is_valid());
    }

    #[test]
    fn test_metadata_injector_set() {
        let mut metadata = MetadataMap::new();
        {
            let mut injector = MetadataInjector(&mut metadata);
            injector.set("traceparent", "00-abc-def-01".to_string());
        }
        assert_eq!(
            metadata.get("traceparent").unwrap().to_str().unwrap(),
            "00-abc-def-01"
        );
    }

    #[test]
    fn test_metadata_extractor_get() {
        let mut metadata = MetadataMap::new();
        metadata.insert("traceparent", "00-abc-def-01".parse().unwrap());

        let extractor = MetadataExtractor(&metadata);
        assert_eq!(extractor.get("traceparent"), Some("00-abc-def-01"));
        assert_eq!(extractor.get("missing"), None);
    }

    #[test]
    fn test_metadata_extractor_keys() {
        let mut metadata = MetadataMap::new();
        metadata.insert("traceparent", "value1".parse().unwrap());
        metadata.insert("tracestate", "value2".parse().unwrap());

        let extractor = MetadataExtractor(&metadata);
        let keys = extractor.keys();
        assert!(keys.contains(&"traceparent"));
        assert!(keys.contains(&"tracestate"));
    }
}

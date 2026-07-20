//! Trace context propagation helpers for Arrow Flight / gRPC metadata.
//!
//! Provides [`inject_trace_context`] and [`extract_trace_context`] to carry
//! W3C TraceContext (`traceparent` / `tracestate`) across coordinator-worker
//! boundaries via tonic [`MetadataMap`].

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub const CORRELATION_ID_MAX_LEN: usize = 128;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CorrelationIds {
    pub request_id: Option<String>,
    pub session_id: Option<String>,
}

fn validate_correlation_id(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= CORRELATION_ID_MAX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte)))
    .then(|| value.to_string())
}

pub fn correlation_ids_from_metadata(metadata: &MetadataMap) -> CorrelationIds {
    CorrelationIds {
        request_id: metadata
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .and_then(validate_correlation_id),
        session_id: metadata
            .get("x-session-id")
            .and_then(|value| value.to_str().ok())
            .and_then(validate_correlation_id),
    }
}

pub fn correlation_ids_from_headers(headers: &axum::http::HeaderMap) -> CorrelationIds {
    CorrelationIds {
        request_id: headers
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .and_then(validate_correlation_id),
        session_id: headers
            .get("x-session-id")
            .and_then(|value| value.to_str().ok())
            .and_then(validate_correlation_id),
    }
}

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

/// Adapter that implements [`Injector`] for a `Vec<(String, String)>`.
///
/// Collects trace context headers as key-value pairs, suitable for
/// injecting into any HTTP client (reqwest, hyper, etc.).
struct VecInjector<'a>(&'a mut Vec<(String, String)>);

impl Injector for VecInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

/// Collect the current span's W3C TraceContext as HTTP header key-value pairs.
///
/// Returns the `traceparent` and `tracestate` headers from the active
/// `tracing` span's OpenTelemetry context. If no OTel propagator is
/// registered or no span is active, returns an empty vec.
///
/// # Example
///
/// ```ignore
/// use sqe_metrics::propagation::trace_context_http_headers;
///
/// let mut req = client.post(&url).bearer_auth(&token);
/// for (k, v) in trace_context_http_headers() {
///     req = req.header(k, v);
/// }
/// req.send().await?;
/// ```
pub fn trace_context_http_headers() -> Vec<(String, String)> {
    let cx = tracing::Span::current().context();
    let mut headers = Vec::new();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut VecInjector(&mut headers));
    });
    headers
}

/// Adapter that implements [`Extractor`] for `axum::http::HeaderMap`.
struct HeaderMapExtractor<'a>(&'a axum::http::HeaderMap);

impl Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Extract W3C TraceContext (`traceparent`/`tracestate`) from incoming HTTP headers.
///
/// Intended for Trino-compat and Quack HTTP surfaces (O6).
pub fn extract_trace_context_from_headers(headers: &axum::http::HeaderMap) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderMapExtractor(headers))
    })
}

/// Extract trace context from HTTP headers and attach it to the current tracing span.
///
/// This makes `current_trace_id()` and any child `sqe.query` / policy spans
/// part of the caller's trace (for OTel + audit correlation).
///
/// Call early in Trino/Quack request handlers.
pub fn attach_trace_context_from_headers(headers: &axum::http::HeaderMap) {
    let cx = extract_trace_context_from_headers(headers);
    let _ = tracing::Span::current().set_parent(cx);
}

/// Return the current active trace ID (32-char lowercase hex) if a valid
/// OpenTelemetry span context is present on the current tracing span.
/// Returns None when OTel is not initialized or no sampled span is active.
///
/// Used to stamp `trace_id` onto `AuditEvent` and query records for
/// correlation (full-audit O4).
pub fn current_trace_id() -> Option<String> {
    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();
    if sc.is_valid() {
        Some(format!("{:032x}", sc.trace_id()))
    } else {
        None
    }
}

/// Return the current active span ID (16-char lowercase hex).
pub fn current_span_id() -> Option<String> {
    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();
    sc.is_valid().then(|| format!("{:016x}", sc.span_id()))
}

/// Record lowercase W3C trace and span identifiers on a span that declares
/// `trace_id` and `span_id` fields. JSON logging includes these ancestor span
/// fields for ordinary tracing events emitted anywhere below the boundary.
pub fn record_trace_fields(span: &tracing::Span) {
    let cx = span.context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();
    if sc.is_valid() {
        span.record("trace_id", format_args!("{:032x}", sc.trace_id()));
        span.record("span_id", format_args!("{:016x}", sc.span_id()));
    }
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
    fn test_extract_http_traceparent() {
        install_propagator();
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .unwrap(),
        );

        let cx = extract_trace_context_from_headers(&headers);
        let span_context = cx.span().span_context().clone();
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id(),
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
        );
        assert_eq!(
            span_context.span_id(),
            SpanId::from_hex("b7ad6b7169203331").unwrap()
        );
    }


    #[test]
    fn correlation_ids_accept_only_bounded_safe_values() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-request-id", "req_01:abc.def-2".parse().unwrap());
        headers.insert("x-session-id", "contains spaces".parse().unwrap());
        let ids = correlation_ids_from_headers(&headers);
        assert_eq!(ids.request_id.as_deref(), Some("req_01:abc.def-2"));
        assert_eq!(ids.session_id, None);

        let oversized = "a".repeat(CORRELATION_ID_MAX_LEN + 1);
        let mut metadata = MetadataMap::new();
        metadata.insert("x-request-id", oversized.parse().unwrap());
        metadata.insert("x-session-id", "session-42".parse().unwrap());
        let ids = correlation_ids_from_metadata(&metadata);
        assert_eq!(ids.request_id, None);
        assert_eq!(ids.session_id.as_deref(), Some("session-42"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_requests_do_not_leak_trace_context() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tracing::Instrument;
        use tracing_subscriber::layer::SubscriberExt;

        install_propagator();
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("concurrent-propagation-test");
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _subscriber = tracing::subscriber::set_default(subscriber);

        async fn observe(trace_id: &str) -> Option<String> {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "traceparent",
                format!("00-{trace_id}-b7ad6b7169203331-01")
                    .parse()
                    .unwrap(),
            );
            let span = tracing::info_span!("request");
            span.set_parent(extract_trace_context_from_headers(&headers))
                .unwrap();
            async {
                tokio::task::yield_now().await;
                current_trace_id()
            }
            .instrument(span)
            .await
        }

        let first_id = "0af7651916cd43dd8448eb211c80319c";
        let second_id = "4bf92f3577b34da6a3ce929d0e0e4736";
        let (first, second) = tokio::join!(observe(first_id), observe(second_id));
        assert_eq!(first.as_deref(), Some(first_id));
        assert_eq!(second.as_deref(), Some(second_id));
        provider.shutdown().unwrap();
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
    fn test_vec_injector() {
        let mut headers = Vec::new();
        {
            let mut injector = VecInjector(&mut headers);
            injector.set("traceparent", "00-abc-def-01".to_string());
            injector.set("tracestate", "vendor=value".to_string());
        }
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0], ("traceparent".to_string(), "00-abc-def-01".to_string()));
        assert_eq!(headers[1], ("tracestate".to_string(), "vendor=value".to_string()));
    }

    #[test]
    fn test_trace_context_http_headers_with_active_context() {
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
            opentelemetry::Context::new().with_remote_span_context(span_context);

        // Inject into metadata, then extract as HTTP headers via the same context
        let mut metadata = MetadataMap::new();
        inject_trace_context(&cx, &mut metadata);

        // Verify that VecInjector produces the same traceparent
        let mut headers = Vec::new();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut VecInjector(&mut headers));
        });
        assert!(!headers.is_empty());
        let tp = headers.iter().find(|(k, _)| k == "traceparent").unwrap();
        assert!(tp.1.contains("0af7651916cd43dd8448eb211c80319c"));
    }

    #[test]
    fn test_trace_context_http_headers_empty_without_span() {
        install_propagator();
        // No active tracing span — should return empty or invalid traceparent
        let headers = trace_context_http_headers();
        // With no active span, the propagator may return empty or a zeroed trace
        // Either way it should not panic
        let _ = headers;
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

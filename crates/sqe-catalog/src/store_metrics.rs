//! Object-store request metrics (#815), per scheme and bucket.
//!
//! SQE reaches object storage through two clients:
//!
//! - **OpenDAL**, for every Iceberg `FileIO` (metadata, manifests, and the
//!   coordinator's own scans). The vendored storage crate ships a Prometheus
//!   layer; [`install_storage_metrics`] turns it on, which exports the
//!   `opendal_operation_*` and `opendal_http_*` families labelled by
//!   `scheme`, `namespace` (bucket) and `root`.
//! - **object_store**, for worker data-file scans, Hive roots and the file
//!   TVFs. It has no metrics hook, so [`metered`] wraps a store in
//!   [`MeteredStore`], which exports the `sqe_object_store_*` families below.
//!
//! Both are process-wide and opt-in: until a server calls
//! [`install_storage_metrics`], [`metered`] returns the store unchanged and
//! OpenDAL operators carry no layer, so the embedded CLI and unit tests pay
//! nothing. The first registry installed wins, like the OpenDAL layer: the
//! counters are registered once and every wrapped store shares them.
//!
//! The object_store client retries inside the request, so a retry is not a
//! separate request here; on the OpenDAL side `opendal_http_request_*`
//! counts each HTTP attempt and `opendal_operation_*` each operation, and
//! their difference is the retry count.
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};

/// Request-duration buckets in seconds: object-store latency runs from a
/// few milliseconds (same-zone GET) to tens of seconds (a stalled LIST).
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

struct StoreMetrics {
    requests: IntCounterVec,
    errors: IntCounterVec,
    duration: HistogramVec,
    ttfb: HistogramVec,
}

static METRICS: OnceLock<StoreMetrics> = OnceLock::new();

/// Turns on object-store metrics for this process and registers them in
/// `registry`: the OpenDAL layer for Iceberg I/O and the
/// `sqe_object_store_*` families for object_store clients.
///
/// Call it once at startup, before any catalog or store is built: OpenDAL
/// caches operators, and one built before the call never carries the layer.
/// A second call is a no-op.
pub fn install_storage_metrics(registry: &Registry) -> Result<(), String> {
    iceberg_storage_opendal::install_prometheus_metrics(registry)
        .map_err(|e| format!("OpenDAL metrics: {e}"))?;
    if METRICS.get().is_some() {
        return Ok(());
    }
    let metrics = register(registry).map_err(|e| format!("object-store metrics: {e}"))?;
    let _ = METRICS.set(metrics);
    Ok(())
}

fn register(registry: &Registry) -> prometheus::Result<StoreMetrics> {
    let labels = ["scheme", "bucket", "operation"];
    let requests = IntCounterVec::new(
        Opts::new(
            "sqe_object_store_requests_total",
            "Object-store requests made through object_store clients (worker scans, Hive, file TVFs)",
        ),
        &labels,
    )?;
    let errors = IntCounterVec::new(
        Opts::new(
            "sqe_object_store_errors_total",
            "Object-store requests that failed, by error class",
        ),
        &["scheme", "bucket", "operation", "error"],
    )?;
    let duration = HistogramVec::new(
        HistogramOpts::new(
            "sqe_object_store_request_duration_seconds",
            "Object-store request duration; for get, until the response headers arrive",
        )
        .buckets(DURATION_BUCKETS.to_vec()),
        &labels,
    )?;
    let ttfb = HistogramVec::new(
        HistogramOpts::new(
            "sqe_object_store_ttfb_seconds",
            "Time from issuing a streamed get to its first body byte",
        )
        .buckets(DURATION_BUCKETS.to_vec()),
        &["scheme", "bucket"],
    )?;
    registry.register(Box::new(requests.clone()))?;
    registry.register(Box::new(errors.clone()))?;
    registry.register(Box::new(duration.clone()))?;
    registry.register(Box::new(ttfb.clone()))?;
    Ok(StoreMetrics {
        requests,
        errors,
        duration,
        ttfb,
    })
}

/// `store` wrapped in a [`MeteredStore`] labelled `scheme` / `bucket`, or
/// `store` itself when metrics are not installed or it is already metered.
pub fn metered(store: Arc<dyn ObjectStore>, scheme: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    if METRICS.get().is_none() || is_metered(store.as_ref()) {
        return store;
    }
    Arc::new(MeteredStore {
        inner: store,
        scheme: scheme.to_string(),
        bucket: bucket.to_string(),
    })
}

/// [`metered`] with the labels taken from a registry key such as
/// `s3://bucket` or `https://host:port`.
pub fn metered_for_url(store: Arc<dyn ObjectStore>, url: &url::Url) -> Arc<dyn ObjectStore> {
    let bucket = match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        (None, _) => String::new(),
    };
    metered(store, url.scheme(), &bucket)
}

/// `ObjectStore` has no `Any` bound, so a store that went through a registry
/// and is registered again is recognised by its `Debug` prefix; without the
/// check every request would count twice.
fn is_metered(store: &dyn ObjectStore) -> bool {
    format!("{store:?}").starts_with(METERED_DEBUG_PREFIX)
}

const METERED_DEBUG_PREFIX: &str = "MeteredStore {";

/// An [`ObjectStore`] that counts requests, errors and durations of the store
/// it wraps. Its `Display` is the inner store's, so log lines do not change.
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    scheme: String,
    bucket: String,
}

impl fmt::Debug for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{METERED_DEBUG_PREFIX} scheme: {:?}, bucket: {:?}, inner: {:?} }}",
            self.scheme, self.bucket, self.inner
        )
    }
}

impl fmt::Display for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

/// Error class label: the `object_store::Error` variant.
fn error_class(error: &object_store::Error) -> &'static str {
    use object_store::Error as E;
    match error {
        E::NotFound { .. } => "not_found",
        E::PermissionDenied { .. } => "permission_denied",
        E::Unauthenticated { .. } => "unauthenticated",
        E::Precondition { .. } => "precondition",
        E::NotModified { .. } => "not_modified",
        E::AlreadyExists { .. } => "already_exists",
        E::NotSupported { .. } | E::NotImplemented { .. } => "not_supported",
        E::InvalidPath { .. } => "invalid_path",
        E::Generic { .. } => "generic",
        _ => "other",
    }
}

impl MeteredStore {
    /// Counts one request; the returned guard records its duration and, on
    /// error, its class.
    fn start(&self, operation: &'static str) -> Option<Request<'_>> {
        let metrics = METRICS.get()?;
        metrics
            .requests
            .with_label_values(&[&self.scheme, &self.bucket, operation])
            .inc();
        Some(Request {
            store: self,
            metrics,
            operation,
            started: Instant::now(),
        })
    }

    /// Counts one streamed request (list, delete) and every error item it
    /// yields; a stream has no single duration.
    fn count_stream<T: Send + 'static>(
        &self,
        operation: &'static str,
        stream: BoxStream<'static, object_store::Result<T>>,
    ) -> BoxStream<'static, object_store::Result<T>> {
        let Some(metrics) = METRICS.get() else {
            return stream;
        };
        metrics
            .requests
            .with_label_values(&[&self.scheme, &self.bucket, operation])
            .inc();
        let errors = metrics.errors.clone();
        let (scheme, bucket) = (self.scheme.clone(), self.bucket.clone());
        stream
            .inspect(move |item| {
                if let Err(e) = item {
                    errors
                        .with_label_values(&[&scheme, &bucket, operation, error_class(e)])
                        .inc();
                }
            })
            .boxed()
    }
}

struct Request<'a> {
    store: &'a MeteredStore,
    metrics: &'static StoreMetrics,
    operation: &'static str,
    started: Instant,
}

impl Request<'_> {
    fn finish<T>(self, result: object_store::Result<T>) -> object_store::Result<T> {
        let MeteredStore { scheme, bucket, .. } = self.store;
        self.metrics
            .duration
            .with_label_values(&[scheme, bucket, self.operation])
            .observe(self.started.elapsed().as_secs_f64());
        if let Err(e) = &result {
            self.metrics
                .errors
                .with_label_values(&[scheme, bucket, self.operation, error_class(e)])
                .inc();
        }
        result
    }
}

/// Runs `call` as one metered request when metrics are installed.
macro_rules! metered_call {
    ($self:ident, $operation:literal, $call:expr) => {
        match $self.start($operation) {
            Some(request) => request.finish($call.await),
            None => $call.await,
        }
    };
}

#[async_trait]
impl ObjectStore for MeteredStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        metered_call!(self, "put", self.inner.put_opts(location, payload, opts))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        metered_call!(
            self,
            "put_multipart",
            self.inner.put_multipart_opts(location, opts)
        )
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let operation = if options.head { "head" } else { "get" };
        let Some(request) = self.start(operation) else {
            return self.inner.get_opts(location, options).await;
        };
        let started = request.started;
        let mut result = request.finish(self.inner.get_opts(location, options).await)?;
        if operation == "get" {
            result.payload = self.time_first_byte(result.payload, started);
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        metered_call!(self, "get_ranges", self.inner.get_ranges(location, ranges))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.count_stream("delete", self.inner.delete_stream(locations))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.count_stream("list", self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.count_stream("list", self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        metered_call!(
            self,
            "list_with_delimiter",
            self.inner.list_with_delimiter(prefix)
        )
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        metered_call!(self, "copy", self.inner.copy_opts(from, to, options))
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        metered_call!(self, "rename", self.inner.rename_opts(from, to, options))
    }
}

impl MeteredStore {
    /// Records the time from issuing the get to its first body chunk. A local
    /// file payload has no network first byte and is left as is.
    fn time_first_byte(&self, payload: GetResultPayload, started: Instant) -> GetResultPayload {
        let (metrics, stream) = match (METRICS.get(), payload) {
            (Some(metrics), GetResultPayload::Stream(stream)) => (metrics, stream),
            (_, payload) => return payload,
        };
        let ttfb = metrics
            .ttfb
            .with_label_values(&[&self.scheme, &self.bucket]);
        let mut first = true;
        GetResultPayload::Stream(
            stream
                .inspect(move |_| {
                    if std::mem::take(&mut first) {
                        ttfb.observe(started.elapsed().as_secs_f64());
                    }
                })
                .boxed(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    /// Value of `family{labels}`, summing counters and histogram sample
    /// counts; 0 when absent.
    fn sample(registry: &Registry, family: &str, labels: &[(&str, &str)]) -> f64 {
        registry
            .gather()
            .iter()
            .filter(|f| f.name() == family)
            .flat_map(|f| {
                let counter = f.get_field_type() == prometheus::proto::MetricType::COUNTER;
                f.get_metric().iter().map(move |m| (counter, m))
            })
            .filter(|(_, m)| {
                labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                })
            })
            .map(|(counter, m)| match counter {
                true => m.get_counter().value(),
                false => m.get_histogram().get_sample_count() as f64,
            })
            .sum()
    }

    // The only test in this binary that installs: the first registry
    // installed wins for the whole process.
    #[tokio::test]
    async fn installed_metrics_count_object_store_and_opendal_requests() {
        let registry = Registry::new();
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            !is_metered(metered(Arc::clone(&raw), "s3", "b").as_ref()),
            "nothing is metered before install"
        );

        install_storage_metrics(&registry).unwrap();
        install_storage_metrics(&registry).expect("a second install is a no-op");

        let store = metered(raw, "s3", "lake");
        let again = metered(Arc::clone(&store), "s3", "lake");
        assert!(
            Arc::ptr_eq(&store, &again),
            "a metered store is not wrapped twice"
        );

        let path = Path::from("t/data.parquet");
        store
            .put(&path, Bytes::from_static(b"abcdef").into())
            .await
            .unwrap();
        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(body.as_ref(), b"abcdef");
        store.head(&path).await.unwrap();
        store.get_ranges(&path, &[0..2, 3..5]).await.unwrap();
        assert!(store.get(&Path::from("t/missing")).await.is_err());
        let listed: Vec<_> = store.list(Some(&Path::from("t"))).collect().await;
        assert_eq!(listed.len(), 1);

        let lake = [("scheme", "s3"), ("bucket", "lake")];
        let requests = "sqe_object_store_requests_total";
        let op = |name| [lake[0], lake[1], ("operation", name)];
        assert_eq!(sample(&registry, requests, &op("put")), 1.0);
        assert_eq!(sample(&registry, requests, &op("get")), 2.0);
        assert_eq!(sample(&registry, requests, &op("head")), 1.0);
        assert_eq!(sample(&registry, requests, &op("get_ranges")), 1.0);
        assert_eq!(sample(&registry, requests, &op("list")), 1.0);
        let not_found = [
            lake[0],
            lake[1],
            ("operation", "get"),
            ("error", "not_found"),
        ];
        assert_eq!(
            sample(&registry, "sqe_object_store_errors_total", &not_found),
            1.0
        );
        let durations = "sqe_object_store_request_duration_seconds";
        assert_eq!(sample(&registry, durations, &op("get")), 2.0);
        assert_eq!(
            sample(&registry, "sqe_object_store_ttfb_seconds", &lake),
            1.0
        );

        // Iceberg I/O: an operator built after install carries the OpenDAL
        // layer.
        let dir = tempfile::tempdir().unwrap();
        let file_io = iceberg::io::FileIOBuilder::new(crate::storage_factory()).build();
        let location = format!("file://{}/m.avro", dir.path().display());
        let output = file_io.new_output(&location).unwrap();
        output.write(Bytes::from_static(b"manifest")).await.unwrap();
        let read = file_io.new_input(&location).unwrap().read().await.unwrap();
        assert_eq!(read.as_ref(), b"manifest");
        let opendal = sample(&registry, "opendal_operation_duration_seconds", &[]);
        assert!(opendal >= 2.0, "OpenDAL operations recorded: {opendal}");
    }
}

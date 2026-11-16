//! The `x-sqe-snapshot-id` gRPC trailer on a Flight SQL ingest (#904).
//!
//! arrow-flight's `FlightSqlService::do_put` wraps the `i64` that
//! `do_put_statement_ingest` returns into `DoPutUpdateResult { record_count }`
//! and has no field for anything else, so the snapshot an ingest committed
//! travels as trailing metadata instead. [`SnapshotTrailerLayer`] puts an
//! empty [`SnapshotIdSlot`] into the extensions of every `DoPut` request; the
//! ingest handler fills it after the commit, and the layer adds the id to the
//! response trailers, next to `grpc-status`.
//!
//! No trailer is sent when the slot stays empty: a failed ingest, an ingest
//! inside a transaction (its snapshot is on the transaction branch until
//! COMMIT), a replace (see `IngestOutcome`), an empty append that committed
//! nothing, and every other `DoPut` command.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::codegen::http;
use tower::{Layer, Service};

/// The trailer that carries the committed snapshot id, as a decimal `i64`.
pub const SNAPSHOT_ID_TRAILER: &str = "x-sqe-snapshot-id";

/// The gRPC path of `FlightService.DoPut`.
const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";

/// Where a `DoPut` handler leaves the snapshot id it committed. Shared
/// between the request's extensions and the response body, so the handler
/// sets it before the response is built and the body reads it at the end.
#[derive(Clone, Debug, Default)]
pub struct SnapshotIdSlot(Arc<OnceLock<i64>>);

impl SnapshotIdSlot {
    /// Record the committed snapshot. The first value wins.
    pub fn set(&self, snapshot_id: i64) {
        let _ = self.0.set(snapshot_id);
    }

    fn get(&self) -> Option<i64> {
        self.0.get().copied()
    }
}

/// Adds the [`SNAPSHOT_ID_TRAILER`] to `DoPut` responses whose handler filled
/// the [`SnapshotIdSlot`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SnapshotTrailerLayer;

impl<S> Layer<S> for SnapshotTrailerLayer {
    type Service = SnapshotTrailerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SnapshotTrailerService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotTrailerService<S> {
    inner: S,
}

type ResponseFuture<E> =
    Pin<Box<dyn Future<Output = Result<http::Response<tonic::body::Body>, E>> + Send>>;

impl<S, B> Service<http::Request<B>> for SnapshotTrailerService<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        if request.uri().path() != DO_PUT_PATH {
            return Box::pin(self.inner.call(request));
        }
        let slot = SnapshotIdSlot::default();
        request.extensions_mut().insert(slot.clone());
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            Ok(response.map(|inner| tonic::body::Body::new(TrailerBody { inner, slot })))
        })
    }
}

/// The response body with the snapshot id added to its trailers frame.
struct TrailerBody {
    inner: tonic::body::Body,
    slot: SnapshotIdSlot,
}

impl HttpBody for TrailerBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                Poll::Ready(Some(Ok(with_snapshot_trailer(frame, &this.slot))))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// `frame` with the snapshot id added when it is the trailers frame and the
/// slot holds an id; any other frame unchanged.
fn with_snapshot_trailer(frame: Frame<Bytes>, slot: &SnapshotIdSlot) -> Frame<Bytes> {
    let Some(snapshot_id) = slot.get() else {
        return frame;
    };
    match frame.into_trailers() {
        Ok(mut trailers) => {
            trailers.insert(SNAPSHOT_ID_TRAILER, http::HeaderValue::from(snapshot_id));
            Frame::trailers(trailers)
        }
        Err(frame) => frame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use tower::ServiceExt;

    /// A body that yields one data frame, then a `grpc-status: 0` trailers
    /// frame, the way tonic ends a successful unary-like stream.
    struct GrpcBody(VecDeque<Frame<Bytes>>);

    impl GrpcBody {
        fn new() -> Self {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            Self(VecDeque::from([
                Frame::data(Bytes::from_static(b"put-result")),
                Frame::trailers(trailers),
            ]))
        }
    }

    impl HttpBody for GrpcBody {
        type Data = Bytes;
        type Error = tonic::Status;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
            Poll::Ready(self.get_mut().0.pop_front().map(Ok))
        }
    }

    /// Call the layered service on `path`; the inner service plays the
    /// handler and fills the slot with `commit` when the layer provided one.
    /// Returns the trailers the client would read.
    async fn trailers_after(path: &str, commit: Option<i64>) -> http::HeaderMap {
        let service = SnapshotTrailerLayer.layer(tower::service_fn(
            move |request: http::Request<()>| async move {
                if let (Some(slot), Some(id)) =
                    (request.extensions().get::<SnapshotIdSlot>(), commit)
                {
                    slot.set(id);
                }
                Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::new(
                    GrpcBody::new(),
                )))
            },
        ));
        let request = http::Request::builder().uri(path).body(()).unwrap();
        let mut body = service.oneshot(request).await.unwrap().into_body();
        let mut trailers = None;
        while let Some(frame) =
            futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
        {
            if let Ok(t) = frame.unwrap().into_trailers() {
                trailers = Some(t);
            }
        }
        trailers.expect("a trailers frame")
    }

    #[tokio::test]
    async fn a_committed_do_put_carries_the_snapshot_id_trailer() {
        let trailers = trailers_after(DO_PUT_PATH, Some(4_611_686_018_427_387_904)).await;
        assert_eq!(trailers["grpc-status"], "0", "tonic's status is kept");
        assert_eq!(trailers[SNAPSHOT_ID_TRAILER], "4611686018427387904");
    }

    #[tokio::test]
    async fn no_trailer_without_a_commit_or_outside_do_put() {
        let uncommitted = trailers_after(DO_PUT_PATH, None).await;
        assert!(uncommitted.get(SNAPSHOT_ID_TRAILER).is_none());
        assert_eq!(uncommitted["grpc-status"], "0");

        // Other RPCs get no slot, so a handler there cannot set one.
        let do_get = trailers_after("/arrow.flight.protocol.FlightService/DoGet", Some(7)).await;
        assert!(do_get.get(SNAPSHOT_ID_TRAILER).is_none());
    }
}

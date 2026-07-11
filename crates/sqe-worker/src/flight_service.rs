use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, info_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use datafusion::prelude::SessionContext;
use sqe_catalog::FooterCache;
use sqe_core::FlightCompression;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_metrics::propagation::extract_trace_context;
use sqe_planner::ScanTask;

use crate::credential_channel::{CredentialStore, RefreshableCredentials};
use crate::executor;
use crate::shuffle::{ExchangeDescriptor, ShuffleManager};

/// Build [`IpcWriteOptions`] for a given compression setting.
fn ipc_options_for(compression: FlightCompression) -> Result<IpcWriteOptions, Status> {
    let codec = match compression {
        FlightCompression::None => None,
        FlightCompression::Lz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        FlightCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
    };
    IpcWriteOptions::default()
        .try_with_compression(codec)
        .map_err(|e| Status::internal(format!("Failed to set IPC compression: {e}")))
}

/// Metadata header carrying the shared coordinator/worker secret.
/// Same name as the coordinator's heartbeat handler so a single rotation
/// covers both directions.
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Metadata header carrying the HMAC-SHA256 tag (hex) over the ScanTask ticket
/// bytes (issue #206). The worker recomputes the tag over the received bytes
/// and constant-time compares before executing the task, proving the
/// coordinator authored the exact file paths, credentials, predicate, and
/// limit. Empty `worker_secret` (dev mode) skips this check.
const SCAN_SIGNATURE_HEADER: &str = "x-sqe-scan-signature";

/// Lowercase hex encoding for the 32-byte HMAC tag (#206). Kept local to avoid
/// an extra crate dependency for such a small need.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Worker's Arrow Flight service.
///
/// Handles three operations:
/// - `do_get`: Execute a scan task and stream results back
/// - `do_action("health_check")`: Return OK for coordinator health monitoring
/// - `do_action("refresh_credentials")`: Accept refreshed S3 credentials from coordinator
///
/// The service holds a [`SessionContext`] whose `RuntimeEnv` carries the
/// configured memory pool and disk manager so that every scan execution
/// respects the worker's memory limits.
#[derive(Clone)]
pub struct WorkerFlightService {
    metrics: Arc<WorkerMetricsRegistry>,
    credential_store: CredentialStore,
    session_ctx: SessionContext,
    footer_cache: Option<Arc<FooterCache>>,
    shuffle_manager: ShuffleManager,
    /// Maximum duration for a single scan task. 0 means no timeout.
    scan_timeout: std::time::Duration,
    /// IPC compression for DoGet responses (worker -> coordinator).
    /// Default: ZSTD (internal traffic benefits from better ratio).
    flight_compression: FlightCompression,
    /// IPC compression for DoExchange shuffle responses.
    /// Default: ZSTD.
    shuffle_compression: FlightCompression,
    /// Shared secret used to authenticate inbound Flight calls. When empty
    /// the worker accepts unauthenticated traffic (operators must opt in
    /// via `worker.allow_unauthenticated = true`, enforced at config load).
    worker_secret: String,
}

impl WorkerFlightService {
    pub fn new(metrics: Arc<WorkerMetricsRegistry>, session_ctx: SessionContext) -> Self {
        Self {
            metrics,
            credential_store: CredentialStore::new(),
            session_ctx,
            footer_cache: None,
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
        }
    }

    /// Create a new service with an externally provided credential store.
    ///
    /// This is useful when the store needs to be shared with other components
    /// (e.g. the executor needs to subscribe before the Flight service starts).
    pub fn with_credential_store(
        metrics: Arc<WorkerMetricsRegistry>,
        session_ctx: SessionContext,
        credential_store: CredentialStore,
    ) -> Self {
        Self {
            metrics,
            credential_store,
            session_ctx,
            footer_cache: None,
            shuffle_manager: ShuffleManager::new(),
            scan_timeout: std::time::Duration::from_secs(600),
            flight_compression: FlightCompression::Zstd,
            shuffle_compression: FlightCompression::Zstd,
            worker_secret: String::new(),
        }
    }

    /// Set the Parquet footer cache for this service.
    #[must_use = "with_footer_cache consumes self; bind the returned service"]
    pub fn with_footer_cache(mut self, cache: Arc<FooterCache>) -> Self {
        self.footer_cache = Some(cache);
        self
    }

    /// Set the scan timeout from config.
    #[must_use = "with_scan_timeout consumes self; bind the returned service"]
    pub fn with_scan_timeout(mut self, timeout_secs: u64) -> Self {
        self.scan_timeout = std::time::Duration::from_secs(timeout_secs);
        self
    }

    /// Set the IPC compression for DoGet responses.
    #[must_use = "with_flight_compression consumes self; bind the returned service"]
    pub fn with_flight_compression(mut self, compression: FlightCompression) -> Self {
        self.flight_compression = compression;
        self
    }

    /// Set the IPC compression for DoExchange shuffle responses.
    #[must_use = "with_shuffle_compression consumes self; bind the returned service"]
    pub fn with_shuffle_compression(mut self, compression: FlightCompression) -> Self {
        self.shuffle_compression = compression;
        self
    }

    /// Set the shared secret used to authenticate inbound Flight calls
    /// (`do_get` scan tickets and `do_action("refresh_credentials")`).
    /// An empty secret disables enforcement: callers must explicitly opt
    /// in via `worker.allow_unauthenticated = true` at config load time.
    #[must_use = "with_worker_secret consumes self; bind the returned service"]
    pub fn with_worker_secret(mut self, secret: String) -> Self {
        self.worker_secret = secret;
        self
    }

    /// Constant-time check of the `x-sqe-worker-secret` metadata header.
    /// Returns `Ok(())` when the secret matches or when no secret is
    /// configured. Returns `Status::unauthenticated` on mismatch.
    fn verify_worker_secret(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        if self.worker_secret.is_empty() {
            return Ok(());
        }
        use subtle::ConstantTimeEq;
        let provided = metadata
            .get(WORKER_SECRET_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided_bytes = provided.as_bytes();
        let secret_bytes = self.worker_secret.as_bytes();
        if provided_bytes.len() != secret_bytes.len()
            || !bool::from(provided_bytes.ct_eq(secret_bytes))
        {
            return Err(Status::unauthenticated("Invalid worker secret"));
        }
        Ok(())
    }

    /// Verify the HMAC-SHA256 signature over the raw ScanTask ticket bytes
    /// (issue #206). Recomputes the tag over `ticket_bytes` keyed by the shared
    /// `worker_secret` and constant-time compares it to the
    /// `x-sqe-scan-signature` header.
    ///
    /// Signing the wire bytes (rather than a re-serialized struct) means the
    /// tag covers exactly what we decode: file paths, credentials, and the
    /// #233 predicate/limit fields. A tampered ticket (swapped path, stripped
    /// predicate) changes the bytes and fails verification.
    ///
    /// When `worker_secret` is empty the deployment opted into
    /// `worker.allow_unauthenticated`; there is no key to sign with, so this
    /// returns `Ok(())` (the insecure dev path, already gated at config load).
    fn verify_scan_signature(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        ticket_bytes: &[u8],
    ) -> Result<(), Status> {
        if self.worker_secret.is_empty() {
            return Ok(());
        }
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        use subtle::ConstantTimeEq;

        let provided = metadata
            .get(SCAN_SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut mac = <Hmac<Sha256>>::new_from_slice(self.worker_secret.as_bytes())
            .expect("HMAC accepts keys of any length");
        mac.update(ticket_bytes);
        let expected = mac.finalize().into_bytes();
        let expected_hex = hex_encode(&expected);

        let provided_bytes = provided.as_bytes();
        let expected_bytes = expected_hex.as_bytes();
        if provided_bytes.len() != expected_bytes.len()
            || !bool::from(provided_bytes.ct_eq(expected_bytes))
        {
            return Err(Status::unauthenticated("Invalid scan task signature"));
        }
        Ok(())
    }

    /// Returns a reference to the credential store for use by executors.
    pub fn credential_store(&self) -> &CredentialStore {
        &self.credential_store
    }

    /// Returns a reference to the shuffle manager.
    pub fn shuffle_manager(&self) -> &ShuffleManager {
        &self.shuffle_manager
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Reject anonymous callers before parsing the ticket: the ticket
        // body carries the user's S3 credentials, so we must not log the
        // scan task or even validate its shape until the coordinator has
        // proven itself.
        self.verify_worker_secret(request.metadata())?;

        // Verify the HMAC over the exact ticket bytes (#206) before decoding or
        // executing. This proves the coordinator authored this precise task; a
        // tampered ticket (swapped file path, stripped predicate) fails here.
        // Must run before from_bytes so a forged task is never even parsed.
        let metadata_owned = request.metadata().clone();
        self.verify_scan_signature(&metadata_owned, &request.get_ref().ticket)?;

        // Extract W3C TraceContext from incoming gRPC metadata so this
        // worker span becomes a child of the coordinator's trace.
        let parent_cx = extract_trace_context(request.metadata());

        let ticket = request.into_inner();

        let scan_task = ScanTask::from_bytes(&ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!("Failed to decode ScanTask: {e}"))
        })?;

        let worker_span = info_span!(
            "worker_execute_scan",
            fragment_id = %scan_task.fragment_id,
            file_count = scan_task.data_file_paths.len(),
        );
        // Link this span to the coordinator's trace
        let _set_parent_result = worker_span.set_parent(parent_cx);

        let metrics = self.metrics.clone();
        let credential_store = self.credential_store.clone();
        let session_ctx = self.session_ctx.clone();
        let footer_cache = self.footer_cache.clone();
        let scan_timeout = self.scan_timeout;
        let flight_compression = self.flight_compression;
        async move {
            info!(
                fragment_id = %scan_task.fragment_id,
                file_count = scan_task.data_file_paths.len(),
                "Worker received scan task"
            );

            // Subscribe to credential updates for this fragment. The guard
            // removes the entry on drop so timeouts, setup errors, and panics
            // can't leak `watch::Sender`s into the store. Issue #76.
            let cred_rx = credential_store.subscribe(&scan_task.fragment_id).await;
            let fragment_id = scan_task.fragment_id.clone();
            let cleanup_guard = credential_store.cleanup_guard(fragment_id.clone());

            // The pushed-down predicate (#233) is carried inside `scan_task`
            // (`predicate_proto`) and decoded by the executor; the late
            // materialization RowFilter is wired from it there.
            let prepare = executor::execute_scan_streaming(
                scan_task,
                Some(metrics.clone()),
                session_ctx.clone(),
                Some(cred_rx),
                footer_cache.clone(),
                None, // Coordinator metrics (workers don't have coordinator registry)
            );

            let (schema, batch_stream) = if scan_timeout.is_zero() {
                prepare.await
            } else {
                tokio::time::timeout(scan_timeout, prepare)
                    .await
                    .map_err(|_| {
                        warn!(
                            fragment_id = %fragment_id,
                            timeout_secs = scan_timeout.as_secs(),
                            "Scan task setup timed out"
                        );
                        Status::deadline_exceeded(format!(
                            "Scan task {} setup timed out after {}s",
                            fragment_id,
                            scan_timeout.as_secs()
                        ))
                    })?
            }
            .map_err(|e| {
                warn!(error = %e, "Scan task setup failed");
                Status::internal(format!("Scan execution failed: {e}"))
            })?;

            // Stream lifetime carries the guard; once the encoder finishes
            // (or the client disconnects mid-stream) the guard drops and the
            // credential entry is removed via `tokio::spawn`.
            let mapped_stream = batch_stream
                .map(|item| match item {
                    Ok(batch) => Ok(batch),
                    Err(e) => Err(arrow_flight::error::FlightError::from_external_error(
                        Box::new(std::io::Error::other(e.to_string())),
                    )),
                })
                .chain(stream::once(async move {
                    drop(cleanup_guard);
                    Err::<arrow_array::RecordBatch, arrow_flight::error::FlightError>(
                        arrow_flight::error::FlightError::from_external_error(
                            Box::new(std::io::Error::other("__SQE_CLEANUP_SENTINEL__")),
                        ),
                    )
                }))
                .filter_map(|item| async move {
                    match item {
                        Ok(b) => Some(Ok(b)),
                        Err(e) if e.to_string().contains("__SQE_CLEANUP_SENTINEL__") => None,
                        Err(e) => Some(Err(e)),
                    }
                });

            let schema_arc = Arc::new((*schema).clone());
            let ipc_opts = ipc_options_for(flight_compression)?;
            let flight_stream = FlightDataEncoderBuilder::new()
                .with_schema(schema_arc)
                .with_options(ipc_opts)
                .build(mapped_stream)
                .map_err(Status::from);

            Ok(Response::new(
                Box::pin(flight_stream) as Self::DoGetStream
            ))
        }
        .instrument(worker_span)
        .await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let (metadata, _, action) = request.into_parts();

        match action.r#type.as_str() {
            "health_check" => {
                debug!("Health check OK");
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "refresh_credentials" => {
                // Credential refresh swaps the S3 keys that the executor
                // will use for the next file read. An attacker who pushes
                // their own bucket here either exfiltrates data or causes
                // a table-swap. Require the worker secret on every call.
                self.verify_worker_secret(&metadata)?;

                let creds: RefreshableCredentials =
                    serde_json::from_slice(&action.body).map_err(|e| {
                        Status::invalid_argument(format!(
                            "Failed to decode RefreshableCredentials: {e}"
                        ))
                    })?;

                info!(
                    fragment_id = %creds.fragment_id,
                    expiry = %creds.expiry,
                    "Received credential refresh from coordinator"
                );

                let published = self.credential_store.publish(creds).await;

                let body = if published {
                    b"accepted".to_vec()
                } else {
                    b"no_active_scan".to_vec()
                };

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(body),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            other => Err(Status::unimplemented(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Workers don't support handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Workers don't support list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Workers don't support get_flight_info",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Workers don't support get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Workers don't support do_put"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        // Gate the shuffle stream behind the worker secret before consuming
        // the request, exactly as `do_get`/`refresh_credentials` do. Without
        // this an attacker with network access could push arbitrary
        // RecordBatches into a stage receiver (result poisoning) or drain a
        // partition channel for an in-flight distributed query.
        self.verify_worker_secret(request.metadata())?;

        let mut stream = request.into_inner();

        // 1. Read the first FlightData message to get the descriptor.
        let first_msg = stream.next().await.ok_or_else(|| {
            Status::invalid_argument("DoExchange stream ended before descriptor message")
        })??;

        let descriptor = first_msg
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| {
                Status::invalid_argument(
                    "First DoExchange message must contain a FlightDescriptor",
                )
            })?;

        let exchange_desc =
            ExchangeDescriptor::from_bytes(&descriptor.cmd).map_err(|e| {
                Status::invalid_argument(format!(
                    "Failed to decode ExchangeDescriptor from descriptor cmd: {e}"
                ))
            })?;

        let (query_id, stage_id) = exchange_desc.stage_key();
        let partition_id = exchange_desc.partition_id();

        info!(
            query_id = %query_id,
            stage_id = %stage_id,
            partition_id = partition_id,
            "DoExchange: receiving shuffle data"
        );

        // 2. Look up the ShuffleReceiver for this (query_id, stage_id).
        let shuffle_receiver = self
            .shuffle_manager
            .get(&query_id, &stage_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "No shuffle receiver registered for query={query_id}, stage={stage_id}"
                ))
            })?;

        let sender = shuffle_receiver
            .sender(partition_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "No sender for partition {partition_id} in query={query_id}, stage={stage_id}"
                ))
            })?
            .clone();

        // 3. Decode and buffer incoming RecordBatches.
        //    Chain the first message (which may also contain data) with the rest.
        let remaining_stream = stream.map_err(|e| {
            arrow_flight::error::FlightError::Tonic(Box::new(e))
        });
        let first_stream = futures::stream::once(async move { Ok(first_msg) });
        let combined =
            first_stream.chain(remaining_stream);

        let mut flight_batch_stream =
            FlightRecordBatchStream::new_from_flight_data(combined);

        let schema = shuffle_receiver.schema().clone();

        // Spawn a task to receive batches and forward to the mpsc channel.
        let query_id_clone = query_id.clone();
        let stage_id_clone = stage_id.clone();
        tokio::spawn(async move {
            let mut batch_count = 0u64;
            while let Some(batch_result) = flight_batch_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        batch_count += 1;
                        if sender.send(batch).await.is_err() {
                            warn!(
                                query_id = %query_id_clone,
                                stage_id = %stage_id_clone,
                                partition_id = partition_id,
                                "Shuffle receiver channel closed, stopping intake"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(
                            query_id = %query_id_clone,
                            stage_id = %stage_id_clone,
                            partition_id = partition_id,
                            error = %e,
                            "Error decoding flight data in DoExchange"
                        );
                        break;
                    }
                }
            }
            debug!(
                query_id = %query_id_clone,
                stage_id = %stage_id_clone,
                partition_id = partition_id,
                batch_count = batch_count,
                "DoExchange intake complete"
            );
            // Sender is dropped here, closing the channel for the receiver side.
        });

        // 4. Return a stream that drains the partition channel.
        let rx = shuffle_receiver
            .take_receiver(partition_id)
            .await
            .ok_or_else(|| {
                Status::already_exists(format!(
                    "Receiver for partition {partition_id} already taken \
                     (query={query_id}, stage={stage_id})"
                ))
            })?;

        // Wrap the mpsc receiver as a futures::Stream of RecordBatch.
        let output_stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        });

        let shuffle_opts = ipc_options_for(self.shuffle_compression)?;
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(shuffle_opts)
            .build(output_stream.map(Ok))
            .map_err(Status::from);

        Ok(Response::new(
            Box::pin(flight_stream) as Self::DoExchangeStream
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "health_check".to_string(),
                description: "Check worker health".to_string(),
            },
            ActionType {
                r#type: "refresh_credentials".to_string(),
                description: "Accept refreshed S3 credentials from coordinator".to_string(),
            },
        ];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use datafusion::prelude::SessionContext;
    use tonic::Request;

    fn make_service(secret: &str) -> WorkerFlightService {
        let metrics = Arc::new(WorkerMetricsRegistry::new().unwrap());
        WorkerFlightService::new(metrics, SessionContext::new())
            .with_worker_secret(secret.to_string())
    }

    /// Recompute the HMAC-SHA256 tag (hex) the coordinator would attach, used
    /// by the #206 signature tests to forge a valid header.
    fn sign(secret: &str, bytes: &[u8]) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(bytes);
        hex_encode(&mac.finalize().into_bytes())
    }

    /// Build a real ScanTask ticket so the signature test exercises the actual
    /// wire bytes (and so decoding succeeds past the signature gate).
    fn make_scan_task_bytes() -> Vec<u8> {
        sqe_planner::ScanTask {
            fragment_id: "frag-sig".to_string(),
            data_file_paths: vec!["s3://bucket/f.parquet".to_string()],
            file_sizes_bytes: vec![1024],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: false,
            predicate_proto: None,
            limit: None,
        }
        .to_bytes()
        .unwrap()
    }

    fn make_refresh_creds() -> RefreshableCredentials {
        RefreshableCredentials {
            fragment_id: "frag-test".to_string(),
            access_key_id: "AKID".to_string(),
            secret_access_key: "SECRET".to_string(),
            session_token: "TOKEN".to_string(),
            expiry: Utc::now() + Duration::hours(1),
        }
    }

    fn unwrap_err<T>(r: Result<T, Status>) -> Status {
        match r {
            Ok(_) => panic!("expected Status error, got Ok"),
            Err(s) => s,
        }
    }

    fn unwrap_ok<T>(r: Result<T, Status>) -> T {
        match r {
            Ok(v) => v,
            Err(s) => panic!("expected Ok, got Status: {s}"),
        }
    }

    #[tokio::test]
    async fn do_get_rejects_missing_secret_header() {
        let svc = make_service("expected-secret");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let request = Request::new(ticket);
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_rejects_wrong_secret() {
        let svc = make_service("expected-secret");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let mut request = Request::new(ticket);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "wrong".parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_accepts_correct_secret_and_signature_then_fails_on_bad_ticket() {
        // With the right secret AND a valid signature over the (junk) body, both
        // auth gates pass; ticket decoding then fails. The error must NOT be
        // Unauthenticated, proving both gates let the call through.
        let svc = make_service("expected-secret");
        let body = b"junk";
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from_static(body),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request.metadata_mut().insert(
            SCAN_SIGNATURE_HEADER,
            sign("expected-secret", body).parse().unwrap(),
        );
        let err = unwrap_err(svc.do_get(request).await);
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_rejects_missing_signature() {
        // Right secret, no signature header: the #206 gate rejects before decode.
        let svc = make_service("expected-secret");
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(make_scan_task_bytes()),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_rejects_tampered_ticket() {
        // A signature valid for the ORIGINAL bytes must fail once the ticket is
        // mutated (e.g. a swapped file path). Sign the original, then tamper.
        let svc = make_service("expected-secret");
        let original = make_scan_task_bytes();
        let signature = sign("expected-secret", &original);
        let mut tampered = original.clone();
        // Flip a byte to simulate a swapped file path / stripped predicate.
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(tampered),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(SCAN_SIGNATURE_HEADER, signature.parse().unwrap());
        let err = unwrap_err(svc.do_get(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn do_get_accepts_correctly_signed_ticket() {
        // A correctly-signed, well-formed ScanTask passes both gates. Execution
        // then fails downstream (no real S3), but NOT with Unauthenticated.
        let svc = make_service("expected-secret");
        let body = make_scan_task_bytes();
        let signature = sign("expected-secret", &body);
        let mut request = Request::new(Ticket {
            ticket: bytes::Bytes::from(body),
        });
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert(SCAN_SIGNATURE_HEADER, signature.parse().unwrap());
        let result = svc.do_get(request).await;
        if let Err(s) = result {
            assert_ne!(s.code(), tonic::Code::Unauthenticated);
        }
    }

    #[tokio::test]
    async fn do_get_empty_secret_accepts_anonymous() {
        // Empty worker_secret means unauthenticated mode (opt-in via
        // config). Auth gate disabled; failure comes from ticket decoding.
        let svc = make_service("");
        let ticket = Ticket {
            ticket: bytes::Bytes::from_static(b"junk"),
        };
        let request = Request::new(ticket);
        let err = unwrap_err(svc.do_get(request).await);
        assert_ne!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_credentials_rejects_missing_secret() {
        let svc = make_service("expected-secret");
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let request = Request::new(action);
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_credentials_rejects_wrong_secret() {
        let svc = make_service("expected-secret");
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "wrong".parse().unwrap());
        let err = unwrap_err(svc.do_action(request).await);
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_credentials_accepts_correct_secret() {
        let svc = make_service("expected-secret");
        let _rx = svc.credential_store.subscribe("frag-test").await;
        let body = serde_json::to_vec(&make_refresh_creds()).unwrap();
        let action = Action {
            r#type: "refresh_credentials".to_string(),
            body: bytes::Bytes::from(body),
        };
        let mut request = Request::new(action);
        request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, "expected-secret".parse().unwrap());
        let response = unwrap_ok(svc.do_action(request).await);
        let mut stream = response.into_inner();
        let first = stream.next().await.expect("body present").expect("ok");
        assert_eq!(first.body.as_ref(), b"accepted");
    }

    #[tokio::test]
    async fn health_check_remains_open_when_secret_configured() {
        // Health probes from the coordinator worker_registry do not carry
        // the secret today; keep them open so liveness still works. The
        // call must not leak any credential state.
        let svc = make_service("expected-secret");
        let action = Action {
            r#type: "health_check".to_string(),
            body: bytes::Bytes::new(),
        };
        let request = Request::new(action);
        let response = unwrap_ok(svc.do_action(request).await);
        let mut stream = response.into_inner();
        let first = stream.next().await.expect("body").expect("ok");
        assert_eq!(first.body.as_ref(), b"ok");
    }
}

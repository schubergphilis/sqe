use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, info_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use datafusion::prelude::SessionContext;
use sqe_catalog::FooterCache;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_metrics::propagation::extract_trace_context;
use sqe_planner::ScanTask;

use crate::credential_channel::{CredentialStore, RefreshableCredentials};
use crate::executor;
use crate::shuffle::{ExchangeDescriptor, ShuffleManager};

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
}

impl WorkerFlightService {
    pub fn new(metrics: Arc<WorkerMetricsRegistry>, session_ctx: SessionContext) -> Self {
        Self {
            metrics,
            credential_store: CredentialStore::new(),
            session_ctx,
            footer_cache: None,
            shuffle_manager: ShuffleManager::new(),
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
        }
    }

    /// Set the Parquet footer cache for this service.
    pub fn with_footer_cache(mut self, cache: Arc<FooterCache>) -> Self {
        self.footer_cache = Some(cache);
        self
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
        async move {
            info!(
                fragment_id = %scan_task.fragment_id,
                file_count = scan_task.data_file_paths.len(),
                "Worker received scan task"
            );

            // Subscribe to credential updates for this fragment
            let cred_rx = credential_store.subscribe(&scan_task.fragment_id).await;

            let (schema, batches) =
                executor::execute_scan(
                    &scan_task,
                    Some(&metrics),
                    &session_ctx,
                    Some(cred_rx),
                    footer_cache.as_ref(),
                    None, // Late materialization filter (not yet wired from coordinator)
                    None, // Coordinator metrics (workers don't have coordinator registry)
                )
                    .await
                    .map_err(|e| {
                        warn!(error = %e, "Scan task execution failed");
                        Status::internal(format!("Scan execution failed: {e}"))
                    })?;

            // Clean up the credential channel now that the scan is complete
            credential_store.remove(&scan_task.fragment_id).await;

            let schema = Arc::new((*schema).clone());
            let batch_stream = stream::iter(batches.into_iter().map(Ok));
            let flight_stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream)
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
        let action = request.into_inner();

        match action.r#type.as_str() {
            "health_check" => {
                debug!("Health check OK");
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "refresh_credentials" => {
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

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
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

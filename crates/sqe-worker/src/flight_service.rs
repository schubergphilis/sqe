use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt, stream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use sqe_planner::ScanTask;

use crate::executor;

/// Worker's Arrow Flight service.
///
/// Handles two operations:
/// - `do_get`: Execute a scan task and stream results back
/// - `do_action("health_check")`: Return OK for coordinator health monitoring
#[derive(Clone, Default)]
pub struct WorkerFlightService {}

impl WorkerFlightService {
    pub fn new() -> Self {
        Self {}
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
        let ticket = request.into_inner();

        let scan_task = ScanTask::from_bytes(&ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!("Failed to decode ScanTask: {e}"))
        })?;

        info!(
            fragment_id = %scan_task.fragment_id,
            file_count = scan_task.data_file_paths.len(),
            "Worker received scan task"
        );

        let (schema, batches) = executor::execute_scan(&scan_task).await.map_err(|e| {
            warn!(error = %e, "Scan task execution failed");
            Status::internal(format!("Scan execution failed: {e}"))
        })?;

        let schema = Arc::new((*schema).clone());
        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(flight_stream)))
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
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Workers don't support do_exchange"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "health_check".to_string(),
            description: "Check worker health".to_string(),
        }];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}

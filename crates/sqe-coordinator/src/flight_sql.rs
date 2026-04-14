use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementIngest, CommandStatementQuery, CommandStatementSubstraitPlan,
    CommandStatementUpdate, DoPutPreparedStatementResult, Nullable, ProstMessageExt, Searchable,
    SqlInfo, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::sql::metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoDataBuilder};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use base64::Engine;
use futures::{Stream, TryStreamExt, stream};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use sqe_core::SqeConfig;

use crate::query_handler::QueryHandler;
use crate::query_tracker::QueryTracker;
use crate::session_manager::SessionManager;
use crate::worker_registry::WorkerRegistry;

// Re-export helpers so callers that imported directly from this module
// keep working without changes.
pub use crate::flight_sql_helpers::FetchResults;
use crate::flight_sql_helpers::{FlightStream, sqe_error_to_status};

/// Build [`IpcWriteOptions`] for a given compression setting.
///
/// Used by both the coordinator (client-facing DoGet) and shared with workers
/// for shuffle DoExchange encoding.
pub fn ipc_options_for(
    compression: sqe_core::FlightCompression,
) -> Result<IpcWriteOptions, Status> {
    let codec = match compression {
        sqe_core::FlightCompression::None => None,
        sqe_core::FlightCompression::Lz4 => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        sqe_core::FlightCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
    };
    IpcWriteOptions::default()
        .try_with_compression(codec)
        .map_err(|e| Status::internal(format!("Failed to set IPC compression: {e}")))
}

/// Encode `RecordBatch`es into a streaming Flight response using the given IPC options.
///
/// Standalone function so that tests and other callers can use it without
/// constructing a full `SqeFlightSqlService`.
pub fn encode_batches_to_stream(
    batches: Vec<RecordBatch>,
    options: IpcWriteOptions,
) -> Result<Response<FlightStream>, Status> {
    if batches.is_empty() {
        let stream = futures::stream::empty();
        let flight_stream: FlightStream = Box::pin(stream);
        return Ok(Response::new(flight_stream));
    }

    let schema = batches[0].schema();
    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(options)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_stream)))
}


/// Flight SQL service implementation for SQE.
///
/// Wires together session management (OIDC auth) and query execution
/// (DataFusion + Polaris catalog + policy enforcement) into the Arrow
/// Flight SQL protocol.
#[derive(Clone)]
pub struct SqeFlightSqlService {
    session_manager: Arc<SessionManager>,
    query_handler: Arc<QueryHandler>,
    config: SqeConfig,
    worker_registry: Option<Arc<WorkerRegistry>>,
    query_tracker: Arc<QueryTracker>,
    worker_secret: String,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    rate_limiter: Option<Arc<crate::rate_limiter::QueryRateLimiter>>,
}

impl SqeFlightSqlService {
    pub fn new(
        session_manager: Arc<SessionManager>,
        query_handler: Arc<QueryHandler>,
        config: SqeConfig,
    ) -> Self {
        let worker_secret = config.coordinator.worker_secret.clone();
        let query_tracker = Arc::clone(query_handler.query_tracker());
        Self {
            session_manager,
            query_handler,
            config,
            worker_registry: None,
            query_tracker,
            worker_secret,
            metrics: None,
            rate_limiter: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns a reference to the query tracker for external access
    /// (e.g., metrics, admin endpoints).
    pub fn query_tracker(&self) -> &Arc<QueryTracker> {
        &self.query_tracker
    }

    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    pub fn with_rate_limiter(mut self, limiter: Arc<crate::rate_limiter::QueryRateLimiter>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// Extract client IP from request metadata (x-forwarded-for or peer addr).
    fn extract_client_ip<T>(request: &Request<T>) -> String {
        request
            .metadata()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
            .unwrap_or_else(|| {
                request
                    .remote_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            })
    }

    /// Extract and validate a bearer token from the request metadata,
    /// returning the associated session.
    ///
    /// Supports two token types:
    /// 1. SQE session ID (from do_handshake) — looked up in session manager
    /// 2. Raw JWT (from backend BFF pass-through) — validated via the auth
    ///    provider chain (JWKS signature check) and converted into a session
    async fn get_session_from_request<T: Send + Sync>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<sqe_core::Session>, Status> {
        let metadata = request.metadata();
        let client_ip = Self::extract_client_ip(request);

        let auth = metadata
            .get("authorization")
            .ok_or_else(|| {
                debug!(client_ip = %client_ip, "Request missing authorization header");
                Status::unauthenticated("No authorization header")
            })?
            .to_str()
            .map_err(|e| Status::internal(format!("Invalid authorization header: {e}")))?;

        let bearer_prefix = "Bearer ";
        if !auth.starts_with(bearer_prefix) {
            return Err(Status::unauthenticated(
                "Authorization header must use Bearer scheme",
            ));
        }

        let token = &auth[bearer_prefix.len()..];

        // Try session lookup first (handshake flow)
        if let Some(session) = self.session_manager.get_session(token) {
            debug!(
                username = %session.user.username,
                client_ip = %client_ip,
                session_id = %session.id,
                "Request authenticated via session"
            );
            return Ok(session);
        }

        // If the token looks like a JWT (contains dots), validate it through
        // the auth provider chain (JWKS signature verification) and create a
        // proper session. The username comes from the validated JWT claims,
        // not from a client-supplied header.
        if token.contains('.') {
            let credentials = sqe_auth::FlightCredentials {
                bearer_token: Some(token.to_string()),
                ..Default::default()
            };
            let session = self
                .session_manager
                .authenticate_credentials(&credentials)
                .await
                .map_err(|e| {
                    warn!(
                        client_ip = %client_ip,
                        error = %e,
                        "Flight: JWT bearer token validation failed"
                    );
                    Status::unauthenticated("Invalid or expired bearer token")
                })?;
            debug!(
                username = %session.user.username,
                client_ip = %client_ip,
                session_id = %session.id,
                "Flight: authenticated via validated JWT bearer token"
            );
            return Ok(session);
        }

        warn!(client_ip = %client_ip, "Invalid or expired session token");
        Err(Status::unauthenticated("Invalid or expired session token"))
    }

    /// Parse the `[coordinator] flight_compression` config into IPC write options.
    fn flight_ipc_options(&self) -> Result<IpcWriteOptions, Status> {
        let compression = sqe_core::FlightCompression::from_config(&self.config.coordinator.flight_compression)
            .map_err(|e| Status::internal(format!("Invalid flight_compression config: {e}")))?;
        ipc_options_for(compression)
    }

    /// Convert RecordBatches into a streaming Flight response with IPC compression.
    #[allow(clippy::type_complexity)]
    fn batches_to_stream(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Response<FlightStream>, Status> {
        let options = self.flight_ipc_options()?;
        encode_batches_to_stream(batches, options)
    }

    // -----------------------------------------------------------------
    // Multi-endpoint FlightInfo builders (Task 28 — Stream 9)
    // -----------------------------------------------------------------

    /// Build a `FlightInfo` with a single endpoint pointing at the
    /// coordinator (the current node).  This is the default for
    /// non-distributed queries and preserves backward compatibility —
    /// the endpoint carries no explicit `Location`, which tells
    /// the client to fetch from the same server that returned the
    /// `FlightInfo`.
    pub fn build_flight_info_single(
        schema: &arrow_schema::Schema,
        ticket: Ticket,
    ) -> Result<FlightInfo, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        FlightInfo::new()
            .try_with_schema(schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))
            .map(|info| {
                info.with_endpoint(endpoint)
                    .with_total_records(-1)
                    .with_ordered(false)
            })
    }

    /// Build a `FlightInfo` with one `FlightEndpoint` per worker that
    /// holds result data.  Each endpoint contains a `Ticket` identifying
    /// the result partition on that worker, and a `Location` pointing to
    /// the worker's Flight endpoint URL.
    ///
    /// When `executor_endpoints` is empty the method falls back to a
    /// single coordinator endpoint (no location) using
    /// `fallback_ticket` — this keeps the coordinator as the data
    /// source, which is the correct behavior when no workers were
    /// involved.
    pub fn build_flight_info_distributed(
        schema: &arrow_schema::Schema,
        executor_endpoints: &[(String, Ticket)],
        fallback_ticket: Ticket,
    ) -> Result<FlightInfo, Status> {
        if executor_endpoints.is_empty() {
            return Self::build_flight_info_single(schema, fallback_ticket);
        }

        let mut info = FlightInfo::new()
            .try_with_schema(schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .with_total_records(-1)
            .with_ordered(false);

        for (url, ticket) in executor_endpoints {
            let endpoint = FlightEndpoint::new()
                .with_ticket(ticket.clone())
                .with_location(url.as_str());
            info = info.with_endpoint(endpoint);
        }

        Ok(info)
    }
}

/// Indicates whether a query's results are available locally on the
/// coordinator or distributed across workers.
///
/// The Flight SQL layer inspects this after query planning / execution
/// to decide whether to return a single-endpoint or multi-endpoint
/// `FlightInfo`.
#[derive(Debug, Clone)]
pub enum QueryResultLocation {
    /// Results are (or will be) on the coordinator — single endpoint.
    Local,
    /// Results are distributed across the listed workers.
    /// Each entry is `(worker_flight_url, ticket)`.
    Distributed(Vec<(String, Ticket)>),
}

impl QueryResultLocation {
    /// Returns `true` when results are distributed across workers.
    pub fn is_distributed(&self) -> bool {
        matches!(self, Self::Distributed(eps) if !eps.is_empty())
    }
}

#[tonic::async_trait]
impl FlightSqlService for SqeFlightSqlService {
    type FlightService = SqeFlightSqlService;

    /// Handle client authentication via Basic auth.
    ///
    /// Extracts username:password from the Basic auth header, authenticates
    /// via the configured OIDC provider, and returns the session ID as a bearer token.
    #[tracing::instrument(skip_all, name = "flight_sql.handshake")]
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let basic_prefix = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("Authorization header not present"))?
            .to_str()
            .map_err(|e| Status::internal(format!("Authorization header not parsable: {e}")))?
            .to_string();

        if !authorization.starts_with(basic_prefix) {
            return Err(Status::invalid_argument(format!(
                "Auth type not supported: expected Basic, got: {}",
                &authorization[..authorization.len().min(10)]
            )));
        }

        let base64_encoded = &authorization[basic_prefix.len()..];
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(base64_encoded)
            .map_err(|e| Status::invalid_argument(format!("Invalid base64 in auth: {e}")))?;
        let decoded_str = std::str::from_utf8(&decoded)
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in auth: {e}")))?;

        let parts: Vec<&str> = decoded_str.splitn(2, ':').collect();
        let (username, password) = match parts.as_slice() {
            [user, pass] => (*user, *pass),
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid authorization: expected username:password",
                ));
            }
        };

        let client_ip = Self::extract_client_ip(&request);

        info!(username = username, "Handshake authentication attempt");

        let credentials = sqe_auth::FlightCredentials {
            username: Some(username.to_string()),
            password: Some(password.to_string()),
            ..Default::default()
        };

        let auth_start = std::time::Instant::now();

        let auth_result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.session_manager.authenticate_credentials(&credentials),
        )
        .await;

        let auth_elapsed = auth_start.elapsed();

        // Record auth duration regardless of outcome
        if let Some(ref metrics) = self.metrics {
            metrics.auth_duration_seconds.observe(auth_elapsed.as_secs_f64());
        }

        let session = match auth_result {
            Ok(Ok(session)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.auth_attempts_total.with_label_values(&["oidc", "success"]).inc();
                }
                session
            }
            Ok(Err(e)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.auth_attempts_total.with_label_values(&["oidc", "failed"]).inc();
                }
                warn!(username = username, client_ip = %client_ip, error = %e, "Authentication failed");
                return Err(Status::unauthenticated(format!("Authentication failed: {e}")));
            }
            Err(_) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.auth_attempts_total.with_label_values(&["oidc", "failed"]).inc();
                }
                warn!(username = username, "Handshake timed out after 30s");
                return Err(Status::deadline_exceeded("Authentication timed out"));
            }
        };

        info!(
            username = username,
            session_id = %session.id,
            "Handshake authentication successful"
        );

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: session.id.as_bytes().to_vec().into(),
        };

        let output = futures::stream::iter(vec![Ok(result)]);

        let token = format!("Bearer {}", session.id);
        let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
            Response::new(Box::pin(output));
        response.metadata_mut().append(
            "authorization",
            MetadataValue::from_str(&token)
                .map_err(|e| Status::internal(format!("Failed to create auth metadata: {e}")))?,
        );

        Ok(response)
    }

    /// Handle SQL statement queries by creating a ticket for execution.
    #[tracing::instrument(skip_all, name = "flight_sql.get_flight_info")]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let session = self.get_session_from_request(&request).await?;
        let sql = &query.query;

        debug!(
            username = %session.user.username,
            "get_flight_info_statement"
        );

        // Execute the query to get the schema (and cache results)
        // For now, we store the SQL in the ticket and re-execute on do_get
        let fetch = FetchResults {
            handle: sql.clone(),
        };
        let ticket = Ticket {
            ticket: fetch.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        // Plan the query to extract the schema without executing it
        let schema = self
            .query_handler
            .get_schema(&session, sql)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint)
            .with_total_records(-1)
            .with_ordered(false);

        Ok(Response::new(info))
    }

    /// Execute a SQL query and stream results.
    #[tracing::instrument(skip_all, name = "flight_sql.do_get")]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Rate limiting: reject if the user has exceeded their query rate.
        if let Some(ref limiter) = self.rate_limiter {
            limiter
                .check(&session.user.username)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        let sql = &ticket.statement_handle;

        debug!(
            username = %session.user.username,
            "do_get_statement with handle"
        );

        // The handle is the SQL query string
        let sql_str = std::str::from_utf8(sql)
            .map_err(|e| Status::internal(format!("Invalid statement handle: {e}")))?;

        let batches = self
            .query_handler
            .execute(&session, sql_str)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        self.batches_to_stream(batches)
    }

    /// Handle fallback do_get for tickets that don't match known Flight SQL types.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Try to decode as our FetchResults message
        if message.type_url == FetchResults::type_url() {
            let fetch: FetchResults = Message::decode(&*message.value)
                .map_err(|e| Status::internal(format!("Failed to decode ticket: {e}")))?;

            debug!(
                username = %session.user.username,
                "do_get_fallback executing query"
            );

            let batches = self
                .query_handler
                .execute(&session, &fetch.handle)
                .await
                .map_err(|e| sqe_error_to_status(&e, None))?;

            return self.batches_to_stream(batches);
        }

        Err(Status::unimplemented(format!(
            "Unsupported ticket type: {}",
            message.type_url
        )))
    }

    // ------------------------------------------------------------------
    // Catalog metadata endpoints
    // ------------------------------------------------------------------

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        info!("Flight SQL: do_get_catalogs called");
        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        let mut builder = query.into_builder();
        builder.append(&catalog_name);
        let batch = builder.build().map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
        self.batches_to_stream(vec![batch])
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("Flight SQL: do_get_schemas called");
        let session = self.get_session_from_request(&request).await?;

        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        // Use query handler to list schemas via the session catalog
        let batches = self
            .query_handler
            .execute(&session, "SHOW SCHEMAS")
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        // Build the Flight SQL GetDbSchemas response using the builder
        let mut builder = query.into_builder();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Status::internal("Expected string column for schema names"))?;
            for i in 0..col.len() {
                if !col.is_null(i) {
                    builder.append(&catalog_name, col.value(i));
                }
            }
        }

        let _schema = builder.schema();
        let batch = builder.build().map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
        self.batches_to_stream(vec![batch])
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("Flight SQL: do_get_tables called");
        let session = self.get_session_from_request(&request).await?;

        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        // SQE doesn't have information_schema — use SHOW SCHEMAS + SHOW TABLES
        // to enumerate all schemas and their tables.
        let schema_batches = self
            .query_handler
            .execute(&session, "SHOW SCHEMAS")
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        // Collect schema names
        let mut schema_names: Vec<String> = Vec::new();
        for batch in &schema_batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Status::internal("Expected string column for schema names"))?;
            for i in 0..col.len() {
                if !col.is_null(i) {
                    schema_names.push(col.value(i).to_string());
                }
            }
        }

        let mut builder = query.into_builder();
        let empty_schema = arrow_schema::Schema::empty();

        // For each schema, list its tables
        for ns in &schema_names {
            let sql = format!("SHOW TABLES IN \"{}\"", ns.replace('"', "\"\""));
            match self.query_handler.execute(&session, &sql).await {
                Ok(table_batches) => {
                    for batch in &table_batches {
                        // SHOW TABLES returns (namespace, table_name) — column 1 is the table name
                        let col = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .ok_or_else(|| {
                                Status::internal("Expected string column for table names")
                            })?;
                        for i in 0..col.len() {
                            if !col.is_null(i) {
                                builder
                                    .append(
                                        &catalog_name,
                                        ns,
                                        col.value(i),
                                        "TABLE",
                                        &empty_schema,
                                    )
                                    .map_err(|e| {
                                        Status::internal(format!("Failed to append table: {e}"))
                                    })?;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(schema = %ns, error = %e, "Failed to list tables in schema");
                }
            }
        }

        let batch = builder.build().map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
        self.batches_to_stream(vec![batch])
    }

    // ------------------------------------------------------------------
    // Required trait methods with default "not implemented" responses
    // ------------------------------------------------------------------

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Substrait plans not supported"))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Decode the SQL from the prepared statement handle
        let fetch: FetchResults =
            Message::decode(&*cmd.prepared_statement_handle)
                .map_err(|e| Status::internal(format!("Failed to decode prepared statement handle: {e}")))?;

        let schema = self
            .query_handler
            .get_schema(&session, &fetch.handle)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        let ticket = Ticket {
            ticket: cmd.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint)
            .with_total_records(-1);

        Ok(Response::new(info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let mut sql_info_builder = SqlInfoDataBuilder::new();
        sql_info_builder.append(SqlInfo::FlightSqlServerName, "SQE Coordinator");
        sql_info_builder.append(SqlInfo::FlightSqlServerVersion, "0.1.0");
        sql_info_builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        let sql_info_data = sql_info_builder.build().map_err(|e| {
            Status::internal(format!("Failed to build SQL info: {e}"))
        })?;

        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&sql_info_data).schema().as_ref())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket { ticket: query.as_any().encode_to_vec().into() };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket { ticket: query.as_any().encode_to_vec().into() };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket { ticket: query.as_any().encode_to_vec().into() };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket { ticket: query.as_any().encode_to_vec().into() };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Decode the SQL from the prepared statement handle
        let fetch: FetchResults =
            Message::decode(&*query.prepared_statement_handle)
                .map_err(|e| Status::internal(format!("Failed to decode prepared statement handle: {e}")))?;

        debug!(
            username = %session.user.username,
            sql = %fetch.handle,
            "Executing prepared statement"
        );

        let batches = self
            .query_handler
            .execute(&session, &fetch.handle)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        self.batches_to_stream(batches)
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        let mut builder = arrow_array::builder::StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        let arr = builder.finish();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("table_type", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])
            .map_err(|e| Status::internal(format!("Failed to build table types: {e}")))?;
        self.batches_to_stream(vec![batch])
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        let mut sql_info_builder = SqlInfoDataBuilder::new();
        sql_info_builder.append(SqlInfo::FlightSqlServerName, "SQE Coordinator");
        sql_info_builder.append(SqlInfo::FlightSqlServerVersion, "0.1.0");
        sql_info_builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        let sql_info_data = sql_info_builder.build().map_err(|e| {
            Status::internal(format!("Failed to build SQL info: {e}"))
        })?;

        let builder = query.into_builder(&sql_info_data);
        let schema = builder.schema();
        let batch = builder.build();
        let options = self.flight_ipc_options()?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(options)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        // Iceberg tables have no primary keys — return empty stream
        self.batches_to_stream(vec![])
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        self.batches_to_stream(vec![])
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        self.batches_to_stream(vec![])
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        self.batches_to_stream(vec![])
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        let mut builder = XdbcTypeInfoDataBuilder::new();

        builder.append(XdbcTypeInfo {
            type_name: "boolean".into(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcBit,
            num_prec_radix: Some(0),
            ..Default::default()
        });

        for (name, dt, size, radix) in [
            ("tinyint",  XdbcDataType::XdbcTinyint,  3,  10),
            ("smallint", XdbcDataType::XdbcSmallint, 5,  10),
            ("integer",  XdbcDataType::XdbcInteger,  10, 10),
            ("bigint",   XdbcDataType::XdbcBigint,   19, 10),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(radix),
                ..Default::default()
            });
        }

        for (name, dt, size) in [
            ("real",   XdbcDataType::XdbcReal,   7),
            ("double", XdbcDataType::XdbcDouble, 15),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(10),
                ..Default::default()
            });
        }

        builder.append(XdbcTypeInfo {
            type_name: "decimal".into(),
            data_type: XdbcDataType::XdbcDecimal,
            column_size: Some(38),
            create_params: Some(vec!["precision".into(), "scale".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: true,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcDecimal,
            minimum_scale: Some(0),
            maximum_scale: Some(38),
            num_prec_radix: Some(10),
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "varchar".into(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("'".into()),
            literal_suffix: Some("'".into()),
            create_params: Some(vec!["length".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarchar,
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "varbinary".into(),
            data_type: XdbcDataType::XdbcVarbinary,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("X'".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarbinary,
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "date".into(),
            data_type: XdbcDataType::XdbcDate,
            column_size: Some(10),
            literal_prefix: Some("DATE '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcDate,
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "time".into(),
            data_type: XdbcDataType::XdbcTime,
            column_size: Some(15),
            literal_prefix: Some("TIME '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTime,
            ..Default::default()
        });

        builder.append(XdbcTypeInfo {
            type_name: "timestamp".into(),
            data_type: XdbcDataType::XdbcTimestamp,
            column_size: Some(29),
            literal_prefix: Some("TIMESTAMP '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTimestamp,
            ..Default::default()
        });

        let xdbc_data = builder.build().map_err(|e| {
            Status::internal(format!("Failed to build XDBC type info: {e}"))
        })?;

        let batch = xdbc_data.record_batch(query.data_type).map_err(|e| {
            Status::internal(format!("Failed to filter XDBC type info: {e}"))
        })?;

        self.batches_to_stream(vec![batch])
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request).await?;
        let batches = self
            .query_handler
            .execute(&session, &ticket.query)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(rows)
    }

    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Build qualified table name from catalog + schema + table
        let mut qualified = String::new();
        if let Some(ref cat) = ticket.catalog {
            qualified.push_str(cat);
            qualified.push('.');
        }
        if let Some(ref schema) = ticket.schema {
            qualified.push_str(schema);
            qualified.push('.');
        }
        qualified.push_str(&ticket.table);

        debug!(
            username = %session.user.username,
            table = %qualified,
            "DoPut statement ingest"
        );

        // Decode the Arrow stream into RecordBatches
        let stream = request.into_inner();
        let flight_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to decode Arrow stream: {e}")))?;

        let rows = self
            .query_handler
            .write_handler()
            .handle_ingest(&session, &qualified, batches)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        Ok(rows as i64)
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Substrait plans not supported"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // Parameter binding not yet supported — return the existing handle unchanged.
        // This allows JDBC drivers to complete the prepared statement flow even without
        // actual parameter substitution.
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request).await?;

        // Decode the SQL from the prepared statement handle
        let fetch: FetchResults =
            Message::decode(&*query.prepared_statement_handle)
                .map_err(|e| Status::internal(format!("Failed to decode prepared statement handle: {e}")))?;

        let batches = self
            .query_handler
            .execute(&session, &fetch.handle)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(rows)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let session = self.get_session_from_request(&request).await?;
        let sql = &query.query;

        debug!(username = %session.user.username, sql_len = sql.len(), "Creating prepared statement");

        // Get schema by planning the query
        let schema = self
            .query_handler
            .get_schema(&session, sql)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        // Encode the SQL in the handle so we can execute it later
        let fetch = FetchResults {
            handle: sql.clone(),
        };
        let handle = fetch.encode_to_vec();

        // Encode the schema as IPC for the prepared statement result.
        // Use FlightInfo's try_with_schema to get the encoded bytes, then extract them.
        let encoded_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: encoded_info.schema,
            parameter_schema: Default::default(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented("Substrait plans not supported"))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented("Transactions not supported"))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Transactions not supported"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("Savepoints not supported"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Savepoints not supported"))
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        // ActionCancelQueryRequest.info contains the serialized FlightInfo
        // from get_flight_info_statement. Decode it to extract the query
        // handle from the first endpoint ticket.
        let flight_info: arrow_flight::FlightInfo =
            Message::decode(&*query.info).map_err(|e| {
                Status::invalid_argument(format!(
                    "CancelQuery: failed to decode FlightInfo: {e}"
                ))
            })?;

        let query_id = flight_info
            .endpoint
            .first()
            .and_then(|ep| ep.ticket.as_ref())
            .map(|t| {
                // Try to decode as our FetchResults protobuf to get the handle
                if let Ok(fetch) = <FetchResults as Message>::decode(&*t.ticket) {
                    fetch.handle
                } else {
                    String::from_utf8_lossy(&t.ticket).to_string()
                }
            })
            .ok_or_else(|| {
                Status::invalid_argument(
                    "CancelQuery request missing ticket in FlightInfo endpoint",
                )
            })?;

        // QueryTracker uses Uuid keys. Try to parse the handle as a UUID;
        // if it's a SQL string (legacy ticket format) we cannot map it to a
        // tracked query yet — full query-ID propagation via tickets is planned.
        let cancelled = if let Ok(uuid) = uuid::Uuid::parse_str(&query_id) {
            self.query_tracker.cancel(&uuid)
        } else {
            debug!(
                query_id = %query_id,
                "CancelQuery: handle is not a UUID, cannot map to tracked query"
            );
            false
        };

        if cancelled {
            info!(query_id = %query_id, "Query cancelled via Flight CancelQuery action");
        } else {
            debug!(
                query_id = %query_id,
                "CancelQuery: query not found in tracker (already completed or unknown)"
            );
        }

        // ActionCancelQueryResult.result is an i32 matching the CancelResult
        // protobuf enum: 0 = UNSPECIFIED, 1 = CANCELLED, 2 = CANCELLING,
        // 3 = NOT_CANCELLABLE.
        Ok(ActionCancelQueryResult {
            result: if cancelled { 1 } else { 0 },
        })
    }

    /// Handle custom (non-Flight-SQL) actions such as worker heartbeats.
    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let (metadata, _, action) = request.into_parts();
        match action.r#type.as_str() {
            "heartbeat" => {
                // Validate the worker secret when one is configured.
                if !self.worker_secret.is_empty() {
                    use subtle::ConstantTimeEq;
                    let provided = metadata
                        .get("x-sqe-worker-secret")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    let provided_bytes = provided.as_bytes();
                    let secret_bytes = self.worker_secret.as_bytes();
                    if provided_bytes.len() != secret_bytes.len()
                        || !bool::from(provided_bytes.ct_eq(secret_bytes))
                    {
                        return Err(Status::unauthenticated("Invalid worker secret"));
                    }
                }

                let worker_url = std::str::from_utf8(&action.body).map_err(|e| {
                    Status::invalid_argument(format!("Invalid heartbeat body: {e}"))
                })?;

                if worker_url.is_empty() {
                    return Err(Status::invalid_argument(
                        "Heartbeat body must contain the worker URL",
                    ));
                }

                if let Some(ref registry) = self.worker_registry {
                    debug!(worker = %worker_url, "Received heartbeat from worker");
                    registry.register_heartbeat(worker_url).await;
                } else {
                    debug!(
                        worker = %worker_url,
                        "Received heartbeat but no worker registry configured, ignoring"
                    );
                }

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                Ok(Response::new(
                    Box::pin(stream::once(async { Ok(result) }))
                        as <Self as FlightService>::DoActionStream,
                ))
            }
            other => Err(Status::invalid_argument(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::builder::StringBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::RecordBatch;
    use arrow_flight::sql::metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoDataBuilder};
    use arrow_flight::sql::{Nullable, Searchable, SqlInfo, XdbcDataType};
    use arrow_schema::{DataType, Field, Schema};

    // -----------------------------------------------------------------------
    // batches_to_stream: empty input
    // -----------------------------------------------------------------------

    #[test]
    fn batches_to_stream_empty_returns_ok() {
        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let result = super::encode_batches_to_stream(vec![], options);
        assert!(result.is_ok(), "empty batches should produce Ok response");
    }

    // -----------------------------------------------------------------------
    // batches_to_stream: single batch
    // -----------------------------------------------------------------------

    #[test]
    fn batches_to_stream_single_batch_returns_ok() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let result = super::encode_batches_to_stream(vec![batch], options);
        assert!(result.is_ok(), "single batch should produce Ok response");
    }

    // -----------------------------------------------------------------------
    // table_types RecordBatch: TABLE and VIEW rows
    // -----------------------------------------------------------------------

    #[test]
    fn table_types_batch_contains_table_and_view() {
        // Replicate the exact logic from do_get_table_types so we can test it
        // without gRPC overhead.
        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        let arr = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("batch should be valid");

        assert_eq!(batch.num_rows(), 2);
        let col = batch.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "TABLE");
        assert_eq!(col.value(1), "VIEW");
    }

    #[test]
    fn table_types_batch_schema_has_expected_field() {
        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        let arr = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).expect("batch should be valid");

        let batch_schema = batch.schema();
        let field = batch_schema.field(0);
        assert_eq!(field.name(), "table_type");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(!field.is_nullable());
    }

    // -----------------------------------------------------------------------
    // XdbcTypeInfoDataBuilder: expected type count
    // -----------------------------------------------------------------------

    #[test]
    fn xdbc_type_info_builder_produces_expected_type_count() {
        let mut builder = XdbcTypeInfoDataBuilder::new();

        // boolean
        builder.append(XdbcTypeInfo {
            type_name: "boolean".into(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcBit,
            num_prec_radix: Some(0),
            ..Default::default()
        });

        // integer types
        for (name, dt, size, radix) in [
            ("tinyint", XdbcDataType::XdbcTinyint, 3, 10),
            ("smallint", XdbcDataType::XdbcSmallint, 5, 10),
            ("integer", XdbcDataType::XdbcInteger, 10, 10),
            ("bigint", XdbcDataType::XdbcBigint, 19, 10),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(radix),
                ..Default::default()
            });
        }

        // floating-point types
        for (name, dt, size) in [
            ("real", XdbcDataType::XdbcReal, 7),
            ("double", XdbcDataType::XdbcDouble, 15),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(10),
                ..Default::default()
            });
        }

        // decimal
        builder.append(XdbcTypeInfo {
            type_name: "decimal".into(),
            data_type: XdbcDataType::XdbcDecimal,
            column_size: Some(38),
            create_params: Some(vec!["precision".into(), "scale".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: true,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcDecimal,
            minimum_scale: Some(0),
            maximum_scale: Some(38),
            num_prec_radix: Some(10),
            ..Default::default()
        });

        // varchar
        builder.append(XdbcTypeInfo {
            type_name: "varchar".into(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("'".into()),
            literal_suffix: Some("'".into()),
            create_params: Some(vec!["length".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarchar,
            ..Default::default()
        });

        // varbinary
        builder.append(XdbcTypeInfo {
            type_name: "varbinary".into(),
            data_type: XdbcDataType::XdbcVarbinary,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("X'".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarbinary,
            ..Default::default()
        });

        // date
        builder.append(XdbcTypeInfo {
            type_name: "date".into(),
            data_type: XdbcDataType::XdbcDate,
            column_size: Some(10),
            literal_prefix: Some("DATE '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcDate,
            ..Default::default()
        });

        // time
        builder.append(XdbcTypeInfo {
            type_name: "time".into(),
            data_type: XdbcDataType::XdbcTime,
            column_size: Some(15),
            literal_prefix: Some("TIME '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTime,
            ..Default::default()
        });

        // timestamp
        builder.append(XdbcTypeInfo {
            type_name: "timestamp".into(),
            data_type: XdbcDataType::XdbcTimestamp,
            column_size: Some(29),
            literal_prefix: Some("TIMESTAMP '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTimestamp,
            ..Default::default()
        });

        let xdbc_data = builder.build().expect("builder should produce valid data");

        // No data_type filter → all 13 types returned.
        let batch = xdbc_data
            .record_batch(None)
            .expect("record_batch should succeed");

        // 1 boolean + 4 integer types + 2 floating + 1 decimal + 1 varchar
        // + 1 varbinary + 1 date + 1 time + 1 timestamp = 13
        assert_eq!(
            batch.num_rows(),
            13,
            "expected 13 XDBC types but got {}",
            batch.num_rows()
        );
    }

    #[test]
    fn xdbc_type_info_first_type_is_boolean() {
        let mut builder = XdbcTypeInfoDataBuilder::new();
        builder.append(XdbcTypeInfo {
            type_name: "boolean".into(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcBit,
            num_prec_radix: Some(0),
            ..Default::default()
        });

        let xdbc_data = builder.build().expect("builder should produce valid data");
        let batch = xdbc_data
            .record_batch(None)
            .expect("record_batch should succeed");

        assert_eq!(batch.num_rows(), 1);
        // Column 0 is type_name.
        let type_name_col = batch.column(0).as_string::<i32>();
        assert_eq!(type_name_col.value(0), "boolean");
    }

    // -----------------------------------------------------------------------
    // SqlInfoDataBuilder: server name, version, Arrow version
    // -----------------------------------------------------------------------

    #[test]
    fn sql_info_builder_builds_without_error() {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "SQE Coordinator");
        builder.append(SqlInfo::FlightSqlServerVersion, "0.1.0");
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");

        let result = builder.build();
        assert!(result.is_ok(), "SqlInfoDataBuilder::build() should succeed");
    }

    #[test]
    fn sql_info_data_produces_non_empty_batch() {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "SQE Coordinator");
        builder.append(SqlInfo::FlightSqlServerVersion, "0.1.0");
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");

        let sql_info_data = builder.build().expect("build should succeed");

        // Build a CommandGetSqlInfo with no filters (return all info keys).
        use arrow_flight::sql::CommandGetSqlInfo;
        let query = CommandGetSqlInfo { info: vec![] };
        let info_builder = query.into_builder(&sql_info_data);
        let batch = info_builder.build().expect("info_builder.build() should succeed");

        // We appended 3 entries; the batch must contain at least those rows.
        assert!(
            batch.num_rows() >= 3,
            "expected at least 3 sql info rows, got {}",
            batch.num_rows()
        );
    }

    // -----------------------------------------------------------------------
    // Multi-endpoint FlightInfo (Task 28 — Stream 9)
    // -----------------------------------------------------------------------

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn test_ticket(handle: &str) -> Ticket {
        Ticket {
            ticket: handle.as_bytes().to_vec().into(),
        }
    }

    #[test]
    fn build_flight_info_single_returns_one_endpoint() {
        let schema = test_schema();
        let ticket = test_ticket("SELECT 1");

        let info = SqeFlightSqlService::build_flight_info_single(&schema, ticket.clone())
            .expect("build_flight_info_single should succeed");

        assert_eq!(
            info.endpoint.len(),
            1,
            "single-endpoint FlightInfo must have exactly 1 endpoint"
        );
        let ep = &info.endpoint[0];
        assert_eq!(
            ep.ticket.as_ref().expect("ticket must be set"),
            &ticket,
            "ticket must match the input"
        );
        assert!(
            ep.location.is_empty(),
            "single-endpoint should have no explicit location (client uses same server)"
        );
        assert_eq!(info.total_records, -1);
    }

    #[test]
    fn build_flight_info_distributed_with_multiple_workers() {
        let schema = test_schema();
        let endpoints = vec![
            (
                "grpc://worker-1:50051".to_string(),
                test_ticket("part-0"),
            ),
            (
                "grpc://worker-2:50051".to_string(),
                test_ticket("part-1"),
            ),
            (
                "grpc://worker-3:50051".to_string(),
                test_ticket("part-2"),
            ),
        ];
        let fallback = test_ticket("fallback");

        let info = SqeFlightSqlService::build_flight_info_distributed(
            &schema, &endpoints, fallback,
        )
        .expect("build_flight_info_distributed should succeed");

        assert_eq!(
            info.endpoint.len(),
            3,
            "distributed FlightInfo must have one endpoint per worker"
        );

        for (i, (expected_url, expected_ticket)) in endpoints.iter().enumerate() {
            let ep = &info.endpoint[i];
            assert_eq!(
                ep.ticket.as_ref().expect("ticket must be set"),
                expected_ticket,
                "endpoint {} ticket must match",
                i
            );
            assert_eq!(
                ep.location.len(),
                1,
                "endpoint {} must have exactly 1 location",
                i
            );
            assert_eq!(
                ep.location[0].uri, *expected_url,
                "endpoint {} location URI must match worker URL",
                i
            );
        }
    }

    #[test]
    fn build_flight_info_distributed_empty_falls_back_to_single() {
        let schema = test_schema();
        let empty: Vec<(String, Ticket)> = vec![];
        let fallback = test_ticket("SELECT 1");

        let info = SqeFlightSqlService::build_flight_info_distributed(
            &schema,
            &empty,
            fallback.clone(),
        )
        .expect("empty executor list should fall back to single endpoint");

        assert_eq!(
            info.endpoint.len(),
            1,
            "fallback must produce exactly 1 endpoint"
        );
        let ep = &info.endpoint[0];
        assert_eq!(
            ep.ticket.as_ref().expect("ticket must be set"),
            &fallback,
            "fallback ticket must match"
        );
        assert!(
            ep.location.is_empty(),
            "fallback endpoint should have no explicit location"
        );
    }

    #[test]
    fn build_flight_info_distributed_single_worker() {
        let schema = test_schema();
        let endpoints = vec![(
            "grpc://worker-1:50051".to_string(),
            test_ticket("part-0"),
        )];
        let fallback = test_ticket("fallback");

        let info = SqeFlightSqlService::build_flight_info_distributed(
            &schema, &endpoints, fallback,
        )
        .expect("single-worker distributed should succeed");

        assert_eq!(info.endpoint.len(), 1);
        let ep = &info.endpoint[0];
        assert_eq!(
            ep.location[0].uri,
            "grpc://worker-1:50051"
        );
        assert_eq!(
            ep.ticket.as_ref().unwrap().ticket.as_ref(),
            b"part-0"
        );
    }

    #[test]
    fn build_flight_info_distributed_carries_schema_bytes() {
        let schema = test_schema();
        let endpoints = vec![(
            "grpc://worker-1:50051".to_string(),
            test_ticket("part-0"),
        )];
        let fallback = test_ticket("fallback");

        let info = SqeFlightSqlService::build_flight_info_distributed(
            &schema, &endpoints, fallback,
        )
        .expect("should succeed");

        // The FlightInfo must carry encoded schema bytes so the client can
        // decode the result schema before opening DoGet streams.
        assert!(
            !info.schema.is_empty(),
            "FlightInfo must carry encoded schema bytes"
        );
    }

    // -----------------------------------------------------------------------
    // QueryResultLocation enum
    // -----------------------------------------------------------------------

    #[test]
    fn query_result_location_local_is_not_distributed() {
        let loc = QueryResultLocation::Local;
        assert!(!loc.is_distributed());
    }

    #[test]
    fn query_result_location_distributed_empty_is_not_distributed() {
        let loc = QueryResultLocation::Distributed(vec![]);
        assert!(
            !loc.is_distributed(),
            "empty distributed list should be treated as non-distributed"
        );
    }

    #[test]
    fn query_result_location_distributed_non_empty_is_distributed() {
        let loc = QueryResultLocation::Distributed(vec![(
            "grpc://worker-1:50051".to_string(),
            test_ticket("part-0"),
        )]);
        assert!(loc.is_distributed());
    }
}

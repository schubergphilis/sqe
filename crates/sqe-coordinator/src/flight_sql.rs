use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoDataBuilder};
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
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use base64::Engine;

/// Base64 engine for the HTTP Basic `authorization` handshake header that
/// accepts input WITH or WITHOUT `=` padding. RFC 4648 mandates padding and the
/// Rust Flight clients send it, but the Go ADBC FlightSQL driver (and therefore
/// the dbt-sqe adapter, which connects over ADBC) sends UNPADDED base64. A
/// padding-strict decoder rejects those with "Invalid padding", breaking the
/// handshake. `DecodePaddingMode::Indifferent` accepts both.
const BASIC_AUTH_B64: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tower::{Layer, Service};
use tracing::{debug, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use sqe_core::SqeConfig;
use sqe_sql::{
    classify_normalized, pre_parse_pipeline, SqlFrontDoor, StatementKind, UserSql, WireDialect,
};

use crate::prepared_statements::{PreparedStatement, PreparedStatementStore};
use crate::query_handler::QueryHandler;
use crate::query_tracker::QueryTracker;
use crate::session_manager::SessionManager;
use crate::worker_registry::WorkerRegistry;

// Re-export helpers so callers that imported directly from this module
// keep working without changes.
pub use crate::flight_sql_helpers::FetchResults;
use crate::flight_sql_helpers::{sqe_error_to_status, FlightStream};

/// Instruments every Flight SQL RPC before tonic dispatches to a handler.
///
/// Doing this at the service boundary ensures authentication, session lookup,
/// planning, prepared statements, actions, and stream construction all inherit
/// the remote W3C parent. The instrumented future also keeps that context when
/// tonic moves request work between runtime tasks.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlightTraceLayer;

impl<S> Layer<S> for FlightTraceLayer {
    type Service = FlightTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FlightTraceService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct FlightTraceService<S> {
    inner: S,
}

impl<S, B> Service<tonic::codegen::http::Request<B>> for FlightTraceService<S>
where
    S: Service<tonic::codegen::http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = tracing::instrument::Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let parent =
            sqe_metrics::propagation::extract_trace_context_from_headers(request.headers());
        let correlation = sqe_metrics::propagation::correlation_ids_from_headers(request.headers());
        let span = tracing::info_span!(
            "flight_sql.request",
            rpc.system = "grpc",
            rpc.method = %request.uri().path(),
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
            request_id = tracing::field::Empty,
            session_id = tracing::field::Empty,
            http.request_id = tracing::field::Empty,
            sqe.session.id = tracing::field::Empty,
        );
        let _ = span.set_parent(parent);
        sqe_metrics::propagation::record_trace_fields(&span);
        if let Some(request_id) = correlation.request_id.as_deref() {
            span.record("request_id", request_id);
            span.record("http.request_id", request_id);
        }
        if let Some(session_id) = correlation.session_id.as_deref() {
            span.record("session_id", session_id);
            span.record("sqe.session.id", session_id);
        }
        self.inner.call(request).instrument(span)
    }
}

/// Populate the full Flight SQL `SqlInfo` table.
///
/// Apache ADBC, Tableau's Flight SQL connector, and dbt-sqe read these
/// keys to decide which catalog APIs to call and which SQL grammar to
/// emit. Absent keys silently degrade clients (Tableau falls back to no
/// identifier quoting; JDBC `DatabaseMetaData.getIdentifierQuoteString`
/// returns `""`). Apache Arrow Flight SQL spec section
/// `CommandGetSqlInfo` enumerates the standard set.
pub fn build_sql_info_data() -> Result<arrow_flight::sql::metadata::SqlInfoData, Status> {
    use arrow_flight::sql::SqlInfo as I;

    let mut b = SqlInfoDataBuilder::new();

    // Server identity.
    b.append(I::FlightSqlServerName, "SQE Coordinator");
    b.append(I::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(I::FlightSqlServerArrowVersion, "1.3");

    // Server capabilities. SQE accepts writes (CTAS/INSERT/UPDATE/DELETE).
    b.append(I::FlightSqlServerReadOnly, false);
    b.append(I::FlightSqlServerSql, true);
    b.append(I::FlightSqlServerSubstrait, false);
    // SqlSupportedTransactions.SQL_TRANSACTIONS_UNSPECIFIED = 0.
    b.append(I::FlightSqlServerTransaction, 0i32);
    b.append(I::FlightSqlServerCancel, true);
    b.append(I::FlightSqlServerBulkIngestion, false);
    b.append(I::FlightSqlServerIngestTransactionsSupported, false);

    // DDL support.
    b.append(I::SqlDdlCatalog, false);
    b.append(I::SqlDdlSchema, true);
    b.append(I::SqlDdlTable, true);

    // Identifier handling. Iceberg / SQE preserve identifier case when
    // double-quoted; bare identifiers fold to lower-case (CaseSensitivityLowerCase = 2).
    b.append(I::SqlIdentifierCase, 2i32);
    b.append(I::SqlIdentifierQuoteChar, "\"");
    b.append(I::SqlQuotedIdentifierCase, 3i32); // CaseSensitive = 3

    // Catalog and schema semantics.
    b.append(I::SqlAllTablesAreSelectable, true);
    b.append(I::SqlCatalogTerm, "catalog");
    b.append(I::SqlSchemaTerm, "schema");
    b.append(I::SqlProcedureTerm, "procedure");
    b.append(I::SqlCatalogAtStart, true);
    // SqlNullOrdering.SQL_NULLS_SORTED_HIGH = 0.
    b.append(I::SqlNullOrdering, 0i32);

    // Function lists. The values are kept in sync with the trino-compat
    // function registry (`sqe-trino-functions`) and the DataFusion stock
    // function set.
    b.append(
        I::SqlNumericFunctions,
        vec![
            "ABS".into(),
            "CEIL".into(),
            "FLOOR".into(),
            "ROUND".into(),
            "SQRT".into(),
            "POWER".into(),
            "MOD".into(),
            "EXP".into(),
            "LN".into(),
            "LOG10".into(),
            "SIGN".into(),
            "RAND".into(),
        ],
    );
    b.append(
        I::SqlStringFunctions,
        vec![
            "CONCAT".into(),
            "LENGTH".into(),
            "LOWER".into(),
            "UPPER".into(),
            "SUBSTRING".into(),
            "TRIM".into(),
            "LTRIM".into(),
            "RTRIM".into(),
            "REPLACE".into(),
            "POSITION".into(),
            "REGEXP_LIKE".into(),
            "REGEXP_REPLACE".into(),
        ],
    );
    b.append(
        I::SqlDatetimeFunctions,
        vec![
            "CURRENT_DATE".into(),
            "CURRENT_TIME".into(),
            "CURRENT_TIMESTAMP".into(),
            "DATE_TRUNC".into(),
            "DATE_DIFF".into(),
            "DATE_ADD".into(),
            "EXTRACT".into(),
            "FROM_UNIXTIME".into(),
            "TO_TIMESTAMP".into(),
            "YEAR".into(),
            "MONTH".into(),
            "DAY".into(),
        ],
    );
    b.append(
        I::SqlSystemFunctions,
        vec![
            "CURRENT_USER".into(),
            "CURRENT_CATALOG".into(),
            "CURRENT_SCHEMA".into(),
            "VERSION".into(),
        ],
    );

    // Reserved-word list (subset relevant to the engine's parser).
    b.append(
        I::SqlKeywords,
        vec![
            "ALTER".into(),
            "AND".into(),
            "AS".into(),
            "ASC".into(),
            "BETWEEN".into(),
            "BIGINT".into(),
            "BY".into(),
            "CASE".into(),
            "CAST".into(),
            "COLUMN".into(),
            "CREATE".into(),
            "DELETE".into(),
            "DESC".into(),
            "DISTINCT".into(),
            "DROP".into(),
            "ELSE".into(),
            "END".into(),
            "EXISTS".into(),
            "FALSE".into(),
            "FOR".into(),
            "FROM".into(),
            "FULL".into(),
            "GROUP".into(),
            "HAVING".into(),
            "IN".into(),
            "INNER".into(),
            "INSERT".into(),
            "INTO".into(),
            "IS".into(),
            "JOIN".into(),
            "LEFT".into(),
            "LIKE".into(),
            "LIMIT".into(),
            "MERGE".into(),
            "NOT".into(),
            "NULL".into(),
            "ON".into(),
            "OR".into(),
            "ORDER".into(),
            "OUTER".into(),
            "PARTITION".into(),
            "PREPARE".into(),
            "RIGHT".into(),
            "SELECT".into(),
            "SET".into(),
            "TABLE".into(),
            "THEN".into(),
            "TRUE".into(),
            "UNION".into(),
            "UPDATE".into(),
            "USING".into(),
            "VALUES".into(),
            "VIEW".into(),
            "WHEN".into(),
            "WHERE".into(),
            "WITH".into(),
        ],
    );
    b.append(I::SqlSearchStringEscape, "\\");
    b.append(I::SqlExtraNameCharacters, "");

    // Grammar level. SqlSupportedGrammar.SQL_MINIMUM_GRAMMAR = 0.
    b.append(I::SqlSupportedGrammar, 0i32);
    // SqlAnsi92SupportedLevel.ANSI92_INTERMEDIATE_SQL = 1.
    b.append(I::SqlAnsi92SupportedLevel, 1i32);

    // Common capability booleans.
    b.append(I::SqlSupportsColumnAliasing, true);
    b.append(I::SqlNullPlusNullIsNull, true);
    b.append(I::SqlSupportsTableCorrelationNames, true);
    b.append(I::SqlSupportsDifferentTableCorrelationNames, false);
    b.append(I::SqlSupportsExpressionsInOrderBy, true);
    b.append(I::SqlSupportsOrderByUnrelated, true);
    b.append(I::SqlSupportsLikeEscapeClause, true);
    b.append(I::SqlSupportsNonNullableColumns, true);
    b.append(I::SqlSupportsIntegrityEnhancementFacility, false);
    b.append(I::SqlSelectForUpdateSupported, false);
    b.append(I::SqlStoredProceduresSupported, false);
    b.append(I::SqlCorrelatedSubqueriesSupported, true);
    // SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE = 0 / UPDATE = 1.
    // SQE doesn't support cursors; emit empty bitmask.
    b.append(I::SqlSupportedPositionedCommands, 0i32);

    // Limits. Iceberg + SQE have no fixed hard limits, but JDBC expects
    // a value. `0` means "unknown / no limit" per the spec.
    b.append(I::SqlMaxColumnNameLength, 1024i64);
    b.append(I::SqlMaxColumnsInGroupBy, 0i64);
    b.append(I::SqlMaxColumnsInIndex, 0i64);
    b.append(I::SqlMaxColumnsInOrderBy, 0i64);
    b.append(I::SqlMaxColumnsInSelect, 0i64);
    b.append(I::SqlMaxColumnsInTable, 0i64);
    b.append(I::SqlMaxConnections, 0i64);
    b.append(I::SqlMaxCursorNameLength, 0i64);
    b.append(I::SqlMaxIndexLength, 0i64);
    b.append(I::SqlDbSchemaNameLength, 1024i64);
    b.append(I::SqlMaxProcedureNameLength, 0i64);
    b.append(I::SqlMaxCatalogNameLength, 1024i64);
    b.append(I::SqlMaxRowSize, 0i64);
    b.append(I::SqlMaxRowSizeIncludesBlobs, true);
    b.append(I::SqlMaxStatementLength, 0i64);
    b.append(I::SqlMaxStatements, 0i64);
    b.append(I::SqlMaxTableNameLength, 1024i64);
    b.append(I::SqlMaxTablesInSelect, 0i64);
    b.append(I::SqlMaxUsernameLength, 1024i64);
    b.append(I::SqlMaxBinaryLiteralLength, 0i64);
    b.append(I::SqlMaxCharLiteralLength, 0i64);

    // Transactions. SQE runs in autocommit; no isolation levels supported.
    b.append(I::SqlDefaultTransactionIsolation, 0i64);
    b.append(I::SqlTransactionsSupported, false);
    b.append(I::SqlDataDefinitionCausesTransactionCommit, true);

    b.build()
        .map_err(|e| Status::internal(format!("Failed to build SQL info: {e}")))
}

/// SQL LIKE matching used for Flight SQL filter patterns.
///
/// `%` matches zero or more characters; `_` matches a single character.
/// All other characters match literally. Comparison is case-sensitive
/// because the Flight SQL spec keeps the identifier case the catalog
/// reports it as. Catalogs that fold to lower-case at the source still
/// match a lower-case pattern.
pub fn like_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    like_match_inner(&p, &v)
}

fn like_match_inner(pattern: &[char], value: &[char]) -> bool {
    match (pattern.first(), value.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some('%'), _) => {
            // % matches zero characters
            if like_match_inner(&pattern[1..], value) {
                return true;
            }
            // or any non-empty prefix
            if !value.is_empty() {
                return like_match_inner(pattern, &value[1..]);
            }
            false
        }
        (Some('_'), Some(_)) => like_match_inner(&pattern[1..], &value[1..]),
        (Some('_'), None) => false,
        (Some(p), Some(v)) if p == v => like_match_inner(&pattern[1..], &value[1..]),
        _ => false,
    }
}

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

/// Strip the `:port` suffix from a peer-address string so the auth
/// rate-limit key is stable across the ephemeral source ports a single
/// IP cycles through.
fn strip_port_for_key(s: &str) -> &str {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return &rest[..end];
        }
    }
    match s.rfind(':') {
        Some(idx) if !s[..idx].contains(':') => &s[..idx],
        _ => s,
    }
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
    worker_secret: sqe_core::SecretString,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    rate_limiter: Option<Arc<crate::rate_limiter::QueryRateLimiter>>,
    auth_rate_limiter: Option<Arc<crate::rate_limiter::AuthRateLimiter>>,
    metadata_rate_limiter: Option<Arc<crate::rate_limiter::MetadataRateLimiter>>,
    /// Audit logger inherited from the `QueryHandler`. Used to emit
    /// `AuditKind::Auth` events on handshake success/failure and on
    /// per-request bearer/session validation failures.
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    /// Server-side prepared statements (finding 406, #586).
    ///
    /// Keyed by `(authenticated session id, statement id)` so one session can
    /// never read, bind or execute another session's statement (WEB-03). Each
    /// entry holds the plan built once at `CreatePreparedStatement` with its
    /// `$n` placeholders, the inferred parameter types, the output schema and
    /// the current typed bind. Bounded and idle-expiring (WEB-04) so abandoned
    /// statements cannot exhaust coordinator memory.
    prepared_statements: PreparedStatementStore,
    /// Statements whose `FlightInfo` has been issued but whose `DoGet` has
    /// not yet arrived, keyed by the query id carried in the ticket.
    ///
    /// Finding 403: the ticket used to carry the raw SQL and the query id
    /// was minted only inside `execute_stream`, so the client never learned
    /// an id it could cancel. The id is now minted at `GetFlightInfo`, the
    /// ticket carries it, `DoGet` resolves the SQL here and runs under the
    /// same id, and `CancelQuery` can reach the tracker. Bounded and TTL'd:
    /// an abandoned `FlightInfo` expires.
    pending_statements: Arc<moka::future::Cache<uuid::Uuid, PendingStatement>>,
}

/// A statement awaiting its `DoGet`, bound to the user who planned it.
#[derive(Clone, Debug)]
struct PendingStatement {
    sql: String,
    /// Owner: the ticket may only be redeemed by the user it was issued to.
    username: String,
    /// For prepared statements, the statement id so `DoGet` runs the stored
    /// plan with the bind current at execution time (finding 406).
    prepared: Option<uuid::Uuid>,
}

/// What a redeemed ticket resolves to.
enum ResolvedStatement {
    /// Ad-hoc SQL text (`GetFlightInfo(CommandStatementQuery)` or a legacy
    /// raw-SQL ticket).
    Sql(String),
    /// A prepared statement: the stored plan plus whatever is bound.
    Prepared(Arc<PreparedStatement>),
}

/// Maximum issued-but-not-fetched statement tickets across all sessions.
const PENDING_STATEMENTS_MAX_ENTRIES: u64 = 10_000;
/// How long a ticket from `GetFlightInfo` stays redeemable by `DoGet`.
const PENDING_STATEMENTS_TTL: std::time::Duration = std::time::Duration::from_secs(300);

impl SqeFlightSqlService {
    pub fn new(
        session_manager: Arc<SessionManager>,
        query_handler: Arc<QueryHandler>,
        config: SqeConfig,
    ) -> Self {
        let worker_secret = config.coordinator.worker_secret.clone();
        let query_tracker = Arc::clone(query_handler.query_tracker());
        let audit = query_handler.audit().cloned();
        Self {
            session_manager,
            query_handler,
            config,
            worker_registry: None,
            query_tracker,
            worker_secret,
            metrics: None,
            rate_limiter: None,
            auth_rate_limiter: None,
            metadata_rate_limiter: None,
            audit,
            prepared_statements: PreparedStatementStore::default(),
            pending_statements: Arc::new(
                moka::future::Cache::builder()
                    .max_capacity(PENDING_STATEMENTS_MAX_ENTRIES)
                    .time_to_live(PENDING_STATEMENTS_TTL)
                    .build(),
            ),
        }
    }

    /// Mint a query id for a statement, park it for `DoGet`, and return the
    /// ticket carrying the id. Finding 403.
    async fn issue_statement_ticket(
        &self,
        session: &sqe_core::Session,
        sql: String,
        prepared: Option<uuid::Uuid>,
    ) -> (uuid::Uuid, Ticket) {
        let query_id = uuid::Uuid::now_v7();
        self.pending_statements
            .insert(
                query_id,
                PendingStatement {
                    sql,
                    username: session.user.username.clone(),
                    prepared,
                },
            )
            .await;
        let fetch = FetchResults {
            handle: query_id.to_string(),
        };
        let ticket = Ticket {
            ticket: fetch.as_any().encode_to_vec().into(),
        };
        (query_id, ticket)
    }

    /// Resolve a ticket handle into the statement to run and the query id to
    /// run it under.
    ///
    /// New tickets carry a UUID minted by [`Self::issue_statement_ticket`];
    /// the entry is consumed (single-use). A prepared-statement ticket
    /// resolves to the stored statement (finding 406), whose bind current at
    /// execution time is applied. A handle that is not a UUID is a legacy
    /// raw-SQL ticket from an older client and runs under a fresh id.
    async fn resolve_statement_handle(
        &self,
        session: &sqe_core::Session,
        handle: &str,
    ) -> Result<(ResolvedStatement, Option<uuid::Uuid>), Status> {
        let Ok(query_id) = uuid::Uuid::parse_str(handle) else {
            return Ok((ResolvedStatement::Sql(handle.to_string()), None));
        };
        let Some(pending) = self.pending_statements.get(&query_id).await else {
            return Err(Status::not_found(format!(
                "Statement handle {query_id} is unknown, expired, or already fetched; \
                 call GetFlightInfo again"
            )));
        };
        if pending.username != session.user.username {
            warn!(
                query_id = %query_id,
                caller = %session.user.username,
                owner = %pending.username,
                "DoGet denied: ticket was issued to another user"
            );
            return Err(Status::permission_denied(
                "Statement handle was issued to another user",
            ));
        }
        self.pending_statements.invalidate(&query_id).await;

        let resolved = match pending.prepared {
            Some(statement_id) => {
                ResolvedStatement::Prepared(self.lookup_prepared(session, statement_id).await?)
            }
            None => ResolvedStatement::Sql(pending.sql),
        };
        Ok((resolved, Some(query_id)))
    }

    /// Look up a prepared statement by id, scoped to the caller's session
    /// (WEB-03): another session's id is simply unknown.
    async fn lookup_prepared(
        &self,
        session: &sqe_core::Session,
        statement_id: uuid::Uuid,
    ) -> Result<Arc<PreparedStatement>, Status> {
        self.prepared_statements
            .get(&session.id, statement_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Prepared statement {statement_id} is unknown, closed, or expired; \
                     prepare it again"
                ))
            })
    }

    /// Decode a client-supplied prepared-statement handle and look it up.
    async fn prepared_from_handle(
        &self,
        session: &sqe_core::Session,
        handle: &[u8],
    ) -> Result<(uuid::Uuid, Arc<PreparedStatement>), Status> {
        let statement_id =
            PreparedStatementStore::parse_handle(handle).map_err(Status::invalid_argument)?;
        let statement = self.lookup_prepared(session, statement_id).await?;
        Ok((statement_id, statement))
    }

    /// Decode an inbound parameter stream and bind it to `statement`.
    ///
    /// Finding 406: values are bound as typed Arrow scalars and checked
    /// against the inferred placeholder types here, so a wrong-type bind is
    /// rejected before anything is planned or executed. An empty batch (a
    /// driver's bind call for a statement without parameters) leaves any
    /// existing bind alone; the bind otherwise persists until `Close`, a
    /// re-bind, or eviction, so a second `executeQuery()` without new
    /// `setX` calls runs with the previous values.
    async fn bind_prepared_from_stream<S>(
        &self,
        statement: &PreparedStatement,
        stream: S,
    ) -> Result<(), Status>
    where
        S: futures::Stream<Item = Result<arrow_flight::FlightData, Status>>
            + Send
            + Unpin
            + 'static,
    {
        let values = match decode_parameter_stream(stream).await {
            Ok(values) => values,
            // A statement without parameters has nothing to bind; tolerate a
            // driver that sends a malformed empty bind for it.
            Err(e) if statement.param_count() == 0 => {
                warn!(error = %e, "Ignoring undecodable bind for a statement without parameters");
                return Ok(());
            }
            Err(e) => return Err(Status::invalid_argument(e)),
        };
        if values.is_empty() {
            return Ok(());
        }
        statement.bind(values).map_err(Status::invalid_argument)
    }

    #[must_use = "with_metrics consumes self; bind the returned service"]
    pub fn with_metrics(mut self, metrics: Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns a reference to the query tracker for external access
    /// (e.g., metrics, admin endpoints).
    pub fn query_tracker(&self) -> &Arc<QueryTracker> {
        &self.query_tracker
    }

    #[must_use = "with_worker_registry consumes self; bind the returned service"]
    pub fn with_worker_registry(mut self, registry: Arc<WorkerRegistry>) -> Self {
        self.worker_registry = Some(registry);
        self
    }

    #[must_use = "with_rate_limiter consumes self; bind the returned service"]
    pub fn with_rate_limiter(
        mut self,
        limiter: Arc<crate::rate_limiter::QueryRateLimiter>,
    ) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    #[must_use = "with_auth_rate_limiter consumes self; bind the returned service"]
    pub fn with_auth_rate_limiter(
        mut self,
        limiter: Arc<crate::rate_limiter::AuthRateLimiter>,
    ) -> Self {
        self.auth_rate_limiter = Some(limiter);
        self
    }

    #[must_use = "with_metadata_rate_limiter consumes self; bind the returned service"]
    pub fn with_metadata_rate_limiter(
        mut self,
        limiter: Arc<crate::rate_limiter::MetadataRateLimiter>,
    ) -> Self {
        self.metadata_rate_limiter = Some(limiter);
        self
    }

    /// Emit an OCSF Authentication audit event (class_uid 3002).
    ///
    /// Called from `do_handshake` and `get_session_from_request` at every
    /// authentication outcome. Credential material (passwords, raw tokens) is
    /// NEVER included in the event; the `message` field carries only a static,
    /// non-sensitive reason string.
    fn emit_auth_event(
        &self,
        outcome: sqe_metrics::audit::Outcome,
        actor: sqe_metrics::audit::Actor,
        client_ip: Option<String>,
        session_id: Option<String>,
    ) {
        if let Some(ref audit) = self.audit {
            let event = sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::Auth,
                actor,
                outcome,
                resources: vec![],
                policy: None,
                timing: None,
                stats: None,
                query: None,
                session_id,
                client_ip,
                trace_id: sqe_metrics::propagation::current_trace_id(),
                query_id: None,
                integrity: sqe_metrics::audit::Integrity::default(),
            };
            audit.log_event(event);
        }
    }

    /// Emit a Session lifecycle event (OCSF Authorize Session 3003).
    ///
    /// Called ONLY from `do_handshake` on a successful password-credential
    /// exchange. The JWT bearer path (`get_session_from_request`) deliberately
    /// does NOT call this function: it mints a new ephemeral session UUID on
    /// every RPC, so emitting a Session event there would produce one event per
    /// query rather than one per login. Auth events record authentication
    /// attempts; Session events record the establishment of a named interactive
    /// session. A fresh handshake produces both; a per-request JWT bearer
    /// produces only an Auth event.
    ///
    /// Only emitted on success; eviction and expiry are not yet tracked
    /// (no AuditLogger path from SessionManager without a second wiring).
    fn emit_session_event(
        &self,
        actor: sqe_metrics::audit::Actor,
        client_ip: Option<String>,
        session_id: Option<String>,
    ) {
        if let Some(ref audit) = self.audit {
            let event = sqe_metrics::audit::AuditEvent {
                time: chrono::Utc::now(),
                kind: sqe_metrics::audit::AuditKind::Session,
                actor,
                outcome: sqe_metrics::audit::Outcome::Success,
                resources: vec![],
                policy: None,
                timing: None,
                stats: None,
                query: None,
                session_id,
                client_ip,
                trace_id: sqe_metrics::propagation::current_trace_id(),
                query_id: None,
                integrity: sqe_metrics::audit::Integrity::default(),
            };
            audit.log_event(event);
        }
    }

    /// Resolve the source IP for audit and rate-limit purposes.
    ///
    /// Issue #74: `x-forwarded-for` is honoured only when the request's
    /// peer address appears in `[security] trusted_proxies`. Otherwise
    /// the peer address is used directly. Empty allowlist (default)
    /// means audit IPs always reflect the actual TCP peer.
    fn extract_client_ip<T>(&self, request: &Request<T>) -> String {
        let peer = request.remote_addr().map(|a| a.to_string());
        let xff = request
            .metadata()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        self.config
            .security
            .resolve_client_ip(peer.as_deref(), xff.as_deref())
    }

    /// Extract and validate a bearer token from the request metadata,
    /// returning the associated session.
    ///
    /// Supports two token types:
    /// 1. SQE session ID (from do_handshake) — looked up in session manager
    /// 2. Raw JWT (from backend BFF pass-through) — validated via the auth
    ///    provider chain (JWKS signature check) and converted into a session
    ///
    /// Boxed for compile time, not behaviour: every `FlightSqlService` method
    /// awaits this inside its own `async_trait` (`Send`) future, and rustc
    /// re-proved `Send` over the auth chain's state machine once per method
    /// (about 1.3 s each, 20-odd methods, `-Zself-profile` 2026-09-07).
    fn get_session_from_request<'a, T: Send + Sync>(
        &'a self,
        request: &'a Request<T>,
    ) -> futures::future::BoxFuture<'a, Result<Arc<sqe_core::Session>, Status>> {
        Box::pin(self.get_session_from_request_inner(request))
    }

    async fn get_session_from_request_inner<T: Send + Sync>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<sqe_core::Session>, Status> {
        let metadata = request.metadata();
        let client_ip = self.extract_client_ip(request);

        let auth = match metadata.get("authorization") {
            Some(v) => v,
            None => {
                debug!(client_ip = %client_ip, "Request missing authorization header");
                self.emit_auth_event(
                    sqe_metrics::audit::Outcome::Failure {
                        error_type: Some("AuthFailed".to_string()),
                        error_code: Some("UNAUTHENTICATED".to_string()),
                        message: Some("No authorization header".to_string()),
                    },
                    sqe_metrics::audit::Actor::from_parts(
                        "unknown".to_string(),
                        None,
                        None,
                        vec![],
                        vec![],
                    ),
                    Some(client_ip.clone()),
                    None,
                );
                return Err(Status::unauthenticated("No authorization header"));
            }
        };
        let auth = auth
            .to_str()
            .map_err(|e| Status::internal(format!("Invalid authorization header: {e}")))?;

        let bearer_prefix = "Bearer ";
        if !auth.starts_with(bearer_prefix) {
            return Err(Status::unauthenticated(
                "Authorization header must use Bearer scheme",
            ));
        }

        let token = &auth[bearer_prefix.len()..];

        // Try session lookup first (handshake flow). Session reuse is NOT an
        // auth establishment event; the auth event was already emitted when the
        // session was created via do_handshake or JWT validation below.
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
            // Finding 405: the token fingerprint indexes the session, so a
            // client replaying the same JWT on every RPC authenticates once
            // and reuses one session. `created` is true only on that first
            // sight, which is the only time an auth-success event belongs.
            match self.session_manager.session_for_bearer(token).await {
                Ok((session, created)) => {
                    if !created {
                        debug!(
                            username = %session.user.username,
                            client_ip = %client_ip,
                            session_id = %session.id,
                            "Flight: JWT bearer matched an existing session"
                        );
                        return Ok(session);
                    }
                    debug!(
                        username = %session.user.username,
                        client_ip = %client_ip,
                        session_id = %session.id,
                        "Flight: authenticated via validated JWT bearer token"
                    );
                    let jwt_actor = sqe_metrics::audit::Actor::from_parts(
                        session.user.username.clone(),
                        session.user.subject.clone(),
                        session.user.email.clone(),
                        session.user.roles.clone(),
                        session.user.groups.clone(),
                    );
                    self.emit_auth_event(
                        sqe_metrics::audit::Outcome::Success,
                        jwt_actor,
                        Some(client_ip),
                        Some(session.id.clone()),
                    );
                    // Session events (OCSF 3003) are only emitted on
                    // do_handshake(), the session-establishment gate for
                    // interactive clients; bearer replay clients establish
                    // no handshake session.
                    return Ok(session);
                }
                Err(e) => {
                    warn!(
                        client_ip = %client_ip,
                        error = %e,
                        "Flight: JWT bearer token validation failed"
                    );
                    self.emit_auth_event(
                        sqe_metrics::audit::Outcome::Failure {
                            error_type: Some("AuthFailed".to_string()),
                            error_code: Some("INVALID_TOKEN".to_string()),
                            message: Some("Invalid or expired bearer token".to_string()),
                        },
                        sqe_metrics::audit::Actor::from_parts(
                            "unknown".to_string(),
                            None,
                            None,
                            vec![],
                            vec![],
                        ),
                        Some(client_ip),
                        None,
                    );
                    return Err(Status::unauthenticated("Invalid or expired bearer token"));
                }
            }
        }

        warn!(client_ip = %client_ip, "Invalid or expired session token");
        self.emit_auth_event(
            sqe_metrics::audit::Outcome::Failure {
                error_type: Some("AuthFailed".to_string()),
                error_code: Some("INVALID_SESSION".to_string()),
                message: Some("Invalid or expired session token".to_string()),
            },
            sqe_metrics::audit::Actor::from_parts(
                "unknown".to_string(),
                None,
                None,
                vec![],
                vec![],
            ),
            Some(client_ip),
            None,
        );
        Err(Status::unauthenticated("Invalid or expired session token"))
    }

    /// Parse the `[coordinator] flight_compression` config into IPC write options.
    fn flight_ipc_options(&self) -> Result<IpcWriteOptions, Status> {
        let compression = self.config.coordinator.flight_compression;
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

    /// Route a SQL statement into a Flight SQL response.
    ///
    /// SELECT queries flow through [`QueryHandler::execute_stream`], which
    /// hands back a DataFusion [`SendableRecordBatchStream`]. Batches pass
    /// directly into the Flight encoder -- the coordinator never holds more
    /// than a few in-flight batches in memory, so 20M+ row results no longer
    /// OOM the process. Memory-heavy operators spill to disk via the
    /// configured `FairSpillPool` + `spill_dir`.
    ///
    /// Every other statement kind (DML, DDL, SHOW, GRANT, etc.) still goes
    /// through the buffered [`QueryHandler::execute`] path and is converted
    /// into a Flight response with [`Self::batches_to_stream`]. Those
    /// statements produce small result sets (row counts, catalog metadata)
    /// where buffering is free and the existing handlers rely on owning the
    /// full `Vec<RecordBatch>`.
    ///
    /// `query_id` is the id minted at `GetFlightInfo` (finding 403) so the
    /// tracker and the client's ticket agree; `None` mints a fresh one.
    async fn run_sql_into_flight_response(
        &self,
        session: &sqe_core::Session,
        sql: &str,
        client_ip: Option<String>,
        query_id: Option<uuid::Uuid>,
    ) -> Result<Response<FlightStream>, Status> {
        // Issue #403: Flight SQL classifies (parses) before query_handler,
        // so the size cap must fire here or a huge statement still burns
        // the parser. execute/execute_stream re-check for the other paths.
        self.config
            .query
            .check_sql_bytes(sql)
            .map_err(|e| sqe_error_to_status(&e, None))?;

        // Classify through the pre-parse pipeline (strips FOR INCREMENTAL /
        // VERSION AS OF, rewrites Hive `PARTITIONED BY` -> sqlparser `PARTITION
        // BY`) and the SQL front door (`WireDialect::Flight`; finding 700) so
        // routing matches the execute path and accepts the same Trino shapes
        // (`INSERT INTO t VALUES 1`, `TABLE t`) the Trino door accepts. The
        // raw text is handed on; the engine normalizes it once when planning.
        let kind = pre_parse_pipeline(&UserSql::from(sql))
            .and_then(|classifiable| {
                SqlFrontDoor::new(WireDialect::Flight)
                    .with_session_catalog(session.default_catalog.clone())
                    .normalize(classifiable.as_str())
            })
            .and_then(classify_normalized)
            .map_err(|e| sqe_error_to_status(&e, None))?;

        if matches!(kind, StatementKind::Query(_)) {
            let (schema, stream) = self
                .query_handler
                .execute_stream_with_id(session, sql, client_ip, query_id)
                .await
                .map_err(|e| sqe_error_to_status(&e, None))?;
            return self.stream_to_flight_response(schema, stream);
        }

        let batches = self
            .query_handler
            .execute(session, sql, client_ip)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;
        self.batches_to_stream(batches)
    }

    /// Wrap an opened DataFusion record-batch stream in the Flight encoder.
    /// Batches pass straight through; the coordinator never holds more than
    /// a few in flight.
    fn stream_to_flight_response(
        &self,
        schema: arrow_schema::SchemaRef,
        stream: datafusion::execution::SendableRecordBatchStream,
    ) -> Result<Response<FlightStream>, Status> {
        let options = self.flight_ipc_options()?;
        let batch_stream =
            stream.map(|res| res.map_err(|e| FlightError::ExternalError(Box::new(e))));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(options)
            .build(batch_stream)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Run whatever a redeemed ticket resolved to.
    async fn run_resolved_into_flight_response(
        &self,
        session: &sqe_core::Session,
        resolved: ResolvedStatement,
        client_ip: Option<String>,
        query_id: Option<uuid::Uuid>,
    ) -> Result<Response<FlightStream>, Status> {
        match resolved {
            ResolvedStatement::Sql(sql) => {
                self.run_sql_into_flight_response(session, &sql, client_ip, query_id)
                    .await
            }
            ResolvedStatement::Prepared(statement) => {
                self.run_prepared_into_flight_response(session, &statement, client_ip, query_id)
                    .await
            }
        }
    }

    /// Execute a prepared statement (finding 406).
    ///
    /// Queries reuse the plan built at `CreatePreparedStatement`: the current
    /// bind is substituted with `LogicalPlan::with_param_values` and the
    /// bound plan goes through [`QueryHandler::execute_plan_stream_with_id`],
    /// which runs the policy enforcer for the executing user before physical
    /// planning exactly as the SQL path does. The bind is not consumed, so
    /// the next execution without a re-bind reuses it.
    ///
    /// Statements without a stored plan (DML, DDL, metadata) still execute
    /// through the buffered text path; their bound values are rendered as SQL
    /// literals into the original text.
    async fn run_prepared_into_flight_response(
        &self,
        session: &sqe_core::Session,
        statement: &PreparedStatement,
        client_ip: Option<String>,
        query_id: Option<uuid::Uuid>,
    ) -> Result<Response<FlightStream>, Status> {
        if statement.plan.is_some() {
            let plan = statement
                .bound_plan()
                .map_err(Status::failed_precondition)?;
            let (schema, stream) = self
                .query_handler
                .execute_plan_stream_with_id(
                    session,
                    &statement.numbered_sql,
                    plan,
                    client_ip,
                    query_id,
                )
                .await
                .map_err(|e| sqe_error_to_status(&e, None))?;
            return self.stream_to_flight_response(schema, stream);
        }
        let sql = render_bound_sql(statement)?;
        self.run_sql_into_flight_response(session, &sql, client_ip, query_id)
            .await
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
        // Padding-indifferent: ADBC/Go FlightSQL clients send unpadded base64.
        let decoded = BASIC_AUTH_B64
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

        let client_ip = self.extract_client_ip(&request);

        // Pre-auth rate limit, keyed by (peer-ip, username). Without
        // this, do_handshake forwards every attempt to the upstream
        // IdP -- unbounded credential stuffing plus a DoS vector
        // against the IdP itself. Issue #57.
        if let Some(ref limiter) = self.auth_rate_limiter {
            if limiter
                .check(strip_port_for_key(&client_ip), username)
                .is_err()
            {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .auth_attempts_total
                        .with_label_values(&["oidc", "rate_limited"])
                        .inc();
                }
                warn!(
                    username = username,
                    client_ip = %client_ip,
                    "Handshake rejected by auth rate limiter"
                );
                return Err(Status::resource_exhausted("authentication rate limit"));
            }
        }

        info!(username = username, "Handshake authentication attempt");

        let credentials = sqe_auth::FlightCredentials {
            username: Some(username.to_string()),
            password: Some(sqe_core::SecretString::new(password.to_string())),
            ..Default::default()
        };

        let auth_start = std::time::Instant::now();

        let auth_result = tokio::time::timeout(
            std::time::Duration::from_secs(self.config.coordinator.auth_handshake_timeout_secs),
            self.session_manager.authenticate_credentials(&credentials),
        )
        .await;

        let auth_elapsed = auth_start.elapsed();

        // Record auth duration regardless of outcome
        if let Some(ref metrics) = self.metrics {
            metrics
                .auth_duration_seconds
                .observe(auth_elapsed.as_secs_f64());
        }

        let session = match auth_result {
            Ok(Ok(session)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .auth_attempts_total
                        .with_label_values(&["oidc", "success"])
                        .inc();
                }
                session
            }
            Ok(Err(e)) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .auth_attempts_total
                        .with_label_values(&["oidc", "failed"])
                        .inc();
                }
                // Server-side detail stays in the warn log. The
                // unauthenticated peer receives an opaque message so
                // we don't leak audience / issuer / kid hints that
                // turn the handler into a JWT enumeration oracle.
                // Issue #38.
                warn!(username = username, client_ip = %client_ip, error = %e, "Authentication failed");
                self.emit_auth_event(
                    sqe_metrics::audit::Outcome::Failure {
                        error_type: Some("AuthFailed".to_string()),
                        error_code: Some("INVALID_CREDENTIALS".to_string()),
                        message: Some("Authentication failed".to_string()),
                    },
                    sqe_metrics::audit::Actor::from_parts(
                        username.to_string(),
                        None,
                        None,
                        vec![],
                        vec![],
                    ),
                    Some(client_ip.clone()),
                    None,
                );
                return Err(Status::unauthenticated("authentication failed"));
            }
            Err(_) => {
                if let Some(ref metrics) = self.metrics {
                    metrics
                        .auth_attempts_total
                        .with_label_values(&["oidc", "failed"])
                        .inc();
                }
                warn!(username = username, "Handshake timed out after 30s");
                self.emit_auth_event(
                    sqe_metrics::audit::Outcome::Failure {
                        error_type: Some("AuthFailed".to_string()),
                        error_code: Some("TIMEOUT".to_string()),
                        message: Some("Authentication timed out".to_string()),
                    },
                    sqe_metrics::audit::Actor::from_parts(
                        username.to_string(),
                        None,
                        None,
                        vec![],
                        vec![],
                    ),
                    Some(client_ip.clone()),
                    None,
                );
                return Err(Status::deadline_exceeded("Authentication timed out"));
            }
        };

        info!(
            username = username,
            session_id = %session.id,
            "Handshake authentication successful"
        );
        let handshake_actor = sqe_metrics::audit::Actor::from_parts(
            session.user.username.clone(),
            session.user.subject.clone(),
            session.user.email.clone(),
            session.user.roles.clone(),
            session.user.groups.clone(),
        );
        self.emit_auth_event(
            sqe_metrics::audit::Outcome::Success,
            handshake_actor.clone(),
            Some(client_ip.clone()),
            Some(session.id.clone()),
        );
        self.emit_session_event(handshake_actor, Some(client_ip), Some(session.id.clone()));

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

        // Plan the query to extract the schema without executing it
        let schema = self
            .query_handler
            .get_schema(&session, sql)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        // Finding 403: the ticket carries a query id, not the SQL. DoGet
        // resolves the SQL by id and runs under that id, so CancelQuery
        // with this FlightInfo reaches the tracked query.
        let (query_id, ticket) = self
            .issue_statement_ticket(&session, sql.clone(), None)
            .await;
        debug!(query_id = %query_id, "issued statement ticket");
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

        let handle = &ticket.statement_handle;

        debug!(
            username = %session.user.username,
            "do_get_statement with handle"
        );

        // The handle is a query id (finding 403) or, for legacy clients,
        // the raw SQL string.
        let handle_str = std::str::from_utf8(handle)
            .map_err(|e| Status::internal(format!("Invalid statement handle: {e}")))?;
        let (resolved, query_id) = self.resolve_statement_handle(&session, handle_str).await?;

        let client_ip = Some(self.extract_client_ip(&request));
        self.run_resolved_into_flight_response(&session, resolved, client_ip, query_id)
            .await
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

            let (resolved, query_id) = self
                .resolve_statement_handle(&session, &fetch.handle)
                .await?;
            let client_ip = Some(self.extract_client_ip(&request));
            return self
                .run_resolved_into_flight_response(&session, resolved, client_ip, query_id)
                .await;
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
        let session = self.get_session_from_request(&request).await?;
        if let Some(ref limiter) = self.metadata_rate_limiter {
            limiter
                .check(&session.user.username)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }
        info!("Flight SQL: do_get_catalogs called");
        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        let mut builder = query.into_builder();
        builder.append(&catalog_name);
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
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
        if let Some(ref limiter) = self.metadata_rate_limiter {
            limiter
                .check(&session.user.username)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        // Use query handler to list schemas via the session catalog
        let client_ip = Some(self.extract_client_ip(&request));
        let batches = self
            .query_handler
            .execute(&session, "SHOW SCHEMAS", client_ip)
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
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
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
        if let Some(ref limiter) = self.metadata_rate_limiter {
            limiter
                .check(&session.user.username)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        let catalog_name = if self.config.catalog.warehouse.is_empty() {
            "default".to_string()
        } else {
            self.config.catalog.warehouse.clone()
        };

        let schema_pattern = query.db_schema_filter_pattern.clone();
        let table_pattern = query.table_name_filter_pattern.clone();
        let include_schema = query.include_schema;
        let type_filter = if query.table_types.is_empty() {
            None
        } else {
            Some(
                query
                    .table_types
                    .iter()
                    .map(|s| s.to_ascii_uppercase())
                    .collect::<Vec<_>>(),
            )
        };

        // Catalog filter: drop everything when the request asks for a
        // different catalog than the one this coordinator hosts.
        if let Some(ref c) = query.catalog {
            if !c.is_empty() && !c.eq_ignore_ascii_case(&catalog_name) {
                let builder = query.into_builder();
                let batch = builder
                    .build()
                    .map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
                return self.batches_to_stream(vec![batch]);
            }
        }

        // Table-type filter: short-circuit when the requested set excludes
        // TABLE (the only kind `list_metadata_tables` returns today).
        if let Some(ref types) = type_filter {
            if !types.iter().any(|t| t == "TABLE") {
                let builder = query.into_builder();
                let batch = builder
                    .build()
                    .map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
                return self.batches_to_stream(vec![batch]);
            }
        }

        // Walk the catalog directly. The previous path ran `SHOW SCHEMAS`
        // followed by `SHOW TABLES IN "<ns>"` once per namespace through
        // the SQL planner -- N+1 planner invocations plus N Polaris round
        // trips per `DatabaseMetaData.getTables()`. It also concatenated
        // catalog-returned namespace names into SQL with only
        // backslash-quote escaping, so a federated catalog could pivot to
        // SQL execution as the browsing user. `list_metadata_tables`
        // closes both holes (issues #7, #9, #15).
        let pairs = self
            .query_handler
            .list_metadata_tables(&session)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;

        let mut builder = query.into_builder();
        let empty_schema = arrow_schema::Schema::empty();

        for (ns, table_name) in &pairs {
            if let Some(ref pat) = schema_pattern {
                if !like_match(pat, ns) {
                    continue;
                }
            }
            if let Some(ref pat) = table_pattern {
                if !like_match(pat, table_name) {
                    continue;
                }
            }

            // include_schema=true asks for the table's Arrow schema in the
            // output column. The previous handler always passed
            // `Schema::empty()`; load the real schema best-effort when the
            // client asked for it. Failure (e.g. the table dropped between
            // listing and load) falls back to empty rather than failing
            // the whole `getTables()` call.
            if include_schema {
                let qualified = format!("{ns}.{table_name}");
                match self.query_handler.get_schema(&session, &qualified).await {
                    Ok(schema) => {
                        builder
                            .append(&catalog_name, ns, table_name, "TABLE", &schema)
                            .map_err(|e| {
                                Status::internal(format!("Failed to append table: {e}"))
                            })?;
                        continue;
                    }
                    Err(_) => {
                        // Fall through to the empty-schema append below.
                    }
                }
            }

            builder
                .append(&catalog_name, ns, table_name, "TABLE", &empty_schema)
                .map_err(|e| Status::internal(format!("Failed to append table: {e}")))?;
        }

        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build batch: {e}")))?;
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

        let (statement_id, statement) = self
            .prepared_from_handle(&session, &cmd.prepared_statement_handle)
            .await?;
        // Finding 406: the schema comes from the plan built at
        // CreatePreparedStatement; nothing is re-planned here.
        let schema = Arc::clone(&statement.dataset_schema);

        // Finding 403: each execution of a prepared statement is its own
        // query, so mint the id here (not at CreatePreparedStatement, whose
        // handle is reused across executions). The pending entry keeps the
        // statement id so DoGet runs the stored plan with the current bind.
        let (query_id, ticket) = self
            .issue_statement_ticket(&session, statement.sql.clone(), Some(statement_id))
            .await;
        debug!(query_id = %query_id, "issued prepared statement ticket");
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
        let sql_info_data = build_sql_info_data()?;

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
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
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
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
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
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
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
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
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

        // A client that puts the CommandPreparedStatementQuery itself in the
        // ticket (instead of the FlightInfo ticket we issued) still runs the
        // stored plan with its current bind, under a fresh query id. The
        // lookup is scoped to this session (WEB-03).
        let (_, statement) = self
            .prepared_from_handle(&session, &query.prepared_statement_handle)
            .await?;

        debug!(
            username = %session.user.username,
            sql = %statement.sql,
            "Executing prepared statement"
        );

        let client_ip = Some(self.extract_client_ip(&request));
        self.run_prepared_into_flight_response(&session, &statement, client_ip, None)
            .await
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _session = self.get_session_from_request(&request).await?;
        // SQE backs every metadata row from `list_metadata_tables` as a
        // physical Iceberg table; VIEW is not yet implemented. Returning
        // both TABLE and VIEW caused JDBC tools to issue follow-up
        // `getTables(types=["VIEW"])` calls that always came back empty
        // and confused schema browsers.
        let mut builder = arrow_array::builder::StringBuilder::new();
        builder.append_value("TABLE");
        let arr = builder.finish();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "table_type",
            arrow_schema::DataType::Utf8,
            false,
        )]));
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
        let sql_info_data = build_sql_info_data()?;

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

        let xdbc_data = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build XDBC type info: {e}")))?;

        let batch = xdbc_data
            .record_batch(query.data_type)
            .map_err(|e| Status::internal(format!("Failed to filter XDBC type info: {e}")))?;

        self.batches_to_stream(vec![batch])
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request).await?;
        let client_ip = Some(self.extract_client_ip(&request));
        let batches = self
            .query_handler
            .execute(&session, &ticket.query, client_ip)
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

        // Decode the Arrow stream into RecordBatches. Feed the decoded stream
        // straight into the streaming write sink instead of `try_collect`-ing
        // the whole upload into a Vec: an arbitrarily large client upload no
        // longer buffers unbounded on the coordinator (write-path memory safety).
        let stream = request.into_inner();
        let flight_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        // The post-commit cache invalidation is the handler's, not this arm's
        // (`WriteHandler::after_commit`, architecture review 507). This path
        // never passes through `QueryHandler::execute`, which is where the
        // invalidation used to live for every other write, so an ADBC client
        // that bulk-loaded here and then counted rows on the same connection
        // read the pre-ingest snapshot until the cache TTL expired. Keep the
        // ingest going through a `WriteHandler` entry point for that reason.
        let rows = self
            .query_handler
            .write_handler()
            .handle_ingest_streaming(&session, &qualified, Box::pin(flight_stream))
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
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // WEB-03: this handler used to be unauthenticated, so any peer could
        // write parameter values onto a deterministic, session-unbound handle
        // that a victim's later `do_get` would execute. Authenticate first and
        // key the bind to this session.
        let session = self.get_session_from_request(&request).await?;

        // The inbound stream carries the parameter `RecordBatch` produced by
        // the driver. Finding 406: it is decoded into typed scalars and bound
        // onto the stored statement, type-checked against the planned
        // placeholders; a mismatch is rejected here, before any execution.
        let (_, statement) = self
            .prepared_from_handle(&session, &query.prepared_statement_handle)
            .await?;
        self.bind_prepared_from_stream(&statement, request.into_inner())
            .await?;
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
        let client_ip = Some(self.extract_client_ip(&request));

        // Prepared DML: bind whatever parameters the DoPut stream carries,
        // then run the statement through the buffered path with the bound
        // values rendered as literals (these statement kinds have no stored
        // plan; see `run_prepared_into_flight_response`).
        let (_, statement) = self
            .prepared_from_handle(&session, &query.prepared_statement_handle)
            .await?;
        self.bind_prepared_from_stream(&statement, request.into_inner())
            .await?;
        let sql = render_bound_sql(&statement)?;

        let batches = self
            .query_handler
            .execute(&session, &sql, client_ip)
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
        if let Some(ref limiter) = self.metadata_rate_limiter {
            limiter
                .check(&session.user.username)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }
        let sql = &query.query;

        debug!(username = %session.user.username, sql_len = sql.len(), "Creating prepared statement");

        // Finding 406: plan once. `?` placeholders become `$1..$N` so
        // DataFusion can plan the statement without values, inferring each
        // placeholder's type and the output schema. The plan is stored and
        // reused by every GetFlightInfo/DoGet for this statement; policy
        // enforcement is applied per execution, not here.
        let (numbered_sql, numbered_count) = sqe_core::number_placeholders(sql);
        let plan = self
            .query_handler
            .plan_prepared_statement(&session, &numbered_sql)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;
        let (param_types, dataset_schema): (
            Vec<Option<arrow_schema::DataType>>,
            arrow_schema::SchemaRef,
        ) = match &plan {
            Some(plan) => (
                PreparedStatement::param_types_from_plan(plan, numbered_count)
                    .map_err(Status::internal)?,
                Arc::new(plan.schema().as_arrow().clone()),
            ),
            // DML, DDL and metadata statements have no streaming plan;
            // their result schema is the one `get_schema` advertises and
            // their parameters are untyped until bound.
            None => (
                vec![None; numbered_count],
                self.query_handler
                    .get_schema(&session, sql)
                    .await
                    .map_err(|e| sqe_error_to_status(&e, None))?,
            ),
        };
        let statement = PreparedStatement::new(
            sql.clone(),
            numbered_sql,
            plan,
            param_types,
            Arc::clone(&dataset_schema),
        );
        let parameter_schema = statement.parameter_schema();
        let statement_id = self
            .prepared_statements
            .insert(&session.id, statement)
            .await;
        debug!(
            statement_id = %statement_id,
            parameters = parameter_schema.fields().len(),
            "Prepared statement stored"
        );

        // Encode both schemas as IPC for the result. FlightInfo's
        // try_with_schema produces the encoded bytes.
        let dataset_schema = FlightInfo::new()
            .try_with_schema(&dataset_schema)
            .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
            .schema;
        let parameter_schema = if parameter_schema.fields().is_empty() {
            Default::default()
        } else {
            FlightInfo::new()
                .try_with_schema(&parameter_schema)
                .map_err(|e| Status::internal(format!("Failed to encode parameter schema: {e}")))?
                .schema
        };

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: PreparedStatementStore::handle_bytes(statement_id).into(),
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        // WEB-03: authenticate so a peer cannot evict another session's
        // statement, and scope the lookup to this session. Closing an unknown
        // or already-closed handle is a no-op, as JDBC expects.
        let session = self.get_session_from_request(&request).await?;
        if let Ok(statement_id) =
            PreparedStatementStore::parse_handle(&query.prepared_statement_handle)
        {
            self.prepared_statements
                .remove(&session.id, statement_id)
                .await;
        }
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
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        let session = self.get_session_from_request(&request).await?;

        // ActionCancelQueryRequest.info contains the serialized FlightInfo
        // from get_flight_info_statement. Decode it to extract the query
        // handle from the first endpoint ticket.
        let flight_info: arrow_flight::FlightInfo = Message::decode(&*query.info).map_err(|e| {
            Status::invalid_argument(format!("CancelQuery: failed to decode FlightInfo: {e}"))
        })?;

        let query_id = flight_info
            .endpoint
            .first()
            .and_then(|ep| ep.ticket.as_ref())
            .map(|t| {
                // Tickets are `Any`-wrapped `FetchResults`. Decoding the raw
                // bytes straight into `FetchResults` "succeeds" leniently with
                // the type URL as the handle, which is why the old code never
                // found a UUID: unwrap the `Any` first.
                let from_any = Any::decode(&*t.ticket).ok().and_then(|any| {
                    (any.type_url == FetchResults::type_url())
                        .then(|| <FetchResults as Message>::decode(&*any.value).ok())
                        .flatten()
                });
                match from_any {
                    Some(fetch) => fetch.handle,
                    None => String::from_utf8_lossy(&t.ticket).to_string(),
                }
            })
            .ok_or_else(|| {
                Status::invalid_argument(
                    "CancelQuery request missing ticket in FlightInfo endpoint",
                )
            })?;

        // Tickets issued by this server carry the query id (finding 403);
        // only a legacy raw-SQL ticket from an old client fails to parse,
        // and those never had a tracked id to cancel.
        let cancelled = if let Ok(uuid) = uuid::Uuid::parse_str(&query_id) {
            let is_admin = self.config.auth.has_admin_role(&session.user.roles);
            // Issued but not yet fetched: drop the pending entry so a late
            // DoGet fails with NotFound instead of starting the query.
            let pending_dropped = match self.pending_statements.get(&uuid).await {
                Some(p) if p.username == session.user.username || is_admin => {
                    self.pending_statements.invalidate(&uuid).await;
                    true
                }
                Some(_) => {
                    return Err(Status::permission_denied(
                        "Cancel denied: caller does not own this query",
                    ));
                }
                None => false,
            };
            match self.query_tracker.owner_of(&uuid) {
                Some(owner) if owner == session.user.username || is_admin => {
                    // `cancel` is false once the query already reached a
                    // terminal state; a dropped pending entry still counts.
                    self.query_tracker.cancel(&uuid) || pending_dropped
                }
                Some(owner) => {
                    warn!(
                        query_id = %query_id,
                        caller = %session.user.username,
                        owner = %owner,
                        "CancelQuery denied: caller does not own query"
                    );
                    return Err(Status::permission_denied(
                        "Cancel denied: caller does not own this query",
                    ));
                }
                None => {
                    if !pending_dropped {
                        debug!(
                            query_id = %query_id,
                            "CancelQuery: query not found in tracker (already completed or unknown)"
                        );
                    }
                    pending_dropped
                }
            }
        } else {
            debug!(
                query_id = %query_id,
                "CancelQuery: handle is not a UUID, cannot map to tracked query"
            );
            false
        };

        if cancelled {
            info!(
                query_id = %query_id,
                user = %session.user.username,
                "Query cancelled via Flight CancelQuery action"
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
        let peer = request.remote_addr();
        let (metadata, _, action) = request.into_parts();
        match action.r#type.as_str() {
            "heartbeat" => {
                // Validate the worker secret when one is configured.
                // An empty secret reaches this code path only when the
                // operator explicitly set
                // `coordinator.allow_unauthenticated_workers = true`;
                // SqeConfig::validate refuses to boot otherwise.
                if !self.worker_secret.is_empty() {
                    use subtle::ConstantTimeEq;
                    let provided = metadata
                        .get("x-sqe-worker-secret")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    let provided_bytes = provided.as_bytes();
                    let secret_bytes = self.worker_secret.expose_bytes();
                    if provided_bytes.len() != secret_bytes.len()
                        || !bool::from(provided_bytes.ct_eq(secret_bytes))
                    {
                        return Err(Status::unauthenticated("Invalid worker secret"));
                    }
                }

                // Finding 609: the body is a JSON `WorkerHello` (url,
                // instance_id, build, ScanTask/proto/DataFusion versions,
                // capacity, failures_since_last). Older workers send the 408
                // `HeartbeatPayload` or a bare URL; `parse` accepts all three
                // and reports the older two as legacy (`v == 0`).
                let hello =
                    sqe_planner::WorkerHello::parse(&action.body).map_err(Status::invalid_argument)?;
                let worker_url = hello.url.as_str();

                if worker_url.is_empty() {
                    return Err(Status::invalid_argument(
                        "Heartbeat body must contain the worker URL",
                    ));
                }

                // Finding 408: refuse a protocol / DataFusion skew here, with a
                // clear Status, instead of failing at ScanTask predicate decode
                // mid-query. A versionless heartbeat is refused only under
                // production_mode; otherwise it is accepted with a warning.
                if let Err(err) = crate::worker_registry::check_worker_protocol(
                    &hello.heartbeat_payload(),
                    self.config.coordinator.production_mode,
                ) {
                    return Err(Status::failed_precondition(format!(
                        "worker heartbeat rejected: {err}"
                    )));
                }

                // Finding 601: a secret holder must not be able to register an
                // attacker URL and receive other users' tickets. The advertised
                // host has to resolve to the peer that sent the heartbeat.
                if !self.config.coordinator.allow_unverified_advertise {
                    if let Err(reason) =
                        crate::worker_registry::advertise_matches_peer(worker_url, peer).await
                    {
                        warn!(
                            worker = %worker_url,
                            peer = ?peer,
                            reason = %reason,
                            "Rejected heartbeat: advertised URL does not resolve to the sending peer"
                        );
                        self.emit_auth_event(
                            sqe_metrics::audit::Outcome::Failure {
                                error_type: Some("WorkerAdvertiseMismatch".to_string()),
                                error_code: Some("PERMISSION_DENIED".to_string()),
                                message: Some(reason.clone()),
                            },
                            sqe_metrics::audit::Actor::from_parts(
                                format!("worker:{worker_url}"),
                                None,
                                None,
                                vec![],
                                vec![],
                            ),
                            peer.map(|p| p.ip().to_string()),
                            None,
                        );
                        return Err(Status::permission_denied(format!(
                            "heartbeat advertise URL does not match the sending peer: {reason}"
                        )));
                    }
                }

                let ticket_public_key = self.query_handler.ticket_signer().public_key_b64();
                let cluster_id = coordinator_cluster_id(ticket_public_key);

                let reply = if let Some(ref registry) = self.worker_registry {
                    debug!(
                        worker = %worker_url,
                        instance_id = %hello.instance_id,
                        legacy = hello.is_legacy(),
                        "Received heartbeat from worker"
                    );
                    // Finding 609: the registry checks version compatibility,
                    // detects a restarted process (new instance id) and applies
                    // the re-admission policy; it reports what it saw.
                    let outcome = match registry.register_hello(&hello).await {
                        Ok(outcome) => outcome,
                        Err(err) => {
                            // A mismatched worker heartbeats every interval;
                            // say it once per (worker, reason).
                            if let crate::worker_registry::RegistrationError::HelloMismatch {
                                reason,
                                ..
                            } = &err
                            {
                                if registry.note_hello_rejected(worker_url, reason) {
                                    warn!(
                                        worker = %worker_url,
                                        build = %hello.build,
                                        reason = %reason,
                                        "Rejected worker hello: version mismatch \
                                         (logged once per worker until it changes)"
                                    );
                                } else {
                                    debug!(worker = %worker_url, reason = %reason, "Rejected worker hello again");
                                }
                            }
                            return Err(registration_error_status(&err));
                        }
                    };
                    if outcome.newly_discovered {
                        // Finding 601: registry changes are security-relevant; audit them.
                        self.emit_auth_event(
                            sqe_metrics::audit::Outcome::Success,
                            sqe_metrics::audit::Actor::from_parts(
                                format!("worker:{worker_url}"),
                                None,
                                None,
                                vec![],
                                vec![],
                            ),
                            peer.map(|p| p.ip().to_string()),
                            None,
                        );
                    }
                    if outcome.instance_changed {
                        // Finding 609: same URL, new process. Everything the
                        // coordinator remembered per fragment about the old
                        // process is gone; drop it rather than push credential
                        // refreshes at fragment ids the new process never saw.
                        let dropped = purge_restarted_worker_state(
                            self.query_handler.credential_tracker(),
                            worker_url,
                        )
                        .await;
                        info!(
                            worker = %worker_url,
                            previous_instance = ?outcome.previous_instance,
                            instance_id = %hello.instance_id,
                            build = %hello.build,
                            dropped_fragment_entries = dropped,
                            "Worker restarted; fragment-scoped state dropped"
                        );
                        self.emit_auth_event(
                            sqe_metrics::audit::Outcome::Success,
                            sqe_metrics::audit::Actor::from_parts(
                                format!("worker:{worker_url}"),
                                None,
                                None,
                                vec![],
                                vec![],
                            ),
                            peer.map(|p| p.ip().to_string()),
                            None,
                        );
                    }
                    // Finding 609: the reported load vector, for the scheduler
                    // (via the registry) and for operators (gauges).
                    if let Some(ref metrics) = self.metrics {
                        let cap = hello.capacity;
                        metrics
                            .worker_reported_memory_limit_bytes
                            .with_label_values(&[worker_url])
                            .set(cap.memory_limit_bytes.min(i64::MAX as u64) as i64);
                        metrics
                            .worker_reported_scan_budget_free_bytes
                            .with_label_values(&[worker_url])
                            .set(cap.scan_budget_free_bytes.min(i64::MAX as u64) as i64);
                        metrics
                            .worker_reported_spill_free_bytes
                            .with_label_values(&[worker_url])
                            .set(cap.spill_free_bytes.min(i64::MAX as u64) as i64);
                        metrics
                            .worker_reported_inflight_scans
                            .with_label_values(&[worker_url])
                            .set(i64::from(cap.inflight_scans));
                    }
                    sqe_planner::CoordinatorHello::accepted(cluster_id, COORDINATOR_BUILD)
                } else {
                    // No registry: still refuse a mismatched build loudly, and
                    // tell a compatible one it has NOT joined a cluster instead
                    // of the old bare `ok` (finding 609).
                    if let sqe_planner::HelloCompat::Mismatch(reason) =
                        hello.check_compat(&sqe_planner::CoordinatorVersions::current())
                    {
                        warn!(worker = %worker_url, reason = %reason, "Rejected worker hello: version mismatch");
                        return Err(Status::failed_precondition(format!(
                            "worker hello rejected: {reason}"
                        )));
                    }
                    warn!(
                        worker = %worker_url,
                        "Received a worker heartbeat but the worker registry is not \
                         wired -- the coordinator is running single-node and is \
                         IGNORING this worker. Set coordinator.worker_secret (or \
                         coordinator.worker_urls) to enable dynamic discovery."
                    );
                    sqe_planner::CoordinatorHello::rejected(
                        cluster_id,
                        COORDINATOR_BUILD,
                        "coordinator has no worker registry (single-node mode); set \
                         coordinator.worker_secret or coordinator.worker_urls",
                    )
                };

                // Finding 601: hand the worker the public half of the ticket-signing
                // key over this secret-authenticated channel so it can verify
                // tickets without ever holding a signing key. The reply keeps
                // `status` and `ticket_public_key` as top-level keys so a
                // pre-609 worker's pin logic still finds them.
                let body = reply.with_ticket_public_key(ticket_public_key).encode();
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(body),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))
                    as <Self as FlightService>::DoActionStream))
            }
            other => Err(Status::invalid_argument(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Build identifier the coordinator reports in its hello (finding 609).
const COORDINATOR_BUILD: &str = env!("CARGO_PKG_VERSION");

/// Finding 609: a stable identity for "the cluster this coordinator fronts".
///
/// Derived from the ticket-signing public key: workers already pin that key
/// (finding 601), it is stable across restarts when
/// `coordinator.ticket_signing_key_file` is set, and two coordinators with
/// different keys are, for a worker, different clusters. A worker pointed at
/// the wrong coordinator sees a different id instead of a bare `ok`.
fn coordinator_cluster_id(ticket_public_key_b64: &str) -> String {
    let prefix: String = ticket_public_key_b64.chars().take(12).collect();
    format!("sqe-{prefix}")
}

/// Finding 609: the gRPC status a refused registration produces.
fn registration_error_status(err: &crate::worker_registry::RegistrationError) -> Status {
    use crate::worker_registry::RegistrationError;
    let msg = format!("worker heartbeat rejected: {err}");
    match err {
        // A bad advertise URL is the worker's fault, not a coordinator
        // capacity problem: invalid_argument.
        RegistrationError::InvalidAdvertiseUrl { .. } => Status::invalid_argument(msg),
        RegistrationError::CapacityExceeded { .. } => Status::resource_exhausted(msg),
        // Version skew of either generation: the worker must not receive
        // tickets it cannot honour.
        RegistrationError::ProtocolMismatch { .. } | RegistrationError::HelloMismatch { .. } => {
            Status::failed_precondition(msg)
        }
    }
}

/// Finding 609: drop the fragment-scoped bookkeeping the coordinator holds for
/// a worker URL whose process restarted. Today that is the credential-refresh
/// tracker; returns how many entries were dropped.
async fn purge_restarted_worker_state(
    credential_tracker: Option<&Arc<crate::credential_refresh::CredentialRefreshTracker>>,
    worker_url: &str,
) -> usize {
    match credential_tracker {
        Some(tracker) => tracker.unregister_worker(worker_url).await,
        None => 0,
    }
}

// ---------------------------------------------------------------------------
/// Decode a Flight inbound parameter stream into one typed scalar per
/// placeholder.
///
/// The Flight SQL spec ships parameter values as a single-row `RecordBatch`
/// where each column corresponds to one placeholder, in order. Only the
/// first non-empty row is read; one bind row is the JDBC contract.
async fn decode_parameter_stream<S>(
    stream: S,
) -> Result<Vec<datafusion::scalar::ScalarValue>, String>
where
    S: futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + Unpin + 'static,
{
    use arrow_flight::decode::FlightRecordBatchStream;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;

    let mapped = stream.map(|item| item.map_err(arrow_flight::error::FlightError::from));
    let mut batches = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut values: Vec<ScalarValue> = Vec::new();
    while let Some(item) = batches.next().await {
        let batch = item.map_err(|e| format!("Failed to read parameter batch: {e}"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        for (col_idx, column) in batch.columns().iter().enumerate() {
            values.push(
                ScalarValue::try_from_array(column.as_ref(), 0).map_err(|e| {
                    format!("parameter ${}: unsupported Arrow value: {e}", col_idx + 1)
                })?,
            );
        }
        break;
    }
    Ok(values)
}

/// Render a prepared statement's bound values into its original text for the
/// buffered (non-query) path, which has no stored plan to bind into.
fn render_bound_sql(statement: &PreparedStatement) -> Result<String, Status> {
    if statement.param_count() == 0 {
        return Ok(statement.sql.clone());
    }
    let values = statement.bound_values().ok_or_else(|| {
        Status::failed_precondition(format!(
            "prepared statement has {} parameters but no values are bound",
            statement.param_count()
        ))
    })?;
    let literals = values
        .iter()
        .map(|value| {
            value
                .to_array()
                .map(|array| arrow_value_to_sql_literal(array.as_ref(), 0))
                .map_err(|e| Status::internal(format!("Failed to render bound parameter: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sqe_core::substitute_placeholders(&statement.sql, &literals).map_err(Status::invalid_argument)
}

/// Render one Arrow scalar as a SQL literal usable in textual substitution.
///
/// Strings are single-quoted with embedded quotes escaped. Binary is
/// rendered as `X'<hex>'`. Numerics and booleans are formatted literally.
/// Unsupported / structured types fall back to a quoted display string;
/// the caller is expected to reject those before relying on the result.
fn arrow_value_to_sql_literal(array: &dyn arrow_array::Array, row: usize) -> String {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(row) {
        return "NULL".to_string();
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if a.value(row) { "TRUE" } else { "FALSE" }.to_string()
        }
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            sql_quote_string(a.value(row))
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            sql_quote_string(a.value(row))
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            format!("X'{}'", hex::encode(a.value(row)))
        }
        DataType::LargeBinary => {
            let a = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            format!("X'{}'", hex::encode(a.value(row)))
        }
        _ => {
            // Fall back to display rendering wrapped in single quotes so the
            // result is still syntactically valid SQL. Callers that need
            // exact type fidelity should round-trip through DataFusion's
            // ParamValues path (deferred follow-up).
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            sql_quote_string(&s)
        }
    }
}

fn sql_quote_string(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

// `?` placeholder substitution lives in `sqe_core::substitute_placeholders`,
// shared with the Trino HTTP `EXECUTE ... USING` path.

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

    #[tokio::test]
    async fn flight_trace_layer_uses_remote_w3c_parent() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tower::{service_fn, ServiceExt};
        use tracing_subscriber::layer::SubscriberExt;

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("flight-propagation-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _subscriber = tracing::subscriber::set_default(subscriber);

        let service = FlightTraceLayer.layer(service_fn(
            |_request: tonic::codegen::http::Request<()>| async move {
                Ok::<_, std::convert::Infallible>(tonic::codegen::http::Response::new(
                    sqe_metrics::propagation::current_trace_id(),
                ))
            },
        ));
        let mut request = tonic::codegen::http::Request::new(());
        request.headers_mut().insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .unwrap(),
        );

        let response = service.oneshot(request).await.unwrap();
        assert_eq!(
            response.into_body().as_deref(),
            Some("0af7651916cd43dd8448eb211c80319c")
        );
        provider.shutdown().unwrap();
    }

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
    fn table_types_batch_contains_only_table() {
        // Replicate the exact logic from do_get_table_types so we can test it
        // without gRPC overhead. SQE does not yet implement VIEW, so the
        // table_type result set is `TABLE` only (issue #98).
        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        let arr = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("batch should be valid");

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "TABLE");
    }

    #[test]
    fn table_types_batch_schema_has_expected_field() {
        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        let arr = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)])
            .expect("batch should be valid");

        let batch_schema = batch.schema();
        let field = batch_schema.field(0);
        assert_eq!(field.name(), "table_type");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(!field.is_nullable());
    }

    #[test]
    fn like_match_basic() {
        assert!(super::like_match("orders", "orders"));
        assert!(!super::like_match("orders", "other"));
    }

    #[test]
    fn like_match_percent() {
        assert!(super::like_match("dimension_%", "dimension_users"));
        assert!(super::like_match("%users%", "fact_users_2024"));
        assert!(super::like_match("%", "anything"));
        assert!(!super::like_match("dimension_%", "fact_users"));
    }

    #[test]
    fn like_match_underscore() {
        assert!(super::like_match("a_c", "abc"));
        assert!(!super::like_match("a_c", "abbc"));
        assert!(!super::like_match("a_c", "ac"));
    }

    #[test]
    fn sql_quote_string_escapes_quotes() {
        assert_eq!(super::sql_quote_string("o'malley"), "'o''malley'");
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
        let batch = info_builder
            .build()
            .expect("info_builder.build() should succeed");

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
            ("grpc://worker-1:50051".to_string(), test_ticket("part-0")),
            ("grpc://worker-2:50051".to_string(), test_ticket("part-1")),
            ("grpc://worker-3:50051".to_string(), test_ticket("part-2")),
        ];
        let fallback = test_ticket("fallback");

        let info =
            SqeFlightSqlService::build_flight_info_distributed(&schema, &endpoints, fallback)
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

        let info =
            SqeFlightSqlService::build_flight_info_distributed(&schema, &empty, fallback.clone())
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
        let endpoints = vec![("grpc://worker-1:50051".to_string(), test_ticket("part-0"))];
        let fallback = test_ticket("fallback");

        let info =
            SqeFlightSqlService::build_flight_info_distributed(&schema, &endpoints, fallback)
                .expect("single-worker distributed should succeed");

        assert_eq!(info.endpoint.len(), 1);
        let ep = &info.endpoint[0];
        assert_eq!(ep.location[0].uri, "grpc://worker-1:50051");
        assert_eq!(ep.ticket.as_ref().unwrap().ticket.as_ref(), b"part-0");
    }

    #[test]
    fn build_flight_info_distributed_carries_schema_bytes() {
        let schema = test_schema();
        let endpoints = vec![("grpc://worker-1:50051".to_string(), test_ticket("part-0"))];
        let fallback = test_ticket("fallback");

        let info =
            SqeFlightSqlService::build_flight_info_distributed(&schema, &endpoints, fallback)
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

    /// The handshake Basic-auth decoder must accept base64 WITH and WITHOUT
    /// padding. The Go ADBC FlightSQL driver (dbt-sqe's transport) sends
    /// unpadded base64; a padding-strict decoder rejected it with "Invalid
    /// padding", breaking the handshake. Regression guard.
    #[test]
    fn basic_auth_b64_accepts_padded_and_unpadded() {
        let creds = b"root:s3cr3t";
        let padded = base64::engine::general_purpose::STANDARD.encode(creds);
        assert!(padded.ends_with('='), "fixture must actually be padded");
        let unpadded = padded.trim_end_matches('=');
        assert_ne!(padded, unpadded, "fixture must differ");

        assert_eq!(
            BASIC_AUTH_B64.decode(&padded).expect("padded must decode"),
            creds,
            "padded base64 (Rust clients) must decode"
        );
        assert_eq!(
            BASIC_AUTH_B64
                .decode(unpadded)
                .expect("unpadded must decode"),
            creds,
            "unpadded base64 (ADBC/Go clients) must decode -- the bug"
        );
    }

    // -----------------------------------------------------------------------
    // Finding 403: statement tickets carry the query id; CancelQuery works
    // -----------------------------------------------------------------------

    /// Catalog-free config: no REST catalog is configured at all, so the
    /// session context builds without any network and `SELECT 1` /
    /// `generate_series` plan locally.
    const CANCEL_TEST_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = ""
"#;

    /// An in-process service plus an authenticated session token. Uses the
    /// anonymous provider so no identity backend is needed; `get_session_from_request`
    /// resolves the session id as a bearer token.
    async fn cancel_test_service() -> (SqeFlightSqlService, String, Arc<QueryTracker>) {
        let config: SqeConfig = toml::from_str(CANCEL_TEST_TOML).expect("config parses");
        let tracker = Arc::new(QueryTracker::new(&config.query_history));
        let handler = QueryHandler::new(
            Arc::new(sqe_policy::PassthroughEnforcer),
            None,
            config.clone(),
            None,
            None,
            None,
            None,
            Arc::clone(&tracker),
            None,
            None,
            None,
            crate::RuntimeCatalogRegistry::new(),
            sqe_core::SecretStore::new(),
        )
        .expect("QueryHandler::new");
        let provider = Arc::new(sqe_auth::AnonymousProvider::new(
            sqe_auth::AnonymousProviderConfig::default(),
        ));
        let session_manager = Arc::new(SessionManager::with_provider(provider));
        let session = session_manager
            .authenticate("alice", "pw")
            .await
            .expect("anonymous provider accepts any credentials");
        let service = SqeFlightSqlService::new(session_manager, Arc::new(handler), config);
        (service, session.id.clone(), tracker)
    }

    /// Unwrap the `Any`-wrapped `FetchResults` a statement ticket carries.
    fn decode_ticket(ticket: &Ticket) -> FetchResults {
        let any = Any::decode(&*ticket.ticket).expect("ticket is an Any");
        assert_eq!(any.type_url, FetchResults::type_url());
        <FetchResults as Message>::decode(&*any.value).expect("FetchResults")
    }

    fn authed<T>(token: &str, body: T) -> Request<T> {
        let mut req = Request::new(body);
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("valid header"),
        );
        req
    }

    #[tokio::test]
    async fn statement_ticket_is_a_uuid_and_cancel_query_returns_cancelled() {
        let (service, token, tracker) = cancel_test_service().await;

        let info = service
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1".to_string(),
                    transaction_id: None,
                },
                authed(&token, FlightDescriptor::new_cmd(vec![])),
            )
            .await
            .expect("get_flight_info_statement")
            .into_inner();

        // The ticket carries a UUID, not the SQL.
        let ticket = info.endpoint[0].ticket.as_ref().expect("ticket").clone();
        let fetch = decode_ticket(&ticket);
        let query_id = uuid::Uuid::parse_str(&fetch.handle)
            .unwrap_or_else(|_| panic!("ticket handle must be a UUID, got {:?}", fetch.handle));
        assert!(
            service.pending_statements.get(&query_id).await.is_some(),
            "issued ticket must be redeemable"
        );

        // CancelQuery with the FlightInfo the client holds.
        let result = service
            .do_action_cancel_query(
                ActionCancelQueryRequest {
                    info: info.encode_to_vec().into(),
                },
                authed(&token, Action::new("CancelQuery", vec![])),
            )
            .await
            .expect("do_action_cancel_query");
        assert_eq!(result.result, 1, "CancelResult must be CANCELLED (1)");

        // The pending entry is gone, so a late DoGet cannot start the query.
        assert!(service.pending_statements.get(&query_id).await.is_none());
        let err = service
            .do_get_fallback(authed(&token, ticket.clone()), fetch.as_any())
            .await
            .err()
            .expect("DoGet after cancel must fail");
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(tracker.active_count(), 0);
    }

    /// The running-query half: DoGet runs the statement under the ticket's
    /// id, so a cancel while it streams is found in the tracker and records
    /// `Canceled`.
    #[tokio::test(flavor = "multi_thread")]
    async fn do_get_runs_under_ticket_id_and_cancel_reaches_tracker() {
        let (service, token, tracker) = cancel_test_service().await;

        // Effectively unbounded work so the stream is still open when the
        // cancel lands.
        let info = service
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT count(*) FROM generate_series(1, 100000000000)".to_string(),
                    transaction_id: None,
                },
                authed(&token, FlightDescriptor::new_cmd(vec![])),
            )
            .await
            .expect("get_flight_info_statement")
            .into_inner();
        let ticket = info.endpoint[0].ticket.as_ref().expect("ticket").clone();
        let fetch = decode_ticket(&ticket);
        let query_id = uuid::Uuid::parse_str(&fetch.handle).expect("uuid ticket");

        let mut stream = service
            .do_get_fallback(authed(&token, ticket.clone()), fetch.as_any())
            .await
            .expect("do_get starts the query")
            .into_inner();
        // Pull the schema message so the stream is live, then leave it parked.
        let first = stream.next().await;
        assert!(first.is_some(), "schema frame expected");
        assert!(
            tracker.owner_of(&query_id).is_some(),
            "tracker must know the ticket's id while the stream runs"
        );

        let result = service
            .do_action_cancel_query(
                ActionCancelQueryRequest {
                    info: info.encode_to_vec().into(),
                },
                authed(&token, Action::new("CancelQuery", vec![])),
            )
            .await
            .expect("do_action_cancel_query");
        assert_eq!(result.result, 1, "CancelResult must be CANCELLED (1)");
        assert_eq!(tracker.active_count(), 0);
        assert_eq!(
            tracker.get(&query_id).expect("record").state,
            crate::query_tracker::QueryState::Canceled
        );

        // The parked stream is woken and ends promptly.
        let rest = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while stream.next().await.is_some() {}
        })
        .await;
        assert!(rest.is_ok(), "cancel must end the open DoGet stream");
    }

    // ---------------------------------------------------------------
    // Prepared statements (finding 406)
    // ---------------------------------------------------------------

    /// Drain a DoGet response into record batches.
    async fn collect_batches<S>(stream: S) -> Vec<RecordBatch>
    where
        S: Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + Unpin + 'static,
    {
        arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(FlightError::from),
        )
        .try_collect()
        .await
        .expect("decodes result batches")
    }

    /// The single Int64 value of a one-row, one-column result.
    fn single_i64(batches: &[RecordBatch]) -> i64 {
        let rows: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
        rows[0]
    }

    /// Encode a one-row parameter batch the way a Flight SQL driver does.
    fn param_stream(
        values: Vec<(DataType, arrow_array::ArrayRef)>,
    ) -> impl Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + Unpin + 'static {
        let schema = Arc::new(Schema::new(
            values
                .iter()
                .enumerate()
                .map(|(i, (dt, _))| Field::new(format!("${}", i + 1), dt.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            values.into_iter().map(|(_, a)| a).collect(),
        )
        .expect("parameter batch");
        let data = arrow_flight::utils::batches_to_flight_data(&schema, vec![batch])
            .expect("encodes parameter batch");
        stream::iter(data.into_iter().map(Ok))
    }

    /// GetFlightInfo + DoGet for a prepared statement, returning the result.
    async fn execute_prepared(
        service: &SqeFlightSqlService,
        token: &str,
        handle: &bytes::Bytes,
    ) -> Result<Vec<RecordBatch>, Status> {
        let info = service
            .get_flight_info_prepared_statement(
                CommandPreparedStatementQuery {
                    prepared_statement_handle: handle.clone(),
                },
                authed(token, FlightDescriptor::new_cmd(vec![])),
            )
            .await?
            .into_inner();
        let ticket = info.endpoint[0].ticket.as_ref().expect("ticket").clone();
        let fetch = decode_ticket(&ticket);
        let stream = service
            .do_get_fallback(authed(token, ticket), fetch.as_any())
            .await?
            .into_inner();
        Ok(collect_batches(stream).await)
    }

    /// Acceptance for finding 406: the handle is a statement id, the
    /// parameter schema is real, a typed bind executes, and a second
    /// execution WITHOUT re-binding still returns rows (the bind is not
    /// consumed). A wrong-type bind is rejected before anything runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_statement_binds_once_and_executes_twice() {
        let (service, token, tracker) = cancel_test_service().await;

        let created = service
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "SELECT ? + 1 AS v".to_string(),
                    transaction_id: None,
                },
                authed(&token, Action::new("CreatePreparedStatement", vec![])),
            )
            .await
            .expect("create prepared statement");

        // The handle is a server-issued statement id, not the SQL.
        let statement_id = PreparedStatementStore::parse_handle(&created.prepared_statement_handle)
            .expect("handle is a statement id");
        let stored = service
            .prepared_statements
            .get(&token, statement_id)
            .await
            .expect("statement stored under the session");
        assert!(
            stored.plan.is_some(),
            "query plan is stored at prepare time"
        );
        assert_eq!(stored.param_types, vec![Some(DataType::Int64)]);

        // Non-empty parameter schema carrying the inferred type.
        assert!(!created.parameter_schema.is_empty());
        let param_schema: Schema = arrow_flight::IpcMessage(created.parameter_schema.clone())
            .try_into()
            .expect("parameter schema decodes");
        assert_eq!(param_schema.fields().len(), 1);
        assert_eq!(param_schema.field(0).data_type(), &DataType::Int64);

        // Unbound: executing must fail cleanly rather than run with a bare `$1`.
        let err = execute_prepared(&service, &token, &created.prepared_statement_handle)
            .await
            .expect_err("unbound parameters must not execute");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        // Wrong Arrow type is rejected at bind time, before execution.
        let err = service
            .bind_prepared_from_stream(
                &stored,
                param_stream(vec![(
                    DataType::Utf8,
                    Arc::new(arrow_array::StringArray::from(vec!["abc"])),
                )]),
            )
            .await
            .expect_err("Utf8 must not bind to an Int64 placeholder");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("expected Int64, got Utf8"), "{err}");
        assert!(!stored.is_bound());

        // Correct bind (Int32, as JDBC setInt would send for a BIGINT param).
        service
            .bind_prepared_from_stream(
                &stored,
                param_stream(vec![(
                    DataType::Int32,
                    Arc::new(arrow_array::Int32Array::from(vec![41])),
                )]),
            )
            .await
            .expect("bind");

        let first = execute_prepared(&service, &token, &created.prepared_statement_handle)
            .await
            .expect("first execution");
        assert_eq!(single_i64(&first), 42);

        // Second executeQuery() without re-binding: same rows, bind intact.
        let second = execute_prepared(&service, &token, &created.prepared_statement_handle)
            .await
            .expect("second execution without re-bind");
        assert_eq!(single_i64(&second), 42);
        assert!(stored.is_bound());

        // Re-bind replaces the values.
        service
            .bind_prepared_from_stream(
                &stored,
                param_stream(vec![(
                    DataType::Int64,
                    Arc::new(arrow_array::Int64Array::from(vec![99])),
                )]),
            )
            .await
            .expect("re-bind");
        let third = execute_prepared(&service, &token, &created.prepared_statement_handle)
            .await
            .expect("third execution");
        assert_eq!(single_i64(&third), 100);

        // Close evicts; the handle is then unknown.
        service
            .do_action_close_prepared_statement(
                ActionClosePreparedStatementRequest {
                    prepared_statement_handle: created.prepared_statement_handle.clone(),
                },
                authed(&token, Action::new("ClosePreparedStatement", vec![])),
            )
            .await
            .expect("close");
        let err = execute_prepared(&service, &token, &created.prepared_statement_handle)
            .await
            .expect_err("closed statement must be unknown");
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(tracker.active_count(), 0);
    }

    /// Plan-once evidence: DoGet executes the plan stored at prepare time,
    /// not a re-plan of the SQL text. The stored plan deliberately disagrees
    /// with the text; the result follows the plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_execution_reuses_stored_plan_instead_of_replanning() {
        let (service, token, _tracker) = cancel_test_service().await;

        let ctx = datafusion::prelude::SessionContext::new();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT $1 + 100 AS v")
            .await
            .expect("plans");
        let types = PreparedStatement::param_types_from_plan(&plan, 1).expect("types");
        let schema = Arc::new(plan.schema().as_arrow().clone());
        let statement = PreparedStatement::new(
            "SELECT $1 + 1 AS v".to_string(),
            "SELECT $1 + 1 AS v".to_string(),
            Some(plan),
            types,
            schema,
        );
        statement
            .bind(vec![datafusion::scalar::ScalarValue::Int64(Some(1))])
            .expect("bind");
        let id = service.prepared_statements.insert(&token, statement).await;
        let handle = bytes::Bytes::from(PreparedStatementStore::handle_bytes(id));

        let batches = execute_prepared(&service, &token, &handle)
            .await
            .expect("executes");
        assert_eq!(
            single_i64(&batches),
            101,
            "result must come from the stored plan, not from re-planning the SQL"
        );

        // Another session cannot see or run this statement.
        let other = service
            .session_manager
            .authenticate("bob", "pw")
            .await
            .expect("second session");
        let err = execute_prepared(&service, &other.id, &handle)
            .await
            .expect_err("foreign session must not resolve the handle");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    /// A statement without parameters needs no bind and runs repeatedly.
    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_statement_without_parameters_runs_without_bind() {
        let (service, token, _tracker) = cancel_test_service().await;
        let created = service
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "SELECT 7 AS v -- what?".to_string(),
                    transaction_id: None,
                },
                authed(&token, Action::new("CreatePreparedStatement", vec![])),
            )
            .await
            .expect("create");
        assert!(created.parameter_schema.is_empty());
        for _ in 0..2 {
            let batches = execute_prepared(&service, &token, &created.prepared_statement_handle)
                .await
                .expect("executes");
            assert_eq!(single_i64(&batches), 7);
        }
        // A handle that is not a statement id is rejected, not executed as SQL.
        let err = execute_prepared(&service, &token, &bytes::Bytes::from_static(b"SELECT 1"))
            .await
            .expect_err("raw SQL handle must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // -----------------------------------------------------------------------
    // Finding 609: heartbeat hello handling
    // -----------------------------------------------------------------------

    /// 609 acceptance: a worker whose hello reports a `ScanTask` version the
    /// coordinator does not plan for is refused by the registry, the refusal
    /// maps to `FailedPrecondition`, and the worker is never registered (so it
    /// can never be handed a ticket).
    #[tokio::test]
    async fn heartbeat_hello_version_mismatch_is_failed_precondition_and_unregistered() {
        let registry = crate::worker_registry::WorkerRegistry::new(vec![]);
        let mut hello = sqe_planner::WorkerHello::current(
            "http://w1:50052",
            uuid::Uuid::new_v4(),
            "other-build",
            datafusion::DATAFUSION_VERSION,
        );
        hello.scan_task_version = sqe_planner::SCAN_TASK_VERSION_CURRENT - 1;

        let err = registry
            .register_hello(&hello)
            .await
            .expect_err("skewed hello must be refused");
        let status = super::registration_error_status(&err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("ScanTask version"), "{status}");
        assert_eq!(registry.total_workers().await, 0);
        assert!(registry.healthy_workers().await.is_empty());
        // Logged once: the first sighting is loud, the repeat is not.
        assert!(registry.note_hello_rejected("http://w1:50052", status.message()));
        assert!(!registry.note_hello_rejected("http://w1:50052", status.message()));
    }

    /// 609 acceptance: the same URL reporting a new `instance_id` clears the
    /// credential-refresh entries registered against that worker, and only
    /// those.
    #[tokio::test]
    async fn heartbeat_new_instance_id_clears_credential_refresh_state() {
        use crate::credential_refresh::CredentialRefreshTracker;

        let registry = crate::worker_registry::WorkerRegistry::new(vec![]);
        let tracker = Arc::new(CredentialRefreshTracker::new());
        tracker
            .register("frag-a".into(), "http://w1:50052".into(), None)
            .await;
        tracker
            .register("frag-b".into(), "http://w1:50052".into(), None)
            .await;
        tracker
            .register("frag-c".into(), "http://w2:50052".into(), None)
            .await;

        let first = uuid::Uuid::new_v4();
        let hello = |instance| {
            sqe_planner::WorkerHello::current(
                "http://w1:50052",
                instance,
                "build",
                datafusion::DATAFUSION_VERSION,
            )
        };
        let outcome = registry.register_hello(&hello(first)).await.unwrap();
        assert!(!outcome.instance_changed);

        let outcome = registry
            .register_hello(&hello(uuid::Uuid::new_v4()))
            .await
            .unwrap();
        assert!(outcome.instance_changed);
        assert_eq!(outcome.previous_instance, Some(first));

        let dropped = super::purge_restarted_worker_state(Some(&tracker), "http://w1:50052").await;
        assert_eq!(dropped, 2);
        assert_eq!(
            tracker.active_count().await,
            1,
            "the other worker's fragment must survive"
        );
        assert_eq!(
            super::purge_restarted_worker_state(None, "http://w1:50052").await,
            0
        );
    }

    /// The reply is a `CoordinatorHello` that a pre-609 worker's pin logic can
    /// still read: `status == "ok"` and a top-level `ticket_public_key`.
    #[test]
    fn heartbeat_reply_keeps_legacy_status_and_ticket_public_key() {
        let body = sqe_planner::CoordinatorHello::accepted(
            super::coordinator_cluster_id("AbCdEfGhIjKlMnOp"),
            super::COORDINATOR_BUILD,
        )
        .with_ticket_public_key("pk-b64")
        .encode();

        let raw: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(raw["status"], "ok", "legacy pin logic reads status");
        assert_eq!(raw["ticket_public_key"], "pk-b64");

        let hello = sqe_planner::CoordinatorHello::parse(&body).expect("new workers decode it");
        assert!(hello.accepted);
        assert_eq!(hello.cluster_id, "sqe-AbCdEfGhIjKl");
        assert_eq!(hello.build, super::COORDINATOR_BUILD);
        assert_eq!(hello.ticket_public_key.as_deref(), Some("pk-b64"));

        let rejected = sqe_planner::CoordinatorHello::rejected("sqe-x", "b", "single-node")
            .with_ticket_public_key("pk")
            .encode();
        let hello = sqe_planner::CoordinatorHello::parse(&rejected).unwrap();
        assert!(!hello.accepted);
        assert_eq!(hello.reason.as_deref(), Some("single-node"));
    }

    /// Legacy bodies keep registering: a bare URL and a 408 `HeartbeatPayload`
    /// both parse as a legacy hello with the right URL and are admitted.
    #[tokio::test]
    async fn heartbeat_legacy_bodies_still_register() {
        let registry = crate::worker_registry::WorkerRegistry::new(vec![]);

        let bare = sqe_planner::WorkerHello::parse(b"http://w1:50052").unwrap();
        assert!(bare.is_legacy());
        assert_eq!(bare.url, "http://w1:50052");
        registry.register_hello(&bare).await.unwrap();

        let payload = sqe_planner::HeartbeatPayload::current(
            "http://w2:50052",
            datafusion::DATAFUSION_VERSION,
        )
        .to_json();
        let hello = sqe_planner::WorkerHello::parse(payload.as_bytes()).unwrap();
        assert!(hello.is_legacy());
        assert_eq!(hello.url, "http://w2:50052");
        crate::worker_registry::check_worker_protocol(&hello.heartbeat_payload(), true)
            .expect("408 payload carries matching versions");
        registry.register_hello(&hello).await.unwrap();

        let mut healthy = registry.healthy_workers().await;
        healthy.sort();
        assert_eq!(healthy, vec!["http://w1:50052", "http://w2:50052"]);
    }
}

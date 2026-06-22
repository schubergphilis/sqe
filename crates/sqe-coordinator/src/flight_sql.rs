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
use arrow_flight::error::FlightError;
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
use futures::{Stream, StreamExt, TryStreamExt, stream};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use sqe_core::SqeConfig;
use sqe_sql::{StatementKind, parse_and_classify_typed, pre_parse_pipeline, UserSql};

use crate::query_handler::QueryHandler;
use crate::query_tracker::QueryTracker;
use crate::session_manager::SessionManager;
use crate::worker_registry::WorkerRegistry;

// Re-export helpers so callers that imported directly from this module
// keep working without changes.
pub use crate::flight_sql_helpers::FetchResults;
use crate::flight_sql_helpers::{FlightStream, sqe_error_to_status};

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
            "ALTER".into(), "AND".into(), "AS".into(), "ASC".into(),
            "BETWEEN".into(), "BIGINT".into(), "BY".into(),
            "CASE".into(), "CAST".into(), "COLUMN".into(), "CREATE".into(),
            "DELETE".into(), "DESC".into(), "DISTINCT".into(), "DROP".into(),
            "ELSE".into(), "END".into(), "EXISTS".into(),
            "FALSE".into(), "FOR".into(), "FROM".into(), "FULL".into(),
            "GROUP".into(), "HAVING".into(),
            "IN".into(), "INNER".into(), "INSERT".into(), "INTO".into(), "IS".into(),
            "JOIN".into(),
            "LEFT".into(), "LIKE".into(), "LIMIT".into(),
            "MERGE".into(),
            "NOT".into(), "NULL".into(),
            "ON".into(), "OR".into(), "ORDER".into(), "OUTER".into(),
            "PARTITION".into(), "PREPARE".into(),
            "RIGHT".into(),
            "SELECT".into(), "SET".into(),
            "TABLE".into(), "THEN".into(), "TRUE".into(),
            "UNION".into(), "UPDATE".into(), "USING".into(),
            "VALUES".into(), "VIEW".into(),
            "WHEN".into(), "WHERE".into(), "WITH".into(),
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
    /// Parameter bindings for outstanding prepared statements.
    ///
    /// Keyed by `(authenticated session id, encoded statement handle)` so one
    /// session can never read or overwrite another session's bound parameters
    /// (WEB-03). Each entry holds the inbound parameter `RecordBatch` decoded
    /// into a list of SQL literals; `do_get_prepared_statement` substitutes `?`
    /// placeholders in left-to-right order before executing.
    ///
    /// A bounded, TTL'd `moka` cache (WEB-04): abandoned binds expire instead
    /// of leaking forever, and the entry count is capped so a client looping
    /// binds it never fetches cannot exhaust coordinator memory.
    prepared_params: Arc<moka::future::Cache<Vec<u8>, Vec<String>>>,
}

/// Maximum outstanding prepared-statement parameter binds across all sessions.
const PREPARED_PARAMS_MAX_ENTRIES: u64 = 10_000;
/// How long an unused parameter bind survives before eviction.
const PREPARED_PARAMS_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Build the per-session prepared-statement key from the authenticated session
/// id and the client-supplied handle bytes. Binding to `session.id` (server
/// assigned at authentication) means a peer cannot target another session's
/// bind by guessing the deterministic handle (WEB-03).
fn prepared_param_key(session_id: &str, handle: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(session_id.len() + 1 + handle.len());
    key.extend_from_slice(session_id.as_bytes());
    key.push(0); // separator that cannot appear in the utf-8 session id
    key.extend_from_slice(handle);
    key
}

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
            prepared_params: Arc::new(
                moka::future::Cache::builder()
                    .max_capacity(PREPARED_PARAMS_MAX_ENTRIES)
                    .time_to_live(PREPARED_PARAMS_TTL)
                    .build(),
            ),
        }
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
    pub fn with_rate_limiter(mut self, limiter: Arc<crate::rate_limiter::QueryRateLimiter>) -> Self {
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
    async fn get_session_from_request<T: Send + Sync>(
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
                        "unknown".to_string(), None, None, vec![], vec![],
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
            let credentials = sqe_auth::FlightCredentials {
                bearer_token: Some(sqe_core::SecretString::new(token.to_string())),
                ..Default::default()
            };
            match self
                .session_manager
                .authenticate_credentials(&credentials)
                .await
            {
                Ok(session) => {
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
                    // No Session event here: authenticate_credentials mints a
                    // new session on every JWT bearer call (no dedup by token),
                    // so emitting Session here would fire per-request rather
                    // than per-session-establishment. Session events (OCSF 3003)
                    // are only emitted on do_handshake(), which is the single
                    // true session-establishment gate for interactive clients.
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
                            "unknown".to_string(), None, None, vec![], vec![],
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
                "unknown".to_string(), None, None, vec![], vec![],
            ),
            Some(client_ip),
            None,
        );
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
    async fn run_sql_into_flight_response(
        &self,
        session: &sqe_core::Session,
        sql: &str,
        client_ip: Option<String>,
    ) -> Result<Response<FlightStream>, Status> {
        // Classify through the pre-parse pipeline (strips FOR INCREMENTAL /
        // VERSION AS OF, rewrites Hive `PARTITIONED BY` -> sqlparser `PARTITION
        // BY`) so routing matches the execute path. Classifying raw SQL here
        // parse-fails on `PARTITIONED BY (month(col))` before the normalized
        // execute() path is ever reached.
        let kind = parse_and_classify_typed(
            &pre_parse_pipeline(&UserSql::from(sql)).map_err(|e| sqe_error_to_status(&e, None))?,
        )
        .map_err(|e| sqe_error_to_status(&e, None))?;

        if matches!(kind, StatementKind::Query(_)) {
            let (schema, stream) = self
                .query_handler
                .execute_stream(session, sql, client_ip)
                .await
                .map_err(|e| sqe_error_to_status(&e, None))?;

            let options = self.flight_ipc_options()?;
            let batch_stream = stream.map(|res| {
                res.map_err(|e| FlightError::ExternalError(Box::new(e)))
            });
            let flight_stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_options(options)
                .build(batch_stream)
                .map_err(Status::from);

            return Ok(Response::new(Box::pin(flight_stream)));
        }

        let batches = self
            .query_handler
            .execute(session, sql, client_ip)
            .await
            .map_err(|e| sqe_error_to_status(&e, None))?;
        self.batches_to_stream(batches)
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
                        username.to_string(), None, None, vec![], vec![],
                    ),
                    Some(client_ip.clone()),
                    None,
                );
                return Err(Status::unauthenticated("authentication failed"));
            }
            Err(_) => {
                if let Some(ref metrics) = self.metrics {
                    metrics.auth_attempts_total.with_label_values(&["oidc", "failed"]).inc();
                }
                warn!(username = username, "Handshake timed out after 30s");
                self.emit_auth_event(
                    sqe_metrics::audit::Outcome::Failure {
                        error_type: Some("AuthFailed".to_string()),
                        error_code: Some("TIMEOUT".to_string()),
                        message: Some("Authentication timed out".to_string()),
                    },
                    sqe_metrics::audit::Actor::from_parts(
                        username.to_string(), None, None, vec![], vec![],
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
        self.emit_session_event(
            handshake_actor,
            Some(client_ip),
            Some(session.id.clone()),
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

        let client_ip = Some(self.extract_client_ip(&request));
        self.run_sql_into_flight_response(&session, sql_str, client_ip).await
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

            let client_ip = Some(self.extract_client_ip(&request));
            return self.run_sql_into_flight_response(&session, &fetch.handle, client_ip).await;
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
                            .map_err(|e| Status::internal(format!("Failed to append table: {e}")))?;
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

        // Substitute any bound parameters before execution. Without this
        // the SQL still contains `?` placeholders and DataFusion rejects
        // the plan; previous behaviour silently returned wrong results.
        // The bind is looked up under this authenticated session's key, so a
        // peer cannot inject parameters into another user's statement (WEB-03).
        let key = prepared_param_key(&session.id, &query.prepared_statement_handle);
        let sql = match self.prepared_params.get(&key).await {
            Some(params) => {
                self.prepared_params.invalidate(&key).await;
                substitute_placeholders(&fetch.handle, &params)
                    .map_err(Status::invalid_argument)?
            }
            None => fetch.handle.clone(),
        };

        debug!(
            username = %session.user.username,
            sql = %sql,
            "Executing prepared statement"
        );

        let client_ip = Some(self.extract_client_ip(&request));
        self.run_sql_into_flight_response(&session, &sql, client_ip).await
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
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // WEB-03: this handler used to be unauthenticated, so any peer could
        // write parameter values onto a deterministic, session-unbound handle
        // that a victim's later `do_get` would execute. Authenticate first and
        // key the bind to this session.
        let session = self.get_session_from_request(&request).await?;

        // The inbound stream carries a parameter `RecordBatch` produced by
        // the JDBC driver. Decode it into a flat list of SQL literals and
        // park the list on the per-session handle key; `do_get_prepared_statement`
        // then substitutes `?` placeholders in left-to-right order before
        // executing.
        let key = prepared_param_key(&session.id, &query.prepared_statement_handle);
        let stream = request.into_inner();
        match decode_parameter_stream(stream).await {
            Ok(params) if !params.is_empty() => {
                self.prepared_params.insert(key, params).await;
            }
            Ok(_) => {
                // Empty parameter batch (driver issued bind with zero params).
                self.prepared_params.invalidate(&key).await;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to decode prepared statement parameter stream; ignoring bind",
                );
            }
        }
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

        // Decode the SQL from the prepared statement handle
        let fetch: FetchResults =
            Message::decode(&*query.prepared_statement_handle)
                .map_err(|e| Status::internal(format!("Failed to decode prepared statement handle: {e}")))?;

        let batches = self
            .query_handler
            .execute(&session, &fetch.handle, client_ip)
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
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        // WEB-03: authenticate so a peer cannot evict another session's bind,
        // and scope the key to this session.
        let session = self.get_session_from_request(&request).await?;
        let key = prepared_param_key(&session.id, &query.prepared_statement_handle);
        self.prepared_params.invalidate(&key).await;
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
        // tracked query yet.
        let cancelled = if let Ok(uuid) = uuid::Uuid::parse_str(&query_id) {
            let is_admin = self.config.auth.has_admin_role(&session.user.roles);
            match self.query_tracker.owner_of(&uuid) {
                Some(owner) if owner == session.user.username || is_admin => {
                    self.query_tracker.cancel(&uuid)
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
                    debug!(
                        query_id = %query_id,
                        "CancelQuery: query not found in tracker (already completed or unknown)"
                    );
                    false
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
                    if let Err(err) = registry.register_heartbeat(worker_url).await {
                        use crate::worker_registry::RegistrationError;
                        let msg = format!("worker heartbeat rejected: {err}");
                        return Err(match err {
                            // A bad advertise URL is the worker's fault, not a
                            // coordinator capacity problem: invalid_argument.
                            RegistrationError::InvalidAdvertiseUrl { .. } => {
                                Status::invalid_argument(msg)
                            }
                            RegistrationError::CapacityExceeded { .. } => {
                                Status::resource_exhausted(msg)
                            }
                        });
                    }
                } else {
                    warn!(
                        worker = %worker_url,
                        "Received a worker heartbeat but the worker registry is not \
                         wired -- the coordinator is running single-node and is \
                         IGNORING this worker. Set coordinator.worker_secret (or \
                         coordinator.worker_urls) to enable dynamic discovery."
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
/// Decode a Flight inbound parameter stream into a flat list of SQL literals.
///
/// The Flight SQL spec ships parameter values as a single-row `RecordBatch`
/// where each column corresponds to one `?` placeholder. JDBC drivers stay
/// within this row-count contract; we still walk every row in case a future
/// driver issues batched binds.
async fn decode_parameter_stream<S>(stream: S) -> Result<Vec<String>, String>
where
    S: futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + Unpin + 'static,
{
    use arrow_flight::decode::FlightRecordBatchStream;
    use futures::StreamExt;

    let mapped = stream.map(|item| item.map_err(arrow_flight::error::FlightError::from));
    let mut batches = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut literals: Vec<String> = Vec::new();
    while let Some(item) = batches.next().await {
        let batch = item.map_err(|e| format!("Failed to read parameter batch: {e}"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx).as_ref();
            literals.push(arrow_value_to_sql_literal(array, 0));
        }
        // Subsequent rows would replay against the same SQL; one bind row
        // is the JDBC contract. Stop here.
        break;
    }
    Ok(literals)
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
        DataType::Int8 => array.as_any().downcast_ref::<Int8Array>().unwrap().value(row).to_string(),
        DataType::Int16 => array.as_any().downcast_ref::<Int16Array>().unwrap().value(row).to_string(),
        DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(row).to_string(),
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(row).to_string(),
        DataType::UInt8 => array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row).to_string(),
        DataType::UInt16 => array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row).to_string(),
        DataType::UInt32 => array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row).to_string(),
        DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row).to_string(),
        DataType::Float32 => array.as_any().downcast_ref::<Float32Array>().unwrap().value(row).to_string(),
        DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_string(),
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

/// Replace `?` placeholders in the SQL string with the bound literals,
/// in order. Returns an error if the placeholder count differs from the
/// number of bound values, mirroring the JDBC behaviour of a
/// `SQLException` rather than executing with a partial bind.
fn substitute_placeholders(sql: &str, params: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len() + params.iter().map(|s| s.len()).sum::<usize>());
    let mut next: usize = 0;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = sql.as_bytes();
    let mut i: usize = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if !in_single && !in_double && c == '?' {
            if next >= params.len() {
                return Err(format!(
                    "prepared statement expected {} parameters but bind supplied {}",
                    next + 1,
                    params.len()
                ));
            }
            out.push_str(&params[next]);
            next += 1;
            i += 1;
            continue;
        }
        if !in_double && c == '\'' {
            in_single = !in_single;
        } else if !in_single && c == '"' {
            in_double = !in_double;
        }
        out.push(c);
        i += 1;
    }
    if next != params.len() {
        return Err(format!(
            "prepared statement consumed {next} parameters but bind supplied {}",
            params.len()
        ));
    }
    Ok(out)
}

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
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).expect("batch should be valid");

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
    fn substitute_placeholders_basic() {
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        let out = super::substitute_placeholders(sql, &["1".into(), "'foo'".into()]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'foo'");
    }

    #[test]
    fn substitute_placeholders_skips_inside_quotes() {
        // The `?` inside a string literal must be preserved as-is.
        let sql = "SELECT '? not bound' FROM t WHERE a = ?";
        let out = super::substitute_placeholders(sql, &["42".into()]).unwrap();
        assert_eq!(out, "SELECT '? not bound' FROM t WHERE a = 42");
    }

    #[test]
    fn substitute_placeholders_mismatch_errors() {
        assert!(super::substitute_placeholders("?", &[]).is_err());
        assert!(super::substitute_placeholders("?", &["a".into(), "b".into()]).is_err());
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
            BASIC_AUTH_B64.decode(unpadded).expect("unpadded must decode"),
            creds,
            "unpadded base64 (ADBC/Go clients) must decode -- the bug"
        );
    }
}

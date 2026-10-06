use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::net::SocketAddr;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use axum::extract::{ConnectInfo, Path, State};
use axum::Extension;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::{Stream, StreamExt};
use moka::notification::RemovalCause;
use moka::sync::Cache as MokaCache;
use parking_lot::Mutex;
use tracing::{error, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use sqe_core::config::SecurityConfig;
use sqe_core::Session;
use sqe_sql::UpdateType;

use crate::explain_compat;
use crate::info_schema_compat;
use crate::prepared;
use crate::protocol::{
    self, NodeVersion, ServerInfo, TrinoColumn, TrinoError, TrinoErrorExt, TrinoResponse,
    TrinoStats,
};
use crate::session_props;

/// Default number of rows per page for result pagination.
const DEFAULT_PAGE_SIZE: usize = 1000;

/// How long the id of a registry entry evicted for idleness is remembered
/// (key only, no payload) so a late page fetch can be told "result expired"
/// rather than "unknown query" (#621).
const EXPIRED_ID_TTL_SECS: u64 = 3600;

/// Cap on remembered expired ids; each is a 36-byte UUID string.
const EXPIRED_ID_MAX_ENTRIES: u64 = 100_000;

/// A query-registry entry idle (un-polled) for this long is evicted. This is
/// `time_to_idle`, NOT `time_to_live`: every poll's `queries.get(&id)` counts
/// as an access and resets the timer, so a query that runs longer than this
/// but is actively polled is never reaped. Only a genuinely abandoned client
/// (no poll for this long) triggers eviction, whose listener aborts the
/// still-running background task. Using `time_to_live` here would abort any
/// query still executing at the 300s mark mid-flight — the opposite of the
/// feature's purpose.
const QUERY_REGISTRY_IDLE_SECS: u64 = 300;

/// Default bounded wait applied to the POST and to a poll with no explicit
/// `maxWait`: no single HTTP call blocks longer than this on query progress.
const DEFAULT_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Hard upper bound a client-supplied `maxWait` is clamped to.
const MAX_WAIT_CAP: std::time::Duration = std::time::Duration::from_secs(10);

/// Ceiling on how long a Basic-auth session validated by the provider chain is
/// replayed without going back to the IdP. Every read also checks the access
/// token's own expiry, so this is only an upper bound, never an extension of
/// the token's life. Issue #622.
const AUTH_SESSION_CACHE_TTL_SECS: u64 = 300;

/// Number of distinct Basic credentials held in the session cache. One entry
/// per live (user, password) pair; a few thousand covers any realistic client
/// population and caps what a credential-spraying peer can pin in memory.
const AUTH_SESSION_CACHE_MAX_ENTRIES: u64 = 4096;

/// Tombstones for query ids the registry evicted for idleness. Consulted only
/// on a registry miss, so a bogus id and an expired one produce different
/// error bodies (#621).
fn build_expired_ids() -> MokaCache<String, ()> {
    MokaCache::builder()
        .max_capacity(EXPIRED_ID_MAX_ENTRIES)
        .time_to_live(std::time::Duration::from_secs(EXPIRED_ID_TTL_SECS))
        .build()
}

/// Sessions already validated for a Basic credential, so the once-per-second
/// poll of a long-running query does not replay the OIDC password grant
/// against the IdP on every hop (issue #622: ten dbt threads used to generate
/// ten grants per second, which trips Keycloak's brute-force lockout on a
/// legitimate user).
///
/// The key is `sha256(user ":" pass)` — the password itself is never stored.
pub fn build_auth_session_cache() -> MokaCache<String, Session> {
    MokaCache::builder()
        .max_capacity(AUTH_SESSION_CACHE_MAX_ENTRIES)
        .time_to_live(std::time::Duration::from_secs(AUTH_SESSION_CACHE_TTL_SECS))
        .build()
}

/// Shared context for Trino /v1/info endpoints.
pub struct NodeContext {
    pub version: String,
    pub ready: Arc<AtomicBool>,
    pub started_at: Instant,
}

pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    /// In-flight / recently-finished query lifecycle handles, keyed by
    /// `query_id`. A finished query's open [`ResultCursor`] lives on its
    /// handle, so one idle-evicted registry is the only result store.
    pub queries: MokaCache<String, Arc<QueryHandle>>,
    /// Ids evicted from `queries` for idleness; see [`build_expired_ids`].
    pub expired: MokaCache<String, ()>,
    pub node: NodeContext,
    /// Number of rows per page. Configurable for testing; defaults to [`DEFAULT_PAGE_SIZE`].
    pub page_size: usize,
    /// The TCP port the Trino-compat HTTP server is bound to.
    pub port: u16,
    /// OAuth2 external auth state. None if [auth.external] is not configured.
    pub oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
    /// Trusted-proxy allowlist used to decide whether to honour
    /// `x-forwarded-for`. Empty = ignore it. Issue #74.
    pub security: SecurityConfig,
    /// Basic-auth sessions already validated by the provider chain, keyed on
    /// a digest of the credential. Issue #622.
    pub auth_sessions: MokaCache<String, Session>,
    /// When true, /v1/info omits the build version so unauthenticated
    /// callers cannot fingerprint the engine. Issue #40.
    pub expose_version: bool,
    /// `[coordinator] public_base_url`; see [`extract_base_url`].
    pub public_base_url: Option<String>,
}

/// Who submitted a query, and the secret that lets that same client keep
/// polling it.
///
/// Both halves exist because neither alone is enough (issue #622):
/// the poll token makes the common case (a client following `nextUri`) need no
/// credentials at all, and the stable principal id makes the credential
/// fallback compare something an attacker cannot collide with.
#[derive(Debug, Clone)]
pub struct QueryOwner {
    /// Display username. Diagnostics only: a provider's `user_claim` can
    /// remap it and it is not unique across the providers in the chain, so it
    /// is never the value an authorization decision compares.
    pub username: String,
    /// Stable principal id — the IdP `sub` when the provider supplied one,
    /// else the username. Ownership is decided on this.
    pub id: String,
    /// Random token minted at submit, handed to the client inside `nextUri`
    /// and replayed verbatim on every poll. Only the submitting client ever
    /// sees it, so presenting it proves ownership without a credential.
    pub poll_token: String,
}

impl QueryOwner {
    /// Mint an owner record, with a fresh poll token, for a submitting session.
    pub fn new(session: &Session) -> Self {
        Self {
            username: session.user.username.clone(),
            id: stable_principal_id(session),
            poll_token: mint_poll_token(),
        }
    }

    /// True when `session` is the principal that submitted the query.
    fn is(&self, session: &Session) -> bool {
        self.id == stable_principal_id(session)
    }

    /// True when the caller replayed the poll token minted at submit time.
    fn owns_token(&self, presented: &str) -> bool {
        constant_time_eq(self.poll_token.as_bytes(), presented.as_bytes())
    }
}

/// Stable identity of an authenticated principal; see
/// [`sqe_core::SessionUser::principal_id`] (issue #622).
fn stable_principal_id(session: &Session) -> String {
    session.user.principal_id().to_string()
}

/// 32 random bytes, base64url without padding: 43 URL-path-safe characters,
/// the same construction the OAuth2 PKCE verifier uses.
fn mint_poll_token() -> String {
    use base64::Engine as _;
    use rand::RngExt as _;
    let bytes: Vec<u8> = (0..32).map(|_| rand::rng().random::<u8>()).collect();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Byte comparison that does not short-circuit, so the poll-token check cannot
/// be turned into a byte-at-a-time timing oracle.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Batch stream an executor hands back: the Trino path's equivalent of
/// DataFusion's `SendableRecordBatchStream`, but carrying `SqeError` so the
/// Trino error mapping keeps the original variant.
pub type TrinoBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, sqe_core::SqeError>> + Send>>;

/// A statement's result as a schema plus a lazily pulled batch stream. The
/// server converts one page at a time; the executor never materializes the
/// result (#621).
pub struct TrinoResultStream {
    pub schema: SchemaRef,
    pub stream: TrinoBatchStream,
}

impl TrinoResultStream {
    pub fn new(schema: SchemaRef, stream: TrinoBatchStream) -> Self {
        Self { schema, stream }
    }

    /// Wrap an already-materialized result. Used for the small, shape-rewritten
    /// results (DDL/DML counts, `information_schema`, `EXPLAIN`, `DESCRIBE`)
    /// and by test executors. The schema is taken from the first batch; an
    /// empty result has an empty schema, which the wire layer renders as a
    /// column-less update (data omitted), exactly as before.
    pub fn from_batches(batches: Vec<RecordBatch>) -> Self {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        Self {
            schema,
            stream: Box::pin(futures::stream::iter(batches.into_iter().map(Ok))),
        }
    }

    /// Drain the stream. Only for the reshaping paths above, whose inputs
    /// are catalog-sized.
    pub async fn collect(mut self) -> Result<Vec<RecordBatch>, sqe_core::SqeError> {
        let mut out = Vec::new();
        while let Some(batch) = self.stream.next().await {
            out.push(batch?);
        }
        Ok(out)
    }
}

impl std::fmt::Debug for TrinoResultStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoResultStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// One converted result page, retained on the cursor so the token that
/// produced it can be re-requested once (Trino clients retry the last
/// `nextUri` after a transport error).
#[derive(Debug)]
pub struct ReadyPage {
    /// Page token this page was served under.
    pub token: usize,
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Rows served up to and including this page; reported as `processedRows`.
    pub processed_rows: usize,
    /// True when the stream ended and no rows remain after this page.
    pub is_last: bool,
}

/// Outcome of asking the cursor for a page token.
#[derive(Debug)]
pub enum PageFetch {
    Page(Arc<ReadyPage>),
    /// The token is neither the page just served nor the next one.
    OutOfRange {
        /// Token the client should have asked for (one past the last served).
        expected: usize,
    },
}

/// Per-query streaming result cursor. Replaces the old whole-result JSON
/// cache: batches are pulled on demand, one page is converted per client
/// fetch (off the runtime, under `spawn_blocking`), the buffered page is
/// charged to the DataFusion memory pool via `reservation`, and only the most
/// recently served page is retained for a single retry.
pub struct ResultCursor {
    columns: Vec<TrinoColumn>,
    /// `None` once exhausted (or failed).
    stream: Option<TrinoBatchStream>,
    /// Unconsumed tail of the last batch pulled when it overran the page.
    carry: Option<RecordBatch>,
    page_size: usize,
    /// Token a fresh pull will produce.
    next_token: usize,
    /// Most recently produced page; served again if its token is re-requested.
    served: Option<Arc<ReadyPage>>,
    rows_served: usize,
    reservation: Option<MemoryReservation>,
    update_type: Option<String>,
    update_count: Option<i64>,
    created_at: Instant,
}

impl std::fmt::Debug for ResultCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCursor")
            .field("columns", &self.columns.len())
            .field("open", &self.stream.is_some())
            .field("next_token", &self.next_token)
            .field("rows_served", &self.rows_served)
            .finish_non_exhaustive()
    }
}

impl ResultCursor {
    pub fn new(
        result: TrinoResultStream,
        page_size: usize,
        reservation: Option<MemoryReservation>,
        update_type: Option<String>,
        update_count: Option<i64>,
    ) -> Self {
        Self {
            columns: protocol::columns_for_schema(result.schema.as_ref()),
            stream: Some(result.stream),
            carry: None,
            page_size: page_size.max(1),
            next_token: 0,
            served: None,
            rows_served: 0,
            reservation,
            update_type,
            update_count,
            created_at: Instant::now(),
        }
    }

    /// Cursor over an in-memory result with no pool accounting; test and
    /// reshaped-result convenience.
    pub fn from_batches(batches: Vec<RecordBatch>, page_size: usize) -> Self {
        Self::new(
            TrinoResultStream::from_batches(batches),
            page_size,
            None,
            None,
            None,
        )
    }

    pub fn columns(&self) -> &[TrinoColumn] {
        &self.columns
    }

    /// Serve `token`: the page just served (retry) is returned as-is, the next
    /// token pulls and converts a fresh page, anything else is out of range.
    /// A stream error closes the cursor and is returned to the caller, which
    /// marks the query failed.
    pub async fn fetch(&mut self, token: usize) -> Result<PageFetch, sqe_core::SqeError> {
        if let Some(page) = self.served.as_ref() {
            if page.token == token {
                return Ok(PageFetch::Page(Arc::clone(page)));
            }
            if page.is_last || token != self.next_token {
                return Ok(PageFetch::OutOfRange {
                    expected: self.next_token,
                });
            }
        } else if token != self.next_token {
            return Ok(PageFetch::OutOfRange {
                expected: self.next_token,
            });
        }
        let page = self.pull_page().await?;
        self.rows_served = page.processed_rows;
        self.next_token = token + 1;
        self.served = Some(Arc::clone(&page));
        Ok(PageFetch::Page(page))
    }

    /// Pull batches until a page is full or the stream ends, then convert
    /// that page to JSON off the runtime and charge it to the pool.
    async fn pull_page(&mut self) -> Result<Arc<ReadyPage>, sqe_core::SqeError> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut rows = 0usize;
        if let Some(carry) = self.carry.take() {
            self.take_into(&mut batches, &mut rows, carry);
        }
        // Fill the page; once full with nothing carried, pull one batch more
        // (into the carry) so a page that exactly drains the stream is
        // reported as the last one instead of forcing an empty trailing page.
        loop {
            let full = rows >= self.page_size;
            if full && self.carry.is_some() {
                break;
            }
            let Some(stream) = self.stream.as_mut() else {
                break;
            };
            match stream.next().await {
                Some(Ok(batch)) if full => {
                    if batch.num_rows() > 0 {
                        self.carry = Some(batch);
                        break;
                    }
                }
                Some(Ok(batch)) => self.take_into(&mut batches, &mut rows, batch),
                Some(Err(e)) => {
                    self.close();
                    return Err(e);
                }
                None => self.stream = None,
            }
        }
        let is_last = self.stream.is_none() && self.carry.is_none();
        // The carried slice shares its buffers with the parent batch, so the
        // whole batch stays resident until the next page consumes it.
        let carry_bytes = self
            .carry
            .as_ref()
            .map(|b| b.get_array_memory_size())
            .unwrap_or(0);

        let (json_rows, json_bytes) = tokio::task::spawn_blocking(move || {
            let rows = protocol::batches_to_rows(&batches);
            let bytes: usize = rows
                .iter()
                .flat_map(|row| row.iter().map(estimate_json_bytes))
                .fold(0usize, usize::saturating_add);
            (rows, bytes)
        })
        .await
        .map_err(|e| {
            sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::InternalError,
                format!("result page conversion task failed: {e}"),
            )
        })?;

        if let Some(reservation) = self.reservation.as_mut() {
            if let Err(e) = reservation.try_resize(json_bytes.saturating_add(carry_bytes)) {
                self.close();
                return Err(sqe_core::SqeError::from(e));
            }
        }

        let processed_rows = self.rows_served + json_rows.len();
        Ok(Arc::new(ReadyPage {
            token: self.next_token,
            rows: json_rows,
            processed_rows,
            is_last,
        }))
    }

    /// Append `batch` to the page, slicing off and carrying any overrun.
    fn take_into(&mut self, batches: &mut Vec<RecordBatch>, rows: &mut usize, batch: RecordBatch) {
        let room = self.page_size.saturating_sub(*rows);
        if batch.num_rows() <= room {
            *rows += batch.num_rows();
            batches.push(batch);
        } else {
            *rows += room;
            batches.push(batch.slice(0, room));
            self.carry = Some(batch.slice(room, batch.num_rows() - room));
        }
    }

    /// Release the stream (cancelling upstream execution) and the carry.
    /// The served page stays for a retry; the reservation shrinks with it.
    fn close(&mut self) {
        self.stream = None;
        self.carry = None;
    }
}

fn estimate_json_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(_) => 16,
        serde_json::Value::String(s) => s.len() + 16,
        serde_json::Value::Array(items) => {
            items.iter().map(estimate_json_bytes).sum::<usize>() + 16
        }
        serde_json::Value::Object(map) => {
            map.iter()
                .map(|(k, v)| k.len() + estimate_json_bytes(v) + 8)
                .sum::<usize>()
                + 16
        }
    }
}

/// Lifecycle state of a submitted statement executing on a background task.
#[derive(Debug)]
pub enum QueryStatus {
    /// Registered, background task not yet observed to start.
    Queued,
    /// Background task is running `Q::execute`.
    Running,
    /// The executor produced a result stream; its [`ResultCursor`] is on the
    /// handle and pages are pulled by the results route. A stream error
    /// during paging transitions to `Failed`.
    Finished,
    /// Execution failed; carries the mapped Trino error to replay on poll.
    /// Boxed to keep the enum small (the error struct is ~200 bytes).
    Failed(Box<protocol::TrinoError>),
    /// Cancelled via DELETE or evicted while still running.
    Cancelled,
}

impl QueryStatus {
    /// True once the query will never transition again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryStatus::Finished | QueryStatus::Failed(_) | QueryStatus::Cancelled
        )
    }
}

/// Shared handle for a statement executing in the background. Stored in the
/// query registry under `query_id`; the background task and every poll share
/// the same `Arc<QueryHandle>`.
///
/// `status` and `abort` are `parking_lot` mutexes: no poisoning, so a panic
/// while one is held cannot turn the status endpoint into a permanent 500
/// (#643), and every hold is a short synchronous critical section. `cursor`
/// is a tokio mutex because pulling a page awaits the stream while holding it.
#[derive(Debug)]
pub struct QueryHandle {
    /// Current lifecycle state; mutated by the background task and by cancel.
    pub status: Mutex<QueryStatus>,
    /// Woken on every status transition so waiting polls re-check promptly.
    pub notify: tokio::sync::Notify,
    /// Abort handle for the background task; set just after spawn.
    pub abort: Mutex<Option<tokio::task::AbortHandle>>,
    /// Principal that submitted the query plus its poll token; used for
    /// poll/cancel authorization.
    pub owner: QueryOwner,
    /// Session-state mutation (USE / SET CATALOG) to echo as response headers
    /// when the query finishes. `None` for ordinary statements.
    pub session_update: Option<protocol::UpdatedSessionState>,
    /// Registration time; used for diagnostics.
    pub created_at: std::time::Instant,
    /// Open result once the executor returned a stream (`status == Finished`).
    /// Cleared on failure, cancel, or when the handle is dropped, which also
    /// drops the upstream stream and releases the pool reservation.
    pub cursor: tokio::sync::Mutex<Option<ResultCursor>>,
}

impl QueryHandle {
    pub fn new(
        status: QueryStatus,
        owner: QueryOwner,
        session_update: Option<protocol::UpdatedSessionState>,
    ) -> Self {
        Self {
            status: Mutex::new(status),
            notify: tokio::sync::Notify::new(),
            abort: Mutex::new(None),
            owner,
            session_update,
            created_at: std::time::Instant::now(),
            cursor: tokio::sync::Mutex::new(None),
        }
    }

    /// Set a terminal status, drop any open cursor (tolerating a concurrent
    /// page fetch, which will observe the status on its next lock) and wake
    /// waiters.
    fn finish_with(&self, status: QueryStatus) {
        *self.status.lock() = status;
        if let Ok(mut cursor) = self.cursor.try_lock() {
            *cursor = None;
        }
        self.notify.notify_waiters();
    }
}

/// Build the query-state registry. Idle-evicts abandoned entries and, on
/// eviction of a still-running query, aborts its background task.
fn build_query_registry(expired: MokaCache<String, ()>) -> MokaCache<String, Arc<QueryHandle>> {
    MokaCache::builder()
        .time_to_idle(std::time::Duration::from_secs(QUERY_REGISTRY_IDLE_SECS))
        .eviction_listener(move |key: Arc<String>, handle: Arc<QueryHandle>, cause| {
            {
                let mut status = handle.status.lock();
                if !status.is_terminal() {
                    if let Some(abort) = handle.abort.lock().as_ref() {
                        abort.abort();
                    }
                    *status = QueryStatus::Cancelled;
                }
            }
            // Explicit invalidation is a cancel; only idle/size eviction is
            // "expired" from the client's point of view.
            if matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                expired.insert(key.as_str().to_string(), ());
            }
        })
        .build()
}

/// Trino client headers extracted from the request.
#[derive(Debug, Clone, Default)]
pub struct TrinoClientHeaders {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub user: Option<String>,
    pub source: Option<String>,
    /// `iceberg.compression_codec` from the `X-Trino-Session` property list, if set.
    pub compression_codec: Option<String>,
}

/// `peer_ip` on both methods is the port-stripped, proxy-resolved client
/// address. The coordinator adapter puts it in `FlightCredentials.peer_ip`,
/// which is what the chain-level pre-auth rate limiter keys on (#562); a
/// denial comes back as an `SqeError` whose code is `ResourceExhausted` and
/// the server answers 429.
#[async_trait::async_trait]
pub trait TrinoAuthenticator: Send + Sync + 'static {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
        peer_ip: &str,
    ) -> Result<Session, sqe_core::SqeError>;

    /// Validate a raw bearer token (JWT) and return an authenticated session.
    ///
    /// The default implementation rejects all bearer tokens. Override this in
    /// the coordinator adapter to route through the JWKS-validating auth chain.
    async fn authenticate_bearer(
        &self,
        _token: &str,
        _peer_ip: &str,
    ) -> Result<Session, sqe_core::SqeError> {
        Err(sqe_core::SqeError::Auth(
            "Bearer token authentication not configured".to_string(),
        ))
    }
}

#[async_trait::async_trait]
pub trait TrinoQueryExecutor: Send + Sync + 'static {
    /// Plan a SQL statement and return its result as a schema plus a batch
    /// stream. The server pulls one page at a time, so the executor must not
    /// buffer the result (#621); small non-query results can be wrapped with
    /// [`TrinoResultStream::from_batches`].
    ///
    /// Returns the full `SqeError` so the Trino mapping in
    /// `TrinoError::from_sqe_error` dispatches on the original variant. The
    /// previous `Result<_, String>` shape lost the variant, forcing the
    /// caller to substring-classify the `Display` output and produced
    /// inconsistent error codes across Flight SQL and Trino HTTP for the
    /// same fault (issue #102).
    async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> Result<TrinoResultStream, sqe_core::SqeError>;

    /// Memory pool the buffered result page is charged to, so Trino-path
    /// memory is visible to the same limit as every other consumer. `None`
    /// (the default) disables accounting; the coordinator returns its
    /// `RuntimeEnv` pool.
    fn memory_pool(&self) -> Option<Arc<dyn MemoryPool>> {
        None
    }

    /// Describe a prepared statement's output columns (`DESCRIBE OUTPUT`) or
    /// bind parameters (`DESCRIBE INPUT`), returning a synthetic result set.
    ///
    /// `prepared_sql` is the resolved statement template (with `?`
    /// placeholders) looked up from the session's prepared statements. The
    /// default returns `NotImplemented` so executors that do not support
    /// prepared-statement introspection (test mocks) are unaffected.
    async fn describe_prepared(
        &self,
        _session: &Session,
        _prepared_sql: &str,
        _kind: crate::protocol::DescribeKind,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        Err(sqe_core::SqeError::NotImplemented(
            "DESCRIBE OUTPUT / DESCRIBE INPUT is not supported by this executor".into(),
        ))
    }

    /// Record a statement this server's SQL front door rejected as
    /// unparseable. It never reaches [`Self::execute`], which is where the
    /// engine audits statements, so without this it leaves no audit line.
    /// The default does nothing (test mocks).
    fn audit_rejected(&self, _session: &Session, _sql: &str, _error: &sqe_core::SqeError) {}
}

#[derive(Default)]
pub struct TrinoServerOptions {
    pub security: SecurityConfig,
    /// Whether `/v1/info` may return the exact build version. Default
    /// `false` for production deployments. Issue #40.
    pub expose_version: bool,
    /// `[coordinator] public_base_url`: the ingress URL to put in `nextUri`
    /// / `infoUri` for requests addressed to its host.
    pub public_base_url: Option<String>,
    /// `[coordinator] trino_https_port`: a second listener serving the same
    /// routes over TLS. The plain listener keeps running alongside it.
    pub https: Option<HttpsListener>,
}

/// The TLS listener for [`TrinoServerOptions::https`]. `tls` is shared with
/// the certificate reload watcher, so a rotated certificate applies to new
/// handshakes without a restart.
pub struct HttpsListener {
    pub port: u16,
    pub tls: axum_server::tls_rustls::RustlsConfig,
}

/// Request extension set on every request accepted by the HTTPS listener.
/// An extension, not a header: a client cannot forge it on the plain port.
#[derive(Clone, Copy, Debug)]
struct HttpsListenerMarker {
    port: u16,
}

/// The connection a request arrived on: the TCP peer and, on the HTTPS
/// listener, its [`HttpsListenerMarker`].
#[derive(Clone, Copy, Debug)]
struct Conn {
    peer: SocketAddr,
    tls: Option<HttpsListenerMarker>,
}

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Conn {
    type Rejection = <ConnectInfo<SocketAddr> as axum::extract::FromRequestParts<S>>::Rejection;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ConnectInfo(peer) =
            <ConnectInfo<SocketAddr> as axum::extract::FromRequestParts<S>>::from_request_parts(
                parts, state,
            )
            .await?;
        let tls = parts.extensions.get::<HttpsListenerMarker>().copied();
        Ok(Conn { peer, tls })
    }
}

/// Assemble the statement/info routes with state. Extracted so a unit test can
/// build the router and fail fast if a route pattern conflicts (matchit panics
/// at insert time).
fn build_statement_router<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    state: Arc<TrinoState<A, Q>>,
) -> Router {
    Router::new()
        .route("/v1/info", get(server_info::<A, Q>))
        .route("/v1/info/state", get(server_state::<A, Q>))
        .route("/v1/statement", post(submit_query::<A, Q>))
        // The `{slug}` segment carries the query's poll token, the way real
        // Trino carries a slug in `/v1/statement/executing/{id}/{slug}/{token}`.
        // Clients replay `nextUri` verbatim, so it is invisible to them; what
        // it buys is a poll that proves ownership without re-presenting (and
        // re-validating) a credential. Issue #622.
        .route(
            "/v1/statement/{id}/{slug}/{token}",
            get(get_results::<A, Q>).delete(cancel_query_slug::<A, Q>),
        )
        .route(
            "/v1/statement/queued/{id}/{slug}/{token}",
            get(get_queued_results::<A, Q>).delete(cancel_query_slug::<A, Q>),
        )
        .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
        .with_state(state)
}

pub fn start_trino_server<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    port: u16,
    node: NodeContext,
    oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    start_trino_server_with_options(
        authenticator,
        query_handler,
        port,
        node,
        oauth2,
        TrinoServerOptions::default(),
    )
}

pub fn start_trino_server_with_options<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    port: u16,
    node: NodeContext,
    oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
    options: TrinoServerOptions,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    let expired = build_expired_ids();
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        queries: build_query_registry(expired.clone()),
        expired,
        node,
        page_size: DEFAULT_PAGE_SIZE,
        port,
        oauth2: oauth2.clone(),
        security: options.security,
        auth_sessions: build_auth_session_cache(),
        expose_version: options.expose_version,
        public_base_url: options.public_base_url,
    });
    let https = options.https;

    tokio::spawn(async move {
        // Restrictive CORS: no cross-origin by default. The Trino compat endpoint
        // is designed for JDBC/CLI clients, not browsers. An explicit empty
        // Access-Control-Allow-Origin blocks browser-based cross-origin requests.
        let cors_layer = axum::middleware::from_fn(
            |req: axum::extract::Request, next: axum::middleware::Next| async move {
                if req.method() == axum::http::Method::OPTIONS {
                    // Preflight: respond with 204 and restrictive headers.
                    return axum::response::Response::builder()
                    .status(204)
                    .header("Access-Control-Allow-Methods", "GET, POST, DELETE")
                    .header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Trino-User, X-Trino-Catalog, X-Trino-Schema, X-Trino-Source")
                    .header("Access-Control-Max-Age", "3600")
                    .body(axum::body::Body::empty())
                    .unwrap()
                    .into_response();
                }
                next.run(req).await
            },
        );

        let mut app = build_statement_router(state).layer(cors_layer);

        if let Some(oauth2_state) = oauth2 {
            let oauth2_routes = Router::new()
                .route(
                    "/oauth2/token/initiate/{auth_id_hash}",
                    get(crate::oauth2::initiate_handler),
                )
                .route("/oauth2/callback", get(crate::oauth2::callback_handler))
                .route(
                    "/oauth2/token/{auth_id}",
                    get(crate::oauth2::poll_token_handler),
                )
                .route(
                    "/oauth2/token/{auth_id}",
                    delete(crate::oauth2::delete_token_handler),
                )
                .with_state(oauth2_state);
            app = app.merge(oauth2_routes);
        }

        let https = https.map(|https| spawn_https_listener(app.clone(), https));
        // `trino_http_port = 0` with an HTTPS port: TLS only.
        if port == 0 {
            if let Some(handle) = https {
                let _ = handle.await;
            }
            return;
        }

        let addr = format!("0.0.0.0:{port}");
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(addr = %addr, error = %e, "Failed to bind Trino-compat HTTP server");
                return;
            }
        };

        info!("Trino-compat HTTP server listening on {addr}");

        if let Err(e) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            error!(error = %e, "Trino-compat HTTP server exited with error");
        }
    })
}

/// Serve `app` over TLS on `https.port`, tagging each request with
/// [`HttpsListenerMarker`] so `nextUri` keeps the https scheme.
fn spawn_https_listener(app: Router, https: HttpsListener) -> tokio::task::JoinHandle<()> {
    let marker = HttpsListenerMarker { port: https.port };
    let app = app.layer(Extension(marker));
    let addr = SocketAddr::from(([0, 0, 0, 0], https.port));
    tokio::spawn(async move {
        info!("Trino-compat HTTPS server listening on {addr}");
        if let Err(e) = axum_server::bind_rustls(addr, https.tls)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
        {
            error!(addr = %addr, error = %e, "Trino-compat HTTPS server exited with error");
        }
    })
}

/// Compute the trusted client IP for a Trino request: honour
/// `x-forwarded-for` only when the peer is in `[security] trusted_proxies`.
fn trino_client_ip<A, Q>(
    state: &Arc<TrinoState<A, Q>>,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> String {
    let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    state
        .security
        .resolve_client_ip(Some(&peer.to_string()), xff)
}

/// Drop the `:port` suffix from a resolved client address so the rate-limiter
/// bucket is the host, not the ephemeral source port (which would give every
/// connection a fresh budget).
///
/// A resolved address arrives in three shapes: `1.2.3.4:5678` from a peer
/// socket, `[::1]:5678` from an IPv6 peer socket, and a bare address from
/// `x-forwarded-for`. A bare IPv6 must survive intact — naively splitting on
/// the last colon would fold `2001:db8::1` and `2001:db8::2` into one bucket.
fn strip_port(client_ip: &str) -> &str {
    if let Some(rest) = client_ip.strip_prefix('[') {
        return match rest.find(']') {
            Some(end) => &client_ip[..end + 2],
            None => client_ip,
        };
    }
    // Exactly one colon followed by digits is a port; anything else (no
    // colon, or several) is the address itself.
    let mut parts = client_ip.split(':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(host), Some(port), None)
            if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) =>
        {
            host
        }
        _ => client_ip,
    }
}

/// Why credential resolution failed, so each route maps it to the status code
/// its protocol expects without repeating the auth ladder.
enum AuthFailure {
    /// No usable `Authorization` header on the request.
    Missing,
    /// A credential was presented and the provider chain rejected it.
    Rejected,
    /// The (peer, credential) tuple is over the pre-auth budget.
    RateLimited,
}

/// Rate-limit bucket for a bearer credential: a truncated SHA-256 of the
/// token. Bearers are attacker-supplied and unbounded in number, so the raw
/// value must never become a map key or a log field; the digest gives the
/// same per-credential budget passwords get without storing the secret.
///
/// Public because the coordinator's chain-level limiter (#562) keys bearers
/// the same way; one fingerprint, one definition of "the same credential".
pub fn bearer_fingerprint(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(token.as_bytes()));
    format!("bearer:{}", &digest[..16])
}

/// Session-cache key for a Basic credential. The password is hashed, never
/// stored: a cache dump must not yield a replayable credential.
///
/// The username's length is mixed in first because a plain `user ":" pass`
/// digest is ambiguous — user `a:b` with password `c` and user `a` with
/// password `b:c` hash identically, and would then share one cached session.
fn basic_session_key(user: &str, pass: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update((user.len() as u64).to_le_bytes());
    hasher.update(user.as_bytes());
    hasher.update(b":");
    hasher.update(pass.as_bytes());
    hex::encode(hasher.finalize())
}

/// Classify a provider failure: the chain-level pre-auth limiter (#562)
/// surfaces as `ResourceExhausted`, everything else is a rejection. The
/// principal logged is a username or a bearer fingerprint, never a raw
/// credential.
fn classify_auth_error(
    error: &sqe_core::SqeError,
    principal: &str,
    client_ip: &str,
) -> AuthFailure {
    if error.error_code() == sqe_core::SqeErrorCode::ResourceExhausted {
        warn!(
            principal = %principal,
            client_ip = %client_ip,
            "Trino auth rejected by rate limiter"
        );
        return AuthFailure::RateLimited;
    }
    AuthFailure::Rejected
}

/// The 429 every route returns when the pre-auth limiter denies a credential.
/// `Retry-After` is what the POST route already sent; the other routes now
/// match it instead of silently letting the attempt through (issue #622).
fn rate_limited_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, "1")],
        "authentication rate limit",
    )
        .into_response()
}

/// Resolve the caller's session from the `Authorization` header.
///
/// The order is the point of this function (issues #622, #562):
///
/// 1. A Basic credential this process already validated short-circuits on the
///    session cache. Replaying it costs the IdP nothing, so it does not spend
///    pre-auth budget — otherwise a legitimate client polling once a second
///    would exhaust a 10/min budget in its first ten seconds.
/// 2. Everything that would reach a provider, bearer included, carries the
///    peer host to the authenticator; the chain-level pre-auth limiter keys
///    on (peer ip, username or bearer fingerprint) and denies before any
///    provider runs. The denial is `ResourceExhausted` and becomes 429 here.
/// 3. Only then is the provider chain asked to validate.
async fn authenticate_request<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    state: &Arc<TrinoState<A, Q>>,
    headers: &HeaderMap,
    peer_host: &str,
    client_ip: &str,
) -> Result<Session, AuthFailure> {
    if let Some(token) = extract_bearer_token(headers) {
        return match state
            .authenticator
            .authenticate_bearer(&token, peer_host)
            .await
        {
            Ok(session) => Ok(session),
            Err(e) => {
                let failure = classify_auth_error(&e, &bearer_fingerprint(&token), client_ip);
                if matches!(failure, AuthFailure::Rejected) {
                    warn!(error = %e, client_ip = %client_ip, "Trino bearer token validation failed");
                }
                Err(failure)
            }
        };
    }

    if let Some((user, pass)) = extract_basic_auth(headers) {
        let key = basic_session_key(&user, &pass);
        if let Some(session) = state.auth_sessions.get(&key) {
            // The cache TTL is a ceiling, not a lifetime grant: a session
            // whose access token has expired is dropped and re-validated so
            // the downstream Polaris/S3 calls never carry a dead token.
            if session.token_expiry() > chrono::Utc::now() {
                return Ok(session);
            }
            state.auth_sessions.invalidate(&key);
        }
        return match state
            .authenticator
            .authenticate(&user, &pass, peer_host)
            .await
        {
            Ok(session) => {
                state.auth_sessions.insert(key, session.clone());
                Ok(session)
            }
            Err(e) => {
                let failure = classify_auth_error(&e, &user, client_ip);
                if matches!(failure, AuthFailure::Rejected) {
                    warn!(error = %e, user = %user, client_ip = %client_ip, "Trino authentication failed");
                }
                Err(failure)
            }
        };
    }

    Err(AuthFailure::Missing)
}

/// Map a credential failure on a poll/cancel route to its response: 429 for a
/// rate-limit denial, a bare 401 for everything else.
///
/// `Missing` and `Rejected` collapse on purpose — the caller learns nothing
/// about whether the query id exists or whether the credential was merely
/// absent, so neither route can be used to enumerate query ids.
fn poll_auth_response(failure: AuthFailure) -> Response {
    match failure {
        AuthFailure::RateLimited => rate_limited_response(),
        AuthFailure::Missing | AuthFailure::Rejected => StatusCode::UNAUTHORIZED.into_response(),
    }
}

// ── Trino /v1/info ────────────────────────────────────────────

fn format_uptime(started_at: Instant) -> String {
    let secs = started_at.elapsed().as_secs();
    if secs < 60 {
        format!("{secs}.00s")
    } else if secs < 3600 {
        format!("{:.2}m", secs as f64 / 60.0)
    } else if secs < 86400 {
        format!("{:.2}h", secs as f64 / 3600.0)
    } else {
        format!("{:.2}d", secs as f64 / 86400.0)
    }
}

async fn server_info<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> Json<ServerInfo> {
    let ready = state.node.ready.load(Ordering::Relaxed);
    // Issue #40: unauthenticated callers used to receive the exact
    // build version, which makes targeted-CVE matching trivial. The
    // Trino spec says /v1/info needs no auth, so the right fix is to
    // scrub the version unless the operator opts in. Clients that
    // need exact-version handshake (rare) can set `expose_version`.
    let version = if state.expose_version {
        state.node.version.clone()
    } else {
        coarse_version(&state.node.version)
    };
    Json(ServerInfo {
        node_version: NodeVersion { version },
        environment: "production".to_string(),
        coordinator: true,
        starting: !ready,
        uptime: format_uptime(state.node.started_at),
    })
}

/// Return `"<major>.<minor>"` from a semver-like input, or `"unknown"`
/// when the input cannot be parsed. The major.minor pair is enough for
/// JDBC drivers that branch on Trino protocol generation but does not
/// pin the exact build for CVE matching.
fn coarse_version(full: &str) -> String {
    let trimmed = full.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }
    let mut parts = trimmed.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        _ => "unknown".to_string(),
    }
}

async fn server_state<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> String {
    let ready = state.node.ready.load(Ordering::Relaxed);
    if ready {
        "ACTIVE".to_string()
    } else {
        "STARTING".to_string()
    }
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> Response {
    let code = match status {
        StatusCode::UNAUTHORIZED => sqe_core::SqeErrorCode::AuthenticationFailed,
        StatusCode::FORBIDDEN => sqe_core::SqeErrorCode::AccessDenied,
        StatusCode::NOT_FOUND => sqe_core::SqeErrorCode::CatalogNotFound,
        StatusCode::TOO_MANY_REQUESTS => sqe_core::SqeErrorCode::ResourceExhausted,
        s if s.is_server_error() => sqe_core::SqeErrorCode::InternalError,
        _ => sqe_core::SqeErrorCode::InvalidArguments,
    };
    let msg = msg.into();
    let body = TrinoResponse {
        id: String::new(),
        info_uri: None,
        stats: TrinoStats::failed(),
        error: Some(TrinoError::from_code(code, msg, None)),
        ..Default::default()
    };
    (status, Json(body)).into_response()
}

/// What the protocol layer knows about a statement from its AST, decided once
/// at submit time and carried to the result cursor. Replaces the leading-word
/// and substring tests on the SQL text (findings 704, 708): a user query with
/// `information_schema` in a literal or comment is not a metadata query, and
/// `CREATE VIEW` is not `CREATE TABLE`.
#[derive(Debug, Clone, Default)]
struct StatementFacts {
    /// Trino `updateType` (`INSERT`, `CREATE VIEW`, `USE`, ...); `None` for
    /// reads.
    update_type: Option<&'static str>,
    /// Reads `information_schema`, or is `SHOW COLUMNS` / `DESCRIBE <table>`:
    /// the result is reshaped as catalog metadata.
    is_metadata: bool,
    /// `SHOW COLUMNS` / `DESCRIBE <table>`: the result is reshaped into
    /// Trino's `DESCRIBE` column layout.
    is_describe_or_show_columns: bool,
    /// Any `EXPLAIN` variant: the result is collapsed to Trino's single
    /// `Query Plan` column.
    is_explain: bool,
}

impl StatementFacts {
    fn from_normalized(normalized: &sqe_sql::NormalizedSql) -> Self {
        use sqlparser::ast::{DescribeAlias, Statement};
        let stmt = normalized.statement();
        let is_describe_or_show_columns = matches!(
            stmt,
            Some(Statement::ShowColumns { .. })
                | Some(Statement::ExplainTable {
                    describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                    ..
                })
        );
        Self {
            update_type: normalized.update_type().map(UpdateType::as_trino_str),
            is_metadata: normalized.is_metadata_query(),
            is_describe_or_show_columns,
            is_explain: matches!(stmt, Some(Statement::Explain { .. })),
        }
    }

    /// True when the whole result must be drained and reshaped before the
    /// first page is served.
    fn needs_reshape(&self) -> bool {
        self.update_type.is_some() || self.is_metadata || self.is_explain
    }
}

/// How a submitted statement is answered.
#[derive(Debug)]
enum ExecPlan {
    /// Answered by the protocol layer alone (`SET SESSION`, `PREPARE`, ...):
    /// an empty finished response carrying session headers, no executor.
    Immediate,
    /// `SHOW SESSION [LIKE ...]`: the allow-listed properties with the values
    /// the client is carrying.
    ShowSession { like: Option<String> },
    /// `DESCRIBE OUTPUT|INPUT <name>` against a header-carried template.
    DescribePrepared {
        kind: protocol::DescribeKind,
        template: String,
    },
    /// Hand the normalized (and, for `EXECUTE`, bound) SQL to the executor.
    Sql(String),
}

/// A statement after the front door and the protocol-level dispatch.
#[derive(Debug)]
struct ResolvedStatement {
    plan: ExecPlan,
    facts: StatementFacts,
    /// Session-state mutation to echo on the statement's terminal response.
    session_update: Option<protocol::UpdatedSessionState>,
}

/// Run `sql` through the SQL front door and decide how to answer it.
///
/// This is the only place statement text is interpreted on the Trino path
/// (finding 700): one `SqlFrontDoor::normalize` with `WireDialect::Trino`,
/// classification from the AST it produced, and no rewrite afterwards. The
/// session-control grammar is handled here because it never reaches the
/// executor:
///
/// - `USE` goes to the executor; its `X-Trino-Set-Catalog` / `-Schema`
///   headers are attached to the terminal response only, so a failing `USE`
///   never switches the client.
/// - `SET SESSION` accepts only [`session_props::SESSION_PROPERTIES`] and
///   validates the value; anything else is `INVALID_SESSION_PROPERTY`.
///   `RESET SESSION` and `SHOW SESSION` use the same list.
/// - `PREPARE` has already had its body parsed by the classifier, so a
///   malformed body fails here, at PREPARE time, with a `SYNTAX_ERROR`.
/// - `EXECUTE` binds the parsed `USING` expressions into the header-carried
///   template through the shared comment-aware scanner, then normalizes the
///   bound text. `DEALLOCATE` and `DESCRIBE OUTPUT|INPUT` of an unknown name
///   are `NOT_FOUND`, as in Trino.
fn resolve_statement(
    sql: &str,
    session_catalog: Option<&str>,
    prepared: &std::collections::HashMap<String, String>,
    query_id: &str,
    on_parse_error: impl FnOnce(&sqe_core::SqeError),
) -> Result<ResolvedStatement, Box<TrinoError>> {
    use sqe_sql::{classify_normalized, SqlFrontDoor, StatementKind, WireDialect};
    use sqlparser::ast::{Expr, Statement, Use, Value};

    let door = SqlFrontDoor::new(WireDialect::Trino).with_session_catalog(session_catalog);
    let normalized = door.normalize(sql).map_err(|e| {
        on_parse_error(&e);
        TrinoError::from_sqe_error(&e, Some(query_id))
    })?;
    let facts = StatementFacts::from_normalized(&normalized);
    let exec_sql = normalized.sql().to_string();

    // `EXECUTE IMMEDIATE '<sql>' [USING ...]` (Trino JDBC with
    // explicitPrepare=false). sqlparser's Generic dialect reads it as
    // `EXECUTE immediate '<sql>'`: a statement named IMMEDIATE with one
    // positional string argument. Recognised from that AST shape; a prepared
    // statement that is really called `immediate` has no positional string.
    if let Some(Statement::Execute {
        name: Some(name),
        parameters,
        has_parentheses: false,
        immediate: false,
        using,
        ..
    }) = normalized.statement()
    {
        if name.to_string().eq_ignore_ascii_case("immediate") {
            if let [Expr::Value(inline)] = parameters.as_slice() {
                if let Value::SingleQuotedString(inline_sql) = &inline.value {
                    let params: Vec<Expr> = using.iter().map(|u| u.expr.clone()).collect();
                    return resolve_bound_sql(&door, inline_sql, &params, query_id);
                }
            }
        }
    }

    // `StatementKind::Use` carries only the printed target; keep the AST so
    // quoted parts are unquoted the way the parser saw them.
    let use_target = match normalized.statement() {
        Some(Statement::Use(u)) => Some(u.clone()),
        _ => None,
    };

    let kind = classify_normalized(normalized)
        .map_err(|e| TrinoError::from_sqe_error(&e, Some(query_id)))?;
    let immediate = |session_update: protocol::UpdatedSessionState| ResolvedStatement {
        plan: ExecPlan::Immediate,
        facts: facts.clone(),
        session_update: Some(session_update),
    };
    let describe = |name: String,
                    kind: protocol::DescribeKind|
     -> Result<ResolvedStatement, Box<TrinoError>> {
        let template = prepared.get(&name).ok_or_else(|| {
            Box::new(TrinoError::prepared_statement_not_found(
                &name,
                Some(query_id),
            ))
        })?;
        Ok(ResolvedStatement {
            plan: ExecPlan::DescribePrepared {
                kind,
                template: template.clone(),
            },
            facts: facts.clone(),
            session_update: None,
        })
    };

    Ok(match kind {
        StatementKind::Use(_) => {
            let mut update = protocol::UpdatedSessionState::default();
            let Some(target) = use_target else {
                return Err(Box::new(TrinoError::internal_error(
                    "USE classified without a parsed statement",
                    Some(query_id),
                )));
            };
            let parts = |name: &sqlparser::ast::ObjectName| -> Vec<String> {
                name.0
                    .iter()
                    .map(|p| match p {
                        sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                        other => other.to_string(),
                    })
                    .collect()
            };
            match &target {
                Use::Object(name) | Use::Schema(name) | Use::Database(name) => {
                    match parts(name).as_slice() {
                        [schema] => update.set_schema = Some(schema.clone()),
                        [catalog, schema] => {
                            update.set_catalog = Some(catalog.clone());
                            update.set_schema = Some(schema.clone());
                        }
                        _ => {
                            return Err(Box::new(TrinoError::user_error(
                                format!("Invalid USE target '{name}': expected [catalog.]schema"),
                                Some(query_id),
                            )))
                        }
                    }
                }
                Use::Catalog(name) => update.set_catalog = parts(name).first().cloned(),
                other => {
                    return Err(Box::new(TrinoError::user_error(
                        format!("Unsupported USE form: {other}"),
                        Some(query_id),
                    )))
                }
            }
            ResolvedStatement {
                plan: ExecPlan::Sql(exec_sql),
                facts,
                session_update: Some(update),
            }
        }
        StatementKind::SetCatalog(catalog) => immediate(protocol::UpdatedSessionState {
            set_catalog: Some(catalog),
            ..Default::default()
        }),
        StatementKind::SetSession { name, value } => {
            let prop = session_props::lookup(&name).ok_or_else(|| {
                TrinoError::invalid_session_property(
                    session_props::unknown_property_message(&name),
                    Some(query_id),
                )
            })?;
            prop.validate(&value)
                .map_err(|msg| TrinoError::invalid_session_property(msg, Some(query_id)))?;
            immediate(protocol::UpdatedSessionState {
                set_session: vec![(name, value)],
                ..Default::default()
            })
        }
        StatementKind::ResetSession(name) => {
            if session_props::lookup(&name).is_none() {
                return Err(Box::new(TrinoError::invalid_session_property(
                    session_props::unknown_property_message(&name),
                    Some(query_id),
                )));
            }
            immediate(protocol::UpdatedSessionState {
                clear_session: vec![name],
                ..Default::default()
            })
        }
        StatementKind::ShowSession { like } => ResolvedStatement {
            plan: ExecPlan::ShowSession { like },
            facts,
            session_update: None,
        },
        StatementKind::Prepare { name, body } => immediate(protocol::UpdatedSessionState {
            added_prepare: vec![(name, body.to_string())],
            ..Default::default()
        }),
        StatementKind::Deallocate(name) => {
            if !prepared.contains_key(&name) {
                return Err(Box::new(TrinoError::prepared_statement_not_found(
                    &name,
                    Some(query_id),
                )));
            }
            immediate(protocol::UpdatedSessionState {
                deallocated_prepare: vec![name],
                ..Default::default()
            })
        }
        StatementKind::Execute { name, params } => {
            let template = prepared
                .get(&name)
                .ok_or_else(|| TrinoError::prepared_statement_not_found(&name, Some(query_id)))?;
            return resolve_bound_sql(&door, template, &params, query_id);
        }
        StatementKind::DescribeOutput(name) => describe(name, protocol::DescribeKind::Output)?,
        StatementKind::DescribeInput(name) => describe(name, protocol::DescribeKind::Input)?,
        // `EXPLAIN FULL` is SQE grammar the front door classifies without a
        // sqlparser AST; it is still an EXPLAIN for the result reshape.
        StatementKind::ExplainFull(_) => ResolvedStatement {
            plan: ExecPlan::Sql(exec_sql),
            facts: StatementFacts {
                is_explain: true,
                ..facts
            },
            session_update: None,
        },
        _ => ResolvedStatement {
            plan: ExecPlan::Sql(exec_sql),
            facts,
            session_update: None,
        },
    })
}

/// Bind `params` into `template` and normalize the bound statement: the
/// executed text of an `EXECUTE` is the template, not the `EXECUTE` itself,
/// so the front door runs on it once here.
fn resolve_bound_sql(
    door: &sqe_sql::SqlFrontDoor,
    template: &str,
    params: &[sqlparser::ast::Expr],
    query_id: &str,
) -> Result<ResolvedStatement, Box<TrinoError>> {
    let bound = prepared::bind_execute(template, params)
        .map_err(|msg| TrinoError::user_error(msg, Some(query_id)))?;
    let normalized = door
        .normalize(&bound)
        .map_err(|e| TrinoError::from_sqe_error(&e, Some(query_id)))?;
    Ok(ResolvedStatement {
        facts: StatementFacts::from_normalized(&normalized),
        plan: ExecPlan::Sql(normalized.into_parts().0),
        session_update: None,
    })
}

/// Extract the affected-row count from a write-path response.
///
/// DataFusion returns a single 1x1 batch with a `count` column for
/// INSERT/UPDATE/DELETE/MERGE. We grovel through that shape and fall back
/// to 0 if the engine returned nothing.
fn extract_update_count(batches: &[arrow_array::RecordBatch]) -> Option<i64> {
    let batch = batches.first()?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return None;
    }
    let arr = batch.column(0);
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(a.value(0));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return Some(a.value(0) as i64);
    }
    None
}

/// Emit `X-Trino-Set-*` / `X-Trino-Clear-*` response headers describing
/// the session-state mutation the executor just performed. Clients
/// observe these and resend the resulting state on the next request.
fn apply_session_headers(
    headers: &mut axum::http::HeaderMap,
    update: &protocol::UpdatedSessionState,
) {
    use axum::http::{HeaderName, HeaderValue};

    fn insert(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.insert(name, v);
        }
    }

    fn append(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.append(name, v);
        }
    }

    if let Some(catalog) = &update.set_catalog {
        insert(
            headers,
            HeaderName::from_static("x-trino-set-catalog"),
            catalog,
        );
    }
    if let Some(schema) = &update.set_schema {
        insert(
            headers,
            HeaderName::from_static("x-trino-set-schema"),
            schema,
        );
    }
    for (name, value) in &update.set_session {
        append(
            headers,
            HeaderName::from_static("x-trino-set-session"),
            &format!("{name}={value}"),
        );
    }
    for name in &update.clear_session {
        append(
            headers,
            HeaderName::from_static("x-trino-clear-session"),
            name,
        );
    }
    for (name, sql) in &update.added_prepare {
        // The SQL value must be URL-encoded: it contains spaces, commas, and
        // `=` that would otherwise corrupt the header. The Trino client replays
        // this verbatim in X-Trino-Prepared-Statement, where we URL-decode it.
        append(
            headers,
            HeaderName::from_static("x-trino-added-prepare"),
            &format!("{name}={}", prepared::form_urlencode(sql)),
        );
    }
    for name in &update.deallocated_prepare {
        append(
            headers,
            HeaderName::from_static("x-trino-deallocated-prepare"),
            name,
        );
    }
}

// ── Header extraction ─────────────────────────────────────────

/// Extract Trino client headers from the HTTP request.
fn extract_trino_headers(headers: &HeaderMap) -> TrinoClientHeaders {
    // The client replays `SET SESSION` values in `X-Trino-Session`; pick out
    // `iceberg.compression_codec` so writes in this request honour it (#353).
    let compression_codec = incoming_session_properties(headers)
        .into_iter()
        .find(|(k, _)| k == "iceberg.compression_codec")
        .map(|(_, v)| v);
    TrinoClientHeaders {
        catalog: extract_header(headers, "x-trino-catalog"),
        schema: extract_header(headers, "x-trino-schema"),
        user: extract_header(headers, "x-trino-user"),
        source: extract_header(headers, "x-trino-source"),
        compression_codec,
    }
}

/// Extract a single header value as a trimmed, non-empty string.
fn extract_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Parse the catalog session properties the client carries in `X-Trino-Session`
/// (the values it replays after a `SET SESSION`). The header is a
/// comma-separated list of `name=value` pairs, possibly split across multiple
/// header lines. Used to answer `SHOW SESSION`. (#323)
fn incoming_session_properties(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut props = Vec::new();
    for value in headers.get_all("x-trino-session") {
        let Ok(raw) = value.to_str() else { continue };
        for pair in raw.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((name, val)) = pair.split_once('=') {
                props.push((name.trim().to_string(), val.trim().to_string()));
            }
        }
    }
    props
}

/// Match a value against a Trino `LIKE` pattern (`%` = any sequence, `_` = any
/// single char). Used to filter `SHOW SESSION LIKE '...'`. Case-sensitive,
/// matching Trino's session-property names.
fn like_match(value: &str, pattern: &str) -> bool {
    // Anchored glob match via classic dynamic programming over chars.
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (n, m) = (v.len(), p.len());
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;
    for j in 1..=m {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=n {
        for j in 1..=m {
            dp[i][j] = match p[j - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && v[i - 1] == c,
            };
        }
    }
    dp[n][m]
}

/// Build the `SHOW SESSION` result set in Trino's column shape
/// (`Name, Value, Default, Type, Description`): one row per allow-listed
/// property ([`session_props::SESSION_PROPERTIES`]), showing the value the
/// client is carrying in `X-Trino-Session` when it has set one, otherwise the
/// default. Filtered by an optional `LIKE` pattern on the name. (#323, 708)
fn build_show_session_batches(
    props: &[(String, String)],
    like: Option<&str>,
) -> Vec<arrow_array::RecordBatch> {
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Value", DataType::Utf8, true),
        Field::new("Default", DataType::Utf8, true),
        Field::new("Type", DataType::Utf8, true),
        Field::new("Description", DataType::Utf8, true),
    ]));

    let mut names = Vec::new();
    let mut values: Vec<Option<String>> = Vec::new();
    let mut defaults: Vec<Option<String>> = Vec::new();
    let mut types = Vec::new();
    let mut descriptions = Vec::new();
    for prop in session_props::SESSION_PROPERTIES {
        if let Some(pat) = like {
            if !like_match(prop.name, pat) {
                continue;
            }
        }
        let current = props
            .iter()
            .find(|(k, _)| k == prop.name)
            .map(|(_, v)| v.clone());
        names.push(prop.name.to_string());
        values.push(current.or_else(|| prop.default.map(str::to_string)));
        defaults.push(prop.default.map(str::to_string));
        types.push(prop.type_name.to_string());
        descriptions.push(prop.description.to_string());
    }

    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(values)),
            Arc::new(StringArray::from(defaults)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(descriptions)),
        ],
    );
    match batch {
        Ok(b) => vec![b],
        Err(_) => vec![],
    }
}

/// Apply extracted Trino headers to a session (catalog, schema, source).
fn apply_trino_headers(session: Session, trino_headers: &TrinoClientHeaders) -> Session {
    session
        .with_catalog(trino_headers.catalog.clone())
        .with_schema(trino_headers.schema.clone())
        .with_source(trino_headers.source.clone())
        .with_compression_codec(trino_headers.compression_codec.clone())
}

// ── Pagination helpers ────────────────────────────────────────

/// Build a `nextUri` for the page after `token`, or `None` if `is_last`.
fn next_uri(
    base_url: &str,
    query_id: &str,
    slug: &str,
    token: usize,
    is_last: bool,
) -> Option<String> {
    if is_last {
        None
    } else {
        Some(results_uri(base_url, query_id, slug, token + 1))
    }
}

/// Build an absolute `infoUri` for the given query.
fn info_uri(base_url: &str, query_id: &str) -> String {
    format!("{base_url}/v1/query/{query_id}")
}

/// Build a results-paging URI. The `slug` segment is the query's poll token.
fn results_uri(base_url: &str, query_id: &str, slug: &str, token: usize) -> String {
    format!("{base_url}/v1/statement/{query_id}/{slug}/{token}")
}

/// Build a `queued`-route URI (the status/poll namespace).
fn queued_uri(base_url: &str, query_id: &str, slug: &str, token: usize) -> String {
    format!("{base_url}/v1/statement/queued/{query_id}/{slug}/{token}")
}

/// Response for a POST whose query did not finish within the bounded wait:
/// no data, `state=QUEUED`, `nextUri` -> the queued poll route (token 1).
fn build_started_response(base_url: &str, query_id: &str, slug: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, slug, 1)),
        stats: TrinoStats::queued(),
        ..Default::default()
    }
}

/// Response for a poll on a query still running: no data, `state=RUNNING`,
/// `nextUri` -> the next queued poll token.
fn build_running_response(
    base_url: &str,
    query_id: &str,
    slug: &str,
    next_token: usize,
) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, slug, next_token)),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}

/// Response for a poll on a query that just finished: no data, `state=RUNNING`,
/// `nextUri` -> the results-paging route at token 0. The queued route stays a
/// status/redirect endpoint; the results route stays pure data paging.
fn build_finished_redirect_response(base_url: &str, query_id: &str, slug: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(results_uri(base_url, query_id, slug, 0)),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}

/// Wait until `handle.status` is terminal or `max_wait` elapses. A missed
/// `notify_waiters` (tokio `Notify` does not store permits) only defers to the
/// next client poll — correctness holds, at most one extra round-trip.
async fn await_terminal_or_timeout(handle: &QueryHandle, max_wait: std::time::Duration) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        {
            if handle.status.lock().is_terminal() {
                return;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        // Ignore the timeout result: on either arm we loop and re-check status,
        // and the deadline check above terminates the loop when time is up.
        let _ = tokio::time::timeout(remaining, handle.notify.notified()).await;
    }
}

/// Parse Trino's `maxWait` duration string and clamp to `[0, MAX_WAIT_CAP]`.
/// Absent or unparseable input falls back to `DEFAULT_MAX_WAIT`.
fn clamp_max_wait(raw: Option<&str>) -> std::time::Duration {
    match raw.and_then(parse_trino_duration) {
        Some(d) => d.min(MAX_WAIT_CAP),
        None => DEFAULT_MAX_WAIT,
    }
}

/// Parse a Trino duration literal: an integer followed by `ms`, `s`, or `m`.
fn parse_trino_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_millis);
    }
    if let Some(num) = s.strip_suffix('s') {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_secs);
    }
    if let Some(num) = s.strip_suffix('m') {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(|m| std::time::Duration::from_secs(m * 60));
    }
    None
}

/// Is the connecting peer one of `[security] trusted_proxies`? Same rule
/// `resolve_client_ip` applies to `x-forwarded-for`.
fn peer_is_trusted_proxy<A, Q>(state: &TrinoState<A, Q>, peer: SocketAddr) -> bool {
    state.security.is_trusted_proxy(&peer.ip().to_string())
}

/// Derive the base URL for `nextUri`/`infoUri`, falling back to
/// `localhost:<port>`. Issue #726.
///
/// `x-forwarded-proto` / `-host` / `-port` are honoured only from a trusted
/// proxy (`[security] trusted_proxies`, exact IPs or CIDR ranges); anyone
/// else gets the plain `Host` header over this listener's own scheme, http.
/// Behind a TLS-terminating ingress that SQE cannot pin by IP,
/// `public_base_url` (`[coordinator]`) supplies the https URL instead, but
/// only for requests addressed to its host: in-cluster clients calling the
/// service directly keep URIs that point back at the service.
///
/// A trusted `x-forwarded-proto: https` always yields https, port or no
/// port. Clients follow `nextUri` with their bearer token, so a downgraded
/// scheme sends the token in cleartext to port 80 before any redirect.
#[cfg(test)]
fn extract_base_url(
    headers: &HeaderMap,
    bound_port: u16,
    trusted_proxy: bool,
    public_base_url: Option<&str>,
) -> String {
    extract_base_url_on(headers, None, bound_port, trusted_proxy, public_base_url)
}

/// The base URL for a request that arrived on `tls` (the HTTPS listener) or,
/// with `None`, on the plain listener.
fn request_base_url<A, Q>(
    state: &TrinoState<A, Q>,
    peer: SocketAddr,
    headers: &HeaderMap,
    tls: Option<&HttpsListenerMarker>,
) -> String {
    extract_base_url_on(
        headers,
        tls,
        state.port,
        peer_is_trusted_proxy(state, peer),
        state.public_base_url.as_deref(),
    )
}

/// [`extract_base_url`] for a request on either listener. A request that
/// reached the HTTPS listener defaults to https: its `nextUri` must not send
/// the client back over plain HTTP. A trusted proxy's `x-forwarded-proto`
/// still names the client-facing scheme.
fn extract_base_url_on(
    headers: &HeaderMap,
    tls: Option<&HttpsListenerMarker>,
    plain_port: u16,
    trusted_proxy: bool,
    public_base_url: Option<&str>,
) -> String {
    let (listener_scheme, bound_port) = match tls {
        Some(marker) => ("https", marker.port),
        None => ("http", plain_port),
    };
    // A proxy chain appends: the first value is the client-facing hop.
    let forwarded = |name: &str| {
        trusted_proxy
            .then(|| extract_header(headers, name))
            .flatten()
            .and_then(|v| v.split(',').next().map(|first| first.trim().to_string()))
            .filter(|v| !v.is_empty())
    };
    let Some(host) = forwarded("x-forwarded-host").or_else(|| extract_header(headers, "host")) else {
        return format!("{listener_scheme}://localhost:{bound_port}");
    };
    if let Some((scheme, authority)) =
        public_base_url.and_then(|url| sqe_core::config::parse_public_base_url(url).ok())
    {
        if host_name(&authority).eq_ignore_ascii_case(host_name(&host)) {
            return format!("{scheme}://{authority}");
        }
    }
    let scheme = match forwarded("x-forwarded-proto").map(|p| p.to_ascii_lowercase()) {
        Some(p) if p == "https" || p == "http" => p,
        _ => listener_scheme.to_string(),
    };
    let default_port = if scheme == "https" { 443 } else { 80 };
    let port = forwarded("x-forwarded-port")
        .and_then(|p| p.parse::<u16>().ok())
        .filter(|p| *p != default_port && host_name(&host) == host);
    match port {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

/// The host of a `host[:port]` / `[ipv6]:port` authority, without the port.
fn host_name(authority: &str) -> &str {
    if authority.starts_with('[') {
        return authority.find(']').map_or(authority, |end| &authority[..=end]);
    }
    authority.rsplit_once(':').map_or(authority, |(host, _)| host)
}

/// Build a [`TrinoResponse`] for one page pulled from a [`ResultCursor`].
fn build_page_response(
    base_url: &str,
    query_id: &str,
    poll_token: &str,
    cursor: &ResultCursor,
    page: &ReadyPage,
) -> TrinoResponse {
    // DDL/update statements produce no columns. The Trino 465 JDBC client's
    // ResultRowsDecoder early-returns on `data == null` but otherwise asserts
    // `columns` is non-empty, so a non-null `data` with empty `columns` is
    // rejected ("Columns must be set when decoding data"). Real Trino omits
    // `data` entirely for updates; match that by emitting None when there are
    // no columns. A SELECT returning zero rows still has columns, so its
    // (empty) data stays present. See issue #314.
    let data = if cursor.columns.is_empty() {
        None
    } else {
        Some(page.rows.clone())
    };

    let metrics = protocol::ExecutionMetrics {
        elapsed_millis: cursor.created_at.elapsed().as_millis() as u64,
        processed_rows: page.processed_rows as u64,
        ..protocol::ExecutionMetrics::default()
    };
    // The total page count is unknown while streaming; report "this page done,
    // at least one more" so the split counters keep moving.
    let stats = if page.is_last {
        TrinoStats::finished_with_metrics(&metrics)
    } else {
        TrinoStats::running_with_metrics(page.token + 1, page.token + 2, &metrics)
    };
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: next_uri(base_url, query_id, poll_token, page.token, page.is_last),
        columns: Some(cursor.columns.clone()),
        data,
        stats,
        update_type: cursor.update_type.clone(),
        update_count: cursor.update_count,
        ..Default::default()
    }
}

/// Error body for a results-route request whose query id is not registered.
/// An id evicted for idleness says so; anything else is unknown (#621).
fn not_found_response<A, Q>(state: &TrinoState<A, Q>, base_url: &str, id: &str) -> Response {
    let message = if state.expired.contains_key(id) {
        format!(
            "Query result expired: no page was requested for {QUERY_REGISTRY_IDLE_SECS}s and the result was released"
        )
    } else {
        "Query not found".to_string()
    };
    let response = TrinoResponse {
        id: id.to_string(),
        info_uri: Some(info_uri(base_url, id)),
        stats: TrinoStats::failed(),
        error: Some(TrinoError {
            message,
            error_code: 1,
            error_name: "USER_ERROR".to_string(),
            error_type: "USER_ERROR".to_string(),
            query_id: None,
            failure_info: None,
            error_location: None,
        }),
        ..Default::default()
    };
    (StatusCode::NOT_FOUND, Json(response)).into_response()
}

/// Terminal-status replay shared by the poll and results routes: the mapped
/// error for `Failed`, the cancel error for `Cancelled`, `None` otherwise.
fn terminal_error_response(base_url: &str, id: &str, status: &QueryStatus) -> Option<Response> {
    match status {
        QueryStatus::Failed(trino_error) => {
            let is_rate_limited = trino_error.error_name == "RESOURCE_EXHAUSTED";
            let response = TrinoResponse {
                id: id.to_string(),
                info_uri: Some(info_uri(base_url, id)),
                stats: TrinoStats::failed(),
                error: Some(trino_error.as_ref().clone()),
                ..Default::default()
            };
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if is_rate_limited {
                resp.headers_mut().insert(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
            }
            Some(resp)
        }
        QueryStatus::Cancelled => {
            let response = TrinoResponse {
                id: id.to_string(),
                info_uri: Some(info_uri(base_url, id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError::user_error("Query was canceled", Some(id))),
                ..Default::default()
            };
            Some((StatusCode::OK, Json(response)).into_response())
        }
        QueryStatus::Finished | QueryStatus::Queued | QueryStatus::Running => None,
    }
}

/// Serve page `token` of a query whose status is `Finished`: pull it from
/// the cursor (or replay the page just served), or, when the stream fails
/// mid-way, mark the query failed and return the mapped error body.
async fn serve_page(handle: &QueryHandle, base_url: &str, id: &str, token: usize) -> Response {
    let mut cursor_guard = handle.cursor.lock().await;
    let Some(cursor) = cursor_guard.as_mut() else {
        // Cursor already released: a cancel or failure raced this fetch.
        drop(cursor_guard);
        let status = handle.status.lock();
        return terminal_error_response(base_url, id, &status).unwrap_or_else(|| {
            error_response(StatusCode::NOT_FOUND, "Query result is no longer available")
        });
    };
    // Pulling the stream is where the query runs, and this fetch holds the
    // cursor lock while it does, so `finish_with` cannot drop the cursor
    // under it. Race the fetch against a cancel or failure: on either, drop
    // the stream here, which stops the execution and releases its
    // reservation, instead of running the query to completion.
    let fetched = {
        let fetch = cursor.fetch(token);
        tokio::pin!(fetch);
        loop {
            let notified = handle.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if matches!(
                *handle.status.lock(),
                QueryStatus::Cancelled | QueryStatus::Failed(_)
            ) {
                break None;
            }
            tokio::select! {
                result = &mut fetch => break Some(result),
                () = &mut notified => {}
            }
        }
    };
    let Some(fetched) = fetched else {
        *cursor_guard = None;
        drop(cursor_guard);
        let status = handle.status.lock();
        return terminal_error_response(base_url, id, &status).unwrap_or_else(|| {
            error_response(StatusCode::NOT_FOUND, "Query result is no longer available")
        });
    };
    match fetched {
        Ok(PageFetch::Page(page)) => {
            let response =
                build_page_response(base_url, id, &handle.owner.poll_token, cursor, &page);
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(PageFetch::OutOfRange { expected }) => {
            let response = TrinoResponse {
                id: id.to_string(),
                info_uri: Some(info_uri(base_url, id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: format!(
                        "Page token {token} out of range: pages are served in order and only the last page ({}) may be re-requested",
                        expected.saturating_sub(1)
                    ),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                    query_id: None,
                    failure_info: None,
                    error_location: None,
                }),
                ..Default::default()
            };
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
        Err(sqe_err) => {
            warn!(
                error_code = %sqe_err.error_code(),
                query_id = %id,
                error = %sqe_err,
                "Trino result stream failed while paging"
            );
            *cursor_guard = None;
            drop(cursor_guard);
            let trino_error = protocol::TrinoError::from_sqe_error(&sqe_err, Some(id));
            let status = QueryStatus::Failed(Box::new(trino_error));
            let resp = terminal_error_response(base_url, id, &status);
            *handle.status.lock() = status;
            handle.notify.notify_waiters();
            resp.unwrap_or_else(|| error_response(StatusCode::INTERNAL_SERVER_ERROR, "unreachable"))
        }
    }
}

// ── Handlers ──────────────────────────────────────────────────

/// Run a resolved statement: DESCRIBE of a prepared template, SHOW SESSION
/// from local state, or the executor. `Immediate` plans never get here; they
/// are answered on the POST.
async fn run_statement<Q: TrinoQueryExecutor>(
    handler: &Q,
    session: &Session,
    plan: &ExecPlan,
    show_session_props: &[(String, String)],
) -> Result<TrinoResultStream, sqe_core::SqeError> {
    match plan {
        ExecPlan::Immediate => Ok(TrinoResultStream::from_batches(vec![])),
        ExecPlan::DescribePrepared { kind, template } => handler
            .describe_prepared(session, template, *kind)
            .await
            .map(TrinoResultStream::from_batches),
        ExecPlan::ShowSession { like } => Ok(TrinoResultStream::from_batches(
            build_show_session_batches(show_session_props, like.as_deref()),
        )),
        ExecPlan::Sql(sql) => handler.execute(session, sql).await,
    }
}

/// Turn a successful statement's result stream into a [`ResultCursor`].
///
/// Plain queries stream straight through. The shape-rewritten statement
/// classes (DDL/DML counts, `information_schema`, `DESCRIBE`, `EXPLAIN`)
/// need the whole result to rewrite it, so those are drained first; they are
/// catalog-sized by construction. Which class a statement is in was decided
/// from its AST at submit time ([`StatementFacts`]), never from the text.
async fn open_cursor(
    result: TrinoResultStream,
    facts: &StatementFacts,
    session_catalog: Option<&str>,
    page_size: usize,
    reservation: Option<MemoryReservation>,
) -> Result<ResultCursor, sqe_core::SqeError> {
    if !facts.needs_reshape() {
        return Ok(ResultCursor::new(
            result,
            page_size,
            reservation,
            None,
            None,
        ));
    }

    let batches = result.collect().await?;
    let update_count = if facts.update_type.is_some() {
        extract_update_count(&batches).or(Some(0))
    } else {
        None
    };
    let batches = if facts.is_metadata {
        let batches = info_schema_compat::apply_info_schema_compat(batches, session_catalog);
        if facts.is_describe_or_show_columns {
            info_schema_compat::reshape_describe_to_trino(batches)
        } else {
            batches
        }
    } else {
        batches
    };
    let batches = if facts.is_explain {
        explain_compat::reshape_explain_to_trino(batches)
    } else {
        batches
    };
    Ok(ResultCursor::new(
        TrinoResultStream::from_batches(batches),
        page_size,
        reservation,
        facts.update_type.map(str::to_string),
        update_count,
    ))
}

async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Conn { peer, tls }: Conn,
    headers: HeaderMap,
    body: String,
) -> Response {
    let parent = sqe_metrics::propagation::extract_trace_context_from_headers(&headers);
    let correlation = sqe_metrics::propagation::correlation_ids_from_headers(&headers);
    let span = tracing::info_span!(
        "trino.submit_query",
        db.system.name = "sqe",
        db.operation.name = tracing::field::Empty,
        db.namespace = tracing::field::Empty,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
        request_id = tracing::field::Empty,
        session_id = tracing::field::Empty,
        http.request_id = tracing::field::Empty,
        sqe.session.id = tracing::field::Empty,
    );
    let _ = span.set_parent(parent);
    sqe_metrics::propagation::record_trace_fields(&span);
    record_correlation_fields(&span, &correlation);
    async move {
        let sql = body.trim();
        if sql.is_empty() {
            return error_response(StatusCode::BAD_REQUEST, "Empty query");
        }

        let client_ip = trino_client_ip(&state, &headers, peer);
        let peer_host = strip_port(&client_ip).to_string();

        let trino_headers = extract_trino_headers(&headers);

        let session = match authenticate_request(&state, &headers, &peer_host, &client_ip).await {
            Ok(session) => session,
            Err(AuthFailure::RateLimited) => return rate_limited_response(),
            Err(AuthFailure::Rejected) => {
                return error_response(StatusCode::UNAUTHORIZED, "authentication failed")
            }
            Err(AuthFailure::Missing) => {
                if let Some(ref oauth2_state) = state.oauth2 {
                    return match crate::oauth2::generate_challenge(oauth2_state).await {
                        Ok((_auth_id, www_authenticate)) => (
                            StatusCode::UNAUTHORIZED,
                            [(axum::http::header::WWW_AUTHENTICATE, www_authenticate)],
                            "Authentication required",
                        )
                            .into_response(),
                        Err(status) => error_response(status, "Failed to generate auth challenge"),
                    };
                }
                return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
            }
        };

        // Apply Trino client headers (catalog, schema, source) to the session.
        let session = apply_trino_headers(session, &trino_headers);

        // Record OTel semantic convention attributes on the current span.
        {
            let span = tracing::Span::current();
            // Best-effort: classify the SQL to get the operation name.
            let op = sql
                .split_whitespace()
                .next()
                .unwrap_or("unknown")
                .to_uppercase();
            span.record("db.operation.name", op.as_str());
            if let Some(ref schema) = session.default_schema {
                span.record("db.namespace", schema.as_str());
            }
        }

        let sql_hash = {
            use sha2::{Digest, Sha256};
            let normalised: String = sql
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase();
            hex::encode(Sha256::digest(normalised.as_bytes()))
        };
        info!(
            user = %session.user.username,
            client_ip = %client_ip,
            catalog = ?session.default_catalog,
            schema = ?session.default_schema,
            source = ?session.source,
            sql_hash = %sql_hash,
            sql_len = sql.len(),
            "Trino query submitted"
        );

        let query_id = Uuid::new_v4().to_string();
        let base_url = request_base_url(&state, peer, &headers, tls.as_ref());

        // Trino prepared statements are stateless: the client carries the prepared
        // SQL in X-Trino-Prepared-Statement headers and submits `EXECUTE <name>
        // USING ...`; `DEALLOCATE` and `DESCRIBE OUTPUT|INPUT` resolve against
        // the same map.
        let prepared = {
            let values: Vec<String> = headers
                .get_all("x-trino-prepared-statement")
                .iter()
                .filter_map(|v| v.to_str().ok().map(str::to_string))
                .collect();
            prepared::parse_prepared_statements(&values)
        };

        // The one SQL front door for the Trino wire (finding 700): normalize
        // and classify once, from the AST. Session-control statements are
        // decided here and never reach the executor; everything else is handed
        // over as the normalized text with no further rewriting.
        let ResolvedStatement {
            plan,
            facts,
            session_update,
        } = match resolve_statement(
            sql,
            session.default_catalog.as_deref(),
            &prepared,
            &query_id,
            |e| state.query_handler.audit_rejected(&session, sql, e),
        ) {
            Ok(resolved) => resolved,
            Err(error) => {
                let response = TrinoResponse {
                    id: query_id.clone(),
                    info_uri: Some(info_uri(&base_url, &query_id)),
                    stats: TrinoStats::failed(),
                    error: Some(*error),
                    ..Default::default()
                };
                return (StatusCode::OK, Json(response)).into_response();
            }
        };

        // Statements the protocol layer answers by itself (PREPARE, DEALLOCATE
        // PREPARE, SET SESSION, RESET SESSION, SET CATALOG) have no result set
        // and are driven purely by response headers:
        //   - PREPARE / DEALLOCATE PREPARE register/forget prepared SQL via
        //     x-trino-added-prepare / x-trino-deallocated-prepare. This is the
        //     Metabase/JDBC connect gate: the driver issues PREPARE before any
        //     query. (#1)
        //   - SET SESSION / RESET SESSION set/clear the allow-listed session
        //     properties via x-trino-set-session / x-trino-clear-session; the
        //     client replays them in X-Trino-Session. (#323, #628)
        // This response is the statement's terminal response, so the headers
        // belong on it. USE goes to the executor and gets its headers on its
        // own terminal response below.
        if matches!(plan, ExecPlan::Immediate) {
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                stats: TrinoStats::finished(),
                // Non-null (empty) columns: the field is skip_serializing_if
                // None, so omitting it makes the JDBC driver see `columns: null`
                // and getColumns() throws. Real Trino returns [] for these
                // non-query statements, so the driver builds a 0-column
                // ResultSet and (with updateType set) treats it as a completed
                // update rather than waiting for a result set.
                columns: Some(vec![]),
                update_type: facts.update_type.map(str::to_string),
                ..Default::default()
            };
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if let Some(ref update) = session_update {
                apply_session_headers(resp.headers_mut(), update);
            }
            return resp;
        }

        // Mint the poll token once, here: the same value goes on the handle, on
        // the finished result, and into every `nextUri` this query ever emits, so
        // a client that follows the protocol never presents a credential again.
        let owner = QueryOwner::new(&session);

        // Register the query handle before spawning so a fast poll always finds it.
        let handle = Arc::new(QueryHandle::new(
            QueryStatus::Queued,
            owner.clone(),
            session_update.clone(),
        ));
        state.queries.insert(query_id.clone(), handle.clone());

        // Move owned state into the background task.
        let show_session_props = incoming_session_properties(&headers);
        let task_handler = state.query_handler.clone();
        let task_handle = handle.clone();
        let task_query_id = query_id.clone();
        let task_session = session.clone();
        let task_plan = plan;
        let task_facts = facts;
        let task_props = show_session_props;
        let task_catalog = session.default_catalog.clone();
        let page_size = state.page_size;
        let task_pool = state.query_handler.memory_pool();

        let execution_span = tracing::info_span!(
            "trino.query.execution",
            sqe.query.id = %query_id,
            trace_id = tracing::field::Empty,
            span_id = tracing::field::Empty,
        );
        sqe_metrics::propagation::record_trace_fields(&execution_span);
        let task = tokio::spawn(
            async move {
                // Drop guard: if `run_statement` panics (or the task is otherwise torn
                // down) without a terminal status set, force `Failed` on unwind so the
                // next poll returns an error instead of the client polling a
                // never-terminal `Running` entry forever (idle eviction would never
                // fire because each poll resets the idle timer). Normal completion sets
                // a terminal status first, so this no-ops.
                struct TerminalGuard(Arc<QueryHandle>);
                impl Drop for TerminalGuard {
                    fn drop(&mut self) {
                        let mut s = self.0.status.lock();
                        if !s.is_terminal() {
                            *s = QueryStatus::Failed(Box::new(
                                protocol::TrinoError::internal_error(
                                    "query task terminated unexpectedly",
                                    None,
                                ),
                            ));
                            self.0.notify.notify_waiters();
                        }
                    }
                }
                let _guard = TerminalGuard(task_handle.clone());

                *task_handle.status.lock() = QueryStatus::Running;
                task_handle.notify.notify_waiters();
                let result = run_statement(
                    task_handler.as_ref(),
                    &task_session,
                    &task_plan,
                    &task_props,
                )
                .await;
                let cursor = match result {
                    Ok(stream) => {
                        let reservation = task_pool.as_ref().map(|pool| {
                            MemoryConsumer::new(format!("TrinoResultCursor[{task_query_id}]"))
                                .register(pool)
                        });
                        open_cursor(
                            stream,
                            &task_facts,
                            task_catalog.as_deref(),
                            page_size,
                            reservation,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                };
                match cursor {
                    Ok(cursor) => {
                        // Install the cursor before flipping the status so a poll
                        // that sees `Finished` always finds it.
                        *task_handle.cursor.lock().await = Some(cursor);
                        *task_handle.status.lock() = QueryStatus::Finished;
                    }
                    Err(sqe_err) => {
                        tracing::warn!(
                            error_code = %sqe_err.error_code(),
                            query_id = %task_query_id,
                            error = %sqe_err,
                            "Trino query execution failed"
                        );
                        let trino_error =
                            protocol::TrinoError::from_sqe_error(&sqe_err, Some(&task_query_id));
                        *task_handle.status.lock() = QueryStatus::Failed(Box::new(trino_error));
                    }
                }
                task_handle.notify.notify_waiters();
            }
            .instrument(execution_span),
        );
        *handle.abort.lock() = Some(task.abort_handle());

        // Bounded wait: a query that finished already skips the queued route
        // and points straight at results page 0. The POST itself never carries
        // columns or data: trino-go-client (ADBC Trino, Grafana) only reads
        // rows from `nextUri` pages, exactly like real Trino serves them (#767).
        await_terminal_or_timeout(&handle, DEFAULT_MAX_WAIT).await;

        let status_is_finished = matches!(*handle.status.lock(), QueryStatus::Finished);
        if status_is_finished {
            let mut resp = (
                StatusCode::OK,
                Json(build_finished_redirect_response(
                    &base_url,
                    &query_id,
                    &owner.poll_token,
                )),
            )
                .into_response();
            if let Some(ref update) = session_update {
                apply_session_headers(resp.headers_mut(), update);
            }
            return resp;
        }

        // Failed within the wait: replay the mapped Trino error immediately so fast
        // failures still surface on the POST rather than forcing a poll. Preserve
        // the `Retry-After` header the old synchronous arm added for
        // ResourceExhausted (rate-limit/OOM), detected via the stable error name.
        {
            let status = handle.status.lock();
            if let QueryStatus::Failed(_) = *status {
                if let Some(resp) = terminal_error_response(&base_url, &query_id, &status) {
                    return resp;
                }
            }
        }

        // Still running: hand the client to the queued poll route. No session
        // headers here: the statement has not finished, so a `USE` that fails
        // after this point must not have switched the client already (708).
        // `get_queued_results` attaches them on the terminal `Finished` hop.
        (
            StatusCode::OK,
            Json(build_started_response(
                &base_url,
                &query_id,
                &owner.poll_token,
            )),
        )
            .into_response()
    }
    .instrument(span)
    .await
}

async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Conn { peer, tls }: Conn,
    headers: HeaderMap,
    Path((id, slug, token)): Path<(String, String, String)>,
) -> Response {
    let parent = sqe_metrics::propagation::extract_trace_context_from_headers(&headers);
    let correlation = sqe_metrics::propagation::correlation_ids_from_headers(&headers);
    let span = tracing::info_span!(
        "trino.get_results",
        sqe.query.id = %id,
        query_id = %id,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
        request_id = tracing::field::Empty,
        session_id = tracing::field::Empty,
        http.request_id = tracing::field::Empty,
        sqe.session.id = tracing::field::Empty,
    );
    let _ = span.set_parent(parent);
    sqe_metrics::propagation::record_trace_fields(&span);
    record_correlation_fields(&span, &correlation);
    async move {
        let client_ip = trino_client_ip(&state, &headers, peer);
        let peer_host = strip_port(&client_ip).to_string();

        // Fast path: the client replayed the poll token minted at submit,
        // which only the submitting client ever saw. That is proof of
        // ownership on its own, so the provider chain is never touched and
        // the pre-auth budget is never spent (issue #622).
        let registered = state.queries.get(&id);
        let by_poll_token = registered
            .as_ref()
            .is_some_and(|handle| handle.owner.owns_token(&slug));

        // Only when the token is absent or wrong do credentials get
        // validated — and then the rate limiter runs first.
        let session = if by_poll_token {
            None
        } else {
            match authenticate_request(&state, &headers, &peer_host, &client_ip).await {
                Ok(session) => Some(session),
                Err(failure) => return poll_auth_response(failure),
            }
        };

        // Parse the token as a page index.
        let page_token: usize = match token.parse() {
            Ok(t) => t,
            Err(_) => {
                return error_response(StatusCode::BAD_REQUEST, "Invalid page token");
            }
        };

        let base_url = request_base_url(&state, peer, &headers, tls.as_ref());

        let Some(handle) = registered else {
            return not_found_response(state.as_ref(), &base_url, &id);
        };

        if let Some(ref session) = session {
            if !handle.owner.is(session) {
                warn!(
                    query_id = %id,
                    caller = %session.user.username,
                    owner = %handle.owner.username,
                    "get_results denied: caller does not own query"
                );
                return StatusCode::FORBIDDEN.into_response();
            }
        }

        // Snapshot the lifecycle state in a short critical section; the
        // guard must not live across the page pull below.
        let early = {
            let status = handle.status.lock();
            match &*status {
                QueryStatus::Finished => None,
                QueryStatus::Queued | QueryStatus::Running => {
                    // Off-protocol: the results route before the poll route
                    // reported completion. Send the client back to polling.
                    Some(
                        (
                            StatusCode::OK,
                            Json(build_running_response(
                                &base_url,
                                &id,
                                &handle.owner.poll_token,
                                1,
                            )),
                        )
                            .into_response(),
                    )
                }
                terminal => terminal_error_response(&base_url, &id, terminal),
            }
        };
        if let Some(resp) = early {
            return resp;
        }

        serve_page(&handle, &base_url, &id, page_token).await
    }
    .instrument(span)
    .await
}

async fn get_queued_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Conn { peer, tls }: Conn,
    headers: HeaderMap,
    Path((id, slug, token)): Path<(String, String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let parent = sqe_metrics::propagation::extract_trace_context_from_headers(&headers);
    let correlation = sqe_metrics::propagation::correlation_ids_from_headers(&headers);
    let span = tracing::info_span!(
        "trino.get_queued_results",
        sqe.query.id = %id,
        query_id = %id,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
        request_id = tracing::field::Empty,
        session_id = tracing::field::Empty,
        http.request_id = tracing::field::Empty,
        sqe.session.id = tracing::field::Empty,
    );
    let _ = span.set_parent(parent);
    sqe_metrics::propagation::record_trace_fields(&span);
    record_correlation_fields(&span, &correlation);
    async move {
        let client_ip = trino_client_ip(&state, &headers, peer);
        let peer_host = strip_port(&client_ip).to_string();

        // Same fast path as `get_results`: a replayed poll token is proof of
        // ownership, so a 1/s poll of a long query costs the IdP nothing.
        let registered = state.queries.get(&id);
        let by_poll_token = registered
            .as_ref()
            .is_some_and(|handle| handle.owner.owns_token(&slug));

        let session = if by_poll_token {
            None
        } else {
            match authenticate_request(&state, &headers, &peer_host, &client_ip).await {
                Ok(session) => Some(session),
                Err(failure) => return poll_auth_response(failure),
            }
        };

        let base_url = request_base_url(&state, peer, &headers, tls.as_ref());
        let next_token: usize = token.parse().unwrap_or(1usize).saturating_add(1);

        let handle = match registered {
            Some(h) => h,
            // Unknown/evicted id -> Trino "query not found" / "result expired"
            // (same shape as get_results' miss arm).
            None => return not_found_response(state.as_ref(), &base_url, &id),
        };

        if let Some(ref session) = session {
            if !handle.owner.is(session) {
                warn!(
                    query_id = %id,
                    caller = %session.user.username,
                    owner = %handle.owner.username,
                    "get_queued_results denied: caller does not own query"
                );
                return StatusCode::FORBIDDEN.into_response();
            }
        }

        // Extract maxWait from the raw query string ("maxWait=1s").
        let max_wait_raw = raw_query
            .as_deref()
            .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("maxWait=")));
        let max_wait = clamp_max_wait(max_wait_raw);

        await_terminal_or_timeout(&handle, max_wait).await;

        // No `.await` below this lock; the whole match is one synchronous
        // critical section on a non-poisoning mutex.
        let status = handle.status.lock();
        match &*status {
            QueryStatus::Finished => {
                let mut resp = (
                    StatusCode::OK,
                    Json(build_finished_redirect_response(
                        &base_url,
                        &id,
                        &handle.owner.poll_token,
                    )),
                )
                    .into_response();
                // Echo the session-state mutation of a `USE` on this terminal
                // hop. Normally applied inline on the POST because such
                // statements finish in <1s; this covers the case where the task
                // finishes only after the POST's bounded wait elapsed. The
                // started response deliberately carried nothing (708), and the
                // results route that follows never repeats it.
                if let Some(ref update) = handle.session_update {
                    apply_session_headers(resp.headers_mut(), update);
                }
                resp
            }
            QueryStatus::Queued | QueryStatus::Running => (
                StatusCode::OK,
                Json(build_running_response(
                    &base_url,
                    &id,
                    &handle.owner.poll_token,
                    next_token,
                )),
            )
                .into_response(),
            terminal => terminal_error_response(&base_url, &id, terminal).unwrap_or_else(|| {
                error_response(StatusCode::INTERNAL_SERVER_ERROR, "unreachable")
            }),
        }
    }
    .instrument(span)
    .await
}

/// `DELETE` on the executing/queued URI the client is currently holding. The
/// slug segment is the query's poll token, so the submitting client cancels
/// without presenting a credential; anything else falls back to the
/// rate-limited credential path.
async fn cancel_query_slug<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    connect_info: Conn,
    headers: HeaderMap,
    Path((id, slug, _token)): Path<(String, String, String)>,
) -> Response {
    cancel_query_inner(state, connect_info, headers, id, slug).await
}

/// Legacy `DELETE /v1/statement/{id}`: no slug on the wire, so it always takes
/// the credential path.
async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    connect_info: Conn,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    // An empty slug can never equal a 43-character poll token, so the
    // constant-time comparison rejects it and the fallback runs.
    cancel_query_inner(state, connect_info, headers, id, String::new()).await
}

async fn cancel_query_inner<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    state: Arc<TrinoState<A, Q>>,
    Conn { peer, .. }: Conn,
    headers: HeaderMap,
    id: String,
    slug: String,
) -> Response {
    let parent = sqe_metrics::propagation::extract_trace_context_from_headers(&headers);
    let correlation = sqe_metrics::propagation::correlation_ids_from_headers(&headers);
    let span = tracing::info_span!(
        "trino.cancel_query",
        sqe.query.id = %id,
        query_id = %id,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
        request_id = tracing::field::Empty,
        session_id = tracing::field::Empty,
        http.request_id = tracing::field::Empty,
        sqe.session.id = tracing::field::Empty,
    );
    let _ = span.set_parent(parent);
    sqe_metrics::propagation::record_trace_fields(&span);
    record_correlation_fields(&span, &correlation);
    async move {
        let client_ip = trino_client_ip(&state, &headers, peer);
        let peer_host = strip_port(&client_ip).to_string();

        // Running and finished queries both live in `queries`; the owner
        // record carries the poll token.
        let registered = state.queries.get(&id);
        let by_poll_token = registered
            .as_ref()
            .is_some_and(|handle| handle.owner.owns_token(&slug));

        let session = if by_poll_token {
            None
        } else {
            match authenticate_request(&state, &headers, &peer_host, &client_ip).await {
                Ok(session) => Some(session),
                Err(failure) => return poll_auth_response(failure),
            }
        };

        // Verify the caller owns the query.
        if let Some(ref session) = session {
            if let Some(ref handle) = registered {
                if !handle.owner.is(session) {
                    warn!(
                        query_id = %id,
                        caller = %session.user.username,
                        owner = %handle.owner.username,
                        "Cancel denied: caller does not own query"
                    );
                    return StatusCode::FORBIDDEN.into_response();
                }
            }
        }

        // Abort the planning task if still in flight, and drop the cursor so
        // the upstream stream (and its pool reservation) is released now
        // rather than at idle eviction.
        if let Some(handle) = registered {
            if let Some(abort) = handle.abort.lock().as_ref() {
                abort.abort();
            }
            handle.finish_with(QueryStatus::Cancelled);
        }

        state.queries.invalidate(&id);
        StatusCode::NO_CONTENT.into_response()
    }
    .instrument(span)
    .await
}

fn record_correlation_fields(
    span: &tracing::Span,
    correlation: &sqe_metrics::propagation::CorrelationIds,
) {
    if let Some(request_id) = correlation.request_id.as_deref() {
        span.record("request_id", request_id);
        span.record("http.request_id", request_id);
    }
    if let Some(session_id) = correlation.session_id.as_deref() {
        span.record("session_id", session_id);
        span.record("sqe.session.id", session_id);
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let token = auth.strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).ok()?,
    )
    .ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer() -> Conn {
        Conn {
            peer: "127.0.0.1:12345".parse().unwrap(),
            tls: None,
        }
    }

    #[test]
    fn test_format_uptime_seconds() {
        let started = Instant::now();
        let uptime = format_uptime(started);
        assert!(
            uptime.ends_with('s'),
            "Expected seconds format, got: {uptime}"
        );
    }

    #[tokio::test]
    async fn test_server_info_when_ready() {
        let ready = Arc::new(AtomicBool::new(true));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: ready.clone(),
                started_at,
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let Json(info) = server_info(State(state)).await;
        assert_eq!(info.node_version.version, "0.1.0");
        assert!(info.coordinator);
        assert!(!info.starting);
        assert_eq!(info.environment, "production");
    }

    #[tokio::test]
    async fn test_server_info_when_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at,
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let Json(info) = server_info(State(state)).await;
        assert!(info.starting);
    }

    #[tokio::test]
    async fn test_server_state_active() {
        let ready = Arc::new(AtomicBool::new(true));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "ACTIVE");
    }

    #[tokio::test]
    async fn test_server_state_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "STARTING");
    }

    // Minimal mock types for handler tests
    struct MockAuth;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuth {
        async fn authenticate(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            Err(sqe_core::SqeError::Auth("mock".to_string()))
        }
    }

    /// Mock auth that returns a session for whatever username the
    /// caller passes. Used by ownership tests that need auth to succeed
    /// so the post-auth branch is the one under test.
    struct MockAuthOk;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuthOk {
        async fn authenticate(
            &self,
            user: &str,
            _: &str,
            _: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            Ok(Session::new(
                user.to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
        async fn authenticate_bearer(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            Ok(Session::new(
                "bearer-user".to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
    }

    fn basic_auth_header(user: &str, pass: &str) -> HeaderMap {
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        headers
    }

    struct MockQuery;
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for MockQuery {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                "mock".to_string(),
            ))
        }
    }

    struct TraceRecordingAuth {
        observed: Mutex<Vec<Option<String>>>,
    }

    #[async_trait::async_trait]
    impl TrinoAuthenticator for TraceRecordingAuth {
        async fn authenticate(
            &self,
            user: &str,
            _: &str,
            _: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            self.observed
                .lock()
                .push(sqe_metrics::propagation::current_trace_id());
            Ok(Session::new(
                user.to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn submit_poll_and_cancel_use_the_remote_trace_id() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tracing_subscriber::layer::SubscriberExt;

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer("trino-propagation-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let auth = Arc::new(TraceRecordingAuth {
            observed: Mutex::new(Vec::new()),
        });
        let state = Arc::new(TrinoState {
            authenticator: auth.clone(),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "test".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: false,
            public_base_url: None,
        });
        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        // A distinct credential per call: this test is about trace
        // propagation into `authenticate`, and the Basic-auth session cache
        // would otherwise (correctly) serve calls 2 and 3 without one.
        let headers = |user: &str| {
            let mut headers = basic_auth_header(user, "pw");
            headers.insert(
                "traceparent",
                format!("00-{trace_id}-b7ad6b7169203331-01")
                    .parse()
                    .unwrap(),
            );
            headers
        };

        let _ = submit_query(
            State(state.clone()),
            test_peer(),
            headers("submitter"),
            "SELECT 1".to_string(),
        )
        .await;
        let _ = get_results(
            State(state.clone()),
            test_peer(),
            headers("poller"),
            Path((
                "query-42".to_string(),
                "no-such-slug".to_string(),
                "bad-page".to_string(),
            )),
        )
        .await;
        let _ = cancel_query(
            State(state),
            test_peer(),
            headers("canceller"),
            Path("query-42".to_string()),
        )
        .await;

        let observed = auth.observed.lock().clone();
        assert_eq!(observed, vec![Some(trace_id.to_string()); 3]);
        provider.shutdown().unwrap();
    }

    /// Captures the SQL strings the executor is asked to run, so a test can
    /// assert what the prepared-statement rewrite produced.
    struct RecordingQuery {
        seen: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for RecordingQuery {
        async fn execute(
            &self,
            _: &Session,
            sql: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            self.seen.lock().push(sql.to_string());
            Ok(TrinoResultStream::from_batches(vec![]))
        }
    }

    fn recording_state(
        seen: Arc<Mutex<Vec<String>>>,
    ) -> Arc<TrinoState<MockAuthOk, RecordingQuery>> {
        Arc::new(TrinoState::<MockAuthOk, RecordingQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(RecordingQuery { seen }),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        })
    }

    /// Executor whose `execute` blocks until `gate` is fired, unless `open` is
    /// already set (fast-path returning immediately). Lets a test force either
    /// the "did not finish in the bounded wait" path or the inline-finish path
    /// deterministically. Returns an empty result set.
    struct GatedQuery {
        open: Arc<AtomicBool>,
        gate: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl TrinoQueryExecutor for GatedQuery {
        async fn execute(
            &self,
            _session: &Session,
            _sql: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            if !self.open.load(Ordering::SeqCst) {
                self.gate.notified().await;
            }
            Ok(TrinoResultStream::from_batches(vec![]))
        }
    }

    fn gated_state(
        open: Arc<AtomicBool>,
        gate: Arc<tokio::sync::Notify>,
    ) -> Arc<TrinoState<MockAuthOk, GatedQuery>> {
        Arc::new(TrinoState::<MockAuthOk, GatedQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(GatedQuery { open, gate }),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "test".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: false,
            public_base_url: None,
        })
    }

    #[tokio::test]
    async fn submit_slow_query_returns_started_response_with_next_uri() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate.clone());
        let resp = submit_query(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SELECT 1".to_string(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        // Did not finish in the bounded wait: a nextUri to the queued route, no data.
        assert!(json
            .next_uri
            .as_deref()
            .unwrap()
            .contains("/v1/statement/queued/"));
        assert!(json.data.is_none());
        assert_ne!(json.stats.state, "FINISHED");
        // Release so the background task does not leak.
        gate.notify_waiters();
    }

    #[tokio::test]
    async fn submit_fast_query_never_inlines_data_and_pages_via_next_uri() {
        // open=true: execute returns immediately, so the query finishes within
        // the bounded wait. Even then the POST carries no columns/data (#767:
        // trino-go-client only reads nextUri pages); it points at results page 0.
        let open = Arc::new(AtomicBool::new(true));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        let resp = submit_query(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SELECT 1".to_string(),
        )
        .await;
        let json = body_json(resp).await;
        assert!(json.data.is_none());
        assert!(json.columns.is_none());
        assert_ne!(json.stats.state, "FINISHED");
        let next = json.next_uri.clone().expect("POST always carries nextUri");
        assert!(
            next.contains("/v1/statement/") && !next.contains("/queued/"),
            "{next}"
        );
        // Following it yields the (columnless, empty) result and ends the chain.
        let (id, slug) = split_queued_uri(&next);
        let page = body_json(
            get_results(
                State(state.clone()),
                test_peer(),
                HeaderMap::new(),
                Path((id, slug, "0".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(page.stats.state, "FINISHED");
        assert!(page.next_uri.is_none());
        assert_eq!(page.columns.map(|c| c.len()), Some(0));
    }

    /// Behind a TLS-terminating ingress trusted by CIDR range, the POST and
    /// every page keep the https public base: Trino clients replay `nextUri`
    /// with their bearer token, so an http URI would send it in cleartext.
    #[tokio::test]
    async fn trusted_cidr_proxy_keeps_https_next_uri_through_paging() {
        fn via_ingress(mut headers: HeaderMap) -> HeaderMap {
            headers.insert("host", "sqe.sqe:8080".parse().unwrap());
            headers.insert("x-forwarded-proto", "https".parse().unwrap());
            headers.insert("x-forwarded-host", "sql.example.org".parse().unwrap());
            headers
        }
        let trusted = SecurityConfig {
            trusted_proxies: vec!["127.0.0.0/8".to_string()],
            ..Default::default()
        };

        let mut state = gated_state(
            Arc::new(AtomicBool::new(true)),
            Arc::new(tokio::sync::Notify::new()),
        );
        Arc::get_mut(&mut state).unwrap().security = trusted.clone();
        let resp = submit_query(
            State(state.clone()),
            test_peer(),
            via_ingress(basic_auth_header("alice", "pw")),
            "SELECT 1".to_string(),
        )
        .await;
        let next = body_json(resp).await.next_uri.expect("POST carries nextUri");
        assert!(next.starts_with("https://sql.example.org/v1/statement/"), "{next}");

        // Three one-row pages: each intermediate page points onward over https.
        let mut state = gated_state(
            Arc::new(AtomicBool::new(true)),
            Arc::new(tokio::sync::Notify::new()),
        );
        Arc::get_mut(&mut state).unwrap().security = trusted;
        state.queries.insert(
            "q-https".to_string(),
            finished_handle_with("alice", vec![int_batch(&[1, 2, 3])], 1),
        );
        for token in ["0", "1"] {
            let page = body_json(
                get_results(
                    State(state.clone()),
                    test_peer(),
                    via_ingress(basic_auth_header("alice", "pw")),
                    Path(("q-https".to_string(), "poll-token-alice".to_string(), token.to_string())),
                )
                .await,
            )
            .await;
            let next = page.next_uri.expect("intermediate page has nextUri");
            assert!(next.starts_with("https://sql.example.org/v1/statement/"), "{next}");
            assert!(page.info_uri.unwrap().starts_with("https://sql.example.org/"));
        }
    }

    #[tokio::test]
    async fn statement_router_builds_without_route_conflict() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        // Panics here if the queued route conflicts with the results route.
        let _router = build_statement_router(state);
    }

    /// Owner record for a fixture. The poll token is deterministic so a test
    /// can either thread it through the URL (fast path) or deliberately send
    /// the wrong one (credential fallback).
    fn test_owner(username: &str) -> QueryOwner {
        QueryOwner {
            username: username.to_string(),
            id: username.to_string(),
            poll_token: format!("poll-token-{username}"),
        }
    }

    fn handle_with(status: QueryStatus, owner: &str) -> Arc<QueryHandle> {
        Arc::new(QueryHandle::new(status, test_owner(owner), None))
    }

    /// A `Finished` handle whose cursor pages `batches` at `page_size` rows.
    fn finished_handle_with(
        owner: &str,
        batches: Vec<RecordBatch>,
        page_size: usize,
    ) -> Arc<QueryHandle> {
        let handle = handle_with(QueryStatus::Finished, owner);
        *handle.cursor.try_lock().unwrap() = Some(ResultCursor::from_batches(batches, page_size));
        handle
    }

    /// Single-column `bigint` batch.
    fn int_batch(values: &[i64]) -> RecordBatch {
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    async fn body_json(resp: Response) -> TrinoResponse {
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// What every Trino client does after the POST: follow `nextUri` through
    /// the results route (poll token only, no credentials) and merge the
    /// pages. Returns the terminal page with all rows accumulated.
    async fn follow_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
        state: &Arc<TrinoState<A, Q>>,
        resp: Response,
    ) -> TrinoResponse {
        let mut tr = body_json(resp).await;
        let mut data: Option<Vec<Vec<serde_json::Value>>> = None;
        while let Some(next) = tr.next_uri.take() {
            assert!(
                !next.contains("/queued/"),
                "fast query must skip the queued route: {next}"
            );
            let parts: Vec<&str> = next.rsplitn(4, '/').collect();
            let path = Path((
                parts[2].to_string(),
                parts[1].to_string(),
                parts[0].to_string(),
            ));
            tr = body_json(
                get_results(State(state.clone()), test_peer(), HeaderMap::new(), path).await,
            )
            .await;
            if let Some(rows) = tr.data.take() {
                data.get_or_insert_with(Vec::new).extend(rows);
            }
        }
        tr.data = data;
        tr
    }

    #[tokio::test]
    async fn queued_poll_running_returns_running_with_incremented_token() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        state
            .queries
            .insert("q1".to_string(), handle_with(QueryStatus::Running, "alice"));
        let resp = get_queued_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path((
                "q1".to_string(),
                "poll-token-alice".to_string(),
                "3".to_string(),
            )),
            axum::extract::RawQuery(Some("maxWait=50ms".to_string())),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.stats.state, "RUNNING");
        assert_eq!(
            json.next_uri.as_deref(),
            Some("http://localhost:8080/v1/statement/queued/q1/poll-token-alice/4")
        );
        assert!(json.data.is_none());
    }

    #[tokio::test]
    async fn queued_poll_finished_redirects_to_results_route() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        state.queries.insert(
            "q1".to_string(),
            handle_with(QueryStatus::Finished, "alice"),
        );
        let resp = get_queued_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path((
                "q1".to_string(),
                "poll-token-alice".to_string(),
                "1".to_string(),
            )),
            axum::extract::RawQuery(None),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json.next_uri.as_deref(),
            Some("http://localhost:8080/v1/statement/q1/poll-token-alice/0")
        );
        assert!(json.data.is_none());
    }

    #[tokio::test]
    async fn queued_poll_failed_returns_error_json() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        let err = protocol::TrinoError::user_error("boom", Some("q1"));
        state.queries.insert(
            "q1".to_string(),
            handle_with(QueryStatus::Failed(Box::new(err)), "alice"),
        );
        let resp = get_queued_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path((
                "q1".to_string(),
                "poll-token-alice".to_string(),
                "1".to_string(),
            )),
            axum::extract::RawQuery(None),
        )
        .await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.stats.state, "FAILED");
        assert!(json.error.is_some());
        assert!(json.next_uri.is_none());
    }

    #[tokio::test]
    async fn queued_poll_unknown_id_returns_not_found() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        let resp = get_queued_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path((
                "missing".to_string(),
                "poll-token-alice".to_string(),
                "1".to_string(),
            )),
            axum::extract::RawQuery(None),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn queued_poll_rejects_non_owner() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate);
        state
            .queries
            .insert("q1".to_string(), handle_with(QueryStatus::Running, "bob"));
        let resp = get_queued_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"), // MockAuthOk authenticates as "alice"
            // Wrong slug for this query, so the credential fallback decides.
            Path((
                "q1".to_string(),
                "poll-token-alice".to_string(),
                "1".to_string(),
            )),
            axum::extract::RawQuery(Some("maxWait=10ms".to_string())),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cancel_aborts_running_query_and_marks_cancelled() {
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = gated_state(open, gate.clone());
        let handle = handle_with(QueryStatus::Running, "alice");
        let gate2 = gate.clone();
        let task = tokio::spawn(async move {
            gate2.notified().await; // never released in this test
        });
        *handle.abort.lock() = Some(task.abort_handle());
        state.queries.insert("q1".to_string(), handle.clone());

        let resp = cancel_query(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path("q1".to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(matches!(*handle.status.lock(), QueryStatus::Cancelled));
        // Give the runtime a tick to process the abort.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(task.is_finished());
    }

    /// Executor that panics inside `execute`.
    struct PanickingQuery;

    #[async_trait::async_trait]
    impl TrinoQueryExecutor for PanickingQuery {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            panic!("executor blew up");
        }
    }

    /// #643 acceptance: a panicking executor must surface as a QUERY_FAILED
    /// style error body on the POST / next poll, never as an HTTP 500 from a
    /// poisoned status mutex.
    #[tokio::test]
    async fn panicking_executor_yields_error_body_not_500() {
        let state = state_with(Arc::new(MockAuthOk), Arc::new(PanickingQuery));
        let resp = submit_query(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SELECT 1".to_string(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let id = body.id.clone();
        let handle = state.queries.get(&id).expect("handle registered");
        assert!(handle.status.lock().is_terminal());

        let err = match body.error {
            Some(err) => err,
            None => {
                // Panic landed after the bounded wait: poll once more.
                let poll = get_queued_results(
                    State(state.clone()),
                    test_peer(),
                    basic_auth_header("alice", "pw"),
                    Path((id.clone(), handle.owner.poll_token.clone(), "1".to_string())),
                    axum::extract::RawQuery(None),
                )
                .await;
                assert_eq!(poll.status(), StatusCode::OK);
                body_json(poll).await.error.expect("error body")
            }
        };
        assert_eq!(err.error_type, "INTERNAL_ERROR");
        assert!(err.message.contains("terminated unexpectedly"), "{err:?}");

        // The status endpoint keeps answering 200 afterwards.
        let again = get_queued_results(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path((id, handle.owner.poll_token.clone(), "2".to_string())),
            axum::extract::RawQuery(None),
        )
        .await;
        assert_eq!(again.status(), StatusCode::OK);
    }

    /// A stream that fails after the first page: the failing page fetch
    /// returns the mapped error body and flips the handle to `Failed`.
    #[tokio::test]
    async fn stream_failure_while_paging_marks_query_failed() {
        // Two good batches: the cursor looks one batch ahead when a page fills,
        // so the error is first observed while producing page 1.
        let stream = futures::stream::iter(vec![
            Ok(int_batch(&[1])),
            Ok(int_batch(&[2])),
            Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                "worker went away",
            )),
        ]);
        let handle = handle_with(QueryStatus::Finished, "alice");
        *handle.cursor.try_lock().unwrap() = Some(ResultCursor::new(
            TrinoResultStream::new(int_batch(&[]).schema(), Box::pin(stream)),
            1,
            None,
            None,
            None,
        ));
        let state = state_with(Arc::new(MockAuthOk), Arc::new(MockQuery));
        state.queries.insert("q-stream".to_string(), handle.clone());

        let fetch = |token: &'static str| {
            get_results(
                State(state.clone()),
                test_peer(),
                basic_auth_header("alice", "pw"),
                Path((
                    "q-stream".to_string(),
                    "poll-token-alice".to_string(),
                    token.to_string(),
                )),
            )
        };
        let first = body_json(fetch("0").await).await;
        assert_eq!(first.data.unwrap()[0][0], serde_json::json!(1));

        let failed = fetch("1").await;
        assert_eq!(failed.status(), StatusCode::OK);
        let body = body_json(failed).await;
        assert_eq!(body.stats.state, "FAILED");
        assert!(body.error.unwrap().message.contains("worker went away"));
        assert!(matches!(*handle.status.lock(), QueryStatus::Failed(_)));
        assert!(handle.cursor.try_lock().unwrap().is_none());
    }

    #[test]
    fn like_match_handles_wildcards() {
        assert!(like_match(
            "iceberg.compression_codec",
            "iceberg.compression_codec"
        ));
        assert!(like_match("iceberg.compression_codec", "iceberg.%"));
        assert!(like_match("iceberg.compression_codec", "%codec"));
        assert!(like_match("abc", "a_c"));
        assert!(!like_match("iceberg.x", "hive.%"));
        assert!(!like_match("abc", "a_"));
        assert!(like_match("", "%"));
    }

    #[test]
    fn incoming_session_properties_parses_comma_list() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h"
                .parse()
                .unwrap(),
        );
        let props = incoming_session_properties(&headers);
        assert_eq!(
            props,
            vec![
                ("iceberg.compression_codec".to_string(), "ZSTD".to_string()),
                ("query_max_run_time".to_string(), "1h".to_string()),
            ]
        );
    }

    #[test]
    fn extract_trino_headers_picks_up_compression_codec() {
        // #353: the codec set via SET SESSION is replayed in X-Trino-Session and
        // must land on the parsed headers so it can reach the write path.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h"
                .parse()
                .unwrap(),
        );
        let parsed = extract_trino_headers(&headers);
        assert_eq!(parsed.compression_codec.as_deref(), Some("ZSTD"));

        // Absent -> None (writer falls back to config default).
        let empty = extract_trino_headers(&HeaderMap::new());
        assert_eq!(empty.compression_codec, None);
    }

    #[test]
    fn apply_trino_headers_propagates_compression_codec_to_session() {
        let session = Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        let trino_headers = TrinoClientHeaders {
            catalog: None,
            schema: None,
            user: None,
            source: None,
            compression_codec: Some("SNAPPY".to_string()),
        };
        let session = apply_trino_headers(session, &trino_headers);
        assert_eq!(session.compression_codec.as_deref(), Some("SNAPPY"));
    }

    #[tokio::test]
    async fn submit_set_session_echoes_via_header_without_executing() {
        // SET SESSION must NOT reach the executor (which rejects "SET SESSION
        // ..." as an unsupported utility statement). It returns 200 +
        // x-trino-set-session header the client replays, with updateType set.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SET SESSION iceberg.compression_codec = 'ZSTD'".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().is_empty(),
            "SET SESSION must not be sent to the executor"
        );
        let hdr = resp
            .headers()
            .get("x-trino-set-session")
            .expect("x-trino-set-session header")
            .to_str()
            .unwrap()
            .to_string();
        // The value is the evaluated literal, as Trino emits it, not the
        // quoted source text.
        assert_eq!(hdr, "iceberg.compression_codec=ZSTD");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("SET SESSION"));
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    #[tokio::test]
    async fn submit_show_session_returns_current_properties() {
        // SHOW SESSION surfaces the properties the client carries in
        // X-Trino-Session, filtered by LIKE, without hitting the executor.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let mut headers = basic_auth_header("alice", "pw");
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h"
                .parse()
                .unwrap(),
        );

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            "SHOW SESSION LIKE 'iceberg.compression_codec'".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().is_empty(),
            "SHOW SESSION must not be sent to the executor"
        );
        let tr = follow_results(&state, resp).await;
        // Trino's 5-column shape.
        let cols: Vec<String> = tr
            .columns
            .expect("columns")
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(
            cols,
            vec!["Name", "Value", "Default", "Type", "Description"]
        );
        // Exactly the LIKE-matched property, with its value.
        let data = tr.data.expect("data");
        assert_eq!(data.len(), 1, "LIKE filter keeps one property: {data:?}");
        assert_eq!(data[0][0], serde_json::json!("iceberg.compression_codec"));
        assert_eq!(data[0][1], serde_json::json!("ZSTD"));
    }

    #[tokio::test]
    async fn submit_resolves_prepared_statement_from_header() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let mut headers = basic_auth_header("alice", "pw");
        // q1 = "SELECT * FROM t WHERE a = ?" url-encoded, as a real Trino
        // client carries it back from x-trino-added-prepare.
        headers.insert(
            "x-trino-prepared-statement",
            "q1=SELECT+%2A+FROM+t+WHERE+a+%3D+%3F".parse().unwrap(),
        );

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            headers,
            "EXECUTE q1 USING 5".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock();
        assert_eq!(seen.len(), 1, "executor should run exactly once");
        assert_eq!(seen[0], "SELECT * FROM t WHERE a = 5");
    }

    #[tokio::test]
    async fn submit_prepare_registers_via_header_without_executing() {
        // PREPARE is the JDBC/Metabase connect gate. It must NOT reach the
        // executor (the SQL parser rejects `PREPARE <name> FROM <sql>`); it
        // returns 200 + an x-trino-added-prepare header the client replays.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "PREPARE ps FROM SELECT 1".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().is_empty(),
            "PREPARE must not be sent to the executor"
        );
        let hdr = resp
            .headers()
            .get("x-trino-added-prepare")
            .expect("x-trino-added-prepare header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(hdr.starts_with("ps="), "header was: {hdr}");
        // SQL is URL-encoded (space -> '+'), so the client can replay it.
        assert!(hdr.contains("SELECT"), "header was: {hdr}");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("PREPARE"));
        // Non-null empty columns (omitting it -> JSON null -> driver throws).
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    #[tokio::test]
    async fn submit_deallocate_returns_update_type_without_executing() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        // The client carries the statement it is deallocating.
        let mut headers = basic_auth_header("alice", "pw");
        headers.insert("x-trino-prepared-statement", "ps=SELECT+1".parse().unwrap());
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            headers,
            "DEALLOCATE PREPARE ps".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().is_empty(),
            "DEALLOCATE must not be sent to the executor"
        );
        assert_eq!(
            resp.headers()
                .get("x-trino-deallocated-prepare")
                .and_then(|v| v.to_str().ok()),
            Some("ps")
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("DEALLOCATE"));
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    /// Finding 708: `DEALLOCATE PREPARE` of a name the session does not hold
    /// is Trino's `NOT_FOUND`, not a silent success with a header.
    #[tokio::test]
    async fn submit_deallocate_unknown_name_is_not_found() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "DEALLOCATE PREPARE nobody".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(seen.lock().is_empty());
        assert!(resp.headers().get("x-trino-deallocated-prepare").is_none());
        let tr = body_json(resp).await;
        assert_eq!(tr.stats.state, "FAILED");
        let err = tr.error.expect("error body");
        assert_eq!(err.error_name, "NOT_FOUND");
        assert_eq!(err.error_type, "USER_ERROR");
        assert_eq!(err.message, "Prepared statement not found: nobody");
    }

    /// Finding 708: the PREPARE body is validated at PREPARE time. A malformed
    /// body is a `SYNTAX_ERROR` on the POST and nothing is registered.
    #[tokio::test]
    async fn submit_prepare_with_malformed_body_is_a_syntax_error() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "PREPARE p FROM SELEC 1".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(seen.lock().is_empty());
        assert!(
            resp.headers().get("x-trino-added-prepare").is_none(),
            "a rejected PREPARE must not be echoed to the client"
        );
        let tr = body_json(resp).await;
        assert_eq!(tr.stats.state, "FAILED");
        let err = tr.error.expect("error body");
        assert_eq!(err.error_name, "SYNTAX_ERROR");
        assert!(err.message.contains("SELEC"), "{}", err.message);
    }

    /// Finding 708: `EXECUTE` binds the parsed `USING` expressions through the
    /// shared comment-aware scanner; a `?` inside a comment is not a
    /// placeholder. `EXECUTE IMMEDIATE` carries the template inline.
    #[tokio::test]
    async fn submit_execute_binds_parameters_comment_aware() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let mut headers = basic_auth_header("alice", "pw");
        // q = "SELECT ? AS v /* why? */"
        headers.insert(
            "x-trino-prepared-statement",
            "q=SELECT+%3F+AS+v+%2F%2A+why%3F+%2A%2F".parse().unwrap(),
        );
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            "EXECUTE q USING 1".to_string(),
        )
        .await
        .into_response();
        assert_eq!(follow_results(&state, resp).await.stats.state, "FINISHED");

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "EXECUTE IMMEDIATE 'SELECT ? AS a, ? AS b' USING 42, 'x'".to_string(),
        )
        .await
        .into_response();
        assert_eq!(follow_results(&state, resp).await.stats.state, "FINISHED");

        let seen = seen.lock();
        assert_eq!(
            *seen,
            vec![
                "SELECT 1 AS v /* why? */".to_string(),
                "SELECT 42 AS a, 'x' AS b".to_string(),
            ]
        );
    }

    /// Finding 708: `SET SESSION` accepts only allow-listed properties with a
    /// valid value; anything else is `INVALID_SESSION_PROPERTY` and nothing is
    /// echoed for the client to replay. `RESET SESSION` uses the same list.
    #[tokio::test]
    async fn submit_set_session_rejects_unknown_property_and_bad_value() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        for (sql, needle) in [
            (
                "SET SESSION bogus = 1",
                "Session property 'bogus' does not exist",
            ),
            ("SET SESSION query_max_memory = '1GB'", "query_max_memory"),
            (
                "SET SESSION iceberg.compression_codec = 'gzip'",
                "Invalid value 'gzip'",
            ),
            (
                "RESET SESSION bogus",
                "Session property 'bogus' does not exist",
            ),
        ] {
            let resp = submit_query::<MockAuthOk, RecordingQuery>(
                State(state.clone()),
                test_peer(),
                basic_auth_header("alice", "pw"),
                sql.to_string(),
            )
            .await
            .into_response();
            assert_eq!(resp.status(), StatusCode::OK, "{sql}");
            assert!(resp.headers().get("x-trino-set-session").is_none(), "{sql}");
            assert!(
                resp.headers().get("x-trino-clear-session").is_none(),
                "{sql}"
            );
            let tr = body_json(resp).await;
            assert_eq!(tr.stats.state, "FAILED", "{sql}");
            let err = tr.error.expect("error body");
            assert_eq!(err.error_name, "INVALID_SESSION_PROPERTY", "{sql}");
            assert_eq!(err.error_code, 14, "{sql}");
            assert_eq!(err.error_type, "USER_ERROR", "{sql}");
            assert!(err.message.contains(needle), "{sql}: {}", err.message);
        }
        assert!(seen.lock().is_empty(), "session statements never execute");

        // A known property resets with the clear header, on this terminal
        // response.
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "RESET SESSION iceberg.compression_codec".to_string(),
        )
        .await
        .into_response();
        assert_eq!(
            resp.headers()
                .get("x-trino-clear-session")
                .and_then(|v| v.to_str().ok()),
            Some("iceberg.compression_codec")
        );
        let tr = body_json(resp).await;
        assert_eq!(tr.update_type.as_deref(), Some("RESET SESSION"));
    }

    /// `SHOW SESSION` lists the allow-list: every property SQE honours, with
    /// the value the client is carrying when it set one, else the default.
    #[tokio::test]
    async fn submit_show_session_lists_the_allow_list() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SHOW SESSION".to_string(),
        )
        .await
        .into_response();
        let tr = follow_results(&state, resp).await;
        let data = tr.data.expect("data");
        assert_eq!(data.len(), session_props::SESSION_PROPERTIES.len());
        assert_eq!(data[0][0], serde_json::json!("iceberg.compression_codec"));
        assert_eq!(data[0][1], serde_json::Value::Null, "unset: default (NULL)");
        assert_eq!(data[0][3], serde_json::json!("varchar"));

        // A property the client replays but SQE does not honour is not listed.
        let mut headers = basic_auth_header("alice", "pw");
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=LZ4, query_max_run_time=1h"
                .parse()
                .unwrap(),
        );
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            "SHOW SESSION LIKE '%'".to_string(),
        )
        .await
        .into_response();
        let data = follow_results(&state, resp).await.data.expect("data");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0][1], serde_json::json!("LZ4"));
        assert!(seen.lock().is_empty());
    }

    /// Finding 708: `SET CATALOG` and `USE` update the session through
    /// headers; `USE` is recognised behind a leading comment (prefix matching
    /// used to miss it) and gets its headers on its terminal response.
    #[tokio::test]
    async fn submit_use_and_set_catalog_update_the_session() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SET CATALOG lake".to_string(),
        )
        .await
        .into_response();
        assert_eq!(
            resp.headers()
                .get("x-trino-set-catalog")
                .and_then(|v| v.to_str().ok()),
            Some("lake")
        );
        assert!(seen.lock().is_empty(), "SET CATALOG never executes");
        assert_eq!(body_json(resp).await.update_type.as_deref(), Some("USE"));

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "-- switch\nUSE \"My Lake\".gold".to_string(),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("x-trino-set-catalog")
                .and_then(|v| v.to_str().ok()),
            Some("My Lake")
        );
        assert_eq!(
            resp.headers()
                .get("x-trino-set-schema")
                .and_then(|v| v.to_str().ok()),
            Some("gold")
        );
        let tr = follow_results(&state, resp).await;
        assert_eq!(tr.stats.state, "FINISHED");
        assert_eq!(tr.update_type.as_deref(), Some("USE"));
        assert_eq!(seen.lock().len(), 1, "USE reaches the executor once");
    }

    /// Executor that fails after the POST's bounded wait has elapsed, so the
    /// failure is only visible to a later poll.
    struct LateFailingQuery;
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for LateFailingQuery {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            tokio::time::sleep(DEFAULT_MAX_WAIT + std::time::Duration::from_millis(300)).await;
            Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::SchemaNotFound,
                "Schema 'missing' does not exist",
            ))
        }
    }

    /// Finding 708 acceptance: a `USE` that fails after the bounded wait never
    /// emits `X-Trino-Set-Schema`, neither on the started response nor on the
    /// terminal poll.
    #[tokio::test]
    async fn late_failing_use_never_switches_the_client() {
        let state = state_with(Arc::new(MockAuthOk), Arc::new(LateFailingQuery));

        let resp = submit_query::<MockAuthOk, LateFailingQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "USE missing".to_string(),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers().get("x-trino-set-schema").is_none(),
            "the started response must not carry session headers"
        );
        let started = body_json(resp).await;
        let next = started.next_uri.expect("started response has nextUri");
        assert!(next.contains("/v1/statement/queued/"), "{next}");
        let (id, slug) = split_queued_uri(&next);

        // Poll until terminal (the poll itself waits up to maxWait).
        let mut terminal = None;
        for _ in 0..10 {
            let resp = get_queued_results::<MockAuthOk, LateFailingQuery>(
                State(state.clone()),
                test_peer(),
                HeaderMap::new(),
                Path((id.clone(), slug.clone(), "1".to_string())),
                axum::extract::RawQuery(Some("maxWait=1s".to_string())),
            )
            .await;
            let has_schema_header = resp.headers().get("x-trino-set-schema").is_some();
            let tr = body_json(resp).await;
            assert!(!has_schema_header, "poll leaked X-Trino-Set-Schema");
            if tr.stats.state == "FAILED" {
                terminal = Some(tr);
                break;
            }
        }
        let failed = terminal.expect("USE failed within the polling budget");
        assert_eq!(
            failed.error.map(|e| e.error_name),
            Some("SCHEMA_NOT_FOUND".to_string())
        );
    }

    /// Finding 704 acceptance: `updateType` comes from the parsed statement.
    /// `CREATE VIEW` is `CREATE VIEW`, not `CREATE TABLE`.
    #[tokio::test]
    async fn submit_reports_update_type_from_the_ast() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        for (sql, expected) in [
            ("CREATE VIEW v AS SELECT 1", Some("CREATE VIEW")),
            ("CREATE SCHEMA s", Some("CREATE SCHEMA")),
            ("INSERT INTO t VALUES 1", Some("INSERT")),
            ("DROP VIEW v", Some("DROP VIEW")),
            ("SELECT 1 AS one", None),
        ] {
            let resp = submit_query::<MockAuthOk, RecordingQuery>(
                State(state.clone()),
                test_peer(),
                basic_auth_header("alice", "pw"),
                sql.to_string(),
            )
            .await
            .into_response();
            let tr = follow_results(&state, resp).await;
            assert_eq!(tr.stats.state, "FINISHED", "{sql}");
            assert_eq!(tr.update_type.as_deref(), expected, "{sql}");
        }
        // The paren-less VALUES reached the executor normalized.
        assert!(
            seen.lock().iter().any(|s| s == "INSERT INTO t VALUES (1)"),
            "{:?}",
            seen.lock()
        );
    }

    /// Finding 704 acceptance: a user table with `table_catalog` and
    /// `data_type` columns, queried with `information_schema` in a literal and
    /// in a comment, keeps every row and its original `data_type` values.
    #[tokio::test]
    async fn submit_user_query_mentioning_information_schema_is_not_reshaped() {
        struct AuditQuery;
        #[async_trait::async_trait]
        impl TrinoQueryExecutor for AuditQuery {
            async fn execute(
                &self,
                _: &Session,
                _: &str,
            ) -> Result<TrinoResultStream, sqe_core::SqeError> {
                use arrow_array::StringArray;
                use arrow_schema::{DataType, Field, Schema};
                let schema = Arc::new(Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                ]));
                let batch = arrow_array::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["iceberg", "system"])),
                        Arc::new(StringArray::from(vec!["Int64", "Utf8"])),
                    ],
                )
                .unwrap();
                Ok(TrinoResultStream::from_batches(vec![batch]))
            }
        }
        let state = state_with(Arc::new(MockAuthOk), Arc::new(AuditQuery));

        for sql in [
            "SELECT table_catalog, data_type FROM audit.events WHERE note LIKE '%information_schema%'",
            "SELECT table_catalog, data_type FROM audit.events -- information_schema.tables",
        ] {
            let mut headers = basic_auth_header("alice", "pw");
            headers.insert("x-trino-catalog", "iceberg".parse().unwrap());
            let resp = submit_query::<MockAuthOk, AuditQuery>(
                State(state.clone()),
                test_peer(),
                headers,
                sql.to_string(),
            )
            .await
            .into_response();
            let tr = follow_results(&state, resp).await;
            let data = tr.data.expect("data rows");
            assert_eq!(data.len(), 2, "{sql}: no row may be scoped out");
            assert_eq!(data[1][0], serde_json::json!("system"), "{sql}");
            assert_eq!(data[0][1], serde_json::json!("Int64"), "{sql}: data_type verbatim");
        }
    }

    #[tokio::test]
    async fn submit_metadata_query_translates_types_and_scopes_catalog() {
        // Executor returns an information_schema.columns-shaped batch with Arrow
        // type names and a foreign (system) catalog row -- exactly what the
        // built-in information_schema produces.
        struct InfoSchemaQuery;
        #[async_trait::async_trait]
        impl TrinoQueryExecutor for InfoSchemaQuery {
            async fn execute(
                &self,
                _: &Session,
                _: &str,
            ) -> Result<TrinoResultStream, sqe_core::SqeError> {
                use arrow_array::StringArray;
                use arrow_schema::{DataType, Field, Schema};
                let schema = Arc::new(Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, false),
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                ]));
                let batch = arrow_array::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["iceberg", "system"])),
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(StringArray::from(vec!["Int64", "Utf8"])),
                    ],
                )
                .unwrap();
                Ok(TrinoResultStream::from_batches(vec![batch]))
            }
        }

        let state = Arc::new(TrinoState::<MockAuthOk, InfoSchemaQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(InfoSchemaQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let mut headers = basic_auth_header("alice", "pw");
        headers.insert("x-trino-catalog", "iceberg".parse().unwrap());

        let resp = submit_query::<MockAuthOk, InfoSchemaQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            "SELECT table_catalog, column_name, data_type FROM information_schema.columns"
                .to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let tr = follow_results(&state, resp).await;
        let data = tr.data.expect("data rows");

        // The `system` row is scoped out (session catalog = iceberg), and the
        // Arrow `Int64` is translated to Trino `bigint`.
        assert_eq!(data.len(), 1, "only the iceberg-catalog row should remain");
        assert_eq!(data[0][0], serde_json::json!("iceberg"));
        assert_eq!(data[0][2], serde_json::json!("bigint"));
    }

    #[tokio::test]
    async fn submit_unresolved_execute_never_reaches_executor() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        // EXECUTE naming a statement the client never sent: must error out
        // before execution, so the executor is never called.
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "EXECUTE missing USING 1".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().is_empty(),
            "unresolved EXECUTE must not reach the executor"
        );
    }

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer eyJhbGciOi.payload.sig".parse().unwrap(),
        );
        let token = extract_bearer_token(&headers).unwrap();
        assert_eq!(token, "eyJhbGciOi.payload.sig");
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let headers = HeaderMap::new();
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_empty() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer ".parse().unwrap());
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_basic_auth_ignored() {
        let mut headers = HeaderMap::new();
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"user:pass");
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth() {
        let mut headers = HeaderMap::new();
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"root:root123");
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());

        let (user, pass) = extract_basic_auth(&headers).unwrap();
        assert_eq!(user, "root");
        assert_eq!(pass, "root123");
    }

    #[test]
    fn test_extract_basic_auth_missing() {
        let headers = HeaderMap::new();
        assert!(extract_basic_auth(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token123".parse().unwrap());
        assert!(extract_basic_auth(&headers).is_none());
    }

    // ── Pagination unit tests ─────────────────────────────────

    #[tokio::test]
    async fn cursor_pages_rows_in_fixed_sizes() {
        let rows: Vec<i64> = (0..25).collect();
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&rows)], 10);
        assert_eq!(page(&mut cursor, 0).await.rows.len(), 10);
        assert_eq!(page(&mut cursor, 1).await.rows.len(), 10);
        let last = page(&mut cursor, 2).await;
        assert_eq!(last.rows.len(), 5);
        assert!(last.is_last);
    }

    #[tokio::test]
    async fn cursor_empty_result_is_a_single_last_page() {
        let mut cursor = ResultCursor::from_batches(vec![], 10);
        let only = page(&mut cursor, 0).await;
        assert!(only.rows.is_empty());
        assert!(only.is_last);
    }

    #[test]
    fn test_next_uri_has_next() {
        let uri = next_uri("http://localhost:8080", "q-123", "slug1", 0, false);
        assert_eq!(
            uri,
            Some("http://localhost:8080/v1/statement/q-123/slug1/1".to_string())
        );
    }

    #[test]
    fn test_next_uri_last_page() {
        let uri = next_uri("http://localhost:8080", "q-123", "slug1", 2, true);
        assert!(uri.is_none());
    }

    #[test]
    fn clamp_max_wait_parses_and_clamps() {
        use std::time::Duration;
        assert_eq!(clamp_max_wait(Some("500ms")), Duration::from_millis(500));
        assert_eq!(clamp_max_wait(Some("2s")), Duration::from_secs(2));
        assert_eq!(clamp_max_wait(Some("1m")), MAX_WAIT_CAP); // 60s clamped to cap
        assert_eq!(clamp_max_wait(Some("garbage")), DEFAULT_MAX_WAIT);
        assert_eq!(clamp_max_wait(None), DEFAULT_MAX_WAIT);
    }

    #[tokio::test]
    async fn await_terminal_returns_when_finished() {
        let handle = Arc::new(QueryHandle::new(
            QueryStatus::Running,
            test_owner("u"),
            None,
        ));
        let h2 = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            *h2.status.lock() = QueryStatus::Finished;
            h2.notify.notify_waiters();
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            await_terminal_or_timeout(&handle, std::time::Duration::from_secs(5)),
        )
        .await
        .expect("returned before outer timeout");
        assert!(handle.status.lock().is_terminal());
    }

    #[tokio::test]
    async fn await_terminal_returns_on_timeout_when_still_running() {
        let handle = Arc::new(QueryHandle::new(
            QueryStatus::Running,
            test_owner("u"),
            None,
        ));
        await_terminal_or_timeout(&handle, std::time::Duration::from_millis(30)).await;
        assert!(!handle.status.lock().is_terminal());
    }

    #[test]
    fn started_response_points_at_queued_route_without_data() {
        let resp = build_started_response("http://h:8080", "q1", "slug1");
        assert_eq!(resp.stats.state, "QUEUED");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/queued/q1/slug1/1")
        );
        assert!(resp.data.is_none());
        assert!(resp.columns.is_none());
    }

    #[test]
    fn running_response_increments_queued_token() {
        let resp = build_running_response("http://h:8080", "q1", "slug1", 4);
        assert_eq!(resp.stats.state, "RUNNING");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/queued/q1/slug1/4")
        );
        assert!(resp.data.is_none());
    }

    #[test]
    fn finished_redirect_response_points_at_results_route() {
        let resp = build_finished_redirect_response("http://h:8080", "q1", "slug1");
        assert_eq!(resp.stats.state, "RUNNING");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/q1/slug1/0")
        );
        assert!(resp.data.is_none());
    }

    async fn page(cursor: &mut ResultCursor, token: usize) -> Arc<ReadyPage> {
        match cursor.fetch(token).await.unwrap() {
            PageFetch::Page(p) => p,
            other => panic!("expected a page for token {token}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_build_page_response_first_page() {
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&[1, 2, 3])], 2);
        let p0 = page(&mut cursor, 0).await;

        let resp = build_page_response(
            "http://localhost:8080",
            "q-abc",
            "poll-token-test",
            &cursor,
            &p0,
        );
        assert_eq!(resp.id, "q-abc");
        assert_eq!(
            resp.info_uri,
            Some("http://localhost:8080/v1/query/q-abc".to_string())
        );
        assert_eq!(
            resp.next_uri,
            Some("http://localhost:8080/v1/statement/q-abc/poll-token-test/1".to_string())
        );
        assert_eq!(resp.data.as_ref().unwrap().len(), 2);
        assert_eq!(resp.stats.state, "RUNNING");
        assert_eq!(resp.stats.processed_rows, 2);
    }

    #[tokio::test]
    async fn test_build_page_response_last_page() {
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&[1, 2, 3])], 2);
        let _ = page(&mut cursor, 0).await;
        let p1 = page(&mut cursor, 1).await;

        let resp = build_page_response(
            "http://localhost:8080",
            "q-abc",
            "poll-token-test",
            &cursor,
            &p1,
        );
        assert!(resp.next_uri.is_none());
        assert_eq!(resp.data.as_ref().unwrap().len(), 1);
        assert_eq!(resp.data.as_ref().unwrap()[0][0], serde_json::json!(3));
        assert_eq!(resp.stats.state, "FINISHED");
        assert_eq!(resp.stats.processed_rows, 3);
    }

    #[tokio::test]
    async fn test_build_page_response_omits_data_for_columnless_update() {
        // Trino 465 JDBC client rejects a non-null `data` with empty `columns`
        // ("Columns must be set when decoding data"). Real Trino omits `data`
        // entirely for DDL/update statements. SQE must match: column-less result
        // -> data: None. See issue #314.
        let mut cursor = ResultCursor::new(
            TrinoResultStream::from_batches(vec![]),
            DEFAULT_PAGE_SIZE,
            None,
            Some("CREATE TABLE".to_string()),
            None,
        );
        let p0 = page(&mut cursor, 0).await;

        let resp = build_page_response(
            "http://localhost:8080",
            "q-ddl",
            "poll-token-test",
            &cursor,
            &p0,
        );
        assert!(
            resp.data.is_none(),
            "column-less update must omit data (got {:?})",
            resp.data
        );
        assert_eq!(resp.update_type, Some("CREATE TABLE".to_string()));
        assert_eq!(resp.stats.state, "FINISHED");

        // Wire-level check: `data` is skip_serializing_if = Option::is_none, so
        // a None value drops the field entirely. This is exactly what the Trino
        // 465 client's `data == null -> NULL_ROWS` early return needs to see.
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            !obj.contains_key("data"),
            "serialized response must omit the data field for a column-less update, got {json}"
        );
        assert_eq!(
            obj.get("updateType").and_then(|v| v.as_str()),
            Some("CREATE TABLE")
        );
    }

    #[tokio::test]
    async fn test_build_page_response_keeps_data_for_empty_select() {
        // A SELECT that legitimately returns zero rows still has columns, so
        // `data` must remain present (Some) and non-null. Gating is on empty
        // columns, not empty data.
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&[])], DEFAULT_PAGE_SIZE);
        let p0 = page(&mut cursor, 0).await;

        let resp = build_page_response(
            "http://localhost:8080",
            "q-empty",
            "poll-token-test",
            &cursor,
            &p0,
        );
        assert!(
            resp.data.is_some(),
            "SELECT with columns must keep data present even when empty"
        );
        assert!(resp.data.as_ref().unwrap().is_empty());
        assert_eq!(resp.stats.state, "FINISHED");
    }

    #[tokio::test]
    async fn test_build_page_response_single_page() {
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&[42])], DEFAULT_PAGE_SIZE);
        let p0 = page(&mut cursor, 0).await;

        let resp = build_page_response(
            "http://localhost:8080",
            "q-single",
            "poll-token-test",
            &cursor,
            &p0,
        );
        assert!(resp.next_uri.is_none());
        assert_eq!(resp.stats.state, "FINISHED");
    }

    /// Trino clients retry the `nextUri` they last received after a transport
    /// error; the page just served must come back identical, and only that one.
    #[tokio::test]
    async fn cursor_replays_the_last_served_page_once_and_rejects_skips() {
        let mut cursor = ResultCursor::from_batches(vec![int_batch(&[1, 2, 3, 4])], 2);
        assert!(matches!(
            cursor.fetch(1).await.unwrap(),
            PageFetch::OutOfRange { expected: 0 }
        ));
        let first = page(&mut cursor, 0).await;
        let again = page(&mut cursor, 0).await;
        assert!(
            Arc::ptr_eq(&first, &again),
            "retry must replay the buffered page"
        );
        let second = page(&mut cursor, 1).await;
        assert_eq!(second.rows[0][0], serde_json::json!(3));
        // Page 0 is gone once page 1 was produced.
        assert!(matches!(
            cursor.fetch(0).await.unwrap(),
            PageFetch::OutOfRange { expected: 2 }
        ));
        // Exactly page_size rows drained the stream: the look-ahead reports
        // this page as the last one, no empty trailing page.
        assert!(second.is_last);
        assert!(matches!(
            cursor.fetch(2).await.unwrap(),
            PageFetch::OutOfRange { .. }
        ));
    }

    /// Batches larger than a page are sliced; the remainder carries into the
    /// next page so rows are never dropped or duplicated across batches.
    #[tokio::test]
    async fn cursor_slices_across_batch_boundaries() {
        let mut cursor = ResultCursor::from_batches(
            vec![
                int_batch(&[1, 2, 3]),
                int_batch(&[4]),
                int_batch(&[5, 6, 7]),
            ],
            2,
        );
        let mut seen = Vec::new();
        let mut token = 0;
        loop {
            let p = page(&mut cursor, token).await;
            seen.extend(p.rows.iter().map(|r| r[0].as_i64().unwrap()));
            if p.is_last {
                break;
            }
            token += 1;
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    /// A stream error mid-way surfaces from `fetch` and closes the cursor.
    #[tokio::test]
    async fn cursor_surfaces_stream_error_and_closes() {
        let stream = futures::stream::iter(vec![
            Ok(int_batch(&[1])),
            Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                "worker went away",
            )),
        ]);
        let result = TrinoResultStream::new(int_batch(&[]).schema(), Box::pin(stream));
        let mut cursor = ResultCursor::new(result, 10, None, None, None);
        let err = cursor.fetch(0).await.unwrap_err();
        assert!(err.to_string().contains("worker went away"));
        assert!(
            cursor.stream.is_none(),
            "cursor must release the stream on error"
        );
    }

    /// The buffered page is charged to the DataFusion pool: a pool too small
    /// for one page rejects the fetch with ResourceExhausted instead of
    /// growing outside the budget.
    #[tokio::test]
    async fn cursor_page_is_charged_to_memory_pool() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let reservation = MemoryConsumer::new("test-cursor").register(&pool);
        let rows: Vec<i64> = (0..100).collect();
        let mut cursor = ResultCursor::new(
            TrinoResultStream::from_batches(vec![int_batch(&rows)]),
            DEFAULT_PAGE_SIZE,
            Some(reservation),
            None,
            None,
        );
        let err = cursor.fetch(0).await.unwrap_err();
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::ResourceExhausted);

        // A pool with room accounts for the page and releases it on drop.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let reservation = MemoryConsumer::new("test-cursor").register(&pool);
        let mut cursor = ResultCursor::new(
            TrinoResultStream::from_batches(vec![int_batch(&rows)]),
            DEFAULT_PAGE_SIZE,
            Some(reservation),
            None,
            None,
        );
        let _ = page(&mut cursor, 0).await;
        assert!(
            pool.reserved() > 0,
            "page bytes must be visible in the pool"
        );
        drop(cursor);
        assert_eq!(pool.reserved(), 0);
    }

    // ── Header extraction unit tests ──────────────────────────

    #[test]
    fn test_extract_trino_headers_all_present() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "iceberg".parse().unwrap());
        headers.insert("x-trino-schema", "public".parse().unwrap());
        headers.insert("x-trino-user", "alice".parse().unwrap());
        headers.insert("x-trino-source", "trino-cli".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert_eq!(trino.catalog.as_deref(), Some("iceberg"));
        assert_eq!(trino.schema.as_deref(), Some("public"));
        assert_eq!(trino.user.as_deref(), Some("alice"));
        assert_eq!(trino.source.as_deref(), Some("trino-cli"));
    }

    #[test]
    fn test_extract_trino_headers_none_present() {
        let headers = HeaderMap::new();
        let trino = extract_trino_headers(&headers);
        assert!(trino.catalog.is_none());
        assert!(trino.schema.is_none());
        assert!(trino.user.is_none());
        assert!(trino.source.is_none());
    }

    #[test]
    fn test_extract_trino_headers_partial() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "hive".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert_eq!(trino.catalog.as_deref(), Some("hive"));
        assert!(trino.schema.is_none());
        assert!(trino.user.is_none());
        assert!(trino.source.is_none());
    }

    #[test]
    fn test_extract_trino_headers_empty_values_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "".parse().unwrap());
        headers.insert("x-trino-schema", "  ".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert!(trino.catalog.is_none());
        assert!(trino.schema.is_none());
    }

    #[test]
    fn test_apply_trino_headers_to_session() {
        let session = Session::new(
            "testuser".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );

        let trino_headers = TrinoClientHeaders {
            catalog: Some("iceberg".to_string()),
            schema: Some("analytics".to_string()),
            user: Some("testuser".to_string()),
            source: Some("trino-jdbc".to_string()),
            compression_codec: None,
        };

        let session = apply_trino_headers(session, &trino_headers);
        assert_eq!(session.default_catalog.as_deref(), Some("iceberg"));
        assert_eq!(session.default_schema.as_deref(), Some("analytics"));
        assert_eq!(session.source.as_deref(), Some("trino-jdbc"));
    }

    #[test]
    fn test_apply_trino_headers_none_values() {
        let session = Session::new(
            "testuser".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );

        let trino_headers = TrinoClientHeaders::default();

        let session = apply_trino_headers(session, &trino_headers);
        assert!(session.default_catalog.is_none());
        assert!(session.default_schema.is_none());
        assert!(session.source.is_none());
    }

    // ── Integration-style tests for pagination flow ───────────

    #[test]
    fn query_registry_stores_and_reports_terminal_status() {
        let registry = build_query_registry(build_expired_ids());
        let handle = Arc::new(QueryHandle::new(
            QueryStatus::Running,
            test_owner("alice"),
            None,
        ));
        registry.insert("q1".to_string(), handle.clone());

        let got = registry.get("q1").expect("handle present");
        assert!(!got.status.lock().is_terminal());

        *got.status.lock() = QueryStatus::Finished;
        assert!(got.status.lock().is_terminal());
    }

    /// Idle eviction of a finished query leaves a tombstone so a late page
    /// fetch is told the result expired, not that the id never existed (#621).
    #[tokio::test]
    async fn expired_query_is_reported_as_expired_not_unknown() {
        let state = state_with(Arc::new(MockAuthOk), Arc::new(MockQuery));
        state.expired.insert("q-old".to_string(), ());

        let expired = get_results(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path(("q-old".to_string(), "x".to_string(), "1".to_string())),
        )
        .await;
        assert_eq!(expired.status(), StatusCode::NOT_FOUND);
        let body = body_json(expired).await;
        assert!(
            body.error.as_ref().unwrap().message.contains("expired"),
            "got {:?}",
            body.error
        );

        let unknown = get_results(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            Path(("q-never".to_string(), "x".to_string(), "1".to_string())),
        )
        .await;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let body = body_json(unknown).await;
        assert_eq!(body.error.as_ref().unwrap().message, "Query not found");
    }

    /// Eviction for idleness (not an explicit cancel) records the tombstone.
    #[test]
    fn registry_eviction_records_expired_id() {
        let expired = build_expired_ids();
        let registry: MokaCache<String, Arc<QueryHandle>> = MokaCache::builder()
            .max_capacity(1)
            .eviction_listener({
                let expired = expired.clone();
                move |key: Arc<String>, _handle: Arc<QueryHandle>, cause| {
                    if matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                        expired.insert(key.as_str().to_string(), ());
                    }
                }
            })
            .build();
        registry.insert("q1".to_string(), handle_with(QueryStatus::Finished, "a"));
        registry.insert("q2".to_string(), handle_with(QueryStatus::Finished, "a"));
        registry.run_pending_tasks();
        assert!(
            expired.contains_key("q1") || expired.contains_key("q2"),
            "size eviction must leave a tombstone"
        );
    }

    #[tokio::test]
    async fn test_get_results_invalid_token() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        // Authenticate so we exercise the page-token parse branch.
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path((
                "q-123".to_string(),
                "poll-token-test".to_string(),
                "not-a-number".to_string(),
            )),
        )
        .await;

        assert_eq!(response.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_results_out_of_range_token() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        state
            .queries
            .insert("q-456".to_string(), finished_handle_with("test", vec![], 1));

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            HeaderMap::new(),
            Path((
                "q-456".to_string(),
                "wrong-slug".to_string(),
                "5".to_string(),
            )),
        )
        .await;

        // Missing Authorization header now rejects with 401 before the
        // out-of-range branch runs. The 401 covers issue #34.
        assert_eq!(response.into_response().status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_get_results_returns_correct_page() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        state.queries.insert(
            "q-paged".to_string(),
            finished_handle_with("test", vec![int_batch(&[10, 20, 30])], 1),
        );

        // Pages are served in order: pull page 0 first.
        let first = get_results::<MockAuthOk, MockQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path((
                "q-paged".to_string(),
                "poll-token-test".to_string(),
                "0".to_string(),
            )),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            body_json(first).await.data.unwrap()[0][0],
            serde_json::json!(10)
        );

        // Fetch page 1 (middle); authenticate as the result owner.
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path((
                "q-paged".to_string(),
                "poll-token-test".to_string(),
                "1".to_string(),
            )),
        )
        .await;

        let resp = response.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let trino_resp: TrinoResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(trino_resp.data.as_ref().unwrap().len(), 1);
        assert_eq!(
            trino_resp.data.as_ref().unwrap()[0][0],
            serde_json::json!(20)
        );
        assert_eq!(
            trino_resp.next_uri,
            Some("http://localhost:8080/v1/statement/q-paged/poll-token-test/2".to_string())
        );
        assert_eq!(trino_resp.stats.state, "RUNNING");
        assert!(trino_resp.info_uri.is_some(), "infoUri must be present");

        // Result should still exist (not the last page)
        assert!(state.queries.get("q-paged").is_some());
    }

    #[tokio::test]
    async fn test_get_results_last_page_cleans_up() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        state.queries.insert(
            "q-cleanup".to_string(),
            finished_handle_with("test", vec![int_batch(&[1, 2])], 1),
        );

        let fetch = |token: &'static str| {
            get_results::<MockAuthOk, MockQuery>(
                State(state.clone()),
                test_peer(),
                basic_auth_header("test", "pw"),
                Path((
                    "q-cleanup".to_string(),
                    "poll-token-test".to_string(),
                    token.to_string(),
                )),
            )
        };
        let _ = fetch("0").await;
        // Fetch the last page (token = 1) twice: the eager invalidation that
        // used to turn a client's retry into "Query not found" is gone (#621).
        let last = body_json(fetch("1").await).await;
        assert_eq!(last.stats.state, "FINISHED");
        assert!(last.next_uri.is_none());
        let retry = body_json(fetch("1").await).await;
        assert_eq!(retry.stats.state, "FINISHED");
        assert_eq!(retry.data, last.data);

        // The handle stays until idle eviction; a skip past the end is rejected.
        assert!(state.queries.get("q-cleanup").is_some());
        let beyond = fetch("2").await;
        assert_eq!(beyond.status(), StatusCode::NOT_FOUND);
        assert!(body_json(beyond)
            .await
            .error
            .unwrap()
            .message
            .contains("out of range"));
    }

    /// Regression for #34: an authenticated caller who is not the
    /// owner cannot pull the result set.
    #[tokio::test]
    async fn test_get_results_rejects_non_owner() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });
        state.queries.insert(
            "q-secret".to_string(),
            finished_handle_with("alice", vec![], 1),
        );
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state),
            test_peer(),
            basic_auth_header("mallory", "pw"),
            // Mallory does not hold alice's poll token, so the ownership
            // check on the credential fallback is what decides.
            Path((
                "q-secret".to_string(),
                "not-alices-token".to_string(),
                "0".to_string(),
            )),
        )
        .await;
        assert_eq!(response.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_cancel_query_removes_result() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        state.queries.insert(
            "q-cancel".to_string(),
            finished_handle_with("test-user", vec![], 1),
        );

        // Build Basic auth header for "test-user:pass"
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"test-user:pass",
        );
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());

        let response = cancel_query::<MockAuth, MockQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            Path("q-cancel".to_string()),
        )
        .await;

        // MockAuth rejects all credentials, so we expect UNAUTHORIZED
        assert_eq!(response.into_response().status(), StatusCode::UNAUTHORIZED);
    }

    // ── Base URL extraction tests ────────────────────────────────

    #[test]
    fn test_extract_base_url_from_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "myhost:9090".parse().unwrap());
        assert_eq!(
            extract_base_url(&headers, 8080, false, None),
            "http://myhost:9090"
        );
    }

    #[test]
    fn test_extract_base_url_fallback() {
        let headers = HeaderMap::new();
        assert_eq!(
            extract_base_url(&headers, 8080, false, None),
            "http://localhost:8080"
        );
    }

    #[test]
    fn test_extract_base_url_with_forwarded_proto() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "myhost:9090".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "public.example".parse().unwrap());
        assert_eq!(
            extract_base_url(&headers, 8080, true, None),
            "https://public.example"
        );
    }

    /// #726: forwarded headers from an untrusted peer must not steer nextUri.
    #[test]
    fn test_extract_base_url_ignores_forwarded_headers_from_untrusted_peer() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "myhost:9090".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "evil.example".parse().unwrap());
        assert_eq!(
            extract_base_url(&headers, 8080, false, None),
            "http://myhost:9090"
        );
    }

    fn proxied(host: &str, proto: Option<&str>, fwd_host: Option<&str>, fwd_port: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        if let Some(v) = proto {
            headers.insert("x-forwarded-proto", v.parse().unwrap());
        }
        if let Some(v) = fwd_host {
            headers.insert("x-forwarded-host", v.parse().unwrap());
        }
        if let Some(v) = fwd_port {
            headers.insert("x-forwarded-port", v.parse().unwrap());
        }
        headers
    }

    /// A trusted ingress that terminated TLS keeps https in nextUri, with and
    /// without a port on the forwarded host; a list keeps the first hop.
    #[test]
    fn trusted_forwarded_https_is_never_downgraded() {
        let h = proxied("sqe.sqe:8080", Some("https"), Some("sql.example.org"), None);
        assert_eq!(extract_base_url(&h, 8080, true, None), "https://sql.example.org");
        let h = proxied("sqe.sqe:8080", Some("https, http"), Some("sql.example.org, sqe"), None);
        assert_eq!(extract_base_url(&h, 8080, true, None), "https://sql.example.org");
        // An unknown scheme is ignored rather than echoed into the URI.
        let h = proxied("sqe.sqe:8080", Some("javascript"), Some("sql.example.org"), None);
        assert_eq!(extract_base_url(&h, 8080, true, None), "http://sql.example.org");
    }

    #[test]
    fn trusted_forwarded_port_is_applied_when_not_default() {
        let h = proxied("sqe:8080", Some("https"), Some("sql.example.org"), Some("8443"));
        assert_eq!(extract_base_url(&h, 8080, true, None), "https://sql.example.org:8443");
        let h = proxied("sqe:8080", Some("https"), Some("sql.example.org"), Some("443"));
        assert_eq!(extract_base_url(&h, 8080, true, None), "https://sql.example.org");
        // A forwarded host that already names a port keeps it.
        let h = proxied("sqe:8080", Some("https"), Some("sql.example.org:9443"), Some("8443"));
        assert_eq!(extract_base_url(&h, 8080, true, None), "https://sql.example.org:9443");
        // Untrusted peers cannot set the port either.
        let h = proxied("sqe:8080", Some("https"), Some("x"), Some("8443"));
        assert_eq!(extract_base_url(&h, 8080, false, None), "http://sqe:8080");
    }

    /// `public_base_url` applies only to requests addressed to its host.
    #[test]
    fn public_base_url_applies_only_on_host_match() {
        let public = Some("https://sql.example.org/");
        // Ingress request, proxy not trusted: plain Host header matches.
        let h = proxied("sql.example.org", None, None, None);
        assert_eq!(extract_base_url(&h, 8080, false, public), "https://sql.example.org");
        let h = proxied("SQL.example.org:80", None, None, None);
        assert_eq!(extract_base_url(&h, 8080, false, public), "https://sql.example.org");
        // In-cluster client calling the service directly keeps its own URI.
        let h = proxied("sqe.sqe:8080", None, None, None);
        assert_eq!(extract_base_url(&h, 8080, false, public), "http://sqe.sqe:8080");
        // A trusted proxy's forwarded host is what is matched.
        let h = proxied("sqe.sqe:8080", Some("http"), Some("sql.example.org"), None);
        assert_eq!(extract_base_url(&h, 8080, true, public), "https://sql.example.org");
        // An untrusted forwarded host cannot select it.
        let h = proxied("sqe.sqe:8080", None, Some("sql.example.org"), None);
        assert_eq!(extract_base_url(&h, 8080, false, public), "http://sqe.sqe:8080");
    }

    /// A request on the HTTPS listener gets https URIs on that listener's
    /// port; a trusted proxy's forwarded scheme still names the client hop.
    #[test]
    fn https_listener_defaults_to_https() {
        let tls = HttpsListenerMarker { port: 8443 };
        let h = proxied("sqe.sqe.svc:8443", None, None, None);
        assert_eq!(
            extract_base_url_on(&h, Some(&tls), 8080, false, None),
            "https://sqe.sqe.svc:8443"
        );
        // An untrusted forwarded proto cannot downgrade it.
        let h = proxied("sqe.sqe.svc:8443", Some("http"), None, None);
        assert_eq!(
            extract_base_url_on(&h, Some(&tls), 8080, false, None),
            "https://sqe.sqe.svc:8443"
        );
        // No Host header: fall back to localhost on the HTTPS port.
        assert_eq!(
            extract_base_url_on(&HeaderMap::new(), Some(&tls), 8080, false, None),
            "https://localhost:8443"
        );
        // The plain listener is unchanged.
        let h = proxied("sqe.sqe.svc:8080", None, None, None);
        assert_eq!(
            extract_base_url_on(&h, None, 8080, false, None),
            "http://sqe.sqe.svc:8080"
        );
    }

    // ── Bearer JWT passthrough tests ────────────────────────────

    /// MockAuth has no bearer provider, so `authenticate_bearer` returns Err.
    /// An invalid bearer token (not validated by any provider) should be rejected
    /// with 401 Unauthorized.
    #[tokio::test]
    async fn test_submit_query_bearer_jwt_passthrough() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let mut headers = HeaderMap::new();
        // JWT-shaped bearer token (three dot-separated segments)
        headers.insert(
            "authorization",
            "Bearer eyJhbGciOi.payload.signature".parse().unwrap(),
        );
        headers.insert("x-trino-user", "alice".parse().unwrap());

        let response = submit_query::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            headers,
            "SELECT 1".to_string(),
        )
        .await;

        let resp = response.into_response();
        // No bearer provider configured -> token validation fails -> 401
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Invalid bearer token should return 401"
        );
    }

    /// A non-JWT bearer token (no dots) should still return 401 when the
    /// bearer provider is not configured.
    #[tokio::test]
    async fn test_submit_query_bearer_opaque_token_rejected() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: true,
            public_base_url: None,
        });

        let mut headers = HeaderMap::new();
        // Opaque token (no dots) — not a JWT, no fallback
        headers.insert("authorization", "Bearer some-opaque-token".parse().unwrap());

        let response = submit_query::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            headers,
            "SELECT 1".to_string(),
        )
        .await;

        let resp = response.into_response();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Opaque bearer tokens must be rejected when no provider is configured"
        );
    }

    // ── Trusted-proxy and coarse-version regression tests ─────────

    #[tokio::test]
    async fn test_server_info_scrubs_version_by_default() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "1.2.3-4567".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: false,
            public_base_url: None,
        });
        let Json(info) = server_info(State(state)).await;
        // Exact build is hidden; coarse major.minor is fine.
        assert_eq!(info.node_version.version, "1.2");
    }

    #[test]
    fn coarse_version_handles_short_input() {
        assert_eq!(coarse_version(""), "unknown");
        assert_eq!(coarse_version("dev"), "unknown");
        assert_eq!(coarse_version("0.1.0"), "0.1");
        assert_eq!(coarse_version("0.31.4-build123"), "0.31");
    }

    #[tokio::test]
    async fn test_trino_client_ip_ignores_xff_by_default() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: false,
            public_base_url: None,
        });
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        let resolved = trino_client_ip(&state, &headers, "10.0.0.1:33333".parse().unwrap());
        assert_eq!(resolved, "10.0.0.1:33333");
    }

    // ── Poll-token binding and pre-auth rate limiting (#622, #562) ──

    /// Stand-in for the coordinator's rate-limited provider chain (#562):
    /// allows `budget` attempts, then answers `ResourceExhausted` the way
    /// `RateLimitedAuthProvider` does, and records the `(peer, principal)`
    /// it was handed so a test can assert *what* was keyed as well as
    /// *whether*. Every attempt inside the budget is rejected, so the test
    /// can tell a 401 (provider ran) from a 429 (budget spent).
    struct BudgetAuth {
        remaining: std::sync::atomic::AtomicUsize,
        seen: Mutex<Vec<(String, String)>>,
    }

    impl BudgetAuth {
        fn new(budget: usize) -> Arc<Self> {
            Arc::new(Self {
                remaining: std::sync::atomic::AtomicUsize::new(budget),
                seen: Mutex::new(Vec::new()),
            })
        }

        fn seen(&self) -> Vec<(String, String)> {
            self.seen.lock().clone()
        }

        fn spend(&self, peer_ip: &str, principal: &str) -> Result<(), sqe_core::SqeError> {
            self.seen
                .lock()
                .push((peer_ip.to_string(), principal.to_string()));
            self.remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .map(|_| ())
                .map_err(|_| {
                    sqe_core::SqeError::with_code(
                        sqe_core::SqeErrorCode::ResourceExhausted,
                        "authentication rate limit".to_string(),
                    )
                })
        }
    }

    #[async_trait::async_trait]
    impl TrinoAuthenticator for BudgetAuth {
        async fn authenticate(
            &self,
            user: &str,
            _: &str,
            peer_ip: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            self.spend(peer_ip, user)?;
            Err(sqe_core::SqeError::Auth("wrong password".to_string()))
        }
        async fn authenticate_bearer(
            &self,
            token: &str,
            peer_ip: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            self.spend(peer_ip, &bearer_fingerprint(token))?;
            Err(sqe_core::SqeError::Auth("bad bearer".to_string()))
        }
    }

    /// Counts how many times the provider chain was actually asked to
    /// validate a credential.
    struct CountingAuth {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TrinoAuthenticator for CountingAuth {
        async fn authenticate(
            &self,
            user: &str,
            _: &str,
            _: &str,
        ) -> Result<Session, sqe_core::SqeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Session::new(
                user.to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
    }

    /// Handler-test state with the caches empty and everything but the
    /// authenticator and executor at its default.
    fn state_with<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
        authenticator: Arc<A>,
        query_handler: Arc<Q>,
    ) -> Arc<TrinoState<A, Q>> {
        Arc::new(TrinoState {
            authenticator,
            query_handler,
            queries: build_query_registry(build_expired_ids()),
            expired: build_expired_ids(),
            node: NodeContext {
                version: "test".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_sessions: build_auth_session_cache(),
            expose_version: false,
            public_base_url: None,
        })
    }

    fn retry_after(resp: &Response) -> Option<String> {
        resp.headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    /// Records every `audit_rejected` call; `execute` is never expected.
    #[derive(Default)]
    struct RejectRecorder {
        rejected: Mutex<Vec<(String, String)>>,
        executed: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TrinoQueryExecutor for RejectRecorder {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<TrinoResultStream, sqe_core::SqeError> {
            self.executed.fetch_add(1, Ordering::SeqCst);
            Ok(TrinoResultStream::from_batches(vec![]))
        }

        fn audit_rejected(&self, session: &Session, sql: &str, _: &sqe_core::SqeError) {
            self.rejected
                .lock()
                .push((session.user.username.clone(), sql.to_string()));
        }
    }

    /// A statement the Trino front door cannot parse is answered here and
    /// never reaches `execute`, where the engine audits; it is handed to
    /// `audit_rejected` instead, exactly once. A parseable one is not.
    #[tokio::test]
    async fn unparseable_statement_is_handed_to_audit_rejected() {
        let executor = Arc::new(RejectRecorder::default());
        let state = state_with(Arc::new(MockAuthOk), executor.clone());
        let submit = |sql: &str| {
            submit_query(
                State(state.clone()),
                test_peer(),
                basic_auth_header("alice", "pw"),
                sql.to_string(),
            )
        };

        let json = body_json(submit("SELEC nonsense FROM").await).await;
        assert!(json.error.is_some(), "parse error reported to the client");
        assert_eq!(
            *executor.rejected.lock(),
            vec![("alice".to_string(), "SELEC nonsense FROM".to_string())]
        );
        assert_eq!(executor.executed.load(Ordering::SeqCst), 0);

        let _ = submit("SELECT 1").await;
        assert_eq!(
            executor.rejected.lock().len(),
            1,
            "valid SQL is not rejected"
        );
    }

    /// #622: the POST route's limiter used to be trivially bypassable by
    /// guessing credentials against a GET poll instead. Every route hands
    /// the peer host to the (rate-limited) chain and maps its denial to 429.
    #[tokio::test]
    async fn poll_with_wrong_credentials_is_rate_limited() {
        let limiter = BudgetAuth::new(2);
        let state = state_with(limiter.clone(), Arc::new(MockQuery));

        let poll = |attempt: usize| {
            let state = state.clone();
            async move {
                get_results(
                    State(state),
                    test_peer(),
                    basic_auth_header("mallory", &format!("guess-{attempt}")),
                    Path((
                        "q-victim".to_string(),
                        "no-slug".to_string(),
                        "0".to_string(),
                    )),
                )
                .await
            }
        };

        assert_eq!(poll(1).await.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(poll(2).await.status(), StatusCode::UNAUTHORIZED);

        let denied = poll(3).await;
        assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(retry_after(&denied).as_deref(), Some("1"));

        // Keyed on the host, not the ephemeral source port, and on the
        // username, not the password.
        assert_eq!(
            limiter.seen(),
            vec![("127.0.0.1".to_string(), "mallory".to_string()); 3]
        );
    }

    /// #622 acceptance criterion: a submit plus N polls of the same query hit
    /// the provider chain exactly once. The polls carry credentials, the way a
    /// real Trino client does — they are simply never validated again.
    #[tokio::test]
    async fn submit_plus_polls_authenticates_exactly_once() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let open = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Notify::new());
        let state = state_with(
            Arc::new(CountingAuth {
                calls: calls.clone(),
            }),
            Arc::new(GatedQuery {
                open,
                gate: gate.clone(),
            }),
        );

        let submitted = submit_query(
            State(state.clone()),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SELECT 1".to_string(),
        )
        .await;
        let body = axum::body::to_bytes(submitted.into_body(), usize::MAX)
            .await
            .unwrap();
        let started: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        let (query_id, slug) = split_queued_uri(started.next_uri.as_deref().unwrap());

        for token in 1..=5 {
            let resp = get_queued_results(
                State(state.clone()),
                test_peer(),
                basic_auth_header("alice", "pw"),
                Path((query_id.clone(), slug.clone(), token.to_string())),
                axum::extract::RawQuery(Some("maxWait=1ms".to_string())),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // One provider call is also one pre-auth budget spend: the limiter
        // sits inside the chain, so what the chain never sees costs nothing.
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the five polls must not have re-run the password grant"
        );

        gate.notify_waiters();
    }

    /// Split `.../v1/statement/queued/{id}/{slug}/{token}` into (id, slug).
    fn split_queued_uri(uri: &str) -> (String, String) {
        let parts: Vec<&str> = uri.rsplitn(4, '/').collect();
        // rsplitn yields [token, slug, id, prefix]
        (parts[2].to_string(), parts[1].to_string())
    }

    /// The poll token alone is sufficient: no `Authorization` header at all,
    /// and an authenticator that would reject one if there were.
    #[tokio::test]
    async fn poll_with_valid_poll_token_needs_no_credentials() {
        let state = state_with(Arc::new(MockAuth), Arc::new(MockQuery));
        state
            .queries
            .insert("q1".to_string(), handle_with(QueryStatus::Running, "alice"));

        let resp = get_queued_results(
            State(state),
            test_peer(),
            HeaderMap::new(),
            Path((
                "q1".to_string(),
                "poll-token-alice".to_string(),
                "1".to_string(),
            )),
            axum::extract::RawQuery(Some("maxWait=1ms".to_string())),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.stats.state, "RUNNING");
    }

    /// A wrong poll token with no credentials is indistinguishable from a
    /// poll for a query id that does not exist: both are a bare 401, so the
    /// route cannot be used to enumerate query ids.
    #[tokio::test]
    async fn wrong_poll_token_without_credentials_does_not_leak_existence() {
        let state = state_with(Arc::new(MockAuth), Arc::new(MockQuery));
        state
            .queries
            .insert("q1".to_string(), handle_with(QueryStatus::Running, "alice"));

        let poll = |id: &str, slug: &str| {
            let state = state.clone();
            let (id, slug) = (id.to_string(), slug.to_string());
            async move {
                get_queued_results(
                    State(state),
                    test_peer(),
                    HeaderMap::new(),
                    Path((id, slug, "1".to_string())),
                    axum::extract::RawQuery(Some("maxWait=1ms".to_string())),
                )
                .await
            }
        };

        let existing = poll("q1", "wrong-token").await;
        let missing = poll("q-does-not-exist", "wrong-token").await;

        assert_eq!(existing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            axum::body::to_bytes(existing.into_body(), usize::MAX)
                .await
                .unwrap(),
            axum::body::to_bytes(missing.into_body(), usize::MAX)
                .await
                .unwrap(),
            "the two 401 bodies must be identical"
        );
    }

    /// #562: the Bearer branch on `POST /v1/statement` used to go straight to
    /// `authenticate_bearer`, so garbage bearers could drive JWKS refetches
    /// and IdP round trips without limit. The peer host now travels with the
    /// bearer and the chain's denial is a 429.
    #[tokio::test]
    async fn bearer_post_past_limiter_budget_returns_429() {
        let limiter = BudgetAuth::new(1);
        let state = state_with(limiter.clone(), Arc::new(MockQuery));

        let post = || {
            let state = state.clone();
            async move {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "authorization",
                    "Bearer eyJhbGciOi.payload.signature".parse().unwrap(),
                );
                submit_query(State(state), test_peer(), headers, "SELECT 1".to_string()).await
            }
        };

        assert_eq!(post().await.status(), StatusCode::UNAUTHORIZED);

        let denied = post().await;
        assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(retry_after(&denied).as_deref(), Some("1"));

        // The bucket is a digest, never the token: a bearer is
        // attacker-supplied and must not become a map key or a log field.
        let seen = limiter.seen();
        assert_eq!(seen.len(), 2);
        for (peer, principal) in seen {
            assert_eq!(peer, "127.0.0.1");
            assert!(principal.starts_with("bearer:"), "got {principal}");
            assert!(!principal.contains("eyJhbGciOi"), "got {principal}");
        }
    }

    #[test]
    fn poll_tokens_are_long_and_unique() {
        let a = mint_poll_token();
        let b = mint_poll_token();
        assert_eq!(a.len(), 43, "32 bytes base64url without padding");
        assert_ne!(a, b);
        assert!(
            a.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "must be safe in a URL path segment: {a}"
        );
    }

    #[test]
    fn constant_time_eq_matches_ordinary_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"", b"a"));
    }

    #[test]
    fn strip_port_keeps_the_host() {
        assert_eq!(strip_port("127.0.0.1:12345"), "127.0.0.1");
        assert_eq!(strip_port("127.0.0.1"), "127.0.0.1");
        assert_eq!(strip_port("[::1]:8080"), "[::1]");
        assert_eq!(strip_port("[::1]"), "[::1]");
        // A bare IPv6 from `x-forwarded-for` keeps every group: truncating at
        // the last colon would fold whole /64s into one rate-limit bucket.
        assert_eq!(strip_port("::1"), "::1");
        assert_eq!(strip_port("2001:db8::1"), "2001:db8::1");
        assert_eq!(strip_port("1.2.3.4:notaport"), "1.2.3.4:notaport");
    }

    /// Ownership is decided on the IdP subject, not the display name, so a
    /// second provider that happens to mint the same username cannot read
    /// another session's results.
    #[test]
    fn ownership_compares_subject_not_display_name() {
        let mut submitter = Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("t".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        submitter.user.subject = Some("idp-1|alice".to_string());
        let owner = QueryOwner::new(&submitter);

        let mut impostor = submitter.clone();
        impostor.user.subject = Some("idp-2|alice".to_string());

        assert!(owner.is(&submitter));
        assert!(
            !owner.is(&impostor),
            "same display name, different subject: not the owner"
        );
    }

    #[test]
    fn basic_session_key_hides_the_password() {
        let key = basic_session_key("alice", "hunter2");
        assert_eq!(key.len(), 64, "hex sha256");
        assert!(!key.contains("hunter2"));
        assert!(!key.contains("alice"));
        assert_ne!(key, basic_session_key("alice", "hunter3"));
        // No ambiguity between ("a:b", "c") and ("a", "b:c").
        assert_ne!(basic_session_key("a:b", "c"), basic_session_key("a", "b:c"));
    }
}

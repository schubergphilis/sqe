use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use moka::future::Cache as MokaCache;
use tracing::{debug, info, instrument, warn, Instrument};
use uuid::Uuid;

use sqe_core::config::{CatalogConfig, SqeConfig, StorageConfig};
use sqe_core::{SqeError, SqeErrorCode};
use sqe_metrics::propagation::trace_context_http_headers;

use crate::circuit_breaker::CircuitBreaker;
use sqe_core::config::BreakerFailureClass;

/// Build the reqwest client every Polaris request goes through.
///
/// Both deadlines come from `[catalog]` (`request_timeout_secs`,
/// `connect_timeout_secs`). A default-built client has neither, so a Polaris
/// pod that accepts TCP and then goes silent (GC pause, half-open LB
/// connection) used to park every planning call forever, and the circuit
/// breaker never saw a failure to record (architecture review 300). Zero is
/// clamped to one second: "no deadline" is the bug this exists to remove.
pub fn build_catalog_http_client(
    request_timeout_secs: u64,
    connect_timeout_secs: u64,
) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs.max(1)))
        .timeout(std::time::Duration::from_secs(request_timeout_secs.max(1)))
        .build()
        .unwrap_or_else(|e| {
            // Builder failure means a broken TLS backend, not bad input; the
            // process cannot talk to any catalog either way.
            panic!("failed to build catalog HTTP client: {e}")
        })
}

/// The reqwest 0.12 twin of [`build_catalog_http_client`], handed to the
/// vendored `RestCatalogBuilder::with_client` so iceberg-rust's own requests
/// (`list_namespaces`, `load_table`, `update_table`) carry the same deadlines.
/// Two clients only because the vendored crate has not moved to reqwest 0.13;
/// both are built from the same two config values.
fn build_vendored_catalog_http_client(
    request_timeout_secs: u64,
    connect_timeout_secs: u64,
) -> reqwest_legacy::Client {
    reqwest_legacy::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs.max(1)))
        .timeout(std::time::Duration::from_secs(request_timeout_secs.max(1)))
        .build()
        .unwrap_or_else(|e| panic!("failed to build vendored catalog HTTP client: {e}"))
}

/// The pair of process-wide clients every SessionCatalog shares, so each
/// authenticated session does not open a fresh connection pool and a fresh
/// TLS slow-start. `raw` serves SQE's own HEAD/GET path; `vendored` is the
/// reqwest 0.12 client the vendored REST catalog runs on.
struct CatalogHttpClients {
    raw: reqwest::Client,
    vendored: reqwest_legacy::Client,
}

impl CatalogHttpClients {
    fn build(request_timeout_secs: u64, connect_timeout_secs: u64) -> Self {
        Self {
            raw: build_catalog_http_client(request_timeout_secs, connect_timeout_secs),
            vendored: build_vendored_catalog_http_client(
                request_timeout_secs,
                connect_timeout_secs,
            ),
        }
    }
}

/// Initialised once from the first `CatalogConfig` seen by
/// [`shared_http_client_for`]; callers that never carry a config
/// ([`SessionCatalog::unconfigured`], tests) fall back to the defaults from
/// `CatalogConfig` (30 s request, 5 s connect).
static SHARED_HTTP_CLIENTS: std::sync::OnceLock<CatalogHttpClients> = std::sync::OnceLock::new();

fn shared_http_clients_with_defaults() -> &'static CatalogHttpClients {
    SHARED_HTTP_CLIENTS.get_or_init(|| {
        CatalogHttpClients::build(
            sqe_core::config::default_catalog_request_timeout_secs(),
            sqe_core::config::default_catalog_connect_timeout_secs(),
        )
    })
}

/// Shared client, built with `catalog`'s deadlines if nothing built it yet.
///
/// The client is process-wide, so the first catalog's deadlines win; a later
/// `[catalogs.*]` entry with different values is logged and ignored.
pub fn shared_http_client_for(catalog: &CatalogConfig) -> reqwest::Client {
    let mut built_here = false;
    let clients = SHARED_HTTP_CLIENTS.get_or_init(|| {
        built_here = true;
        CatalogHttpClients::build(catalog.request_timeout_secs, catalog.connect_timeout_secs)
    });
    if !built_here {
        debug!(
            request_timeout_secs = catalog.request_timeout_secs,
            connect_timeout_secs = catalog.connect_timeout_secs,
            "shared catalog HTTP client already built; per-catalog deadlines are process-wide"
        );
    }
    clients.raw.clone()
}

/// Shared raw client with the `CatalogConfig` default deadlines when no
/// config has reached [`shared_http_client_for`] yet.
fn shared_http_client() -> reqwest::Client {
    shared_http_clients_with_defaults().raw.clone()
}

/// Shared reqwest 0.12 client for the vendored REST catalog; same deadlines
/// as [`shared_http_client`].
fn shared_vendored_http_client() -> reqwest_legacy::Client {
    shared_http_clients_with_defaults().vendored.clone()
}

/// Idle window after which a per-user breaker is dropped. Matches the
/// `REST_CATALOG_CACHE` TTL: a token that has not touched the catalog for
/// this long has no failure budget worth remembering.
const USER_CIRCUIT_BREAKER_IDLE: std::time::Duration = std::time::Duration::from_secs(300);

/// Hard cap on live per-user breakers. Bounds the registry against a login
/// storm inside one idle window; the same order of magnitude as
/// `REST_CATALOG_CACHE_MAX_CAPACITY`.
const USER_CIRCUIT_BREAKER_MAX_CAPACITY: u64 = 10_000;

/// Registry of per-(token, catalog) circuit breakers.
///
/// A single global breaker blast-radiused one user's bad token (rapid
/// retries on an expired bearer count as failures) into "no user can talk to
/// Polaris until the recovery timeout". Keying by token fingerprint isolates
/// each user's failure budget. Keying on the token alone let one unreachable
/// catalog take out every other catalog the same user could reach, so the key
/// is `(fingerprint, catalog)`: a breaker exists to stop hammering an upstream
/// that is down, and that argument is per upstream.
///
/// The old `DashMap` never dropped an entry. OIDC tokens rotate on every dbt
/// run and every login, so the map grew with logins over process lifetime,
/// not with concurrent users (architecture review 301). Entries now expire
/// after [`USER_CIRCUIT_BREAKER_IDLE`] without a lookup and the registry is
/// capped at [`USER_CIRCUIT_BREAKER_MAX_CAPACITY`].
pub(crate) struct UserCircuitBreakers {
    cache: moka::sync::Cache<String, Arc<CircuitBreaker>>,
}

impl UserCircuitBreakers {
    pub(crate) fn new(idle: std::time::Duration, max_capacity: u64) -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_idle(idle)
                .build(),
        }
    }

    fn key(token_fingerprint: &str, catalog: &str) -> String {
        format!("{token_fingerprint}|{catalog}")
    }

    /// Breaker for one `(token, catalog)` pair, created on first use.
    ///
    /// The breaker's *name* still carries the fingerprint because it is only
    /// ever written to logs, where per-user diagnosis is the point. It must
    /// never become a metric label (see [`SessionCatalog::record_catalog_call`]).
    pub(crate) fn get_or_create(
        &self,
        token_fingerprint: &str,
        catalog: &str,
    ) -> Arc<CircuitBreaker> {
        self.get_or_create_for(token_fingerprint, catalog, token_fingerprint)
    }

    /// [`Self::get_or_create`], naming the token's `subject` for the log line
    /// written when the breaker opens.
    pub(crate) fn get_or_create_for(
        &self,
        token_fingerprint: &str,
        catalog: &str,
        subject: &str,
    ) -> Arc<CircuitBreaker> {
        self.cache
            .entry(Self::key(token_fingerprint, catalog))
            .or_insert_with(|| {
                Arc::new(
                    CircuitBreaker::new(
                        format!("polaris-user-{token_fingerprint}-{catalog}"),
                        5,
                        std::time::Duration::from_secs(30),
                    )
                    .with_subject(subject),
                )
            })
            .into_value()
    }

    /// Drop every breaker built for one bearer fingerprint. Called alongside
    /// `REST_CATALOG_CACHE` invalidation so an expired token releases both.
    pub(crate) fn evict_token(&self, token_fingerprint: &str) -> usize {
        if token_fingerprint.is_empty() {
            return 0;
        }
        let prefix = format!("{token_fingerprint}|");
        let keys: Vec<String> = self
            .cache
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| (*k).clone())
            .collect();
        for key in &keys {
            self.cache.invalidate(key);
        }
        keys.len()
    }

    /// Roll every breaker on `catalog` up into `(max_state_code, open_count)`.
    ///
    /// This is what the Prometheus gauges export: one series per catalog, not
    /// one per token. The pass is bounded by the registry capacity and costs
    /// atomic loads only; every caller has just paid a network round trip.
    pub(crate) fn aggregate_for_catalog(&self, catalog: &str) -> (u8, u64) {
        let suffix = format!("|{catalog}");
        let mut max_state = 0u8;
        let mut open = 0u64;
        for (key, breaker) in self.cache.iter() {
            if !key.ends_with(&suffix) {
                continue;
            }
            let code = breaker.state_code();
            max_state = max_state.max(code);
            if code == 2 {
                open += 1;
            }
        }
        (max_state, open)
    }

    /// Flush pending moka maintenance (expiry, capacity eviction) so a test
    /// can observe `entry_count` deterministically.
    #[cfg(test)]
    pub(crate) fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }

    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

static USER_CIRCUIT_BREAKERS: std::sync::LazyLock<UserCircuitBreakers> =
    std::sync::LazyLock::new(|| {
        UserCircuitBreakers::new(USER_CIRCUIT_BREAKER_IDLE, USER_CIRCUIT_BREAKER_MAX_CAPACITY)
    });

/// One breaker per (user, catalog); see [`UserCircuitBreakers`].
fn user_circuit_breaker(token_fingerprint: &str, catalog: &str) -> Arc<CircuitBreaker> {
    USER_CIRCUIT_BREAKERS.get_or_create(token_fingerprint, catalog)
}

/// [`user_circuit_breaker`] labelled with the bearer's subject.
fn user_circuit_breaker_for(bearer_token: &str, token_fingerprint: &str, catalog: &str) -> Arc<CircuitBreaker> {
    let subject = token_subject_for_logs(bearer_token).unwrap_or_else(|| token_fingerprint.to_string());
    USER_CIRCUIT_BREAKERS.get_or_create_for(token_fingerprint, catalog, &subject)
}

/// `sub` (else `preferred_username`) from a JWT bearer's payload, for log
/// lines only. Not verified: authentication happened upstream, and the value
/// never decides anything. `None` for opaque tokens.
fn token_subject_for_logs(bearer_token: &str) -> Option<String> {
    use base64::Engine;
    let payload = bearer_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    ["sub", "preferred_username"]
        .iter()
        .find_map(|k| claims.get(k).and_then(|v| v.as_str()).map(str::to_string))
}

/// REST catalog properties that switch the vendored client to SigV4 on the
/// client side.
///
/// AWS's REST endpoints (Glue, S3 Tables) advertise `rest.sigv4-enabled`
/// plus the signing name and region in their `/v1/config`, and the client
/// picks that up on its own; with `auth = { type = "aws" }` and no fields
/// set this returns nothing and that path is unchanged. A catalog that
/// requires SigV4 without advertising it (RustFS S3 Tables answers
/// `/v1/config` with no `rest.*` keys and rejects unsigned requests with
/// 403 "Signature is required") needs `signing_name` in the config; the
/// region defaults to `[storage] s3_region`, which is also what the
/// object-store side signs with.
pub fn client_side_sigv4_props(
    catalog: &sqe_core::config::CatalogConfig,
    storage: &StorageConfig,
) -> HashMap<String, String> {
    let mut props = HashMap::new();
    if let Some(sqe_core::config::CatalogAuthConfig::Aws {
        signing_name: Some(name),
        signing_region,
    }) = catalog.auth.as_ref()
    {
        let region = signing_region
            .as_deref()
            .filter(|r| !r.trim().is_empty())
            .unwrap_or(storage.s3_region.as_str());
        props.insert("rest.sigv4-enabled".to_string(), "true".to_string());
        props.insert("rest.signing-name".to_string(), name.clone());
        props.insert("rest.signing-region".to_string(), region.to_string());
    }
    props
}

/// REST catalog property that makes the vendored client send
/// `X-Iceberg-Access-Delegation` on every request (`header.*` props become
/// request headers).
pub const ACCESS_DELEGATION_HEADER_PROP: &str = "header.X-Iceberg-Access-Delegation";

/// REST catalog properties that ask the catalog to vend table-scoped
/// storage credentials, per `[catalog] access_delegation` (or implied by
/// `require_vended_credentials`). Empty when no delegation is configured,
/// which keeps static-key mode byte-for-byte unchanged.
pub fn access_delegation_props(
    catalog: &sqe_core::config::CatalogConfig,
) -> HashMap<String, String> {
    catalog
        .effective_access_delegation()
        .header_value()
        .map(|v| HashMap::from([(ACCESS_DELEGATION_HEADER_PROP.to_string(), v.to_string())]))
        .unwrap_or_default()
}

/// SigV4 signer for SQE's own raw HTTP calls to the catalog (the HEAD
/// authorization check, ETag revalidation, the view and commit endpoints).
/// Built from the same `rest.*` and `s3.*` properties the vendored client
/// receives, so both paths sign with one identity.
#[cfg(feature = "rest-sigv4")]
type RawSigner = iceberg_catalog_rest::SigV4Signer;

/// Stand-in when SigV4 is compiled out. Never constructed:
/// [`raw_sigv4_signer`] returns `None` without the feature.
#[cfg(not(feature = "rest-sigv4"))]
struct RawSigner;

#[cfg(not(feature = "rest-sigv4"))]
impl RawSigner {
    async fn sign_headers(
        &self,
        _method: &str,
        _url: &str,
        _headers: &mut reqwest::header::HeaderMap,
        _body: &[u8],
    ) -> iceberg::Result<()> {
        Ok(())
    }
}

/// The raw-path signer for a catalog whose configured properties turn on
/// client-side SigV4 (see [`client_side_sigv4_props`]). Static `s3.*` keys
/// take precedence over the AWS SDK provider chain, mirroring the vendored
/// client. SigV4 that a server only advertises through `/v1/config` is not
/// visible here; those catalogs keep the bearer on the raw path as before.
fn raw_sigv4_signer(props: &HashMap<String, String>) -> Option<Arc<RawSigner>> {
    #[cfg(feature = "rest-sigv4")]
    {
        let enabled = props
            .get("rest.sigv4-enabled")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if !enabled {
            return None;
        }
        let name = props.get("rest.signing-name")?;
        let region = props.get("rest.signing-region")?;
        let signer = RawSigner::new(region.clone(), name.clone());
        let signer = match (
            props.get("s3.access-key-id"),
            props.get("s3.secret-access-key"),
        ) {
            (Some(access_key), Some(secret_key))
                if !access_key.is_empty() && !secret_key.is_empty() =>
            {
                signer.with_static_credentials(
                    access_key.clone(),
                    secret_key.clone(),
                    props.get("s3.session-token").cloned(),
                )
            }
            _ => signer,
        };
        Some(Arc::new(signer))
    }
    #[cfg(not(feature = "rest-sigv4"))]
    {
        let _ = props;
        None
    }
}

/// Attach SQE's catalog identity to a raw request: a SigV4 signature when
/// the catalog is signed, otherwise the session bearer. The body must
/// already be set, because the signature covers its bytes.
async fn authenticate_raw_request(
    req: reqwest::RequestBuilder,
    sigv4: Option<&RawSigner>,
    bearer_token: &str,
) -> sqe_core::Result<reqwest::Request> {
    let build_err =
        |e: reqwest::Error| SqeError::catalog_src(format!("Failed to build catalog request: {e}"), e);
    let Some(signer) = sigv4 else {
        return req.bearer_auth(bearer_token).build().map_err(build_err);
    };
    let mut req = req.build().map_err(build_err)?;
    let body = req
        .body()
        .and_then(|b| b.as_bytes())
        .map(<[u8]>::to_vec)
        .unwrap_or_default();
    let url = req.url().to_string();
    let method = req.method().as_str().to_string();
    signer
        .sign_headers(&method, &url, req.headers_mut(), &body)
        .await
        .map_err(|e| SqeError::catalog_src(format!("Failed to sign catalog request: {e}"), e))?;
    Ok(req)
}

fn insert_s3_location_props(props: &mut HashMap<String, String>, storage: &StorageConfig) {
    if !storage.s3_endpoint.is_empty() {
        props.insert("s3.endpoint".to_string(), storage.s3_endpoint.clone());
    }
    if !storage.s3_region.is_empty() {
        props.insert("s3.region".to_string(), storage.s3_region.clone());
    }
    if storage.s3_path_style {
        props.insert("s3.path-style-access".to_string(), "true".to_string());
    }
}

fn insert_s3_credential_props(props: &mut HashMap<String, String>, storage: &StorageConfig) {
    if !storage.s3_access_key.is_empty() {
        props.insert(
            "s3.access-key-id".to_string(),
            storage.s3_access_key.clone(),
        );
    }
    if !storage.s3_secret_key.is_empty() {
        props.insert(
            "s3.secret-access-key".to_string(),
            storage.s3_secret_key.expose().to_string(),
        );
    }
}

/// Block env / profile / IRSA / IMDSv2 so FileIO cannot pick up the
/// coordinator identity when vended STS is missing (#395 hold).
fn insert_s3_vended_isolation_props(props: &mut HashMap<String, String>) {
    props.insert(
        iceberg::io::S3_DISABLE_CONFIG_LOAD.to_string(),
        "true".to_string(),
    );
    props.insert(
        iceberg::io::S3_DISABLE_EC2_METADATA.to_string(),
        "true".to_string(),
    );
}

/// Apply a catalog RPC outcome to the breaker.
///
/// Auth answers (401/403) mean Polaris is reachable. Record success so a
/// Half-Open probe cannot latch. Real faults still trip the breaker.
fn record_catalog_breaker_outcome<T>(cb: &CircuitBreaker, result: &sqe_core::Result<T>) {
    match result {
        Ok(_) => cb.record_success(),
        Err(e) => record_breaker_class(cb, BreakerFailureClass::of_error(e)),
    }
}

/// Failure classes that count toward opening a per-user breaker; `None`
/// until [`configure_breaker_failure_classes`] runs, which means the default
/// ([`BreakerFailureClass::DEFAULT_COUNTED`]).
static COUNTED_BREAKER_CLASSES: std::sync::RwLock<Option<Vec<BreakerFailureClass>>> =
    std::sync::RwLock::new(None);

/// Set which failure classes open the catalog circuit breakers, from
/// `[catalog] circuit_breaker_failure_classes`. Called once at startup;
/// unknown names are rejected (config validation already refuses them).
pub fn configure_breaker_failure_classes(names: &[String]) -> Result<(), String> {
    let classes = names
        .iter()
        .map(|n| BreakerFailureClass::parse(n))
        .collect::<Result<Vec<_>, _>>()?;
    *COUNTED_BREAKER_CLASSES.write().unwrap_or_else(|e| e.into_inner()) = Some(classes);
    Ok(())
}

fn breaker_counts(class: BreakerFailureClass) -> bool {
    match &*COUNTED_BREAKER_CLASSES.read().unwrap_or_else(|e| e.into_inner()) {
        Some(classes) => classes.contains(&class),
        None => BreakerFailureClass::DEFAULT_COUNTED.contains(&class),
    }
}

/// Apply one classified catalog outcome to a breaker.
///
/// A counted class records a failure, with the class as the logged reason.
/// Anything else where the catalog answered (an authorization denial, a
/// missing object, another client error) is a normal response from a healthy
/// upstream: it records success, so a client probing for objects it cannot
/// see or that do not exist never locks its own token out, and a Half-Open
/// probe answered that way closes the breaker. An uncounted timeout or
/// connection failure records nothing: the upstream did not answer.
fn record_breaker_class(cb: &CircuitBreaker, class: BreakerFailureClass) {
    apply_breaker_class(cb, class, breaker_counts(class));
}

fn apply_breaker_class(cb: &CircuitBreaker, class: BreakerFailureClass, counted: bool) {
    if counted {
        cb.record_failure_with_reason(class.as_str());
    } else if !matches!(class, BreakerFailureClass::Timeout | BreakerFailureClass::Connection) {
        cb.record_success();
    }
}

/// Classify a transport-level `reqwest` failure (no HTTP answer).
fn transport_failure_class(e: &reqwest::Error) -> BreakerFailureClass {
    if e.is_timeout() {
        BreakerFailureClass::Timeout
    } else {
        BreakerFailureClass::Connection
    }
}

/// Largest fraction by which [`TableMetadataCache`] shortens one key's soft
/// TTL, so entries cached together do not all go stale together (#816).
const SOFT_TTL_JITTER: f64 = 0.2;

/// A cached table entry holding metadata, an optional ETag, and the time it was
/// last validated against Polaris.
#[derive(Clone)]
struct CachedTableEntry {
    table: Table,
    etag: Option<String>,
    /// When this entry was last confirmed fresh (inserted or revalidated via 304).
    validated_at: Instant,
    /// When the catalog-vended credentials in `table`'s FileIO stop being
    /// served from the cache (refresh margin before their expiry). `None`
    /// for static or non-expiring credentials. A 304 revalidation proves
    /// the metadata unchanged, not the keys, so it does not move this.
    credentials_usable_until: Option<std::time::SystemTime>,
}

impl CachedTableEntry {
    fn new(table: Table, etag: Option<String>, validated_at: Instant) -> Self {
        let now = std::time::SystemTime::now();
        let credentials_usable_until = crate::vended_credentials::table_credential_expiry(&table)
            .map(|expires_at| crate::vended_credentials::refresh_deadline(expires_at, now));
        Self {
            table,
            etag,
            validated_at,
            credentials_usable_until,
        }
    }

    fn credentials_usable(&self) -> bool {
        self.credentials_usable_until
            .is_none_or(|until| std::time::SystemTime::now() < until)
    }
}

/// Global table metadata cache shared across all sessions.
///
/// Cache keys are scoped per user via the session's token fingerprint
/// (`"{token_fingerprint}|{warehouse}|{namespace}.{table}"`). The warehouse
/// component prevents identically named tables in separate Polaris catalogs
/// from sharing metadata. The cached `Table` carries a
/// `FileIO` configured with vended S3 credentials returned by Polaris in the
/// `LoadTableResponse.config` block; those credentials are per-user STS, so
/// sharing a cache slot across users would silently hand User A's STS creds to
/// User B on every cache hit. Issue #49.
///
/// When a cached entry's soft TTL expires, the cache performs an ETag-based
/// conditional request (`If-None-Match`) to Polaris. If Polaris returns
/// `304 Not Modified`, the cached metadata is reused without re-downloading.
/// This avoids the full metadata fetch (~50-200 KB JSON) when only a freshness
/// check is needed.
///
/// The cache is created once at coordinator startup and passed into every
/// `SessionCatalog` via [`SessionCatalog::new`]. Each session falls through to
/// Polaris on a miss and populates the shared cache on success.
///
/// Use [`TableMetadataCache::invalidate`] after any DDL/DML that changes table
/// structure (DROP TABLE, ALTER TABLE, INSERT, MERGE, DELETE).
///
/// Process-global cache of `RestCatalog` instances, keyed by
/// `{catalog_url}-{warehouse}-{token_fingerprint}`. Each entry holds an
/// `Arc<RestCatalog>` that bakes in the bearer token at construction
/// time, so when Polaris-side token expiry crosses the 5-minute TTL boundary
/// the cached entry returns 401 on every subsequent call. Issue #20 covers
/// the symptom (dbt models 401'ing partway through a run). The query
/// handler now calls [`invalidate_rest_catalog_cache_for_token`] on
/// `AuthenticationFailed` / `AccessDenied` so one expired bearer does not
/// evict every other tenant (issue #410). [`invalidate_rest_catalog_cache_all`]
/// stays for the admin-gated catalog-refresh hook.
///
/// The cached value used to be `Arc<RwLock<RestCatalog>>` but every caller
/// only ever took the lock for read — `RestCatalog` is `Send + Sync` and
/// stateless per call. The lock plus its yield point were pure overhead
/// (~100-500 ns per dispatch, compounding on metadata fan-out). Issue #18.
/// Max number of cached `RestCatalog` instances. Keyed per
/// `catalog_url + warehouse + token_fingerprint`, so the bound is really
/// "distinct concurrent user-token-warehouse triples". At 100 the 101st
/// concurrent user evicted someone else's entry, and each rebuild costs a
/// `/v1/config` round trip (~250ms) plus a `list_namespaces` on the next
/// catalog-provider build. Raised to 2000 so a few thousand concurrent users
/// stop thrashing; the 300s TTL still bounds memory by expiring idle entries.
/// Entries are `Arc<RestCatalog>`, so the per-entry cost is small. Issue #240.
pub(crate) const REST_CATALOG_CACHE_MAX_CAPACITY: u64 = 2000;

pub(crate) static REST_CATALOG_CACHE: std::sync::LazyLock<
    moka::future::Cache<String, Arc<RestCatalog>>,
> = std::sync::LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(REST_CATALOG_CACHE_MAX_CAPACITY)
        .time_to_live(std::time::Duration::from_secs(300))
        .build()
});

/// Drop every cached `RestCatalog` entry.
///
/// Reserved for the admin-gated catalog-refresh hook. Auth-failure recovery
/// must use [`invalidate_rest_catalog_cache_for_token`] so one expired
/// bearer does not evict every other user's catalog (issue #410).
pub async fn invalidate_rest_catalog_cache_all() {
    REST_CATALOG_CACHE.invalidate_all();
    REST_CATALOG_CACHE.run_pending_tasks().await;
    debug!("REST_CATALOG_CACHE invalidated (admin catalog refresh)");
}

/// SHA-256 prefix used as the last segment of a `REST_CATALOG_CACHE` key.
///
/// Must stay in lockstep with `SessionCatalog::new`
/// (`{url}-{warehouse}-vended{n}-deleg{n}-{fp}`: fingerprint last).
/// Do not use [`sqe_core::Session::token_fingerprint`]: that string also
/// prefixes the username and will not match these keys.
pub fn rest_catalog_token_fingerprint(bearer: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = hex::encode(Sha256::digest(bearer.as_bytes()));
    hash[..16].to_string()
}

/// Keys whose last `-` segment equals `token_fingerprint`.
fn rest_catalog_keys_for_token(
    keys: impl IntoIterator<Item = String>,
    token_fingerprint: &str,
) -> Vec<String> {
    if token_fingerprint.is_empty() {
        return Vec::new();
    }
    let suffix = format!("-{token_fingerprint}");
    keys.into_iter().filter(|k| k.ends_with(&suffix)).collect()
}

/// Drop only the `RestCatalog` entries built with this bearer fingerprint.
///
/// Called from the query handler on `AuthenticationFailed` / `AccessDenied`
/// so the next query rebuilds that identity's catalog. Other tenants stay
/// cached (issue #410).
pub async fn invalidate_rest_catalog_cache_for_token(token_fingerprint: &str) {
    let keys = rest_catalog_keys_for_token(
        REST_CATALOG_CACHE.iter().map(|(k, _)| (*k).clone()),
        token_fingerprint,
    );
    let dropped = keys.len();
    for key in keys {
        REST_CATALOG_CACHE.invalidate(&key).await;
    }
    REST_CATALOG_CACHE.run_pending_tasks().await;
    // The same token's circuit breakers go with it: an expired bearer must
    // not keep a failure budget alive once its catalog handle is gone.
    let breakers_dropped = USER_CIRCUIT_BREAKERS.evict_token(token_fingerprint);
    debug!(
        token_fingerprint,
        dropped, breakers_dropped, "REST_CATALOG_CACHE invalidated for one token"
    );
}

/// A monotonic snapshot of every invalidation counter that can evict one cache
/// key, taken *before* a network fetch and checked again before the result is
/// stored.
///
/// Three counters, because three eviction scopes exist and a cold load has no
/// entry in the cache for the eviction to hit: `invalidate_all` bumps `global`,
/// `invalidate_table_all_tokens` bumps the counter for `{ns}.{table}`, and
/// `invalidate` bumps the counter for one full cache key. A reader that was
/// already awaiting the REST `GET` when any of the three fired must not store
/// what it fetched: the writer's eviction found nothing to evict, so an
/// unfenced insert would resurrect pre-commit metadata and mark it fresh for a
/// whole soft TTL (architecture review 304).
///
/// Generation values come from one process-monotonic counter and are never
/// reused, so a pruned (absent) entry can never compare equal to a generation
/// some in-flight reader is holding — there is no ABA window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheGeneration {
    /// `{namespace_display}.{table_name}` — the key tail every token's entry
    /// for this table shares, i.e. what `invalidate_table_all_tokens` bumps.
    table_suffix: String,
    global: u64,
    table: u64,
    key: u64,
}

/// Cap on the per-key/per-table generation maps. Entries are only created by
/// an actual invalidation, so this is reached only by a coordinator that has
/// written to tens of thousands of distinct tables; past it the maps are
/// dropped and every absent key reads the prune floor instead, which is
/// strictly newer than any generation an in-flight reader can hold (so pruning
/// can only refuse an insert, never admit a stale one).
const GENERATION_MAP_MAX_KEYS: usize = 10_000;

/// Global table metadata cache shared by every session.
///
/// # Invalidation invariant
///
/// **Any commit evicts all tokens; only authorization failures evict one.**
///
/// Entries are keyed `{token_fingerprint}|{warehouse}|{ns}.{table}` because a
/// cached `Table` carries per-user vended S3 credentials (issue #49), but the
/// *metadata* it wraps is a property of the table, not of the principal. So
/// every path that commits — `create_table`, `drop_table`, `rename_table`
/// (source and destination), `register_table`, `update_table`,
/// `commit_schema_update` — must call
/// [`TableMetadataCache::invalidate_table_all_tokens`]. Evicting only the
/// writer's own key leaves every other principal planning against pre-commit
/// metadata until the soft TTL: the user who ran `ALTER TABLE ADD COLUMN` sees
/// the column and everyone else gets "column not found" (architecture review
/// 302).
///
/// [`TableMetadataCache::invalidate`] — single key — is reserved for
/// `SessionCatalog::authorize_table_access`, where dropping exactly the
/// no-longer-authorized principal's vended credentials *is* the point and
/// evicting other principals would be a denial of service.
///
/// Callers that populate the cache after a network fetch must fence the insert
/// with [`CacheGeneration`]; see [`TableMetadataCache::insert_if_generation`].
#[derive(Clone)]
pub struct TableMetadataCache {
    /// Long-lived cache (1 hour hard TTL) holding table metadata + ETag.
    /// Soft freshness is checked via `CachedTableEntry::validated_at`.
    inner: MokaCache<String, CachedTableEntry>,
    /// Soft TTL: entries older than this are revalidated via conditional GET.
    soft_ttl: std::time::Duration,
    /// Last successful `HEAD loadTable` per cache key (same
    /// `{token_fingerprint}|{warehouse}|{ns}.{table}` shape as `inner`, so the
    /// principal is part of the key and a new bearer is always re-checked).
    /// A fresh metadata hit used to pay one HEAD per lookup; within
    /// `authorization_recheck` the check is served from here instead
    /// (architecture review 307).
    authorization: MokaCache<String, Instant>,
    /// How long a successful authorization check is reused. `ZERO` re-checks
    /// on every lookup. Sourced from `catalog.authorization_recheck_secs`.
    authorization_recheck: std::time::Duration,
    /// Optional Prometheus metrics for catalog roundtrip + circuit
    /// breaker state. Threaded through the cache so every
    /// `SessionCatalog` clone shares the same handle without changing
    /// the constructor signature.
    metrics: Option<std::sync::Arc<sqe_metrics::MetricsRegistry>>,
    /// Source of generation stamps. Shared by all three scopes so a stamp is
    /// unique cache-wide and never reused.
    generation_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Generation of the last `invalidate_all`.
    global_generation: Arc<std::sync::atomic::AtomicU64>,
    /// Generation of the last `invalidate_table_all_tokens`, per
    /// `{ns}.{table}`.
    table_generations: Arc<DashMap<String, u64>>,
    /// Generation of the last single-key `invalidate`, per full cache key.
    key_generations: Arc<DashMap<String, u64>>,
    /// Highest stamp issued when the generation maps were last pruned. An
    /// absent map entry reads as this value, which is newer than anything a
    /// reader that snapshotted before the prune can hold.
    prune_floor: Arc<std::sync::atomic::AtomicU64>,
}

impl TableMetadataCache {
    /// Create a global table metadata cache with the given TTL.
    ///
    /// `ttl_secs` is the *soft* TTL — after this period, cached entries are
    /// revalidated via `If-None-Match`. The *hard* TTL (moka eviction) is set
    /// to 1 hour to keep stale entries available for conditional revalidation.
    ///
    /// Pass `ttl_secs = 0` to disable the cache (entries are never stored).
    pub fn new(ttl_secs: u64) -> Self {
        let (inner, soft_ttl) = if ttl_secs == 0 {
            (
                MokaCache::builder().max_capacity(0).build(),
                std::time::Duration::ZERO,
            )
        } else {
            (
                MokaCache::builder()
                    .max_capacity(1000)
                    // Hard TTL: keep entries for 1 hour so ETag revalidation can work
                    // even if the soft TTL is much shorter (e.g. 30s).
                    .time_to_live(std::time::Duration::from_secs(3600))
                    .build(),
                std::time::Duration::from_secs(ttl_secs),
            )
        };
        Self {
            inner,
            soft_ttl,
            authorization: MokaCache::builder()
                .max_capacity(10_000)
                .time_to_live(std::time::Duration::from_secs(3600))
                .build(),
            authorization_recheck: std::time::Duration::ZERO,
            metrics: None,
            generation_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            global_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            table_generations: Arc::new(DashMap::new()),
            key_generations: Arc::new(DashMap::new()),
            prune_floor: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Attach a metrics registry. Every `SessionCatalog` clone of this
    /// cache will see the same handle and report catalog roundtrip
    /// latency + circuit breaker state into it.
    #[must_use = "with_metrics consumes self; bind the returned cache"]
    pub fn with_metrics(mut self, metrics: std::sync::Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns the attached metrics registry, if any.
    pub fn metrics(&self) -> Option<&std::sync::Arc<sqe_metrics::MetricsRegistry>> {
        self.metrics.as_ref()
    }

    /// Reuse a successful `HEAD loadTable` authorization for `secs` seconds
    /// per `(token, table)` before asking Polaris again. `0` re-checks on
    /// every lookup (the pre-307 behaviour). Ranger / Polaris revokes are
    /// observed within this window; see
    /// `CatalogConfig::authorization_recheck_secs`.
    #[must_use = "with_authorization_recheck consumes self; bind the returned cache"]
    pub fn with_authorization_recheck(mut self, secs: u64) -> Self {
        self.authorization_recheck = std::time::Duration::from_secs(secs);
        self
    }

    /// Override the soft TTL with a sub-second value. Test seam only: the
    /// freshness clock is `std::time::Instant`, which `tokio::time::pause()`
    /// does not advance, so a state-machine test that must cross the soft TTL
    /// either sleeps for real or shortens the TTL. Production reads
    /// `catalog.metadata_cache_ttl_secs` through [`Self::new`].
    #[doc(hidden)]
    #[must_use = "with_soft_ttl consumes self; bind the returned cache"]
    pub fn with_soft_ttl(mut self, soft_ttl: std::time::Duration) -> Self {
        self.soft_ttl = soft_ttl;
        self
    }

    /// True when `key` was authorized less than `authorization_recheck` ago.
    /// Always false when the recheck window is zero.
    pub async fn authorization_fresh(&self, key: &str) -> bool {
        if self.authorization_recheck.is_zero() {
            return false;
        }
        match self.authorization.get(key).await {
            Some(at) => at.elapsed() < self.authorization_recheck,
            None => false,
        }
    }

    /// Record a successful authorization check for `key`. No-op when the
    /// recheck window is zero so a disabled cache stores nothing.
    pub async fn mark_authorized(&self, key: &str) {
        if self.authorization_recheck.is_zero() {
            return;
        }
        self.authorization
            .insert(key.to_string(), Instant::now())
            .await;
    }

    /// Issue the next cache-wide generation stamp. Strictly increasing and
    /// never reused, which is what lets an absent map entry stand in for "older
    /// than everything" without an ABA hazard.
    fn next_generation(&self) -> u64 {
        self.generation_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
    }

    /// Read one generation map, treating an absent key as the prune floor.
    fn lookup_generation(&self, map: &DashMap<String, u64>, key: &str) -> u64 {
        map.get(key).map_or_else(
            || self.prune_floor.load(std::sync::atomic::Ordering::SeqCst),
            |g| *g,
        )
    }

    /// Stamp `key` in `map` with a fresh generation, pruning the map first if
    /// it has grown past [`GENERATION_MAP_MAX_KEYS`].
    fn bump_generation(&self, map: &DashMap<String, u64>, key: &str) {
        let stamp = self.next_generation();
        if map.len() >= GENERATION_MAP_MAX_KEYS {
            // Raise the floor BEFORE clearing: between the clear and the store,
            // an absent key must not read as an older generation than it did a
            // moment ago, or a fenced insert could wrongly conclude nothing
            // changed.
            self.prune_floor
                .store(stamp, std::sync::atomic::Ordering::SeqCst);
            map.clear();
        }
        map.insert(key.to_string(), stamp);
    }

    /// Snapshot the invalidation generations that govern `key`, whose table
    /// tail is `ns_table_suffix` (`"{namespace_display}.{table_name}"`, the
    /// same string [`Self::invalidate_table_all_tokens`] takes).
    ///
    /// Take this BEFORE the network fetch whose result you intend to cache,
    /// then hand it to [`Self::insert_if_generation`], [`Self::revalidate`] or
    /// [`Self::update_etag`].
    pub fn generation(&self, key: &str, ns_table_suffix: &str) -> CacheGeneration {
        CacheGeneration {
            table_suffix: ns_table_suffix.to_string(),
            global: self
                .global_generation
                .load(std::sync::atomic::Ordering::SeqCst),
            table: self.lookup_generation(&self.table_generations, ns_table_suffix),
            key: self.lookup_generation(&self.key_generations, key),
        }
    }

    /// True when any invalidation touched `key` since `snapshot` was taken.
    fn generation_changed(&self, key: &str, snapshot: &CacheGeneration) -> bool {
        self.generation(key, &snapshot.table_suffix) != *snapshot
    }

    /// The soft TTL of `key`: `soft_ttl` shortened by up to
    /// [`SOFT_TTL_JITTER`], by an amount fixed per key (#816).
    ///
    /// Entries loaded together (a dashboard opening twenty tables, a burst
    /// after a deploy) would otherwise all go stale at the same instant and
    /// revalidate against the catalog together. The jitter only ever
    /// shortens: the policy-tag path reads fresh entries alone, and a longer
    /// TTL would widen the window in which a tag set by another engine is
    /// not seen (architecture review 303).
    fn soft_ttl_for(&self, key: &str) -> std::time::Duration {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let unit = (hasher.finish() % 10_000) as f64 / 10_000.0;
        self.soft_ttl.mul_f64(1.0 - SOFT_TTL_JITTER * unit)
    }

    /// Whether `entry`, cached under `key`, is inside its soft TTL.
    fn is_fresh(&self, key: &str, entry: &CachedTableEntry) -> bool {
        entry.validated_at.elapsed() < self.soft_ttl_for(key)
    }

    /// Get a cached entry if it exists and is still fresh (within soft TTL).
    /// Returns `None` if no entry exists or if the entry has expired.
    pub async fn get_fresh(&self, key: &str) -> Option<Table> {
        let entry = self.usable_entry(key).await?;
        if self.is_fresh(key, &entry) {
            Some(entry.table)
        } else {
            None
        }
    }

    /// Get a stale cached entry for conditional revalidation.
    /// Returns the table and its ETag if the entry exists (regardless of soft TTL).
    ///
    /// Never an entry whose vended credentials are within the refresh margin
    /// of expiry: a 304 would hand those dead keys back as current.
    pub async fn get_stale(&self, key: &str) -> Option<(Table, Option<String>)> {
        let entry = self.usable_entry(key).await?;
        Some((entry.table, entry.etag))
    }

    /// The entry for `key`, dropped instead when its vended credentials are
    /// due for refresh, so the caller reloads the table (and gets new keys)
    /// rather than serving FileIO that will fail mid-scan.
    async fn usable_entry(&self, key: &str) -> Option<CachedTableEntry> {
        let entry = self.inner.get(key).await?;
        if entry.credentials_usable() {
            return Some(entry);
        }
        debug!(cache_key = %key, "Vended credentials due for refresh; dropping cached table");
        // Only this principal's entry: the keys were vended to it alone.
        self.inner.invalidate(key).await;
        None
    }

    /// Refresh the soft TTL on an existing entry (called after a 304 revalidation).
    ///
    /// Fenced by `snapshot`, taken before the conditional `GET` that produced
    /// the 304: a `get`-then-`insert` pair whose two halves straddle an
    /// invalidation would otherwise put the evicted entry back and stamp it
    /// fresh (architecture review 304).
    pub async fn revalidate(&self, key: &str, snapshot: &CacheGeneration) {
        if self.generation_changed(key, snapshot) {
            return;
        }
        if let Some(mut entry) = self.inner.get(key).await {
            entry.validated_at = Instant::now();
            self.inner.insert(key.to_string(), entry).await;
            self.undo_if_raced(key, snapshot).await;
        }
    }

    /// Backwards-compatible `get` — returns fresh entries only.
    pub async fn get(&self, key: &str) -> Option<Table> {
        self.get_fresh(key).await
    }

    pub async fn insert(&self, key: String, table: Table) {
        self.insert_with_etag(key, table, None).await;
    }

    /// Insert a table with an optional ETag from the response headers.
    ///
    /// Unfenced: only safe when nothing could have been invalidated since the
    /// value was produced. Anything that awaits the catalog before inserting
    /// must use [`Self::insert_if_generation`].
    pub async fn insert_with_etag(&self, key: String, table: Table, etag: Option<String>) {
        self.inner
            .insert(key, CachedTableEntry::new(table, etag, Instant::now()))
            .await;
    }

    /// Insert `table` only if no invalidation touched `key`, its table, or the
    /// whole cache since `snapshot` was taken. Returns whether the entry was
    /// stored.
    ///
    /// The fence is lock-free because the two sides run their steps in
    /// opposite orders: every `invalidate*` bumps its generation BEFORE it
    /// evicts, and this checks the generation both before and after its own
    /// insert. So for any interleaving either the eviction runs after the
    /// insert and removes it, or the bump is already visible to the post-check
    /// and this call removes what it just wrote. The moka cache offers no
    /// compare-and-swap, and holding a lock across the insert would serialise
    /// every session on one table.
    pub async fn insert_if_generation(
        &self,
        key: String,
        snapshot: &CacheGeneration,
        table: Table,
        etag: Option<String>,
    ) -> bool {
        if self.generation_changed(&key, snapshot) {
            debug!(
                cache_key = %key,
                "Dropping table metadata fetched before an invalidation (pre-check)"
            );
            return false;
        }
        self.inner
            .insert(
                key.clone(),
                CachedTableEntry::new(table, etag, Instant::now()),
            )
            .await;
        !self.undo_if_raced(&key, snapshot).await
    }

    /// Evict `key` when an invalidation raced the write that just stored it.
    /// Returns whether the entry was rolled back.
    async fn undo_if_raced(&self, key: &str, snapshot: &CacheGeneration) -> bool {
        if self.generation_changed(key, snapshot) {
            self.inner.invalidate(key).await;
            debug!(
                cache_key = %key,
                "Rolled back a table metadata insert that raced an invalidation"
            );
            return true;
        }
        false
    }

    /// Insert an entry whose freshness clock is already `age` old. Test-only:
    /// lets the soft-TTL rules be exercised without sleeping.
    #[cfg(test)]
    pub(crate) async fn insert_aged(&self, key: String, table: Table, age: std::time::Duration) {
        self.inner
            .insert(
                key,
                CachedTableEntry::new(table, None, Instant::now() - age),
            )
            .await;
    }

    /// Attach an ETag to an existing cache entry without changing the cached
    /// table or refreshing the soft-TTL clock. Used by the background HEAD
    /// path so the ETag becomes available for future conditional revalidation
    /// without blocking the cold load_table().
    ///
    /// Fenced by `snapshot`, taken before the `HEAD`: the task runs
    /// fire-and-forget and can land after a commit invalidated the entry, at
    /// which point re-inserting it would resurrect pre-commit metadata
    /// (architecture review 304).
    pub async fn update_etag(&self, key: &str, snapshot: &CacheGeneration, etag: Option<String>) {
        if self.generation_changed(key, snapshot) {
            return;
        }
        if let Some(mut entry) = self.inner.get(key).await {
            entry.etag = etag;
            self.inner.insert(key.to_string(), entry).await;
            self.undo_if_raced(key, snapshot).await;
        }
    }

    /// Evict ONE principal's entry.
    ///
    /// Reserved for `SessionCatalog::authorize_table_access`; commits must use
    /// [`Self::invalidate_table_all_tokens`]. See the type-level invariant.
    pub async fn invalidate(&self, key: &str) {
        // Bump before evicting: a reader awaiting a fetch for this key has no
        // entry here to remove, so the generation is the only thing that can
        // stop it from storing what it read.
        self.bump_generation(&self.key_generations, key);
        self.inner.invalidate(key).await;
        // A denial is exactly the moment the cached authorization is wrong.
        self.authorization.invalidate(key).await;
    }

    /// Evict EVERY cached table entry, across all token fingerprints and
    /// tables. Called from the admin-gated `POST /api/v1/catalogs/refresh`
    /// handler so a catalog created or rebound out-of-band is visible on the
    /// immediate next read instead of surviving the 1 hour hard TTL.
    ///
    /// Process-global, like [`invalidate_rest_catalog_cache_all`]: SQE keeps no
    /// `username -> token_fingerprint` reverse index, so per-user scoping is not
    /// possible here without a side-band map. Because it clears every tenant's
    /// entries, it must stay behind the admin gate -- never wire it into the
    /// self-scoped `CALL system.refresh_catalog_cache()` path.
    pub async fn invalidate_all(&self) {
        // Bump before evicting, and bump the scope that also covers keys with
        // no entry yet: an in-flight load must not repopulate the cache the
        // refresh just cleared.
        let stamp = self.next_generation();
        self.global_generation
            .store(stamp, std::sync::atomic::Ordering::SeqCst);
        self.inner.invalidate_all();
        self.authorization.invalidate_all();
        self.inner.run_pending_tasks().await;
        self.authorization.run_pending_tasks().await;
        debug!("TableMetadataCache invalidated (catalog refresh)");
    }

    /// Evict EVERY cache entry for `(namespace, table)` regardless of which
    /// token fingerprint it was cached under.
    ///
    /// Cache keys are `{token_fingerprint}|{warehouse}|{ns}.{table}`.
    /// `invalidate` (via `SessionCatalog::invalidate_table`) only evicts the
    /// caller's OWN token key, but `properties_for` reads an entry matching the
    /// suffix from ANY token. After a tag/property write, that leaves other
    /// users reading the stale tag map until TTL. This scans for every key
    /// ending in `|{ns}.{table}` and evicts each so the write is visible to all
    /// users immediately.
    ///
    /// `ns_table_suffix` must be the key tail WITHOUT the leading `|`, i.e.
    /// `"{namespace_display}.{table_name}"` (same shape as
    /// `table_ident.namespace()` + `.` + `table_ident.name()`).
    pub async fn invalidate_table_all_tokens(&self, ns_table_suffix: &str) {
        // Bump before evicting. The scan below can only remove entries that
        // already exist; a session whose `load_table` is still awaiting Polaris
        // has no entry to find, and the generation is what stops it from
        // storing the pre-commit metadata it is about to receive.
        self.bump_generation(&self.table_generations, ns_table_suffix);
        // Collect first, then invalidate, to avoid mutating the map mid-iter.
        let keys = select_keys_for_suffix(
            self.inner.iter().map(|(k, _)| (*k).clone()),
            ns_table_suffix,
        );
        for key in keys {
            self.inner.invalidate(&key).await;
        }
    }

    /// Number of entries currently held in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    /// Return the table properties for `(namespace, table_name)` if ANY user's
    /// entry is present in the cache.
    ///
    /// Cache keys are scoped by
    /// `{token_fingerprint}|{warehouse}|{ns}.{table}` to prevent per-user S3
    /// vended credentials from crossing user boundaries (issue #49). Table
    /// *properties* (e.g. `sqe.column-tags`) are user-independent within one
    /// warehouse: they live in the Iceberg table metadata, not in the vended
    /// FileIO credentials.
    ///
    /// The scan is synchronous via `moka::future::Cache::iter()`, which
    /// exposes the current snapshot of the cache without blocking on async I/O.
    /// Returns `None` when no entry with a matching identity suffix exists.
    ///
    /// When `warehouse` is supplied, the exact suffix is
    /// `"|{warehouse}|{namespace_display}.{table_name}"`. Without it, this
    /// returns properties only when all matching entries belong to the same
    /// warehouse. Multiple users normally create multiple token-scoped cache
    /// entries for that warehouse and are not ambiguous; matches from distinct
    /// warehouses fail closed. Entries past the soft TTL are ignored, so a
    /// stale copy held under another principal's token never decides masking.
    ///
    /// The `ends_with` scan is O(entries), but `entries` is bounded by the
    /// cache `max_capacity` (1000) and this runs once per table per query, so
    /// the cost is microseconds -- noise versus the catalog HTTP round-trips it
    /// replaces. A suffix-to-key index is only worth adding if `max_capacity`
    /// is ever raised by orders of magnitude.
    pub fn properties_for(
        &self,
        warehouse: Option<&str>,
        namespace_display: &str,
        table_name: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        let table_suffix = format!("|{}.{}", namespace_display, table_name);
        let exact_suffix = warehouse.map(|warehouse| format!("|{warehouse}{table_suffix}"));
        // Only entries still inside the soft TTL may decide column masking.
        // The data path revalidates a stale entry with an ETag before using
        // it; the policy path has no such step, so a stale entry here would
        // mean a tag added by another engine is ignored until the 1h hard
        // TTL (architecture review 303). No fresh entry => `None`, which the
        // tag source treats as UNKNOWN and the rewriter fails closed on.
        if let Some(exact_suffix) = exact_suffix {
            for (key, entry) in self.inner.iter() {
                if key.ends_with(&exact_suffix) && self.is_fresh(&key, &entry) {
                    return Some(entry.table.metadata().properties().clone());
                }
            }
        }

        let mut fallback = None;
        let mut fallback_warehouse = None;
        for (key, entry) in self.inner.iter() {
            if key.ends_with(&table_suffix) {
                if !self.is_fresh(&key, &entry) {
                    continue;
                }
                let warehouse = key
                    .strip_suffix(&table_suffix)
                    .and_then(|prefix| prefix.rsplit_once('|'))
                    .map(|(_, warehouse)| warehouse);
                let Some(warehouse) = warehouse else {
                    // A malformed key must not make an unqualified policy
                    // lookup less restrictive.
                    return None;
                };
                if fallback_warehouse
                    .as_deref()
                    .is_some_and(|cached| cached != warehouse)
                {
                    return None;
                }
                fallback_warehouse = Some(warehouse.to_string());
                fallback = Some(entry.table.metadata().properties().clone());
            }
        }
        fallback
    }
}

/// Select every cache key belonging to `(ns, table)` regardless of token
/// fingerprint, i.e. every key ending in `|{ns_table_suffix}`.
///
/// Cache keys are `{token_fingerprint}|{warehouse}|{ns}.{table}`. The final
/// `|` boundary is included in the match so a table named `foo` never matches
/// a key for a table named `barfoo` in the same namespace. Pure (no cache access) so the
/// cross-token eviction contract is unit-testable without an iceberg `Table`
/// fixture.
fn select_keys_for_suffix<I>(keys: I, ns_table_suffix: &str) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let suffix = format!("|{ns_table_suffix}");
    keys.into_iter().filter(|k| k.ends_with(&suffix)).collect()
}

/// Backend handle inside `SessionCatalog`.
///
/// REST keeps an `Arc<RestCatalog>` because the per-session
/// `REST_CATALOG_CACHE` (keyed by URL + token fingerprint) hands out
/// the same Arc to every session that authenticates with the same
/// token. The earlier `Arc<RwLock<RestCatalog>>` shape had zero
/// `.write()` callers anywhere in the codebase — every dispatch
/// took `read().await`, which is purely a futex acquisition plus an
/// extra scheduler yield point. `RestCatalog` is `Send + Sync` and
/// stateless per call, so the lock was overhead.
///
/// Non-REST backends construct their iceberg::Catalog implementation
/// once during `for_session` and store it directly as a trait
/// object; there is no equivalent shared cache today (HMS / Glue /
/// JDBC catalog construction is cheap and idempotent).
pub(crate) enum CatalogHandle {
    Rest(Arc<RestCatalog>),
    Other(Arc<dyn iceberg::Catalog>),
}

impl std::fmt::Debug for CatalogHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rest(_) => f.debug_tuple("Rest").field(&"<RestCatalog>").finish(),
            Self::Other(c) => f.debug_tuple("Other").field(c).finish(),
        }
    }
}

/// Match on a `CatalogHandle` and run the same iceberg::Catalog
/// trait method against either variant. Macro because each method
/// signature is different and async closures aren't stable.
///
/// Usage:
/// ```ignore
/// dispatch_catalog!(self.inner, list_namespaces(parent))
/// ```
/// Expands to a match that acquires the REST read lock when needed
/// and returns the awaited iceberg::Catalog method result. Used by
/// every `SessionCatalog` and `SessionCatalogBridge` method that
/// only needs the standard trait surface.
macro_rules! dispatch_catalog {
    ($handle:expr, $method:ident($($args:expr),* $(,)?)) => {
        match &$handle {
            $crate::rest_catalog::CatalogHandle::Rest(rest) => {
                // No `.read().await` here: `RestCatalog` is `Send + Sync`
                // and stateless per call, so the previous `RwLock` was
                // pure overhead. Issue #18.
                rest.$method($($args),*).await
            }
            $crate::rest_catalog::CatalogHandle::Other(catalog) => {
                catalog.$method($($args),*).await
            }
        }
    };
}
// Re-export the macro for use within this module. `pub(crate) use`
// is the documented idiom even though the import looks self-referential.
#[allow(unused_imports)]
pub(crate) use dispatch_catalog;

/// Per-session Iceberg REST catalog wrapper.
///
/// Each authenticated user session gets its own `SessionCatalog` instance
/// configured with the user's bearer token. The token is passed directly to
/// the Polaris REST catalog so that table-level authorization is enforced by
/// the catalog server.
///
/// A single `reqwest::Client` and `CircuitBreaker` are shared across all
/// sessions (passed in at construction time) to ensure:
/// * **Connection reuse**: one hyper connection pool, no per-session teardown.
/// * **Fault isolation**: when Polaris is unavailable the circuit opens and
///   subsequent requests fail fast without wasting threads / connections.
pub struct SessionCatalog {
    /// Backend handle. The REST variant holds an `Arc<RestCatalog>`
    /// (previously `Arc<RwLock<RestCatalog>>`; the lock was removed
    /// in issue #18 because every dispatch only ever did a read).
    /// Non-REST variants hold the iceberg trait object directly.
    /// REST-specific methods on `SessionCatalog` (view DDL, raw
    /// `commit_schema_update`, ETag revalidation in `load_table`)
    /// match on this and error out when the backend isn't REST.
    inner: CatalogHandle,
    catalog_url: String,
    warehouse: String,
    bearer_token: String,
    token_fingerprint: String,
    storage_config: StorageConfig,
    http_client: reqwest::Client,
    /// Signer for the raw REST path when the catalog is configured for
    /// client-side SigV4; `None` means the raw path sends the bearer.
    sigv4: Option<Arc<RawSigner>>,
    /// Extra catalog properties this session was built with (the
    /// client-side SigV4 set), carried over to sibling warehouses.
    extra_props: HashMap<String, String>,
    /// Shared circuit breaker for Polaris REST calls.
    circuit_breaker: Arc<CircuitBreaker>,
    /// When true, catalog FileIO must use Polaris-vended credentials.
    /// Configured `[storage]` access/secret keys are not injected (#395).
    require_vended_credentials: bool,
    /// Shared table metadata cache.
    ///
    /// When a global `TableMetadataCache` is provided at construction time it is
    /// used directly (shared across all sessions). Otherwise a private per-session
    /// cache is created as a fallback (used in tests / when the caller passes `None`).
    ///
    /// Cache is keyed by `"{token_fingerprint}|{namespace}.{table_name}"` so vended
    /// per-user S3 credentials baked into the cached `Table` never cross sessions
    /// (issue #49). TTL and capacity are configured when the global cache is created
    /// (see `TableMetadataCache::new`).
    table_cache: TableMetadataCache,
}

/// Local placeholder used when a deployment has no Iceberg catalog.
///
/// The coordinator still carries a `SessionCatalog` through a handful of
/// legacy write and metadata-TVF APIs. Making all of those APIs optional would
/// spread the absence check across the whole query layer. This catalog keeps
/// that compatibility handle local and fail-fast without constructing a REST
/// client or touching `/v1/config`.
#[derive(Debug)]
struct UnconfiguredCatalog;

fn unconfigured_catalog_error() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "no Iceberg catalog is configured; configure catalog.catalog_url or a named Iceberg catalog",
    )
}

#[async_trait::async_trait]
impl iceberg::Catalog for UnconfiguredCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&iceberg::NamespaceIdent>,
    ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
        Err(unconfigured_catalog_error())
    }

    async fn create_namespace(
        &self,
        _namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        Err(unconfigured_catalog_error())
    }

    async fn get_namespace(
        &self,
        _namespace: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        Err(unconfigured_catalog_error())
    }

    async fn namespace_exists(
        &self,
        _namespace: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<bool> {
        Err(unconfigured_catalog_error())
    }

    async fn update_namespace(
        &self,
        _namespace: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Err(unconfigured_catalog_error())
    }

    async fn drop_namespace(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
        Err(unconfigured_catalog_error())
    }

    async fn list_tables(
        &self,
        _namespace: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
        Err(unconfigured_catalog_error())
    }

    async fn create_table(
        &self,
        _namespace: &iceberg::NamespaceIdent,
        _creation: iceberg::TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        Err(unconfigured_catalog_error())
    }

    async fn load_table(
        &self,
        _table: &iceberg::TableIdent,
    ) -> iceberg::Result<iceberg::table::Table> {
        Err(unconfigured_catalog_error())
    }

    async fn drop_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<()> {
        Err(unconfigured_catalog_error())
    }

    async fn purge_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<()> {
        Err(unconfigured_catalog_error())
    }

    async fn table_exists(&self, _table: &iceberg::TableIdent) -> iceberg::Result<bool> {
        Err(unconfigured_catalog_error())
    }

    async fn rename_table(
        &self,
        _src: &iceberg::TableIdent,
        _dest: &iceberg::TableIdent,
    ) -> iceberg::Result<()> {
        Err(unconfigured_catalog_error())
    }

    async fn register_table(
        &self,
        _table: &iceberg::TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<iceberg::table::Table> {
        Err(unconfigured_catalog_error())
    }

    async fn update_table(
        &self,
        _commit: iceberg::TableCommit,
    ) -> iceberg::Result<iceberg::table::Table> {
        Err(unconfigured_catalog_error())
    }
}

impl std::fmt::Debug for SessionCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCatalog")
            .field("catalog_url", &self.catalog_url)
            .field("warehouse", &self.warehouse)
            .field("token_fingerprint", &self.token_fingerprint)
            .field("circuit_breaker", &self.circuit_breaker.state_label())
            .field("table_cache_entries", &self.table_cache.entry_count())
            .finish()
    }
}

/// CAT-02: emit the shared-identity warning at most once per backend variant
/// per process. Non-REST catalog backends do not forward the user's bearer to
/// the catalog or to cloud storage, so per-user identity is not enforced below
/// the in-engine policy layer. `for_session_other_backend_with` runs once per
/// session-catalog build; a plain `warn!` there would log on every login, so we
/// dedup by backend kind.
fn warn_shared_backend_identity_once(backend: &sqe_core::config::CatalogBackend) {
    use sqe_core::config::CatalogBackend;
    use std::sync::Once;

    let (once, kind): (&'static Once, &'static str) = match backend {
        CatalogBackend::Rest => return, // REST forwards the user bearer; no warning.
        CatalogBackend::Glue { .. } => {
            static W: Once = Once::new();
            (&W, "glue")
        }
        CatalogBackend::Hms { .. } => {
            static W: Once = Once::new();
            (&W, "hms")
        }
        CatalogBackend::Jdbc { .. } => {
            static W: Once = Once::new();
            (&W, "jdbc")
        }
        CatalogBackend::S3tables { .. } => {
            static W: Once = Once::new();
            (&W, "s3tables")
        }
    };

    once.call_once(|| {
        warn!(
            backend = kind,
            "Catalog backend `{kind}` uses the coordinator's shared service \
             identity (IAM role / AWS_PROFILE / HMS connection), NOT the \
             authenticated user. Per-user identity is enforced only by SQE's \
             in-engine policy layer; catalog and cloud-storage access are shared \
             across all users. Use the REST/Polaris backend for per-user bearer \
             passthrough to catalog + S3."
        );
    });
}

/// Detect a Forbidden answer across both error shapes iceberg-rust's REST
/// client produces:
///
/// 1. The typed catalog `ErrorResponse`/`ErrorModel` path, rendered as
///    `Error::new(DataInvalid, message).with_context("code", "403")` (the
///    message usually also says Forbidden / NotAuthorized).
/// 2. The raw wrapped HTTP status path,
///    `Error::new(Unexpected, "Received response with unexpected status
///    code").with_context("status", "403 Forbidden")`.
///
/// Both shapes only surface the status through the rendered context, and
/// `iceberg::Error`'s kind cannot distinguish 403 from 500, so matching on
/// the Debug rendering is the only detection that covers both. Timeouts,
/// 5xx, and auth-refresh failures contain neither marker and therefore
/// read as NOT forbidden — visibility probes must fail the listing on
/// those rather than leak the namespace name (#401).
pub(crate) fn iceberg_error_is_forbidden(e: &iceberg::Error) -> bool {
    e.http_status() == Some(403)
}

fn table_metadata_cache_key(
    token_fingerprint: &str,
    warehouse: &str,
    table_ident: &TableIdent,
) -> String {
    format!(
        "{token_fingerprint}|{warehouse}|{}.{}",
        table_ident.namespace(),
        table_ident.name()
    )
}

/// The `{namespace_display}.{table_name}` tail every token's cache key for this
/// table shares. This is both the eviction scope of
/// [`TableMetadataCache::invalidate_table_all_tokens`] and the scope a fenced
/// insert snapshots, so the two must be built the same way — hence one helper
/// rather than two `format!`s that could drift.
fn table_metadata_cache_suffix(table_ident: &TableIdent) -> String {
    format!("{}.{}", table_ident.namespace(), table_ident.name())
}

/// Wrap an Iceberg catalog failure, classifying a Forbidden (403) answer as
/// [`sqe_core::SqeErrorCode::AccessDenied`] even when the status surfaces only
/// in the error's Debug rendering (the Display string the message-based
/// classifier sees can omit it). This is used for both listings and table loads:
/// callers must distinguish an absent table from one Polaris refused.
fn classify_iceberg_catalog_error(context: &str, e: iceberg::Error) -> SqeError {
    if let Some(status) = e.http_status() {
        let body = format!("{context}: {e}");
        return SqeError::CatalogHttp {
            status,
            op: sqe_core::CatalogOp::Other,
            body,
        };
    }
    match e.kind() {
        iceberg::ErrorKind::TableNotFound => SqeError::sourced(
            sqe_core::SqeErrorCode::TableNotFound,
            format!("{context}: {e}"),
            e,
        ),
        iceberg::ErrorKind::NamespaceNotFound => SqeError::sourced(
            sqe_core::SqeErrorCode::SchemaNotFound,
            format!("{context}: {e}"),
            e,
        ),
        iceberg::ErrorKind::TableAlreadyExists | iceberg::ErrorKind::NamespaceAlreadyExists => {
            SqeError::sourced(
                sqe_core::SqeErrorCode::DuplicateTable,
                format!("{context}: {e}"),
                e,
            )
        }
        iceberg::ErrorKind::CatalogCommitConflicts => {
            SqeError::IcebergCommitConflict(format!("{context}: {e}"))
        }
        iceberg::ErrorKind::DataInvalid => SqeError::sourced(
            sqe_core::SqeErrorCode::TypeMismatch,
            format!("{context}: {e}"),
            e,
        ),
        iceberg::ErrorKind::FeatureUnsupported => SqeError::sourced(
            sqe_core::SqeErrorCode::NotSupported,
            format!("{context}: {e}"),
            e,
        ),
        iceberg::ErrorKind::PreconditionFailed | iceberg::ErrorKind::Unexpected => {
            SqeError::catalog_src(format!("{context}: {e}"), e)
        }
        _ => SqeError::catalog_src(format!("{context}: {e}"), e),
    }
}

/// True when a catalog listing error means the principal is not authorized to
/// see that catalog/namespace (a 403). Enumerators skip such namespaces
/// quietly; any other error is still surfaced/logged.
pub(crate) fn listing_error_is_forbidden(e: &SqeError) -> bool {
    e.error_code() == sqe_core::SqeErrorCode::AccessDenied
}

/// Classify a `get_namespace` visibility-probe outcome without a live catalog.
///
/// * `Ok(true)`  — probe succeeded; the name is visible
/// * `Ok(false)` — 403/Forbidden; hide the name
/// * `Err`       — timeout, 5xx, or other failure; caller must fail the listing
pub(crate) fn namespace_visible_from_probe<T>(
    result: Result<T, iceberg::Error>,
) -> sqe_core::Result<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(e) if iceberg_error_is_forbidden(&e) => Ok(false),
        Err(e) => Err(classify_iceberg_catalog_error(
            "Namespace visibility probe failed",
            e,
        )),
    }
}

impl SessionCatalog {
    /// Construct the compatibility handle for a deployment with no Iceberg
    /// catalog. No network client or backend catalog is created.
    pub fn unconfigured(
        storage_config: &StorageConfig,
        table_cache: Option<TableMetadataCache>,
    ) -> Self {
        Self {
            inner: CatalogHandle::Other(Arc::new(UnconfiguredCatalog)),
            catalog_url: String::new(),
            warehouse: String::new(),
            bearer_token: String::new(),
            token_fingerprint: "unconfigured".to_string(),
            storage_config: storage_config.clone(),
            http_client: shared_http_client(),
            sigv4: None,
            extra_props: HashMap::new(),
            circuit_breaker: user_circuit_breaker("unconfigured", "unconfigured"),
            require_vended_credentials: false,
            table_cache: table_cache.unwrap_or_else(|| TableMetadataCache::new(0)),
        }
    }

    /// Create a new per-session catalog configured with the user's bearer token.
    ///
    /// The `bearer_token` is set as the `token` property in the REST catalog config,
    /// which causes iceberg-rust to send it as a Bearer token in the Authorization header.
    ///
    /// A token fingerprint (last 8 chars) is included in the session identifier to
    /// ensure that token refreshes invalidate the iceberg-rust internal REST session cache.
    ///
    /// `http_client` and `circuit_breaker` should be shared across all sessions (created
    /// once at startup) so that TCP connections and failure state are pooled globally.
    /// Pass `None` for either to fall back to per-instance defaults (suitable for tests).
    ///
    /// `table_cache` is the shared global `TableMetadataCache` created once at coordinator
    /// startup. When `Some`, all sessions share the same cache so cache misses are amortised
    /// across users. When `None` a private cache is built locally (used in tests).
    /// Build a `SessionCatalog` from the coordinator's `SqeConfig` plus
    /// the user's bearer token. Optional shared table cache is forwarded
    /// to `Self::new`.
    ///
    /// This is the helper every `*_handler.rs` should use; it keeps the
    /// 8-arg `new()` constructor available for the rare callers (tests)
    /// that need to wire a custom HTTP client or circuit breaker. The
    /// 13 coordinator call sites that all pass the same `(config,
    /// session.access_token, table_cache, None, None)` tuple should go
    /// through this single entry point so changes to the catalog
    /// construction path don't have to touch each handler.
    ///
    /// Phase O+ step 2: dispatches on `config.catalog.backend`. The
    /// REST variant (default) goes through `Self::new` and gives back
    /// the legacy `CatalogHandle::Rest` shape so view DDL and
    /// `commit_schema_update` keep working. HMS / Glue / JDBC build
    /// the matching iceberg::Catalog through the per-backend
    /// constructor in `crates/sqe-catalog/src/backends/` and store
    /// it as `CatalogHandle::Other`. REST-only methods on
    /// SessionCatalog return an error under non-REST handles.
    pub async fn for_session(
        config: &SqeConfig,
        table_cache: Option<TableMetadataCache>,
        bearer_token: &str,
    ) -> sqe_core::Result<Self> {
        Self::for_session_with(&config.catalog, &config.storage, table_cache, bearer_token).await
    }

    /// Per-catalog variant of `for_session`. Takes a single
    /// `CatalogConfig` rather than reaching into `SqeConfig.catalog`,
    /// so the multi-catalog cluster path can build one
    /// `SessionCatalog` per entry in `[catalogs.*]`. The `storage`
    /// argument is shared across all catalogs because S3 credentials
    /// today are a coordinator-wide concern (per-catalog credential
    /// scoping is a future change).
    pub async fn for_session_with(
        catalog: &sqe_core::config::CatalogConfig,
        storage: &sqe_core::config::StorageConfig,
        table_cache: Option<TableMetadataCache>,
        bearer_token: &str,
    ) -> sqe_core::Result<Self> {
        match &catalog.backend {
            sqe_core::config::CatalogBackend::Rest => {
                let mut extra_props = client_side_sigv4_props(catalog, storage);
                extra_props.extend(access_delegation_props(catalog));
                Self::new_with_props(
                    &catalog.catalog_url,
                    &catalog.warehouse,
                    bearer_token,
                    storage,
                    table_cache,
                    Some(shared_http_client_for(catalog)),
                    None,
                    catalog.require_vended_credentials,
                    extra_props,
                )
                .await
            }
            other => {
                Self::for_session_other_backend_with(
                    catalog,
                    storage,
                    bearer_token,
                    table_cache.unwrap_or_else(|| TableMetadataCache::new(0)),
                    other,
                )
                .await
            }
        }
    }

    /// Per-catalog variant. Same logic as `for_session_other_backend`
    /// but scoped to a single `CatalogConfig` so the multi-catalog
    /// path can iterate over `[catalogs.*]` entries.
    ///
    /// (See the free function [`client_side_sigv4_props`] for the REST
    /// arm's SigV4 switch.)
    ///
    /// `catalog`, `storage`, `bearer_token`, and `table_cache` are consumed
    /// only when one of the optional backend features (`hms`, `glue`,
    /// `sql-postgres`, `s3tables`) is enabled; the default REST-only build
    /// uses the early `unreachable!` arm for `Rest`.
    #[allow(unused_variables, unreachable_code)]
    async fn for_session_other_backend_with(
        catalog: &sqe_core::config::CatalogConfig,
        storage: &sqe_core::config::StorageConfig,
        bearer_token: &str,
        table_cache: TableMetadataCache,
        backend: &sqe_core::config::CatalogBackend,
    ) -> sqe_core::Result<Self> {
        let inner: Arc<dyn iceberg::Catalog> = Self::build_backend_catalog(backend).await?;
        Ok(Self::from_backend_catalog(
            inner,
            catalog,
            storage,
            bearer_token,
            table_cache,
            backend,
        ))
    }

    /// Wrap an already-built non-REST `iceberg::Catalog` as a session catalog.
    ///
    /// The tail of [`Self::for_session_other_backend_with`] after the backend
    /// is constructed, split out so the `CatalogHandle::Other` dispatch arms
    /// can be driven by an in-process `iceberg::Catalog` mock under plain
    /// `cargo test` (architecture review 313). The live backends stay behind
    /// [`Self::build_backend_catalog`].
    pub(crate) fn from_backend_catalog(
        inner: Arc<dyn iceberg::Catalog>,
        catalog: &sqe_core::config::CatalogConfig,
        storage: &sqe_core::config::StorageConfig,
        bearer_token: &str,
        table_cache: TableMetadataCache,
        backend: &sqe_core::config::CatalogBackend,
    ) -> Self {
        let token_fingerprint = rest_catalog_token_fingerprint(bearer_token);

        info!(
            backend = ?backend,
            token_fingerprint = %token_fingerprint,
            "Creating SessionCatalog over non-REST backend"
        );

        // CAT-02: SQE's identity model is "no service account; every query runs
        // as the authenticated user." That holds for REST/Polaris, where the
        // user's bearer is forwarded to the catalog and S3 (rest_catalog
        // `new(...)`). Non-REST backends (Glue / HMS / JDBC / S3 Tables) build a
        // catalog from `[catalog.backend]` props alone; the bearer is stored for
        // cache-keying and logging only (`bearer_token` above) and is NOT passed
        // to the backend. Catalog + cloud-storage access therefore runs under
        // the pod's single shared identity (IAM role / `AWS_PROFILE` / HMS
        // connection), not the user's. Surface that to operators loudly, once
        // per process, mirroring the ClientCredentials warning in sqe-auth.
        warn_shared_backend_identity_once(backend);

        let circuit_breaker =
            user_circuit_breaker_for(bearer_token, &token_fingerprint, &catalog.warehouse);
        Self {
            inner: CatalogHandle::Other(inner),
            catalog_url: String::new(),
            warehouse: catalog.warehouse.clone(),
            bearer_token: bearer_token.to_string(),
            token_fingerprint,
            storage_config: storage.clone(),
            http_client: shared_http_client(),
            sigv4: None,
            extra_props: HashMap::new(),
            circuit_breaker,
            require_vended_credentials: catalog.require_vended_credentials,
            table_cache,
        }
    }

    // (helper defined as a free function below `impl`; see
    // `warn_shared_backend_identity_once`)

    /// Build a bare `iceberg::Catalog` for a non-REST backend.
    ///
    /// Shared by the coordinator's per-session catalog
    /// (`for_session_other_backend_with`) and the embedded CLI, which
    /// attaches the result through `iceberg-datafusion`'s read-only
    /// catalog provider. Each backend translates its typed config into
    /// the upstream loader's `(catalog_type, props)` shape; the loader
    /// picks the matching `CatalogBuilder` and returns the catalog.
    /// Backend support is gated by the same cargo features as the rest
    /// of `sqe-catalog`. See `vendor/iceberg-rust/README.md` and
    /// `docs/site/book/src/getting-started/catalogs.md` for the supported prop keys per backend.
    pub async fn build_backend_catalog(
        backend: &sqe_core::config::CatalogBackend,
    ) -> sqe_core::Result<Arc<dyn iceberg::Catalog>> {
        use sqe_core::config::CatalogBackend;

        let (catalog_type, name, props): (&str, &str, HashMap<String, String>) = match backend {
            CatalogBackend::Rest => {
                return Err(SqeError::Catalog(
                    "build_backend_catalog is for non-REST backends; REST is built via for_session"
                        .to_string(),
                ));
            }

            #[cfg(feature = "hms")]
            CatalogBackend::Hms { uri, warehouse } => {
                use iceberg_catalog_hms::{HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE};
                let mut p = HashMap::new();
                p.insert(HMS_CATALOG_PROP_URI.to_string(), uri.clone());
                p.insert(HMS_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                ("hms", "sqe-hms-session", p)
            }
            #[cfg(not(feature = "hms"))]
            CatalogBackend::Hms { .. } => {
                return Err(SqeError::Catalog(
                    "HMS backend requires the `hms` cargo feature on sqe-catalog".to_string(),
                ));
            }

            #[cfg(feature = "glue")]
            CatalogBackend::Glue {
                region,
                warehouse,
                endpoint,
            } => {
                use iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE;
                let mut p = HashMap::new();
                p.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                // The Glue builder reads `region` and `endpoint` via
                // the AWS SDK shared config layer; pass them through
                // as standard AWS keys so LocalStack and custom
                // endpoints work.
                p.insert("aws.region".to_string(), region.clone());
                if let Some(ep) = endpoint {
                    p.insert("aws.endpoint_url".to_string(), ep.clone());
                }
                ("glue", "sqe-glue-session", p)
            }
            #[cfg(not(feature = "glue"))]
            CatalogBackend::Glue { .. } => {
                return Err(SqeError::Catalog(
                    "Glue backend requires the `glue` cargo feature on sqe-catalog".to_string(),
                ));
            }

            // A `sqlite:` URL is served by the pure-Rust engine, with the
            // same row-scope name the sqlx path used, so a file created
            // under either engine reads the same under the other.
            #[cfg(feature = "sql-sqlite-rs")]
            CatalogBackend::Jdbc { url, warehouse } if url.trim().starts_with("sqlite:") => {
                use crate::sqlite_rs_catalog::SqliteRsCatalog;
                let db_path =
                    crate::mount::sqlite_url_to_path(url.trim()).map_err(SqeError::Catalog)?;
                let catalog = SqliteRsCatalog::open("sqe-jdbc-session", db_path, warehouse)
                    .await
                    .map_err(|e| {
                        SqeError::catalog_src(format!("SQLite catalog build failed: {e}"), e)
                    })?;
                return Ok(Arc::new(catalog));
            }
            #[cfg(feature = "sql-postgres")]
            CatalogBackend::Jdbc { url, warehouse } => {
                use iceberg_catalog_sql::{SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
                let mut p = HashMap::new();
                p.insert(SQL_CATALOG_PROP_URI.to_string(), url.clone());
                p.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                ("sql", "sqe-jdbc-session", p)
            }
            #[cfg(not(feature = "sql-postgres"))]
            CatalogBackend::Jdbc { .. } => {
                return Err(SqeError::Catalog(
                    "JDBC backend with a non-sqlite URL requires the `sql-postgres` cargo \
                     feature on sqe-catalog (`sqlite:` URLs need `sql-sqlite-rs`)"
                        .to_string(),
                ));
            }

            #[cfg(feature = "s3tables")]
            CatalogBackend::S3tables {
                table_bucket_arn,
                endpoint_url,
            } => {
                use iceberg_catalog_s3tables::{
                    S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
                };
                let mut p = HashMap::new();
                p.insert(
                    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                    table_bucket_arn.clone(),
                );
                if let Some(ep) = endpoint_url {
                    p.insert(S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(), ep.clone());
                }
                ("s3tables", "sqe-s3tables-session", p)
            }
            #[cfg(not(feature = "s3tables"))]
            CatalogBackend::S3tables { .. } => {
                return Err(SqeError::Catalog(
                    "S3 Tables backend requires the `s3tables` cargo feature on sqe-catalog"
                        .to_string(),
                ));
            }
        };

        // In a default-features build every backend arm above is a
        // `return Err(feature not enabled)`, so rustc proves this tail
        // unreachable and warns; with any backend feature enabled the arms
        // return `(catalog_type, name, props)` and this is the live path.
        // The allow silences the feature-combination-dependent warning only.
        #[allow(unreachable_code)]
        {
            iceberg_catalog_loader::load(catalog_type)
                .map_err(|e| {
                    SqeError::catalog_src(
                        format!("Catalog loader rejected type `{catalog_type}`: {e}"),
                        e,
                    )
                })?
                .with_storage_factory(crate::storage_factory())
                .load(name.to_string(), props)
                .await
                .map_err(|e| {
                    SqeError::catalog_src(format!("Catalog `{catalog_type}` build failed: {e}"), e)
                })
        }
    }

    /// Build a `SessionCatalog` over an arbitrary `iceberg::Catalog`.
    ///
    /// Test hook, in the same spirit as `TableMetadataCache::insert_aged`: it
    /// lets the cache-invalidation and cache-fencing paths be driven with a
    /// backend that can block or succeed on demand, without a live Polaris and
    /// without a mock server per DDL verb. `catalog_url` points at a closed
    /// port so any background HTTP the paths under test fire is refused
    /// immediately rather than reaching a real host.
    #[cfg(test)]
    pub(crate) fn for_test_with_backend(
        backend: Arc<dyn iceberg::Catalog>,
        table_cache: TableMetadataCache,
        token_fingerprint: &str,
    ) -> Self {
        Self {
            inner: CatalogHandle::Other(backend),
            catalog_url: "http://127.0.0.1:1".to_string(),
            warehouse: "test-warehouse".to_string(),
            bearer_token: "test-token".to_string(),
            token_fingerprint: token_fingerprint.to_string(),
            storage_config: StorageConfig::default(),
            http_client: shared_http_client(),
            sigv4: None,
            extra_props: HashMap::new(),
            circuit_breaker: user_circuit_breaker(token_fingerprint, "http://127.0.0.1:1"),
            require_vended_credentials: false,
            table_cache,
        }
    }

    /// Build a `SessionCatalog` for a SIBLING warehouse on the same Polaris
    /// endpoint, reusing this session's bearer token, storage config, shared
    /// HTTP client, circuit breaker, and table cache.
    ///
    /// Used by the view planner: a view body may reference tables in a
    /// different catalog than the one the view lives in (e.g. a workspace
    /// ontology view in `ws_team_a.ontology` selecting from
    /// `team_a_data.public.events`). Authorization is unchanged — the same
    /// user token is presented, so Polaris/OPA still decide access.
    pub async fn for_sibling_warehouse(&self, warehouse: &str) -> sqe_core::Result<Self> {
        Self::new_with_props(
            &self.catalog_url,
            warehouse,
            &self.bearer_token,
            &self.storage_config,
            Some(self.table_cache.clone()),
            Some(self.http_client.clone()),
            Some(self.circuit_breaker.clone()),
            self.require_vended_credentials,
            self.extra_props.clone(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        catalog_url: &str,
        warehouse: &str,
        bearer_token: &str,
        storage_config: &StorageConfig,
        table_cache: Option<TableMetadataCache>,
        http_client: Option<reqwest::Client>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        require_vended_credentials: bool,
    ) -> sqe_core::Result<Self> {
        Self::new_with_props(
            catalog_url,
            warehouse,
            bearer_token,
            storage_config,
            table_cache,
            http_client,
            circuit_breaker,
            require_vended_credentials,
            HashMap::new(),
        )
        .await
    }

    /// [`Self::new`] plus caller-supplied REST catalog properties, merged
    /// last so they win over the derived ones. Used for the client-side
    /// SigV4 switch (see [`client_side_sigv4_props`]).
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_props(
        catalog_url: &str,
        warehouse: &str,
        bearer_token: &str,
        storage_config: &StorageConfig,
        table_cache: Option<TableMetadataCache>,
        http_client: Option<reqwest::Client>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        require_vended_credentials: bool,
        extra_props: HashMap<String, String>,
    ) -> sqe_core::Result<Self> {
        let token_fingerprint = rest_catalog_token_fingerprint(bearer_token);

        info!(
            catalog_url = catalog_url,
            warehouse = warehouse,
            token_fingerprint = %token_fingerprint,
            "Creating per-session REST catalog"
        );

        let mut props = HashMap::new();
        // Set the bearer token; iceberg-rust's RestCatalog reads the "token" prop
        // and uses it in the Authorization: Bearer header.
        //
        // Per `crates/sqe-auth/src/per_catalog.rs`, an empty bearer is the
        // documented signal for "no Authorization header" (Anonymous / Aws
        // catalogs, sessions before OIDC has issued a token). Inserting
        // "token" -> "" makes iceberg-rust treat the catalog as authenticated
        // with an empty bearer, which the recent defensive guard in
        // `HttpClient::authenticate` correctly rejects, but only at request
        // time. We refuse it here at construction time so the misconfiguration
        // surfaces with a clearer call stack and never reaches the wire.
        if !bearer_token.is_empty() {
            props.insert("token".to_string(), bearer_token.to_string());
        }

        // Set the REST catalog URI and warehouse
        props.insert("uri".to_string(), catalog_url.to_string());
        props.insert("warehouse".to_string(), warehouse.to_string());

        // Endpoint / region / path-style are location, not identity.
        // Access keys are the engine-wide fallback. When vended-only,
        // also disable opendal config/IMDS load so FileIO cannot fall
        // back to AWS_ACCESS_KEY_ID, ~/.aws/config, IRSA, or IMDSv2.
        insert_s3_location_props(&mut props, storage_config);
        props.extend(extra_props.iter().map(|(k, v)| (k.clone(), v.clone())));
        if require_vended_credentials {
            // Vended-only: ask for credentials, and leave nothing static
            // for FileIO to fall back to.
            props.insert(
                ACCESS_DELEGATION_HEADER_PROP.to_string(),
                "vended-credentials".to_string(),
            );
            insert_s3_vended_isolation_props(&mut props);
        } else {
            // Static keys stay the fallback. With `access_delegation` on,
            // the vendored client lets any vended set replace them per
            // table (`SQE PATCH (vended-credentials)`).
            insert_s3_credential_props(&mut props, storage_config);
        }
        let delegated = props.contains_key(ACCESS_DELEGATION_HEADER_PROP);
        // SQE's raw HEAD/GET/POST calls bypass the vendored client's
        // authenticator, so they need their own signer for a SigV4 catalog.
        let sigv4 = raw_sigv4_signer(&props);

        // RisingWave fork uses CatalogBuilder::load(name, props) pattern.
        // Storage factory (OpenDAL S3) is configured automatically from the s3.*
        // properties in the props HashMap — no explicit with_storage_factory() needed.
        //
        // Include the warehouse in the cache key. The cache stores one
        // `RestCatalog` per key, and a `RestCatalog`'s `context()` (which calls
        // `/v1/config?warehouse=...`) is memoized in a `OnceCell` on first use.
        // Without the warehouse in the key, a second warehouse at the same
        // `catalog_url`+token would reuse the first warehouse's cached context
        // and silently resolve to the wrong warehouse. Issue: lazy Polaris
        // catalog discovery + static multi-warehouse-same-URL configs.
        // Include the FileIO identity mode. A catalog built with static
        // keys must not be reused when a later session asks for vended-only
        // FileIO (same URL, warehouse, and bearer).
        // Likewise the delegation header: a catalog built without it never
        // receives vended keys, so it must not serve a delegated session.
        // The fingerprint stays the LAST segment: per-token eviction
        // (`rest_catalog_keys_for_token`) matches on the `-{fp}` suffix, and
        // the earlier `{fp}-vended{n}` order never matched it.
        let catalog_key = format!(
            "{}-{}-vended{}-deleg{}-{}",
            catalog_url,
            warehouse,
            require_vended_credentials as u8,
            delegated as u8,
            token_fingerprint
        );
        // iceberg-rust's own calls (list_namespaces, load_table, update_table)
        // get a client with the same deadlines as SQE's raw HEAD/GET path.
        // Left to its default, the vendored client has no timeout at all
        // (review 300). It is a separate reqwest 0.12 object only because the
        // vendored crate has not moved to 0.13 yet.
        let http_client = http_client.unwrap_or_else(shared_http_client);
        let inner = if let Some(cached) = REST_CATALOG_CACHE.get(&catalog_key).await {
            debug!(token_fingerprint = %token_fingerprint, "REST catalog cache hit");
            cached
        } else {
            debug!(token_fingerprint = %token_fingerprint, "REST catalog cache miss, creating");
            let catalog = RestCatalogBuilder::default()
                .with_client(shared_vendored_http_client())
                .with_storage_factory(crate::storage_factory())
                .load(format!("sqe-session-{}", token_fingerprint), props)
                .await
                .map_err(|e| {
                    SqeError::catalog_src(format!("Failed to create REST catalog: {e}"), e)
                })?;
            let arc_catalog = Arc::new(catalog);
            REST_CATALOG_CACHE
                .insert(catalog_key, arc_catalog.clone())
                .await;
            arc_catalog
        };

        let circuit_breaker = circuit_breaker
            .unwrap_or_else(|| user_circuit_breaker_for(bearer_token, &token_fingerprint, catalog_url));

        // Use the shared global cache when provided; fall back to a private
        // per-session cache (disabled — max_capacity 0) so that call sites that
        // pass `None` (tests, one-shot DDL helpers) don't pollute a global state.
        let table_cache = table_cache.unwrap_or_else(|| TableMetadataCache::new(0));

        Ok(Self {
            inner: CatalogHandle::Rest(inner),
            catalog_url: catalog_url.to_string(),
            warehouse: warehouse.to_string(),
            bearer_token: bearer_token.to_string(),
            token_fingerprint,
            storage_config: storage_config.clone(),
            http_client,
            sigv4,
            extra_props,
            circuit_breaker,
            require_vended_credentials,
            table_cache,
        })
    }

    /// Returns the token fingerprint for this session (last 8 chars of the bearer token).
    pub fn token_fingerprint(&self) -> &str {
        &self.token_fingerprint
    }

    /// Returns the storage config used for fallback S3 credentials.
    pub fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    /// Returns the REST catalog URL.
    pub fn catalog_url(&self) -> &str {
        &self.catalog_url
    }

    /// Returns the warehouse name.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Record a catalog-call outcome on the circuit breaker.
    ///
    /// A 401/403 is not a catalog fault: counting it as failure would open
    /// the breaker and turn later authorized calls into CircuitBreakerOpen
    /// errors. But it is still a completed RPC, so a Half-Open probe must
    /// resolve. Treating auth as success closes the probe without tripping
    /// the breaker. Only 5xx / network / timeout trip it. (#5, #427)
    fn record_breaker_outcome<T>(&self, result: &sqe_core::Result<T>) {
        record_catalog_breaker_outcome(&self.circuit_breaker, result);
    }

    /// List all namespaces in the catalog.
    #[instrument(name = "sqe.catalog", skip(self), fields(op = "list_namespaces", warehouse = %self.warehouse))]
    pub async fn list_namespaces(&self) -> sqe_core::Result<Vec<NamespaceIdent>> {
        debug!(token_fingerprint = %self.token_fingerprint, "Listing namespaces");
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, list_namespaces(None))
            .map_err(|e| classify_iceberg_catalog_error("Failed to list namespaces", e));
        self.record_breaker_outcome(&result);
        self.record_catalog_call("list_namespaces", started, result.is_ok());
        result
    }

    /// Get a namespace by its identifier.
    ///
    /// Returns the `Namespace` object which includes namespace properties.
    pub async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<iceberg::Namespace> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            namespace = ?namespace,
            "Getting namespace"
        );
        dispatch_catalog!(self.inner, get_namespace(namespace))
            .map_err(|e| classify_iceberg_catalog_error("Failed to get namespace", e))
    }

    /// True when the backend is the REST/Polaris variant — the only backend
    /// that forwards the caller's bearer per request and can therefore
    /// answer per-caller visibility probes. Single-identity backends
    /// (Glue/HMS/JDBC/Hadoop) have no caller to scope a namespace list to.
    pub fn is_rest_backend(&self) -> bool {
        matches!(self.inner, CatalogHandle::Rest(_))
    }

    /// Probe whether this session's caller may see `namespace` at all.
    ///
    /// Issues `get_namespace` (Polaris `LOAD_NAMESPACE_METADATA`) on the
    /// session bearer.
    ///
    /// * `Ok(true)`  — probe succeeded; the name is visible
    /// * `Ok(false)` — Forbidden/403: Polaris+OPA denied this caller, so
    ///   the NAME must be hidden from metadata listings
    /// * `Err`       — timeout, 5xx, expired token, or other failure.
    ///   Callers (`list_visible_namespace_names` /
    ///   `filter_visible_namespaces`) must fail the listing as catalog
    ///   unavailable rather than leak the name. (#401)
    pub async fn namespace_visible(&self, namespace: &NamespaceIdent) -> sqe_core::Result<bool> {
        let decision =
            namespace_visible_from_probe(dispatch_catalog!(self.inner, get_namespace(namespace)));
        match &decision {
            Ok(false) => debug!(
                namespace = ?namespace,
                "Namespace hidden from listings: visibility probe was denied (403)"
            ),
            Err(e) => debug!(
                namespace = ?namespace,
                error = %e,
                "Namespace visibility probe failed; failing the listing"
            ),
            Ok(true) => {}
        }
        decision
    }

    /// List all tables in the given namespace.
    #[instrument(name = "sqe.catalog", skip(self), fields(op = "list_tables", namespace = ?namespace, warehouse = %self.warehouse))]
    pub async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<Vec<TableIdent>> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            namespace = ?namespace,
            "Listing tables"
        );
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, list_tables(namespace))
            .map_err(|e| classify_iceberg_catalog_error("Failed to list tables", e));
        self.record_breaker_outcome(&result);
        self.record_catalog_call("list_tables", started, result.is_ok());
        result
    }

    /// Cache key for the table metadata cache.
    ///
    /// Scoped to the session's token fingerprint so vended S3 credentials baked
    /// into the cached `Table` (per-user STS creds returned by Polaris) never
    /// leak across users. Issue #49.
    fn table_cache_key(&self, table_ident: &TableIdent) -> String {
        table_metadata_cache_key(&self.token_fingerprint, &self.warehouse, table_ident)
    }

    /// Load a table by its identifier.
    ///
    /// The returned `Table` includes metadata and a FileIO configured with
    /// vended credentials (if Polaris provides them) or static S3 config.
    ///
    /// Results are cached in the shared global `TableMetadataCache` (passed at construction
    /// time). When a cached entry's soft TTL expires, the cache sends a conditional
    /// `If-None-Match` request using the stored ETag. If Polaris returns 304, the
    /// cached metadata is reused without re-downloading.
    ///
    /// Use [`invalidate_table`] to evict an entry after DDL/DML.
    ///
    /// Returns a boxed future on purpose (compile time, see
    /// docs/internal/plans/2026-09-07-compile-time-async-hubs.md): this is
    /// the catalog's busiest async hub, awaited from a dozen `async_trait`
    /// methods (`SchemaProvider::table`, `information_schema`, the metadata
    /// TVFs), and each of those re-proved the concrete future `Send` under
    /// its own parameter environment. Boxing here proves it once.
    pub fn load_table<'a>(
        &'a self,
        table_ident: &'a TableIdent,
    ) -> futures::future::BoxFuture<'a, sqe_core::Result<Table>> {
        Box::pin(async move {
            let table = self.load_table_inner(table_ident).await?;
            self.pin_to_txn_branch(table_ident, table).await
        })
    }

    /// Inside a transaction that wrote this table, read it at the
    /// transaction's branch (#821). Only the returned value is pinned: the
    /// shared metadata cache keeps the real table, which other sessions of
    /// the same principal read.
    async fn pin_to_txn_branch(
        &self,
        table_ident: &TableIdent,
        table: Table,
    ) -> sqe_core::Result<Table> {
        use crate::txn::{pin, Pin};
        match pin(table) {
            Pin::Unpinned(table) | Pin::Pinned(table) => return Ok(table),
            Pin::Stale { .. } => {}
        }
        // A cached copy from before the branch existed. Never read `main`
        // in its place: that would drop the transaction's own writes.
        match pin(self.load_table_uncached(table_ident).await?) {
            Pin::Unpinned(table) | Pin::Pinned(table) => Ok(table),
            Pin::Stale { branch } => Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::TransactionConflict,
                format!(
                    "transaction branch {branch} of {table_ident} is gone; \
                     the transaction was rolled back"
                ),
            )),
        }
    }

    /// A [`crate::vended_credentials::TableReloader`] for `table_ident` that
    /// reloads through this session (same bearer, same catalog) with
    /// [`Self::load_table_uncached`].
    pub fn table_reloader(
        self: &Arc<Self>,
        table_ident: TableIdent,
    ) -> crate::vended_credentials::TableReloader {
        let catalog = Arc::clone(self);
        crate::vended_credentials::TableReloader::new(move || {
            let catalog = Arc::clone(&catalog);
            let table_ident = table_ident.clone();
            Box::pin(async move {
                let table = catalog.load_table_uncached(&table_ident).await?;
                catalog.pin_to_txn_branch(&table_ident, table).await
            })
        })
    }

    /// Load `table_ident` from the catalog as this session's principal,
    /// bypassing the metadata cache in both directions (nothing read from
    /// it, nothing written to it).
    ///
    /// For the credential-refresh path: a distributed scan whose vended keys
    /// near expiry needs a fresh `loadTable` answer (new keys) for the same
    /// user, not whatever the cache holds. The catalog authorizes the call
    /// again, so a revoked user gets an error, not keys.
    pub fn load_table_uncached<'a>(
        &'a self,
        table_ident: &'a TableIdent,
    ) -> futures::future::BoxFuture<'a, sqe_core::Result<Table>> {
        Box::pin(async move {
            self.circuit_breaker
                .check()
                .map_err(sqe_core::SqeError::Catalog)?;
            let started = Instant::now();
            let result = dispatch_catalog!(self.inner, load_table(table_ident))
                .map_err(|e| classify_iceberg_catalog_error("Failed to reload table", e));
            match &result {
                Ok(_) => self.circuit_breaker.record_success(),
                Err(e) => {
                    record_breaker_class(&self.circuit_breaker, BreakerFailureClass::of_error(e))
                }
            }
            self.record_catalog_call("load_table", started, result.is_ok());
            result
        })
    }

    #[instrument(name = "sqe.catalog", skip(self), fields(op = "load_table", table = %table_ident, warehouse = %self.warehouse))]
    async fn load_table_inner(&self, table_ident: &TableIdent) -> sqe_core::Result<Table> {
        let cache_key = self.table_cache_key(table_ident);
        // Snapshot the invalidation generations BEFORE any fetch below. Every
        // insert on this path is fenced with it, so a commit by another session
        // that lands while we are awaiting Polaris discards what we read
        // instead of being overwritten by it (architecture review 304).
        let generation = self
            .table_cache
            .generation(&cache_key, &table_metadata_cache_suffix(table_ident));

        // CAT-04: when the ETag revalidation GET below fetches a changed
        // metadata document (non-304), its response carries a fresh `ETag`
        // header. Capture it here so the post-reload path can store it directly
        // instead of firing a second fire-and-forget HEAD request to Polaris.
        let mut etag_from_revalidation: Option<String> = None;

        // Fast path: return cached table that is still within the soft TTL.
        if let Some(cached) = self.table_cache.get_fresh(&cache_key).await {
            // A metadata cache hit must not be an authorization cache hit.
            // Polaris/Ranger grants can change out-of-band while the user's
            // bearer token and this cache key stay unchanged. Re-check the
            // cheap HEAD form of loadTable before handing back metadata whose
            // FileIO may still carry previously vended credentials.
            //
            // Within `authorization_recheck_secs` of the last successful check
            // for this (token, table) the HEAD is skipped: a burst of lookups
            // inside one query or one dbt step pays one round trip, and a
            // revoke is observed once the window lapses (review 307).
            if !self.table_cache.authorization_fresh(&cache_key).await {
                self.authorize_table_access(table_ident).await?;
            }
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                "Table cache hit (fresh)"
            );
            return Ok(cached);
        }

        // Check for a stale entry with an ETag for conditional revalidation.
        if let Some((stale_table, Some(etag))) = self.table_cache.get_stale(&cache_key).await {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                etag = %etag,
                "Table cache stale, attempting ETag revalidation"
            );

            // Conditional GET on the loadTable endpoint through the shared
            // REST helper (breaker check, trace headers, metrics, breaker
            // outcome). The helper treats 304 as success.
            let url = self.table_url(table_ident);
            match self
                .rest_request(
                    sqe_core::CatalogOp::LoadTable,
                    reqwest::Method::GET,
                    &url,
                    None,
                    &[("If-None-Match", etag.as_str())],
                )
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::NOT_MODIFIED {
                        // 304: metadata unchanged, refresh the soft TTL.
                        debug!(
                            table = ?table_ident,
                            "ETag revalidation: 304 Not Modified, reusing cached metadata"
                        );
                        self.table_cache.revalidate(&cache_key, &generation).await;
                        return Ok(stale_table);
                    }
                    // Non-304: fall through to the full load path below.
                    // The response body from this GET could in theory be parsed,
                    // but the Polaris loadTable response is complex (includes
                    // credential vending, FileIO config, etc.) and is best handled
                    // by iceberg-rust's RestCatalog. So we discard the body and
                    // let the normal path reload it -- but we DO keep this
                    // response's `ETag` header (CAT-04) so the reload below can
                    // store the fresh ETag without an extra HEAD round-trip.
                    etag_from_revalidation = resp
                        .headers()
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    debug!(
                        table = ?table_ident,
                        status = %status,
                        "ETag revalidation: metadata changed, performing full reload"
                    );
                }
                Err(e) if e.error_code() == sqe_core::SqeErrorCode::CircuitBreakerOpen => {
                    return Err(e);
                }
                Err(e) => {
                    // Network or HTTP error during revalidation: fall through to
                    // the normal load, which decides whether the table is gone.
                    warn!(
                        table = ?table_ident,
                        error = %e,
                        "ETag revalidation request failed, falling back to full load"
                    );
                }
            }
        } else {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                "Loading table (cache miss)"
            );
        }

        // Full load via the configured catalog backend (REST or
        // any iceberg::Catalog). Non-REST backends skip the ETag
        // capture below since they do not expose REST headers.
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, load_table(table_ident))
            .map_err(|e| classify_iceberg_catalog_error("Failed to load table", e));
        match &result {
            Ok(table) => {
                self.circuit_breaker.record_success();

                if let Some(etag) = etag_from_revalidation {
                    // CAT-04: the revalidation GET already returned the fresh
                    // ETag header, so store it directly with the metadata. No
                    // extra HEAD request to Polaris.
                    debug!(table = ?table_ident, etag = %etag, "Captured ETag from revalidation response");
                    self.table_cache
                        .insert_if_generation(
                            cache_key.clone(),
                            &generation,
                            table.clone(),
                            Some(etag),
                        )
                        .await;
                } else {
                    // Cold load with no prior stale entry: iceberg-rust's
                    // `load_table` hides the HTTP response headers, so the ETag
                    // is only obtainable via a follow-up HEAD. Keep it
                    // fire-and-forget so the cold load isn't blocked on it.
                    let cached = self
                        .table_cache
                        .insert_if_generation(cache_key.clone(), &generation, table.clone(), None)
                        .await;

                    // Skip the HEAD when a commit raced this load: nothing was
                    // cached, so there is no entry for an ETag to attach to.
                    if cached {
                        let http_client = self.http_client.clone();
                        let sigv4 = self.sigv4.clone();
                        let bearer_token = self.bearer_token.clone();
                        let url = self.table_url(table_ident);
                        let table_cache = self.table_cache.clone();
                        let table_ident_log = table_ident.clone();
                        let generation = generation.clone();
                        let etag_span = tracing::info_span!(
                            "iceberg.rest.etag",
                            db.system.name = "iceberg",
                            catalog.operation = "load_table_etag",
                            table = %table_ident,
                        );
                        tokio::spawn(
                            async move {
                                let etag = fetch_table_etag_inner(
                                    &http_client,
                                    sigv4.as_deref(),
                                    &bearer_token,
                                    &url,
                                )
                                .await;
                                if let Some(e) = etag.as_deref() {
                                    debug!(table = %table_ident_log, etag = %e, "Captured ETag for table");
                                }
                                // Fenced: this task outlives the load, so a
                                // commit can have evicted the entry by now.
                                table_cache.update_etag(&cache_key, &generation, etag).await;
                            }
                            .instrument(etag_span),
                        );
                    }
                }
            }
            // Only upstream faults count; a denial or a missing table is a
            // normal answer (#5). See `record_breaker_class`.
            Err(e) => record_breaker_class(&self.circuit_breaker, BreakerFailureClass::of_error(e)),
        }
        self.record_catalog_call("load_table", started, result.is_ok());
        result
    }

    /// Revalidate the caller's current permission to load a table without
    /// downloading its metadata.
    ///
    /// Table metadata and query-result caches are performance caches, not
    /// authorization caches. A Ranger REVOKE does not change the user's bearer
    /// token, so cache keys alone cannot make that revoke observable. Polaris
    /// authorizes `HEAD loadTable` exactly like `GET loadTable`; use it as the
    /// low-bandwidth security check before serving either kind of cached data.
    ///
    /// Non-REST backends use a shared service identity and have no per-user
    /// catalog authorization to revalidate here; their in-engine policy layer
    /// remains authoritative.
    pub async fn authorize_table_access(&self, table_ident: &TableIdent) -> sqe_core::Result<()> {
        if !matches!(self.inner, CatalogHandle::Rest(_)) {
            return Ok(());
        }

        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;

        let started = Instant::now();
        let mut req = self
            .http_client
            .head(self.table_url(table_ident))
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let req =
            authenticate_raw_request(req, self.sigv4.as_deref(), &self.bearer_token).await?;

        let response = self.http_client.execute(req).await.map_err(|e| {
            record_breaker_class(&self.circuit_breaker, transport_failure_class(&e));
            SqeError::catalog_src(format!("Failed to authorize table {table_ident}: {e}"), e)
        })?;
        let status = response.status();

        if status.is_success() {
            self.circuit_breaker.record_success();
            self.record_catalog_call("authorize_table", started, true);
            self.table_cache
                .mark_authorized(&self.table_cache_key(table_ident))
                .await;
            return Ok(());
        }

        // HEAD is not required by every Iceberg REST implementation. Fall
        // back to the catalog client's fully authenticated load when the
        // endpoint explicitly says the method is unsupported.
        if matches!(
            status,
            reqwest::StatusCode::METHOD_NOT_ALLOWED | reqwest::StatusCode::NOT_IMPLEMENTED
        ) {
            let result = dispatch_catalog!(self.inner, load_table(table_ident))
                .map(|_| ())
                .map_err(|e| classify_iceberg_catalog_error("Failed to authorize table", e));
            self.record_breaker_outcome(&result);
            self.record_catalog_call("authorize_table", started, result.is_ok());
            if result.is_ok() {
                self.table_cache
                    .mark_authorized(&self.table_cache_key(table_ident))
                    .await;
            }
            return result;
        }

        let status_code = status.as_u16();
        if matches!(status_code, 401 | 403 | 404) {
            // Drop the metadata carrying the no-longer-authorized principal's
            // vended FileIO credentials before returning the denial.
            self.table_cache
                .invalidate(&self.table_cache_key(table_ident))
                .await;
        }
        record_breaker_class(
            &self.circuit_breaker,
            BreakerFailureClass::from_http_status(status_code),
        );
        self.record_catalog_call("authorize_table", started, false);
        Err(SqeError::CatalogHttp {
            status: status_code,
            op: sqe_core::CatalogOp::LoadTable,
            body: if status == reqwest::StatusCode::FORBIDDEN {
                format!("Access denied to table {table_ident}")
            } else {
                format!("Authorization check failed for table {table_ident}")
            },
        })
    }

    /// Prometheus label for this session's catalog. The configured endpoint
    /// (or warehouse for non-REST backends): one value per configured
    /// catalog, never anything derived from a bearer token.
    fn breaker_metric_catalog(&self) -> &str {
        if self.catalog_url.is_empty() {
            &self.warehouse
        } else {
            &self.catalog_url
        }
    }

    /// Record catalog roundtrip latency + circuit breaker state into
    /// the optional MetricsRegistry attached to the table cache. The
    /// helper is a no-op when no metrics handle is attached, so
    /// test-only SessionCatalogs pay nothing.
    ///
    /// Breaker gauges are labelled by catalog and aggregated over every
    /// per-token breaker on that catalog. Labelling by breaker name put a
    /// token fingerprint in a Prometheus label, so `/metrics` grew a series
    /// per login for the life of the process (architecture review 301).
    fn record_catalog_call(&self, op: &'static str, started: Instant, ok: bool) {
        if let Some(metrics) = self.table_cache.metrics() {
            let status = if ok { "ok" } else { "err" };
            metrics
                .catalog_request_duration_seconds
                .with_label_values(&[op, status])
                .observe(started.elapsed().as_secs_f64());
            let catalog = self.breaker_metric_catalog();
            let (max_state, open) = USER_CIRCUIT_BREAKERS.aggregate_for_catalog(catalog);
            // A breaker handed in by a caller (tests, sibling warehouses)
            // may not live in the registry; fold it in so its state counts.
            let own = self.circuit_breaker.state_code();
            let max_state = max_state.max(own);
            metrics
                .catalog_circuit_breaker_state
                .with_label_values(&[catalog])
                .set(max_state as f64);
            metrics
                .catalog_circuit_breakers_open
                .with_label_values(&[catalog])
                .set(open as f64);
        }
    }

    fn table_url(&self, table_ident: &TableIdent) -> String {
        self.rest_url(&[
            "namespaces",
            &namespace_path_segment(table_ident.namespace()),
            "tables",
            table_ident.name(),
        ])
    }

    fn views_url(&self, namespace: &NamespaceIdent) -> String {
        self.rest_url(&["namespaces", &namespace_path_segment(namespace), "views"])
    }

    fn view_url(&self, namespace: &NamespaceIdent, name: &str) -> String {
        self.rest_url(&[
            "namespaces",
            &namespace_path_segment(namespace),
            "views",
            name,
        ])
    }

    /// `{rest_prefix}/{segments...}` with every segment percent-encoded as a
    /// path segment, so a table or view named `a/b?c%` and the `\u{1F}`
    /// namespace separator both travel as their escaped forms
    /// (architecture review 311).
    fn rest_url(&self, segments: &[&str]) -> String {
        let prefix = self.rest_prefix();
        match url::Url::parse(&prefix) {
            Ok(mut base) => {
                if let Ok(mut path) = base.path_segments_mut() {
                    path.pop_if_empty().extend(segments);
                }
                base.to_string()
            }
            // Not a URL (unconfigured catalog_url): keep the legacy join so
            // the failure surfaces as the connection error it always was.
            Err(_) => format!("{prefix}/{}", segments.join("/")),
        }
    }

    /// One authenticated Iceberg REST round trip with the operational
    /// plumbing every raw call needs (architecture review 311): breaker
    /// admission, `X-Request-ID` + W3C trace headers, the
    /// `sqe_catalog_request_duration_seconds{op}` observation, the breaker
    /// outcome, and the typed status mapping.
    ///
    /// Returns the response for any 2xx or 304. Any other status is read and
    /// returned as [`SqeError::CatalogHttp`] (409 on a commit becomes
    /// [`SqeError::IcebergCommitConflict`]), so callers match on
    /// `error_code()` / `is_not_found()` instead of on status text.
    ///
    /// Breaker accounting: transport failures, 429 and 5xx trip it; a 4xx
    /// answer means the catalog is reachable and counts as success, matching
    /// [`record_catalog_breaker_outcome`].
    async fn rest_request(
        &self,
        op: sqe_core::CatalogOp,
        method: reqwest::Method,
        url: &str,
        body: Option<serde_json::Value>,
        extra_headers: &[(&str, &str)],
    ) -> sqe_core::Result<reqwest::Response> {
        self.circuit_breaker
            .check()
            .map_err(|msg| SqeError::with_code(SqeErrorCode::CircuitBreakerOpen, msg))?;
        let started = Instant::now();
        let mut req = self
            .http_client
            .request(method, url)
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        for (k, v) in extra_headers {
            req = req.header(*k, *v);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let req =
            authenticate_raw_request(req, self.sigv4.as_deref(), &self.bearer_token).await?;

        let resp = match self.http_client.execute(req).await {
            Ok(resp) => resp,
            Err(e) => {
                record_breaker_class(&self.circuit_breaker, transport_failure_class(&e));
                self.record_catalog_call(op.name(), started, false);
                return Err(SqeError::catalog_src(
                    format!("Catalog request failed ({}): {e}", op.name()),
                    e,
                ));
            }
        };

        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_MODIFIED {
            self.circuit_breaker.record_success();
            self.record_catalog_call(op.name(), started, true);
            return Ok(resp);
        }

        let body = resp.text().await.unwrap_or_default();
        record_breaker_class(
            &self.circuit_breaker,
            BreakerFailureClass::from_http_status(status.as_u16()),
        );
        self.record_catalog_call(op.name(), started, false);
        Err(map_rest_status(op, status.as_u16(), body))
    }
}

/// Typed error for a non-2xx Iceberg REST answer. Status codes carry the
/// classification ([`SqeError::error_code`]); a 409 on a commit is the
/// retryable [`SqeError::IcebergCommitConflict`].
fn map_rest_status(op: sqe_core::CatalogOp, status: u16, body: String) -> SqeError {
    if status == 409 && op == sqe_core::CatalogOp::Commit {
        return SqeError::IcebergCommitConflict(format!("Catalog commit conflict: {body}"));
    }
    SqeError::CatalogHttp { status, op, body }
}

/// Iceberg REST encodes a multi-level namespace as one path segment joined
/// by the unit separator; [`SessionCatalog::rest_url`] percent-encodes it.
fn namespace_path_segment(namespace: &NamespaceIdent) -> String {
    namespace
        .as_ref()
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\u{1F}")
}

/// The dialects SQE can plan, in preference order (architecture review 311).
/// `sqe` is native; `trino` and `spark` go through the compatibility rewrite
/// the query path already applies to client SQL.
const SUPPORTED_VIEW_DIALECTS: [&str; 3] = ["sqe", "trino", "spark"];

/// Select the SQL of the view's *current* version (`current-version-id`,
/// not `versions.last()`), preferring representation dialects in
/// [`SUPPORTED_VIEW_DIALECTS`] order.
///
/// Returns a typed `NotSupported` error when no representation carries a
/// dialect SQE can plan, so a Trino- or Spark-only view never gets planned as
/// SQE SQL by accident.
fn select_view_sql(
    metadata: &iceberg::spec::ViewMetadata,
    view_name: &str,
) -> sqe_core::Result<String> {
    let version = metadata.current_version();
    let sql_reps: Vec<&iceberg::spec::SqlViewRepresentation> = version
        .representations()
        .iter()
        .map(|rep| match rep {
            iceberg::spec::ViewRepresentation::Sql(sql) => sql,
        })
        .collect();
    for dialect in SUPPORTED_VIEW_DIALECTS {
        if let Some(rep) = sql_reps
            .iter()
            .find(|rep| rep.dialect.eq_ignore_ascii_case(dialect))
        {
            return Ok(rep.sql.clone());
        }
    }
    let found: Vec<&str> = sql_reps.iter().map(|rep| rep.dialect.as_str()).collect();
    Err(SqeError::with_code(
        SqeErrorCode::NotSupported,
        format!(
            "Unsupported view dialect for view '{view_name}' (version {}): found {:?}, \
             SQE can plan {:?}",
            version.version_id(),
            found,
            SUPPORTED_VIEW_DIALECTS
        ),
    ))
}

/// `loadView` response body: the metadata document plus its location.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadViewResponse {
    #[allow(dead_code)]
    metadata_location: Option<String>,
    metadata: iceberg::spec::ViewMetadata,
}

/// `createView` request body (Iceberg REST `CreateViewRequest`).
#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateViewRequest<'a> {
    name: &'a str,
    schema: &'a iceberg::spec::Schema,
    view_version: &'a iceberg::spec::ViewVersion,
    properties: &'a HashMap<String, String>,
}

/// `replaceView` request body (Iceberg REST `CommitViewRequest`). The
/// requirement list is a plain value: the vendored crate has no
/// `ViewRequirement` type yet, and `assert-view-uuid` is its only member.
#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct CommitViewRequest<'a> {
    identifier: TableIdent,
    requirements: Vec<serde_json::Value>,
    updates: &'a [iceberg::ViewUpdate],
}

/// Build the single SQE-dialect version SQE writes for a view definition.
fn sqe_view_version(
    version_id: i32,
    schema_id: i32,
    sql: &str,
    namespace: &NamespaceIdent,
    default_catalog: &str,
) -> sqe_core::Result<iceberg::spec::ViewVersion> {
    let representation: iceberg::spec::ViewRepresentation = iceberg::spec::SqlViewRepresentation {
        sql: sql.to_string(),
        dialect: "sqe".to_string(),
    }
    .into();
    // `ViewRepresentations` has no public constructor; it is a transparent
    // newtype over the list, so round-trip the typed representation.
    let representations: iceberg::spec::ViewRepresentations =
        serde_json::from_value(serde_json::to_value(vec![representation]).map_err(|e| {
            SqeError::execution_src(format!("Failed to serialize view representation: {e}"), e)
        })?)
        .map_err(|e| {
            SqeError::execution_src(format!("Failed to build view representations: {e}"), e)
        })?;
    let summary = HashMap::from([
        ("engine-name".to_string(), "sqe".to_string()),
        (
            "engine-version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ),
    ]);
    Ok(iceberg::spec::ViewVersion::builder()
        .with_version_id(version_id)
        .with_schema_id(schema_id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_summary(summary)
        .with_representations(representations)
        .with_default_catalog(Some(default_catalog.to_string()))
        .with_default_namespace(namespace.clone())
        .build())
}

/// The schema and version updates of a `replaceView` commit.
///
/// The server's view builder reuses the id of an existing schema or version
/// that is structurally identical to the one added, and then records it as
/// "last added" only when it was added in the same commit. So `-1` ("the one
/// added in this commit") fails once the server deduplicated: an unchanged
/// schema gave "Cannot set last added schema: no schema has been added"
/// (#832), and an unchanged definition would fail the same way at
/// `set-current-view-version`. Both are resolved here against the existing
/// metadata with the server's own equality, so a replace that changes
/// nothing just points the view at the version it already has.
/// `-1` still names a schema that is new, whatever id the server gives it
/// (#770).
fn view_replace_updates(
    existing: &iceberg::spec::ViewMetadata,
    schema: iceberg::spec::Schema,
    version_for_schema: impl FnOnce(i32) -> sqe_core::Result<iceberg::spec::ViewVersion>,
) -> sqe_core::Result<Vec<iceberg::ViewUpdate>> {
    let mut updates = Vec::with_capacity(3);
    let same_schema = existing.schemas_iter().find(|s| {
        s.as_struct() == schema.as_struct() && s.identifier_field_ids().eq(schema.identifier_field_ids())
    });
    let schema_id = match same_schema {
        Some(s) => s.schema_id(),
        None => {
            let last_column_id = Some(schema.highest_field_id());
            updates.push(iceberg::ViewUpdate::AddSchema {
                schema,
                last_column_id,
            });
            -1
        }
    };
    let version = version_for_schema(schema_id)?;
    let same_version = (schema_id != -1)
        .then(|| {
            existing.versions().find(|v| {
                v.summary() == version.summary()
                    && v.representations() == version.representations()
                    && v.default_catalog() == version.default_catalog()
                    && v.default_namespace() == version.default_namespace()
                    && v.schema_id() == version.schema_id()
            })
        })
        .flatten();
    let view_version_id = match same_version {
        Some(v) => v.version_id(),
        None => {
            updates.push(iceberg::ViewUpdate::AddViewVersion {
                view_version: version,
            });
            -1
        }
    };
    updates.push(iceberg::ViewUpdate::SetCurrentViewVersion { view_version_id });
    Ok(updates)
}

/// Parse the coordinator's Iceberg-JSON view schema into the typed schema,
/// re-numbered to `schema_id` so a replace never collides with the schema
/// ids already in the view document.
fn view_schema_from_json(
    schema: &serde_json::Value,
    schema_id: i32,
) -> sqe_core::Result<iceberg::spec::Schema> {
    let mut schema = schema.clone();
    if let Some(obj) = schema.as_object_mut() {
        obj.entry("schema-id").or_insert(serde_json::json!(0));
        obj.entry("type").or_insert(serde_json::json!("struct"));
    }
    let parsed: iceberg::spec::Schema = serde_json::from_value(schema)
        .map_err(|e| SqeError::execution_src(format!("Invalid view schema document: {e}"), e))?;
    if parsed.schema_id() == schema_id {
        return Ok(parsed);
    }
    iceberg::spec::Schema::builder()
        .with_schema_id(schema_id)
        .with_fields(parsed.as_struct().fields().iter().cloned())
        .build()
        .map_err(|e| SqeError::execution_src(format!("Invalid view schema: {e}"), e))
}

/// HEAD-based ETag fetch usable from a background `tokio::spawn`.
async fn fetch_table_etag_inner(
    http_client: &reqwest::Client,
    sigv4: Option<&RawSigner>,
    bearer_token: &str,
    url: &str,
) -> Option<String> {
    let mut req = http_client
        .head(url)
        .header("X-Request-ID", Uuid::new_v4().to_string());
    for (k, v) in trace_context_http_headers() {
        req = req.header(k, v);
    }
    let req = match authenticate_raw_request(req, sigv4, bearer_token).await {
        Ok(req) => req,
        Err(e) => {
            debug!(url = %url, error = %e, "Failed to sign ETag request (non-fatal)");
            return None;
        }
    };
    match http_client.execute(req).await {
        Ok(resp) => resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        Err(e) => {
            debug!(url = %url, error = %e, "Failed to fetch ETag (non-fatal)");
            None
        }
    }
}

impl SessionCatalog {
    /// Evict THIS session's cached copy of a table.
    ///
    /// Not for DDL/DML. A commit is visible to every principal, so it must use
    /// [`Self::invalidate_table_all_tokens`]; evicting only the writer's key
    /// leaves other principals planning against pre-commit metadata until the
    /// soft TTL (architecture review 302). This form exists for the
    /// authorization-revoke path, where dropping exactly the affected
    /// principal's vended credentials is the point, and for a write to a
    /// transaction branch, which `main` readers never see.
    pub async fn invalidate_table(&self, table_ident: &TableIdent) {
        let key = self.table_cache_key(table_ident);
        self.table_cache.invalidate(&key).await;
        debug!(table = %table_ident, "Table cache invalidated");
    }

    /// Evict every cached copy of a table, regardless of the bearer token that
    /// populated it. Every commit path uses this form: table metadata is a
    /// property of the table, not of the principal that wrote it, and a Trino
    /// client may even present a freshly exchanged access token for its next
    /// statement.
    pub async fn invalidate_table_all_tokens(&self, table_ident: &TableIdent) {
        let suffix = table_metadata_cache_suffix(table_ident);
        self.table_cache.invalidate_table_all_tokens(&suffix).await;
        crate::schema_provider::invalidate_namespace_listing_all_tokens(
            &table_ident.namespace().as_ref().join("."),
        );
        debug!(table = %table_ident, "Table cache and name listings invalidated for all tokens");
    }

    /// Identifies this catalog to a transaction: every table it writes must
    /// share the key, because COMMIT publishes them in one catalog call.
    pub(crate) fn txn_catalog_key(&self) -> String {
        format!("{}|{}", self.catalog_url.trim_end_matches('/'), self.warehouse)
    }

    /// Refuse a write to `table_ident` that the current transaction could not
    /// publish atomically with the tables it already wrote (#823): a table in
    /// another catalog, or a second table in a catalog with no multi-table
    /// commit. Call it before the statement writes any data file; outside a
    /// transaction it does nothing.
    pub async fn admit_txn_write(&self, table_ident: &TableIdent) -> sqe_core::Result<()> {
        match crate::txn::current() {
            Some(txn) => self.admit_txn_write_in(&txn, table_ident).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn admit_txn_write_in(
        &self,
        txn: &crate::txn::TxnScope,
        table_ident: &TableIdent,
    ) -> sqe_core::Result<()> {
        let needs_atomic = txn.tables().iter().any(|t| &t.ident != table_ident);
        // For REST, ask what the server advertised (`GET /v1/config`), so a
        // server without `POST /v1/transactions/commit` is refused here and
        // not at COMMIT, after both tables were written.
        let atomic = match (&self.inner, needs_atomic) {
            (_, false) => true,
            (CatalogHandle::Rest(rest), true) => {
                rest.advertises_commit_transaction().await.map_err(|e| {
                    SqeError::catalog_src(
                        "Failed to read the catalog's advertised endpoints".to_string(),
                        e,
                    )
                })?
            }
            (CatalogHandle::Other(catalog), true) => catalog.supports_commit_transaction(),
        };
        crate::txn::admit(txn, table_ident, &self.txn_catalog_key(), || atomic)
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, table_ident: &TableIdent) -> sqe_core::Result<bool> {
        dispatch_catalog!(self.inner, table_exists(table_ident)).map_err(|e| {
            sqe_core::SqeError::catalog_src(format!("Failed to check table existence: {e}"), e)
        })
    }

    /// Build the Polaris REST URL prefix for this warehouse.
    fn rest_prefix(&self) -> String {
        let base = self.catalog_url.trim_end_matches('/');
        format!("{base}/v1/{}", self.warehouse)
    }

    /// Guard for methods that talk to the REST catalog directly
    /// (view DDL, raw `commit_schema_update`). These bypass the
    /// iceberg::Catalog trait, so they only function under
    /// `CatalogHandle::Rest`. Returns an error pointing at the
    /// backend mismatch rather than letting a downstream HTTP call
    /// fail with an opaque "connection refused" against an empty
    /// catalog_url.
    fn require_rest_backend(&self, op: &str) -> sqe_core::Result<()> {
        if matches!(self.inner, CatalogHandle::Rest(_)) {
            Ok(())
        } else {
            Err(SqeError::Catalog(format!(
                "{op} requires the REST catalog backend; the configured \
                 backend ({:?}) does not expose this operation through the \
                 iceberg::Catalog trait. Switch [catalog].backend to `rest` \
                 or use a tool that talks directly to the configured backend.",
                self.inner
            )))
        }
    }

    /// Create a view via the Iceberg REST API.
    ///
    /// Calls `POST /v1/{prefix}/namespaces/{namespace}/views` with a typed
    /// `CreateViewRequest`: the schema parsed into [`iceberg::spec::Schema`]
    /// and one [`iceberg::spec::ViewVersion`] carrying a single `sqe`-dialect
    /// SQL representation, `default-catalog` = this warehouse and
    /// `default-namespace` = the view's namespace.
    #[instrument(skip(self, sql, schema), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
        sql: &str,
        schema: &serde_json::Value,
        properties: &std::collections::HashMap<String, String>,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("create_view")?;
        let url = self.views_url(namespace);
        let schema = view_schema_from_json(schema, 0)?;
        let version = sqe_view_version(1, schema.schema_id(), sql, namespace, &self.warehouse)?;
        let body = serde_json::to_value(CreateViewRequest {
            name,
            schema: &schema,
            view_version: &version,
            properties,
        })
        .map_err(|e| {
            SqeError::execution_src(format!("Failed to serialize view request: {e}"), e)
        })?;

        debug!(url = %url, view = name, "Creating view via Iceberg REST");
        self.rest_request(
            sqe_core::CatalogOp::CreateView,
            reqwest::Method::POST,
            &url,
            Some(body),
            &[],
        )
        .await?;

        // A new or dropped name changes every session's listing of the
        // namespace (information_schema, SHOW TABLES), as a table commit does.
        crate::schema_provider::invalidate_namespace_listing_all_tokens(
            &namespace.as_ref().join("."),
        );
        info!(view = name, namespace = ?namespace, "View created");
        Ok(())
    }

    /// `CREATE OR REPLACE VIEW` with real replace semantics.
    ///
    /// When the view exists, its definition is replaced in one
    /// `replaceView` commit (`POST .../views/{view}` with an
    /// `assert-view-uuid` requirement and `add-schema`, `add-view-version`,
    /// `set-current-view-version` updates), so readers never observe a window
    /// in which the view is missing and the version history is kept. When it
    /// does not exist, this is [`Self::create_view`].
    #[instrument(skip(self, sql, schema), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn create_or_replace_view(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
        sql: &str,
        schema: &serde_json::Value,
        properties: &std::collections::HashMap<String, String>,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("create_or_replace_view")?;
        let Some(existing) = self.load_view_metadata(namespace, name).await? else {
            return self
                .create_view(namespace, name, sql, schema, properties)
                .await;
        };

        let next_schema_id = existing
            .schemas_iter()
            .map(|s| s.schema_id())
            .max()
            .map_or(0, |id| id + 1);
        let next_version_id = existing
            .versions()
            .map(|v| v.version_id())
            .max()
            .map_or(1, |id| id + 1);
        let schema = view_schema_from_json(schema, next_schema_id)?;
        let mut updates = view_replace_updates(&existing, schema, |schema_id| {
            sqe_view_version(next_version_id, schema_id, sql, namespace, &self.warehouse)
        })?;
        if !properties.is_empty() {
            updates.push(iceberg::ViewUpdate::SetProperties {
                updates: properties.clone(),
            });
        }
        let body = serde_json::to_value(CommitViewRequest {
            identifier: TableIdent::new(namespace.clone(), name.to_string()),
            requirements: vec![serde_json::json!({
                "type": "assert-view-uuid",
                "uuid": existing.uuid(),
            })],
            updates: &updates,
        })
        .map_err(|e| SqeError::execution_src(format!("Failed to serialize view commit: {e}"), e))?;

        let url = self.view_url(namespace, name);
        debug!(url = %url, view = name, version = next_version_id, "Replacing view via Iceberg REST");
        self.rest_request(
            sqe_core::CatalogOp::Commit,
            reqwest::Method::POST,
            &url,
            Some(body),
            &[],
        )
        .await?;

        info!(view = name, namespace = ?namespace, version = next_version_id, "View replaced");
        Ok(())
    }

    /// List views in a namespace via the Iceberg REST API.
    #[instrument(skip(self), fields(namespace = ?namespace, warehouse = %self.warehouse))]
    pub async fn list_views(&self, namespace: &NamespaceIdent) -> sqe_core::Result<Vec<String>> {
        self.require_rest_backend("list_views")?;
        let url = self.views_url(namespace);
        let resp = self
            .rest_request(
                sqe_core::CatalogOp::LoadView,
                reqwest::Method::GET,
                &url,
                None,
                &[],
            )
            .await?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SqeError::catalog_src(format!("Failed to parse views list: {e}"), e))?;

        let names = body["identifiers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(names)
    }

    /// Load a view's metadata document. `None` when the view does not exist.
    async fn load_view_metadata(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> sqe_core::Result<Option<iceberg::spec::ViewMetadata>> {
        let url = self.view_url(namespace, name);
        let resp = match self
            .rest_request(
                sqe_core::CatalogOp::LoadView,
                reqwest::Method::GET,
                &url,
                None,
                &[],
            )
            .await
        {
            Ok(resp) => resp,
            Err(e) if e.is_not_found() => return Ok(None),
            Err(e) => return Err(e),
        };
        let body: LoadViewResponse = resp.json().await.map_err(|e| {
            SqeError::catalog_src(
                format!("Failed to parse view response for '{name}': {e}"),
                e,
            )
        })?;
        Ok(Some(body.metadata))
    }

    /// Load a view's SQL definition from the Iceberg REST API.
    ///
    /// Returns `None` if the view does not exist (404). The SQL comes from
    /// the version `current-version-id` points at, preferring a `sqe`, then
    /// `trino`, then `spark` representation; a view with none of those fails
    /// with a typed `NotSupported` error (architecture review 311).
    #[instrument(skip(self), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn load_view_sql(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> sqe_core::Result<Option<String>> {
        self.require_rest_backend("load_view_sql")?;
        match self.load_view_metadata(namespace, name).await? {
            Some(metadata) => select_view_sql(&metadata, name).map(Some),
            None => Ok(None),
        }
    }

    /// Drop a view via the Iceberg REST API.
    ///
    /// Calls `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`.
    #[instrument(skip(self), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn drop_view(&self, namespace: &NamespaceIdent, name: &str) -> sqe_core::Result<()> {
        self.require_rest_backend("drop_view")?;
        let url = self.view_url(namespace, name);
        debug!(url = %url, view = name, "Dropping view via Iceberg REST");
        self.rest_request(
            sqe_core::CatalogOp::DropView,
            reqwest::Method::DELETE,
            &url,
            None,
            &[],
        )
        .await?;

        // A new or dropped name changes every session's listing of the
        // namespace (information_schema, SHOW TABLES), as a table commit does.
        crate::schema_provider::invalidate_namespace_listing_all_tokens(
            &namespace.as_ref().join("."),
        );
        info!(view = name, namespace = ?namespace, "View dropped");
        Ok(())
    }

    /// Commit a schema update to a table via the Polaris REST API.
    ///
    /// Sends `POST /v1/{warehouse}/namespaces/{namespace}/tables/{table}` with
    /// the provided `TableRequirement` list and `TableUpdate` list. This is the
    /// Iceberg REST Catalog table-commit endpoint used for schema evolution
    /// (ADD/DROP/RENAME/ALTER COLUMN).
    ///
    /// `TableUpdate` and `TableRequirement` are serialized directly; we build the
    /// JSON payload ourselves rather than going through `TableCommit::builder()`,
    /// whose `build()` method is crate-private in the upstream iceberg crate.
    #[instrument(name = "sqe.catalog", skip(self, updates, requirements), fields(op = "commit_schema_update", table = %table_ident, warehouse = %self.warehouse))]
    pub async fn commit_schema_update(
        &self,
        table_ident: &TableIdent,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("commit_schema_update")?;
        let url = self.table_url(table_ident);

        let body = serde_json::json!({
            "identifier": {
                "namespace": table_ident.namespace().as_ref(),
                "name": table_ident.name(),
            },
            "requirements": serde_json::to_value(&requirements)
                .map_err(|e| SqeError::execution_src(format!("Failed to serialize requirements: {e}"), e))?,
            "updates": serde_json::to_value(&updates)
                .map_err(|e| SqeError::execution_src(format!("Failed to serialize updates: {e}"), e))?,
        });

        debug!(url = %url, table = %table_ident, "Committing schema update via Iceberg REST");
        self.rest_request(
            sqe_core::CatalogOp::Commit,
            reqwest::Method::POST,
            &url,
            Some(body),
            &[],
        )
        .await
        .map_err(|e| match e {
            SqeError::CatalogHttp { status, op, body } => SqeError::CatalogHttp {
                status,
                op,
                body: format!("schema update for '{table_ident}': {body}"),
            },
            other => other,
        })?;

        info!(table = %table_ident, "Schema update committed");
        // Evict EVERY token's entry, not just the committer's: an added or
        // dropped column, a rename, or a changed `sqe.column-tags` property is
        // visible to every principal the moment Polaris accepts it. Evicting
        // one key is how `ALTER TABLE ADD COLUMN` used to succeed for the
        // author while everyone else got "column not found" until the soft TTL
        // (architecture review 302).
        self.invalidate_table_all_tokens(table_ident).await;
        Ok(())
    }

    /// Return a bridge that implements iceberg's `Catalog` trait.
    /// This is useful for passing to iceberg-datafusion providers.
    pub fn as_catalog(self: &Arc<Self>) -> Arc<SessionCatalogBridge> {
        Arc::new(SessionCatalogBridge {
            session: self.clone(),
        })
    }
}

/// Bridge type that implements iceberg's `Catalog` trait by delegating
/// to our `SessionCatalog`. This is needed because `SessionCatalog` wraps
/// the inner `RestCatalog` in `CatalogHandle::Rest(Arc<RestCatalog>)`
/// (previously `Arc<RwLock<RestCatalog>>`, removed in issue #18) and we
/// need a plain `Catalog` trait object for the iceberg-datafusion
/// providers.
#[derive(Debug)]
pub struct SessionCatalogBridge {
    session: Arc<SessionCatalog>,
}

/// Hand an `SqeError` back to iceberg-rust callers. The message keeps the
/// SQE text; a catalog HTTP status also rides along as the `status` context,
/// so `iceberg::Error::http_status` sees it on the far side (the commit
/// retry tells a 401 from a 5xx by it, sqe#803).
fn bridge_error(e: SqeError) -> iceberg::Error {
    let status = match &e {
        SqeError::CatalogHttp { status, .. } => Some(*status),
        _ => None,
    };
    let bridged = iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string());
    match status {
        Some(status) => bridged.with_context("status", status.to_string()),
        None => bridged,
    }
}

#[async_trait::async_trait]
impl Catalog for SessionCatalogBridge {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        dispatch_catalog!(self.session.inner, list_namespaces(parent))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        dispatch_catalog!(self.session.inner, create_namespace(namespace, properties))
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        dispatch_catalog!(self.session.inner, get_namespace(namespace))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        dispatch_catalog!(self.session.inner, namespace_exists(namespace))
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        dispatch_catalog!(self.session.inner, update_namespace(namespace, properties))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        dispatch_catalog!(self.session.inner, drop_namespace(namespace))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        dispatch_catalog!(self.session.inner, list_tables(namespace))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<Table> {
        let table_name = creation.name.clone();
        let result = dispatch_catalog!(self.session.inner, create_table(namespace, creation))?;
        // Evict every token's entry for this name, not just the creator's: a
        // DROP+CREATE with a different schema otherwise leaves other principals
        // holding a `Table` whose FileIO and metadata location point at the
        // dropped table's files (architecture review 302).
        let ident = TableIdent::new(namespace.clone(), table_name);
        self.session.invalidate_table_all_tokens(&ident).await;
        Ok(result)
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        // Delegate through SessionCatalog::load_table so the cache is used.
        self.session.load_table(table).await.map_err(bridge_error)
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let result = dispatch_catalog!(self.session.inner, drop_table(table));
        // Invalidate on success or failure: stale data is worse than a miss.
        // All tokens, because a dropped table is dropped for everyone and a
        // surviving entry hands out a FileIO for deleted files.
        self.session.invalidate_table_all_tokens(table).await;
        result
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let result = dispatch_catalog!(self.session.inner, purge_table(table));
        // Same invalidation as drop_table: the files are gone for everyone.
        self.session.invalidate_table_all_tokens(table).await;
        result
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        dispatch_catalog!(self.session.inner, table_exists(table))
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        let result = dispatch_catalog!(self.session.inner, rename_table(src, dest));
        // BOTH names, and every token holding either. The source name no longer
        // resolves and the destination may have shadowed a cached entry, so a
        // surviving copy of either is metadata for a table that is not there.
        // On failure too: the rename may have partially applied, and a miss is
        // cheaper than a wrong answer.
        self.session.invalidate_table_all_tokens(src).await;
        self.session.invalidate_table_all_tokens(dest).await;
        result
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        let result =
            dispatch_catalog!(self.session.inner, register_table(table, metadata_location));
        // Registering binds this name to a different metadata document; any
        // principal's cached entry for the name is now for the wrong table.
        self.session.invalidate_table_all_tokens(table).await;
        result
    }

    fn supports_commit_transaction(&self) -> bool {
        match &self.session.inner {
            CatalogHandle::Rest(_) => true,
            CatalogHandle::Other(catalog) => catalog.supports_commit_transaction(),
        }
    }

    async fn commit_transaction(&self, commits: Vec<iceberg::TableCommit>) -> iceberg::Result<()> {
        let idents: Vec<TableIdent> = commits.iter().map(|c| c.identifier().clone()).collect();
        let result = dispatch_catalog!(self.session.inner, commit_transaction(commits));
        // As in `update_table`: every principal's cached copy is stale, and
        // a failure may still have committed (unknown commit state).
        for ident in &idents {
            self.session.invalidate_table_all_tokens(ident).await;
        }
        result
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let ident = commit.identifier().clone();
        // Inside a transaction every write goes to the transaction branch and
        // only COMMIT, which runs outside the scope, moves `main` (#821). A
        // write path that forgot the branch fails here instead of publishing
        // part of the transaction early.
        let txn = crate::txn::current();
        let sets_ref = |name: &str| {
            commit.updates().iter().any(|u| {
                matches!(u, iceberg::TableUpdate::SetSnapshotRef { ref_name, .. }
                    if ref_name == name)
            })
        };
        if txn.is_some() && sets_ref(iceberg::spec::MAIN_BRANCH) {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                format!(
                    "this write to {ident} is not supported inside a transaction: \
                     it would commit outside the transaction branch"
                ),
            ));
        }
        // Same rule for a data write on the session's WRITE_BRANCH (#829):
        // a write path that ignored the branch must not land on `main`.
        if let Some(branch) = crate::txn::write_branch() {
            if sets_ref(iceberg::spec::MAIN_BRANCH) {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::FeatureUnsupported,
                    format!(
                        "this write to {ident} is not supported with WRITE_BRANCH \
                         '{branch}': it would commit to main; RESET WRITE_BRANCH first"
                    ),
                ));
            }
        }
        let branch_commit = txn.filter(|txn| sets_ref(&txn.branch_name()));
        if let Some(txn) = &branch_commit {
            // The statement was admitted before it wrote; this catches a
            // write path that skipped the check, before its branch lands
            // where COMMIT cannot publish it (#823).
            self.session
                .admit_txn_write_in(txn, &ident)
                .await
                .map_err(bridge_error)?;
        }
        let branch_commit_seen = branch_commit.is_some();
        let result = dispatch_catalog!(self.session.inner, update_table(commit));
        if let (Some(txn), Ok(_)) = (branch_commit, &result) {
            txn.bind_catalog(
                self.session.txn_catalog_key(),
                Arc::new(SessionCatalogBridge {
                    session: Arc::clone(&self.session),
                }),
            );
        }
        // A table commit is visible to every principal. Evict every token's
        // copy so a different user cannot rebuild a SessionContext from stale
        // metadata after INSERT/DELETE/UPDATE/MERGE. A transaction branch
        // write leaves `main` where it was, so only the writer's copy, which
        // pins the branch head for its next statement, is stale; `COMMIT`
        // runs outside the scope and evicts every token (openspec 2.4).
        if branch_commit_seen {
            self.session.invalidate_table(&ident).await;
        } else {
            self.session.invalidate_table_all_tokens(&ident).await;
        }
        result
    }
}

#[cfg(test)]
mod cache_capacity_tests {
    use super::{
        iceberg_error_is_forbidden, insert_s3_credential_props, insert_s3_location_props,
        insert_s3_vended_isolation_props, record_catalog_breaker_outcome, select_keys_for_suffix,
        SessionCatalog, REST_CATALOG_CACHE_MAX_CAPACITY,
    };
    use super::{
        apply_breaker_class, configure_breaker_failure_classes, record_breaker_class,
        token_subject_for_logs, BreakerFailureClass,
    };
    use sqe_core::config::StorageConfig;
    use std::collections::HashMap;

    fn storage_with_keys() -> StorageConfig {
        StorageConfig {
            s3_endpoint: "http://s3.local".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_access_key: "AKIAEXAMPLE".to_string(),
            s3_secret_key: sqe_core::SecretString::new("secret".to_string()),
            s3_path_style: true,
            ..Default::default()
        }
    }

    /// Client-side SigV4 (RustFS S3 Tables): `signing_name` on the `aws`
    /// auth block turns the switch on, the region defaults to the storage
    /// region, and a plain `aws` block (Glue, AWS S3 Tables, which advertise
    /// SigV4 themselves) adds nothing.
    #[test]
    fn client_side_sigv4_props_follow_the_aws_auth_block() {
        let storage = storage_with_keys();

        let rustfs: sqe_core::config::CatalogConfig = toml::from_str(
            r#"
catalog_url = "http://rustfs:9000/iceberg"
warehouse = "warehouse"
[auth]
type = "aws"
signing_name = "s3"
"#,
        )
        .unwrap();
        let props = super::client_side_sigv4_props(&rustfs, &storage);
        assert_eq!(props.get("rest.sigv4-enabled").map(String::as_str), Some("true"));
        assert_eq!(props.get("rest.signing-name").map(String::as_str), Some("s3"));
        assert_eq!(
            props.get("rest.signing-region").map(String::as_str),
            Some("us-east-1"),
            "region falls back to [storage] s3_region"
        );

        let explicit: sqe_core::config::CatalogConfig = toml::from_str(
            r#"
catalog_url = "http://rustfs:9000/iceberg"
warehouse = "warehouse"
[auth]
type = "aws"
signing_name = "s3"
signing_region = "eu-west-1"
"#,
        )
        .unwrap();
        assert_eq!(
            super::client_side_sigv4_props(&explicit, &storage)
                .get("rest.signing-region")
                .map(String::as_str),
            Some("eu-west-1")
        );

        let aws_native: sqe_core::config::CatalogConfig = toml::from_str(
            r#"
catalog_url = "https://glue.eu-central-1.amazonaws.com/iceberg"
warehouse = "123456789012"
[auth]
type = "aws"
"#,
        )
        .unwrap();
        assert!(
            super::client_side_sigv4_props(&aws_native, &storage).is_empty(),
            "AWS endpoints advertise SigV4 themselves; nothing is forced client-side"
        );

        let bearer: sqe_core::config::CatalogConfig =
            toml::from_str("catalog_url = \"http://polaris:8181/api/catalog\"\nwarehouse = \"w\"")
                .unwrap();
        assert!(super::client_side_sigv4_props(&bearer, &storage).is_empty());
    }

    #[tokio::test]
    async fn unconfigured_catalog_fails_fast_without_a_rest_backend() {
        let catalog = SessionCatalog::unconfigured(&StorageConfig::default(), None);
        let err = catalog
            .list_namespaces()
            .await
            .expect_err("an Iceberg operation needs a configured catalog");
        assert!(err.to_string().contains("no Iceberg catalog is configured"));
    }

    #[test]
    fn vended_mode_omits_static_s3_keys_keeps_location() {
        let storage = storage_with_keys();
        let mut props = HashMap::new();
        insert_s3_location_props(&mut props, &storage);
        insert_s3_vended_isolation_props(&mut props);
        assert_eq!(
            props.get("s3.endpoint").map(String::as_str),
            Some("http://s3.local")
        );
        assert_eq!(
            props.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            props.get("s3.path-style-access").map(String::as_str),
            Some("true")
        );
        assert!(!props.contains_key("s3.access-key-id"));
        assert!(!props.contains_key("s3.secret-access-key"));
        assert_eq!(
            props
                .get(iceberg::io::S3_DISABLE_CONFIG_LOAD)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            props
                .get(iceberg::io::S3_DISABLE_EC2_METADATA)
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn vended_mode_disables_config_load_and_ec2_metadata() {
        let mut props = HashMap::new();
        insert_s3_vended_isolation_props(&mut props);
        assert_eq!(
            props
                .get(iceberg::io::S3_DISABLE_CONFIG_LOAD)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            props
                .get(iceberg::io::S3_DISABLE_EC2_METADATA)
                .map(String::as_str),
            Some("true")
        );
        assert!(!props.contains_key("s3.access-key-id"));
        assert!(!props.contains_key("s3.secret-access-key"));
    }

    #[test]
    fn fallback_mode_injects_static_s3_keys() {
        let storage = storage_with_keys();
        let mut props = HashMap::new();
        insert_s3_credential_props(&mut props, &storage);
        assert_eq!(
            props.get("s3.access-key-id").map(String::as_str),
            Some("AKIAEXAMPLE")
        );
        assert_eq!(
            props.get("s3.secret-access-key").map(String::as_str),
            Some("secret")
        );
    }

    #[test]
    fn access_denied_closes_half_open_probe() {
        let cb = crate::circuit_breaker::CircuitBreaker::new(
            "t",
            1,
            std::time::Duration::from_millis(0),
        );
        cb.record_failure();
        assert!(cb.check().is_ok());
        assert_eq!(cb.state_label(), "half_open");
        let err: sqe_core::Result<()> =
            Err(sqe_core::SqeError::Catalog("403 Forbidden".to_string()));
        assert_eq!(
            err.as_ref().unwrap_err().error_code(),
            sqe_core::SqeErrorCode::AccessDenied
        );
        record_catalog_breaker_outcome(&cb, &err);
        assert_eq!(
            cb.state_label(),
            "closed",
            "a 403 must resolve the half-open probe, not latch it"
        );
    }

    fn five_failure_breaker() -> crate::circuit_breaker::CircuitBreaker {
        crate::circuit_breaker::CircuitBreaker::new("t", 5, std::time::Duration::from_secs(30))
    }

    fn catalog_http(status: u16) -> sqe_core::SqeError {
        sqe_core::SqeError::CatalogHttp {
            status,
            op: sqe_core::CatalogOp::LoadTable,
            body: String::new(),
        }
    }

    /// dbt's existence checks and `read_parquet` name lookups probe for
    /// objects that are missing or hidden: 404s and 403s must never lock the
    /// token out.
    #[test]
    fn not_found_and_denied_never_open_the_breaker() {
        let cb = five_failure_breaker();
        for _ in 0..20 {
            record_catalog_breaker_outcome::<()>(&cb, &Err(catalog_http(404)));
            record_catalog_breaker_outcome::<()>(&cb, &Err(catalog_http(403)));
            record_catalog_breaker_outcome::<()>(
                &cb,
                &Err(sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::SchemaNotFound,
                    "Namespace does not exist: cbs_silver",
                )),
            );
            record_breaker_class(&cb, BreakerFailureClass::from_http_status(400));
        }
        assert_eq!(cb.state_label(), "closed");
        assert!(cb.check().is_ok());
    }

    #[test]
    fn server_errors_and_timeouts_open_the_breaker() {
        let cb = five_failure_breaker();
        for _ in 0..5 {
            record_catalog_breaker_outcome::<()>(&cb, &Err(catalog_http(503)));
        }
        assert_eq!(cb.state_label(), "open");

        let cb = five_failure_breaker();
        for _ in 0..5 {
            record_catalog_breaker_outcome::<()>(
                &cb,
                &Err(sqe_core::SqeError::Catalog("operation timed out".to_string())),
            );
        }
        assert_eq!(cb.state_label(), "open");

        let cb = five_failure_breaker();
        for _ in 0..5 {
            record_breaker_class(&cb, BreakerFailureClass::Connection);
        }
        assert_eq!(cb.state_label(), "open");
    }

    /// The counted classes are configurable: an operator who wants missing
    /// objects to count can opt in, and an uncounted timeout records nothing
    /// (no answer) rather than a success.
    #[test]
    fn counted_classes_are_configurable() {
        let cb = five_failure_breaker();
        for _ in 0..5 {
            apply_breaker_class(&cb, BreakerFailureClass::NotFound, true);
        }
        assert_eq!(cb.state_label(), "open");

        let cb = five_failure_breaker();
        for _ in 0..4 {
            apply_breaker_class(&cb, BreakerFailureClass::ServerError, true);
        }
        // An uncounted timeout neither trips nor resets the run.
        apply_breaker_class(&cb, BreakerFailureClass::Timeout, false);
        apply_breaker_class(&cb, BreakerFailureClass::ServerError, true);
        assert_eq!(cb.state_label(), "open");

        assert!(configure_breaker_failure_classes(&["bogus".to_string()]).is_err());
    }

    #[test]
    fn breaker_subject_comes_from_the_jwt_payload() {
        use base64::Engine;
        let enc = |v: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v);
        let jwt = |claims: &str| format!("{}.{}.sig", enc(r#"{"alg":"RS256"}"#), enc(claims));
        assert_eq!(
            token_subject_for_logs(&jwt(r#"{"sub":"svc-dbt","preferred_username":"dbt"}"#)),
            Some("svc-dbt".to_string())
        );
        assert_eq!(
            token_subject_for_logs(&jwt(r#"{"preferred_username":"airflow"}"#)),
            Some("airflow".to_string())
        );
        assert_eq!(token_subject_for_logs("opaque-token"), None);
    }

    /// Cross-token invalidation must evict EVERY token's entry for the table,
    /// not just one. Two users (different token fingerprints) cached the same
    /// `ns.tbl`; after a tag write both keys must be selected for eviction so
    /// the new tags are visible to all users immediately (issue: stale-tags-
    /// cross-token). Entries for other tables must be left alone.
    #[test]
    fn select_keys_for_suffix_matches_all_tokens_same_table() {
        let keys = vec![
            "tokA|sales.orders".to_string(),
            "tokB|sales.orders".to_string(),
            "tokA|sales.customers".to_string(), // different table, keep
            "tokC|other.orders".to_string(),    // different namespace, keep
        ];
        let mut got = select_keys_for_suffix(keys, "sales.orders");
        got.sort();
        assert_eq!(
            got,
            vec![
                "tokA|sales.orders".to_string(),
                "tokB|sales.orders".to_string()
            ],
            "both tokens' entries for sales.orders must be selected; others untouched"
        );
    }

    /// The `|` boundary must be part of the match so a table `foo` does not
    /// evict an entry for a table `barfoo` in the same namespace.
    #[test]
    fn select_keys_for_suffix_respects_pipe_boundary() {
        let keys = vec![
            "tokA|ns.foo".to_string(),
            "tokA|ns.barfoo".to_string(), // must NOT match suffix "ns.foo"
        ];
        let got = select_keys_for_suffix(keys, "ns.foo");
        assert_eq!(got, vec!["tokA|ns.foo".to_string()]);
    }

    #[test]
    fn select_keys_for_suffix_empty_when_no_match() {
        let keys = vec!["tokA|ns.other".to_string()];
        assert!(select_keys_for_suffix(keys, "ns.missing").is_empty());
    }

    #[test]
    fn rest_catalog_keys_for_token_drops_only_that_fingerprint() {
        let keys = vec![
            "http://polaris/a-whA-aaaabbbbccccdddd".to_string(),
            "http://polaris/a-whB-aaaabbbbccccdddd".to_string(),
            "http://polaris/a-whA-eeeeffff00001111".to_string(),
        ];
        let mut got = super::rest_catalog_keys_for_token(keys, "aaaabbbbccccdddd");
        got.sort();
        assert_eq!(
            got,
            vec![
                "http://polaris/a-whA-aaaabbbbccccdddd".to_string(),
                "http://polaris/a-whB-aaaabbbbccccdddd".to_string(),
            ]
        );
    }

    #[test]
    fn rest_catalog_keys_for_token_empty_fingerprint_is_noop() {
        let keys = vec!["http://polaris/a-whA-aaaabbbbccccdddd".to_string()];
        assert!(super::rest_catalog_keys_for_token(keys, "").is_empty());
    }

    #[test]
    fn rest_catalog_token_fingerprint_is_16_hex_and_stable() {
        let fp = super::rest_catalog_token_fingerprint("bearer-token");
        assert_eq!(fp.len(), 16);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(fp, super::rest_catalog_token_fingerprint("bearer-token"));
        assert_ne!(fp, super::rest_catalog_token_fingerprint("other-token"));
        let key = format!("http://polaris-whA-{fp}");
        assert_eq!(
            super::rest_catalog_keys_for_token(vec![key.clone()], &fp),
            vec![key]
        );
    }

    #[tokio::test]
    async fn table_metadata_cache_separates_warehouses() {
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        );
        let key_a = super::table_metadata_cache_key("tok", "warehouse_a", &ident);
        let key_b = super::table_metadata_cache_key("tok", "warehouse_b", &ident);
        assert_ne!(
            key_a, key_b,
            "the warehouse must participate in the cache key"
        );

        let cache = super::TableMetadataCache::new(30);
        cache.insert(key_a, test_table()).await;
        cache.insert(key_b, test_table()).await;

        assert!(cache
            .properties_for(Some("warehouse_a"), "sales", "orders")
            .is_some());
        assert!(cache
            .properties_for(Some("warehouse_b"), "sales", "orders")
            .is_some());
        assert!(
            cache.properties_for(None, "sales", "orders").is_none(),
            "an unqualified property lookup must fail closed when two warehouses match"
        );
    }

    #[tokio::test]
    async fn table_metadata_cache_accepts_multiple_tokens_for_same_warehouse() {
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        );
        let key_alice = super::table_metadata_cache_key("tok_alice", "warehouse_a", &ident);
        let key_carol = super::table_metadata_cache_key("tok_carol", "warehouse_a", &ident);
        assert_ne!(
            key_alice, key_carol,
            "tokens must remain isolated cache entries"
        );

        let cache = super::TableMetadataCache::new(30);
        cache.insert(key_alice, test_table()).await;
        cache.insert(key_carol, test_table()).await;

        assert!(
            cache.properties_for(None, "sales", "orders").is_some(),
            "multiple principals in one warehouse must share table tag metadata"
        );
    }

    /// 303: a stale entry (past the soft TTL) held under another principal's
    /// token must never decide column masking; a fresh entry from any
    /// principal may.
    #[tokio::test]
    async fn table_metadata_cache_properties_ignore_stale_entries() {
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        );
        let key_a = super::table_metadata_cache_key("tok_a", "warehouse_a", &ident);
        let key_b = super::table_metadata_cache_key("tok_b", "warehouse_a", &ident);

        let cache = super::TableMetadataCache::new(30);
        cache
            .insert_aged(
                key_a.clone(),
                test_table(),
                std::time::Duration::from_secs(31),
            )
            .await;
        assert!(
            cache
                .properties_for(Some("warehouse_a"), "sales", "orders")
                .is_none(),
            "only a stale entry exists: tag state must be UNKNOWN (fail closed)"
        );
        assert!(cache.properties_for(None, "sales", "orders").is_none());

        cache.insert(key_b, test_table()).await;
        assert!(
            cache
                .properties_for(Some("warehouse_a"), "sales", "orders")
                .is_some(),
            "a fresh entry under another token is authoritative"
        );
        assert!(cache.properties_for(None, "sales", "orders").is_some());

        let generation = cache.generation(&key_a, "sales.orders");
        cache.revalidate(&key_a, &generation).await;
        assert!(cache
            .properties_for(Some("warehouse_a"), "sales", "orders")
            .is_some());
    }

    /// Issue #240: the REST catalog cache must hold well beyond the old cap of
    /// 100 so concurrent user-token-warehouse triples stop evicting each other
    /// and re-paying the ~250 ms `/v1/config` + `list_namespaces` rebuild. We
    /// build a moka cache with the same capacity constant and confirm it
    /// retains far more than 100 keys.
    #[tokio::test]
    async fn rest_catalog_cache_holds_well_beyond_100_keys() {
        let cache: moka::future::Cache<String, u64> = moka::future::Cache::builder()
            .max_capacity(REST_CATALOG_CACHE_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_secs(300))
            .build();

        for i in 0..500u64 {
            cache.insert(format!("url-wh-token-{i}"), i).await;
        }
        cache.run_pending_tasks().await;

        assert!(
            cache.entry_count() > 100,
            "cache should retain far more than the old cap of 100, got {}",
            cache.entry_count()
        );
    }

    /// Review 301: 10,000 rotated tokens against one catalog must not leave
    /// 10,000 breakers behind. With a short idle window the registry drains to
    /// zero once nothing touches it, and a later lookup simply rebuilds one.
    #[tokio::test]
    async fn user_circuit_breakers_drain_after_idle() {
        let registry = super::UserCircuitBreakers::new(
            std::time::Duration::from_millis(50),
            super::USER_CIRCUIT_BREAKER_MAX_CAPACITY,
        );
        for i in 0..10_000u32 {
            registry.get_or_create(&format!("{i:016x}"), "http://polaris:8181");
        }
        registry.run_pending_tasks();
        assert!(
            registry.entry_count() <= super::USER_CIRCUIT_BREAKER_MAX_CAPACITY,
            "registry exceeded its cap: {}",
            registry.entry_count()
        );
        assert!(registry.entry_count() > 0);

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        registry.run_pending_tasks();
        assert_eq!(
            registry.entry_count(),
            0,
            "idle breakers must be evicted, {} remain",
            registry.entry_count()
        );

        let again = registry.get_or_create("0000000000000001", "http://polaris:8181");
        assert_eq!(again.state_code(), 0, "a rebuilt breaker starts closed");
    }

    /// Token invalidation drops that token's breakers on every catalog and
    /// leaves other tokens alone.
    #[test]
    fn user_circuit_breakers_evict_token_is_scoped() {
        let registry = super::UserCircuitBreakers::new(
            std::time::Duration::from_secs(300),
            super::USER_CIRCUIT_BREAKER_MAX_CAPACITY,
        );
        registry.get_or_create("aaaa", "http://a");
        registry.get_or_create("aaaa", "http://b");
        registry.get_or_create("bbbb", "http://a");
        registry.run_pending_tasks();
        assert_eq!(registry.entry_count(), 3);

        assert_eq!(registry.evict_token("aaaa"), 2);
        assert_eq!(registry.evict_token(""), 0);
        registry.run_pending_tasks();
        assert_eq!(registry.entry_count(), 1);
    }

    /// The exported gauges roll per-token breakers up per catalog: worst
    /// state and open count, with no token in sight.
    #[test]
    fn user_circuit_breakers_aggregate_per_catalog() {
        let registry = super::UserCircuitBreakers::new(
            std::time::Duration::from_secs(300),
            super::USER_CIRCUIT_BREAKER_MAX_CAPACITY,
        );
        let healthy = registry.get_or_create("t1", "http://polaris");
        let tripped = registry.get_or_create("t2", "http://polaris");
        let other = registry.get_or_create("t3", "http://nessie");
        for _ in 0..5 {
            tripped.record_failure();
            other.record_failure();
        }
        assert_eq!(healthy.state_code(), 0);
        assert_eq!(tripped.state_code(), 2);

        assert_eq!(registry.aggregate_for_catalog("http://polaris"), (2, 1));
        assert_eq!(registry.aggregate_for_catalog("http://nessie"), (2, 1));
        assert_eq!(registry.aggregate_for_catalog("http://unknown"), (0, 0));
    }

    /// The metric label is the configured endpoint (or warehouse), never the
    /// breaker name that carries the token fingerprint.
    #[test]
    fn breaker_metric_label_never_contains_fingerprint() {
        let fp = super::rest_catalog_token_fingerprint("some-bearer-token");
        let catalog =
            SessionCatalog::unconfigured(&sqe_core::config::StorageConfig::default(), None);
        let label = catalog.breaker_metric_catalog().to_string();
        assert!(!label.contains(&fp));
        assert!(!label.contains(catalog.circuit_breaker.name()));
        assert!(catalog.circuit_breaker.name().contains("unconfigured"));
    }

    /// Review 300: the shared client carries both deadlines; zero is clamped
    /// rather than meaning "no deadline".
    #[test]
    fn build_catalog_http_client_accepts_zero_by_clamping() {
        let _client = super::build_catalog_http_client(0, 0);
        let _client = super::build_catalog_http_client(
            sqe_core::config::default_catalog_request_timeout_secs(),
            sqe_core::config::default_catalog_connect_timeout_secs(),
        );
    }

    /// Review 307: a successful `HEAD` is reused for the recheck window, so a
    /// burst of fresh-hit lookups on one (token, table) pays one round trip;
    /// a zero window never caches; a single-key `invalidate` (the denial
    /// path) and `invalidate_all` both drop the authorization.
    #[tokio::test]
    async fn authorization_recheck_window_is_honoured() {
        let key = "tokenA|wh|ns.t1";

        let disabled = TableMetadataCache::new(60);
        disabled.mark_authorized(key).await;
        assert!(
            !disabled.authorization_fresh(key).await,
            "zero window must re-check on every lookup"
        );

        let cache = TableMetadataCache::new(60).with_authorization_recheck(10);
        assert!(
            !cache.authorization_fresh(key).await,
            "never authorized yet"
        );
        cache.mark_authorized(key).await;
        assert!(cache.authorization_fresh(key).await);
        assert!(
            !cache.authorization_fresh("tokenB|wh|ns.t1").await,
            "another principal's bearer is never covered"
        );

        cache.invalidate(key).await;
        assert!(
            !cache.authorization_fresh(key).await,
            "a denial evicts the cached authorization"
        );

        cache.mark_authorized(key).await;
        cache.invalidate_all().await;
        assert!(!cache.authorization_fresh(key).await);
    }

    /// #816: 1,000 entries cached at one instant do not all go stale at once.
    /// Each key's soft TTL falls in [0.8, 1.0] of the configured one, spread
    /// so that no 2 s slice of that 20 s window holds more than 15% of the
    /// keys (10% is even),
    /// and never beyond the configured TTL (the policy-tag path reads only
    /// fresh entries).
    #[tokio::test]
    async fn entries_cached_together_go_stale_spread_out() {
        let ttl = std::time::Duration::from_secs(100);
        let cache = TableMetadataCache::new(100);
        let keys: Vec<String> = (0..1000).map(|i| format!("tok{i}|wh|ns.t{i}")).collect();

        let mut buckets = [0usize; 10];
        for key in &keys {
            let own = cache.soft_ttl_for(key);
            assert!(own <= ttl && own > ttl.mul_f64(0.8), "{key}: {own:?}");
            let slot = ((ttl - own).as_secs_f64() / 2.0) as usize;
            buckets[slot.min(9)] += 1;
        }
        assert!(buckets.iter().all(|&n| n <= 150), "expiry clustered: {buckets:?}");

        // The same through the read path: every entry 90 s old, so roughly
        // half are still fresh and half revalidate, instead of all of them.
        for key in &keys {
            cache
                .insert_aged(key.clone(), test_table(), std::time::Duration::from_secs(90))
                .await;
        }
        let mut fresh = 0;
        for key in &keys {
            if cache.get_fresh(key).await.is_some() {
                fresh += 1;
            }
        }
        assert!((300..=700).contains(&fresh), "fresh at 0.9 x TTL: {fresh}");
    }

    /// Minimal in-memory `Table` for cache tests: a valid v1 metadata document
    /// plus a filesystem `FileIO`. `disable_cache()` skips the per-table
    /// `ObjectCache` (no I/O). Identifier is required by the builder but the
    /// cache is keyed by the string we pass to `insert`, so its value is
    /// irrelevant to what we assert.
    pub(super) fn test_table() -> iceberg::table::Table {
        test_table_with_file_io(iceberg::io::FileIO::new_with_fs())
    }

    /// [`test_table`] whose FileIO carries `props` (e.g. vended keys).
    pub(super) fn test_table_with_props(props: &[(&str, &str)]) -> iceberg::table::Table {
        let factory = std::sync::Arc::new(iceberg::io::LocalFsStorageFactory);
        let file_io = iceberg::io::FileIOBuilder::new(factory)
            .with_props(props.iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .build();
        test_table_with_file_io(file_io)
    }

    fn test_table_with_file_io(file_io: iceberg::io::FileIO) -> iceberg::table::Table {
        const META_V1: &str = r#"{
            "format-version": 1,
            "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
            "location": "s3://bucket/test/location",
            "last-updated-ms": 1602638573874,
            "last-column-id": 1,
            "schema": {
                "type": "struct",
                "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]
            },
            "partition-spec": [],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": []
        }"#;
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(META_V1).unwrap();
        iceberg::table::Table::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "t".to_string(),
            ))
            .file_io(file_io)
            .disable_cache()
            .build()
            .unwrap()
    }

    /// `invalidate_all` must evict EVERY entry, across all token fingerprints
    /// and tables -- the process-global drop the admin catalog-refresh endpoint
    /// relies on so an out-of-band catalog rebind is visible on the next read
    /// instead of surviving the 1 hour hard TTL.
    #[tokio::test]
    async fn invalidate_all_evicts_every_table_entry() {
        let cache = TableMetadataCache::new(60);
        cache.insert("tokenA|ns.t1".to_string(), test_table()).await;
        cache.insert("tokenB|ns.t2".to_string(), test_table()).await;
        assert!(cache.get_fresh("tokenA|ns.t1").await.is_some());
        assert!(cache.get_fresh("tokenB|ns.t2").await.is_some());

        cache.invalidate_all().await;

        assert!(
            cache.get_fresh("tokenA|ns.t1").await.is_none(),
            "entry under token A survived global invalidation"
        );
        assert!(
            cache.get_fresh("tokenB|ns.t2").await.is_none(),
            "entry under token B survived global invalidation"
        );
        assert_eq!(
            cache.entry_count(),
            0,
            "cache should be empty after invalidate_all"
        );
    }

    /// A zero-TTL (disabled) cache never stores entries, so `invalidate_all` is
    /// a safe no-op rather than a panic.
    #[tokio::test]
    async fn invalidate_all_on_disabled_cache_is_noop() {
        let cache = TableMetadataCache::new(0);
        cache.insert("tokenA|ns.t1".to_string(), test_table()).await;
        cache.invalidate_all().await;
        assert_eq!(cache.entry_count(), 0);
    }

    fn expires_in(secs: i64) -> String {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        (now_ms + secs * 1000).to_string()
    }

    /// Vended credentials inside the refresh margin are never served from the
    /// cache, fresh OR stale: a stale hit would be revalidated with a 304 and
    /// handed back with dead keys. The entry is dropped so the next lookup
    /// reloads the table (and gets new keys).
    #[tokio::test]
    async fn cached_table_with_expiring_vended_credentials_is_not_served() {
        let cache = TableMetadataCache::new(60);
        let soon = expires_in(30);
        let expiring = test_table_with_props(&[
            ("s3.access-key-id", "VENDED"),
            ("s3.session-token", "tok"),
            ("s3.session-token-expires-at-ms", &soon),
        ]);
        cache
            .insert_with_etag("tok|wh|ns.t".to_string(), expiring, Some("e1".into()))
            .await;
        // 30 s left on a token obtained now: the deadline is half-life (15 s),
        // so it is still usable.
        assert!(cache.get_fresh("tok|wh|ns.t").await.is_some());

        let past = expires_in(-1);
        let expired = test_table_with_props(&[("s3.session-token-expires-at-ms", &past)]);
        let key = "tok|wh|ns.t".to_string();
        cache
            .insert_with_etag(key.clone(), expired.clone(), Some("e1".into()))
            .await;
        assert!(cache.get_fresh("tok|wh|ns.t").await.is_none());
        cache
            .insert_with_etag(key, expired, Some("e1".into()))
            .await;
        assert!(
            cache.get_stale("tok|wh|ns.t").await.is_none(),
            "an ETag revalidation must not resurrect expired keys"
        );
        cache.inner.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 0, "the expired entry is evicted");
    }

    /// Hour-long credentials are cached normally until the refresh margin;
    /// static keys (no expiry) are unaffected.
    #[tokio::test]
    async fn long_lived_and_static_credentials_are_cached() {
        let cache = TableMetadataCache::new(60);
        let hour = expires_in(3600);
        let vended = test_table_with_props(&[("s3.session-token-expires-at-ms", &hour)]);
        cache.insert("a|wh|ns.t".to_string(), vended).await;
        assert!(cache.get_fresh("a|wh|ns.t").await.is_some());
        assert!(cache.get_stale("a|wh|ns.t").await.is_some());

        // Inside the 5-minute margin of an hour-long token: refreshed.
        let entry = cache.inner.get("a|wh|ns.t").await.unwrap();
        let mut entry = entry.clone();
        entry.credentials_usable_until =
            Some(std::time::SystemTime::now() - std::time::Duration::from_secs(1));
        cache.inner.insert("a|wh|ns.t".to_string(), entry).await;
        assert!(cache.get_fresh("a|wh|ns.t").await.is_none());

        let static_keys = test_table_with_props(&[("s3.access-key-id", "STATIC")]);
        cache.insert("b|wh|ns.t".to_string(), static_keys).await;
        assert!(cache.get_fresh("b|wh|ns.t").await.is_some());
    }

    use super::{NamespaceIdent, TableIdent, TableMetadataCache};

    /// Shape 1: the typed ErrorResponse/ErrorModel path. The status only
    /// survives as a `code` context entry on a DataInvalid error.
    #[test]
    fn forbidden_detected_on_typed_error_response_shape() {
        let e = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "Not authorized")
            .with_context("type", "NotAuthorizedException")
            .with_context("code", "403");
        assert!(iceberg_error_is_forbidden(&e));
    }

    /// Shape 2: the wrapped raw HTTP status path
    /// (`deserialize_unexpected_catalog_error`).
    #[test]
    fn forbidden_detected_on_wrapped_http_status_shape() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "403 Forbidden");
        assert!(iceberg_error_is_forbidden(&e));
    }

    /// Timeout- and 5xx-shaped errors must NOT read as forbidden: the
    /// visibility probe propagates them so the listing fails (#401).
    #[test]
    fn timeout_and_5xx_shapes_are_not_forbidden() {
        let timeout = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "error sending request: operation timed out",
        );
        assert!(!iceberg_error_is_forbidden(&timeout));

        let server_err = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "500 Internal Server Error");
        assert!(!iceberg_error_is_forbidden(&server_err));

        let unauthorized = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "401 Unauthorized");
        assert!(!iceberg_error_is_forbidden(&unauthorized));
    }

    /// A 403 whose status shows only in the Debug rendering (not the Display
    /// string the message-based classifier sees) must still be classified
    /// AccessDenied, so metadata-sweep enumerators skip it quietly (#318).
    #[test]
    fn classify_iceberg_catalog_error_marks_forbidden_as_access_denied() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "403 Forbidden");
        let wrapped = super::classify_iceberg_catalog_error("Failed to list tables", e);
        assert_eq!(wrapped.error_code(), sqe_core::SqeErrorCode::AccessDenied);
        assert!(
            wrapped.client_message().contains("403"),
            "client message should carry the typed status, got {}",
            wrapped.client_message()
        );
        assert!(super::listing_error_is_forbidden(&wrapped));
    }

    /// Polaris' typed `not authorized for op 'LIST_TABLES'` answer carries the
    /// 403 in a context entry; it must classify as AccessDenied too.
    #[test]
    fn classify_iceberg_catalog_error_handles_polaris_not_authorized_shape() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Principal 'energyco-viewer' is not authorized for op 'LIST_TABLES' on namespace ontology",
        )
        .with_context("type", "NotAuthorizedException")
        .with_context("code", "403");
        let wrapped = super::classify_iceberg_catalog_error("Failed to list tables", e);
        assert!(super::listing_error_is_forbidden(&wrapped));
    }

    /// A genuine failure (5xx / timeout) is NOT forbidden: enumerators still
    /// surface it (WARN), they do not silently drop the namespace.
    #[test]
    fn classify_iceberg_catalog_error_keeps_server_errors_non_forbidden() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "500 Internal Server Error");
        let wrapped = super::classify_iceberg_catalog_error("Failed to list tables", e);
        assert!(!super::listing_error_is_forbidden(&wrapped));
        assert_eq!(
            wrapped.error_code(),
            sqe_core::SqeErrorCode::CatalogUnavailable
        );
    }

    /// sqe#803: the bridge used to drop the status, leaving the commit retry
    /// to find "status: 401" in the text. It now carries it as context too,
    /// and the SQE text is unchanged.
    #[test]
    fn bridge_error_carries_the_catalog_http_status() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "401 Unauthorized");
        let sqe = super::classify_iceberg_catalog_error("Failed to load table", e);
        let bridged = super::bridge_error(sqe);
        assert_eq!(bridged.http_status(), Some(401));
        assert!(
            bridged
                .message()
                .starts_with("Catalog HTTP 401 (other): Failed to load table: "),
            "{bridged}"
        );
        assert!(
            bridged.to_string().contains("status: 401 Unauthorized"),
            "{bridged}"
        );

        let other = super::bridge_error(sqe_core::SqeError::Catalog("boom".into()));
        assert_eq!(other.http_status(), None);
        assert_eq!(other.message(), "Catalog error: boom");
    }

    /// A table whose name contains "forbidden" with a 500 must not classify as
    /// AccessDenied — status is the only signal (finding 305).
    #[test]
    fn classify_iceberg_does_not_match_forbidden_in_table_name() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Failed to load table forbidden_words",
        )
        .with_context("status", "500 Internal Server Error");
        let wrapped = super::classify_iceberg_catalog_error("load table forbidden_words", e);
        assert_eq!(
            wrapped.error_code(),
            sqe_core::SqeErrorCode::CatalogUnavailable
        );
        assert!(!super::listing_error_is_forbidden(&wrapped));
    }

    /// #401: a successful probe is visible; 403 hides the name; timeout/5xx
    /// propagate so the listing fails instead of leaking the name.
    #[test]
    fn namespace_visible_from_probe_ok_is_true() {
        let visible = super::namespace_visible_from_probe(Ok(())).unwrap();
        assert!(visible);
    }

    #[test]
    fn namespace_visible_from_probe_403_is_false() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "403 Forbidden");
        let visible = super::namespace_visible_from_probe::<()>(Err(e)).unwrap();
        assert!(!visible);
    }

    #[test]
    fn namespace_visible_from_probe_timeout_propagates() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "error sending request: operation timed out",
        );
        let err = super::namespace_visible_from_probe::<()>(Err(e))
            .expect_err("timeout must fail the listing, not keep the name");
        assert_ne!(err.error_code(), sqe_core::SqeErrorCode::AccessDenied);
    }

    #[test]
    fn namespace_visible_from_probe_5xx_propagates() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", "500 Internal Server Error");
        let err = super::namespace_visible_from_probe::<()>(Err(e))
            .expect_err("5xx must fail the listing, not keep the name");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::CatalogUnavailable);
    }
}

/// Invalidation-scope (architecture review 302) and insert-fencing
/// (architecture review 304) tests.
///
/// Two token fingerprints cache the same table throughout: the whole point of
/// both findings is what happens to the OTHER principal's copy.
#[cfg(test)]
mod invalidation_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::cache_capacity_tests::test_table;
    use super::{
        rest_catalog_token_fingerprint, table_metadata_cache_key, SessionCatalog,
        TableMetadataCache,
    };

    const WAREHOUSE: &str = "test-warehouse";
    const NS: &str = "sales";
    const SUFFIX: &str = "sales.orders";
    /// Some other principal's cached copy of the same table.
    const OTHER_TOKEN: &str = "fingerprint_of_another_user";

    fn orders() -> TableIdent {
        TableIdent::new(NamespaceIdent::new(NS.to_string()), "orders".to_string())
    }

    fn orders_v2() -> TableIdent {
        TableIdent::new(NamespaceIdent::new(NS.to_string()), "orders_v2".to_string())
    }

    fn key_for(fingerprint: &str, ident: &TableIdent) -> String {
        table_metadata_cache_key(fingerprint, WAREHOUSE, ident)
    }

    fn unsupported<T>() -> iceberg::Result<T> {
        Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "not exercised by the invalidation tests",
        ))
    }

    /// Catalog backend stub. Mutations succeed without a server so the bridge's
    /// eviction can be asserted per DDL verb, and `load_table` can be held open
    /// so an invalidation can be made to land strictly inside a load.
    #[derive(Debug, Default)]
    struct StubCatalog {
        block_load: bool,
        /// Fired as `load_table` enters, before it blocks.
        entered: tokio::sync::Notify,
        /// Awaited by a blocking `load_table` before it returns.
        release: tokio::sync::Notify,
    }

    impl StubCatalog {
        fn blocking() -> Self {
            Self {
                block_load: true,
                ..Self::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl Catalog for StubCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            Ok(Vec::new())
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            unsupported()
        }

        async fn get_namespace(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            unsupported()
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            Ok(true)
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unsupported()
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            unsupported()
        }

        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            Ok(Vec::new())
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            Ok(test_table())
        }

        async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
            if self.block_load {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(test_table())
        }

        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Ok(())
        }

        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Ok(())
        }

        async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
            Ok(true)
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            Ok(())
        }

        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            Ok(test_table())
        }

        async fn update_table(
            &self,
            _commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            Ok(test_table())
        }
    }

    /// A cache holding `idents` under the writer's fingerprint AND under a
    /// second principal's, which is the copy every one of these tests is about.
    async fn cache_with_two_tokens(writer: &str, idents: &[TableIdent]) -> TableMetadataCache {
        let cache = TableMetadataCache::new(300);
        for ident in idents {
            cache.insert(key_for(writer, ident), test_table()).await;
            cache
                .insert(key_for(OTHER_TOKEN, ident), test_table())
                .await;
            // Guard against a vacuous assertion later: an eviction can only be
            // proved by entries that were really there to begin with.
            assert!(
                cache.get_fresh(&key_for(writer, ident)).await.is_some(),
                "seeding the writer's entry for {ident} failed"
            );
            assert!(
                cache
                    .get_fresh(&key_for(OTHER_TOKEN, ident))
                    .await
                    .is_some(),
                "seeding the other principal's entry for {ident} failed"
            );
        }
        cache
    }

    async fn assert_evicted_for_both_tokens(
        cache: &TableMetadataCache,
        writer: &str,
        ident: &TableIdent,
    ) {
        assert!(
            cache.get_fresh(&key_for(writer, ident)).await.is_none(),
            "the committer's own entry for {ident} survived"
        );
        assert!(
            cache
                .get_fresh(&key_for(OTHER_TOKEN, ident))
                .await
                .is_none(),
            "another principal's entry for {ident} survived: they keep planning \
             against pre-commit metadata until the soft TTL (finding 302)"
        );
    }

    fn bridged(
        backend: Arc<dyn Catalog>,
        cache: TableMetadataCache,
        fingerprint: &str,
    ) -> Arc<super::SessionCatalogBridge> {
        let session = Arc::new(SessionCatalog::for_test_with_backend(
            backend,
            cache,
            fingerprint,
        ));
        session.as_catalog()
    }

    fn creation(name: &str) -> TableCreation {
        TableCreation::builder()
            .name(name.to_string())
            .schema(test_table().metadata().current_schema().as_ref().clone())
            .build()
    }

    #[tokio::test]
    async fn bridge_create_table_evicts_every_token() {
        let cache = cache_with_two_tokens("tok_create", &[orders()]).await;
        let bridge = bridged(
            Arc::new(StubCatalog::default()),
            cache.clone(),
            "tok_create",
        );

        bridge
            .create_table(&NamespaceIdent::new(NS.to_string()), creation("orders"))
            .await
            .expect("stub create_table succeeds");

        assert_evicted_for_both_tokens(&cache, "tok_create", &orders()).await;
    }

    #[tokio::test]
    async fn bridge_drop_table_evicts_every_token() {
        let cache = cache_with_two_tokens("tok_drop", &[orders()]).await;
        let bridge = bridged(Arc::new(StubCatalog::default()), cache.clone(), "tok_drop");

        bridge
            .drop_table(&orders())
            .await
            .expect("stub drop_table succeeds");

        assert_evicted_for_both_tokens(&cache, "tok_drop", &orders()).await;
    }

    #[tokio::test]
    async fn bridge_rename_table_evicts_every_token_for_source_and_destination() {
        let cache = cache_with_two_tokens("tok_rename", &[orders(), orders_v2()]).await;
        let bridge = bridged(
            Arc::new(StubCatalog::default()),
            cache.clone(),
            "tok_rename",
        );

        bridge
            .rename_table(&orders(), &orders_v2())
            .await
            .expect("stub rename_table succeeds");

        assert_evicted_for_both_tokens(&cache, "tok_rename", &orders()).await;
        assert_evicted_for_both_tokens(&cache, "tok_rename", &orders_v2()).await;
    }

    #[tokio::test]
    async fn bridge_register_table_evicts_every_token() {
        let cache = cache_with_two_tokens("tok_register", &[orders()]).await;
        let bridge = bridged(
            Arc::new(StubCatalog::default()),
            cache.clone(),
            "tok_register",
        );

        bridge
            .register_table(&orders(), "s3://bucket/meta.json".to_string())
            .await
            .expect("stub register_table succeeds");

        assert_evicted_for_both_tokens(&cache, "tok_register", &orders()).await;
    }

    #[tokio::test]
    async fn bridge_update_table_evicts_every_token() {
        // `update_table` already evicted all tokens before finding 302; the
        // test pins the behaviour the other four verbs were aligned to.
        let cache = cache_with_two_tokens("tok_update", &[orders()]).await;
        let session = Arc::new(SessionCatalog::for_test_with_backend(
            Arc::new(StubCatalog::default()),
            cache.clone(),
            "tok_update",
        ));

        session.invalidate_table_all_tokens(&orders()).await;

        assert_evicted_for_both_tokens(&cache, "tok_update", &orders()).await;
    }

    /// Schema evolution (`ALTER TABLE ADD/DROP/RENAME COLUMN`, `SET
    /// TBLPROPERTIES`) used to evict only the committer's key, so the author
    /// saw the new column and everybody else got "column not found".
    #[tokio::test]
    async fn commit_schema_update_evicts_every_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/test-warehouse/namespaces/sales/tables/orders"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;

        let bearer = "commit-schema-update-token";
        let fingerprint = rest_catalog_token_fingerprint(bearer);
        let cache = cache_with_two_tokens(&fingerprint, &[orders()]).await;

        let session = SessionCatalog::new(
            &server.uri(),
            WAREHOUSE,
            bearer,
            &sqe_core::config::StorageConfig::default(),
            Some(cache.clone()),
            None,
            None,
            false,
        )
        .await
        .expect("construct session catalog");

        session
            .commit_schema_update(&orders(), Vec::new(), Vec::new())
            .await
            .expect("mocked commit succeeds");

        assert_evicted_for_both_tokens(&cache, &fingerprint, &orders()).await;
    }

    /// Positive control for the test below: the identical blocked load, with no
    /// invalidation, DOES populate the cache. Without this, an eviction
    /// assertion would pass just as happily against a load that never caches.
    #[tokio::test]
    async fn load_blocked_without_an_invalidation_populates_the_cache() {
        let cache = TableMetadataCache::new(300);
        let stub = Arc::new(StubCatalog::blocking());
        let session = Arc::new(SessionCatalog::for_test_with_backend(
            stub.clone(),
            cache.clone(),
            "tok_unblocked",
        ));
        let key = key_for("tok_unblocked", &orders());

        let loader = {
            let session = Arc::clone(&session);
            tokio::spawn(async move { session.load_table(&orders()).await })
        };

        stub.entered.notified().await;
        stub.release.notify_one();
        loader.await.expect("loader task").expect("load succeeds");

        assert!(
            cache.get_fresh(&key).await.is_some(),
            "the load path must cache when nothing invalidated"
        );
    }

    /// Finding 304, the case the eviction alone cannot cover: while the reader
    /// is inside the catalog fetch there is no entry for the writer's eviction
    /// to remove, so only the generation fence can stop the pre-commit snapshot
    /// from being stored and marked fresh for a whole soft TTL.
    #[tokio::test]
    async fn load_blocked_across_an_invalidation_does_not_repopulate_the_cache() {
        let cache = TableMetadataCache::new(300);
        let stub = Arc::new(StubCatalog::blocking());
        let session = Arc::new(SessionCatalog::for_test_with_backend(
            stub.clone(),
            cache.clone(),
            "tok_blocked",
        ));
        let key = key_for("tok_blocked", &orders());

        let loader = {
            let session = Arc::clone(&session);
            tokio::spawn(async move { session.load_table(&orders()).await })
        };

        // The load is now inside the backend fetch, holding the pre-commit
        // metadata it is about to return.
        stub.entered.notified().await;
        cache.invalidate_table_all_tokens(SUFFIX).await;
        stub.release.notify_one();

        loader
            .await
            .expect("loader task")
            .expect("the blocked load still answers its own caller");

        assert!(
            cache.get_fresh(&key).await.is_none(),
            "a load that started before the commit repopulated the cache with \
             pre-commit metadata (finding 304)"
        );
        assert!(
            cache.get_stale(&key).await.is_none(),
            "the stale copy is what ETag revalidation would rebuild from"
        );
    }

    /// Control for the fenced-insert tests: the unfenced primitive — what the
    /// load path called before finding 304 — puts the evicted entry straight
    /// back and stamps it fresh. Without this, "the entry is gone" could pass
    /// against a cache that simply never stores anything.
    #[tokio::test]
    async fn unfenced_insert_after_an_invalidation_resurrects_the_entry() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_unfenced", &orders());

        cache.invalidate_table_all_tokens(SUFFIX).await;
        cache
            .insert_with_etag(key.clone(), test_table(), None)
            .await;

        assert!(
            cache.get_fresh(&key).await.is_some(),
            "the unfenced insert is the behaviour finding 304 is about; if this \
             stops resurrecting the entry the fenced tests prove nothing"
        );
    }

    /// Control for the all-token eviction tests: evicting one key — what every
    /// commit path did before finding 302 — leaves the other principal's copy
    /// in place.
    #[tokio::test]
    async fn single_key_invalidate_leaves_other_principals_cached() {
        let cache = cache_with_two_tokens("tok_single", &[orders()]).await;

        cache.invalidate(&key_for("tok_single", &orders())).await;

        assert!(
            cache
                .get_fresh(&key_for("tok_single", &orders()))
                .await
                .is_none(),
            "the targeted key must go"
        );
        assert!(
            cache
                .get_fresh(&key_for(OTHER_TOKEN, &orders()))
                .await
                .is_some(),
            "single-key eviction leaving other principals cached is the \
             behaviour finding 302 is about"
        );
    }

    #[tokio::test]
    async fn fenced_insert_on_a_cold_key_is_refused_after_a_commit() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_cold", &orders());

        // Nothing cached yet: this is exactly the window the all-token eviction
        // scan cannot see.
        let generation = cache.generation(&key, SUFFIX);
        cache.invalidate_table_all_tokens(SUFFIX).await;

        assert!(
            !cache
                .insert_if_generation(key.clone(), &generation, test_table(), None)
                .await,
            "the insert must be refused"
        );
        assert!(cache.get_fresh(&key).await.is_none());
    }

    #[tokio::test]
    async fn fenced_insert_is_refused_after_a_single_key_invalidate() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_revoked", &orders());

        let generation = cache.generation(&key, SUFFIX);
        // The authorization-revoke path: one principal's entry, dropped because
        // their vended credentials are no longer valid.
        cache.invalidate(&key).await;

        assert!(
            !cache
                .insert_if_generation(key.clone(), &generation, test_table(), None)
                .await
        );
        assert!(cache.get_fresh(&key).await.is_none());
    }

    #[tokio::test]
    async fn fenced_insert_is_refused_after_invalidate_all() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_refresh", &orders());

        let generation = cache.generation(&key, SUFFIX);
        cache.invalidate_all().await;

        assert!(
            !cache
                .insert_if_generation(key.clone(), &generation, test_table(), None)
                .await,
            "an admin catalog refresh must not be undone by a load already in flight"
        );
        assert!(cache.get_fresh(&key).await.is_none());
    }

    #[tokio::test]
    async fn fenced_insert_succeeds_when_nothing_was_invalidated() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_quiet", &orders());
        let generation = cache.generation(&key, SUFFIX);

        assert!(
            cache
                .insert_if_generation(key.clone(), &generation, test_table(), None)
                .await,
            "the fence must not block the ordinary path"
        );
        assert!(cache.get_fresh(&key).await.is_some());
    }

    #[tokio::test]
    async fn update_etag_after_invalidate_does_not_resurrect_the_entry() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_etag", &orders());
        cache.insert(key.clone(), test_table()).await;

        // The background HEAD task snapshots when the entry is inserted and can
        // land arbitrarily later.
        let generation = cache.generation(&key, SUFFIX);
        cache.invalidate_table_all_tokens(SUFFIX).await;
        cache
            .update_etag(&key, &generation, Some("etag-after-commit".to_string()))
            .await;

        assert!(
            cache.get_fresh(&key).await.is_none(),
            "the ETag task put the evicted entry back (finding 304)"
        );
        assert!(cache.get_stale(&key).await.is_none());
    }

    #[tokio::test]
    async fn revalidate_after_invalidate_does_not_undo_it() {
        let cache = TableMetadataCache::new(300);
        let key = key_for("tok_reval", &orders());
        cache.insert(key.clone(), test_table()).await;

        // Snapshot taken before the conditional GET, 304 processed after a
        // commit evicted the entry.
        let generation = cache.generation(&key, SUFFIX);
        cache.invalidate_table_all_tokens(SUFFIX).await;
        cache.revalidate(&key, &generation).await;

        assert!(
            cache.get_fresh(&key).await.is_none(),
            "revalidate re-inserted an evicted entry and stamped it fresh"
        );
    }

    /// The same pair with no imposed ordering: whatever the runtime interleaves
    /// at the awaits inside `revalidate` and `invalidate_table_all_tokens`, the
    /// invalidation must win. Spinning the revalidator a varying number of
    /// yields first samples the orderings a fixed-order test cannot.
    #[tokio::test]
    async fn revalidate_racing_invalidate_never_leaves_an_entry() {
        for spins in 0..8 {
            let cache = TableMetadataCache::new(300);
            let key = key_for("tok_race", &orders());
            cache.insert(key.clone(), test_table()).await;
            let generation = cache.generation(&key, SUFFIX);

            let revalidator = {
                let cache = cache.clone();
                let key = key.clone();
                tokio::spawn(async move {
                    for _ in 0..spins {
                        tokio::task::yield_now().await;
                    }
                    cache.revalidate(&key, &generation).await;
                })
            };
            cache.invalidate_table_all_tokens(SUFFIX).await;
            revalidator.await.expect("revalidator task");

            assert!(
                cache.get_fresh(&key).await.is_none(),
                "revalidate undid the invalidation at spins={spins}"
            );
        }
    }
}

/// Architecture review 311: view version/dialect selection, percent-encoded
/// REST paths, and the shared `rest_request` plumbing (breaker + metrics).
/// The `CatalogHandle::Other` arms of `dispatch_catalog!` and the non-REST
/// session constructor, driven by an in-process `iceberg::Catalog` mock
/// (architecture review 313). The live Glue / HMS / JDBC / S3 Tables tests in
/// `tests/backends_integration.rs` stay `#[ignore]`d; this is the coverage
/// that runs under plain `cargo test`.
#[cfg(test)]
mod other_backend_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

    use super::cache_capacity_tests::test_table;
    use super::{rest_catalog_token_fingerprint, SessionCatalog, TableMetadataCache};
    use sqe_core::config::{CatalogBackend, CatalogConfig, StorageConfig};

    const BEARER: &str = "other-backend-bearer";

    fn sales() -> NamespaceIdent {
        NamespaceIdent::new("sales".to_string())
    }

    fn orders() -> TableIdent {
        TableIdent::new(sales(), "orders".to_string())
    }

    fn hms_config() -> CatalogConfig {
        let mut config: CatalogConfig = toml::from_str("warehouse = \"hms-warehouse\"").unwrap();
        config.backend = CatalogBackend::Hms {
            uri: "metastore:9083".to_string(),
            warehouse: "s3://hms-warehouse".to_string(),
        };
        config
    }

    /// Records every trait method the session dispatches into it.
    #[derive(Debug, Default)]
    struct RecordingCatalog {
        calls: Mutex<Vec<&'static str>>,
    }

    impl RecordingCatalog {
        fn record(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Catalog for RecordingCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.record("list_namespaces");
            Ok(vec![sales()])
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.record("create_namespace");
            Ok(iceberg::Namespace::with_properties(
                namespace.clone(),
                properties,
            ))
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.record("get_namespace");
            Ok(iceberg::Namespace::new(namespace.clone()))
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.record("namespace_exists");
            Ok(true)
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.record("update_namespace");
            Ok(())
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            self.record("drop_namespace");
            Ok(())
        }

        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.record("list_tables");
            Ok(vec![orders()])
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.record("create_table");
            Ok(test_table())
        }

        async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
            self.record("load_table");
            Ok(test_table())
        }

        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            self.record("drop_table");
            Ok(())
        }

        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            self.record("purge_table");
            Ok(())
        }

        async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
            self.record("table_exists");
            Ok(true)
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            self.record("rename_table");
            Ok(())
        }

        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.record("register_table");
            Ok(test_table())
        }

        async fn update_table(
            &self,
            _commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.record("update_table");
            Ok(test_table())
        }
    }

    fn session_over(backend: Arc<RecordingCatalog>, cache: TableMetadataCache) -> SessionCatalog {
        let config = hms_config();
        SessionCatalog::from_backend_catalog(
            backend,
            &config,
            &StorageConfig::default(),
            BEARER,
            cache,
            &config.backend.clone(),
        )
    }

    /// The non-REST constructor produces an `Other` handle keyed by the
    /// bearer fingerprint, with no REST URL and the configured warehouse.
    #[test]
    fn from_backend_catalog_builds_an_other_handle() {
        let backend = Arc::new(RecordingCatalog::default());
        let session = session_over(backend.clone(), TableMetadataCache::new(0));

        assert!(!session.is_rest_backend());
        assert_eq!(session.catalog_url(), "");
        assert_eq!(session.warehouse(), "hms-warehouse");
        assert_eq!(
            session.token_fingerprint(),
            rest_catalog_token_fingerprint(BEARER)
        );
        assert!(
            backend.calls().is_empty(),
            "construction makes no catalog call"
        );
    }

    /// Every read-side `SessionCatalog` method reaches the trait object
    /// through the `Other` arm, and a cached `load_table` is served without
    /// a second backend call or any HTTP authorization probe.
    #[tokio::test]
    async fn other_handle_dispatches_reads_through_the_trait_object() {
        let backend = Arc::new(RecordingCatalog::default());
        let session = session_over(
            backend.clone(),
            TableMetadataCache::new(300).with_authorization_recheck(300),
        );

        assert_eq!(session.list_namespaces().await.unwrap(), vec![sales()]);
        assert_eq!(
            session.get_namespace(&sales()).await.unwrap().name(),
            &sales()
        );
        assert!(session.namespace_visible(&sales()).await.unwrap());
        assert_eq!(session.list_tables(&sales()).await.unwrap(), vec![orders()]);
        assert!(session.table_exists(&orders()).await.unwrap());
        // No per-user catalog authorization exists on a shared-identity
        // backend: the probe is a no-op, not an HTTP call to an empty URL.
        session.authorize_table_access(&orders()).await.unwrap();

        let first = session.load_table(&orders()).await.unwrap();
        let second = session.load_table(&orders()).await.unwrap();
        assert_eq!(first.metadata().uuid(), second.metadata().uuid());

        assert_eq!(
            backend.calls(),
            [
                "list_namespaces",
                "get_namespace",
                "get_namespace",
                "list_tables",
                "table_exists",
                "load_table",
            ],
            "the second load_table is a cache hit"
        );
    }

    /// With the cache disabled every `load_table` goes to the backend.
    #[tokio::test]
    async fn other_handle_reloads_when_the_cache_is_disabled() {
        let backend = Arc::new(RecordingCatalog::default());
        let session = session_over(backend.clone(), TableMetadataCache::new(0));

        session.load_table(&orders()).await.unwrap();
        session.load_table(&orders()).await.unwrap();

        assert_eq!(backend.calls(), ["load_table", "load_table"]);
    }

    /// REST-only surfaces refuse with a typed error naming the backend and
    /// never touch the trait object.
    #[tokio::test]
    async fn other_handle_rejects_rest_only_operations() {
        let backend = Arc::new(RecordingCatalog::default());
        let session = session_over(backend.clone(), TableMetadataCache::new(0));
        let schema = serde_json::json!({"type": "struct", "fields": []});
        let props = HashMap::new();

        let errors = [
            session
                .create_view(&sales(), "v", "SELECT 1", &schema, &props)
                .await
                .err(),
            session
                .create_or_replace_view(&sales(), "v", "SELECT 1", &schema, &props)
                .await
                .err(),
            session.list_views(&sales()).await.err(),
            session.load_view_sql(&sales(), "v").await.err(),
            session.drop_view(&sales(), "v").await.err(),
            session
                .commit_schema_update(&orders(), Vec::new(), Vec::new())
                .await
                .err(),
        ];
        for err in errors {
            let err = err.expect("REST-only operation must fail on an Other handle");
            let text = err.to_string();
            assert!(text.contains("requires the REST catalog backend"), "{text}");
            assert!(text.contains("Other"), "{text}");
        }
        assert!(backend.calls().is_empty());
    }

    /// The `SessionCatalogBridge` forwards every namespace and table
    /// mutation into the `Other` arm as well.
    #[tokio::test]
    async fn bridge_over_other_handle_dispatches_every_mutation() {
        let backend = Arc::new(RecordingCatalog::default());
        let session = Arc::new(session_over(backend.clone(), TableMetadataCache::new(300)));
        let bridge = session.as_catalog();

        bridge
            .create_namespace(&sales(), HashMap::new())
            .await
            .unwrap();
        assert!(bridge.namespace_exists(&sales()).await.unwrap());
        bridge
            .update_namespace(&sales(), HashMap::new())
            .await
            .unwrap();
        assert_eq!(bridge.list_namespaces(None).await.unwrap(), vec![sales()]);
        assert_eq!(bridge.list_tables(&sales()).await.unwrap(), vec![orders()]);
        let creation = TableCreation::builder()
            .name("orders".to_string())
            .schema(test_table().metadata().current_schema().as_ref().clone())
            .build();
        bridge.create_table(&sales(), creation).await.unwrap();
        bridge.load_table(&orders()).await.unwrap();
        assert!(bridge.table_exists(&orders()).await.unwrap());
        bridge
            .rename_table(
                &orders(),
                &TableIdent::new(sales(), "orders_v2".to_string()),
            )
            .await
            .unwrap();
        bridge
            .register_table(&orders(), "s3://bucket/meta.json".to_string())
            .await
            .unwrap();
        bridge.drop_table(&orders()).await.unwrap();
        bridge.drop_namespace(&sales()).await.unwrap();

        assert_eq!(
            backend.calls(),
            [
                "create_namespace",
                "namespace_exists",
                "update_namespace",
                "list_namespaces",
                "list_tables",
                "create_table",
                "load_table",
                "table_exists",
                "rename_table",
                "register_table",
                "drop_table",
                "drop_namespace",
            ]
        );
    }

    /// `for_session_with` on a non-REST backend the build did not compile in
    /// is a typed configuration error naming the missing cargo feature.
    #[cfg(not(feature = "hms"))]
    #[tokio::test]
    async fn for_session_with_hms_without_the_feature_names_the_feature() {
        let err = SessionCatalog::for_session_with(
            &hms_config(),
            &StorageConfig::default(),
            None,
            BEARER,
        )
        .await
        .expect_err("HMS is not compiled in");
        let text = err.to_string();
        assert!(text.contains("`hms` cargo feature"), "{text}");
    }

    /// `build_backend_catalog` refuses the REST variant: REST sessions are
    /// built by `for_session`, never through the loader.
    #[tokio::test]
    async fn build_backend_catalog_rejects_rest() {
        let err = SessionCatalog::build_backend_catalog(&CatalogBackend::Rest)
            .await
            .expect_err("REST is not a loader backend");
        assert!(err.to_string().contains("non-REST"), "{err}");
    }
}

#[cfg(test)]
mod view_rest_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use iceberg::{NamespaceIdent, TableIdent};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        select_view_sql, sqe_view_version, view_replace_updates, view_schema_from_json,
        SessionCatalog, TableMetadataCache,
    };
    use crate::circuit_breaker::CircuitBreaker;

    const WAREHOUSE: &str = "test-warehouse";

    fn sales() -> NamespaceIdent {
        NamespaceIdent::new("sales".to_string())
    }

    fn view_schema_json() -> serde_json::Value {
        serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "x", "required": false, "type": "long"}]
        })
    }

    /// A view document with two versions. `current` picks the active one;
    /// `reps` are the `(dialect, sql)` representations of version 1.
    fn view_metadata_json(current: i32, v1_reps: &[(&str, &str)]) -> serde_json::Value {
        let reps: Vec<serde_json::Value> = v1_reps
            .iter()
            .map(
                |(dialect, sql)| serde_json::json!({"type": "sql", "sql": sql, "dialect": dialect}),
            )
            .collect();
        serde_json::json!({
            "format-version": 1,
            "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
            "location": "s3://bucket/sales/v",
            "current-version-id": current,
            "versions": [
                {
                    "version-id": 1,
                    "schema-id": 0,
                    "timestamp-ms": 1,
                    "summary": {"engine-name": "sqe"},
                    "representations": reps,
                    "default-namespace": ["sales"]
                },
                {
                    "version-id": 2,
                    "schema-id": 0,
                    "timestamp-ms": 2,
                    "summary": {"engine-name": "spark"},
                    "representations": [
                        {"type": "sql", "sql": "SELECT 2 AS x", "dialect": "spark"}
                    ],
                    "default-namespace": ["sales"]
                }
            ],
            "version-log": [{"version-id": 1, "timestamp-ms": 1}],
            "schemas": [view_schema_json()],
            "properties": {}
        })
    }

    fn metadata(current: i32, v1_reps: &[(&str, &str)]) -> iceberg::spec::ViewMetadata {
        serde_json::from_value(view_metadata_json(current, v1_reps)).expect("valid view document")
    }

    #[test]
    fn current_version_id_wins_over_last_version() {
        let md = metadata(1, &[("sqe", "SELECT 1 AS x")]);
        assert_eq!(
            select_view_sql(&md, "v").unwrap(),
            "SELECT 1 AS x",
            "current-version-id points at version 1; versions.last() would be version 2"
        );
        let md = metadata(2, &[("sqe", "SELECT 1 AS x")]);
        assert_eq!(select_view_sql(&md, "v").unwrap(), "SELECT 2 AS x");
    }

    #[test]
    fn dialect_preference_is_sqe_then_trino_then_spark() {
        let md = metadata(
            1,
            &[
                ("trino", "SELECT 't'"),
                ("spark", "SELECT 's'"),
                ("SQE", "SELECT 'q'"),
            ],
        );
        assert_eq!(select_view_sql(&md, "v").unwrap(), "SELECT 'q'");
        let md = metadata(1, &[("spark", "SELECT 's'"), ("trino", "SELECT 't'")]);
        assert_eq!(select_view_sql(&md, "v").unwrap(), "SELECT 't'");
    }

    #[test]
    fn unsupported_dialect_is_a_typed_error_not_planned_sql() {
        let md = metadata(1, &[("hive", "SELECT 1")]);
        let err = select_view_sql(&md, "v").expect_err("hive-only view must be refused");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::NotSupported);
        let msg = err.to_string();
        assert!(msg.contains("Unsupported view dialect"), "{msg}");
        assert!(msg.contains("hive"), "{msg}");
    }

    #[test]
    fn rest_paths_are_percent_encoded() {
        let session = SessionCatalog::for_test_with_backend(
            Arc::new(super::UnconfiguredCatalog),
            TableMetadataCache::new(0),
            "tok_paths",
        );
        let ns = NamespaceIdent::from_vec(vec!["a".to_string(), "b c".to_string()]).unwrap();
        let url = session.view_url(&ns, "x/y?z%");
        assert_eq!(
            url,
            "http://127.0.0.1:1/v1/test-warehouse/namespaces/a%1Fb%20c/views/x%2Fy%3Fz%25"
        );
        let table = session.table_url(&TableIdent::new(ns, "t".to_string()));
        assert!(table.ends_with("/namespaces/a%1Fb%20c/tables/t"), "{table}");
    }

    async fn session_with(
        server: &MockServer,
        cache: TableMetadataCache,
        breaker: Option<Arc<CircuitBreaker>>,
    ) -> SessionCatalog {
        SessionCatalog::new(
            &server.uri(),
            WAREHOUSE,
            "view-rest-token",
            &sqe_core::config::StorageConfig::default(),
            Some(cache),
            None,
            breaker,
            false,
        )
        .await
        .expect("construct session catalog")
    }

    /// `create_view` posts a typed `CreateViewRequest` and shows up in
    /// `sqe_catalog_request_duration_seconds{op="create_view"}`.
    #[tokio::test]
    async fn create_view_records_the_catalog_call_metric() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/test-warehouse/namespaces/sales/views"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let cache = TableMetadataCache::new(300).with_metrics(Arc::clone(&metrics));
        let session = session_with(&server, cache, None).await;

        session
            .create_view(
                &sales(),
                "v",
                "SELECT 1 AS x",
                &view_schema_json(),
                &HashMap::from([("comment".to_string(), "c".to_string())]),
            )
            .await
            .expect("mocked create succeeds");

        let observed = metrics
            .catalog_request_duration_seconds
            .with_label_values(&["create_view", "ok"])
            .get_sample_count();
        assert_eq!(
            observed, 1,
            "create_view must be visible in the catalog latency histogram"
        );

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["name"], "v");
        assert_eq!(body["schema"]["schema-id"], 0);
        assert_eq!(body["view-version"]["version-id"], 1);
        assert_eq!(body["view-version"]["representations"][0]["dialect"], "sqe");
        assert_eq!(
            body["view-version"]["representations"][0]["sql"],
            "SELECT 1 AS x"
        );
        assert_eq!(body["view-version"]["default-catalog"], WAREHOUSE);
        assert_eq!(
            body["view-version"]["default-namespace"],
            serde_json::json!(["sales"])
        );
        assert_eq!(body["properties"]["comment"], "c");
        assert!(
            requests[0].headers.get("x-request-id").is_some(),
            "raw REST calls carry a request id"
        );
    }

    /// An open breaker refuses the call before any bytes hit the wire and is
    /// reported as the typed `CircuitBreakerOpen` code.
    #[tokio::test]
    async fn create_view_respects_an_open_breaker() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        let breaker = Arc::new(CircuitBreaker::new(
            "view-test",
            1,
            Duration::from_secs(3600),
        ));
        breaker.record_failure();
        assert!(breaker.check().is_err(), "precondition: breaker is open");
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let cache = TableMetadataCache::new(300).with_metrics(Arc::clone(&metrics));
        let session = session_with(&server, cache, Some(breaker)).await;

        let err = session
            .create_view(
                &sales(),
                "v",
                "SELECT 1",
                &view_schema_json(),
                &HashMap::new(),
            )
            .await
            .expect_err("open breaker must refuse");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::CircuitBreakerOpen);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            0,
            "nothing may reach the catalog while the breaker is open"
        );
        assert_eq!(
            metrics
                .catalog_request_duration_seconds
                .with_label_values(&["create_view", "err"])
                .get_sample_count(),
            0,
            "a refused call is not a catalog round trip"
        );
    }

    /// `load_view_sql` parses the typed document and applies the version and
    /// dialect selection; 404 is `None`, 403 is a typed denial (not "not
    /// found").
    #[tokio::test]
    async fn load_view_sql_selects_current_version_and_maps_status() {
        let server = MockServer::start().await;
        let doc = serde_json::json!({
            "metadata-location": "s3://bucket/sales/v/metadata/v2.json",
            "metadata": view_metadata_json(1, &[("sqe", "SELECT 1 AS x")]),
        });
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces/sales/views/v"))
            .respond_with(ResponseTemplate::new(200).set_body_json(doc))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces/sales/views/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces/sales/views/secret"))
            .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
            .mount(&server)
            .await;
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let cache = TableMetadataCache::new(300).with_metrics(Arc::clone(&metrics));
        let session = session_with(&server, cache, None).await;

        assert_eq!(
            session.load_view_sql(&sales(), "v").await.unwrap(),
            Some("SELECT 1 AS x".to_string())
        );
        assert_eq!(
            session.load_view_sql(&sales(), "missing").await.unwrap(),
            None
        );
        let err = session
            .load_view_sql(&sales(), "secret")
            .await
            .expect_err("403 must not read as not-found");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::AccessDenied);
        assert!(!err.is_not_found());

        let ok = metrics
            .catalog_request_duration_seconds
            .with_label_values(&["load_view", "ok"])
            .get_sample_count();
        let failed = metrics
            .catalog_request_duration_seconds
            .with_label_values(&["load_view", "err"])
            .get_sample_count();
        assert_eq!((ok, failed), (1, 2));
    }

    /// `CREATE OR REPLACE VIEW` on an existing view is one `replaceView`
    /// commit carrying `assert-view-uuid` and a new version, never a drop.
    #[tokio::test]
    async fn create_or_replace_view_commits_a_new_version() {
        let server = MockServer::start().await;
        let doc = serde_json::json!({
            "metadata-location": "s3://bucket/sales/v/metadata/v2.json",
            "metadata": view_metadata_json(2, &[("sqe", "SELECT 1 AS x")]),
        });
        Mock::given(method("GET"))
            .and(path("/v1/test-warehouse/namespaces/sales/views/v"))
            .respond_with(ResponseTemplate::new(200).set_body_json(doc))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/test-warehouse/namespaces/sales/views/v"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        let session = session_with(&server, TableMetadataCache::new(300), None).await;

        session
            .create_or_replace_view(
                &sales(),
                "v",
                "SELECT 3 AS x",
                &view_schema_json(),
                &HashMap::new(),
            )
            .await
            .expect("mocked replace succeeds");

        let requests = server.received_requests().await.unwrap();
        let posts: Vec<_> = requests
            .iter()
            .filter(|r| r.method == wiremock::http::Method::POST)
            .collect();
        assert_eq!(posts.len(), 1, "exactly one commit, no drop/create");
        let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
        assert_eq!(body["requirements"][0]["type"], "assert-view-uuid");
        assert_eq!(
            body["requirements"][0]["uuid"],
            "fa6506c3-7681-40c8-86dc-e36561f83385"
        );
        let actions: Vec<&str> = body["updates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["action"].as_str().unwrap())
            .collect();
        // The schema is unchanged, so no add-schema: the server would reuse
        // schema 0 and then refuse `-1` (#832).
        assert_eq!(actions, ["add-view-version", "set-current-view-version"]);
        assert_eq!(
            body["updates"][0]["view-version"]["version-id"], 3,
            "max(1,2)+1"
        );
        assert_eq!(body["updates"][0]["view-version"]["schema-id"], 0);
        assert_eq!(
            body["updates"][0]["view-version"]["representations"][0]["sql"],
            "SELECT 3 AS x"
        );
        assert_eq!(body["updates"][1]["view-version-id"], -1);
        assert!(
            !requests
                .iter()
                .any(|r| r.method == wiremock::http::Method::DELETE),
            "replace must not drop the view"
        );
    }

    fn update_actions(updates: &[iceberg::ViewUpdate]) -> Vec<String> {
        updates
            .iter()
            .map(|u| serde_json::to_value(u).unwrap()["action"].as_str().unwrap().to_string())
            .collect()
    }

    /// A changed schema is added and referenced as `-1`: the server may give
    /// it any id (#770).
    #[test]
    fn view_replace_adds_a_changed_schema() {
        let schema = view_schema_from_json(
            &serde_json::json!({
                "fields": [{"id": 1, "name": "y", "required": false, "type": "string"}]
            }),
            1,
        )
        .unwrap();
        let updates = view_replace_updates(&metadata(1, &[("sqe", "SELECT 1 AS x")]), schema, |id| {
            sqe_view_version(3, id, "SELECT 'a' AS y", &sales(), "test-warehouse")
        })
        .unwrap();
        assert_eq!(
            update_actions(&updates),
            ["add-schema", "add-view-version", "set-current-view-version"]
        );
        let body = serde_json::to_value(&updates).unwrap();
        assert_eq!(body[1]["view-version"]["schema-id"], -1);
        assert_eq!(body[2]["view-version-id"], -1);
    }

    /// Replacing a view with the definition it already has (a dbt view
    /// model re-run) adds nothing and points at the existing version: the
    /// server would dedupe a re-added version and refuse `-1` (#832).
    #[test]
    fn view_replace_with_the_same_definition_reuses_the_version() {
        let current =
            sqe_view_version(1, 0, "SELECT 1 AS x", &sales(), "test-warehouse").unwrap();
        let mut doc = view_metadata_json(1, &[]);
        doc["versions"][0] = serde_json::to_value(&current).unwrap();
        let existing: iceberg::spec::ViewMetadata = serde_json::from_value(doc).unwrap();
        let schema = view_schema_from_json(&view_schema_json(), 1).unwrap();
        let updates = view_replace_updates(&existing, schema, |id| {
            sqe_view_version(3, id, "SELECT 1 AS x", &sales(), "test-warehouse")
        })
        .unwrap();
        assert_eq!(update_actions(&updates), ["set-current-view-version"]);
        assert_eq!(serde_json::to_value(&updates).unwrap()[0]["view-version-id"], 1);
    }
}

/// SQE's raw REST path (HEAD authorization, views, commits) bypasses the
/// vendored client's authenticator. Against a catalog configured for
/// client-side SigV4 (RustFS S3 Tables) it must sign those calls itself
/// instead of sending an empty bearer, which RustFS answers with 400
/// "invalid header: authorization".
#[cfg(all(test, feature = "rest-sigv4"))]
mod sigv4_raw_tests {
    use std::collections::HashMap;

    use iceberg::{NamespaceIdent, TableIdent};
    use wiremock::matchers::{header_exists, header_regex, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::SessionCatalog;
    use sqe_core::config::StorageConfig;

    const WAREHOUSE: &str = "warehouse";
    const TABLE_PATH: &str = "/v1/warehouse/namespaces/demo/tables/events";

    fn events() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("demo".to_string()), "events".to_string())
    }

    fn rustfs_storage() -> StorageConfig {
        StorageConfig {
            s3_access_key: "root".to_string(),
            s3_secret_key: "s3cr3t".to_string().into(),
            s3_region: "us-east-1".to_string(),
            ..StorageConfig::default()
        }
    }

    fn sigv4_props() -> HashMap<String, String> {
        HashMap::from([
            ("rest.sigv4-enabled".to_string(), "true".to_string()),
            ("rest.signing-name".to_string(), "s3".to_string()),
            ("rest.signing-region".to_string(), "us-east-1".to_string()),
        ])
    }

    async fn session(server: &MockServer, props: HashMap<String, String>) -> SessionCatalog {
        SessionCatalog::new_with_props(
            &server.uri(),
            WAREHOUSE,
            "",
            &rustfs_storage(),
            None,
            None,
            None,
            false,
            props,
        )
        .await
        .expect("construct session catalog")
    }

    #[tokio::test]
    async fn raw_head_is_sigv4_signed_with_the_payload_hash_header() {
        let server = MockServer::start().await;
        // Only a signed HEAD carrying the S3 payload hash is answered;
        // anything else falls through to wiremock's 404.
        Mock::given(method("HEAD"))
            .and(path(TABLE_PATH))
            .and(header_regex(
                "authorization",
                r"^AWS4-HMAC-SHA256 Credential=root/\d{8}/us-east-1/s3/aws4_request",
            ))
            .and(header_exists("x-amz-content-sha256"))
            .and(header_exists("x-amz-date"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let session = session(&server, sigv4_props()).await;
        session
            .authorize_table_access(&events())
            .await
            .expect("signed HEAD is accepted");
    }

    #[tokio::test]
    async fn raw_post_signs_the_json_body_it_sends() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(TABLE_PATH))
            .and(header_regex("authorization", r"^AWS4-HMAC-SHA256 "))
            // Hex SHA-256 of the exact body bytes; a signer that hashed an
            // empty body would send e3b0c442... instead.
            .and(header_regex("x-amz-content-sha256", r"^[0-9a-f]{64}$"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;

        let session = session(&server, sigv4_props()).await;
        session
            .commit_schema_update(&events(), Vec::new(), Vec::new())
            .await
            .expect("signed commit is accepted");

        let requests = server.received_requests().await.unwrap();
        let post = requests
            .iter()
            .find(|r| r.method == wiremock::http::Method::POST)
            .expect("one POST");
        let hash = post.headers.get("x-amz-content-sha256").unwrap();
        let expected = {
            use sha2::{Digest, Sha256};
            Sha256::digest(&post.body)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        assert_eq!(hash.to_str().unwrap(), expected, "payload hash covers the body");
        assert!(
            post.headers.get("authorization").unwrap().to_str().unwrap().starts_with("AWS4-HMAC-SHA256"),
            "no bearer on a signed catalog"
        );
    }

    #[tokio::test]
    async fn without_sigv4_props_the_raw_path_still_sends_the_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path(TABLE_PATH))
            .and(header_regex("authorization", r"^Bearer session-token$"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let session = SessionCatalog::new_with_props(
            &server.uri(),
            WAREHOUSE,
            "session-token",
            &rustfs_storage(),
            None,
            None,
            None,
            false,
            HashMap::new(),
        )
        .await
        .expect("construct session catalog");
        session
            .authorize_table_access(&events())
            .await
            .expect("bearer HEAD is accepted");
    }
}

/// Credential vending end to end through the vendored REST client against a
/// mock catalog: the delegation header goes out, vended keys win over the
/// static `[storage]` pair, and vended-only mode carries no static keys.
#[cfg(test)]
mod vended_credentials_rest_tests {
    use iceberg::{NamespaceIdent, TableIdent};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{access_delegation_props, SessionCatalog};

    const LOAD_TABLE_RESPONSE: &str = include_str!(
        "../../../vendor/iceberg-rust/crates/catalog/rest/testdata/load_table_response.json"
    );

    fn orders() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        )
    }

    fn static_storage() -> sqe_core::config::StorageConfig {
        sqe_core::config::StorageConfig {
            s3_access_key: "STATIC".to_string(),
            s3_secret_key: sqe_core::SecretString::new("static-secret".to_string()),
            ..Default::default()
        }
    }

    fn catalog_config(server: &MockServer, extra: &str) -> sqe_core::config::CatalogConfig {
        toml::from_str(&format!(
            "catalog_url = \"{}\"\nwarehouse = \"wh\"\n{extra}",
            server.uri()
        ))
        .expect("catalog config")
    }

    /// Mount `/v1/config` and a `loadTable` answer that only matches when the
    /// delegation header is present. `vended` is merged into the response
    /// `config`.
    async fn mock_catalog(vended: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "defaults": {}, "overrides": {"prefix": "wh"}
            })))
            .mount(&server)
            .await;
        let mut body: serde_json::Value = serde_json::from_str(LOAD_TABLE_RESPONSE).unwrap();
        body["config"] = vended;
        Mock::given(method("GET"))
            .and(path("/v1/wh/namespaces/sales/tables/orders"))
            .and(header("x-iceberg-access-delegation", "vended-credentials"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    #[test]
    fn delegation_props_follow_the_effective_setting() {
        let off: sqe_core::config::CatalogConfig = toml::from_str("").unwrap();
        assert!(access_delegation_props(&off).is_empty());
        let on: sqe_core::config::CatalogConfig =
            toml::from_str("access_delegation = \"vended-credentials\"").unwrap();
        assert_eq!(
            access_delegation_props(&on)
                .get("header.X-Iceberg-Access-Delegation")
                .map(String::as_str),
            Some("vended-credentials")
        );
        let required: sqe_core::config::CatalogConfig =
            toml::from_str("require_vended_credentials = true").unwrap();
        assert!(!access_delegation_props(&required).is_empty());
    }

    /// `access_delegation = "vended-credentials"` with a static pair
    /// configured: the header is sent and the table's FileIO carries the
    /// vended, table-scoped keys, not the static ones.
    #[tokio::test]
    async fn delegation_header_is_sent_and_vended_keys_beat_static() {
        let server = mock_catalog(serde_json::json!({
            "s3.access-key-id": "VENDED",
            "s3.secret-access-key": "vended-secret",
            "s3.session-token": "vended-token",
            "s3.session-token-expires-at-ms": "4102444800000",
        }))
        .await;
        let cfg = catalog_config(&server, "access_delegation = \"vended-credentials\"");
        let session = SessionCatalog::for_session_with(&cfg, &static_storage(), None, "tok-a")
            .await
            .unwrap();

        let table = session.load_table(&orders()).await.expect("header matched");
        let props = table.file_io().config().props();
        assert_eq!(props["s3.access-key-id"], "VENDED");
        assert_eq!(props["s3.secret-access-key"], "vended-secret");
        assert_eq!(props["s3.session-token"], "vended-token");
        assert_eq!(props["s3.session-token-expires-at-ms"], "4102444800000");

        let requests = server.received_requests().await.unwrap();
        assert!(requests
            .iter()
            .filter(|r| r.url.path().starts_with("/v1/"))
            .all(|r| r.headers.get("x-iceberg-access-delegation").is_some()
                || r.method == wiremock::http::Method::HEAD));
    }

    /// `require_vended_credentials`: the header is implied, and a catalog
    /// that vends nothing leaves FileIO with NO static keys to fall back to.
    #[tokio::test]
    async fn require_vended_sends_header_and_never_carries_static_keys() {
        let server = mock_catalog(serde_json::json!({})).await;
        let cfg = catalog_config(&server, "require_vended_credentials = true");
        let session = SessionCatalog::for_session_with(&cfg, &static_storage(), None, "tok-b")
            .await
            .unwrap();

        let table = session.load_table(&orders()).await.expect("header matched");
        let props = table.file_io().config().props();
        let keys: Vec<_> = props.keys().collect();
        assert!(!props.contains_key("s3.access-key-id"), "{keys:?}");
        assert!(!props.contains_key("s3.secret-access-key"));
    }

    /// Default static mode: no header goes out (the delegated mock does not
    /// match), and nothing else about the request changes.
    #[tokio::test]
    async fn static_mode_sends_no_delegation_header() {
        let server = mock_catalog(serde_json::json!({})).await;
        let cfg = catalog_config(&server, "");
        let session = SessionCatalog::for_session_with(&cfg, &static_storage(), None, "tok-c")
            .await
            .unwrap();
        assert!(session.load_table(&orders()).await.is_err());
        let requests = server.received_requests().await.unwrap();
        assert!(!requests.is_empty());
        assert!(requests
            .iter()
            .all(|r| r.headers.get("x-iceberg-access-delegation").is_none()));
    }

    /// `load_table_uncached` asks the catalog again (new keys) even when a
    /// cached copy exists.
    #[tokio::test]
    async fn uncached_reload_always_reaches_the_catalog() {
        let server = mock_catalog(serde_json::json!({"s3.access-key-id": "V"})).await;
        let cfg = catalog_config(&server, "access_delegation = \"vended-credentials\"");
        let cache = super::TableMetadataCache::new(300);
        let session =
            SessionCatalog::for_session_with(&cfg, &static_storage(), Some(cache), "tok-d")
                .await
                .unwrap();
        let loads = |reqs: &[wiremock::Request]| {
            reqs.iter()
                .filter(|r| {
                    r.method == wiremock::http::Method::GET
                        && r.url.path() == "/v1/wh/namespaces/sales/tables/orders"
                })
                .count()
        };
        session.load_table(&orders()).await.unwrap();
        let before = loads(&server.received_requests().await.unwrap());
        session.load_table_uncached(&orders()).await.unwrap();
        let after = loads(&server.received_requests().await.unwrap());
        assert_eq!(after, before + 1);
    }
}

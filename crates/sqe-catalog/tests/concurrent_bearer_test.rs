//! Regression test for GitLab issue #2:
//! "Outbound bearer dropped on concurrent Polaris REST calls".
//!
//! The diagnostic warn shipped at
//! `vendor/iceberg-rust/crates/catalog/rest/src/client.rs::authenticate`
//! fires the line `"Outbound REST request issued WITHOUT Authorization"`
//! every time an outbound REST call is dispatched without a bearer
//! token. We pin a `tracing-subscriber` layer that writes into an
//! in-memory `Vec<u8>` and assert the count of those warns is zero
//! across N parallel `list_namespaces` calls through the SAME
//! `SessionCatalog`.
//!
//! What we want to surface:
//!   * The race between `tokio::sync::OnceCell::get_or_try_init` and
//!     concurrent `RestCatalog` calls that share the same
//!     `Arc<RestCatalog>` from SQE's `REST_CATALOG_CACHE` (formerly
//!     `Arc<RwLock<RestCatalog>>`; lock removed in issue #18).
//!   * Whether `HttpClient::update_with` (called once at OnceCell
//!     init) drops the user's `token` prop when the server-supplied
//!     `/v1/config` overrides do not include it.
//!   * Whether multiple in-flight calls observe the OnceCell pre-init
//!     state and try to authenticate before the token is wired up.
//!
//! The wiremock fixture below differentiates "no bearer" from
//! "with bearer" via two mounts. Polaris would 401 the bare call;
//! the test mirrors that so the request log can be inspected on
//! failure to identify which path lost auth.
//!
//! ## Reproduction status (as of this commit)
//!
//! All four scenarios PASS today: the diagnostic warn never fires
//! through the wiremock fixture, even with 8 concurrent
//! `list_namespaces` calls and a 50ms `/v1/config` delay to widen
//! the OnceCell init window. That means **this test is currently a
//! green baseline, not a failing reproducer.** It catches the bug
//! if it ever fires through this code path under load, but the
//! production regression reported in issue #2 (dbt+threads:4
//! against real Polaris) is not yet reachable from a self-contained
//! Rust test.
//!
//! Possible reasons the wiremock setup misses the real bug:
//!   * Network timing variance (TLS handshake, real RTT) widens
//!     the race window in ways `set_delay` does not.
//!   * Real Polaris emits non-empty `/v1/config` overrides; if any
//!     interact with the `token` prop merge in `merge_with_config`,
//!     the wiremock empty-overrides body wouldn't trigger it.
//!   * The bug may need a token-refresh path that this fixture
//!     does not exercise.
//!
//! Keep this test as a regression guard. When the real fix lands,
//! the same test should turn red against a pre-fix baseline.

use std::sync::{Arc, Mutex};

use sqe_catalog::SessionCatalog;
use sqe_core::config::StorageConfig;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Captures every `tracing` event into a shared `Vec<u8>` so the test
/// can grep for the diagnostic warn message after all tasks complete.
///
/// We can't use `tracing_subscriber::fmt::TestWriter` because that
/// pipes to the test harness's stdout and isn't programmatically
/// inspectable. A `Mutex<Vec<u8>>` plus a `MakeWriter` that hands out
/// guards over the same buffer is the standard recipe for log
/// capture in tests.
#[derive(Clone)]
struct CaptureWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl CaptureWriter {
    fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> String {
        let guard = self.buf.lock().expect("CaptureWriter buffer poisoned");
        String::from_utf8_lossy(&guard).to_string()
    }
}

struct CaptureGuard {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl std::io::Write for CaptureGuard {
    fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.buf.lock().expect("CaptureWriter buffer poisoned");
        guard.extend_from_slice(src);
        Ok(src.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureGuard;

    fn make_writer(&'a self) -> Self::Writer {
        CaptureGuard {
            buf: self.buf.clone(),
        }
    }
}

/// Build a Polaris-shaped wiremock fixture.
///
/// * `GET /v1/config` returns the empty-overrides body iceberg-rust
///   expects, but only if the request carries Authorization. The
///   optional `config_delay` argument widens the OnceCell init window
///   so concurrent callers are more likely to hit the race the bug
///   blames.
/// * `GET /v1/{prefix}/namespaces` returns the empty list, again only
///   when Authorization is present.
/// * Any request that lands without Authorization is matched by a
///   catch-all 401 mount so the wiremock request log shows the path
///   that was visited without a bearer.
async fn polaris_mock(server: &MockServer, config_delay: std::time::Duration) {
    // Authenticated /v1/config — matches when an Authorization header
    // exists at all, regardless of bearer value (the diagnostic warn
    // doesn't care about the token *content*, only its presence).
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header_exists("Authorization"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#)
                .set_delay(config_delay),
        )
        .mount(server)
        .await;

    // Authenticated /v1/namespaces — note iceberg-rust does not embed
    // a `prefix` segment unless the `/v1/config` response advertises
    // one, which the empty-overrides body above does not. So the
    // namespaces endpoint resolves to `/v1/namespaces`.
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(header_exists("Authorization"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(server)
        .await;

    // Catch-all: any request that slipped through without auth.
    // wiremock returns 404 on no-match by default, which would mask
    // the real failure mode (the test needs to see whether the warn
    // fired AND whether the server saw an unauthed call). Returning
    // 401 lets the SessionCatalog surface a clean Catalog error
    // separately from the warn count.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("www-authenticate", "Bearer realm=\"polaris\""),
        )
        .mount(server)
        .await;
}

/// Build a `SessionCatalog` pointing at the wiremock server, with the
/// bearer token wired through SQE's normal construction path. Each
/// test gets a unique token so the static `REST_CATALOG_CACHE` inside
/// `SessionCatalog::new` does not return a sibling test's cached
/// catalog (the cache key is `format!("{url}-{fingerprint}")`).
async fn make_session(server_uri: &str, token: &str) -> SessionCatalog {
    SessionCatalog::new(
        server_uri,
        "test-warehouse",
        token,
        &StorageConfig::default(),
        None, // no shared TableMetadataCache
        None, // default reqwest client
        None, // default circuit breaker
    )
    .await
    .expect("SessionCatalog::new should succeed against the wiremock server")
}

/// Helper: count how many lines in the captured log carry the
/// diagnostic warn signature. The exact message string is what the
/// fix to commit `3ea5dd3` added; if anyone refactors that warn line
/// the test should fail loudly here so we re-anchor the assertion.
fn count_unauth_warns(log: &str) -> usize {
    log.lines()
        .filter(|line| line.contains("Outbound REST request issued WITHOUT Authorization"))
        .count()
}

/// Helper: dump the wiremock request log for diagnosis. Includes path,
/// method, and whether the inbound request had Authorization. The
/// header content is not logged because that would print the bearer
/// in the test output.
async fn format_request_log(server: &MockServer) -> String {
    let mut out = String::new();
    let received = server.received_requests().await.unwrap_or_default();
    out.push_str(&format!(
        "=== wiremock saw {} request(s) ===\n",
        received.len()
    ));
    for (i, req) in received.iter().enumerate() {
        let auth_present = req.headers.get("authorization").is_some()
            || req.headers.get("Authorization").is_some();
        out.push_str(&format!(
            "[{i}] {} {} authorization={}\n",
            req.method,
            req.url.path(),
            if auth_present { "PRESENT" } else { "MISSING" },
        ));
    }
    out
}

/// Run N parallel `list_namespaces` calls through the same
/// `SessionCatalog` instance and assert no diagnostic "no auth" warns
/// fired, no matter the order in which OnceCell context init and
/// outbound calls interleave.
///
/// We build one Arc<SessionCatalog> and clone it across all tasks so
/// every task hits the *same* `Arc<RestCatalog>` — that's the shared
/// state the bug report blames. If we constructed N catalogs in
/// parallel we would be testing the moka cache double-build path
/// instead, which is a different (also worth-covering) concern.
async fn drive_concurrent_calls(
    server_uri: &str,
    token: &str,
    n: usize,
    capture: CaptureWriter,
) -> (Vec<sqe_core::Result<Vec<iceberg::NamespaceIdent>>>, String) {
    // Pin the capturing subscriber for the duration of the calls.
    // Layered through `Registry` so we keep the standard fmt::Layer
    // formatting (single-line, no ANSI) and let the env-filter pull
    // in TRACE+ from the iceberg-catalog-rest crate so the diagnostic
    // warn (and the matching debug! "Outbound REST request authenticated
    // ...") both reach the buffer.
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(capture.clone());
    let filter =
        tracing_subscriber::EnvFilter::new("iceberg_catalog_rest=trace,sqe_catalog=trace,warn");
    let subscriber = Registry::default().with(filter).with(layer);
    let dispatch = tracing::dispatcher::Dispatch::new(subscriber);

    // SessionCatalog::new also emits diagnostic events; attach the
    // capturing dispatcher to that future too so any anomalies during
    // construction are visible in the same log buffer the assertion
    // grep runs against.
    let session = Arc::new(
        make_session(server_uri, token)
            .with_subscriber(dispatch.clone())
            .await,
    );

    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let s = session.clone();
        let d = dispatch.clone();
        handles.push(tokio::spawn(async move {
            // Attach the capturing dispatcher to the spawned future so
            // the events fired on whichever worker thread tokio picks
            // for this task land in *our* in-memory buffer rather than
            // the harness default. `with_subscriber` is the documented
            // recipe for cross-thread dispatcher propagation.
            s.list_namespaces().with_subscriber(d).await
        }));
    }
    let mut results = Vec::with_capacity(n);
    for h in handles {
        results.push(h.await.expect("spawned list_namespaces task panicked"));
    }

    (results, capture.contents())
}

/// Multi-threaded reproducer: drives 8 concurrent calls through one
/// SessionCatalog. If the bug exists we expect at least one warn line
/// to land in the captured log (and the assertion below to fail with
/// the count + the wiremock request log for diagnosis).
///
/// Why multi_thread: the OnceCell race the issue blames is a
/// scheduling phenomenon. A current_thread runtime would serialise
/// the spawned tasks and mask the issue. We still want the test to
/// be deterministic in its *assertion* — the assertion fails if even
/// one bare call slips through, and never spuriously when the
/// pipeline is correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_list_namespaces_keeps_bearer_on_every_call() {
    let server = MockServer::start().await;
    polaris_mock(&server, std::time::Duration::ZERO).await;

    let capture = CaptureWriter::new();

    // Unique token so the static REST_CATALOG_CACHE inside
    // SessionCatalog::new doesn't return a leftover from another
    // concurrently-running test in the same `cargo test` invocation.
    let token = format!("repro-multi-{}", uuid::Uuid::new_v4());
    let (results, log) = drive_concurrent_calls(&server.uri(), &token, 8, capture).await;

    let warn_count = count_unauth_warns(&log);
    let request_log = format_request_log(&server).await;

    // All 8 calls should succeed (Polaris would have 401'd anything
    // without auth via the catch-all mock). If any failed we surface
    // the errors in the panic message because the failure shape gives
    // the strongest hint about whether we are hitting the bug or a
    // wiremock matcher mismatch.
    let errors: Vec<String> = results
        .iter()
        .filter_map(|r| r.as_ref().err().map(|e| e.to_string()))
        .collect();

    assert_eq!(
        warn_count,
        0,
        "Expected zero \"Outbound REST request issued WITHOUT Authorization\" \
         warns under {n} concurrent list_namespaces calls. Got {warn_count}.\n\n\
         {request_log}\n\
         === errors observed ===\n{errors:#?}\n\n\
         === full captured log ===\n{log}",
        n = 8,
    );

    assert!(
        errors.is_empty(),
        "Concurrent list_namespaces calls failed without tripping the warn — \
         that means a different code path is breaking auth. Errors:\n{errors:#?}\n\n\
         {request_log}\n\
         === full captured log ===\n{log}",
    );
}

/// Same multi-thread shape, but the wiremock `/v1/config` reply is
/// held for 50ms so multiple concurrent callers all queue on the
/// `RestCatalog::context()` `OnceCell::get_or_try_init` and exit it
/// at staggered times. If the bug is a window-of-init issue (caller
/// B observes a partly-initialised state, or wins the future cancel
/// path), this test gives it the largest window an in-process mock
/// can provide.
///
/// Still expects zero unauthed warns. If this lights up while the
/// no-delay variant stays clean, the root cause is in the OnceCell
/// init lifecycle, not the moka cache or the token mutex.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_list_namespaces_with_slow_config_init_keeps_bearer() {
    let server = MockServer::start().await;
    polaris_mock(&server, std::time::Duration::from_millis(50)).await;

    let capture = CaptureWriter::new();
    let token = format!("repro-slow-{}", uuid::Uuid::new_v4());
    let (results, log) = drive_concurrent_calls(&server.uri(), &token, 8, capture).await;

    let warn_count = count_unauth_warns(&log);
    let request_log = format_request_log(&server).await;

    let errors: Vec<String> = results
        .iter()
        .filter_map(|r| r.as_ref().err().map(|e| e.to_string()))
        .collect();

    assert_eq!(
        warn_count, 0,
        "Expected zero unauthed warns even with a 50ms /v1/config delay. \
         If this fires, the bug is an OnceCell init race.\n\n\
         {request_log}\n\
         === errors observed ===\n{errors:#?}\n\n\
         === full captured log ===\n{log}",
    );

    assert!(
        errors.is_empty(),
        "Slow-config concurrent variant failed unexpectedly. Errors:\n{errors:#?}\n\n\
         {request_log}\n\
         === full captured log ===\n{log}",
    );
}

/// Sanity check on the test infrastructure: drive a `RestCatalog`
/// with no token, no credential, and no GCP creds — exactly the
/// shape that fires the diagnostic warn — and assert the
/// `CaptureWriter` actually picks it up. If this test fails the
/// problem is in our log-capture wiring, not in the production
/// code path; the regression test results above are then suspect.
///
/// Uses the upstream `RestCatalogBuilder` directly rather than
/// `SessionCatalog::new` because SessionCatalog always sets the
/// token prop. The upstream builder lets us reach the unauthed
/// branch of `authenticate` without circumventing public API.
#[tokio::test(flavor = "current_thread")]
async fn capture_layer_observes_unauth_warn() {
    use iceberg::{Catalog, CatalogBuilder};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use std::collections::HashMap;

    let server = MockServer::start().await;
    // Permissive mock: respond 200 to any GET, regardless of auth.
    // We don't care about the wire-level result; we just want the
    // unauthed authenticate() call to flow through and fire the warn.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{},"namespaces":[]}"#),
        )
        .mount(&server)
        .await;

    let capture = CaptureWriter::new();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(capture.clone());
    let filter =
        tracing_subscriber::EnvFilter::new("iceberg_catalog_rest=trace,sqe_catalog=trace,warn");
    let dispatch = tracing::dispatcher::Dispatch::new(Registry::default().with(filter).with(layer));

    // Build a RestCatalog with NO token, NO credential. This is the
    // exact precondition for the diagnostic warn at client.rs:281.
    let mut props = HashMap::new();
    props.insert("uri".to_string(), server.uri());
    let catalog = RestCatalogBuilder::default()
        .load("sanity-no-auth".to_string(), props)
        .await
        .expect("RestCatalogBuilder accepts an empty-props build");

    // One call is enough — the OnceCell init triggers authenticate
    // for the /v1/config request.
    let _ = catalog
        .list_namespaces(None)
        .with_subscriber(dispatch)
        .await;

    let log = capture.contents();
    let warns = count_unauth_warns(&log);
    assert!(
        warns >= 1,
        "Capture infrastructure failed to record the diagnostic warn even \
         though we deliberately built an unauthed RestCatalog. The other \
         tests in this file are not trustworthy until this passes.\n\n\
         === full captured log ===\n{log}",
    );
}

/// Verifies the defensive guard added for issue #2: a `RestCatalog`
/// that was constructed with a bearer token must refuse to issue an
/// outbound REST call without `Authorization` if the live token has
/// been cleared after init.
///
/// We construct the catalog the normal way (token in props, init runs,
/// `auth_required` flag flips to true), then call the public
/// `invalidate_token()` to simulate the end state of any race that
/// would leave the mutex holding `None` while `auth_required` says we
/// should still be authenticated. The next `list_namespaces` call must
/// hit the new guard at `client::authenticate` and surface a clear
/// `DataInvalid` error rather than silently going on the wire bare.
#[tokio::test(flavor = "current_thread")]
async fn auth_required_guard_refuses_unauthed_request_after_token_cleared() {
    use iceberg::{Catalog, CatalogBuilder};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use std::collections::HashMap;

    let server = MockServer::start().await;
    polaris_mock(&server, std::time::Duration::ZERO).await;

    // Build a RestCatalog with a token so `auth_required = true`
    // propagates through both `HttpClient::new` and `update_with`.
    let mut props = HashMap::new();
    props.insert("uri".to_string(), server.uri());
    props.insert("token".to_string(), "live-token-xyz".to_string());
    props.insert("warehouse".to_string(), "test-warehouse".to_string());
    let catalog = RestCatalogBuilder::default()
        .load("auth-required-guard".to_string(), props)
        .await
        .expect("RestCatalogBuilder accepts an authenticated build");

    // Drive one successful call so the OnceCell context is initialised
    // with a live token. Without this the invalidate_token below
    // would race the init future and the assertion below would just
    // re-test the no-auth-at-all path.
    catalog
        .list_namespaces(None)
        .await
        .expect("first list_namespaces should authenticate cleanly");

    // Now simulate the end state of any concurrency race that drops
    // the token: clear it directly via the public API.
    catalog
        .invalidate_token()
        .await
        .expect("invalidate_token is infallible");

    // The next call must hit the auth_required guard and fail with a
    // DataInvalid error containing "Refusing to send outbound REST
    // request without Authorization". It must NOT silently send a bare
    // request to the wiremock server.
    let err = catalog
        .list_namespaces(None)
        .await
        .expect_err("list_namespaces after invalidate_token should fail loudly");

    let msg = err.to_string();
    assert!(
        msg.contains("Refusing to send outbound REST request without Authorization"),
        "Expected the auth_required guard message; got: {msg}",
    );

    // Server should NOT have seen a second outbound call without auth.
    // wiremock's catch-all 401 mock would have responded if the request
    // had escaped the guard. The request log lets us assert exactly
    // that.
    let request_log = format_request_log(&server).await;
    let unauth_outbound_count = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            r.headers.get("authorization").is_none() && r.headers.get("Authorization").is_none()
        })
        .count();
    assert_eq!(
        unauth_outbound_count, 0,
        "An unauthenticated request reached the wiremock server even though \
         the auth_required guard should have rejected it.\n\n{request_log}",
    );
}

/// Verifies SQE's own construction-time guard: passing an empty bearer
/// to `SessionCatalog::new` must NOT result in `"token" -> ""` landing
/// in the props map handed to `iceberg-catalog-rest`. The previous
/// behaviour silently sent `Bearer ` (literal empty) on the wire.
///
/// We can't easily inspect the internal props after construction, so
/// instead we drive a call through and assert the request that reached
/// the wiremock server carried no `Authorization` header at all (the
/// "empty token means anonymous catalog" contract from
/// `crates/sqe-auth/src/per_catalog.rs`). Before the fix this call
/// would have arrived with `Authorization: Bearer ` and Polaris would
/// have 401'd; the iceberg-rust empty-bearer guard later masked it as
/// a `DataInvalid` error.
#[tokio::test(flavor = "current_thread")]
async fn empty_bearer_does_not_inject_token_prop() {
    let server = MockServer::start().await;
    // Permissive: respond 200 to any GET regardless of auth so we can
    // observe what header (if any) SQE actually sent.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{},"namespaces":[]}"#),
        )
        .mount(&server)
        .await;

    // Empty bearer signals an Anonymous catalog per
    // `resolve_bearer(CatalogAuthConfig::Anonymous, _)`. SessionCatalog
    // must accept this and skip the token prop entirely.
    let session = make_session(&server.uri(), "").await;

    // One call drives the OnceCell init and the first outbound REST
    // request. Either Ok or Err is acceptable here as long as the
    // request body shape is correct.
    let _ = session.list_namespaces().await;

    let received = server.received_requests().await.unwrap_or_default();
    assert!(
        !received.is_empty(),
        "wiremock saw no requests; the test isn't exercising the wire path",
    );

    for req in &received {
        let auth = req
            .headers
            .get("authorization")
            .or_else(|| req.headers.get("Authorization"));
        assert!(
            auth.is_none(),
            "Empty-bearer SessionCatalog sent an Authorization header on \
             {} {} (was {:?}). The Anonymous-catalog contract is broken.",
            req.method,
            req.url.path(),
            auth,
        );
    }
}

/// Same shape as the multi_thread test, but with current_thread + a
/// low concurrency. Acts as the baseline: if the bug only repros on
/// multi_thread, the warn count here should stay at zero. If we see
/// the warn fire even on current_thread, the root cause is *not* a
/// multi-core race and we should look at the OnceCell init lifecycle
/// / token mutex paths.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_list_namespaces_current_thread_baseline() {
    let server = MockServer::start().await;
    polaris_mock(&server, std::time::Duration::ZERO).await;

    let capture = CaptureWriter::new();
    let token = format!("repro-single-{}", uuid::Uuid::new_v4());
    let (results, log) = drive_concurrent_calls(&server.uri(), &token, 4, capture).await;

    let warn_count = count_unauth_warns(&log);
    let request_log = format_request_log(&server).await;

    let errors: Vec<String> = results
        .iter()
        .filter_map(|r| r.as_ref().err().map(|e| e.to_string()))
        .collect();

    assert_eq!(
        warn_count, 0,
        "Expected zero unauthed warns even on the current_thread runtime. \
         Got {warn_count} which means the bug is not a multi-core scheduling \
         race — investigate OnceCell init + token mutex sequencing.\n\n\
         {request_log}\n\
         === full captured log ===\n{log}",
    );

    assert!(
        errors.is_empty(),
        "current_thread baseline failed unexpectedly. Errors:\n{errors:#?}\n\n\
         {request_log}\n\
         === full captured log ===\n{log}",
    );
}

/// Issue #2 root-cause hypothesis: the server's `/v1/config` `overrides`
/// map can clobber the user-supplied token with an empty string. Pre-
/// fix the merge silently propagated that empty value all the way to
/// the `token` mutex; downstream `authenticate()` errored at request
/// time with a defensive guard, but the error pointed at the request
/// site rather than the actual misconfiguration at the merge boundary.
///
/// This test mounts a wiremock `/v1/config` that returns
/// `{"overrides":{"token":""}, "defaults":{}}` — exactly the shape a
/// misconfigured Polaris (or a federated catalog that wants to opt the
/// user into anonymous access without saying so) would emit. With the
/// fix in `HttpClient::update_with`, SessionCatalog construction errors
/// loudly at the merge boundary with operator-actionable text. Without
/// the fix, construction succeeded and the first outbound call 401'd.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_catalog_rejects_server_overriding_token_to_empty() {
    let server = MockServer::start().await;

    // /v1/config with an empty-string token override. The mock only
    // matches when Authorization is present — we want to confirm the
    // first call carries the user's bearer, then verify the merge
    // boundary rejects the response.
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(header_exists("Authorization"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{"token":""},"defaults":{}}"#),
        )
        .mount(&server)
        .await;

    // Catch-all 401 mock so a request slipping through without auth
    // shows up clearly in the wiremock log.
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("www-authenticate", "Bearer realm=\"polaris\""),
        )
        .mount(&server)
        .await;

    let token = format!("repro-clobber-{}", uuid::Uuid::new_v4());
    let session = SessionCatalog::new(
        &server.uri(),
        "test-warehouse",
        &token,
        &StorageConfig::default(),
        None,
        None,
        None,
    )
    .await
    .expect("SessionCatalog::new returns Ok; the merge guard fires on first use");

    // Driving the first actual catalog call triggers the OnceCell init
    // which calls /v1/config and then update_with. The merge guard
    // surfaces as a Catalog error.
    let result = session.list_namespaces().await;
    let request_log = format_request_log(&server).await;
    let err = result.expect_err(&format!(
        "Expected a loud merge-boundary error when /v1/config clobbers \
             the token with empty.\n{request_log}"
    ));

    let msg = format!("{err}");
    assert!(
        msg.contains("token") && (msg.contains("misconfiguration") || msg.contains("empty")),
        "Error must point at the token clobber: {msg}\n{request_log}"
    );

    // Confirm wiremock saw the /v1/config call WITH the user bearer
    // (so we know the failure is the merge guard, not auth on the
    // initial call).
    let received = server.received_requests().await.unwrap_or_default();
    let config_call = received
        .iter()
        .find(|r| r.url.path() == "/v1/config")
        .expect("at least one /v1/config call");
    assert!(
        config_call.headers.get("authorization").is_some(),
        "the /v1/config call must carry Authorization; otherwise the \
         test isn't exercising the merge boundary at all"
    );
}

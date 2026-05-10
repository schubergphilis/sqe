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
//!     `Arc<RwLock<RestCatalog>>` from SQE's `REST_CATALOG_CACHE`.
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
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"namespaces":[]}"#,
        ))
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
    out.push_str(&format!("=== wiremock saw {} request(s) ===\n", received.len()));
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
/// every task hits the *same* `Arc<RwLock<RestCatalog>>` — that's the
/// shared state the bug report blames. If we constructed N catalogs
/// in parallel we would be testing the moka cache double-build path
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
    let filter = tracing_subscriber::EnvFilter::new(
        "iceberg_catalog_rest=trace,sqe_catalog=trace,warn",
    );
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
    let (results, log) =
        drive_concurrent_calls(&server.uri(), &token, 8, capture).await;

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
        warn_count, 0,
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
    let (results, log) =
        drive_concurrent_calls(&server.uri(), &token, 8, capture).await;

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
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"overrides":{},"defaults":{},"namespaces":[]}"#,
        ))
        .mount(&server)
        .await;

    let capture = CaptureWriter::new();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(capture.clone());
    let filter = tracing_subscriber::EnvFilter::new(
        "iceberg_catalog_rest=trace,sqe_catalog=trace,warn",
    );
    let dispatch = tracing::dispatcher::Dispatch::new(
        Registry::default().with(filter).with(layer),
    );

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
    let _ = catalog.list_namespaces(None).with_subscriber(dispatch).await;

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
    let (results, log) =
        drive_concurrent_calls(&server.uri(), &token, 4, capture).await;

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

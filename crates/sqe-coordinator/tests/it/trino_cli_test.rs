//! The official `trino-cli` against the coordinator's Trino endpoint, output
//! diffed against a golden (issue #630).
//!
//! `make test-integration FILTER=trino_cli`
//!
//! Every other Trino-path test in this crate speaks the protocol with
//! `reqwest`, i.e. with our own reading of the spec. This one lets Trino's
//! client do the talking: submit, follow `nextUri`, decode column types,
//! render rows. A protocol drift a hand-rolled client tolerates (a missing
//! `typeSignature`, `data` present on a column-less update, a `nextUri` the
//! CLI cannot follow) surfaces here as a CLI error or a changed rendering.
//!
//! Two practicalities shape the harness:
//!
//! 1. trino-cli refuses to send a password or an access token over plain
//!    HTTP ("TLS/SSL is required for authentication with username and
//!    password"), and SQE has no anonymous mode. The test therefore fronts
//!    the in-process Trino listener with a tiny TLS-terminating reverse proxy
//!    (self-signed cert, `--insecure` on the CLI) that adds
//!    `x-forwarded-proto: https`, exactly the shape a production ingress
//!    presents. The proxy is ~40 lines of axum + reqwest.
//! 2. The CLI is a Java program that is not fetched by this repo. It is
//!    looked up as `$TRINO_CLI`, then `trino`, then `trino-cli` on `PATH`
//!    (Homebrew's `trino` formula and the upstream executable jar both fit).
//!    When none is found the test prints a SKIP line and passes, so an
//!    unfiltered `make test-integration` is not held hostage by a JVM.
//!    `openssl` (for the throwaway cert) is treated the same way.
//!
//! Goldens: `tests/it/fixtures/trino_cli.golden`. Query ids and JVM noise are
//! normalised. Re-generate after an intentional change with
//! `UPDATE_GOLDENS=1 make test-integration FILTER=trino_cli` and review the diff.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::Router;

// ── Adapters: real authenticator + real QueryHandler behind the Trino traits ─

struct CliTrinoAuth(Arc<sqe_auth::Authenticator>);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoAuthenticator for CliTrinoAuth {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        self.0.authenticate(username, password).await
    }
}

struct CliTrinoQuery(Arc<sqe_coordinator::QueryHandler>);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoQueryExecutor for CliTrinoQuery {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<sqe_trino_compat::server::TrinoResultStream, sqe_core::SqeError> {
        self.0
            .execute(session, sql, None)
            .await
            .map(sqe_trino_compat::server::TrinoResultStream::from_batches)
    }
}

// ── TLS front ────────────────────────────────────────────────────────────────

struct ProxyState {
    client: reqwest::Client,
    /// Plain-HTTP origin (`http://127.0.0.1:<trino_port>`).
    origin: String,
    /// What the CLI dials (`https://127.0.0.1:<tls_port>`); `nextUri`s are
    /// rewritten to it so the client keeps coming back through the front.
    public: String,
}

async fn proxy(State(st): State<Arc<ProxyState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("request body");
    let path_q = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let mut upstream = st
        .client
        .request(parts.method.clone(), format!("{}{path_q}", st.origin))
        .body(body.to_vec());
    for (name, value) in &parts.headers {
        if name == axum::http::header::HOST || name == axum::http::header::CONTENT_LENGTH {
            continue;
        }
        upstream = upstream.header(name, value);
    }
    let resp = upstream
        .header("x-forwarded-proto", "https")
        .send()
        .await
        .expect("upstream reachable");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.bytes().await.expect("upstream body");
    // Belt and braces: even if the origin built its base URL from the
    // forwarded Host, make sure no plain-HTTP origin URL leaks to the client.
    let text = String::from_utf8_lossy(&bytes).replace(&st.origin, &st.public);

    let mut out = Response::builder().status(status);
    for (name, value) in &headers {
        if name == axum::http::header::CONTENT_LENGTH
            || name == axum::http::header::TRANSFER_ENCODING
        {
            continue;
        }
        out = out.header(name, value);
    }
    out.body(Body::from(text)).expect("response")
}

/// Bind a TLS listener on an ephemeral port that forwards to `origin`.
/// Returns the public `https://` base URL.
async fn start_tls_front(origin: String, cert_dir: &Path) -> String {
    sqe_coordinator::tls::ensure_crypto_provider();
    let cert = cert_dir.join("cert.pem");
    let key = cert_dir.join("key.pem");
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1,DNS:localhost",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .expect("openssl present (checked by caller)");
    assert!(
        status.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let rustls = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
        .await
        .expect("load self-signed pem");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind tls front");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().expect("addr").port();
    let public = format!("https://127.0.0.1:{port}");

    let state = Arc::new(ProxyState {
        client: reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("proxy client"),
        origin,
        public: public.clone(),
    });
    let app = Router::new().fallback(proxy).with_state(state);
    tokio::spawn(async move {
        axum_server::from_tcp_rustls(listener, rustls)
            .expect("tls server")
            .serve(app.into_make_service())
            .await
            .expect("tls front serve");
    });
    public
}

// ── CLI discovery and invocation ─────────────────────────────────────────────

fn find_on_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(name))
            .find(|candidate| candidate.is_file())
    })
}

/// `$TRINO_CLI`, then `trino`, then `trino-cli` on `PATH`.
fn find_trino_cli() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("TRINO_CLI") {
        let p = PathBuf::from(explicit);
        return p.is_file().then_some(p);
    }
    find_on_path("trino").or_else(|| find_on_path("trino-cli"))
}

/// Run one statement through the CLI and render `exit=<code>` plus the
/// normalised stdout and stderr.
fn run_cli(cli: &Path, server: &str, user: &str, password: &str, sql: &str) -> String {
    let output = Command::new(cli)
        .args([
            "--server",
            server,
            "--insecure",
            "--user",
            user,
            "--password",
            "--catalog",
            "test_warehouse",
            "--schema",
            "default",
            "--output-format",
            "CSV_HEADER",
            "--execute",
            sql,
        ])
        .env("TRINO_PASSWORD", password)
        .output()
        .expect("spawn trino-cli");
    let code = output
        .status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "signal".to_string());
    let mut rendered = format!("exit={code}\n");
    let stdout = normalise(&String::from_utf8_lossy(&output.stdout));
    if !stdout.is_empty() {
        rendered.push_str(&stdout);
        rendered.push('\n');
    }
    let stderr = normalise(&String::from_utf8_lossy(&output.stderr));
    if !stderr.is_empty() {
        rendered.push_str("stderr:\n");
        rendered.push_str(&stderr);
        rendered.push('\n');
    }
    rendered
}

fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// Drop JVM/jline chatter, replace query ids, trim trailing whitespace.
fn normalise(raw: &str) -> String {
    raw.lines()
        .filter(|line| {
            !(line.starts_with("WARNING:") || line.contains("org.jline") || line.trim().is_empty())
        })
        .map(|line| {
            line.split(' ')
                .map(|word| {
                    let core = word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
                    if is_uuid(core) {
                        word.replace(core, "<query-id>")
                    } else {
                        word.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The fixed statement set. Deterministic against any bootstrapped test stack:
/// no table dependency, and the one error case names a schema nothing creates.
const STATEMENTS: &[&str] = &[
    "SELECT 1 AS one, 'x' AS s, CAST(1.5 AS DOUBLE) AS d, true AS b, CAST(NULL AS VARCHAR) AS n",
    "SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x",
    "SELECT count(*) AS n FROM (VALUES 1, 2, 3) AS t(x)",
    "SELECT CAST(row(1, row(10)) AS row(a int, b row(x int))) AS r",
    "SELECT * FROM no_such_schema_for_trino_cli.nope",
];

fn golden_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("it")
        .join("fixtures")
        .join("trino_cli.golden")
}

// ── The test ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris; plus trino-cli on PATH or TRINO_CLI=/path"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn trino_cli_golden() {
    crate::common::init_tracing();

    let Some(cli) = find_trino_cli() else {
        eprintln!(
            "SKIP trino_cli_golden: no trino-cli found (set TRINO_CLI=/path/to/trino or put `trino` / `trino-cli` on PATH)"
        );
        return;
    };
    if find_on_path("openssl").is_none() {
        eprintln!(
            "SKIP trino_cli_golden: `openssl` not on PATH (needed for the throwaway TLS cert)"
        );
        return;
    }

    let user = std::env::var("SQE_TEST_USER").unwrap_or_else(|_| "root".to_string());
    let password = std::env::var("SQE_TEST_PASSWORD").unwrap_or_else(|_| "s3cr3t".to_string());

    // Real authenticator and QueryHandler, as in integration_test::test_trino_http_query.
    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    let authenticator = Arc::new(
        sqe_auth::Authenticator::new(&config.auth)
            .await
            .expect("Failed to create authenticator"),
    );
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let handler = Arc::new(
        sqe_coordinator::QueryHandler::new(
            policy,
            None,
            config,
            None,
            None,
            None,
            None,
            query_tracker,
            None,
            None, // grant_backend
            None, // lineage observer
            sqe_coordinator::RuntimeCatalogRegistry::default(),
            sqe_core::SecretStore::default(),
        )
        .expect("Failed to create QueryHandler"),
    );

    let trino_port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        probe.local_addr().expect("probe addr").port()
    };
    let node = sqe_trino_compat::server::NodeContext {
        version: "test".to_string(),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        started_at: std::time::Instant::now(),
    };
    let _server = sqe_trino_compat::server::start_trino_server(
        Arc::new(CliTrinoAuth(authenticator)),
        Arc::new(CliTrinoQuery(handler)),
        trino_port,
        node,
        None,
    );
    let origin = format!("http://127.0.0.1:{trino_port}");
    let probe = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("client");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while probe.get(format!("{origin}/v1/info")).send().await.is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "Trino listener did not come up"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let cert_dir = tempfile::tempdir().expect("tempdir");
    let public = start_tls_front(origin, cert_dir.path()).await;

    let mut transcript = String::new();
    for sql in STATEMENTS {
        transcript.push_str(&format!("== {sql} ==\n"));
        let cli = cli.clone();
        let (public, user, password) = (public.clone(), user.clone(), password.clone());
        // The CLI is a blocking JVM; keep it off the runtime threads that
        // serve the very requests it is making.
        let rendered =
            tokio::task::spawn_blocking(move || run_cli(&cli, &public, &user, &password, sql))
                .await
                .expect("cli task");
        transcript.push_str(&rendered);
        transcript.push('\n');
    }

    let path = golden_path();
    if std::env::var_os("UPDATE_GOLDENS").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture dir")).expect("mkdir");
        std::fs::write(&path, &transcript).expect("write golden");
        println!("wrote {}", path.display());
        return;
    }
    // The golden can only be recorded against a live stack with a JVM CLI
    // present, which the machine that first wired this test did not have. A
    // missing golden is therefore reported loudly and leaves the transcript
    // behind for review instead of failing the unfiltered gate.
    let Ok(expected) = std::fs::read_to_string(&path) else {
        let actual = std::env::temp_dir().join("sqe-trino-cli.golden.actual");
        std::fs::write(&actual, &transcript).expect("write transcript");
        eprintln!(
            "SKIP trino_cli_golden: no golden at {}\n  transcript written to {}\n  review it, then commit it as the golden (or re-run with UPDATE_GOLDENS=1)\n--- transcript ---\n{transcript}",
            path.display(),
            actual.display()
        );
        return;
    };
    assert!(
        transcript == expected,
        "trino-cli output drifted from {}\n--- expected ---\n{expected}\n--- actual ---\n{transcript}\nIf intentional, re-run with UPDATE_GOLDENS=1 and review the diff.",
        path.display()
    );
}

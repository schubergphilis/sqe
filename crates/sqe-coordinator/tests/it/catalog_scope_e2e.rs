//! Signed catalog scope (#908): acceptance 1-3 of
//! `openspec/changes/signed-catalog-scope`, against a test issuer.
//!
//! The fixture is the issuer: it signs scopes with a fixed ES256 test key and
//! serves the public key as a JWKS over https from a local listener (the
//! coordinator's own TLS test certificate; the verifier is configured with
//! `accept_invalid_certs`, as its docs allow for development). The gate under
//! test is the production one (`catalog_scope_gate`), installed from a
//! `[session.catalog_scope]` config exactly as coordinator startup does.
//!
//! - Acceptance 1 (a scoped client needs a valid scope) needs no catalog and
//!   always runs.
//! - Acceptance 2 and 3 (narrowing, read-only) read and write real Polaris
//!   warehouses and are `#[ignore = "needs:polaris"]`:
//!   `make test-integration FILTER=catalog_scope_e2e`.
//!
//! The gate is process-global (`install` also records the scoped client in
//! `sqe-core`), so every test holds `GATE_LOCK` and removes the gate on exit.
//! Sessions of other clients are unaffected by it, which the first test pins.

use std::sync::Arc;

use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use sqe_core::catalog_scope::TokenBinding;
use sqe_core::config::{CatalogConfig, CatalogScopeConfig, TlsConfig};
use sqe_core::{Session, SqeConfig};

const SCOPED_CLIENT: &str = "scope-test-runs";
const ISSUER: &str = "https://scope-issuer.test";
const KID: &str = "scope-key-1";
const SUBJECT: &str = "scope-user-1";

// The issuer's ES256 test keypair, the same public test material as the
// bearer provider's unit tests. NOT for production use.
// nosemgrep: opt.aikido.semgrep-rules.generic.secrets.security.detected-private-key -- published test key, signs only test JWTs
const SIGNING_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgcb/PT1Xj+xW0FvVc
0YZLwJCPra8OSopguEXNw3d3TbihRANCAASkMxNpk8e4ypVo60ZUB3Q4TIHOcXSJ
Gsk+TB4BOZOZvvRKOPj1Dj5tcKc/tv2Hx8kiS4egVEC2qWmKaBDbv9kN
-----END PRIVATE KEY-----";
const PUBLIC_X: &str = "pDMTaZPHuMqVaOtGVAd0OEyBznF0iRrJPkweATmTmb4";
const PUBLIC_Y: &str = "9Eo4-PUOPm1wpz-2_YfHySJLh6BUQLapaYpoENu_2Q0";

/// Serialises the tests: the gate and the scoped client are process-global.
static GATE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Removes the gate when a test ends, pass or fail.
struct GateGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        let _ = sqe_coordinator::catalog_scope_gate::install(None, None);
    }
}

/// The test issuer: a JWKS over https, and the gate installed against it.
struct Issuer {
    _gate: GateGuard,
    audit_path: std::path::PathBuf,
    audit: Arc<sqe_metrics::audit::AuditLogger>,
    _audit_dir: tempfile::TempDir,
}

impl Issuer {
    async fn start() -> Self {
        crate::common::init_tracing();
        let lock = GATE_LOCK.lock().await;
        let gate = GateGuard { _lock: lock };

        let jwks_url = serve_jwks().await;
        let config: CatalogScopeConfig = toml::from_str(&format!(
            "client_id = \"{SCOPED_CLIENT}\"\nissuer = \"{ISSUER}\"\njwks_url = \"{jwks_url}\"\n\
             accept_invalid_certs = true"
        ))
        .expect("scope config parses");
        // https is required: no `allow_insecure_jwks` here.
        assert!(config.jwks_url.starts_with("https://"));
        assert!(!config.allow_insecure_jwks);

        let audit_dir = tempfile::tempdir().expect("tempdir");
        let audit_path = audit_dir.path().join("audit.jsonl");
        let audit = Arc::new(
            sqe_metrics::audit::AuditLogger::new(audit_path.to_str().unwrap())
                .expect("audit logger opens"),
        );
        sqe_coordinator::catalog_scope_gate::install(Some(&config), Some(Arc::clone(&audit)))
            .expect("gate installs");
        Self {
            _gate: gate,
            audit_path,
            audit,
            _audit_dir: audit_dir,
        }
    }

    /// Audit lines that record a refused scope.
    fn refusals(&self) -> Vec<String> {
        self.audit.flush();
        std::fs::read_to_string(&self.audit_path)
            .unwrap_or_default()
            .lines()
            .filter(|l| l.contains("CATALOG_SCOPE_REFUSED"))
            .map(str::to_string)
            .collect()
    }
}

/// Serve `{"keys": [<the issuer's public key>]}` over https on an
/// OS-assigned loopback port; returns the JWKS URL. The server lives as long
/// as the test's runtime.
async fn serve_jwks() -> String {
    sqe_coordinator::tls::ensure_crypto_provider();
    let tls_dir = format!(
        "{}/../sqe-core/tests/fixtures/tls",
        env!("CARGO_MANIFEST_DIR")
    );
    let material = sqe_core::ServerTlsMaterial::load(&TlsConfig {
        cert_file: format!("{tls_dir}/server.crt"),
        key_file: format!("{tls_dir}/server.key"),
        ..TlsConfig::default()
    })
    .expect("load the TLS test certificate");
    let rustls_config =
        axum_server::tls_rustls::RustlsConfig::from_config(material.rustls_server_config());

    let jwks = json!({ "keys": [{
        "kty": "EC", "kid": KID, "alg": "ES256", "crv": "P-256",
        "use": "sig", "x": PUBLIC_X, "y": PUBLIC_Y
    }]});
    let app = axum::Router::new().route(
        "/jwks",
        axum::routing::get(move || {
            let body = jwks.clone();
            async move { axum::Json(body) }
        }),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the JWKS listener");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum_server::from_tcp_rustls(listener, rustls_config)
            .expect("serve tls")
            .serve(app.into_make_service())
            .await
            .expect("the JWKS server runs");
    });
    format!("https://127.0.0.1:{port}/jwks")
}

/// A compact ES256 JWS carrying `claims`, signed by `pem` with `kid`.
fn sign(claims: &Value, kid: &str, pem: &str) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    encode(
        &header,
        claims,
        &EncodingKey::from_ec_pem(pem.as_bytes()).expect("test key parses"),
    )
    .expect("sign the scope")
}

fn scope_claims(sub: &str, jti: &str, exp_offset_secs: i64, catalogs: Value) -> Value {
    json!({
        "iss": ISSUER,
        "sub": sub,
        "jti": jti,
        "exp": Utc::now().timestamp() + exp_offset_secs,
        "catalogs": catalogs,
    })
}

/// A scope the issuer signs for `jti`, valid for ten minutes.
fn scope_for(jti: &str, catalogs: Value) -> String {
    sign(
        &scope_claims(SUBJECT, jti, 600, catalogs),
        KID,
        SIGNING_KEY_PEM,
    )
}

/// A token id no other test (or run) uses.
fn fresh_jti(tag: &str) -> String {
    format!(
        "{tag}-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    )
}

fn bound(session: Session, client: Option<&str>, jti: &str) -> Session {
    session.with_token_binding(Some(TokenBinding {
        client_id: client.map(str::to_string),
        token_id: Some(jti.to_string()),
        subject: Some(SUBJECT.to_string()),
    }))
}

fn offline_session() -> Session {
    Session::new(
        "scope-user".to_string(),
        sqe_core::SecretString::new("tok_unused".to_string()),
        None,
        Utc::now() + chrono::Duration::hours(1),
        vec![],
    )
}

fn handler_for(config: SqeConfig) -> sqe_coordinator::QueryHandler {
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
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
        None,
        None,
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("QueryHandler::new succeeds")
}

fn offline_config() -> SqeConfig {
    toml::from_str::<SqeConfig>(
        r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "http://127.0.0.1:9/unused"
warehouse = "test_wh"

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#,
    )
    .expect("config parses")
}

/// Acceptance 1: a session of the configured client is refused without a
/// valid scope, with a reason and an audit record; other clients are
/// unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn acceptance_1_scoped_client_needs_a_valid_scope() {
    let issuer = Issuer::start().await;
    let handler = handler_for(offline_config());
    let jti = fresh_jti("a1");
    let session = bound(offline_session(), Some(SCOPED_CLIENT), &jti);
    let door = sqe_coordinator::catalog_scope_gate::attach;
    let catalogs = json!({ "alpha": "read_write", "beta": "read_only" });

    // Missing scope.
    let err = door(session.clone(), None, Some("127.0.0.1"))
        .await
        .expect_err("a missing scope refuses the session");
    assert!(
        err.to_string().contains("catalog scope is required"),
        "{err}"
    );
    let err = door(session.clone(), Some("   "), None)
        .await
        .expect_err("a blank scope is a missing scope");
    assert!(
        err.to_string().contains("catalog scope is required"),
        "{err}"
    );

    // Another token's scope (`jti`), another person's scope (`sub`), expired,
    // a signature the issuer's key does not verify, an unknown key, ES256 only.
    let other_jti = scope_for("some-other-jti", catalogs.clone());
    let err = door(session.clone(), Some(&other_jti), None)
        .await
        .expect_err("a scope for another jti is refused");
    assert!(err.to_string().contains("jti"), "{err}");

    let other_sub = sign(
        &scope_claims("someone-else", &jti, 600, catalogs.clone()),
        KID,
        SIGNING_KEY_PEM,
    );
    let err = door(session.clone(), Some(&other_sub), None)
        .await
        .expect_err("a scope for another subject is refused");
    assert!(err.to_string().contains("subject"), "{err}");

    let expired = sign(
        &scope_claims(SUBJECT, &jti, -600, catalogs.clone()),
        KID,
        SIGNING_KEY_PEM,
    );
    let err = door(session.clone(), Some(&expired), None)
        .await
        .expect_err("an expired scope is refused");
    assert!(err.to_string().contains("invalid"), "{err}");

    // Claims widened after signing: the header and payload of a scope that
    // grants every catalog, carrying the signature of the narrow one.
    let narrow = sign(
        &scope_claims(SUBJECT, &jti, 600, catalogs.clone()),
        KID,
        SIGNING_KEY_PEM,
    );
    let wide = sign(
        &scope_claims(SUBJECT, &jti, 600, json!({"*": "read_write"})),
        KID,
        SIGNING_KEY_PEM,
    );
    let signed_part = |t: &str| t.rsplit_once('.').expect("compact JWS").0.to_string();
    let signature = |t: &str| t.rsplit_once('.').expect("compact JWS").1.to_string();
    let forged = format!("{}.{}", signed_part(&wide), signature(&narrow));
    let err = door(session.clone(), Some(&forged), None)
        .await
        .expect_err("a scope whose claims changed after signing is refused");
    assert!(err.to_string().contains("invalid"), "{err}");

    let unknown_kid = sign(
        &scope_claims(SUBJECT, &jti, 600, catalogs.clone()),
        "unknown-kid",
        SIGNING_KEY_PEM,
    );
    door(session.clone(), Some(&unknown_kid), None)
        .await
        .expect_err("a scope under an unknown key id is refused");

    door(session.clone(), Some("not-a-jws"), None)
        .await
        .expect_err("garbage is refused");

    // Every refusal above left an audit record: names the reason, never the
    // scope or the bearer.
    let refusals = issuer.refusals();
    assert_eq!(
        refusals.len(),
        8,
        "one audit record per refusal: {refusals:?}"
    );
    for line in &refusals {
        assert!(
            !line.contains(&forged) && !line.contains(&other_jti),
            "{line}"
        );
        assert!(!line.contains("tok_unused"), "{line}");
    }

    // The valid scope opens the session, and it carries the catalogs.
    let scoped = door(session.clone(), Some(&scope_for(&jti, catalogs)), None)
        .await
        .expect("a valid scope opens the session");
    let scope = scoped.catalog_scope().expect("the scope is attached");
    assert!(scope.access("alpha").is_some() && scope.access("beta").is_some());
    assert!(scope.access("gamma").is_none());

    // A session of the scoped client that never went through a door that
    // reads the scope (MCP, Quack, web UI) is refused when it runs anything.
    let err = handler
        .execute(&session, "SELECT 1", None)
        .await
        .expect_err("no scope, no statement");
    assert!(err.to_string().contains("catalog scope"), "{err}");

    // Tokens of other clients, and sessions with no binding at all, behave as
    // before: no scope needed, none attached, even if they send one.
    for other in [
        bound(offline_session(), Some("trino-cli"), &fresh_jti("other")),
        offline_session(),
    ] {
        let passed = door(other.clone(), None, None)
            .await
            .expect("another client needs no scope");
        assert!(passed.catalog_scope().is_none());
        let ignored = door(other, Some("not-a-jws"), None)
            .await
            .expect("another client's scope header is ignored");
        assert!(ignored.catalog_scope().is_none());
        assert!(passed.effective_scope().expect("no refusal").is_none());
    }
}

// ---------------------------------------------------------------------------
// Live stack: acceptance 2 and 3
// ---------------------------------------------------------------------------

const CAT_A: &str = "scope_a";
const CAT_B: &str = "scope_b";
const CAT_C: &str = "scope_c";
const TABLE_A: &str = "scope_a.test_ns.sc908_a";
const TABLE_B: &str = "scope_b.disc_ns.sc908_b";
const TABLE_C: &str = "scope_c.access.sc908_c";

/// The test config with three Polaris warehouses as three catalogs: A is
/// `test_warehouse`, B `discovery_test_wh`, C the Polaris catalog `system`
/// (all created by `scripts/bootstrap-test.sh`). The same user (`root`) is
/// granted all three, so only the scope can hide one.
fn three_catalog_config() -> SqeConfig {
    let mut config = SqeConfig::load(&crate::common::test_config_path()).expect("load test config");
    let url = config.catalog.catalog_url.clone();
    for (name, warehouse) in [
        (CAT_A, "test_warehouse"),
        (CAT_B, "discovery_test_wh"),
        (CAT_C, "system"),
    ] {
        let cat: CatalogConfig = toml::from_str(&format!(
            "catalog_url = \"{url}\"\nwarehouse = \"{warehouse}\""
        ))
        .expect("catalog config parses");
        config.catalogs.insert(name.to_string(), cat);
    }
    config
}

/// `root`, authenticated against the live Polaris: no token binding, so no
/// scope applies.
async fn root_session() -> Session {
    let config = SqeConfig::load(&crate::common::test_config_path()).expect("load test config");
    let auth = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("create authenticator");
    auth.authenticate("root", "")
        .await
        .expect("authenticate as root")
}

async fn count(handler: &sqe_coordinator::QueryHandler, session: &Session, table: &str) -> i64 {
    let batches = handler
        .execute(session, &format!("SELECT count(*) AS n FROM {table}"), None)
        .await
        .unwrap_or_else(|e| panic!("count {table}: {e}"));
    crate::common::i64s(batches[0].column(0)).value(0)
}

async fn drop_tables(handler: &sqe_coordinator::QueryHandler, session: &Session) {
    for table in [TABLE_A, TABLE_B, TABLE_C, "scope_b.disc_ns.sc908_b_ctas"] {
        let _ = handler
            .execute(session, &format!("DROP TABLE IF EXISTS {table}"), None)
            .await;
    }
}

/// Acceptance 2 and 3, one stack bring-up: a scope of A (`read_write`) and B
/// (`read_only`) over three catalogs the user may all read.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn acceptance_2_and_3_a_scoped_session_sees_and_writes_only_its_catalogs() {
    let _issuer = Issuer::start().await;
    let handler = handler_for(three_catalog_config());
    let root = root_session().await;

    // Seed one table per catalog as the unscoped user, and show that the
    // grants alone allow reading all three.
    drop_tables(&handler, &root).await;
    for (table, id) in [(TABLE_A, 1), (TABLE_B, 2), (TABLE_C, 3)] {
        handler
            .execute(
                &root,
                &format!("CREATE TABLE {table} AS SELECT {id} AS id"),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("seed {table}: {e}"));
        assert_eq!(
            count(&handler, &root, table).await,
            1,
            "unscoped reads {table}"
        );
    }
    // B gets a second data file, so a `rewrite_data_files` with
    // `min_input_files => 2` has real work and would commit if it were let in
    // (with one file it skips, and a skip proves nothing).
    handler
        .execute(&root, &format!("INSERT INTO {TABLE_B} VALUES (4)"), None)
        .await
        .expect("second file in B");

    let jti = fresh_jti("a23");
    let scope = scope_for(&jti, json!({ CAT_A: "read_write", CAT_B: "read_only" }));
    let scoped = sqe_coordinator::catalog_scope_gate::attach(
        bound(root.clone(), Some(SCOPED_CLIENT), &jti),
        Some(&scope),
        None,
    )
    .await
    .expect("the valid scope opens the session");

    async {
        // -- Acceptance 2: only A and B exist. ------------------------------
        let err = handler
            .execute(&scoped, &format!("SELECT * FROM {TABLE_C}"), None)
            .await
            .expect_err("a catalog outside the scope is hidden");
        let msg = err.to_string();
        assert!(
            msg.to_ascii_lowercase().contains("not found"),
            "an out-of-scope catalog reads as not found, got: {msg}"
        );

        let batches = handler
            .execute(&scoped, "SHOW CATALOGS", None)
            .await
            .expect("SHOW CATALOGS");
        let mut listed: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                crate::common::utf8(b.column(0))
                    .iter()
                    .flatten()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect();
        listed.sort();
        assert_eq!(listed, vec![CAT_A.to_string(), CAT_B.to_string()]);

        // USE picks only scoped catalogs; ATTACH and DCL are refused outright.
        handler
            .execute(&scoped, &format!("USE {CAT_C}.access"), None)
            .await
            .expect_err("USE of a hidden catalog is refused");
        let err = handler
            .execute(
                &scoped,
                "GRANT SELECT ON TABLE test_ns.sc908_a TO ROLE r",
                None,
            )
            .await
            .expect_err("DCL is refused in a scoped session");
        assert!(err.to_string().contains("scope"), "{err}");

        // -- Acceptance 3: read A and B, write A; B refuses every write. ----
        assert_eq!(count(&handler, &scoped, TABLE_A).await, 1);
        assert_eq!(count(&handler, &scoped, TABLE_B).await, 2);
        handler
            .execute(&scoped, &format!("INSERT INTO {TABLE_A} VALUES (10)"), None)
            .await
            .expect("INSERT into the read_write catalog");
        assert_eq!(
            count(&handler, &scoped, TABLE_A).await,
            2,
            "the write to A landed"
        );

        for (what, sql) in [
            ("INSERT", format!("INSERT INTO {TABLE_B} VALUES (20)")),
            ("DELETE", format!("DELETE FROM {TABLE_B} WHERE id = 2")),
            (
                "UPDATE",
                format!("UPDATE {TABLE_B} SET id = 99 WHERE id = 2"),
            ),
            (
                "CREATE TABLE",
                "CREATE TABLE scope_b.disc_ns.sc908_b_new (id BIGINT)".to_string(),
            ),
            (
                "CTAS",
                "CREATE TABLE scope_b.disc_ns.sc908_b_ctas AS SELECT 1 AS id".to_string(),
            ),
            ("DROP TABLE", format!("DROP TABLE {TABLE_B}")),
            (
                "CALL",
                "CALL system.rewrite_data_files(table => 'scope_b.disc_ns.sc908_b', \
                 min_input_files => 2)"
                    .to_string(),
            ),
        ] {
            let err = handler
                .execute(&scoped, &sql, None)
                .await
                .expect_err(&format!("{what} into a read_only catalog is refused"));
            assert!(
                err.to_string().to_ascii_lowercase().contains("read-only"),
                "{what}: expected a read-only refusal, got: {err}"
            );
        }

        // A write into a catalog outside the scope is refused (the write path
        // words it "did not build for this session", the read path "not
        // found"; both name only what the caller typed), and C is checked
        // unchanged below.
        handler
            .execute(&scoped, &format!("INSERT INTO {TABLE_C} VALUES (30)"), None)
            .await
            .expect_err("INSERT into a hidden catalog is refused");
    }
    .await;

    // Nothing the refused statements aimed at changed (checked unscoped,
    // directly against the catalogs).
    assert_eq!(count(&handler, &root, TABLE_B).await, 2, "B is unchanged");
    assert_eq!(count(&handler, &root, TABLE_C).await, 1, "C is unchanged");
    for absent in [
        "scope_b.disc_ns.sc908_b_new",
        "scope_b.disc_ns.sc908_b_ctas",
    ] {
        handler
            .execute(&root, &format!("SELECT * FROM {absent}"), None)
            .await
            .expect_err(&format!("{absent} must not exist"));
    }

    drop_tables(&handler, &root).await;
    sqe_coordinator::session_context::invalidate_all_session_caches().await;
}

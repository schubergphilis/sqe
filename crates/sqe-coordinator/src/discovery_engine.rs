//! Engine reads for discovered warehouses (`[query] discovery_engine_read`).
//!
//! A warehouse that is not in config is built from the discovery template and
//! reads with the person's delegated keys. With `"template"`, it may read
//! with the template's engine `credentials` instead, but only after the
//! proof below. Any refusal leaves the delegated read in place, and so does
//! `"off"`, which runs no proof and writes no event.
//!
//! The proof, in order:
//! 1. the warehouse is on `discovery_engine_warehouses`;
//! 2. it is not a configured catalog key, warehouse alias, Hive root or
//!    reserved catalog name;
//! 3. the person's `GET /v1/config?warehouse=W` at the template URL succeeds;
//! 4. the engine's succeeds too, and both name the same `overrides.prefix`
//!    (a missing prefix on either side proves nothing).

use sqe_catalog::rest_catalog::warehouse_config_prefix;
use sqe_core::config::{CatalogAuthConfig, CatalogConfig, DiscoveryEngineRead};
use sqe_core::{Session, SqeConfig};

/// The proof step that refused a warehouse; its name goes in the audit event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    Allowlist,
    ConfiguredName,
    PersonConfig,
    EngineConfig,
    PrefixMismatch,
}

impl Refusal {
    pub(crate) fn step(self) -> &'static str {
        match self {
            Self::Allowlist => "allowlist",
            Self::ConfiguredName => "configured_name",
            Self::PersonConfig => "person_config",
            Self::EngineConfig => "engine_config",
            Self::PrefixMismatch => "prefix_mismatch",
        }
    }
}

/// The two `/v1/config` answers name one catalog: both carry a prefix and
/// the prefixes are equal.
pub(crate) fn same_catalog(person: Option<&str>, engine: Option<&str>) -> bool {
    matches!((person, engine), (Some(p), Some(e)) if p == e)
}

/// The template's engine credentials for the discovered `warehouse` when the
/// proof passes, else `None`. `template` is the discovery clone the caller
/// builds the catalog from (its URL is the only one called). Writes one audit
/// event when a proof ran; none with `"off"` or outside the session's
/// catalog scope (the build refuses that warehouse before any call).
pub(crate) async fn engine_credentials_for(
    warehouse: &str,
    config: &SqeConfig,
    template: &CatalogConfig,
    session: &Session,
) -> Option<CatalogAuthConfig> {
    if config.query.discovery_engine_read == DiscoveryEngineRead::Off {
        return None;
    }
    session.check_catalog_read(warehouse).ok()?;
    let (_, source) = config.discovery_template_catalog()?;
    // The engine credentials only ever go to the catalog they were
    // configured for: a clone of another catalog never gets them.
    if source.catalog_url != template.catalog_url {
        return None;
    }
    let credentials = source.credentials.clone()?;
    let CatalogAuthConfig::ClientCredentials { client_id, .. } = &credentials else {
        return None;
    };
    let verdict = prove(warehouse, config, template, &credentials, session).await;
    crate::engine_identity_wiring::record_discovery_proof(
        session,
        client_id,
        warehouse,
        verdict.map_err(Refusal::step),
    );
    verdict.ok().map(|()| credentials)
}

async fn prove(
    warehouse: &str,
    config: &SqeConfig,
    template: &CatalogConfig,
    credentials: &CatalogAuthConfig,
    session: &Session,
) -> Result<(), Refusal> {
    if !config.query.discovery_engine_allows(warehouse) {
        return Err(Refusal::Allowlist);
    }
    if sqe_core::system_catalog::is_reserved_catalog_name(warehouse)
        || config
            .configured_catalog_names()
            .iter()
            .any(|n| n == warehouse)
    {
        return Err(Refusal::ConfiguredName);
    }
    let auth = template.auth.clone().unwrap_or_default();
    let person_bearer =
        sqe_auth::per_catalog::resolve_bearer(&auth, session.access_token().expose())
            .await
            .map_err(|_| Refusal::PersonConfig)?;
    let person = warehouse_config_prefix(template, warehouse, &person_bearer)
        .await
        .map_err(|e| failed(warehouse, Refusal::PersonConfig, &e))?;
    let engine_bearer = crate::engine_identity_wiring::engine_token(credentials, warehouse)
        .await
        .map_err(|e| failed(warehouse, Refusal::EngineConfig, &e))?;
    let engine = warehouse_config_prefix(template, warehouse, &engine_bearer)
        .await
        .map_err(|e| failed(warehouse, Refusal::EngineConfig, &e))?;
    if same_catalog(person.as_deref(), engine.as_deref()) {
        Ok(())
    } else {
        Err(Refusal::PrefixMismatch)
    }
}

/// Log why a call failed (status or transport text, never a token).
fn failed(warehouse: &str, refusal: Refusal, error: &sqe_core::SqeError) -> Refusal {
    tracing::debug!(warehouse, step = refusal.step(), %error, "discovery engine proof call failed");
    refusal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_catalog_needs_two_equal_prefixes() {
        assert!(same_catalog(Some("ws1"), Some("ws1")));
        assert!(!same_catalog(Some("ws1"), Some("ws2")), "different");
        assert!(!same_catalog(None, Some("ws1")), "person has none");
        assert!(!same_catalog(Some("ws1"), None), "engine has none");
        assert!(!same_catalog(None, None), "neither");
    }

    #[test]
    fn refusal_steps_use_the_audit_names() {
        let steps = [
            Refusal::Allowlist,
            Refusal::ConfiguredName,
            Refusal::PersonConfig,
            Refusal::EngineConfig,
            Refusal::PrefixMismatch,
        ]
        .map(Refusal::step);
        assert_eq!(
            steps,
            [
                "allowlist",
                "configured_name",
                "person_config",
                "engine_config",
                "prefix_mismatch"
            ]
        );
    }

    mod proof {
        use super::*;
        use wiremock::matchers::{body_string_contains, header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const PERSON: &str = "person-tok";
        const ENGINE: &str = "engine-tok";

        fn engine_client_id() -> String {
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            format!("sqe-engine-{n}")
        }

        fn config(server: &MockServer, mode: DiscoveryEngineRead) -> SqeConfig {
            let toml_text = format!(
                r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "{uri}"
warehouse = "home"

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#,
                uri = server.uri()
            );
            let mut config = toml::from_str::<SqeConfig>(&toml_text).expect("test config parses");
            config.catalog.credentials = Some(CatalogAuthConfig::ClientCredentials {
                token_endpoint: format!("{}/token", server.uri()),
                // The engine token cache is process-wide and keyed by
                // endpoint and client; wiremock reuses ports, so a shared
                // client id would serve one test the token another fetched.
                client_id: engine_client_id(),
                client_secret: "s".into(),
                scope: Some("PRINCIPAL_ROLE:reader".into()),
                client_secret_file: None,
            });
            config.query.discovery_engine_read = mode;
            config.query.discovery_engine_warehouses = vec!["ws_*".into(), "home".into()];
            config
        }

        fn template(config: &SqeConfig, warehouse: &str) -> CatalogConfig {
            let mut cfg =
                crate::session_context::discovery_template(config).expect("REST template");
            cfg.warehouse = warehouse.to_string();
            cfg
        }

        fn person() -> Session {
            Session::new(
                "alice".to_string(),
                sqe_core::SecretString::new(PERSON.to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            )
        }

        fn prefix(p: Option<&str>) -> ResponseTemplate {
            let overrides = match p {
                Some(p) => serde_json::json!({ "prefix": p }),
                None => serde_json::json!({}),
            };
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "defaults": {}, "overrides": overrides }))
        }

        /// `/v1/config?warehouse=W` answers `response` to `bearer`, `times` times.
        async fn config_for(
            server: &MockServer,
            warehouse: &str,
            bearer: &str,
            response: ResponseTemplate,
            times: u64,
        ) {
            Mock::given(method("GET"))
                .and(path("/v1/config"))
                .and(query_param("warehouse", warehouse))
                .and(header("authorization", format!("Bearer {bearer}").as_str()))
                .respond_with(response)
                .expect(times)
                .mount(server)
                .await;
        }

        /// The engine token endpoint, called `times` times, with the
        /// configured scope sent as is.
        async fn engine_token(server: &MockServer, times: u64) {
            Mock::given(method("POST"))
                .and(path("/token"))
                .and(body_string_contains("grant_type=client_credentials"))
                .and(body_string_contains("scope=PRINCIPAL_ROLE%3Areader"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": ENGINE,
                    "token_type": "bearer",
                    "expires_in": 3600
                })))
                .expect(times)
                .mount(server)
                .await;
        }

        /// Nothing at all may reach the server.
        async fn no_calls(server: &MockServer) {
            Mock::given(wiremock::matchers::any())
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .mount(server)
                .await;
        }

        async fn credentials_for(config: &SqeConfig, warehouse: &str) -> Option<CatalogAuthConfig> {
            engine_credentials_for(warehouse, config, &template(config, warehouse), &person()).await
        }

        /// Off is today's behaviour: no proof, no token, no `/v1/config`, and
        /// the warehouse keeps the person's delegated keys.
        #[tokio::test]
        async fn off_runs_no_proof_and_keeps_delegated_keys() {
            let server = MockServer::start().await;
            no_calls(&server).await;
            let config = config(&server, DiscoveryEngineRead::Off);
            assert!(credentials_for(&config, "ws_a").await.is_none());
            assert!(
                template(&config, "ws_a").credentials.is_none(),
                "the discovery template carries no engine credentials"
            );
        }

        #[tokio::test]
        async fn a_proved_warehouse_reads_with_the_engine_credentials() {
            let server = MockServer::start().await;
            config_for(&server, "ws_a", PERSON, prefix(Some("p-a")), 1).await;
            config_for(&server, "ws_a", ENGINE, prefix(Some("p-a")), 1).await;
            engine_token(&server, 1).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            let got = credentials_for(&config, "ws_a").await;
            assert!(
                matches!(got, Some(CatalogAuthConfig::ClientCredentials { ref client_id, .. }) if client_id.starts_with("sqe-engine-")),
                "{got:?}"
            );
            assert!(
                template(&config, "ws_a").credentials.is_none(),
                "the clone gets credentials only from a proved verdict"
            );
        }

        /// The template picker here and in sqe-core agree, with and without
        /// a default catalog.
        #[tokio::test]
        async fn both_template_pickers_choose_the_default_catalog() {
            let server = MockServer::start().await;
            let mut config = config(&server, DiscoveryEngineRead::Template);
            for name in ["alpha", "beta"] {
                let mut cat = config.catalog.clone();
                cat.catalog_url = format!("http://{name}");
                cat.warehouse = name.into();
                config.catalogs.insert(name.into(), cat);
            }
            let mut urls = Vec::new();
            // No default: the first REST catalog. A default: that catalog.
            for default in [None, Some("beta")] {
                config.query.default_catalog = default.map(str::to_string);
                let (_, picked) = config.discovery_template_catalog().expect("core pick");
                let clone =
                    crate::session_context::discovery_template(&config).expect("coordinator pick");
                assert_eq!(clone.catalog_url, picked.catalog_url, "{default:?}");
                urls.push(clone.catalog_url);
            }
            assert_ne!(urls[0], urls[1], "the default moves the pick");
        }

        /// A clone whose URL is not the template catalog's never gets the
        /// engine credentials and calls nothing.
        #[tokio::test]
        async fn a_clone_of_another_catalog_gets_no_engine_credentials() {
            let server = MockServer::start().await;
            let elsewhere = MockServer::start().await;
            no_calls(&server).await;
            no_calls(&elsewhere).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            let mut clone = template(&config, "ws_a");
            clone.catalog_url = elsewhere.uri();
            assert!(engine_credentials_for("ws_a", &config, &clone, &person())
                .await
                .is_none());
        }

        #[tokio::test]
        async fn an_allowlist_miss_makes_no_call_and_keeps_delegated_keys() {
            let server = MockServer::start().await;
            no_calls(&server).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "other").await.is_none());
            assert!(
                credentials_for(&config, "WS_A").await.is_none(),
                "case-sensitive"
            );
        }

        #[tokio::test]
        async fn a_configured_or_reserved_name_makes_no_call() {
            let server = MockServer::start().await;
            no_calls(&server).await;
            let mut config = config(&server, DiscoveryEngineRead::Template);
            config.query.discovery_engine_warehouses = vec!["*".into()];
            for name in ["home", "system", "datafusion"] {
                assert!(credentials_for(&config, name).await.is_none(), "{name}");
            }
        }

        #[tokio::test]
        async fn a_config_prefix_mismatch_keeps_delegated_keys() {
            let server = MockServer::start().await;
            config_for(&server, "ws_a", PERSON, prefix(Some("p-a")), 1).await;
            config_for(&server, "ws_a", ENGINE, prefix(Some("p-other")), 1).await;
            engine_token(&server, 1).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "ws_a").await.is_none());
        }

        #[tokio::test]
        async fn a_missing_prefix_on_either_side_proves_nothing() {
            let server = MockServer::start().await;
            config_for(&server, "ws_a", PERSON, prefix(None), 1).await;
            config_for(&server, "ws_a", ENGINE, prefix(None), 1).await;
            config_for(&server, "ws_b", PERSON, prefix(Some("p-b")), 1).await;
            config_for(&server, "ws_b", ENGINE, prefix(None), 1).await;
            engine_token(&server, 1).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "ws_a").await.is_none());
            assert!(credentials_for(&config, "ws_b").await.is_none());
        }

        /// A person who cannot see the warehouse stops the proof before the
        /// engine is asked for anything.
        #[tokio::test]
        async fn a_person_refused_by_the_catalog_never_reaches_the_engine() {
            let server = MockServer::start().await;
            config_for(&server, "ws_a", PERSON, ResponseTemplate::new(403), 1).await;
            engine_token(&server, 0).await;
            config_for(&server, "ws_a", ENGINE, prefix(Some("p-a")), 0).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "ws_a").await.is_none());
        }

        #[tokio::test]
        async fn an_engine_refused_by_the_catalog_keeps_delegated_keys() {
            let server = MockServer::start().await;
            config_for(&server, "ws_a", PERSON, prefix(Some("p-a")), 1).await;
            config_for(&server, "ws_a", ENGINE, ResponseTemplate::new(403), 1).await;
            engine_token(&server, 1).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "ws_a").await.is_none());
        }

        /// Two discovered warehouses share one engine token.
        #[tokio::test]
        async fn two_warehouses_fetch_one_engine_token() {
            let server = MockServer::start().await;
            for (w, p) in [("ws_a", "p-a"), ("ws_b", "p-b")] {
                config_for(&server, w, PERSON, prefix(Some(p)), 1).await;
                config_for(&server, w, ENGINE, prefix(Some(p)), 1).await;
            }
            engine_token(&server, 1).await;
            let config = config(&server, DiscoveryEngineRead::Template);
            assert!(credentials_for(&config, "ws_a").await.is_some());
            assert!(credentials_for(&config, "ws_b").await.is_some());
        }
    }
}

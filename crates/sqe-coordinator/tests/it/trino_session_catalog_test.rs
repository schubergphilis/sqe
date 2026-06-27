//! Session-catalog / session-schema resolution for the Trino wire (BLOCKER 1).
//!
//! A Trino client sends `X-Trino-Catalog` / `X-Trino-Schema`, which land on
//! `Session::default_catalog` / `Session::default_schema`. The bug: the
//! DataFusion `SessionContext` built in `create_session_context` hardcoded its
//! default catalog from config (`main_warehouse`) and its default schema to
//! `"default"`, ignoring the session. So `FROM gold.t` resolved to
//! `main_warehouse.gold.t` and bare `FROM t` to `main_warehouse.default.t`.
//!
//! The fix makes the context honor the session: the default catalog becomes the
//! session catalog (a config catalog, or a Polaris warehouse discovered via the
//! same path 3-part names use), and the default schema becomes the session
//! schema. If the session catalog cannot be resolved for this principal, the
//! context falls back to the config default catalog so unqualified queries never
//! break with "unknown catalog".
//!
//! These tests build a real `SessionContext` against a mock Polaris REST
//! endpoint (empty namespaces are enough) and assert the DataFusion default
//! catalog/schema directly.

use std::sync::Arc;

use chrono::{Duration, Utc};
use sqe_coordinator::query_tracker::QueryTracker;
use sqe_coordinator::session_context::create_session_context;
use sqe_coordinator::RuntimeCatalogRegistry;
use sqe_core::{Session, SqeConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount the minimal Polaris REST surface `create_session_context` touches:
/// `/v1/config` (empty overrides) and `/v1/namespaces` (no namespaces). Path
/// matchers ignore the `?warehouse=` query, so the same mock serves both the
/// config catalog and any discovered warehouse.
async fn mount_empty_polaris() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

/// Polaris mock that serves one namespace (`gold`) with one table, so
/// `system.jdbc.tables` enumeration produces rows. Path matchers ignore the
/// `?warehouse=` query, so every warehouse (config + discovered) sees the same
/// content.
async fn mount_polaris_with_table() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[["gold"]]}"#))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/gold"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"namespace":["gold"],"properties":{}}"#),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/gold/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"identifiers":[{"namespace":["gold"],"name":"fct_revenue_monthly"}]}"#,
        ))
        .mount(&server)
        .await;
    server
}

fn session(token: &str, catalog: Option<&str>, schema: Option<&str>) -> Session {
    Session::new(
        format!("user_{token}"),
        sqe_core::SecretString::new(token.to_string()),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    )
    .with_catalog(catalog.map(str::to_string))
    .with_schema(schema.map(str::to_string))
}

async fn build_ctx(config: &SqeConfig, session: &Session) -> (String, String, bool) {
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let registry = RuntimeCatalogRegistry::default();
    let (ctx, _catalog) = create_session_context(
        config,
        session,
        None,
        &tracker,
        None,
        None,
        None,
        &registry,
    )
    .await
    .expect("create_session_context succeeds against mock Polaris");

    let cfg = ctx.copied_config();
    let opts = cfg.options();
    let default_catalog = opts.catalog.default_catalog.clone();
    let default_schema = opts.catalog.default_schema.clone();
    let session_catalog_registered = session
        .default_catalog
        .as_deref()
        .map(|c| ctx.catalog(c).is_some())
        .unwrap_or(false);
    (default_catalog, default_schema, session_catalog_registered)
}

/// BLOCKER 2: `system.jdbc.tables` / `system.jdbc.catalogs` must enumerate the
/// session's own (discovered) catalog, not just the default warehouse, so JDBC
/// schema sync (Metabase/DBeaver) sees workspace tables.
#[tokio::test]
async fn system_jdbc_enumerates_session_catalog_tables() {
    let server = mount_polaris_with_table().await;
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[query]\ncatalog_discovery = \"polaris-auto\"\n\n\
         [catalog]\ncatalog_url = \"{}\"\nwarehouse = \"main_warehouse\"\n",
        server.uri()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    let session = session("tok_enum", Some("ws_energy_co"), Some("gold"));

    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let registry = RuntimeCatalogRegistry::default();
    let (ctx, _catalog) = create_session_context(
        &config, &session, None, &tracker, None, None, None, &registry,
    )
    .await
    .expect("session context builds");

    // system.jdbc.catalogs must list the session catalog.
    let cats = ctx
        .sql("SELECT table_cat FROM system.jdbc.catalogs ORDER BY table_cat")
        .await
        .expect("query catalogs")
        .collect()
        .await
        .expect("collect catalogs");
    let cat_csv = batches_to_string(&cats);
    assert!(
        cat_csv.contains("ws_energy_co"),
        "system.jdbc.catalogs must include the session catalog: {cat_csv}"
    );

    // system.jdbc.tables filtered to the session catalog must return its table.
    let tables = ctx
        .sql(
            "SELECT table_cat, table_schem, table_name FROM system.jdbc.tables \
             WHERE table_cat = 'ws_energy_co' AND table_schem = 'gold'",
        )
        .await
        .expect("query tables")
        .collect()
        .await
        .expect("collect tables");
    let tbl_csv = batches_to_string(&tables);
    assert!(
        tbl_csv.contains("fct_revenue_monthly"),
        "system.jdbc.tables WHERE table_cat='ws_energy_co' must return the gold mart: {tbl_csv}"
    );
}

/// Render record batches to a flat string for substring assertions.
fn batches_to_string(batches: &[arrow_array::RecordBatch]) -> String {
    let mut out = String::new();
    for b in batches {
        for col in b.columns() {
            for row in 0..b.num_rows() {
                out.push_str(&arrow::util::display::array_value_to_string(col, row).unwrap_or_default());
                out.push(' ');
            }
        }
    }
    out
}

/// The real bug: a session catalog that is NOT in static config is discovered
/// via `polaris-auto` (the same path 3-part names use) and becomes the
/// DataFusion default catalog; the session schema becomes the default schema.
#[tokio::test]
async fn session_catalog_discovered_and_becomes_default() {
    let server = mount_empty_polaris().await;
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[query]\ncatalog_discovery = \"polaris-auto\"\n\n\
         [catalog]\ncatalog_url = \"{}\"\nwarehouse = \"main_warehouse\"\n",
        server.uri()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    let session = session("tok_discover", Some("ws_energy_co"), Some("gold"));

    let (default_catalog, default_schema, registered) = build_ctx(&config, &session).await;

    assert_eq!(
        default_catalog, "ws_energy_co",
        "discovered session catalog must be the DataFusion default catalog"
    );
    assert_eq!(
        default_schema, "gold",
        "session schema (X-Trino-Schema) must be the default schema"
    );
    assert!(
        registered,
        "the discovered session catalog must be registered so 2-part / bare names resolve against it"
    );
}

/// A session catalog that IS in static config becomes the default, overriding
/// the configured `query.default_catalog`.
#[tokio::test]
async fn session_catalog_from_config_overrides_config_default() {
    let server = mount_empty_polaris().await;
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[query]\ndefault_catalog = \"iceberg\"\n\n\
         [catalog]\ncatalog_url = \"{0}\"\nwarehouse = \"main_warehouse\"\n\n\
         [catalogs.ws_energy_co]\ncatalog_url = \"{0}\"\nwarehouse = \"ws_energy_co\"\n",
        server.uri()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    // Config default is "iceberg"; the session points at the in-config
    // "ws_energy_co", which must win.
    let session = session("tok_config", Some("ws_energy_co"), Some("gold"));

    let (default_catalog, default_schema, registered) = build_ctx(&config, &session).await;

    assert_eq!(default_catalog, "ws_energy_co");
    assert_eq!(default_schema, "gold");
    assert!(registered);
}

/// Guard: when the session names a catalog that cannot be resolved (discovery
/// off, or Polaris rejects it), the default catalog falls back to the config
/// default so unqualified queries never break. The session schema still
/// applies.
#[tokio::test]
async fn unresolvable_session_catalog_falls_back_to_config_default() {
    let server = mount_empty_polaris().await;
    // catalog_discovery defaults to static (off), so discovery returns None
    // for a non-config catalog without any network probe.
    let toml = format!(
        "[coordinator]\n\n[auth]\n\n[query]\ndefault_catalog = \"iceberg\"\n\n\
         [catalog]\ncatalog_url = \"{}\"\nwarehouse = \"main_warehouse\"\n",
        server.uri()
    );
    let config: SqeConfig = toml::from_str(&toml).expect("config parses");
    let session = session("tok_fallback", Some("ghost_catalog"), Some("gold"));

    let (default_catalog, default_schema, registered) = build_ctx(&config, &session).await;

    assert_eq!(
        default_catalog, "iceberg",
        "unresolvable session catalog must fall back to the config default"
    );
    assert_eq!(
        default_schema, "gold",
        "session schema applies even when the session catalog is unresolvable"
    );
    assert!(
        !registered,
        "an unresolvable session catalog must not be registered"
    );
}

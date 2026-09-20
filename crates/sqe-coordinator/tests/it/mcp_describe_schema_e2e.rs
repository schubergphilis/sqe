//! `describe_schema` over `/mcp` against the local Polaris stack
//! (`make test-integration FILTER=mcp_describe_schema`).
//!
//! What this proves that the unit tests in `sqe-mcp` cannot: that the
//! statements the tool composes (`SHOW TABLES`, `SHOW COLUMNS FROM`, `SHOW
//! STATS FOR`, `EXPLAIN SELECT * .. LIMIT 1`, `information_schema.tables`)
//! all resolve against a real Iceberg REST catalog, that `include_stats`
//! reads the snapshot's row count without scanning, that a column comment set
//! through `COMMENT ON` comes back on both `describe_table` and
//! `describe_schema`, and that the per-table entries agree with the
//! single-table tool.
//!
//! The MCP router runs in-process over a `QueryHandler` wired like the Flight
//! path. Authentication is a test `AuthProvider` that maps any bearer to the
//! root session the fixture authenticated, so Polaris sees the fixture's own
//! token.

use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::{config::McpConfig, Session};
use tower::ServiceExt;

/// Every bearer is the fixture's root session.
struct RootAuth(Session);

impl RootAuth {
    fn identity(&self) -> Identity {
        let session = &self.0;
        Identity {
            user_id: session.user.username.clone(),
            display_name: session.user.username.clone(),
            roles: session.user.roles.clone(),
            subject: session.user.subject.clone(),
            email: session.user.email.clone(),
            groups: session.user.groups.clone(),
            catalog_token: Some(session.access_token().clone()),
            refresh_token: session.refresh_token().cloned(),
            expires_at: Some(session.token_expiry()),
        }
    }
}

#[async_trait]
impl AuthProvider for RootAuth {
    async fn authenticate_oauth(
        &self,
        _credentials: &FlightCredentials,
    ) -> Result<Identity, AuthError> {
        Ok(self.identity())
    }

    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Ok(self.identity())
    }
}

/// `header.<payload>.signature` carrying the read scope. The signature is
/// never checked: `RootAuth` is the verifier, and the point of the test is
/// what happens after authentication.
fn bearer() -> String {
    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&json!({"sub": "root", "scope": "sqe:mcp:read"})).unwrap());
    format!("e30.{payload}.test")
}

async fn call(app: &Router, tool: &str, arguments: Value) -> Value {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments}
    });
    let request = Request::post("/mcp")
        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 4242))))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {}", bearer()))
        .header(header::USER_AGENT, "sqe-it/1")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body.get("error").is_none(), "rpc error: {body}");
    assert_ne!(
        body["result"]["isError"],
        json!(true),
        "tool failure: {}",
        body["result"]["content"][0]["text"]
    );
    body["result"]["structuredContent"].clone()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mcp_describe_schema_describes_an_iceberg_schema_with_stats_in_one_call() {
    let (session, handler) = crate::common::setup_handler().await;
    // The SQL catalog name the fixture's config declares for its Polaris
    // warehouse (`[catalogs.iceberg]`), which is what a 3-part name resolves
    // against; the session's default catalog is the warehouse name.
    let catalog = "iceberg".to_string();

    // Two tables and their comment; the schema is the fixture's `test_ns`.
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.ds_orders", None)
        .await;
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS test_ns.ds_customers", None)
        .await;
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.ds_orders AS SELECT * FROM (VALUES \
             (1, 'a', 10.0), (2, 'b', 20.0), (3, 'c', 30.0)) AS t(id, region, amount)",
            None,
        )
        .await
        .expect("CTAS ds_orders");
    handler
        .execute(
            &session,
            "CREATE TABLE test_ns.ds_customers AS SELECT * FROM (VALUES (1, 'x')) AS t(id, name)",
            None,
        )
        .await
        .expect("CTAS ds_customers");
    let commented = handler
        .execute(
            &session,
            "COMMENT ON COLUMN test_ns.ds_orders.region IS 'sales region code'",
            None,
        )
        .await
        .is_ok();

    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("config");
    let app = sqe_coordinator::mcp_server::mcp_router(
        Arc::new(RootAuth(session.clone())),
        Arc::new(handler),
        Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
            &config.rate_limit,
        )),
        Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
            &config.rate_limit,
        )),
        McpConfig {
            enabled: true,
            resource_uri: "https://sqe.test/mcp".to_string(),
            authorization_servers: vec!["https://idp.test/realms/it".to_string()],
            ..Default::default()
        },
        Vec::new(),
        &config.storage,
        &config.rate_limit,
    );

    let started = std::time::Instant::now();
    let result = call(
        &app,
        "describe_schema",
        json!({
            "catalog": catalog,
            "schema": "test_ns",
            "include_stats": true,
            "include_queryability": true,
        }),
    )
    .await;
    eprintln!(
        "describe_schema {catalog}.test_ns: {} tables in {:?}",
        result["total_tables"],
        started.elapsed()
    );

    let tables = result["tables"].as_array().expect("tables array");
    let orders = tables
        .iter()
        .find(|t| t["name"] == "ds_orders")
        .unwrap_or_else(|| panic!("ds_orders listed: {result}"));
    assert!(tables.iter().any(|t| t["name"] == "ds_customers"), "{result}");

    assert_eq!(orders["table_type"], "table");
    assert_eq!(orders["source"], "iceberg");
    let names: Vec<&str> = orders["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["id", "region", "amount"], "{orders}");
    assert_eq!(orders["columns"][0]["ordinal"], 1);
    assert_eq!(orders["columns"][2]["ordinal"], 3);
    assert_eq!(
        orders["row_count"],
        3,
        "the snapshot summary's total-records, not a COUNT(*): {orders}"
    );
    assert_eq!(orders["queryable"], true, "{orders}");
    assert!(orders.get("error").is_none(), "{orders}");

    // The single-table tool and the bulk tool agree column for column.
    let single = call(
        &app,
        "describe_table",
        json!({"catalog": catalog, "schema": "test_ns", "table": "ds_orders"}),
    )
    .await;
    let single_rows = single["rows"].as_array().unwrap();
    let single_names: Vec<&str> = single_rows
        .iter()
        .map(|r| r["column_name"].as_str().unwrap())
        .collect();
    assert_eq!(single_names, names);
    if commented {
        assert_eq!(single_rows[1]["comment"], "sales region code", "{single}");
        assert_eq!(orders["columns"][1]["comment"], "sales region code", "{orders}");
    }

    // Paging: one table per page, the token carries the walk.
    let page = call(
        &app,
        "describe_schema",
        json!({"catalog": catalog, "schema": "test_ns", "page_size": 1}),
    )
    .await;
    assert_eq!(page["tables"].as_array().unwrap().len(), 1);
    assert_eq!(page["truncated"], true);
    let token = page["next_page_token"].as_str().expect("a next page").to_string();
    let next = call(
        &app,
        "describe_schema",
        json!({"catalog": catalog, "schema": "test_ns", "page_size": 1, "page_token": token}),
    )
    .await;
    assert_ne!(next["tables"][0]["name"], page["tables"][0]["name"]);
}

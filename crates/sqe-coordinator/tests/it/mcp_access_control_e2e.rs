//! MCP over the live Polaris + Ranger + Keycloak stack
//! (`quickstart/polaris-ranger-keycloak`): the same fixture, personas and
//! policies as `access_control_e2e`, driven through `/mcp` instead of the
//! handler directly.
//!
//! Run with `scripts/access-control-test.sh`; gated on `SQE_AC_E2E=1` like the
//! rest of the suite (see `access_control_e2e` for why a gate that skips when
//! it never ran is worse than none).
//!
//! What this proves that the unit tests in `sqe-mcp` cannot: that a masked
//! column comes back masked THROUGH the MCP tools, that a row filter shrinks
//! `sample_rows`, that a table the user may not read fails over MCP while the
//! same call works for the admin, and that the change tool's role and catalog
//! gates sit in front of a real Polaris grant. Before this the masking suite
//! drove Flight SQL only, and "MCP reaches the same QueryHandler" was reasoned,
//! not measured.
//!
//! The MCP router runs in-process over a `QueryHandler` wired exactly like the
//! Flight path (shared warm `TableMetadataCache`), so the only thing under test
//! is the MCP layer. Authentication is a test `AuthProvider` that maps a
//! synthetic bearer (`sub` = persona, scopes in the payload) to the persona's
//! REAL Keycloak session, so Polaris still sees the user's own token.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    Router,
};
use base64::Engine;
use serde_json::{json, Value};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::{config::McpConfig, Session};
use tower::ServiceExt;

use crate::access_control_e2e::{
    ac_setup, col_strings, exec_ok, grant_read_to_both_roles, hive_mask_policy,
    hive_rowfilter_policy, total_rows, AcCtx, ORDERS,
};
use crate::common::ranger_fixture::PREFIX;

macro_rules! ac_gate {
    () => {
        if !crate::common::ac_enabled() {
            eprintln!(
                "skipping mcp_access_control_e2e: set SQE_AC_E2E=1 (use scripts/access-control-test.sh)"
            );
            return;
        }
    };
}

const READ_WRITE_SCOPES: &str = "sqe:mcp:read sqe:mcp:write";

/// Maps the `sub` of a synthetic bearer to a persona's live Keycloak session.
/// The catalog token handed to SQE is the persona's REAL access token, so the
/// Polaris side of the test is untouched by the shortcut.
struct SessionAuth(HashMap<String, Session>);

impl SessionAuth {
    fn identity_for(&self, token: &str) -> Result<Identity, AuthError> {
        let payload = token
            .split('.')
            .nth(1)
            .ok_or_else(|| AuthError::AuthFailed("not a JWT".into()))?;
        let claims: Value = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| AuthError::AuthFailed("unreadable payload".into()))?;
        let sub = claims["sub"]
            .as_str()
            .ok_or_else(|| AuthError::AuthFailed("no sub".into()))?;
        let session = self
            .0
            .get(sub)
            .ok_or_else(|| AuthError::AuthFailed(format!("unknown persona {sub}")))?;
        Ok(Identity {
            user_id: session.user.username.clone(),
            display_name: session.user.username.clone(),
            roles: session.user.roles.clone(),
            subject: session.user.subject.clone(),
            email: session.user.email.clone(),
            groups: session.user.groups.clone(),
            catalog_token: Some(session.access_token().clone()),
            refresh_token: session.refresh_token().cloned(),
            expires_at: Some(session.token_expiry()),
            scopes: vec![],
        })
    }
}

#[async_trait]
impl AuthProvider for SessionAuth {
    async fn authenticate_oauth(
        &self,
        credentials: &FlightCredentials,
    ) -> Result<Identity, AuthError> {
        let token = credentials
            .bearer_token
            .as_ref()
            .ok_or(AuthError::NotMyCredentials)?;
        self.identity_for(token.expose())
    }

    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Err(AuthError::NotMyCredentials)
    }
}

/// `header.<payload>.signature` with the persona as `sub` and both MCP scopes.
/// The signature is never checked here: `SessionAuth` is the verifier, and the
/// point of the test is what happens AFTER authentication.
fn bearer_for(persona: &str) -> String {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({"sub": persona, "scope": READ_WRITE_SCOPES})).unwrap(),
    );
    format!("e30.{payload}.test")
}

/// The MCP router over a fresh handler that shares the fixture's warm cache.
/// `write_catalogs` is the only knob tests vary.
async fn mcp_app(ctx: &AcCtx, write_catalogs: Vec<String>) -> Router {
    let (handler, _cache) =
        crate::common::setup_ranger_handler_sharing(Some(ctx.cache.clone()), |_| {}).await;
    let config = sqe_core::SqeConfig::load(&crate::common::ranger_config_path())
        .expect("load tests/sqe-ranger-test.toml");
    let sessions = [&ctx.carol, &ctx.alice, &ctx.bob, &ctx.dave]
        .into_iter()
        .map(|s| (s.user.username.clone(), s.clone()))
        .collect();
    let mcp = McpConfig {
        enabled: true,
        resource_uri: "https://sqe.test/mcp".to_string(),
        authorization_servers: vec!["https://keycloak.test/realms/iceberg".to_string()],
        allow_writes: true,
        write_roles: vec!["engineer".to_string()],
        write_catalogs,
        ..Default::default()
    };
    sqe_coordinator::mcp_server::mcp_router(
        Arc::new(SessionAuth(sessions)),
        Arc::new(handler),
        Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
            &config.rate_limit,
        )),
        Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
            &config.rate_limit,
        )),
        mcp,
        Vec::new(),
        &config.storage,
        &config.rate_limit,
    )
}

/// One `tools/call` as `persona`; returns the JSON-RPC body.
async fn call(app: &Router, persona: &str, tool: &str, arguments: Value) -> Value {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments}
    });
    let request = Request::post("/mcp")
        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 4242))))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {}", bearer_for(persona)))
        .header(header::USER_AGENT, "sqe-it/1")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "tools/call is JSON-RPC: transport-level status is 200 even for a refused tool"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// The rows of a successful tool result, or a description of why it was not one.
fn rows(body: &Value) -> Result<Vec<Value>, String> {
    if let Some(error) = body.get("error") {
        return Err(format!("rpc error: {error}"));
    }
    let result = &body["result"];
    if result["isError"] == json!(true) {
        return Err(format!(
            "tool failure: {}",
            result["content"][0]["text"].as_str().unwrap_or("?")
        ));
    }
    Ok(result["structuredContent"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

fn column(rows: &[Value], name: &str) -> Vec<Value> {
    rows.iter().map(|r| r[name].clone()).collect()
}

fn read_sql(sql: &str) -> Value {
    json!({"sql": sql})
}

fn orders_ref() -> Value {
    json!({"catalog": "sales_wh", "schema": "ac", "table": "orders"})
}

// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:ranger; run scripts/access-control-test.sh"]
async fn mcp_reads_are_denied_for_the_ungranted_user_and_valid_for_the_admin() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    let app = mcp_app(&ctx, Vec::new()).await;
    let sql = format!("SELECT id FROM {ORDERS} ORDER BY id");

    // Control: the statement is valid and the fixture holds three rows.
    let carol = rows(&call(&app, "carol", "execute_sql_readonly", read_sql(&sql)).await)
        .expect("carol reads the fixture over MCP");
    assert_eq!(column(&carol, "id"), vec![json!(1), json!(2), json!(3)]);

    // alice holds no grant on orders: the same call is a tool failure, not a
    // transport error, and never a result. Waited for, because ac_setup's
    // REVOKEs reach Polaris on its Ranger poll.
    crate::common::eventually("alice's MCP read of orders to be denied", || async {
        match rows(&call(&app, "alice", "execute_sql_readonly", read_sql(&sql)).await) {
            Err(_) => Ok(()),
            Ok(r) => Err(format!("alice still reads {} rows over MCP", r.len())),
        }
    })
    .await;

    // Metadata tools show alice exactly what SHOW TABLES shows her over the
    // handler: parity, whichever way the catalog answers an ungranted lister.
    let via_handler = exec_ok(&ctx, &ctx.alice, "SHOW TABLES FROM sales_wh.ac").await;
    let mut handler_names = col_strings(&via_handler, "Table");
    handler_names.sort();
    let via_mcp = rows(&call(
        &app,
        "alice",
        "list_tables",
        json!({"catalog": "sales_wh", "schema": "ac"}),
    )
    .await)
    .expect("list_tables as alice");
    let mut mcp_names: Vec<String> = column(&via_mcp, "Table")
        .into_iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    mcp_names.sort();
    assert_eq!(
        mcp_names, handler_names,
        "list_tables over MCP must show alice the same tables as SHOW TABLES over the handler"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:ranger; run scripts/access-control-test.sh"]
async fn mcp_masks_and_row_filters_match_the_handler() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mcp-mask-amount"),
            "amount",
            json!({"dataMaskType": "MASK_NULL"}),
        ))
        .await
        .expect("create amount mask");
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mcp-mask-ssn"),
            "ssn",
            json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create ssn mask");
    ctx.ranger
        .create_policy(hive_rowfilter_policy(
            &format!("{PREFIX}mcp-rowfilter"),
            "region = 'EU'",
        ))
        .await
        .expect("create row filter");

    let sql = format!("SELECT id, amount, ssn FROM {ORDERS} ORDER BY id");

    // Settle through the handler first, so a slow policy cache shows up as a
    // handler failure and not as an MCP finding.
    let bob_handler = crate::common::eventually("bob's policies to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if total_rows(&b) == 2 && col_strings(&b, "amount").iter().all(|v| v == "NULL") => {
                Ok(b)
            }
            Ok(b) => Err(format!(
                "rows={} amount={:?}",
                total_rows(&b),
                col_strings(&b, "amount")
            )),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&bob_handler, "id"), vec!["1", "3"]);

    // The same statement over MCP, as bob: masked, filtered, identical.
    let app = mcp_app(&ctx, Vec::new()).await;
    let bob = rows(&call(&app, "bob", "execute_sql_readonly", read_sql(&sql)).await)
        .expect("bob reads over MCP");
    assert_eq!(column(&bob, "id"), vec![json!(1), json!(3)], "row filter over MCP");
    assert_eq!(
        column(&bob, "amount"),
        vec![Value::Null, Value::Null],
        "MASK_NULL over MCP"
    );
    assert_eq!(
        column(&bob, "ssn"),
        vec![json!("xxx-xx-1111"), json!("xxx-xx-3333")],
        "MASK_SHOW_LAST_4 over MCP"
    );

    // sample_rows is SELECT * LIMIT n as the caller: same filter, same masks.
    let sample = rows(&call(&app, "bob", "sample_rows", orders_ref()).await).expect("sample_rows as bob");
    assert_eq!(sample.len(), 2, "sample_rows sees only the EU rows");
    assert!(
        sample.iter().all(|r| r["amount"].is_null()),
        "sample_rows masks amount: {sample:?}"
    );

    // alice is analyst-only: none of the engineer policies apply to her.
    let alice = rows(&call(&app, "alice", "execute_sql_readonly", read_sql(&sql)).await)
        .expect("alice reads over MCP");
    assert_eq!(column(&alice, "id"), vec![json!(1), json!(2), json!(3)]);
    assert_eq!(
        column(&alice, "amount"),
        vec![json!(10.0), json!(20.0), json!(30.0)]
    );
    assert_eq!(column(&alice, "ssn")[0], json!("111-11-1111"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:ranger; run scripts/access-control-test.sh"]
async fn mcp_change_tool_sits_behind_role_catalog_and_source_grants() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    for stmt in [
        format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
        format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
        format!("GRANT INSERT ON {ORDERS} TO ROLE \"engineer\""),
    ] {
        exec_ok(&ctx, &ctx.carol, &stmt).await;
    }
    let app = mcp_app(&ctx, vec!["sales_wh".to_string()]).await;
    let insert = |id: i64| {
        json!({"sql": format!(
            "INSERT INTO {ORDERS} VALUES ({id},'EU',40.0,'444-44-4444','d@x',DATE '2024-01-01')"
        )})
    };

    // bob: engineer (in write_roles), target in write_catalogs, INSERT granted
    // at the source. Waited for: the Polaris-side grant lands on its poll.
    crate::common::eventually("bob's MCP INSERT to be allowed", || async {
        match rows(&call(&app, "bob", "execute_sql_change", insert(4)).await) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await;
    let after = exec_ok(&ctx, &ctx.carol, &format!("SELECT id FROM {ORDERS} ORDER BY id")).await;
    assert_eq!(col_strings(&after, "id"), vec!["1", "2", "3", "4"]);

    // alice: analyst, not a writer role. Refused by the MCP role gate before
    // any SQL runs, as a JSON-RPC error, not a tool failure.
    let alice = call(&app, "alice", "execute_sql_change", insert(5)).await;
    assert_eq!(alice["error"]["code"], -32003, "{alice}");
    assert!(
        alice["error"]["message"]
            .as_str()
            .unwrap()
            .contains("writer role"),
        "{alice}"
    );

    // bob against a catalog outside write_catalogs: refused by the catalog
    // gate, whatever the source would have said.
    let other = call(
        &app,
        "bob",
        "execute_sql_change",
        json!({"sql": "INSERT INTO ops_wh.ac.audit VALUES (3,'mcp')"}),
    )
    .await;
    assert_eq!(other["error"]["code"], -32003, "{other}");
    assert!(
        other["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not in mcp.write_catalogs"),
        "{other}"
    );

    // dave holds no role at all and no grant: a tool failure from the source,
    // with the fixture untouched.
    let dave = call(&app, "dave", "execute_sql_change", insert(6)).await;
    assert!(
        dave.get("error").is_some() || dave["result"]["isError"] == json!(true),
        "dave must not write: {dave}"
    );
    let still = exec_ok(&ctx, &ctx.carol, &format!("SELECT count(*) AS n FROM {ORDERS}")).await;
    assert_eq!(col_strings(&still, "n"), vec!["4"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:ranger; run scripts/access-control-test.sh"]
async fn mcp_search_tables_finds_the_fixture_in_its_catalog() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    let app = mcp_app(&ctx, Vec::new()).await;

    // Not information_schema: `sales_wh.information_schema.tables` is answered
    // by DataFusion's built-in listing and never held `ac.orders` here
    // (measured: 40 system rows, zero fixture tables). search_tables walks
    // SHOW SCHEMAS / SHOW TABLES, which do reach the catalog.
    let found = rows(&call(
        &app,
        "carol",
        "search_tables",
        json!({"pattern": "ORD", "catalog": "sales_wh"}),
    )
    .await)
    .expect("search_tables as carol");
    assert!(
        found
            .iter()
            .any(|r| r["table_schema"] == json!("ac") && r["table_name"] == json!("orders")),
        "search is a case-insensitive substring match: {found:?}"
    );

    // No catalog and no session default: the tool says what to pass instead
    // of guessing a catalog.
    let missing = call(&app, "carol", "search_tables", json!({"pattern": "orders"})).await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");
    assert!(
        missing["error"]["message"]
            .as_str()
            .unwrap()
            .contains("needs a catalog"),
        "{missing}"
    );

    // A pattern cannot break out of its literal.
    let injected = call(
        &app,
        "carol",
        "search_tables",
        json!({"pattern": "x' OR '1'='1", "catalog": "sales_wh"}),
    )
    .await;
    assert_eq!(
        rows(&injected).expect("the quote is a character, not syntax").len(),
        0
    );
}

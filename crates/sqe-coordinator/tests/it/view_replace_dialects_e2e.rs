//! `CREATE OR REPLACE VIEW` keeps other engines' texts (#900): a replace
//! that would drop a `spark` representation is refused unless the statement
//! says `WITH (drop_dialects = 'spark')`.
//!
//! SQE writes only `sqe` SQL, so the `spark` text is committed straight to
//! Polaris, as a migrator would.

use arrow_array::cast::AsArray;

const POLARIS: &str = "http://localhost:18181/api/catalog";
const WAREHOUSE: &str = "test_warehouse";

async fn polaris_root_token(http: &reqwest::Client) -> String {
    let resp: serde_json::Value = http
        .post(format!("{POLARIS}/v1/oauth/tokens"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", "root"),
            ("client_secret", "s3cr3t"),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ])
        .send()
        .await
        .expect("token request")
        .json()
        .await
        .expect("token json");
    resp["access_token"]
        .as_str()
        .expect("access_token")
        .to_string()
}

fn view_url(view: &str) -> String {
    format!("{POLARIS}/v1/{WAREHOUSE}/namespaces/test_ns/views/{view}")
}

async fn load_view(http: &reqwest::Client, token: &str, view: &str) -> serde_json::Value {
    http.get(view_url(view))
        .bearer_auth(token)
        .send()
        .await
        .expect("load view")
        .error_for_status()
        .expect("load view status")
        .json()
        .await
        .expect("view json")
}

/// The dialects of the view's current version, in order.
async fn current_dialects(http: &reqwest::Client, token: &str, view: &str) -> Vec<String> {
    let doc = load_view(http, token, view).await;
    let md = &doc["metadata"];
    let current = &md["current-version-id"];
    let version = md["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| &v["version-id"] == current)
        .expect("current version");
    version["representations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["dialect"].as_str().unwrap().to_string())
        .collect()
}

/// Commit a new current version holding the current `sqe` text plus a
/// `spark` text, as an engine other than SQE would.
async fn add_spark_text(http: &reqwest::Client, token: &str, view: &str) {
    let doc = load_view(http, token, view).await;
    let md = &doc["metadata"];
    let current = &md["current-version-id"];
    let versions = md["versions"].as_array().unwrap();
    let cur = versions
        .iter()
        .find(|v| &v["version-id"] == current)
        .expect("current version");
    let next = versions
        .iter()
        .map(|v| v["version-id"].as_i64().unwrap())
        .max()
        .unwrap()
        + 1;
    let mut reps = cur["representations"].as_array().unwrap().clone();
    reps.push(serde_json::json!({
        "type": "sql",
        "sql": "SELECT CAST(1 AS BIGINT) AS a",
        "dialect": "spark",
    }));
    let body = serde_json::json!({
        "requirements": [{"type": "assert-view-uuid", "uuid": md["view-uuid"]}],
        "updates": [
            {
                "action": "add-view-version",
                "view-version": {
                    "version-id": next,
                    "schema-id": cur["schema-id"],
                    "timestamp-ms": chrono::Utc::now().timestamp_millis(),
                    "summary": {"engine-name": "spark"},
                    "representations": reps,
                    "default-namespace": cur["default-namespace"],
                }
            },
            {"action": "set-current-view-version", "view-version-id": -1}
        ]
    });
    http.post(view_url(view))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .expect("replace view")
        .error_for_status()
        .expect("raw replace accepted");
}

async fn first_int(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    let col = arrow::compute::cast(batches[0].column(0), &arrow_schema::DataType::Int64).unwrap();
    col.as_primitive::<arrow_array::types::Int64Type>().value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn view_replace_keeps_spark_text_unless_dropped() {
    let (session, handler) = crate::common::setup_handler().await;
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..10];
    let view = format!("vrd_{suffix}");
    let other = format!("vrd_new_{suffix}");
    let fq = format!("iceberg.test_ns.{view}");
    let http = reqwest::Client::new();
    let token = polaris_root_token(&http).await;

    let create = format!("CREATE VIEW {fq} AS SELECT CAST(1 AS BIGINT) AS a");
    handler
        .execute(&session, &create, None)
        .await
        .expect(&create);
    add_spark_text(&http, &token, &view).await;
    assert_eq!(
        current_dialects(&http, &token, &view).await,
        ["sqe", "spark"]
    );

    // 1. A plain replace is refused and names the dialect.
    let replace = format!("CREATE OR REPLACE VIEW {fq} AS SELECT CAST(2 AS BIGINT) AS a");
    let err = handler
        .execute(&session, &replace, None)
        .await
        .expect_err("replace would drop the spark text");
    let text = err.to_string();
    assert!(text.contains("spark"), "{text}");
    assert!(text.contains("drop_dialects"), "{text}");
    assert_eq!(
        current_dialects(&http, &token, &view).await,
        ["sqe", "spark"]
    );
    assert_eq!(
        first_int(&handler, &session, &format!("SELECT a FROM {fq}")).await,
        1
    );

    // 2. With the option it succeeds and the new version is `sqe` only.
    let opt_in = format!(
        "CREATE OR REPLACE VIEW {fq} WITH (drop_dialects = 'spark') \
         AS SELECT CAST(2 AS BIGINT) AS a"
    );
    handler
        .execute(&session, &opt_in, None)
        .await
        .expect(&opt_in);
    assert_eq!(current_dialects(&http, &token, &view).await, ["sqe"]);
    assert_eq!(
        first_int(&handler, &session, &format!("SELECT a FROM {fq}")).await,
        2
    );
    let props = &load_view(&http, &token, &view).await["metadata"]["properties"];
    assert!(props.get("drop_dialects").is_none(), "{props}");
    assert!(
        props.get("replace.drop-dialect.allowed").is_none(),
        "the catalog waiver must not outlive the statement: {props}"
    );

    // 3. An `sqe`-only view is replaced without the option.
    let again = format!("CREATE OR REPLACE VIEW {fq} AS SELECT CAST(3 AS BIGINT) AS a");
    handler.execute(&session, &again, None).await.expect(&again);
    assert_eq!(
        first_int(&handler, &session, &format!("SELECT a FROM {fq}")).await,
        3
    );

    // 4. DROP VIEW and CREATE VIEW on a new name behave as before.
    let drop = format!("DROP VIEW {fq}");
    handler.execute(&session, &drop, None).await.expect(&drop);
    let fresh = format!("CREATE VIEW iceberg.test_ns.{other} AS SELECT CAST(4 AS BIGINT) AS a");
    handler.execute(&session, &fresh, None).await.expect(&fresh);
    let read = format!("SELECT a FROM iceberg.test_ns.{other}");
    assert_eq!(first_int(&handler, &session, &read).await, 4);
    let cleanup = format!("DROP VIEW iceberg.test_ns.{other}");
    handler
        .execute(&session, &cleanup, None)
        .await
        .expect(&cleanup);
}

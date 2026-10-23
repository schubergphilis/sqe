//! `{catalog}` in `[storage.tvf] allowed_object_store_prefixes` against the
//! live stack with `catalog_discovery = "polaris-auto"`.
//!
//! `discovery_test_wh` plays workspace ws1 (a discovered catalog) and
//! `test_warehouse` plays ws2 (the config catalog):
//! - root, a member of ws1, reads ws1's staging path in a ws1 session;
//! - root in a ws2 session reads ws2's path and is refused ws1's;
//! - a principal with no grant on either is refused both, even when its
//!   session names the catalog: naming a catalog is not access to it.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Duration, Utc};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use sqe_core::{SecretString, Session, SqeConfig};

const POLARIS: &str = "http://localhost:18181/api";
const WS1: &str = "discovery_test_wh";
const WS2: &str = "test_warehouse";
const BUCKET: &str = "warehouse-discovery";
const OUTSIDER: &str = "tvf_catalog_outsider";

fn ws_path(catalog: &str) -> String {
    format!("s3://{BUCKET}/ingest/ws/{catalog}/x.parquet")
}

fn config() -> SqeConfig {
    let mut cfg = SqeConfig::load(&crate::common::test_config_path()).expect("load test config");
    cfg.query.catalog_discovery = sqe_core::config::CatalogDiscovery::PolarisAuto;
    cfg.storage.tvf.allowed_object_store_prefixes =
        vec![format!("s3://{BUCKET}/ingest/ws/{{catalog}}/")];
    cfg.storage.tvf.allow_inline_credentials = false;
    cfg
}

fn handler(config: SqeConfig) -> sqe_coordinator::QueryHandler {
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    sqe_coordinator::QueryHandler::new(
        Arc::new(sqe_policy::PassthroughEnforcer),
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
    .expect("QueryHandler::new")
}

async fn polaris_token(client_id: &str, client_secret: &str) -> String {
    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{POLARIS}/catalog/v1/oauth/tokens"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ])
        .send()
        .await
        .expect("token request")
        .error_for_status()
        .expect("token granted")
        .json()
        .await
        .expect("token json");
    resp["access_token"]
        .as_str()
        .expect("access_token")
        .to_string()
}

/// A fresh Polaris principal with no catalog grants; returns its token.
async fn outsider_token(root: &str) -> String {
    let http = reqwest::Client::new();
    let _ = http
        .delete(format!("{POLARIS}/management/v1/principals/{OUTSIDER}"))
        .bearer_auth(root)
        .send()
        .await;
    let created: serde_json::Value = http
        .post(format!("{POLARIS}/management/v1/principals"))
        .bearer_auth(root)
        .json(&serde_json::json!({ "principal": { "name": OUTSIDER } }))
        .send()
        .await
        .expect("create principal")
        .error_for_status()
        .expect("principal created")
        .json()
        .await
        .expect("principal json");
    let creds = &created["credentials"];
    polaris_token(
        creds["clientId"].as_str().expect("clientId"),
        creds["clientSecret"].as_str().expect("clientSecret"),
    )
    .await
}

fn session(user: &str, token: String, catalog: &str) -> Session {
    let mut s = Session::new(
        user.to_string(),
        SecretString::new(token),
        None,
        Utc::now() + Duration::hours(1),
        vec![],
    );
    s.default_catalog = Some(catalog.to_string());
    s
}

async fn upload_fixture() {
    let store = AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_endpoint("http://localhost:19000")
        .with_region("us-east-1")
        .with_access_key_id("s3admin")
        .with_secret_access_key("s3admin")
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("S3 client");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("batch");
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    let payload = PutPayload::from(buf);
    for ws in [WS1, WS2] {
        store
            .put(
                &ObjectPath::from(format!("ingest/ws/{ws}/x.parquet")),
                payload.clone(),
            )
            .await
            .expect("upload fixture");
    }
}

async fn read(
    handler: &sqe_coordinator::QueryHandler,
    s: &Session,
    path: &str,
) -> Result<usize, String> {
    let sql = format!("SELECT id FROM read_parquet('{path}')");
    handler
        .execute(s, &sql, None)
        .await
        .map(|b| b.iter().map(|b| b.num_rows()).sum())
        .map_err(|e| e.to_string())
}

fn assert_refused(result: Result<usize, String>, who: &str) {
    let err = result.expect_err(who);
    assert!(
        err.contains("allowed_object_store_prefixes"),
        "{who}: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn catalog_placeholder_opens_only_the_members_own_workspace() {
    crate::common::init_tracing();
    upload_fixture().await;
    let root = polaris_token("root", "s3cr3t").await;
    let outsider = outsider_token(&root).await;
    let handler = handler(config());

    let member_in_ws1 = session("root", root.clone(), WS1);
    assert_eq!(
        read(&handler, &member_in_ws1, &ws_path(WS1)).await,
        Ok(3),
        "a member reads its workspace's staging path"
    );

    let member_in_ws2 = session("root", root, WS2);
    assert_eq!(read(&handler, &member_in_ws2, &ws_path(WS2)).await, Ok(3));
    assert_refused(
        read(&handler, &member_in_ws2, &ws_path(WS1)).await,
        "a member scoped to ws2 reading ws1's path",
    );

    for ws in [WS1, WS2] {
        assert_refused(
            read(
                &handler,
                &session(OUTSIDER, outsider.clone(), ws),
                &ws_path(ws),
            )
            .await,
            &format!("a non-member naming {ws}"),
        );
    }
}

/// `read_csv` and `read_json` take the same `[storage]` key and the same
/// prefix gate as `read_parquet`: inside the workspace's staging prefix they
/// return the rows, the same file outside it is an error, never 0 rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn file_tvf_csv_and_json_read_inside_the_prefix_and_refuse_outside() {
    crate::common::init_tracing();
    let store = AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_endpoint("http://localhost:19000")
        .with_region("us-east-1")
        .with_access_key_id("s3admin")
        .with_secret_access_key("s3admin")
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("S3 client");
    let files = [
        ("x.csv", "id\n1\n2\n3\n"),
        ("x.json", "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n"),
    ];
    for dir in [format!("ingest/ws/{WS1}"), "ingest/other".to_string()] {
        for (name, body) in files {
            store
                .put(
                    &ObjectPath::from(format!("{dir}/{name}")),
                    PutPayload::from(body.as_bytes().to_vec()),
                )
                .await
                .expect("upload fixture");
        }
    }
    let root = polaris_token("root", "s3cr3t").await;
    let handler = handler(config());
    let member = session("root", root, WS1);

    for (func, name) in [("read_csv", "x.csv"), ("read_json", "x.json")] {
        let count = |path: String| {
            let sql = format!("SELECT id FROM {func}('{path}')");
            let (handler, member) = (&handler, &member);
            async move {
                handler
                    .execute(member, &sql, None)
                    .await
                    .map(|b| b.iter().map(|b| b.num_rows()).sum::<usize>())
                    .map_err(|e| e.to_string())
            }
        };
        let inside = format!("s3://{BUCKET}/ingest/ws/{WS1}/{name}");
        assert_eq!(count(inside).await, Ok(3), "{func} inside the prefix");
        let outside = format!("s3://{BUCKET}/ingest/other/{name}");
        assert_refused(count(outside).await, &format!("{func} outside the prefix"));
    }
}

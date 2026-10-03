//! Namespace and table snapshots under concurrent writers (dbt-trino runs
//! models in parallel on connections that share one JWT).
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

const POLARIS: &str = "http://localhost:18181/api/catalog";
const WAREHOUSE: &str = "test_warehouse";

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", &uuid::Uuid::new_v4().simple().to_string()[..10])
}

fn rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Root token for writing to Polaris directly, as a writer outside SQE (or
/// another coordinator replica) would.
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
    resp["access_token"].as_str().expect("access_token").to_string()
}

/// Two sessions of one user: one creates a schema and a table, the other
/// reads them straight away. dbt had to run with `max_active_tasks=1` to
/// avoid "schema not found" here.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn a_parallel_session_sees_a_schema_and_table_created_by_another() {
    let (writer, handler) = crate::common::setup_handler().await;
    let mut reader = writer.clone();
    reader.id = uuid::Uuid::new_v4().to_string();
    let ns = unique("cbs_silver");

    // The reader has planned against the catalog before the DDL.
    handler
        .execute(&reader, "SELECT 1 FROM test_ns.nonexistent_probe_table LIMIT 0", None)
        .await
        .ok();

    handler
        .execute(&writer, &format!("CREATE SCHEMA IF NOT EXISTS {ns}"), None)
        .await
        .expect("create schema");
    handler
        .execute(&writer, &format!("CREATE TABLE {ns}.t AS SELECT 1 AS id"), None)
        .await
        .expect("ctas");

    let read = handler
        .execute(&reader, &format!("SELECT id FROM {ns}.t"), None)
        .await
        .expect("the parallel session must see the new schema and table");
    assert_eq!(rows(&read), 1);
    let listed = handler
        .execute(
            &reader,
            &format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{ns}'"
            ),
            None,
        )
        .await
        .expect("information_schema");
    assert_eq!(rows(&listed), 1, "the new table is listed for the other session");

    handler.execute(&writer, &format!("DROP TABLE {ns}.t"), None).await.ok();
    handler.execute(&writer, &format!("DROP SCHEMA {ns}"), None).await.ok();
}

/// A namespace created outside this coordinator (another replica, Spark,
/// Airflow) right after a re-list: the miss is resolved by one probe of the
/// catalog instead of "schema not found" for the rest of the cooldown.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn a_namespace_created_outside_sqe_is_found_on_the_next_statement() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("ext_ns");

    // Plan once so the session has a namespace snapshot, then miss on a name
    // that does not exist: that spends the re-list and starts the cooldown.
    handler.execute(&session, "SELECT 1", None).await.expect("warm");
    let missing = unique("missing");
    let err = handler
        .execute(&session, &format!("SELECT * FROM {missing}.t"), None)
        .await
        .expect_err("a namespace that does not exist still misses");
    assert!(err.to_string().to_lowercase().contains("not found"), "{err}");

    // Another writer creates the namespace and a table, bypassing SQE.
    let http = reqwest::Client::new();
    let token = polaris_root_token(&http).await;
    let created = http
        .post(format!("{POLARIS}/v1/{WAREHOUSE}/namespaces"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"namespace": [ns], "properties": {}}))
        .send()
        .await
        .expect("create namespace");
    assert!(created.status().is_success(), "{}", created.status());
    let created = http
        .post(format!("{POLARIS}/v1/{WAREHOUSE}/namespaces/{ns}/tables"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "t",
            "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": false, "type": "long"}
            ]}
        }))
        .send()
        .await
        .expect("create table");
    assert!(created.status().is_success(), "{}", created.status());

    // Immediately, well inside the 5 s re-list cooldown.
    let read = handler
        .execute(&session, &format!("SELECT id FROM {ns}.t"), None)
        .await
        .expect("a namespace created outside SQE is resolved by the miss probe");
    assert_eq!(rows(&read), 0);

    handler.execute(&session, &format!("DROP TABLE {ns}.t"), None).await.ok();
    handler.execute(&session, &format!("DROP SCHEMA {ns}"), None).await.ok();
}

//! `DROP SCHEMA ... CASCADE` (#895): the cascade drops the schema's views and
//! tables; without it a non-empty schema is a clear error; a schema with child
//! schemas is refused untouched; a cascade refused part-way names what was
//! dropped and what is left; `DROP SCHEMA a, b` drops both.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

const POLARIS: &str = "http://localhost:18181/api/catalog";
const WAREHOUSE: &str = "test_warehouse";

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

fn rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Root token for Polaris, to create a child namespace SQE's SQL cannot
/// name (`CREATE SCHEMA a.b` reads `a` as the catalog).
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

async fn schema_listed(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
) -> bool {
    let sql =
        format!("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{ns}'");
    let listed = handler
        .execute(session, &sql, None)
        .await
        .expect("schemata");
    rows(&listed) > 0
}

async fn schema_with_tables_and_view(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
) {
    for sql in [
        format!("CREATE SCHEMA {ns}"),
        format!("CREATE TABLE {ns}.t1 AS SELECT 1 AS id"),
        format!("CREATE TABLE {ns}.t2 AS SELECT 'a' AS name"),
        format!("CREATE VIEW {ns}.v AS SELECT id FROM {ns}.t1"),
    ] {
        handler
            .execute(session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cascade_drops_the_schemas_views_and_tables() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("cascade");
    schema_with_tables_and_view(&handler, &session, &ns).await;

    handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect("DROP SCHEMA ... CASCADE drops a schema with tables and a view");

    assert!(
        !schema_listed(&handler, &session, &ns).await,
        "the schema is gone"
    );
    for object in ["t1", "t2", "v"] {
        let err = handler
            .execute(&session, &format!("SELECT * FROM {ns}.{object}"), None)
            .await
            .expect_err("every object in the schema is gone");
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "{object}: {err}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn without_cascade_a_non_empty_schema_is_a_clear_error_and_nothing_is_dropped() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("restrict");
    schema_with_tables_and_view(&handler, &session, &ns).await;

    for sql in [
        format!("DROP SCHEMA {ns}"),
        format!("DROP SCHEMA IF EXISTS {ns} RESTRICT"),
    ] {
        let err = handler
            .execute(&session, &sql, None)
            .await
            .expect_err("a non-empty schema is not dropped without CASCADE");
        let msg = err.to_string();
        assert!(
            msg.contains("not empty") && msg.contains("CASCADE"),
            "{sql}: {msg}"
        );
    }
    let read = handler
        .execute(&session, &format!("SELECT id FROM {ns}.v"), None)
        .await
        .expect("the view and its table are still there");
    assert_eq!(rows(&read), 1);

    handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect("cleanup");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cascade_refuses_a_schema_with_child_schemas_before_dropping_anything() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("parent");
    schema_with_tables_and_view(&handler, &session, &ns).await;

    let http = reqwest::Client::new();
    let token = polaris_root_token(&http).await;
    let created = http
        .post(format!("{POLARIS}/v1/{WAREHOUSE}/namespaces"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"namespace": [ns, "child"], "properties": {}}))
        .send()
        .await
        .expect("create child namespace");
    assert!(created.status().is_success(), "{}", created.status());

    let err = handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect_err("a schema with a child schema is refused");
    let msg = err.to_string();
    assert!(
        msg.contains("contains the schemas") && msg.contains("Nothing was dropped"),
        "{msg}"
    );
    let read = handler
        .execute(&session, &format!("SELECT id FROM {ns}.v"), None)
        .await
        .expect("nothing was dropped");
    assert_eq!(rows(&read), 1);

    let deleted = http
        .delete(format!("{POLARIS}/v1/{WAREHOUSE}/namespaces/{ns}%1Fchild"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete child namespace");
    assert!(deleted.status().is_success(), "{}", deleted.status());
    handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect("cleanup");
}

/// The cascade drops with the caller's token. A caller who may drop views but
/// not tables gets the view dropped, then a refusal on the first table that
/// names what was dropped and what is still there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn a_cascade_refused_part_way_names_what_was_dropped_and_what_is_left() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("partial");
    schema_with_tables_and_view(&handler, &session, &ns).await;

    let root = polaris_root_token(&reqwest::Client::new()).await;
    let name = "drop_cascade_view_dropper";
    let token =
        crate::dml_write_precheck_e2e::principal_token(&root, name, &["VIEW_LIST", "VIEW_DROP"])
            .await;
    let dropper = crate::dml_write_precheck_e2e::session_for(name, token);

    let err = handler
        .execute(&dropper, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect_err("the caller may not drop the tables");
    let msg = err.to_string();
    assert!(
        msg.contains("A cascade is not atomic")
            && msg.contains(&format!("Dropped: view {ns}.v."))
            && msg.contains(&format!("table {ns}.t1"))
            && msg.contains(&format!("table {ns}.t2"))
            && msg.contains("and the schema itself"),
        "{msg}"
    );

    let view = handler
        .execute(&session, &format!("SELECT id FROM {ns}.v"), None)
        .await
        .expect_err("the view was dropped");
    assert!(
        view.to_string().to_lowercase().contains("not found"),
        "{view}"
    );
    for table in ["t1", "t2"] {
        let read = handler
            .execute(&session, &format!("SELECT * FROM {ns}.{table}"), None)
            .await
            .unwrap_or_else(|e| panic!("{table} is still there: {e}"));
        assert_eq!(rows(&read), 1, "{table}");
    }
    assert!(
        schema_listed(&handler, &session, &ns).await,
        "the schema is still there"
    );

    handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect("cleanup");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cascade_on_a_missing_schema_honours_if_exists() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = unique("missing");

    handler
        .execute(
            &session,
            &format!("DROP SCHEMA IF EXISTS {ns} CASCADE"),
            None,
        )
        .await
        .expect("IF EXISTS ... CASCADE on a missing schema succeeds");
    let err = handler
        .execute(&session, &format!("DROP SCHEMA {ns} CASCADE"), None)
        .await
        .expect_err("without IF EXISTS a missing schema is an error");
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn drop_schema_with_two_names_drops_both() {
    let (session, handler) = crate::common::setup_handler().await;
    let (a, b) = (unique("two_a"), unique("two_b"));
    for ns in [&a, &b] {
        handler
            .execute(&session, &format!("CREATE SCHEMA {ns}"), None)
            .await
            .expect("create");
    }

    handler
        .execute(&session, &format!("DROP SCHEMA {a}, {b}"), None)
        .await
        .expect("DROP SCHEMA a, b");

    assert!(!schema_listed(&handler, &session, &a).await, "{a} is gone");
    assert!(
        !schema_listed(&handler, &session, &b).await,
        "{b} is gone, not skipped"
    );
}

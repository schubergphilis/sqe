//! Table DDL keeps Ranger's tag projection in step (#928): SET TBLPROPERTIES,
//! CREATE [OR REPLACE] / CTAS, RENAME and every drop path project their tag
//! change. A recording projector stands in for Ranger and replays the calls
//! into the tag store it would hold.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use sqe_policy::tag_projector::{ColumnTags, TagProjector, TagTableKey};

/// One `project` call: the table name, what Ranger held, what it holds after.
type Call = (String, ColumnTags, ColumnTags);

#[derive(Default)]
struct Recording {
    calls: Mutex<Vec<Call>>,
    fail: bool,
}

#[async_trait::async_trait]
impl TagProjector for Recording {
    async fn project(
        &self,
        table: &TagTableKey,
        previous: &ColumnTags,
        tags: &ColumnTags,
    ) -> sqe_core::Result<()> {
        if self.fail {
            return Err(sqe_core::SqeError::Execution(
                "injected projection failure".to_string(),
            ));
        }
        let call = (table.table.clone(), previous.clone(), tags.clone());
        self.calls.lock().expect("calls").push(call);
        Ok(())
    }
    fn enabled(&self) -> bool {
        true
    }
}

impl Recording {
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("calls").clone()
    }

    /// What Ranger holds for `table` after every call so far.
    fn held(&self, table: &str) -> ColumnTags {
        let calls = self.calls();
        let last = calls.iter().rev().find(|(t, _, _)| t == table);
        last.map(|(_, _, tags)| tags.clone()).unwrap_or_default()
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

fn tags(pairs: &[(&str, &str)]) -> ColumnTags {
    let mut map = BTreeMap::new();
    for (column, tag) in pairs {
        map.entry(column.to_string())
            .or_insert_with(Vec::new)
            .push(tag.to_string());
    }
    map
}

/// A handler whose projector is `projector`, and an admin session (tag
/// properties and `system.drop_table` are admin-only).
async fn setup(projector: Arc<Recording>) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let (mut session, handler) = crate::common::setup_handler().await;
    session.user.roles = vec!["service_admin".to_string()];
    (session, handler.with_tag_projector(projector))
}

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn set_tags_sql(table: &str, map: &str) -> String {
    format!("ALTER TABLE default.{table} SET TBLPROPERTIES ('sqe.column-tags' = '{map}')")
}

async fn tagged_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) {
    let create = format!("CREATE TABLE default.{table} AS SELECT 1 AS id, 'x' AS ssn");
    exec(handler, session, &create).await;
    exec(handler, session, &set_tags_sql(table, r#"{"ssn":["pii"]}"#)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_set_tblproperties_adds_before_it_removes() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let t = unique("tagproj928_props");
    let create = format!("CREATE TABLE default.{t} AS SELECT 1 AS id, 'x' AS ssn, 'y' AS email");
    exec(&handler, &session, &create).await;

    exec(&handler, &session, &set_tags_sql(&t, r#"{"ssn":["pii"]}"#)).await;
    assert_eq!(projector.held(&t), tags(&[("ssn", "pii")]));

    let before = projector.calls().len();
    let replace = set_tags_sql(&t, r#"{"email":["pii"]}"#);
    exec(&handler, &session, &replace).await;
    assert_eq!(projector.held(&t), tags(&[("email", "pii")]));
    // The new tag lands before the old one goes: Ranger never holds less
    // than either side of the change.
    let change: Vec<Call> = projector.calls()[before..].to_vec();
    let union = tags(&[("ssn", "pii"), ("email", "pii")]);
    assert_eq!(change.len(), 2, "add, then remove: {change:?}");
    assert_eq!(change[0].2, union, "first the union: {change:?}");

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_failed_set_tblproperties_commits_nothing() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let t = unique("tagproj928_fail");
    tagged_table(&handler, &session, &t).await;

    let failing = Arc::new(Recording {
        fail: true,
        ..Recording::default()
    });
    let (_, failing_handler) = setup(failing).await;
    let err = failing_handler
        .execute(&session, &set_tags_sql(&t, r#"{"id":["pii"]}"#), None)
        .await
        .expect_err("a failed projection must fail the statement");
    assert!(err.to_string().contains("nothing was committed"), "{err}");

    let show = format!("SHOW TAGS ON default.{t}");
    let shown = handler.execute(&session, &show, None).await.expect("tags");
    let text = arrow::util::pretty::pretty_format_batches(&shown)
        .expect("format")
        .to_string();
    assert!(text.contains("ssn"), "the old tags stay: {text}");
    assert!(!text.contains("| id"), "new tag committed: {text}");

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_rename_tags_the_destination_then_clears_the_source() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let (a, b) = (unique("tagproj928_a"), unique("tagproj928_b"));
    tagged_table(&handler, &session, &a).await;

    let before = projector.calls().len();
    let rename = format!("ALTER TABLE default.{a} RENAME TO default.{b}");
    exec(&handler, &session, &rename).await;
    let change: Vec<Call> = projector.calls()[before..].to_vec();
    let order: Vec<&str> = change.iter().map(|(t, _, _)| t.as_str()).collect();
    assert_eq!(order, [b.as_str(), a.as_str()], "{change:?}");
    assert_eq!(projector.held(&b), tags(&[("ssn", "pii")]));
    assert_eq!(projector.held(&a), ColumnTags::new());

    exec(&handler, &session, &format!("DROP TABLE default.{b}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_every_drop_path_clears_the_tags() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let plain = unique("tagproj928_drop");
    let call = unique("tagproj928_call");
    let purge = unique("tagproj928_purge");
    let call_sql = format!("CALL system.drop_table(table => 'default.{call}')");
    let drops = [
        (plain.clone(), format!("DROP TABLE default.{plain}")),
        (call.clone(), call_sql),
        (purge.clone(), format!("DROP TABLE default.{purge} PURGE")),
    ];
    for (table, drop) in &drops {
        tagged_table(&handler, &session, table).await;
        assert_eq!(projector.held(table), tags(&[("ssn", "pii")]), "{drop}");
        exec(&handler, &session, drop).await;
        assert_eq!(projector.held(table), ColumnTags::new(), "{drop}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_cascade_clears_the_tags() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let ns = unique("tagproj928_ns");
    let t = "tagged";
    exec(&handler, &session, &format!("CREATE SCHEMA {ns}")).await;
    let create = format!("CREATE TABLE {ns}.{t} AS SELECT 1 AS id, 'x' AS ssn");
    exec(&handler, &session, &create).await;
    let map = r#"{"ssn":["pii"]}"#;
    let set = format!("ALTER TABLE {ns}.{t} SET TBLPROPERTIES ('sqe.column-tags' = '{map}')");
    exec(&handler, &session, &set).await;
    assert_eq!(projector.held(t), tags(&[("ssn", "pii")]));

    exec(&handler, &session, &format!("DROP SCHEMA {ns} CASCADE")).await;
    assert_eq!(projector.held(t), ColumnTags::new());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_projection_928_create_and_replace_project_their_tags() {
    let projector = Arc::new(Recording::default());
    let (session, handler) = setup(projector.clone()).await;
    let t = unique("tagproj928_ctas");
    let create = format!(
        "CREATE TABLE default.{t} TBLPROPERTIES \
         ('sqe.column-tags' = '{{\"ssn\":[\"pii\"],\"gone\":[\"pii\"]}}') \
         AS SELECT 1 AS id, 'x' AS ssn, 'y' AS gone"
    );
    exec(&handler, &session, &create).await;
    assert_eq!(projector.held(&t), tags(&[("gone", "pii"), ("ssn", "pii")]));

    // `gone` is dropped by the replace, and its tag with it (#896).
    let replace = format!("CREATE OR REPLACE TABLE default.{t} AS SELECT 2 AS id, 'z' AS ssn");
    exec(&handler, &session, &replace).await;
    assert_eq!(projector.held(&t), tags(&[("ssn", "pii")]));

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

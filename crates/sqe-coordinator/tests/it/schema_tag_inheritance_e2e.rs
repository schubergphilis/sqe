//! A schema's tags are inherited by the columns of its tables (#924): a tag
//! policy on a key the schema carries masks or filters as if every column
//! carried it, once `policy.inherit_schema_tags` is on.
//!
//! The policies come from an in-process store; the Ranger run of the same
//! rules belongs to the access-control suite.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use datafusion::logical_expr::{col, lit, Expr};
use sqe_core::config::MaskPrecedence;
use sqe_core::SessionUser;
use sqe_policy::{MaskType, PolicyStore, ResolvedPolicy, TagMaskSpec};

/// `pii` nullifies, `eu` keeps the rows with `id > 1`; no resource policies.
struct SchemaTagPolicies;

#[async_trait::async_trait]
impl PolicyStore for SchemaTagPolicies {
    async fn resolve(
        &self,
        _user: &SessionUser,
        _table: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }

    async fn resolve_tags(
        &self,
        _user: &SessionUser,
        tags: &HashSet<String>,
    ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
        let masks = tags
            .contains("pii")
            .then(|| ("pii".to_string(), TagMaskSpec::Ready(MaskType::Nullify)))
            .into_iter()
            .collect();
        let filters = if tags.contains("eu") {
            vec![col("id").gt(lit(1))]
        } else {
            vec![]
        };
        (masks, filters, HashSet::new())
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

/// A tag-enforcing handler with schema tag inheritance `inherit`, and an
/// admin session (tag writes are admin-only in the test config).
async fn setup(inherit: bool) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let store: Arc<dyn PolicyStore> = Arc::new(SchemaTagPolicies);
    let (mut session, handler) = crate::common::setup_handler_with_schema_tag_inheritance(
        store,
        MaskPrecedence::Tag,
        inherit,
    )
    .await;
    session.user.roles = vec!["service_admin".to_string()];
    (session, handler)
}

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A fresh schema `ns` holding `t` with `(1, 'a')` and `(2, 'b')` as
/// `(id, ssn)`.
async fn schema_with_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    t: &str,
) {
    exec(handler, session, &format!("CREATE SCHEMA {ns}")).await;
    table(handler, session, ns, t).await;
}

async fn table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    t: &str,
) {
    let create =
        format!("CREATE TABLE {ns}.{t} AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, ssn)");
    exec(handler, session, &create).await;
}

async fn drop_schema(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
) {
    exec(handler, session, &format!("DROP SCHEMA {ns} CASCADE")).await;
}

/// `(id, ssn)` rows as text, NULL as `None`, ordered by the raw ssn.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    t: &str,
) -> Vec<(Option<String>, Option<String>)> {
    let sql = format!("SELECT CAST(id AS VARCHAR), ssn FROM {ns}.{t} ORDER BY ssn");
    let batches = handler
        .execute(session, &sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for batch in &batches {
        let ids = crate::common::utf8(batch.column(0).as_ref());
        let ssns = crate::common::utf8(batch.column(1).as_ref());
        for row in 0..batch.num_rows() {
            let text = |a: &arrow_array::StringArray| {
                use arrow_array::Array;
                (!a.is_null(row)).then(|| a.value(row).to_string())
            };
            out.push((text(&ids), text(&ssns)));
        }
    }
    out
}

fn row(id: &str, ssn: &str) -> (Option<String>, Option<String>) {
    (Some(id.to_string()), Some(ssn.to_string()))
}

fn both() -> Vec<(Option<String>, Option<String>)> {
    vec![row("1", "a"), row("2", "b")]
}

fn nulls() -> Vec<(Option<String>, Option<String>)> {
    vec![(None, None), (None, None)]
}

/// Every `(id, ssn)` row of `ns.t`, read by an unrestricted handler. A fresh
/// handler per read: a handler caches the snapshot it read.
async fn raw_rows(ns: &str, t: &str) -> Vec<(i64, String)> {
    let (session, handler) = crate::common::setup_handler().await;
    let sql = format!("SELECT CAST(id AS BIGINT), ssn FROM {ns}.{t} ORDER BY id");
    let batches = handler
        .execute(&session, &sql, None)
        .await
        .expect("raw read");
    let mut out = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_primitive::<arrow_array::types::Int64Type>();
        let vs = arrow::compute::cast(b.column(1), &arrow_schema::DataType::Utf8).unwrap();
        let vs = vs.as_string::<i32>();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vs.value(i).to_string()));
        }
    }
    out
}

/// With the flag off a schema tag changes nothing: the namespace is not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn schema_tag_inheritance_off_leaves_the_values_readable() {
    let (session, handler) = setup(false).await;
    let (ns, t) = (unique("schematag924_off"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    exec(
        &handler,
        &session,
        &format!("ALTER SCHEMA {ns} SET TAGS ('pii')"),
    )
    .await;

    assert_eq!(rows(&handler, &session, &ns, t).await, both());

    drop_schema(&handler, &session, &ns).await;
}

/// A schema tag with a nullifying policy blanks every column, also of a
/// table created after the tag and of a session that read the table before;
/// UNSET gives the values back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn schema_tag_inheritance_on_masks_every_column_of_the_schema() {
    let (session, handler) = setup(true).await;
    let (ns, t) = (unique("schematag924_on"), "t");
    schema_with_table(&handler, &session, &ns, t).await;

    // A second session reads the table before the schema is tagged.
    let (other_session, other) = setup(true).await;
    assert_eq!(rows(&other, &other_session, &ns, t).await, both());

    exec(
        &handler,
        &session,
        &format!("ALTER SCHEMA {ns} SET TAGS ('pii')"),
    )
    .await;
    assert_eq!(rows(&handler, &session, &ns, t).await, nulls());
    assert_eq!(
        rows(&other, &other_session, &ns, t).await,
        nulls(),
        "a session that read the table earlier sees the tag"
    );

    // A table created after the tag carries it too.
    table(&handler, &session, &ns, "later").await;
    assert_eq!(rows(&handler, &session, &ns, "later").await, nulls());

    exec(
        &handler,
        &session,
        &format!("ALTER SCHEMA {ns} UNSET TAGS ('pii')"),
    )
    .await;
    assert_eq!(rows(&handler, &session, &ns, t).await, both());
    assert_eq!(rows(&other, &other_session, &ns, t).await, both());

    drop_schema(&handler, &session, &ns).await;
}

/// Schema tags that cannot be read leave the tag state unknown: the read is
/// refused, never run without the inherited policies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn schema_tag_inheritance_unreadable_schema_tags_fail_closed() {
    let (session, handler) = setup(true).await;
    let (ns, t) = (unique("schematag924_bad"), "t");
    schema_with_table(&handler, &session, &ns, t).await;

    let catalog = handler
        .session_catalog(&session)
        .await
        .expect("session catalog");
    catalog
        .update_namespace(
            &iceberg::NamespaceIdent::new(ns.clone()),
            HashMap::from([(
                sqe_core::object_tags::SCHEMA_PROP_KEY.to_string(),
                r#"{"not":"a list"}"#.to_string(),
            )]),
        )
        .await
        .expect("write a malformed sqe.schema-tags");

    let sql = format!("SELECT ssn FROM {ns}.{t}");
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("unreadable schema tags");
    assert!(err.to_string().contains("tag state unknown"), "{err}");

    drop_schema(&handler, &session, &ns).await;
}

/// A row filter on a schema tag keeps UPDATE and DELETE off the hidden row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn schema_tag_inheritance_row_filter_guards_update_and_delete() {
    let (session, handler) = setup(true).await;
    let (ns, t) = (unique("schematag924_rf"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    exec(
        &handler,
        &session,
        &format!("ALTER SCHEMA {ns} SET TAGS ('eu')"),
    )
    .await;

    assert_eq!(rows(&handler, &session, &ns, t).await, vec![row("2", "b")]);

    exec(
        &handler,
        &session,
        &format!("UPDATE {ns}.{t} SET ssn = 'z'"),
    )
    .await;
    assert_eq!(
        raw_rows(&ns, t).await,
        vec![(1, "a".to_string()), (2, "z".to_string())],
        "the hidden row is not updated"
    );

    exec(&handler, &session, &format!("DELETE FROM {ns}.{t}")).await;
    assert_eq!(
        raw_rows(&ns, t).await,
        vec![(1, "a".to_string())],
        "the hidden row is not deleted"
    );

    drop_schema(&handler, &session, &ns).await;
}

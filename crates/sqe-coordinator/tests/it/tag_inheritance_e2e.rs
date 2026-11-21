//! A table's tags are inherited by its columns (#924): a tag policy on a key
//! the table carries masks or filters as if every column carried it.
//!
//! The policies come from an in-process store; the Ranger run of the same
//! rules belongs to the access-control suite.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::{col, lit, Expr};
use sqe_core::config::MaskPrecedence;
use sqe_core::SessionUser;
use sqe_policy::{MaskType, PolicyStore, ResolvedPolicy, TagMaskSpec};

/// Tag masks and tag row filters for everyone; no resource policies.
#[derive(Default)]
struct TagPolicies {
    masks: HashMap<String, MaskType>,
    filters: HashMap<String, Expr>,
}

#[async_trait::async_trait]
impl PolicyStore for TagPolicies {
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
        let masks = (self.masks.iter())
            .filter(|(tag, _)| tags.contains(*tag))
            .map(|(tag, mask)| (tag.clone(), TagMaskSpec::Ready(mask.clone())))
            .collect();
        let filters = (self.filters.iter())
            .filter(|(tag, _)| tags.contains(*tag))
            .map(|(_, filter)| filter.clone())
            .collect();
        (masks, filters, HashSet::new())
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

/// A handler enforcing `policies`, with an admin session (tag writes and
/// raw tag properties are admin-only in the test config).
async fn setup(
    policies: TagPolicies,
    precedence: MaskPrecedence,
) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let store: Arc<dyn PolicyStore> = Arc::new(policies);
    let (mut session, handler) =
        crate::common::setup_handler_with_tag_store(store, precedence).await;
    session.user.roles = vec!["service_admin".to_string()];
    (session, handler)
}

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `test_ns.<table>` holding `(1, 'a')` and `(2, 'b')` as `(id, ssn)`.
async fn table(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, t: &str) {
    let create = format!(
        "CREATE TABLE test_ns.{t} AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, ssn)"
    );
    exec(handler, session, &create).await;
}

/// `(id, ssn)` rows as text, NULL as `None`, ordered by the raw id.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
) -> Vec<(Option<String>, Option<String>)> {
    let sql = format!("SELECT CAST(id AS VARCHAR), ssn FROM test_ns.{t} ORDER BY ssn");
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

/// The columns `SHOW EFFECTIVE POLICY ON test_ns.<t>` lists as masked.
async fn masked_columns(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
) -> Vec<String> {
    let sql = format!("SHOW EFFECTIVE POLICY ON test_ns.{t}");
    let batches = handler
        .execute(session, &sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for batch in &batches {
        let kinds = crate::common::utf8(batch.column(0).as_ref());
        let columns = crate::common::utf8(batch.column(1).as_ref());
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "column_mask" {
                out.push(columns.value(row).to_string());
            }
        }
    }
    out
}

/// A mask on a table tag nullifies every column, and SHOW EFFECTIVE POLICY
/// says so; UNSET of the tag gives the columns back.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_inheritance_924_a_table_tag_masks_every_column() {
    let policies = TagPolicies {
        masks: HashMap::from([("pii".to_string(), MaskType::Nullify)]),
        ..Default::default()
    };
    let (session, handler) = setup(policies, MaskPrecedence::Tag).await;
    let t = unique("taginherit924_mask");
    table(&handler, &session, &t).await;
    assert_eq!(
        rows(&handler, &session, &t).await,
        vec![row("1", "a"), row("2", "b")]
    );

    exec(
        &handler,
        &session,
        &format!("ALTER TABLE test_ns.{t} SET TAGS ('pii' = 'all')"),
    )
    .await;
    assert_eq!(
        rows(&handler, &session, &t).await,
        vec![(None, None), (None, None)]
    );
    assert_eq!(
        masked_columns(&handler, &session, &t).await,
        vec!["id", "ssn"],
        "SHOW EFFECTIVE POLICY reports the inherited masks"
    );

    exec(
        &handler,
        &session,
        &format!("ALTER TABLE test_ns.{t} UNSET TAGS ('pii')"),
    )
    .await;
    assert_eq!(
        rows(&handler, &session, &t).await,
        vec![row("1", "a"), row("2", "b")]
    );

    exec(&handler, &session, &format!("DROP TABLE test_ns.{t}")).await;
}

/// A row filter on a table tag applies to reads, to UPDATE and DELETE
/// (a hidden row is not written) and to the row count SHOW STATS reports.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_inheritance_924_a_table_tag_row_filter_applies() {
    let policies = TagPolicies {
        filters: HashMap::from([("eu".to_string(), col("id").gt(lit(1)))]),
        ..Default::default()
    };
    let (session, handler) = setup(policies, MaskPrecedence::Tag).await;
    let t = unique("taginherit924_filter");
    table(&handler, &session, &t).await;
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE test_ns.{t} SET TAGS ('eu')"),
    )
    .await;

    assert_eq!(rows(&handler, &session, &t).await, vec![row("2", "b")]);

    exec(
        &handler,
        &session,
        &format!("UPDATE test_ns.{t} SET ssn = 'z'"),
    )
    .await;
    let (admin, plain) = crate::common::setup_handler().await;
    assert_eq!(
        rows(&plain, &admin, &t).await,
        vec![row("1", "a"), row("2", "z")],
        "the hidden row is not updated"
    );

    exec(&handler, &session, &format!("DELETE FROM test_ns.{t}")).await;
    // A fresh handler: `plain` caches the snapshot it read.
    let (admin, plain) = crate::common::setup_handler().await;
    assert_eq!(
        rows(&plain, &admin, &t).await,
        vec![row("1", "a")],
        "the hidden row is not deleted"
    );

    let stats = handler
        .execute(&session, &format!("SHOW STATS FOR test_ns.{t}"), None)
        .await
        .expect("SHOW STATS");
    let last = stats.last().expect("a stats batch");
    let count = last.column(4);
    assert!(
        count.is_null(last.num_rows() - 1),
        "a row-filtered caller gets no total row count"
    );

    exec(&handler, &session, &format!("DROP TABLE test_ns.{t}")).await;
}

/// Under `refuse`, a mask inherited from the table and a different mask on
/// the column's own tag conflict, and the read is refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_inheritance_924_an_inherited_mask_conflict_is_refused() {
    let policies = TagPolicies {
        masks: HashMap::from([
            ("pii".to_string(), MaskType::Nullify),
            ("fin".to_string(), MaskType::Redact("***".to_string())),
        ]),
        ..Default::default()
    };
    let (session, handler) = setup(policies, MaskPrecedence::Refuse).await;
    let t = unique("taginherit924_conflict");
    table(&handler, &session, &t).await;
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE test_ns.{t} SET TAGS ('pii')"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE test_ns.{t} SET TAGS (ssn = ('fin'))"),
    )
    .await;

    let sql = format!("SELECT ssn FROM test_ns.{t}");
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("two masks on ssn");
    assert!(err.to_string().contains("conflict"), "{err}");

    exec(&handler, &session, &format!("DROP TABLE test_ns.{t}")).await;
}

/// Table tags that cannot be read leave the tag state unknown: the read is
/// refused, never run without the inherited policies.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn tag_inheritance_924_unreadable_table_tags_fail_closed() {
    let (session, handler) = setup(TagPolicies::default(), MaskPrecedence::Tag).await;
    let t = unique("taginherit924_malformed");
    table(&handler, &session, &t).await;
    let raw = format!(
        "ALTER TABLE test_ns.{t} SET TBLPROPERTIES ('sqe.table-tags' = '{{\"not\":\"a list\"}}')"
    );
    exec(&handler, &session, &raw).await;

    let sql = format!("SELECT ssn FROM test_ns.{t}");
    let err = handler
        .execute(&session, &sql, None)
        .await
        .expect_err("unreadable table tags");
    assert!(err.to_string().contains("tag state unknown"), "{err}");

    exec(&handler, &session, &format!("DROP TABLE test_ns.{t}")).await;
}

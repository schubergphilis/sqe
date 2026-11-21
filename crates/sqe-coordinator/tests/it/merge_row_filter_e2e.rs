//! MERGE into a row-filtered table.
//!
//! The write path renders the caller's row filter to SQL and evaluates it
//! over the MERGE target's scratch relation (`__sqe_merge_target_*`). The
//! filter used to keep the probe table's qualifier (`<table>.id > 1`), which
//! does not resolve there, so every MERGE into a row-filtered table failed
//! to plan its cardinality check. DELETE and UPDATE alias their scratch to
//! the table name and never saw it. Each case runs with a resource row
//! filter and with a row filter matched by a table tag.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use datafusion::logical_expr::{col, lit, Expr};
use sqe_core::SessionUser;
use sqe_policy::{PolicyStore, ResolvedPolicy, TagMaskSpec};

/// Where the row filter `id > 1` comes from.
#[derive(Clone, Copy, Debug)]
enum Filter {
    /// A row filter on the table itself.
    Resource,
    /// A row filter on the tag `eu`, which the table carries.
    Tag,
}

/// The row filter `id > 1` on the tag `eu`; no resource policies.
struct EuTagFilter;

#[async_trait::async_trait]
impl PolicyStore for EuTagFilter {
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
        let filters = if tags.contains("eu") {
            vec![col("id").gt(lit(1))]
        } else {
            vec![]
        };
        (HashMap::new(), filters, HashSet::new())
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

/// Every `(id, v)` row of `test_ns.<t>`, read by an unrestricted handler.
/// A fresh handler per read: a handler caches the snapshot it read.
async fn raw_rows(t: &str) -> Vec<(i64, String)> {
    let (session, handler) = crate::common::setup_handler().await;
    let sql = format!("SELECT CAST(id AS BIGINT), v FROM test_ns.{t} ORDER BY id");
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

fn rows(expected: &[(i64, &str)]) -> Vec<(i64, String)> {
    expected
        .iter()
        .map(|(id, v)| (*id, v.to_string()))
        .collect()
}

async fn merge_under_a_row_filter(mode: &str, filter: Filter) {
    let t = unique("merge_rf");
    let (admin, plain) = crate::common::setup_handler().await;
    let create = format!(
        "CREATE TABLE test_ns.{t} TBLPROPERTIES ( \
             'write.delete.mode' = '{mode}', \
             'write.update.mode' = '{mode}', \
             'write.merge.mode' = '{mode}' \
         ) AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS s(id, v)"
    );
    plain.execute(&admin, &create, None).await.expect("CTAS");

    let (session, handler) = match filter {
        Filter::Resource => {
            let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
            let policy = ResolvedPolicy {
                row_filters: vec![col("id").gt(lit(1))],
                ..Default::default()
            };
            store.add_table_policy("test_ns", &t, policy).await;
            crate::common::setup_handler_with_policy_store(store).await
        }
        Filter::Tag => {
            let store: Arc<dyn PolicyStore> = Arc::new(EuTagFilter);
            let precedence = sqe_core::config::MaskPrecedence::Tag;
            let (mut session, handler) =
                crate::common::setup_handler_with_tag_store(store, precedence).await;
            // Tag writes are admin-only in the test config.
            session.user.roles = vec!["service_admin".to_string()];
            let tag = format!("ALTER TABLE test_ns.{t} SET TAGS ('eu')");
            handler
                .execute(&session, &tag, None)
                .await
                .expect("SET TAGS");
            (session, handler)
        }
    };
    let mode = format!("{mode}, {filter:?} filter");
    let run = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move { handler.execute(session, &sql, None).await.map(|_| ()) }
    };

    let update = format!(
        "MERGE INTO test_ns.{t} AS t \
         USING (SELECT * FROM (VALUES (1, 'm'), (2, 'm')) AS s(id, v)) AS s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
    );
    run(update).await.expect("MERGE UPDATE");
    assert_eq!(
        raw_rows(&t).await,
        rows(&[(1, "a"), (2, "m"), (3, "c")]),
        "{mode}: the hidden row is not matched"
    );

    let unaliased = format!(
        "MERGE INTO test_ns.{t} \
         USING (SELECT * FROM (VALUES (3, 'u')) AS s(id, v)) AS s \
         ON {t}.id = s.id WHEN MATCHED AND test_ns.{t}.v = 'c' THEN UPDATE SET v = s.v"
    );
    run(unaliased).await.expect("MERGE without a target alias");
    assert_eq!(
        raw_rows(&t).await,
        rows(&[(1, "a"), (2, "m"), (3, "u")]),
        "{mode}: unaliased target"
    );

    let delete = format!(
        "MERGE INTO test_ns.{t} AS t \
         USING (SELECT * FROM (VALUES (1), (3)) AS s(id)) AS s \
         ON t.id = s.id WHEN MATCHED THEN DELETE"
    );
    run(delete).await.expect("MERGE DELETE");
    assert_eq!(
        raw_rows(&t).await,
        rows(&[(1, "a"), (2, "m")]),
        "{mode}: the hidden row is not deleted"
    );

    let insert = |id: i64| {
        format!(
            "MERGE INTO test_ns.{t} AS t \
             USING (SELECT * FROM (VALUES ({id}, 'i')) AS s(id, v)) AS s \
             ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
    };
    run(insert(5))
        .await
        .expect("MERGE INSERT inside the filter");
    let err = run(insert(0))
        .await
        .expect_err("MERGE INSERT outside the filter");
    assert!(
        err.to_string().contains("row-level security"),
        "{mode}: {err}"
    );
    assert_eq!(
        raw_rows(&t).await,
        rows(&[(1, "a"), (2, "m"), (5, "i")]),
        "{mode}: only the admitted insert lands"
    );

    plain
        .execute(&admin, &format!("DROP TABLE test_ns.{t}"), None)
        .await
        .expect("drop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn merge_row_filter_copy_on_write() {
    merge_under_a_row_filter("copy-on-write", Filter::Resource).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn merge_row_filter_merge_on_read() {
    merge_under_a_row_filter("merge-on-read", Filter::Resource).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn merge_row_filter_tag_copy_on_write() {
    merge_under_a_row_filter("copy-on-write", Filter::Tag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn merge_row_filter_tag_merge_on_read() {
    merge_under_a_row_filter("merge-on-read", Filter::Tag).await;
}

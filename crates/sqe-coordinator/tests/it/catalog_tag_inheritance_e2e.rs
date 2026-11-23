//! A catalog's tags are inherited by the columns of every table in it
//! (#924): a tag policy on a key the catalog carries masks or filters as if
//! every column carried it, once `policy.inherit_catalog_tags` is on. The
//! chain is column, table, schema, catalog; the nearest key wins. Catalog
//! tags are enforced by SQE only (they are not projected to Ranger).
//!
//! The policies come from an in-process store.
//!
//! The catalog entity is shared by every test here, so they run one at a
//! time (`CATALOG_LOCK`) and clear the catalog's tags before and after.
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

/// Serialises the tests: they all write the one catalog's tags.
static CATALOG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `pii` nullifies, `eu` keeps the rows with `id > 1`; no resource policies.
struct CatalogTagPolicies;

#[async_trait::async_trait]
impl PolicyStore for CatalogTagPolicies {
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

/// A tag-enforcing handler with catalog tag inheritance `inherit`, and an
/// admin session (tag writes are admin-only in the test config).
async fn setup(inherit: bool) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let store: Arc<dyn PolicyStore> = Arc::new(CatalogTagPolicies);
    let (mut session, handler) = crate::common::setup_handler_with_catalog_tag_inheritance(
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

/// The default catalog's name, as `ALTER CATALOG` must spell it.
fn catalog_name() -> String {
    sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("load test config")
        .resolve_default_catalog()
}

/// Remove `sqe.catalog-tags` from the catalog, whatever it holds.
async fn clear_catalog_tags(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session) {
    let catalog = handler
        .session_catalog(session)
        .await
        .expect("session catalog");
    catalog
        .update_catalog_property(sqe_core::object_tags::CATALOG_PROP_KEY, |_| Ok(None))
        .await
        .expect("clear sqe.catalog-tags");
}

/// With the flag off a catalog tag changes nothing: the catalog is not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_off_leaves_the_values_readable() {
    let _guard = CATALOG_LOCK.lock().await;
    let (session, handler) = setup(false).await;
    clear_catalog_tags(&handler, &session).await;
    let (ns, t) = (unique("cattag924_off"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    let cat = catalog_name();
    exec(
        &handler,
        &session,
        &format!("ALTER CATALOG {cat} SET TAGS ('pii')"),
    )
    .await;

    let seen = rows(&handler, &session, &ns, t).await;

    clear_catalog_tags(&handler, &session).await;
    drop_schema(&handler, &session, &ns).await;
    assert_eq!(seen, both());
}

/// A catalog tag with a nullifying policy blanks every column of every
/// table, also of a table created after the tag and of a session that read
/// the table before; UNSET gives the values back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_on_masks_every_column_of_the_catalog() {
    let _guard = CATALOG_LOCK.lock().await;
    let (session, handler) = setup(true).await;
    clear_catalog_tags(&handler, &session).await;
    let (ns, t) = (unique("cattag924_on"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    let cat = catalog_name();

    // A second session reads the table before the catalog is tagged.
    let (other_session, other) = setup(true).await;
    let before = rows(&other, &other_session, &ns, t).await;

    exec(
        &handler,
        &session,
        &format!("ALTER CATALOG {cat} SET TAGS ('pii')"),
    )
    .await;
    let masked = rows(&handler, &session, &ns, t).await;
    let masked_other = rows(&other, &other_session, &ns, t).await;
    table(&handler, &session, &ns, "later").await;
    let masked_later = rows(&handler, &session, &ns, "later").await;

    exec(
        &handler,
        &session,
        &format!("ALTER CATALOG {cat} UNSET TAGS ('pii')"),
    )
    .await;
    let restored = rows(&handler, &session, &ns, t).await;
    let restored_other = rows(&other, &other_session, &ns, t).await;

    clear_catalog_tags(&handler, &session).await;
    drop_schema(&handler, &session, &ns).await;

    assert_eq!(before, both());
    assert_eq!(masked, nulls());
    assert_eq!(
        masked_other,
        nulls(),
        "a session that read the table earlier sees the tag"
    );
    assert_eq!(masked_later, nulls(), "a table created after the tag");
    assert_eq!(restored, both());
    assert_eq!(restored_other, both());
}

/// The catalog flag is independent of the schema flag: with only the
/// schema flag on, a catalog tag does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_is_independent_of_the_schema_flag() {
    let _guard = CATALOG_LOCK.lock().await;
    let store: Arc<dyn PolicyStore> = Arc::new(CatalogTagPolicies);
    let (mut session, handler) =
        crate::common::setup_handler_with_schema_tag_inheritance(store, MaskPrecedence::Tag, true)
            .await;
    session.user.roles = vec!["service_admin".to_string()];
    clear_catalog_tags(&handler, &session).await;
    let (ns, t) = (unique("cattag924_ind"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    let cat = catalog_name();
    exec(
        &handler,
        &session,
        &format!("ALTER CATALOG {cat} SET TAGS ('pii')"),
    )
    .await;

    let seen = rows(&handler, &session, &ns, t).await;

    clear_catalog_tags(&handler, &session).await;
    drop_schema(&handler, &session, &ns).await;
    assert_eq!(seen, both());
}

/// Catalog tags that cannot be read leave the tag state unknown: the read
/// is refused, never run without the inherited policies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_unreadable_catalog_tags_fail_closed() {
    let _guard = CATALOG_LOCK.lock().await;
    let (session, handler) = setup(true).await;
    clear_catalog_tags(&handler, &session).await;
    let (ns, t) = (unique("cattag924_bad"), "t");
    schema_with_table(&handler, &session, &ns, t).await;

    let catalog = handler
        .session_catalog(&session)
        .await
        .expect("session catalog");
    catalog
        .update_catalog_property(sqe_core::object_tags::CATALOG_PROP_KEY, |_| {
            Ok(Some(r#"{"not":"a list"}"#.to_string()))
        })
        .await
        .expect("write a malformed sqe.catalog-tags");

    let sql = format!("SELECT ssn FROM {ns}.{t}");
    let result = handler.execute(&session, &sql, None).await;

    clear_catalog_tags(&handler, &session).await;
    drop_schema(&handler, &session, &ns).await;
    let err = result.expect_err("unreadable catalog tags");
    assert!(err.to_string().contains("tag state unknown"), "{err}");
}

/// A row filter on a catalog tag keeps UPDATE and DELETE off the hidden row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_row_filter_guards_update_and_delete() {
    let _guard = CATALOG_LOCK.lock().await;
    let (session, handler) = setup(true).await;
    clear_catalog_tags(&handler, &session).await;
    let (ns, t) = (unique("cattag924_rf"), "t");
    schema_with_table(&handler, &session, &ns, t).await;
    let cat = catalog_name();
    exec(
        &handler,
        &session,
        &format!("ALTER CATALOG {cat} SET TAGS ('eu')"),
    )
    .await;

    let visible = rows(&handler, &session, &ns, t).await;
    exec(
        &handler,
        &session,
        &format!("UPDATE {ns}.{t} SET ssn = 'z'"),
    )
    .await;
    let after_update = raw_rows(&ns, t).await;
    exec(&handler, &session, &format!("DELETE FROM {ns}.{t}")).await;
    let after_delete = raw_rows(&ns, t).await;

    clear_catalog_tags(&handler, &session).await;
    drop_schema(&handler, &session, &ns).await;

    assert_eq!(visible, vec![row("2", "b")]);
    assert_eq!(
        after_update,
        vec![(1, "a".to_string()), (2, "z".to_string())],
        "the hidden row is not updated"
    );
    assert_eq!(
        after_delete,
        vec![(1, "a".to_string())],
        "the hidden row is not deleted"
    );
}

/// With the flag on, every principal that reads tables in the catalog needs
/// `CATALOG_READ_PROPERTIES` on it: Polaris answers the catalog entity only
/// to a holder, so without it the tag state is unknown and the read is
/// refused, never run unmasked. Granting it opens the read, masked when the
/// catalog carries the masking tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn catalog_tag_inheritance_reader_needs_catalog_read_properties() {
    use crate::dml_write_precheck_e2e::{principal_token_with, session_for, CATALOG, POLARIS};

    let _guard = CATALOG_LOCK.lock().await;
    let (admin, handler) = setup(true).await;
    clear_catalog_tags(&handler, &admin).await;
    let (ns, t) = (unique("cattag924_grant"), "t");
    schema_with_table(&handler, &admin, &ns, t).await;
    let cat = catalog_name();
    exec(
        &handler,
        &admin,
        &format!("ALTER CATALOG {cat} SET TAGS ('pii')"),
    )
    .await;

    // Table read rights, but no CATALOG_READ_PROPERTIES.
    let root = admin.access_token().expose().to_string();
    let name = "cattag924_reader";
    let token = principal_token_with(
        &root,
        name,
        &[
            "NAMESPACE_LIST",
            "NAMESPACE_READ_PROPERTIES",
            "TABLE_LIST",
            "TABLE_READ_PROPERTIES",
            "TABLE_READ_DATA",
        ],
    )
    .await;
    let reader = session_for(name, token);
    let sql = format!("SELECT CAST(id AS VARCHAR), ssn FROM {ns}.{t} ORDER BY ssn");

    let refused = handler.execute(&reader, &sql, None).await;

    let http = reqwest::Client::new();
    let grant = http
        .put(format!(
            "{POLARIS}/management/v1/catalogs/{CATALOG}/catalog-roles/{name}_cr/grants"
        ))
        .bearer_auth(&root)
        .json(&serde_json::json!({
            "grant": {"type": "catalog", "privilege": "CATALOG_READ_PROPERTIES"}
        }))
        .send()
        .await
        .and_then(|r| r.error_for_status());
    let granted = if grant.is_ok() {
        Some(handler.execute(&reader, &sql, None).await)
    } else {
        None
    };

    // Clean up before asserting: tags, then the principal and its roles.
    clear_catalog_tags(&handler, &admin).await;
    drop_schema(&handler, &admin, &ns).await;
    let mgmt = format!("{POLARIS}/management/v1");
    for url in [
        format!("{mgmt}/principals/{name}"),
        format!("{mgmt}/principal-roles/{name}_role"),
        format!("{mgmt}/catalogs/{CATALOG}/catalog-roles/{name}_cr"),
    ] {
        let _ = http.delete(url).bearer_auth(&root).send().await;
    }

    let err = refused.expect_err("no CATALOG_READ_PROPERTIES: the read is refused");
    assert!(
        err.to_string().contains("tag state unknown")
            || err.to_string().contains("could not be read"),
        "{err}"
    );
    grant.expect("grant CATALOG_READ_PROPERTIES");
    let batches = granted
        .expect("granted")
        .expect("the read succeeds once CATALOG_READ_PROPERTIES is granted");
    let mut seen = Vec::new();
    for batch in &batches {
        let ids = crate::common::utf8(batch.column(0).as_ref());
        let ssns = crate::common::utf8(batch.column(1).as_ref());
        for row in 0..batch.num_rows() {
            use arrow_array::Array;
            seen.push((
                (!ids.is_null(row)).then(|| ids.value(row).to_string()),
                (!ssns.is_null(row)).then(|| ssns.value(row).to_string()),
            ));
        }
    }
    assert_eq!(seen, nulls(), "granted: rows come back masked, not empty");
}

//! Column tags follow the field id, not the name (#920). Another engine's
//! rename or drop + re-add is committed through the catalog directly, never
//! through SQE's ALTER, and SHOW TAGS shows where the tags apply.
//!
//! Needs the local test stack:
//! `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.

use arrow_array::{Array, BooleanArray};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{TableIdent, TableRequirement, TableUpdate};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

/// An admin session (raw tag properties are admin-only) and its handler.
async fn setup() -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    let (mut session, handler) = crate::common::setup_handler().await;
    session.user.roles = vec!["service_admin".to_string()];
    (session, handler)
}

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `default.<table>` with `id` and a `ssn` tagged PII through SET TAGS.
async fn tagged_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) {
    let create = format!("CREATE TABLE default.{table} AS SELECT 1 AS id, 'x' AS ssn");
    exec(handler, session, &create).await;
    let tag = format!("ALTER TABLE default.{table} SET TAGS (ssn = ('PII'))");
    exec(handler, session, &tag).await;
}

/// SHOW TAGS as `(column, tag, applied)` rows.
async fn show_tags(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<(String, String, bool)> {
    let sql = format!("SHOW TAGS ON default.{table}");
    let batches = handler
        .execute(session, &sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    for batch in &batches {
        let columns = crate::common::utf8(batch.column(0).as_ref());
        let tags = crate::common::utf8(batch.column(1).as_ref());
        let applied = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("applied is boolean");
        for row in 0..batch.num_rows() {
            let (column, tag) = (columns.value(row), tags.value(row));
            rows.push((column.to_string(), tag.to_string(), applied.value(row)));
        }
    }
    rows
}

/// Commit the schema `edit` makes of the current one, as another engine
/// would: straight to the catalog, pinned to the schema it read.
async fn foreign_schema_change(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    edit: impl FnOnce(&Schema, i32) -> Vec<NestedField>,
) {
    let catalog = handler.session_catalog(session).await.expect("catalog");
    let ident = TableIdent::from_strs(["default", table]).expect("ident");
    let loaded = catalog.load_table(&ident).await.expect("load table");
    let metadata = loaded.metadata();
    let current = metadata.current_schema();
    let fields = edit(current, metadata.last_column_id() + 1);
    let schema = Schema::builder()
        .with_schema_id(current.schema_id() + 1)
        .with_fields(fields.into_iter().map(Into::into).collect::<Vec<_>>())
        .build()
        .expect("schema");
    let updates = vec![
        TableUpdate::AddSchema { schema },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
    ];
    let pin = TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: current.schema_id(),
    };
    catalog
        .commit_schema_update(&ident, updates, vec![pin])
        .await
        .expect("foreign schema commit");
}

fn fields(schema: &Schema) -> Vec<NestedField> {
    let fields = schema.as_struct().fields().iter();
    fields.map(|f| f.as_ref().clone()).collect()
}

/// Another engine renames `ssn` to `tax_id`: the tag goes with the column.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn column_tag_ids_920_foreign_rename_keeps_the_tag() {
    let (session, handler) = setup().await;
    let t = unique("tagids920_rename");
    tagged_table(&handler, &session, &t).await;

    foreign_schema_change(&handler, &session, &t, |schema, _| {
        let rename = |f: NestedField| match f.name.as_str() {
            "ssn" => NestedField {
                name: "tax_id".to_string(),
                ..f
            },
            _ => f,
        };
        fields(schema).into_iter().map(rename).collect()
    })
    .await;

    let want = vec![("tax_id".to_string(), "PII".to_string(), true)];
    assert_eq!(show_tags(&handler, &session, &t).await, want);

    // The next SQE tag write keeps it on the renamed column.
    let more = format!("ALTER TABLE default.{t} SET TAGS (id = ('KEY'))");
    exec(&handler, &session, &more).await;
    let rows = show_tags(&handler, &session, &t).await;
    let (pii, key) = (
        ("tax_id".into(), "PII".into(), true),
        ("id".into(), "KEY".into(), true),
    );
    assert!(rows.contains(&pii) && rows.contains(&key), "{rows:?}");

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

/// Another engine drops `ssn` and adds a new `ssn`: the old tag does not
/// apply to the new column, and SHOW TAGS lists it as not applied.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn column_tag_ids_920_foreign_drop_and_re_add_orphans_the_tag() {
    let (session, handler) = setup().await;
    let t = unique("tagids920_readd");
    tagged_table(&handler, &session, &t).await;

    foreign_schema_change(&handler, &session, &t, |schema, next_id| {
        let mut kept: Vec<_> = fields(schema)
            .into_iter()
            .filter(|f| f.name != "ssn")
            .collect();
        let ssn = Type::Primitive(PrimitiveType::String);
        kept.push(NestedField::optional(next_id, "ssn", ssn));
        kept
    })
    .await;

    let want = vec![("ssn".to_string(), "PII".to_string(), false)];
    assert_eq!(show_tags(&handler, &session, &t).await, want);

    // Tagging the new column replaces the orphan of the same name.
    let tag = format!("ALTER TABLE default.{t} SET TAGS (ssn = ('LAST4'))");
    exec(&handler, &session, &tag).await;
    let want = vec![("ssn".to_string(), "LAST4".to_string(), true)];
    assert_eq!(show_tags(&handler, &session, &t).await, want);

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

/// A map written without ids (a legacy table, another engine's write)
/// applies by name, and only an admin writes the ids property raw.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn column_tag_ids_920_a_map_without_ids_applies_by_name() {
    let (session, handler) = setup().await;
    let t = unique("tagids920_legacy");
    tagged_table(&handler, &session, &t).await;

    let raw = format!(
        "ALTER TABLE default.{t} SET TBLPROPERTIES ('sqe.column-tags' = '{{\"id\":[\"KEY\"]}}')"
    );
    exec(&handler, &session, &raw).await;
    let want = vec![("id".to_string(), "KEY".to_string(), true)];
    assert_eq!(show_tags(&handler, &session, &t).await, want);

    let mut user = session.clone();
    user.user.roles = Vec::new();
    let forged =
        format!("ALTER TABLE default.{t} SET TBLPROPERTIES ('sqe.column-tag-ids' = '{{\"v\":1}}')");
    let err = handler
        .execute(&user, &forged, None)
        .await
        .expect_err("a non-admin may not write the ids");
    assert!(err.to_string().contains("sqe.column-tag-ids"), "{err}");

    exec(&handler, &session, &format!("DROP TABLE default.{t}")).await;
}

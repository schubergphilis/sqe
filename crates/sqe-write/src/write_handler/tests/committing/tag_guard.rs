//! #917: a `SET TBLPROPERTIES` of `sqe.column-tags` that the coordinator
//! authorised for some columns only. The write re-reads the live map under the
//! per-table tag lock and refuses when it would change any other column.

use super::*;
use crate::catalog_ops::CatalogOps;
use crate::tag_source_impl::{parse_column_tags, PROP_KEY};
use std::collections::{BTreeSet, HashMap};

async fn alter(rig: &SqliteRig, sql: &str, authorised: Option<&[&str]>) -> sqe_core::Result<()> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let stmt = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .expect("parse")
        .remove(0);
    let set: Option<BTreeSet<String>> =
        authorised.map(|cs| cs.iter().map(|c| c.to_string()).collect());
    CatalogOps::new(rig.config.clone())
        .set_table_properties_guarded(&rig.session(), &stmt, set.as_ref(), None)
        .await
}

async fn tags(rig: &SqliteRig) -> HashMap<String, Vec<String>> {
    parse_column_tags(rig.load(&rig.ident("t")).await.metadata().properties()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_guarded_tag_write_changes_only_authorised_columns() {
    let rig = SqliteRig::new().await;
    let h = WriteHandler::new(rig.config.clone());
    run_ctas(&h, &rig, "CREATE TABLE t AS SELECT 1 AS ssn, 2 AS phone").await;
    let set = |json: &str| format!("ALTER TABLE t SET TBLPROPERTIES ('{PROP_KEY}' = '{json}')");

    alter(&rig, &set(r#"{"ssn":["PII"]}"#), None)
        .await
        .expect("unguarded");

    // Authorised for `phone`: adding it and keeping `ssn` as is passes.
    alter(
        &rig,
        &set(r#"{"ssn":["PII"],"phone":["PII"]}"#),
        Some(&["phone"]),
    )
    .await
    .expect("authorised column");
    assert_eq!(tags(&rig).await.len(), 2);

    // The live map moved on (say a concurrent SET TAGS on `ssn`) since a check
    // that authorised `phone` only: the write would now change `ssn` too.
    let err = alter(&rig, &set(r#"{"phone":["PII"]}"#), Some(&["phone"]))
        .await
        .expect_err("ssn not authorised");
    assert!(err.to_string().contains("ssn"), "{err}");
    assert_eq!(tags(&rig).await.len(), 2, "nothing written");

    // Other properties in the statement do not get through either.
    let err = alter(
        &rig,
        &format!("ALTER TABLE t SET TBLPROPERTIES ('a' = 'x', '{PROP_KEY}' = '{{}}')"),
        Some(&["phone"]),
    )
    .await
    .expect_err("clearing touches ssn");
    assert!(err.to_string().contains("403"), "{err}");
    assert!(!rig
        .load(&rig.ident("t"))
        .await
        .metadata()
        .properties()
        .contains_key("a"));

    // A new value that is not a tag map is refused.
    let err = alter(&rig, &set(r#"{"phone":"PII"}"#), Some(&["phone"]))
        .await
        .expect_err("malformed new");
    assert!(err.to_string().contains("403"), "{err}");

    // A live value that is not a tag map is refused to a guarded writer.
    alter(&rig, &set("not-json"), None)
        .await
        .expect("admin overwrite");
    let err = alter(&rig, &set(r#"{"phone":["PII"]}"#), Some(&["phone"]))
        .await
        .expect_err("malformed live");
    assert!(err.to_string().contains("not a tag map"), "{err}");

    // A guard does not touch statements that leave the key alone.
    alter(
        &rig,
        "ALTER TABLE t SET TBLPROPERTIES ('b' = 'y')",
        Some(&[]),
    )
    .await
    .expect("no tag key");
}

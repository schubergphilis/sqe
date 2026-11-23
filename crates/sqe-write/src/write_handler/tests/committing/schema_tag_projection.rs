//! #935: schema tags projected onto the schema-wide Ranger resource, with
//! two coordinators (two `CatalogOps`, each with its own memory) sharing one
//! fake Ranger that has the semantics probed on Ranger 2.8: a non-empty
//! add-or-update replaces a resource's tags, a delete drops it.

use super::*;
use crate::catalog_ops::CatalogOps;
use sqe_policy::tag_projector::{ColumnTags, TagProjector, TagTableKey};
use sqe_sql::object_tags::{ObjectTagsStatement, TagObject};
use sqe_sql::tags::TagAction;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

#[derive(Default)]
struct FakeRanger {
    store: Mutex<HashMap<String, Vec<String>>>,
}

impl FakeRanger {
    fn held(&self, key: &str) -> Vec<String> {
        self.store
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl TagProjector for FakeRanger {
    async fn project(
        &self,
        _table: &TagTableKey,
        _previous: &ColumnTags,
        _tags: &ColumnTags,
    ) -> sqe_core::Result<()> {
        Ok(())
    }

    async fn project_table_level(
        &self,
        table: &TagTableKey,
        previous: &[String],
        tags: &[String],
    ) -> sqe_core::Result<()> {
        let key = format!("{}|{}", table.database, table.table);
        let mut store = self.store.lock().unwrap();
        if !tags.is_empty() {
            store.insert(key, tags.to_vec());
        } else if !previous.is_empty() {
            store.remove(&key);
        }
        Ok(())
    }

    async fn held_table_level(&self, table: &TagTableKey) -> sqe_core::Result<Vec<String>> {
        Ok(self.held(&format!("{}|{}", table.database, table.table)))
    }

    fn enabled(&self) -> bool {
        true
    }
}

fn coordinator(rig: &SqliteRig, ranger: &Arc<FakeRanger>) -> CatalogOps {
    let mut config = rig.config.clone();
    config.policy.inherit_schema_tags = true;
    CatalogOps::new(config).with_tag_projector(ranger.clone())
}

fn schema_tags(schema: &str, action: TagAction, tags: &[&str]) -> ObjectTagsStatement {
    ObjectTagsStatement {
        object: TagObject::Schema(schema.into()),
        action,
        tags: tags.iter().map(|t| t.to_string()).collect(),
        values: BTreeMap::new(),
    }
}

/// Coordinator B remembers the schema-wide resource holding `pii`; A has
/// since removed it. B setting `pii` again must write it, not skip on its
/// memory: a skip leaves the schema unmasked in Spark while SQE masks it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_set_after_another_coordinators_unset_writes_ranger() {
    let rig = SqliteRig::new().await;
    let ranger = Arc::new(FakeRanger::default());
    let (a, b) = (coordinator(&rig, &ranger), coordinator(&rig, &ranger));
    let session = rig.session();
    let set = schema_tags("default", TagAction::Set, &["pii"]);
    let unset = schema_tags("default", TagAction::Unset, &["pii"]);

    b.set_object_tags(&session, &set).await.expect("B sets");
    assert_eq!(ranger.held("default|*"), vec!["pii".to_string()]);
    a.set_object_tags(&session, &unset).await.expect("A unsets");
    assert!(ranger.held("default|*").is_empty());

    b.set_object_tags(&session, &set)
        .await
        .expect("B sets again");
    assert_eq!(
        ranger.held("default|*"),
        vec!["pii".to_string()],
        "Ranger must hold the tag the schema carries"
    );
}

/// `DROP SCHEMA` deletes the schema-wide resource, so a schema created
/// later under the same name does not inherit the old tags in Spark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_clears_the_schema_wide_resource() {
    let rig = SqliteRig::new().await;
    rig.ensure_namespace("gone").await;
    let ranger = Arc::new(FakeRanger::default());
    let ops = coordinator(&rig, &ranger);
    let session = rig.session();
    let set = schema_tags("gone", TagAction::Set, &["pii"]);
    ops.set_object_tags(&session, &set).await.expect("set");
    assert_eq!(ranger.held("gone|*"), vec!["pii".to_string()]);

    let dialect = sqlparser::dialect::GenericDialect {};
    let drop = sqlparser::parser::Parser::parse_sql(&dialect, "DROP SCHEMA gone")
        .expect("parse")
        .remove(0);
    ops.drop_schema(&session, &drop).await.expect("drop");
    assert!(ranger.held("gone|*").is_empty());
}

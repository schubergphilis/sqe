//! Project column-tag associations into Ranger's TAG STORE, so engines other than
//! SQE can enforce tag-based policies.
//!
//! Tag associations are authored by `ALTER TABLE ... SET TAG` and stored in the
//! Iceberg table property `sqe.column-tags`, which is the source of truth and the
//! only thing SQE reads. Spark's Kyuubi plugin reads Ranger's tag store instead and
//! has no reader for Iceberg properties, so WITHOUT this projection a tag-masked
//! column is protected in SQE and returned RAW by Spark.
//!
//! The projection is one `PUT /service/tags/importservicetags` per table. Verified
//! against Ranger 2.8: `op: add_or_update` writes the tag definition, the service
//! resource and the association together and MERGES, so a table's projection does
//! not disturb another table's. `op: delete` removes them.
//!
//! What this deliberately does NOT do is become the source of truth. Ranger's tag
//! store is a projection; the Iceberg property still travels with the table.

use std::collections::BTreeMap;

use async_trait::async_trait;
use sqe_core::config::RangerPolicyConfig;
use tracing::{debug, instrument};

/// `column -> [tag, ...]`, the shape stored in `sqe.column-tags`.
pub type ColumnTags = BTreeMap<String, Vec<String>>;

/// Which table a projection is for, in the resource vocabulary the FRONTEND
/// service uses.
///
/// `database` is the namespace as the frontend service names it, which is what
/// Kyuubi sends: it has no catalog level, so the catalog is deliberately absent
/// here. Two same-named tables in different catalogs therefore share one tag
/// resource, matching how the frontend service already behaves for masks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagTableKey {
    pub database: String,
    pub table: String,
}

impl TagTableKey {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    /// Build the key from a namespace and table name, applying the SAME
    /// convention the mask path uses.
    ///
    /// `plan_rewriter::resolve_policy_key` sends the LAST dotted component of the
    /// schema as the Ranger `database`, so a mask on `sales_wh.sales.orders` is
    /// written against `database=sales`. A projected tag resource has to agree, or
    /// the tag lands on a resource no engine resolves and the projection silently
    /// protects nothing.
    pub fn from_namespace(namespace: &str, table: impl Into<String>) -> Self {
        let database = namespace.rsplit('.').next().unwrap_or(namespace);
        Self::new(database, table)
    }
}

#[async_trait]
pub trait TagProjector: Send + Sync {
    /// Make Ranger's tag store match `tags` for this table.
    ///
    /// `previous` is the map BEFORE this change. It is required, not a convenience:
    /// a column whose last tag was just unset does not appear in `tags` at all, so a
    /// delete built from `tags` alone names nothing and the stale association
    /// survives. Spark then keeps masking a column SQE no longer tags.
    async fn project(
        &self,
        table: &TagTableKey,
        previous: &ColumnTags,
        tags: &ColumnTags,
    ) -> sqe_core::Result<()>;

    /// False when projection is off, which lets callers skip the work and, more
    /// importantly, skip the ROLLBACK path that only exists to keep the two stores
    /// consistent.
    fn enabled(&self) -> bool {
        false
    }
}

/// The default. Tags stay SQE-only.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTagProjector;

#[async_trait]
impl TagProjector for NoopTagProjector {
    async fn project(
        &self,
        _table: &TagTableKey,
        _previous: &ColumnTags,
        _tags: &ColumnTags,
    ) -> sqe_core::Result<()> {
        Ok(())
    }
}

/// Builds the `ServiceTags` document Ranger's bulk import accepts.
///
/// Pure, and unit-tested against the payload shape the live probes accepted, so the
/// document can be checked without a Ranger.
///
/// Ids are local to the document: Ranger resolves tag definitions by NAME and
/// service resources by their resource signature, so a per-call 0..n numbering is
/// stable enough and avoids having to read the store first.
pub fn build_import_document(
    service: &str,
    table: &TagTableKey,
    tags: &ColumnTags,
    op: &str,
) -> serde_json::Value {
    let mut tag_definitions = serde_json::Map::new();
    let mut tag_instances = serde_json::Map::new();
    let mut service_resources = Vec::new();
    let mut resource_to_tag_ids = serde_json::Map::new();

    // One id space for tag names, one for resources.
    let mut tag_id_of: BTreeMap<&str, usize> = BTreeMap::new();
    for (column, column_tags) in tags {
        let mut ids = Vec::new();
        for tag in column_tags {
            let next = tag_id_of.len();
            let id = *tag_id_of.entry(tag.as_str()).or_insert(next);
            tag_definitions.insert(
                id.to_string(),
                serde_json::json!({"id": id, "name": tag, "source": "sqe"}),
            );
            tag_instances.insert(
                id.to_string(),
                serde_json::json!({"id": id, "type": tag}),
            );
            ids.push(id);
        }
        if ids.is_empty() {
            continue;
        }
        let resource_id = service_resources.len();
        service_resources.push(serde_json::json!({
            "id": resource_id,
            "serviceName": service,
            "resourceElements": {
                "database": {"values": [table.database]},
                "table":    {"values": [table.table]},
                "column":   {"values": [column]},
            }
        }));
        resource_to_tag_ids.insert(resource_id.to_string(), serde_json::json!(ids));
    }

    serde_json::json!({
        "op": op,
        "serviceName": service,
        "tagVersion": 1,
        "tagDefinitions": tag_definitions,
        "tags": tag_instances,
        "serviceResources": service_resources,
        "resourceToTagIds": resource_to_tag_ids,
    })
}

/// Projects into Ranger's tag store over the Ranger Admin REST API.
pub struct RangerTagProjector {
    client: reqwest::Client,
    base_url: String,
    service_name: String,
    admin_user: String,
    admin_password: sqe_core::SecretString,
}

impl RangerTagProjector {
    pub fn new(cfg: &RangerPolicyConfig) -> sqe_core::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_secs.max(5)))
            .danger_accept_invalid_certs(cfg.accept_invalid_certs)
            .build()
            .map_err(|e| {
                sqe_core::SqeError::Config(format!(
                    "failed to build the Ranger tag-projector HTTP client: {e}"
                ))
            })?;
        Ok(Self {
            client,
            base_url: cfg.url.trim_end_matches('/').to_string(),
            service_name: cfg.service_name.clone(),
            admin_user: cfg.admin_user.clone(),
            admin_password: cfg.admin_password.clone(),
        })
    }

    async fn import(&self, body: serde_json::Value) -> sqe_core::Result<()> {
        let url = format!("{}/service/tags/importservicetags", self.base_url);
        let resp = self
            .client
            .put(&url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .header("X-XSRF-HEADER", "x")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Ranger tag projection request to {url} failed: {e}"
                ))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger tag projection returned HTTP {status}: {text}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl TagProjector for RangerTagProjector {
    #[instrument(skip(self, previous, tags), fields(service = %self.service_name))]
    async fn project(
        &self,
        table: &TagTableKey,
        previous: &ColumnTags,
        tags: &ColumnTags,
    ) -> sqe_core::Result<()> {
        // Delete what WAS there, then add what should be. The delete has to be built
        // from `previous`: an unset column is absent from `tags`, so a delete built
        // from `tags` names nothing, the stale association survives, and Spark keeps
        // masking a column SQE no longer tags. Measured exactly that way before
        // `previous` was threaded through.
        if !previous.is_empty() {
            let delete_doc =
                build_import_document(&self.service_name, table, previous, "delete");
            self.import(delete_doc).await?;
        }
        let add_doc = build_import_document(&self.service_name, table, tags, "add_or_update");
        self.import(add_doc).await?;
        debug!(
            database = %table.database,
            table = %table.table,
            columns = tags.len(),
            "projected column tags into the Ranger tag store"
        );
        Ok(())
    }

    fn enabled(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &[&str])]) -> ColumnTags {
        pairs
            .iter()
            .map(|(c, t)| {
                (
                    (*c).to_string(),
                    t.iter().map(|x| (*x).to_string()).collect(),
                )
            })
            .collect()
    }

    /// The shape asserted here is the one Ranger 2.8 accepted with HTTP 204 in the
    /// live probe. A change that breaks it will not be caught by a type check.
    #[test]
    fn the_document_matches_what_ranger_accepts() {
        let doc = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &tags(&[("ssn", &["pii"])]),
            "add_or_update",
        );
        assert_eq!(doc["op"], "add_or_update");
        assert_eq!(doc["serviceName"], "query");
        assert_eq!(doc["tagDefinitions"]["0"]["name"], "pii");
        assert_eq!(doc["tags"]["0"]["type"], "pii");
        let res = &doc["serviceResources"][0]["resourceElements"];
        assert_eq!(res["database"]["values"][0], "ac");
        assert_eq!(res["table"]["values"][0], "orders");
        assert_eq!(res["column"]["values"][0], "ssn");
        assert_eq!(doc["resourceToTagIds"]["0"][0], 0);
    }

    /// A tag on two columns must be ONE definition referenced twice, not two
    /// definitions. Ranger resolves definitions by name, so duplicates would
    /// collide.
    #[test]
    fn one_tag_on_two_columns_is_one_definition() {
        let doc = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &tags(&[("ssn", &["pii"]), ("email", &["pii"])]),
            "add_or_update",
        );
        assert_eq!(
            doc["tagDefinitions"].as_object().unwrap().len(),
            1,
            "one tag name means one definition"
        );
        assert_eq!(
            doc["serviceResources"].as_array().unwrap().len(),
            2,
            "but two resources, one per tagged column"
        );
    }

    #[test]
    fn a_column_with_no_tags_gets_no_resource() {
        let doc = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &tags(&[("ssn", &[]), ("email", &["pii"])]),
            "add_or_update",
        );
        let resources = doc["serviceResources"].as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(
            resources[0]["resourceElements"]["column"]["values"][0],
            "email"
        );
    }

    /// Pins the convention against the mask path. `resolve_policy_key` sends the
    /// LAST dotted component, so a projected tag resource must too: disagreeing
    /// would put the tag on a resource nothing resolves, and the projection would
    /// appear to succeed while protecting nothing.
    #[test]
    fn the_database_key_is_the_last_namespace_component() {
        assert_eq!(
            TagTableKey::from_namespace("sales_wh.sales", "orders"),
            TagTableKey::new("sales", "orders")
        );
        assert_eq!(
            TagTableKey::from_namespace("sales", "orders"),
            TagTableKey::new("sales", "orders")
        );
        assert_eq!(
            TagTableKey::from_namespace("a.b.c", "t"),
            TagTableKey::new("c", "t")
        );
    }

    #[test]
    fn an_empty_map_projects_nothing() {
        let doc = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &ColumnTags::new(),
            "add_or_update",
        );
        assert!(doc["serviceResources"].as_array().unwrap().is_empty());
        assert!(doc["tagDefinitions"].as_object().unwrap().is_empty());
    }

    /// The noop projector must report itself DISABLED, because the caller uses that
    /// to decide whether the rollback path is needed at all.
    #[tokio::test]
    async fn the_noop_projector_is_disabled_and_succeeds() {
        let p = NoopTagProjector;
        assert!(!p.enabled());
        p.project(
            &TagTableKey::new("ac", "orders"),
            &ColumnTags::new(),
            &ColumnTags::new(),
        )
        .await
        .expect("noop never fails");
    }

    /// The delete document must name the column being UNSET, which only `previous`
    /// knows about. Built from the new map it names nothing, which is how the stale
    /// association survived and Spark kept masking an untagged column.
    #[test]
    fn the_delete_document_names_the_unset_column() {
        let previous = tags(&[("ssn", &["pii"])]);
        let now = ColumnTags::new();
        let del = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &previous,
            "delete",
        );
        assert_eq!(del["op"], "delete");
        assert_eq!(
            del["serviceResources"][0]["resourceElements"]["column"]["values"][0],
            "ssn",
            "the delete must name the column that WAS tagged"
        );
        let add =
            build_import_document("query", &TagTableKey::new("ac", "orders"), &now, "add_or_update");
        assert!(
            add["serviceResources"].as_array().unwrap().is_empty(),
            "and the add names nothing, which is why the delete has to carry it"
        );
    }
}

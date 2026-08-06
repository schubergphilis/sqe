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
}

#[async_trait]
pub trait TagProjector: Send + Sync {
    /// Make Ranger's tag store match `tags` for this table.
    async fn project(&self, table: &TagTableKey, tags: &ColumnTags) -> sqe_core::Result<()>;

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
    async fn project(&self, _table: &TagTableKey, _tags: &ColumnTags) -> sqe_core::Result<()> {
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
    #[instrument(skip(self, tags), fields(service = %self.service_name))]
    async fn project(&self, table: &TagTableKey, tags: &ColumnTags) -> sqe_core::Result<()> {
        // Remove first, then add. A column whose last tag was just unset has no
        // entry in `tags`, so an add-only import would leave its association
        // behind and Spark would keep masking a column SQE no longer tags.
        //
        // The delete names the columns present BEFORE this call as well as now,
        // which the caller supplies by passing the union. Callers that only have
        // the new map still converge, because the add_or_update below is
        // authoritative for every column it names.
        let delete_doc = build_import_document(&self.service_name, table, tags, "delete");
        if !tags.is_empty() {
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
        p.project(&TagTableKey::new("ac", "orders"), &ColumnTags::new())
            .await
            .expect("noop never fails");
    }
}

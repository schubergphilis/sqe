//! Project column-tag associations into Ranger's TAG STORE, so engines other than
//! SQE can enforce tag-based policies.
//!
//! Tag associations are authored by `ALTER TABLE ... SET TAG` and stored in the
//! Iceberg table property `sqe.column-tags`, which is the source of truth and the
//! only thing SQE reads. Spark's Kyuubi plugin reads Ranger's tag store instead and
//! has no reader for Iceberg properties, so WITHOUT this projection a tag-masked
//! column is protected in SQE and returned RAW by Spark.
//!
//! The projection is `PUT /service/tags/importservicetags`. Verified against
//! Ranger 2.8 (#935): `op: add_or_update` writes the tag definitions, the service
//! resources and their associations, and REPLACES the tag set of each resource it
//! names; a resource it does not name is left alone, so one table's projection does
//! not disturb another's. `op: delete` drops each resource it names whole, whatever
//! tags the document lists, and succeeds on a resource Ranger does not hold.
//!
//! So a change is projected as add-or-update first, then a delete of only the
//! resources left with no tags. A delete before the add would leave the column
//! unmasked in other engines between the two calls.
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
/// `database` is the Ranger `database` a mask on the same table is keyed by
/// ([`TagTableKey::for_table`]): the full dotted namespace, prefixed with the
/// catalog under `[policy.ranger] catalog_scoped`. Without that setting two
/// same-named tables in different catalogs share one tag resource, as they
/// share one mask key.
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

    /// The key for `table` in `namespace` (one entry per level) of the catalog
    /// the planner calls `catalog`, by the rule every mask lookup uses
    /// ([`resolve_policy_key_for_ident_scoped`]). A tag resource keyed any other
    /// way lands where no engine resolves it and protects nothing.
    ///
    /// With `catalog_scoped` the `database` is `{catalog}.{namespace}`, so
    /// `a.sales.orders` and `b.sales.orders` get distinct tag resources (#899).
    /// Without it, it is the dotted namespace: `sales` for a single level, and
    /// `wh.sales` for two, which used to be projected as the `\x1F`-joined form
    /// that no mask key matches.
    ///
    /// [`resolve_policy_key_for_ident_scoped`]: crate::plan_rewriter::resolve_policy_key_for_ident_scoped
    pub fn for_table(
        catalog: &str,
        namespace: &[String],
        table: &str,
        catalog_scoped: bool,
    ) -> sqe_core::Result<Self> {
        let hive = crate::plan_rewriter::HiveCatalogs::new();
        crate::plan_rewriter::resolve_policy_key_for_ident_scoped(
            Some(catalog).filter(|c| !c.is_empty()),
            namespace,
            table,
            &hive,
            catalog_scoped,
        )
        .map(|(database, table)| Self::new(database, table))
        .ok_or_else(|| {
            sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                format!(
                    "cannot key the column-tag projection for {}.{table}: catalog_scoped \
                     needs a catalog",
                    namespace.join(".")
                ),
            )
        })
    }
}

/// Project one table's tags under its key (see [`TagTableKey::for_table`]).
///
/// Under `catalog_scoped`, also delete the resources the unscoped key may still
/// hold from before the setting was switched on. Those name only `<namespace>`,
/// so the frontend service would apply them to the same-named table in every
/// catalog. That cleanup is best effort and only logged: the scoped projection,
/// which is what the statement asked for, already succeeded.
pub async fn project_table_tags(
    projector: &dyn TagProjector,
    catalog: &str,
    namespace: &[String],
    table: &str,
    catalog_scoped: bool,
    previous: &ColumnTags,
    tags: &ColumnTags,
) -> sqe_core::Result<()> {
    let key = TagTableKey::for_table(catalog, namespace, table, catalog_scoped)?;
    if catalog_scoped {
        // Both names, so Ranger's evaluator applies the catalog's tag policy
        // and the bare one, as SQE does (#919, `crate::tag_scope`).
        projector
            .project(
                &key,
                &with_catalog_tags(catalog, previous),
                &with_catalog_tags(catalog, tags),
            )
            .await?;
    } else {
        projector.project(&key, previous, tags).await?;
    }
    if catalog_scoped {
        let legacy = TagTableKey::for_table(catalog, namespace, table, false)?;
        let mut stale = previous.clone();
        stale.extend(tags.iter().map(|(c, t)| (c.clone(), t.clone())));
        if let Err(e) = projector.project(&legacy, &stale, &ColumnTags::new()).await {
            tracing::warn!(
                database = %legacy.database,
                table = %legacy.table,
                error = %e,
                "could not delete the unscoped column-tag resources left from before \
                 catalog_scoped; other engines may still apply them in every catalog"
            );
        }
    }
    Ok(())
}

/// Project the tags on the table ITSELF (`sqe.table-tags`, #935) under the same
/// key and with the same catalog rules as [`project_table_tags`].
///
/// They go on two resources of the table, see
/// [`build_table_level_import_document`]: together they reach every column
/// of the table in Spark, including columns added later, without one
/// association per column. The column resources [`project_table_tags`]
/// writes are not touched.
pub async fn project_table_level_tags(
    projector: &dyn TagProjector,
    catalog: &str,
    namespace: &[String],
    table: &str,
    catalog_scoped: bool,
    previous: &[String],
    tags: &[String],
) -> sqe_core::Result<()> {
    let key = TagTableKey::for_table(catalog, namespace, table, catalog_scoped)?;
    if !catalog_scoped {
        return projector.project_table_level(&key, previous, tags).await;
    }
    let scoped = |names: &[String]| -> Vec<String> {
        let mut all = names.to_vec();
        all.extend(
            names
                .iter()
                .map(|t| crate::tag_scope::qualify_tag(catalog, t)),
        );
        all
    };
    projector
        .project_table_level(&key, &scoped(previous), &scoped(tags))
        .await?;
    let legacy = TagTableKey::for_table(catalog, namespace, table, false)?;
    let mut stale = previous.to_vec();
    stale.extend(tags.iter().cloned());
    if let Err(e) = projector.project_table_level(&legacy, &stale, &[]).await {
        tracing::warn!(
            database = %legacy.database,
            table = %legacy.table,
            error = %e,
            "could not delete the unscoped table-tag resource left from before \
             catalog_scoped; other engines may still apply it in every catalog"
        );
    }
    Ok(())
}

/// What Ranger holds on the table-level resources of `table` (`*` for the
/// schema-wide one), keyed like [`project_table_level_tags`] and given back
/// as the tag keys that function takes: under `catalog_scoped` the
/// catalog-qualified copies are folded into their bare key, so a read
/// written back is not qualified twice.
pub async fn held_table_level_tags(
    projector: &dyn TagProjector,
    catalog: &str,
    namespace: &[String],
    table: &str,
    catalog_scoped: bool,
) -> sqe_core::Result<Vec<String>> {
    let key = TagTableKey::for_table(catalog, namespace, table, catalog_scoped)?;
    let held = projector.held_table_level(&key).await?;
    let prefix = format!("{catalog}.");
    let mut keys: Vec<String> = Vec::new();
    for tag in held {
        let bare = match catalog_scoped {
            true => tag.strip_prefix(&prefix).map(str::to_string).unwrap_or(tag),
            false => tag,
        };
        if !keys.contains(&bare) {
            keys.push(bare);
        }
    }
    Ok(keys)
}

/// The two writes that take a table's column resources from `previous` to
/// `tags`: the add-or-update of every column with tags, then the delete of
/// every column `previous` tagged that has none now. Add first, so other
/// engines never see a column untagged in between.
pub fn column_replace_plan(previous: &ColumnTags, tags: &ColumnTags) -> (ColumnTags, ColumnTags) {
    let add: ColumnTags = tags
        .iter()
        .filter(|(_, t)| !t.is_empty())
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    let delete: ColumnTags = previous
        .iter()
        .filter(|(c, t)| !t.is_empty() && !add.contains_key(*c))
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    (add, delete)
}

/// The tags of the table-level resources of `table` in a service-tags
/// download (`GET /service/tags/secure/download/{service}`): the union of the
/// `{database, table, column: *}` and `{database, table}` resources.
pub fn held_in_download(doc: &serde_json::Value, table: &TagTableKey) -> Vec<String> {
    let values = |e: &serde_json::Value, k: &str| -> Option<Vec<String>> {
        let v = e.get(k)?.get("values")?.as_array()?;
        Some(
            v.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
        )
    };
    let is_table_level = |e: &serde_json::Value| -> bool {
        let Some(obj) = e.as_object() else {
            return false;
        };
        let column_ok = match obj.get("column") {
            None => obj.len() == 2,
            Some(_) => obj.len() == 3 && values(e, "column") == Some(vec!["*".to_string()]),
        };
        column_ok
            && values(e, "database") == Some(vec![table.database.clone()])
            && values(e, "table") == Some(vec![table.table.clone()])
    };
    let mut held: Vec<String> = Vec::new();
    let resources = doc["serviceResources"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for resource in resources {
        if !is_table_level(&resource["resourceElements"]) {
            continue;
        }
        let Some(id) = resource["id"].as_i64() else {
            continue;
        };
        let ids = doc["resourceToTagIds"][id.to_string()]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for tag_id in ids.iter().filter_map(serde_json::Value::as_i64) {
            if let Some(name) = doc["tags"][tag_id.to_string()]["type"].as_str() {
                if !held.iter().any(|h| h == name) {
                    held.push(name.to_string());
                }
            }
        }
    }
    held
}

/// `tags` with each column's tags followed by their catalog-qualified names
/// (`pii` -> `pii`, `sales.pii`).
fn with_catalog_tags(catalog: &str, tags: &ColumnTags) -> ColumnTags {
    use crate::tag_scope::qualify_tag;
    tags.iter()
        .map(|(column, names)| {
            let mut all = names.clone();
            all.extend(names.iter().map(|t| qualify_tag(catalog, t)));
            (column.clone(), all)
        })
        .collect()
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

    /// Make the table-level resources carry exactly `tags` (the table's own
    /// tags, not its columns'): a non-empty `tags` replaces what they hold, an
    /// empty one deletes them when `previous` names any tag.
    async fn project_table_level(
        &self,
        _table: &TagTableKey,
        _previous: &[String],
        _tags: &[String],
    ) -> sqe_core::Result<()> {
        Ok(())
    }

    /// The tags Ranger holds on the table-level resources of `table`, read
    /// back from the tag store: the union of both resources. An error means
    /// the store could not be read, never that it holds nothing.
    async fn held_table_level(&self, _table: &TagTableKey) -> sqe_core::Result<Vec<String>> {
        Ok(Vec::new())
    }

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
    let resources = tags.iter().map(|(column, column_tags)| {
        let elements = serde_json::json!({
            "database": {"values": [table.database]},
            "table":    {"values": [table.table]},
            "column":   {"values": [column]},
        });
        (elements, column_tags.as_slice())
    });
    import_document(service, resources, op)
}

/// [`build_import_document`] for the table's own tags (#935), on two
/// resources:
///
/// - `database`, `table`, `column: *`: what a column request (a mask)
///   matches. Kyuubi's plugin does not apply a tag on the bare table
///   resource to the table's columns, though Ranger's enricher accepts the
///   ancestor match (checked live on the Spark stack).
/// - `database`, `table`: what a table request (a row filter) matches.
///
/// A column is never named `*`, so the first resource is no column's own.
pub fn build_table_level_import_document(
    service: &str,
    table: &TagTableKey,
    tags: &[String],
    op: &str,
) -> serde_json::Value {
    let every_column = serde_json::json!({
        "database": {"values": [table.database]},
        "table":    {"values": [table.table]},
        "column":   {"values": ["*"]},
    });
    let the_table = serde_json::json!({
        "database": {"values": [table.database]},
        "table":    {"values": [table.table]},
    });
    import_document(
        service,
        [(every_column, tags), (the_table, tags)].into_iter(),
        op,
    )
}

/// One `ServiceTags` document: each resource (its `resourceElements`) with
/// the tags it carries. A resource with no tags is left out.
fn import_document<'a>(
    service: &str,
    resources: impl Iterator<Item = (serde_json::Value, &'a [String])>,
    op: &str,
) -> serde_json::Value {
    let mut tag_definitions = serde_json::Map::new();
    let mut tag_instances = serde_json::Map::new();
    let mut service_resources = Vec::new();
    let mut resource_to_tag_ids = serde_json::Map::new();

    // One id space for tag names, one for resources.
    let mut tag_id_of: BTreeMap<&str, usize> = BTreeMap::new();
    for (elements, resource_tags) in resources {
        let mut ids = Vec::new();
        for tag in resource_tags {
            let next = tag_id_of.len();
            let id = *tag_id_of.entry(tag.as_str()).or_insert(next);
            tag_definitions.insert(
                id.to_string(),
                serde_json::json!({"id": id, "name": tag, "source": "sqe"}),
            );
            tag_instances.insert(id.to_string(), serde_json::json!({"id": id, "type": tag}));
            ids.push(id);
        }
        if ids.is_empty() {
            continue;
        }
        let resource_id = service_resources.len();
        service_resources.push(serde_json::json!({
            "id": resource_id,
            "serviceName": service,
            "resourceElements": elements,
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
    /// Counts failed calls in `sqe_tag_projection_failures_total`.
    metrics: Option<std::sync::Arc<sqe_metrics::MetricsRegistry>>,
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
            metrics: None,
        })
    }

    /// Count failed Ranger calls in `sqe_tag_projection_failures_total`.
    #[must_use = "with_metrics consumes self"]
    pub fn with_metrics(mut self, metrics: std::sync::Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn count_failure(&self, op: &str) {
        if let Some(m) = &self.metrics {
            m.tag_projection_failures_total
                .with_label_values(&[op])
                .inc();
        }
    }

    async fn import(&self, body: serde_json::Value) -> sqe_core::Result<()> {
        let done = self.send_import(body).await;
        if done.is_err() {
            self.count_failure("write");
        }
        done
    }

    /// The whole tag store of the service. Ranger's per-resource tag
    /// listings ignore their resource filter (checked live on 2.8), so this
    /// is the one read that answers for one resource.
    async fn download(&self) -> sqe_core::Result<serde_json::Value> {
        let done = self.send_download().await;
        if done.is_err() {
            self.count_failure("read");
        }
        done
    }

    async fn send_download(&self) -> sqe_core::Result<serde_json::Value> {
        let url = format!(
            // The secure path: the plain one is unauthenticated, and Ranger
            // 2.9 stops serving it unless that is allowed explicitly.
            "{}/service/tags/secure/download/{}",
            self.base_url, self.service_name
        );
        let failed = |what: String| {
            sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                format!("reading Ranger's tag store from {url} failed: {what}"),
            )
        };
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .header("X-XSRF-HEADER", "x")
            .send()
            .await
            .map_err(|e| failed(e.to_string()))?;
        let status = resp.status();
        // No tag store for the service yet: it holds nothing.
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(serde_json::json!({}));
        }
        if !status.is_success() {
            return Err(failed(format!("HTTP {status}")));
        }
        resp.json().await.map_err(|e| failed(e.to_string()))
    }

    async fn send_import(&self, body: serde_json::Value) -> sqe_core::Result<()> {
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
                sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::ExecutionFailed,
                    format!("Ranger tag projection request to {url} failed: {e}"),
                )
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = match resp.text().await {
                Ok(b) => b,
                Err(e) => format!("<body unreadable: {e}>"),
            };
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                format!("Ranger tag projection returned HTTP {status}: {text}"),
            ));
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
        // Add-or-update replaces each named column's tags, then the columns
        // that lost their last tag are deleted. Those are known only from
        // `previous`: an unset column is absent from `tags`. Deleting first
        // would leave every re-tagged column unmasked in between.
        let (add, delete) = column_replace_plan(previous, tags);
        if !add.is_empty() {
            let add_doc = build_import_document(&self.service_name, table, &add, "add_or_update");
            self.import(add_doc).await?;
        }
        if !delete.is_empty() {
            let delete_doc = build_import_document(&self.service_name, table, &delete, "delete");
            self.import(delete_doc).await?;
        }
        debug!(
            database = %table.database,
            table = %table.table,
            columns = tags.len(),
            "projected column tags into the Ranger tag store"
        );
        Ok(())
    }

    #[instrument(skip(self, previous, tags), fields(service = %self.service_name))]
    async fn project_table_level(
        &self,
        table: &TagTableKey,
        previous: &[String],
        tags: &[String],
    ) -> sqe_core::Result<()> {
        // Add-or-update replaces, so a non-empty set goes alone; a delete
        // first would unmask the table in between. The delete document names
        // `previous` only because Ranger skips a resource with no tags in it;
        // the delete drops the resource whatever it holds.
        let service = &self.service_name;
        if !tags.is_empty() {
            let doc = build_table_level_import_document(service, table, tags, "add_or_update");
            self.import(doc).await?;
        } else if !previous.is_empty() {
            let doc = build_table_level_import_document(service, table, previous, "delete");
            self.import(doc).await?;
        }
        debug!(
            database = %table.database,
            table = %table.table,
            tags = tags.len(),
            "projected table tags into the Ranger tag store"
        );
        Ok(())
    }

    async fn held_table_level(&self, table: &TagTableKey) -> sqe_core::Result<Vec<String>> {
        let doc = self.download().await?;
        Ok(held_in_download(&doc, table))
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
    fn the_database_key_is_the_mask_key() {
        let ns = |parts: &[&str]| parts.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        let key = |c, n: &[&str], scoped| TagTableKey::for_table(c, &ns(n), "orders", scoped);
        let db = |c, n: &[&str], scoped| key(c, n, scoped).unwrap().database;
        // Unscoped: as before for one level; the dotted path for several.
        let unscoped = TagTableKey::new("sales", "orders");
        assert_eq!(key("a", &["sales"], false).unwrap(), unscoped);
        assert_eq!(db("a", &["wh", "sales"], false), "wh.sales");
        // Scoped (#899): the catalog leads, as in `resolve_policy_key_scoped`.
        assert_eq!(db("a", &["sales"], true), "a.sales");
        assert_eq!(db("b", &["sales"], true), "b.sales");
        assert_eq!(db("a", &["wh", "sales"], true), "a.wh.sales");
        assert!(key("", &["sales"], true).is_err(), "scoped needs a catalog");
    }

    /// Under `catalog_scoped` the scoped key is projected and the unscoped one
    /// is cleared of every column it may hold; without it, only the one call.
    #[tokio::test]
    async fn scoped_projection_clears_the_unscoped_resource() {
        #[derive(Default)]
        struct Rec(std::sync::Mutex<Vec<(String, usize, usize)>>);
        #[async_trait]
        impl TagProjector for Rec {
            async fn project(
                &self,
                t: &TagTableKey,
                prev: &ColumnTags,
                tags: &ColumnTags,
            ) -> sqe_core::Result<()> {
                let call = (t.database.clone(), prev.len(), tags.len());
                self.0.lock().unwrap().push(call);
                Ok(())
            }
        }
        let ns = vec!["sales".to_string()];
        let prev = tags(&[("ssn", &["pii"])]);
        let now = tags(&[("email", &["pii"])]);

        let rec = Rec::default();
        let run = project_table_tags(&rec, "a", &ns, "orders", true, &prev, &now);
        run.await.unwrap();
        assert_eq!(
            *rec.0.lock().unwrap(),
            [("a.sales".into(), 1, 1), ("sales".into(), 2, 0)]
        );

        let rec = Rec::default();
        let run = project_table_tags(&rec, "a", &ns, "orders", false, &prev, &now);
        run.await.unwrap();
        assert_eq!(*rec.0.lock().unwrap(), [("sales".into(), 1, 1)]);
    }

    /// Under `catalog_scoped` each column carries its bare tag and the
    /// catalog-qualified one, so Ranger applies both tag policies (#919).
    #[tokio::test]
    async fn scoped_projection_adds_the_catalog_qualified_tag() {
        #[derive(Default)]
        struct Rec(std::sync::Mutex<Vec<(String, ColumnTags, ColumnTags)>>);
        #[async_trait]
        impl TagProjector for Rec {
            async fn project(
                &self,
                t: &TagTableKey,
                prev: &ColumnTags,
                tags: &ColumnTags,
            ) -> sqe_core::Result<()> {
                let call = (t.database.clone(), prev.clone(), tags.clone());
                self.0.lock().unwrap().push(call);
                Ok(())
            }
        }
        let ns = vec!["sales".to_string()];
        let prev = tags(&[("ssn", &["pii"])]);
        let now = tags(&[("email", &["pii"])]);

        let rec = Rec::default();
        let run = project_table_tags(&rec, "a", &ns, "orders", true, &prev, &now);
        run.await.unwrap();
        let calls = rec.0.lock().unwrap().clone();
        assert_eq!(calls[0].0, "a.sales");
        assert_eq!(calls[0].1, tags(&[("ssn", &["pii", "a.pii"])]));
        assert_eq!(calls[0].2, tags(&[("email", &["pii", "a.pii"])]));

        let rec = Rec::default();
        let run = project_table_tags(&rec, "a", &ns, "orders", false, &prev, &now);
        run.await.unwrap();
        assert_eq!(rec.0.lock().unwrap()[0].2, now);
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

    /// Table tags go on the table's every-column resource (masks) and on its
    /// table resource (row filters), both with every tag (#935).
    #[test]
    fn table_tags_go_on_the_table_level_resource() {
        let names = vec!["gold".to_string(), "pii".to_string()];
        let key = TagTableKey::new("ac", "orders");
        let doc = build_table_level_import_document("query", &key, &names, "add_or_update");
        let resources = doc["serviceResources"].as_array().unwrap();
        assert_eq!(resources.len(), 2);
        let every = &resources[0]["resourceElements"];
        assert_eq!(every["database"]["values"][0], "ac");
        assert_eq!(every["table"]["values"][0], "orders");
        assert_eq!(every["column"]["values"], serde_json::json!(["*"]));
        let table = &resources[1]["resourceElements"];
        assert_eq!(table["table"]["values"][0], "orders");
        assert!(table.get("column").is_none(), "no column element: {table}");
        assert_eq!(doc["resourceToTagIds"]["0"], serde_json::json!([0, 1]));
        assert_eq!(doc["resourceToTagIds"]["1"], serde_json::json!([0, 1]));
        assert_eq!(doc["tagDefinitions"]["1"]["name"], "pii");

        let none = build_table_level_import_document("query", &key, &[], "add_or_update");
        assert!(none["serviceResources"].as_array().unwrap().is_empty());
    }

    /// Table-level projection keys and scopes like the column projection: the
    /// catalog-qualified names are added under `catalog_scoped` and the
    /// unscoped resource is cleared. Column resources are never named.
    #[tokio::test]
    async fn table_level_projection_is_scoped_like_columns() {
        type Call = (String, Vec<String>, Vec<String>);
        #[derive(Default)]
        struct Rec(std::sync::Mutex<Vec<Call>>);
        #[async_trait]
        impl TagProjector for Rec {
            async fn project(
                &self,
                _: &TagTableKey,
                _: &ColumnTags,
                _: &ColumnTags,
            ) -> sqe_core::Result<()> {
                panic!("table tags must not touch column resources");
            }
            async fn project_table_level(
                &self,
                t: &TagTableKey,
                prev: &[String],
                tags: &[String],
            ) -> sqe_core::Result<()> {
                let call = (t.database.clone(), prev.to_vec(), tags.to_vec());
                self.0.lock().unwrap().push(call);
                Ok(())
            }
        }
        let ns = vec!["sales".to_string()];
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let (prev, now) = (s(&["gold"]), s(&["pii"]));

        let rec = Rec::default();
        let run = project_table_level_tags(&rec, "a", &ns, "orders", true, &prev, &now);
        run.await.unwrap();
        assert_eq!(
            *rec.0.lock().unwrap(),
            [
                (
                    "a.sales".into(),
                    s(&["gold", "a.gold"]),
                    s(&["pii", "a.pii"])
                ),
                ("sales".into(), s(&["gold", "pii"]), vec![]),
            ]
        );

        let rec = Rec::default();
        let run = project_table_level_tags(&rec, "a", &ns, "orders", false, &prev, &now);
        run.await.unwrap();
        assert_eq!(*rec.0.lock().unwrap(), [("sales".into(), prev, now)]);
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
            del["serviceResources"][0]["resourceElements"]["column"]["values"][0], "ssn",
            "the delete must name the column that WAS tagged"
        );
        let add = build_import_document(
            "query",
            &TagTableKey::new("ac", "orders"),
            &now,
            "add_or_update",
        );
        assert!(
            add["serviceResources"].as_array().unwrap().is_empty(),
            "and the add names nothing, which is why the delete has to carry it"
        );
    }

    /// A change is the add-or-update of every tagged column, then the delete
    /// of the columns that lost their last tag; a re-tagged column is never
    /// deleted, so no engine sees it untagged in between (#935).
    #[test]
    fn a_column_change_adds_first_and_deletes_only_untagged_columns() {
        let previous = tags(&[("ssn", &["pii"]), ("email", &["pii"]), ("note", &[])]);
        let now = tags(&[("ssn", &["fin"]), ("phone", &["pii"]), ("note", &[])]);
        let (add, delete) = column_replace_plan(&previous, &now);
        assert_eq!(add, tags(&[("ssn", &["fin"]), ("phone", &["pii"])]));
        assert_eq!(delete, tags(&[("email", &["pii"])]));

        let (add, delete) = column_replace_plan(&ColumnTags::new(), &now);
        assert_eq!(add.len(), 2);
        assert!(delete.is_empty(), "no previous map deletes nothing");
    }

    fn download(resources: &[(&str, &str, Option<&str>, &[&str])]) -> serde_json::Value {
        let (mut res, mut map, mut defs) =
            (Vec::new(), serde_json::Map::new(), serde_json::Map::new());
        let mut names: Vec<String> = Vec::new();
        for (i, (db, table, column, tags)) in resources.iter().enumerate() {
            let mut elements = serde_json::json!({
                "database": {"values": [db], "isExcludes": false, "isRecursive": false},
                "table": {"values": [table], "isExcludes": false, "isRecursive": false},
            });
            if let Some(c) = column {
                elements["column"] = serde_json::json!({"values": [c]});
            }
            let id = 100 + i as i64;
            res.push(serde_json::json!({"id": id, "resourceElements": elements}));
            let ids: Vec<i64> = tags
                .iter()
                .map(|t| {
                    let pos = names.iter().position(|n| n == t).unwrap_or_else(|| {
                        names.push(t.to_string());
                        names.len() - 1
                    });
                    let tag_id = 7 + pos as i64;
                    defs.insert(
                        tag_id.to_string(),
                        serde_json::json!({"id": tag_id, "type": t}),
                    );
                    tag_id
                })
                .collect();
            map.insert(id.to_string(), serde_json::json!(ids));
        }
        serde_json::json!({"serviceResources": res, "resourceToTagIds": map, "tags": defs})
    }

    /// The read-back takes only the two table-level resources of the one
    /// table asked for: another schema's, a column's and a wider resource's
    /// tags are not counted as held (#935).
    #[test]
    fn the_read_back_takes_only_this_tables_table_level_resources() {
        let doc = download(&[
            ("sales", "*", Some("*"), &["pii"]),
            ("sales", "*", None, &["pii", "gold"]),
            ("hr", "*", Some("*"), &["fin"]),
            ("hr", "*", None, &["fin"]),
            ("sales", "*", Some("ssn"), &["secret"]),
            ("sales", "orders", Some("*"), &["other"]),
        ]);
        let key = TagTableKey::new("sales", "*");
        assert_eq!(held_in_download(&doc, &key), vec!["pii", "gold"]);
        assert_eq!(
            held_in_download(&doc, &TagTableKey::new("hr", "*")),
            vec!["fin"]
        );
        assert!(held_in_download(&doc, &TagTableKey::new("ops", "*")).is_empty());
        assert!(held_in_download(&serde_json::json!({}), &key).is_empty());
    }

    /// A fake tag store with the semantics probed on Ranger 2.8: add-or-update
    /// replaces a resource's tags, delete drops the resource.
    #[derive(Default)]
    struct Store(std::sync::Mutex<BTreeMap<String, Vec<String>>>);

    #[async_trait]
    impl TagProjector for Store {
        async fn project(
            &self,
            _: &TagTableKey,
            _: &ColumnTags,
            _: &ColumnTags,
        ) -> sqe_core::Result<()> {
            Ok(())
        }
        async fn project_table_level(
            &self,
            t: &TagTableKey,
            previous: &[String],
            tags: &[String],
        ) -> sqe_core::Result<()> {
            let key = format!("{}|{}", t.database, t.table);
            let mut store = self.0.lock().unwrap();
            if !tags.is_empty() {
                store.insert(key, tags.to_vec());
            } else if !previous.is_empty() {
                store.remove(&key);
            }
            Ok(())
        }
        async fn held_table_level(&self, t: &TagTableKey) -> sqe_core::Result<Vec<String>> {
            let key = format!("{}|{}", t.database, t.table);
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default())
        }
    }

    /// Under `catalog_scoped` the read-back folds `a.pii` into `pii`, so a
    /// read written back keeps the same set however often it cycles, instead
    /// of growing `a.a.pii` (#935).
    #[tokio::test]
    async fn a_scoped_read_written_back_is_stable() {
        let store = Store::default();
        let ns = vec!["sales".to_string()];
        let pii = vec!["pii".to_string()];
        project_table_level_tags(&store, "a", &ns, "*", true, &[], &pii)
            .await
            .unwrap();
        for _ in 0..2 {
            let held = held_table_level_tags(&store, "a", &ns, "*", true)
                .await
                .unwrap();
            assert_eq!(held, pii);
            project_table_level_tags(&store, "a", &ns, "*", true, &[], &held)
                .await
                .unwrap();
        }
        let raw = store.0.lock().unwrap().get("a.sales|*").cloned().unwrap();
        assert_eq!(raw, vec!["pii".to_string(), "a.pii".to_string()]);
    }
}

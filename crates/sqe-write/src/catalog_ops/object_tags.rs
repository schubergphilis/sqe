//! `ALTER TABLE|SCHEMA|CATALOG ... SET|UNSET TAGS ('k' = 'v')`: tags on the object
//! itself (#922), stored in the format [`sqe_core::object_tags`] defines. The
//! query handler gates the statement first (`require_object_tag_authority`).
//!
//! Table tags reach every column of the table (#924), so tag policies mask
//! through them. They are projected onto the table's table and every-column
//! Ranger resources, so Spark masks and filters with them too (#935). Schema
//! tags are not projected yet. Catalog tags are metadata only: nothing
//! inherits them, so they drive no mask and are not projected.

use std::collections::HashMap;

use iceberg::{NamespaceIdent, TableUpdate};
use sqe_core::column_tags::ColumnTag;
use sqe_core::object_tags::{self, CATALOG_PROP_KEY, SCHEMA_PROP_KEY, TABLE_PROP_KEY};
use sqe_core::{Session, SqeError, SqeErrorCode};
use sqe_sql::object_tags::{ObjectTagsStatement, TagObject};
use sqe_sql::tags::TagAction;
use tracing::info;

use super::{
    catalog_qualifier, keyed_lock, namespace_without_catalog, parse_object_name,
    resolve_table_ident, resolve_target_catalog, schema_catalog_qualifier, CatalogOps,
};

/// The tags `stmt` leaves on an object that carries `raw` now. Refuses a
/// value that is not a tag list rather than overwrite it.
fn next_tags(
    raw: Option<&str>,
    stmt: &ObjectTagsStatement,
    key: &str,
    on: &str,
) -> sqe_core::Result<String> {
    let current: Vec<ColumnTag> = object_tags::parse(raw).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "malformed {key} JSON on {on}: {e}; refusing to apply SET/UNSET TAGS \
                 so existing tags are not wiped"
            ),
        )
    })?;
    let next = object_tags::apply(
        &current,
        stmt.action == TagAction::Set,
        &stmt.tags,
        &stmt.values,
    );
    object_tags::to_json(&next).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("failed to serialize {key}: {e}"),
        )
    })
}

/// The keys of a tag list value; `next_tags` has already refused a malformed
/// one.
fn tag_keys(raw: Option<&str>) -> sqe_core::Result<Vec<String>> {
    let tags = object_tags::parse(raw).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("malformed {TABLE_PROP_KEY} JSON: {e}"),
        )
    })?;
    Ok(tags.into_iter().map(|t| t.key).collect())
}

/// Each key of `a`, then each key of `b` not in `a`.
fn union(a: &[String], b: &[String]) -> Vec<String> {
    let mut all = a.to_vec();
    all.extend(b.iter().filter(|k| !a.contains(k)).cloned());
    all
}

impl CatalogOps {
    /// Write the tags of the table, schema or catalog `stmt` names.
    pub async fn set_object_tags(
        &self,
        session: &Session,
        stmt: &ObjectTagsStatement,
    ) -> sqe_core::Result<()> {
        match &stmt.object {
            TagObject::Table(name) => self.set_table_tags(session, name, stmt).await,
            TagObject::Schema(name) => self.set_schema_tags(session, name, stmt).await,
            TagObject::Catalog(name) => self.set_catalog_tags(session, name, stmt).await,
        }
    }

    /// Catalog tags are a Polaris catalog property, written through the
    /// management API on the caller's token: Polaris authorizes the update
    /// (`UPDATE_CATALOG`) and its entity version makes it a compare and set.
    async fn set_catalog_tags(
        &self,
        session: &Session,
        name: &str,
        stmt: &ObjectTagsStatement,
    ) -> sqe_core::Result<()> {
        let catalog = catalog_target(&parse_object_name(name)?)?;
        let session_catalog = self.session_catalog_for(session, Some(&catalog)).await?;
        info!(
            username = %session.user.username,
            catalog = %catalog,
            action = ?stmt.action,
            "Authoring catalog tags"
        );
        let on = format!("catalog '{catalog}'");
        session_catalog
            .update_catalog_property(CATALOG_PROP_KEY, |raw| {
                next_tags(raw, stmt, CATALOG_PROP_KEY, &on).map(Some)
            })
            .await
    }

    async fn set_table_tags(
        &self,
        session: &Session,
        name: &str,
        stmt: &ObjectTagsStatement,
    ) -> sqe_core::Result<()> {
        let object_name = parse_object_name(name)?;
        let table_ident = resolve_table_ident(&object_name, session)?;
        // Resolved as `set_column_tags` resolves it, so the gate's key and the
        // written table agree.
        let target = resolve_target_catalog(catalog_qualifier(&object_name), session);
        let session_catalog = self.session_catalog_for(session, target.as_deref()).await?;

        // Read-modify-write: serialised per table for the reason
        // `set_column_tags` gives (a `SetProperties` commit has no CAS).
        let table_lock = self.table_lock_for(&table_ident);
        let _guard = table_lock.lock().await;

        let table = session_catalog.load_table(&table_ident).await?;
        let raw = table
            .metadata()
            .properties()
            .get(TABLE_PROP_KEY)
            .map(String::as_str);
        let json = next_tags(raw, stmt, TABLE_PROP_KEY, &format!("table '{table_ident}'"))?;
        info!(
            username = %session.user.username,
            table = %table_ident,
            action = ?stmt.action,
            "Authoring table tags"
        );
        // Ranger's table-level resource (#935), in the order column tags use:
        // what the statement adds is there before the commit, what it
        // removes goes after, so other engines never mask less than SQE.
        let projection = match self.tag_projector.enabled() {
            true => Some((
                self.planner_catalog(session, catalog_qualifier(&object_name)),
                tag_keys(raw)?,
                tag_keys(Some(&json))?,
            )),
            false => None,
        };
        if let Some((catalog, before, after)) = &projection {
            let added: Vec<String> = after
                .iter()
                .filter(|k| !before.contains(k))
                .cloned()
                .collect();
            if !added.is_empty() {
                let during = union(before, after);
                let run = self.project_table_level_keys(catalog, &table_ident, &[], &during);
                run.await.map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!(
                            "could not project the tags of '{table_ident}' into Ranger ({e}); \
                             nothing was changed"
                        ),
                    )
                })?;
            }
        }
        session_catalog
            .commit_schema_update(
                &table_ident,
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from([(TABLE_PROP_KEY.to_string(), json)]),
                }],
                vec![],
            )
            .await?;
        // Every user's cached metadata, under the lock, so the next writer
        // and every reader see the new list.
        self.invalidate_table_all_tokens(&table_ident).await;
        if let Some((catalog, before, after)) = &projection {
            if before.iter().any(|k| !after.contains(k)) {
                let run = self.project_table_level_keys(catalog, &table_ident, before, after);
                if let Err(e) = run.await {
                    tracing::warn!(
                        table = %table_ident,
                        error = %e,
                        "table tags were removed but Ranger still holds them; other engines \
                         over-mask this table until `CALL system.reproject_column_tags` or \
                         the next tag change"
                    );
                }
            }
        }
        Ok(())
    }

    async fn set_schema_tags(
        &self,
        session: &Session,
        name: &str,
        stmt: &ObjectTagsStatement,
    ) -> sqe_core::Result<()> {
        let object_name = parse_object_name(name)?;
        let (target, namespace) = schema_target(&object_name, session)?;
        let session_catalog = self.session_catalog_for(session, target.as_deref()).await?;

        // A namespace property update merges keys but cannot compare and set
        // one, so the read-modify-write is serialised here, per catalog and
        // namespace. SessionCatalog keeps no namespace cache to evict.
        let lock = keyed_lock(
            &self.table_locks,
            format!(
                "schema-tags|{}|{}",
                target.as_deref().unwrap_or(""),
                namespace.to_url_string()
            ),
        );
        let _guard = lock.lock().await;

        let current = session_catalog.get_namespace(&namespace).await?;
        let json = next_tags(
            current
                .properties()
                .get(SCHEMA_PROP_KEY)
                .map(String::as_str),
            stmt,
            SCHEMA_PROP_KEY,
            &format!("schema '{}'", namespace.join(".")),
        )?;
        info!(
            username = %session.user.username,
            namespace = ?namespace,
            target_catalog = ?target,
            action = ?stmt.action,
            "Authoring schema tags"
        );
        session_catalog
            .update_namespace(
                &namespace,
                HashMap::from([(SCHEMA_PROP_KEY.to_string(), json)]),
            )
            .await
    }
}

/// The catalog and namespace a schema name resolves to, as `CREATE SCHEMA`
/// resolves it: `c.s` is schema `s` of catalog `c`, a bare `s` lives in the
/// session's catalog.
pub fn schema_target(
    name: &sqlparser::ast::ObjectName,
    session: &Session,
) -> sqe_core::Result<(Option<String>, NamespaceIdent)> {
    let target = schema_catalog_qualifier(name).or_else(|| session.default_catalog.clone());
    Ok((target, namespace_without_catalog(name)?))
}

/// The catalog `ALTER CATALOG <name>` names: exactly one identifier.
pub fn catalog_target(name: &sqlparser::ast::ObjectName) -> sqe_core::Result<String> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .collect();
    match parts.as_slice() {
        [catalog] if name.0.len() == 1 => Ok((*catalog).to_string()),
        _ => Err(SqeError::with_code(
            SqeErrorCode::InvalidArguments,
            format!("ALTER CATALOG takes a single catalog name, got '{name}'"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn stmt(action: TagAction, tags: &[&str], values: &[(&str, &str)]) -> ObjectTagsStatement {
        ObjectTagsStatement {
            object: TagObject::Table("t".into()),
            action,
            tags: tags.iter().map(|s| s.to_string()).collect(),
            values: values
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn next_tags_merges_into_the_live_list() {
        let set = stmt(TagAction::Set, &["owner", "gold"], &[("owner", "risk")]);
        let got = next_tags(Some(r#"[{"owner":"finance"},"pii"]"#), &set, "k", "t").unwrap();
        assert_eq!(got, r#"[{"owner":"risk"},"pii","gold"]"#);
        let unset = stmt(TagAction::Unset, &["pii", "owner", "gold"], &[]);
        assert_eq!(next_tags(Some(&got), &unset, "k", "t").unwrap(), "[]");
        assert_eq!(
            next_tags(None, &set, "k", "t").unwrap(),
            r#"[{"owner":"risk"},"gold"]"#
        );
    }

    #[test]
    fn a_catalog_target_is_one_name() {
        let one = super::super::parse_object_name("sales_wh").unwrap();
        assert_eq!(catalog_target(&one).unwrap(), "sales_wh");
        let two = super::super::parse_object_name("a.b").unwrap();
        assert!(catalog_target(&two).is_err());
    }

    #[test]
    fn a_malformed_live_value_is_not_overwritten() {
        let set = stmt(TagAction::Set, &["gold"], &[]);
        let err = next_tags(Some("not json"), &set, TABLE_PROP_KEY, "table 't'").unwrap_err();
        assert!(err.to_string().contains("not wiped"), "{err}");
    }
}

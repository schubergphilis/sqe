//! Who may tag a table or schema itself (#922), and the statement's handler.
//!
//! Object tags drive no mask, but they are governance metadata a steward
//! owns, so they follow `tag_authority` like column tags. Admins always
//! pass. Under `admin-role` only admins may write them. Under
//! `ranger-delegate` a table needs the configured privilege on the table
//! (a column grant does not count) and a schema needs it on the database
//! with table `*`. A catalog has no resource in the query service, so under
//! `ranger-delegate` Polaris decides: the write goes to its management API
//! on the caller's token and needs catalog-properties write there. `tag_names`
//! checks the keys a `SET` names, after the privilege, as for column tags.

use std::collections::BTreeSet;

use super::*;
use sqe_sql::object_tags::{ObjectTagsStatement, TagObject};
use sqe_sql::tags::TagAction;
use sqe_write::catalog_ops::object_tags::{catalog_target, schema_target};

impl QueryHandler {
    /// `ALTER TABLE|SCHEMA|CATALOG <name> SET|UNSET TAGS ('k' = 'v')`.
    pub(crate) async fn handle_set_object_tags(
        &self,
        session: &Session,
        stmt: &ObjectTagsStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let label = format!("ALTER {} ... SET/UNSET TAGS", stmt.object.kind());
        let catalog = self
            .require_object_tag_authority(session, stmt, &label)
            .await?;
        match &stmt.object {
            TagObject::Table(name) => {
                self.reject_if_hive_table(session, &TableReference::from(name.as_str()), &label, 3)?
            }
            TagObject::Schema(_) | TagObject::Catalog(_) => {
                if self
                    .hive_catalogs
                    .as_deref()
                    .is_some_and(|h| h.contains_name(Some(&catalog)))
                {
                    return Err(self.hive_ddl_handler.reject_unsupported(&label, 3));
                }
            }
        }
        self.catalog_ops.set_object_tags(session, stmt).await?;
        crate::session_context::invalidate_session_cache(&session.user.username).await;
        Ok(vec![])
    }

    /// Gate an object tag write; returns the catalog the object resolves in.
    async fn require_object_tag_authority(
        &self,
        session: &Session,
        stmt: &ObjectTagsStatement,
        statement: &str,
    ) -> sqe_core::Result<String> {
        let name = crate::catalog_ops::parse_object_name(stmt.object.name())?;
        // Resolved as the write resolves it, so the key checked is the
        // object written.
        let (catalog, namespace, table) = match &stmt.object {
            TagObject::Table(_) => {
                let ident = crate::ident::resolve_table_ident(&name, session)?;
                let catalog = crate::ident::catalog_qualifier(&name)
                    .unwrap_or_else(|| self.session_catalog_name(session));
                (catalog, ident.namespace().clone(), ident.name().to_string())
            }
            TagObject::Schema(_) => {
                let (target, namespace) = schema_target(&name, session)?;
                let catalog = target.unwrap_or_else(|| self.session_catalog_name(session));
                (catalog, namespace, "*".to_string())
            }
            TagObject::Catalog(_) => {
                return self
                    .require_catalog_tag_authority(session, stmt, catalog_target(&name)?, statement)
                    .await;
            }
        };
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(catalog);
        }
        let Some(store) = self.tag_delegate_store() else {
            self.require_admin(session, statement)?;
            return Ok(catalog);
        };
        let privilege = self.tag_privilege();
        let default_hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
        let hive_catalogs = self.hive_catalogs.as_deref().unwrap_or(&default_hive);
        let (policy_ns, policy_table) =
            sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
                Some(&catalog),
                namespace.as_ref(),
                &table,
                hive_catalogs,
                store.catalog_scoped(),
            )
            .ok_or_else(|| {
                self.refuse_tag_write(
                    session,
                    statement,
                    &format!("cannot key '{}' for a policy check", stmt.object.name()),
                )
            })?;
        let granted = store
            .grants_table_privilege(&session.user, &policy_table, &policy_ns, privilege)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "tag authority check failed; treating as not granted");
                false
            });
        if !granted {
            return Err(self.refuse_tag_write(
                session,
                statement,
                &format!(
                    "requires {} on {policy_ns}.{policy_table}",
                    privilege.access_type()
                ),
            ));
        }
        self.require_defined_object_tags(session, stmt, &catalog, statement)
            .await?;
        Ok(catalog)
    }

    /// The catalog gate: admins pass, `admin-role` refuses everyone else,
    /// and under `ranger-delegate` the privilege is Polaris's to check when
    /// the write reaches it. The name check still runs here.
    async fn require_catalog_tag_authority(
        &self,
        session: &Session,
        stmt: &ObjectTagsStatement,
        catalog: String,
        statement: &str,
    ) -> sqe_core::Result<String> {
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(catalog);
        }
        if self.tag_delegate_store().is_none() {
            self.require_admin(session, statement)?;
            return Ok(catalog);
        }
        self.require_defined_object_tags(session, stmt, &catalog, statement)
            .await?;
        Ok(catalog)
    }

    /// `tag_names` for the keys a `SET` adds. UNSET only removes names.
    async fn require_defined_object_tags(
        &self,
        session: &Session,
        stmt: &ObjectTagsStatement,
        catalog: &str,
        statement: &str,
    ) -> sqe_core::Result<()> {
        if stmt.action != TagAction::Set {
            return Ok(());
        }
        let names: BTreeSet<&str> = stmt.tags.iter().map(String::as_str).collect();
        self.require_defined_tags_in(session, catalog.to_string(), &names, statement)
            .await
    }
}

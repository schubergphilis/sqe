//! Who may write column tags, per `[access_control] tag_authority` and
//! `tag_privilege`. Every statement that writes the `sqe.column-tags` table
//! property is gated here: `ALTER TABLE ... SET / UNSET TAGS`, `SET / UNSET
//! MASKING POLICY`, `ALTER TABLE ... SET TBLPROPERTIES` and `CREATE [OR
//! REPLACE] TABLE ... TBLPROPERTIES` (#911, #917).
//!
//! Admins always pass. Under `admin-role`, or with no policy store, the tag
//! statements, `SET TBLPROPERTIES` and `CREATE OR REPLACE` need an admin role
//! and a plain `CREATE TABLE` is not gated, as in 0.55. Under
//! `ranger-delegate` each column a statement changes needs the configured
//! privilege (`alter` or `apply_tag`) from the store: the columns of a tag
//! statement, and for a raw property write the columns whose tags differ
//! between the live map and the new one (a new table: every column named).

use std::collections::{BTreeSet, HashMap};

use super::*;
use crate::tag_source_impl::{changed_tag_columns, tag_property_write};
use sqe_core::config::{TagAuthority, TagPrivilege};
use sqe_policy::TablePrivilege;

const RAW_TAG_WRITE: &str = "setting the sqe.column-tags table property";

/// True when an `ALTER TABLE ... SET TBLPROPERTIES` (or a `CREATE OR REPLACE
/// TABLE`) writes `sqe.column-tags`, in any case and through any spelling
/// (`extra_properties` included). Under `admin-role` that needs the admin role
/// SET/UNSET TAGS needs. An in-place replace sets the statement's properties
/// on the live table, whose older snapshots still hold the data those tags
/// mask (#894). A plain `CREATE TABLE` is a new table and stays ungated there.
fn alters_or_replaces(stmt: &Statement) -> bool {
    match stmt {
        Statement::AlterTable(_) => true,
        Statement::CreateTable(ct) => ct.or_replace,
        _ => false,
    }
}

impl QueryHandler {
    /// The store that decides tag writes: set under `ranger-delegate` when one
    /// is wired. `None` means only admins may tag.
    fn tag_delegate_store(&self) -> Option<&dyn PolicyStore> {
        match self.config.access_control.tag_authority {
            TagAuthority::RangerDelegate => self.policy_store.as_deref(),
            TagAuthority::AdminRole => None,
        }
    }

    /// The column privilege a tag write needs under `ranger-delegate`.
    fn tag_privilege(&self) -> TablePrivilege {
        match self.config.access_control.tag_privilege {
            TagPrivilege::Alter => TablePrivilege::Alter,
            TagPrivilege::ApplyTag => TablePrivilege::ApplyTag,
        }
    }

    /// Log the refusal (the audit line an operator greps for) and build the
    /// error `classify_catalog_error` maps to access denied.
    fn refuse_tag_write(&self, session: &Session, statement: &str, why: &str) -> SqeError {
        warn!(
            username = %session.user.username,
            roles = ?session.user.roles,
            statement,
            privilege = self.tag_privilege().access_type(),
            "denied: {why}"
        );
        SqeError::Catalog(format!("403 Forbidden: {statement} {why}"))
    }

    /// Gate a column-tag write (SET/UNSET TAGS, SET/UNSET MASKING POLICY).
    /// Under `ranger-delegate` every column of `stmt` must be granted the
    /// configured privilege before anything is written.
    pub(super) async fn require_tag_authority(
        &self,
        session: &Session,
        stmt: &sqe_sql::tags::SetTagsStatement,
        statement: &str,
    ) -> sqe_core::Result<()> {
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(());
        }
        let Some(store) = self.tag_delegate_store() else {
            return self.require_admin(session, statement);
        };
        let object_name = crate::catalog_ops::parse_object_name(&stmt.table)?;
        self.require_tag_columns(
            session,
            store,
            &object_name,
            &stmt
                .ops
                .iter()
                .map(|op| op.column.as_str())
                .collect::<Vec<_>>(),
            statement,
        )
        .await
    }

    /// Every column in `columns` of `table` must be granted the configured
    /// privilege by `store`. The policy key is derived with the same functions
    /// the tag writes resolve their table with, so the resource checked is the
    /// resource written (a bare name resolves against the session schema). A
    /// store error grants nothing.
    async fn require_tag_columns(
        &self,
        session: &Session,
        store: &dyn PolicyStore,
        table: &sqlparser::ast::ObjectName,
        columns: &[&str],
        statement: &str,
    ) -> sqe_core::Result<()> {
        let privilege = self.tag_privilege();
        let table_ident = crate::ident::resolve_table_ident(table, session)?;
        let catalog_name = crate::ident::catalog_qualifier(table)
            .unwrap_or_else(|| self.session_catalog_name(session));
        let default_hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
        let hive_catalogs = self.hive_catalogs.as_deref().unwrap_or(&default_hive);
        let (namespace, table_name) =
            sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
                Some(&catalog_name),
                table_ident.namespace().as_ref(),
                table_ident.name(),
                hive_catalogs,
                store.catalog_scoped(),
            )
            .ok_or_else(|| {
                self.refuse_tag_write(
                    session,
                    statement,
                    &format!("cannot key table '{table}' for a policy check"),
                )
            })?;
        for &column in columns {
            let granted = store
                .grants_column_privilege(&session.user, &table_name, &namespace, column, privilege)
                .await
                .unwrap_or_else(|e| {
                    warn!(error = %e, column, "tag authority check failed; treating as not granted");
                    false
                });
            if !granted {
                return Err(self.refuse_tag_write(
                    session,
                    statement,
                    &format!(
                        "requires {} on {namespace}.{table_name}.{column}",
                        privilege.access_type()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The part of the raw `sqe.column-tags` gate that needs no catalog: run
    /// before the catalog preflight, so a refused caller learns nothing about
    /// which catalogs or tables exist. Under `admin-role` this is the whole
    /// gate. Under `ranger-delegate` it refuses an off-case key and a value
    /// that is not a tag map; [`Self::authorise_tag_property`] checks the
    /// columns once the catalogs resolve.
    pub(super) fn gate_tag_property_statically(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let Some(write) = tag_property_write(stmt)? else {
            return Ok(());
        };
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(());
        }
        if self.tag_delegate_store().is_none() {
            return if alters_or_replaces(stmt) {
                self.require_admin(session, RAW_TAG_WRITE)
            } else {
                Ok(())
            };
        }
        if write.off_case_key {
            return Err(self.refuse_tag_write(
                session,
                RAW_TAG_WRITE,
                "only an admin may write a key spelled like sqe.column-tags in another case",
            ));
        }
        parse_tag_value(write.value.as_deref())
            .map(|_| ())
            .map_err(|why| self.refuse_tag_write(session, RAW_TAG_WRITE, &why))
    }

    /// The per-column part of the raw `sqe.column-tags` gate under
    /// `ranger-delegate`, after the catalog preflight. Diffs the live tag map
    /// against the one `stmt` writes and requires the configured privilege on
    /// every added, changed or removed column. Returns those columns for a
    /// non-admin under `ranger-delegate` (the `ALTER` write re-checks them
    /// under its table lock), `None` when no per-column check applies.
    ///
    /// A `CREATE OR REPLACE` is checked against the live table and then
    /// replaced without a lock spanning both: a tag write landing in between
    /// is replaced by the statement's map. The window is the one any replace
    /// has against a concurrent write.
    pub(super) async fn authorise_tag_property(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Option<BTreeSet<String>>> {
        if self.config.auth.has_admin_role(&session.user.roles) {
            return Ok(None);
        }
        let Some(store) = self.tag_delegate_store() else {
            return Ok(None);
        };
        let Some(write) = tag_property_write(stmt)? else {
            return Ok(None);
        };
        if write.off_case_key {
            return Err(self.refuse_tag_write(
                session,
                RAW_TAG_WRITE,
                "only an admin may write a key spelled like sqe.column-tags in another case",
            ));
        }
        let new = parse_tag_value(write.value.as_deref())
            .map_err(|why| self.refuse_tag_write(session, RAW_TAG_WRITE, &why))?;
        let (table, live) = match stmt {
            Statement::AlterTable(at) => (&at.name, true),
            Statement::CreateTable(ct) => (&ct.name, ct.or_replace),
            _ => return Ok(None),
        };
        let old = if live {
            match self.catalog_ops.live_column_tags(session, table).await? {
                Some(Ok(map)) => map,
                Some(Err(e)) => {
                    return Err(self.refuse_tag_write(
                        session,
                        RAW_TAG_WRITE,
                        &format!("the live value on '{table}' is not a tag map ({e}); only an admin can overwrite it"),
                    ))
                }
                None => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        let changed = changed_tag_columns(&old, &new);
        self.require_tag_columns(
            session,
            store,
            table,
            &changed.iter().map(String::as_str).collect::<Vec<_>>(),
            RAW_TAG_WRITE,
        )
        .await?;
        Ok(Some(changed))
    }
}

/// The tag map a raw property write sets, or why it is refused.
fn parse_tag_value(value: Option<&str>) -> Result<HashMap<String, Vec<String>>, String> {
    let raw = value.ok_or_else(|| "names no sqe.column-tags value".to_string())?;
    serde_json::from_str(raw)
        .map_err(|e| format!("sqe.column-tags must be a JSON object of column to tag list ({e})"))
}

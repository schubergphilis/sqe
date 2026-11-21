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
//!
//! Under `[access_control] tag_names = "ranger-tagdefs"` a non-admin write
//! must also name only tags Ranger defines (#926), checked after the
//! privilege so a refused caller learns nothing about the definitions.

use std::collections::{BTreeSet, HashMap, HashSet};

use super::*;
use crate::tag_source_impl::{changed_tag_columns, tag_property_write};
use sqe_core::column_tags::ColumnTagMap;
use sqe_core::config::{MaskPrecedence, TagAuthority, TagNames, TagPrivilege};
use sqe_policy::tag_conflict_guard::TagConflictGuard;
use sqe_policy::TablePrivilege;

mod object_tags;

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
        .await?;
        let set: BTreeSet<&str> = stmt
            .ops
            .iter()
            .filter(|op| op.action == sqe_sql::tags::TagAction::Set)
            .flat_map(|op| op.tags.iter().map(String::as_str))
            .collect();
        self.require_defined_tags(session, &object_name, &set, statement)
            .await
    }

    /// `[access_control] tag_names = "ranger-tagdefs"`: every name in `names`
    /// must have a Ranger tag definition. Under `[policy.ranger]
    /// catalog_scoped` a definition `<catalog>.<tag>` (the name the tag
    /// projector writes for a catalog-scoped tag) also defines `<tag>` for a
    /// table of `<catalog>`; the catalog is derived as the privilege check
    /// derives it. The cached list is re-read once before a name is refused,
    /// so a tag defined a moment ago is accepted. Fails closed: no policy
    /// store, or a list that cannot be read, refuses the write. Callers
    /// exempt admins.
    async fn require_defined_tags(
        &self,
        session: &Session,
        table: &sqlparser::ast::ObjectName,
        names: &BTreeSet<&str>,
        statement: &str,
    ) -> sqe_core::Result<()> {
        let catalog = crate::ident::catalog_qualifier(table)
            .unwrap_or_else(|| self.session_catalog_name(session));
        self.require_defined_tags_in(session, catalog, names, statement)
            .await
    }

    /// [`Self::require_defined_tags`] for an object in `catalog`: a table,
    /// or a schema (#922).
    pub(super) async fn require_defined_tags_in(
        &self,
        session: &Session,
        catalog: String,
        names: &BTreeSet<&str>,
        statement: &str,
    ) -> sqe_core::Result<()> {
        if self.config.access_control.tag_names == TagNames::Any || names.is_empty() {
            return Ok(());
        }
        let Some(store) = self.policy_store.as_deref() else {
            return Err(self.refuse_tag_write(
                session,
                statement,
                "cannot check the tag names: no policy backend is configured \
                 (tag_names = \"ranger-tagdefs\" needs [policy] engine = \"ranger\")",
            ));
        };
        let catalog = store.catalog_scoped().then_some(catalog);
        let undefined = |defs: &HashSet<String>| -> Vec<&str> {
            names
                .iter()
                .copied()
                .filter(|name| {
                    !defs.contains(*name)
                        && !catalog
                            .as_ref()
                            .is_some_and(|c| defs.contains(&format!("{c}.{name}")))
                })
                .collect()
        };
        let mut missing = Vec::new();
        for fresh in [false, true] {
            let defs = store.tag_definitions(fresh).await.map_err(|e| {
                self.refuse_tag_write(
                    session,
                    statement,
                    &format!("cannot check the tag names: {e}"),
                )
            })?;
            missing = undefined(&defs);
            if missing.is_empty() {
                return Ok(());
            }
        }
        let list = missing
            .iter()
            .map(|n| format!("'{n}'"))
            .collect::<Vec<_>>()
            .join(", ");
        warn!(
            username = %session.user.username,
            statement,
            tags = %list,
            "denied: undefined tag name"
        );
        Err(SqeError::with_code(
            sqe_core::SqeErrorCode::InvalidArguments,
            format!(
                "{statement}: tag {list} is not defined in Ranger. Check the spelling, \
                 or ask an admin to define the tag first"
            ),
        ))
    }

    /// The policy key `(namespace, table)` of the table a tag write of `table`
    /// commits to, and its qualified name: the catalog the write resolves (the
    /// session's for a bare name), keyed by the store's `catalog_scoped` rule
    /// as enforcement keys it. `None` when the table cannot be keyed.
    fn tag_policy_key(
        &self,
        session: &Session,
        store: &dyn PolicyStore,
        table: &sqlparser::ast::ObjectName,
    ) -> sqe_core::Result<Option<(String, String, String)>> {
        let table_ident = crate::ident::resolve_table_ident(table, session)?;
        let catalog_name = crate::ident::catalog_qualifier(table)
            .unwrap_or_else(|| self.session_catalog_name(session));
        let default_hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
        let hive_catalogs = self.hive_catalogs.as_deref().unwrap_or(&default_hive);
        let key = sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
            Some(&catalog_name),
            table_ident.namespace().as_ref(),
            table_ident.name(),
            hive_catalogs,
            store.catalog_scoped(),
        );
        let display = format!("{catalog_name}.{table_ident}");
        Ok(key.map(|(namespace, table)| (namespace, table, display)))
    }

    /// Under `mask_precedence = refuse`, the check a tag write of `table` runs
    /// under its table lock: it refuses a change that creates a mask or row
    /// filter conflict for a principal the policies name (#918). `None` in
    /// the other modes and without a policy store.
    pub(super) fn tag_conflict_guard(
        &self,
        session: &Session,
        table: &sqlparser::ast::ObjectName,
    ) -> sqe_core::Result<Option<TagConflictGuard>> {
        if self.config.policy.mask_precedence != MaskPrecedence::Refuse {
            return Ok(None);
        }
        let Some(store) = self.policy_store.clone() else {
            return Ok(None);
        };
        let (namespace, table_name, display) = self
            .tag_policy_key(session, store.as_ref(), table)?
            .ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::AccessDenied,
                    format!("cannot key table '{table}' for a policy conflict check"),
                )
            })?;
        let catalog = crate::ident::catalog_qualifier(table)
            .unwrap_or_else(|| self.session_catalog_name(session));
        Ok(Some(
            TagConflictGuard::new(store, namespace, table_name, display).with_catalog(catalog),
        ))
    }

    /// [`Self::tag_conflict_guard`] for SET/UNSET TAGS and the masking-policy
    /// statements, which reach the same write.
    pub(super) fn tag_statement_conflict_guard(
        &self,
        session: &Session,
        stmt: &sqe_sql::tags::SetTagsStatement,
    ) -> sqe_core::Result<Option<TagConflictGuard>> {
        let table = crate::catalog_ops::parse_object_name(&stmt.table)?;
        self.tag_conflict_guard(session, &table)
    }

    /// [`Self::tag_conflict_guard`] for a raw property statement: only one
    /// that writes `sqe.column-tags` to an existing table gets the check.
    pub(super) fn tag_property_conflict_guard(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Option<TagConflictGuard>> {
        match stmt {
            Statement::AlterTable(at) if tag_property_write(stmt)?.is_some() => {
                self.tag_conflict_guard(session, &at.name)
            }
            _ => Ok(None),
        }
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
        let (namespace, table_name, _) =
            self.tag_policy_key(session, store, table)?.ok_or_else(|| {
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
        // The table's own tags (#922) have no raw writer: in every mode only
        // an admin may write `sqe.table-tags` as a property.
        if !self.config.auth.has_admin_role(&session.user.roles)
            && crate::tag_source_impl::writes_table_property(
                stmt,
                sqe_core::object_tags::TABLE_PROP_KEY,
            )?
        {
            return Err(self.refuse_tag_write(
                session,
                "setting the sqe.table-tags table property",
                "is reserved for admins; use ALTER TABLE ... SET TAGS ('key' = 'value')",
            ));
        }
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
        let Some(write) = tag_property_write(stmt)? else {
            return Ok(None);
        };
        let Some(store) = self.tag_delegate_store() else {
            // `admin-role`: the static gate let a non-admin through only for a
            // plain CREATE TABLE, a new table, so every name it writes is new.
            if self.config.access_control.tag_names != TagNames::Any {
                let new = parse_tag_value(write.value.as_deref())
                    .map_err(|why| self.refuse_tag_write(session, RAW_TAG_WRITE, &why))?;
                if let Some(table) = written_table(stmt) {
                    let names = added_tag_names(&HashMap::new(), &new);
                    self.require_defined_tags(session, table, &names, RAW_TAG_WRITE)
                        .await?;
                }
            }
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
        self.require_defined_tags(session, table, &added_tag_names(&old, &new), RAW_TAG_WRITE)
            .await?;
        Ok(Some(changed))
    }
}

/// The table an `ALTER TABLE` or `CREATE TABLE` writes.
fn written_table(stmt: &Statement) -> Option<&sqlparser::ast::ObjectName> {
    match stmt {
        Statement::AlterTable(at) => Some(&at.name),
        Statement::CreateTable(ct) => Some(&ct.name),
        _ => None,
    }
}

/// The tag keys `new` gives a column that `old` did not: the names a raw
/// `sqe.column-tags` write adds. A tag the column already carries is not
/// re-checked, so a steward can edit a table that holds a tag written before
/// `tag_names` was turned on.
fn added_tag_names<'a>(old: &ColumnTagMap, new: &'a ColumnTagMap) -> BTreeSet<&'a str> {
    new.iter()
        .flat_map(|(column, tags)| {
            let had: HashSet<&str> = old
                .get(column)
                .map(|t| t.iter().map(|t| t.key.as_str()).collect())
                .unwrap_or_default();
            tags.iter()
                .map(|t| t.key.as_str())
                .filter(move |key| !had.contains(key))
        })
        .collect()
}

/// The tag map a raw property write sets, or why it is refused.
fn parse_tag_value(value: Option<&str>) -> Result<ColumnTagMap, String> {
    let raw = value.ok_or_else(|| "names no sqe.column-tags value".to_string())?;
    sqe_core::column_tags::parse(raw)
        .map_err(|e| format!("sqe.column-tags must be a JSON object of column to tag list ({e})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::column_tags::ColumnTag;

    /// `(tag key, value)` pairs of one column.
    type Tags<'a> = &'a [(&'a str, Option<&'a str>)];

    fn map(entries: &[(&str, Tags<'_>)]) -> ColumnTagMap {
        entries
            .iter()
            .map(|(c, tags)| {
                let tags = tags
                    .iter()
                    .map(|(k, v)| match v {
                        Some(v) => ColumnTag::with_value(*k, *v),
                        None => ColumnTag::key(*k),
                    })
                    .collect();
                (c.to_string(), tags)
            })
            .collect()
    }

    #[test]
    fn added_tag_names_are_the_keys_a_column_gains() {
        let old = map(&[("ssn", &[("legacy.typo", None), ("PII", Some("a"))])]);
        let new = map(&[
            // Kept tags (a changed value included) are not re-checked.
            (
                "ssn",
                &[
                    ("legacy.typo", None),
                    ("PII", Some("b")),
                    ("live.pii", None),
                ],
            ),
            // A tag another column already had is new on this one.
            ("email", &[("PII", None)]),
        ]);
        let got: Vec<&str> = added_tag_names(&old, &new).into_iter().collect();
        assert_eq!(got, ["PII", "live.pii"]);
        // Removing tags adds nothing.
        assert!(added_tag_names(&old, &HashMap::new()).is_empty());
        // A new table adds every name.
        let got: Vec<&str> = added_tag_names(&HashMap::new(), &old).into_iter().collect();
        assert_eq!(got, ["PII", "legacy.typo"]);
    }
}

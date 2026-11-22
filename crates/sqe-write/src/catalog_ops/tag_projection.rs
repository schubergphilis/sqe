//! Keeping Ranger's tag store in step with a table's `sqe.column-tags`:
//! the projection helpers table DDL calls (#928), the create and
//! procedure-drop hooks the query handler calls, and
//! `CALL system.reproject_column_tags` (#421).

use std::collections::HashMap;

use iceberg::TableIdent;
use sqlparser::ast::ObjectName;
use tracing::instrument;

use sqe_catalog::SessionCatalog;
use sqe_core::column_tags::{self, ColumnTagMap};
use sqe_core::{Session, SqeError, SqeErrorCode};
use sqe_sql::{ReprojectScope, TableRef};

use super::{
    catalog_qualifier, nested_namespace, resolve_table_ident, table_ref_to_ident, CatalogOps,
};

/// `column -> [tag keys]`: what Ranger's tag store holds for a table.
pub(super) type TagKeys = HashMap<String, Vec<String>>;

/// Everything Ranger holds for one table: its columns' tag keys and the keys
/// of the tags on the table itself (#935).
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct DdlTags {
    pub(super) columns: TagKeys,
    pub(super) table: Vec<String>,
}

/// The keys of the tags on `table` itself (`sqe.table-tags`), as projected
/// on its table-level Ranger resource (#935).
pub(crate) fn table_tag_keys(
    table: &iceberg::table::Table,
) -> Result<Vec<String>, serde_json::Error> {
    let raw = table
        .metadata()
        .properties()
        .get(sqe_core::object_tags::TABLE_PROP_KEY);
    let tags = sqe_core::object_tags::parse(raw.map(String::as_str))?;
    Ok(tags.into_iter().map(|t| t.key).collect())
}

/// The tags of a table a procedure is about to drop, with its table lock
/// held until they are cleared (#928).
pub struct ProcedureDropTags {
    catalog: String,
    ident: TableIdent,
    tags: DdlTags,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// Each column's tag keys in `a` or in `b`. Ranger holds this while a change
/// from `a` to `b` is in flight, so other engines mask at least what either
/// side masks (#928).
pub(super) fn union_tag_keys(a: &TagKeys, b: &TagKeys) -> TagKeys {
    let mut all = a.clone();
    for (column, keys) in b {
        let held = all.entry(column.clone()).or_default();
        for key in keys {
            if !held.contains(key) {
                held.push(key.clone());
            }
        }
    }
    all
}

impl CatalogOps {
    /// Project `ident`'s tags (`previous` is the map before the change) under
    /// the Ranger key a mask on it uses. `catalog` is the planner's name for
    /// the table's catalog ([`Self::planner_catalog`]), which a
    /// `catalog_scoped` key carries (#899).
    pub(super) async fn project_tags(
        &self,
        catalog: &str,
        ident: &TableIdent,
        previous: &ColumnTagMap,
        tags: &ColumnTagMap,
    ) -> sqe_core::Result<()> {
        // Ranger's tag store gets the keys only; tag values stay in Iceberg.
        let (prev, tags) = (column_tags::keys(previous), column_tags::keys(tags));
        self.project_tag_keys(catalog, ident, &prev, &tags).await
    }

    /// [`Self::project_tags`] for maps of tag keys. Afterwards the load
    /// observer has forgotten `ident`, so its next load projects the table
    /// afresh rather than skip it on a stale memory (#928).
    pub(super) async fn project_tag_keys(
        &self,
        catalog: &str,
        ident: &TableIdent,
        previous: &TagKeys,
        tags: &TagKeys,
    ) -> sqe_core::Result<()> {
        use sqe_policy::tag_projector::{project_table_tags, ColumnTags};
        let to_map = |m: &TagKeys| -> ColumnTags { m.clone().into_iter().collect() };
        let (ns, name) = (ident.namespace().as_ref(), ident.name());
        let (scoped, prev, tags) = (self.catalog_scoped(), to_map(previous), to_map(tags));
        let projector = self.tag_projector.as_ref();
        let done = project_table_tags(projector, catalog, ns, name, scoped, &prev, &tags).await;
        self.tag_load_memory.forget(ident);
        done
    }

    /// Project the tags on `ident` itself onto its table-level Ranger
    /// resource (#935): `previous` deleted, then `tags` added. Like
    /// [`Self::project_tag_keys`] it makes the load observer forget `ident`.
    pub(super) async fn project_table_level_keys(
        &self,
        catalog: &str,
        ident: &TableIdent,
        previous: &[String],
        tags: &[String],
    ) -> sqe_core::Result<()> {
        use sqe_policy::tag_projector::project_table_level_tags;
        let (ns, name, scoped) = (
            ident.namespace().as_ref(),
            ident.name(),
            self.catalog_scoped(),
        );
        let projector = self.tag_projector.as_ref();
        let run = project_table_level_tags(projector, catalog, ns, name, scoped, previous, tags);
        let done = run.await;
        self.tag_load_memory.forget(ident);
        done
    }

    /// Project `tags` (columns and table level) onto `ident`, which has none
    /// in Ranger yet: the destination of a rename (#928, #935).
    pub(super) async fn project_ddl_tags(
        &self,
        catalog: &str,
        ident: &TableIdent,
        tags: &DdlTags,
    ) -> sqe_core::Result<()> {
        if !tags.columns.is_empty() {
            let none = TagKeys::new();
            self.project_tag_keys(catalog, ident, &none, &tags.columns)
                .await?;
        }
        if !tags.table.is_empty() {
            self.project_table_level_keys(catalog, ident, &[], &tags.table)
                .await?;
        }
        Ok(())
    }

    /// What Ranger holds for `ident` before it is dropped or renamed (#928,
    /// #935), or `None` when projection is off, the table cannot be loaded
    /// (a drop with `IF EXISTS` of a missing table) or it has no tags. A
    /// malformed tag value contributes nothing, as on load: it was never
    /// projected, so there is nothing of it to move or clear.
    pub(super) async fn tag_keys_before_ddl(
        &self,
        session_catalog: &SessionCatalog,
        ident: &TableIdent,
    ) -> Option<DdlTags> {
        if !self.tag_projector.enabled() {
            return None;
        }
        let table = session_catalog.load_table(ident).await.ok()?;
        let columns = crate::tag_source_impl::table_column_tags(&table).unwrap_or_else(|e| {
            tracing::warn!(table = %ident, error = %e, "malformed sqe.column-tags; not projected");
            TagKeys::new()
        });
        let table_level = table_tag_keys(&table).unwrap_or_else(|e| {
            tracing::error!(
                table = %ident,
                error = %e,
                "malformed sqe.table-tags; its table-level Ranger resource is not moved or \
                 cleared"
            );
            Vec::new()
        });
        let tags = DdlTags {
            columns,
            table: table_level,
        };
        (tags != DdlTags::default()).then_some(tags)
    }

    /// Clear the Ranger tags of a table that is gone: dropped, or renamed
    /// away (#928). A failure is a warning, not an error: the statement
    /// already happened, and the resource left behind only over-masks, in
    /// other engines, a table created later under this name.
    pub(super) async fn clear_tags_of_gone_table(
        &self,
        catalog: &str,
        ident: &TableIdent,
        tags: &DdlTags,
    ) {
        let columns = async {
            if tags.columns.is_empty() {
                return Ok(());
            }
            let none = TagKeys::new();
            self.project_tag_keys(catalog, ident, &tags.columns, &none)
                .await
        };
        let table_level = async {
            if tags.table.is_empty() {
                return Ok(());
            }
            self.project_table_level_keys(catalog, ident, &tags.table, &[])
                .await
        };
        if let Err(e) = columns.await.and(table_level.await) {
            tracing::warn!(
                table = %ident,
                error = %e,
                "the table is gone but clearing its tags in Ranger failed; other \
                 engines mask a table created later under this name with the old tags"
            );
        }
    }

    /// Read the tags of the table `CALL system.drop_table` (or `DROP TABLE
    /// ... PURGE`, which runs it) is about to drop, and hold its table lock
    /// until [`Self::clear_tags_after_procedure_drop`] (#928). `None` when
    /// projection is off or the table has no tags to clear. The table is
    /// resolved as the procedure resolves it.
    pub async fn tags_before_procedure_drop(
        &self,
        session: &Session,
        table: &TableRef,
    ) -> Option<ProcedureDropTags> {
        if !self.tag_projector.enabled() {
            return None;
        }
        let ident = crate::procedures::to_table_ident(table);
        let session_default = session.default_catalog.as_deref().filter(|c| !c.is_empty());
        let target = table.catalog.as_deref().or(session_default);
        let session_catalog = match self.session_catalog_for(session, target).await {
            Ok(catalog) => catalog,
            Err(e) => {
                tracing::warn!(table = %ident, error = %e, "no catalog to read dropped tags from");
                return None;
            }
        };
        let _ = session_catalog.list_namespaces().await;
        let guard = self.table_lock_for(&ident).lock_owned().await;
        let tags = self.tag_keys_before_ddl(&session_catalog, &ident).await?;
        let catalog = self.planner_catalog(session, table.catalog.clone());
        Some(ProcedureDropTags {
            catalog,
            ident,
            tags,
            _guard: guard,
        })
    }

    /// Clear the Ranger tags read by [`Self::tags_before_procedure_drop`]
    /// once the table is dropped, then release its table lock.
    pub async fn clear_tags_after_procedure_drop(&self, dropped: ProcedureDropTags) {
        let ProcedureDropTags {
            catalog,
            ident,
            tags,
            ..
        } = &dropped;
        self.clear_tags_of_gone_table(catalog, ident, tags).await;
    }

    /// The tag keys of `table` before a CREATE [OR REPLACE] TABLE or CTAS
    /// writes it, for [`Self::project_created_table_tags`] (#928). `None`
    /// when projection is off; empty when the table does not exist yet or
    /// cannot be read.
    pub async fn tags_before_create(
        &self,
        session: &Session,
        table: &ObjectName,
    ) -> Option<HashMap<String, Vec<String>>> {
        if !self.tag_projector.enabled() {
            return None;
        }
        let before = match self.live_column_tags(session, table).await {
            Ok(Some(Ok(live))) => column_tags::keys(&live),
            _ => TagKeys::new(),
        };
        Some(before)
    }

    /// Project the tags a CREATE [OR REPLACE] TABLE or CTAS committed:
    /// tags written in its properties, or a replaced table's tags trimmed to
    /// the surviving columns (#928). `before` is
    /// [`Self::tags_before_create`]'s answer.
    ///
    /// The table is already committed, so a failed projection is reported
    /// with the repair rather than undone.
    pub async fn project_created_table_tags(
        &self,
        session: &Session,
        table: &ObjectName,
        before: Option<HashMap<String, Vec<String>>>,
    ) -> sqe_core::Result<()> {
        let Some(before) = before else {
            return Ok(());
        };
        let table_ident = resolve_table_ident(table, session)?;
        let table_lock = self.table_lock_for(&table_ident);
        let _guard = table_lock.lock().await;
        let after = match self.live_column_tags(session, table).await {
            Ok(Some(Ok(live))) => column_tags::keys(&live),
            Ok(None) => return Ok(()),
            Ok(Some(Err(e))) => {
                tracing::warn!(table = %table_ident, error = %e, "malformed sqe.column-tags; not projected");
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    table = %table_ident,
                    error = %e,
                    "could not read the created table's column tags back; the next load \
                     projects them"
                );
                return Ok(());
            }
        };
        if after == before {
            return Ok(());
        }
        let planner = self.planner_catalog(session, catalog_qualifier(table));
        let projected = self.project_tag_keys(&planner, &table_ident, &before, &after);
        projected.await.map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!(
                    "'{table_ident}' was created and COMMITTED, but projecting its column tags \
                     into Ranger failed ({e}). Do not re-run the statement. Until \
                     `CALL system.reproject_column_tags(table => '{table_ident}')` succeeds, \
                     engines that read Ranger's tag store return its tagged columns raw."
                ),
            )
        })
    }

    /// Project existing Iceberg `sqe.column-tags` into Ranger for tables
    /// tagged before the projector existed (issue #421).
    ///
    /// Does not write Iceberg, so there is no SET TAG rollback. One failed
    /// table is reported as a row, not a CALL-wide error. Requires
    /// `project-tags = true`; with it off the CALL fails rather than
    /// silently walking tables.
    #[instrument(skip(self, session), fields(username = %session.user.username))]
    pub async fn reproject_column_tags(
        &self,
        session: &Session,
        scope: &ReprojectScope,
    ) -> sqe_core::Result<Vec<arrow_array::RecordBatch>> {
        if !self.tag_projector.enabled() {
            return Err(SqeError::Execution(
                "CALL system.reproject_column_tags requires [policy.ranger] \
                 project-tags = true; with it off, tags stay in Iceberg only \
                 and other engines cannot see them"
                    .into(),
            ));
        }

        let catalog_name = match scope {
            ReprojectScope::Table(t) => t.catalog.as_deref(),
            ReprojectScope::Namespace(ns) => ns.catalog.as_deref(),
            ReprojectScope::Catalog(c) => Some(c.as_str()),
        };
        let session_catalog = self.session_catalog_for(session, catalog_name).await?;
        let targets = collect_reproject_targets(&session_catalog, session, scope).await?;
        let catalog = self.planner_catalog(session, catalog_name.map(str::to_string));

        let mut tables = Vec::with_capacity(targets.len());
        let mut statuses = Vec::with_capacity(targets.len());
        let mut columns = Vec::with_capacity(targets.len());
        let mut messages = Vec::with_capacity(targets.len());

        for ident in targets {
            let table_lock = self.table_lock_for(&ident);
            let _guard = table_lock.lock().await;
            let row = self
                .reproject_one_table(&session_catalog, &catalog, &ident)
                .await;
            tables.push(row.table);
            statuses.push(row.status);
            columns.push(row.columns_projected);
            messages.push(row.message);
        }

        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("table_identifier", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("status", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("columns_projected", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("message", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(arrow_array::StringArray::from(tables)),
                std::sync::Arc::new(arrow_array::StringArray::from(statuses)),
                std::sync::Arc::new(arrow_array::Int64Array::from(columns)),
                std::sync::Arc::new(arrow_array::StringArray::from(messages)),
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("reproject_column_tags summary: {e}"),
            )
        })?;
        Ok(vec![batch])
    }

    async fn reproject_one_table(
        &self,
        session_catalog: &SessionCatalog,
        catalog: &str,
        ident: &TableIdent,
    ) -> ReprojectRow {
        let table = match session_catalog.load_table(ident).await {
            Ok(t) => t,
            Err(e) => {
                return ReprojectRow::error(ident.to_string(), format!("load_table failed: {e}"));
            }
        };
        let tags = match crate::tag_source_impl::table_column_tags(&table) {
            Ok(t) => t,
            Err(e) => {
                return ReprojectRow::error(
                    ident.to_string(),
                    format!("malformed sqe.column-tags JSON: {e}"),
                );
            }
        };
        let table_tags = match table_tag_keys(&table) {
            Ok(t) => t,
            Err(e) => {
                return ReprojectRow::error(
                    ident.to_string(),
                    format!("malformed sqe.table-tags JSON: {e}"),
                );
            }
        };
        let scoped = self.catalog_scoped();
        let projector = self.tag_projector.as_ref();
        reproject_loaded_tags(projector, catalog, scoped, ident, &tags, &table_tags).await
    }
}

/// One result row of `CALL system.reproject_column_tags`.
struct ReprojectRow {
    table: String,
    status: String,
    columns_projected: i64,
    message: String,
}

impl ReprojectRow {
    fn projected(table: String, columns: i64) -> Self {
        Self {
            table,
            status: "projected".into(),
            columns_projected: columns,
            message: String::new(),
        }
    }

    fn skipped(table: String) -> Self {
        Self {
            table,
            status: "skipped".into(),
            columns_projected: 0,
            message: "no tags".into(),
        }
    }

    fn error(table: String, message: String) -> Self {
        Self {
            table,
            status: "error".into(),
            columns_projected: 0,
            message,
        }
    }
}

/// Project a already-loaded tag map. Isolated so tests can drive the
/// projector without a live catalog.
///
/// `catalog` is the planner's name for the table's catalog; with
/// `catalog_scoped` it leads the key, and the unscoped resource left from
/// before the setting is deleted (#899).
async fn reproject_loaded_tags(
    projector: &dyn sqe_policy::tag_projector::TagProjector,
    catalog: &str,
    catalog_scoped: bool,
    ident: &TableIdent,
    tags: &HashMap<String, Vec<String>>,
    table_tags: &[String],
) -> ReprojectRow {
    use sqe_policy::tag_projector::{project_table_level_tags, project_table_tags, ColumnTags};
    let table = ident.to_string();
    if tags.is_empty() && table_tags.is_empty() {
        return ReprojectRow::skipped(table);
    }
    let projected: ColumnTags = tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let (ns, name, scoped) = (ident.namespace().as_ref(), ident.name(), catalog_scoped);
    // previous is empty: this is a first-time (or idempotent add_or_update)
    // projection of the Iceberg source of truth. Deleting-then-adding the
    // current map would briefly unmask Spark on a repair of a live table.
    let prev = ColumnTags::new();
    if !tags.is_empty() {
        let run = project_table_tags(projector, catalog, ns, name, scoped, &prev, &projected);
        if let Err(e) = run.await {
            return ReprojectRow::error(table, e.to_string());
        }
    }
    // The table's own tags (#935), add-only for the same reason.
    if !table_tags.is_empty() {
        let run = project_table_level_tags(projector, catalog, ns, name, scoped, &[], table_tags);
        if let Err(e) = run.await {
            return ReprojectRow::error(table, format!("table-level tags: {e}"));
        }
    }
    ReprojectRow::projected(table, (tags.len() + table_tags.len()) as i64)
}

async fn collect_reproject_targets(
    session_catalog: &SessionCatalog,
    session: &Session,
    scope: &ReprojectScope,
) -> sqe_core::Result<Vec<TableIdent>> {
    match scope {
        ReprojectScope::Table(table) => Ok(vec![table_ref_to_ident(table, session)]),
        ReprojectScope::Namespace(ns) => {
            let ns_ident = nested_namespace(ns.namespace_parts());
            session_catalog.list_tables(&ns_ident).await
        }
        ReprojectScope::Catalog(_) => {
            let namespaces = session_catalog.list_namespaces().await?;
            let mut out = Vec::new();
            for ns in namespaces {
                match session_catalog.list_tables(&ns).await {
                    Ok(mut tables) => out.append(&mut tables),
                    Err(e) => {
                        // A 403 on one namespace must not abort a catalog-wide
                        // repair; skip it the same way SHOW TABLES hides denied
                        // names.
                        tracing::warn!(
                            namespace = %ns,
                            error = %e,
                            "reproject_column_tags: skipping namespace"
                        );
                    }
                }
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::NamespaceIdent;

    struct RecordingProjector {
        calls: std::sync::Mutex<Vec<(String, String, usize)>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl sqe_policy::tag_projector::TagProjector for RecordingProjector {
        async fn project(
            &self,
            table: &sqe_policy::tag_projector::TagTableKey,
            _previous: &sqe_policy::tag_projector::ColumnTags,
            tags: &sqe_policy::tag_projector::ColumnTags,
        ) -> sqe_core::Result<()> {
            if self.fail {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "ranger down",
                ));
            }
            self.calls.lock().unwrap().push((
                table.database.clone(),
                table.table.clone(),
                tags.len(),
            ));
            Ok(())
        }

        /// Recorded as table `<name>#table`, so a test tells the two apart.
        async fn project_table_level(
            &self,
            table: &sqe_policy::tag_projector::TagTableKey,
            _previous: &[String],
            tags: &[String],
        ) -> sqe_core::Result<()> {
            if self.fail {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "ranger down",
                ));
            }
            self.calls.lock().unwrap().push((
                table.database.clone(),
                format!("{}#table", table.table),
                tags.len(),
            ));
            Ok(())
        }

        fn enabled(&self) -> bool {
            true
        }
    }

    fn ident(ns: &str, name: &str) -> TableIdent {
        TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string())
    }

    #[tokio::test]
    async fn reproject_loaded_tags_skips_empty_map() {
        let projector = RecordingProjector {
            calls: std::sync::Mutex::new(Vec::new()),
            fail: false,
        };
        let (none, orders) = (HashMap::new(), ident("sales", "orders"));
        let row = reproject_loaded_tags(&projector, "a", true, &orders, &none, &[]).await;
        assert_eq!(row.status, "skipped");
        assert_eq!(row.columns_projected, 0);
        assert!(projector.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reproject_loaded_tags_projects_and_reports_error() {
        let ok = RecordingProjector {
            calls: std::sync::Mutex::new(Vec::new()),
            fail: false,
        };
        let mut tags = HashMap::new();
        tags.insert("ssn".into(), vec!["PII".into()]);
        tags.insert("email".into(), vec!["PII".into(), "GDPR".into()]);
        let orders = ident("sales", "orders");
        let row = reproject_loaded_tags(&ok, "a", false, &orders, &tags, &[]).await;
        assert_eq!(row.status, "projected");
        assert_eq!(row.columns_projected, 2);
        assert_eq!(
            ok.calls.lock().unwrap().as_slice(),
            &[("sales".into(), "orders".into(), 2)]
        );

        // #899: catalog-scoped, the catalog leads the key and the unscoped
        // resource from before the setting is cleared (no tags added).
        ok.calls.lock().unwrap().clear();
        let row = reproject_loaded_tags(&ok, "a", true, &orders, &tags, &[]).await;
        assert_eq!(row.status, "projected");
        let scoped = ("a.sales".into(), "orders".into(), 2);
        let cleared = ("sales".into(), "orders".into(), 0);
        assert_eq!(ok.calls.lock().unwrap().as_slice(), &[scoped, cleared]);

        let fail = RecordingProjector {
            calls: std::sync::Mutex::new(Vec::new()),
            fail: true,
        };
        let row = reproject_loaded_tags(&fail, "a", false, &orders, &tags, &[]).await;
        assert_eq!(row.status, "error");
        assert!(row.message.contains("ranger down"), "got: {}", row.message);
        assert_eq!(row.columns_projected, 0);
    }

    /// A table with only table tags is projected, onto the table-level
    /// resource and add-only (#935).
    #[tokio::test]
    async fn reproject_loaded_tags_projects_table_level_tags() {
        let ok = RecordingProjector {
            calls: std::sync::Mutex::new(Vec::new()),
            fail: false,
        };
        let (none, orders) = (HashMap::new(), ident("sales", "orders"));
        let table_tags = vec!["pii".to_string()];
        let row = reproject_loaded_tags(&ok, "a", false, &orders, &none, &table_tags).await;
        assert_eq!(row.status, "projected");
        assert_eq!(row.columns_projected, 1);
        assert_eq!(
            ok.calls.lock().unwrap().as_slice(),
            &[("sales".into(), "orders#table".into(), 1)]
        );

        let fail = RecordingProjector {
            calls: std::sync::Mutex::new(Vec::new()),
            fail: true,
        };
        let row = reproject_loaded_tags(&fail, "a", false, &orders, &none, &table_tags).await;
        assert_eq!(row.status, "error");
        assert!(row.message.contains("table-level"), "got: {}", row.message);
    }

    /// Ranger holds the union while a tag change is in flight (#928).
    #[test]
    fn union_tag_keys_keeps_every_key_of_both_sides_once() {
        let keys = |pairs: &[(&str, &[&str])]| -> TagKeys {
            let to_vec = |ks: &[&str]| ks.iter().map(ToString::to_string).collect();
            pairs
                .iter()
                .map(|(c, ks)| (c.to_string(), to_vec(ks)))
                .collect()
        };
        let before = keys(&[("ssn", &["pii"]), ("email", &["pii"])]);
        let after = keys(&[("ssn", &["pii", "gdpr"]), ("phone", &["pii"])]);
        let union = union_tag_keys(&before, &after);
        let want = keys(&[
            ("ssn", &["pii", "gdpr"]),
            ("email", &["pii"]),
            ("phone", &["pii"]),
        ]);
        assert_eq!(union, want);
        assert_eq!(union_tag_keys(&before, &TagKeys::new()), before);
    }
}

//! Lineage for DDL: the session scope that names unqualified tables, the
//! `TableLifecycle` / `ViewLifecycle` / `TableRename` hints the coordinator
//! records for CREATE TABLE, DROP TABLE, DROP VIEW and a rename, and the
//! `CreateView` plan it records for CREATE VIEW.

use datafusion::common::TableReference;
use datafusion::logical_expr::{CreateView, DdlStatement, LogicalPlan};
use datafusion::sql::sqlparser::ast::ObjectName;
use sqe_core::{Session, SqeConfig};
use std::sync::Arc;

fn table_reference_of(name: &ObjectName) -> TableReference {
    TableReference::from(name.to_string())
}

/// The catalog and schema a session resolves unqualified and 2-part table
/// names in, as `session_context` hands them to DataFusion
/// (`with_default_catalog_and_schema`): the session's catalog
/// (`X-Trino-Catalog`, `USE`) or the configured default, and the session's
/// schema or `default`. Lineage names a scan of `customers` after it.
pub fn lineage_scope(config: &SqeConfig, session: &Session) -> sqe_lineage::LineageScope {
    sqe_lineage::LineageScope {
        catalog: session
            .default_catalog
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| config.resolve_default_catalog()),
        schema: session
            .default_schema
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "default".to_string()),
    }
}

/// `(catalog, schema, table)` of a DDL target, unqualified parts filled from
/// [`lineage_scope`].
pub fn lineage_table(
    config: &SqeConfig,
    session: &Session,
    name: &ObjectName,
) -> (String, String, String) {
    let scope = lineage_scope(config, session);
    let table_ref = table_reference_of(name);
    (
        table_ref
            .catalog()
            .map(str::to_string)
            .unwrap_or(scope.catalog),
        table_ref
            .schema()
            .map(str::to_string)
            .unwrap_or(scope.schema),
        table_ref.table().to_string(),
    )
}

/// Lineage hint for a DDL statement that changes a table's lifecycle without
/// a source plan: the first table of `DROP TABLE` (the one `drop_table`
/// acts on) and the target of a plain `CREATE TABLE`.
pub fn lineage_lifecycle_hint(
    config: &SqeConfig,
    session: &Session,
    name: &ObjectName,
    change: &str,
) -> sqe_lineage::PlanOrHint {
    let (catalog, schema, table) = lineage_table(config, session, name);
    sqe_lineage::PlanOrHint::Hint(sqe_lineage::LineageHint::TableLifecycle {
        catalog,
        schema,
        table,
        change: change.to_string(),
    })
}

/// [`lineage_lifecycle_hint`] for a view (`DROP VIEW`): typed `VIEW`.
pub fn lineage_view_lifecycle_hint(
    config: &SqeConfig,
    session: &Session,
    name: &ObjectName,
    change: &str,
) -> sqe_lineage::PlanOrHint {
    let (catalog, schema, view) = lineage_table(config, session, name);
    sqe_lineage::PlanOrHint::Hint(sqe_lineage::LineageHint::ViewLifecycle {
        catalog,
        schema,
        view,
        change: change.to_string(),
    })
}

/// CREATE VIEW lineage: the planned body under a DataFusion `CreateView`
/// named after the view, from which the extractor reads the view's inputs,
/// schema and column lineage.
pub fn lineage_view_plan(
    config: &SqeConfig,
    session: &Session,
    name: &ObjectName,
    body: LogicalPlan,
    or_replace: bool,
) -> sqe_lineage::PlanOrHint {
    let (catalog, schema, view) = lineage_table(config, session, name);
    let create = CreateView {
        name: TableReference::full(catalog, schema, view),
        input: Arc::new(body),
        or_replace,
        definition: None,
        temporary: false,
    };
    sqe_lineage::PlanOrHint::Plan(Box::new(LogicalPlan::Ddl(DdlStatement::CreateView(create))))
}

/// Rename hint: new name, old one as `previousIdentifier`; a bare new name keeps the schema.
pub fn lineage_rename_hint(
    c: &SqeConfig,
    s: &Session,
    old: &ObjectName,
    new: &ObjectName,
) -> Option<sqe_lineage::PlanOrHint> {
    let (catalog, previous_schema, previous_table) = lineage_table(c, s, old);
    let (_, schema, table) = lineage_table(c, s, new);
    let schema = if new.0.len() > 1 {
        schema
    } else {
        previous_schema.clone()
    };
    let hint = sqe_lineage::LineageHint::TableRename {
        catalog,
        schema,
        table,
        previous_schema,
        previous_table,
    };
    Some(sqe_lineage::PlanOrHint::Hint(hint))
}

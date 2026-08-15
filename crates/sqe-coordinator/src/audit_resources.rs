//! Resolve fully-qualified `catalog.namespace.object` resources from a
//! DataFusion `LogicalPlan`, producing typed `Resource` values for audit logs.
//!
//! # Design
//!
//! The function walks the plan tree using `TreeNode::apply` (the same approach
//! the policy rewriter uses in `sqe-policy/src/plan_rewriter.rs`) and collects
//! every `TableScan`, `Dml`, and DDL node that names a relation.
//!
//! ## View detection
//!
//! DataFusion 54 inlines views into their base `TableScan` nodes at `ctx.sql`
//! planning time, so by the time this function sees the plan, un-inlined
//! `ViewTable` providers are rare (the rewriter denies them fail-closed). The
//! practical source of `ObjectType::View` in a normal audit path is a
//! `Ddl::CreateView` node. As a defence-in-depth measure, `TableScan` nodes
//! whose provider downcasts to `datafusion::datasource::ViewTable` are also
//! tagged `View`, but this path will almost never fire on a governed query.
//!
//! Limitation: tables accessed through a fully-inlined view appear as `Table`
//! (the base table), because the view boundary is erased by the planner before
//! this function is called. This matches the policy-rewriter's observation at
//! `plan_rewriter.rs:93-100` and is acceptable for the audit trail: the real
//! access is to the base table, not the view name.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{DdlStatement, DmlStatement, LogicalPlan, TableScan};
use sqe_metrics::audit::{ObjectType, Resource};
use std::collections::BTreeSet;

/// Walk `plan` and return one `Resource` per distinct relation (table or view)
/// referenced by the plan, deduplicated by fully-qualified name.
///
/// `default_catalog` is substituted for any `Bare` or `Partial` reference that
/// has no explicit catalog component.
///
/// # Object type detection
///
/// - `Ddl::CreateView` -> `ObjectType::View`
/// - `TableScan` whose provider is still a `ViewTable` -> `ObjectType::View`
///   (rare after DF54 view inlining; see module doc)
/// - Everything else -> `ObjectType::Table`
pub fn resources_from_plan(plan: &LogicalPlan, default_catalog: Option<&str>) -> Vec<Resource> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut resources: Vec<Resource> = Vec::new();

    let _ = plan.apply(|node| {
        let maybe = match node {
            LogicalPlan::TableScan(TableScan {
                table_name, source, ..
            }) => {
                // Check whether this scan's provider is an un-inlined ViewTable.
                // After DF54 this is rare; governed views are inlined to their
                // base scans. Un-inlined ViewTable scans are typically fail-closed
                // denied by the policy rewriter, but we tag them View here for
                // completeness.
                let object_type =
                    if let Ok(provider) = datafusion::datasource::source_as_provider(source) {
                        let any_ref: &dyn std::any::Any = provider.as_ref();
                        if any_ref
                            .downcast_ref::<datafusion::datasource::ViewTable>()
                            .is_some()
                        {
                            ObjectType::View
                        } else {
                            ObjectType::Table
                        }
                    } else {
                        ObjectType::Table
                    };
                Some((table_name.clone(), object_type))
            }
            LogicalPlan::Dml(DmlStatement { table_name, .. }) => {
                // INSERT INTO / UPDATE / DELETE: the target is always a Table.
                Some((table_name.clone(), ObjectType::Table))
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(ct)) => {
                Some((ct.name.clone(), ObjectType::Table))
            }
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(ct)) => {
                Some((ct.name.clone(), ObjectType::Table))
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(cv)) => {
                // The plan node explicitly names a view being created.
                Some((cv.name.clone(), ObjectType::View))
            }
            _ => None,
        };

        if let Some((table_ref, object_type)) = maybe {
            let resource = table_ref_to_resource(table_ref, object_type, default_catalog);
            let fqn = resource.fqn();
            if seen.insert(fqn) {
                resources.push(resource);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });

    resources
}

/// Convert a DataFusion `TableReference` into a `Resource`.
///
/// `TableReference::Full` carries catalog + schema + table directly.
/// `TableReference::Partial` carries schema + table; `default_catalog` fills the catalog.
/// `TableReference::Bare` carries only the table name.
fn table_ref_to_resource(
    table_ref: datafusion::common::TableReference,
    object_type: ObjectType,
    default_catalog: Option<&str>,
) -> Resource {
    use datafusion::common::TableReference;

    match table_ref {
        TableReference::Full {
            catalog,
            schema,
            table,
        } => Resource {
            catalog: Some(catalog.to_string()),
            namespace: vec![schema.to_string()],
            name: table.to_string(),
            object_type,
        },
        TableReference::Partial { schema, table } => Resource {
            catalog: default_catalog.map(str::to_owned),
            namespace: vec![schema.to_string()],
            name: table.to_string(),
            object_type,
        },
        TableReference::Bare { table } => Resource {
            catalog: default_catalog.map(str::to_owned),
            namespace: vec![],
            name: table.to_string(),
            object_type,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::logical_expr::builder::table_scan;
    use sqe_metrics::audit::ObjectType;

    fn simple_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, false)])
    }

    fn plan_for_ref(table_ref: TableReference) -> LogicalPlan {
        table_scan(Some(table_ref), &simple_schema(), None)
            .unwrap()
            .build()
            .unwrap()
    }

    // --- TDD RED step: write failing test first ---

    #[test]
    fn full_reference_resolves_catalog_namespace_name() {
        let plan = plan_for_ref(TableReference::full("catalog", "ns", "table"));
        let resources = resources_from_plan(&plan, None);
        assert_eq!(resources.len(), 1);
        let r = &resources[0];
        assert_eq!(r.catalog, Some("catalog".to_string()));
        assert_eq!(r.namespace, vec!["ns".to_string()]);
        assert_eq!(r.name, "table");
        assert_eq!(r.object_type, ObjectType::Table);
    }

    #[test]
    fn partial_reference_uses_default_catalog() {
        let plan = plan_for_ref(TableReference::partial("hr", "employees"));
        let resources = resources_from_plan(&plan, Some("polaris"));
        assert_eq!(resources.len(), 1);
        let r = &resources[0];
        assert_eq!(r.catalog, Some("polaris".to_string()));
        assert_eq!(r.namespace, vec!["hr".to_string()]);
        assert_eq!(r.name, "employees");
        assert_eq!(r.object_type, ObjectType::Table);
    }

    #[test]
    fn bare_reference_no_default_catalog() {
        let plan = plan_for_ref(TableReference::bare("mytable"));
        let resources = resources_from_plan(&plan, None);
        assert_eq!(resources.len(), 1);
        let r = &resources[0];
        assert_eq!(r.catalog, None);
        assert_eq!(r.namespace, Vec::<String>::new());
        assert_eq!(r.name, "mytable");
        assert_eq!(r.object_type, ObjectType::Table);
    }

    #[test]
    fn bare_reference_with_default_catalog() {
        let plan = plan_for_ref(TableReference::bare("mytable"));
        let resources = resources_from_plan(&plan, Some("polaris"));
        assert_eq!(resources.len(), 1);
        let r = &resources[0];
        assert_eq!(r.catalog, Some("polaris".to_string()));
        assert_eq!(r.namespace, Vec::<String>::new());
        assert_eq!(r.name, "mytable");
    }

    #[test]
    fn dedup_same_table_scanned_twice() {
        // UNION ALL of a table with itself creates two identical TableScan nodes
        // under a single plan. resources_from_plan must dedup them to one Resource.
        //
        // A cross-join of a table with itself is rejected by DataFusion's schema
        // validator (duplicate qualified fields), but Union accepts identical schemas.
        let left = plan_for_ref(TableReference::full("cat", "ns", "t1"));
        let right = plan_for_ref(TableReference::full("cat", "ns", "t1"));
        let unioned = datafusion::logical_expr::LogicalPlanBuilder::from(left)
            .union(right)
            .unwrap()
            .build()
            .unwrap();
        let resources = resources_from_plan(&unioned, None);
        // Dedup: only one resource expected even though two TableScan nodes exist.
        assert_eq!(resources.len(), 1, "duplicate scans must be deduped");
        assert_eq!(resources[0].fqn(), "cat.ns.t1");
    }

    #[test]
    fn fqn_format_full_reference() {
        let plan = plan_for_ref(TableReference::full("cat", "ns", "tbl"));
        let resources = resources_from_plan(&plan, None);
        assert_eq!(resources[0].fqn(), "cat.ns.tbl");
    }

    #[test]
    fn fqn_format_partial_with_default_catalog() {
        let plan = plan_for_ref(TableReference::partial("ns", "tbl"));
        let resources = resources_from_plan(&plan, Some("cat"));
        assert_eq!(resources[0].fqn(), "cat.ns.tbl");
    }
}

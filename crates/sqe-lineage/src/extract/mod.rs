//! Lineage extraction from DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.
//!
//! Phase E fills in the per-node trace rules. Until then, the entry points
//! return empty input/output lists so the emitter can run end-to-end with
//! real channel + sinks plumbing.

pub mod columns;
pub mod datasets;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{DmlStatement, TableScan};

/// Return the fully-qualified table names this plan reads or writes,
/// deduplicated and sorted. Used for audit-log `tables_touched`.
///
/// Read sources come from `TableScan` nodes; write targets come from
/// `Dml` / `CreateMemoryTable` / `CreateExternalTable` / `CreateView`
/// statements.
pub fn extract_table_names(plan: &LogicalPlan) -> Vec<String> {
    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let _ = plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(TableScan { table_name, .. }) => {
                names.insert(table_name.to_string());
            }
            LogicalPlan::Dml(DmlStatement { table_name, .. }) => {
                names.insert(table_name.to_string());
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(ct)) => {
                names.insert(ct.name.to_string());
            }
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(ct)) => {
                names.insert(ct.name.to_string());
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(cv)) => {
                names.insert(cv.name.to_string());
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
    names.into_iter().collect()
}

use crate::event::{
    ColumnLineageEntry, ColumnLineageFacet, ColumnLineageInput, DataSourceFacet, DatasetFacets,
    InputDataset, OutputDataset, OutputDatasetFacets, SchemaFacet, SchemaField,
};
use crate::observer::LineageHint;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Catalog-name -> namespace-URI lookup, threaded through the emitter so dataset
/// URIs respect SQE's multi-catalog config (spec §4.4).
pub type CatalogLookup = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Extract input + output datasets (with column lineage on outputs) from a
/// DataFusion `LogicalPlan`.
pub fn extract_lineage(
    plan: &LogicalPlan,
    lookup: &CatalogLookup,
) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    let inputs = datasets::extract_inputs(plan, lookup);
    let mut outputs = datasets::extract_outputs(plan, lookup);

    // For DML/DDL writes, attach the projected target schema and column
    // lineage by tracing the source plan. The source plan's output
    // schema IS the target schema (the planner aligns columns by
    // ordinal at plan-build time per spec §5.3), so we can lift fields
    // and types directly from it without consulting the catalog.
    if let Some(out) = outputs.first_mut() {
        if let Some(source_plan) = source_of_write(plan) {
            out.facets.schema = Some(SchemaFacet {
                fields: source_plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| SchemaField {
                        name: f.name().clone(),
                        field_type: f.data_type().to_string(),
                    })
                    .collect(),
            });
            let trace = columns::trace_plan(source_plan);
            let target_fields = output_field_names(source_plan);
            out.facets.columnLineage =
                Some(build_column_lineage_facet(&target_fields, &trace, lookup));
        }
    }

    (inputs, outputs)
}

/// For a write plan, return the source `LogicalPlan` whose columns feed the
/// target. INSERT / CTAS / CREATE VIEW all expose the SELECT subplan via the
/// `input` field; non-write plans return `None`.
fn source_of_write(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Dml(stmt) => Some(stmt.input.as_ref()),
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(c)) => Some(c.input.as_ref()),
        LogicalPlan::Ddl(DdlStatement::CreateView(c)) => Some(c.input.as_ref()),
        _ => None,
    }
}

/// Column names of the source plan in order. By spec §5.3 the planner has
/// already aligned source columns with target schema by position, so the
/// source plan's schema field names are the target column names.
fn output_field_names(source: &LogicalPlan) -> Vec<String> {
    source
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Build the `ColumnLineageFacet` from a positional target-field list and a
/// source-plan column trace, mapping each `ColumnDep` through `lookup` to the
/// OL `(namespace, name)` pair.
fn build_column_lineage_facet(
    fields: &[String],
    trace: &columns::ColumnTrace,
    lookup: &CatalogLookup,
) -> ColumnLineageFacet {
    let mut map: BTreeMap<String, ColumnLineageEntry> = BTreeMap::new();
    for (i, name) in fields.iter().enumerate() {
        let Some(deps) = trace.get(i) else { continue };
        let inputs: Vec<ColumnLineageInput> = deps
            .iter()
            .map(|d| ColumnLineageInput {
                namespace: lookup(&d.catalog),
                name: format!("{}.{}", d.schema, d.table),
                field: d.field.clone(),
                transformations: vec![d.transformation.clone()],
            })
            .collect();
        map.insert(
            name.clone(),
            ColumnLineageEntry {
                inputFields: inputs,
            },
        );
    }
    ColumnLineageFacet { fields: map }
}

/// Extract output dataset from a DDL hint (CREATE TABLE / DROP / ALTER carry
/// no source plan but do have target schema).
pub fn extract_from_hint(
    hint: &LineageHint,
    lookup: &CatalogLookup,
) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    match hint {
        LineageHint::DdlSchema {
            catalog,
            schema,
            table,
            columns,
        } => {
            let namespace = lookup(catalog);
            let schema_facet = SchemaFacet {
                fields: columns
                    .iter()
                    .map(|(name, ty)| SchemaField {
                        name: name.clone(),
                        field_type: ty.clone(),
                    })
                    .collect(),
            };
            let output = OutputDataset {
                namespace: namespace.clone(),
                name: format!("{schema}.{table}"),
                facets: DatasetFacets {
                    schema: Some(schema_facet),
                    dataSource: Some(DataSourceFacet {
                        name: catalog.clone(),
                        uri: namespace,
                    }),
                    columnLineage: None,
                },
                outputFacets: OutputDatasetFacets::default(),
            };
            (vec![], vec![output])
        }
    }
}

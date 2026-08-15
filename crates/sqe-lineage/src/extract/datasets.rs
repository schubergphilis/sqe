//! Dataset extraction from a DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §4.4.

use crate::event::*;
use crate::extract::CatalogLookup;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, TableScan};

/// Parse a (possibly quoted) qualified table name into (catalog, schema, table)
/// tuple, applying SQE's "default catalog" / "default schema" fallbacks per
/// spec §4.4.
fn parse_table_ref(name: &str) -> Option<(String, String, String)> {
    let parts: Vec<String> = name
        .split('.')
        .map(|s| s.trim_matches('"').to_string())
        .collect();
    match parts.len() {
        3 => Some((parts[0].clone(), parts[1].clone(), parts[2].clone())),
        2 => Some(("default".to_string(), parts[0].clone(), parts[1].clone())),
        1 => Some((
            "default".to_string(),
            "default".to_string(),
            parts[0].clone(),
        )),
        _ => None,
    }
}

/// Walk the plan for TableScan nodes and produce one InputDataset per scan.
pub fn extract_inputs(plan: &LogicalPlan, lookup: &CatalogLookup) -> Vec<InputDataset> {
    let mut out = Vec::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan {
            table_name, source, ..
        }) = node
        {
            let Some((catalog, schema, table)) = parse_table_ref(&table_name.to_string()) else {
                return Ok(TreeNodeRecursion::Continue);
            };

            let namespace = lookup(&catalog);
            let schema_facet = SchemaFacet {
                fields: source
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| SchemaField {
                        name: f.name().clone(),
                        field_type: f.data_type().to_string(),
                    })
                    .collect(),
            };

            out.push(InputDataset {
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
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// Walk the plan for write nodes (INSERT, CTAS via CreateMemoryTable, CREATE
/// EXTERNAL TABLE, CREATE VIEW) and produce one OutputDataset per write target.
///
/// The schema facet is populated by the caller (``extract::extract_lineage``)
/// after this function runs — the projected output schema comes from the DML
/// source plan, which this layer doesn't see. We leave ``facets.schema = None``
/// here as a sentinel so the caller's "did this output already get a schema?"
/// check is unambiguous.
pub fn extract_outputs(plan: &LogicalPlan, lookup: &CatalogLookup) -> Vec<OutputDataset> {
    let mut out = Vec::new();
    let _ = plan.apply(|node| {
        let target_name: Option<String> = match node {
            LogicalPlan::Dml(stmt) => Some(stmt.table_name.to_string()),
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(ct)) => Some(ct.name.to_string()),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(ct)) => Some(ct.name.to_string()),
            LogicalPlan::Ddl(DdlStatement::CreateView(cv)) => Some(cv.name.to_string()),
            _ => None,
        };

        if let Some(name) = target_name {
            if let Some((catalog, schema, table)) = parse_table_ref(&name) {
                let namespace = lookup(&catalog);
                out.push(OutputDataset {
                    namespace: namespace.clone(),
                    name: format!("{schema}.{table}"),
                    facets: DatasetFacets {
                        schema: None,
                        dataSource: Some(DataSourceFacet {
                            name: catalog.clone(),
                            uri: namespace,
                        }),
                        columnLineage: None,
                    },
                    outputFacets: OutputDatasetFacets::default(),
                });
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

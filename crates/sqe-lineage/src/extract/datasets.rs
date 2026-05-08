//! Dataset extraction from a DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §4.4.

use crate::event::*;
use crate::extract::CatalogLookup;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{LogicalPlan, TableScan};

/// Walk the plan for TableScan nodes and produce one InputDataset per scan.
pub fn extract_inputs(plan: &LogicalPlan, lookup: &CatalogLookup) -> Vec<InputDataset> {
    let mut out = Vec::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan {
            table_name, source, ..
        }) = node
        {
            let parts: Vec<String> = table_name
                .to_string()
                .split('.')
                .map(|s| s.trim_matches('"').to_string())
                .collect();

            let (catalog, schema, table) = match parts.len() {
                3 => (parts[0].clone(), parts[1].clone(), parts[2].clone()),
                2 => ("default".to_string(), parts[0].clone(), parts[1].clone()),
                1 => (
                    "default".to_string(),
                    "default".to_string(),
                    parts[0].clone(),
                ),
                _ => return Ok(TreeNodeRecursion::Continue),
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
                },
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

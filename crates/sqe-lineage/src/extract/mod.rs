//! Lineage extraction from DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.
//!
//! Phase E fills in the per-node trace rules. Until then, the entry points
//! return empty input/output lists so the emitter can run end-to-end with
//! real channel + sinks plumbing.

pub mod columns;
pub mod datasets;

use crate::event::LineageQualityFacet;

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
    DatasetVersionFacet, InputDataset, LifecycleStateChangeFacet, OutputDataset,
    OutputDatasetFacets, SchemaFacet, SchemaField, SymlinkIdentifier, SymlinksFacet,
};
use crate::observer::{LineageHint, LineageScope};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use std::collections::BTreeMap;
use std::sync::Arc;

/// What lineage needs to know about an SQE catalog name: the Iceberg
/// catalog it is (for a Polaris REST catalog, the Polaris catalog /
/// warehouse name, which a legacy `[catalog]` block registers under the SQL
/// name `iceberg`), and that catalog's REST URI when SQE knows one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogInfo {
    /// Iceberg catalog name; becomes the dataset namespace `sqe://<name>`.
    pub name: String,
    /// Catalog REST URI (e.g. `http://polaris:8181/api/catalog`); becomes the
    /// `dataSource.uri` and the `symlinks` namespace. `None` when unknown.
    pub uri: Option<String>,
}

impl CatalogInfo {
    pub fn new(name: impl Into<String>, uri: Option<String>) -> Self {
        Self {
            name: name.into(),
            uri,
        }
    }
}

/// SQE catalog name -> [`CatalogInfo`] lookup, threaded through the emitter so
/// dataset identities respect SQE's multi-catalog config (spec §4.4).
pub type CatalogLookup = Arc<dyn Fn(&str) -> CatalogInfo + Send + Sync>;

/// The OpenLineage dataset namespace SQE uses for a table in Iceberg catalog
/// `catalog`, for inputs, outputs and column-lineage `inputFields` alike.
pub fn dataset_namespace(catalog: &str) -> String {
    format!("sqe://{catalog}")
}

/// Schema-metadata key a write path sets on its lineage target stub to carry
/// the Iceberg snapshot id it committed; the extractor turns it into the OL
/// `version` dataset facet.
pub const SNAPSHOT_ID_METADATA_KEY: &str = "sqe.lineage.snapshot_id";

/// One table's OL identity: namespace, name, and the identity facets
/// (`dataSource`, `symlinks`).
pub(crate) struct DatasetIdentity {
    pub namespace: String,
    pub name: String,
    pub facets: DatasetFacets,
}

/// The one place a `(catalog, schema, table)` becomes an OL dataset:
/// namespace `sqe://<iceberg catalog>`, name `<schema>.<table>`, `dataSource`
/// naming the catalog and its URI, and (when the URI is known) a `symlinks`
/// TABLE identifier `<catalog uri>` / `<catalog>.<schema>.<table>`.
pub(crate) fn dataset_identity(
    lookup: &CatalogLookup,
    catalog: &str,
    schema: &str,
    table: &str,
) -> DatasetIdentity {
    let info = lookup(catalog);
    let namespace = dataset_namespace(&info.name);
    let symlinks = info.uri.as_ref().map(|uri| SymlinksFacet {
        identifiers: vec![SymlinkIdentifier {
            namespace: uri.clone(),
            name: format!("{}.{schema}.{table}", info.name),
            kind: "TABLE".into(),
        }],
    });
    DatasetIdentity {
        name: format!("{schema}.{table}"),
        facets: DatasetFacets {
            dataSource: Some(DataSourceFacet {
                name: info.name.clone(),
                uri: info.uri.clone().unwrap_or_else(|| namespace.clone()),
            }),
            symlinks,
            ..DatasetFacets::default()
        },
        namespace,
    }
}

/// Extract input + output datasets (with column lineage on outputs) from a
/// DataFusion `LogicalPlan`, resolving unqualified names in the default
/// `default.default` scope.
pub fn extract_lineage(
    plan: &LogicalPlan,
    lookup: &CatalogLookup,
) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    let extracted = extract_lineage_governed(plan, lookup, &[], &LineageScope::default());
    (extracted.inputs, extracted.outputs)
}

/// [`extract_lineage`] with what the policy enforcer did to the plan
/// (architecture review 610) and the session scope it was planned in.
///
/// `masked_fields` is the enforcer's `(table, column)` list of masked and
/// restricted scan columns, `table` spelled as the scan's `TableReference`
/// (`catalog.ns.t`, `ns.t` or `t`). Every column-lineage dependency on one of
/// them is reported with the `MASKED` transformation instead of identity
/// passthrough: the target holds hashes, `***` or NULL, not the value, so a
/// consumer must neither propagate the source column's tags nor conclude the
/// raw value was copied. The enforcer's projection sits between the scan and
/// the user's query, so this is decided from the enforcer's own record rather
/// than by guessing at expression shapes; `sqe-lineage` stays free of a
/// policy-crate dependency.
///
/// `scope` is the catalog and schema unqualified and 2-part table names
/// resolved in, so `customers` is named after the catalog it was read from.
///
/// `quality` reports the plan node kinds the column tracer has no rule for;
/// when non-empty the emitter attaches the `sqe_lineageQuality` run facet.
pub fn extract_lineage_governed(
    plan: &LogicalPlan,
    lookup: &CatalogLookup,
    masked_fields: &[(String, String)],
    scope: &LineageScope,
) -> Extracted {
    let inputs = datasets::extract_inputs(plan, lookup, scope);
    let mut outputs = datasets::extract_outputs(plan, lookup, scope);
    let mut untraced = Vec::new();

    // For DML/DDL writes, attach the target schema and column lineage by
    // tracing the source plan. The planner aligns source columns with the
    // target by ordinal (spec §5.3), so output column i is named after the
    // target table's column i (`target_fields`), not the SELECT expression
    // that feeds it.
    if let Some(out) = outputs.first_mut() {
        if let Some(source_plan) = source_of_write(plan) {
            let fields = target_fields(plan, source_plan);
            out.facets.schema = Some(SchemaFacet {
                fields: fields.clone(),
            });
            let (mut trace, missing) = columns::trace_plan_reporting(source_plan);
            untraced = missing;
            columns::resolve_scope(&mut trace, scope);
            mark_masked(&mut trace, masked_fields, scope);
            let target_names: Vec<String> = fields.into_iter().map(|f| f.name).collect();
            out.facets.columnLineage =
                Some(build_column_lineage_facet(&target_names, &trace, lookup));
        }
        out.facets.version =
            committed_snapshot(plan).map(|id| DatasetVersionFacet { datasetVersion: id });
    }

    Extracted {
        inputs,
        outputs,
        quality: LineageQualityFacet {
            complete: untraced.is_empty(),
            untraced,
        },
    }
}

/// Result of [`extract_lineage_governed`].
pub struct Extracted {
    pub inputs: Vec<InputDataset>,
    pub outputs: Vec<OutputDataset>,
    pub quality: LineageQualityFacet,
}

/// Rewrite the transformation of every dependency on a masked or restricted
/// scan column to `MASKED`. The enforcer's table key is parsed with the same
/// rule the tracer applies to a scan's `table_name` and resolved in the same
/// scope, so `t`, `ns.t` and `catalog.ns.t` compare as the planner resolved
/// them.
fn mark_masked(
    trace: &mut columns::ColumnTrace,
    masked_fields: &[(String, String)],
    scope: &LineageScope,
) {
    if masked_fields.is_empty() {
        return;
    }
    let masked: Vec<((String, String, String), &str)> = masked_fields
        .iter()
        .map(|(table, column)| {
            let (_, schema, table) = columns::parse_table_ref(table);
            let schema = if schema.is_empty() {
                scope.schema.clone()
            } else {
                schema
            };
            ((String::new(), schema, table), column.as_str())
        })
        .collect();
    for deps in trace.iter_mut() {
        for dep in deps.iter_mut() {
            let hit = masked.iter().any(|((_, schema, table), column)| {
                schema == &dep.schema && table == &dep.table && *column == dep.field
            });
            if hit {
                dep.transformation = columns::masked();
            }
        }
    }
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

/// The written dataset's fields, in source-column order.
///
/// For a DML write the target table's own columns name the fields: source
/// column i lands in target column i (spec §5.3), so
/// `INSERT INTO t SELECT country, segment, 1 FROM c` describes `t`'s
/// `country, seg, n`, not `country, segment, Int64(1)`. The source plan's
/// names are the fallback for CTAS / CREATE VIEW (the target is made from
/// the source) and for a target whose arity differs from the source's.
fn target_fields(plan: &LogicalPlan, source: &LogicalPlan) -> Vec<SchemaField> {
    let source_fields = source.schema().fields();
    if let LogicalPlan::Dml(stmt) = plan {
        let target = stmt.target.schema();
        if target.fields().len() == source_fields.len() {
            return target
                .fields()
                .iter()
                .map(|f| SchemaField {
                    name: f.name().clone(),
                    field_type: f.data_type().to_string(),
                })
                .collect();
        }
    }
    source_fields
        .iter()
        .map(|f| SchemaField {
            name: f.name().clone(),
            field_type: f.data_type().to_string(),
        })
        .collect()
}

/// The Iceberg snapshot id a write committed, when its write path recorded
/// one on the lineage target ([`SNAPSHOT_ID_METADATA_KEY`]).
fn committed_snapshot(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Dml(stmt) => stmt
            .target
            .schema()
            .metadata()
            .get(SNAPSHOT_ID_METADATA_KEY)
            .cloned(),
        _ => None,
    }
}

/// Build the `ColumnLineageFacet` from a positional target-field list and a
/// source-plan column trace, mapping each `ColumnDep` to its OL
/// `(namespace, name)` pair through the same identity as the datasets.
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
            .map(|d| {
                let id = dataset_identity(lookup, &d.catalog, &d.schema, &d.table);
                ColumnLineageInput {
                    namespace: id.namespace,
                    name: id.name,
                    field: d.field.clone(),
                    transformations: vec![d.transformation.clone()],
                }
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

/// Set the OL `lifecycleStateChange` facet (e.g. `CREATE` for a CTAS) on
/// every output dataset.
pub fn apply_lifecycle(outputs: &mut [OutputDataset], change: &str) {
    for out in outputs {
        out.facets.lifecycleStateChange = Some(LifecycleStateChangeFacet {
            lifecycleStateChange: change.to_string(),
        });
    }
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
            let id = dataset_identity(lookup, catalog, schema, table);
            let mut facets = id.facets;
            facets.schema = Some(SchemaFacet {
                fields: columns
                    .iter()
                    .map(|(name, ty)| SchemaField {
                        name: name.clone(),
                        field_type: ty.clone(),
                    })
                    .collect(),
            });
            let output = OutputDataset {
                namespace: id.namespace,
                name: id.name,
                facets,
                outputFacets: OutputDatasetFacets::default(),
            };
            (vec![], vec![output])
        }
        LineageHint::MaintenanceTable {
            catalog,
            schema,
            table,
        } => {
            let id = dataset_identity(lookup, catalog, schema, table);
            let input = InputDataset {
                namespace: id.namespace.clone(),
                name: id.name.clone(),
                facets: id.facets.clone(),
            };
            let output = OutputDataset {
                namespace: id.namespace,
                name: id.name,
                facets: id.facets,
                outputFacets: OutputDatasetFacets::default(),
            };
            (vec![input], vec![output])
        }
        LineageHint::TableLifecycle {
            catalog,
            schema,
            table,
            change,
        } => {
            let id = dataset_identity(lookup, catalog, schema, table);
            let mut outputs = vec![OutputDataset {
                namespace: id.namespace,
                name: id.name,
                facets: id.facets,
                outputFacets: OutputDatasetFacets::default(),
            }];
            apply_lifecycle(&mut outputs, change);
            (vec![], outputs)
        }
    }
}

#[cfg(test)]
mod hint_tests {
    use super::*;
    use crate::event::lifecycle;

    fn lookup() -> CatalogLookup {
        Arc::new(|catalog| CatalogInfo::new(catalog, Some(format!("https://catalog/{catalog}"))))
    }

    #[test]
    fn maintenance_hint_emits_target_as_input_and_output() {
        let hint = LineageHint::MaintenanceTable {
            catalog: "iceberg".into(),
            schema: "analytics".into(),
            table: "events".into(),
        };
        let (inputs, outputs) = extract_from_hint(&hint, &lookup());
        assert_eq!(inputs.len(), 1);
        assert_eq!(outputs.len(), 1);
        assert_eq!(inputs[0].name, "analytics.events");
        assert_eq!(outputs[0].name, "analytics.events");
        assert_eq!(inputs[0].namespace, "sqe://iceberg");
        assert_eq!(outputs[0].namespace, "sqe://iceberg");
        assert_eq!(
            outputs[0].facets.dataSource.as_ref().unwrap().uri,
            "https://catalog/iceberg"
        );
    }

    #[test]
    fn drop_hint_emits_the_table_with_lifecycle_drop() {
        let hint = LineageHint::TableLifecycle {
            catalog: "workspace1".into(),
            schema: "default".into(),
            table: "lin_probe".into(),
            change: lifecycle::DROP.into(),
        };
        let (inputs, outputs) = extract_from_hint(&hint, &lookup());
        assert!(inputs.is_empty());
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].namespace, "sqe://workspace1");
        assert_eq!(outputs[0].name, "default.lin_probe");
        assert_eq!(
            outputs[0]
                .facets
                .lifecycleStateChange
                .as_ref()
                .unwrap()
                .lifecycleStateChange,
            "DROP"
        );
        assert!(outputs[0].facets.schema.is_none());
    }
}

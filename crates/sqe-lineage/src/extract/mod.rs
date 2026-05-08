//! Lineage extraction from DataFusion LogicalPlan.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.
//!
//! Phase E fills in the per-node trace rules. Until then, the entry points
//! return empty input/output lists so the emitter can run end-to-end with
//! real channel + sinks plumbing.

pub mod datasets;
pub mod columns;

use crate::event::{InputDataset, OutputDataset};
use crate::observer::LineageHint;
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;

/// Catalog-name -> namespace-URI lookup, threaded through the emitter so dataset
/// URIs respect SQE's multi-catalog config (spec §4.4).
pub type CatalogLookup = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Extract input + output datasets (with column lineage on outputs) from a
/// DataFusion `LogicalPlan`.
///
/// Phase E stub: returns `(vec![], vec![])`.
pub fn extract_lineage(
    _plan: &LogicalPlan,
    _lookup: &CatalogLookup,
) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    (vec![], vec![])
}

/// Extract output dataset from a DDL hint (CREATE TABLE / DROP / ALTER carry
/// no source plan but do have target schema).
///
/// Phase E stub.
pub fn extract_from_hint(
    _hint: &LineageHint,
    _lookup: &CatalogLookup,
) -> (Vec<InputDataset>, Vec<OutputDataset>) {
    (vec![], vec![])
}

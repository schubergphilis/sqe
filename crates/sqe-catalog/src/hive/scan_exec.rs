//! The one plan node a Hive scan can be recognized by.
//!
//! ## Why this exists
//!
//! The coordinator decides what to distribute by finding a single node:
//! `find_iceberg_scan` then `downcast_ref::<IcebergScanExec>()`. Everything
//! downstream -- `dim_build_swap`, `explain.rs`, `parallel_probe_scan` -- finds
//! it the same way.
//!
//! A Hive scan had no such node. `scan_from_entries` emits a `UnionExec` of
//! per-partition `ListingTable` scans, each under two `ProjectionExec`s, and
//! nothing in that tree carries the table's identity. So there was nothing to
//! downcast to, and no amount of `ScanTask` work would have changed that.
//!
//! ## Why it wraps rather than replaces, for now
//!
//! The full refactor is for `HiveScanExec` to OWN execution: describe the scan,
//! and materialize partitions itself, so that logic sits next to the worker-side
//! copy it must agree with. That is the right end state and it is a real
//! refactor of `scan_from_entries`, which currently builds the plan.
//!
//! This node instead wraps the plan the provider already builds and carries the
//! metadata the coordinator needs beside it. The wrapper is a deliberate
//! intermediate: it makes a Hive scan findable TODAY, with provably zero change
//! to what a query returns (`execute` delegates), so dispatch can be built and
//! measured before the execution path is rewritten underneath it. Building the
//! inner plan costs nothing meaningful even when it is later discarded --
//! `ListingTable` lists lazily at execute time, so an unused plan does no I/O.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectMeta;
use url::Url;

use arrow_schema::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};

use super::manifest::PartitionIndexEntry;
use super::provider::{table_location_url, HiveDataStoreFactory};
use crate::hive::HiveError;
use super::serde_map::ResolvedFormat;

/// What the coordinator needs to turn this scan into worker tasks, without
/// having to re-derive any of it from the plan tree.
#[derive(Debug, Clone)]
pub struct HiveScanSpec {
    /// The partitions that survived pruning. One `ScanTask` per entry.
    pub entries: Vec<PartitionIndexEntry>,
    /// Format and SerDe settings, for the task descriptor.
    pub resolved: ResolvedFormat,
    /// Declared partition key order. Load-bearing: the projection indexes by
    /// position, so this must not be re-derived from a map.
    pub partition_columns: Vec<String>,
    /// File columns followed by partition columns.
    pub full_schema: SchemaRef,
    /// Named secret for the data store, if the manifest declared one.
    pub secret: Option<String>,
    /// Builds an object store for a partition's location. Carried on the spec
    /// so the node can list its own files, the way `IcebergScanExec` answers
    /// `data_file_info()` for itself, rather than making the coordinator
    /// reconstruct storage access from the plan.
    pub data_store_factory: Arc<dyn HiveDataStoreFactory>,
}

/// A Hive external table scan, wrapping the plan that executes it.
pub struct HiveScanExec {
    input: Arc<dyn ExecutionPlan>,
    spec: HiveScanSpec,
}

impl HiveScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, spec: HiveScanSpec) -> Self {
        Self { input, spec }
    }

    /// Everything needed to build worker tasks for this scan.
    pub fn spec(&self) -> &HiveScanSpec {
        &self.spec
    }

    /// The plan that executes this scan coordinator-local.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// List the data files of each surviving partition, as
    /// `(partition values, [(url, size_bytes)])`.
    ///
    /// The Hive analogue of `IcebergScanExec::data_file_info()`, and the input
    /// `build_hive_scan_tasks` needs. Sizes are collected because placement
    /// without them is blind: that is the whole reason the coordinator lists
    /// rather than shipping a prefix for the worker to expand.
    ///
    /// Only partitions that survived pruning are listed. That bound is what the
    /// cycle-2 partition index bought, and this spends it deliberately rather
    /// than walking the table.
    ///
    /// A partition whose listing fails is an error rather than an empty
    /// partition: silently returning no files would turn an unreachable
    /// location into a query that succeeds with missing rows.
    pub async fn partition_files(
        &self,
    ) -> Result<Vec<(BTreeMap<String, String>, Vec<(String, u64)>)>, HiveError> {
        let ext = self.spec.resolved.file_extension.clone();
        let mut out = Vec::with_capacity(self.spec.entries.len());

        for entry in &self.spec.entries {
            let url = table_location_url(&entry.location)?;
            let store = self
                .spec
                .data_store_factory
                .store_for(&entry.location, self.spec.secret.as_deref())?;

            let metas: Vec<ObjectMeta> = store
                .list(Some(url.prefix()))
                .try_collect()
                .await
                .map_err(|e| {
                    HiveError::Validation(format!(
                        "listing partition `{}`: {e}",
                        entry.location
                    ))
                })?;

            let base = Url::parse(&entry.location).map_err(|e| {
                HiveError::Validation(format!("partition location `{}`: {e}", entry.location))
            })?;

            let mut files = Vec::new();
            for m in metas {
                let key = m.location.as_ref();
                // Skip directory markers and anything the table's format would
                // not read. An empty declared extension means "no filter",
                // which is how Hive's extensionless part files are handled.
                if key.ends_with('/') || (!ext.is_empty() && !key.ends_with(&ext)) {
                    continue;
                }
                let mut full = base.clone();
                full.set_path(key);
                files.push((full.to_string(), m.size));
            }
            out.push((entry.values.clone(), files));
        }
        Ok(out)
    }
}

impl fmt::Debug for HiveScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveScanExec")
            .field("partitions", &self.spec.entries.len())
            .field("format", &self.spec.resolved.kind)
            .field("partition_columns", &self.spec.partition_columns)
            .finish()
    }
}

impl DisplayAs for HiveScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "HiveScanExec: partitions={}, format={:?}",
                self.spec.entries.len(),
                self.spec.resolved.kind
            ),
            DisplayFormatType::TreeRender => write!(f, "HiveScanExec"),
        }
    }
}

impl ExecutionPlan for HiveScanExec {
    fn name(&self) -> &str {
        "HiveScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Identical to the wrapped plan: this node changes nothing about the
        // data, its partitioning or its ordering.
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// SQE PATCH (DataFusion 55): owns no expressions of its own. The wrapped
    /// plan is a child and is visited in its own right.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// DataFusion 55 deprecated this but kept it required; upstream's shim.
    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // The spec must survive a rebuild. If an optimizer rule rebuilt this
        // node and dropped it, the scan would stop being recognizable and would
        // silently stop distributing -- the same class of invisibility
        // HiveLocalScanExec exists to prevent.
        let child = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "HiveScanExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(HiveScanExec::new(child, self.spec.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

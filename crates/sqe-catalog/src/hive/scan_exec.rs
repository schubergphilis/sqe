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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectMeta;
use url::Url;

use arrow_schema::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result as DFResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, ReplaceChildrenOptions,
};

use super::manifest::PartitionIndexEntry;
use super::provider::{table_location_url, HiveDataStoreFactory};
use crate::hive::HiveError;
use super::serde_map::ResolvedFormat;

/// A listing of one partition: its directory-encoded values, and each data
/// file's `(url, size_bytes)`.
///
/// `sqe_coordinator::hive_scan_tasks::HivePartitionListing` is the same type on
/// the consuming side. Two aliases rather than one because the catalog crate
/// cannot depend on the coordinator; they are named identically so a change to
/// one is visibly a change to the other.
pub type PartitionListing = (BTreeMap<String, String>, Vec<(String, u64)>);

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
    ///
    /// Partitions are listed [`PARTITION_LIST_CONCURRENCY`] at a time, in
    /// entry order. Each listing is one object-store round trip and none
    /// depends on another; serially, a 1,241-partition table paid 1,241
    /// round trips end to end before the first task could be built.
    pub async fn partition_files(&self) -> Result<Vec<PartitionListing>, HiveError> {
        use futures::StreamExt;
        let ext = self.spec.resolved.file_extension.clone();
        // Plain futures first, driven by the stream after. A `StreamExt::map`
        // closure returning an `async move` over the borrowed entry is the
        // "implementation of `Send` is not general enough" shape: the
        // closure's argument lifetime is higher-ranked and the coordinator's
        // `Send` proof over the future that awaits this cannot close it.
        let listings: Vec<_> = self
            .spec
            .entries
            .iter()
            .map(|entry| {
                let ext = ext.clone();
                async move {
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
                        HiveError::Validation(format!(
                            "partition location `{}`: {e}",
                            entry.location
                        ))
                    })?;

                    let mut files = Vec::new();
                    for m in metas {
                        let key = m.location.as_ref();
                        // Skip directory markers and anything the table's
                        // format would not read. An empty declared extension
                        // means "no filter", which is how Hive's extensionless
                        // part files are handled.
                        if key.ends_with('/') || (!ext.is_empty() && !key.ends_with(&ext)) {
                            continue;
                        }
                        let mut full = base.clone();
                        full.set_path(key);
                        files.push((full.to_string(), m.size));
                    }
                    Ok::<PartitionListing, HiveError>((entry.values.clone(), files))
                }
            })
            .collect();
        futures::stream::iter(listings)
            .buffered(PARTITION_LIST_CONCURRENCY)
            .try_collect()
            .await
    }
}

/// Partition listings in flight at once, for both the coordinator-local plan
/// build and the worker-task listing. Bounded so a wide table does not open
/// thousands of connections; high enough that a listing round trip of a
/// second amortises to a few milliseconds per partition.
pub const PARTITION_LIST_CONCURRENCY: usize = 16;

/// A Hive scan under a row limit that lists and reads one partition at a
/// time, newest first, and stops when the limit is met.
///
/// `scan_from_entries` used to build one `ListingTable` scan per surviving
/// partition at plan time, and building it lists the partition's prefix.
/// Under `LIMIT 10` on a 1,241-day snapshot table that was 1,241 listings,
/// one per day, to read ten rows from one file, and the plan held every
/// file's metadata in memory before the first byte was read. Here the plan
/// holds the entries and everything needed to build a partition's scan; the
/// build happens in `execute`, entry by entry, and the next partition is not
/// touched once `limit` rows have been produced. A `LIMIT` without `ORDER BY`
/// promises nothing about which rows come back, so reading the newest
/// partition first is a valid answer and the useful one for a snapshot table.
///
/// Only a limited scan comes here. Without a limit every partition is read
/// anyway, and the eager union keeps its parallelism across partitions.
pub struct LazyPartitionScanExec {
    parts: Arc<LazyScanParts>,
    properties: Arc<PlanProperties>,
}

/// What building one partition's scan needs; shared with the stream so a
/// clone of the node is one `Arc` bump.
pub struct LazyScanParts {
    /// In the order partitions are read. `scan_from_entries` sorts them
    /// newest first.
    pub entries: Vec<PartitionIndexEntry>,
    pub state: SessionState,
    pub resolved: ResolvedFormat,
    pub file_schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub partition_columns: Vec<String>,
    pub projection: Option<Vec<usize>>,
    pub data_filters: Vec<Expr>,
    pub limit: usize,
    pub secret: Option<String>,
    pub data_store_factory: Arc<dyn HiveDataStoreFactory>,
    pub runtime: Arc<RuntimeEnv>,
}

impl LazyPartitionScanExec {
    pub fn new(parts: LazyScanParts) -> Self {
        let schema = projected_schema(&parts.full_schema, parts.projection.as_deref());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            parts: Arc::new(parts),
            properties: Arc::new(properties),
        }
    }

    pub fn partition_count(&self) -> usize {
        self.parts.entries.len()
    }

    pub fn limit(&self) -> usize {
        self.parts.limit
    }
}

impl fmt::Debug for LazyPartitionScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyPartitionScanExec")
            .field("partitions", &self.parts.entries.len())
            .field("limit", &self.parts.limit)
            .field("format", &self.parts.resolved.kind)
            .finish()
    }
}

impl DisplayAs for LazyPartitionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "LazyPartitionScanExec: partitions={}, limit={}, format={:?}, order=newest-first",
                self.parts.entries.len(),
                self.parts.limit,
                self.parts.resolved.kind
            ),
            DisplayFormatType::TreeRender => write!(f, "LazyPartitionScanExec"),
        }
    }
}

impl ExecutionPlan for LazyPartitionScanExec {
    fn name(&self) -> &str {
        "LazyPartitionScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

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
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "LazyPartitionScanExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        use futures::StreamExt;
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "LazyPartitionScanExec has one output partition, asked for {partition}"
            )));
        }
        let schema = self.schema();
        let parts = Arc::clone(&self.parts);
        // Rows still owed. Shared with the per-entry step so a partition
        // whose turn comes after the limit is met is never listed: the
        // flatten below asks for the next inner stream as soon as the
        // previous one ends, before the limit step sees that it is done.
        let remaining = Arc::new(AtomicUsize::new(parts.limit));
        let empty_schema = Arc::clone(&schema);
        let entries: Vec<PartitionIndexEntry> = parts.entries.clone();
        let inner = futures::stream::iter(entries)
            .then({
                let remaining = Arc::clone(&remaining);
                move |entry| {
                    let parts = Arc::clone(&parts);
                    let context = Arc::clone(&context);
                    let remaining = Arc::clone(&remaining);
                    let empty_schema = Arc::clone(&empty_schema);
                    async move {
                        if remaining.load(Ordering::SeqCst) == 0 {
                            let done: SendableRecordBatchStream = Box::pin(
                                RecordBatchStreamAdapter::new(empty_schema, futures::stream::empty()),
                            );
                            return Ok(done);
                        }
                        let plan = super::provider::plan_partition_scan(
                            &parts.state,
                            &entry,
                            &parts.resolved,
                            &parts.file_schema,
                            &parts.full_schema,
                            &parts.partition_columns,
                            parts.projection.as_deref(),
                            &parts.data_filters,
                            Some(parts.limit),
                            &parts.data_store_factory,
                            parts.secret.as_deref(),
                            &parts.runtime,
                        )
                        .await?;
                        let plan: Arc<dyn ExecutionPlan> =
                            if plan.output_partitioning().partition_count() > 1 {
                                Arc::new(CoalescePartitionsExec::new(plan))
                            } else {
                                plan
                            };
                        plan.execute(0, context)
                    }
                }
            })
            .try_flatten()
            .scan(remaining, |remaining, item| {
                let out = match item {
                    Err(e) => Some(Err(e)),
                    Ok(batch) => {
                        let left = remaining.load(Ordering::SeqCst);
                        if left == 0 {
                            None
                        } else {
                            let take = batch.num_rows().min(left);
                            remaining.fetch_sub(take, Ordering::SeqCst);
                            Some(Ok(if take < batch.num_rows() {
                                batch.slice(0, take)
                            } else {
                                batch
                            }))
                        }
                    }
                };
                futures::future::ready(out)
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, inner)))
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    match projection {
        Some(idx) => {
            let fields: Vec<arrow_schema::Field> =
                idx.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(
                arrow_schema::Schema::new(fields).with_metadata(schema.metadata().clone()),
            )
        }
        None => Arc::clone(schema),
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

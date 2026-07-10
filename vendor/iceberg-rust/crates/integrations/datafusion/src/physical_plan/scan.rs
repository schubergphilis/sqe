// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::pin::Pin;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    PushedDown,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::expr::{DynamicPredicate, Predicate};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use super::physical_to_predicate::RuntimeFiltersDynamicPredicate;
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Projection column names, None means all columns
    projection: Option<Vec<String>>,
    /// Static filters converted to an Iceberg [`Predicate`] at planning time
    /// and pushed into manifest pruning + Parquet row-group eval.
    predicates: Option<Predicate>,
    /// Optional limit on the number of rows to return
    limit: Option<usize>,
    /// Runtime filters absorbed via [`ExecutionPlan::handle_child_pushdown_result`].
    ///
    /// These typically come from a `HashJoinExec` build side
    /// ([`datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr`])
    /// and start as `lit(true)`. The hash-join build phase replaces the
    /// inner expression with a real predicate (e.g. an `IN`-list of build
    /// keys) once the build side completes. We evaluate these per-batch
    /// during `execute()` so any filtering effect kicks in as soon as the
    /// build side finishes, even for scans that have already started
    /// streaming.
    runtime_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema.clone());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            limit,
            runtime_filters: Vec::new(),
        }
    }

    /// Return a copy of this scan with `extra_filters` appended to the
    /// runtime filter list. Used by [`ExecutionPlan::handle_child_pushdown_result`]
    /// when a parent (typically `HashJoinExec`) hands us a dynamic filter.
    fn with_runtime_filters(&self, extra_filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let mut combined = self.runtime_filters.clone();
        combined.extend(extra_filters);
        Self {
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            plan_properties: self.plan_properties.clone(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            limit: self.limit,
            runtime_filters: combined,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.predicates.as_ref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Reconstruct a scan from already-resolved parts.
    ///
    /// SQE-only patch (sqe-ballista): a `PhysicalExtensionCodec` on the
    /// ballista executor needs to rebuild an `IcebergTableScan` from the
    /// wire — it has the post-planning `projection` (column names),
    /// `predicates`, and `limit`, plus a freshly-loaded `Table`, but the
    /// stock `new()` takes raw DataFusion `Expr` filters and projection
    /// indices that aren't recoverable after planning.  This constructor
    /// takes the resolved fields directly.  `output_schema` must already
    /// reflect the projection.
    ///
    /// Upstream shape: iceberg-datafusion should own its
    /// `PhysicalExtensionCodec` and this constructor (or an equivalent) so
    /// the scan node round-trips for distributed engines (ballista,
    /// datafusion-comet).  Filed-shape TODO in the spike report.
    #[allow(clippy::too_many_arguments)]
    pub fn from_codec_parts(
        table: Table,
        snapshot_id: Option<i64>,
        output_schema: ArrowSchemaRef,
        projection: Option<Vec<String>>,
        predicates: Option<Predicate>,
        limit: Option<usize>,
    ) -> Self {
        let plan_properties = Self::compute_properties(output_schema);
        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            limit,
            runtime_filters: Vec::new(),
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> Arc<PlanProperties> {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Bridge any runtime filters (e.g. dynamic filters from a
        // HashJoinExec build side) into the iceberg-rust scan via the
        // DynamicPredicate API. The bridge samples the latest filter
        // state once per FileScanTask and ANDs it into the static
        // predicate so row-group min/max + page-index pruning kick in
        // mid-stream once the build side completes.
        let dynamic_predicate: Option<Arc<dyn DynamicPredicate>> =
            if self.runtime_filters.is_empty() {
                None
            } else {
                Some(RuntimeFiltersDynamicPredicate::new(
                    self.runtime_filters.clone(),
                ))
            };

        let fut = get_batch_stream(
            self.table.clone(),
            self.snapshot_id,
            self.projection.clone(),
            self.predicates.clone(),
            dynamic_predicate,
        );
        let stream = futures::stream::once(fut).try_flatten();

        // Apply runtime filters (e.g. join build-side dynamic filters)
        // per batch. The filters start as `lit(true)` while the build
        // side is still loading and become selective once it completes,
        // so we evaluate fresh on every batch.
        let runtime_filters = self.runtime_filters.clone();
        let filtered_stream: Pin<
            Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>,
        > = if runtime_filters.is_empty() {
            Box::pin(stream)
        } else {
            Box::pin(stream.map(move |batch_res| match batch_res {
                Ok(batch) => apply_runtime_filters(batch, &runtime_filters),
                Err(e) => Err(e),
            }))
        };

        // Apply limit if specified
        let limited_stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            if let Some(limit) = self.limit {
                let mut remaining = limit;
                Box::pin(filtered_stream.try_filter_map(move |batch| {
                    futures::future::ready(if remaining == 0 {
                        Ok(None)
                    } else if batch.num_rows() <= remaining {
                        remaining -= batch.num_rows();
                        Ok(Some(batch))
                    } else {
                        let limited_batch = batch.slice(0, remaining);
                        remaining = 0;
                        Ok(Some(limited_batch))
                    })
                }))
            } else {
                filtered_stream
            };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            limited_stream,
        )))
    }

    /// Accept runtime filters from a parent node (e.g. dynamic filters
    /// from a `HashJoinExec` build side). Leaf scans return an empty
    /// [`FilterDescription`] because they have no children to push to;
    /// the absorption happens in [`Self::handle_child_pushdown_result`].
    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        // Trace at `debug!` (not `info!`) so production logs stay clean
        // by default. Set `RUST_LOG=iceberg_datafusion=debug` to see
        // whether DataFusion is offering filters to this scan; useful
        // for diagnosing why Path B-2 (runtime filter pushdown) is or
        // is not engaging on a given query.
        tracing::debug!(
            phase = ?phase,
            parent_filter_count = parent_filters.len(),
            "IcebergTableScan::gather_filters_for_pushdown"
        );
        Ok(FilterDescription::new())
    }

    /// Bind the runtime filters that the framework has decided to push
    /// down to this scan. Returns a clone of the scan with the filters
    /// stored, so `execute()` can apply them per batch. We mark the
    /// parent filters as "supported" (yes) so the framework knows it
    /// can drop the wrapping `FilterExec` and avoid double-evaluating.
    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let absorbed: Vec<Arc<dyn PhysicalExpr>> = child_pushdown_result
            .parent_filters
            .iter()
            .map(|f| Arc::clone(&f.filter))
            .collect();

        // Trace whether this scan absorbed runtime filters. When the
        // count is non-zero the absorbed filters reach
        // `RuntimeFiltersDynamicPredicate` in `execute()` and feed
        // the iceberg-rust row-group pruning. When the count is zero
        // and a join above had a dynamic filter, the framework chose
        // not to push it down (intermediate node blocked, partition
        // mode mismatch, etc.).
        tracing::debug!(
            phase = ?phase,
            absorbed_filter_count = absorbed.len(),
            "IcebergTableScan::handle_child_pushdown_result"
        );

        if absorbed.is_empty() {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        // Mark each absorbed parent filter as supported so the framework
        // knows it can drop the wrapping `FilterExec` and avoid double
        // evaluation.
        let supported: Vec<PushedDown> =
            vec![PushedDown::Yes; child_pushdown_result.parent_filters.len()];
        let new_node = self.with_runtime_filters(absorbed);
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(supported)
            .with_updated_node(Arc::new(new_node) as Arc<dyn ExecutionPlan>))
    }
}

/// Helper: tiny wrapper around DataFusion's `PhysicalExpr::evaluate` +
/// `arrow::compute::filter_record_batch` to apply a list of conjunctive
/// runtime filters to a batch and return the surviving rows.
///
/// Each filter is evaluated in turn; as soon as a batch goes empty we
/// return an empty batch with the same schema rather than continuing to
/// evaluate the rest.
fn apply_runtime_filters(
    mut batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> DFResult<RecordBatch> {
    for filter in filters {
        if batch.num_rows() == 0 {
            break;
        }
        let mask_value = filter.evaluate(&batch)?;
        let mask: BooleanArray = match mask_value {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "runtime filter must produce a BooleanArray".into(),
                    )
                })?
                .clone(),
            ColumnarValue::Scalar(scalar) => {
                // Scalar true (the initial DynamicFilterPhysicalExpr value)
                // means "keep all rows". Scalar false / null means "drop
                // everything in this batch".
                let keep_all = matches!(
                    scalar,
                    datafusion::common::ScalarValue::Boolean(Some(true))
                );
                if keep_all {
                    continue;
                }
                BooleanArray::from(vec![false; batch.num_rows()])
            }
        };
        batch = filter_record_batch(&batch, &mask).map_err(DataFusionError::from)?;
    }
    Ok(batch)
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or(String::from(""), |p| format!("{p}"))
        )?;
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        Ok(())
    }
}

/// Asynchronously retrieves a stream of [`RecordBatch`] instances
/// from a given table.
///
/// This function initializes a [`TableScan`], builds it,
/// and then converts it into a stream of Arrow [`RecordBatch`]es.
async fn get_batch_stream(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
    dynamic_predicate: Option<Arc<dyn DynamicPredicate>>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };

    let mut scan_builder = match column_names {
        Some(column_names) => scan_builder.select(column_names),
        None => scan_builder.select_all(),
    };
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    if let Some(dp) = dynamic_predicate {
        scan_builder = scan_builder.with_dynamic_predicate(dp);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);
    Ok(Box::pin(stream))
}

fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}

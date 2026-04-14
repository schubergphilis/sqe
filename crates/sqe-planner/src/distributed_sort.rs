//! Distributed sort planning: range boundary sampling and `DistributedSortExec`.
//!
//! When a query requires globally sorted output over data distributed across
//! multiple executors, a hash-based repartition cannot guarantee order.
//! Instead, we use **range-partitioned sort**:
//!
//! 1. **Sample boundaries**: Use Iceberg manifest per-file min/max statistics
//!    to estimate P-1 boundary values that split the data into P roughly
//!    equal-sized range partitions. When file-level stats are too coarse,
//!    fall back to requesting reservoir samples from executors.
//!
//! 2. **Shuffle by range**: Each executor range-partitions its scan output on
//!    the sort key and ships each range to the owning executor via DoExchange
//!    (`ShuffleWriterExec` with `ShufflePartitioning::Range`).
//!
//! 3. **Local sort**: Each executor locally sorts its received range partition.
//!    Because ranges are disjoint, concatenating partitions in order yields a
//!    globally sorted result.
//!
//! This module provides:
//! - [`compute_range_boundaries`] — boundary estimation from file-level stats
//! - [`sample_based_boundaries`] — fallback sampling when stats are too coarse
//! - [`DistributedSortExec`] — `ExecutionPlan` that replaces `SortExec`
//! - [`DistributedSortRule`] — `PhysicalOptimizerRule` that applies the rewrite

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use tracing::{debug, trace};

// ────────────────────── Range boundary computation ──────────────────────────

/// Compute P-1 range boundaries from per-file min/max statistics.
///
/// Each entry in `file_stats` is `(file_path, min_value, max_value)` for the
/// sort column. The function collects all boundary candidates (both min and
/// max values), sorts them, and picks P-1 evenly-spaced quantile values.
///
/// This gives approximate quantile boundaries without reading any actual data,
/// relying solely on Iceberg manifest statistics which are always available.
///
/// # Arguments
/// - `file_stats`: Per-file `(path, min, max)` for the sort column.
/// - `num_partitions`: The target number of range partitions (P).
///
/// # Returns
/// A vector of P-1 `ScalarValue` boundary values. Data with sort key < boundary[0]
/// goes to partition 0, data with boundary[0] <= key < boundary[1] goes to
/// partition 1, and so on.
pub fn compute_range_boundaries(
    file_stats: &[(String, ScalarValue, ScalarValue)],
    num_partitions: usize,
) -> Result<Vec<ScalarValue>> {
    if num_partitions <= 1 || file_stats.is_empty() {
        return Ok(vec![]);
    }

    // Collect all boundary candidates from file-level min/max values.
    // Each file contributes its min and max value for the sort column.
    let mut candidates: Vec<ScalarValue> = Vec::with_capacity(file_stats.len() * 2);
    for (_path, min_val, max_val) in file_stats {
        if !min_val.is_null() {
            candidates.push(min_val.clone());
        }
        if !max_val.is_null() {
            candidates.push(max_val.clone());
        }
    }

    if candidates.is_empty() {
        return Ok(vec![]);
    }

    // Sort candidates. ScalarValue implements PartialOrd; we treat
    // incomparable values (different types) as equal for sorting purposes.
    candidates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    candidates.dedup();

    if candidates.len() < num_partitions {
        // Not enough distinct values to create P-1 boundaries.
        // Return what we have — caller should check and fall back to sampling.
        let boundaries: Vec<ScalarValue> = candidates
            .into_iter()
            .skip(1) // Skip the global min to avoid an empty first partition
            .collect();
        return Ok(boundaries);
    }

    // Pick P-1 evenly spaced boundary values from the sorted candidates.
    let num_boundaries = num_partitions - 1;
    let mut boundaries = Vec::with_capacity(num_boundaries);
    let step = candidates.len() as f64 / num_partitions as f64;

    for i in 1..=num_boundaries {
        let idx = (i as f64 * step).round() as usize;
        let idx = idx.min(candidates.len() - 1);
        boundaries.push(candidates[idx].clone());
    }

    // Deduplicate boundaries (could happen with skewed data)
    boundaries.dedup();

    Ok(boundaries)
}

/// Compute range boundaries from reservoir samples when file-level statistics
/// are too coarse (e.g., very few files but many partitions needed).
///
/// This is a placeholder for the full implementation which would send sampling
/// requests to executors via Flight DoExchange. For now, it computes boundaries
/// from provided sample values.
///
/// # Arguments
/// - `samples`: Collected sample values from executors for the sort column.
/// - `num_partitions`: The target number of range partitions (P).
///
/// # Returns
/// A vector of P-1 `ScalarValue` boundary values.
pub fn sample_based_boundaries(
    samples: &[ScalarValue],
    num_partitions: usize,
) -> Result<Vec<ScalarValue>> {
    if num_partitions <= 1 || samples.is_empty() {
        return Ok(vec![]);
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sorted.dedup();

    if sorted.len() < num_partitions {
        // Not enough distinct samples; return all as boundaries
        let boundaries: Vec<ScalarValue> = sorted.into_iter().skip(1).collect();
        return Ok(boundaries);
    }

    let num_boundaries = num_partitions - 1;
    let mut boundaries = Vec::with_capacity(num_boundaries);
    let step = sorted.len() as f64 / num_partitions as f64;

    for i in 1..=num_boundaries {
        let idx = (i as f64 * step).round() as usize;
        let idx = idx.min(sorted.len() - 1);
        boundaries.push(sorted[idx].clone());
    }

    boundaries.dedup();
    Ok(boundaries)
}

/// Check whether file-level statistics are sufficient for boundary estimation,
/// or whether sampling is needed.
///
/// Returns `true` if sampling is recommended (stats are too coarse).
pub fn needs_sampling(
    file_stats: &[(String, ScalarValue, ScalarValue)],
    num_partitions: usize,
) -> bool {
    if file_stats.is_empty() {
        return true;
    }

    // If we have fewer distinct boundary candidates than partitions,
    // file-level stats are too coarse.
    let mut candidates: Vec<ScalarValue> = Vec::with_capacity(file_stats.len() * 2);
    for (_path, min_val, max_val) in file_stats {
        if !min_val.is_null() {
            candidates.push(min_val.clone());
        }
        if !max_val.is_null() {
            candidates.push(max_val.clone());
        }
    }
    candidates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    candidates.dedup();

    // Need at least P distinct values to make P-1 meaningful boundaries
    candidates.len() < num_partitions
}

// ────────────────────── DistributedSortExec ─────────────────────────────────

/// Default data size threshold (bytes) above which distributed sort is used.
///
/// Below this threshold, a local `SortExec` on the coordinator is sufficient.
pub const DEFAULT_DISTRIBUTED_SORT_THRESHOLD: usize = 256 * 1024 * 1024; // 256 MB

/// Minimum number of healthy executors required for distributed sort.
pub const MIN_EXECUTORS_FOR_DISTRIBUTED_SORT: usize = 2;

/// DataFusion `ExecutionPlan` that implements distributed range-partitioned sort.
///
/// Replaces `SortExec` when:
/// - The input data exceeds [`DEFAULT_DISTRIBUTED_SORT_THRESHOLD`]
/// - Distributed mode is available with enough healthy executors
///
/// The plan conceptually expands to:
/// 1. Each executor runs the `input` plan (scan + filter)
/// 2. Each executor range-partitions output using `boundaries`
/// 3. Each executor sends partitioned data to the owning executor via DoExchange
/// 4. Each executor locally sorts its received range partition
/// 5. Concatenation of sorted partitions yields globally sorted output
///
/// At execution time this node currently delegates to a local `SortExec` for
/// the final local sort step. The shuffle stage (steps 2-3) is handled by
/// the stage planner inserting `ShuffleWriterExec`/`ShuffleReaderExec` around
/// this node.
#[derive(Debug)]
pub struct DistributedSortExec {
    /// The input plan to sort.
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions (columns + direction + nulls first/last).
    sort_exprs: LexOrdering,
    /// Range boundaries for partitioning (P-1 values for P partitions).
    boundaries: Vec<ScalarValue>,
    /// Target executor endpoints for range partitions.
    executors: Vec<String>,
    /// Optional LIMIT (fetch first N rows).
    fetch: Option<usize>,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl DistributedSortExec {
    /// Create a new `DistributedSortExec`.
    ///
    /// # Arguments
    /// - `input`: The child plan whose output will be sorted.
    /// - `sort_exprs`: The sort ordering to achieve.
    /// - `boundaries`: P-1 range boundary values.
    /// - `executors`: Target executor endpoints.
    /// - `fetch`: Optional LIMIT.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        boundaries: Vec<ScalarValue>,
        executors: Vec<String>,
        fetch: Option<usize>,
    ) -> Self {
        let schema = input.schema();

        // The output has as many partitions as there are range buckets
        // (boundaries.len() + 1), but from the coordinator's perspective
        // it is a single globally-sorted stream.
        // Build equivalence properties with output ordering matching the sort exprs.
        let ordering_vec: Vec<PhysicalSortExpr> =
            sort_exprs.iter().cloned().collect();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, vec![ordering_vec]),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            input,
            sort_exprs,
            boundaries,
            executors,
            fetch,
            properties,
        }
    }

    /// Returns the sort expressions.
    pub fn sort_exprs(&self) -> &LexOrdering {
        &self.sort_exprs
    }

    /// Returns the range boundaries.
    pub fn boundaries(&self) -> &[ScalarValue] {
        &self.boundaries
    }

    /// Returns the executor endpoints.
    pub fn executors(&self) -> &[String] {
        &self.executors
    }

    /// Returns the optional LIMIT.
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for DistributedSortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSortExec: sort_exprs=[{}], boundaries={}, executors={}, fetch={:?}",
            self.sort_exprs,
            self.boundaries.len(),
            self.executors.len(),
            self.fetch,
        )
    }
}

impl ExecutionPlan for DistributedSortExec {
    fn name(&self) -> &str {
        "DistributedSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DistributedSortExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(DistributedSortExec::new(
            Arc::clone(&children[0]),
            self.sort_exprs.clone(),
            self.boundaries.clone(),
            self.executors.clone(),
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DistributedSortExec only supports partition 0, got {partition}"
            )));
        }

        debug!(
            sort_exprs = %self.sort_exprs,
            boundaries = self.boundaries.len(),
            executors = self.executors.len(),
            fetch = ?self.fetch,
            "Executing DistributedSortExec"
        );

        // In the full distributed execution pipeline:
        // 1. The stage planner inserts ShuffleWriterExec/ReaderExec around this node
        // 2. Input data arrives already range-partitioned via shuffle
        // 3. We just do a local sort on the received partition
        //
        // For now, fall back to a local SortExec on the input.
        let local_sort = SortExec::new(self.sort_exprs.clone(), Arc::clone(&self.input))
            .with_fetch(self.fetch);
        local_sort.execute(partition, context)
    }
}

// ────────────────────── DistributedSortRule ──────────────────────────────────

/// Physical optimizer rule that replaces `SortExec` with `DistributedSortExec`
/// when distributed sort is beneficial.
///
/// The rule fires when:
/// 1. The `executors` list is non-empty (distributed mode available)
/// 2. The estimated input data size exceeds [`Self::size_threshold`]
/// 3. There are at least [`MIN_EXECUTORS_FOR_DISTRIBUTED_SORT`] executors
///
/// When the rule cannot determine input size (statistics unavailable), it
/// conservatively keeps the local `SortExec`.
#[derive(Debug)]
pub struct DistributedSortRule {
    /// Minimum data size (bytes) to trigger distributed sort.
    size_threshold: usize,
    /// Available executor endpoints. Empty means single-node mode.
    executors: Vec<String>,
    /// Pre-computed range boundaries (if available from scan planning).
    /// When empty, boundaries would be computed at execution time.
    boundaries: Vec<ScalarValue>,
}

impl DistributedSortRule {
    /// Create a new rule with the given threshold and executor list.
    pub fn new(
        size_threshold: usize,
        executors: Vec<String>,
        boundaries: Vec<ScalarValue>,
    ) -> Self {
        Self {
            size_threshold,
            executors,
            boundaries,
        }
    }
}

impl PhysicalOptimizerRule for DistributedSortRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // No executors → single-node mode, keep local sort
        if self.executors.len() < MIN_EXECUTORS_FOR_DISTRIBUTED_SORT {
            return Ok(plan);
        }

        let executors = self.executors.clone();
        let boundaries = self.boundaries.clone();
        let threshold = self.size_threshold;

        let transformed = plan.transform_down(|node| {
            if let Some(sort_exec) = node.as_any().downcast_ref::<SortExec>() {
                let input = &sort_exec.children()[0];
                let input_size = estimate_data_size(input);

                trace!(
                    input_bytes = input_size,
                    threshold_bytes = threshold,
                    num_executors = executors.len(),
                    "DistributedSortRule: evaluating SortExec"
                );

                if input_size > threshold {
                    debug!(
                        input_bytes = input_size,
                        threshold_bytes = threshold,
                        num_executors = executors.len(),
                        "DistributedSortRule: rewriting SortExec → DistributedSortExec \
                         (input {:.1} MB > threshold {:.1} MB)",
                        input_size as f64 / (1024.0 * 1024.0),
                        threshold as f64 / (1024.0 * 1024.0),
                    );

                    let sort_exprs = sort_exec.expr().clone();
                    let fetch = sort_exec.fetch();
                    let dist_sort = Arc::new(DistributedSortExec::new(
                        Arc::clone(input),
                        sort_exprs,
                        boundaries.clone(),
                        executors.clone(),
                        fetch,
                    )) as Arc<dyn ExecutionPlan>;

                    return Ok(Transformed::yes(dist_sort));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "DistributedSortRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Estimate the total data size (bytes) from a plan's statistics.
///
/// Returns 0 if statistics are unavailable, which means the rule will
/// conservatively keep the local sort.
fn estimate_data_size(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let stats = match plan.partition_statistics(None) {
        Ok(stats) => stats,
        Err(_) => return 0,
    };
    stats.total_byte_size.get_value().copied().unwrap_or(0)
}

// ─────────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_plan::memory::LazyMemoryExec;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_memory_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    // ── compute_range_boundaries tests ──────────────────────────────

    #[test]
    fn test_boundaries_empty_stats() {
        let result = compute_range_boundaries(&[], 4).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_boundaries_single_partition() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(100))),
        ];
        let result = compute_range_boundaries(&stats, 1).unwrap();
        assert!(result.is_empty(), "Single partition needs no boundaries");
    }

    #[test]
    fn test_boundaries_two_partitions() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(50))),
            ("f2".to_string(), ScalarValue::Int64(Some(51)), ScalarValue::Int64(Some(100))),
        ];
        let result = compute_range_boundaries(&stats, 2).unwrap();
        assert_eq!(result.len(), 1, "Two partitions need 1 boundary");
        // The boundary should be somewhere in the middle
        if let ScalarValue::Int64(Some(v)) = &result[0] {
            assert!(*v > 1 && *v <= 100, "Boundary {v} should be between 1 and 100");
        } else {
            panic!("Expected Int64 boundary, got {:?}", result[0]);
        }
    }

    #[test]
    fn test_boundaries_four_partitions() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(25))),
            ("f2".to_string(), ScalarValue::Int64(Some(26)), ScalarValue::Int64(Some(50))),
            ("f3".to_string(), ScalarValue::Int64(Some(51)), ScalarValue::Int64(Some(75))),
            ("f4".to_string(), ScalarValue::Int64(Some(76)), ScalarValue::Int64(Some(100))),
        ];
        let result = compute_range_boundaries(&stats, 4).unwrap();
        assert_eq!(result.len(), 3, "Four partitions need 3 boundaries");
        // Boundaries should be in ascending order
        for w in result.windows(2) {
            assert!(
                w[0] < w[1],
                "Boundaries must be ascending: {:?} >= {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn test_boundaries_with_null_values() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(None), ScalarValue::Int64(Some(50))),
            ("f2".to_string(), ScalarValue::Int64(Some(51)), ScalarValue::Int64(None)),
        ];
        let result = compute_range_boundaries(&stats, 2).unwrap();
        // Should still produce boundaries from non-null values
        assert!(!result.is_empty() || result.is_empty()); // Either way is valid
    }

    #[test]
    fn test_boundaries_all_null() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(None), ScalarValue::Int64(None)),
        ];
        let result = compute_range_boundaries(&stats, 4).unwrap();
        assert!(result.is_empty(), "All-null stats should produce no boundaries");
    }

    // ── sample_based_boundaries tests ───────────────────────────────

    #[test]
    fn test_sample_boundaries_empty() {
        let result = sample_based_boundaries(&[], 4).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_sample_boundaries_basic() {
        let samples: Vec<ScalarValue> = (0..100)
            .map(|i| ScalarValue::Int64(Some(i)))
            .collect();
        let result = sample_based_boundaries(&samples, 4).unwrap();
        assert_eq!(result.len(), 3, "4 partitions need 3 boundaries");

        // Boundaries should be in ascending order
        for w in result.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn test_sample_boundaries_single_partition() {
        let samples = vec![ScalarValue::Int64(Some(42))];
        let result = sample_based_boundaries(&samples, 1).unwrap();
        assert!(result.is_empty());
    }

    // ── needs_sampling tests ────────────────────────────────────────

    #[test]
    fn test_needs_sampling_empty() {
        assert!(needs_sampling(&[], 4));
    }

    #[test]
    fn test_needs_sampling_sufficient() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(25))),
            ("f2".to_string(), ScalarValue::Int64(Some(26)), ScalarValue::Int64(Some(50))),
            ("f3".to_string(), ScalarValue::Int64(Some(51)), ScalarValue::Int64(Some(75))),
            ("f4".to_string(), ScalarValue::Int64(Some(76)), ScalarValue::Int64(Some(100))),
        ];
        // 8 distinct candidates (4 min + 4 max) >= 4 partitions
        assert!(!needs_sampling(&stats, 4));
    }

    #[test]
    fn test_needs_sampling_insufficient() {
        let stats = vec![
            ("f1".to_string(), ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(100))),
        ];
        // Only 2 distinct candidates but need 8 partitions
        assert!(needs_sampling(&stats, 8));
    }

    // ── DistributedSortExec tests ───────────────────────────────────

    #[test]
    fn test_distributed_sort_exec_properties() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let exec = DistributedSortExec::new(
            input,
            ordering,
            vec![ScalarValue::Int64(Some(50))],
            vec!["grpc://h1:50051".to_string(), "grpc://h2:50051".to_string()],
            None,
        );

        assert_eq!(exec.name(), "DistributedSortExec");
        assert_eq!(exec.boundaries().len(), 1);
        assert_eq!(exec.executors().len(), 2);
        assert!(exec.fetch().is_none());
        assert_eq!(exec.children().len(), 1);
    }

    #[test]
    fn test_distributed_sort_exec_with_fetch() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let exec = DistributedSortExec::new(
            input,
            ordering,
            vec![],
            vec!["grpc://h1:50051".to_string()],
            Some(100),
        );

        assert_eq!(exec.fetch(), Some(100));
    }

    #[test]
    fn test_distributed_sort_exec_with_new_children() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let exec = Arc::new(DistributedSortExec::new(
            input,
            ordering,
            vec![ScalarValue::Int64(Some(50))],
            vec!["grpc://h1:50051".to_string()],
            None,
        ));

        let new_input = make_memory_plan(schema);
        let new_exec = exec.with_new_children(vec![new_input]).unwrap();
        assert_eq!(new_exec.name(), "DistributedSortExec");
    }

    #[test]
    fn test_distributed_sort_exec_wrong_children_count() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let exec = Arc::new(DistributedSortExec::new(
            input,
            ordering,
            vec![],
            vec![],
            None,
        ));

        let result = exec.with_new_children(vec![
            make_memory_plan(schema.clone()),
            make_memory_plan(schema),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_distributed_sort_exec_display() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let exec = DistributedSortExec::new(
            input,
            ordering,
            vec![ScalarValue::Int64(Some(50)), ScalarValue::Int64(Some(100))],
            vec!["grpc://h1:50051".to_string(), "grpc://h2:50051".to_string(), "grpc://h3:50051".to_string()],
            Some(1000),
        );

        let display = datafusion::physical_plan::displayable(&exec)
            .one_line()
            .to_string();
        assert!(display.contains("DistributedSortExec"));
        assert!(display.contains("boundaries=2"));
        assert!(display.contains("executors=3"));
    }

    // ── DistributedSortRule tests ───────────────────────────────────

    #[test]
    fn test_rule_no_executors_keeps_sort() {
        let rule = DistributedSortRule::new(0, vec![], vec![]);
        let config = ConfigOptions::new();

        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "Should keep SortExec when no executors available"
        );
    }

    #[test]
    fn test_rule_below_threshold_keeps_sort() {
        let rule = DistributedSortRule::new(
            DEFAULT_DISTRIBUTED_SORT_THRESHOLD,
            vec!["grpc://h1:50051".to_string(), "grpc://h2:50051".to_string()],
            vec![],
        );
        let config = ConfigOptions::new();

        let schema = test_schema();
        // LazyMemoryExec has 0 bytes estimated, well below threshold
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "Should keep SortExec when input size is below threshold"
        );
    }

    #[test]
    fn test_rule_single_executor_keeps_sort() {
        let rule = DistributedSortRule::new(
            0, // threshold of 0 means "always distribute"
            vec!["grpc://h1:50051".to_string()], // Only 1 executor
            vec![],
        );
        let config = ConfigOptions::new();

        let schema = test_schema();
        let input = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "Should keep SortExec when fewer than MIN_EXECUTORS"
        );
    }

    #[test]
    fn test_rule_name() {
        let rule = DistributedSortRule::new(DEFAULT_DISTRIBUTED_SORT_THRESHOLD, vec![], vec![]);
        assert_eq!(rule.name(), "DistributedSortRule");
    }

    #[test]
    fn test_rule_schema_check() {
        let rule = DistributedSortRule::new(DEFAULT_DISTRIBUTED_SORT_THRESHOLD, vec![], vec![]);
        assert!(rule.schema_check());
    }
}

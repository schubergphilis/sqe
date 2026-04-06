//! Stage decomposition for distributed query execution.
//!
//! A distributed query is broken into stages separated by shuffle boundaries.
//! Each stage contains a plan fragment that can be executed on a set of
//! executors. Stages form a DAG where edges represent data dependencies
//! (i.e., shuffles).
//!
//! The [`decompose_plan`] function walks a physical plan tree and splits it
//! into [`QueryStage`]s at shuffle boundaries. The returned stages are in
//! topological order (leaf stages first), ready for wave-based execution:
//!
//! - Wave 1: Leaf stages (scans, no input dependencies)
//! - Wave 2: Stages consuming Wave 1 output (joins, aggregates)
//! - Wave 3: Final result stage
//!
//! This module provides the framework. Streams 7 and 8 add specific
//! distributed sort/join planning on top.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use tracing::debug;

// ─────────────────────────── ShuffleType ─────────────────────────────────────

/// Describes the type of shuffle at a stage boundary.
#[derive(Debug, Clone)]
pub enum ShuffleType {
    /// Hash-partition on key columns into N buckets.
    Hash {
        key_columns: Vec<String>,
        num_partitions: usize,
    },
    /// Range-partition on a single key column using boundary values.
    Range {
        key_column: String,
        boundaries: Vec<ScalarValue>,
    },
    /// Broadcast the full output to all downstream executors.
    Broadcast,
}

impl std::fmt::Display for ShuffleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShuffleType::Hash {
                key_columns,
                num_partitions,
            } => write!(
                f,
                "Hash(keys=[{}], partitions={})",
                key_columns.join(", "),
                num_partitions
            ),
            ShuffleType::Range {
                key_column,
                boundaries,
            } => write!(
                f,
                "Range(key={}, boundaries={})",
                key_column,
                boundaries.len()
            ),
            ShuffleType::Broadcast => write!(f, "Broadcast"),
        }
    }
}

// ─────────────────────────── QueryStage ──────────────────────────────────────

/// A single stage in a distributed query execution plan.
///
/// Each stage contains a plan fragment (a sub-tree of the physical plan)
/// that can be executed independently on a set of executors. Stages are
/// connected by shuffle boundaries where data must be redistributed.
#[derive(Debug)]
pub struct QueryStage {
    /// Unique identifier for this stage within the query.
    pub stage_id: String,
    /// The physical plan fragment to execute on each assigned executor.
    pub plan_fragment: Arc<dyn ExecutionPlan>,
    /// IDs of stages that must complete before this stage can start.
    /// Empty for leaf stages (scans).
    pub input_stages: Vec<String>,
    /// How the output of this stage should be shuffled to downstream stages.
    /// `None` for the final result stage.
    pub shuffle_type: Option<ShuffleType>,
    /// Which executors should run this stage.
    /// Populated later by the scheduler; initially empty.
    pub assigned_executors: Vec<String>,
}

// ─────────────────────────── decompose_plan ──────────────────────────────────

/// Internal state for tracking stages during plan decomposition.
struct StageBuilder {
    /// Counter for generating stage IDs.
    next_id: usize,
    /// Accumulated stages in topological order.
    stages: Vec<QueryStage>,
}

impl StageBuilder {
    fn new() -> Self {
        Self {
            next_id: 0,
            stages: Vec::new(),
        }
    }

    fn next_stage_id(&mut self) -> String {
        let id = format!("stage_{}", self.next_id);
        self.next_id += 1;
        id
    }
}

/// Decompose a physical plan into stages separated by shuffle boundaries.
///
/// Walks the plan tree bottom-up. At each shuffle boundary (join or
/// sort that requires data redistribution), the sub-tree below the boundary
/// is split into a separate stage.
///
/// Returns a `Vec<QueryStage>` in topological order (leaf stages first,
/// final result stage last).
///
/// # Shuffle boundaries detected
///
/// - **`SortMergeJoinExec`**: Both sides need hash-shuffle on join keys.
/// - **`HashJoinExec`**: Build side (left) needs hash-shuffle on join keys.
/// - **`SortExec`**: If the input has multiple partitions, may need
///   range-shuffle for distributed sort (detected but not yet fully
///   planned — Streams 7-8 will refine this).
///
/// # Example
///
/// ```text
/// ProjectionExec
///   SortMergeJoinExec(a.id = b.id)
///     FilterExec(a.x > 10)           ← becomes stage_0
///       IcebergScanExec(table_a)
///     FilterExec(b.y < 20)           ← becomes stage_1
///       IcebergScanExec(table_b)
///   ↓ join + projection              ← becomes stage_2 (inputs: [stage_0, stage_1])
/// ```
pub fn decompose_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<QueryStage> {
    let mut builder = StageBuilder::new();
    let _final_plan = decompose_recursive(&plan, &mut builder);

    // The final stage is the root of the plan tree, containing whatever
    // remains after extracting shuffle-boundary sub-trees.
    let final_stage_id = builder.next_stage_id();
    let input_stage_ids: Vec<String> = builder.stages.iter().map(|s| s.stage_id.clone()).collect();

    debug!(
        stage_id = %final_stage_id,
        input_stages = ?input_stage_ids,
        num_prior_stages = builder.stages.len(),
        "Created final result stage"
    );

    builder.stages.push(QueryStage {
        stage_id: final_stage_id,
        plan_fragment: plan,
        input_stages: input_stage_ids,
        shuffle_type: None, // Final stage — no further shuffle
        assigned_executors: vec![],
    });

    builder.stages
}

/// Recursively walk the plan tree and extract shuffle-boundary sub-trees
/// into separate stages.
///
/// Returns a map of which child indices were extracted as separate stages,
/// keyed by the stage IDs. The caller uses this to know which children
/// have been replaced by `ShuffleReaderExec` placeholders.
fn decompose_recursive(
    plan: &Arc<dyn ExecutionPlan>,
    builder: &mut StageBuilder,
) -> HashMap<usize, String> {
    let mut extracted = HashMap::new();

    // Check if this node is a shuffle boundary
    if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        // SortMergeJoinExec: both sides need shuffle on join keys.
        let children = plan.children();
        let left = children[0];
        let right = children[1];

        // Extract join key column names from the SortMergeJoin's on() conditions
        let key_columns = extract_smj_key_columns(smj);

        // Recursively decompose children first
        decompose_recursive(left, builder);
        decompose_recursive(right, builder);

        // Create stages for left and right inputs
        let left_stage_id = builder.next_stage_id();
        debug!(
            stage_id = %left_stage_id,
            plan_name = left.name(),
            shuffle = "Hash",
            side = "left",
            "Created stage for SortMergeJoin left input"
        );
        builder.stages.push(QueryStage {
            stage_id: left_stage_id.clone(),
            plan_fragment: Arc::clone(left),
            input_stages: vec![],
            shuffle_type: Some(ShuffleType::Hash {
                key_columns: key_columns.clone(),
                num_partitions: 0, // Determined by scheduler
            }),
            assigned_executors: vec![],
        });

        let right_stage_id = builder.next_stage_id();
        debug!(
            stage_id = %right_stage_id,
            plan_name = right.name(),
            shuffle = "Hash",
            side = "right",
            "Created stage for SortMergeJoin right input"
        );
        builder.stages.push(QueryStage {
            stage_id: right_stage_id.clone(),
            plan_fragment: Arc::clone(right),
            input_stages: vec![],
            shuffle_type: Some(ShuffleType::Hash {
                key_columns,
                num_partitions: 0,
            }),
            assigned_executors: vec![],
        });

        extracted.insert(0, left_stage_id);
        extracted.insert(1, right_stage_id);
    } else if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        // HashJoinExec: build side (left) needs shuffle on join keys.
        let children = plan.children();
        let left = children[0];

        let key_columns = extract_hj_key_columns(hj);

        // Recursively decompose build side
        decompose_recursive(left, builder);

        let left_stage_id = builder.next_stage_id();
        debug!(
            stage_id = %left_stage_id,
            plan_name = left.name(),
            shuffle = "Hash",
            side = "build",
            "Created stage for HashJoin build side"
        );
        builder.stages.push(QueryStage {
            stage_id: left_stage_id.clone(),
            plan_fragment: Arc::clone(left),
            input_stages: vec![],
            shuffle_type: Some(ShuffleType::Hash {
                key_columns,
                num_partitions: 0,
            }),
            assigned_executors: vec![],
        });

        extracted.insert(0, left_stage_id);

        // Also recurse into probe side (right), which may have its own boundaries
        let right = children[1];
        decompose_recursive(right, builder);
    } else if let Some(_sort) = plan.as_any().downcast_ref::<SortExec>() {
        // SortExec with multiple input partitions may need range-shuffle
        // for distributed sort. For now, just mark it as a potential
        // boundary — the actual range boundary computation is in Stream 7.
        let children = plan.children();
        if !children.is_empty() {
            let input = children[0];
            let input_partitions = input.properties().partitioning.partition_count();

            if input_partitions > 1 {
                // Multi-partition sort → potential distributed sort boundary
                decompose_recursive(input, builder);

                let sort_key = extract_sort_key_column(_sort);
                let input_stage_id = builder.next_stage_id();
                debug!(
                    stage_id = %input_stage_id,
                    plan_name = input.name(),
                    shuffle = "Range",
                    input_partitions = input_partitions,
                    sort_key = ?sort_key,
                    "Created stage for distributed sort input"
                );
                builder.stages.push(QueryStage {
                    stage_id: input_stage_id.clone(),
                    plan_fragment: Arc::clone(input),
                    input_stages: vec![],
                    shuffle_type: sort_key.map(|key| ShuffleType::Range {
                        key_column: key,
                        boundaries: vec![], // Computed at runtime via sampling
                    }),
                    assigned_executors: vec![],
                });

                extracted.insert(0, input_stage_id);
            } else {
                // Single-partition sort — no shuffle needed, just recurse
                decompose_recursive(input, builder);
            }
        }
    } else {
        // Not a shuffle boundary — recurse into all children
        for child in plan.children() {
            decompose_recursive(child, builder);
        }
    }

    extracted
}

// ─────────────────────── Helper functions ─────────────────────────────────────

/// Extract join key column names from a `SortMergeJoinExec`.
///
/// Returns the column names used as join keys on both sides.
/// Falls back to string representation if column extraction fails.
fn extract_smj_key_columns(smj: &SortMergeJoinExec) -> Vec<String> {
    smj.on()
        .iter()
        .flat_map(|(left, right)| {
            vec![format!("{left}"), format!("{right}")]
        })
        .collect()
}

/// Extract join key column names from a `HashJoinExec`.
fn extract_hj_key_columns(hj: &HashJoinExec) -> Vec<String> {
    hj.on()
        .iter()
        .flat_map(|(left, right)| {
            vec![format!("{left}"), format!("{right}")]
        })
        .collect()
}

/// Extract the primary sort key column name from a `SortExec`.
///
/// Returns the first sort expression's column name if available.
fn extract_sort_key_column(sort: &SortExec) -> Option<String> {
    sort.expr()
        .iter()
        .next()
        .map(|expr| format!("{}", expr.expr))
}

// ─────────────────────────── Topological ordering ────────────────────────────

/// Compute execution waves from stages.
///
/// Each wave contains stages that can execute in parallel. A stage is
/// in wave N if all its input stages are in waves < N.
///
/// Returns a `Vec<Vec<&QueryStage>>` where index = wave number.
pub fn compute_waves(stages: &[QueryStage]) -> Vec<Vec<&QueryStage>> {
    if stages.is_empty() {
        return vec![];
    }

    // Build a map from stage_id → wave number
    let mut wave_map: HashMap<&str, usize> = HashMap::new();

    // Since stages are in topological order, we can compute waves in a single pass
    for stage in stages {
        let wave = if stage.input_stages.is_empty() {
            0
        } else {
            stage
                .input_stages
                .iter()
                .filter_map(|dep| wave_map.get(dep.as_str()))
                .copied()
                .max()
                .map(|w| w + 1)
                .unwrap_or(0)
        };
        wave_map.insert(&stage.stage_id, wave);
    }

    // Group stages by wave
    let max_wave = wave_map.values().copied().max().unwrap_or(0);
    let mut waves: Vec<Vec<&QueryStage>> = vec![vec![]; max_wave + 1];
    for stage in stages {
        let wave = wave_map[stage.stage_id.as_str()];
        waves[wave].push(stage);
    }

    waves
}

// ─────────────────────────────── Tests ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use std::fmt;

    use datafusion::error::Result as DFResult;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Partitioning, PlanProperties,
    };
    use std::any::Any;

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

    /// A trivial plan with configurable partition count for testing.
    #[derive(Debug)]
    struct MockScanExec {
        schema: SchemaRef,
        properties: PlanProperties,
    }

    impl MockScanExec {
        fn new(schema: SchemaRef, num_partitions: usize) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(num_partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self { schema, properties }
        }
    }

    impl DisplayAs for MockScanExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockScanExec")
        }
    }

    impl ExecutionPlan for MockScanExec {
        fn name(&self) -> &str {
            "MockScanExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &PlanProperties {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            unimplemented!("MockScanExec::execute not needed for planning tests")
        }
    }

    fn make_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", left_schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", right_schema).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }

    fn make_sort_merge_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_plan::sorts::sort::SortExec;

        let on = vec![(
            datafusion::physical_expr::expressions::col("id", left_schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", right_schema).unwrap(),
        )];
        let sort_options: Vec<SortOptions> = on.iter().map(|_| SortOptions::default()).collect();

        // Wrap both sides in SortExec as required by SMJ
        let left_sort_expr = datafusion::physical_expr::PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", left_schema).unwrap(),
            SortOptions::default(),
        );
        let right_sort_expr = datafusion::physical_expr::PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", right_schema).unwrap(),
            SortOptions::default(),
        );

        let left_ordering =
            datafusion::physical_expr::LexOrdering::new(vec![left_sort_expr]).unwrap();
        let right_ordering =
            datafusion::physical_expr::LexOrdering::new(vec![right_sort_expr]).unwrap();

        let sorted_left: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(left_ordering, left));
        let sorted_right: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(right_ordering, right));

        Arc::new(
            SortMergeJoinExec::try_new(
                sorted_left,
                sorted_right,
                on,
                None,
                JoinType::Inner,
                sort_options,
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }

    // ─── decompose_plan tests ───

    #[test]
    fn test_simple_scan_one_stage() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);

        let stages = decompose_plan(plan);

        // A simple scan should produce exactly 1 stage (the final result stage)
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].stage_id, "stage_0");
        assert!(stages[0].input_stages.is_empty());
        assert!(stages[0].shuffle_type.is_none());
    }

    #[test]
    fn test_hash_join_creates_build_side_stage() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let join = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let stages = decompose_plan(join);

        // Should have: stage_0 (build side) + stage_1 (final with join)
        assert_eq!(stages.len(), 2);

        // First stage is the build side
        assert_eq!(stages[0].stage_id, "stage_0");
        assert!(stages[0].input_stages.is_empty());
        assert!(stages[0].shuffle_type.is_some());
        match &stages[0].shuffle_type {
            Some(ShuffleType::Hash { key_columns, .. }) => {
                assert!(!key_columns.is_empty());
            }
            other => panic!("Expected Hash shuffle type, got {other:?}"),
        }

        // Final stage references the build side
        assert_eq!(stages[1].stage_id, "stage_1");
        assert_eq!(stages[1].input_stages, vec!["stage_0"]);
        assert!(stages[1].shuffle_type.is_none()); // Final stage
    }

    #[test]
    fn test_sort_merge_join_creates_two_input_stages() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let join = make_sort_merge_join(left, right, &schema, &schema);

        let stages = decompose_plan(join);

        // Should have: stage_0 (left sort), stage_1 (right sort), stage_2 (final with SMJ)
        // Note: The SortExecs wrapping the inputs may also be detected as boundaries,
        // but since they have 0 input partitions (LazyMemoryExec empty), they won't
        // trigger the multi-partition condition.
        assert!(
            stages.len() >= 3,
            "Expected at least 3 stages for SMJ, got {}",
            stages.len()
        );

        // Final stage should reference prior stages
        let final_stage = stages.last().unwrap();
        assert!(
            !final_stage.input_stages.is_empty(),
            "Final stage should have input dependencies"
        );
    }

    #[test]
    fn test_sort_with_multi_partition_input() {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

        let schema = test_schema();
        // Create a plan with multiple partitions
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(MockScanExec::new(schema.clone(), 4));

        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let stages = decompose_plan(sort);

        // Should have: stage_0 (input with Range shuffle) + stage_1 (final with sort)
        assert_eq!(stages.len(), 2);

        // First stage should have Range shuffle type
        match &stages[0].shuffle_type {
            Some(ShuffleType::Range { key_column, .. }) => {
                assert!(
                    key_column.contains("id"),
                    "Expected sort key to contain 'id', got '{key_column}'"
                );
            }
            other => panic!("Expected Range shuffle type for multi-partition sort, got {other:?}"),
        }
    }

    #[test]
    fn test_sort_with_single_partition_no_stage() {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

        let schema = test_schema();
        // Single-partition input — no shuffle needed
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(MockScanExec::new(schema.clone(), 1));

        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));

        let stages = decompose_plan(sort);

        // Only the final result stage — no additional stage for single-partition sort
        assert_eq!(stages.len(), 1);
        assert!(stages[0].shuffle_type.is_none());
    }

    // ─── compute_waves tests ───

    #[test]
    fn test_compute_waves_empty() {
        let waves = compute_waves(&[]);
        assert!(waves.is_empty());
    }

    #[test]
    fn test_compute_waves_single_stage() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);
        let stages = decompose_plan(plan);

        let waves = compute_waves(&stages);
        assert_eq!(waves.len(), 1);
        assert_eq!(waves[0].len(), 1);
    }

    #[test]
    fn test_compute_waves_hash_join() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let join = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let stages = decompose_plan(join);
        let waves = compute_waves(&stages);

        // Wave 0: build side stage (no dependencies)
        // Wave 1: final stage (depends on build side)
        assert_eq!(waves.len(), 2);
        assert_eq!(waves[0].len(), 1); // build side
        assert_eq!(waves[1].len(), 1); // final
    }

    #[test]
    fn test_compute_waves_smj_parallel_inputs() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let join = make_sort_merge_join(left, right, &schema, &schema);

        let stages = decompose_plan(join);
        let waves = compute_waves(&stages);

        // The two input stages (left sort, right sort) should be in the same wave
        // since they have no dependencies on each other.
        // Wave 0: both input stages
        // Wave 1: final stage
        assert!(
            waves.len() >= 2,
            "Expected at least 2 waves, got {}",
            waves.len()
        );

        // First wave should have the independent input stages
        assert!(
            waves[0].len() >= 2,
            "Expected at least 2 stages in wave 0 (parallel inputs), got {}",
            waves[0].len()
        );
    }

    // ─── ShuffleType display tests ───

    #[test]
    fn test_shuffle_type_display() {
        let hash = ShuffleType::Hash {
            key_columns: vec!["id".to_string(), "name".to_string()],
            num_partitions: 8,
        };
        assert_eq!(format!("{hash}"), "Hash(keys=[id, name], partitions=8)");

        let range = ShuffleType::Range {
            key_column: "ts".to_string(),
            boundaries: vec![ScalarValue::Int64(Some(100)), ScalarValue::Int64(Some(200))],
        };
        assert_eq!(format!("{range}"), "Range(key=ts, boundaries=2)");

        let broadcast = ShuffleType::Broadcast;
        assert_eq!(format!("{broadcast}"), "Broadcast");
    }

    // ─── Helper function tests ───

    #[test]
    fn test_extract_hj_key_columns() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
        )];
        let hj = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let keys = extract_hj_key_columns(&hj);
        assert!(!keys.is_empty());
        // Should contain string representations of the join key columns
        assert!(
            keys.iter().any(|k| k.contains("id")),
            "Expected 'id' in key columns, got {keys:?}"
        );
    }
}

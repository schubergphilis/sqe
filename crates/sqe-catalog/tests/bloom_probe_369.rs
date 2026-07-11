//! Issue #369: runtime-filter-to-bloom row-group pruning.
//!
//! Three layers under test:
//! 1. The vendored `SbbfRowGroupEvaluator` (membership conjunct
//!    collection + SBBF probing) -- tested here because the vendored
//!    crate's standalone test target does not compile (see the vendor
//!    README).
//! 2. The vendored reader hook, end-to-end: write a parquet file with
//!    bloom filters, scan it with an `IN` predicate, and assert the
//!    `row_groups_pruned_bloom` scan metric and the returned rows.
//! 3. The CASE-of-InLists union in `physical_to_predicate` that lets a
//!    PARTITIONED hash join's sealed dynamic filter reach the bloom
//!    (and stats) pruning paths as a single `Predicate::Set`.

use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

use iceberg::expr::sbbf_row_group_evaluator::{BloomProbeConjunct, SbbfRowGroupEvaluator};
use iceberg::expr::{Bind, Predicate, Reference};
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
use parquet::bloom_filter::Sbbf;

fn test_schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "other", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap(),
    )
}

/// iceberg field id -> parquet leaf index for [`test_schema`].
fn test_field_id_map() -> HashMap<i32, usize> {
    HashMap::from([(1, 0), (2, 1)])
}

// ---------------------------------------------------------------------------
// 1. SbbfRowGroupEvaluator: conjunct collection
// ---------------------------------------------------------------------------

#[test]
fn collect_gathers_in_sets_and_eq_under_and() {
    let schema = test_schema();
    let pred = Reference::new("id")
        .is_in([Datum::long(1), Datum::long(2)])
        .and(Reference::new("other").equal_to(Datum::long(7)))
        .bind(schema, true)
        .unwrap();

    let conjuncts = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 65536);
    assert_eq!(conjuncts.len(), 2);
    let by_col: HashMap<usize, usize> = conjuncts
        .iter()
        .map(|c| (c.parquet_column_index, c.literals.len()))
        .collect();
    assert_eq!(by_col[&0], 2, "IN set on id (leaf 0) carries both keys");
    assert_eq!(by_col[&1], 1, "eq on other (leaf 1) is a singleton");
}

#[test]
fn collect_ignores_or_subtrees() {
    let schema = test_schema();
    let pred = Reference::new("id")
        .is_in([Datum::long(1)])
        .or(Reference::new("other").equal_to(Datum::long(7)))
        .bind(schema, true)
        .unwrap();

    let conjuncts = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 65536);
    assert!(
        conjuncts.is_empty(),
        "membership under OR is not a required conjunct"
    );
}

#[test]
fn collect_ignores_negated_membership() {
    let schema = test_schema();
    // The shape equality-delete predicates take: NOT(match) rewritten
    // to NotIn / NotEq. Treating these as membership would prune row
    // groups that must be scanned.
    let pred = Reference::new("id")
        .is_in([Datum::long(1), Datum::long(2)])
        .not()
        .rewrite_not()
        .bind(schema.clone(), true)
        .unwrap();
    let conjuncts = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 65536);
    assert!(conjuncts.is_empty(), "NotIn must not be collected");

    let pred = Reference::new("other")
        .equal_to(Datum::long(7))
        .not()
        .rewrite_not()
        .bind(schema, true)
        .unwrap();
    let conjuncts = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 65536);
    assert!(conjuncts.is_empty(), "NotEq must not be collected");
}

#[test]
fn collect_respects_max_values_cap() {
    let schema = test_schema();
    let pred = Reference::new("id")
        .is_in([Datum::long(1), Datum::long(2), Datum::long(3)])
        .bind(schema, true)
        .unwrap();

    let capped = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 2);
    assert!(capped.is_empty(), "3-key set above cap 2 is dropped");
    let uncapped = SbbfRowGroupEvaluator::collect(&pred, &test_field_id_map(), 3);
    assert_eq!(uncapped.len(), 1);
}

#[test]
fn collect_skips_unmapped_columns() {
    let schema = test_schema();
    let pred = Reference::new("id")
        .is_in([Datum::long(1)])
        .bind(schema, true)
        .unwrap();

    // Field id 1 missing from the map (e.g. column absent from the file).
    let map = HashMap::from([(2, 1)]);
    assert!(SbbfRowGroupEvaluator::collect(&pred, &map, 65536).is_empty());
}

// ---------------------------------------------------------------------------
// 1b. SbbfRowGroupEvaluator: probing a real SBBF
// ---------------------------------------------------------------------------

fn sbbf_with_longs(values: &[i64]) -> Sbbf {
    let mut sbbf = Sbbf::new_with_ndv_fpp(values.len().max(8) as u64, 0.01).unwrap();
    for v in values {
        sbbf.insert(v);
    }
    sbbf
}

#[test]
fn all_absent_prunes_only_when_every_key_is_negative() {
    let sbbf = sbbf_with_longs(&[1, 2, 3]);

    let all_negative = BloomProbeConjunct {
        parquet_column_index: 0,
        literals: vec![Datum::long(10), Datum::long(11)],
    };
    assert!(all_negative.all_absent(&sbbf), "all keys absent -> prune");

    let one_positive = BloomProbeConjunct {
        parquet_column_index: 0,
        literals: vec![Datum::long(10), Datum::long(2)],
    };
    assert!(
        !one_positive.all_absent(&sbbf),
        "a single bloom-positive key keeps the row group"
    );
}

#[test]
fn all_absent_keeps_on_untestable_literal_types() {
    let sbbf = sbbf_with_longs(&[1, 2, 3]);
    let conjunct = BloomProbeConjunct {
        parquet_column_index: 0,
        literals: vec![Datum::long(10), Datum::bool(true)],
    };
    assert!(
        !conjunct.all_absent(&sbbf),
        "an unprobeable key makes the conjunct inconclusive -> keep"
    );
}

#[test]
fn all_absent_never_prunes_on_empty_literals() {
    let sbbf = sbbf_with_longs(&[1]);
    let conjunct = BloomProbeConjunct {
        parquet_column_index: 0,
        literals: vec![],
    };
    assert!(!conjunct.all_absent(&sbbf));
}

// ---------------------------------------------------------------------------
// 2. End-to-end: reader prunes bloom-negative row groups and reports it
// ---------------------------------------------------------------------------

mod reader_e2e {
    use super::*;

    use arrow_array::{Int64Array, RecordBatch};
    use futures::TryStreamExt;
    use iceberg::arrow::ArrowReaderBuilder;
    use iceberg::io::FileIOBuilder;
    use iceberg::scan::{FileScanTask, FileScanTaskStream};
    use iceberg::spec::{DataContentType, DataFileFormat};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::properties::WriterProperties;

    /// Write a two-row-group parquet file of EVEN ids: row group 0
    /// holds 0,2,..,198 and row group 1 holds 2000,2002,..,2198. Odd
    /// probe keys inside those ranges pass min/max stats pruning (the
    /// IN set is under the 200-literal stats limit) and only the bloom
    /// filter can prove them absent -- exactly the sealed runtime
    /// filter case this feature targets.
    fn write_test_file(dir: &std::path::Path, with_blooms: bool) -> String {
        let arrow_field = arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )]));
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_field]));

        let mut props = WriterProperties::builder().set_max_row_group_row_count(Some(100));
        if with_blooms {
            props = props.set_bloom_filter_enabled(true);
        }
        let path = dir.join("data.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.build())).unwrap();
        let ids: Vec<i64> = (0..100)
            .map(|i| i * 2)
            .chain((1000..1100).map(|i| i * 2))
            .collect();
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(ids)) as arrow_array::ArrayRef],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    fn file_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .unwrap(),
        )
    }

    fn scan_task(path: &str, schema: Arc<Schema>, predicate: Predicate) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: std::fs::metadata(path).unwrap().len(),
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: path.to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: Some(predicate.bind(schema, true).unwrap()),
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
            sequence_number: 0,
            equality_ids: None,
        }
    }

    /// Run one scan; returns (total rows, row_groups_pruned_bloom).
    async fn run_scan(reader_builder: ArrowReaderBuilder, task: FileScanTask) -> (usize, u64) {
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let scan_result = reader_builder.build().read(tasks).unwrap();
        let metrics = scan_result.metrics().clone();
        let batches: Vec<RecordBatch> = scan_result.try_collect().await.unwrap();
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        (rows, metrics.row_groups_pruned_bloom())
    }

    fn reader_builder() -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(FileIOBuilder::new_fs_io().build().unwrap())
            .with_data_file_concurrency_limit(1)
    }

    #[tokio::test]
    async fn prunes_all_row_groups_when_every_key_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), true);
        // 5 sits inside row group 0's [0, 198] and 2001 inside row
        // group 1's [2000, 2198]: min/max stats keep both row groups,
        // but both keys are odd and therefore bloom-negative.
        let pred = Reference::new("id").is_in([Datum::long(5), Datum::long(2001)]);

        let (rows, pruned) = run_scan(reader_builder(), scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 0);
        assert_eq!(pruned, 2, "both row groups bloom-negative for both keys");
    }

    #[tokio::test]
    async fn keeps_row_groups_containing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), true);
        // 4 lives in row group 0 (bloom-positive there, keeps it); 2001
        // sits inside row group 1's min/max but both keys are
        // bloom-negative for row group 1, so it gets pruned.
        let pred = Reference::new("id").is_in([Datum::long(4), Datum::long(2001)]);

        let (rows, pruned) = run_scan(reader_builder(), scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 1, "the matching row comes back");
        assert_eq!(pruned, 1, "only the keyless row group is pruned");
    }

    #[tokio::test]
    async fn missing_blooms_keep_all_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), false);
        let pred = Reference::new("id").is_in([Datum::long(5), Datum::long(2001)]);

        let (rows, pruned) = run_scan(reader_builder(), scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 0, "row filter still removes non-matching rows");
        assert_eq!(pruned, 0, "no blooms -> nothing pruned by the probe");
    }

    #[tokio::test]
    async fn toggle_off_disables_probing() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), true);
        let pred = Reference::new("id").is_in([Datum::long(5), Datum::long(2001)]);

        let builder = reader_builder().with_bloom_filter_probing_enabled(false);
        let (rows, pruned) = run_scan(builder, scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 0);
        assert_eq!(pruned, 0, "probe disabled -> no bloom pruning");
    }

    #[tokio::test]
    async fn cap_disables_probing_for_oversized_sets() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), true);
        let pred = Reference::new("id").is_in([Datum::long(5), Datum::long(2001)]);

        let builder = reader_builder().with_bloom_probe_max_values(1);
        let (rows, pruned) = run_scan(builder, scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 0);
        assert_eq!(pruned, 0, "2-key set above cap 1 is not probed");
    }

    #[tokio::test]
    async fn membership_under_or_never_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_file(dir.path(), true);
        // The IN set is bloom-negative everywhere, but it sits under an
        // OR whose other branch matches every row. Bloom pruning must
        // not fire; all 200 rows come back.
        let pred = Reference::new("id")
            .is_in([Datum::long(5), Datum::long(2001)])
            .or(Reference::new("id").greater_than_or_equal_to(Datum::long(0)));

        let (rows, pruned) = run_scan(reader_builder(), scan_task(&path, file_schema(), pred)).await;
        assert_eq!(rows, 200, "OR branch admits every row");
        assert_eq!(pruned, 0, "membership under OR is never probed");
    }
}

// ---------------------------------------------------------------------------
// 3. physical_to_predicate: CASE-of-InLists union
// ---------------------------------------------------------------------------

mod case_union {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::physical_expr::expressions::{lit, CaseExpr, Column};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::scalar::ScalarValue;
    use iceberg_datafusion::physical_plan::physical_to_predicate::convert_physical_filters_to_predicate;

    fn arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("other", DataType::Int64, false),
        ])
    }

    fn col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn int_lit(v: i64) -> Arc<dyn PhysicalExpr> {
        lit(ScalarValue::Int64(Some(v)))
    }

    fn in_list_expr(column: &str, index: usize, values: &[i64]) -> Arc<dyn PhysicalExpr> {
        datafusion::physical_expr::expressions::in_list(
            col(column, index),
            values.iter().map(|v| int_lit(*v)).collect(),
            &false,
            &arrow_schema(),
        )
        .unwrap()
    }

    /// The sealed shape of a partitioned hash join dynamic filter:
    /// `CASE <selector> WHEN <p> THEN <arm> ... ELSE <else> END`.
    fn case_expr(
        arms: Vec<Arc<dyn PhysicalExpr>>,
        else_expr: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        let when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = arms
            .into_iter()
            .enumerate()
            .map(|(i, arm)| (int_lit(i as i64), arm))
            .collect();
        Arc::new(CaseExpr::try_new(Some(col("id", 0)), when_then, Some(else_expr)).unwrap())
    }

    fn convert(expr: Arc<dyn PhysicalExpr>) -> Option<Predicate> {
        convert_physical_filters_to_predicate(&[expr])
    }

    #[test]
    fn unions_multi_arm_in_lists_into_one_set() {
        let case = case_expr(
            vec![
                in_list_expr("id", 0, &[1, 2]),
                in_list_expr("id", 0, &[3]),
                in_list_expr("id", 0, &[2, 4]),
            ],
            lit(ScalarValue::Boolean(Some(false))),
        );
        let expected = Reference::new("id").is_in([
            Datum::long(1),
            Datum::long(2),
            Datum::long(3),
            Datum::long(4),
        ]);
        assert_eq!(convert(case), Some(expected));
    }

    #[test]
    fn empty_partition_arms_contribute_nothing() {
        // DataFusion seals empty build partitions as `WHEN p THEN false`.
        let case = case_expr(
            vec![
                lit(ScalarValue::Boolean(Some(false))),
                in_list_expr("id", 0, &[7, 8]),
            ],
            lit(ScalarValue::Boolean(Some(false))),
        );
        let expected = Reference::new("id").is_in([Datum::long(7), Datum::long(8)]);
        assert_eq!(convert(case), Some(expected));
    }

    #[test]
    fn all_false_arms_convert_to_always_false() {
        let case = case_expr(
            vec![
                lit(ScalarValue::Boolean(Some(false))),
                lit(ScalarValue::Boolean(Some(false))),
            ],
            lit(ScalarValue::Boolean(Some(false))),
        );
        assert_eq!(convert(case), Some(Predicate::AlwaysFalse));
    }

    #[test]
    fn mixed_column_arms_degrade_to_none() {
        // Arm 1 constrains `id`, arm 2 constrains `other`: no column is
        // constrained by every arm, so no sound union exists.
        let case = case_expr(
            vec![
                in_list_expr("id", 0, &[1]),
                in_list_expr("other", 1, &[2]),
            ],
            lit(ScalarValue::Boolean(Some(false))),
        );
        assert_eq!(convert(case), None);
    }

    #[test]
    fn unbounded_arm_degrades_to_none() {
        // `lit(true)` arm admits arbitrary rows (bounds-less seal).
        let case = case_expr(
            vec![
                in_list_expr("id", 0, &[1]),
                lit(ScalarValue::Boolean(Some(true))),
            ],
            lit(ScalarValue::Boolean(Some(false))),
        );
        assert_eq!(convert(case), None);
    }

    #[test]
    fn non_false_else_degrades_to_none() {
        // The canceled-unknown seal uses `ELSE true`: rows from unknown
        // partitions pass freely, so no membership bound exists.
        let case = case_expr(
            vec![in_list_expr("id", 0, &[1])],
            lit(ScalarValue::Boolean(Some(true))),
        );
        assert_eq!(convert(case), None);
    }

    #[test]
    fn bounds_conjuncts_inside_arms_are_ignored_safely() {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::BinaryExpr;

        // Arm shape with bounds: `id IN (1,2) AND id >= 1 AND id <= 2`.
        let bounds = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(col("id", 0), Operator::GtEq, int_lit(1))),
            Operator::And,
            Arc::new(BinaryExpr::new(col("id", 0), Operator::LtEq, int_lit(2))),
        ));
        let arm = Arc::new(BinaryExpr::new(
            in_list_expr("id", 0, &[1, 2]),
            Operator::And,
            bounds,
        ));
        let case = case_expr(
            vec![arm, in_list_expr("id", 0, &[9])],
            lit(ScalarValue::Boolean(Some(false))),
        );
        // The eq-style bounds comparisons on `id` may not add keys, but
        // the IN sets union across arms.
        let expected = Reference::new("id").is_in([
            Datum::long(1),
            Datum::long(2),
            Datum::long(9),
        ]);
        assert_eq!(convert(case), Some(expected));
    }

    #[test]
    fn plain_in_list_still_converts() {
        // Regression guard: the CollectLeft (non-partitioned) seal.
        let expected = Reference::new("id").is_in([Datum::long(1), Datum::long(2)]);
        assert_eq!(convert(in_list_expr("id", 0, &[1, 2])), Some(expected));
    }
}

// ---------------------------------------------------------------------------
// 4. Config plumbing
// ---------------------------------------------------------------------------

mod config {
    use sqe_core::config::RuntimeFiltersConfig;

    #[test]
    fn bloom_defaults() {
        let cfg = RuntimeFiltersConfig::default();
        assert!(cfg.bloom_probe, "bloom probing defaults on");
        assert_eq!(cfg.bloom_max_values, 65536);

        // Missing keys deserialize to the same defaults.
        let parsed: RuntimeFiltersConfig = toml::from_str("").unwrap();
        assert!(parsed.bloom_probe);
        assert_eq!(parsed.bloom_max_values, 65536);
    }

    #[test]
    fn bloom_overrides_parse() {
        let parsed: RuntimeFiltersConfig =
            toml::from_str("bloom_probe = false\nbloom_max_values = 128").unwrap();
        assert!(!parsed.bloom_probe);
        assert_eq!(parsed.bloom_max_values, 128);
    }
}

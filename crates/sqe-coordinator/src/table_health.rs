//! Pure analysis for `CALL system.table_health` (Phase 4a advisory
//! compaction, Task 3).
//!
//! `table_health` is read-only: it reports compaction debt for a table
//! without rewriting anything. It reuses the exact helpers
//! `rewrite_data_files` uses to plan a real rewrite
//! (`collect_live_data_files`, `collect_live_delete_files`,
//! `plan_delete_aware_read`, `delete_heavy_files`,
//! `pack_file_groups_partition_aware`, all in `crate::maintenance`). The
//! later advisory scheduler task reuses [`analyze_table_health`] directly to
//! emit per-table metrics without going through the `CALL` surface.
//!
//! `eligible_groups` / `est_rewrite_bytes` report the PURE bin-pack count
//! (`pack_file_groups_partition_aware` with no forced inclusions, filtered on
//! `min_input_files`) and therefore UNDER-count relative to a subsequent
//! `rewrite_data_files(delete_file_threshold => N)` call: that call also
//! force-includes and rewrites any group containing a delete-heavy file even
//! when the group is smaller than `min_input_files`. `delete_heavy_files`
//! (this struct's own field) is exactly that set; a caller that wants the
//! full picture should treat "small-file debt" (`eligible_groups`) and
//! "delete-heavy debt" (`delete_heavy_files`) as two separate signals rather
//! than assuming the former subsumes the latter.
//!
//! [`analyze_table_health`] itself is pure: it takes already-collected
//! `DataFile`/delete/task data and never touches the catalog, object store,
//! or a live `TableScan`. That is what makes it unit-testable with synthetic
//! files instead of a docker-backed integration test.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::scan::FileScanTask;
use iceberg::spec::DataFile;

use sqe_core::config::MaintenanceCompactionConfig;

/// Table property that opts a table into the (later) advisory scheduler.
/// `table_health` reports whether the scheduler would even consider this
/// table; the scheduler itself does not exist yet in Phase 4a.
pub(crate) const MAINTENANCE_ENABLED_PROPERTY: &str = "sqe.maintenance.enabled";

/// Compaction-debt snapshot for one table.
///
/// Read-only: computed entirely from already-collected file/delete/task
/// data. Never loads, scans, or mutates anything itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableHealth {
    /// Number of live data files in the current snapshot.
    pub live_data_files: u64,
    /// Number of live data files below `target_file_size_bytes`.
    pub small_files: u64,
    /// Mean data file size in bytes (0 when there are no data files).
    pub avg_file_bytes: u64,
    /// Median data file size in bytes (0 when there are no data files).
    pub p50_file_bytes: u64,
    /// Number of live delete files (position + equality).
    pub delete_files: u64,
    /// Number of data files with at least `delete_file_threshold` distinct
    /// delete files applying to them.
    pub delete_heavy_files: u64,
    /// Number of pure bin-pack groups meeting `min_input_files`
    /// (`group.len() >= min_input_files`, no forced inclusions). This is the
    /// small-file debt signal only: it does NOT include groups a
    /// `rewrite_data_files(delete_file_threshold => N)` call would also
    /// rewrite purely because they contain a delete-heavy file (see
    /// `delete_heavy_files`, a separate signal).
    pub eligible_groups: u64,
    /// Total bytes of the data files in `eligible_groups`. Same caveat as
    /// `eligible_groups`: excludes delete-heavy-only groups.
    pub est_rewrite_bytes: u64,
    /// Timestamp (epoch ms) of the most recent snapshot stamped by a
    /// compaction job, or `None` if none has run. Always `None` in Phase 4a:
    /// nothing stamps a `sqe.maintenance.job-id` into a snapshot summary
    /// yet. The active scheduler (Phase 4b+) sets this from the newest
    /// snapshot carrying that property.
    pub last_compaction_snapshot_ms: Option<i64>,
    /// Whether `sqe.maintenance.enabled == "true"` is set on the table, i.e.
    /// whether the (later) advisory scheduler would consider this table.
    pub maintenance_enabled: bool,
}

/// Analyze compaction debt for one table's already-collected data files,
/// delete files, and delete-aware read plan task map.
///
/// Pure and unit-testable: takes data the caller already collected (via
/// `crate::maintenance::collect_live_data_files`,
/// `collect_live_delete_files`, and `plan_delete_aware_read`) and never loads
/// or scans anything itself.
///
/// `tasks_by_path` is `DeleteAwareReadPlan::tasks_by_path` (the per-data-file
/// scan task list, each carrying its applicable delete files). It is passed
/// as a bare map rather than the `DeleteAwareReadPlan` wrapper because that
/// wrapper also carries a live `TableScan`, which only a real table can
/// produce; keeping this function's inputs to plain data lets tests build
/// them with synthetic `FileScanTask`s instead of a docker-backed table.
pub fn analyze_table_health(
    data: &[DataFile],
    deletes: &[DataFile],
    tasks_by_path: &HashMap<String, Vec<FileScanTask>>,
    cfg: &MaintenanceCompactionConfig,
    props: &HashMap<String, String>,
) -> TableHealth {
    let target = cfg.target_file_size_bytes;

    let mut sizes: Vec<u64> = data.iter().map(DataFile::file_size_in_bytes).collect();
    let small_files = sizes.iter().filter(|&&s| s < target).count() as u64;
    let avg_file_bytes = if sizes.is_empty() {
        0
    } else {
        sizes.iter().sum::<u64>() / sizes.len() as u64
    };
    sizes.sort_unstable();
    let p50_file_bytes = sizes.get(sizes.len() / 2).copied().unwrap_or(0);

    // Mirrors the guard in `rewrite_data_files_once`: a threshold of 0 means
    // the delete-heavy override is off, not "every file qualifies".
    let delete_heavy: HashSet<String> = if cfg.delete_file_threshold > 0 {
        crate::maintenance::delete_heavy_files(tasks_by_path, cfg.delete_file_threshold)
    } else {
        HashSet::new()
    };

    let no_force: HashSet<String> = HashSet::new();
    let groups = crate::maintenance::pack_file_groups_partition_aware(data, target, &no_force);
    let eligible_groups: Vec<&Vec<DataFile>> = groups
        .iter()
        .filter(|g| g.len() >= cfg.min_input_files)
        .collect();
    let est_rewrite_bytes: u64 = eligible_groups
        .iter()
        .flat_map(|g| g.iter())
        .map(DataFile::file_size_in_bytes)
        .sum();

    let maintenance_enabled = props
        .get(MAINTENANCE_ENABLED_PROPERTY)
        .map(|v| v == "true")
        .unwrap_or(false);

    TableHealth {
        live_data_files: data.len() as u64,
        small_files,
        avg_file_bytes,
        p50_file_bytes,
        delete_files: deletes.len() as u64,
        delete_heavy_files: delete_heavy.len() as u64,
        eligible_groups: eligible_groups.len() as u64,
        est_rewrite_bytes,
        last_compaction_snapshot_ms: None,
        maintenance_enabled,
    }
}

/// Shape `health` into the single-row `RecordBatch` the `CALL` returns.
pub fn table_health_batch(health: &TableHealth) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("live_data_files", DataType::Int64, false),
        Field::new("small_files", DataType::Int64, false),
        Field::new("avg_file_bytes", DataType::Int64, false),
        Field::new("p50_file_bytes", DataType::Int64, false),
        Field::new("delete_files", DataType::Int64, false),
        Field::new("delete_heavy_files", DataType::Int64, false),
        Field::new("eligible_groups", DataType::Int64, false),
        Field::new("est_rewrite_bytes", DataType::Int64, false),
        Field::new("last_compaction_snapshot_ms", DataType::Int64, true),
        Field::new("maintenance_enabled", DataType::Boolean, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![health.live_data_files as i64])),
            Arc::new(Int64Array::from(vec![health.small_files as i64])),
            Arc::new(Int64Array::from(vec![health.avg_file_bytes as i64])),
            Arc::new(Int64Array::from(vec![health.p50_file_bytes as i64])),
            Arc::new(Int64Array::from(vec![health.delete_files as i64])),
            Arc::new(Int64Array::from(vec![health.delete_heavy_files as i64])),
            Arc::new(Int64Array::from(vec![health.eligible_groups as i64])),
            Arc::new(Int64Array::from(vec![health.est_rewrite_bytes as i64])),
            Arc::new(Int64Array::from(vec![health.last_compaction_snapshot_ms])),
            Arc::new(BooleanArray::from(vec![health.maintenance_enabled])),
        ],
    )
    .expect("table_health_batch: fixed single-row schema construction cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, NestedField, PrimitiveType,
        Schema, Struct, Type,
    };

    fn cfg(target_bytes: u64, min_input_files: usize, delete_file_threshold: usize) -> MaintenanceCompactionConfig {
        MaintenanceCompactionConfig {
            target_file_size_bytes: target_bytes,
            min_input_files,
            delete_file_threshold,
            strategy: "binpack".to_string(),
        }
    }

    fn data_file_of_size(path: &str, size: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    /// Like `data_file_of_size` but lets the test vary the partition value,
    /// so partition-aware grouping can be exercised the same way the
    /// maintenance-module tests do.
    fn data_file_part(path: &str, size: u64, part: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(part))]))
            .partition_spec_id(0)
            .build()
            .expect("build data file")
    }

    fn delete_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .expect("build delete file")
    }

    fn minimal_schema() -> std::sync::Arc<Schema> {
        std::sync::Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("build schema"),
        )
    }

    /// A scan task for `data_path` whose `.deletes` list carries one nested
    /// `FileScanTask` per entry in `delete_paths`, mirroring what the scan
    /// planner attaches for position/equality deletes.
    fn scan_task(data_path: &str, delete_paths: &[&str]) -> FileScanTask {
        let schema = minimal_schema();
        let delete_task = |path: &str| FileScanTask {
            file_size_in_bytes: 1,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: path.to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::PositionDeletes,
            data_file_format: DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        };
        let deletes: Vec<std::sync::Arc<FileScanTask>> = delete_paths
            .iter()
            .map(|p| std::sync::Arc::new(delete_task(p)))
            .collect();
        FileScanTask {
            file_size_in_bytes: 1,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: data_path.to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes,
            sequence_number: 0,
            equality_ids: None,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn tasks_by_path(tasks: Vec<FileScanTask>) -> HashMap<String, Vec<FileScanTask>> {
        let mut map: HashMap<String, Vec<FileScanTask>> = HashMap::new();
        for t in tasks {
            map.entry(t.data_file_path.clone()).or_default().push(t);
        }
        map
    }

    #[test]
    fn empty_table_reports_zeros() {
        let health = analyze_table_health(
            &[],
            &[],
            &HashMap::new(),
            &cfg(1024, 5, 2),
            &HashMap::new(),
        );
        assert_eq!(health.live_data_files, 0);
        assert_eq!(health.small_files, 0);
        assert_eq!(health.avg_file_bytes, 0);
        assert_eq!(health.p50_file_bytes, 0);
        assert_eq!(health.delete_files, 0);
        assert_eq!(health.delete_heavy_files, 0);
        assert_eq!(health.eligible_groups, 0);
        assert_eq!(health.est_rewrite_bytes, 0);
        assert_eq!(health.last_compaction_snapshot_ms, None);
        assert!(!health.maintenance_enabled);
    }

    #[test]
    fn small_files_counts_below_target() {
        let target = 1000u64;
        // 3 below target, 7 at/above target.
        let mut files: Vec<DataFile> = (0..3)
            .map(|i| data_file_of_size(&format!("small{i}"), 100))
            .collect();
        files.extend((0..7).map(|i| data_file_of_size(&format!("big{i}"), 2000)));

        let health = analyze_table_health(
            &files,
            &[],
            &HashMap::new(),
            &cfg(target, 5, 2),
            &HashMap::new(),
        );
        assert_eq!(health.live_data_files, 10);
        assert_eq!(health.small_files, 3);
    }

    #[test]
    fn delete_files_counts_live_deletes() {
        let files = vec![data_file_of_size("a", 100)];
        let deletes = vec![delete_file("d1"), delete_file("d2")];
        let health = analyze_table_health(
            &files,
            &deletes,
            &HashMap::new(),
            &cfg(1000, 5, 2),
            &HashMap::new(),
        );
        assert_eq!(health.delete_files, 2);
    }

    #[test]
    fn delete_heavy_files_matches_threshold() {
        // "heavy" has 2 distinct delete files (meets threshold=2); "light"
        // has 1 (below threshold).
        let tasks = tasks_by_path(vec![
            scan_task("heavy", &["d1", "d2"]),
            scan_task("light", &["d1"]),
        ]);
        let files = vec![data_file_of_size("heavy", 100), data_file_of_size("light", 100)];
        let health = analyze_table_health(&files, &[], &tasks, &cfg(1000, 5, 2), &HashMap::new());
        assert_eq!(health.delete_heavy_files, 1);
    }

    #[test]
    fn delete_heavy_files_zero_threshold_disables_override() {
        // threshold=0 must not flag every file as delete-heavy (mirrors the
        // rewrite_data_files_once guard).
        let tasks = tasks_by_path(vec![scan_task("a", &["d1"])]);
        let files = vec![data_file_of_size("a", 100)];
        let health = analyze_table_health(&files, &[], &tasks, &cfg(1000, 5, 0), &HashMap::new());
        assert_eq!(health.delete_heavy_files, 0);
    }

    #[test]
    fn eligible_groups_matches_binpack_min_input_filter() {
        let target = 1000u64;
        let min_input = 3usize;
        // 10 tiny files in one partition pack into a single group of 10
        // (well under target), which meets min_input_files=3.
        let files: Vec<DataFile> = (0..10)
            .map(|i| data_file_part(&format!("f{i}"), 10, 0))
            .collect();

        let health = analyze_table_health(
            &files,
            &[],
            &HashMap::new(),
            &cfg(target, min_input, 2),
            &HashMap::new(),
        );

        let expected_groups = crate::maintenance::pack_file_groups_partition_aware(
            &files,
            target,
            &HashSet::new(),
        );
        let expected_eligible = expected_groups
            .iter()
            .filter(|g| g.len() >= min_input)
            .count() as u64;
        assert_eq!(health.eligible_groups, expected_eligible);
        assert_eq!(health.eligible_groups, 1);
    }

    #[test]
    fn eligible_groups_below_min_input_excluded() {
        let target = 1000u64;
        // 2 tiny files in one partition: one group of 2, below min_input=5.
        let files = vec![
            data_file_part("f0", 10, 0),
            data_file_part("f1", 10, 0),
        ];
        let health = analyze_table_health(
            &files,
            &[],
            &HashMap::new(),
            &cfg(target, 5, 2),
            &HashMap::new(),
        );
        assert_eq!(health.eligible_groups, 0);
        assert_eq!(health.est_rewrite_bytes, 0);
    }

    #[test]
    fn est_rewrite_bytes_sums_eligible_group_bytes() {
        let target = 1000u64;
        let files: Vec<DataFile> = (0..5)
            .map(|i| data_file_part(&format!("f{i}"), 100, 0))
            .collect();
        let health = analyze_table_health(
            &files,
            &[],
            &HashMap::new(),
            &cfg(target, 3, 2),
            &HashMap::new(),
        );
        assert_eq!(health.eligible_groups, 1);
        assert_eq!(health.est_rewrite_bytes, 500);
    }

    #[test]
    fn avg_and_p50_file_bytes() {
        let files = vec![
            data_file_of_size("a", 100),
            data_file_of_size("b", 200),
            data_file_of_size("c", 300),
        ];
        let health = analyze_table_health(
            &files,
            &[],
            &HashMap::new(),
            &cfg(1_000_000, 5, 2),
            &HashMap::new(),
        );
        assert_eq!(health.avg_file_bytes, 200);
        assert_eq!(health.p50_file_bytes, 200);
    }

    #[test]
    fn maintenance_enabled_reads_table_property() {
        let files = vec![data_file_of_size("a", 100)];
        let mut props = HashMap::new();
        props.insert("sqe.maintenance.enabled".to_string(), "true".to_string());
        let health = analyze_table_health(&files, &[], &HashMap::new(), &cfg(1000, 5, 2), &props);
        assert!(health.maintenance_enabled);
    }

    #[test]
    fn maintenance_enabled_defaults_false() {
        let files = vec![data_file_of_size("a", 100)];
        let health =
            analyze_table_health(&files, &[], &HashMap::new(), &cfg(1000, 5, 2), &HashMap::new());
        assert!(!health.maintenance_enabled);
    }

    #[test]
    fn maintenance_enabled_false_value_is_not_enabled() {
        let files = vec![data_file_of_size("a", 100)];
        let mut props = HashMap::new();
        props.insert("sqe.maintenance.enabled".to_string(), "false".to_string());
        let health = analyze_table_health(&files, &[], &HashMap::new(), &cfg(1000, 5, 2), &props);
        assert!(!health.maintenance_enabled);
    }

    #[test]
    fn last_compaction_snapshot_ms_always_none_in_phase_4a() {
        let files = vec![data_file_of_size("a", 100)];
        let health =
            analyze_table_health(&files, &[], &HashMap::new(), &cfg(1000, 5, 2), &HashMap::new());
        assert_eq!(health.last_compaction_snapshot_ms, None);
    }

    #[test]
    fn table_health_batch_shapes_single_row() {
        let health = TableHealth {
            live_data_files: 10,
            small_files: 3,
            avg_file_bytes: 500,
            p50_file_bytes: 450,
            delete_files: 2,
            delete_heavy_files: 1,
            eligible_groups: 1,
            est_rewrite_bytes: 5000,
            last_compaction_snapshot_ms: None,
            maintenance_enabled: true,
        };
        let batch = table_health_batch(&health);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);

        let live = batch
            .column_by_name("live_data_files")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(live.value(0), 10);

        let snapshot_col = batch
            .column_by_name("last_compaction_snapshot_ms")
            .unwrap();
        assert!(snapshot_col.is_null(0), "None must round-trip as a null cell");

        let enabled = batch
            .column_by_name("maintenance_enabled")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(enabled.value(0));
    }
}

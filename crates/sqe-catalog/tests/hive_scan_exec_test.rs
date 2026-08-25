//! Cycle 5, task 3c: a Hive scan must be findable by downcast, the way an
//! Iceberg one is, and the spec must survive a plan rebuild.
//!
//! Without this node the coordinator has nothing to recognize: a Hive scan is a
//! UnionExec of projections over ListingTable scans, and nothing in that tree
//! names the table.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sqe_catalog::hive::manifest::PartitionIndexEntry;
use sqe_catalog::hive::scan_exec::{HiveScanExec, HiveScanSpec};
use sqe_catalog::hive::serde_map::{HiveFileFormat, ResolvedFormat};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("dt", DataType::Utf8, true),
    ]))
}

async fn inner_plan() -> Arc<dyn ExecutionPlan> {
    let s = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&s),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let mem = MemTable::try_new(s, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(mem)).unwrap();
    ctx.table("t").await.unwrap().create_physical_plan().await.unwrap()
}

fn spec(n_partitions: usize) -> HiveScanSpec {
    HiveScanSpec {
        entries: (0..n_partitions)
            .map(|i| PartitionIndexEntry {
                values: std::collections::BTreeMap::from([(
                    "dt".to_string(),
                    format!("2024-01-0{}", i + 1),
                )]),
                location: format!("s3://b/t/dt=2024-01-0{}/", i + 1),
                file_count: 1,
                total_bytes: 100,
            })
            .collect(),
        resolved: ResolvedFormat {
            kind: HiveFileFormat::Parquet,
            delimiter: b',',
            quote: None,
            escape: None,
            has_header: false,
            file_extension: ".parquet".to_string(),
        },
        partition_columns: vec!["dt".to_string()],
        full_schema: schema(),
        secret: None,
    }
}

#[tokio::test]
async fn a_hive_scan_is_findable_by_downcast() {
    // This is the property the whole of cycle 5's dispatch depends on. The
    // coordinator finds an Iceberg scan with downcast_ref::<IcebergScanExec>();
    // a Hive scan must be findable the same way.
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(HiveScanExec::new(inner_plan().await, spec(2)));

    let found = plan
        .downcast_ref::<HiveScanExec>()
        .expect("a Hive scan must be recognizable by downcast");
    assert_eq!(found.spec().entries.len(), 2);
    assert_eq!(found.spec().partition_columns, vec!["dt".to_string()]);
}

#[tokio::test]
async fn the_spec_survives_a_plan_rebuild() {
    // If an optimizer rule rebuilt the node and dropped the spec, the scan
    // would stop being recognizable and would silently stop distributing --
    // the same invisibility HiveLocalScanExec exists to prevent, one level down.
    let original = Arc::new(HiveScanExec::new(inner_plan().await, spec(3)));
    let child = original.children()[0].clone();

    let rebuilt = Arc::clone(&original)
        .replace_children(
            vec![child],
            datafusion::physical_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
            ),
        )
        .expect("rebuild");

    let found = rebuilt
        .downcast_ref::<HiveScanExec>()
        .expect("still a HiveScanExec after rebuild");
    assert_eq!(
        found.spec().entries.len(),
        3,
        "the spec must survive rebuild or dispatch silently stops working"
    );
}

#[tokio::test]
async fn the_node_changes_no_rows() {
    // 3c.5: the wrapper is introduced BEFORE any dispatch exists precisely so
    // this can be asserted in isolation. If it altered results, every later
    // differential test would be comparing against a moved baseline.
    let inner = inner_plan().await;
    let wrapped: Arc<dyn ExecutionPlan> =
        Arc::new(HiveScanExec::new(Arc::clone(&inner), spec(1)));

    let ctx = SessionContext::new();
    let tc = ctx.task_ctx();
    let bare = datafusion::physical_plan::collect(inner, Arc::clone(&tc))
        .await
        .unwrap();
    let through = datafusion::physical_plan::collect(wrapped, tc).await.unwrap();

    let rows = |b: &[RecordBatch]| b.iter().map(|x| x.num_rows()).sum::<usize>();
    assert_eq!(rows(&bare), rows(&through));
    assert_eq!(rows(&through), 3);
}

#[tokio::test]
async fn explain_names_the_scan_and_its_partition_count() {
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(HiveScanExec::new(inner_plan().await, spec(4)));
    let rendered = format!("{}", displayable(plan.as_ref()).indent(false));
    assert!(rendered.contains("HiveScanExec"), "got:\n{rendered}");
    assert!(
        rendered.contains("partitions=4"),
        "EXPLAIN should show how many partitions survived pruning, got:\n{rendered}"
    );
}

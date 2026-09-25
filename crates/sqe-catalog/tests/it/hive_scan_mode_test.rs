//! Cycle 5, task 3.4: the coordinator-local fallback must be OBSERVABLE.
//!
//! The trap this file exists for: a test that only checks rows passes on a
//! silent fallback. Falling back is always correct, so correct rows prove
//! nothing about whether the feature engaged. These assert the reason survives
//! into EXPLAIN, and that it survives a plan rebuild.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sqe_catalog::hive::scan_mode::HiveLocalScanExec;
use sqe_catalog::hive::serde_map::HiveFileFormat;
use sqe_catalog::hive::worker_scan::{servability, Unservable};

async fn a_scan() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(mem)).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

#[tokio::test]
async fn explain_says_the_scan_was_local_and_why() {
    let reason = Unservable::Format(HiveFileFormat::Csv).reason();
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(HiveLocalScanExec::new(a_scan().await, reason.clone()));

    let rendered = format!("{}", displayable(plan.as_ref()).indent(false));
    assert!(
        rendered.contains("HiveLocalScanExec"),
        "the marker must appear in EXPLAIN, got:\n{rendered}"
    );
    assert!(
        rendered.contains("coordinator-local"),
        "EXPLAIN must say the scan did not distribute, got:\n{rendered}"
    );
    assert!(
        rendered.contains("csv"),
        "EXPLAIN must carry the REASON, not just the fact; got:\n{rendered}"
    );
}

#[tokio::test]
async fn the_reason_survives_a_plan_rebuild() {
    // An optimizer rule that rebuilds the plan must not silently drop the
    // marker. If it did, the fallback would become invisible again -- the same
    // bug wearing a different hat, and one that would only show up after some
    // unrelated rule started firing.
    let reason = Unservable::NoPartitionEntries.reason();
    let original = Arc::new(HiveLocalScanExec::new(a_scan().await, reason.clone()));
    let child = original.children()[0].clone();

    let rebuilt = Arc::clone(&original)
        .replace_children(
            vec![child],
            datafusion::physical_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
            ),
        )
        .expect("rebuild");

    let rendered = format!("{}", displayable(rebuilt.as_ref()).indent(false));
    assert!(
        rendered.contains("coordinator-local"),
        "the marker vanished on rebuild, got:\n{rendered}"
    );
    assert!(
        rendered.contains("no partition entries"),
        "the reason vanished on rebuild, got:\n{rendered}"
    );
}

#[tokio::test]
async fn the_wrapper_changes_no_rows() {
    // The marker is diagnostic only. If it altered results it would be a far
    // worse bug than the invisibility it exists to fix.
    let inner = a_scan().await;
    let wrapped: Arc<dyn ExecutionPlan> =
        Arc::new(HiveLocalScanExec::new(Arc::clone(&inner), "because".to_string()));

    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let bare = datafusion::physical_plan::collect(inner, Arc::clone(&task_ctx))
        .await
        .unwrap();
    let through = datafusion::physical_plan::collect(wrapped, task_ctx)
        .await
        .unwrap();

    let sum = |bs: &[RecordBatch]| -> i64 {
        bs.iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .sum()
    };
    assert_eq!(sum(&bare), sum(&through));
    assert_eq!(
        bare.iter().map(|b| b.num_rows()).sum::<usize>(),
        through.iter().map(|b| b.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn a_servable_table_gets_no_marker_at_all() {
    // The control: if every Hive scan were wrapped, the EXPLAIN assertions
    // above would pass whether or not the decision function was consulted.
    assert!(
        servability(true, HiveFileFormat::Parquet, 4).is_ok(),
        "parquet with entries is servable, so nothing should mark it local"
    );
}

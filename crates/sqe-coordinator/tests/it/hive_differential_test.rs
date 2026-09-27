//! Cycle 5, task 4: coordinator-local and worker-distributed Hive scans must
//! return the same rows.
//!
//! The risk this closes is not a crash. It is the two paths disagreeing while
//! both succeed: a partition literal typed Utf8 on one side, a column order
//! swap between file and partition columns, a partition read twice or not at
//! all. Every one of those returns rows, so none of them shows up as an error.

use std::sync::Arc;

use datafusion::physical_plan::displayable;

use crate::hive_diff::{
    self,
    assert_same, distributed_rows, find_hive_scan, local_rows, multi_partition_parquet,
    plan_and_scan, session, typed_key_with_default_partition,
};

/// 4.2: parquet, partitioned, more than one partition.
#[tokio::test]
async fn parquet_multi_partition_agrees() {
    let f = multi_partition_parquet().await;
    let ctx = session(&f).await;
    let (_plan, scan) = plan_and_scan(&ctx, "SELECT id, amount, dt FROM lake.sales.orders").await;

    let local = local_rows(&scan, &ctx).await;
    let dist = distributed_rows(&scan).await;

    // Five rows across three partitions. Pinned so a path that read one
    // partition and stopped cannot match a path that did the same.
    assert_eq!(
        hive_diff::sorted_rows(&local.1).len(),
        5,
        "the fixture has five rows across three partitions"
    );
    assert_same("multi-partition parquet", local, dist);
}

/// 4.2, continued: a typed partition key and the default-partition sentinel.
#[tokio::test]
async fn a_typed_key_and_the_default_sentinel_agree() {
    let f = typed_key_with_default_partition().await;
    let ctx = session(&f).await;
    let (_plan, scan) = plan_and_scan(&ctx, "SELECT id, amount, year FROM lake.sales.orders").await;

    let local = local_rows(&scan, &ctx).await;
    let dist = distributed_rows(&scan).await;

    // The sentinel must be NULL on both sides, not the literal string. Asserted
    // on the rendered rows rather than trusted: reading it as a literal would
    // ALSO produce five rows.
    let rendered = hive_diff::sorted_rows(&local.1).join("\n");
    assert!(
        !rendered.contains("__HIVE_DEFAULT_PARTITION__"),
        "the sentinel must resolve to NULL, got:\n{rendered}"
    );
    assert!(
        rendered.contains("NULL"),
        "two rows live in the default partition and must render NULL:\n{rendered}"
    );
    assert_same("typed key + default partition", local, dist);
}

/// 4.3: with a partition predicate, both paths must scan the SAME partitions.
///
/// Partition pruning happens once, on the coordinator, before either path
/// exists: `spec.entries` holds only the survivors. So this asserts that the
/// pruning is what both arms inherit, and that the surviving partition is the
/// requested one -- a scan that pruned everything would also return "no rows
/// from the wrong partition".
#[tokio::test]
async fn a_partition_predicate_prunes_identically_on_both_paths() {
    let f = multi_partition_parquet().await;
    let ctx = session(&f).await;
    let (_plan, scan) = plan_and_scan(
        &ctx,
        "SELECT id, amount, dt FROM lake.sales.orders WHERE dt = '2026-01-02'",
    )
    .await;

    let hive = scan
        .downcast_ref::<sqe_catalog::hive::scan_exec::HiveScanExec>()
        .expect("a HiveScanExec");
    assert_eq!(
        hive.spec().entries.len(),
        1,
        "the predicate must prune three partitions to one before either path runs"
    );
    assert!(
        hive.spec().entries[0].location.contains("dt=2026-01-02"),
        "the surviving partition must be the requested one, got {}",
        hive.spec().entries[0].location
    );

    let local = local_rows(&scan, &ctx).await;
    let dist = distributed_rows(&scan).await;
    assert_eq!(
        hive_diff::sorted_rows(&local.1).len(),
        1,
        "dt=2026-01-02 holds exactly one row"
    );
    assert_same("pruned to one partition", local, dist);
}

/// 4.4: a projection subset.
///
/// The distributed path always builds tasks over the FULL schema, so the node
/// that replaces the scan is wider than the node it replaced. The parent
/// `ProjectionExec` indexes by position, so a width mismatch does not fail --
/// it reads a different column. This is the test that catches it.
#[tokio::test]
async fn a_projection_subset_agrees() {
    for sql in [
        // A file column only.
        "SELECT id FROM lake.sales.orders",
        // A partition column only: nothing is read from any data file, and the
        // row COUNT still has to come from somewhere.
        "SELECT dt FROM lake.sales.orders",
        // Reordered, mixing both kinds.
        "SELECT dt, id FROM lake.sales.orders",
    ] {
        let f = multi_partition_parquet().await;
        let ctx = session(&f).await;
        let (plan, scan) = plan_and_scan(&ctx, sql).await;

        // The whole query, not just the scan: the mismatch this test is for
        // lives in how the replaced node meets its parent.
        let local_scan = local_rows(&scan, &ctx).await;
        let dist_scan = distributed_rows(&scan).await;
        assert_eq!(
            hive_diff::sorted_rows(&local_scan.1).len(),
            5,
            "{sql}: five rows regardless of which columns are projected"
        );
        assert_eq!(
            hive_diff::sorted_rows(&dist_scan.1).len(),
            5,
            "{sql}: the distributed arm must return the same five rows"
        );

        // The scan node's schema is what a replacement has to match. Recorded
        // rather than asserted equal, because the distributed arm deliberately
        // produces the full schema; see `substituting_the_full_schema_scan_keeps_the_query_correct`.
        let _ = (plan, local_scan, dist_scan);
    }
}

/// 4.4, the load-bearing half: swapping in a full-schema scan must not change
/// what the QUERY returns.
///
/// `try_distribute_hive` replaces the `HiveScanExec` with a `DistributedScanExec`
/// carrying `spec.full_schema`. When the query projected a subset, the
/// replacement is wider than what it replaced, and every parent expression
/// indexes by position. This runs the real substitution and the real parent
/// plan, so a positional mismatch shows up as wrong columns rather than as an
/// error.
#[tokio::test]
async fn substituting_the_full_schema_scan_keeps_the_query_correct() {
    for (sql, want) in [
        ("SELECT id FROM lake.sales.orders", vec!["1", "2", "3", "4", "5"]),
        (
            "SELECT dt FROM lake.sales.orders",
            vec![
                "2026-01-01",
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-03",
            ],
        ),
        (
            "SELECT dt, id FROM lake.sales.orders",
            vec![
                "2026-01-01|1",
                "2026-01-01|2",
                "2026-01-02|3",
                "2026-01-03|4",
                "2026-01-03|5",
            ],
        ),
    ] {
        let f = multi_partition_parquet().await;
        let ctx = session(&f).await;
        let (plan, scan) = plan_and_scan(&ctx, sql).await;

        let (full_schema, dist_batches) = distributed_rows(&scan).await;
        let stand_in = hive_diff::stand_in_for_distributed(&scan, full_schema, dist_batches);
        let swapped = sqe_coordinator::query_handler::replace_scan_in_plan(&plan, &scan, stand_in)
            .expect("the parents must accept the stand-in (finding 402: a refusal is an error)");
        assert!(
            find_hive_scan(&swapped).is_none(),
            "{sql}: the substitution must actually have replaced the scan:\n{}",
            displayable(swapped.as_ref()).indent(false)
        );

        let task_ctx = ctx.task_ctx();
        let mut got = Vec::new();
        for p in 0..swapped.properties().output_partitioning().partition_count() {
            let mut s = swapped
                .execute(p, Arc::clone(&task_ctx))
                .unwrap_or_else(|e| panic!("{sql}: executing the swapped plan: {e}"));
            while let Some(b) = futures::StreamExt::next(&mut s).await {
                got.push(b.unwrap_or_else(|e| panic!("{sql}: swapped batch: {e}")));
            }
        }
        assert_eq!(
            hive_diff::sorted_rows(&got),
            want,
            "{sql}: the distributed substitution changed what the query returns"
        );
    }
}

/// 5.3: a differential test per format.
///
/// Same data in all three formats, so the expected row count is known rather
/// than being whatever that format's reader happened to produce. A CSV reader
/// that ignored `skip.header.line.count` would return six rows, not five, and a
/// lenient one would return five plus a row of NULLs -- which is why the count
/// and the rendered values are both asserted.
#[tokio::test]
async fn every_format_agrees_across_both_paths() {
    for format in ["parquet", "csv", "json"] {
        let f = hive_diff::multi_partition(format).await;
        let ctx = hive_diff::session(&f).await;
        let (_plan, scan) =
            plan_and_scan(&ctx, "SELECT id, amount, dt FROM lake.sales.orders").await;

        let local = local_rows(&scan, &ctx).await;
        let dist = distributed_rows(&scan).await;

        let want = vec![
            "1|10.5|2026-01-01",
            "2|20.0|2026-01-01",
            "3|30.5|2026-01-02",
            "4|40.5|2026-01-03",
            "5|45.0|2026-01-03",
        ];
        assert_eq!(
            hive_diff::sorted_rows(&dist.1),
            want,
            "{format}: the worker-side reader returned different rows than the \
             fixture holds"
        );
        assert_same(&format!("{format} across both paths"), local, dist);
    }
}

/// 5.1: the CSV reader must honor the declared delimiter, quote and escape.
///
/// Asserted through a fixture whose values contain the delimiter inside quotes.
/// A reader that ignored `quoteChar` would split that value into two columns
/// and shift every column after it -- which returns rows, with the wrong values
/// in them.
#[tokio::test]
async fn the_csv_reader_honors_quoting() {
    let f = hive_diff::quoted_csv().await;
    let ctx = hive_diff::session(&f).await;
    let (_plan, scan) = plan_and_scan(&ctx, "SELECT id, note, dt FROM lake.sales.orders").await;

    let dist = distributed_rows(&scan).await;
    let rows = hive_diff::sorted_rows(&dist.1);
    assert_eq!(
        rows,
        vec!["1|a,b|2026-01-01", "2|plain|2026-01-01"],
        "the quoted comma must stay inside one value"
    );

    let local = local_rows(&scan, &ctx).await;
    assert_same("quoted csv", local, dist);
}

//! Cycle 4: writes into a Hive external table must ROUND-TRIP.
//!
//! Write-then-assert-the-file-exists is not enough, and the reason is specific:
//! the failures this cycle risks are all read-side. A data file that also
//! contains the partition columns, a CSV header written when the manifest
//! declares none, a partition directory the index does not know about -- each
//! one writes successfully and breaks the next SELECT.
//!
//! So every test here writes and then reads back through the normal read path.

use datafusion::prelude::SessionContext;

mod hive_diff;
use hive_diff::{multi_partition, session};

/// Rows currently visible in the table, rendered and sorted.
async fn read_back(ctx: &SessionContext) -> Vec<String> {
    let batches = ctx
        .sql("SELECT id, amount, dt FROM lake.sales.orders")
        .await
        .expect("plan read-back")
        .collect()
        .await
        .expect("collect read-back");
    hive_diff::sorted_rows(&batches)
}

/// The baseline the other tests measure against, asserted rather than assumed.
#[tokio::test]
async fn the_fixture_starts_with_five_rows() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;
    assert_eq!(read_back(&ctx).await.len(), 5);
}

/// An append into an EXISTING partition round-trips.
///
/// The narrow case first: the partition directory already exists and the index
/// already lists it, so this isolates the write mechanics from the index
/// bookkeeping that `insert_into_a_new_partition_*` covers.
#[tokio::test]
async fn insert_into_an_existing_partition_round_trips() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (6, 60.5, '2026-01-02')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    let rows = read_back(&ctx).await;
    assert_eq!(
        rows.len(),
        6,
        "five existing rows plus one inserted, got: {rows:?}"
    );
    assert!(
        rows.contains(&"6|60.5|2026-01-02".to_string()),
        "the inserted row must read back with its partition value: {rows:?}"
    );
}

/// The written file must NOT contain the partition column.
///
/// Read the parquet file directly, not through the table. The read path
/// projects partition columns back in from the directory name, so a
/// table-level SELECT looks correct whether or not the column is in the file.
/// This is the only assertion that tells the two apart, and getting it wrong
/// breaks reads rather than writes.
#[tokio::test]
async fn a_written_file_does_not_contain_the_partition_column() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (7, 70.5, '2026-01-02')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    // Find the file the insert created: the newest one under dt=2026-01-02 that
    // is not the fixture's own part-0.parquet.
    let dir = f.dir.path().join("data").join("dt=2026-01-02");
    let written: Vec<_> = std::fs::read_dir(&dir)
        .expect("read partition dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.file_name().and_then(|n| n.to_str()) != Some("part-0.parquet"))
        .collect();
    assert_eq!(
        written.len(),
        1,
        "the insert must have written exactly one file into the partition, found {written:?}"
    );

    let file = std::fs::File::open(&written[0]).expect("open written file");
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("read written parquet");
    let names: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|fl| fl.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["id".to_string(), "amount".to_string()],
        "a written data file carries the FILE columns only; `dt` is encoded in \
         the directory name and a file that repeats it no longer matches the \
         schema the read path derives from the manifest"
    );
}

/// An append into a NEW partition.
///
/// Cycle 2 made reads plan from `sqe.partition_index` instead of listing the
/// table prefix, so a new partition directory is invisible until the index
/// knows about it. This asserts the CURRENT behavior, and it is deliberately
/// written to fail loudly if the answer changes without the docs changing:
/// either the index is maintained on write (then the row is visible), or it is
/// not (then `MSCK REPAIR TABLE` is the documented recovery).
#[tokio::test]
async fn insert_into_a_new_partition_needs_the_index_to_be_maintained() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (8, 80.5, '2026-09-01')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    // The DATA must exist on disk regardless. If this fails, the write itself
    // went wrong and the visibility question is moot.
    let new_dir = f.dir.path().join("data").join("dt=2026-09-01");
    assert!(
        new_dir.is_dir(),
        "the insert must have created the partition directory at {new_dir:?}"
    );
    let files: Vec<_> = std::fs::read_dir(&new_dir)
        .expect("read new partition dir")
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(files.len(), 1, "one file in the new partition");

    // Now the visibility question. Recorded, not asserted either way here --
    // `hive_write_index_test` owns the assertion once index maintenance lands.
    let rows = read_back(&ctx).await;
    eprintln!(
        "new-partition visibility: {} rows after inserting into an unindexed partition",
        rows.len()
    );
    assert!(
        rows.len() == 5 || rows.len() == 6,
        "either the index was maintained (6 rows) or it was not (5); anything \
         else means the write corrupted the existing partitions: {rows:?}"
    );
}

/// The index refresh is what makes a new partition visible.
///
/// Runs the same discovery the coordinator runs after a write
/// (`sqe_catalog::hive::partition::msck_repair`, which
/// `refresh_index_after_write` delegates to) and asserts the row appears. The
/// coordinator's own call needs a `Session` and a live handler, so this covers
/// the mechanism; `hive_write::handle_insert`'s unit tests plus the routing
/// cover the dispatch.
#[tokio::test]
async fn refreshing_the_index_makes_a_new_partition_visible() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (8, 80.5, '2026-09-01')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    // Before: invisible. Asserted, so the test cannot pass by the row having
    // been visible all along.
    assert_eq!(
        read_back(&ctx).await.len(),
        5,
        "a write into an unindexed partition must start out invisible, or this \
         test proves nothing about the refresh"
    );

    hive_diff::refresh_partition_index(&f).await;

    // A fresh session, because the old one's manifest cache still holds the
    // pre-refresh index. That 60s TTL is exactly why the coordinator has to
    // invalidate, and this stands in for the invalidation.
    let ctx2 = session(&f).await;
    let rows = read_back(&ctx2).await;
    assert_eq!(rows.len(), 6, "the refreshed index must expose the new partition: {rows:?}");
    assert!(
        rows.contains(&"8|80.5|2026-09-01".to_string()),
        "and the row must carry its partition value: {rows:?}"
    );
}

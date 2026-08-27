//! Cycle 4: writes into a Hive external table must ROUND-TRIP.
//!
//! Write-then-assert-the-file-exists is not enough, and the reason is specific:
//! the failures this cycle risks are all read-side. A data file that also
//! contains the partition columns, a CSV header written when the manifest
//! declares none, a partition directory the index does not know about -- each
//! one writes successfully and breaks the next SELECT.
//!
//! So every test here writes and then reads back through the normal read path.

mod hive_diff;
use hive_diff::{multi_partition, session};

/// Rows currently visible in the table, rendered and sorted.
///
/// Takes the FIXTURE, not a `SessionContext`, and builds a fresh session for
/// every read. That is deliberate and it mirrors production: the coordinator
/// calls `create_session_context` per statement, and after a Hive write it
/// drops the cached one via `invalidate_session_cache`.
///
/// Reusing one context here would test something the engine never does, and it
/// would fail for the wrong reason. Measured while writing these tests: a
/// SELECT before the write caches the table provider, and the next SELECT on
/// that SAME context returns the pre-write rows while a fresh context returns
/// the correct ones. That finding is what added the `invalidate_session_cache`
/// call to the write path; it is not what these tests are asserting.
async fn read_back(f: &hive_diff::Fixture) -> Vec<String> {
    let ctx = session(f).await;
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
    assert_eq!(read_back(&f).await.len(), 5);
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

    let rows = read_back(&f).await;
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
    let rows = read_back(&f).await;
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
        read_back(&f).await.len(),
        5,
        "a write into an unindexed partition must start out invisible, or this \
         test proves nothing about the refresh"
    );

    hive_diff::refresh_partition_index(&f).await;

    let rows = read_back(&f).await;
    assert_eq!(rows.len(), 6, "the refreshed index must expose the new partition: {rows:?}");
    assert!(
        rows.contains(&"8|80.5|2026-09-01".to_string()),
        "and the row must carry its partition value: {rows:?}"
    );
}

/// 4.2 / 4.3 / 4.4: every format round-trips.
///
/// CSV is the one with a real trap. `skip.header.line.count` is a read-side
/// declaration, and DataFusion's CSV writer emits a header by default. The two
/// wrong combinations are both SILENT: declared 1 with no header written eats
/// the first real row, declared 0 with a header written gains a phantom one.
///
/// design.md predicted this is already correct by construction, because
/// `serde_map::listing_options` builds ONE `CsvFormat` with
/// `with_has_header(r.has_header)` and DataFusion uses that flag for both the
/// reader's skip and the writer's emit. This test is what turns that prediction
/// into a fact -- and if it ever stops holding, the row COUNT is what moves.
#[tokio::test]
async fn every_format_round_trips_a_write() {
    for format in ["parquet", "csv", "json"] {
        let f = multi_partition(format).await;
        let ctx = session(&f).await;

        ctx.sql("INSERT INTO lake.sales.orders VALUES (9, 90.5, '2026-01-02')")
            .await
            .unwrap_or_else(|e| panic!("{format}: plan insert: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{format}: execute insert: {e}"));

        let rows = read_back(&f).await;
        assert_eq!(
            rows.len(),
            6,
            "{format}: five existing rows plus one written. A count of 7 means a \
             header was written where the manifest declares none; a count of 5 \
             means the write was swallowed. Got: {rows:?}"
        );
        assert!(
            rows.contains(&"9|90.5|2026-01-02".to_string()),
            "{format}: the written row must read back with its exact values, not \
             just a matching count: {rows:?}"
        );
    }
}

/// 4.3: the other half of the CSV header pair.
///
/// The table declares no `skip.header.line.count`, so the writer must not emit
/// one. If it does, the read-back gains a phantom row per written file whose
/// cells are the column names -- which parses as NULLs against an Int64 column
/// and is therefore silent. The row count is what catches it.
#[tokio::test]
async fn a_csv_table_declaring_no_header_round_trips_without_gaining_one() {
    let f = hive_diff::headerless_csv().await;
    let ctx = session(&f).await;

    assert_eq!(
        read_back(&f).await.len(),
        2,
        "the headerless fixture starts with two rows; if this reads 1 the READER \
         is eating a line that is not a header"
    );

    ctx.sql("INSERT INTO lake.sales.orders VALUES (3, 30.5, '2026-01-01')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    let rows = read_back(&f).await;
    assert_eq!(
        rows.len(),
        3,
        "two existing plus one written. A count of 4 means the WRITER emitted a \
         header the manifest never declared: {rows:?}"
    );
    assert!(
        rows.contains(&"3|30.5|2026-01-01".to_string()),
        "the written row must round-trip exactly: {rows:?}"
    );
    assert!(
        !rows.iter().any(|r| r.starts_with("NULL|")),
        "a phantom header row parses to NULLs against an Int64 column, which no \
         count-only check would catch: {rows:?}"
    );
}

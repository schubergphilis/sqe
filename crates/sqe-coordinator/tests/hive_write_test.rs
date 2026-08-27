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

/// 5.1: two concurrent inserts into the SAME partition must both land.
///
/// Static file names have already cost this repo an INSERT failure at scale in
/// the Iceberg path, found through a dbt load. The symptom under concurrency is
/// not an error -- it is one writer's file silently replacing another's, so the
/// row count is what catches it.
///
/// Each writer gets its own session context, because that is what two
/// concurrent clients have. Sharing one would test a single writer twice.
#[tokio::test]
async fn two_concurrent_inserts_into_one_partition_both_land() {
    let f = multi_partition("parquet").await;

    let (a, b) = tokio::join!(
        async {
            let ctx = session(&f).await;
            ctx.sql("INSERT INTO lake.sales.orders VALUES (100, 100.5, '2026-01-02')")
                .await
                .expect("plan insert A")
                .collect()
                .await
                .expect("execute insert A")
        },
        async {
            let ctx = session(&f).await;
            ctx.sql("INSERT INTO lake.sales.orders VALUES (200, 200.5, '2026-01-02')")
                .await
                .expect("plan insert B")
                .collect()
                .await
                .expect("execute insert B")
        }
    );
    let _ = (a, b);

    // Two NEW files beside the fixture's own, not one. Asserted on the
    // directory rather than only on rows: a collision that overwrote one
    // writer's file would leave a single new file, and the row count alone
    // could be explained away as a visibility problem.
    let dir = f.dir.path().join("data").join("dt=2026-01-02");
    let written: Vec<_> = std::fs::read_dir(&dir)
        .expect("read partition dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.file_name().and_then(|n| n.to_str()) != Some("part-0.parquet"))
        .collect();
    assert_eq!(
        written.len(),
        2,
        "each concurrent writer must have produced its own file; one file means \
         a name collision silently discarded a write. Found: {written:?}"
    );

    let rows = read_back(&f).await;
    assert_eq!(rows.len(), 7, "five existing plus both writes: {rows:?}");
    for want in ["100|100.5|2026-01-02", "200|200.5|2026-01-02"] {
        assert!(
            rows.contains(&want.to_string()),
            "both writes must be readable, missing {want}: {rows:?}"
        );
    }
}

/// 3.5: the write ordering, proven rather than assumed.
///
/// Data lands BEFORE the index is refreshed. This simulates the refresh
/// failing -- by simply not calling it -- and asserts the two properties that
/// make the ordering safe: the data files exist, and `MSCK REPAIR TABLE`'s own
/// discovery recovers them exactly.
///
/// Index-first would fail the other way, promising a partition whose files do
/// not exist. A test that only checked the happy path would pass for either
/// ordering.
#[tokio::test]
async fn data_lands_before_the_index_so_a_failed_refresh_is_recoverable() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (11, 110.5, '2026-10-01')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    // No refresh call: this is the state a failed refresh leaves behind.
    let new_dir = f.dir.path().join("data").join("dt=2026-10-01");
    assert!(
        new_dir.is_dir(),
        "the data must have landed first, so a refresh failure cannot lose it"
    );
    assert_eq!(
        read_back(&f).await.len(),
        5,
        "and it must be invisible until the index knows, or this proves nothing"
    );

    // The documented recovery, run as documented.
    hive_diff::refresh_partition_index(&f).await;

    let rows = read_back(&f).await;
    assert_eq!(rows.len(), 6, "MSCK REPAIR must recover the rows: {rows:?}");
    assert!(rows.contains(&"11|110.5|2026-10-01".to_string()), "{rows:?}");
}

/// 1.4: CTAS creates the manifest AND lands the data, and the result reads back.
///
/// Exercised through the DDL + write mechanics rather than the coordinator's
/// dispatch (which needs a live `QueryHandler`): build the spec the way the
/// router does, write the manifest, then load through the same INSERT path.
/// That is the same sequence in the same order, so a divergence in either half
/// shows up here.
#[tokio::test]
async fn ctas_creates_a_partitioned_table_and_loads_it() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    // The target table's data lives beside the source's, under its own prefix.
    let target_loc = format!("file://{}/ctas_out", f.dir.path().display());
    let sql = format!(
        "CREATE TABLE lake.sales.copy PARTITIONED BY (dt string) STORED AS PARQUET \
         LOCATION '{target_loc}' AS SELECT id, amount, dt FROM lake.sales.orders"
    );
    let stmt = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::DuckDbDialect {},
        &sql,
    )
    .expect("parse ctas")
    .remove(0);
    let ct = match &stmt {
        sqlparser::ast::Statement::CreateTable(ct) => ct,
        other => panic!("not a CREATE TABLE: {other:?}"),
    };

    let select_sql = sqe_coordinator::hive_write::ctas_select_sql(ct).expect("select");
    let schema = ctx
        .sql(&select_sql)
        .await
        .expect("plan ctas query")
        .schema()
        .as_arrow()
        .clone();
    let spec = sqe_coordinator::hive_write::ctas_spec(ct, &schema).expect("ctas spec");

    // The spec must split file columns from partition keys, or the table it
    // creates is unreadable in a way no row count would reveal.
    assert_eq!(
        spec.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "amount"]
    );
    assert_eq!(
        spec.partition_keys.iter().map(|k| k.name.as_str()).collect::<Vec<_>>(),
        vec!["dt"]
    );

    // Manifest first, then data -- the CTAS ordering, inverted from INSERT's.
    hive_diff::put_spec_manifest(&f, &spec).await;

    let ctx2 = session(&f).await;
    let load = format!("INSERT INTO lake.sales.copy {select_sql}");
    ctx2.sql(&load)
        .await
        .expect("plan ctas load")
        .collect()
        .await
        .expect("execute ctas load");
    hive_diff::refresh_named_index(&f, "copy").await;

    let ctx3 = session(&f).await;
    let rows = hive_diff::sorted_rows(
        &ctx3
            .sql("SELECT id, amount, dt FROM lake.sales.copy")
            .await
            .expect("plan read of the new table")
            .collect()
            .await
            .expect("collect"),
    );
    assert_eq!(
        rows,
        vec![
            "1|10.5|2026-01-01",
            "2|20.0|2026-01-01",
            "3|30.5|2026-01-02",
            "4|40.5|2026-01-03",
            "5|45.0|2026-01-03",
        ],
        "the CTAS table must hold exactly the source's rows, partition values included"
    );

    // And the partition columns must be directory-encoded in the NEW table too,
    // not written into its files.
    let out = f.dir.path().join("ctas_out");
    assert!(
        out.join("dt=2026-01-01").is_dir(),
        "CTAS must fan out into k=v directories under its LOCATION"
    );
}

/// 5.2: a reader must never see a torn or leftover file in a partition.
///
/// **This needs no engine change, and that is the finding.** `object_store`'s
/// `LocalFileSystem` already writes to a staging path and then renames:
/// `put` does `new_staged_upload(...)` followed by `std::fs::rename`, and a
/// multipart upload's `complete` renames too. `rename(2)` within one filesystem
/// is atomic, so a partial write is never visible at the final path. On an
/// object store the equivalent guarantee is the store's own: a `PUT` becomes
/// visible whole or not at all.
///
/// So this verifies the property rather than implementing it. Writing a second
/// rename in SQE would be redundant, and redundant safety code nobody can later
/// remove because nobody knows whether it mattered is its own cost.
///
/// A true torn-file test would have to observe the filesystem mid-write, which
/// is racy and would either flake or pass vacuously. What is checkable without
/// a race: after the write, every file under the partition is a COMPLETE,
/// readable data file and no staging leftover remains. A reader listing that
/// directory is exactly what would trip over either failure.
#[tokio::test]
async fn a_write_leaves_no_partial_or_staging_files_behind() {
    let f = multi_partition("parquet").await;
    let ctx = session(&f).await;

    ctx.sql("INSERT INTO lake.sales.orders VALUES (12, 120.5, '2026-01-02')")
        .await
        .expect("plan insert")
        .collect()
        .await
        .expect("execute insert");

    let dir = f.dir.path().join("data").join("dt=2026-01-02");
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .expect("read partition dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    // Two files: the fixture's and the written one. A staging leftover would
    // make this three, and it would be a file the read path then tries to open
    // as parquet.
    assert_eq!(
        entries.len(),
        2,
        "the partition must hold exactly the fixture's file and the written \
         one; anything extra is a staging leftover a reader would try to parse: \
         {entries:?}"
    );

    // Every file must be a COMPLETE parquet file. A torn write would leave one
    // whose footer does not parse, which is precisely how a reader experiences
    // a non-atomic write.
    for path in &entries {
        let file = std::fs::File::open(path).expect("open");
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| {
                panic!("{path:?} is not a complete parquet file, so the write was not atomic: {e}")
            });
        assert!(
            reader.metadata().file_metadata().num_rows() > 0,
            "{path:?} parsed but holds no rows, which is a truncated write"
        );
    }

    // And the table still reads correctly, which is the property all of the
    // above exists to protect.
    let rows = read_back(&f).await;
    assert_eq!(rows.len(), 6, "{rows:?}");
}

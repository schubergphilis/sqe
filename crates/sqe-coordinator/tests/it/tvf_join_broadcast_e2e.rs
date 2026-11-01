//! A join against a small `read_parquet()` file broadcasts the file through
//! the coordinator's planning path (DataFusion's join selection plus the SQE
//! plan-shape rules), whichever side of the join the query names it on.
//!
//! The file reader's scan is wrapped in `ScopedFileScan`. Under DataFusion 55
//! that wrapper reported no statistics until it implemented
//! `statistics_from_inputs`, and with no row count DataFusion never picks
//! `CollectLeft`: the 150M-row side of a join against a 50,000-row file was
//! hash-partitioned instead (4.9 s and 4.0 GiB instead of 1.5 s and 0.14 GiB).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

const DIM_ROWS: i64 = 100;
/// Above DataFusion's 131,072-row broadcast threshold, so only the
/// dimension can be the build side.
const FACT_ROWS: i64 = 300_000;

fn write(path: &std::path::Path, batch: RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
}

fn write_files(dir: &std::path::Path) -> (String, String) {
    let dim = dir.join("dim.parquet");
    write(
        &dim,
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("label", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..DIM_ROWS)),
                Arc::new(StringArray::from_iter_values(
                    (0..DIM_ROWS).map(|i| format!("l{i}")),
                )),
            ],
        )
        .expect("dim batch"),
    );
    let fact = dir.join("fact.parquet");
    write(
        &fact,
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..FACT_ROWS).map(|i| i % DIM_ROWS),
                )),
                Arc::new(Int64Array::from_iter_values(0..FACT_ROWS)),
            ],
        )
        .expect("fact batch"),
    );
    (
        dim.to_str().expect("utf-8").to_string(),
        fact.to_str().expect("utf-8").to_string(),
    )
}

fn physical_plan(batches: &[RecordBatch]) -> String {
    for batch in batches {
        let types = batch.column_by_name("plan_type").expect("plan_type");
        let types = types.as_any().downcast_ref::<StringArray>().expect("utf8");
        let plans = batch.column_by_name("plan").expect("plan");
        let plans = plans.as_any().downcast_ref::<StringArray>().expect("utf8");
        for r in 0..batch.num_rows() {
            if types.value(r) == "physical_plan" {
                return plans.value(r).to_string();
            }
        }
    }
    panic!("no physical_plan row in EXPLAIN output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn a_small_read_parquet_file_is_the_broadcast_build_side() {
    let (session, handler) = crate::common::setup_handler().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let (dim, fact) = write_files(tmp.path());

    let fact_first = format!(
        "SELECT count(*) AS n, sum(f.v) AS s FROM read_parquet('{fact}') f \
         JOIN read_parquet('{dim}') d ON f.key = d.key"
    );
    let dim_first = format!(
        "SELECT count(*) AS n, sum(f.v) AS s FROM read_parquet('{dim}') d \
         JOIN read_parquet('{fact}') f ON f.key = d.key"
    );
    for sql in [fact_first, dim_first] {
        let explain = format!("EXPLAIN {sql}");
        let plan = physical_plan(
            &handler
                .execute(&session, &explain, None)
                .await
                .expect(&explain),
        );
        let lines: Vec<&str> = plan.lines().collect();
        let join = lines
            .iter()
            .position(|l| l.contains("HashJoinExec"))
            .unwrap_or_else(|| panic!("no HashJoinExec in:\n{plan}"));
        assert!(
            lines[join].contains("mode=CollectLeft"),
            "the small file is broadcast, not hash-partitioned:\n{plan}"
        );
        // The build side is the join's first child, so the first scan below
        // the join line is the one collected.
        let build = lines[join + 1..]
            .iter()
            .find(|l| l.contains("DataSourceExec"))
            .unwrap_or_else(|| panic!("no scan under the join:\n{plan}"));
        assert!(
            build.contains("dim.parquet"),
            "the 100-row file is the build side:\n{plan}"
        );

        let batches = handler.execute(&session, &sql, None).await.expect(&sql);
        let row = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
        let n = row
            .column_by_name("n")
            .expect("n")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64")
            .value(0);
        let s = row
            .column_by_name("s")
            .expect("s")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert!(!s.is_null(0));
        assert_eq!(n, FACT_ROWS, "{sql}");
        assert_eq!(s.value(0), FACT_ROWS * (FACT_ROWS - 1) / 2, "{sql}");
    }
}

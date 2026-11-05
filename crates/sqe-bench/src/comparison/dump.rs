//! Write both engines' result sets to disk when they differ, so a value or
//! row diff can be traced without rerunning the query by hand.
//!
//! Off unless `SQE_BENCH_DUMP_DIFFS=<dir>` is set. Each differing query
//! writes `<dir>/<suite>-sf<scale>-<query>.{sql,sqe.txt,trino.txt}`, rows in
//! the order each engine returned them.

use crate::report::CompareStatusReport;
use arrow::util::pretty::pretty_format_batches;
use arrow_array::RecordBatch;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Environment variable naming the dump directory.
pub const DUMP_DIR_ENV: &str = "SQE_BENCH_DUMP_DIFFS";

/// Dump the query and both result sets when `status` is a row or value
/// diff and [`DUMP_DIR_ENV`] is set. Failures are logged, never fatal: a
/// dump must not fail a benchmark sweep.
pub fn dump_if_differs(
    benchmark: &str,
    scale: f64,
    query: &str,
    sql: &str,
    status: &CompareStatusReport,
    sqe: Option<&[RecordBatch]>,
    trino: Option<&[RecordBatch]>,
) {
    if !matches!(
        status,
        CompareStatusReport::RowDiff | CompareStatusReport::ValueDiff
    ) {
        return;
    }
    let Some(dir) = std::env::var_os(DUMP_DIR_ENV).filter(|d| !d.is_empty()) else {
        return;
    };
    let stem = format!("{benchmark}-sf{}-{query}", crate::format_scale(scale));
    match write_dump(Path::new(&dir), &stem, sql, sqe, trino) {
        Ok(path) => info!("  {query} result sets dumped to {}.*", path.display()),
        Err(e) => warn!("  {query} result dump failed: {e}"),
    }
}

fn write_dump(
    dir: &Path,
    stem: &str,
    sql: &str,
    sqe: Option<&[RecordBatch]>,
    trino: Option<&[RecordBatch]>,
) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let base = dir.join(stem);
    std::fs::write(base.with_extension("sql"), sql)?;
    for (side, batches) in [("sqe", sqe), ("trino", trino)] {
        let text = match batches {
            Some(b) => pretty_format_batches(b)?.to_string(),
            None => "(no result)".to_string(),
        };
        std::fs::write(dir.join(format!("{stem}.{side}.txt")), text)?;
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[test]
    fn writes_query_and_both_sides() {
        let dir = tempfile::tempdir().unwrap();
        let base = write_dump(
            dir.path(),
            "tpcds-sf1-q27",
            "SELECT 1",
            Some(&[batch(1)]),
            None,
        )
        .unwrap();
        assert_eq!(base, dir.path().join("tpcds-sf1-q27"));
        let read = |f: &str| std::fs::read_to_string(dir.path().join(f)).unwrap();
        assert_eq!(read("tpcds-sf1-q27.sql"), "SELECT 1");
        assert!(read("tpcds-sf1-q27.sqe.txt").contains("| 1 |"));
        assert_eq!(read("tpcds-sf1-q27.trino.txt"), "(no result)");
    }
}

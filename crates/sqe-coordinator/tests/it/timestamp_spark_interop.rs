//! Timestamps and row-level writes across engines, found against Databricks
//! Unity Catalog and reproduced with Spark: a file another engine wrote has
//! no Arrow schema, so a `timestamptz` column decodes as `"UTC"` where the
//! table schema says `"+00:00"`, and the scan refused the batch. And SQE's
//! copy-on-write DELETE committed a `replace` snapshot, which incremental
//! readers skip as compaction. Needs the integration stack and the Spark
//! container (`./scripts/start-spark-interop.sh`); without the container the
//! test skips with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::{spark_rows, sqe_rows};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_reads_and_deletes_from_a_spark_timestamp_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_reads_and_deletes_from_a_spark_timestamp_table: {reason}");
        return;
    }
    let fq = "default.ts_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ)
             USING iceberg TBLPROPERTIES ('format-version' = '2');
           INSERT INTO rest.{fq} VALUES
             (1, TIMESTAMP '2026-01-02 03:04:05.123456 UTC', TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'),
             (2, TIMESTAMP '1969-12-31 23:59:59 UTC', TIMESTAMP_NTZ '1969-12-31 23:59:59'),
             (3, NULL, NULL);"#
    ))
    .expect("spark writes the timestamp table");

    let (session, handler) = crate::common::setup_handler().await;

    let read = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id, arrow_typeof(ts), CAST(ts AS BIGINT), CAST(ntz AS BIGINT) \
             FROM {fq} ORDER BY id"
        ),
    )
    .await;
    // The predicate runs against the decoded batch inside the scan too.
    let filtered = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id FROM {fq} WHERE ts > TIMESTAMP '2000-01-01 00:00:00 UTC'"),
    )
    .await;
    let delete = handler
        .execute(&session, &format!("DELETE FROM {fq} WHERE id = 2"), None)
        .await;
    let spark_read = spark_rows(&format!(
        "SELECT id, unix_micros(ts), unix_micros(CAST(ntz AS TIMESTAMP)) \
         FROM rest.{fq} ORDER BY id"
    ));
    let operation = spark_rows(&format!(
        "SELECT operation FROM rest.{fq}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    assert_eq!(
        read,
        vec![
            "1|Timestamp(µs, \"+00:00\")|1767323045123456|1767323045123456",
            "2|Timestamp(µs, \"+00:00\")|-1000000|-1000000",
            "3|Timestamp(µs, \"+00:00\")|NULL|NULL",
        ]
    );
    assert_eq!(filtered, vec!["1"]);
    delete.expect("SQE DELETE on the Spark table");
    assert_eq!(
        spark_read,
        vec!["1|1767323045123456|1767323045123456", "3|NULL|NULL"]
    );
    assert_eq!(operation, vec!["overwrite"], "a DELETE is not a compaction");
}

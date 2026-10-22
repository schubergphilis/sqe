//! Iceberg spec bloom keys across engines (#856): Spark configures a table
//! with `write.parquet.bloom-filter-enabled.column.<col>`, SQE inserts into
//! it, and the Parquet footer of SQE's data file carries a bloom filter for
//! exactly those columns. Needs the integration stack and the Spark container
//! (`./scripts/start-spark-interop.sh`); without the container the test skips
//! with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::{spark_rows, split_s3, test_store};
use object_store::ObjectStoreExt;
use parquet::file::reader::FileReader;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bloom_spec_spark_interop_sqe_insert_writes_bloom_v2() {
    sqe_insert_writes_spec_bloom(2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bloom_spec_spark_interop_sqe_insert_writes_bloom_v3() {
    sqe_insert_writes_spec_bloom(3).await;
}

/// No column is `_sk`-shaped, so auto-detection cannot produce a bloom here:
/// every bloom in the footer comes from a spec key.
async fn sqe_insert_writes_spec_bloom(format_version: u8) {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping bloom_spec_spark_interop v{format_version}: {reason}");
        return;
    }
    let fq = format!("default.bloom_spec_from_spark_v{format_version}");
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id BIGINT, name STRING, s STRUCT<x: BIGINT, y: BIGINT>)
             USING iceberg
             TBLPROPERTIES ('format-version' = '{format_version}',
                            'write.parquet.bloom-filter-enabled.column.id' = 'true',
                            'write.parquet.bloom-filter-fpp.column.id' = '0.05',
                            'write.parquet.bloom-filter-enabled.column.s.x' = 'true');"#
    ))
    .expect("spark creates the bloom-configured table");

    let (session, handler) = crate::common::setup_handler().await;
    let insert = format!(
        "INSERT INTO {fq} VALUES \
         (1, 'a', named_struct('x', 10, 'y', 100)), \
         (2, 'b', named_struct('x', 20, 'y', 200)), \
         (3, 'c', named_struct('x', 30, 'y', 300))"
    );
    handler
        .execute(&session, &insert, None)
        .await
        .expect("SQE inserts");

    let files = spark_rows(&format!("SELECT file_path FROM rest.{fq}.files"));
    let count = spark_rows(&format!("SELECT count(*) FROM rest.{fq} WHERE id = 2"));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    assert_eq!(files.len(), 1, "one SQE data file: {files:?}");
    let (bucket, key) = split_s3(&files[0]);
    let bytes = test_store(&bucket)
        .get(&object_store::path::Path::from(key.as_str()))
        .await
        .expect("get data file")
        .bytes()
        .await
        .expect("read data file");
    let reader =
        parquet::file::serialized_reader::SerializedFileReader::new(bytes).expect("parquet footer");
    let mut bloomed = Vec::new();
    for rg in reader.metadata().row_groups() {
        for col in rg.columns() {
            if col.bloom_filter_offset().is_some() {
                bloomed.push(col.column_path().string());
            }
        }
    }
    bloomed.sort();
    bloomed.dedup();
    assert_eq!(
        bloomed,
        vec!["id".to_string(), "s.x".to_string()],
        "columns with a bloom filter in SQE's data file"
    );
    assert_eq!(count, vec!["1"], "Spark reads the SQE-written file");
}

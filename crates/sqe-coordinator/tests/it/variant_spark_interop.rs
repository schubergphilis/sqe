//! Iceberg V3 variants across engines (#849): SQE and Spark 4.1 with
//! Iceberg 1.12 share the Polaris catalog, so each reads the other's
//! format-version 3 tables. Needs the integration stack and the Spark
//! container (`./scripts/start-spark-interop.sh`); without the container the
//! tests skip with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use object_store::ObjectStoreExt;

/// Every row of `sql` as `col|col|...`, a variant cell as its JSON text.
pub(crate) async fn sqe_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    let mut out = Vec::new();
    for batch in &batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| {
                if sqe_core::variant::is_variant_storage(c.data_type()) {
                    sqe_core::variant::to_json(c.as_ref()).expect("variant to JSON")
                } else {
                    c.clone()
                }
            })
            .collect();
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = columns
                .iter()
                .map(|c| crate::common::fmt_val(c.as_ref(), r))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// Spark's TSV output as `col|col|...` rows.
pub(crate) fn spark_rows(sql: &str) -> Vec<String> {
    spark_sql(sql)
        .unwrap_or_else(|e| panic!("spark: {sql}: {e}"))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.replace('\t', "|"))
        .collect()
}

/// A plain `CREATE TABLE` is V3 under the default configuration, so its
/// `JSON` column is a variant. Spark reads the variant and the deletion
/// vector a merge-on-read DELETE wrote.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn spark_reads_an_sqe_json_variant_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping spark_reads_an_sqe_json_variant_table: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_to_spark";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!(
            "CREATE TABLE {fq} (id INT, j JSON) \
             WITH ('write.delete.mode' = 'merge-on-read')"
        ),
        format!(
            r#"INSERT INTO {fq} VALUES (1, '{{"a": {{"b": 2}}, "s": "x"}}'), (2, '[1, 2]'), (3, NULL), (4, '"gone"')"#
        ),
        format!("DELETE FROM {fq} WHERE id = 4"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    let spark = spark_rows(&format!(
        "SELECT id, to_json(j), variant_get(j, '$.a.b', 'int'), \
                typeof(j) \
         FROM rest.{fq} ORDER BY id"
    ));
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    assert_eq!(
        spark,
        vec![
            r#"1|{"a":{"b":2},"s":"x"}|2|variant"#,
            "2|[1,2]|NULL|variant",
            "3|NULL|NULL|variant",
        ],
        "Spark's rows of the SQE-written V3 table"
    );
}

/// Spark writes a V3 table with a `VARIANT` column; SQE reads the values as
/// JSON and the JSON functions take them.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_reads_a_spark_variant_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_reads_a_spark_variant_table: {reason}");
        return;
    }
    let fq = "default.variant_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, v VARIANT) USING iceberg
             TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES
             (1, parse_json('{{"a": {{"b": 2}}, "s": "x"}}')),
             (2, parse_json('[1, 2]')),
             (3, NULL);"#
    ))
    .expect("spark writes the variant table");

    let (session, handler) = crate::common::setup_handler().await;
    let read = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, v, json_get_int(v, 'a', 'b') FROM {fq} ORDER BY id"),
    )
    .await;
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));
    assert_eq!(
        read,
        vec![
            r#"1|{"a":{"b":2},"s":"x"}|2"#,
            "2|[1,2]|NULL",
            "3|NULL|NULL"
        ],
        "SQE's rows of the Spark-written V3 table"
    );
}

/// Spark 4.1 with `write.parquet.shred-variants` writes consistently shaped
/// objects with a `typed_value` group beside `value`. SQE reads the shredded
/// fields back, not just the residual binary.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_reads_a_spark_shredded_variant_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_reads_a_spark_shredded_variant_table: {reason}");
        return;
    }
    let fq = "default.variant_shredded_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, v VARIANT) USING iceberg
             TBLPROPERTIES ('format-version' = '3',
                            'write.parquet.shred-variants' = 'true');
           INSERT INTO rest.{fq} VALUES
             (1, parse_json('{{"a": 1, "s": "x"}}')),
             (2, parse_json('{{"a": 2, "s": "y"}}')),
             (3, parse_json('{{"a": 3, "s": "z", "extra": true}}')),
             (4, parse_json('[1, 2]')),
             (5, NULL);"#
    ))
    .expect("spark writes the shredded variant table");

    // The data file must really be shredded, or this test proves nothing.
    let files = spark_rows(&format!("SELECT file_path FROM rest.{fq}.files"));
    let mut shredded = false;
    for path in &files {
        let (bucket, key) = split_s3(path);
        let store = test_store(&bucket);
        let bytes = store
            .get(&object_store::path::Path::from(key.as_str()))
            .await
            .expect("get data file")
            .bytes()
            .await
            .expect("read data file");
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(bytes)
            .expect("parquet footer");
        let schema = parquet::file::reader::FileReader::metadata(&reader)
            .file_metadata()
            .schema_descr()
            .root_schema()
            .clone();
        shredded |= format!("{schema:?}").contains("typed_value");
    }

    let (session, handler) = crate::common::setup_handler().await;
    let read = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, v, json_get_int(v, 'a'), json_get_str(v, 's') FROM {fq} ORDER BY id"),
    )
    .await;
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));
    assert!(
        shredded,
        "Spark did not shred the variant column in {files:?}"
    );
    assert_eq!(
        read,
        vec![
            r#"1|{"a":1,"s":"x"}|1|x"#,
            r#"2|{"a":2,"s":"y"}|2|y"#,
            r#"3|{"a":3,"extra":true,"s":"z"}|3|z"#,
            "4|[1,2]|NULL|NULL",
            "5|NULL|NULL|NULL",
        ],
        "SQE's rows of the shredded Spark-written V3 table"
    );
}

/// A plain client on the test stack's RustFS, independent of SQE's reader.
pub(crate) fn test_store(bucket: &str) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint("http://localhost:19000")
        .with_region("us-east-1")
        .with_access_key_id("s3admin")
        .with_secret_access_key("s3admin")
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("build test S3 client")
}

/// Split `s3://bucket/key` into the bucket and the key.
pub(crate) fn split_s3(uri: &str) -> (String, String) {
    let rest = uri
        .strip_prefix("s3://")
        .unwrap_or_else(|| panic!("not an s3 uri: {uri}"));
    let (bucket, key) = rest.split_once('/').expect("s3 uri has a key");
    (bucket.to_string(), key.to_string())
}

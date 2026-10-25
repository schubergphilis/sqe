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
        shredded |= parquet_schema(path).await.contains("typed_value");
    }

    let (session, handler) = crate::common::setup_handler().await;
    let read = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, v, json_get_int(v, 'a'), json_get_str(v, 's') FROM {fq} ORDER BY id"),
    )
    .await;
    // Shredded statistics may prune these filters (#870); on a Spark-written
    // file they must still return the rows a full scan returns.
    let mut filtered = Vec::new();
    for filter in [
        "variant_get(v, '$.a', 'bigint') >= 2",
        "2 > try_variant_get(v, '$.a', 'int')",
        "variant_get(v, '$.s', 'string') = 'z'",
        "variant_get(v, '$.a', 'bigint') > 100",
    ] {
        let sql = format!("SELECT count(*), min(id) FROM {fq} WHERE {filter}");
        filtered.push(sqe_rows(&handler, &session, &sql).await.join(","));
    }
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));
    assert_eq!(filtered, vec!["2|2", "1|1", "1|3", "0|NULL"]);
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

/// Spark 4.1 / Iceberg 1.12 writes variant bounds into the manifest: per
/// shredded path, the min and max over the file, and none for a path whose
/// values mix types. SQE skips the files those bounds exclude (#870), and
/// keeps a file without bounds.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_prunes_spark_files_on_variant_bounds() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_prunes_spark_files_on_variant_bounds: {reason}");
        return;
    }
    let fq = "default.variant_bounds_from_spark";
    // `COALESCE(1)`: one data file per INSERT, so three files.
    let insert = |rows: &str| {
        format!(
            "INSERT INTO rest.{fq} SELECT /*+ COALESCE(1) */ id, parse_json(j) \
             FROM VALUES {rows} AS t(id, j);"
        )
    };
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, v VARIANT) USING iceberg
             TBLPROPERTIES ('format-version' = '3',
                            'write.parquet.shred-variants' = 'true');
           {}
           {}
           {}"#,
        insert(r#"(1, '{"a": 1, "s": "a"}'), (2, '{"a": 3, "s": "b"}')"#),
        insert(r#"(3, '{"a": 10, "s": "c"}'), (4, '{"a": 12, "s": "d"}')"#),
        // `$.a` mixes types: Spark writes no bound for it.
        insert(r#"(5, '{"a": 7}'), (6, '{"a": "seven"}')"#),
    ))
    .expect("spark writes the variant table");

    let (session, handler) = crate::common::setup_handler().await;
    let mut got = Vec::new();
    for filter in [
        "try_variant_get(v, '$.a', 'bigint') >= 9",
        // `+ 0` is not a shape the scan prunes on: the full-scan baseline.
        "try_variant_get(v, '$.a', 'bigint') + 0 >= 9",
        "variant_get(v, '$.s', 'string') = 'd'",
        "try_variant_get(v, '$.a', 'int') < 2",
    ] {
        let sql = format!("SELECT count(*), min(id) FROM {fq} WHERE {filter}");
        let rows = sqe_rows(&handler, &session, &sql).await.join(",");
        let (scanned, total) = scan_files(&handler, &session, &sql).await;
        got.push(format!("{rows} files {scanned}/{total}"));
    }
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));
    assert_eq!(
        got,
        vec![
            // The first file's bounds [1, 3] exclude it; the mixed file stays.
            "2|3 files 2/3",
            "2|3 files 3/3",
            // The third file has no `$.s` at all, so no bound: it stays.
            "1|4 files 2/3",
            "1|1 files 2/3",
        ]
    );
}

/// `files_scanned` and `files_total` of the single Iceberg scan in
/// `EXPLAIN FULL sql`.
async fn scan_files(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> (i32, i32) {
    use arrow::array::AsArray;
    let explain = format!("EXPLAIN FULL {sql}");
    let batches = handler
        .execute(session, &explain, None)
        .await
        .expect(&explain);
    let mut out = None;
    for batch in &batches {
        let ops = batch
            .column_by_name("operation")
            .expect("operation")
            .as_string::<i32>();
        let scanned = batch
            .column_by_name("files_scanned")
            .expect("files_scanned");
        let scanned = scanned.as_primitive::<arrow::datatypes::Int32Type>();
        let total = batch.column_by_name("files_total").expect("files_total");
        let total = total.as_primitive::<arrow::datatypes::Int32Type>();
        for r in 0..batch.num_rows() {
            if ops.value(r) == "IcebergScanExec" {
                assert!(out.is_none(), "one scan in {sql}");
                out = Some((scanned.value(r), total.value(r)));
            }
        }
    }
    out.unwrap_or_else(|| panic!("no IcebergScanExec in {explain}"))
}

/// SQE writes shredded variants when the table sets
/// `write.parquet.shred-variants` (#858). Each data file infers its own
/// shredding from its first batch, so the second INSERT shreds `a` as a
/// string where the first shredded it as an integer. Spark 4.1 and SQE read
/// every value back unchanged, and `rewrite_data_files` keeps the shredding.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn spark_reads_an_sqe_shredded_variant_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping spark_reads_an_sqe_shredded_variant_table: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.variant_shredded_to_spark";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!(
            "CREATE TABLE {fq} (id INT, j JSON) \
             WITH ('write.parquet.shred-variants' = 'true')"
        ),
        format!(
            r#"INSERT INTO {fq} VALUES
                 (1, '{{"a": 1, "s": "x", "o": {{"b": true}}}}'),
                 (2, '{{"a": 2, "s": "y", "extra": [1, 2]}}'),
                 (3, '[1, 2]'),
                 (4, NULL)"#
        ),
        format!(r#"INSERT INTO {fq} VALUES (5, '{{"a": "five", "s": "z"}}')"#),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    let files = sqe_rows(
        &handler,
        &session,
        "SELECT file_path FROM table_files('default', 'variant_shredded_to_spark')",
    )
    .await;
    let mut unshredded = Vec::new();
    for path in &files {
        if !parquet_schema(path).await.contains("typed_value") {
            unshredded.push(path.clone());
        }
    }

    let want = vec![
        r#"1|{"a":1,"o":{"b":true},"s":"x"}|1|true"#.to_string(),
        r#"2|{"a":2,"extra":[1,2],"s":"y"}|2|NULL"#.to_string(),
        "3|[1,2]|NULL|NULL".to_string(),
        "4|NULL|NULL|NULL".to_string(),
        r#"5|{"a":"five","s":"z"}|NULL|NULL"#.to_string(),
    ];
    let spark = spark_rows(&format!(
        "SELECT id, to_json(j), try_variant_get(j, '$.a', 'int'), \
                variant_get(j, '$.o.b', 'boolean') \
         FROM rest.{fq} ORDER BY id"
    ));
    let sqe_sql = format!(
        "SELECT id, j, json_get_int(j, 'a'), json_get_bool(j, 'o', 'b') FROM {fq} ORDER BY id"
    );
    let sqe = sqe_rows(&handler, &session, &sqe_sql).await;

    handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{fq}', min_input_files => 2)"),
            None,
        )
        .await
        .expect("rewrite_data_files on a shredded variant table");
    let rewritten = sqe_rows(
        &handler,
        &session,
        "SELECT file_path FROM table_files('default', 'variant_shredded_to_spark')",
    )
    .await;
    let mut rewritten_shredded = true;
    for path in &rewritten {
        rewritten_shredded &= parquet_schema(path).await.contains("typed_value");
    }
    let after = sqe_rows(&handler, &session, &sqe_sql).await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(files.len(), 2, "one data file per INSERT: {files:?}");
    assert!(unshredded.is_empty(), "not shredded: {unshredded:?}");
    assert_eq!(spark, want, "Spark's rows of the shredded SQE table");
    assert_eq!(sqe, want, "SQE's rows of its shredded table");
    assert_eq!(rewritten.len(), 1, "the two files merge: {rewritten:?}");
    assert!(rewritten_shredded, "the rewrite shreds: {rewritten:?}");
    assert_eq!(after, want, "rows after rewrite_data_files");
}

/// The Parquet schema of the data file at `path`, as debug text.
async fn parquet_schema(path: &str) -> String {
    let (bucket, key) = split_s3(path);
    let store = test_store(&bucket);
    let bytes = store
        .get(&object_store::path::Path::from(key.as_str()))
        .await
        .expect("get data file")
        .bytes()
        .await
        .expect("read data file");
    let reader =
        parquet::file::serialized_reader::SerializedFileReader::new(bytes).expect("parquet footer");
    let schema = parquet::file::reader::FileReader::metadata(&reader)
        .file_metadata()
        .schema_descr()
        .root_schema()
        .clone();
    format!("{schema:?}")
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

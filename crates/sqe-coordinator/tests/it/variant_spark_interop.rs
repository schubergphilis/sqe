//! Iceberg V3 variants across engines (#849): SQE and Spark 4.1 with
//! Iceberg 1.12 share the Polaris catalog, so each reads the other's
//! format-version 3 tables. Needs the integration stack and the Spark
//! container (`./scripts/start-spark-interop.sh`); without the container the
//! tests skip with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};

/// Every row of `sql` as `col|col|...`, a variant cell as its JSON text.
async fn sqe_rows(
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
fn spark_rows(sql: &str) -> Vec<String> {
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

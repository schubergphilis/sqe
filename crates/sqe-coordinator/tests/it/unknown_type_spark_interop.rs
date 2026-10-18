//! Iceberg V3 `unknown` columns across engines: Spark 4.1 with Iceberg 1.12
//! declares a `VOID` column, SQE reads it as all-NULL, inserts around it and
//! promotes it to a concrete type without rewriting the existing files.
//! Needs the integration stack and the Spark container
//! (`./scripts/start-spark-interop.sh`); without the container the test skips
//! with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::{spark_rows, sqe_rows};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn unknown_column_reads_writes_and_promotes() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping unknown_column_reads_writes_and_promotes: {reason}");
        return;
    }
    let fq = "default.unknown_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, u VOID) USING iceberg
             TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES (1, NULL), (2, NULL);"#
    ))
    .expect("spark writes the unknown-column table");

    let (session, handler) = crate::common::setup_handler().await;
    let declared = sqe_rows(
        &handler,
        &session,
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_schema = 'default' AND table_name = 'unknown_from_spark' \
         ORDER BY ordinal_position",
    )
    .await;
    let before = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u, u IS NULL FROM {fq} ORDER BY id"),
    )
    .await;
    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (3, NULL)"),
            None,
        )
        .await
        .expect("SQE inserts around the unknown column");
    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ALTER COLUMN u SET DATA TYPE INT"),
            None,
        )
        .await
        .expect("SQE promotes unknown to int");
    handler
        .execute(&session, &format!("INSERT INTO {fq} VALUES (4, 40)"), None)
        .await
        .expect("SQE inserts into the promoted column");
    let after = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u FROM {fq} ORDER BY id"),
    )
    .await;
    let spark = spark_rows(&format!(
        "SELECT id, u, typeof(u) FROM rest.{fq} ORDER BY id"
    ));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    // DataFusion's information_schema names Arrow types; unknown is Null.
    assert_eq!(declared, vec!["id|Int32", "u|Null"], "declared types");
    assert_eq!(before, vec!["1|NULL|true", "2|NULL|true"], "Spark's rows");
    assert_eq!(
        after,
        vec!["1|NULL", "2|NULL", "3|NULL", "4|40"],
        "rows after the promotion"
    );
    assert_eq!(
        spark,
        vec!["1|NULL|int", "2|NULL|int", "3|NULL|int", "4|40|int"],
        "Spark's rows after SQE promoted the column"
    );
}

/// SQE declares the `unknown` column itself (`UNKNOWN`, or Spark's `VOID`),
/// writes rows around it, then promotes it; Spark reads every stage.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_declares_an_unknown_column_spark_reads_it() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_declares_an_unknown_column_spark_reads_it: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.unknown_to_spark";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, u UNKNOWN, w VOID)"),
        format!("INSERT INTO {fq} VALUES (1, NULL, NULL), (2, NULL, NULL)"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let declared = spark_rows(&format!(
        "SELECT id, typeof(u), typeof(w) FROM rest.{fq} ORDER BY id"
    ));
    for sql in [
        format!("ALTER TABLE {fq} ALTER COLUMN u SET DATA TYPE VARCHAR"),
        format!("INSERT INTO {fq} VALUES (3, 'x', NULL)"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }
    let promoted = spark_rows(&format!("SELECT id, u, w FROM rest.{fq} ORDER BY id"));
    let sqe = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, u, w FROM {fq} ORDER BY id"),
    )
    .await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        declared,
        vec!["1|void|void", "2|void|void"],
        "Spark's types"
    );
    assert_eq!(
        promoted,
        vec!["1|NULL|NULL", "2|NULL|NULL", "3|x|NULL"],
        "Spark's rows after the promotion"
    );
    assert_eq!(sqe, promoted, "SQE's rows match Spark's");
}

/// An `unknown` field nested in a struct is not supported yet: SQE refuses
/// to write one rather than produce a Null parquet column Spark cannot read.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn nested_unknown_field_is_refused_at_write() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping nested_unknown_field_is_refused_at_write: {reason}");
        return;
    }
    let fq = "default.nested_unknown_from_spark";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, s STRUCT<a: INT, u: VOID>) USING iceberg
             TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES (1, named_struct('a', 10, 'u', NULL));"#
    ))
    .expect("spark writes the nested unknown table");

    let (session, handler) = crate::common::setup_handler().await;
    let insert = format!("INSERT INTO {fq} VALUES (2, named_struct('a', 20, 'u', NULL))");
    let refused = handler.execute(&session, &insert, None).await;
    let spark = spark_rows(&format!("SELECT id, s.a, s.u FROM rest.{fq} ORDER BY id"));
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    let Err(err) = refused else {
        panic!("the nested write is refused");
    };
    let err = format!("{err:?}");
    assert!(err.contains("nested unknown field"), "{err}");
    assert_eq!(spark, vec!["1|10|NULL"], "Spark's table is untouched");
}
